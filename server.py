"""
Веб-обёртка над HdRezkaApi + источник Kinogo.

Запуск:
    python3 server.py
Затем открой http://127.0.0.1:8000/

Источники:
    kinogo  - https://kinogo.sarl  (работает без логина, встроенный плеер)
    hdrezka - зеркало hdrezka (страницы фильмов часто требуют логина)

Переменные окружения:
    KINOGO_ORIGIN    - зеркало Киного (задаётся окружением)
    HDREZKA_ORIGIN   - зеркало hdrezka (задаётся окружением)
    HDREZKA_USER_ID / HDREZKA_PASSWORD_HASH - cookies hdrezka (если есть аккаунт)
    HDREZKA_PROXY    - свой прокси для страниц сайта; off/none/direct — отключить
    PROXY_GATEWAY    - шлюз прокси-провайдера: prt (по умолчанию) | usa | bra | nzl
    VIDEO_PROXY      - прокси для видеосегментов (по умолчанию — напрямую
                       с CDN источника; задайте URL, чтобы пустить через прокси)
    PROXY_CONN_LIMIT - сколько запросов к прокси держать одновременно (по умолчанию 4;
                       0 — без ограничения). Провайдер отвечает 429 при перегрузе.
    RELAY_ORIGIN     - внешний релей видеопотока (Cloudflare Worker и т.п.); задан ->
                       плеер тянет видео мимо Railway (egress Railway ≈ 0), а /api/proxy
                       остаётся запасным путём при сбое релея. Пусто — только /api/proxy.
    FIREBASE_WEB_API_KEY         - публичный Web API key для Firebase Auth клиента
    FIREBASE_SERVICE_ACCOUNT_JSON - приватный JSON Service Account (только Railway)
    FIREBASE_DB_URL              - URL Realtime Database
    FIREBASE_ROOT                - корневой узел Realtime Database (по умолчанию kino88)
    PORT             - порт сервера (по умолчанию 8000)
    HOST             - адрес привязки (по умолчанию 0.0.0.0)

Деплой (Railway): команда запуска в Procfile -> gunicorn server:app.
"""
import io
import base64
import hashlib
import hmac
import json
import os
import random
import re
import secrets
import time
import threading
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse, urljoin, quote

import requests
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup
from flask import Flask, request, jsonify, Response, send_file, stream_with_context

try:  # gzip/brotli-сжатие ответов (HTML, JSON) — быстрее загрузка на мобильных
    from flask_compress import Compress
except Exception:
    Compress = None

try:  # ресайз постеров на сервере: карточки получают резкие WebP вместо «мыла»
    from PIL import Image, ImageFilter
except Exception:
    Image = ImageFilter = None

from HdRezkaApi import HdRezkaApi
from HdRezkaApi.types import default_headers

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

KINOGO_ORIGIN = os.environ.get("KINOGO_ORIGIN", "").strip().rstrip("/")
_hdrezka_origin = os.environ.get("HDREZKA_ORIGIN", "").strip().rstrip("/")
HDREZKA_ORIGIN = _hdrezka_origin + "/" if _hdrezka_origin else ""
# Прокси-провайдер: в коде ничего не зашито, всё берётся из окружения.
#   PROXY_GATEWAYS="usa=1.2.3.4,bra=5.6.7.8" — шлюзы по имени (для ротации);
#   PROXY_LOGIN / PROXY_PASSWORD / PROXY_PORT — общие креды шлюзов;
#   HDREZKA_PROXY / VIDEO_PROXY (см. ниже) — готовый прокси-URL целиком.
def _parse_gateways(raw):
    out = {}
    for part in (raw or "").split(","):
        part = part.strip()
        if not part:
            continue
        name, sep, host = part.partition("=")
        if not sep:
            host = name
        name, host = name.strip().lower(), host.strip()
        if name and host:
            out[name] = host
    return out


PROXY_GATEWAYS = _parse_gateways(os.environ.get("PROXY_GATEWAYS", ""))
PROXY_LOGIN = os.environ.get("PROXY_LOGIN", "").strip()
PROXY_PASSWORD = os.environ.get("PROXY_PASSWORD", "").strip()
PROXY_PORT = os.environ.get("PROXY_PORT", "").strip()


def _proxy_for(gateway):
    host = PROXY_GATEWAYS.get(gateway) or next(iter(PROXY_GATEWAYS.values()), "")
    if not host:
        return ""
    auth = f"{PROXY_LOGIN}:{PROXY_PASSWORD}@" if PROXY_LOGIN else ""
    port = f":{PROXY_PORT}" if PROXY_PORT else ""
    return f"http://{auth}{host}{port}"


def _pick_proxy():
    """Прокси для страниц сайта (на Railway датацентровый IP часто закрыт)."""
    raw = os.environ.get("HDREZKA_PROXY", "").strip()
    if raw.lower() in ("off", "none", "direct"):
        return ""
    if raw:
        return raw
    return _proxy_for(os.environ.get("PROXY_GATEWAY", "usa").strip().lower())


def _proxy_candidates():
    """Шлюзы провайдера по порядку: один отвечает 429 — пробуем следующий.

    Лимит одновременных соединений провайдер считает на конкретный вход, поэтому
    когда первый шлюз перегружен, соседний часто ещё отвечает.
    """
    if not PROXY:
        return []
    if os.environ.get("HDREZKA_PROXY", "").strip():   # свой прокси — ротации нет
        return [PROXY]
    active = os.environ.get("PROXY_GATEWAY", "usa").strip().lower()
    order = [active] + [g for g in PROXY_GATEWAYS if g != active]
    out = []
    for gw in order:
        u = _proxy_for(gw)
        if u not in out:
            out.append(u)
    return out


PROXY = _pick_proxy()
# Видео по умолчанию идёт НАПРЯМУЮ с CDN источника — прокси для потока не нужен.
# Если прокси для видео всё же нужен, задайте VIDEO_PROXY=<url> явно.
_video_raw = os.environ.get("VIDEO_PROXY", "").strip()
if _video_raw.lower() in ("off", "none", "direct"):
    VIDEO_PROXY = ""
elif _video_raw:
    VIDEO_PROXY = _video_raw
else:
    VIDEO_PROXY = ""

# Транспорт видео:
#   on (по умолчанию) — всегда напрямую с CDN источника;
#   auto — сначала напрямую, при блокировке CDN — через прокси;
#   off — всегда через прокси.
VIDEO_DIRECT = (os.environ.get("VIDEO_DIRECT", "on").strip().lower() or "on")

# Внешний релей потока (Cloudflare Worker и т.п.). Задан — браузер тянет видео
# напрямую с релея, минуя Railway (egress ≈ 0); /api/proxy остаётся запасным путём.
RELAY_ORIGIN = os.environ.get("RELAY_ORIGIN", "").strip().rstrip("/")
FIREBASE_WEB_API_KEY = os.environ.get("FIREBASE_WEB_API_KEY", "").strip()
FIREBASE_DB_URL = os.environ.get("FIREBASE_DB_URL", "").strip().rstrip("/")
FIREBASE_ROOT = os.environ.get("FIREBASE_ROOT", "kino88").strip().strip("/") or "kino88"

app = Flask(__name__, static_folder=None)
app.config["MAX_CONTENT_LENGTH"] = 64 * 1024

_auth_rate_lock = threading.Lock()
_auth_rate = defaultdict(deque)


def _auth_rate_limited():
    """Small per-process brake for password endpoints."""
    key = request.remote_addr or "unknown"
    now = time.monotonic()
    with _auth_rate_lock:
        q = _auth_rate[key]
        while q and now - q[0] > 60:
            q.popleft()
        if len(q) >= 30:
            return True
        q.append(now)
        if len(_auth_rate) > 2048:
            for old_key, old_q in list(_auth_rate.items()):
                while old_q and now - old_q[0] > 60:
                    old_q.popleft()
                if not old_q:
                    _auth_rate.pop(old_key, None)
    return False


def _auth_json_error(message, status):
    response = jsonify({"ok": False, "error": message})
    response.headers["Cache-Control"] = "no-store"
    return response, status


def _auth_login_value(value):
    return bool(re.fullmatch(r"[a-z0-9_-]{3,20}", value or ""))


def _password_record(password):
    salt = secrets.token_bytes(16)
    iterations = 150000
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations, 32)
    return {
        "salt": base64.b64encode(salt).decode("ascii"),
        "hash": base64.b64encode(digest).decode("ascii"),
        "it": iterations,
    }


def _password_matches(password, record):
    try:
        salt = base64.b64decode(record["salt"], validate=True)
        expected = base64.b64decode(record["hash"], validate=True)
        iterations = int(record.get("it") or 150000)
        if len(salt) != 16 or len(expected) != 32 or not 100000 <= iterations <= 300000:
            return False
    except (KeyError, TypeError, ValueError):
        return False
    actual = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations, 32)
    return hmac.compare_digest(actual, expected)


def _firebase_auth_ready():
    return bool(
        FIREBASE_WEB_API_KEY
        and FIREBASE_DB_URL
        and FIREBASE_ROOT
        and os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON", "").strip()
    )

# собственная аналитика: /api/track (приём) и /admin/stats (дашборд, нужен ADMIN_TOKEN)
try:
    from analytics import bp as analytics_bp
    app.register_blueprint(analytics_bp)
except Exception as _e:
    print("analytics отключена:", _e)

if Compress is not None:
    # сжимаем только текст/JSON — видео-сегменты и плейлисты идут как есть
    app.config["COMPRESS_MIMETYPES"] = [
        "text/html", "text/css", "application/json",
        "application/javascript", "image/svg+xml",
    ]
    app.config["COMPRESS_MIN_SIZE"] = 1024
    Compress(app)

# ------------------------------------------------------------------ кэш
_cache = {}
_cache_lock = threading.Lock()


def cache_get(key, ttl):
    with _cache_lock:
        item = _cache.get(key)
        if item and time.time() - item[0] < ttl:
            return item[1]
    return None


def cache_set(key, value):
    with _cache_lock:
        _cache[key] = (time.time(), value)
    return value


# ------------------------------------------------------------------ общее
def make_proxy():
    return {"http": PROXY, "https": PROXY} if PROXY else {}


def make_video_proxy():
    return {"http": VIDEO_PROXY, "https": VIDEO_PROXY} if VIDEO_PROXY else {}


# CDN видео (в т.ч. hdrezka) фильтрует по TLS-отпечатку: обычный requests
# получает 410 Gone, а curl_cffi с имперсонацией — 200. Сессия на поток:
# keep-alive для сегментов + потокобезопасность.
_video_local = threading.local()

# Какой транспорт работает для конкретного потока: hdrezka-сегменты требуют
# TLS-имперсонацию curl_cffi, а kinogo — обычный requests (оба на одном CDN).
# Запоминаем результат по «корню» потока (папка .../*.mp4/ или каталог mpd),
# чтобы не делать холостой запрос на каждый сегмент.
_transport_memo = {}
_transport_lock = threading.Lock()

# Нужен ли прокси для конкретного потока: None — неизвестно, True/False — установлено.
# В режиме auto прямой доступ пробуем первым, а при блокировке CDN запоминаем это,
# чтобы не делать холостую попытку на каждый следующий сегмент.
_video_proxy_memo = {}
_video_proxy_lock = threading.Lock()
_direct_state = {"strikes": 0, "off_until": 0.0}

# Прокси-провайдер режет по лимиту одновременных соединений: при пачке
# параллельных запросов он отвечает HTTP 429 на CONNECT, и видео перестаёт
# грузиться/перематываться. Телефон (iPhone, нативный HLS) открывает сразу
# несколько соединений, поэтому упираемся в лимит. Пропускаем через прокси не
# больше PROXY_CONN_LIMIT одновременных запросов (0 — без ограничения).
try:
    PROXY_CONN_LIMIT = int(os.environ.get("PROXY_CONN_LIMIT", "4") or 4)
except ValueError:
    PROXY_CONN_LIMIT = 4
_proxy_slots = (threading.BoundedSemaphore(PROXY_CONN_LIMIT)
                if PROXY_CONN_LIMIT > 0 and VIDEO_PROXY else None)


def _stream_root(url):
    m = re.match(r"^(.*?\.mp4/)", url)
    if m:
        return m.group(1)
    p = urlparse(url)
    return p.scheme + "://" + p.netloc + p.path.rsplit("/", 1)[0] + "/"


def _memo_set(root, value):
    with _transport_lock:
        if len(_transport_memo) > 4096:  # не даём словарю расти бесконечно
            _transport_memo.clear()
        _transport_memo[root] = value


def hint_transport(url, source):
    """Подсказка от плеера (src=kinogo|hdrezka): выбирает транспорт сразу,
    без холостого запроса на первый сегмент."""
    if _cffi is None or not url or source not in ("kinogo", "hdrezka"):
        return
    with _transport_lock:
        if len(_transport_memo) > 4096:
            _transport_memo.clear()
        _transport_memo.setdefault(_stream_root(url), source == "hdrezka")


def _direct_allowed():
    if VIDEO_DIRECT == "off":
        return False
    if VIDEO_DIRECT == "on":
        return True
    with _video_proxy_lock:
        return time.time() >= _direct_state["off_until"]


def _proxy_memo_set(root, value):
    with _video_proxy_lock:
        if len(_video_proxy_memo) > 4096:
            _video_proxy_memo.clear()
        _video_proxy_memo[root] = value


def _note_direct(ok):
    with _video_proxy_lock:
        if ok:
            _direct_state["strikes"] = 0
        else:
            _direct_state["strikes"] += 1
            if _direct_state["strikes"] >= 3:  # CDN стабильно режет — 15 мин не пробуем
                _direct_state["off_until"] = time.time() + 900


def _note_proxy_limited():
    """Прокси ответил 429/5xx — снимаем паузу с прямого доступа: не бьём дальше
    по уже перегруженному прокси, ещё раз пробуем напрямую."""
    with _video_proxy_lock:
        _direct_state["off_until"] = 0.0
        _direct_state["strikes"] = 0


# 429 — прокси-провайдер режет по лимиту; 407/408/5xx — временный сбой на пути.
_RETRY_CODES = {407, 408, 425, 429, 500, 502, 503, 504}


def _video_ok(r):
    """Годен ли ответ как видеопоток (а не страница-заглушка CDN или ошибка прокси)."""
    if r is None or r.status_code in (403, 404, 410, 451) or r.status_code in _RETRY_CODES:
        return False
    return "text/html" not in (r.headers.get("Content-Type") or "").lower()


def _retry_get(fn, attempts=3, delay=0.5):
    """GET с повтором при временных ошибках (лимит прокси, 5xx).

    Между попытками пауза с ростом — короткие всплески 429 так проходят.
    Возвращает последний ответ; если все попытки упали с исключением — бросает его.
    """
    last, last_exc = None, None
    for i in range(attempts):
        last_exc = None
        try:
            last = fn()
        except Exception as e:
            last, last_exc = None, e
        if last is not None and last.status_code not in _RETRY_CODES:
            return last
        if i + 1 < attempts:
            if last is not None:      # промежуточный «плохой» ответ не держим
                last.close()
                last = None
            time.sleep(delay * (i + 1))
    if last is not None:
        return last
    if last_exc is not None:
        raise last_exc
    return None


def _cffi_session(proxied, proxy_url=None):
    # Сессия на (поток, транспорт): keep-alive для сегментов. Для прокси сессия
    # отдельная на каждый шлюз, чтобы можно было перебирать их без пересоздания.
    sessions = getattr(_video_local, "sessions", None)
    if sessions is None:
        sessions = {}
        _video_local.sessions = sessions
    if proxied:
        purl = proxy_url or VIDEO_PROXY
        if not purl:
            return None
        key, cfg = "p:" + purl, {"http": purl, "https": purl}
    else:
        key, cfg = "direct", None
    s = sessions.get(key)
    if s is None:
        s = _cffi.Session(impersonate=HDREZKA_IMPERSONATE, proxies=cfg)
        sessions[key] = s
    return s


def _cffi_try(headers, url, stream, timeout, proxied, proxy_url=None):
    try:
        s = _cffi_session(proxied, proxy_url)
        if s is None:
            return None
        return s.get(url, headers=headers, stream=stream, timeout=timeout)
    except Exception:
        return None


def _video_proxy_candidates():
    """Шлюзы для видео по порядку. Провайдер режет по лимиту соединений и отдельные
    входы отваливаются, поэтому сегменты/плейлисты так же перебирают шлюзы, как и
    страницы каталога (иначе один мёртвый вход ломает и воспроизведение, и перемотку)."""
    if not VIDEO_PROXY:
        return []
    explicit = os.environ.get("VIDEO_PROXY", "").strip()
    if explicit and explicit.lower() not in ("off", "none", "direct"):
        return [VIDEO_PROXY]              # свой прокси — без ротации
    return _proxy_candidates() or [VIDEO_PROXY]


def video_get(url, headers=None, stream=False, timeout=30):
    headers = dict(headers or {})
    if _cffi is not None:
        root = _stream_root(url)
        # CDN stravers пускает только с прямым подключением (прокси он режет),
        # поэтому здесь транспорт не «отравляем»: иначе один запрос без ref
        # навсегда (до рестарта) уводит поток в прокси и фильм отваливается.
        stravers = _is_stravers_host(url)
        with _transport_lock:
            prefer = _transport_memo.get(root)
        with _video_proxy_lock:
            need_proxy = _video_proxy_memo.get(root)
        if stravers:
            # для stravers единственно рабочий транспорт — прямой curl_cffi;
            # что бы ни записали прошлые неудачи/чужие запросы, идём прямым путём
            need_proxy = False
            prefer = None
        if prefer is not False:  # неизвестно или известно, что нужен curl_cffi
            # User-Agent задаёт имперсонация: свой UA конфликтует с TLS-отпечатком
            # (CDN отвечает 410), поэтому его выкидываем.
            cffi_headers = dict(headers)
            cffi_headers.pop("User-Agent", None)

            # 1) прямое подключение — обычно в разы быстрее прокси. Для stravers
            # прямой доступ обязателен (прокси он режет), поэтому общий брейкер
            # _direct_allowed() тут не применяем.
            if (stravers or _direct_allowed()) and need_proxy is not True:
                r = _cffi_try(cffi_headers, url, stream, min(timeout, 6), proxied=False)
                if _video_ok(r):
                    _note_direct(True)
                    _proxy_memo_set(root, False)
                    if prefer is None:
                        _memo_set(root, True)
                    return r
                if r is not None:
                    r.close()
                if VIDEO_PROXY and not stravers:
                    _note_direct(False)
                    if VIDEO_DIRECT != "on":
                        _proxy_memo_set(root, True)

            # 2) через прокси (прямой доступ закрыт или выключен). Перебираем
            # шлюзы: провайдер режет по лимиту (429) и отдельные входы отваливаются,
            # а для мобильного HLS это ломает и воспроизведение, и перемотку.
            if VIDEO_PROXY and VIDEO_DIRECT != "on" and not stravers:
                for purl in _video_proxy_candidates():
                    r = _retry_get(
                        lambda p=purl: _cffi_try(cffi_headers, url, stream, timeout,
                                                 proxied=True, proxy_url=p),
                        attempts=2, delay=0.5)
                    if _video_ok(r):
                        _proxy_memo_set(root, True)
                        if prefer is None:
                            _memo_set(root, True)
                        return r
                    if r is not None:
                        if r.status_code in _RETRY_CODES:
                            _note_proxy_limited()
                        r.close()
            _memo_set(root, False)
    return _retry_get(
        lambda: _get_session().get(url, headers=headers or None, stream=stream,
                                   timeout=timeout, proxies=make_video_proxy()),
        attempts=3, delay=0.6)


# общий пул keep-alive соединений: убирает TLS-рукопожатие на каждом
# запросе к источнику/прокси (медиа-запросы ускоряются примерно втрое)
_session = None
_session_lock = threading.Lock()


def _get_session():
    global _session
    if _session is None:
        with _session_lock:
            if _session is None:
                s = requests.Session()
                adapter = HTTPAdapter(pool_connections=64, pool_maxsize=64, max_retries=0)
                s.mount("http://", adapter)
                s.mount("https://", adapter)
                _session = s
    return _session


def get_hdrezka_cookies():
    c = {}
    uid = os.environ.get("HDREZKA_USER_ID")
    phash = os.environ.get("HDREZKA_PASSWORD_HASH")
    if uid and phash:
        c.update(HdRezkaApi.make_cookies(uid, phash))
    return c


def detect_source(url):
    host = urlparse(url).netloc.lower()
    return "kinogo" if "kinogo" in host else "hdrezka"


def json_error(message, code=500):
    return jsonify({"ok": False, "error": message}), code


def json_cached(payload, ttl):
    """JSON-ответ с разрешением браузерного кэша (повторное открытие — мгновенно)."""
    resp = jsonify(payload)
    resp.headers["Cache-Control"] = "private, max-age=%d" % ttl
    return resp


def _direct_get(url, headers, timeout):
    return _retry_get(
        lambda: _get_session().get(url, headers=headers,
                                   timeout=min(timeout, DIRECT_TIMEOUT)),
        attempts=1, delay=0.6)


def http_get(url, timeout=25, video=False, use_proxy=True, referer=None):
    # use_proxy=False — идём напрямую: страницы плееров-эмбедов (nextembed,
    # ortified и пр.) блокируют IP прокси и отдают 422 с пустым телом.
    # referer — страницы плееров (stravers и пр.) отдают 404 без Referer:
    # с ним в iframe приходит и наш origin, и плеер открывается штатно.
    headers = default_headers
    if referer:
        headers = dict(default_headers)
        headers["Referer"] = _header_safe(referer)
    if not use_proxy:
        return _retry_get(
            lambda: _get_session().get(url, headers=headers, timeout=timeout),
            attempts=1, delay=0.6)
    proxies = make_video_proxy() if video else make_proxy()
    if not proxies:
        return _retry_get(
            lambda: _get_session().get(url, headers=headers, timeout=timeout),
            attempts=1, delay=0.6)
    # На «домашнем» IP источник открыт напрямую и отвечает в разы быстрее прокси,
    # а прокси-провайдер режет по лимиту соединений. Пробуем напрямую и
    # запоминаем результат по хосту (видео не трогаем — у него свой транспорт).
    host = urlparse(url).netloc
    if not video and _hd_mode_of(host) != "proxy":
        try:
            r = _direct_get(url, headers, timeout)
        except Exception:
            r = None
        if r is not None and r.status_code < 400 and len(r.content) > 200:
            _hd_note_mode(host, True)
            return r
        _hd_note_mode(host, False)
    return _retry_get(
        lambda: _get_session().get(url, headers=default_headers, timeout=timeout,
                                   proxies=proxies),
        attempts=3, delay=0.6)


# Страницы плееров-эмбедов (nextembed, ortified, stravers …) блокируют IP прокси
# (HTTP 422 с пустым телом), а сам сайт источника закрыт Cloudflare и с
# датацентрового IP (Railway) без прокси не открывается. Поэтому для таких
# страниц пробуем оба транспорта и запоминаем рабочий отдельно по каждому хосту.
# EMBED_TRANSPORT=auto|direct|proxy — принудительный режим, если нужно.
EMBED_TRANSPORT = os.environ.get("EMBED_TRANSPORT", "auto").strip().lower()
_embed_mode = {}
_embed_lock = threading.Lock()

# Заглушка hdrezka: для фильмов без своего источника во вкладке «Альтернатива»
# стоит один и тот же embed, который играет чужой фильм (сейчас — 93196).
# Такие embed-URL запоминаем и не берём: лучше «нет потока», чем чужое видео.
_embed_owner_lock = threading.Lock()
_embed_owner = {}                                   # embed-url -> фильм, что его принёс
_generic_embeds = {"https://api.ortified.ws/embed/movie/93196"}

# Заглушки в списке озвучек/плееров: у части релизов источник подмешивает
# рекламу казино/букмекеров («Dragon Money Studio», «Mostbet», «1xBet»),
# подписывает дорожки именем сайта («HDrezka Studio», «Дубляж HDrezka St.») и
# отдаёт узбекскую/казахскую озвучку канала MOST с вшитой рекламой. В выборе
# плеера и озвучки такие варианты не показываем.
BAD_SOURCE_MARKERS = (
    "dragon money",        # реклама казино
    "hdrezka",             # дорожки-«заглушки» самого сайта
    "most uz", "most kz",  # озвучка канала MOST (с вшитой рекламой)
    "mostbet", "мостбет",  # букмекеры
    "1xbet", "1хбет", "xbet", "хбет",
)


def _is_bad_source_title(title):
    t = (title or "").strip().lower()
    return any(m in t for m in BAD_SOURCE_MARKERS)


def _drop_bad_sources(sources):
    """Убирает источники-рекламу (Dragon Money) и дорожки самого hdrezka."""
    return [s for s in (sources or []) if not _is_bad_source_title(s.get("title"))]


def _drop_bad_players(players):
    """Убирает вкладки-плееры с теми же именами."""
    return [p for p in (players or []) if not _is_bad_source_title(p.get("title"))]


def _film_embeds(players, sources):
    """URL плееров-эмбедов для iframe: клиент открывает страницу плеера напрямую,
    и видео идёт с CDN источника, минуя наш сервер (трафик Railway ≈ 0).

    Первыми — плееры, чей домен совпадает с origin потока (ref): именно они
    отдают рабочий поток. Остальные — запасные, если основной не откроется.
    """
    refs = {urlparse(s["ref"]).netloc for s in (sources or []) if s.get("ref")}
    urls = [p["url"] for p in (players or []) if p.get("url")]
    urls.sort(key=lambda u: 0 if urlparse(u).netloc in refs else 1)
    return urls


def _drop_placeholder_players(players, film_url):
    """Убирает общие embed-заглушки, которые hdrezka подсовывает вместо источника."""
    keep = []
    with _embed_owner_lock:
        for p in players:
            u = p.get("url")
            if not u or u in _generic_embeds:
                continue
            owner = _embed_owner.get(u)
            if owner is None or owner == film_url:
                _embed_owner[u] = film_url
                keep.append(p)
            else:
                # тот же embed отдали другому фильму — это общая заглушка
                _generic_embeds.add(u)
    return keep


# Хосты эмбедов, что отдают 422 на обоих транспортах: страницу у них не получить,
# и повтор на каждой карточке лишь растягивает открытие фильма на секунды.
_embed_dead = set()


def fetch_embed(url, getter, timeout=30):
    """HTML страницы плеера. getter(url, use_proxy=<bool>, timeout=<int>) -> response."""
    host = urlparse(url).netloc
    with _embed_lock:
        if host in _embed_dead:
            return None
    if EMBED_TRANSPORT == "direct":
        order = [False]
    elif EMBED_TRANSPORT == "proxy":
        order = [True]
    else:
        with _embed_lock:
            pref = _embed_mode.get(host)
        order = [True, False] if pref == "proxy" else [False, True]

    for use_proxy in order:
        try:
            r = getter(url, use_proxy=use_proxy, timeout=timeout)
        except Exception as e:
            app.logger.info("embed %s via %s: %s", url, "proxy" if use_proxy else "direct", e)
            continue
        if r.status_code == 200 and r.text and len(r.text) > 200:
            if EMBED_TRANSPORT == "auto":
                with _embed_lock:
                    if len(_embed_mode) > 512:
                        _embed_mode.clear()
                    _embed_mode[host] = "proxy" if use_proxy else "direct"
            return r.text
        app.logger.info("embed %s via %s -> %s (%d байт)", url,
                        "proxy" if use_proxy else "direct", r.status_code, len(r.content))
        if r.status_code == 422:
            # 422 — плеер отказывается отдавать страницу (nextembed и пр.): у этих
            # провайдеров ответ одинаков на обоих транспортах, а медленный прокси
            # тратит на него ещё ~10 с. Помечаем хост и больше не пробуем.
            with _embed_lock:
                _embed_dead.add(host)
            return None
    return None


def abs_url(src):
    if not src:
        return None
    if src.startswith("//"):
        return "https:" + src
    if src.startswith("/"):
        return KINOGO_ORIGIN + src
    return src


# ================================================================== KINOGO
def kinogo_search(query):
    r = _get_session().post(
        KINOGO_ORIGIN + "/",
        data={"do": "search", "subaction": "search", "story": query, "full_search": "1"},
        headers=default_headers, timeout=25, proxies=make_proxy(),
    )
    soup = BeautifulSoup(r.content, "html.parser")
    out = []
    for item in soup.select(".short"):
        a = item.select_one(".short-title")
        if not a:
            continue
        url = a.get("href")
        title = a.get_text(" ", strip=True)
        img = item.select_one(".short-img img") or item.select_one("img")
        image = abs_url(img.get("src")) if img else None
        m = re.search(r"\((\d{4})\)", title)
        out.append({
            "title": title,
            "url": url,
            "image": image,
            "rating": None,
            "year": int(m.group(1)) if m else None,
        })
    return out


def _clean_title(t):
    if not t:
        return None
    t = re.sub(r"\s*смотреть онлайн\s*", " ", t, flags=re.I)
    return re.sub(r"\s+", " ", t).strip(" -–—|")


def _brace_block(text, start):
    """Возвращает подстроку от '{' (начиная с start) до парной '}'."""
    i = text.index("{", start)
    depth = 0
    in_str = None
    j = i
    while j < len(text):
        c = text[j]
        if in_str:
            if c == "\\":
                j += 2
                continue
            if c == in_str:
                in_str = None
        elif c in "\"'":
            in_str = c
        elif c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return text[i:j + 1]
        j += 1
    return text[i:]


def _parse_movie_source(html):
    """Фильм: прямые ссылки (hls/dash/mp4) из блока source:{...}."""
    m = re.search(r"source:\s*\{", html)
    if not m:
        return None
    block = _brace_block(html, m.start())

    urls = dict(re.findall(r"\b(dash|dasha|hls|mp4|file|url)\s*:\s*\"([^\"]+)\"", block))
    names = re.search(r'"?names"?\s*:\s*\[([^\]]*)\]', block)
    audio = re.findall(r'"([^"]*)"', names.group(1)) if names else []
    subs = [
        {"url": u, "name": n}
        for u, n in re.findall(r'\{\s*"url"\s*:\s*"([^"]+)"\s*,\s*"name"\s*:\s*"([^"]+)"\s*\}', block)
    ]
    if not any(k in urls for k in ("hls", "mp4", "dash", "dasha", "file", "url")):
        return None
    return {"hls": urls.get("hls"), "mp4": urls.get("mp4") or urls.get("file"),
            "dash": urls.get("dash") or urls.get("dasha"),
            "audio": audio, "subtitles": subs}


def _page_origin(url):
    parsed = urlparse(url or "")
    if not parsed.netloc:
        return ""
    return "%s://%s" % (parsed.scheme or "https", parsed.netloc)


def _json_array(text, start):
    """Возвращает подстроку сбалансированного JSON-массива, начиная с '['."""
    depth, in_str, esc = 0, False, False
    i = start
    while i < len(text):
        c = text[i]
        if in_str:
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == '"':
                in_str = False
        elif c == '"':
            in_str = True
        elif c == "[":
            depth += 1
        elif c == "]":
            depth -= 1
            if depth == 0:
                return text[start:i + 1]
        i += 1
    return None


def _parse_serial_seasons(html):
    """Сериал: сезоны/эпизоды с прямыми ссылками из массива seasons:[...]."""
    m = re.search(r"\bseasons\s*:\s*\[", html)
    if not m:
        return []
    arr = _json_array(html, html.index("[", m.start()))
    if not arr:
        return []
    try:
        raw = json.loads(arr)
    except Exception:
        return []
    seasons = []
    for s in raw:
        if not isinstance(s, dict):
            continue
        eps = []
        for e in s.get("episodes", []):
            if not isinstance(e, dict) or not (e.get("dash") or e.get("hls")):
                continue
            eps.append({
                "episode": str(e.get("episode")),
                "dash": e.get("dash"),
                "hls": e.get("hls"),
                "mp4": None,
                "audio": [],
                "subtitles": [],
            })
        if eps:
            seasons.append({"season": s.get("season"), "episodes": eps})
    return seasons


def _media_fields(link, subs):
    """Раскладывает прямую ссылку в поля источника по типу: HLS-плейлист (.m3u8)
    или прогрессивный mp4."""
    if link and ".m3u8" in link.lower():
        return {"hls": link, "mp4": None, "dash": None, "audio": [], "subtitles": subs}
    return {"hls": None, "mp4": link, "dash": None, "audio": [], "subtitles": subs}


def _with_quality(source, quality):
    quality = str(quality or "").strip()
    if quality:
        source["quality"] = quality
        title = source.get("title") or "Плеер"
        if quality.casefold() not in title.casefold():
            source["title"] = "%s · %s" % (title, quality)
    return source


def _source_quality_rank(source):
    label = "%s %s" % (source.get("quality") or "", source.get("title") or "")
    if re.search(r"\b(?:CAM(?:[\s.-]?RIP)?|HD[\s.-]?CAM|HQCAM|HD[\s.-]?TS|TS|TC|"
                 r"TELESYNC|TELECINE|DVDSCR|SCREENER)\b",
                 label, re.I) or re.search(
                     r"ЭКРАНК|КАМРИП|ТЕЛЕСИНК|ТЕЛЕКИН|СЪ[ЕЁ]МК.{0,12}(?:В КИНО|С ЭКРАНА)",
                     label, re.I):
        return 0
    if re.search(r"\b(?:BDRIP|BLU[\s.-]?RAY|WEB[\s.-]?DL|WEB[\s.-]?RIP|HDRIP|HDTV|REMUX)\b",
                 label, re.I):
        return 2
    return 1


# Единый плеер: всегда берём лучшую русскую дорожку (дубляж/многоголосый выше
# оригинала и субтитров) в максимальном доступном качестве. Раньше все озвучки
# отдавались списком и пользователь выбирал из них сам.
_RU_DUB_RE = re.compile(r"дубл|многоголос|двухголос|одноголос|закадров|профессиональн|любительск", re.I)
_NON_RU_RE = re.compile(r"украин|укр\.|україн|оригинал|субтитр|не требуется|track \d|^default", re.I)


def _source_lang_rank(source):
    t = (source.get("title") or "").strip().lower()
    if _NON_RU_RE.search(t):
        return 1                      # оригинал/субтитры/украинская — только как запас
    if _RU_DUB_RE.search(t):
        return 3                      # русский дубляж/многоголосый
    return 2                          # студия без пометки — обычно русская


def _source_sort_key(source):
    return (_source_lang_rank(source), _source_quality_rank(source))


def _pick_single_source(sources):
    """Оставляет один лучший источник: единый плеер без выбора озвучки."""
    if not sources:
        return []
    best = max(sources, key=_source_sort_key)
    best["title"] = "Плеер"
    return [best]


def _balancer_playlist(filepath, referer, headers):
    """variant.filepath у семейства balancer-api — уже готовая прямая ссылка
    (HLS .m3u8 или прогрессивный .mp4), отдаём её сразу, без обращения по сети.
    Раньше по ней делался лишний запрос с ожиданием JSON: на сериале это сотни
    запросов, и открытие карточки висело минутами. Если источник всё же отдаёт
    JSON с sources[].link — разбираем его."""
    if not filepath:
        return None
    low = filepath.lower()
    if ".m3u8" in low or ".mp4" in low:
        return filepath, []
    try:
        data = _hd_get(filepath, referer=referer, headers=headers, timeout=30).json()
    except Exception:
        return None
    srcs = data.get("sources") or []
    link = next((s.get("link") for s in srcs if isinstance(s, dict) and s.get("link")), None)
    if not link:
        return None
    subs = [{"url": s.get("url"), "name": s.get("name") or s.get("label") or "Субтитры"}
            for s in (data.get("subtitles") or []) if isinstance(s, dict) and s.get("url")]
    return link, subs


def _parse_balancer_player(iframe_url, html):
    """Плееры семейства balancer-api (perepolokha.link, player.shortener2.link …):
    прямые HLS отдаёт их catalog-api.

    Страница игрока — SPA без ссылок в HTML, поэтому берём потоки через API:
    episodes?content-id=<id> -> episodeVariants[].filepath -> JSON с sources[].link.
    Возвращает (sources, seasons).
    """
    m = re.search(r"window\.ENV_BASE_URL\s*=\s*'([^']+)'", html)
    mid = re.search(r"[?&]movie_id=(\d+)", iframe_url)
    tok = re.search(r"DLE-API-TOKEN'\s*:\s*'([^']+)'", html)
    if not (m and mid and tok):
        return [], []
    base = m.group(1).rstrip("/")            # .../proxy/playlists
    origin = "https://" + urlparse(iframe_url).netloc
    hdr = {"DLE-API-TOKEN": tok.group(1), "Origin": origin}
    rid = re.search(r"Iframe-Request-Id'\s*:\s*'([^']+)'", html)
    if rid:
        hdr["Iframe-Request-Id"] = rid.group(1)

    try:
        eps = _hd_get(f"{base}/catalog-api/episodes?content-id={mid.group(1)}",
                      referer=iframe_url, headers=hdr, timeout=30).json()
    except Exception:
        return [], []
    if not isinstance(eps, list):
        return [], []
    eps = [e for e in eps if isinstance(e, dict)]
    from concurrent.futures import ThreadPoolExecutor

    # фильм: одна «серия», каждая озвучка (variant) -> отдельный источник
    if len(eps) == 1:
        variants = [v for v in (eps[0].get("episodeVariants") or []) if isinstance(v, dict)]

        def job(v):
            got = _balancer_playlist(v.get("filepath"), iframe_url, hdr)
            if not got:
                return None
            link, subs = got
            source = {"title": v.get("title") or "Плеер", "ref": origin,
                      **_media_fields(link, subs)}
            return _with_quality(source, v.get("streamQuality") or v.get("quality"))

        out = []
        with ThreadPoolExecutor(max_workers=min(6, len(variants) or 1)) as ex:
            for s in ex.map(job, variants):
                if s:
                    out.append(s)
        return out, []

    # сериал: по эпизоду (первая озвучка) -> seasons
    tasks = []
    for e in eps:
        vs = [v for v in (e.get("episodeVariants") or []) if isinstance(v, dict)]
        if not vs:
            continue
        so = (e.get("season") or {}).get("order")
        tasks.append((0 if so is None else so, e, vs[0]))
    if not tasks:
        return [], []

    def job2(t):
        return t[0], t[1], _balancer_playlist(t[2].get("filepath"), iframe_url, hdr)

    seas = {}
    order = []
    with ThreadPoolExecutor(max_workers=min(6, len(tasks))) as ex:
        for so, e, got in ex.map(job2, tasks):
            if not got:
                continue
            link, subs = got
            if so not in seas:
                seas[so] = []
                order.append(so)
            num = e.get("order")
            seas[so].append({"episode": str(num if num is not None else len(seas[so])),
                             "ref": origin,
                             **_media_fields(link, subs)})
    return [], [{"season": so, "episodes": seas[so]} for so in order if seas.get(so)]


# ---------------------------------------------------------------- stravers
# Плеер stravers (etelstan-as.stravers.live) — основной на hdrezka.my. Прямую
# HLS-ссылку он отдаёт только через свой API: страница игрока содержит
# meta[name=viewporti] (закодированный контент), token и fileList, а поток
# приходит ответом на POST /bnsi/movies/<file-id> с заголовком
# Borth = <device-отпечаток>|<Kx(KD(Kb(viewporti)))>. Сам мастер-плейлист CDN
# (userplay.live и родственные) без заголовков Authorizations/Accepts-Controls/
# Origin отвечает 403 — их добавляет /api/proxy.
STRAVERS_FP = "3d3f8df92bfcf386fa0e4d0274f8284467749bade62f2f71a23c200c41df7844"
STRAVERS_BEARER = (
    "pXzvbyDGLYyB6VkwsWZDv3iMKZtsXNzpzRyxZUcsKHXxsSeaYakbo3hw9mBFRc5VQTpqAX6BW8aDEqyLaHYcXSQi"
    "V6KHYTVTK6MYRphNAy5sBjtrevqkDzKmLqNdfMZGEU9NELjmtKfZy3RNGzCd767sNh1mXEj4tCcvqndHtzmwAbZNkhm4gh"
    "DEasodotMBewypNQ56uotJAQGX11csfeRfBAPk8DcUWWkkqzxca8vbnEw12vUFbBzT6hz8ZB3F3dzUhUXoL2cr1WM1bXQAr"
    "RCS1MUNMz3X5WDMMQoZKxj2AMTRqp7QQX4dDB9B7VzEZTmyFULhm1AcHHMkoMvSVvKYoBoAKLycYAgMHeD4ECJcGEAGpnkJ"
    "hrV57zQ7"
)
# CDN-хост потока -> origin плеера: по нему /api/proxy понимает, каким запросам
# нужны заголовки stravers. Заполняется при разборе карточки.
_stravers_src = {}
_stravers_lock = threading.Lock()


def _stravers_bits(x):
    return x.bit_length()


def _stravers_tz(x, kb):
    if x == 0:
        return kb
    c = 0
    while not (x & 1):
        c += 1
        x >>= 1
    return c


def _stravers_kb(s):
    """Перестановка Kb: раскладывает строку по бакетам bit_length(позиции)."""
    n = len(s)
    if n <= 1:
        return s
    kb = 0
    while (1 << kb) < n:
        kb += 1
    cnt = [0] * (kb + 1)
    for i in range(n):
        cnt[_stravers_bits(i)] += 1
    buckets = [None] * (kb + 1)
    p = 0
    for i in range(kb, -1, -1):
        buckets[i] = list(s[p:p + cnt[i]])
        p += cnt[i]
    occ = [0] * (kb + 1)
    out = []
    for i in range(n):
        b = _stravers_bits(i)
        out.append(buckets[b][occ[b]])
        occ[b] += 1
    return "".join(out)


def _stravers_kd(s):
    """Перестановка KD: как Kb, но позиции группируются по числу младших нулей."""
    n = len(s)
    if n <= 1:
        return s
    kb = 0
    while (1 << kb) < n:
        kb += 1
    cnt = [0] * (kb + 1)
    for i in range(n):
        cnt[_stravers_tz(i, kb)] += 1
    buckets = [None] * (kb + 1)
    p = 0
    for i in range(kb + 1):
        buckets[i] = list(s[p:p + cnt[i]])
        p += cnt[i]
    occ = [0] * (kb + 1)
    out = []
    for i in range(n):
        b = _stravers_tz(i, kb)
        out.append(buckets[b][occ[b]])
        occ[b] += 1
    return "".join(out)


def _stravers_isprime(x):
    if x < 2:
        return False
    if x % 2 == 0:
        return x == 2
    d = 3
    while d * d <= x:
        if x % d == 0:
            return False
        d += 2
    return True


def _stravers_kx(s):
    """Перестановка Kx: обход индексов шагом 2 по простому модулю n+1."""
    n = len(s)
    if n <= 1:
        return s
    p = n + 1
    while not _stravers_isprime(p):
        p += 1
    used = [False] * n
    order = []
    k = 0
    while len(order) < n:
        k = (k + 2) % p
        if k < n and not used[k]:
            order.append(k)
            used[k] = True
    out = [None] * n
    for i in range(n):
        out[order[i]] = s[i]
    return "".join(out)


def _stravers_signature(viewport):
    return _stravers_kx(_stravers_kd(_stravers_kb(viewport)))


def _stravers_headers(url):
    """Заголовки для CDN stravers (без них мастер-плейлист отвечает 403)."""
    origin = _stravers_origin(url)
    if not origin:
        return {}
    return {"Authorizations": "Bearer " + STRAVERS_BEARER,
            "Accepts-Controls": STRAVERS_FP,
            "Origin": origin, "Referer": origin + "/"}


def _stravers_origin(url):
    """origin плеера для CDN-хоста. Карта живёт в памяти процесса, поэтому её
    показания — лишь запасной вариант: основной путь — ref из ссылки плеера."""
    with _stravers_lock:
        return _stravers_src.get(urlparse(url).netloc)


def _is_stravers_host(url):
    """CDN семейства stravers (карта заполняется при разборе карточки или из ref)."""
    with _stravers_lock:
        return urlparse(url).netloc in _stravers_src


def _remember_stravers(url, origin):
    """Запоминаем origin плеера для CDN-хоста в этом процессе: тогда и вложенные
    ссылки плейлиста (которые hls.js/dash.js запрашивают без ref) получат нужные
    заголовки. Так карта не зависит от того, какой воркер разбирал карточку."""
    if not url or not origin:
        return
    with _stravers_lock:
        if len(_stravers_src) > 512:
            _stravers_src.clear()
        _stravers_src[urlparse(url).netloc] = origin


def _stravers_transport(host):
    """Транспорт, которым fetch_embed открыл страницу плеера: stravers обычно
    не пускает прокси-IP, поэтому по умолчанию идём напрямую."""
    with _embed_lock:
        return _embed_mode.get(host) == "proxy"


def _is_stravers_page(html):
    """Страница плеера семейства stravers: скрытый nonce viewporti + fileList.

    Домены плеера постоянно ротируются (stravers.live, stloadi.live,
    allarknow.online, thealloha.club, …), поэтому определяем по содержимому, а
    не по имени хоста — иначе озвучки с новых зеркал теряются."""
    return bool(html) and "viewporti" in html and "fileList" in html


def _stravers_link(iframe_url, fid, referer):
    """Прямая HLS-ссылка файла: POST /bnsi/movies/<id>.

    meta[name=viewporti] — одноразовый nonce: он подписывается в Borth и после
    первого запроса протухает (сервер отвечает 404). Поэтому перед каждым
    файлом заново открываем страницу игрока и берём свежую viewporti + token."""
    host = urlparse(iframe_url).netloc
    origin = "https://" + host
    use_proxy = _stravers_transport(host)
    try:
        page = _hd_request("GET", iframe_url, use_proxy=use_proxy,
                           headers={"Referer": referer}, timeout=30)
    except Exception:
        return None
    html = page.text if page is not None and page.status_code == 200 else None
    if not html:
        return None
    m3 = re.search(r'name="viewporti"\s+content="([^"]+)"', html)
    mt = re.search(r"token:\s*'([^']+)'", html)
    if not (m3 and mt):
        return None
    headers = {
        "Referer": referer, "Origin": origin, "Accept": "*/*",
        "X-Requested-With": "XMLHttpRequest",
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Borth": STRAVERS_FP + "|" + _stravers_signature(m3.group(1)),
    }
    body = "token=%s&av1=true&autoplay=0&audio=&subtitle=" % mt.group(1)
    try:
        r = _hd_request("POST", "%s/bnsi/movies/%s" % (origin, fid), use_proxy=use_proxy,
                        headers=headers, data=body, timeout=30)
    except Exception:
        return None
    if r.status_code != 200:
        return None
    try:
        data = r.json()
    except Exception:
        return None
    sources = data.get("hlsSource") or []
    if not sources:
        return None
    quality = (sources[0] or {}).get("quality") or {}
    keys = [k for k in quality if str(k).isdigit()]
    if keys:
        raw = quality[max(keys, key=int)]
    elif quality:
        raw = next(iter(quality.values()))
    else:
        return None
    if not raw:
        return None
    # в поле качества CDN перечисляет зеркала через ' or ' — берём первое
    link = raw.split(" or ")[0].strip()
    return link or None


def _parse_stravers_player(iframe_url, html, referer=None):
    """Фильм/сериал плеера stravers. fileList перечисляет файлы (озвучки/качества,
    а у сериала — сезон/серия/озвучка); по каждому берём поток через /bnsi.
    Возвращает (sources, seasons)."""
    mt = re.search(r"token:\s*'([^']+)'", html)
    mf = re.search(r"fileList\s*=\s*JSON\.parse\('(.*?)'\)", html, re.S)
    if not (mt and mf):
        return [], []
    try:
        flist = json.loads(mf.group(1))
    except Exception:
        return [], []

    origin = "https://" + urlparse(iframe_url).netloc
    referer = _header_safe(referer or iframe_url)
    is_series = flist.get("type") in ("series", "serial")

    # листья fileList — playable-узлы с числовым id; у фильма ключ (версия, озвучка),
    # у сериала — (сезон, серия)
    groups = {}

    def walk(node):
        if isinstance(node, dict):
            if isinstance(node.get("id"), int) and node.get("translation") is not None:
                key = ((node.get("seasons"), node.get("episode")) if is_series
                       else (node.get("typeList"), node.get("translation")))
                groups.setdefault(key, []).append(node)
                return
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)

    walk(flist.get("all") or {})
    if not groups:
        return [], []

    from concurrent.futures import ThreadPoolExecutor

    def job(kv):
        key, leaves = kv
        leaves.sort(key=lambda x: x.get("id_quality") or 0, reverse=True)
        for leaf in leaves:
            link = _stravers_link(iframe_url, leaf["id"], referer)
            if link:
                return key, leaf, link
        return key, None, None

    got = []
    with ThreadPoolExecutor(max_workers=min(8, len(groups))) as ex:
        for key, leaf, link in ex.map(job, groups.items()):
            if link:
                got.append((key, leaf, link))
    if not got:
        return [], []

    with _stravers_lock:
        if len(_stravers_src) > 512:
            _stravers_src.clear()
        for _, _, link in got:
            _stravers_src[urlparse(link).netloc] = origin

    if not is_series:
        return [_with_quality({
                    "title": leaf.get("translation") or "Плеер",
                    "ref": origin,
                    "stravers": True,          # CDN требует заголовки stravers (см. /api/proxy)
                    **_media_fields(link, [])
                }, leaf.get("quality"))
                for _, leaf, link in got], []

    seas = {}
    for (season, epi), leaf, link in got:
        seas.setdefault(season if season is not None else 1, []).append((epi, link))
    seasons = []
    for so in sorted(seas):
        eps = sorted(seas[so], key=lambda t: t[0] if t[0] is not None else 0)
        seasons.append({"season": so, "episodes": [
            {"episode": str(ep if ep is not None else i + 1), "ref": origin,
             "stravers": True,          # CDN требует заголовки stravers (см. /api/proxy)
             **_media_fields(link, [])}
            for i, (ep, link) in enumerate(eps)]})
    return [], seasons


def kinogo_film(url):
    r = http_get(url)
    soup = BeautifulSoup(r.content, "html.parser")

    title_el = soup.select_one(".ftitle") or soup.select_one("h1")
    raw_title = title_el.get_text(" ", strip=True) if title_el else None
    name = _clean_title(raw_title)
    if name:
        name = re.sub(r"\s*\((\d{4})\)\s*$", "", name)

    year = None
    if raw_title:
        m = re.search(r"\((\d{4})\)", raw_title)
        if m:
            year = int(m.group(1))

    poster_el = soup.select_one(".fposter img") or soup.select_one(".fposter a img")
    poster = abs_url(poster_el.get("src")) if poster_el else None

    desc = None
    desc_el = soup.select_one(".fdesc")
    if desc_el:
        h = desc_el.find(["h2", "h3"])
        if h:
            h.extract()
        desc = re.sub(r"\s+", " ", desc_el.get_text(" ", strip=True)).strip()

    # категории
    cats = []
    for line in soup.select(".finfo .sd-line"):
        label = line.find("span")
        if label and "Категор" in label.get_text():
            for a in line.find_all("a"):
                t = a.get_text(strip=True)
                if t and t not in cats:
                    cats.append(t)

    # рейтинг: виджет хранит процент (80 -> 8.0)
    rating = None
    bar = soup.select_one(".unit-rating .current-rating")
    if bar:
        m = re.search(r"width:\s*(\d+(?:\.\d+)?)", bar.get("style", ""))
        if m:
            rating = round(float(m.group(1)) / 10, 1)
    if rating is None:
        el = soup.select_one('[itemprop="ratingValue"]')
        if el:
            m = re.search(r"\d+(?:[.,]\d+)?", el.get_text())
            if m:
                rating = float(m.group(0).replace(",", "."))

    # плееры
    players = []
    for i, box in enumerate(soup.select(".fplayer iframe")):
        src = box.get("src")
        if src:
            players.append({"title": f"Плеер {i + 1}", "url": abs_url(src)})
    if not players:
        for i, box in enumerate(soup.find_all("iframe")):
            src = box.get("src")
            if src and ("http" in src):
                players.append({"title": f"Плеер {i + 1}", "url": abs_url(src)})

    # прямые потоки (без рекламного iframe): фильм -> source:{...}, сериал -> seasons:[...]
    sources = []
    seasons = []
    ok_players = []                       # плееры, страница которых реально открылась
    if players:
        from concurrent.futures import ThreadPoolExecutor

        def _fetch_player(url):
            # прямой транспорт предпочтителен (плееры банят прокси-IP), но на
            # Railway может понадобиться прокси — fetch_embed пробует оба.
            # Referer обязателен: без него страницы плееров отдают 404.
            return fetch_embed(url, lambda u, use_proxy, timeout: http_get(
                u, timeout=timeout, use_proxy=use_proxy, referer=url), timeout=30)

        with ThreadPoolExecutor(max_workers=min(8, len(players))) as ex:
            pages = list(ex.map(_fetch_player, [p["url"] for p in players]))

        for p, html in zip(players, pages):
            if not html:
                continue
            ok_players.append(p)
            player_origin = _page_origin(p["url"])
            if "stravers" in p["url"] or _is_stravers_page(html):
                # семейство stravers (домены ротируются) — потоки через /bnsi
                ssrc, ssea = _parse_stravers_player(p["url"], html, referer=url)
                sources.extend(ssrc)
                if not seasons and ssea:
                    seasons = ssea
                continue
            src = _parse_movie_source(html)
            if src and (src.get("hls") or src.get("mp4") or src.get("dash")):
                src["title"] = p["title"]
                src["ref"] = player_origin
                sources.append(src)
                continue
            if not seasons:
                seasons = _parse_serial_seasons(html)
                for season in seasons:
                    for episode in season.get("episodes") or []:
                        episode["ref"] = player_origin

    # Экранки не показываем, если есть релиз получше; иначе оставляем как есть,
    # чтобы фильм не остался без потока. Из оставшихся — одна лучшая русская дорожка.
    sources = _drop_bad_sources(sources)
    clean = [s for s in sources if _source_quality_rank(s) > 0]
    sources = _pick_single_source(clean or sources)
    # плееры-эмбеды для iframe: только те, чья страница открылась
    embed_ok = {p["url"] for p in ok_players}
    embeds = _film_embeds([p for p in players if p["url"] in embed_ok], sources)

    if seasons:
        seasons.sort(key=lambda s: s.get("season") or 0)
    is_series = bool(seasons) or "/serial/" in url

    return {
        "source": "kinogo",
        "url": url,
        "origin": KINOGO_ORIGIN,
        "name": name,
        "origName": None,
        "description": desc,
        "releaseYear": year,
        "thumbnail": poster,
        "thumbnailHQ": poster,
        "rating": {"value": rating, "votes": None},
        "type": "video.tv_series" if is_series else "video.movie",
        "category": "category." + (urlparse(url).path.strip("/").split("/")[0] or ""),
        "categories": cats,
        "isSeries": is_series,
        "sources": sources,
        "players": players,
        "embeds": embeds,
        "translators": {},
        "seasons": seasons,
    }


# ================================================================== HDREZKA
# Новый hdrezka.my закрыт Cloudflare: обычный requests получает 403
# («Just a moment»). Обходим через TLS-имперсонацию curl_cffi.
# Домены эмбед-плееров ротируются (ortified.ws / luxembd.ws / nextembed.ws ...),
# но все отдают блок makePlayer({ source:{hls,dash} }) или seasons:[...] —
# оттуда и забираем прямые потоки в наш кастомный плеер.
try:
    from curl_cffi import requests as _cffi
except Exception:  # curl_cffi не установлен — сайт, скорее всего, не откроется
    _cffi = None

HDREZKA_IMPERSONATE = os.environ.get("HDREZKA_IMPERSONATE", "safari17_0")


# Сессия на поток: keep-alive (без нового TLS-рукопожатия к Cloudflare/прокси
# на каждый запрос) + общие cookies Cloudflare внутри потока.
_hd_local = threading.local()
_hd_direct_local = threading.local()


def _hd_session(proxy_url=None):
    """Сессия на (поток, шлюз): keep-alive + cookies Cloudflare внутри потока.

    Отдельная сессия на каждый шлюз — чтобы перебирать входы, не теряя cookies.
    proxy_url="" — напрямую (без прокси).
    """
    if proxy_url is None:
        proxy_url = _pick_proxy() or None
    if not proxy_url:
        s = getattr(_hd_direct_local, "session", None)
        if s is None:
            s = _cffi.Session(impersonate=HDREZKA_IMPERSONATE)
            _hd_direct_local.session = s
        return s
    sessions = getattr(_hd_local, "sessions", None)
    if sessions is None:
        sessions = {}
        _hd_local.sessions = sessions
    s = sessions.get(proxy_url)
    if s is None:
        s = _cffi.Session(impersonate=HDREZKA_IMPERSONATE,
                          proxies={"http": proxy_url, "https": proxy_url})
        sessions[proxy_url] = s
    return s


def _hd_direct_session():
    """Сессия без прокси: эмбед-плееры (nextembed и др.) банят прокси-IP (422)."""
    return _hd_session("")


# Транспорт страниц источника: на «домашнем» IP сайт открыт напрямую и отвечает
# в разы быстрее прокси, а с датацентрового (Railway) закрыт Cloudflare. Пробуем
# напрямую первым и запоминаем по хосту, что сработало: иначе каждый запрос
# платит за пробу, а весь трафик (включая постеры через /api/img) идёт через
# прокси и упирается в его лимит соединений — поиск и карточки встают в очередь.
_hd_mode = {}                     # host -> "direct" | "proxy"
_hd_mode_lock = threading.Lock()

# Сколько ждать прямое подключение, прежде чем счесть хост для нас закрытым.
DIRECT_TIMEOUT = 8


def _hd_mode_of(host):
    with _hd_mode_lock:
        return _hd_mode.get(host)


def _hd_note_mode(host, ok_direct):
    """Запоминает рабочий транспорт. Обратно на «proxy» не переключаемся: раз
    прямое подключение уже работало, разовый сбой не повод уходить на медленный
    прокси до перезапуска сервера."""
    with _hd_mode_lock:
        if ok_direct:
            _hd_mode[host] = "direct"
        elif _hd_mode.get(host) is None and len(_hd_mode) < 256:
            _hd_mode[host] = "proxy"


def _hd_gateways(host=None):
    """Порядок транспортов для хоста: сначала уже проверенный, иначе — напрямую.
    Провайдер режет 429 на CONNECT на конкретном входе, поэтому прокси-шлюзы
    перебираем по очереди."""
    cands = list(_proxy_candidates())
    if _hd_mode_of(host) == "proxy":
        return cands + [""]
    return [""] + cands


def _hd_request(method, url, use_proxy=True, headers=None, data=None, timeout=30):
    """Запрос к hdrezka: напрямую или через прокси, с перебором шлюзов при
    429/5xx и сбое туннеля. Рабочий транспорт запоминается по хосту.
    """
    if headers:
        headers = {k: _header_safe(v) for k, v in headers.items()}
    if _cffi is not None:
        if not use_proxy:
            s = _hd_direct_session()
            return _retry_get(
                lambda: s.request(method, url, headers=headers, data=data, timeout=timeout),
                attempts=1, delay=0.6)
        host = urlparse(url).netloc
        last_resp = last_exc = None
        patient = False           # настойчивый повтор даём первому прокси-шлюзу
        for purl in _hd_gateways(host):
            attempts = 1
            if purl and not patient:
                attempts, patient = 3, True
            # прямое подключение — коротким таймаутом: если хост для нас закрыт,
            # ждать полные 30 с бессмысленно, сразу уходим на прокси
            t = timeout if purl else min(timeout, DIRECT_TIMEOUT)
            s = _hd_session(purl)
            try:
                r = _retry_get(
                    lambda s=s, t=t: s.request(method, url, headers=headers, data=data,
                                               timeout=t),
                    attempts=attempts, delay=0.6)
            except Exception as e:
                last_exc = e
                if not purl:
                    _hd_note_mode(host, False)
                continue
            if r is not None and r.status_code == 200:
                _hd_note_mode(host, not purl)
                return r
            if r is not None:
                if r.status_code in _RETRY_CODES:
                    _note_proxy_limited()
                if last_resp is not None:
                    last_resp.close()
                last_resp = r
        if last_resp is not None:
            return last_resp
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("hdrezka недоступна")
    sess = _get_session()
    proxies = make_proxy() if use_proxy else None
    return _retry_get(
        lambda: sess.request(method, url, headers={**default_headers, **(headers or {})},
                             data=data, timeout=timeout, proxies=proxies),
        attempts=3 if use_proxy else 1, delay=0.6)


def _header_safe(value):
    """curl_cffi кодирует заголовки в latin-1: кириллица в Referer (URL каталога
    вида /21550-однажды-в-россии.html) валит запрос UnicodeEncodeError → 500.
    Процентное кодирование не-ASCII решает проблему."""
    try:
        value.encode("latin-1")
        return value
    except (UnicodeEncodeError, AttributeError):
        return quote(value, safe=":/?#[]@!$&'()*+,;=%")


def _hd_get(url, referer=None, timeout=30, use_proxy=True, headers=None):
    h = {"Referer": _header_safe(referer or HDREZKA_ORIGIN)}
    if headers:
        h.update(headers)
    return _hd_request("GET", url, use_proxy=use_proxy, headers=h, timeout=timeout)


def _hd_post(url, data, referer=None, timeout=30):
    headers = {"Referer": _header_safe(referer or HDREZKA_ORIGIN)}
    return _hd_request("POST", url, use_proxy=True, headers=headers, data=data,
                       timeout=timeout)


def _hd_abs(src):
    if not src:
        return None
    if src.startswith("//"):
        return "https:" + src
    if src.startswith("/"):
        return HDREZKA_ORIGIN.rstrip("/") + src
    return src


def _hd_parse_cards(html):
    """Карточки из #dle-content (листинги каталога и результаты поиска)."""
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for item in soup.select("#dle-content .card_item") or soup.select(".card_item"):
        a = item.select_one("a.card_item__title") or item.select_one("a[href]")
        if not a or not a.get("href"):
            continue
        image = None
        img = item.select_one("img")
        if img:
            image = _hd_abs(img.get("data-src") or img.get("src"))
            if image and image.endswith("net_postera.png"):  # заглушка «нет постера»
                image = None
        year = None
        misc = item.select_one(".card_item__misc")
        if misc:
            m = re.search(r"\b(19|20)\d{2}\b", misc.get_text(" ", strip=True))
            if m:
                year = int(m.group(0))
        out.append({
            "title": a.get_text(" ", strip=True),
            "url": _hd_abs(a["href"]),
            "image": image,
            "rating": None,
            "year": year,
        })
    return out


# У невышедших фильмов на hdrezka нет ни одной озвучки — только трейлер («В
# качестве: Трейлер»), а в плеере единственная заглушка-«Альтернатива». Потока
# нет ни на hdrezka, ни на kinogo, поэтому такие фильмы не показываем в
# каталоге и поиске. Флаг кэшируем по URL.
_trailer_cache = {}
_trailer_lock = threading.Lock()
_trailer_sem = threading.BoundedSemaphore(6)   # не перегружать источник проверками


def hdrezka_is_trailer(url, timeout=20):
    """True, если у фильма нет озвучек, только трейлер. При сбое запроса считаем
    фильм обычным — лучше показать лишний, чем спрятать рабочий."""
    with _trailer_lock:
        known = _trailer_cache.get(url)
    if known is not None:
        return known
    try:
        with _trailer_sem:
            r = _hd_get(url, timeout=timeout)
        html = r.text if r is not None and r.status_code == 200 else ""
    except Exception:
        return False
    m = re.search(r"В качестве:</span>\s*([^<]+)", html)
    trailer = bool(m and "трейлер" in m.group(1).lower())
    with _trailer_lock:
        if len(_trailer_cache) > 4096:
            _trailer_cache.clear()
        _trailer_cache[url] = trailer
    return trailer


def _drop_trailers(items):
    """Убирает из списка невышедшие фильмы (только трейлер)."""
    if not items:
        return items
    from concurrent.futures import ThreadPoolExecutor

    def keep(it):
        try:
            return not hdrezka_is_trailer(it["url"])
        except Exception:
            return True

    with ThreadPoolExecutor(max_workers=min(8, len(items))) as ex:
        return [it for it, ok in zip(items, ex.map(keep, items)) if ok]


def hdrezka_search(query, find_all=False):
    """Поиск по новому hdrezka.my (DLE-поиск через index.php)."""
    r = _hd_post(HDREZKA_ORIGIN + "index.php",
                 {"do": "search", "subaction": "search", "story": query, "search_start": 0},
                 referer=HDREZKA_ORIGIN)
    return _drop_trailers(_hd_parse_cards(r.content))


# Категории каталога hdrezka (xSort фильтрует внутри выбранной категории).
HDREZKA_CATS = {
    "films": "films/",
    "series": "serialy/",
    "cartoons": "multfilmy/",
    "cartoonseries": "multseries/",
    "anime": "anime/",
    "animeseries": "anime-serialy/",
}


def hdrezka_browse(cat="films", genre=None, country=None, year=None, sort=None, page=1):
    """Каталог с фильтрами жанр/год/страна/сортировка (механизм xSort).

    Фильтры копятся в серверной сессии сайта, поэтому на каждый запрос создаём
    отдельную сессию — иначе фильтры разных пользователей перемешаются.
    """
    path = HDREZKA_CATS.get(cat, "films/")
    list_url = HDREZKA_ORIGIN + path
    page = max(1, int(page or 1))
    url = list_url if page == 1 else "%s%spage/%d/" % (HDREZKA_ORIGIN, path, page)

    def _run(proxy_url, attempts):
        headers = {"Referer": HDREZKA_ORIGIN}
        pmap = {"http": proxy_url, "https": proxy_url} if proxy_url else None
        # прямой доступ — коротким таймаутом: закрытый для нас хост не должен
        # держать запрос все 30 с, сразу уходим на прокси
        to = 30 if proxy_url else DIRECT_TIMEOUT
        if _cffi is not None:
            sess = _cffi.Session(impersonate=HDREZKA_IMPERSONATE, proxies=pmap)
            extra = {}
        else:
            sess = _get_session()
            extra = {"proxies": pmap}
        try:
            # провайдер отвечает 429 при всплеске запросов — повторяем с паузой
            _retry_get(lambda: sess.get(list_url, headers=headers, timeout=to, **extra),
                       attempts=attempts, delay=0.6)
            for field, value in (("defaultsort", sort), ("genre", genre),
                                 ("strana", country), ("year", year)):
                if value:
                    _retry_get(lambda f=field, v=value: sess.post(
                        list_url,
                        data={"xsort": 1, "xs_field": f, "xs_value": v},
                        headers={"Referer": list_url, "X-Requested-With": "XMLHttpRequest"},
                        timeout=to, **extra), attempts=attempts, delay=0.6)
            return _retry_get(lambda: sess.get(url, headers=headers, timeout=to, **extra),
                              attempts=attempts, delay=0.6)
        finally:
            if _cffi is not None:
                try:
                    sess.close()
                except Exception:
                    pass

    # Прокси-провайдер режет 429 на CONNECT — раньше каталог падал целиком.
    # Перебираем транспорты (напрямую пробуем первым, если хост нам открыт), по
    # основному прокси-шлюзу повторяем настойчиво, остальные только пробуем.
    host = urlparse(list_url).netloc
    last = None
    patient = False
    for proxy_url in _hd_gateways(host):
        attempts = 1
        if proxy_url and not patient:
            attempts, patient = 3, True
        try:
            r = _run(proxy_url, attempts)
        except Exception as e:
            last = e
            if not proxy_url:
                _hd_note_mode(host, False)
            continue
        if r is not None and r.status_code == 200:
            _hd_note_mode(host, not proxy_url)
            # невышедшие фильмы (только трейлер) в каталоге не показываем
            return _drop_trailers(_hd_parse_cards(r.content))
        if r is not None and r.status_code == 404:
            _hd_note_mode(host, not proxy_url)
            return []          # страница за пределами каталога — это его конец
        last = RuntimeError("hdrezka вернула %s" % (r.status_code if r is not None else "?"))
    raise last if last is not None else RuntimeError("hdrezka недоступна")


def hdrezka_filters(cat="films"):
    """Список доступных значений фильтров (жанры/годы/страны/сортировка)."""
    path = HDREZKA_CATS.get(cat, "films/")
    r = _hd_get(HDREZKA_ORIGIN + path, timeout=30)
    soup = BeautifulSoup(r.content, "html.parser")
    out = {"genre": [], "year": [], "country": [], "sort": []}
    fmap = {"genre": "genre", "year": "year", "strana": "country", "defaultsort": "sort"}
    for ul in soup.select(".xsort-ul"):
        key = fmap.get(ul.get("data-field"))
        if not key:
            continue
        for li in ul.select("li"):
            value = (li.get("data-val") or "").strip()
            if value:  # пустое значение = «все», оно не нужно
                out[key].append({"value": value, "label": li.get_text(" ", strip=True)})
    return out


# ---- TMDb: настоящие HQ-постеры (у источников только ~200×280, апскейл = мыло) ----
# Ключ и язык задаются окружением (TMDB_API_KEY, TMDB_LANG).
TMDB_KEY = os.environ.get("TMDB_API_KEY", "").strip()
TMDB_LANG = os.environ.get("TMDB_LANG", "").strip()
TMDB_API = "https://api.themoviedb.org/3/"
TMDB_IMG = "https://image.tmdb.org/t/p/"

_tmdb_cache = {}
_tmdb_lock = threading.Lock()


def _poster_norm(s):
    return re.sub(r"[^0-9a-zа-яё]+", "", (s or "").lower())


def _tmdb_year(s):
    m = re.search(r"\b(19|20)\d{2}\b", s or "")
    return int(m.group(0)) if m else None


def _tmdb_score(cand, title, year):
    """Насколько кандидат из TMDb похож на наш фильм (год + совпадение названия)."""
    name = cand.get("title") or cand.get("name") or ""
    oname = cand.get("original_title") or cand.get("original_name") or ""
    cyear = _tmdb_year(cand.get("release_date") or cand.get("first_air_date") or "")
    nt, nn, no = _poster_norm(title), _poster_norm(name), _poster_norm(oname)
    score = 0.0
    if nt and (nn == nt or no == nt):
        score += 3
    elif nt and nn and (nn.startswith(nt) or nt.startswith(nn)):
        score += 1
    if year and cyear == year:
        score += 3
    elif year and cyear and abs(cyear - year) == 1:
        score += 1
    score += min((cand.get("popularity") or 0) / 200.0, 1.0)
    return score


def _tmdb_queries(title):
    """Варианты запроса: полное название и его «голова» до тире/двоеточия."""
    out = []
    for q in (title, re.split(r"\s*[:—–-]\s*", title or "", maxsplit=1)[0]):
        q = (q or "").strip()
        if len(q) >= 2 and q.lower() not in [x.lower() for x in out]:
            out.append(q)
    return out


def _tmdb_pick(results, title, year, best, best_score):
    """Лучший кандидат TMDb среди результатов (обновляет текущий максимум)."""
    for cand in (results or [])[:12]:
        if not cand.get("poster_path"):
            continue
        sc = _tmdb_score(cand, title, year)
        if sc > best_score:
            best, best_score = cand, sc
    return best, best_score


def _tmdb_search(title, year=None, orig=None, store=True):
    """Один поиск в TMDb: возвращает {"p": постер, "b": широкий кадр}.

    Кадр (backdrop) нужен широким слотам — баннеру и плиткам «Сейчас смотрят»:
    у постера формат 2:3, и в широкой карточке он обрезается. Результат кэшируем
    в памяти и на диске, поэтому повторные вызовы мгновенные.
    """
    if not TMDB_KEY or not title:
        return {"p": None, "b": None}
    ck = (_poster_norm(title), year, _poster_norm(orig))
    with _tmdb_lock:
        cached = _tmdb_cache.get(ck)
    if isinstance(cached, dict) and "b" in cached:
        return cached
    # старый кэш хранил только строку-постер — переиспользуем её как фолбэк
    known_poster = cached if isinstance(cached, str) else (cached or {}).get("p")
    best, best_score = None, 0.0
    queries = _tmdb_queries(title)
    if orig and _poster_norm(orig) not in [_poster_norm(t) for t in queries]:
        queries.append(orig)
    try:
        sess = _get_session()          # переиспользуем соединение — экономим TLS-хендшейки
        for q in queries:
            if year:
                # сначала точный поиск по году (фильм, затем сериал) — он самый меткий
                for endpoint, yparam in (("search/movie", "year"),
                                         ("search/tv", "first_air_date_year")):
                    params = {"api_key": TMDB_KEY, "query": q, "language": TMDB_LANG,
                              "include_adult": "false", yparam: year}
                    r = sess.get(TMDB_API + endpoint, params=params, timeout=8)
                    data = r.json() if r.status_code == 200 else {}
                    best, best_score = _tmdb_pick(data.get("results"), title, year,
                                                  best, best_score)
                    if best_score >= 6:      # название и год совпали — дальше нет смысла
                        break
            if best_score >= 6:
                break
            if best_score >= 4:              # уже приличное совпадение — шире не ищем
                continue
            # без года: один запрос search/multi покрывает и фильмы, и сериалы
            params = {"api_key": TMDB_KEY, "query": q, "language": TMDB_LANG,
                      "include_adult": "false"}
            r = sess.get(TMDB_API + "search/multi", params=params, timeout=8)
            data = r.json() if r.status_code == 200 else {}
            best, best_score = _tmdb_pick(data.get("results"), title, year,
                                          best, best_score)
            if best_score >= 6:
                break
    except Exception:
        best = None
    res = {
        "p": (TMDB_IMG + "w780" + best["poster_path"]) if best else known_poster,
        "b": (TMDB_IMG + "w1280" + best["backdrop_path"])
             if (best and best.get("backdrop_path")) else None,
    }
    with _tmdb_lock:
        if store:
            if len(_tmdb_cache) > 8192:
                _tmdb_cache.clear()
            _tmdb_cache[ck] = res
    return res


def tmdb_poster(title, year=None, orig=None):
    """HQ-постер из TMDb по названию/году. None, если не нашли или нет ключа."""
    return _tmdb_search(title, year, orig).get("p")


def tmdb_backdrop(title, year=None, orig=None):
    """Широкий кадр (16:9) из TMDb — для баннера и плиток «Сейчас смотрят»."""
    return _tmdb_search(title, year, orig).get("b")


def _hq_poster(url, title=None, year=None, orig=None):
    """HQ-постер из TMDb. Обложки hdrezka мелкие и зернистые — не используем,
    а если в TMDb ничего нет, клиент покажет заглушку с названием."""
    if TMDB_KEY and title:
        return tmdb_poster(title, year, orig)
    return None


# ---------------------------------------------- кэш HQ-постеров (TMDb)
# Поиск в TMDb небыстрый, поэтому делаем его один раз и помним результат —
# в памяти и на диске. Дальше листинг отдаёт крупный постер сразу, без
# «дозагрузки» и подмены картинки на клиенте.
_POSTER_CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  ".posters_cache.json")
_poster_save_timer = None
_warm_lock = threading.Lock()
_warming = set()


def _poster_year(v):
    """Год из значения любого вида (int/строка «2025», «2025-01-01») или None."""
    try:
        return int(v)
    except (TypeError, ValueError):
        m = re.search(r"\b(19|20)\d{2}\b", str(v or ""))
        return int(m.group(0)) if m else None


def _poster_cache_key(it):
    return (_poster_norm(it.get("title")), _poster_year(it.get("year")), "")


def _load_poster_cache():
    try:
        with open(_POSTER_CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return
        for k, v in data.items():
            parts = k.split("\x1f")
            if len(parts) == 3 and v:
                _tmdb_cache[(parts[0], int(parts[1]) if parts[1] else None, "")] = v
    except Exception:
        pass


def _save_poster_cache():
    global _poster_save_timer
    try:
        with _tmdb_lock:
            _poster_save_timer = None
            dump = {"\x1f".join([k[0], "" if k[1] is None else str(k[1]), ""]): v
                    for k, v in _tmdb_cache.items() if v}
        tmp = _POSTER_CACHE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(dump, f, ensure_ascii=False)
        os.replace(tmp, _POSTER_CACHE_FILE)
    except Exception:
        pass


def _schedule_poster_save():
    """Отложенная запись кэша на диск (не чаще одной записи в 5 секунд)."""
    global _poster_save_timer
    with _tmdb_lock:
        if _poster_save_timer is not None:
            return
        _poster_save_timer = threading.Timer(5.0, _save_poster_cache)
        _poster_save_timer.daemon = True
        _poster_save_timer.start()


def _hd_orig_title(url):
    """Оригинальное название со страницы фильма.

    Нужно, потому что у свежих/редких фильмов в TMDb ещё нет русского названия,
    и поиск по русскому имени ничего не находит. По оригинальному названию постер
    находится. Если на странице оригинала нет — вернём None; при ошибке загрузки
    бросаем исключение, чтобы попробовать ещё раз позже.
    """
    r = _hd_get(url)
    if getattr(r, "status_code", 0) != 200:
        raise RuntimeError("hdrezka вернула %s" % getattr(r, "status_code", "?"))
    soup = BeautifulSoup(r.text, "html.parser")
    el = soup.select_one(".full-article__original-title")
    return el.get_text(" ", strip=True) if el else None


def _tmdb_resolve(url, title, year):
    """Постер/кадр для карточки — тем же способом, что и у страницы фильма.

    Сначала TMDb по русскому названию, а если локализованного имени там ещё нет
    (свежие релизы), подтягиваем оригинальное название со страницы фильма и ищем
    по нему. Иначе карточка в листинге осталась бы без постера, хотя он есть.
    Ошибку загрузки страницы не глотаем: вызывающий сам решает, повторять ли позже.
    """
    res = _tmdb_search(title, year) if title else {"p": None, "b": None}
    if not res.get("p") and title and url:
        orig = _hd_orig_title(url)
        if orig:
            r2 = _tmdb_search(title, year, orig)
            if r2.get("p"):
                res = r2
    return res


def _warm_posters(items):
    """Фоново догружает TMDb-постеры и кадры для листинга — ответ не задерживаем.

    Если по русскому названию TMDb ничего не нашёл, подтягиваем оригинальное
    название со страницы фильма и ищем по нему — так карточка получает тот же
    постер, что и страница фильма. Результат кладём под «обычный» ключ и метим
    его флагом «o» (оригинал проверен), чтобы не искать повторно.
    """
    if not TMDB_KEY:
        return
    todo = []
    with _warm_lock:
        for it in items:
            k = _poster_cache_key(it)
            v = _tmdb_cache.get(k)
            v = v if isinstance(v, dict) else {}
            if k[0] and not v.get("p") and not v.get("o") and k not in _warming:
                _warming.add(k)
                todo.append((k, it))
    if not todo:
        return

    def run():
        try:
            for k, it in todo:
                title = (it.get("title") or "").strip()
                year = _poster_year(it.get("year"))
                try:
                    res = _tmdb_resolve(it.get("url"), title, year)
                except Exception:            # сеть/прокси подвели — повторим в следующий раз
                    continue
                with _tmdb_lock:
                    cur = _tmdb_cache.get(k)
                    cur = cur if isinstance(cur, dict) else {}
                    cur["p"] = res.get("p") or cur.get("p")
                    cur["b"] = res.get("b") or cur.get("b")
                    cur["o"] = True          # оригинал проверен — больше не дёргаем
                    _tmdb_cache[k] = cur
        finally:
            with _warm_lock:
                for k, _ in todo:
                    _warming.discard(k)
            _schedule_poster_save()

    threading.Thread(target=run, daemon=True).start()


def attach_posters(items):
    """Проставляет уже известные HQ-постеры и кадры, остальные греет в фоне."""
    if not items:
        return items
    _warm_posters(items)
    for it in items:
        v = _tmdb_cache.get(_poster_cache_key(it))
        if isinstance(v, str):          # старый кэш: строка = только постер
            v = {"p": v}
        if not isinstance(v, dict):
            continue
        if not it.get("poster") and v.get("p"):
            it["poster"] = v["p"]
        if not it.get("backdrop") and v.get("b"):
            it["backdrop"] = v["b"]
    return items


_load_poster_cache()


def _hd_players_from_video_page(html):
    """Вкладки-плееры со страницы /video/<id>/ (трейлер YouTube пропускаем)."""
    soup = BeautifulSoup(html, "html.parser")
    players = []
    for tab in soup.select(".full-article__player__tabs_item"):
        src = tab.get("data-src")
        if not src or "youtube." in src or "youtu.be" in src:
            continue
        players.append({"title": tab.get_text(" ", strip=True) or "Плеер", "url": _hd_abs(src)})
    return players


def hdrezka_film(url):
    """Карточка фильма/сериала нового hdrezka.my + прямые HLS/DASH в наш плеер."""
    r = _hd_get(url)
    if r.status_code != 200:
        raise RuntimeError(f"hdrezka вернула {r.status_code}")
    html = r.text
    soup = BeautifulSoup(html, "html.parser")

    h1 = soup.select_one("h1.full-article__title") or soup.select_one("h1")
    name, year, raw_title = None, None, None
    if h1:
        season_span = h1.select_one("span.season")  # у сериалов: «1 сезон 1-25 серия»
        if season_span:
            season_span.extract()
        raw_title = h1.get_text(" ", strip=True)
        m = re.search(r"\((\d{4})\)", raw_title)
        if m:
            year = int(m.group(1))
        name = _clean_title(re.sub(r"\s*\((\d{4})\)\s*$", "", raw_title)) or raw_title

    orig_el = soup.select_one(".full-article__original-title")
    orig_name = orig_el.get_text(" ", strip=True) if orig_el else None

    # Обложки hdrezka мелкие и зернистые — вместо них берём HQ-постер из TMDb.
    # Если совпадения нет, оставляем пусто: клиент нарисует заглушку с названием.
    poster_hq = tmdb_poster(name, year, orig_name) if (TMDB_KEY and name) else None
    # широкий кадр — для размытого фона карточки фильма
    backdrop = tmdb_backdrop(name, year, orig_name) if (TMDB_KEY and name) else None

    desc = None
    d = soup.select_one(".full-article__description")
    if d:
        for tag in d.find_all(["script", "style", "h1", "h2", "h3"]):
            tag.extract()
        desc = re.sub(r"\s+", " ", d.get_text(" ", strip=True)).strip()
    if not desc:
        ogd = soup.find("meta", property="og:description")
        if ogd:
            desc = ogd.get("content")

    rating = None
    imdb = soup.select_one(".full-article__list .imdb b") or soup.select_one(".imdb b")
    if imdb:
        m = re.search(r"\d+(?:[.,]\d+)?", imdb.get_text())
        if m:
            rating = float(m.group(0).replace(",", "."))

    cats = []
    for li in soup.select(".full-article__list li"):
        lab = li.find("span", class_="full-article__list_title")
        if lab and "Категор" in lab.get_text():
            for a in li.find_all("a"):
                v = a.get_text(strip=True)
                if v and "4k" not in v.lower() and v not in cats:
                    cats.append(v)

    # страница с плеерами: /video/<id>/
    video_url = None
    online = soup.select_one("a.full-article__buttons--online")
    if online and online.get("href"):
        video_url = _hd_abs(online["href"])
    if not video_url:
        m = re.search(r'href="(/video/\d+/)"', html)
        if m:
            video_url = _hd_abs(m.group(1))

    players = []
    if video_url:
        vr = _hd_get(video_url, referer=url)
        if vr.status_code == 200:
            players = _hd_players_from_video_page(vr.text)
            players = _drop_placeholder_players(players, url)

    # прямые потоки: фильм -> source:{...}, сериал -> seasons:[...]
    sources, seasons = [], []
    ok_players = []                       # плееры, страница которых реально открылась
    if players:
        from concurrent.futures import ThreadPoolExecutor

        def _fetch(p):
            # эмбед-плееры (nextembed и др.) банят прокси-IP (422) — fetch_embed
            # сам подбирает рабочий транспорт и запоминает его по хосту
            return fetch_embed(p["url"], lambda u, use_proxy, timeout: _hd_get(
                u, referer=video_url or url, timeout=timeout, use_proxy=use_proxy),
                timeout=30)

        with ThreadPoolExecutor(max_workers=min(6, len(players))) as ex:
            pages = list(ex.map(_fetch, players))

        for p, ph in zip(players, pages):
            if not ph:
                continue
            ok_players.append(p)
            player_origin = _page_origin(p["url"])
            if "stravers" in p["url"] or _is_stravers_page(ph):
                # Сериал уже собран более дешёвым плеером (серии лежат в HTML
                # массивом seasons). stravers отдаёт то же самое, но тянет поток
                # через API по КАЖДОЙ серии — сотни запросов и минуты на длинных
                # сериалах. Раз результат всё равно не используется, не дёргаем.
                if seasons:
                    continue
                # основной плеер hdrezka (в т.ч. зеркала на других доменах):
                # прямые ссылки только через его API /bnsi
                ssrc, ssea = _parse_stravers_player(p["url"], ph, referer=video_url or url)
                sources.extend(ssrc)
                if ssea:
                    seasons = ssea
                continue
            if not seasons:
                seasons = _parse_serial_seasons(ph)
                for season in seasons:
                    for episode in season.get("episodes") or []:
                        episode["ref"] = player_origin
            src = _parse_movie_source(ph)
            if src and (src.get("hls") or src.get("mp4") or src.get("dash")):
                src["title"] = p["title"]
                src["ref"] = player_origin
                sources.append(src)
            elif "balancer-api/iframe" in p["url"] and not seasons:
                # SPA-плеер семейства balancer-api (perepolokha, shortener2, …):
                # прямых ссылок в HTML нет — берём через его API. Тоже дорого
                # (запрос на каждую серию), поэтому только если сериал ещё не собран.
                psrc, psea = _parse_balancer_player(p["url"], ph)
                sources.extend(psrc)
                if psea:
                    seasons = psea

    # Рекламные заглушки выкидываем всегда; экранки (TS/CAM) — только если есть
    # релиз получше: иначе фильм остался бы вообще без потока. Из оставшихся
    # берём одну лучшую русскую дорожку — единый плеер без выбора озвучки.
    sources = _drop_bad_sources(sources)
    clean = [s for s in sources if _source_quality_rank(s) > 0]
    sources = _pick_single_source(clean or sources)
    players = _drop_bad_players(players)
    # плееры-эмбеды для iframe: только те, чья страница открылась (остальные — 422/пусто)
    embed_ok = {p["url"] for p in ok_players}
    embeds = _film_embeds([p for p in players if p["url"] in embed_ok], sources)

    if seasons:
        seasons.sort(key=lambda s: s.get("season") or 0)

    is_series = bool(seasons) or any("сериал" in c.lower() for c in cats)
    return {
        "source": "hdrezka",
        "url": url,
        "origin": HDREZKA_ORIGIN,
        "name": name,
        "origName": orig_name,
        "description": desc,
        "releaseYear": year,
        "thumbnail": poster_hq,
        "thumbnailHQ": poster_hq,
        "backdrop": backdrop,
        "rating": {"value": rating, "votes": None},
        "type": "video.tv_series" if is_series else "video.movie",
        "category": "category.series" if is_series else "category.film",
        "categories": cats,
        "isSeries": is_series,
        "sources": sources,
        "players": players,
        "embeds": embeds,
        "translators": {},
        "seasons": seasons,
    }


# ================================================================== HTTP API
@app.route("/")
def index():
    return send_file(os.path.join(BASE_DIR, "index.html"))


# ---------------------------------------------------------------- PWA (манифест, иконки, SW)
@app.route("/manifest.json")
def manifest():
    return send_file(
        os.path.join(BASE_DIR, "manifest.json"),
        mimetype="application/manifest+json",
    )


@app.route("/sw.js")
def service_worker():
    r = send_file(os.path.join(BASE_DIR, "sw.js"), mimetype="application/javascript")
    r.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    r.headers["Service-Worker-Allowed"] = "/"
    return r


@app.route("/icon-<string:name>.png")
def icon(name):
    path = os.path.join(BASE_DIR, "icon-%s.png" % name)
    if not os.path.exists(path):
        return ("", 404)
    return send_file(path, mimetype="image/png")


@app.route("/apple-touch-icon.png")
def apple_icon():
    return send_file(os.path.join(BASE_DIR, "apple-touch-icon.png"), mimetype="image/png")



# ================================================================== КАТАЛОГ ГЛАВНОЙ
# Главная собирается из hdrezka: он отдаёт HQ-обложки (550×786) и его плеер
# perepolokha работает через прокси, а значит играет и на Railway, где
# датацентровый IP плееры kinogo (ortified) блокируют. Поля: (заголовок, категория, жанр, сортировка).
HOME_ROWS = [
    ("Новинки",     "films",    None,         "date"),
    ("Боевики",     "films",    "боевик",     None),
    ("Комедии",     "films",    "комедия",    None),
    ("Фантастика",  "films",    "фантастика", None),
    ("Ужасы",       "films",    "ужасы",      None),
    ("Драмы",       "films",    "драма",      None),
    ("Триллеры",    "films",    "триллер",    None),
    ("Мультфильмы", "cartoons", None,         None),
    ("Аниме",       "anime",    None,         None),
    ("Сериалы",     "series",   None,         None),
]


def _safe(fn, default):
    try:
        return fn()
    except Exception:
        return default


def _home_row(cat, genre, sort):
    """Ряд главной: добираем страницы, пока не наберём 24 вышедших фильма.

    У новинок в листинге много карточек-трейлеров (невышедшие фильмы), их
    каталог уже отфильтровывает — без добора ряды заметно пустели.
    """
    out, seen = [], set()
    for page in range(1, 4):
        for it in hdrezka_browse(cat, genre=genre, sort=sort, page=page):
            if it.get("url") in seen:
                continue
            seen.add(it["url"])
            out.append(it)
        if len(out) >= 24:
            break
    return out


@app.route("/api/home")
def api_home():
    rows = cache_get("home", 1800)
    if rows is None:
        from concurrent.futures import ThreadPoolExecutor
        jobs = [(title, (lambda c=cat, g=genre, s=sort: _home_row(c, g, s)))
                for title, cat, genre, sort in HOME_ROWS]

        with ThreadPoolExecutor(max_workers=6) as ex:
            futures = [(t, ex.submit(_safe, fn, [])) for t, fn in jobs]
            rows = []
            seen = set()
            for title, fut in futures:
                items = []
                for it in fut.result():
                    if it.get("url") in seen or not it.get("image"):
                        continue
                    seen.add(it["url"])
                    items.append(it)
                if items:
                    rows.append({"title": title, "items": items[:24]})
        if rows:
            cache_set("home", rows)
    # известные HQ-постеры отдаём сразу вместе со списком, остальные греем в фоне
    attach_posters([it for row in rows for it in row["items"]])
    return json_cached({"ok": True, "rows": rows}, 600)


@app.route("/api/qualities")
def api_qualities():
    """Диагностика: какие разрешения реально отдаёт источник (есть ли 1080p)."""
    url = (request.args.get("url") or "").strip()
    if not url:
        return json_error("Не передан url", 400)
    try:
        film = cache_get(f"film:{url}", 600) or kinogo_film(url)
        report = []
        for src in film.get("sources", []):
            entry = {"player": src.get("title"), "hls": src.get("hls"), "mp4": src.get("mp4"), "levels": []}
            if src.get("hls"):
                txt = video_get(src["hls"], headers=_stravers_headers(src["hls"]) or None).text
                for m in re.finditer(r"#EXT-X-STREAM-INF:([^\n]+)", txt):
                    attrs = m.group(1)
                    res = re.search(r"RESOLUTION=(\d+)x(\d+)", attrs)
                    bw = re.search(r"BANDWIDTH=(\d+)", attrs)
                    entry["levels"].append({
                        "resolution": f"{res.group(1)}x{res.group(2)}" if res else None,
                        "height": int(res.group(2)) if res else None,
                        "bandwidth": int(bw.group(1)) if bw else None,
                    })
                heights = [l["height"] for l in entry["levels"] if l["height"]]
                entry["max_height"] = max(heights) if heights else None
                entry["has_1080"] = bool(heights and max(heights) >= 1000)
            report.append(entry)
        return jsonify({"ok": True, "name": film.get("name"), "sources": report})
    except Exception as e:
        return json_error(f"{type(e).__name__}: {e}")


@app.route("/api/auth/register", methods=["POST"])
def api_auth_register():
    """Create a password account bound to the caller's anonymous Firebase UID."""
    if not _firebase_auth_ready():
        return _auth_json_error("Firebase Auth не настроен", 503)
    if _auth_rate_limited():
        return _auth_json_error("Слишком много попыток. Подождите минуту.", 429)

    data = request.get_json(silent=True) or {}
    login = str(data.get("login") or "").strip().lower()
    password = data.get("password")
    if not _auth_login_value(login):
        return _auth_json_error("Логин: 3–20 символов, латиница, цифры, _ или -", 400)
    if not isinstance(password, str) or not 6 <= len(password) <= 64:
        return _auth_json_error("Пароль должен содержать от 6 до 64 символов", 400)

    auth_header = request.headers.get("Authorization", "")
    id_token = auth_header[7:].strip() if auth_header.lower().startswith("bearer ") else ""
    if not id_token:
        return _auth_json_error("Нужна анонимная Firebase-сессия", 401)

    try:
        from firebase_admin_support import create_custom_token, database_ref, verify_id_token

        claims = verify_id_token(id_token)
        provider = (claims.get("firebase") or {}).get("sign_in_provider")
        uid = str(claims.get("uid") or claims.get("sub") or "")
        if provider != "anonymous" or not uid or len(uid) > 128:
            return _auth_json_error("Регистрация доступна из гостевого профиля", 403)

        record = _password_record(password)
        account = {
            "uid": uid,
            **record,
            "ts": int(time.time() * 1000),
        }
        ref = database_ref("accounts/" + login)
        result = ref.transaction(lambda current: account if current is None else current)
        if result != account:
            return _auth_json_error("Этот логин уже занят", 409)

        token = create_custom_token(uid)
        response = jsonify({"ok": True, "uid": uid, "login": login, "token": token})
        response.headers["Cache-Control"] = "no-store"
        return response
    except Exception as exc:
        app.logger.warning("Firebase registration failed: %s", type(exc).__name__)
        return _auth_json_error("Не удалось создать аккаунт. Проверьте настройку Firebase.", 503)


@app.route("/api/auth/login", methods=["POST"])
def api_auth_login():
    """Verify the legacy PBKDF2 record server-side and issue a Firebase custom token."""
    if not _firebase_auth_ready():
        return _auth_json_error("Firebase Auth не настроен", 503)
    if _auth_rate_limited():
        return _auth_json_error("Слишком много попыток. Подождите минуту.", 429)

    data = request.get_json(silent=True) or {}
    login = str(data.get("login") or "").strip().lower()
    password = data.get("password")
    if not _auth_login_value(login) or not isinstance(password, str) or not 6 <= len(password) <= 64:
        return _auth_json_error("Неверный логин или пароль", 401)

    try:
        from firebase_admin_support import create_custom_token, database_ref

        record = database_ref("accounts/" + login).get()
        if not isinstance(record, dict):
            hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), b"onecinema-login-dummy", 150000, 32)
            return _auth_json_error("Неверный логин или пароль", 401)
        if not _password_matches(password, record):
            return _auth_json_error("Неверный логин или пароль", 401)
        uid = str(record.get("uid") or "")
        if not uid or len(uid) > 128:
            return _auth_json_error("Неверный логин или пароль", 401)

        token = create_custom_token(uid)
        response = jsonify({"ok": True, "uid": uid, "login": login, "token": token})
        response.headers["Cache-Control"] = "no-store"
        return response
    except Exception as exc:
        app.logger.warning("Firebase login failed: %s", type(exc).__name__)
        return _auth_json_error("Не удалось войти. Проверьте настройку Firebase.", 503)


@app.route("/api/config")
def api_config():
    return jsonify({
        "ok": True,
        "sources": {"kinogo": KINOGO_ORIGIN, "hdrezka": HDREZKA_ORIGIN},
        "defaultSource": "hdrezka",
        "proxy": bool(PROXY),
        "videoProxy": bool(VIDEO_PROXY),
        "relay": RELAY_ORIGIN,
        "firebaseApiKey": FIREBASE_WEB_API_KEY,
        "firebaseDatabaseURL": FIREBASE_DB_URL,
        "embedTransport": EMBED_TRANSPORT,
        "hdrezkaAuthorized": bool(get_hdrezka_cookies()),
    })


@app.route("/api/diag")
def api_diag():
    """Диагностика транспорта с IP самого сервера (нужна для Railway).

    Открой /api/diag — покажет исходящий IP и что реально работает:
    сайт источника и страницы плееров напрямую / через прокси.
    С /api/diag?url=<страница фильма> проверит конкретный фильм по шагам.
    """
    def one(fn, url):
        try:
            r = fn(url)
            return {"status": r.status_code, "bytes": len(r.content)}
        except Exception as e:
            return {"error": f"{type(e).__name__}: {e}"}

    out = {
        "ok": True,
        "egressIp": None,
        "proxy": bool(PROXY),
        "videoProxy": bool(VIDEO_PROXY),
        "embedTransport": EMBED_TRANSPORT,
        "embedMode": dict(_embed_mode),
        "checks": {},
    }
    try:
        out["egressIp"] = requests.get("https://api.ipify.org", timeout=10).text
    except Exception as e:
        out["egressIp"] = f"err: {e}"

    for label, u in (("kinogo.sarl", KINOGO_ORIGIN + "/"),
                     ("hdrezka.my", HDREZKA_ORIGIN)):
        out["checks"][label] = {
            "direct": one(lambda x: http_get(x, use_proxy=False), u),
            "proxy": one(lambda x: http_get(x, use_proxy=True), u),
        }

    film_url = (request.args.get("url") or "").strip()
    if film_url:
        steps = out.setdefault("film", {})
        for label, fn in (("direct", lambda x: http_get(x, use_proxy=False)),
                          ("proxy", lambda x: http_get(x, use_proxy=True))):
            r = one(fn, film_url)
            steps.setdefault("page", {})[label] = r

        players = []
        try:
            r = http_get(film_url, use_proxy=False)
            if r.status_code != 200:
                r = http_get(film_url, use_proxy=True)
            if r.status_code == 200:
                soup = BeautifulSoup(r.content, "html.parser")
                players = [abs_url(i.get("src")) for i in soup.find_all("iframe")
                           if i.get("src") and "http" in i.get("src")]
        except Exception as e:
            steps["pageError"] = f"{type(e).__name__}: {e}"

        steps["players"] = []
        for pu in players:
            entry = {"url": pu, "direct": None, "proxy": None}
            for label, use_proxy in (("direct", False), ("proxy", True)):
                try:
                    pr = http_get(pu, use_proxy=use_proxy, timeout=30) if detect_source(
                        film_url) == "kinogo" else _hd_get(pu, use_proxy=use_proxy)
                    src = _parse_movie_source(pr.text) if pr.status_code == 200 else None
                    entry[label] = {
                        "status": pr.status_code,
                        "bytes": len(pr.content),
                        "parsed": bool(src and (src.get("hls") or src.get("dash") or src.get("mp4"))),
                    }
                except Exception as e:
                    entry[label] = {"error": f"{type(e).__name__}: {e}"}
            steps["players"].append(entry)

    out["videoDirectMode"] = VIDEO_DIRECT

    # проверка самого видеопотока: пустит ли этот сервер видео напрямую
    stream_url = (request.args.get("stream") or "").strip()
    if not stream_url and film_url:
        try:
            film = cache_get(f"film:{film_url}", 600) or (
                kinogo_film(film_url) if detect_source(film_url) == "kinogo"
                else hdrezka_film(film_url))
            for s in (film.get("sources") or []):
                if s.get("hls"):
                    stream_url = s["hls"]
                    break
        except Exception as e:
            out["streamResolveError"] = f"{type(e).__name__}: {e}"

    if stream_url:
        def _probe(u, proxied):
            entry = {"ok": False, "ms": None, "status": None, "type": "", "bytes": 0, "error": None}
            t0 = time.time()
            try:
                if _cffi is not None:
                    sess = _cffi.Session(impersonate=HDREZKA_IMPERSONATE,
                                         proxies=(make_video_proxy() or None) if proxied else None)
                    h = dict(default_headers)
                    h.pop("User-Agent", None)
                    r = sess.get(u, headers=h, stream=True, timeout=12)
                else:
                    r = _get_session().get(u, headers=default_headers, stream=True, timeout=12,
                                           proxies=make_video_proxy() if proxied else {})
                entry["status"] = r.status_code
                entry["type"] = r.headers.get("Content-Type", "")
                body = b"".join(r.iter_content(65536))
                entry["bytes"] = len(body)
                entry["ok"] = (r.status_code == 200 and entry["bytes"] > 0
                               and "text/html" not in entry["type"].lower())
                r.close()
            except Exception as e:
                entry["error"] = f"{type(e).__name__}: {e}"
            entry["ms"] = int((time.time() - t0) * 1000)
            return entry

        check = {"url": stream_url, "master": {}, "variant": {}}
        for label, proxied in (("direct", False), ("proxy", True)):
            if proxied and not VIDEO_PROXY:
                continue
            check["master"][label] = _probe(stream_url, proxied)

        # берём рабочий master и проверяем первый вариант (это самый тяжёлый файл)
        for label, proxied in (("direct", False), ("proxy", True)):
            if check["master"].get(label, {}).get("ok"):
                try:
                    if _cffi is not None:
                        sess = _cffi.Session(impersonate=HDREZKA_IMPERSONATE,
                                             proxies=(make_video_proxy() or None) if proxied else None)
                        h = dict(default_headers)
                        h.pop("User-Agent", None)
                        txt = sess.get(stream_url, headers=h, timeout=12).text
                    else:
                        txt = _get_session().get(stream_url, headers=default_headers, timeout=12,
                                                 proxies=make_video_proxy() if proxied else {}).text
                    for line in txt.splitlines():
                        line = line.strip()
                        if line and not line.startswith("#"):
                            var = urljoin(stream_url, line)
                            for l2, p2 in (("direct", False), ("proxy", True)):
                                if p2 and not VIDEO_PROXY:
                                    continue
                                check["variant"][l2] = _probe(var, p2)
                            break
                except Exception as e:
                    check["variantError"] = f"{type(e).__name__}: {e}"
                break

        check["directUsable"] = bool(check["master"].get("direct", {}).get("ok"))
        out["stream"] = check

    return jsonify(out)


@app.route("/api/search")
def api_search():
    query = (request.args.get("q") or "").strip()
    if not query:
        return json_error("Пустой запрос", 400)
    source = request.args.get("source") or "hdrezka"
    find_all = request.args.get("all") in ("1", "true", "yes")

    key = f"search:{source}:{query}:{find_all}"
    cached = cache_get(key, 300)
    if cached is not None:
        return json_cached({"ok": True, "results": attach_posters(cached), "source": source}, 300)

    try:
        if source == "kinogo":
            results = kinogo_search(query)
        else:
            results = hdrezka_search(query, find_all)
            if not results:  # hdrezka ничего не нашла — не оставляем пустой экран
                results = kinogo_search(query)
        cache_set(key, results)
        return json_cached({"ok": True, "results": attach_posters(results), "source": source}, 300)
    except Exception as e:
        return json_error(f"{type(e).__name__}: {e}")


# ------------------------------------------------------------------ подсказки поиска
_suggest_sem = threading.BoundedSemaphore(3)   # не заваливаем источник запросами «на каждую букву»


@app.route("/api/suggest")
def api_suggest():
    """Подсказки при наборе: /api/suggest?q=матр -> до 6 названий.

    Берёт результаты из того же кэша, что и /api/search, поэтому Enter после
    подсказок обычно открывается мгновенно. Источник опрашиваем не более чем
    в 3 потока; если заняты — отдаём пустой список (клиент покажет историю).
    """
    q = re.sub(r"\s+", " ", (request.args.get("q") or "")).strip()[:80]
    if len(q) < 2:
        return jsonify({"ok": True, "items": []})

    def pack(results):
        out, seen = [], set()
        for it in results or []:
            url, title = it.get("url"), (it.get("title") or "").strip()
            if not url or not title or url in seen:
                continue
            seen.add(url)
            out.append({"title": title, "url": url,
                        "year": it.get("year"), "image": it.get("image")})
            if len(out) >= 6:
                break
        return out

    shared = cache_get(f"search:hdrezka:{q}:False", 300)
    if shared is not None:
        return json_cached({"ok": True, "items": pack(shared)}, 120)
    own_key = "suggest:" + q.casefold()
    own = cache_get(own_key, 300)
    if own is not None:
        return json_cached({"ok": True, "items": own}, 120)
    if not _suggest_sem.acquire(blocking=False):
        return jsonify({"ok": True, "items": [], "busy": True})
    try:
        items = pack(hdrezka_search(q, False))
    except Exception:
        return jsonify({"ok": True, "items": []})
    finally:
        _suggest_sem.release()
    cache_set(own_key, items)
    return json_cached({"ok": True, "items": items}, 120)


# ------------------------------------------------------------------ «Мне повезёт» и подбор по настроению
# Настроение -> варианты (категория каталога, жанр по его названию на источнике
# или None). Жанр ищем по подписи в фильтрах каталога, а не по значению: id у
# источника не наши и могут поменяться.
MOODS = {
    "laugh":  {"label": "Посмеяться",
               "opts": [("films", "Комедия"), ("series", "Комедия")]},
    "nerves": {"label": "Пощекотать нервы",
               "opts": [("films", "Триллер"), ("films", "Ужасы"),
                        ("films", "Детектив"), ("series", "Триллер")]},
    "romance": {"label": "Для души",
                "opts": [("films", "Мелодрама"), ("series", "Мелодрама")]},
    "action": {"label": "Адреналин",
               "opts": [("films", "Боевик"), ("films", "Приключения")]},
    "think":  {"label": "Подумать",
               "opts": [("films", "Фантастика"), ("films", "Детектив"),
                        ("films", "Драма"), ("series", "Фантастика")]},
    "family": {"label": "Всей семьёй",
               "opts": [("films", "Семейный"), ("cartoons", None)]},
    "anime":  {"label": "Аниме на вечер",
               "opts": [("anime", None), ("animeseries", None)]},
    "binge":  {"label": "Сериал на вечера",
               "opts": [("series", "Драма"), ("series", "Детектив"),
                        ("series", "Фантастика")]},
}


def _filter_options(cat, kind):
    key = f"filters:{cat}"          # тот же кэш, что у /api/filters
    data = cache_get(key, 21600)
    if data is None:
        data = cache_set(key, hdrezka_filters(cat))
    return data.get(kind) or []


def _filter_value(cat, kind, label):
    want = (label or "").casefold()
    if not want:
        return None
    opts = _filter_options(cat, kind)
    for o in opts:
        if o["label"].casefold() == want:
            return o["value"]
    for o in opts:
        if o["label"].casefold().startswith(want):
            return o["value"]
    return None


def _popular_sort(cat):
    opts = _filter_options(cat, "sort")
    for word in ("популяр", "рейтинг"):
        for o in opts:
            if word in o["label"].casefold():
                return o["value"]
    return None


def _browse_page(cat, genre, sort, page):
    key = f"browse:{cat}:{genre}:None:None:{sort}:{page}"   # общий кэш с /api/browse
    cached = cache_get(key, 900)
    if cached is not None:
        return cached
    items = hdrezka_browse(cat, genre, None, None, sort, page)
    cache_set(key, items)
    return items


def _random_items(cat, genre_label, pages, max_page):
    """Случайные страницы популярного в категории/жанре. [] — вариант не сработал."""
    try:
        genre = _filter_value(cat, "genre", genre_label) if genre_label else None
        sort = _popular_sort(cat)
    except Exception:
        genre, sort = None, None
    if genre_label and not genre:
        return []                    # такого жанра на источнике нет — пробуем другой вариант
    nums = random.sample(range(1, max_page + 1), min(pages, max_page))

    def one(p):
        try:
            return _browse_page(cat, genre, sort, p)
        except Exception:
            return []

    if len(nums) > 1:
        with ThreadPoolExecutor(max_workers=len(nums)) as ex:
            chunks = list(ex.map(one, nums))
    else:
        chunks = [one(nums[0])]
    items = [it for ch in chunks for it in (ch or [])]
    return items or one(1)          # страница за концом каталога -> первая


@app.route("/api/random")
def api_random():
    """Случайная подборка: /api/random?n=12 или /api/random?mood=laugh&n=24.

    Без mood — «Мне повезёт»: пачка кандидатов, клиент берёт из неё по одному
    и сам просит новую, когда пачка кончается.
    """
    mood = (request.args.get("mood") or "").strip()
    try:
        n = max(1, min(30, int(request.args.get("n") or 12)))
    except (TypeError, ValueError):
        n = 12
    if mood:
        m = MOODS.get(mood)
        if not m:
            return json_error("Неизвестное настроение", 400)
        opts = random.sample(m["opts"], len(m["opts"]))
        pages, max_page = 2, 4
    else:
        cat = random.choice(["films", "films", "films", "series", "series", "anime", "cartoons"])
        opts = [(cat, None)]
        pages, max_page = 1, 8

    items = []
    for cat, genre in opts[:3]:
        items = _random_items(cat, genre, pages, max_page)
        if items:
            break
    if not items:                    # ни один жанр не нашёлся — хотя бы категория без жанра
        items = _random_items(opts[0][0], None, pages, max_page)
    seen, uniq = set(), []
    for it in items:
        u = it.get("url")
        if u and u not in seen:
            seen.add(u)
            uniq.append(it)
    if not uniq:
        return json_error("Не удалось подобрать — источник не отвечает", 502)
    random.shuffle(uniq)
    resp = jsonify({"ok": True, "items": attach_posters(uniq[:n]), "mood": mood or None})
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.route("/api/filters")
def api_filters():
    """Доступные значения фильтров каталога (жанр/год/страна/сортировка)."""
    cat = request.args.get("cat") or "films"
    key = f"filters:{cat}"
    cached = cache_get(key, 21600)
    if cached is None:
        try:
            cached = hdrezka_filters(cat)
            cache_set(key, cached)
        except Exception as e:
            return json_error(f"{type(e).__name__}: {e}")
    return json_cached({"ok": True, "filters": cached}, 21600)


@app.route("/api/browse")
def api_browse():
    """Каталог с фильтрами: /api/browse?cat=films&genre=боевик&year=2024&page=1"""
    cat = request.args.get("cat") or "films"
    genre = request.args.get("genre") or None
    country = request.args.get("country") or None
    year = request.args.get("year") or None
    sort = request.args.get("sort") or None
    try:
        page = max(1, int(request.args.get("page") or 1))
    except (TypeError, ValueError):
        page = 1

    key = f"browse:{cat}:{genre}:{country}:{year}:{sort}:{page}"
    cached = cache_get(key, 900)
    if cached is not None:
        return json_cached({"ok": True, "items": attach_posters(cached), "page": page}, 600)

    try:
        items = hdrezka_browse(cat, genre, country, year, sort, page)
        cache_set(key, items)
        return json_cached({"ok": True, "items": attach_posters(items), "page": page}, 600)
    except Exception as e:
        return json_error(f"{type(e).__name__}: {e}")


@app.route("/api/posters", methods=["GET", "POST"])
def api_posters():
    """Пакет HQ-обложек: сперва TMDb (реальное качество), затем фолбэк на источник.

    GET  ?urls=<url,url>                    — только ссылки (легаси)
    POST [{"url","title","year","orig"}, …] — с названием/годом, чтобы найти в TMDb
    """
    if request.method == "POST":
        items = request.get_json(silent=True) or []
    else:
        raw = request.args.get("urls") or ""
        items = [{"url": u} for u in (x.strip() for x in raw.split(",")) if u]
    items = [it for it in items if isinstance(it, dict) and it.get("url")][:24]
    if not items:
        return json_error("Не переданы urls", 400)

    def norm_year(v):
        try:
            return int(v)
        except (TypeError, ValueError):
            m = re.search(r"\b(19|20)\d{2}\b", str(v or ""))
            return int(m.group(0)) if m else None

    def one(it):
        title = (it.get("title") or "").strip() or None
        year = norm_year(it.get("year"))
        orig = (it.get("orig") or "").strip() or None
        if not title:
            return None, None
        res = _tmdb_search(title, year, orig)
        if not res.get("p") and not orig and it.get("url"):
            # русского названия в TMDb ещё нет — ищем по оригинальному,
            # ровно как страница фильма (иначе карточка осталась бы без постера)
            try:
                res = _tmdb_resolve(it.get("url"), title, year) or res
            except Exception:
                pass
        # широкий кадр нужен плиткам «Сейчас смотрят» (слот 16:9)
        return res.get("p"), res.get("b")

    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=min(16, len(items))) as ex:
        pairs = list(ex.map(one, items))
    posters = {it["url"]: p for it, (p, _) in zip(items, pairs)}
    backdrops = {it["url"]: b for it, (_, b) in zip(items, pairs) if b}
    return json_cached({"ok": True, "posters": posters, "backdrops": backdrops}, 86400)


# ---------------------------------------------- актёры и создатели (TMDb)
_cast_cache = {}   # v2: + жанры, слоган, длительность, рейтинг


def tmdb_cast(title, year=None, orig=None, is_series=None):
    """Состав фильма/сериала из TMDb: актёры (фото, роль) и режиссёры/сценаристы."""
    if not TMDB_KEY or not title:
        return None
    ck = (_poster_norm(title), year, bool(is_series))
    with _tmdb_lock:
        if ck in _cast_cache:
            return _cast_cache[ck]
    best, best_kind, best_score = None, None, 0.0
    queries = _tmdb_queries(title)
    if orig and _poster_norm(orig) not in [_poster_norm(t) for t in queries]:
        queries.append(orig)
    kinds = ("tv", "movie") if is_series else ("movie", "tv")
    result = None
    try:
        sess = _get_session()
        for q in queries:
            for kind in kinds:
                params = {"api_key": TMDB_KEY, "query": q, "language": TMDB_LANG,
                          "include_adult": "false"}
                if year:
                    params["first_air_date_year" if kind == "tv" else "year"] = year
                r = sess.get(TMDB_API + "search/" + kind, params=params, timeout=8)
                data = r.json() if r.status_code == 200 else {}
                for cand in (data.get("results") or [])[:10]:
                    sc = _tmdb_score(cand, title, year)
                    if sc > best_score:
                        best, best_kind, best_score = cand, kind, sc
            if best_score >= 6:
                break
        if best and best_score >= 3:
            r = sess.get(TMDB_API + best_kind + "/" + str(best["id"]),
                         params={"api_key": TMDB_KEY, "language": TMDB_LANG,
                                 "append_to_response": "credits"}, timeout=8)
            det = r.json() if r.status_code == 200 else {}
            cr = det.get("credits") or {}
            cast = []
            for c in (cr.get("cast") or [])[:16]:
                if not c.get("name"):
                    continue
                cast.append({
                    "name": c["name"],
                    "role": c.get("character") or "",
                    "photo": (TMDB_IMG + "w185" + c["profile_path"]) if c.get("profile_path") else None,
                })
            crew = cr.get("crew") or []

            def pick(jobs):
                out = []
                for c in crew:
                    if c.get("job") in jobs and c.get("name") and c["name"] not in out:
                        out.append(c["name"])
                return out[:3]
            directors = pick(("Director",))
            if not directors and best_kind == "tv":
                directors = [c["name"] for c in (det.get("created_by") or []) if c.get("name")][:3]
            runtime = det.get("runtime") or ((det.get("episode_run_time") or [None])[0])
            rating = det.get("vote_average") or 0
            result = {"cast": cast,
                      "directors": directors,
                      "writers": pick(("Writer", "Screenplay", "Story", "Creator")),
                      "genres": [g["name"] for g in (det.get("genres") or []) if g.get("name")][:5],
                      "tagline": (det.get("tagline") or "").strip(),
                      "runtime": runtime or None,
                      "rating": round(rating, 1) if (rating and (det.get("vote_count") or 0) >= 20) else None,
                      "countries": [c["name"] for c in (det.get("production_countries") or []) if c.get("name")][:2],
                      "country_codes": [c["iso_3166_1"] for c in (det.get("production_countries") or []) if c.get("iso_3166_1")][:2],
                      "seasons": det.get("number_of_seasons") if best_kind == "tv" else None}
    except Exception:
        result = None
    if result is not None:
        with _tmdb_lock:
            if len(_cast_cache) > 2048:
                _cast_cache.clear()
            _cast_cache[ck] = result
    return result


@app.route("/api/cast")
def api_cast():
    title = (request.args.get("title") or "").strip()
    if not title:
        return json_error("Не передано название", 400)
    try:
        year = int(request.args.get("year") or 0) or None
    except ValueError:
        year = None
    orig = (request.args.get("orig") or "").strip() or None
    series = request.args.get("series") == "1"
    res = tmdb_cast(title, year, orig, series)
    if not res:
        return json_cached({"ok": True, "cast": [], "directors": [], "writers": [], "genres": []}, 300)
    return json_cached({"ok": True, **res}, 86400)


@app.route("/api/film")
def api_film():
    url = (request.args.get("url") or "").strip()
    if not url:
        return json_error("Не передан url", 400)

    key = f"film:{url}"
    cached = cache_get(key, 600)
    if cached is not None:
        return json_cached({"ok": True, "film": cached}, 600)

    try:
        if detect_source(url) == "kinogo":
            film = kinogo_film(url)
        else:
            film = hdrezka_film(url)
        cache_set(key, film)
        return json_cached({"ok": True, "film": film}, 600)
    except Exception as e:
        return json_error(f"{type(e).__name__}: {e}")


@app.route("/api/stream")
def api_stream():
    url = (request.args.get("url") or "").strip()
    if not url:
        return json_error("Не передан url", 400)

    key = f"stream:{url}:{request.args.get('season')}:{request.args.get('episode')}:{request.args.get('translation')}"
    cached = cache_get(key, 300)
    if cached is not None:
        return json_cached({"ok": True, "stream": cached}, 300)

    try:
        if detect_source(url) == "kinogo":
            film = cache_get(f"film:{url}", 600) or kinogo_film(url)
            if not film.get("sources"):
                return json_error("Прямой поток не найден")
            data = {"kind": "hls", "origin": KINOGO_ORIGIN, "sources": film["sources"]}
        else:
            film = cache_get(f"film:{url}", 600) or hdrezka_film(url)
            if not film.get("sources"):
                return json_error("Прямой поток не найден")
            data = {"kind": "hls", "origin": HDREZKA_ORIGIN, "sources": film["sources"]}
        cache_set(key, data)
        return json_cached({"ok": True, "stream": data}, 300)
    except Exception as e:
        return json_error(f"{type(e).__name__}: {e}")


def _proxy_url(u, ref=None):
    out = "/api/proxy?url=" + quote(u, safe="")
    if ref:                        # origin плеера тянем и во вложенные ссылки:
        out += "&ref=" + quote(ref, safe="")   # иначе варианты/сегменты дадут 403
    return out


def _rewrite_m3u8(text, base, ref=None):
    """Абсолютизирует и заворачивает в прокси все URI внутри плейлиста."""
    out = []
    for line in text.splitlines():
        s = line.strip()
        if not s:
            out.append(line)
        elif s.startswith("#"):
            out.append(re.sub(r'URI="([^"]+)"',
                              lambda m: 'URI="%s"' % _proxy_url(urljoin(base, m.group(1)), ref), line))
        else:
            out.append(_proxy_url(urljoin(base, s), ref))
    return "\n".join(out)


def _rewrite_mpd(text, mpd_url, ref=None):
    """Абсолютизирует и заворачивает в прокси все URL внутри DASH-манифеста."""
    import html as _html

    bases = [_html.unescape(b).strip() for b in re.findall(r"<BaseURL>\s*([^<]+?)\s*</BaseURL>", text)]
    base = bases[0] if bases else mpd_url.rsplit("/", 1)[0] + "/"
    text = re.sub(r"<BaseURL>\s*[^<]+?\s*</BaseURL>\s*", "", text)

    def wrap(u):
        u = _html.unescape(u)
        if u.startswith("data:"):
            return u
        # $Number$/$Time$/$Bandwidth$ должны остаться как есть — dash.js подставит их сам
        return _proxy_url(urljoin(base, u), ref).replace("%24", "$")

    for attr in ("initialization", "media", "sourceURL"):
        text = re.sub(
            r'(\b%s=")([^"]+)(")' % attr,
            lambda m: m.group(1) + wrap(m.group(2)) + m.group(3),
            text,
        )
    return text


@app.route("/api/dash")
def api_dash():
    target = request.args.get("url")
    if not target:
        return json_error("Не передан url", 400)
    hint_transport(target, request.args.get("src"))
    ref = request.args.get("ref")
    ref = _header_safe(ref) if ref else _stravers_origin(target)
    if ref:
        _remember_stravers(target, ref)
    headers = dict(default_headers)
    if ref:
        headers["Referer"] = ref.rstrip("/") + "/"
        headers["Origin"] = ref.rstrip("/")
    try:
        r = video_get(target, headers=headers, timeout=30)
        if r.status_code != 200:
            return json_error(f"Источник вернул {r.status_code}", r.status_code)
        body = _rewrite_mpd(r.text, getattr(r, "url", None) or target, ref)
        return Response(body, headers={"Content-Type": "application/dash+xml",
                                       "Cache-Control": "no-store"})
    except Exception as e:
        return json_error(f"{type(e).__name__}: {e}")


@app.route("/api/proxy")
def api_proxy():
    target = request.args.get("url")
    hint_transport(target, request.args.get("src"))

    headers = dict(default_headers)
    # origin плеера: сначала из ссылки (надёжно, переживает рестарт/чужие воркеры),
    # затем — из карты процесса. Без него CDN stravers отвечает 403.
    ref = request.args.get("ref")
    ref = _header_safe(ref) if ref else _stravers_origin(target)
    if ref:                                    # ref надёжнее карты процесса:
        _remember_stravers(target, ref)        # запоминаем origin для этого CDN-хоста
    headers.update(_stravers_headers(target))
    if ref:
        headers["Referer"] = ref.rstrip("/") + "/"
        headers["Origin"] = ref.rstrip("/")
    if request.headers.get("Range"):
        headers["Range"] = request.headers["Range"]

    # Держим не больше PROXY_CONN_LIMIT одновременных запросов к прокси: телефон
    # открывает пачку соединений, а провайдер на лимит отвечает 429.
    slot = _proxy_slots
    if slot is not None and not slot.acquire(timeout=25):
        return json_error("Прокси занят, попробуйте ещё раз", 503)
    released = [slot is None]

    def release():
        if not released[0]:
            released[0] = True
            slot.release()

    try:
        upstream = video_get(target, headers=headers, stream=True, timeout=30)
    except Exception as e:
        release()
        return json_error(f"{type(e).__name__}: {e}")

    # Ошибку прокси не отдаём плееру как «видео» — иначе чёрный экран/поломка перемотки.
    if upstream.status_code in _RETRY_CODES:
        code = upstream.status_code
        upstream.close()
        release()
        return json_error(f"Источник/прокси перегружен ({code})", 503)

    ctype = upstream.headers.get("Content-Type", "").lower()
    it = upstream.iter_content(chunk_size=64 * 1024)
    try:
        first = next(it)
    except StopIteration:
        first = b""

    # плейлист HLS -> переписываем все ссылки на прокси
    if "mpegurl" in ctype or first[:7] == b"#EXTM3U":
        # Источники вроде perepolokha отвечают 307 на CDN-хост, а внутри плейлиста
        # лежат абсолютные пути. Они валидны только на хосте после редиректа: на
        # исходном домене те же пути отдают 401. Поэтому база — финальный URL.
        base = getattr(upstream, "url", None) or target
        body = _rewrite_m3u8((first + b"".join(it)).decode("utf-8", "ignore"), base, ref)
        upstream.close()
        release()
        return Response(body, status=upstream.status_code,
                        headers={"Content-Type": "application/vnd.apple.mpegurl",
                                 "Cache-Control": "no-store"})

    resp_headers = {"Accept-Ranges": "bytes", "Cache-Control": "no-store"}
    for h in ("Content-Type", "Content-Length", "Content-Range"):
        if h in upstream.headers:
            resp_headers[h] = upstream.headers[h]

    def gen():
        try:
            yield first
            for chunk in it:
                yield chunk
        finally:
            upstream.close()  # вернуть соединение в пул (или закрыть при обрыве)
            release()

    resp = Response(stream_with_context(gen()), status=upstream.status_code, headers=resp_headers)
    resp.call_on_close(release)  # подстраховка, если тело так и не поехало
    return resp


@app.route("/api/subtitle")
def api_subtitle():
    target = request.args.get("url")
    if not target:
        return json_error("Не передан url", 400)
    try:
        r = video_get(target, headers=default_headers, timeout=30)
        return Response(r.content, status=r.status_code,
                        headers={"Content-Type": "text/vtt; charset=utf-8", "Cache-Control": "no-store"})
    except Exception as e:
        return json_error(f"{type(e).__name__}: {e}")


_img_cache = {}
_img_lock = threading.Lock()


# источники отдают постеры всего ~200×280, поэтому недостающие пиксели
# добираем апскейлом (LANCZOS) + лёгким unsharp, иначе карточки/баннер мылятся.
_IMG_MAX_UPSCALE = 4.0


def _make_thumb(data, w, h):
    """Приводит постер к боксу w×h: уменьшает или аккуратно апскейлит, отдаёт WebP.

    Сохраняем пропорции (fit=cover, как object-fit в CSS) и не раздуваем картинку
    сильнее _IMG_MAX_UPSCALE, чтобы апскейл из 200×280 не превращался в кашу.
    """
    if Image is None or not w or not h:
        return None
    try:
        im = Image.open(io.BytesIO(data))
        im = im.convert("RGB")
        tw, th = im.size
        if not tw or not th:
            return None
        scale = min(max(w / tw, h / th), _IMG_MAX_UPSCALE)  # cover, но с лимитом
        nw, nh = max(1, round(tw * scale)), max(1, round(th * scale))
        if (nw, nh) != (tw, th):
            im = im.resize((nw, nh), Image.LANCZOS)
        if scale > 1.02:  # апскейл смягчает детали — компенсируем шарпом
            im = im.filter(ImageFilter.UnsharpMask(radius=1.2, percent=88, threshold=3))
        buf = io.BytesIO()
        im.save(buf, "WEBP", quality=92, method=6)
        return buf.getvalue()
    except Exception:
        return None


@app.route("/api/qr")
def api_qr():
    """QR-код как SVG (запасной вариант для ТВ, где не грузится JS-библиотека с CDN)."""
    try:
        import segno
    except Exception:
        return Response("segno not installed", status=500)
    t = (request.args.get("t") or "")[:300]
    if not t:
        return Response("no text", status=400)
    buf = io.BytesIO()
    segno.make(t, error="m").save(buf, kind="svg", scale=10, border=0, dark="#111", light="#fff", xmldecl=False)
    return Response(buf.getvalue(), mimetype="image/svg+xml",
                    headers={"Cache-Control": "public, max-age=3600"})


@app.route("/api/img")
def api_img():
    """Прокси картинок: hdrezka (сайт за Cloudflare) и постеры TMDb.

    Параметры w/h включают серверный ресайз (Retina-резкость + меньше трафика).
    Отдаём с длинным Cache-Control — браузер и service worker держат их локально,
    поэтому повторные открытия показывают постеры мгновенно.
    """
    target = request.args.get("url")
    if not target:
        return json_error("Не передан url", 400)
    try:
        w = min(1600, int(request.args.get("w") or 0))
        h = min(2400, int(request.args.get("h") or 0))
    except (TypeError, ValueError):
        w = h = 0

    key = (target, w, h)
    with _img_lock:
        cached = _img_cache.get(key)
    if cached is not None:
        data, ctype = cached
        return Response(data, content_type=ctype,
                        headers={"Cache-Control": "public, max-age=604800"})

    try:
        # TMDb — прямой доступ; hdrezka за Cloudflare — через наш прокси-сеанс
        if (urlparse(target).hostname or "").lower() == "image.tmdb.org":
            r = _get_session().get(target, timeout=15)
        else:
            r = _hd_get(target, timeout=25)
    except Exception as e:
        return json_error(f"{type(e).__name__}: {e}")
    if r.status_code != 200:
        return ("", r.status_code)

    data = r.content
    ctype = r.headers.get("Content-Type", "image/jpeg")
    thumb = _make_thumb(data, w, h)
    if thumb is not None:
        data, ctype = thumb, "image/webp"
    with _img_lock:
        if len(_img_cache) > 512:  # простая защита от роста памяти
            _img_cache.clear()
        _img_cache[key] = (data, ctype)
    return Response(data, content_type=ctype,
                    headers={"Cache-Control": "public, max-age=604800"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    host = os.environ.get("HOST", "0.0.0.0")
    print(f"Сервер: http://{host}:{port}/")
    print(f"Киного: {KINOGO_ORIGIN}   HDRezka: {HDREZKA_ORIGIN}")
    app.run(host=host, port=port, threaded=True)
