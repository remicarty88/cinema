"""
OneCinema — собственная аналитика (без сторонних сервисов).

Подключение в server.py (две строки после создания `app`):
    from analytics import bp as analytics_bp
    app.register_blueprint(analytics_bp)

Переменные окружения:
    ADMIN_TOKEN            — пароль к админке /admin (без него админка отключена)
    FIREBASE_DB_URL        — адрес Realtime Database для живых данных об аккаунтах и комнатах
                             (задаётся окружением; тот же, что в index.html)
    FIREBASE_ROOT          — корневой узел в базе (задаётся окружением)
    ANALYTICS_DB           — путь к SQLite (задаётся окружением).
                             На Railway/Docker смонтируйте том и укажите путь на нём,
                             иначе база пропадёт при редеплое.
    ANALYTICS_UTC_OFFSET   — часовой пояс для графиков по дням, часы
    ANALYTICS_RETAIN_DAYS  — сколько хранить сырые события (0/не задано — не чистить)

Техработы и блокировки (управляются из админки, вкладка «Доступ»):
    режим техработ и список заблокированных хранятся в той же SQLite (ANALYTICS_DB),
    поэтому для сохранения между редеплоями нужен том, как и для аналитики.

Доступ по разрешению (вкладка «Доступ» → «Вход по разрешению»):
    пока режим включён, сайт открывается только браузерам из списка разрешённых.
    Остальные видят обычную страницу «404, сайт не работает», но при этом попадают
    в админку в список «Ожидают». Нажали «Разрешить» — у человека сайт открывается сам.
    По умолчанию режим выключен. Список лежит в той же SQLite (таблица access).

Приватность: IP-адреса и точные User-Agent не сохраняются. Посетитель — случайный
UUID из localStorage. Пишутся только грубые поля: устройство, браузер, источник.
"""
import hmac
import html as _html
import json
import os
import re
import sqlite3
import threading
import time
from collections import defaultdict, deque
from urllib.parse import urlparse

from flask import Blueprint, Response, jsonify, redirect, request

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("ANALYTICS_DB", "").strip()
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "").strip()
UTC_OFFSET = int(os.environ.get("ANALYTICS_UTC_OFFSET", "").strip() or 0)
RETAIN_DAYS = int(os.environ.get("ANALYTICS_RETAIN_DAYS", "").strip() or 0)
RTDB_URL = os.environ.get("FIREBASE_DB_URL", "").strip().rstrip("/")
RTDB_ROOT = os.environ.get("FIREBASE_ROOT", "kino88").strip().strip("/") or "kino88"
FIREBASE_WEB_API_KEY = os.environ.get("FIREBASE_WEB_API_KEY", "").strip()
ADMIN_HTML = os.path.join(BASE_DIR, "admin.html")

bp = Blueprint("analytics", __name__)

EVENT_TYPES = {
    "session", "ping", "install", "register", "search", "film_open", "play",
    "play_fail", "room_create", "room_join", "chat", "voice_mic",
}
BOT_RE = re.compile(r"bot|crawl|spider|slurp|headless|lighthouse|preview|facebookexternalhit", re.I)
UUID_RE = re.compile(r"^[A-Za-z0-9_-]{8,64}$")

_db_lock = threading.Lock()
_conn = None
_rate = defaultdict(lambda: deque(maxlen=120))
_last_purge = 0


# ------------------------------------------------------------------ БД
def _db():
    global _conn
    if _conn is None:
        d = os.path.dirname(DB_PATH)
        if d:
            os.makedirs(d, exist_ok=True)
        _conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=15)
        _conn.execute("PRAGMA journal_mode=WAL")
        _conn.execute("PRAGMA synchronous=NORMAL")
        _conn.executescript("""
        CREATE TABLE IF NOT EXISTS events(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            vid TEXT NOT NULL,
            sid TEXT,
            type TEXT NOT NULL,
            src TEXT,
            device TEXT,
            browser TEXT,
            pwa INTEGER DEFAULT 0,
            data TEXT
        );
        CREATE INDEX IF NOT EXISTS ix_events_ts ON events(ts);
        CREATE INDEX IF NOT EXISTS ix_events_type_ts ON events(type, ts);
        CREATE INDEX IF NOT EXISTS ix_events_vid ON events(vid);
        CREATE TABLE IF NOT EXISTS visitors(
            vid TEXT PRIMARY KEY,
            first_ts INTEGER NOT NULL,
            last_ts INTEGER NOT NULL,
            src TEXT,
            device TEXT,
            browser TEXT,
            registered INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS settings(
            k TEXT PRIMARY KEY,
            v TEXT
        );
        CREATE TABLE IF NOT EXISTS blocks(
            key TEXT PRIMARY KEY,
            kind TEXT NOT NULL,
            reason TEXT,
            name TEXT,
            parent TEXT,
            ts INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS ix_blocks_parent ON blocks(parent);
        CREATE TABLE IF NOT EXISTS access(
            vid TEXT PRIMARY KEY,
            uid TEXT,
            status TEXT NOT NULL,
            name TEXT,
            device TEXT,
            browser TEXT,
            src TEXT,
            first_ts INTEGER NOT NULL,
            last_ts INTEGER NOT NULL,
            decided_ts INTEGER,
            hits INTEGER DEFAULT 1
        );
        CREATE INDEX IF NOT EXISTS ix_access_status ON access(status, last_ts);
        CREATE TABLE IF NOT EXISTS idmap(
            uid TEXT NOT NULL,
            vid TEXT NOT NULL,
            ts INTEGER NOT NULL,
            PRIMARY KEY(uid, vid)
        );
        """)
        _conn.commit()
    return _conn


def _purge():
    global _last_purge
    now = time.time()
    if now - _last_purge < 6 * 3600:
        return
    _last_purge = now
    if RETAIN_DAYS <= 0:          # срок хранения не задан — сырые события не чистим
        return
    try:
        _db().execute("DELETE FROM events WHERE ts < ?", (int(now - RETAIN_DAYS * 86400),))
        _db().commit()
    except Exception:
        pass


# ------------------------------------------------------------------ разбор UA / источника
def _device(ua):
    if re.search(r"iPad|Tablet", ua):
        return "tablet"
    if re.search(r"Mobi|Android|iPhone", ua):
        return "mobile"
    return "desktop"


def _browser(ua):
    if "Telegram" in ua:
        return "Telegram"
    if re.search(r"YaBrowser", ua):
        return "Yandex"
    if re.search(r"SamsungBrowser", ua):
        return "Samsung"
    if re.search(r"Edg/", ua):
        return "Edge"
    if re.search(r"OPR/|Opera", ua):
        return "Opera"
    if re.search(r"Firefox|FxiOS", ua):
        return "Firefox"
    if re.search(r"CriOS|Chrome", ua):
        return "Chrome"
    if re.search(r"Safari", ua):
        return "Safari"
    return "other"


def _clip(v, n):
    return str(v or "")[:n]


def _source(raw_src, ref):
    """Откуда пришёл: pwa / shortcut / room-invite / домен реферера / direct."""
    s = _clip(raw_src, 40).lower()
    if s:
        return s
    if ref:
        try:
            host = urlparse(ref).netloc.lower().replace("www.", "")
            if host and host != request.host.lower().replace("www.", ""):
                return host[:60]
        except Exception:
            pass
    return "direct"


# ------------------------------------------------------------------ приём событий
@bp.route("/api/track", methods=["POST"])
def track():
    ua = request.headers.get("User-Agent", "")
    if BOT_RE.search(ua):
        return ("", 204)
    try:
        d = json.loads(request.get_data(cache=False, as_text=True)[:4000] or "{}")
    except Exception:
        return ("", 204)
    vid, typ = d.get("vid"), d.get("type")
    if typ not in EVENT_TYPES or not isinstance(vid, str) or not UUID_RE.match(vid):
        return ("", 204)

    now = time.time()
    q = _rate[vid]
    q.append(now)
    if len(q) == q.maxlen and now - q[0] < 60:      # >120 событий/мин от одного клиента
        return ("", 204)

    data = d.get("data") if isinstance(d.get("data"), dict) else {}
    clean = {}
    for k, v in list(data.items())[:8]:
        if isinstance(v, (int, float)):
            clean[_clip(k, 12)] = v
        else:
            clean[_clip(k, 12)] = _clip(v, 200)

    src = _source(d.get("src"), d.get("ref"))
    device, browser = _device(ua), _browser(ua)
    ts = int(now)
    try:
        with _db_lock:
            db = _db()
            row = db.execute("SELECT registered FROM visitors WHERE vid=?", (vid,)).fetchone()
            if row is None:
                db.execute("INSERT INTO visitors(vid,first_ts,last_ts,src,device,browser) VALUES(?,?,?,?,?,?)",
                           (vid, ts, ts, src, device, browser))
            else:
                db.execute("UPDATE visitors SET last_ts=? WHERE vid=?", (ts, vid))
            if typ == "register":
                if row is not None and row[0]:
                    db.commit()
                    return ("", 204)                # регистрация считается один раз
                db.execute("UPDATE visitors SET registered=1 WHERE vid=?", (vid,))
            db.execute(
                "INSERT INTO events(ts,vid,sid,type,src,device,browser,pwa,data) VALUES(?,?,?,?,?,?,?,?,?)",
                (ts, vid, _clip(d.get("sid"), 64), typ, src, device, browser,
                 1 if d.get("pwa") else 0, json.dumps(clean, ensure_ascii=False) if clean else None))
            db.commit()
        _purge()
    except Exception:
        pass
    return ("", 204)


# ------------------------------------------------------------------ авторизация админки
_fails = defaultdict(lambda: deque(maxlen=10))      # ip -> время неудачных попыток входа


def _eq(a, b):
    return hmac.compare_digest(str(a).encode("utf-8"), str(b).encode("utf-8"))


def _cookie_value():
    """В куку кладём не сам пароль, а производный HMAC."""
    return hmac.new(ADMIN_TOKEN.encode("utf-8"), b"oc_admin_v2", "sha256").hexdigest()


def _ip():
    fwd = request.headers.get("X-Forwarded-For", "")
    return (fwd.split(",")[0].strip() if fwd else request.remote_addr) or "?"


def _authed():
    if not ADMIN_TOKEN:
        return False
    c = request.cookies.get("oc_admin", "")
    if c and _eq(c, _cookie_value()):
        return True
    given = request.headers.get("Authorization", "").replace("Bearer ", "").strip()
    return bool(given) and _eq(given, ADMIN_TOKEN)


def _set_cookie(resp):
    secure = request.is_secure or request.headers.get("X-Forwarded-Proto", "") == "https"
    resp.set_cookie("oc_admin", _cookie_value(), max_age=30 * 86400,
                    httponly=True, samesite="Strict", secure=secure)
    return resp


# ------------------------------------------------------------------ агрегаты
def _day_expr():
    return "strftime('%%Y-%%m-%%d', ts + %d, 'unixepoch')" % (UTC_OFFSET * 3600)


def _stats(days):
    days = max(1, min(int(days), 365))
    now = int(time.time())
    since = now - days * 86400
    db = _db()
    day = _day_expr()
    out = {"days": days, "generated": now}

    def one(sql, *a):
        r = db.execute(sql, a).fetchone()
        return r[0] if r and r[0] is not None else 0

    with _db_lock:
        out["online"] = one("SELECT COUNT(DISTINCT vid) FROM events WHERE ts > ?", now - 120)
        out["totals"] = {
            "visitors": one("SELECT COUNT(DISTINCT vid) FROM events WHERE ts >= ?", since),
            "new_visitors": one("SELECT COUNT(*) FROM visitors WHERE first_ts >= ?", since),
            "sessions": one("SELECT COUNT(DISTINCT sid) FROM events WHERE type='session' AND ts >= ?", since),
            "registrations": one("SELECT COUNT(*) FROM events WHERE type='register' AND ts >= ?", since),
            "installs": one("SELECT COUNT(*) FROM events WHERE type='install' AND ts >= ?", since),
            "film_opens": one("SELECT COUNT(*) FROM events WHERE type='film_open' AND ts >= ?", since),
            "plays": one("SELECT COUNT(*) FROM events WHERE type='play' AND ts >= ?", since),
            "play_fails": one("SELECT COUNT(*) FROM events WHERE type='play_fail' AND ts >= ?", since),
            "searches": one("SELECT COUNT(*) FROM events WHERE type='search' AND ts >= ?", since),
            "rooms_created": one("SELECT COUNT(*) FROM events WHERE type='room_create' AND ts >= ?", since),
            "room_joins": one("SELECT COUNT(*) FROM events WHERE type='room_join' AND ts >= ?", since),
            "chat_msgs": one("SELECT COUNT(*) FROM events WHERE type='chat' AND ts >= ?", since),
            "voice_users": one("SELECT COUNT(DISTINCT vid) FROM events WHERE type='voice_mic' AND ts >= ?", since),
            "all_visitors": one("SELECT COUNT(*) FROM visitors"),
            "all_registered": one("SELECT COUNT(*) FROM visitors WHERE registered=1"),
        }
        # среднее «время в приложении»: пингов раз в 30 с -> число пингов на сессию * 30 с
        out["avg_session_sec"] = round(one(
            "SELECT AVG(c) FROM (SELECT COUNT(*) c FROM events WHERE type='ping' AND ts >= ? GROUP BY sid)",
            since) * 30)

        series = {}
        for key, sql in [
            ("visitors", "SELECT %s d, COUNT(DISTINCT vid) FROM events WHERE ts >= ? GROUP BY d"),
            ("new", "SELECT %s d, COUNT(*) FROM visitors WHERE first_ts >= ? GROUP BY d"),
            ("register", "SELECT %s d, COUNT(*) FROM events WHERE type='register' AND ts >= ? GROUP BY d"),
            ("play", "SELECT %s d, COUNT(*) FROM events WHERE type='play' AND ts >= ? GROUP BY d"),
            ("room", "SELECT %s d, COUNT(*) FROM events WHERE type='room_create' AND ts >= ? GROUP BY d"),
        ]:
            col = day if key != "new" else day.replace("ts +", "first_ts +")
            rows = db.execute(sql % col, (since,)).fetchall()
            series[key] = {r[0]: r[1] for r in rows}
        labels = []
        base = now + UTC_OFFSET * 3600
        for i in range(days - 1, -1, -1):
            labels.append(time.strftime("%Y-%m-%d", time.gmtime(base - i * 86400)))
        out["labels"] = labels
        out["series"] = {k: [v.get(l, 0) for l in labels] for k, v in series.items()}

        hours = db.execute(
            "SELECT CAST(strftime('%%H', ts + %d, 'unixepoch') AS INT) h, COUNT(DISTINCT vid) "
            "FROM events WHERE ts >= ? GROUP BY h" % (UTC_OFFSET * 3600), (since,)).fetchall()
        hh = {r[0]: r[1] for r in hours}
        out["hours"] = [hh.get(h, 0) for h in range(24)]

        def top(sql, *a):
            return [list(r) for r in db.execute(sql, a).fetchall()]

        out["sources"] = top(
            "SELECT src, COUNT(DISTINCT vid) c FROM events WHERE type='session' AND ts >= ? "
            "GROUP BY src ORDER BY c DESC LIMIT 12", since)
        out["devices"] = top(
            "SELECT device, COUNT(DISTINCT vid) c FROM events WHERE ts >= ? GROUP BY device ORDER BY c DESC", since)
        out["browsers"] = top(
            "SELECT browser, COUNT(DISTINCT vid) c FROM events WHERE ts >= ? GROUP BY browser ORDER BY c DESC LIMIT 8",
            since)
        out["pwa_share"] = [
            one("SELECT COUNT(DISTINCT vid) FROM events WHERE pwa=1 AND ts >= ?", since),
            out["totals"]["visitors"]]

        out["top_films"] = top(
            "SELECT json_extract(data,'$.t'), COUNT(*) c, COUNT(DISTINCT vid) u FROM events "
            "WHERE type='play' AND ts >= ? AND json_extract(data,'$.t') IS NOT NULL "
            "GROUP BY json_extract(data,'$.u') ORDER BY c DESC LIMIT 15", since)
        out["top_opened"] = top(
            "SELECT json_extract(data,'$.t'), COUNT(*) c FROM events "
            "WHERE type='film_open' AND ts >= ? AND json_extract(data,'$.t') IS NOT NULL "
            "GROUP BY json_extract(data,'$.u') ORDER BY c DESC LIMIT 15", since)
        out["top_searches"] = top(
            "SELECT lower(json_extract(data,'$.q')), COUNT(*) c, MIN(json_extract(data,'$.n')) FROM events "
            "WHERE type='search' AND ts >= ? GROUP BY lower(json_extract(data,'$.q')) "
            "ORDER BY c DESC LIMIT 15", since)
        out["empty_searches"] = top(
            "SELECT lower(json_extract(data,'$.q')), COUNT(*) c FROM events "
            "WHERE type='search' AND ts >= ? AND json_extract(data,'$.n') = 0 "
            "GROUP BY lower(json_extract(data,'$.q')) ORDER BY c DESC LIMIT 10", since)

        # воронка
        v = out["totals"]["visitors"]
        out["funnel"] = [
            ["Посетители", v],
            ["Открыли фильм", one("SELECT COUNT(DISTINCT vid) FROM events WHERE type='film_open' AND ts >= ?", since)],
            ["Запустили видео", one("SELECT COUNT(DISTINCT vid) FROM events WHERE type='play' AND ts >= ?", since)],
            ["Создали профиль", one("SELECT COUNT(DISTINCT vid) FROM events WHERE type='register' AND ts >= ?", since)],
            ["Вошли в комнату", one("SELECT COUNT(DISTINCT vid) FROM events WHERE type IN ('room_create','room_join') AND ts >= ?", since)],
        ]

        # удержание: из пришедших N дней назад, сколько вернулось позже
        def retention(n):
            lo, hi = now - (n + 1) * 86400, now - n * 86400
            cohort = one("SELECT COUNT(*) FROM visitors WHERE first_ts BETWEEN ? AND ?", lo, hi)
            if not cohort:
                return None
            back = one(
                "SELECT COUNT(*) FROM visitors WHERE first_ts BETWEEN ? AND ? AND last_ts >= first_ts + ?",
                lo, hi, n * 86400 - 3600)
            return [round(100.0 * back / cohort, 1), cohort]
        out["retention"] = {"d1": retention(1), "d7": retention(7), "d30": retention(30)}
    return out


# ------------------------------------------------------------------ маршруты админки
def _nocache(resp):
    resp.headers["Cache-Control"] = "no-store"
    resp.headers["X-Robots-Tag"] = "noindex"
    return resp


@bp.route("/admin/stats.json")
def stats_json():
    if not _authed():
        return ("Not found", 404)
    return _nocache(jsonify(_stats(request.args.get("days", 30))))


_FEED_SKIP = ("ping",)


@bp.route("/admin/live.json")
def live_json():
    """Лёгкий эндпоинт для опроса раз в несколько секунд: онлайн + последние события."""
    if not _authed():
        return ("Not found", 404)
    try:
        limit = max(1, min(int(request.args.get("limit", 40)), 100))
    except ValueError:
        limit = 40
    now = int(time.time())
    with _db_lock:
        db = _db()
        online = db.execute("SELECT COUNT(DISTINCT vid) FROM events WHERE ts > ?", (now - 120,)).fetchone()[0] or 0
        today = db.execute("SELECT COUNT(DISTINCT vid) FROM events WHERE ts > ?", (now - 86400,)).fetchone()[0] or 0
        pending = db.execute("SELECT COUNT(*) FROM access WHERE status='pending'").fetchone()[0] or 0
        rows = db.execute(
            "SELECT id,ts,type,vid,src,device,browser,pwa,data FROM events WHERE type!='ping' "
            "ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    feed = []
    for r in rows:
        try:
            data = json.loads(r[8]) if r[8] else {}
        except Exception:
            data = {}
        data.pop("u", None)                      # ссылки на фильмы в ленту не нужны
        feed.append({"id": r[0], "ts": r[1], "type": r[2], "who": r[3][:4], "src": r[4],
                     "device": r[5], "browser": r[6], "pwa": r[7], "data": data})
    return _nocache(jsonify({"now": now, "online": online, "visitors_24h": today, "pending": pending, "feed": feed}))


def _admin_page():
    try:
        with open(ADMIN_HTML, "r", encoding="utf-8") as f:
            html = f.read()
    except OSError:
        return Response("admin.html не найден рядом с analytics.py", status=500, mimetype="text/plain")
    cfg = json.dumps({"url": RTDB_URL, "root": RTDB_ROOT, "apiKey": FIREBASE_WEB_API_KEY})
    html = html.replace("__ADMIN_CFG__", cfg.replace("<", "\\u003c"))
    return _nocache(Response(html, mimetype="text/html"))


@bp.route("/admin/firebase-token")
def admin_firebase_token():
    if not _authed():
        return ("Not found", 404)
    if not FIREBASE_WEB_API_KEY or not RTDB_URL or not RTDB_ROOT:
        response = jsonify({"ok": False, "error": "Firebase Auth is not configured"})
        response.status_code = 503
        return _nocache(response)
    try:
        from firebase_admin_support import create_custom_token

        token = create_custom_token("onecinema-admin", {"admin": True})
        return _nocache(jsonify({"ok": True, "token": token}))
    except Exception as exc:
        response = jsonify({"ok": False, "error": type(exc).__name__})
        response.status_code = 503
        return _nocache(response)


@bp.route("/admin")
@bp.route("/admin/")
@bp.route("/admin/stats")
def admin_index():
    if not ADMIN_TOKEN:
        return ("Not found", 404)
    if _authed():
        return _admin_page()
    return _nocache(Response(LOGIN_HTML.replace("__ERR__", ""), mimetype="text/html", status=200))


@bp.route("/admin/login", methods=["POST"])
def admin_login():
    if not ADMIN_TOKEN:
        return ("Not found", 404)
    ip, now = _ip(), time.time()
    q = _fails[ip]
    if len(q) >= 8 and now - q[0] < 600:
        return _nocache(Response(LOGIN_HTML.replace("__ERR__", "Слишком много попыток. Подождите 10 минут."),
                                 mimetype="text/html", status=429))
    pw = (request.form.get("password") or "").strip()
    if pw and _eq(pw, ADMIN_TOKEN):
        q.clear()
        return _set_cookie(redirect("/admin"))
    q.append(now)
    return _nocache(Response(LOGIN_HTML.replace("__ERR__", "Неверный пароль"), mimetype="text/html", status=401))


@bp.route("/admin/logout")
def admin_logout():
    resp = redirect("/admin")
    resp.delete_cookie("oc_admin")
    return resp


# ------------------------------------------------------------------ техработы и блокировки
# Состояние хранится в SQLite и кэшируется в памяти на 3 с, чтобы проверка на каждый запрос
# ничего не стоила. Если база недоступна, сайт НЕ выключается (работаем по последнему состоянию).
COOKIE_UID, COOKIE_VID = "k88uid", "k88vid"
_GATE_FREE = ("/admin", "/api/status", "/api/hello", "/sw.js", "/manifest.json", "/apple-touch-icon.png",
              "/icon-", "/favicon")
DEFAULT_MAINT_MSG = "Мы обновляем OneCinema. Скоро всё заработает — загляните чуть позже."
ID_RE = re.compile(r"^[A-Za-z0-9_-]{8,64}$")

_ctl_cache = {"t": 0.0, "maint": {"on": False, "msg": "", "until": 0, "since": 0}, "uids": {}, "vids": {},
              "appr": False, "allowed": frozenset()}
_seen_pairs = set()


def _ctl_state(force=False):
    global _ctl_cache
    now = time.time()
    if not force and now - _ctl_cache["t"] < 3:
        return _ctl_cache
    new = {"t": now, "maint": _ctl_cache["maint"], "uids": _ctl_cache["uids"], "vids": _ctl_cache["vids"],
           "appr": _ctl_cache["appr"], "allowed": _ctl_cache["allowed"]}
    try:
        with _db_lock:
            db = _db()
            st = dict(db.execute("SELECT k,v FROM settings").fetchall())
            rows = db.execute("SELECT key,kind,reason,name,parent FROM blocks").fetchall()
            allowed = db.execute("SELECT vid FROM access WHERE status='allowed'").fetchall()
        new["maint"] = {
            "on": st.get("maint_on") == "1",
            "msg": st.get("maint_msg") or "",
            "until": int(st.get("maint_until") or 0),
            "since": int(st.get("maint_since") or 0),
        }
        new["appr"] = st.get("approval_on") == "1"
        new["allowed"] = frozenset(r[0] for r in allowed)
        uids, vids = {}, {}
        for key, kind, reason, name, parent in rows:
            (uids if kind == "uid" else vids)[key] = {"reason": reason or "", "name": name or "", "parent": parent or ""}
        new["uids"], new["vids"] = uids, vids
    except Exception:
        pass
    _ctl_cache = new
    return new


def _cookie_id(name):
    v = request.cookies.get(name, "")
    return v if v and ID_RE.match(v) else ""


def _remember_pair(uid, vid):
    """Запоминаем, с каких браузеров заходил профиль: блокировка профиля закроет и их."""
    k = uid + "|" + vid
    if k in _seen_pairs:
        return
    if len(_seen_pairs) > 50000:
        _seen_pairs.clear()
    _seen_pairs.add(k)
    try:
        with _db_lock:
            db = _db()
            db.execute("INSERT OR IGNORE INTO idmap(uid,vid,ts) VALUES(?,?,?)", (uid, vid, int(time.time())))
            db.commit()
    except Exception:
        pass


def _blocked_info(st, uid, vid):
    if uid and uid in st["uids"]:
        info = st["uids"][uid]
        if vid and vid not in st["vids"]:           # новый браузер заблокированного профиля — закрываем и его
            try:
                with _db_lock:
                    db = _db()
                    db.execute("INSERT OR IGNORE INTO blocks(key,kind,reason,name,parent,ts) VALUES(?,?,?,?,?,?)",
                               (vid, "vid", info["reason"], info["name"], uid, int(time.time())))
                    db.commit()
                _ctl_state(True)
            except Exception:
                pass
        return info
    if vid and vid in st["vids"]:
        return st["vids"][vid]
    return None


def _locked(st, vid):
    """Включён вход по разрешению, а этого браузера нет в списке разрешённых."""
    return bool(st["appr"]) and not (vid and vid in st["allowed"])


def _notfound_response(is_api):
    """Выглядит как «сайта нет»: обычная 404, без намёка на закрытый доступ."""
    if is_api:
        resp = jsonify({"ok": False, "error": "not_found"})
        resp.status_code = 404
    else:
        resp = Response(NOTFOUND_HTML, status=404, mimetype="text/html")
    return resp


def _retry_after(m):
    left = m["until"] - int(time.time()) if m["until"] else 0
    return str(min(max(left, 30), 3600) if left > 0 else 300)


@bp.before_app_request
def _gate():
    """Единая проверка для всего сайта: блокировка -> 403, техработы -> 503,
    нет разрешения -> 404 (когда включён вход по разрешению)."""
    if request.method == "OPTIONS":
        return None
    path = request.path
    if path.startswith(_GATE_FREE):
        return None
    st = _ctl_state()
    uid, vid = _cookie_id(COOKIE_UID), _cookie_id(COOKIE_VID)
    maint = st["maint"]["on"]
    blocked = _blocked_info(st, uid, vid)
    locked = _locked(st, vid)
    if not maint and not blocked and not locked:
        if uid and vid:
            _remember_pair(uid, vid)
        return None
    if _authed():                                    # администратор видит сайт всегда
        return None
    if path == "/api/track":
        return ("", 204)
    is_api = path.startswith("/api/")
    if blocked:
        if is_api:
            resp = jsonify({"ok": False, "blocked": True, "error": "blocked", "reason": blocked["reason"]})
            resp.status_code = 403
        else:
            resp = Response(_status_page("blocked", blocked["reason"], 0), status=403, mimetype="text/html")
    elif maint:
        m = st["maint"]
        if is_api:
            resp = jsonify({"ok": False, "maintenance": True, "error": "maintenance",
                            "message": m["msg"] or DEFAULT_MAINT_MSG, "until": m["until"] or None})
            resp.status_code = 503
        else:
            resp = Response(_status_page("maint", m["msg"], m["until"]), status=503, mimetype="text/html")
        resp.headers["Retry-After"] = _retry_after(m)
    else:
        resp = _notfound_response(is_api)
    resp.headers["Cache-Control"] = "no-store"
    resp.headers["X-Robots-Tag"] = "noindex"
    return resp


@bp.route("/api/status")
def api_status():
    """Лёгкий опрос с клиента: сайт открыт? я не заблокирован? Не кэшируется."""
    st = _ctl_state()
    uid, vid = _cookie_id(COOKIE_UID), _cookie_id(COOKIE_VID)
    adm = _authed()
    blocked = None if adm else _blocked_info(st, uid, vid)
    m = st["maint"]
    resp = jsonify({
        "maintenance": bool(m["on"]) and not adm,
        "blocked": bool(blocked),
        "locked": (not adm) and _locked(st, vid),
        "admin": adm,
        "message": (m["msg"] or DEFAULT_MAINT_MSG) if m["on"] else "",
        "until": m["until"] or None,
        "reason": blocked["reason"] if blocked else "",
    })
    resp.headers["Cache-Control"] = "no-store"
    return resp


_hello_rate = defaultdict(lambda: deque(maxlen=30))
ACCESS_MAX_PENDING = 1000


@bp.route("/api/hello", methods=["POST"])
def api_hello():
    """Страница-404 сообщает: «я здесь». Так человек появляется в админке в списке «Ожидают».
    Сам по себе доступа не даёт."""
    ua = request.headers.get("User-Agent", "")
    if BOT_RE.search(ua):
        return ("", 204)
    now = time.time()
    ip = _ip()
    q = _hello_rate[ip]
    q.append(now)
    if len(q) == q.maxlen and now - q[0] < 600:          # не больше 30 обращений за 10 минут с одного адреса
        return ("", 204)
    if len(_hello_rate) > 5000:
        _hello_rate.clear()
    st = _ctl_state()
    gate_vid = _cookie_id(COOKIE_VID)
    if not st["appr"]:
        return _nocache(jsonify({"ok": True, "locked": False}))
    if gate_vid and gate_vid in st["allowed"]:
        return _nocache(jsonify({"ok": True, "locked": False}))
    d = request.get_json(silent=True, force=True)
    d = d if isinstance(d, dict) else {}
    body_vid = d.get("vid") if isinstance(d.get("vid"), str) else ""
    vid = gate_vid or (body_vid if ID_RE.match(body_vid) else "")
    if not vid:
        return ("", 204)
    body_uid = d.get("uid") if isinstance(d.get("uid"), str) else ""
    uid = body_uid if ID_RE.match(body_uid) else ""
    device, browser = _device(ua), _browser(ua)
    src = _clip(request.args.get("source") or d.get("src"), 40).lower()
    ts = int(now)
    try:
        with _db_lock:
            db = _db()
            row = db.execute("SELECT status FROM access WHERE vid=?", (vid,)).fetchone()
            if row is None:
                n = db.execute("SELECT COUNT(*) FROM access WHERE status='pending'").fetchone()[0]
                if n < ACCESS_MAX_PENDING:
                    db.execute("INSERT INTO access(vid,uid,status,device,browser,src,first_ts,last_ts,hits) "
                               "VALUES(?,?,?,?,?,?,?,?,1)", (vid, uid or None, "pending", device, browser, src, ts, ts))
            else:
                db.execute("UPDATE access SET last_ts=?, hits=hits+1, uid=COALESCE(?,uid), device=?, browser=? "
                           "WHERE vid=?", (ts, uid or None, device, browser, vid))
            db.commit()
    except Exception:
        pass
    return _nocache(jsonify({"ok": True, "locked": True, "code": vid[:6]}))


def _status_page(kind, text, until):
    if kind == "maint":
        title, head = "Технические работы", "Идут технические работы"
        body = _html.escape(text or DEFAULT_MAINT_MSG)
        want = "maintenance"
        icon_cls = "wrench"
    else:
        title, head = "Доступ ограничен", "Доступ ограничен"
        body = "Ваш доступ к OneCinema ограничен администратором." + (
            "<br><span class=\"why\">Причина: " + _html.escape(text) + "</span>" if text else "")
        want = "blocked"
        icon_cls = "lock"
    page = STATUS_HTML
    for k, v in (("__TITLE__", title), ("__HEAD__", head), ("__BODY__", body), ("__WANT__", want),
                 ("__UNTIL__", str(int(until or 0))), ("__ICON__", icon_cls)):
        page = page.replace(k, v)
    return page


# ------------------------------------------------------------------ админка: управление
def _json_body():
    if not request.is_json:                          # форма с чужого сайта не пройдёт: нужен JSON
        return None
    d = request.get_json(silent=True)
    return d if isinstance(d, dict) else None


def _bad(msg, code=400):
    resp = jsonify({"ok": False, "error": msg})
    resp.status_code = code
    return _nocache(resp)


def _control_payload():
    st = _ctl_state(True)
    m = st["maint"]
    vids_by_parent = defaultdict(int)
    for v in st["vids"].values():
        if v["parent"]:
            vids_by_parent[v["parent"]] += 1
    rows, access = [], []
    with _db_lock:
        db = _db()
        for key, reason, name, ts in db.execute(
                "SELECT key,reason,name,ts FROM blocks WHERE kind='uid' ORDER BY ts DESC").fetchall():
            rows.append({"uid": key, "reason": reason or "", "name": name or "", "ts": ts,
                         "browsers": vids_by_parent.get(key, 0)})
        counts = {"pending": 0, "allowed": 0, "denied": 0}
        counts.update(dict(db.execute("SELECT status, COUNT(*) FROM access GROUP BY status").fetchall()))
        for r in db.execute(
                "SELECT vid,uid,status,name,device,browser,src,first_ts,last_ts,decided_ts,hits FROM access "
                "ORDER BY CASE status WHEN 'pending' THEN 0 WHEN 'allowed' THEN 1 ELSE 2 END, last_ts DESC "
                "LIMIT 600").fetchall():
            access.append({"vid": r[0], "uid": r[1] or "", "status": r[2], "name": r[3] or "", "device": r[4] or "",
                           "browser": r[5] or "", "src": r[6] or "", "first": r[7], "last": r[8],
                           "decided": r[9] or 0, "hits": r[10] or 0})
        known = db.execute("SELECT COUNT(*) FROM visitors WHERE vid NOT IN (SELECT vid FROM access)").fetchone()[0]
    return {"ok": True, "now": int(time.time()),
            "maintenance": {"on": m["on"], "message": m["msg"], "until": m["until"], "since": m["since"]},
            "blocks": rows,
            "approval": {"on": st["appr"]},
            "access": access, "access_counts": counts, "access_known": known}


@bp.route("/admin/control.json")
def control_json():
    if not _authed():
        return ("Not found", 404)
    return _nocache(jsonify(_control_payload()))


@bp.route("/admin/maintenance", methods=["POST"])
def control_maintenance():
    if not _authed():
        return ("Not found", 404)
    d = _json_body()
    if d is None:
        return _bad("Ожидается JSON")
    on = bool(d.get("on"))
    msg = _clip(d.get("message"), 300).strip()
    try:
        until = int(d.get("until") or 0)
    except (TypeError, ValueError):
        until = 0
    if until < 0 or until > 4102444800:
        until = 0
    now = int(time.time())
    try:
        with _db_lock:
            db = _db()
            was = (db.execute("SELECT v FROM settings WHERE k='maint_on'").fetchone() or ["0"])[0] == "1"
            vals = {"maint_on": "1" if on else "0", "maint_msg": msg, "maint_until": str(until)}
            if on and not was:
                vals["maint_since"] = str(now)
            for k, v in vals.items():
                db.execute("INSERT INTO settings(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, v))
            db.commit()
    except Exception as e:
        return _bad("Не удалось сохранить: %s" % e, 500)
    return _nocache(jsonify(_control_payload()))


@bp.route("/admin/block", methods=["POST"])
def control_block():
    if not _authed():
        return ("Not found", 404)
    d = _json_body()
    if d is None:
        return _bad("Ожидается JSON")
    uid = str(d.get("uid") or "").strip()
    if not ID_RE.match(uid):
        return _bad("Некорректный ID пользователя")
    reason, name, now = _clip(d.get("reason"), 200).strip(), _clip(d.get("name"), 40).strip(), int(time.time())
    try:
        with _db_lock:
            db = _db()
            db.execute("INSERT INTO blocks(key,kind,reason,name,parent,ts) VALUES(?,?,?,?,?,?) "
                       "ON CONFLICT(key) DO UPDATE SET kind='uid', reason=excluded.reason, name=excluded.name, "
                       "parent=NULL, ts=excluded.ts", (uid, "uid", reason, name, None, now))
            for (vid,) in db.execute("SELECT vid FROM idmap WHERE uid=?", (uid,)).fetchall():
                db.execute("INSERT OR IGNORE INTO blocks(key,kind,reason,name,parent,ts) VALUES(?,?,?,?,?,?)",
                           (vid, "vid", reason, name, uid, now))
            db.commit()
    except Exception as e:
        return _bad("Не удалось сохранить: %s" % e, 500)
    return _nocache(jsonify(_control_payload()))


@bp.route("/admin/unblock", methods=["POST"])
def control_unblock():
    if not _authed():
        return ("Not found", 404)
    d = _json_body()
    if d is None:
        return _bad("Ожидается JSON")
    uid = str(d.get("uid") or "").strip()
    if not ID_RE.match(uid):
        return _bad("Некорректный ID пользователя")
    try:
        with _db_lock:
            db = _db()
            db.execute("DELETE FROM blocks WHERE key=? OR parent=?", (uid, uid))
            db.commit()
    except Exception as e:
        return _bad("Не удалось сохранить: %s" % e, 500)
    return _nocache(jsonify(_control_payload()))


def _vid_arg(d):
    vid = str(d.get("vid") or "").strip()
    return vid if ID_RE.match(vid) else ""


def _set_access(vid, status, name=None):
    """Выставить статус браузеру. Если записи нет — создаём."""
    ts = int(time.time())
    with _db_lock:
        db = _db()
        db.execute(
            "INSERT INTO access(vid,status,name,first_ts,last_ts,decided_ts,hits) VALUES(?,?,?,?,?,?,0) "
            "ON CONFLICT(vid) DO UPDATE SET status=excluded.status, decided_ts=excluded.decided_ts, "
            "name=CASE WHEN ? THEN excluded.name ELSE name END",
            (vid, status, name or None, ts, ts, ts, 1 if name else 0))
        db.commit()


@bp.route("/admin/access/approval", methods=["POST"])
def access_approval():
    """Включить / выключить режим «вход по разрешению»."""
    if not _authed():
        return ("Not found", 404)
    d = _json_body()
    if d is None:
        return _bad("Ожидается JSON")
    try:
        with _db_lock:
            db = _db()
            db.execute("INSERT INTO settings(k,v) VALUES('approval_on',?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                       ("1" if d.get("on") else "0",))
            db.commit()
    except Exception as e:
        return _bad("Не удалось сохранить: %s" % e, 500)
    return _nocache(jsonify(_control_payload()))


@bp.route("/admin/access/<action>", methods=["POST"])
def access_action(action):
    if not _authed():
        return ("Not found", 404)
    if action not in ("allow", "deny", "delete", "allow_known", "allow_pending"):
        return ("Not found", 404)
    d = _json_body()
    if d is None:
        return _bad("Ожидается JSON")
    ts = int(time.time())
    try:
        if action in ("allow", "deny", "delete"):
            vid = _vid_arg(d)
            if not vid:
                return _bad("Некорректный ID браузера")
            if action == "allow":
                _set_access(vid, "allowed", _clip(d.get("name"), 40).strip())
            elif action == "deny":
                _set_access(vid, "denied")
            else:
                with _db_lock:
                    _db().execute("DELETE FROM access WHERE vid=?", (vid,))
                    _db().commit()
        elif action == "allow_pending":
            with _db_lock:
                _db().execute("UPDATE access SET status='allowed', decided_ts=? WHERE status='pending'", (ts,))
                _db().commit()
        else:                                   # allow_known: все, кто заходил на сайт раньше
            with _db_lock:
                _db().execute(
                    "INSERT OR IGNORE INTO access(vid,status,device,browser,src,first_ts,last_ts,decided_ts,hits) "
                    "SELECT vid,'allowed',device,browser,src,first_ts,last_ts,?,0 FROM visitors", (ts,))
                _db().commit()
    except Exception as e:
        return _bad("Не удалось сохранить: %s" % e, 500)
    return _nocache(jsonify(_control_payload()))


NOTFOUND_HTML = r"""<!doctype html>
<html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="robots" content="noindex"><meta name="color-scheme" content="light dark">
<title>404 — Страница не найдена</title>
<style>
*{box-sizing:border-box}
html,body{height:100%}
body{margin:0;display:grid;place-items:center;padding:24px;background:#fff;color:#222;
font:16px/1.5 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif}
@media(prefers-color-scheme:dark){body{background:#121212;color:#ddd}}
main{text-align:center;max-width:420px}
h1{margin:0;font-size:84px;font-weight:300;letter-spacing:-.02em;line-height:1}
h2{margin:14px 0 8px;font-size:20px;font-weight:500}
p{margin:0;color:#777}
.ref{margin-top:40px;font-size:12px;color:#aaa;font-family:ui-monospace,Menlo,Consolas,monospace}
</style></head>
<body>
<main>
<h1>404</h1>
<h2>Сайт не найден</h2>
<p>Сайт временно недоступен или больше не работает.</p>
<div class="ref" id="ref"></div>
</main>
<script>
(function(){
  function ls(k, v){ try{ if(v === undefined) return localStorage.getItem(k); localStorage.setItem(k, v); }catch(e){} }
  function rid(){
    try{ if(crypto.randomUUID) return crypto.randomUUID().replace(/-/g, ""); }catch(e){}
    return (Date.now().toString(36) + Math.random().toString(36).slice(2) + Math.random().toString(36).slice(2)).slice(0, 32);
  }
  /* те же ключи, что и на сайте: после разрешения человек остаётся «тем же» */
  var vid = ls("k88:vid"); if(!vid){ vid = rid(); ls("k88:vid", vid); }
  var uid = ls("k88:uid");
  if(!uid){
    uid = (window.crypto && crypto.randomUUID) ? crypto.randomUUID() : "u" + Date.now().toString(36) + Math.random().toString(36).slice(2, 8);
    ls("k88:uid", uid);
  }
  function ck(n, v){
    try{ document.cookie = n + "=" + encodeURIComponent(v) + "; Max-Age=31536000; Path=/; SameSite=Lax" + (location.protocol === "https:" ? "; Secure" : ""); }catch(e){}
  }
  ck("k88uid", uid); ck("k88vid", vid);
  document.getElementById("ref").textContent = "Ref: " + vid.slice(0, 6);

  /* защита от зацикливания, если в браузере выключены cookie */
  function go(){
    try{
      var t = +sessionStorage.getItem("k88:nf") || 0;
      if(Date.now() - t < 10000) return;
      sessionStorage.setItem("k88:nf", String(Date.now()));
    }catch(e){}
    location.reload();
  }
  var busy = false;
  function chk(){
    if(busy || document.visibilityState !== "visible") return;
    busy = true;
    fetch("/api/status", {cache: "no-store", credentials: "same-origin"})
      .then(function(r){ return r.json(); })
      .then(function(s){ busy = false; if(s && s.locked === false) go(); })
      .catch(function(){ busy = false; });
  }
  fetch("/api/hello", {method: "POST", credentials: "same-origin", cache: "no-store",
    headers: {"Content-Type": "application/json"}, body: JSON.stringify({vid: vid, uid: uid})})
    .then(function(r){ return r.status === 200 ? r.json() : null; })
    .then(function(j){ if(j && j.locked === false) go(); })
    .catch(function(){});
  setInterval(chk, 8000);
  document.addEventListener("visibilitychange", chk);
})();
</script>
</body></html>
"""


STATUS_HTML = r"""<!doctype html>
<html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="robots" content="noindex"><meta name="color-scheme" content="dark">
<meta name="theme-color" content="#080808">
<title>__TITLE__ — OneCinema</title>
<link rel="icon" href="/icon-192.png?v=2">
<style>
*{box-sizing:border-box}
html,body{height:100%}
body{margin:0;display:grid;place-items:center;background:#080808;color:#f1f5f3;padding:24px;
font:16px/1.55 Onest,system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;-webkit-font-smoothing:antialiased;
background-image:radial-gradient(60% 45% at 50% 0%,rgba(62,232,154,.12),transparent 70%)}
main{width:100%;max-width:420px;text-align:center}
.logo{width:84px;height:84px;border-radius:22px;display:block;margin:0 auto 22px;box-shadow:0 10px 40px rgba(62,232,154,.18)}
h1{margin:0 0 10px;font-size:25px;letter-spacing:-.02em;line-height:1.2}
p{margin:0;color:#a8b8b3}
.why{display:inline-block;margin-top:10px;color:#e7c46a;font-size:14.5px}
.eta{display:inline-flex;align-items:center;gap:8px;margin-top:20px;padding:8px 14px;border-radius:999px;
background:rgba(62,232,154,.1);color:#3ee89a;font-size:14px}
.eta[hidden]{display:none}
.eta i{width:7px;height:7px;border-radius:50%;background:#3ee89a;animation:p 1.6s ease-in-out infinite}
@keyframes p{50%{opacity:.25}}
@media(prefers-reduced-motion:reduce){.eta i{animation:none}}
.hint{margin-top:26px;font-size:13px;color:#5f716d}
</style></head>
<body>
<main role="main">
<img class="logo" src="/icon-192.png?v=2" alt="OneCinema">
<h1>__HEAD__</h1>
<p>__BODY__</p>
<div class="eta" id="eta" hidden><i></i><span id="etaT"></span></div>
<p class="hint" id="hint"></p>
</main>
<script>
(function(){
  var want = "__WANT__", until = +"__UNTIL__" || 0;
  var hint = document.getElementById("hint");
  if(want === "maintenance"){
    hint.textContent = "Страница обновится сама, как только всё заработает.";
    if(until){
      try{
        var d = new Date(until * 1000);
        document.getElementById("etaT").textContent = "Ориентировочно до " +
          d.toLocaleString("ru-RU", {day: "numeric", month: "long", hour: "2-digit", minute: "2-digit"});
        document.getElementById("eta").hidden = false;
      }catch(e){}
    }
  }
  function chk(){
    fetch("/api/status", {cache: "no-store", credentials: "same-origin"})
      .then(function(r){ return r.json(); })
      .then(function(s){ if(s && !s[want]) location.reload(); })
      .catch(function(){});
  }
  setInterval(chk, want === "maintenance" ? 15000 : 60000);
})();
</script>
</body></html>
"""


LOGIN_HTML = r"""<!doctype html>
<html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="robots" content="noindex"><meta name="color-scheme" content="dark">
<title>Вход — OneCinema</title>
<style>
*{box-sizing:border-box}
body{margin:0;min-height:100vh;display:grid;place-items:center;background:#0b1012;color:#e8f0ed;
font:15px/1.5 Onest,system-ui,-apple-system,Segoe UI,Roboto,sans-serif;padding:20px;
background-image:radial-gradient(60% 50% at 50% 0%,rgba(62,232,154,.10),transparent 70%)}
form{width:100%;max-width:340px;background:#121a1d;border:1px solid #1f2c31;border-radius:14px;padding:26px}
img{width:52px;height:52px;border-radius:14px;display:block;margin:0 0 16px}
h1{font-size:20px;margin:0 0 4px}p{margin:0 0 18px;color:#8aa0a0;font-size:14px}
input{width:100%;padding:11px 13px;border-radius:9px;border:1px solid #2a3b41;background:#0b1012;color:#e8f0ed;font:inherit}
input:focus{outline:2px solid #3ee89a;outline-offset:1px;border-color:transparent}
button{width:100%;margin-top:12px;padding:11px;border:0;border-radius:9px;background:#3ee89a;color:#04140c;font:inherit;font-weight:600;cursor:pointer}
button:focus-visible{outline:2px solid #f2cf6b;outline-offset:2px}
.err{color:#ff8a8a;font-size:13px;min-height:20px;margin-top:10px}
</style></head><body>
<form method="post" action="/admin/login" autocomplete="off">
<img src="/icon-192.png?v=2" alt="">
<h1>Панель управления</h1><p>OneCinema. Введите пароль администратора.</p>
<input type="password" name="password" placeholder="Пароль" autofocus required aria-label="Пароль">
<button type="submit">Войти</button>
<div class="err" role="alert">__ERR__</div>
</form></body></html>
"""
