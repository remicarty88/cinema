"""
Телеграм-бот OneCinema: поиск и просмотр фильмов и сериалов.

Переиспользует логику server.py (поиск, карточка фильма), отдельный сервер не нужен.

Что умеет:
    * поиск по названию (hdrezka, при пустой выдаче - kinogo);
    * плеер прямо в Telegram (Mini App) - кнопка "Смотреть";
    * "Продолжить" с той серии, где остановились, + история (/history);
    * избранное (/fav), кнопка ⭐ на карточке;
    * новинки, жанры, случайный фильм (/new, /genres, /random);
    * выбор сезона/серии, кнопки "предыдущая / следующая серия";
    * inline-режим: @бот название в любом чате -> карточка со ссылкой на плеер
      (включается в @BotFather: /setinline);
    * состояние (история, избранное, выдача) переживает перезапуск бота.

Переменные окружения:
    BOT_TOKEN     - токен бота
    WEB_BASE_URL  - публичный адрес сайта/плеера (server.py), https://...
                    (на Railway берётся из RAILWAY_PUBLIC_DOMAIN автоматически)
    DATA_DIR      - где хранить состояние бота (по умолчанию текущая папка;
                    на Railway подключите Volume, иначе файл стирается при деплое)
"""
import asyncio
import hashlib
import html
import logging
import os
import random
import time
from urllib.parse import urlencode

from telegram import (
    BotCommand,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InlineQueryResultArticle,
    InputTextMessageContent,
    KeyboardButton,
    MenuButtonWebApp,
    ReplyKeyboardMarkup,
    Update,
    WebAppInfo,
)
from telegram.constants import ChatAction, ParseMode
from telegram.error import Conflict, TelegramError
from telegram.ext import (
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    InlineQueryHandler,
    MessageHandler,
    PicklePersistence,
    filters,
)

import server as S

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s: %(message)s", level=logging.INFO
)
log = logging.getLogger("kino88-bot")

BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
WEB_BASE_URL = os.environ.get("WEB_BASE_URL", "").strip().rstrip("/")
if not WEB_BASE_URL:
    _d = os.environ.get("RAILWAY_PUBLIC_DOMAIN", "").strip().rstrip("/")
    if _d:
        WEB_BASE_URL = "https://" + _d
DATA_DIR = os.environ.get("DATA_DIR", ".").strip() or "."

PAGE_SIZE = 6
CAPTION_LIMIT = 650
HISTORY_MAX = 30
URLS_MAX = 400
THROTTLE_SEC = 1.0

# подписи кнопок нижней клавиатуры
B_NEW, B_GENRES, B_FAV = "🔥 Новинки", "🎭 Жанры", "⭐ Избранное"
B_HIST, B_RANDOM = "🕘 История", "🎲 Случайный"

HELP_TEXT = (
    "🎬 <b>OneCinema</b>\n\n"
    "Напиши название фильма или сериала — найду и открою плеер.\n"
    "Полностью бесплатно.\n\n"
    "• <b>Смотреть</b> — плеер открывается прямо в Telegram;\n"
    "• <b>Продолжить</b> — с серии, где остановился;\n"
    "• ⭐ — сохранить в избранное;\n"
    "• кнопки внизу: новинки, жанры, история, случайный фильм.\n\n"
    "В любом чате можно написать <code>@бот название</code> и отправить "
    "друзьям карточку со ссылкой."
)


# ------------------------------------------------------------------ утилиты
def esc(t) -> str:
    return html.escape(str(t or ""))


def fid_of(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()[:10]


def btn(text, **kw):
    return InlineKeyboardButton(text, **kw)


def player_url(film_url, season=None, episode=None):
    """Ссылка на веб-плеер с глубокой привязкой к фильму/серии."""
    if not WEB_BASE_URL or not film_url:
        return None
    params = {"u": film_url}
    if season is not None:
        params["s"] = season
    if episode is not None:
        params["e"] = episode
    return WEB_BASE_URL + "/?" + urlencode(params)


def play_btn(text, url, private):
    """В личке открываем Mini App внутри Telegram, в группах - обычная ссылка."""
    if not url:
        return None
    if private:
        return btn(text, web_app=WebAppInfo(url=url))
    return btn(text, url=url)


def is_private(message) -> bool:
    return bool(message and message.chat and message.chat.type == "private")


_last_call = {}


def throttled(uid) -> bool:
    now = time.monotonic()
    if now - _last_call.get(uid, 0) < THROTTLE_SEC:
        return True
    _last_call[uid] = now
    if len(_last_call) > 5000:
        _last_call.clear()
    return False


def remember(ctx, items):
    """Запоминает fid -> карточка, чтобы кнопки работали и после перезапуска."""
    urls = ctx.user_data.setdefault("urls", {})
    for it in items:
        u = it.get("url")
        if not u:
            continue
        urls[fid_of(u)] = {"u": u, "t": it.get("title") or it.get("name") or "—",
                           "y": it.get("year") or it.get("releaseYear")}
    while len(urls) > URLS_MAX:
        urls.pop(next(iter(urls)))


def url_by_fid(ctx, fid):
    ent = (ctx.user_data.get("urls") or {}).get(fid)
    return ent["u"] if ent else None


def main_keyboard():
    rows = []
    if WEB_BASE_URL:
        rows.append([KeyboardButton("🎬 OneCinema", web_app=WebAppInfo(url=WEB_BASE_URL))])
    rows.append([B_NEW, B_GENRES])
    rows.append([B_FAV, B_HIST])
    rows.append([B_RANDOM])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


async def edit_panel(query, text, kb):
    """Заменяет содержимое текущей карточки, будь то фото или текст."""
    try:
        await query.edit_message_caption(
            caption=text, parse_mode=ParseMode.HTML, reply_markup=kb
        )
        return
    except Exception:
        pass
    try:
        await query.edit_message_text(
            text=text, parse_mode=ParseMode.HTML, reply_markup=kb,
            disable_web_page_preview=True,
        )
    except Exception:
        pass


# ------------------------------------------------------------------ данные
async def get_film(url):
    """Карточка фильма с общим кэшем server.py."""
    key = f"film:{url}"
    cached = S.cache_get(key, 600)
    if cached is not None:
        return cached
    fn = S.kinogo_film if S.detect_source(url) == "kinogo" else S.hdrezka_film
    film = await asyncio.to_thread(fn, url)
    S.cache_set(key, film)
    return film


def _search_sync(q):
    try:
        res = S.hdrezka_search(q)
    except Exception as e:
        log.warning("hdrezka_search: %s", e)
        res = []
    if not res:
        res = S.kinogo_search(q)
    return res


async def search_items(q):
    key = f"search:hdrezka:{q}:False"
    cached = S.cache_get(key, 300)
    if cached is None:
        cached = await asyncio.to_thread(_search_sync, q)
        S.cache_set(key, cached)
    return [r for r in cached if r.get("url")]


async def home_row(idx):
    """Подборка из главной сайта (новинки/жанры) с кэшем на 30 минут."""
    key = f"botrow:{idx}"
    cached = S.cache_get(key, 1800)
    if cached is None:
        _, cat, genre, sort = S.HOME_ROWS[idx]
        cached = await asyncio.to_thread(S._home_row, cat, genre, sort)
        S.cache_set(key, cached)
    return [r for r in cached if r.get("url")]


# ------------------------------------------------------------------ история / избранное
def hist_get(ctx, fid):
    for h in ctx.user_data.get("history", []):
        if h["fid"] == fid:
            return h
    return None


def hist_add(ctx, film_url, title, season=None, episode=None):
    fid = fid_of(film_url)
    hist = ctx.user_data.setdefault("history", [])
    old = hist_get(ctx, fid)
    if old:
        hist.remove(old)
        if season is None:
            season, episode = old.get("s"), old.get("e")
    hist.insert(0, {"fid": fid, "u": film_url, "t": title, "s": season,
                    "e": episode, "ts": int(time.time())})
    del hist[HISTORY_MAX:]


def is_fav(ctx, fid):
    return fid in (ctx.user_data.get("favs") or {})


def toggle_fav(ctx, film):
    favs = ctx.user_data.setdefault("favs", {})
    fid = fid_of(film["url"])
    if fid in favs:
        favs.pop(fid)
        return False
    favs[fid] = {"fid": fid, "u": film["url"], "t": film.get("name") or "—"}
    return True


# ------------------------------------------------------------------ списки
def results_kb(items, page):
    start = page * PAGE_SIZE
    chunk = items[start:start + PAGE_SIZE]
    rows = []
    for i, r in enumerate(chunk):
        title = r.get("t") or "—"
        title = title if len(title) <= 45 else title[:42] + "…"
        rows.append([btn(f"{start + i + 1}. {title}", callback_data=f"r:{r['fid']}")])
    pages = (len(items) - 1) // PAGE_SIZE + 1
    if pages > 1:
        nav = []
        if page > 0:
            nav.append(btn("◀️", callback_data=f"pg:{page - 1}"))
        nav.append(btn(f"{page + 1}/{pages}", callback_data="noop"))
        if start + PAGE_SIZE < len(items):
            nav.append(btn("▶️", callback_data=f"pg:{page + 1}"))
        rows.append(nav)
    return InlineKeyboardMarkup(rows)


def pack_items(raw):
    """Сохраняемый вид выдачи: короткие записи без лишних полей."""
    out = []
    for r in raw:
        u = r.get("url") or r.get("u")
        if not u:
            continue
        out.append({"fid": fid_of(u), "u": u, "t": r.get("title") or r.get("t") or "—"})
    return out


async def show_list(message, ctx, raw, label, page=0, edit=False):
    items = pack_items(raw)
    ctx.user_data["results"] = items
    ctx.user_data["label"] = label
    remember(ctx, [{"url": i["u"], "title": i["t"]} for i in items])
    await render_list(message, ctx, page, edit)


async def render_list(message, ctx, page=0, edit=False):
    items = ctx.user_data.get("results") or []
    label = ctx.user_data.get("label", "")
    if not items:
        await message.reply_text("Ничего не найдено. Попробуй другое название.")
        return
    text = f"{label}\nНашлось: {len(items)}\n\nВыбери, что смотреть:"
    kb = results_kb(items, page)
    if edit:
        try:
            await message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
            return
        except Exception:
            pass
    await message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)


# ------------------------------------------------------------------ карточка
def film_caption(film):
    name = film.get("name") or "—"
    bits = []
    if film.get("releaseYear"):
        bits.append(str(film["releaseYear"]))
    bits.append("сериал" if film.get("isSeries") else "фильм")
    r = (film.get("rating") or {}).get("value")
    if r is not None:
        bits.append(f"★ {r}")
    head = f"<b>{esc(name)}</b>\n<i>{esc(' • '.join(bits))}</i>"
    desc = (film.get("description") or "").strip()
    if desc:
        if len(desc) > CAPTION_LIMIT:
            desc = desc[:CAPTION_LIMIT].rsplit(" ", 1)[0] + "…"
        head += "\n\n" + esc(desc)
    return head


def film_kb(film, ctx, private):
    url = film["url"]
    fid = fid_of(url)
    rows = []
    h = hist_get(ctx, fid)
    if film.get("isSeries") and h and h.get("s") is not None and h.get("e") is not None:
        b = play_btn(f"▶️ Продолжить: {h['s']} сезон, {h['e']} серия",
                     player_url(url, h["s"], h["e"]), private)
        if b:
            rows.append([b])
    b = play_btn("▶️ Смотреть", player_url(url), private)
    if b:
        rows.append([b])
    if film.get("isSeries") and film.get("seasons"):
        rows.append([btn("📺 Выбрать серию", callback_data=f"ss:{fid}")])
    fav = "★ В избранном" if is_fav(ctx, fid) else "☆ В избранное"
    rows.append([
        btn(fav, callback_data=f"fav:{fid}"),
        btn("📤 Поделиться", switch_inline_query=(film.get("name") or "")[:60]),
    ])
    pu = player_url(url)
    if pu:
        rows.append([btn("🌐 В браузере", url=pu)])
    return InlineKeyboardMarkup(rows)


async def show_film(message, ctx, url):
    await message.chat.send_action(ChatAction.TYPING)
    try:
        film = await get_film(url)
    except Exception as e:
        await message.reply_text(f"⚠️ Не удалось открыть фильм: {type(e).__name__}: {e}")
        return
    film = dict(film)
    film["url"] = film.get("url") or url
    remember(ctx, [{"url": film["url"], "title": film.get("name"),
                    "year": film.get("releaseYear")}])
    hist_add(ctx, film["url"], film.get("name") or "—")
    caption = film_caption(film)
    kb = film_kb(film, ctx, is_private(message))
    thumb = film.get("thumbnail")
    if thumb:
        try:
            await message.reply_photo(photo=thumb, caption=caption,
                                      parse_mode=ParseMode.HTML, reply_markup=kb)
            return
        except Exception:
            pass
    await message.reply_text(caption, parse_mode=ParseMode.HTML, reply_markup=kb)


# ------------------------------------------------------------------ сезоны и серии
def seasons_kb(seasons, fid):
    rows, row = [], []
    for s in seasons:
        label = s.get("season_text") or f"Сезон {s.get('season')}"
        row.append(btn(label, callback_data=f"se:{fid}:{s.get('season')}"))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([btn("⬅️ К фильму", callback_data=f"f:{fid}")])
    return InlineKeyboardMarkup(rows)


def find_season(film, season):
    return next((x for x in film.get("seasons", [])
                 if str(x.get("season")) == str(season)), None)


def episodes_kb(film, fid, season):
    se = find_season(film, season)
    if not se:
        return None
    rows, row = [], []
    for e in se.get("episodes", []):
        n = e.get("episode")
        row.append(btn(str(n), callback_data=f"ep:{fid}:{season}:{n}"))
        if len(row) == 5:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([btn("⬅️ Сезоны", callback_data=f"ss:{fid}")])
    return InlineKeyboardMarkup(rows)


def flat_episodes(film):
    out = []
    for s in film.get("seasons", []):
        for e in s.get("episodes", []):
            out.append((str(s.get("season")), str(e.get("episode"))))
    return out


def episode_kb(film, fid, season, episode, private):
    url = film["url"]
    rows = []
    b = play_btn("▶️ Смотреть серию", player_url(url, season, episode), private)
    if b:
        rows.append([b])
    flat = flat_episodes(film)
    cur = (str(season), str(episode))
    nav = []
    if cur in flat:
        i = flat.index(cur)
        if i > 0:
            ps, pe = flat[i - 1]
            nav.append(btn("⏮ Пред.", callback_data=f"ep:{fid}:{ps}:{pe}"))
        nav.append(btn("📋 Серии", callback_data=f"se:{fid}:{season}"))
        if i < len(flat) - 1:
            ns, ne = flat[i + 1]
            nav.append(btn("След. ⏭", callback_data=f"ep:{fid}:{ns}:{ne}"))
    else:
        nav.append(btn("📋 Серии", callback_data=f"se:{fid}:{season}"))
    rows.append(nav)
    rows.append([btn("⬅️ К фильму", callback_data=f"f:{fid}")])
    return InlineKeyboardMarkup(rows)


# ------------------------------------------------------------------ жанры
def genres_kb():
    rows, row = [], []
    for i, (title, *_rest) in enumerate(S.HOME_ROWS):
        row.append(btn(title, callback_data=f"cat:{i}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(rows)


async def show_category(message, ctx, idx, edit=False):
    title = S.HOME_ROWS[idx][0]
    await message.chat.send_action(ChatAction.TYPING)
    try:
        items = await home_row(idx)
    except Exception as e:
        await message.reply_text(f"⚠️ Не удалось загрузить: {type(e).__name__}: {e}")
        return
    await show_list(message, ctx, items, f"🎭 <b>{esc(title)}</b>", 0, edit)


async def do_random(message, ctx):
    await message.chat.send_action(ChatAction.TYPING)
    order = list(range(len(S.HOME_ROWS)))
    random.shuffle(order)
    for idx in order:
        try:
            items = await home_row(idx)
        except Exception:
            continue
        if items:
            await show_film(message, ctx, random.choice(items)["url"])
            return
    await message.reply_text("⚠️ Не получилось подобрать фильм, попробуй ещё раз.")


async def do_search(message, ctx, query):
    await message.chat.send_action(ChatAction.TYPING)
    try:
        items = await search_items(query)
    except Exception as e:
        await message.reply_text(f"⚠️ Ошибка поиска: {type(e).__name__}: {e}")
        return
    if not items:
        await message.reply_text("Ничего не найдено. Попробуй другое название.")
        return
    await show_list(message, ctx, items, f"🔎 <b>{esc(query)}</b>")


def saved_list(ctx, kind):
    if kind == "fav":
        return list((ctx.user_data.get("favs") or {}).values())
    return list(ctx.user_data.get("history") or [])


async def show_saved(message, ctx, kind):
    items = saved_list(ctx, kind)
    label = "⭐ <b>Избранное</b>" if kind == "fav" else "🕘 <b>История просмотров</b>"
    if not items:
        await message.reply_text(
            "Пока пусто. Нажми ☆ на карточке фильма, чтобы сохранить его."
            if kind == "fav" else "История пока пустая — открой любой фильм."
        )
        return
    await show_list(message, ctx, items, label)


# ------------------------------------------------------------------ хендлеры
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(
        HELP_TEXT, parse_mode=ParseMode.HTML, reply_markup=main_keyboard()
    )


async def cmd_new(update, ctx):
    await show_category(update.effective_message, ctx, 0)


async def cmd_genres(update, ctx):
    await update.effective_message.reply_text(
        "🎭 <b>Выбери жанр или раздел</b>", parse_mode=ParseMode.HTML,
        reply_markup=genres_kb(),
    )


async def cmd_fav(update, ctx):
    await show_saved(update.effective_message, ctx, "fav")


async def cmd_history(update, ctx):
    await show_saved(update.effective_message, ctx, "hist")


async def cmd_random(update, ctx):
    await do_random(update.effective_message, ctx)


async def on_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    text = (msg.text or "").strip()
    if not text or text.startswith("/"):
        return
    if throttled(update.effective_user.id):
        return
    if text == B_NEW:
        return await cmd_new(update, ctx)
    if text == B_GENRES:
        return await cmd_genres(update, ctx)
    if text == B_FAV:
        return await cmd_fav(update, ctx)
    if text == B_HIST:
        return await cmd_history(update, ctx)
    if text == B_RANDOM:
        return await cmd_random(update, ctx)
    low = text.lower()
    if low.startswith("http") and ("kinogo" in low or "hdrezka" in low):
        await show_film(msg, ctx, text)
        return
    await do_search(msg, ctx, text)


async def on_inline(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    iq = update.inline_query
    q = (iq.query or "").strip()
    if len(q) < 2:
        await iq.answer([], cache_time=1)
        return
    try:
        items = await search_items(q)
    except Exception:
        await iq.answer([], cache_time=1)
        return
    results = []
    for it in items[:10]:
        url = it["url"]
        title = it.get("title") or "—"
        pu = player_url(url)
        kb = InlineKeyboardMarkup([[btn("▶️ Смотреть", url=pu)]]) if pu else None
        results.append(InlineQueryResultArticle(
            id=fid_of(url),
            title=title,
            description=str(it.get("year") or "") or "Фильм / сериал",
            thumbnail_url=it.get("image"),
            input_message_content=InputTextMessageContent(
                f"🎬 <b>{esc(title)}</b>", parse_mode=ParseMode.HTML),
            reply_markup=kb,
        ))
    await iq.answer(results, cache_time=60, is_personal=False)


async def on_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    data = q.data or ""
    msg = q.message
    private = is_private(msg)

    if not data.startswith("fav:"):
        await q.answer()

    if data == "noop":
        return

    if data.startswith("pg:"):
        await render_list(msg, ctx, int(data.split(":")[1]), edit=True)
        return

    if data.startswith("cat:"):
        if throttled(q.from_user.id):
            return
        idx = int(data.split(":")[1])
        if 0 <= idx < len(S.HOME_ROWS):
            await show_category(msg, ctx, idx)
        return

    # все остальные колбэки: <действие>:<fid>[:...]
    parts = data.split(":")
    action = parts[0]
    fid = parts[1] if len(parts) > 1 else ""
    url = url_by_fid(ctx, fid)
    if not url:
        await msg.reply_text("Кнопка устарела. Повтори поиск.")
        return

    if action == "r":
        if throttled(q.from_user.id):
            return
        await show_film(msg, ctx, url)
        return

    try:
        film = dict(await get_film(url))
        film["url"] = film.get("url") or url
    except Exception as e:
        await msg.reply_text(f"⚠️ Не удалось загрузить фильм: {type(e).__name__}: {e}")
        return

    if action == "f":
        await edit_panel(q, film_caption(film), film_kb(film, ctx, private))
        return

    if action == "fav":
        added = toggle_fav(ctx, film)
        await q.answer("⭐ Добавлено в избранное" if added else "Убрано из избранного")
        try:
            await q.edit_message_reply_markup(reply_markup=film_kb(film, ctx, private))
        except Exception:
            pass
        return

    if action == "ss":
        if film.get("seasons"):
            await edit_panel(q, "📺 <b>Выбери сезон</b>", seasons_kb(film["seasons"], fid))
        return

    if action == "se":
        season = parts[2]
        kb = episodes_kb(film, fid, season)
        if kb:
            await edit_panel(q, f"📺 <b>{esc(season)} сезон — выбери серию</b>", kb)
        return

    if action == "ep":
        season, episode = parts[2], parts[3]
        hist_add(ctx, url, film.get("name") or "—", season, episode)
        text = (f"📺 <b>{esc(film.get('name'))}</b>\n"
                f"{esc(season)} сезон, серия {esc(episode)}")
        if not WEB_BASE_URL:
            text += "\n\nПлеер недоступен: задай WEB_BASE_URL на сервере."
        await edit_panel(q, text, episode_kb(film, fid, season, episode, private))
        return


async def on_error(update: object, ctx: ContextTypes.DEFAULT_TYPE):
    """Ловит ошибки сети/polling, чтобы не заливать лог трейсбеком."""
    err = ctx.error
    if isinstance(err, Conflict):
        log.warning(
            "409 Conflict: Telegram опрашивают сразу несколько экземпляров бота "
            "(вторая реплика Railway или запущенный локально bot.py). "
            "Оставьте ровно один — бот сам восстановится, когда лишний процесс умрёт."
        )
        return
    if isinstance(err, TelegramError):
        log.warning("Ошибка Telegram API: %s", err)
        return
    log.exception("Ошибка обработки апдейта", exc_info=err)


async def post_init(app):
    """Команды в меню бота и кнопка «OneCinema» рядом с полем ввода."""
    try:
        await app.bot.set_my_commands([
            BotCommand("start", "Меню"),
            BotCommand("new", "Новинки"),
            BotCommand("genres", "Жанры и разделы"),
            BotCommand("random", "Случайный фильм"),
            BotCommand("fav", "Избранное"),
            BotCommand("history", "История просмотров"),
        ])
        if WEB_BASE_URL:
            await app.bot.set_chat_menu_button(
                menu_button=MenuButtonWebApp(
                    text="🎬 OneCinema", web_app=WebAppInfo(url=WEB_BASE_URL))
            )
    except Exception as e:
        log.warning("post_init: %s", e)


def main():
    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN не задан")
    os.makedirs(DATA_DIR, exist_ok=True)
    persistence = PicklePersistence(filepath=os.path.join(DATA_DIR, "bot_state.pkl"))
    app = (ApplicationBuilder().token(BOT_TOKEN)
           .persistence(persistence).post_init(post_init).build())
    app.add_handler(CommandHandler(["start", "help"], cmd_start))
    app.add_handler(CommandHandler("new", cmd_new))
    app.add_handler(CommandHandler("genres", cmd_genres))
    app.add_handler(CommandHandler("fav", cmd_fav))
    app.add_handler(CommandHandler("history", cmd_history))
    app.add_handler(CommandHandler("random", cmd_random))
    app.add_handler(InlineQueryHandler(on_inline))
    app.add_handler(CallbackQueryHandler(on_cb))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    app.add_error_handler(on_error)
    log.info("Бот запущен. Плеер: %s", WEB_BASE_URL or "(WEB_BASE_URL не задан)")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
