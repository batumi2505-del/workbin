import os
from dotenv import load_dotenv

load_dotenv()

import csv
import aiohttp
import asyncio
import zipfile
import logging
import hashlib
import base64
import asyncpg
from datetime import datetime
from zoneinfo import ZoneInfo

from telegram import (
    Update,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)
from aiohttp import web


# =========================
# Константы/настройки
# =========================
TZ = ZoneInfo("Asia/Tbilisi")

def parse_admin_ids() -> set[int]:
    # Новый формат: ADMIN_IDS="111,222,333"
    raw = (os.getenv("ADMIN_IDS", "") or "").strip()
    if raw:
        parts = [p.strip() for p in raw.replace(";", ",").split(",")]
        ids = {int(p) for p in parts if p.isdigit() and int(p) != 0}
        return ids

    # Backward-compatible: если оставил старый ADMIN_ID
    one = (os.getenv("ADMIN_ID", "0") or "0").strip()
    return {int(one)} if one.isdigit() and int(one) != 0 else set()

ADMIN_IDS = parse_admin_ids()

CARD_HASH_SALT = os.getenv("CARD_HASH_SALT", "")

DATABASE_URL = os.getenv("DATABASE_URL", "")  # Supabase Pooler Session URL
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")    # https://<service>.onrender.com/telegram
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")  # любая строка (желательно 32+ символа)

# =========================
# Поддержка / FAQ / Политика / Соцсети
# =========================
SUPPORT_USERNAME = "@cashoutta1"
NEWS_CHANNEL_URL = "https://t.me/bincheker_news"
PRIVACY_URL = "https://telegra.ph/Politika-konfidencialnosti--card-bin-checkerbot-01-13"
FAQ_URL = "https://telegra.ph/FAQ--WorkBin-Bot-01-13"

SUPPORT_TEXT = (
    "📚 <b>Помощь</b>\n\n"
    f"✉️ <b>Контакты</b>:\n{SUPPORT_USERNAME} | Cотрудничество\n\n"
    "💎 <b>Социальные сети</b>:\n"
    "BIN Чекер | Новости и Обновления\n"
    f"{NEWS_CHANNEL_URL}\n\n"
    "📝 <b>Условия пользования</b>:\n"
    f"{PRIVACY_URL}\n\n"
    "📗 <b>F.A.Q</b>:\n"
    f"{FAQ_URL}"
)

SUPPORT_KB = InlineKeyboardMarkup([
    [InlineKeyboardButton("✉️ Контакты (сотрудничество)", url=f"https://t.me/{SUPPORT_USERNAME.lstrip('@')}")],
    [InlineKeyboardButton("💎 Новости и обновления", url=NEWS_CHANNEL_URL)],
    [InlineKeyboardButton("📗 F.A.Q", url=FAQ_URL)],
    [InlineKeyboardButton("📝 Условия пользования", url=PRIVACY_URL)],
])


# =========================
# Глобальные переменные
# =========================
bin_db = {}

# DB (Postgres)
_db_pool: asyncpg.Pool | None = None
_db_lock = asyncio.Lock()

# HTTP session (одна на весь процесс)
_http_session: aiohttp.ClientSession | None = None

# Rapira cache
_rapira_cache = {"ts": 0.0, "data": None}
_RAPIRA_CACHE_SECONDS = 30


# =========================
# Utils
# =========================
def today_str() -> str:
    return datetime.now(TZ).strftime("%Y-%m-%d")

def now_iso() -> str:
    return datetime.now(TZ).isoformat(timespec="seconds")

def is_admin_user(update: Update) -> bool:
    u = update.effective_user
    return bool(u and u.id in ADMIN_IDS)

def build_menu(is_admin: bool) -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton("📚 Помощь"), KeyboardButton("📈 Курс Rapira")]
    ]
    if is_admin:
        rows.append([KeyboardButton("📊 Статистика"), KeyboardButton("📣 Рассылка")])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

def fire_and_forget(task: asyncio.Task):
    """Чтобы фоновые задачи не падали молча и не ломали апдейты."""
    def _done(t: asyncio.Task):
        try:
            t.result()
        except Exception as e:
            logger.error(f"Background task error: {e}")
    task.add_done_callback(_done)


# =========================
# HTTP session (re-use)
# =========================
async def get_http_session() -> aiohttp.ClientSession:
    global _http_session
    if _http_session is None or _http_session.closed:
        timeout = aiohttp.ClientTimeout(total=8)
        connector = aiohttp.TCPConnector(limit=50, ttl_dns_cache=300)
        _http_session = aiohttp.ClientSession(timeout=timeout, connector=connector)
    return _http_session


# =========================
# DB helpers (Supabase Postgres)
# =========================
async def _db_connect():
    global _db_pool
    if _db_pool is None:
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL не задан. Добавь в .env и в Render Env.")
        _db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)

async def _db_init_schema():
    await _db_connect()
    async with _db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id     BIGINT PRIMARY KEY,
                username    TEXT,
                first_seen  TEXT,
                last_seen   TEXT,
                starts      INTEGER DEFAULT 0,
                requests    INTEGER DEFAULT 0
            );
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS daily (
                day          TEXT PRIMARY KEY,
                starts       INTEGER DEFAULT 0,
                requests     INTEGER DEFAULT 0,
                unique_users INTEGER DEFAULT 0
            );
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_day (
                user_id BIGINT,
                day     TEXT,
                PRIMARY KEY (user_id, day)
            );
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS pan_hash (
                h   TEXT PRIMARY KEY,
                cnt INTEGER DEFAULT 0
            );
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS pan_flags (
                h          TEXT PRIMARY KEY,
                is_problem INTEGER DEFAULT 0,
                flagged_at TEXT,
                flagged_by BIGINT
            );
        """)

async def db_init():
    async with _db_lock:
        await _db_init_schema()

async def db_execute(query: str, params=()):
    await _db_connect()
    async with _db_pool.acquire() as conn:
        await conn.execute(query, *params)

async def db_fetchone(query: str, params=()):
    await _db_connect()
    async with _db_pool.acquire() as conn:
        return await conn.fetchrow(query, *params)

async def db_fetchall(query: str, params=()):
    await _db_connect()
    async with _db_pool.acquire() as conn:
        return await conn.fetch(query, *params)

async def ensure_daily_row(day: str):
    await db_execute(
        "INSERT INTO daily (day, starts, requests, unique_users) VALUES ($1, 0, 0, 0) "
        "ON CONFLICT (day) DO NOTHING",
        (day,)
    )

async def mark_unique_user_day(user_id: int, day: str) -> bool:
    row = await db_fetchone(
        """
        INSERT INTO user_day (user_id, day)
        VALUES ($1, $2)
        ON CONFLICT DO NOTHING
        RETURNING 1
        """,
        (user_id, day)
    )
    return row is not None

async def upsert_user(user_id: int, username: str | None):
    day = today_str()
    await ensure_daily_row(day)

    new_username = username if username else None
    ts = now_iso()
    await db_execute(
        """
        INSERT INTO users (user_id, username, first_seen, last_seen, starts, requests)
        VALUES ($1, $2, $3, $4, 0, 0)
        ON CONFLICT (user_id) DO UPDATE SET
            username  = COALESCE(EXCLUDED.username, users.username),
            last_seen = EXCLUDED.last_seen
        """,
        (user_id, new_username, ts, ts)
    )

async def inc_start(user_id: int):
    day = today_str()
    await ensure_daily_row(day)

    is_new = await mark_unique_user_day(user_id, day)
    if is_new:
        await db_execute("UPDATE daily SET unique_users = unique_users + 1 WHERE day = $1", (day,))

    await db_execute("UPDATE users SET starts = starts + 1, last_seen = $1 WHERE user_id = $2", (now_iso(), user_id))
    await db_execute("UPDATE daily SET starts = starts + 1 WHERE day = $1", (day,))

async def inc_request(user_id: int):
    day = today_str()
    await ensure_daily_row(day)

    is_new = await mark_unique_user_day(user_id, day)
    if is_new:
        await db_execute("UPDATE daily SET unique_users = unique_users + 1 WHERE day = $1", (day,))

    await db_execute("UPDATE users SET requests = requests + 1, last_seen = $1 WHERE user_id = $2", (now_iso(), user_id))
    await db_execute("UPDATE daily SET requests = requests + 1 WHERE day = $1", (day,))

async def get_stats_text() -> str:
    day = today_str()
    await ensure_daily_row(day)

    total_users = await db_fetchone("SELECT COUNT(*) AS c FROM users")
    total_starts = await db_fetchone("SELECT COALESCE(SUM(starts),0) AS s FROM users")
    total_requests = await db_fetchone("SELECT COALESCE(SUM(requests),0) AS r FROM users")

    today_row = await db_fetchone("SELECT starts, requests, unique_users FROM daily WHERE day = $1", (day,))
    starts_today = int(today_row["starts"])
    requests_today = int(today_row["requests"])
    dau_today = int(today_row["unique_users"])

    return (
        "📊 <b>Статистика</b>\n\n"
        f"👥 <b>Пользователей всего</b>: {int(total_users['c'])}\n"
        f"▶️ <b>/start за всё время</b>: {int(total_starts['s'])}\n"
        f"🧾 <b>Запросов всего</b>: {int(total_requests['r'])}\n\n"
        f"📅 <b>Сегодня ({day})</b>\n"
        f"👤 <b>DAU</b>: {dau_today}\n"
        f"▶️ <b>/start</b>: {starts_today}\n"
        f"🧾 <b>Запросов</b>: {requests_today}"
    )


# =========================
# BIN DB
# =========================
def load_db():
    try:
        csv_path = "full_bins.csv"
        if not os.path.exists(csv_path):
            logger.info("Распаковываю архив full_bins.zip...")
            with zipfile.ZipFile("full_bins.zip", "r") as zip_ref:
                zip_ref.extractall()
                logger.info("Архив успешно распакован")

        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                bin_db[row["BIN"]] = {
                    "Brand": row.get("Brand", "Unknown"),
                    "Issuer": row.get("Issuer", "Unknown"),
                    "CountryName": row.get("CountryName", "Unknown"),
                }
        logger.info(f"Загружено {len(bin_db)} BIN-кодов")
        return True
    except Exception as e:
        logger.error(f"Ошибка загрузки базы: {str(e)}")
        return False

def get_card_scheme(bin_code: str) -> str:
    if not bin_code.isdigit() or len(bin_code) < 6:
        return "Unknown"

    first_digit = int(bin_code[0])
    first_two = int(bin_code[:2])
    first_four = int(bin_code[:4])

    if first_digit == 4:
        return "Visa"
    elif 51 <= first_two <= 55 or 2221 <= first_four <= 2720:
        return "MasterCard"
    elif 2200 <= first_four <= 2204:
        return "МИР"
    return "Unknown"


# =========================
# Rapira rate
# =========================
async def fetch_rapira_usdt_rub() -> dict | None:
    now_ts = asyncio.get_event_loop().time()
    if _rapira_cache["data"] is not None and (now_ts - _rapira_cache["ts"]) < _RAPIRA_CACHE_SECONDS:
        return _rapira_cache["data"]

    url = "https://api.rapira.net/open/market/rates"
    try:
        session = await get_http_session()
        async with session.get(url, headers={"Accept": "application/json"}) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            items = data.get("data", [])
            for item in items:
                if item.get("symbol") == "USDT/RUB":
                    _rapira_cache["ts"] = now_ts
                    _rapira_cache["data"] = item
                    return item
    except Exception as e:
        logger.error(f"Rapira API error: {e}")
    return None


# =========================
# PAN hash counter (safe)
# =========================
def pan_to_hash(pan_digits: str) -> str:
    salt = CARD_HASH_SALT or "default_salt_change_me"
    digest = hashlib.sha256((salt + pan_digits).encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")[:32]

async def inc_pan_hash(h: str) -> int:
    row = await db_fetchone(
        """
        INSERT INTO pan_hash (h, cnt)
        VALUES ($1, 1)
        ON CONFLICT (h) DO UPDATE SET cnt = pan_hash.cnt + 1
        RETURNING cnt
        """,
        (h,)
    )
    return int(row["cnt"])

async def get_pan_flag(h: str) -> bool:
    row = await db_fetchone("SELECT is_problem FROM pan_flags WHERE h = $1", (h,))
    return bool(row and int(row["is_problem"]) == 1)

async def set_pan_flag(h: str, user_id: int, is_problem: bool):
    await db_execute(
        """
        INSERT INTO pan_flags (h, is_problem, flagged_at, flagged_by)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (h) DO UPDATE SET
            is_problem=EXCLUDED.is_problem,
            flagged_at=EXCLUDED.flagged_at,
            flagged_by=EXCLUDED.flagged_by
        """,
        (h, 1 if is_problem else 0, now_iso(), user_id)
    )


# =========================
# Background tracking (ускорение ответа)
# =========================
async def track_start_bg(user_id: int, username: str | None):
    await upsert_user(user_id, username)
    await inc_start(user_id)

async def track_request_bg(user_id: int, username: str | None):
    await upsert_user(user_id, username)
    await inc_request(user_id)


# =========================
# Handlers
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        return

    fire_and_forget(asyncio.create_task(track_start_bg(user.id, user.username)))

    await update.message.reply_text(
        "🔍 Привет!\n\n"
        "Отправь первые 6 цифр номера карты (BIN) — я определю:\n"
        "🏦 банк • 💳 платёжную систему • 🌍 страну\n\n"
        "🧪 Пример: 424242 → Visa\n\n"
        "🔁 Хочешь узнать, проверяли ли эту карту раньше?\n"
        "Отправь полный номер карты.\n\n"
        "🔐 Для безопасности мы не храним номер карты: "
        "создаём для него уникальный хэш и ищем совпадения.",
        reply_markup=build_menu(is_admin_user(update))
    )

async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin_user(update):
        return
    text = await get_stats_text()
    await update.message.reply_text(text, parse_mode="HTML", reply_markup=build_menu(True))

async def broadcast_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin_user(update):
        return
    context.user_data["awaiting_broadcast"] = True
    await update.message.reply_text(
        "📣 Пришли сообщение (текст/фото/видео), которое нужно разослать всем пользователям.\n"
        "Отмена: /cancel",
        reply_markup=build_menu(True)
    )

async def cancel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin_user(update):
        return
    context.user_data.pop("awaiting_broadcast", None)
    await update.message.reply_text("✅ Отменено.", reply_markup=build_menu(True))

async def do_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = await db_fetchall("SELECT user_id FROM users")
    user_ids = [int(r["user_id"]) for r in rows]

    sent = 0
    failed = 0

    admin_chat_id = update.effective_chat.id
    src_chat_id = update.effective_chat.id
    src_msg_id = update.message.message_id

    await update.message.reply_text(f"🚀 Старт рассылки. Получателей: {len(user_ids)}")

    for uid in user_ids:
        try:
            await context.bot.copy_message(
                chat_id=uid,
                from_chat_id=src_chat_id,
                message_id=src_msg_id
            )
            sent += 1
        except Exception:
            failed += 1

        if (sent + failed) % 25 == 0:
            await asyncio.sleep(1)

    await context.bot.send_message(
        chat_id=admin_chat_id,
        text=f"✅ Рассылка завершена.\nОтправлено: {sent}\nОшибок: {failed}"
    )

async def flag_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data or ""
    user_id = query.from_user.id

    if data.startswith("flag:"):
        h = data.split(":", 1)[1]
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Да", callback_data=f"flag_yes:{h}"),
            InlineKeyboardButton("❌ Нет", callback_data=f"flag_no:{h}"),
        ]])
        await query.edit_message_reply_markup(reply_markup=kb)
        return

    if data.startswith("flag_yes:"):
        h = data.split(":", 1)[1]
        await set_pan_flag(h, user_id=user_id, is_problem=True)
        await query.edit_message_reply_markup(reply_markup=None)
        await query.message.reply_text("✅ Готово. Карта отмечена как проблемная.")
        return

    if data.startswith("flag_no:"):
        await query.edit_message_reply_markup(reply_markup=None)
        await query.message.reply_text("Ок, не отмечаю.")
        return


async def check_card(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        return

    text_raw = (update.message.text or "").strip()

    # Admin flow: broadcast
    if is_admin_user(update) and context.user_data.get("awaiting_broadcast"):
        context.user_data["awaiting_broadcast"] = False
        await do_broadcast(update, context)
        return

    # Menu buttons
    if text_raw == "📚 Помощь":
        await update.message.reply_text(
            SUPPORT_TEXT,
            parse_mode="HTML",
            reply_markup=SUPPORT_KB
        )
        return

    if text_raw == "📈 Курс Rapira":
        rate = await fetch_rapira_usdt_rub()
        if not rate:
            await update.message.reply_text("❌ Не удалось получить курс Rapira сейчас. Попробуй позже.")
            return

        bid_ = rate.get("bidPrice")
        ask_ = rate.get("askPrice")
        close_ = rate.get("close")

        await update.message.reply_text(
            "📈 <b>Rapira USDT/RUB</b>\n"
            f"🟢 <b>Покупка</b>: {bid_}\n"
            f"🔴 <b>Продажа</b>: {ask_}\n"
            f"🔸 <b>Последняя цена</b>: {close_}\n\n"
            "Источник: Rapira Market Rates API",
            parse_mode="HTML",
            reply_markup=build_menu(is_admin_user(update))
        )
        return

    if text_raw == "📊 Статистика" and is_admin_user(update):
        await stats_cmd(update, context)
        return

    if text_raw == "📣 Рассылка" and is_admin_user(update):
        await broadcast_cmd(update, context)
        return

    # Card check
    digits = "".join(ch for ch in text_raw if ch.isdigit())

    if len(digits) < 6:
        await update.message.reply_text(
            "❌ Нужно 6 цифр BIN. Пример: <code>424242</code>",
            parse_mode="HTML",
            reply_markup=build_menu(is_admin_user(update))
        )
        return

    fire_and_forget(asyncio.create_task(track_request_bg(user.id, user.username)))

    is_full_pan = len(digits) >= 12

    bin_code = digits[:6]
    brand = get_card_scheme(bin_code)
    issuer = "Unknown"
    country = "Unknown"

    if bin_code in bin_db:
        data = bin_db[bin_code]
        issuer = data.get("Issuer", issuer)
        country = data.get("CountryName", country)
    else:
        try:
            url = f"https://lookup.binlist.net/{bin_code}"
            headers = {"Accept-Version": "3"}
            session = await get_http_session()
            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    issuer = data.get("bank", {}).get("name", issuer)
                    country = data.get("country", {}).get("name", country)
        except Exception as e:
            logger.error(f"BINLIST API error: {str(e)}")

    extra = ""
    reply_markup_inline = None

    if is_full_pan:
        h = pan_to_hash(digits)

        cnt, is_problem = await asyncio.gather(
            inc_pan_hash(h),
            get_pan_flag(h)
        )

        problem_line = "\n⚠️ <b>Метка</b>: карта отмечена как проблемная" if is_problem else ""
        extra = f"\n\n🔁 <b>Запросов по этому номеру</b>: {cnt}{problem_line}"

        reply_markup_inline = InlineKeyboardMarkup([[
            InlineKeyboardButton("🚩 Отметить карту как проблемную", callback_data=f"flag:{h}")
        ]])

    await update.message.reply_text(
        f"💳 <b>Платёжная система</b>: {brand}\n"
        f"🏦 <b>Банк</b>: {issuer}\n"
        f"🌍 <b>Страна</b>: {country}"
        f"{extra}",
        parse_mode="HTML",
        reply_markup=reply_markup_inline
    )


# =========================
# HTTP server (Render webhook + healthcheck)
# =========================
def build_web_app(application: Application) -> web.Application:
    app = web.Application()

    async def health_check(request):
        return web.Response(text="OK", status=200)

    async def telegram_webhook(request: web.Request):
        if WEBHOOK_SECRET:
            secret_hdr = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
            if secret_hdr != WEBHOOK_SECRET:
                return web.Response(text="Forbidden", status=403)

        data = await request.json()
        upd = Update.de_json(data, application.bot)
        await application.update_queue.put(upd)
        return web.Response(text="OK", status=200)

    app.router.add_get("/", health_check)
    app.router.add_get("/health", health_check)
    app.router.add_post("/telegram", telegram_webhook)
    return app

async def run_http_server(port: int, application: Application):
    app = build_web_app(application)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"HTTP-сервер запущен на порту {port}")
    return runner


# =========================
# Run bot (Webhook mode for Render)
# =========================
async def run_bot():
    if not load_db():
        logger.critical("Не удалось загрузить базу BIN-кодов!")
        return

    token = os.getenv("TELEGRAM_TOKEN")
    if not token:
        logger.error("TELEGRAM_TOKEN не найден!")
        return

    await db_init()

    application = Application.builder() \
        .token(token) \
        .concurrent_updates(False) \
        .build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("stats", stats_cmd))
    application.add_handler(CommandHandler("broadcast", broadcast_cmd))
    application.add_handler(CommandHandler("cancel", cancel_cmd))

    application.add_handler(CallbackQueryHandler(flag_callback, pattern=r"^(flag:|flag_yes:|flag_no:)"))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, check_card))

    port = int(os.environ.get("PORT", 8080))

    logger.info("Бот запускается...")
    await application.initialize()
    await application.start()

    http_runner = await run_http_server(port, application)

    if not WEBHOOK_URL:
        logger.error("WEBHOOK_URL не задан! Добавь WEBHOOK_URL=https://<service>.onrender.com/telegram")
    else:
        await application.bot.set_webhook(
            url=WEBHOOK_URL,
            secret_token=WEBHOOK_SECRET or None,
            drop_pending_updates=True
        )
        logger.info(f"Webhook установлен: {WEBHOOK_URL}")

    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        logger.info("Получен сигнал остановки")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
    finally:
        logger.info("Остановка бота...")
        try:
            if WEBHOOK_URL:
                await application.bot.delete_webhook(drop_pending_updates=False)
        except Exception:
            pass

        try:
            global _http_session
            if _http_session and not _http_session.closed:
                await _http_session.close()
        except Exception:
            pass

        try:
            global _db_pool
            if _db_pool is not None:
                await _db_pool.close()
        except Exception:
            pass

        await application.stop()
        await application.shutdown()
        await http_runner.cleanup()
        logger.info("Бот успешно остановлен")


if __name__ == "__main__":
    try:
        asyncio.run(run_bot())
    except KeyboardInterrupt:
        logger.info("Бот остановлен по запросу пользователя")
    except Exception as e:
        logger.error(f"Фатальная ошибка: {str(e)}")

