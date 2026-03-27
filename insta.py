"""
╔════════════════════════════════════════════════════╗
║   ✨  InstaFetch Bot  v6.0                         ║
║   Instagram + YouTube Shorts Downloader            ║
║   Cookies Support  •  Public Stories               ║
║   Oracle VM Ready  •  No ffmpeg needed             ║
╚════════════════════════════════════════════════════╝

  pip install python-telegram-bot yt-dlp
  python insta.py
"""

# ═══════════════════════════════════════════════════
#  ⚙️  CONFIG
# ═══════════════════════════════════════════════════

BOT_TOKEN        = "YOUR_BOT_TOKEN_HERE"   # ← Your token
ADMIN_USERS      = ["ankushdahiya1"]        # ← No @
COOKIES_FILE     = "cookies.txt"            # ← Same folder
MAX_PARALLEL     = 3
RATE_LIMIT_COUNT = 25
RATE_LIMIT_SECS  = 3600
CONCURRENT_FRAGS = 4
WELCOME_DELETE_S = 60

# ═══════════════════════════════════════════════════

import os, re, time, json, asyncio, logging, tempfile, threading
from collections import deque
from datetime import datetime
from typing import Optional, Dict, List

import yt_dlp
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ChatAction, ParseMode
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, filters, ContextTypes,
)

# ── Logging ─────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    level=logging.INFO,
)
for _lib in ("httpx", "telegram", "hpack"):
    logging.getLogger(_lib).setLevel(logging.WARNING)
log = logging.getLogger("InstaFetch")

# ═══════════════════════════════════════════════════
#  COOKIES HELPER
# ═══════════════════════════════════════════════════

def cookies_opts() -> dict:
    """Returns cookiefile opt only if file exists."""
    if os.path.isfile(COOKIES_FILE):
        return {"cookiefile": COOKIES_FILE}
    return {}

# ═══════════════════════════════════════════════════
#  DATABASE
# ═══════════════════════════════════════════════════

DB_FILE = "instabot_data.json"
DB_LOCK = threading.Lock()

def _blank():
    return {"users": {}, "total_dl": 0, "total_bytes": 0}

def _load():
    if os.path.isfile(DB_FILE):
        try:
            with open(DB_FILE) as f:
                d = json.load(f)
                for k, v in _blank().items():
                    d.setdefault(k, v)
                return d
        except Exception:
            pass
    return _blank()

def _save(db):
    try:
        with open(DB_FILE, "w") as f:
            json.dump(db, f, indent=2)
    except Exception as e:
        log.error(f"DB save: {e}")

def db_register(uid: int, name: str, uname: str):
    with DB_LOCK:
        db = _load(); s = str(uid)
        if s not in db["users"]:
            db["users"][s] = {
                "name": name, "username": uname,
                "joined": datetime.now().isoformat(),
                "downloads": 0, "bytes": 0,
            }
        else:
            db["users"][s].update({"name": name, "username": uname})
        _save(db)

def db_record(uid: int, size: int):
    with DB_LOCK:
        db = _load(); s = str(uid)
        db["total_dl"] += 1
        db["total_bytes"] += size
        if s in db["users"]:
            db["users"][s]["downloads"] += 1
            db["users"][s]["bytes"] += size
        _save(db)

def db_stats() -> dict:
    with DB_LOCK:
        return _load()

# ═══════════════════════════════════════════════════
#  RATE LIMITER
# ═══════════════════════════════════════════════════

_rate: Dict[int, deque] = {}
_rl   = threading.Lock()

def rate_check(uid: int):
    now = time.time()
    with _rl:
        q = _rate.setdefault(uid, deque())
        while q and now - q[0] > RATE_LIMIT_SECS:
            q.popleft()
        used = len(q)
        if used >= RATE_LIMIT_COUNT:
            return False, 0, int(RATE_LIMIT_SECS - (now - q[0]))
        return True, RATE_LIMIT_COUNT - used - 1, 0

def rate_consume(uid: int):
    with _rl:
        _rate.setdefault(uid, deque()).append(time.time())

# ═══════════════════════════════════════════════════
#  URL DETECTION
# ═══════════════════════════════════════════════════

INSTA_RE = re.compile(
    r"https?://(?:www\.)?instagram\.com/"
    r"(?:reel(?:s)?/[\w\-]+|p/[\w\-]+|tv/[\w\-]+|"
    r"stories/[\w.]+/\d+|highlights/\d+)",
    re.IGNORECASE,
)
YT_SHORT_RE = re.compile(
    r"https?://(?:www\.)?(?:youtube\.com/shorts/[\w\-]+|youtu\.be/[\w\-]+)",
    re.IGNORECASE,
)
YT_ANY_RE = re.compile(
    r"https?://(?:www\.)?(?:youtube\.com|youtu\.be)/",
    re.IGNORECASE,
)

def extract_insta(text: str) -> Optional[str]:
    m = INSTA_RE.search(text or "")
    return m.group(0).rstrip(").,>") if m else None

def extract_yt_short(text: str) -> Optional[str]:
    m = YT_SHORT_RE.search(text or "")
    return m.group(0).rstrip(").,>") if m else None

def is_yt_not_short(text: str) -> bool:
    return bool(YT_ANY_RE.search(text or "")) and not bool(YT_SHORT_RE.search(text or ""))

def get_insta_ctype(url: str) -> tuple:
    u = url.lower()
    if "/reel"         in u: return "🎬 Reel",         False
    if "/p/"           in u: return "📸 Post",          False
    if "/tv/"          in u: return "📹 IGTV",          False
    if "/stories/"     in u: return "📖 Story",         False  # now supported via cookies
    if "/highlights/"  in u: return "⭐ Highlight",     False  # now supported via cookies
    return "🎥 Video", False

# ═══════════════════════════════════════════════════
#  ERROR MESSAGES
# ═══════════════════════════════════════════════════

def classify_err(err: str) -> str:
    e = (err or "").lower()
    if any(k in e for k in ["private", "not available", "login required",
                             "not authorized", "401", "login", "checkpoint",
                             "rate-limit reached", "rate_limit"]): return "private"
    if any(k in e for k in ["expired", "no longer available"]):    return "expired"
    if any(k in e for k in ["rate", "429", "too many"]):           return "ratelimit"
    if "404" in e or "not found" in e:                             return "notfound"
    if "network" in e or "connect" in e or "timeout" in e:        return "network"
    return "unknown"

def friendly_error(err: str, ctype: str) -> str:
    k = classify_err(err)
    msgs = {
        "private":  (f"🔒 <b>Private Account</b>\n\n"
                     f"This {ctype} belongs to a private account.\n"
                     f"Only followers can access it.\n\n"
                     f"<i>Private content support coming soon! 🚀</i>"),
        "expired":  ("⏰ <b>Story Expired</b>\n\n"
                     "Instagram Stories disappear after 24 hours.\n"
                     "This one is no longer available."),
        "ratelimit":("⏳ <b>Too Many Requests</b>\n\n"
                     "Instagram is temporarily limiting requests.\n"
                     "Please wait a minute and try again."),
        "notfound": ("🔍 <b>Not Found</b>\n\n"
                     "This post may have been deleted\n"
                     "or the link is no longer valid."),
        "network":  ("📡 <b>Connection Issue</b>\n\n"
                     "Couldn't reach the server.\n"
                     "Please check your connection and try again."),
        "unknown":  (f"😔 <b>Something Went Wrong</b>\n\n"
                     f"Couldn't download this {ctype}.\n"
                     f"Please try again in a moment."),
    }
    return msgs.get(k, msgs["unknown"])

# ═══════════════════════════════════════════════════
#  FORMAT HELPERS
# ═══════════════════════════════════════════════════

def pbar(pct: int, w: int = 16) -> str:
    f = int(w * pct / 100)
    return "▓" * f + "░" * (w - f)

def hsize(b: int) -> str:
    if not b or b <= 0: return "~Unknown"
    if b >= 1 << 30: return f"{b/(1<<30):.2f} GB"
    if b >= 1 << 20: return f"{b/(1<<20):.1f} MB"
    return f"{b / 1024:.0f} KB"

def hspeed(s: float) -> str:
    if not s or s <= 0: return "—"
    if s >= 1 << 20: return f"{s/(1<<20):.1f} MB/s"
    return f"{s / 1024:.0f} KB/s"

def htime(s: float) -> str:
    if not s or s <= 0: return "—"
    s = int(s)
    return f"{s // 60}m {s % 60}s" if s >= 60 else f"{s}s"

def nlink(user) -> str:
    fn = (user.first_name or "Friend").replace("<", "&lt;").replace(">", "&gt;")
    return f'<a href="tg://user?id={user.id}">{fn}</a>'

def is_admin(user) -> bool:
    return (user.username or "").lower().lstrip("@") in [a.lower() for a in ADMIN_USERS]

async def sedit(bot, cid, mid, txt):
    try:
        await bot.edit_message_text(
            chat_id=cid, message_id=mid,
            text=txt, parse_mode=ParseMode.HTML,
        )
    except Exception:
        pass

# ═══════════════════════════════════════════════════
#  YT-DLP
# ═══════════════════════════════════════════════════

INSTA_FORMAT = (
    "best[ext=mp4][vcodec!=none][acodec!=none]"
    "/bestvideo[ext=mp4][acodec!=none]"
    "/best[vcodec!=none][acodec!=none]"
    "/best[ext=mp4]/best"
)

YT_FORMATS = {
    "360":  "best[height<=360][ext=mp4]/best[height<=360]",
    "480":  "best[height<=480][ext=mp4]/best[height<=480]",
    "720":  "best[height<=720][ext=mp4]/best[height<=720]",
    "1080": "best[height<=1080][ext=mp4]/best[height<=1080]",
}

def make_opts(outtmpl: str, fmt: str, hook=None) -> dict:
    o = {
        "format":                        fmt,
        "outtmpl":                       outtmpl,
        "quiet":                         True,
        "no_warnings":                   True,
        "noprogress":                    False,
        "concurrent_fragment_downloads": CONCURRENT_FRAGS,
        "retries":                       5,
        "fragment_retries":              5,
        "continuedl":                    True,
        "postprocessors":                [],
    }
    o.update(cookies_opts())   # adds cookiefile if cookies.txt exists
    if hook:
        o["progress_hooks"] = [hook]
    return o

async def fetch_info(url: str, fmt: str = None) -> tuple:
    loop = asyncio.get_event_loop()
    info, err = None, None

    def _r():
        nonlocal info, err
        try:
            opts = {
                "quiet":          True,
                "no_warnings":    True,
                "skip_download":  True,
                "postprocessors": [],
                "format":         fmt or INSTA_FORMAT,
            }
            opts.update(cookies_opts())
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=False)
        except Exception as e:
            err = str(e)

    await loop.run_in_executor(None, _r)
    return info, err

# ═══════════════════════════════════════════════════
#  QUEUE  (accurate real-time positions)
# ═══════════════════════════════════════════════════

_active_ids: List[str] = []
_ql = threading.Lock()

def q_add(tid: str) -> int:
    with _ql:
        _active_ids.append(tid)
        return len(_active_ids)

def q_remove(tid: str):
    with _ql:
        try:
            _active_ids.remove(tid)
        except ValueError:
            pass

def q_size() -> int:
    with _ql:
        return len(_active_ids)

SEMAPHORE = asyncio.Semaphore(MAX_PARALLEL)
PENDING: Dict[str, Dict] = {}

# ═══════════════════════════════════════════════════
#  COMMON REPLIES
# ═══════════════════════════════════════════════════

async def say_rate_limit(target, uid: int, reset_in: int):
    m, s = divmod(reset_in, 60)
    txt = (
        f"⛔ <b>Hourly Limit Reached!</b>\n\n"
        f"You've used all <b>{RATE_LIMIT_COUNT} downloads</b> this hour.\n\n"
        f"⏳ Resets in: <b>{m}m {s}s</b>\n\n"
        f"<i>Limits keep the bot fast for everyone 💙</i>"
    )
    try:
        await target.reply_text(txt, parse_mode=ParseMode.HTML)
    except Exception:
        try:
            await target.message.reply_text(txt, parse_mode=ParseMode.HTML)
        except Exception:
            pass

async def say_invalid(msg):
    await msg.reply_text(
        "❓ <b>Invalid Link</b>\n\n"
        "Send a valid Instagram or YouTube Shorts link.\n\n"
        "<b>Examples:</b>\n"
        "<code>instagram.com/reel/Cxxxxx/</code>\n"
        "<code>youtube.com/shorts/Cxxxxx</code>",
        parse_mode=ParseMode.HTML,
    )

# ═══════════════════════════════════════════════════
#  /start
# ═══════════════════════════════════════════════════

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u    = update.effective_user
    chat = update.effective_chat
    db_register(u.id, u.first_name or "", u.username or "")

    has_cookies = os.path.isfile(COOKIES_FILE)
    story_note  = "✅ Public Stories &amp; Highlights" if has_cookies else "📖 Stories <i>(coming soon)</i>"

    if chat.type == "private":
        await update.message.reply_text(
            f"✨ <b>Hey {nlink(u)}!</b>\n\n"
            f"Welcome to <b>InstaFetch</b> — your fast\n"
            f"Instagram &amp; YouTube Shorts downloader! 🚀\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>📥 Supported Content:</b>\n\n"
            f"  📸 Instagram Reels, Posts, IGTV\n"
            f"  {story_note}\n"
            f"  🎬 YouTube Shorts\n"
            f"    └ 360p / 480p / 720p / 1080p\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🚀 <b>Just send me any link!</b>\n"
            f"No commands needed in private chat.\n\n"
            f"<code>/help</code> — Help &amp; info\n\n"
            f"⚡ <i>{RATE_LIMIT_COUNT} downloads / hour per user</i>",
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
    else:
        await update.message.reply_text(
            f"👋 <b>InstaFetch Bot</b> is active!\n\n"
            f"<b>Group Commands:</b>\n"
            f"  <code>/down {{link}}</code>  — Download with preview\n"
            f"  <code>/insta {{link}}</code>  — Instant download\n\n"
            f"Supports Instagram &amp; YouTube Shorts 🎬",
            parse_mode=ParseMode.HTML,
        )

# ═══════════════════════════════════════════════════
#  /help
# ═══════════════════════════════════════════════════

async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    has_cookies = os.path.isfile(COOKIES_FILE)
    story_line  = "✅ Stories &amp; Highlights (public)" if has_cookies else "⏳ Stories &amp; Highlights (coming soon)"
    await update.message.reply_text(
        "📖 <b>InstaFetch — Help</b>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "💬 <b>Private Chat:</b>\n"
        "Just send any link — no commands needed!\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "👥 <b>Groups:</b>\n"
        "  <code>/down {link}</code>  — Preview + download\n"
        "  <code>/download {link}</code>  — Same\n"
        "  <code>/insta {link}</code>  — Instant download\n"
        "  <code>/instant {link}</code>  — Same\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "✅ <b>Supported:</b>\n"
        "  📸 Instagram Reels, Posts, IGTV\n"
        f"  {story_line}\n"
        "  🎬 YouTube Shorts (4 quality options)\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "⚠️ <b>Limits:</b>\n"
        f"  • {RATE_LIMIT_COUNT} downloads / hour per user\n"
        "  • Max 50 MB per file (Telegram limit)\n"
        "  • Public content only",
        parse_mode=ParseMode.HTML,
    )

# ═══════════════════════════════════════════════════
#  ADMIN PANEL
# ═══════════════════════════════════════════════════

def _admin_panel():
    db  = db_stats()
    top = sorted(db["users"].items(),
                 key=lambda x: x[1].get("downloads", 0), reverse=True)[:5]
    top_txt = ""
    for i, (_, info) in enumerate(top, 1):
        un = ("@" + info["username"]) if info.get("username") else info.get("name", "—")
        top_txt += f"  {i}. {un} — {info.get('downloads', 0)} DLs ({hsize(info.get('bytes', 0))})\n"

    text = (
        f"🛡️ <b>Admin Panel</b>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👥  Users:      <b>{len(db['users'])}</b>\n"
        f"⬇️   Downloads: <b>{db['total_dl']}</b>\n"
        f"📦  Data:       <b>{hsize(db['total_bytes'])}</b>\n"
        f"🔄  Active:     <b>{q_size()}</b> / {MAX_PARALLEL}\n"
        f"🍪  Cookies:    <b>{'✅ Loaded' if os.path.isfile(COOKIES_FILE) else '❌ Not found'}</b>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏆 <b>Top Users</b>\n{top_txt}"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Live Stats",  callback_data="adm_live"),
         InlineKeyboardButton("👥 Users",       callback_data="adm_users")],
        [InlineKeyboardButton("🗑 Reset Stats", callback_data="adm_reset_ask"),
         InlineKeyboardButton("📢 Broadcast",   callback_data="adm_bcast")],
    ])
    return text, kb


async def cmd_admin(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user):
        await update.message.reply_text("🚫 Admins only.")
        return
    text, kb = _admin_panel()
    await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)


async def admin_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    u = q.from_user
    if not is_admin(u):
        await q.answer("🚫 Admins only.", show_alert=True)
        return

    back = [[InlineKeyboardButton("⬅️ Back", callback_data="adm_home")]]
    db   = db_stats()

    if q.data == "adm_home":
        text, kb = _admin_panel()
        try:
            await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
        except Exception:
            pass

    elif q.data == "adm_live":
        await q.edit_message_text(
            f"📊 <b>Live Stats</b>\n\n"
            f"👥 Users:       <b>{len(db['users'])}</b>\n"
            f"⬇️  Downloads:  <b>{db['total_dl']}</b>\n"
            f"📦 Data:        <b>{hsize(db['total_bytes'])}</b>\n"
            f"🔄 Active DLs: <b>{q_size()}</b> / {MAX_PARALLEL}\n"
            f"🍪 Cookies:     <b>{'✅ Active' if os.path.isfile(COOKIES_FILE) else '❌ Missing'}</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(back),
        )

    elif q.data == "adm_users":
        users = list(db["users"].items())
        lines = []
        for _, info in users[-20:]:
            un = ("@" + info["username"]) if info.get("username") else info.get("name", "—")
            lines.append(f"• {un} — {info.get('downloads', 0)} DLs")
        body = "\n".join(lines) or "No users yet."
        await q.edit_message_text(
            f"👥 <b>Users</b> (last 20 of {len(users)})\n\n{body}",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(back),
        )

    elif q.data == "adm_reset_ask":
        await q.edit_message_text(
            "⚠️ <b>Reset all download stats?</b>\n\nThis cannot be undone.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Yes, Reset", callback_data="adm_reset_do")],
                [InlineKeyboardButton("❌ Cancel",     callback_data="adm_home")],
            ]),
        )

    elif q.data == "adm_reset_do":
        with DB_LOCK:
            db2 = _load()
            db2["total_dl"] = 0
            db2["total_bytes"] = 0
            for s in db2["users"]:
                db2["users"][s]["downloads"] = 0
                db2["users"][s]["bytes"] = 0
            _save(db2)
        await q.edit_message_text(
            "✅ <b>Stats Reset Successfully!</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(back),
        )

    elif q.data == "adm_bcast":
        ctx.user_data["bcast"] = True
        await q.edit_message_text(
            "📢 <b>Broadcast Mode</b>\n\n"
            "Send your message now — it will be\n"
            "forwarded to all bot users.\n\n"
            "<i>Type /cancel to abort.</i>",
            parse_mode=ParseMode.HTML,
        )

# ═══════════════════════════════════════════════════
#  GROUP WELCOME / LEAVE
# ═══════════════════════════════════════════════════

async def on_new_member(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg:
        return
    for member in (msg.new_chat_members or []):
        if member.is_bot:
            continue
        try:
            welcome = await msg.chat.send_message(
                f"🎉 <b>Welcome,</b> {nlink(member)}!\n\n"
                f"Great to have you here 🙌\n"
                f"Use <b>InstaFetch Bot</b> to download\n"
                f"Instagram &amp; YouTube Shorts! 🚀\n\n"
                f"<i>This message disappears in {WELCOME_DELETE_S}s ✨</i>",
                parse_mode=ParseMode.HTML,
            )
            asyncio.create_task(
                _delete_later(ctx.bot, msg.chat_id, welcome.message_id, WELCOME_DELETE_S)
            )
        except Exception as e:
            log.warning(f"Welcome error: {e}")
    # Delete system "joined" message instantly
    try:
        await msg.delete()
    except Exception:
        pass


async def on_left_member(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    # Delete system "left" message instantly
    try:
        await update.message.delete()
    except Exception:
        pass


async def _delete_later(bot, chat_id, msg_id, delay: int):
    await asyncio.sleep(delay)
    try:
        await bot.delete_message(chat_id=chat_id, message_id=msg_id)
    except Exception:
        pass

# ═══════════════════════════════════════════════════
#  INSTAGRAM FLOW
# ═══════════════════════════════════════════════════

async def handle_insta(msg, u, url: str, instant: bool):
    ctype, _ = get_insta_ctype(url)

    allowed, _, reset_in = rate_check(u.id)
    if not allowed:
        await say_rate_limit(msg, u.id, reset_in)
        return

    if instant:
        dl_id = f"{u.id}_{int(time.time() * 1000)}"
        PENDING[dl_id] = {"url": url, "ctype": ctype, "fmt": INSTA_FORMAT}
        qs   = q_add(dl_id)
        wait = qs - MAX_PARALLEL
        if wait > 0:
            status = await msg.reply_text(
                f"📥 <b>Queued!</b>\n\n"
                f"📍 Position: <b>#{wait}</b> in line\n"
                f"⏳ <i>Your {ctype} downloads soon…</i>",
                parse_mode=ParseMode.HTML,
            )
        else:
            status = await msg.reply_text(
                f"⚡ <b>Downloading {ctype}…</b>",
                parse_mode=ParseMode.HTML,
            )
        asyncio.create_task(
            _run_dl(msg.get_bot() if hasattr(msg, "get_bot") else ctx.bot,
                    msg.chat_id, url, ctype, INSTA_FORMAT,
                    status, dl_id, u.id, reply_to=msg.message_id)
        )
        return

    # Preview card mode
    await msg.chat.send_action(ChatAction.TYPING)
    imsg = await msg.reply_text(
        f"🔍 <b>Fetching {ctype} info…</b>",
        parse_mode=ParseMode.HTML,
    )

    info, err = await fetch_info(url, INSTA_FORMAT)
    if not info:
        await imsg.edit_text(friendly_error(err, ctype), parse_mode=ParseMode.HTML)
        return

    raw_desc = (info.get("description") or info.get("title") or
                info.get("uploader") or "No caption")
    caption  = raw_desc[:220] + ("…" if len(raw_desc) > 220 else "")
    thumb    = info.get("thumbnail") or ""
    uploader = info.get("uploader") or "Unknown"
    fmts     = info.get("formats") or []
    fsize    = max(
        (f.get("filesize") or f.get("filesize_approx") or 0 for f in fmts),
        default=0,
    )
    dur   = info.get("duration")
    dur_s = f"⏱ <b>Duration:</b> {int(dur)//60}:{int(dur)%60:02d}\n" if dur else ""
    likes = info.get("like_count")
    views = info.get("view_count")
    stats = ""
    if likes: stats += f"❤️ {likes:,}  "
    if views: stats += f"👁 {views:,}"
    if stats: stats = stats.strip() + "\n"

    dl_id = f"{u.id}_{int(time.time() * 1000)}"
    PENDING[dl_id] = {"url": url, "ctype": ctype, "fmt": INSTA_FORMAT}

    kb   = InlineKeyboardMarkup([[
        InlineKeyboardButton(f"⬇️ Download {ctype}", callback_data=f"dl_{dl_id}")
    ]])
    card = (
        f"✨ {ctype} <b>Ready!</b>\n\n"
        f"👤 <b>{uploader}</b>\n"
        f"{dur_s}"
        f"📦 <b>Size:</b> {hsize(fsize) if fsize else '~Unknown'}\n"
        f"{stats}"
        f"\n📝 <b>Caption:</b>\n<i>{caption}</i>\n\n"
        f"👇 Tap to download"
    )

    await imsg.delete()
    try:
        if thumb:
            await msg.chat.send_action(ChatAction.UPLOAD_PHOTO)
            await msg.reply_photo(
                photo=thumb, caption=card,
                parse_mode=ParseMode.HTML, reply_markup=kb,
            )
        else:
            raise ValueError()
    except Exception:
        await msg.reply_text(card, parse_mode=ParseMode.HTML, reply_markup=kb)

# ═══════════════════════════════════════════════════
#  YOUTUBE SHORTS FLOW
# ═══════════════════════════════════════════════════

async def handle_yt_short(msg, u, url: str):
    allowed, _, reset_in = rate_check(u.id)
    if not allowed:
        await say_rate_limit(msg, u.id, reset_in)
        return

    await msg.chat.send_action(ChatAction.TYPING)
    imsg = await msg.reply_text(
        "🔍 <b>Fetching YouTube Short…</b>",
        parse_mode=ParseMode.HTML,
    )

    info, err = await fetch_info(url, YT_FORMATS["720"])
    if not info:
        await imsg.edit_text(friendly_error(err, "🎬 Short"), parse_mode=ParseMode.HTML)
        return

    title   = (info.get("title") or "YouTube Short")[:80]
    thumb   = info.get("thumbnail") or ""
    dur     = info.get("duration")
    dur_s   = f"⏱ <b>{int(dur)//60}:{int(dur)%60:02d}</b>\n" if dur else ""
    channel = info.get("uploader") or info.get("channel") or "Unknown"
    views   = info.get("view_count")
    v_txt   = f"👁 {views:,}\n" if views else ""

    dl_id = f"yt_{u.id}_{int(time.time() * 1000)}"
    PENDING[dl_id] = {"url": url, "ctype": "🎬 Short", "fmt": None}

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📱 360p",      callback_data=f"ytq_{dl_id}_360"),
         InlineKeyboardButton("📺 480p",      callback_data=f"ytq_{dl_id}_480")],
        [InlineKeyboardButton("🎥 720p HD",   callback_data=f"ytq_{dl_id}_720"),
         InlineKeyboardButton("🔥 1080p FHD", callback_data=f"ytq_{dl_id}_1080")],
    ])
    card = (
        f"🎬 <b>YouTube Short Found!</b>\n\n"
        f"📌 <b>{title}</b>\n\n"
        f"📺 <b>Channel:</b> {channel}\n"
        f"{dur_s}{v_txt}"
        f"\n🎞 <b>Choose Quality:</b>"
    )

    await imsg.delete()
    try:
        if thumb:
            await msg.chat.send_action(ChatAction.UPLOAD_PHOTO)
            await msg.reply_photo(
                photo=thumb, caption=card,
                parse_mode=ParseMode.HTML, reply_markup=kb,
            )
        else:
            raise ValueError()
    except Exception:
        await msg.reply_text(card, parse_mode=ParseMode.HTML, reply_markup=kb)

# ═══════════════════════════════════════════════════
#  PRIVATE CHAT — DIRECT LINK
# ═══════════════════════════════════════════════════

async def handle_private(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u    = update.effective_user
    msg  = update.message
    text = (msg.text or "").strip()

    db_register(u.id, u.first_name or "", u.username or "")

    # Admin broadcast intercept
    if is_admin(u) and ctx.user_data.get("bcast"):
        ctx.user_data.pop("bcast", None)
        await _do_broadcast(ctx, msg)
        return

    # YouTube Short
    yt = extract_yt_short(text)
    if yt:
        await handle_yt_short(msg, u, yt)
        return

    # Reject non-Short YouTube
    if is_yt_not_short(text):
        await msg.reply_text(
            "❌ <b>YouTube Videos Not Supported</b>\n\n"
            "I only support <b>YouTube Shorts</b>.\n\n"
            "✅ Send a link like:\n"
            "<code>youtube.com/shorts/...</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    # Instagram
    ig = extract_insta(text)
    if ig:
        await handle_insta(msg, u, ig, instant=False)
        return

    # Unknown link
    if "http" in text or "www." in text or "instagram" in text or "youtube" in text:
        await msg.reply_text(
            "🚫 <b>Unsupported Link</b>\n\n"
            "I only support:\n"
            "  📸 <b>Instagram</b> — Reels, Posts, IGTV, Stories\n"
            "  🎬 <b>YouTube Shorts</b>\n\n"
            "<i>Regular YouTube, TikTok etc. are not supported.</i>",
            parse_mode=ParseMode.HTML,
        )

# ═══════════════════════════════════════════════════
#  GROUP COMMANDS
# ═══════════════════════════════════════════════════

async def cmd_down(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u, msg, args = update.effective_user, update.message, ctx.args
    db_register(u.id, u.first_name or "", u.username or "")
    if not args:
        await msg.reply_text(
            "📥 <b>Usage:</b> <code>/down {link}</code>",
            parse_mode=ParseMode.HTML)
        return
    raw = " ".join(args)
    yt  = extract_yt_short(raw)
    if yt:
        await handle_yt_short(msg, u, yt)
        return
    if is_yt_not_short(raw):
        await msg.reply_text("❌ Only <b>YouTube Shorts</b> supported.", parse_mode=ParseMode.HTML)
        return
    ig = extract_insta(raw)
    if not ig:
        await say_invalid(msg)
        return
    await handle_insta(msg, u, ig, instant=False)


async def cmd_insta_group(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u, msg, args = update.effective_user, update.message, ctx.args
    db_register(u.id, u.first_name or "", u.username or "")
    if not args:
        await msg.reply_text(
            "⚡ <b>Usage:</b> <code>/insta {link}</code>",
            parse_mode=ParseMode.HTML)
        return
    raw = " ".join(args)
    yt  = extract_yt_short(raw)
    if yt:
        await handle_yt_short(msg, u, yt)
        return
    if is_yt_not_short(raw):
        await msg.reply_text("❌ Only <b>YouTube Shorts</b> supported.", parse_mode=ParseMode.HTML)
        return
    ig = extract_insta(raw)
    if not ig:
        await say_invalid(msg)
        return
    await handle_insta(msg, u, ig, instant=True)

# ═══════════════════════════════════════════════════
#  CALLBACK HANDLERS
# ═══════════════════════════════════════════════════

async def dl_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q    = update.callback_query
    u    = q.from_user
    data = q.data
    await q.answer("✅ Starting…")

    # ── YT quality ────────────────────────────────
    if data.startswith("ytq_"):
        parts   = data.split("_")
        quality = parts[-1]
        dl_id   = "_".join(parts[1:-1])
        task    = PENDING.get(dl_id)
        if not task:
            try: await q.edit_message_reply_markup(InlineKeyboardMarkup([]))
            except Exception: pass
            await ctx.bot.send_message(
                update.effective_chat.id,
                "⏰ <b>Link Expired.</b> Please send the link again.",
                parse_mode=ParseMode.HTML)
            return

        allowed, _, reset_in = rate_check(u.id)
        if not allowed:
            await say_rate_limit(q, u.id, reset_in)
            return

        try: await q.edit_message_reply_markup(InlineKeyboardMarkup([]))
        except Exception: pass

        fmt    = YT_FORMATS.get(quality, YT_FORMATS["720"])
        url    = task["url"]
        qmap   = {"360": "📱 360p", "480": "📺 480p",
                  "720": "🎥 720p HD", "1080": "🔥 1080p FHD"}
        qlabel = qmap.get(quality, quality)

        qs   = q_add(dl_id)
        wait = qs - MAX_PARALLEL
        if wait > 0:
            status = await ctx.bot.send_message(
                update.effective_chat.id,
                f"📥 <b>Queued!</b>\n📍 Position: <b>#{wait}</b>\n"
                f"🎞 Quality: <b>{qlabel}</b>\n⏳ <i>Starting soon…</i>",
                parse_mode=ParseMode.HTML)
        else:
            status = await ctx.bot.send_message(
                update.effective_chat.id,
                f"⚡ <b>Downloading Short…</b>\n🎞 <b>{qlabel}</b>",
                parse_mode=ParseMode.HTML)

        asyncio.create_task(
            _run_dl(ctx.bot, update.effective_chat.id, url,
                    f"🎬 Short ({qlabel})", fmt, status, dl_id, u.id))
        return

    # ── Instagram download button ─────────────────
    if data.startswith("dl_"):
        dl_id = data[3:]
        task  = PENDING.get(dl_id)
        if not task:
            try: await q.edit_message_reply_markup(InlineKeyboardMarkup([]))
            except Exception: pass
            await ctx.bot.send_message(
                update.effective_chat.id,
                "⏰ <b>Link Expired.</b> Please send the link again.",
                parse_mode=ParseMode.HTML)
            return

        allowed, _, reset_in = rate_check(u.id)
        if not allowed:
            await say_rate_limit(q, u.id, reset_in)
            return

        try: await q.edit_message_reply_markup(InlineKeyboardMarkup([]))
        except Exception: pass

        url, ctype, fmt = task["url"], task["ctype"], task.get("fmt", INSTA_FORMAT)
        qs   = q_add(dl_id)
        wait = qs - MAX_PARALLEL
        if wait > 0:
            status = await ctx.bot.send_message(
                update.effective_chat.id,
                f"📥 <b>Queued!</b>\n📍 Position: <b>#{wait}</b>\n⏳ <i>Starting soon…</i>",
                parse_mode=ParseMode.HTML)
        else:
            status = await ctx.bot.send_message(
                update.effective_chat.id,
                f"⚡ <b>Downloading {ctype}…</b>",
                parse_mode=ParseMode.HTML)

        asyncio.create_task(
            _run_dl(ctx.bot, update.effective_chat.id, url,
                    ctype, fmt, status, dl_id, u.id))

# ═══════════════════════════════════════════════════
#  CORE DOWNLOAD + UPLOAD  (fixed — no stuck at 100%)
# ═══════════════════════════════════════════════════

async def _run_dl(bot, chat_id, url, ctype, fmt, status, dl_id, uid, reply_to=None):
    async with SEMAPHORE:
        try:
            await _dl_and_send(bot, chat_id, url, ctype, fmt, status, uid, reply_to)
        finally:
            q_remove(dl_id)
            PENDING.pop(dl_id, None)


async def _dl_and_send(bot, chat_id, url, ctype, fmt, status, uid, reply_to=None):
    loop = asyncio.get_event_loop()

    with tempfile.TemporaryDirectory() as tmpdir:
        outtmpl  = os.path.join(tmpdir, "video.%(ext)s")
        last_upd = [0.0]
        last_pct = [-1]

        # ── Download progress hook ─────────────────
        def _hook(d: dict):
            if d["status"] != "downloading":
                return
            now = time.time()
            if now - last_upd[0] < 4.0:
                return
            last_upd[0] = now
            dl  = d.get("downloaded_bytes") or 0
            tot = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
            spd = d.get("speed") or 0
            eta = d.get("eta") or 0
            if tot > 0:
                pct = int(dl * 100 / tot)
                if pct == last_pct[0]:
                    return
                last_pct[0] = pct
                txt = (
                    f"⬇️ <b>Downloading {ctype}</b>\n\n"
                    f"<code>[{pbar(pct)}] {pct}%</code>\n\n"
                    f"📦 {hsize(dl)} / {hsize(tot)}\n"
                    f"⚡ {hspeed(spd)}  ⏳ {htime(eta)}"
                )
            else:
                txt = (
                    f"⬇️ <b>Downloading {ctype}</b>\n\n"
                    f"📦 {hsize(dl)}\n⚡ {hspeed(spd)}"
                )
            asyncio.run_coroutine_threadsafe(
                sedit(bot, chat_id, status.message_id, txt), loop)

        await bot.send_chat_action(chat_id=chat_id, action=ChatAction.RECORD_VIDEO)

        dl_err  = [None]
        dl_path = [None]

        def _run():
            try:
                with yt_dlp.YoutubeDL(make_opts(outtmpl, fmt, _hook)) as ydl:
                    ydl.extract_info(url, download=True)
                # Find downloaded file
                for fname in sorted(os.listdir(tmpdir)):
                    fp = os.path.join(tmpdir, fname)
                    if os.path.isfile(fp) and not fname.endswith(".part"):
                        dl_path[0] = fp
                        return
                dl_err[0] = "File not found after download."
            except yt_dlp.utils.DownloadError as e:
                dl_err[0] = str(e)
            except Exception as e:
                dl_err[0] = f"Error: {e}"

        await loop.run_in_executor(None, _run)

        if dl_err[0] or not dl_path[0]:
            await sedit(bot, chat_id, status.message_id,
                        friendly_error(dl_err[0], ctype))
            return

        fsize = os.path.getsize(dl_path[0])
        if fsize > 49 * 1024 * 1024:
            await sedit(bot, chat_id, status.message_id,
                f"📦 <b>File Too Large</b>\n\n"
                f"Size: <b>{hsize(fsize)}</b>\n"
                f"Telegram allows max <b>50 MB</b>.\n\n"
                f"<i>Try a lower quality option.</i>")
            return

        # ── Consume rate limit slot ────────────────
        rate_consume(uid)

        # ── Upload  (simple direct upload — no tracker to avoid stuck) ──
        await sedit(bot, chat_id, status.message_id,
            f"✅ <b>Downloaded!</b>  📤 <b>Uploading to Telegram…</b>\n\n"
            f"📦 Size: <b>{hsize(fsize)}</b>\n\n"
            f"<i>Please wait, this may take a moment…</i>")

        await bot.send_chat_action(chat_id=chat_id, action=ChatAction.UPLOAD_VIDEO)

        try:
            with open(dl_path[0], "rb") as video_file:
                kw = dict(
                    chat_id=chat_id,
                    video=video_file,
                    caption=(
                        f"✨ <b>{ctype}</b>\n\n"
                        f"📦 {hsize(fsize)}\n"
                        f"<i>via InstaFetch Bot</i>"
                    ),
                    parse_mode=ParseMode.HTML,
                    supports_streaming=True,
                    write_timeout=600,
                    read_timeout=600,
                    connect_timeout=60,
                )
                if reply_to:
                    kw["reply_to_message_id"] = reply_to
                await bot.send_video(**kw)

            db_record(uid, fsize)

            # Delete status message after successful upload
            try:
                await bot.delete_message(
                    chat_id=chat_id, message_id=status.message_id)
            except Exception:
                await sedit(bot, chat_id, status.message_id,
                            f"✅ <b>Done!</b> Your {ctype} is ready! 🎉")

        except Exception as e:
            log.error(f"Upload error: {e}")
            await sedit(bot, chat_id, status.message_id,
                "😔 <b>Upload Failed</b>\n\n"
                "Something went wrong while sending the file.\n"
                "<i>Please try again.</i>")

# ═══════════════════════════════════════════════════
#  BROADCAST
# ═══════════════════════════════════════════════════

async def _do_broadcast(ctx, msg):
    db   = db_stats()
    sent = failed = 0
    st   = await msg.reply_text(
        f"📢 <b>Broadcasting to {len(db['users'])} users…</b>",
        parse_mode=ParseMode.HTML)
    for uid_s in db["users"]:
        try:
            await ctx.bot.forward_message(
                chat_id=int(uid_s),
                from_chat_id=msg.chat_id,
                message_id=msg.message_id,
            )
            sent += 1
            await asyncio.sleep(0.06)
        except Exception:
            failed += 1
    await st.edit_text(
        f"✅ <b>Broadcast Done!</b>\n\n"
        f"✅ Sent:   <b>{sent}</b>\n"
        f"❌ Failed: <b>{failed}</b>",
        parse_mode=ParseMode.HTML)

# ═══════════════════════════════════════════════════
#  ERROR HANDLER
# ═══════════════════════════════════════════════════

async def on_error(update: object, ctx: ContextTypes.DEFAULT_TYPE):
    log.error(f"Error: {ctx.error}", exc_info=ctx.error)

# ═══════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════

def main():
    if "YOUR_BOT_TOKEN_HERE" in BOT_TOKEN:
        print("⚠️  Set your BOT_TOKEN first!")
        return

    has_cookies = os.path.isfile(COOKIES_FILE)
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("  ✨  InstaFetch Bot  v6.0")
    print(f"  🔧  yt-dlp {yt_dlp.version.__version__}")
    print(f"  🛡️  Admin: {', '.join('@' + a for a in ADMIN_USERS)}")
    print(f"  🍪  Cookies: {'✅ Loaded (' + COOKIES_FILE + ')' if has_cookies else '❌ Not found'}")
    print(f"  ⏳  Rate limit: {RATE_LIMIT_COUNT}/hour per user")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    if not has_cookies:
        print("  ⚠️  WARNING: cookies.txt not found.")
        print("      Instagram may block downloads on server IPs.")
        print(f"      Place cookies.txt in: {os.path.abspath('.')}")
        print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .concurrent_updates(True)
        .build()
    )

    # Commands
    app.add_handler(CommandHandler("start",                   cmd_start))
    app.add_handler(CommandHandler("help",                    cmd_help))
    app.add_handler(CommandHandler("admin",                   cmd_admin))
    app.add_handler(CommandHandler(["down", "download"],      cmd_down))
    app.add_handler(CommandHandler(["insta", "instant"],      cmd_insta_group))

    # Callbacks
    app.add_handler(CallbackQueryHandler(admin_cb,    pattern=r"^adm_"))
    app.add_handler(CallbackQueryHandler(dl_callback, pattern=r"^(dl_|ytq_)"))

    # Group events
    app.add_handler(MessageHandler(
        filters.StatusUpdate.NEW_CHAT_MEMBERS, on_new_member))
    app.add_handler(MessageHandler(
        filters.StatusUpdate.LEFT_CHAT_MEMBER, on_left_member))

    # Private chat — direct links only (no commands needed)
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE,
        handle_private,
    ))

    app.add_error_handler(on_error)

    print("✅  Bot is running!\n")
    app.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
    )

if __name__ == "__main__":
    main()
