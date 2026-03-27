"""
╔═════════════════════════════════════════════════════╗
║   ✨  InstaFetch Bot  v5.0                          ║
║   Instagram  •  YouTube Shorts                      ║
║   Fast  •  Clean  •  No ffmpeg needed               ║
╚═════════════════════════════════════════════════════╝

  pip install python-telegram-bot yt-dlp
  python insta_bot.py
"""

# ══════════════════════════════════════════════════════
#  ⚙️  CONFIG
# ══════════════════════════════════════════════════════

BOT_TOKEN   = "YOUR_BOT_TOKEN_HERE"    # ← Paste token here
ADMIN_USERS = ["ankushdahiya1"]         # Admins (no @)

MAX_PARALLEL       = 3      # simultaneous downloads
RATE_LIMIT_COUNT   = 25     # per user per hour
RATE_LIMIT_SECONDS = 3600
CONCURRENT_FRAGS   = 4
WELCOME_DELETE_S   = 60     # seconds before welcome message auto-deletes

# ══════════════════════════════════════════════════════

import os, re, time, json, asyncio, logging, tempfile, threading
from collections import deque
from datetime import datetime
from typing import Optional, Dict, List

import yt_dlp
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
)
from telegram.constants import ChatAction, ParseMode
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, filters, ContextTypes,
)

# ── Logging ────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    level=logging.INFO,
)
for lib in ("httpx", "telegram", "hpack"):
    logging.getLogger(lib).setLevel(logging.WARNING)
log = logging.getLogger("InstaFetch")

# ══════════════════════════════════════════════════════
#  DATABASE
# ══════════════════════════════════════════════════════

DB_FILE = "instabot_data.json"
DB_LOCK = threading.Lock()

def _blank():
    return {"users": {}, "total_dl": 0, "total_bytes": 0}

def _load():
    if os.path.isfile(DB_FILE):
        try:
            with open(DB_FILE) as f:
                d = json.load(f)
                for k, v in _blank().items(): d.setdefault(k, v)
                return d
        except Exception: pass
    return _blank()

def _save(db):
    try:
        with open(DB_FILE, "w") as f: json.dump(db, f, indent=2)
    except Exception as e: log.error(f"DB: {e}")

def db_register(uid, name, uname):
    with DB_LOCK:
        db = _load(); s = str(uid)
        if s not in db["users"]:
            db["users"][s] = {"name": name, "username": uname,
                              "joined": datetime.now().isoformat(),
                              "downloads": 0, "bytes": 0}
        else:
            db["users"][s].update({"name": name, "username": uname})
        _save(db)

def db_record(uid, size):
    with DB_LOCK:
        db = _load(); s = str(uid)
        db["total_dl"] += 1; db["total_bytes"] += size
        if s in db["users"]:
            db["users"][s]["downloads"] += 1
            db["users"][s]["bytes"] += size
        _save(db)

def db_stats(): 
    with DB_LOCK: return _load()

# ══════════════════════════════════════════════════════
#  RATE LIMITER
# ══════════════════════════════════════════════════════

_rate: Dict[int, deque] = {}
_rl   = threading.Lock()

def rate_check(uid):
    now = time.time()
    with _rl:
        q = _rate.setdefault(uid, deque())
        while q and now - q[0] > RATE_LIMIT_SECONDS: q.popleft()
        used = len(q)
        if used >= RATE_LIMIT_COUNT:
            return False, 0, int(RATE_LIMIT_SECONDS - (now - q[0]))
        return True, RATE_LIMIT_COUNT - used - 1, 0

def rate_consume(uid):
    with _rl: _rate.setdefault(uid, deque()).append(time.time())

# ══════════════════════════════════════════════════════
#  URL DETECTION
# ══════════════════════════════════════════════════════

# Instagram
INSTA_RE = re.compile(
    r"https?://(?:www\.)?instagram\.com/"
    r"(?:reel(?:s)?/[\w\-]+|p/[\w\-]+|tv/[\w\-]+|"
    r"stories/[\w.]+/\d+|highlights/\d+)",
    re.IGNORECASE,
)

# YouTube Shorts only
YT_SHORT_RE = re.compile(
    r"https?://(?:www\.)?(?:youtube\.com/shorts/|youtu\.be/)[\w\-]+",
    re.IGNORECASE,
)

# Any YouTube (to reject non-Shorts)
YT_ANY_RE = re.compile(
    r"https?://(?:www\.)?(?:youtube\.com|youtu\.be)/",
    re.IGNORECASE,
)

def extract_insta(text):
    m = INSTA_RE.search(text or "")
    return m.group(0).rstrip(").,>") if m else None

def extract_yt_short(text):
    m = YT_SHORT_RE.search(text or "")
    return m.group(0).rstrip(").,>") if m else None

def is_yt_non_short(text):
    """True if it's YouTube but NOT a Short."""
    has_yt  = bool(YT_ANY_RE.search(text or ""))
    has_sht = bool(YT_SHORT_RE.search(text or ""))
    return has_yt and not has_sht

def get_insta_ctype(url):
    u = url.lower()
    if "/reel"        in u: return "🎬 Reel",      False
    if "/p/"          in u: return "📸 Post",       False
    if "/tv/"         in u: return "📹 IGTV",       False
    if "/stories/"    in u: return "📖 Story",      True
    if "/highlights/" in u: return "⭐ Highlight",  True
    return "🎥 Video", False

# ── Error classifier ──────────────────────────────────

def classify_err(err):
    e = (err or "").lower()
    if any(k in e for k in ["private","login required","not authorized",
                             "401","login","cookies","checkpoint"]): return "private"
    if any(k in e for k in ["expired","no longer available"]):       return "expired"
    if any(k in e for k in ["rate","429","too many"]):               return "ratelimit"
    if "404" in e or "not found" in e:                               return "notfound"
    if "network" in e or "connect" in e:                             return "network"
    return "unknown"

def friendly_error(err, ctype):
    k = classify_err(err)
    if k == "private":  return (f"🔒 <b>Private Account</b>\n\nThis {ctype} belongs to a private account.\n<i>Private content support coming soon! 🚀</i>")
    if k == "expired":  return  "⏰ <b>Story Expired</b>\n\nInstagram Stories disappear after 24 hours."
    if k == "ratelimit":return  "⏳ <b>Too Many Requests</b>\n\nInstagram is rate-limiting. Wait a minute and try again."
    if k == "notfound": return  "🔍 <b>Not Found</b>\n\nThis post may have been deleted or the link is invalid."
    if k == "network":  return  "📡 <b>Connection Issue</b>\n\nCouldn't reach the server. Check your internet."
    return f"😔 <b>Something Went Wrong</b>\n\nCouldn't download this {ctype}.\nPlease try again in a moment."

# ══════════════════════════════════════════════════════
#  FORMAT HELPERS
# ══════════════════════════════════════════════════════

def pbar(pct, w=16):
    f = int(w * pct / 100)
    return "▓" * f + "░" * (w - f)

def hsize(b):
    if not b or b <= 0: return "~Unknown"
    if b >= 1<<30: return f"{b/(1<<30):.2f} GB"
    if b >= 1<<20: return f"{b/(1<<20):.1f} MB"
    return f"{b/1024:.0f} KB"

def hspeed(s):
    if not s or s <= 0: return "—"
    if s >= 1<<20: return f"{s/(1<<20):.1f} MB/s"
    return f"{s/1024:.0f} KB/s"

def htime(s):
    if not s or s <= 0: return "—"
    s = int(s)
    return f"{s//60}m {s%60}s" if s >= 60 else f"{s}s"

def nlink(user):
    fn = (user.first_name or "Friend").replace("<","&lt;").replace(">","&gt;")
    return f'<a href="tg://user?id={user.id}">{fn}</a>'

async def sedit(bot, cid, mid, txt):
    try:
        await bot.edit_message_text(
            chat_id=cid, message_id=mid,
            text=txt, parse_mode=ParseMode.HTML)
    except Exception: pass

def is_admin(user):
    return (user.username or "").lower().lstrip("@") in [a.lower() for a in ADMIN_USERS]

def is_private(chat): return chat.type == "private"

# ══════════════════════════════════════════════════════
#  YT-DLP OPTIONS
# ══════════════════════════════════════════════════════

# Instagram format (no ffmpeg, pre-muxed)
INSTA_FORMAT = (
    "best[ext=mp4][vcodec!=none][acodec!=none]"
    "/bestvideo[ext=mp4][acodec!=none]"
    "/best[vcodec!=none][acodec!=none]"
    "/best[ext=mp4]/best"
)

# YT Shorts quality formats (height-capped, audio included)
YT_FORMATS = {
    "360":  "best[height<=360][ext=mp4][acodec!=none]/best[height<=360][acodec!=none]/best[height<=360]",
    "480":  "best[height<=480][ext=mp4][acodec!=none]/best[height<=480][acodec!=none]/best[height<=480]",
    "720":  "best[height<=720][ext=mp4][acodec!=none]/best[height<=720][acodec!=none]/best[height<=720]",
    "1080": "best[height<=1080][ext=mp4][acodec!=none]/best[height<=1080][acodec!=none]/best[height<=1080]",
}

def make_opts(outtmpl, fmt, hook=None):
    o = {
        "format": fmt,
        "outtmpl": outtmpl,
        "quiet": True, "no_warnings": True, "noprogress": False,
        "concurrent_fragment_downloads": CONCURRENT_FRAGS,
        "retries": 5, "fragment_retries": 5,
        "continuedl": True, "postprocessors": [],
    }
    if hook: o["progress_hooks"] = [hook]
    return o

async def fetch_info(url, fmt=None):
    loop = asyncio.get_event_loop()
    info, err = None, None
    def _r():
        nonlocal info, err
        try:
            opts = {"quiet": True, "no_warnings": True,
                    "skip_download": True, "postprocessors": [],
                    "format": fmt or INSTA_FORMAT}
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=False)
        except Exception as e: err = str(e)
    await loop.run_in_executor(None, _r)
    return info, err

# ══════════════════════════════════════════════════════
#  QUEUE  (accurate position tracking)
# ══════════════════════════════════════════════════════

_active: List[str] = []
_ql = threading.Lock()

def q_add(tid):
    with _ql:
        _active.append(tid)
        return len(_active)

def q_remove(tid):
    with _ql:
        try: _active.remove(tid)
        except ValueError: pass

def q_size():
    with _ql: return len(_active)

SEMAPHORE = asyncio.Semaphore(MAX_PARALLEL)
PENDING: Dict[str, Dict] = {}

# ══════════════════════════════════════════════════════
#  RATE LIMIT REPLY
# ══════════════════════════════════════════════════════

async def say_rate_limit(target, uid, reset_in):
    m, s = divmod(reset_in, 60)
    txt = (
        f"⛔ <b>Hourly Limit Reached!</b>\n\n"
        f"You've used all <b>{RATE_LIMIT_COUNT} downloads</b> this hour.\n\n"
        f"⏳ Resets in: <b>{m}m {s}s</b>\n\n"
        f"<i>Limits keep the bot fast for everyone 💙</i>"
    )
    if hasattr(target, "reply_text"):
        await target.reply_text(txt, parse_mode=ParseMode.HTML)
    else:
        await target.message.reply_text(txt, parse_mode=ParseMode.HTML)

# ══════════════════════════════════════════════════════
#  /start
# ══════════════════════════════════════════════════════

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    db_register(u.id, u.first_name or "", u.username or "")
    chat = update.effective_chat

    if is_private(chat):
        await update.message.reply_text(
            f"✨ <b>Hey {nlink(u)}!</b>\n\n"
            f"Welcome to <b>InstaFetch</b> 🎉\n"
            f"Your personal media downloader!\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>Just send me a link:</b>\n\n"
            f"  📸 <b>Instagram</b>\n"
            f"  └ Reels, Posts, IGTV\n"
            f"  └ Stories &amp; Highlights <i>(public only)</i>\n\n"
            f"  🎬 <b>YouTube Shorts</b>\n"
            f"  └ Choose quality: 360p / 480p / 720p / 1080p\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🚀 <b>No commands needed!</b>\n"
            f"Just paste any link and I'll handle it.\n\n"
            f"<code>/stats</code> — Your download stats\n"
            f"<code>/help</code>  — Help &amp; info\n\n"
            f"⚡ <i>Limit: {RATE_LIMIT_COUNT} downloads / hour</i>",
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
    else:
        await update.message.reply_text(
            f"👋 <b>InstaFetch Bot is here!</b>\n\n"
            f"<b>Group Commands:</b>\n"
            f"  <code>/down {{link}}</code> — Download with preview\n"
            f"  <code>/insta {{link}}</code> — Instant download\n\n"
            f"<i>Supports Instagram &amp; YouTube Shorts</i>",
            parse_mode=ParseMode.HTML,
        )

# ══════════════════════════════════════════════════════
#  /help
# ══════════════════════════════════════════════════════

async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📖 <b>InstaFetch — Help</b>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "💬 <b>In Private Chat:</b>\n"
        "Just send any supported link directly!\n"
        "No commands needed.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "👥 <b>In Groups:</b>\n"
        "  <code>/down {link}</code>  — Preview + download\n"
        "  <code>/download {link}</code>  — Same\n"
        "  <code>/insta {link}</code>  — Instant download\n"
        "  <code>/instant {link}</code>  — Same\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "✅ <b>Supported Links:</b>\n"
        "  📸 <code>instagram.com/reel/...</code>\n"
        "  📸 <code>instagram.com/p/...</code>\n"
        "  📸 <code>instagram.com/tv/...</code>\n"
        "  📸 <code>instagram.com/stories/...</code>\n"
        "  🎬 <code>youtube.com/shorts/...</code>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "⚠️ <b>Limits:</b>\n"
        f"  • {RATE_LIMIT_COUNT} downloads / hour\n"
        "  • Max 50 MB per file\n"
        "  • Public content only",
        parse_mode=ParseMode.HTML,
    )

# ══════════════════════════════════════════════════════
#  /stats
# ══════════════════════════════════════════════════════

async def cmd_stats(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u  = update.effective_user
    db = db_stats()
    me = db["users"].get(str(u.id), {})
    ok, remaining, _ = rate_check(u.id)
    used_this_hr = RATE_LIMIT_COUNT - remaining - (0 if ok else 1)
    await update.message.reply_text(
        f"📊 <b>Stats</b> — {nlink(u)}\n\n"
        f"⬇️  Downloads:   <b>{me.get('downloads', 0)}</b>\n"
        f"📦  Data saved:  <b>{hsize(me.get('bytes', 0))}</b>\n"
        f"⏳  This hour:   <b>{used_this_hr}</b> / {RATE_LIMIT_COUNT}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🌍 <b>Global</b>\n\n"
        f"👥  Users:     <b>{len(db['users'])}</b>\n"
        f"⬇️  Total DLs: <b>{db['total_dl']}</b>\n"
        f"📦  Total:     <b>{hsize(db['total_bytes'])}</b>",
        parse_mode=ParseMode.HTML,
    )

# ══════════════════════════════════════════════════════
#  ADMIN PANEL
# ══════════════════════════════════════════════════════

def _admin_text_kb():
    db = db_stats()
    top = sorted(db["users"].items(),
                 key=lambda x: x[1].get("downloads", 0), reverse=True)[:5]
    top_txt = ""
    for i, (_, info) in enumerate(top, 1):
        un = ("@" + info["username"]) if info.get("username") else info.get("name", "—")
        top_txt += f"  {i}. {un} — {info.get('downloads',0)} DLs ({hsize(info.get('bytes',0))})\n"

    text = (
        f"🛡️ <b>Admin Panel</b>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>Overview</b>\n\n"
        f"👥  Users:       <b>{len(db['users'])}</b>\n"
        f"⬇️   Downloads:  <b>{db['total_dl']}</b>\n"
        f"📦  Data:        <b>{hsize(db['total_bytes'])}</b>\n"
        f"🔄  Active:      <b>{q_size()}</b> / {MAX_PARALLEL}\n\n"
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
    u = update.effective_user
    if not is_admin(u):
        await update.message.reply_text("🚫 Admins only.")
        return
    text, kb = _admin_text_kb()
    await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)


async def admin_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    u = q.from_user
    if not is_admin(u):
        await q.answer("🚫 Admins only.", show_alert=True)
        return

    back = [[InlineKeyboardButton("⬅️ Back", callback_data="adm_home")]]
    data = q.data
    db   = db_stats()

    if data == "adm_home":
        text, kb = _admin_text_kb()
        try: await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
        except Exception: pass

    elif data == "adm_live":
        await q.edit_message_text(
            f"📊 <b>Live Stats</b>\n\n"
            f"👥 Users:       <b>{len(db['users'])}</b>\n"
            f"⬇️  Total DLs:  <b>{db['total_dl']}</b>\n"
            f"📦 Total Data:  <b>{hsize(db['total_bytes'])}</b>\n"
            f"🔄 Active Tasks:<b>{q_size()}</b> / {MAX_PARALLEL}",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(back),
        )

    elif data == "adm_users":
        users = list(db["users"].items())
        lines = []
        for _, info in users[-20:]:
            un = ("@"+info["username"]) if info.get("username") else info.get("name","—")
            lines.append(f"• {un} — {info.get('downloads',0)} DLs")
        await q.edit_message_text(
            f"👥 <b>Users</b> (last 20 of {len(users)})\n\n" + "\n".join(lines),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(back),
        )

    elif data == "adm_reset_ask":
        await q.edit_message_text(
            "⚠️ <b>Reset all stats?</b>\n\nThis cannot be undone.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Yes, Reset", callback_data="adm_reset_do")],
                [InlineKeyboardButton("❌ Cancel",     callback_data="adm_home")],
            ]),
        )

    elif data == "adm_reset_do":
        with DB_LOCK:
            db2 = _load()
            db2["total_dl"] = 0; db2["total_bytes"] = 0
            for s in db2["users"]:
                db2["users"][s]["downloads"] = 0
                db2["users"][s]["bytes"] = 0
            _save(db2)
        await q.edit_message_text(
            "✅ <b>Stats Reset!</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(back),
        )

    elif data == "adm_bcast":
        ctx.user_data["bcast"] = True
        await q.edit_message_text(
            "📢 <b>Broadcast</b>\n\nSend your message now.\n"
            "It will be forwarded to all users.\n\n"
            "<i>Send /cancel to abort.</i>",
            parse_mode=ParseMode.HTML,
        )

# ══════════════════════════════════════════════════════
#  WELCOME / LEAVE  (Group Events)
# ══════════════════════════════════════════════════════

async def on_new_member(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg: return
    for member in (msg.new_chat_members or []):
        if member.is_bot: continue
        try:
            welcome = await msg.chat.send_message(
                f"🎉 <b>Welcome to the group,</b> {nlink(member)}!\n\n"
                f"Glad to have you here 🙌\n"
                f"Feel free to use <b>InstaFetch Bot</b> to\n"
                f"download Instagram &amp; YouTube Shorts! 🚀\n\n"
                f"<i>This message will disappear in {WELCOME_DELETE_S}s</i>",
                parse_mode=ParseMode.HTML,
            )
            # Schedule deletion after WELCOME_DELETE_S seconds
            asyncio.create_task(_delete_after(ctx.bot, msg.chat_id,
                                              welcome.message_id, WELCOME_DELETE_S))
        except Exception as e:
            log.warning(f"Welcome msg error: {e}")

    # Delete the "X joined the group" system message instantly
    try:
        await msg.delete()
    except Exception:
        pass


async def on_left_member(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    # Delete "X left the group" system message instantly
    try:
        await update.message.delete()
    except Exception:
        pass


async def _delete_after(bot, chat_id, msg_id, delay):
    await asyncio.sleep(delay)
    try:
        await bot.delete_message(chat_id=chat_id, message_id=msg_id)
    except Exception:
        pass

# ══════════════════════════════════════════════════════
#  PRIVATE CHAT — DIRECT LINK HANDLER
# ══════════════════════════════════════════════════════

async def handle_private_msg(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u    = update.effective_user
    msg  = update.message
    text = (msg.text or "").strip()

    db_register(u.id, u.first_name or "", u.username or "")

    # Admin broadcast intercept
    if is_admin(u) and ctx.user_data.get("bcast"):
        ctx.user_data.pop("bcast", None)
        await _do_broadcast(ctx, msg)
        return

    # Try YT Short
    yt_url = extract_yt_short(text)
    if yt_url:
        await _handle_yt_short(msg, u, yt_url)
        return

    # Reject regular YouTube
    if is_yt_non_short(text):
        await msg.reply_text(
            "❌ <b>YouTube Videos Not Supported</b>\n\n"
            "I only support <b>YouTube Shorts</b>.\n\n"
            "✅ Send a link like:\n"
            "<code>youtube.com/shorts/...</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    # Try Instagram
    insta_url = extract_insta(text)
    if insta_url:
        await _handle_insta(msg, u, insta_url, instant=False)
        return

    # Unknown / unsupported
    # Only reply if it looks like they tried to send a link
    if "http" in text or "www." in text or "instagram" in text or "youtube" in text:
        await msg.reply_text(
            "🚫 <b>Unsupported Link</b>\n\n"
            "I only support:\n"
            "  📸 <b>Instagram</b> — Reels, Posts, IGTV, Stories\n"
            "  🎬 <b>YouTube Shorts</b> — <code>youtube.com/shorts/...</code>\n\n"
            "<i>Regular YouTube videos, TikTok, etc. are not supported.</i>",
            parse_mode=ParseMode.HTML,
        )

# ══════════════════════════════════════════════════════
#  GROUP COMMANDS  /down  /insta
# ══════════════════════════════════════════════════════

async def cmd_down(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u, msg, args = update.effective_user, update.message, ctx.args
    db_register(u.id, u.first_name or "", u.username or "")

    if not args:
        await msg.reply_text(
            "📥 <b>Usage:</b>\n<code>/down https://instagram.com/reel/...</code>",
            parse_mode=ParseMode.HTML)
        return

    raw = " ".join(args)

    yt = extract_yt_short(raw)
    if yt:
        await _handle_yt_short(msg, u, yt)
        return

    if is_yt_non_short(raw):
        await msg.reply_text(
            "❌ Only <b>YouTube Shorts</b> are supported.\n"
            "<code>youtube.com/shorts/...</code>",
            parse_mode=ParseMode.HTML)
        return

    ig = extract_insta(raw)
    if not ig:
        await msg.reply_text("❓ <b>Invalid link.</b>\nSend an Instagram or YouTube Shorts URL.", parse_mode=ParseMode.HTML)
        return
    await _handle_insta(msg, u, ig, instant=False)


async def cmd_insta(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u, msg, args = update.effective_user, update.message, ctx.args
    db_register(u.id, u.first_name or "", u.username or "")

    if not args:
        await msg.reply_text(
            "⚡ <b>Usage:</b>\n<code>/insta https://instagram.com/reel/...</code>",
            parse_mode=ParseMode.HTML)
        return

    raw = " ".join(args)

    yt = extract_yt_short(raw)
    if yt:
        await _handle_yt_short(msg, u, yt)
        return

    if is_yt_non_short(raw):
        await msg.reply_text(
            "❌ Only <b>YouTube Shorts</b> are supported.\n"
            "<code>youtube.com/shorts/...</code>",
            parse_mode=ParseMode.HTML)
        return

    ig = extract_insta(raw)
    if not ig:
        await msg.reply_text("❓ <b>Invalid link.</b>", parse_mode=ParseMode.HTML)
        return
    await _handle_insta(msg, u, ig, instant=True)

# ══════════════════════════════════════════════════════
#  INSTAGRAM FLOW
# ══════════════════════════════════════════════════════

async def _handle_insta(msg, u, url, instant: bool):
    ctype, needs_cookies = get_insta_ctype(url)

    if needs_cookies:
        await msg.reply_text(
            f"🚧 <b>{ctype} — Coming Soon!</b>\n\n"
            f"Support for Stories &amp; Highlights requires\n"
            f"account authentication which we're actively\n"
            f"building. Stay tuned! 🚀\n\n"
            f"<i>— InstaFetch Team</i>",
            parse_mode=ParseMode.HTML,
        )
        return

    allowed, _, reset_in = rate_check(u.id)
    if not allowed:
        await say_rate_limit(msg, u.id, reset_in)
        return

    if instant:
        # Go straight to download
        dl_id = f"{u.id}_{int(time.time()*1000)}"
        PENDING[dl_id] = {"url": url, "ctype": ctype, "fmt": INSTA_FORMAT}
        qs = q_add(dl_id)
        wait = qs - MAX_PARALLEL
        if wait > 0:
            status = await msg.reply_text(
                f"📥 <b>Queued!</b>\n\n"
                f"📍 Position: <b>#{wait}</b> in line\n"
                f"⏳ <i>Your {ctype} downloads soon…</i>",
                parse_mode=ParseMode.HTML)
        else:
            status = await msg.reply_text(
                f"⚡ <b>Downloading {ctype}…</b>",
                parse_mode=ParseMode.HTML)
        asyncio.create_task(
            _run_dl(msg._bot if hasattr(msg, '_bot') else msg.get_bot(),
                    msg.chat_id, url, ctype, INSTA_FORMAT, status, dl_id, u.id,
                    ctx_bot=None, reply_to=msg.message_id))
        return

    # Card mode — fetch info first
    await msg.chat.send_action(ChatAction.TYPING)
    imsg = await msg.reply_text(
        f"🔍 <b>Fetching {ctype}…</b>", parse_mode=ParseMode.HTML)

    info, err = await fetch_info(url, INSTA_FORMAT)
    if not info:
        await imsg.edit_text(friendly_error(err, ctype), parse_mode=ParseMode.HTML)
        return

    raw_desc = (info.get("description") or info.get("title") or info.get("uploader") or "No caption")
    caption  = raw_desc[:220] + ("…" if len(raw_desc) > 220 else "")
    thumb    = info.get("thumbnail") or ""
    uploader = info.get("uploader") or "Unknown"
    fmts     = info.get("formats") or []
    fsize    = max((f.get("filesize") or f.get("filesize_approx") or 0 for f in fmts), default=0)
    dur      = info.get("duration")
    dur_s    = f"⏱ <b>Duration:</b> {int(dur)//60}:{int(dur)%60:02d}\n" if dur else ""
    likes    = info.get("like_count")
    views    = info.get("view_count")
    stats    = ""
    if likes: stats += f"❤️ {likes:,}  "
    if views: stats += f"👁 {views:,}"
    if stats: stats = stats.strip() + "\n"

    dl_id = f"{u.id}_{int(time.time()*1000)}"
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
            await msg.reply_photo(photo=thumb, caption=card,
                                  parse_mode=ParseMode.HTML, reply_markup=kb)
        else:
            raise ValueError()
    except Exception:
        await msg.reply_text(card, parse_mode=ParseMode.HTML, reply_markup=kb)

# ══════════════════════════════════════════════════════
#  YOUTUBE SHORTS FLOW
# ══════════════════════════════════════════════════════

async def _handle_yt_short(msg, u, url):
    allowed, _, reset_in = rate_check(u.id)
    if not allowed:
        await say_rate_limit(msg, u.id, reset_in)
        return

    await msg.chat.send_action(ChatAction.TYPING)
    imsg = await msg.reply_text(
        "🔍 <b>Fetching YouTube Short info…</b>",
        parse_mode=ParseMode.HTML)

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

    dl_id = f"yt_{u.id}_{int(time.time()*1000)}"
    PENDING[dl_id] = {"url": url, "ctype": "🎬 Short", "fmt": None}

    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📱 360p",   callback_data=f"ytq_{dl_id}_360"),
            InlineKeyboardButton("📺 480p",   callback_data=f"ytq_{dl_id}_480"),
        ],
        [
            InlineKeyboardButton("🎥 720p HD",   callback_data=f"ytq_{dl_id}_720"),
            InlineKeyboardButton("🔥 1080p FHD", callback_data=f"ytq_{dl_id}_1080"),
        ],
    ])

    card = (
        f"🎬 <b>YouTube Short Found!</b>\n\n"
        f"📌 <b>{title}</b>\n\n"
        f"📺 <b>Channel:</b> {channel}\n"
        f"{dur_s}"
        f"{v_txt}"
        f"\n🎞 <b>Choose Quality:</b>"
    )

    await imsg.delete()
    try:
        if thumb:
            await msg.chat.send_action(ChatAction.UPLOAD_PHOTO)
            await msg.reply_photo(photo=thumb, caption=card,
                                  parse_mode=ParseMode.HTML, reply_markup=kb)
        else:
            raise ValueError()
    except Exception:
        await msg.reply_text(card, parse_mode=ParseMode.HTML, reply_markup=kb)

# ══════════════════════════════════════════════════════
#  CALLBACK HANDLERS
# ══════════════════════════════════════════════════════

async def dl_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q  = update.callback_query
    await q.answer("✅ Starting download…")
    u  = q.from_user
    data = q.data

    # ── YT quality selection ─────────────────────────
    if data.startswith("ytq_"):
        # format: ytq_{dl_id}_{quality}
        parts   = data.split("_")
        quality = parts[-1]                    # last part = quality
        dl_id   = "_".join(parts[1:-1])        # middle = dl_id

        task = PENDING.get(dl_id)
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

        fmt = YT_FORMATS.get(quality, YT_FORMATS["720"])
        try: await q.edit_message_reply_markup(InlineKeyboardMarkup([]))
        except Exception: pass

        url   = task["url"]
        qs    = q_add(dl_id)
        wait  = qs - MAX_PARALLEL
        qmap  = {"360": "📱 360p", "480": "📺 480p",
                 "720": "🎥 720p HD", "1080": "🔥 1080p FHD"}
        qlabel = qmap.get(quality, quality)

        if wait > 0:
            status = await ctx.bot.send_message(
                update.effective_chat.id,
                f"📥 <b>Queued!</b>\n\n"
                f"📍 Position: <b>#{wait}</b> in line\n"
                f"🎞 Quality: <b>{qlabel}</b>\n"
                f"⏳ <i>Downloading soon…</i>",
                parse_mode=ParseMode.HTML)
        else:
            status = await ctx.bot.send_message(
                update.effective_chat.id,
                f"⚡ <b>Downloading Short…</b>\n🎞 Quality: <b>{qlabel}</b>",
                parse_mode=ParseMode.HTML)

        asyncio.create_task(
            _run_dl(ctx.bot, update.effective_chat.id, url,
                    f"🎬 Short ({qlabel})", fmt, status, dl_id, u.id))
        return

    # ── Instagram download button ────────────────────
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
            _run_dl(ctx.bot, update.effective_chat.id, url, ctype, fmt, status, dl_id, u.id))

# ══════════════════════════════════════════════════════
#  CORE DOWNLOAD + UPLOAD
# ══════════════════════════════════════════════════════

async def _run_dl(bot, chat_id, url, ctype, fmt, status, dl_id, uid,
                  ctx_bot=None, reply_to=None):
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

        def _hook(d):
            if d["status"] != "downloading": return
            now = time.time()
            if now - last_upd[0] < 4.0: return
            last_upd[0] = now
            dl  = d.get("downloaded_bytes") or 0
            tot = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
            spd = d.get("speed") or 0
            eta = d.get("eta") or 0
            if tot > 0:
                pct = int(dl * 100 / tot)
                if pct == last_pct[0]: return
                last_pct[0] = pct
                txt = (f"⬇️ <b>Downloading {ctype}</b>\n\n"
                       f"<code>[{pbar(pct)}] {pct}%</code>\n\n"
                       f"📦 {hsize(dl)} / {hsize(tot)}\n"
                       f"⚡ {hspeed(spd)}  ⏳ {htime(eta)}")
            else:
                txt = (f"⬇️ <b>Downloading {ctype}</b>\n\n"
                       f"📦 {hsize(dl)}\n⚡ {hspeed(spd)}")
            asyncio.run_coroutine_threadsafe(
                sedit(bot, chat_id, status.message_id, txt), loop)

        await bot.send_chat_action(chat_id=chat_id, action=ChatAction.RECORD_VIDEO)

        dl_err  = [None]
        dl_path = [None]

        def _run():
            try:
                with yt_dlp.YoutubeDL(make_opts(outtmpl, fmt, _hook)) as ydl:
                    ydl.extract_info(url, download=True)
                for f in sorted(os.listdir(tmpdir)):
                    fp = os.path.join(tmpdir, f)
                    if os.path.isfile(fp) and not f.endswith(".part"):
                        dl_path[0] = fp; return
                dl_err[0] = "File not found."
            except yt_dlp.utils.DownloadError as e: dl_err[0] = str(e)
            except Exception as e:                   dl_err[0] = f"Error: {e}"

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
                f"Telegram limit is <b>50 MB</b>.\n\n"
                f"<i>Try a lower quality option.</i>")
            return

        rate_consume(uid)

        await sedit(bot, chat_id, status.message_id,
            f"✅ <b>Downloaded!</b>  📤 <b>Uploading…</b>\n\n"
            f"<code>[{'░'*16}] 0%</code>\n📦 {hsize(fsize)}")
        await bot.send_chat_action(chat_id=chat_id, action=ChatAction.UPLOAD_VIDEO)

        up_start = time.time()
        up_last  = [0.0]
        up_sent  = [0]

        class _T:
            def __init__(self, fp, total):
                self._fp = fp; self._t = total
            def read(self, n=-1):
                c = self._fp.read(n)
                if c:
                    up_sent[0] += len(c)
                    now = time.time()
                    if now - up_last[0] >= 4.0:
                        up_last[0] = now
                        pct = int(up_sent[0]*100/self._t) if self._t else 0
                        spd = up_sent[0] / max(now - up_start, 0.001)
                        asyncio.run_coroutine_threadsafe(
                            sedit(bot, chat_id, status.message_id,
                                f"📤 <b>Uploading {ctype}</b>\n\n"
                                f"<code>[{pbar(pct)}] {pct}%</code>\n\n"
                                f"📦 {hsize(up_sent[0])} / {hsize(self._t)}\n"
                                f"⚡ {hspeed(spd)}"),
                            loop)
                return c
            def __iter__(self): return self
            def __next__(self):
                c = self.read(65536)
                if not c: raise StopIteration
                return c

        try:
            with open(dl_path[0], "rb") as raw:
                kw = dict(
                    chat_id=chat_id, video=_T(raw, fsize),
                    caption=(f"✨ <b>{ctype}</b>\n📦 {hsize(fsize)}\n"
                             f"<i>via InstaFetch Bot</i>"),
                    parse_mode=ParseMode.HTML, supports_streaming=True,
                    write_timeout=600, read_timeout=600, connect_timeout=60,
                )
                if reply_to: kw["reply_to_message_id"] = reply_to
                await bot.send_video(**kw)

            db_record(uid, fsize)
            try: await bot.delete_message(chat_id=chat_id, message_id=status.message_id)
            except Exception:
                await sedit(bot, chat_id, status.message_id,
                            f"✅ <b>Done!</b> Your {ctype} is ready 🎉")

        except Exception as e:
            log.error(f"Upload error: {e}")
            await sedit(bot, chat_id, status.message_id,
                "😔 <b>Upload Failed</b>\n\nPlease try again.")

# ══════════════════════════════════════════════════════
#  BROADCAST
# ══════════════════════════════════════════════════════

async def _do_broadcast(ctx, msg):
    db = db_stats()
    sent = failed = 0
    st = await msg.reply_text(
        f"📢 <b>Broadcasting to {len(db['users'])} users…</b>",
        parse_mode=ParseMode.HTML)
    for uid_s in db["users"]:
        try:
            await ctx.bot.forward_message(
                chat_id=int(uid_s),
                from_chat_id=msg.chat_id,
                message_id=msg.message_id)
            sent += 1
            await asyncio.sleep(0.06)
        except Exception: failed += 1
    await st.edit_text(
        f"✅ <b>Done!</b>  Sent: <b>{sent}</b>  Failed: <b>{failed}</b>",
        parse_mode=ParseMode.HTML)

# ══════════════════════════════════════════════════════
#  ERROR HANDLER
# ══════════════════════════════════════════════════════

async def on_error(update: object, ctx: ContextTypes.DEFAULT_TYPE):
    log.error(f"Error: {ctx.error}", exc_info=ctx.error)

# ══════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════

def main():
    if "YOUR_BOT_TOKEN_HERE" in BOT_TOKEN:
        print("⚠️  Set BOT_TOKEN first!"); return

    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("  ✨  InstaFetch Bot  v5.0")
    print(f"  🔧  yt-dlp {yt_dlp.version.__version__}")
    print(f"  🛡️  Admin: {', '.join('@'+a for a in ADMIN_USERS)}")
    print(f"  ⏳  Rate limit: {RATE_LIMIT_COUNT}/hour per user")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .concurrent_updates(True)
        .build()
    )

    # Commands (group + private)
    app.add_handler(CommandHandler("start",                   cmd_start))
    app.add_handler(CommandHandler("help",                    cmd_help))
    app.add_handler(CommandHandler("stats",                   cmd_stats))
    app.add_handler(CommandHandler("admin",                   cmd_admin))
    app.add_handler(CommandHandler(["down", "download"],      cmd_down))
    app.add_handler(CommandHandler(["insta", "instant"],      cmd_insta))

    # Callbacks
    app.add_handler(CallbackQueryHandler(admin_cb,    pattern=r"^adm_"))
    app.add_handler(CallbackQueryHandler(dl_callback, pattern=r"^(dl_|ytq_)"))

    # Group events
    app.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, on_new_member))
    app.add_handler(MessageHandler(filters.StatusUpdate.LEFT_CHAT_MEMBER,  on_left_member))

    # Private chat direct links (no commands)
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE,
        handle_private_msg,
    ))

    app.add_error_handler(on_error)

    print("✅  Bot is running!\n")
    app.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
    )

if __name__ == "__main__":
    main()
