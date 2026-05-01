import os
import telebot
from telebot import types
import json
from datetime import datetime, timedelta
import time
import re
import html
from contextlib import contextmanager
import logging
import threading
import csv
import io

# Modular Imports
try:
    import pytz
    IST = pytz.timezone('Asia/Kolkata')
except ImportError:
    IST = None

import db
import sheets
import ui
import scoring

WEBHOOK_SECRET = os.getenv('WEBHOOK_SECRET', 'my_secret_token_123')
from dotenv import load_dotenv
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

from flask import Flask, request, abort

# ===================================================
# CONFIGURATION
# ===================================================
ROLES = ['bat', 'wk', 'ar', 'bowl', 'sub']
ROLE_LIMITS = {
    'wk': (1, 4),
    'bat': (3, 6),
    'ar': (1, 4),
    'bowl': (3, 6),
    'sub': (0, 4)
}
ROLE_NAMES = {'bat': 'Batsmen', 'wk': 'Wicketkeepers', 'ar': 'All-rounders', 'bowl': 'Bowlers', 'sub': 'Impact/Sub'}
NEXT_ROLES = {'bat': 'wk', 'wk': 'ar', 'ar': 'bowl', 'bowl': 'sub'}
MIN_WITHDRAWAL = 200

# FIX 4: Selection Cooldown
_selection_cooldown = {}

# SEC 2: Input Sanitization helper
def sanitize_input(text, max_len=100):
    if not text: return ""
    clean = re.sub(r'<[^>]*?>', '', text) # Strip HTML
    return clean[:max_len].strip()

MATCHES = {}

# Load environment variables from .env file (for local development)
load_dotenv()

# 1. Token aur Admin ID - Priority to environment variables
TOKEN = os.getenv('BOT_TOKEN', '').strip()
ADMIN_ID = os.getenv('ADMIN_ID', '').strip()

# 2. Webhook Host detection
raw_host = os.getenv('WEBHOOK_URL') or os.getenv('RENDER_EXTERNAL_URL')
if not raw_host and os.getenv('RENDER_SERVICE_NAME'):
    # Render service name se underscore hatakar lowercase mein URL banayein
    svc_name = os.getenv('RENDER_SERVICE_NAME').lower().replace('_', '-')
    raw_host = f"https://{svc_name}.onrender.com"

WEBHOOK_HOST = raw_host or ""

if not TOKEN or not ADMIN_ID:
    logging.error("❌ CRITICAL: BOT_TOKEN or ADMIN_ID is missing!")
    raise ValueError("Missing essential Environment Variables.")

def get_now():
    """Returns current time in IST (Indian Standard Time)"""
    if IST:
        return datetime.now(IST).replace(tzinfo=None)
    return datetime.now() + timedelta(hours=5, minutes=30)

def get_payment_channel():
    return db.db_get_setting('PAYMENT_CHANNEL_ID', os.getenv('PAYMENT_CHANNEL_ID', ADMIN_ID))

def get_support_channel():
    return db.db_get_setting('SUPPORT_CHANNEL_ID', os.getenv('SUPPORT_CHANNEL_ID', '-1003909393820'))

PAYMENT_UPI = os.getenv('PAYMENT_UPI', "amankumar8879@ibl")

# Flask Server Setup
server = Flask(__name__)
# CRITICAL: threaded=False is required for Webhook mode in Flask/Gunicorn
bot = telebot.TeleBot(TOKEN, threaded=False)

@server.route('/', methods=['GET'])
def index():
    return "Bot is running!", 200

@server.route('/healthz', methods=['GET'])
def health():
    return "OK", 200

@server.route('/bot-webhook', methods=['GET', 'POST'])
def webhook():
    """Telegram Webhook Endpoint"""
    if request.method == 'GET':
        return "🤖 Webhook is active! Telegram sends updates here via POST.", 200

    if request.headers.get('content-type') == 'application/json':
        logging.info("📥 Webhook hit: POST request received")
        try:
            json_string = request.get_data(as_text=True)
            update = telebot.types.Update.de_json(json_string)
            
            # SEC 1: Webhook Signature Verification
            secret_header = request.headers.get('X-Telegram-Bot-Api-Secret-Token')
            if WEBHOOK_SECRET and secret_header != WEBHOOK_SECRET:
                return "Unauthorized", 403

            if update:
                msg_type = "Message" if update.message else "ChannelPost" if update.channel_post else "Callback" if update.callback_query else "Update"
                logging.info(f"🔔 Update ID {update.update_id} ({msg_type}) parsing to handlers...")
                bot.process_new_updates([update])
            return '', 200
        except Exception as e:
            logging.error(f"❌ Webhook Processing Error: {e}")
            return '', 200 # Telegram ko 200 bhein taaki wo retry na kare
    else:
        abort(403)

def get_support_handle():
    # Handle ko saaf karke return karein taaki link hamesha sahi bane
    return db.db_get_setting('SUPPORT_HANDLE', 'CRICK_Community001').replace('@', '').strip()

def get_channel_handle():
    return db.db_get_setting('CHANNEL_HANDLE', 'crick_channel001')

def is_admin(user_id):
    """Checks if the given user_id is the authorized administrator"""
    return str(user_id) == str(ADMIN_ID)

def sync_matches_from_db():
    """Database se matches load karke global MATCHES dict mein dalta hai"""
    global MATCHES
    db_matches = db.db_get_matches()
    for m in db_matches:
        try:
            MATCHES[m['match_id']] = {
                'name': m['name'],
                'type': m['type'],
                'deadline': datetime.strptime(m['deadline'], '%Y-%m-%d %H:%M').replace(tzinfo=None),
                'points_calculated': bool(m['points_calculated']),
                'manual_lock': m.get('manual_lock', 0) # 0: Auto, 1: Forced Lock, -1: Forced Unlock
            }
        except Exception as e:
            logging.error(f"Error parsing match {m['match_id']}: {e}")

# Memory cache to make UI interaction lightning fast
PLAYERS_CACHE = {}

# 🛠 Admin Active Match Context: To speed up player/point updates
ADMIN_MATCH_CONTEXT = {}

def get_players(match_id):
    """Database se players fetch karke role-wise dictionary return karega (With Cache)"""
    if match_id in PLAYERS_CACHE:
        return PLAYERS_CACHE[match_id]
        
    db_players = db.db_get_players_by_match(match_id)
    # 🛠️ Structure change: Storing dict with name and display info
    formatted_data = {r: [] for r in ROLES + ['cv']} 
    
    for p in db_players:
        role = p.get('role', '').lower()
        desig = p.get('designation', '').lower()
        
        tag = " Ⓒ" if desig == 'c' else " Ⓥ" if desig == 'vc' else ""
        display_name = f"{p['player_name']} ({p['team']}){tag}"
        
        p_obj = {
            'name': p['player_name'],
            'display': display_name
        }
        
        if role in formatted_data:
            formatted_data[role].append(p_obj)
            
        if desig in ['c', 'vc']:
            formatted_data['cv'].append(p_obj)
    
    PLAYERS_CACHE[match_id] = formatted_data
    return formatted_data

user_active_match = {} # Tracks which match user is paying for
user_deposit_amount = {} # Temporary store for deposit amount input
temp_team_cache = {} # Selection cache
user_active_order = {} # user_id -> order_id mapping
def process_payment_success(user_id, amount, ref_id, match_context=None, conn=None):
    """
    Atomic transaction to process successful payments.
    ref_id can be UTR, Webhook ID, or Screenshot File ID.
    If conn is provided, uses existing transaction to avoid database locks.
    """
    def _do_work(c):
        # 1. Idempotency Check
        c.execute("SELECT id FROM LEDGER WHERE reference_id=%s", (ref_id,))
        exists = c.fetchone()
        if exists: return False, "Payment already processed."

        # 2. Add to Ledger
        c.execute("INSERT INTO LEDGER (user_id, amount, type, reference_id, timestamp) VALUES (%s, %s, 'CREDIT', %s, %s)",
                  (str(user_id), amount, ref_id, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

        # 3. Mark team/context as paid
        if match_context and "_" in match_context:
            mid, tnum = match_context.split("_")
            if mid != "wallet":
                c.execute("UPDATE TEAMS SET is_paid=1 WHERE user_id=%s AND match_id=%s AND team_num=%s", (str(user_id), mid, int(tnum)))
                
                # 🎁 Check for Referral Reward (First Contest Join)
                c.execute("SELECT referred_by FROM USERS WHERE user_id=%s", (str(user_id),))
                user_info = c.fetchone()
                if user_info and user_info['referred_by']:
                    referrer_id = user_info['referred_by']
                    # Reward logic (db_reward_referrer provides idempotency check internally)
                    if db.db_reward_referrer(referrer_id, user_id, amount=10):
                        try:
                            bot.send_message(referrer_id, f"🎊 *Referral Bonus!*\n\nAapke referral ne pehla contest join kar liya hai! Aapko ₹10 bonus mila hai.", parse_mode='Markdown')
                        except: pass

        return True, "Success"

    if conn:
        return _do_work(conn)
    else:
        try:
            with db.get_db() as new_conn: return _do_work(new_conn)
        except Exception as e: return False, str(e)

def is_match_locked(match_id='m1'):
    """Checks if the match is locked (Manual override takes priority)"""
    info = MATCHES.get(match_id)
    if not info: return False # Unknown match is not locked by default
    
    m_lock = info.get('manual_lock', 0)
    if m_lock == 1: return True   # Admin forced LOCK
    if m_lock == -1: return False # Admin forced UNLOCK
    
    # Default: Check Deadline
    deadline = info.get('deadline', get_now())
    return get_now() > deadline

def get_time_left(match_id='m1'):
    """Returns countdown string until lock"""
    match_info = MATCHES.get(match_id)
    if not match_info: return "N/A"
    deadline = match_info.get('deadline', get_now())
    delta = deadline - get_now()
    if delta.total_seconds() <= 0: return "LOCKED 🔒"
    return f"{delta.days}d {delta.seconds//3600}h {(delta.seconds//60)%60}m"

# ===================================================
# INITIALIZATION
# ===================================================
db.init_db()
db.run_migrations()
sync_matches_from_db()

# 🆕 Async GSheets Sync: Taki bot start hone mein delay na ho
def async_sheets_sync():
    try:
        sheet_matches = sheets.get_all_rows_safe("MATCHES")
        if sheet_matches:
            for m in sheet_matches:
                db.db_add_match(m['match_id'], m['name'], m['type'], m['deadline'])
            sync_matches_from_db() # Refresh memory cache after sheet sync
    except: pass
threading.Thread(target=async_sheets_sync, daemon=True).start()

def setup_webhook():
    """Sets up the webhook for production environments"""
    if WEBHOOK_HOST and os.getenv('RENDER'):
        # URL ko clean karein (spaces aur trailing slashes hatayein)
        clean_host = WEBHOOK_HOST.strip().lower().rstrip('/')
        if not clean_host.startswith('http'):
            clean_host = f"https://{clean_host}"
        
        webhook_url = f"{clean_host}/bot-webhook"
        logging.info(f"⚙️ Webhook Sync: Attempting to set URL to: {webhook_url}")
        
        try:
            current_info = bot.get_webhook_info()
            if not current_info.url or current_info.url.strip('/') != webhook_url.strip('/'):
                bot.remove_webhook()
                time.sleep(0.5)
                # SEC 1: Set webhook with secret token
                bot.set_webhook(url=webhook_url, drop_pending_updates=True, 
                                allowed_updates=["message", "callback_query", "channel_post"],
                                secret_token=WEBHOOK_SECRET)
                logging.info(f"🚀 Webhook successfully set to: {webhook_url}")
            else:
                logging.info("✅ Webhook already configured correctly.")
        except Exception as e:
            logging.error(f"❌ Webhook Setup Error: {e}")

# Trigger Webhook Setup during module load for Gunicorn
if os.getenv('RENDER'):
    setup_webhook()

# ===================================================
# CONSTANTS
# ===================================================

# ===================================================
# DATABASE FUNCTIONS
# ===================================================

def db_get_team(user_id, match_id='m1', team_num=1):
    """Priority: Return from Cache, then DB"""
    cache_key = (str(user_id), match_id, int(team_num))
    if cache_key in temp_team_cache:
        return temp_team_cache[cache_key]

    # Using internal modular function
    data = db.db_get_team_internal(user_id, match_id, team_num)
    if data:
        temp_team_cache[cache_key] = data
    return data

def db_save_team(user_id, team_data, match_id='m1', team_num=1):
    """Persist to Database and invalidate cache for consistency"""
    try:
        with db.get_db() as conn:
            team_json = json.dumps({k: team_data.get(k, []) for k in ROLES})
            conn.execute("""
                INSERT INTO TEAMS (user_id, match_id, team_num, team_players, captain, vice_captain, team_saved, is_paid) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id, match_id, team_num) 
                DO UPDATE SET 
                    team_players = EXCLUDED.team_players, 
                    captain = COALESCE(EXCLUDED.captain, TEAMS.captain), 
                    vice_captain = COALESCE(EXCLUDED.vice_captain, TEAMS.vice_captain), 
                    team_saved = GREATEST(TEAMS.team_saved, EXCLUDED.team_saved), 
                    is_paid = GREATEST(TEAMS.is_paid, EXCLUDED.is_paid)
            """, (str(user_id), match_id, team_num, team_json, team_data.get('captain'), team_data.get('vice_captain'), team_data.get('team_saved', 0), team_data.get('is_paid', 0)))
        # Clear cache to force refresh on next access
        cache_key = (str(user_id), match_id, int(team_num))
        if cache_key in temp_team_cache:
            del temp_team_cache[cache_key]
    except Exception as e:
        logging.error(f"Error saving team: {e}")

def db_has_saved_team(user_id, match_id):
    """Checks if user has at least one saved team for a match"""
    try:
        with db.get_db() as conn:
            conn.execute("SELECT 1 FROM TEAMS WHERE user_id=%s AND match_id=%s AND team_saved=1 LIMIT 1", (str(user_id), match_id))
            row = conn.fetchone()
            return True if row else False
    except Exception as e:
        logging.error(f"Error checking saved team: {e}")
        return False

def get_total_players(team):
    """Returns count of starting 11 players only"""
    if not team:
        return 0
    count = 0
    for role in ['bat', 'wk', 'ar', 'bowl']:
        if isinstance(team.get(role), list):
            count += len(team.get(role, []))
    return count

def get_paid_count():
    try:
        with db.get_db() as conn:
            conn.execute("SELECT COUNT(*) as cnt FROM USERS WHERE paid=1")
            result = conn.fetchone()
            return result['cnt'] if result else 0
    except Exception as e:
        logging.error(f"Error getting paid count: {e}")
        return 0

def get_leaderboard(limit=10):
    try:
        with db.get_db() as conn:
            conn.execute(
                "SELECT u.username, u.first_name, t.points FROM TEAMS t JOIN USERS u ON t.user_id = u.user_id WHERE t.points > 0 ORDER BY t.points DESC LIMIT %s",
                (limit,)
            )
            rows = conn.fetchall()
            return rows
    except Exception as e:
        logging.error(f"Error getting leaderboard: {e}")
        return []

# ===================================================
# START COMMAND
# ===================================================

@bot.message_handler(commands=['start'])
def start_command(message):
    """Basic Start with Registration Logic"""
    uid = str(message.from_user.id)
    logging.info(f"✅ Handler Triggered: /start command from {uid}")

    # ⚡ Optimized: Single DB call for Register + Get + LastSeen
    is_new_registration, user_data = db.db_register_user_optimized(uid, message.from_user.username, message.from_user.first_name)

    # Existing home message returning users ko milega as before
    if is_new_registration:
        send_onboarding_step1(message.chat.id, message.from_user.first_name)
        return  # Tour start, baaki start_command skip
    referrer = None
    if len(message.text.split()) > 1:
        ref_data = message.text.split()[1]
        if ref_data.startswith('ref'):
            potential_ref = ref_data.replace('ref', '')
            if potential_ref.isdigit() and potential_ref != uid:
                referrer = potential_ref

    # Sheets sync
    try:
        sheets.sync_wrapper({
            "user_id": uid,
            "username": message.from_user.username or "N/A",
            "first_name": message.from_user.first_name or "N/A",
            "paid": 0,
            "balance": 0,
            "joined_date": get_now().strftime('%Y-%m-%d %H:%M:%S')
        }, "USERS")
    except: pass

    if referrer and is_new_registration:
        with db.get_db() as conn:
            conn.execute("UPDATE USERS SET referred_by = %s WHERE user_id = %s AND referred_by IS NULL", (referrer, uid))
    
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add("🏆 CONTEST", "💰 WALLET", "⚾ MY TEAM", "👥 LEADERBOARD", "📊 STATS", "ℹ️ HELP")

    # Channel Link Logic
    c_handle = get_channel_handle().replace('@', '').strip()
    c_url = f"https://t.me/{c_handle}"
    s_url = f"https://t.me/{get_support_handle()}"

    inline_markup = types.InlineKeyboardMarkup()
    inline_markup.add(types.InlineKeyboardButton("💬 PUBLIC QUERY GROUP", url=s_url))
    inline_markup.add(types.InlineKeyboardButton("🔗 SHARE BOT", switch_inline_query=f"Join & Win: https://t.me/{bot.get_me().username}?start=ref{uid}"))

    brief = (
        f"🏏 <b>Welcome, {message.from_user.first_name}!</b>\n\n"
        "� <b>90% Prize Pool</b> • ⚡ <b>Fast UPI Payout</b>\n\n"
        "🎯 <b>Kaise jeetein:</b>\n"
        "1️⃣ Team banao (11 players)\n"
        "2️⃣ Captain/VC set karo\n"
        "3️⃣ Contest join karo\n\n"
        f"📈 <b>Min Withdrawal:</b> ₹{MIN_WITHDRAWAL}\n"
        f"🔥 <b>Next:</b> Click <b>🏆 CONTEST</b> niche menu se match select karne ke liye!"
    )
    bot.send_message(message.chat.id, brief, reply_markup=markup, parse_mode='HTML')
    bot.send_message(message.chat.id, "👇 <b>Updates aur Winner Screenshots ke liye join karein:</b>", reply_markup=inline_markup, parse_mode='HTML')

@bot.callback_query_handler(func=lambda call: call.data.startswith("copy_channel_handle_"))
def callback_copy_channel_handle(call):
    channel_handle = call.data.split("_")[3]
    bot.answer_callback_query(
        call.id,
        f"Channel Username: @{channel_handle}\n\nIs username ko copy karke Telegram search bar mein paste karein.",
        show_alert=True
    )
# ===================================================
# MY TEAM - BUILD TEAM
# ===================================================

@bot.message_handler(commands=['myteam'])
@bot.message_handler(func=lambda m: m.text and "MY TEAM" in m.text)
def cmd_my_team(msg):
    markup = types.InlineKeyboardMarkup(row_width=1)
    for mid, info in MATCHES.items():
        markup.add(types.InlineKeyboardButton(f"⚾ Build for {info['name']}", callback_data=f"team_slots_{mid}"))
    bot.send_message(msg.chat.id, "Select Match to Build Team:", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith("team_slots_"))
def callback_team_slots(call):
    # ⚡ 1. Sabse pehle answer karein taki Telegram ka loading icon hat jaye
    bot.answer_callback_query(call.id)

    # Robust parsing for match_id
    parts = call.data.split("_")
    if len(parts) < 3: return
    
    match_id = parts[2]
    page = int(parts[3]) if len(parts) > 3 else 1
    uid = str(call.from_user.id)

    # ⚡ Optimization: Fetch all teams for this user and match in one single query
    # Loop ke andar db_get_team call karne se N+1 performance issue hota hai
    all_user_teams = db.db_get_all_user_teams(uid, match_id)
    # Create a lookup map: team_num -> data
    teams_map = {int(t['team_num']): t for t in all_user_teams}

    markup = types.InlineKeyboardMarkup(row_width=5)
    buttons = []
    
    # Paginated slots (10 per page)
    start_idx = (page - 1) * 10 + 1
    end_idx = start_idx + 10
    
    for i in range(start_idx, end_idx):
        t_data = teams_map.get(i)
        is_saved = t_data and t_data.get('team_saved') == 1
        
        label = f"T{i}✅" if is_saved else f"T{i}"
        cb = f"view_team_{match_id}_{i}" if is_saved else f"nav_bat_{match_id}_{i}"
        buttons.append(types.InlineKeyboardButton(label, callback_data=cb))

    markup.add(*buttons)
    
    # Pagination Controls
    nav_btns = []
    if page > 1:
        nav_btns.append(types.InlineKeyboardButton("⬅️ Prev", callback_data=f"team_slots_{match_id}_{page-1}"))
    if page < 5:
        nav_btns.append(types.InlineKeyboardButton("Next ➡️", callback_data=f"team_slots_{match_id}_{page+1}"))
    if nav_btns:
        markup.row(*nav_btns)

    markup.add(types.InlineKeyboardButton("🔙 BACK TO MATCHES", callback_data="cmd_my_team_nav"))

    preview_text = f"⚾ *MY TEAMS: {MATCHES[match_id]['name']}*\n"
    preview_text += f"Page {page}/5 (Slots {start_idx}-{end_idx-1})\n\n"
    preview_text += "Select an empty slot to create a team, or a ✅ slot to view/edit."
    
    bot.edit_message_text(preview_text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='Markdown')

@bot.callback_query_handler(func=lambda call: call.data == "cmd_my_team_nav")
def callback_my_team_nav(call):
    cmd_my_team(call.message)
    bot.delete_message(call.message.chat.id, call.message.message_id)

def show_player_selection(chat_id, user_id, role, match_id='m1', team_num=1, message_id=None):
    """Fast UI update using Cache-First data"""
    try:
        if is_match_locked(match_id):
            bot.send_message(chat_id, "🚫 *MATCH LOCKED*\n\nMatch start ho chuka hai, ab team nahi badal sakte!", parse_mode='Markdown')
            return

        team = db_get_team(user_id, match_id, team_num)
        if not team:
            team = {k: [] for k in ROLES}
        
        selected = team.get(role, [])
        
        # Total count for UI
        total = get_total_players(team)
        
        markup = types.InlineKeyboardMarkup(row_width=1)
        
        for player_obj in get_players(match_id)[role]:
            p_name = player_obj['name']
            status = "✅" if p_name in selected else "⬜"
            # 🛠️ Logical change: Callback only uses clean name
            callback = f"sel_{match_id}_{team_num}_{role}_{p_name.replace(' ', '_')}"
            markup.add(types.InlineKeyboardButton(f"{status} {player_obj['display']}", callback_data=callback))
        
        # 🆕 Role Switcher: Direct jump to any category
        role_switcher = []
        for r_code in ['bat', 'wk', 'ar', 'bowl', 'sub']:
            label = r_code.upper()
            if r_code == role:
                label = f"» {label} «"
            role_switcher.append(types.InlineKeyboardButton(label, callback_data=f"nav_{r_code}_{match_id}_{team_num}"))
        
        markup.row(*role_switcher[:3])
        markup.row(*role_switcher[3:])

        nav_row = []
        if total == 11:
            nav_row.append(types.InlineKeyboardButton("🚀 PREVIEW & SAVE TEAM", callback_data=f"team_save_{match_id}_{team_num}"))
        
        markup.row(*nav_row)
        
        role_min, role_max = ROLE_LIMITS[role]
        
        # 🆕 Live Squad Summary
        squad_list = []
        for r_key in ['bat', 'wk', 'ar', 'bowl', 'sub']:
            p_names = team.get(r_key, [])
            if p_names:
                label = "IMP" if r_key == 'sub' else ROLE_NAMES[r_key][:3].upper()
                squad_list.append(f"*{label}:* " + ", ".join(p_names))
        
        summary_text = "\n".join(squad_list) if squad_list else "_Abhi tak koi player select nahi kiya._"

        text = (
            f"🏏 *Team Builder - {MATCHES[match_id]['name']}*\n"
            f"Slot: `T{team_num}` | Section: *{ROLE_NAMES[role]}*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"✅ Category: `{len(selected)}/{role_max}` | 👥 Squad: `{total}/11`\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"👉 *Aapki Team:*\n{summary_text}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"👉 *Players select karne ke liye tap karein:*"
        )

        if message_id:
            try:
                bot.edit_message_text(text, chat_id, message_id, reply_markup=markup, parse_mode='Markdown')
            except telebot.apihelper.ApiTelegramException as e:
                if "message is not modified" not in e.description.lower():
                    logging.error(f"Telegram API Error: {e}")
        else:
            bot.send_message(chat_id, text, reply_markup=markup, parse_mode='Markdown')
    except Exception as e:
        logging.error(f"Error in show_player_selection: {e}")
        # Send technical error to Admin instead of User
        if ADMIN_ID:
            bot.send_message(ADMIN_ID, f"⚠️ <b>UI Error (Selection):</b>\n<code>{html.escape(str(e))}</code>", parse_mode='HTML')
        # Friendly message for User
        bot.send_message(chat_id, "❌ Kuch technical issue hua hai. Kripya thodi der baad try karein ya support se sampark karein.")

# ===================================================
# SAVE TEAM
# ===================================================

@bot.callback_query_handler(func=lambda call: call.data.startswith("team_save_"))
def callback_team_save(call):
    uid = str(call.from_user.id)
    parts = call.data.split("_")
    match_id, team_num = parts[2], int(parts[3])
    
    if is_match_locked(match_id):
        bot.answer_callback_query(call.id, "🚫 Team Locked!", show_alert=True)
        return

    team = db_get_team(uid, match_id, team_num) # Checks cache first
    if not team:
        bot.answer_callback_query(call.id, "❌ Build team first!")
        return

    total_count = get_total_players(team)
    if total_count != 11:
        bot.answer_callback_query(call.id, f"❌ Team mein barabar 11 players (Playing 11) hone chahiye! (Abhi: {total_count})", show_alert=True)
        return

    for role, (r_min, r_max) in ROLE_LIMITS.items():
        count = len(team.get(role, []))
        if not (r_min <= count <= r_max):
            bot.answer_callback_query(call.id, f"❌ {ROLE_NAMES[role]} must be between {r_min}-{r_max}!")
            return

    preview_text = f"📝 *TEAM PREVIEW (T{team_num})*\n"
    for role_key in ROLES:
        players = team.get(role_key, [])
        if players:
            preview_text += f"\n*{ROLE_NAMES[role_key]}:* {', '.join(players)}"
    
    preview_text += f"\n\n👑 *C:* {team.get('captain', 'Not Selected')}\n⭐ *VC:* {team.get('vice_captain', 'Not Selected')}"
    
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("✅ CONFIRM & PROCEED TO C/VC", callback_data=f"final_confirm_save_{match_id}_{team_num}"),
        types.InlineKeyboardButton("✏️ EDIT TEAM", callback_data=f"nav_bat_{match_id}_{team_num}")
    )
    bot.edit_message_text(preview_text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='Markdown')

@bot.callback_query_handler(func=lambda call: call.data.startswith("final_confirm_save_"))
def callback_final_confirm_save(call):
    uid = str(call.from_user.id)
    parts = call.data.split("_")
    match_id, team_num = parts[3], int(parts[4])
    
    team = db_get_team(uid, match_id, team_num)
    team['team_saved'] = 1
    
    # Ensure C/VC are initialized to None if not set, to avoid errors in db_save_team
    if 'captain' not in team: team['captain'] = None
    if 'vice_captain' not in team: team['vice_captain'] = None
    db_save_team(uid, team, match_id, team_num)

    # Final Sync to Sheets (One row per team)
    all_players = []
    for r in ROLES:
        all_players.extend(team.get(r, []))
    
    sheets.sync_wrapper({
        "user_id": uid,
        "match": MATCHES[match_id]['name'],
        "team_num": team_num,
        "players": ", ".join(all_players),
        "captain": team.get('captain', 'N/A'),
        "vice_captain": team.get('vice_captain', 'N/A')
    }, "TEAMS")
    bot.answer_callback_query(call.id, "✅ Team Saved!")
    
    # Directly call the C/VC menu
    # Create a dummy call object to pass to callback_cv_menu
    dummy_call = types.CallbackQuery(id=call.id, from_user=call.from_user, message=call.message, chat_instance=call.chat_instance, data=f"set_cv_menu_{match_id}_{team_num}")
    callback_cv_menu(dummy_call)

@bot.callback_query_handler(func=lambda call: call.data.startswith("view_team_"))
def callback_view_team(call):
    parts = call.data.split("_")
    match_id, team_num = parts[2], int(parts[3])
    uid = str(call.from_user.id)
    
    team = db_get_team(uid, match_id, team_num)
    if not team:
        bot.answer_callback_query(call.id, "Team not found!")
        return

    text = f"🎉 *TEAM {team_num} SAVED! - {MATCHES[match_id]['name']}*\n\n"
    for role in ROLES:
        players = team.get(role, [])
        if players:
            text += f"*{ROLE_NAMES[role]}:* {', '.join(players)}\n"
    
    text += f"\n👑 C: {team.get('captain', '❌')}\n⭐ VC: {team.get('vice_captain', '❌')}"
    text += f"\n\n━━━━━━━━━━━━━━━━━━━━"
    text += f"\n💰 Paid: {'✅ YES' if team.get('is_paid') else '❌ NO'}"

    markup = types.InlineKeyboardMarkup(row_width=1)
    if not is_match_locked(match_id):
        markup.add(types.InlineKeyboardButton("✏️ EDIT TEAM", callback_data=f"nav_bat_{match_id}_{team_num}"))
        # 🚀 UX Improvement: If C/VC are set, show Join Contest button directly
        markup.add(types.InlineKeyboardButton("🎯 SET/CHANGE C & VC", callback_data=f"set_cv_menu_{match_id}_{team_num}"))
        
        if team.get('captain') and team.get('vice_captain') and not team.get('is_paid'):
            markup.add(types.InlineKeyboardButton("🚀 JOIN CONTEST NOW", callback_data=f"show_match_{match_id}"))
            
    markup.add(types.InlineKeyboardButton("📊 POINTS BREAKDOWN", callback_data=f"pts_break_{match_id}_{team_num}"))
    markup.add(types.InlineKeyboardButton("🔙 BACK TO SLOTS", callback_data=f"team_slots_{match_id}"))
    
    bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='Markdown')

@bot.callback_query_handler(func=lambda call: call.data.startswith("pts_break_"))
def callback_points_breakdown(call):
    parts = call.data.split("_")
    match_id, team_num = parts[2], int(parts[3])
    uid = str(call.from_user.id)
    
    team = db_get_team(uid, match_id, team_num)
    player_stats_map = db.db_get_player_live_stats_map(match_id)
    
    markup, text = ui.team_points_breakdown_render(match_id, team_num, team, player_stats_map)
    bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='Markdown')

@bot.callback_query_handler(func=lambda call: call.data.startswith("copy_upi_"))
def callback_copy_upi(call):
    upi = call.data.split("_")[2]
    # Alert dikha kar user ko batana ki copy kaise karein
    bot.answer_callback_query(call.id, f"UPI ID: {upi}\n\nMessage mein di gayi ID par tap karke copy karein!", show_alert=True)

@bot.callback_query_handler(func=lambda call: call.data.startswith("sel_"))
def callback_select_player(call):
    handle_selection(call)

@bot.callback_query_handler(func=lambda call: call.data.startswith("nav_"))
def callback_navigate_role(call):
    parts = call.data.split("_")
    if len(parts) < 4: return
    
    role = parts[1]
    match_id = parts[2]
    try:
        team_num = int(parts[3])
    except ValueError:
        # Handle case where match_id might have underscores
        team_num = int(parts[-1])
        match_id = "_".join(parts[2:-1])
        
    show_player_selection(call.message.chat.id, str(call.from_user.id), role, match_id, team_num, call.message.message_id)

@bot.callback_query_handler(func=lambda call: call.data.startswith("set_cv_menu_"))
def callback_cv_menu(call):
    uid = str(call.from_user.id)
    parts = call.data.split("_")
    match_id, team_num = parts[3], int(parts[4])
    if is_match_locked(match_id):
        bot.answer_callback_query(call.id, "🚫 Team Locked!", show_alert=True)
        return

    team = db_get_team(uid, match_id, team_num)
    players_info = get_players(match_id)
    
    # Create a mapping of clean_name -> display_name for the menu
    display_map = {}
    for role in ROLES:
        for p_obj in players_info.get(role, []):
            display_map[p_obj['name']] = p_obj['display']

    # Filter: Sirf wahi players dikhao jo user ki team mein hain AUR Admin ne C/VC designate kiye hain
    admin_cv_list = players_info.get('cv', []) # Admin designated candidates
    user_selected_names = []
    for r in ['bat', 'wk', 'ar', 'bowl']:
        user_selected_names.extend(team.get(r, []))

    # Intersection of User Team and Admin designated C/VCs
    available_candidates = [p for p in admin_cv_list if p['name'] in user_selected_names]
    
    markup = types.InlineKeyboardMarkup() 
    for p_obj in available_candidates:
        p_name = p_obj['name']
        d_name = p_obj['display']
        c_icon = "👑" if team.get('captain') == p_name else "⚪"
        vc_icon = "⭐" if team.get('vice_captain') == p_name else "⚪"

        # Row 1: Player Name
        markup.row(types.InlineKeyboardButton(f"👤 {d_name}", callback_data="ignore"))
        # Row 2: Inline C and VC buttons
        markup.row(
            types.InlineKeyboardButton(f"{c_icon} CAPTAIN", callback_data=f"cv_{match_id}_{team_num}_c_{p_name.replace(' ', '_')}"),
            types.InlineKeyboardButton(f"{vc_icon} VICE-CAPTAIN", callback_data=f"cv_{match_id}_{team_num}_vc_{p_name.replace(' ', '_')}")
        )

    if not available_candidates:
        markup.row(types.InlineKeyboardButton("⚠️ No designated C/VC in your team!", callback_data="ignore"))

    markup.add(types.InlineKeyboardButton("🔙 BACK", callback_data=f"team_save_{match_id}_{team_num}"))
    
    bot.edit_message_text("🎯 *Select Captain (2x) and Vice-Captain (1.5x)*\n\n_Captain aur Vice-Captain same nahi ho sakte._", 
                         call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='Markdown')

@bot.message_handler(commands=['edit_designation'])
def cmd_edit_designation(msg):
    if not is_admin(msg.from_user.id): return
    help_text = "✏️ *EDIT PLAYER DESIGNATION*\n\nFormat: `mid | Name | desig` \nEx: `m1 | Rohit Sharma | c` (or `vc` or `clear` to remove)"
    sent = bot.send_message(msg.chat.id, help_text, parse_mode='Markdown')
    bot.register_next_step_handler(sent, process_designation_edit)

def process_designation_edit(msg):
    try:
        parts = [p.strip() for p in msg.text.split("|")]
        mid, name, desig = parts[0], parts[1], parts[2].lower()
        if desig in ['c', 'vc', 'clear']:
            final_desig = "" if desig == 'clear' else desig
            with db.get_db() as conn:
                conn.execute("UPDATE PLAYERS SET designation = %s WHERE match_id = %s AND player_name = %s", (final_desig, mid, name))
            PLAYERS_CACHE.pop(mid, None)
            bot.reply_to(msg, f"✅ `{name}` ka designation `{desig.upper()}` update ho gaya hai.")
        else:
            bot.reply_to(msg, "❌ Invalid Designation! Use `c`, `vc`, or `clear`.")
    except Exception as e:
        bot.reply_to(msg, "❌ Format: `mid | Name | desig` use karein.")

@bot.callback_query_handler(func=lambda call: call.data.startswith("cv_"))
def callback_set_cv(call):
    uid = str(call.from_user.id)
    parts = call.data.split("_")
    match_id = parts[1]
    team_num = int(parts[2])
    type_cv = parts[3] # 'c' or 'vc'
    name = "_".join(parts[4:]).replace('_', ' ')
    
    team = db_get_team(uid, match_id, team_num)
    
    # Basic Validation: C and VC cannot be same
    if type_cv == 'c':
        if team.get('vice_captain') == name:
            bot.answer_callback_query(call.id, "❌ Yeh player pehle se Vice-Captain hai!", show_alert=True)
            return
        team['captain'] = name
    else:
        if team.get('captain') == name:
            bot.answer_callback_query(call.id, "❌ Yeh player pehle se Captain hai!", show_alert=True)
            return
        team['vice_captain'] = name
    
    db_save_team(uid, team, match_id, team_num)
    bot.answer_callback_query(call.id, f"{'Captain' if type_cv=='c' else 'VC'} set to {name}")
    
    # Go back to View Team so they can see the change and pick the other one
    call.data = f"view_team_{match_id}_{team_num}"
    callback_view_team(call)

# ===================================================
# CONTEST
# ===================================================

@bot.message_handler(commands=['contest'])
@bot.message_handler(func=lambda m: "CONTEST" in m.text)
def cmd_contest(msg):
    uid = str(msg.from_user.id)
    markup = types.InlineKeyboardMarkup(row_width=1)
    now = get_now()
    
    # Global check if user has ANY saved team across ANY match
    has_any_team = False
    with db.get_db() as conn:
        conn.execute("SELECT 1 FROM TEAMS WHERE user_id=%s AND team_saved=1 LIMIT 1", (uid,))
        row = conn.fetchone()
        has_any_team = bool(row)

    for mid, info in MATCHES.items():
        deadline = info['deadline']
        day_str = "TODAY" if deadline.date() == now.date() else "TOMORROW" if deadline.date() == (now.date() + timedelta(days=1)) else deadline.strftime('%d %b')
        
        if is_match_locked(mid):
            status = "🔒"
        else:
            status = f"🏏 [{info['type']}] {day_str} {deadline.strftime('%H:%M')}"
        markup.add(types.InlineKeyboardButton(f"{status} {info['name']} - {get_time_left(mid)}", callback_data=f"show_match_{mid}"))
    
    text = "🏆 *Matches*\n\n👉 *Next: Select a match to join contests*"
    if not has_any_team:
        text += "\n\n⚠️ *No team?*\n👉 Pehle team banao"
        markup.add(types.InlineKeyboardButton("🏏 CREATE TEAM", callback_data="cmd_my_team_nav"))
    
    markup.add(types.InlineKeyboardButton("🔙 Back", callback_data="app_home"))

    bot.send_message(msg.chat.id, text, reply_markup=markup, parse_mode='Markdown')

@bot.callback_query_handler(func=lambda call: call.data.startswith("join_"))
def callback_join_match(call):
    parts = call.data.split("_")
    match_id, fee = parts[1], int(parts[2])
    uid = str(call.from_user.id)
    
    if is_match_locked(match_id):
        bot.answer_callback_query(call.id, "🚫 Match is Locked!", show_alert=True)
        return

    # Show saved slots to join with
    # ⚡ OPTIMIZATION: Fetch all teams in ONE query instead of 50
    all_user_teams = db.db_get_all_user_teams(uid, match_id)
    teams_map = {int(t['team_num']): t for t in all_user_teams}

    markup = types.InlineKeyboardMarkup(row_width=4)
    buttons = []
    found_any = False
    
    for i in range(1, 51):
        t = teams_map.get(i)
        if t and t.get('team_saved'):
            found_any = True
            # Indicate if already paid for this slot
            label = f"T{i} 💳" if t.get('is_paid') else f"T{i}"
            buttons.append(types.InlineKeyboardButton(label, callback_data=f"confirm_join_{match_id}_{i}_{fee}"))
    
    if not found_any:
        bot.answer_callback_query(call.id, "🚀 Aapki koi saved team nahi mili! Chaliye pehle team banate hain.", show_alert=True)
        # Redirect to Team Builder (Slot 1, starting with Batsmen)
        show_player_selection(call.message.chat.id, uid, 'bat', match_id, 1, call.message.message_id)
        return

    markup.add(*buttons)
    markup.add(types.InlineKeyboardButton("⬅️ BACK", callback_data=f"show_match_{match_id}"))
    bot.edit_message_text(f"🏅 *Join Contest* (Entry: ₹{fee})\nSelect the Team Slot you want to use:", 
                         call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='Markdown')

@bot.callback_query_handler(func=lambda call: call.data.startswith("confirm_join_"))
def callback_confirm_join(call):
    parts = call.data.split("_")
    match_id, team_num, fee = parts[2], parts[3], int(parts[4])
    callback_pay_now(call) # Use the existing payment flow logic

@bot.callback_query_handler(func=lambda call: call.data.startswith("show_match_"))
def callback_show_match(call):
    mid = call.data.split("_")[2]
    uid = str(call.from_user.id)
    default_fee = 100
    
    info = MATCHES.get(mid)
    stats = db.get_contest_stats(mid, default_fee)
    user_summary = db.get_user_match_summary(uid, mid)
    has_team = db_has_saved_team(uid, mid)
    time_left = get_time_left(mid)
    
    # Dynamically fetch all configured contests for this match
    configs = db.db_get_all_contest_configs(mid)

    markup, text = ui.match_dashboard_render(mid, info, stats, user_summary, time_left, configs, default_fee)
    bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='Markdown')

@bot.callback_query_handler(func=lambda call: call.data.startswith("breakup_"))
def callback_prize_breakup(call):
    parts = call.data.split("_")
    match_id, fee = parts[1], int(parts[2])
    
    # Get current contest config to know max slots
    config = db.db_get_contest_config(match_id, fee)
    slots = config['max_slots'] if config else 50
    
    markup, text = ui.prize_breakdown_render(match_id, fee, slots)
    try:
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='Markdown')
    except:
        bot.answer_callback_query(call.id, "Error loading breakup.")

@bot.message_handler(commands=['set_contest_size'])
def cmd_set_contest_size(msg):
    if not is_admin(msg.from_user.id): return
    help_txt = (
        "📏 *ADD/UPDATE CONTEST*\n\n"
        "Format: `match_id | entry_fee | max_slots`\n\n"
        "💡 *Note:* \n"
        "• ₹100+ = 🥇 Mega\n"
        "• ₹50-99 = 🥈 Medium\n"
        "• Under ₹50 = 🥉 Small\n\n"
        "Example: `demo_m1 | 100 | 50`"
    )
    sent = bot.send_message(msg.chat.id, help_txt, parse_mode='Markdown')
    bot.register_next_step_handler(sent, process_contest_size)

@bot.message_handler(commands=['delete_contest'])
def cmd_delete_contest(msg):
    if not is_admin(msg.from_user.id): return
    help_txt = "🗑️ *DELETE CONTEST*\n\nFormat: `match_id | entry_fee` \nExample: `demo_m1 | 20`"
    sent = bot.send_message(msg.chat.id, help_txt, parse_mode='Markdown')
    bot.register_next_step_handler(sent, process_delete_contest)

def process_delete_contest(msg):
    try:
        parts = [p.strip() for p in msg.text.split("|")]
        mid, fee = parts[0], int(parts[1])
        db.db_delete_contest(mid, fee)
        bot.reply_to(msg, f"✅ Match `{mid}` se ₹{fee} wala contest delete ho gaya!")
    except:
        bot.reply_to(msg, "❌ Error! Format: `match_id | fee` use karein.")

def process_contest_size(msg):
    try:
        parts = [p.strip() for p in msg.text.split("|")]
        mid, fee, slots = parts[0], int(parts[1]), int(parts[2])
        db.db_set_contest_config(mid, fee, slots)
        bot.reply_to(msg, f"✅ *Contest Configured!*\nMatch: `{mid}`\nFee: ₹{fee}\nMax Slots: {slots}\n\nAb users ko 70% winners wala breakup dikhega.")
    except Exception as e:
        bot.reply_to(msg, "❌ Error! Use format: `mid | fee | slots`")

@bot.message_handler(commands=['set_prize_config'])
def cmd_set_prize_config(msg):
    if not is_admin(msg.from_user.id): return
    help_txt = (
        "🏆 *PRIZE DISTRIBUTION CONFIG*\n\n"
        "Format: `Commission | Winners% | R1% | R2% | R3%`\n\n"
        "Example: `10 | 70 | 35 | 20 | 12`"
    )
    sent = bot.send_message(msg.chat.id, help_txt, parse_mode='Markdown')
    bot.register_next_step_handler(sent, process_prize_config)

def process_prize_config(msg):
    try:
        parts = [p.strip() for p in msg.text.split("|")]
        comm, wins, r1, r2, r3 = parts[0], parts[1], parts[2], parts[3], parts[4]
        
        db.db_set_setting('PRIZE_COMMISSION', comm)
        db.db_set_setting('PRIZE_WINNERS_PCT', wins)
        db.db_set_setting('PRIZE_R1_PCT', r1)
        db.db_set_setting('PRIZE_R2_PCT', r2)
        db.db_set_setting('PRIZE_R3_PCT', r3)
        
        bot.reply_to(msg, "✅ *Prize Logic Updated!* \n\nAb sabhi naye breakup isi calculation par chalenge.")
    except Exception as e:
        bot.reply_to(msg, "❌ Error! Format: `10 | 70 | 35 | 20 | 12` check karein.")

@bot.callback_query_handler(func=lambda call: call.data.startswith("team_slots_nav_"))
def callback_team_slots_nav(call):
    parts = call.data.split("_")
    callback_team_slots(call)

@bot.callback_query_handler(func=lambda call: call.data == "contest_list")
def callback_contest_list(call):
    cmd_contest(call.message)
    bot.delete_message(call.message.chat.id, call.message.message_id)

@bot.callback_query_handler(func=lambda call: call.data == "build_team")
def callback_build_team(call):
    bot.answer_callback_query(call.id)
    show_player_selection(call.message.chat.id, str(call.from_user.id), 'bat')

# ===================================================
# PAYMENT
# ===================================================

@bot.callback_query_handler(func=lambda call: call.data.startswith("pay_now_"))
def callback_pay_now(call):
    uid = str(call.from_user.id)
    parts = call.data.split("_")
    chat_id = call.message.chat.id

    # Handle direct wallet deposit (without match)
    if len(parts) > 2 and parts[2] == "wallet":
        match_id, team_num = "wallet", "0"
        amount = int(db.db_get_user_state(uid, 'deposit_amount') or 100)
    else:
        match_id, team_num, amount = parts[2], parts[3], int(parts[4])
        team = db_get_team(uid, match_id, int(team_num)) or {}
        if not team or not team.get('team_saved'):
            bot.answer_callback_query(call.id, "❌ Pehle team save karein!", show_alert=True)
            return

    try:
        bot.answer_callback_query(call.id)
    except: pass

    send_payment_ui(chat_id, uid, amount, match_id, team_num)

def send_payment_ui(chat_id, uid, amount, match_id, team_num):
    """Centralized function to send payment instructions"""
    context = f"{match_id}_{team_num}"
    order_id = db.db_create_order(uid, amount, context)
    db.db_set_user_state(uid, 'deposit_amount', amount)
    db.db_set_user_state(uid, 'active_match_context', context)

    pay_msg = (
        "💳 *Add Money*\n\n"
        f"Amount: *₹{amount}*\n"
        f"UPI: `{PAYMENT_UPI}`\n\n"
        "👉 *After payment:*\n"
        "Send **UTR number** ya **Screenshot** isi chat mein bhein.\n\n"
        "⚡ *Fast verification*"
    )

    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(types.InlineKeyboardButton(f"📋 Copy UPI ID: {PAYMENT_UPI}", callback_data=f"copy_upi_{PAYMENT_UPI}"))
    
    # Wallet Check
    balance = db.db_get_wallet_balance(uid)
    if balance >= amount and match_id != "wallet":
        markup.add(types.InlineKeyboardButton(f"💳 PAY FROM WALLET (₹{balance})", callback_data=f"wallet_pay_{match_id}_{team_num}_{amount}"))
    
    markup.add(types.InlineKeyboardButton("📤 UPLOAD SCREENSHOT", callback_data="ready_screenshot"))
    markup.add(types.InlineKeyboardButton("❌ CANCEL", callback_data="payment_cancel"))

    bot.send_message(chat_id, pay_msg, reply_markup=markup, parse_mode='Markdown')

@bot.callback_query_handler(func=lambda call: call.data.startswith("wallet_pay_"))
def callback_wallet_pay(call):
    uid = str(call.from_user.id)
    parts = call.data.split("_")
    match_id, team_num, amount = parts[2], parts[3], int(parts[4])
    
    try:
        with db.get_db() as conn:
            balance = db.db_get_wallet_balance(uid)
            if balance < amount:
                bot.answer_callback_query(call.id, "❌ Insufficient Balance!", show_alert=True)
                return
            
            # Debit Transaction in Ledger
            ref = f"DEBIT_MATCH_{match_id}_{team_num}_{int(time.time())}"
            conn.execute(
                "INSERT INTO LEDGER (user_id, amount, type, reference_id, timestamp) VALUES (%s, %s, 'DEBIT', %s, %s)",
                (uid, -amount, ref, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            )
            conn.execute("UPDATE TEAMS SET is_paid=1 WHERE user_id=%s AND match_id=%s AND team_num=%s", (uid, match_id, team_num))
        
        bot.edit_message_caption(caption=f"✅ *Success!*\n₹{amount} deducted from wallet.\nTeam {team_num} is now active for {MATCHES[match_id]['name']}.", chat_id=call.message.chat.id, message_id=call.message.message_id, parse_mode='Markdown')
        bot.answer_callback_query(call.id, "Match Joined!")
    except Exception as e:
        logging.error(f"Wallet pay error: {e}")
        bot.answer_callback_query(call.id, "Error processing wallet payment.")

@bot.callback_query_handler(func=lambda call: call.data == "ready_screenshot")
def callback_ready_screenshot(call):
    bot.answer_callback_query(call.id)
    bot.send_message(call.message.chat.id,
        "📸 *Screenshot भेजो!*\n\nफोटो या document भेज सकते हो\n\n👇 नीचे भेजो:",
        parse_mode='Markdown')

@bot.message_handler(content_types=['photo', 'document'])
def handle_screenshot(msg):
    uid = str(msg.from_user.id)
    user = db.db_get_user(uid)
    
    if not user:
        bot.reply_to(msg, "❌ Account नहीं है!")
        return

    # Check database for pending intent instead of relying on in-memory dictionary
    # This prevents errors if the bot restarts while a user is making a payment
    with db.get_db() as conn:
        conn.execute(
            "SELECT * FROM PAYMENT_INTENTS WHERE user_id=%s AND status='pending' AND expires_at > %s ORDER BY id DESC LIMIT 1",
            (uid, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        )
        intent = conn.fetchone()

    if not intent:
        bot.reply_to(msg, "❌ *No Active Payment Request:* Pehle 'ADD MONEY' ya contest join button dabayein.")
        return

    active_context = intent['match_context']
    parts = active_context.split("_")
    match_id, team_num = parts[0], parts[1]
    amount = intent['amount']

    if match_id != "wallet":
        team = db_get_team(uid, match_id, int(team_num)) or {}
    else:
        team = None
    
    file_id = msg.photo[-1].file_id if msg.content_type == 'photo' else msg.document.file_id
    file_type = "photo" if msg.content_type == 'photo' else "document"
    
    # Anti-Scam: Check if this exact file was already submitted
    with db.get_db() as conn:
        conn.execute("SELECT id FROM PAYMENTS WHERE upi_txn_id=%s", (file_id,))
        duplicate_file = conn.fetchone()
        if duplicate_file:
            bot.reply_to(msg, "❌ Yeh screenshot pehle hi submit kiya ja chuka hai! Scam karne ki koshish na karein.")
            logging.warning(f"Fraud Alert: User {uid} tried to resubmit same file {file_id}")
            return

    try:
        with db.get_db() as conn:
            conn.execute(
                "INSERT INTO PAYMENTS (user_id, amount, match_id, upi_txn_id, timestamp, status) VALUES (%s, %s, %s, %s, %s, 'pending')",
                (uid, amount, match_id, file_id, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            )
    except Exception as e:
        logging.error(f"Error saving screenshot info: {e}")
    
    bot.send_message(msg.chat.id,
        "✅ *Screenshot Received!*\n\n⏳ Admin verify करेगा 5-10 minutes में",
        parse_mode='Markdown')
    
    team_info = ""
    if team:
        for role in ['bat', 'wk', 'ar', 'bowl']:
            players = team.get(role, [])
            if players:
                team_info += f"\n{ROLE_NAMES[role]}: {', '.join(players)}"
    
    if match_id == "wallet":
        caption = f"💰 WALLET DEPOSIT\n👤 User: {user['first_name']}\n🆔 ID: {uid}\n💰 ₹{amount}"
    else:
        match_name = MATCHES.get(match_id, {}).get('name', match_id)
        caption = f"👤 User: {user['first_name']}\n🆔 ID: {uid}\n🏏 Match: {match_name}\n⚾ Team: {team_num}\n💰 ₹{amount}{team_info}"

    # 📜 Fetch Last 3 Payment History to help Admin spot scammers
    history = db.db_get_user_payment_history(uid, limit=3)
    history_text = "\n\n📋 *RECENT PAYMENTS (History):*"
    if history:
        for h in history:
            status_icon = "✅" if h['status'] == 'completed' else "❌" if h['status'] == 'rejected' else "⏳"
            history_text += f"\n{status_icon} ₹{h['amount']} | {h['timestamp'][5:16]}"
    else:
        history_text += "\nNo previous records."
    
    caption += history_text
    
    admin_markup = types.InlineKeyboardMarkup(row_width=3)
    admin_markup.add(
        types.InlineKeyboardButton("✅ APPROVE", callback_data=f"approve_{uid}_{match_id}_{team_num}"),
        types.InlineKeyboardButton("❌ REJECT", callback_data=f"reject_{uid}"),
        types.InlineKeyboardButton("🚩 RED FLAG", callback_data=f"adm_flag_manual_{uid}")
    )

    pay_chan = get_payment_channel()
    if file_type == "photo":
        bot.send_photo(pay_chan, file_id, caption=caption, reply_markup=admin_markup)
    else:
        bot.send_document(pay_chan, file_id, caption=caption, reply_markup=admin_markup)


@bot.message_handler(func=lambda m: m.text and len(m.text) == 12 and m.text.isdigit())
def handle_utr_input(msg):
    """Auto-Verification Engine for UTR numbers"""
    uid = str(msg.from_user.id)
    utr = msg.text.strip()

    # 1. Check if user is already blocked/flagged
    user = db.db_get_user(uid)
    if user and user['is_flagged']:
        markup = types.InlineKeyboardMarkup()
        s_handle = get_support_handle()
        markup.add(types.InlineKeyboardButton("📞 CONTACT SUPPORT", url=f"https://t.me/{s_handle.replace('@', '')}"))
        bot.reply_to(msg, "⚠️ *ACCOUNT UNDER REVIEW*\n\nAapke account par sandigdh activity payi gayi hai. Admin verify kar raha hai. Agar aapko lagta hai yeh galti hai, toh support se baat karein.", reply_markup=markup, parse_mode='Markdown')
        return

    with db.get_db() as conn:
        # 2. Duplicate Check (Anti-Scam)
        conn.execute("SELECT user_id FROM USED_UTR WHERE utr=%s", (utr,))
        used = conn.fetchone()
        if used:
            bot.reply_to(msg, "❌ *FRAUD DETECTED:* Yeh UTR pehle hi istemal ho chuka hai! Aapka attempt record kar liya gaya hai.", parse_mode='Markdown')
            db.db_log_failed_utr(uid, utr)
            
            failed_count = db.db_get_failed_utr_count(uid)
            if failed_count >= 3:
                db.db_flag_user(uid)
                bot.send_message(get_payment_channel(), f"🚩 *RED FLAG:* User {uid} ne 3+ duplicate UTR bheje hain. Account flag kar diya gaya hai.")
            return

        # 3. Active Order Intent Check
        # Agar user ne 'Add Money' nahi dabaya aur seedha UTR bhej raha hai
        conn.execute(
            "SELECT * FROM PAYMENT_INTENTS WHERE user_id=%s AND status='pending' AND expires_at > %s ORDER BY id DESC LIMIT 1",
            (uid, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        )
        intent = conn.fetchone()

        if not intent:
            # Log as failed attempt for "random" UTR input
            db.db_log_failed_utr(uid, utr)
            failed_count = db.db_get_failed_utr_count(uid)
            
            if failed_count >= 3:
                db.db_flag_user(uid)
                bot.reply_to(msg, "⚠️ *RED FLAG:* Baar-baar galat UTR bhejne ke karan aapka account review mein daal diya gaya hai.")
                bot.send_message(get_payment_channel(), f"🚩 *RED FLAG:* User {uid} ko random UTR spamming ke liye flag kiya gaya hai.")
            else:
                pending_amt = db.db_get_user_state(uid, 'deposit_amount') or "required"
                bot.reply_to(msg, f"❌ *No Active Order:* Pehle 'ADD MONEY' par click karein aur ₹{pending_amt} pay karein, uske baad UTR bhein.\n\n⚠️ Caution: Galat UTR par ID block ho sakti hai. (Attempts left: {3-failed_count})")
            return

        # 4. Success Flow
        try:
            user_info = db.db_get_user(uid) or {"first_name": "User"}
            # Save to used_utr first (Primary Key will block concurrent duplicates)
            conn.execute("INSERT INTO USED_UTR (utr, user_id, amount, timestamp) VALUES (%s, %s, %s, %s)",
                         (utr, uid, intent['amount'], datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            
            # Update Ledger & Activation
            success, res_msg = process_payment_success(uid, intent['amount'], f"UTR_{utr}", intent['match_context'], conn=conn)
            
            if success:
                now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                conn.execute("UPDATE PAYMENT_INTENTS SET status='completed' WHERE id=%s", (intent['id'],))
                bot.reply_to(msg, f"✅ *UTR VERIFIED!*\n\n₹{intent['amount']} added to wallet.\nRef: {utr}", parse_mode='Markdown')
                
                # 📊 Sync to Google Sheets (Auto-completed)
                sheets.sync_wrapper({
                    "user_id": uid,
                    "amount": intent['amount'],
                    "upi_txn_id": utr,
                    "timestamp": now_ts,
                    "status": "completed"
                }, "PAYMENTS")
                
                # 🔔 Admin Notification for Auto-Payment with History
                history = db.db_get_user_payment_history(uid, limit=3)
                hist_text = "\n".join([f"{'✅' if h['status']=='completed' else '❌'} ₹{h['amount']} ({h['timestamp'][5:16]})" for h in history])
                
                admin_markup = types.InlineKeyboardMarkup()
                admin_markup.add(
                    types.InlineKeyboardButton("🚩 FAKE / REVERT & RED FLAG", callback_data=f"adm_revert_{uid}_{utr}_{intent['amount']}")
                )

                admin_alert = (
                    f"💰 *AUTO-PAYMENT SUCCESS (UTR)*\n\n👤 User: {user_info['first_name']}\n🆔 ID: `{uid}`\n"
                    f"💵 Amount: ₹{intent['amount']}\n🔢 UTR: `{utr}`\n\n⚠️ *Action:* Check Bank Statement. If fake, click button below to ban.\n\n📜 *Last 3 Payments:*\n{hist_text if history else 'No records'}"
                )
                bot.send_message(get_payment_channel(), admin_alert, reply_markup=admin_markup, parse_mode='Markdown')
            else:
                bot.reply_to(msg, f"❌ Error: {res_msg}")
        except Exception:
            bot.reply_to(msg, "❌ Duplicate UTR detected.")

@bot.callback_query_handler(func=lambda call: call.data.startswith("approve_"))
def callback_approve(call):
    parts = call.data.split("_")
    uid, mid, tnum = parts[1], parts[2], parts[3]
    bot.answer_callback_query(call.id)

    with db.get_db() as conn:
        conn.execute("SELECT amount, id, upi_txn_id, timestamp FROM PAYMENTS WHERE user_id=%s AND match_id=%s AND status='pending' ORDER BY timestamp DESC", (uid, mid))
        pay_row = conn.fetchone()
        if not pay_row:
            bot.edit_message_caption(caption="❌ No pending request found.", chat_id=call.message.chat.id, message_id=call.message.message_id)
            return

        amount = pay_row['amount']
        ref = f"MANUAL_{pay_row['id']}_{int(time.time())}"
        
        success, _ = process_payment_success(uid, amount, ref, f"{mid}_{tnum}", conn=conn)
        if success:
            conn.execute("UPDATE PAYMENTS SET status='completed' WHERE id=%s", (pay_row['id'],))
            
            # 📊 Sync Update to Google Sheets
            sheets.sync_wrapper({
                "user_id": uid,
                "amount": amount,
                "upi_txn_id": pay_row['upi_txn_id'],
                "timestamp": pay_row['timestamp'],
                "status": "completed"
            }, "PAYMENTS")
            
            bot.send_message(uid, f"🎉 *PAYMENT APPROVED!*\n₹{amount} credited to ledger.", parse_mode='Markdown')
            bot.edit_message_caption(caption=f"✅ APPROVED (₹{amount})", chat_id=call.message.chat.id, message_id=call.message.message_id)

@bot.callback_query_handler(func=lambda call: call.data.startswith("reject_"))
def callback_reject(call):
    uid = call.data.split("_")[1]
    bot.answer_callback_query(call.id)
    
    try:
        with db.get_db() as conn:
            # Fetch info before update for sheet sync
            conn.execute("SELECT amount, upi_txn_id, timestamp FROM PAYMENTS WHERE user_id=%s AND status='pending' ORDER BY timestamp DESC LIMIT 1", (uid,))
            pay_row = conn.fetchone()
            
            conn.execute("UPDATE PAYMENTS SET status='rejected' WHERE user_id=%s AND status='pending'", (uid,))
        
        user = db.db_get_user(uid)
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("💳 TRY AGAIN", callback_data="init_deposit"))

        if pay_row:
            # 📊 Sync Update to Google Sheets
            sheets.sync_wrapper({
                "user_id": uid,
                "amount": pay_row['amount'],
                "upi_txn_id": pay_row['upi_txn_id'],
                "timestamp": pay_row['timestamp'],
                "status": "rejected"
            }, "PAYMENTS")

        warning_msg = "⚠️ *PAYMENT REJECTED & WARNING*\n\nAapka payment reject kar diya gaya hai. Kripya sahi screenshot aur details bhein. Baar-baar galat details bhejne par aapka account restrict kiya ja sakta hai."
        bot.send_message(uid, warning_msg, reply_markup=markup, parse_mode='Markdown')
        bot.edit_message_caption(f"❌ REJECTED & WARNED\nUser: {user['first_name']}", call.message.chat.id, call.message.message_id)
    except Exception as e:
        logging.error(f"Error in callback_reject: {e}")

@bot.callback_query_handler(func=lambda call: call.data.startswith("adm_act_unflag_"))
def callback_unflag_user(call):
    """Admin can unflag a user to allow transactions again"""
    if str(call.from_user.id) != ADMIN_ID: return
    uid = call.data.split("_")[3]
    try:
        db.db_flag_user(uid, status=0)
        bot.answer_callback_query(call.id, "User account cleared!", show_alert=True)
        bot.send_message(uid, "✅ *ACCOUNT VERIFIED*\n\nAapka account verify ho gaya hai. Ab aap transactions aur contests join kar sakte hain.", parse_mode='Markdown')
        
        # Update Admin UI
        markup, text = ui.admin_fraud_render(db.get_fraud_list())
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='Markdown')
    except Exception as e:
        logging.error(f"Unflag error: {e}")

@bot.callback_query_handler(func=lambda call: call.data.startswith("adm_act_block_"))
def callback_block_user(call):
    """Admin can permanently flag a user and warn them"""
    if str(call.from_user.id) != str(ADMIN_ID): return
    uid = call.data.split("_")[3]
    try:
        db.db_flag_user(uid, status=1)
        bot.answer_callback_query(call.id, "User Blocked/Flagged!", show_alert=True)
        bot.send_message(uid, "🚫 *ACCESS RESTRICTED*\n\nAapke account par sandigdh activity ke karan transactions rok di gayi hain.", parse_mode='Markdown')
        
        markup, text = ui.admin_fraud_render(db.get_fraud_list())
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='Markdown')
    except Exception as e:
        logging.error(f"Block error: {e}")

@bot.callback_query_handler(func=lambda call: call.data.startswith("adm_revert_"))
def callback_revert_fake(call):
    """Admin can revert a fake UTR payment and ban the user instantly"""
    if str(call.from_user.id) != ADMIN_ID: return
    
    parts = call.data.split("_")
    uid, utr, amount = parts[2], parts[3], float(parts[4])
    
    try:
        with db.get_db() as conn:
            # 1. Flag the user as fraud
            conn.execute("UPDATE USERS SET is_flagged = 1 WHERE user_id = %s", (uid,))
            
            # 2. Debit the balance (Reverse the credit)
            ref_id = f"REVERT_{utr}"
            # Check if already reverted
            conn.execute("SELECT id FROM LEDGER WHERE reference_id=%s", (ref_id,))
            exists = conn.fetchone()
            if not exists:
                conn.execute(
                    "INSERT INTO LEDGER (user_id, amount, type, reference_id, timestamp) VALUES (%s, %s, 'DEBIT', %s, %s)",
                    (uid, -amount, ref_id, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                )
            
            # 3. If it was for a match, mark team as unpaid
            conn.execute("UPDATE TEAMS SET is_paid = 0 WHERE user_id = %s AND is_paid = 1", (uid,))
            
        bot.answer_callback_query(call.id, "User Banned & Balance Reverted!", show_alert=True)
        bot.edit_message_caption(
            caption=call.message.caption + "\n\n🚩 *ACTION: REVERTED & RED FLAGGED*",
            chat_id=get_payment_channel(),
            message_id=call.message.message_id
        )
        bot.send_message(uid, "⚠️ *ACCOUNT RED FLAGGED*\n\nAapka fake payment UTR record kiya gaya hai. Balance revert kar diya gaya hai aur account review mein hai.", parse_mode='Markdown')
        
    except Exception as e:
        logging.error(f"Revert error: {e}")
        bot.answer_callback_query(call.id, "Error during revert.")

@bot.callback_query_handler(func=lambda call: call.data.startswith("adm_flag_manual_"))
def callback_red_flag_manual(call):
    if str(call.from_user.id) != ADMIN_ID: return
    uid = call.data.split("_")[3]
    try:
        db.db_flag_user(uid)
        bot.answer_callback_query(call.id, "User Red Flagged!", show_alert=True)
        bot.edit_message_caption(
            caption=call.message.caption + "\n\n🚩 *STATUS: RED FLAGGED BY ADMIN*",
            chat_id=get_payment_channel(),
            message_id=call.message.message_id
        )
        bot.send_message(uid, "⚠️ *ACCOUNT UNDER REVIEW*\n\nAapke transactions mein gadbadi payi gayi hai. Aapka account review mein daal diya gaya hai. Support se sampark karein.", parse_mode='Markdown')
        # Clear any pending payments for this user
        with db.get_db() as conn:
            conn.execute("UPDATE PAYMENTS SET status='rejected' WHERE user_id=%s AND status='pending'", (uid,))
    except Exception as e:
        logging.error(f"Manual flag error: {e}")

@bot.callback_query_handler(func=lambda call: call.data == "payment_cancel")
def callback_payment_cancel(call):
    bot.answer_callback_query(call.id)
    bot.edit_message_text("❌ Cancelled", call.message.chat.id, call.message.message_id)

# ===================================================
# MENU
# ===================================================

@bot.message_handler(commands=['leaderboard'])
@bot.message_handler(func=lambda m: m.text and "LEADERBOARD" in m.text)
def cmd_leaderboard(msg):
    rows = get_leaderboard(10)
    text = "🏆 *TOP 10*\n\n"
    
    if not rows:
        text += "No scores yet!"
    else:
        for i, row in enumerate(rows, 1):
            username = row['username'] or row['first_name']
            points = row['points'] or 0
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"#{i}"
            text += f"{medal} @{username} - {points}\n"
    
    bot.send_message(msg.chat.id, text, parse_mode='Markdown')

@bot.message_handler(commands=['wallet'])
@bot.message_handler(func=lambda m: m.text and "WALLET" in m.text)
def cmd_wallet(msg):
    uid = str(msg.from_user.id)
    user = db.db_get_user(uid)
    
    if not user:
        bot.send_message(msg.chat.id, "❌ No account!")
        return
    
    balance = db.db_get_wallet_balance(uid)

    # 🔗 Referral Link Generation
    bot_info = bot.get_me()
    ref_link = f"https://t.me/{bot_info.username}?start=ref{uid}"

    text = (
        "💰 *Wallet*\n\n"
        f"Balance: *₹{balance}*\n\n"
        "➕ Add Money\n"
        "💸 Withdraw\n"
        "🎁 Refer & Earn\n\n"
        f"⚠️ Min Withdraw: ₹{MIN_WITHDRAWAL}\n\n"
        "👉 *Next: Add Money to join paid contests*"
    )

    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("➕ Add Money", callback_data="init_deposit"),
        types.InlineKeyboardButton("💸 Withdraw", callback_data="req_withdraw"),
        types.InlineKeyboardButton("🎁 Refer & Earn", switch_inline_query=f"Join & Win: {ref_link}"),
        types.InlineKeyboardButton("🏠 Home", callback_data="app_home")
    )
    
    bot.send_message(msg.chat.id, text, reply_markup=markup, parse_mode='Markdown')

@bot.callback_query_handler(func=lambda call: call.data == "init_deposit")
def callback_init_deposit(call):
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("❌ CANCEL PAYMENT", callback_data="payment_cancel"))
    sent = bot.send_message(call.message.chat.id, "💰 *Enter amount* to add to your wallet:\n(Min: ₹10, Max: ₹50,000)", reply_markup=markup, parse_mode='Markdown')
    bot.register_next_step_handler(sent, process_deposit_input)

# ADD 1: Transaction History
@bot.callback_query_handler(func=lambda call: call.data == "show_history")
@bot.message_handler(commands=['history'])
def cmd_history(msg_or_call):
    is_cb = isinstance(msg_or_call, telebot.types.CallbackQuery)
    msg = msg_or_call.message if is_cb else msg_or_call
    uid = str(msg_or_call.from_user.id)
    
    history = db.db_get_transaction_history(uid)
    if not history:
        text = "📜 *Transaction History*\n\nAapne abhi tak koi transaction nahi ki hai."
    else:
        text = "📜 *Last 10 Transactions*\n\n"
        for item in history:
            sign = "✅ +" if item['type'] == 'CREDIT' else "❌ -"
            date = item['timestamp'][5:16] # Format: 04-24 19:30
            text += f"`{date}` | {sign}₹{abs(item['amount'])} | {item['reference_id'][:12]}...\n"
    
    if is_cb: bot.answer_callback_query(msg_or_call.id)
    bot.send_message(msg.chat.id, text, parse_mode='Markdown')

# ADD 3: Referral Dashboard
@bot.message_handler(commands=['myreferrals'])
def cmd_my_referrals(msg):
    uid = str(msg.from_user.id)
    stats = db.db_get_referral_stats(uid)
    text = (
        "🎁 *Referral Dashboard*\n\n"
        f"👥 Total Referrals: `{stats['total']}`\n"
        f"💰 Total Bonus Earned: `₹{stats['bonus']}`\n\n"
        "Aapka referral bonus tabhi credit hoga jab aapka referral pehla contest join karega!"
    )
    bot.send_message(msg.chat.id, text, parse_mode='Markdown')

def process_deposit_input(msg):
    uid = str(msg.from_user.id)
    if not msg.text.isdigit():
        bot.send_message(msg.chat.id, "❌ Invalid amount! Please enter numbers only.")
        return
    
    amount = int(msg.text)
    if amount < 10:
        bot.send_message(msg.chat.id, "❌ Minimum deposit ₹10.")
        return
        
    bot.send_message(msg.chat.id, f"✅ *Amount Confirmed:* ₹{amount}")
    send_payment_ui(msg.chat.id, uid, amount, "wallet", "0")

@bot.message_handler(commands=['withdraw'])
def cmd_withdraw(msg):
    uid = str(msg.from_user.id)
    user = db.db_get_user(uid)
    if not user:
        bot.reply_to(msg, "❌ Account nahi mila!")
        return
    balance = db.db_get_wallet_balance(uid)
    if balance < MIN_WITHDRAWAL:
        bot.reply_to(msg, f"❌ Minimum ₹{MIN_WITHDRAWAL} hone chahiye!")
        return
    
    text = (
        "🏧 *WITHDRAWAL REQUEST*\n\n"
        f"💰 Aapka Balance: ₹{balance}\n"
        f"⚠️ Minimum Withdrawal: ₹{MIN_WITHDRAWAL}\n\n"
        "Niche diye gaye format mein apni UPI ID aur Amount bhejien:\n"
        "`UPI_ID AMOUNT`\n\n"
        "Example: `binod@oksbi 500`"
    )
    sent_msg = bot.send_message(msg.chat.id, text, parse_mode='Markdown')
    bot.register_next_step_handler(sent_msg, process_withdrawal_details)

def process_withdrawal_details(msg):
    uid = str(msg.from_user.id)
    try:
        # SEC 2: Sanitize and parse
        clean_text = sanitize_input(msg.text, 100)
        parts = clean_text.split()
        if len(parts) < 2:
            bot.reply_to(msg, "❌ Format galat hai! Dubara `/withdraw` dabayein aur is tarah bhejien:\n`UPI_ID AMOUNT`", parse_mode='Markdown')
            return
        
        upi_id, amount = parts[0], float(parts[1])
        balance = db.db_get_wallet_balance(uid)

        if amount < MIN_WITHDRAWAL:
            bot.reply_to(msg, f"❌ Minimum withdrawal ₹{MIN_WITHDRAWAL} hai.")
            return
        if amount > balance:
            bot.reply_to(msg, f"❌ Insufficient balance! Aapke paas ₹{balance} hain.")
            return

        with db.get_db() as conn:
            conn.execute(
                "INSERT INTO WITHDRAWALS (user_id, amount, upi_id, timestamp) VALUES (%s, %s, %s, %s) RETURNING id",
                (uid, amount, upi_id, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            )
            req_id = conn.fetchone()['id']

        # 📊 Sync to Google Sheets (Withdrawal Request)
        sheets.sync_wrapper({
            "user_id": uid,
            "amount": amount,
            "upi_id": upi_id,
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "status": "pending"
        }, "WITHDRAWALS")

        bot.reply_to(msg, f"✅ *Request Submitted!*\n💰 Amount: ₹{amount}\n🏦 UPI: `{upi_id}`\n\nAdmin 10-15 minute mein verify karke paise bhej dega aur payment ka screenshot yahi share karega.")

        # Admin notification with buttons
        admin_markup = types.InlineKeyboardMarkup()
        admin_markup.add(
            types.InlineKeyboardButton("✅ APPROVE", callback_data=f"wd_approve_{req_id}"),
            types.InlineKeyboardButton("❌ REJECT", callback_data=f"wd_reject_{req_id}"),
            types.InlineKeyboardButton("➡️ SENT", callback_data=f"wd_sent_{req_id}")
        )
        bot.send_message(get_payment_channel(), f"🔔 *NEW WITHDRAWAL*\nUser: {msg.from_user.first_name}\nID: `{uid}`\nAmt: ₹{amount}\nUPI: `{upi_id}`", reply_markup=admin_markup, parse_mode='Markdown')
    except Exception as e:
        logging.error(f"Withdrawal input error: {e}")
        bot.reply_to(msg, "❌ Error! Please enter amount correctly (e.g., binod@oksbi 500).")

@bot.callback_query_handler(func=lambda call: call.data.startswith("wd_"))
def callback_withdrawal_admin(call):
    if str(call.from_user.id) != ADMIN_ID: return
    parts = call.data.split("_")
    action, req_id = parts[1], parts[2]
    
    with db.get_db() as conn:
        conn.execute("SELECT * FROM WITHDRAWALS WHERE id=%s", (req_id,))
        req = conn.fetchone()
        if not req or req['status'] not in ['pending', 'processing']: 
            bot.answer_callback_query(call.id, "Already processed!")
            return

        if action == "approve":
            # ADD 4: Stage -> Processing
            db.db_update_withdrawal_status(req_id, 'processing')
            bot.send_message(req['user_id'], "⏳ *Withdrawal Processing!*\n\nAapki request verify ho gayi hai, paise bhein ja rahe hain.")
            bot.edit_message_text(f"⏳ Processing: ₹{req['amount']} to {req['user_id']}", get_payment_channel(), call.message.message_id)
        
        elif action == "sent":
            # ADD 4: Final Stage -> Sent
            ref = f"WD_REF_{req_id}"
            conn.execute("INSERT INTO LEDGER (user_id, amount, type, reference_id, timestamp) VALUES (%s, %s, 'DEBIT', %s, %s)",
                         (req['user_id'], -req['amount'], ref, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            db.db_update_withdrawal_status(req_id, 'sent')
            bot.edit_message_text(f"✅ Sent: ₹{req['amount']} to {req['user_id']}", get_payment_channel(), call.message.message_id)
            bot.send_message(req['user_id'], f"✅ *Sent to UPI!*\n₹{req['amount']} aapke account mein credit kar diye gaye hain.")
        else:
            db.db_update_withdrawal_status(req_id, 'rejected')
            bot.send_message(req['user_id'], f"❌ *Withdrawal Rejected*\nAapki ₹{req['amount']} ki request cancel kar di gayi hai.")
            bot.edit_message_text(f"❌ Rejected: ₹{req['amount']} for {req['user_id']}", get_payment_channel(), call.message.message_id)
    bot.answer_callback_query(call.id)

@bot.message_handler(commands=['stats'])
@bot.message_handler(func=lambda m: m.text and "STATS" in m.text)
def cmd_stats(msg):
    uid = str(msg.from_user.id)
    user = db.db_get_user(uid)
    
    # Dynamically find the first match the user has a team in
    team = None
    for mid in MATCHES.keys():
        team = db.db_get_team_internal(uid, mid, 1)
        if team: break
    
    if not user:
        bot.send_message(msg.chat.id, "❌ No account!")
        return
    
    points = team.get('points', 0) if team else 0
    team_count = get_total_players(team) if team else 0
    text = f"📊 *STATS*\n\n👤 {user['first_name']}\n⭐ Points: {points}\n🎯 Team: {team_count}/11"
    
    if team_count == 0:
        text += "\n\n🚀 *Abhi tak koi team nahi hai!* \nJeetne ke liye apni pehli team banayein: /myteam"

    bot.send_message(msg.chat.id, text, parse_mode='Markdown')

@bot.message_handler(commands=['help'])
@bot.message_handler(func=lambda m: m.text and "HELP" in m.text)
def cmd_help(msg):
    # Clean handles properly for link generation
    s_handle = get_support_handle()
    c_handle = get_channel_handle().replace('@', '').replace('\\', '').strip()
    
    support_url = f"https://t.me/{s_handle}"
    channel_url = f"https://t.me/{c_handle}"

    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("🏏 Create Team", callback_data="cmd_my_team_nav"),
        types.InlineKeyboardButton("🏆 Contests", callback_data="contest_list")
    )
    markup.add(
        types.InlineKeyboardButton("💰 Wallet", callback_data="app_wallet"),
        types.InlineKeyboardButton("📊 My Rank", callback_data="app_myrank")
    )
    markup.add(
        types.InlineKeyboardButton("🎫 Support Ticket", callback_data="start_support"),
        types.InlineKeyboardButton("📢 Main Channel", url=channel_url)
    )
    
    if is_admin(msg.from_user.id):
        markup.add(types.InlineKeyboardButton("🛠 ADMIN CONTROL GUIDE", callback_data="adm_nav_help"))
    
    help_text = f"""
❓ <b>BOT FEATURES & COMMANDS</b>
━━━━━━━━━━━━━━━━━━━━
⚾ <b>Team Management:</b> /myteam - Team banayein aur edit karein.
🏆 <b>Join Matches:</b> /contest - Available contests mein join karein.
💰 <b>Wallet:</b> /wallet - Balance, Deposit, aur Withdrawal (/withdraw) manage karein.
📊 <b>Live Rank:</b> /myrank - Live match ke waqt apna rank dekhein.
📈 <b>User Stats:</b> /stats - Apni total performance dekhein.
🎁 <b>Referrals:</b> /myreferrals - Apne referral bonus ki details dekhein.
📜 <b>History:</b> /history - Apni transaction history dekhein.
🏆 <b>Leaderboard:</b> /leaderboard - Top 10 users dekhein.
⚖️ <b>Rules:</b> /rules - Scoring system samjhein.
🎫 <b>Support:</b> /support - Kisi bhi problem ke liye ticket banayein.
🚀 <b>Tour:</b> /start - Bot ka intro tour dobara dekhne ke liye.

📞 <b>Official Support:</b> @{s_handle}
"""
    bot.send_message(msg.chat.id, help_text, reply_markup=markup, parse_mode='HTML', disable_web_page_preview=True)

@bot.message_handler(commands=['set_handle'])
def cmd_set_handle(msg):
    if str(msg.from_user.id) != ADMIN_ID: return

    if "|" in msg.text:
        msg.text = re.sub(r'^/\w+\s*', '', msg.text)
        process_handle_setting(msg)
        return

    help_msg = (
        "🛠 *ADMIN: SET SYSTEM HANDLES*\n\n"
        "Format: `TYPE | VALUE` \n"
        "Types: `SUPPORT`, `CHANNEL`, `PAYMENT_ID` \n\n"
        "✅ Example: `SUPPORT | crick_support_help`"
    )
    sent = bot.send_message(msg.chat.id, help_msg, parse_mode='Markdown')
    bot.register_next_step_handler(sent, process_handle_setting)

@bot.message_handler(commands=['support'])
def cmd_support(msg):
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("❌ CANCEL", callback_data="support_cancel"))
    sent = bot.send_message(msg.chat.id,
        "🎫 *SUPPORT TICKET*\n\n"
        "Apni problem likhkar bhejein:\n"
        "(Payment issue, Team issue, kuch bhi)\n\n"
        "👇 Niche type karo:",
        reply_markup=markup, parse_mode='Markdown')
    bot.register_next_step_handler(sent, process_support_ticket)

def process_support_ticket(msg):
    uid = str(msg.from_user.id)
    issue = sanitize_input(msg.text, max_len=500)
    ticket_id = db.db_create_ticket(uid, issue)
    
    bot.reply_to(msg,
        f"✅ *Ticket #{ticket_id} Created!*\n\n"
        f"Admin jald hi reply karega.\n"
        f"Ticket ID: <b>#{ticket_id}</b>",
        parse_mode='HTML')
    
    # Admin ko notify karo with resolve button
    admin_markup = types.InlineKeyboardMarkup()
    admin_markup.add(
        types.InlineKeyboardButton("💬 REPLY", callback_data=f"ticket_reply_{ticket_id}_{uid}"),
        types.InlineKeyboardButton("✅ RESOLVE", callback_data=f"ticket_resolve_{ticket_id}_{uid}")
    )
    user = db.db_get_user(uid)
    support_chan = get_support_channel()
    bot.send_message(support_chan,
        f"🎫 <b>NEW SUPPORT TICKET #{ticket_id}</b>\n\n"
        f"👤 User: {html.escape(user['first_name'])}\n"
        f"🆔 ID: <code>{uid}</code>\n\n"
        f"📝 Issue:\n{html.escape(issue)}",
        reply_markup=admin_markup, parse_mode='HTML')

@bot.callback_query_handler(func=lambda call: call.data.startswith("ticket_reply_"))
def callback_ticket_reply(call):
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "🚫 Sirf Main Admin reply kar sakta hai!", show_alert=True)
        return
        
    parts = call.data.split("_")
    ticket_id, user_id = parts[2], parts[3]
    orig_text = call.message.text # Original ticket ka text save kar lo
    chat_id = str(call.message.chat.id)
    
    try:
        # 1. Pehle loading khatam karein
        bot.answer_callback_query(call.id, "Type your reply now!")

        # 2. State set karein (Database for persistence if bot sleeps/restarts)
        state_data = json.dumps([ticket_id, user_id, call.message.message_id, orig_text])
        db.db_set_user_state(chat_id, 'PENDING_TICKET_REPLY', state_data)
        
        bot.send_message(chat_id,
            f"💬 <b>Ticket #{ticket_id}</b> ka reply likho:\n\n"
            f"Agla message jo aap bhein ge, wo seedha user ko chala jayega.",
            parse_mode='HTML')
    except Exception as e:
        logging.error(f"Reply Trigger Error: {e}")
        bot.answer_callback_query(call.id, "❌ Error: Prompt nahi bhej paya.", show_alert=True)

def check_pending_reply(m):
    if not m.text or m.text.startswith('/') or m.text.startswith('💬'): return False
    state = db.db_get_user_state(str(m.chat.id), 'PENDING_TICKET_REPLY')
    return state is not None

@bot.message_handler(func=check_pending_reply)
@bot.channel_post_handler(func=check_pending_reply)
def handle_ticket_reply_intercept(msg):
    """Admin ka reply intercept karega (Loop se bachne ke liye prompt messages ko ignore karega)"""
    chat_id_str = str(msg.chat.id)
    state_raw = db.db_get_user_state(chat_id_str, 'PENDING_TICKET_REPLY')
    if state_raw:
        db.db_set_user_state(chat_id_str, 'PENDING_TICKET_REPLY', None) # Clear state
        data = json.loads(state_raw)
        ticket_id, user_id, orig_msg_id, orig_text = data
        send_ticket_reply(msg, ticket_id, user_id, orig_msg_id, orig_text, int(chat_id_str))

def send_ticket_reply(msg, ticket_id, user_id, orig_msg_id, orig_text, chat_id):
    if not msg.text:
        bot.send_message(chat_id, "❌ Reply sirf text mein ho sakta hai.")
        return

    target_user = int(str(user_id).strip())
    logging.info(f"🚀 Delivering reply for Ticket #{ticket_id} to User: {target_user}")

    safe_reply = html.escape(msg.text)
    safe_orig = html.escape(orig_text)

    try:
        # 1. User ko reply deliver karein
        bot.send_message(target_user,
            f"📩 <b>Support Reply — Ticket #{ticket_id}</b>\n\n"
            f"{safe_reply}\n\n"
            f"<i>Aur help chahiye? /support likhein</i>",
            parse_mode='HTML')
        
        bot.send_message(chat_id, f"✅ Ticket #{ticket_id} ka reply user ko bhej diya gaya hai.")
        logging.info(f"✅ Reply delivered to {target_user}")

    except Exception as e:
        logging.error(f"❌ Delivery failed: {e}")
        bot.send_message(chat_id, f"⚠️ Delivery Error: User tak message nahi gaya (Shayad block kiya hai).")

    # 2. Support Channel mein ticket update karein (Hamesha chale)
    try:
        new_markup = types.InlineKeyboardMarkup()
        new_markup.add(types.InlineKeyboardButton("✅ RESOLVE", callback_data=f"ticket_resolve_{ticket_id}_{user_id}"))
        
        status_text = f"{safe_orig}\n\n✅ <b>REPLIED:</b> <i>{safe_reply[:60]}...</i>"
        bot.edit_message_text(
            status_text,
            chat_id=chat_id,
            message_id=int(orig_msg_id),
            reply_markup=new_markup,
            parse_mode='HTML'
        )
        logging.info(f"✅ Ticket UI updated.")
    except Exception as e:
        logging.error(f"❌ UI Update Error: {e}")

@bot.callback_query_handler(func=lambda call: call.data.startswith("ticket_resolve_"))
def callback_ticket_resolve(call):
    if not is_admin(call.from_user.id): return
    parts = call.data.split("_")
    ticket_id, user_id = parts[2], parts[3]
    db.db_resolve_ticket(ticket_id)
    bot.answer_callback_query(call.id, "Ticket Resolved!", show_alert=True)
    bot.send_message(user_id,
        f"✅ *Ticket #{ticket_id} Resolved!*\n\n"
        f"Aapki problem solve ho gayi. "
        f"Aur help ke liye /support karein.",
        parse_mode='Markdown')
    bot.edit_message_text(
        call.message.text + "\n\n✅ *RESOLVED*",
        call.message.chat.id, call.message.message_id,
        parse_mode='Markdown')

@bot.callback_query_handler(func=lambda call: call.data == "support_cancel")
def callback_support_cancel(call):
    bot.answer_callback_query(call.id)
    bot.edit_message_text("❌ Cancelled", call.message.chat.id, call.message.message_id)

def process_handle_setting(msg):
    """Admin input handle karne ke liye jo Support ya Channel badalta hai"""
    try:
        if "|" not in msg.text:
            return bot.reply_to(msg, "❌ Invalid Format! Use `TYPE | VALUE`")
        parts = [p.strip() for p in msg.text.split("|")]
        # Proper Cleanup: Remove @ and backslashes in one go
        h_type = parts[0].upper()
        value = parts[1].replace("@", "").replace("\\", "").strip()
        
        if h_type == "SUPPORT":
            db.db_set_setting('SUPPORT_HANDLE', value)
            bot.reply_to(msg, f"✅ Support handle updated to: @{value}")
        elif h_type == "CHANNEL":
            db.db_set_setting('CHANNEL_HANDLE', value)
            bot.reply_to(msg, f"✅ Channel handle updated to: @{value}")
        elif h_type == "PAYMENT_ID":
            db.db_set_setting('PAYMENT_CHANNEL_ID', value)
            bot.reply_to(msg, f"✅ Payment Verification Channel ID updated to: {value}")
        elif h_type == "SUPPORT_ID":
            db.db_set_setting('SUPPORT_CHANNEL_ID', value)
            bot.reply_to(msg, f"✅ Support Ticket Channel ID updated to: {value}")
        else:
            bot.reply_to(msg, "❌ Invalid Type! Use `SUPPORT`, `CHANNEL`, `PAYMENT_ID`, or `SUPPORT_ID`.")
    except:
        bot.reply_to(msg, "❌ Error! Format: `TYPE | HANDLE`")

@bot.message_handler(commands=['clear_database'])
def cmd_clear_db(msg):
    """DANGER: Wipes all test data from the database"""
    if not is_admin(msg.from_user.id): return
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("🔥 YES, WIPE EVERYTHING", callback_data="adm_wipe_confirm"))
    markup.add(types.InlineKeyboardButton("❌ CANCEL", callback_data="app_home"))
    bot.send_message(msg.chat.id, 
        "⚠️ *DANGER ZONE*\n\nKya aap sach mein saara data delete karna chahte hain?\n\n"
        "Isse niche di gayi tables khali ho jayengi:\n"
        "• Matches & Players\n"
        "• Teams & Points\n"
        "• Payments & Ledger\n\n"
        "Yeh action revert nahi ho sakta.", 
        reply_markup=markup, parse_mode='Markdown')

@bot.callback_query_handler(func=lambda call: call.data == "adm_wipe_confirm")
def callback_wipe_confirm(call):
    if not is_admin(call.from_user.id): return
    try:
        with db.get_db() as conn:
            tables = ['MATCHES_LIST', 'PLAYERS', 'TEAMS', 'PAYMENTS', 'PAYMENT_INTENTS', 'USED_UTR', 'LEDGER', 'PLAYER_LIVE_STATS', 'MATCH_EVENTS']
            for table in tables:
                conn.execute(f"DELETE FROM {table}")
        global MATCHES, PLAYERS_CACHE
        MATCHES = {}
        PLAYERS_CACHE = {}
        bot.edit_message_text("✅ *Database Wiped Clean!* Saara test data delete ho gaya hai. Ab aap fresh start kar sakte hain.", call.message.chat.id, call.message.message_id, parse_mode='Markdown')
    except Exception as e:
        bot.edit_message_text(f"❌ Error during wipe: {e}", call.message.chat.id, call.message.message_id)

@bot.message_handler(commands=['rules'])
def cmd_rules(msg):
    """Users ko scoring system samjhane ke liye"""
    rules = f"""
📊 *SCORING*
🏏 Run: 1 | 4s: +4 | 6s: +6
⚾ Wkt: +25 | Maiden: +10
👑 Multiplier: C: 2x | VC: 1.5x
🚫 Lock: Match start hone par.
"""
    bot.send_message(msg.chat.id, rules, parse_mode='Markdown')

# ===================================================
# FEATURE 5: Live Rank During Match
# ===================================================
@bot.message_handler(commands=['myrank'])
def cmd_myrank(msg):
    # Brand new function
    uid = str(msg.from_user.id)
    
    markup = types.InlineKeyboardMarkup(row_width=1)
    for mid, info in MATCHES.items():
        if is_match_locked(mid):
            markup.add(types.InlineKeyboardButton(
                f"📊 {info['name']}", 
                callback_data=f"show_rank_{mid}"))
    
    if not markup.keyboard:
        bot.send_message(msg.chat.id,
            "⏳ *Abhi koi match live nahi hai!*\n\n"
            "Match start hone ke baad /myrank karein.",
            parse_mode='Markdown')
        return
    
    bot.send_message(msg.chat.id,
        "📊 *LIVE RANK*\nKaunse match ka rank dekhna hai?",
        reply_markup=markup, parse_mode='Markdown')

@bot.callback_query_handler(func=lambda call: call.data.startswith("show_rank_"))
def callback_show_rank(call):
    uid = str(call.from_user.id)
    mid = call.data.split("_")[2]
    
    rank_data = db.db_get_user_rank(uid, mid)
    total = db.db_get_match_participant_count(mid)
    
    # Get user's teams for this match
    teams_text = ""
    for i in range(1, 4): # Assuming max 3 teams for simplicity in display
        t = db_get_team(uid, mid, i)
        if t and t.get('is_paid'):
            pts = t.get('points', 0)
            teams_text += f"Team {i}: {pts} pts\n"
    
    if not rank_data:
        text = (
            "❌ *Is match mein join nahi kiya!*\n\n"
            "Paid contest mein hote to rank dikhta."
        )
    else:
        rank = rank_data['rank']
        medal = "🥇" if rank == 1 else "🥈" if rank == 2 else "🥉" if rank == 3 else f"#{rank}"
        text = (
            f"📊 *LIVE RANK — {MATCHES[mid]['name']}*\n\n"
            f"Aapka Rank: *{medal} / {total}*\n\n"
            f"{teams_text}\n"
            f"🔄 Points update hote rehte hain\n"
            f"_/myrank dobara karein latest rank ke liye_"
        )
    
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("🔄 Refresh", callback_data=f"show_rank_{mid}"))
    bot.edit_message_text(text, call.message.chat.id, call.message.message_id,
                         reply_markup=markup, parse_mode='Markdown')

# ===================================================
# CALLBACKS
# ===================================================

@bot.callback_query_handler(func=lambda call: call.data == "rules")
def callback_handler(call):
    cmd_rules(call.message)

@bot.callback_query_handler(func=lambda call: call.data == "app_home")
def callback_app_home(call):
    bot.answer_callback_query(call.id)
    start_command(call.message)

@bot.callback_query_handler(func=lambda call: call.data == "app_wallet")
def callback_app_wallet(call):
    bot.answer_callback_query(call.id)
    cmd_wallet(call.message)

@bot.callback_query_handler(func=lambda call: call.data == "app_myrank")
def callback_app_myrank(call):
    bot.answer_callback_query(call.id)
    cmd_myrank(call.message)

@bot.callback_query_handler(func=lambda call: call.data.startswith("show_player_stats_"))
def callback_show_player_stats(call):
    uid = str(call.from_user.id)
    match_id = call.data.split("_")[3]
    
    bot.answer_callback_query(call.id)
    
    match_name = MATCHES.get(match_id, {}).get('name', match_id)
    # This is fine as it's for viewing stats, not high-speed selection
    with db.get_db() as c:
        c.execute("SELECT player_name, runs, fours, sixes, wickets FROM PLAYER_LIVE_STATS WHERE match_id=%s", (match_id,))
        player_stats = c.fetchall()
    
    # Pass the POINT_SYSTEM from scoring module to UI for calculation
    markup, text = ui.player_stats_render(match_id, match_name, player_stats, scoring.POINT_SYSTEM)
    bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='Markdown')

def send_onboarding_step1(chat_id, first_name):
    """Step 1: Welcome + How it works"""
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("➡️ Next: Team Kaise Banate Hain", callback_data="onboard_step2"))
    
    bot.send_message(chat_id,
        f"🏏 *Welcome {first_name}! Chaliye shuru karte hain!*\n\n"
        f"*Yeh bot kya karta hai?*\n\n"
        f"1️⃣ Aap cricket players choose karte ho\n"
        f"2️⃣ Contest mein entry lete ho\n"
        f"3️⃣ Real match ke hisaab se points milte hain\n"
        f"4️⃣ Top rank pe prize milta hai! 💰\n\n"
        f"*Step 1/3 — Basics samajh liye!*",
        reply_markup=markup, parse_mode='Markdown')

def send_onboarding_step2(chat_id):
    """Step 2: Team building guide"""
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("➡️ Next: Scoring & Prizes", callback_data="onboard_step3"))
    markup.add(types.InlineKeyboardButton("⬅️ Back", callback_data="onboard_step1_back"))
    
    bot.send_message(chat_id,
        f"🧑‍💼 *Team Kaise Banate Hain?*\n\n"
        f"✅ 11 players chunte hain:\n"
        f"• 3-6 Batsmen 🏏\n"
        f"• 1-4 Wicketkeepers 🧤\n"
        f"• 1-4 All-rounders ⭐\n"
        f"• 3-6 Bowlers 🎯\n\n"
        f"👑 *Captain = 2x Points*\n"
        f"⭐ *Vice Captain = 1.5x Points*\n\n"
        f"_Sahi C/VC choose karna sabse zaroori hai!_\n\n"
        f"*Step 2/3*",
        reply_markup=markup, parse_mode='Markdown')

def send_onboarding_step3(chat_id):
    """Step 3: Prizes + CTA"""
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(types.InlineKeyboardButton("🏏 APNI PEHLI TEAM BANAO!", callback_data="cmd_my_team_nav"))
    markup.add(types.InlineKeyboardButton("🏆 CONTESTS DEKHO", callback_data="contest_list"))
    markup.add(types.InlineKeyboardButton("⬅️ Back", callback_data="onboard_step2_back"))
    
    bot.send_message(chat_id,
        f"🏆 *Prizes Kaise Milte Hain?*\n\n"
        f"🥇 Rank #1 → ₹2000\n"
        f"🥈 Rank #2 → ₹800\n"
        f"🥉 Rank #3 → ₹400\n\n"
        f"💰 *Min Withdrawal: ₹{MIN_WITHDRAWAL}*\n"
        f"⚡ *UPI pe instant payout*\n\n"
        f"🎁 *Refer & Earn:* Dost ko refer karo\n"
        f"   → Unke pehle contest pe ₹10 bonus!\n\n"
        f"*Step 3/3 — Ab shuru karo!* 🚀",
        reply_markup=markup, parse_mode='Markdown')

# Onboarding callbacks
@bot.callback_query_handler(func=lambda call: call.data == "onboard_step2")
def callback_onboard_step2(call):
    bot.answer_callback_query(call.id)
    send_onboarding_step2(call.message.chat.id)

@bot.callback_query_handler(func=lambda call: call.data == "onboard_step3")
def callback_onboard_step3(call):
    bot.answer_callback_query(call.id)
    send_onboarding_step3(call.message.chat.id)

@bot.callback_query_handler(func=lambda call: call.data == "onboard_step1_back")
def callback_onboard_back1(call):
    bot.answer_callback_query(call.id)
    send_onboarding_step1(call.message.chat.id, call.from_user.first_name)

@bot.callback_query_handler(func=lambda call: call.data == "onboard_step2_back")
def callback_onboard_back2(call):
    bot.answer_callback_query(call.id)
    send_onboarding_step2(call.message.chat.id)

def handle_selection(call):
    # FIX 4: Rate Limiting
    uid = str(call.from_user.id)
    now = time.time()
    if now - _selection_cooldown.get(uid, 0) < 0.5:
        bot.answer_callback_query(call.id, "⚡ Thoda ruko...")
        return
    _selection_cooldown[uid] = now

    parts = call.data.split("_")
    match_id, team_num, role = parts[1], int(parts[2]), parts[3]
    player_name = " ".join(parts[4:])
    cache_key = (uid, match_id, team_num)

    if is_match_locked(match_id):
        bot.answer_callback_query(call.id, "🚫 Match lock ho chuka hai!", show_alert=True)
        return

    # ⚡ CACHE UPDATE ONLY - NO DATABASE WRITE
    team = db_get_team(uid, match_id, team_num) # Hydrates cache if empty
    if not team: team = {k: [] for k in ROLES}
    
    selected = team.get(role, [])
    _, role_max = ROLE_LIMITS[role]
    total_core = get_total_players(team)
    total_subs = len(team.get('sub', []))

    if player_name in selected:
        selected.remove(player_name)
        bot.answer_callback_query(call.id, f"❌ {player_name} removed!")
    else:
        if role == 'sub':
            if total_subs >= 4:
                bot.answer_callback_query(call.id, "⚠️ Max 4 Impact Players allowed!", show_alert=True)
                return
        else:
            if total_core >= 11:
                bot.answer_callback_query(call.id, "⚠️ Playing 11 full! Extra players sirf 'Sub' category mein add karein.", show_alert=True)
                return

        if len(selected) >= role_max:
            bot.answer_callback_query(call.id, f"⚠️ {ROLE_NAMES[role]} ki limit {role_max} hai!", show_alert=True)
            return
        selected.append(player_name)
        bot.answer_callback_query(call.id, f"✅ {player_name} added!")

    # FIX 3: Persistence for selection
    temp_team_cache[cache_key] = team
    # Refresh UI instantly
    show_player_selection(call.message.chat.id, uid, role, match_id, team_num, call.message.message_id)

@bot.callback_query_handler(func=lambda call: True)
def callback_catchall(call):
    # 1. Ignore dummy buttons instantly
    if call.data == "ignore":
        bot.answer_callback_query(call.id)
        return

    import admin_app # Import at the start of handler to fix UnboundLocalError

    # Route Match Management Callbacks
    if call.data.startswith("adm_m_"):
        if not is_admin(call.from_user.id): return
        parts = call.data.split("_")
        action, mid = parts[2], parts[3]
        
        if action == "add":
            sent = bot.send_message(call.message.chat.id, 
                f"👤 *ADD PLAYERS TO {mid}*\n\nFormat: `player_name | role`\nMultiple lines use karein.\n\nRoles: `bat, wk, ar, bowl, sub`", 
                parse_mode='Markdown', reply_markup=types.ForceReply())
            bot.register_next_step_handler(sent, process_player_addition)
            
        elif action == "view":
            # Re-using the list_players logic
            call.message.text = f"/list_players {mid}"
            call.message.from_user = call.from_user # Admin check pass karne ke liye clicker info add ki
            cmd_list_players(call.message)
            
        elif action == "del":
            markup = types.InlineKeyboardMarkup()
            markup.add(
                types.InlineKeyboardButton("✅ CONFIRM DELETE", callback_data=f"adm_m_realdel_{mid}"),
                types.InlineKeyboardButton("❌ CANCEL", callback_data="app_home")
            )
            bot.edit_message_text(f"⚠️ *ARE YOU SURE?*\n\nMatch `{mid}` delete karne se sabhi teams aur data chala jayega!", 
                                 call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='Markdown')
                                 
        elif action == "realdel":
            try:
                with db.get_db() as conn:
                    conn.execute("DELETE FROM MATCHES_LIST WHERE match_id=%s", (mid,))
                    conn.execute("DELETE FROM PLAYERS WHERE match_id=%s", (mid,))
                    conn.execute("DELETE FROM TEAMS WHERE match_id=%s", (mid,))
                MATCHES.pop(mid, None)
                PLAYERS_CACHE.pop(mid, None)
                bot.edit_message_text(f"✅ Match `{mid}` and all related data deleted successfully.", 
                                     call.message.chat.id, call.message.message_id)
            except Exception:
                bot.answer_callback_query(call.id, f"Error: {e}", show_alert=True)
        
        bot.answer_callback_query(call.id)
        return

    # Route Player & Contest Management Callbacks
    if call.data.startswith("adm_p_"):
        if not is_admin(call.from_user.id): return
        parts = call.data.split("_")
        action, mid = parts[2], parts[3]
        ADMIN_MATCH_CONTEXT[str(call.from_user.id)] = mid

        if action == "vdel":
            # Interactive Delete Confirmation: adm_p_vdel_<mid>_<player_name>
            name = "_".join(parts[4:]).replace('_', ' ')
            markup = types.InlineKeyboardMarkup()
            markup.add(
                types.InlineKeyboardButton("✅ YES, DELETE", callback_data=f"adm_p_realdel_{mid}_{name.replace(' ', '_')}"),
                types.InlineKeyboardButton("❌ NO, CANCEL", callback_data=f"adm_m_view_{mid}")
            )
            bot.edit_message_text(f"🗑️ *Confirm Deletion*\n\nMatch `{mid}` se player `{name}` ko delete karein?", 
                                 call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='Markdown')
            bot.answer_callback_query(call.id)
            return

        elif action == "realdel":
            # Perform the actual deletion
            name = "_".join(parts[4:]).replace('_', ' ')
            db.db_delete_player(mid, name)
            PLAYERS_CACHE.pop(mid, None) # Clear cache
            bot.answer_callback_query(call.id, f"✅ {name} removed!")
            
            # Refresh the squad list
            bot.delete_message(call.message.chat.id, call.message.message_id)
            call.message.text = f"/list_players {mid}"
            cmd_list_players(call.message)
            return

        elif action == "edit":
            sent = bot.send_message(call.message.chat.id, 
                f"✏️ *EDIT PLAYER ROLE ({mid})*\n\nFormat: `Player Name | role` \nExample: `Virat Kohli | wk` \n\nRoles: `bat, wk, ar, bowl, sub`", 
                parse_mode='Markdown', reply_markup=types.ForceReply())
            bot.register_next_step_handler(sent, process_role_edit_callback)
        elif action == "del":
            sent = bot.send_message(call.message.chat.id, 
                f"🗑️ *REMOVE PLAYER FROM {mid}*\n\nPlayer ka full name likhein (exact match):\nExample: `Virat Kohli`", 
                parse_mode='Markdown', reply_markup=types.ForceReply())
            bot.register_next_step_handler(sent, process_player_deletion_callback)
        elif action == "delcont":
             sent = bot.send_message(call.message.chat.id, 
                f"🗑️ *DELETE CONTEST FROM {mid}*\n\nEntry Fee bhein (e.g. `100`):", 
                parse_mode='Markdown', reply_markup=types.ForceReply())
             bot.register_next_step_handler(sent, process_delete_contest_callback)
        
        bot.answer_callback_query(call.id)
        return

    if call.data.startswith("adm_toggle_lock_"):
        parts = call.data.split("_")
        mid, action = parts[3], parts[4]
        new_val = 1 if action == 'lock' else -1
        
        db.db_set_manual_lock(mid, new_val)
        sync_matches_from_db() # Reload cache
        
        bot.answer_callback_query(call.id, f"Match {'Locked' if new_val==1 else 'Unlocked'}!")
        # Refresh the UI
        players_data = get_players(mid)
        markup = admin_app.admin_event_markup(mid, players_data, is_locked=is_match_locked(mid))
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=markup)
        return

    # Route Admin commands
    if call.data.startswith("adm_"):
        if not is_admin(call.from_user.id):
            bot.answer_callback_query(call.id, "🚫 Unauthorized!", show_alert=True)
            return
        import admin_app # Lazy import to avoid circular dependency
        admin_app.handle_admin_nav(call, bot)
        return

    # PATCH: /help command support button handler
    if call.data == "start_support":
        cmd_support(call.message)
        return
    # Route Scoring events
    if call.data.startswith("evt_"):
        parts = call.data.split("_")
        scoring.update_match_event(parts[1], parts[2], parts[3])
        bot.answer_callback_query(call.id, "✅ Point Updated!")
        return

    logging.info(f"Unmatched callback: {call.data}")

# COMMANDS
# ===================================================

@bot.message_handler(commands=['setcaptain', 'setvc'])
def cmd_set_cv(msg):
    uid = str(msg.from_user.id)
    parts = msg.text.split(maxsplit=1)
    
    if len(parts) < 2:
        bot.reply_to(msg, "Usage: /setcaptain Virat Kohli")
        return
    
    name = parts[1].strip()
    team = db_get_team(uid)
    
    if not team:
        bot.reply_to(msg, "❌ Build team first!")
        return
    
    all_players = []
    for role in ROLES:
        all_players.extend(team.get(role, []))
    
    if name not in all_players:
        bot.reply_to(msg, f"'{name}' not in team!")
        return
    
    key = 'captain' if 'setcaptain' in msg.text else 'vice_captain'
    team[key] = name
    db_save_team(uid, team)

    emoji = "👑" if key == 'captain' else "⭐"
    bot.reply_to(msg, f"{emoji} {name} set!")

@bot.message_handler(commands=['admin_panel'])
def cmd_admin(msg):
    if not is_admin(msg.from_user.id):
        bot.reply_to(msg, f"🚫 **Access Denied!**\nAapka User ID (`{msg.from_user.id}`) admin list mein nahi hai.\n\nCheck `.env` file and set `ADMIN_ID={msg.from_user.id}`", parse_mode='Markdown')
        return
    try:
        stats = db.get_admin_stats()
        markup, text = ui.admin_dashboard_home(stats, MATCHES)
        bot.send_message(msg.chat.id, text, reply_markup=markup, parse_mode='Markdown')
    except Exception as e:
        logging.error(f"Admin panel error: {e}")
        bot.reply_to(msg, f"❌ **Dashboard Error:**\n`{str(e)}`", parse_mode='Markdown')

@bot.message_handler(commands=['admin_help'])
def cmd_admin_help(msg):
    if not is_admin(msg.from_user.id):
        return
    try:
        markup, text = ui.admin_help_render()
        bot.send_message(msg.chat.id, text, reply_markup=markup, parse_mode='HTML')
    except Exception as e:
        logging.error(f"Admin help error: {e}")

@bot.message_handler(commands=['download_db'])
def cmd_download_db(msg):
    if not is_admin(msg.from_user.id):
        return
    bot.reply_to(msg, "📂 Aap PostgreSQL use kar rahe hain. Database backup ke liye `/export_data` command ka use karein jo CSV files generate karega.")

@bot.message_handler(commands=['setup_contests'])
def cmd_setup_contests(msg):
    """Ek hi match ke liye teeno (Mega, Med, Small) contests set karne ka wizard"""
    if not is_admin(msg.from_user.id): return
    sent = bot.send_message(msg.chat.id, "🎯 *CONTEST SETUP WIZARD*\n\nMatch ID bhein jiske liye contests set karne hain (e.g. `m1`):", parse_mode='Markdown')
    bot.register_next_step_handler(sent, process_setup_contests_start)

def process_setup_contests_start(msg):
    mid = msg.text.strip()
    if mid not in MATCHES:
        bot.reply_to(msg, f"❌ Match `{mid}` nahi mila! Match list check karein.")
        return
    uid = str(msg.from_user.id)
    ADMIN_MATCH_CONTEXT[uid] = mid
    bot.send_message(msg.chat.id, f"✅ Match `{mid}` selected.\n\nAb **🥇 MEGA Contest** setup karein.\nFormat: `fee | slots` (e.g. `100 | 50`)", parse_mode='Markdown')
    bot.register_next_step_handler(msg, process_mega_setup)

def process_mega_setup(msg):
    if not is_admin(msg.from_user.id): return
    uid = str(msg.from_user.id)
    mid = ADMIN_MATCH_CONTEXT.get(uid)
    
    if msg.text.lower() == 'skip':
        bot.send_message(msg.chat.id, "⏭️ Mega skipped. Ab **🥈 MEDIUM Contest** bhein (`fee | slots`):", parse_mode='Markdown')
        bot.register_next_step_handler(msg, process_medium_setup)
        return

    try:
        parts = [p.strip() for p in msg.text.split("|")]
        fee, slots = int(parts[0]), int(parts[1])
        comm = int(parts[2]) if len(parts) > 2 else None
        
        db.db_set_contest_config(mid, fee, slots)
        bd = ui.get_prize_breakdown(fee, slots, custom_comm=comm, match_id=mid)
        
        txt = (f"✅ *MEGA Contest Set!*\n\n💰 Pool: ₹{bd['pool']} | ✨ Winners: {bd['winners']}\n"
               f"🥇 R1: ₹{bd['1st']} | 🥈 R2: ₹{bd['2nd']}\n🥉 R3: ₹{bd['3rd']} | 🏅 R4-10: ₹{bd['4-10']}\n\n"
               f"Ab **🥈 MEDIUM Contest** details bhein (`fee | slots | comm%`) ya `skip` likhein:")
        bot.send_message(msg.chat.id, txt, parse_mode='Markdown')
        bot.register_next_step_handler_by_chat_id(msg.chat.id, process_medium_setup)
    except Exception as e:
        bot.reply_to(msg, "❌ Invalid format. Use `fee | slots` (e.g. `100 | 50`) or `skip`.")
        bot.register_next_step_handler(msg, process_mega_setup)

def process_medium_setup(msg):
    if not is_admin(msg.from_user.id): return
    uid = str(msg.from_user.id)
    mid = ADMIN_MATCH_CONTEXT.get(uid)

    if msg.text.lower() == 'skip':
        bot.send_message(msg.chat.id, "⏭️ Medium skipped. Ab **🥉 SMALL Contest** bhein (`fee | slots`):", parse_mode='Markdown')
        bot.register_next_step_handler(msg, process_small_setup)
        return

    try:
        parts = [p.strip() for p in msg.text.split("|")]
        fee, slots = int(parts[0]), int(parts[1])
        comm = int(parts[2]) if len(parts) > 2 else None

        db.db_set_contest_config(mid, fee, slots)
        bd = ui.get_prize_breakdown(fee, slots, custom_comm=comm, match_id=mid)
        
        txt = (f"✅ *MEDIUM Contest Set!*\n\n💰 Pool: ₹{bd['pool']} | ✨ Winners: {bd['winners']}\n"
               f"🥇 R1: ₹{bd['1st']} | 🥈 R2: ₹{bd['2nd']}\n🥉 R3: ₹{bd['3rd']} | 🏅 R4-10: ₹{bd['4-10']}\n\n"
               f"Ab **🥉 SMALL Contest** details bhein (`fee | slots | comm%`) ya `skip` likhein:")
        bot.send_message(msg.chat.id, txt, parse_mode='Markdown')
        bot.register_next_step_handler_by_chat_id(msg.chat.id, process_small_setup)
    except Exception:
        bot.reply_to(msg, "❌ Invalid format. Use `fee | slots` (e.g. `50 | 100`) or `skip`.")
        bot.register_next_step_handler_by_chat_id(msg.chat.id, process_medium_setup)

def process_small_setup(msg):
    if not is_admin(msg.from_user.id): return
    uid = str(msg.from_user.id)
    mid = ADMIN_MATCH_CONTEXT.get(uid)

    if msg.text.lower() == 'skip':
        bot.send_message(msg.chat.id, "✅ *Match Setup Complete!* Sabhi updates live hain.")
        return

    try:
        parts = [p.strip() for p in msg.text.split("|")]
        fee, slots = int(parts[0]), int(parts[1])
        comm = int(parts[2]) if len(parts) > 2 else None
        
        db.db_set_contest_config(mid, fee, slots)
        bd = ui.get_prize_breakdown(fee, slots, custom_comm=comm, match_id=mid)
        
        txt = (f"✅ *SMALL Contest Set!*\n\n💰 Pool: ₹{bd['pool']} | ✨ Winners: {bd['winners']}\n"
               f"🥇 R1: ₹{bd['1st']} | 🥈 R2: ₹{bd['2nd']}\n🥉 R3: ₹{bd['3rd']} | 🏅 R4-10: ₹{bd['4-10']}\n\n"
               f"🚀 *Match Setup Complete!* Match ab live hai.")
        
        bot.send_message(msg.chat.id, txt, parse_mode='Markdown')
    except Exception:
        bot.reply_to(msg, "❌ Invalid format. Use `fee | slots` (e.g. `20 | 200`) or `skip`.")
        bot.register_next_step_handler_by_chat_id(msg.chat.id, process_small_setup)

@bot.message_handler(commands=['export_data'])
def cmd_export_data(msg):
    """Saari main tables ko CSV bana kar admin ko bhejta hai"""
    # Admin check: handled via command or callback
    if not is_admin(msg.chat.id) and not is_admin(getattr(msg, 'from_user', msg).id): return
    
    # List ko expand kiya hai taaki poora hisaab mil sake
    tables = ['USERS', 'PAYMENTS', 'WITHDRAWALS', 'MATCHES_LIST', 'TEAMS', 'LEDGER', 'CONTEST_CONFIG']
    bot.send_message(msg.chat.id, "📤 *Generating CSV Backups...*")

    for table in tables:
        try:
            with db.get_db() as conn:
                conn.execute(f"SELECT * FROM {table}")
                rows = conn.fetchall()
                if not rows:
                    bot.send_message(msg.chat.id, f"ℹ️ Table *{table}* abhi khali hai (No Data).", parse_mode='Markdown')
                    continue
                
                output = io.StringIO()
                writer = csv.DictWriter(output, fieldnames=rows[0].keys())
                writer.writeheader()
                writer.writerows(rows)
                
                output.seek(0)
                # Convert to binary stream for Telegram
                bio = io.BytesIO(output.getvalue().encode('utf-8'))
                bio.name = f"{table.lower()}_backup_{datetime.now().strftime('%Y%m%d')}.csv"
                bot.send_document(msg.chat.id, bio, caption=f"📊 Table: {table}")
                time.sleep(1) # Telegram anti-flood delay
        except Exception as e:
            bot.send_message(msg.chat.id, f"❌ Error exporting {table}: {e}")
    
    bot.send_message(msg.chat.id, "✅ Backup complete!")

@bot.message_handler(commands=['broadcast'])
def cmd_broadcast(msg):
    if not is_admin(msg.from_user.id):
        return
    
    markup = types.ForceReply(selective=True)
    bot.send_message(msg.chat.id, "📝 *Enter the message you want to broadcast to all users:*\n\n(Markdown formatting is supported)", 
                     reply_markup=markup, parse_mode='Markdown')
    bot.register_next_step_handler(msg, process_broadcast_message)

def process_broadcast_message(msg):
    if not is_admin(msg.from_user.id):
        return

    # 📸 Support for Photo Broadcasts
    is_photo = msg.content_type == 'photo'
    file_id = msg.photo[-1].file_id if is_photo else None
    caption_or_text = msg.caption if is_photo else msg.text

    broadcast_text = sanitize_input(caption_or_text, 4000)
    if not broadcast_text and not is_photo:
        bot.send_message(ADMIN_ID, "❌ Broadcast message cannot be empty.")
        return

    bot.send_message(ADMIN_ID, "🚀 Starting broadcast... This may take a while.")
    
    my_id = str(bot.get_me().id)
    success_count = 0
    fail_count = 0
    with db.get_db() as conn:
        conn.execute("SELECT user_id FROM USERS")
        users = conn.fetchall()
        for user_row in users:
            # Skip the bot itself if its ID is in the database
            if str(user_row['user_id']) == my_id:
                continue
            try:
                if is_photo:
                    bot.send_photo(user_row['user_id'], file_id, caption=broadcast_text, parse_mode='Markdown')
                else:
                    bot.send_message(user_row['user_id'], broadcast_text, parse_mode='Markdown')
                success_count += 1
                time.sleep(0.05) # Small delay to avoid hitting Telegram API limits
            except telebot.apihelper.ApiTelegramException as e:
                logging.warning(f"Failed to send broadcast to user {user_row['user_id']}: {e}")
                fail_count += 1
    
    bot.send_message(ADMIN_ID, f"✅ Broadcast finished!\n\nSent to {success_count} users.\nFailed for {fail_count} users (likely blocked the bot).")

def send_prematch_reminders():
    """Checks for upcoming matches and notifies users who haven't joined yet, and triggers point calculation"""
    now = get_now()
    for mid, info in MATCHES.items():
        deadline = info['deadline']

        # --- Prematch Reminders ---
        time_to_match = deadline - now
        
        # Match starts in 60-75 minutes
        if timedelta(minutes=60) <= time_to_match <= timedelta(minutes=75):
            # 1. Users with NO team saved
            users_no_team = db.db_get_users_without_team(mid)
            for uid in users_no_team:
                if not db.db_was_reminder_sent(mid, uid, 'prematch'):
                    try:
                        bot.send_message(uid, f"🏏 *Match Starting Soon!* ⏳\n\n`{info['name']}` ka deadline 60 min mein hai. Jaldi apni team banayein aur join karein!", parse_mode='Markdown')
                        db.db_mark_reminder_sent(mid, uid, 'prematch')
                    except: pass
            
            # 2. Users with saved but UNPAID teams
            users_unpaid = db.db_get_users_unpaid_team(mid)
            for uid in users_unpaid:
                if not db.db_was_reminder_sent(mid, uid, 'unpaid_team'):
                    try:
                        bot.send_message(uid, f"⚠️ *Team Not Joined!*\n\nAapne `{info['name']}` ke liye team banayi hai par contest join nahi kiya. Deadline se pehle join karein!", parse_mode='Markdown')
                        db.db_mark_reminder_sent(mid, uid, 'unpaid_team')
                    except: pass
        
        # --- Auto Match Lock ---
        # Match lock hona automatic hai, lekin calculation manual settlement ke baad hoga
        if now > deadline:
            if mid not in _selection_cooldown: # Log only once
                logging.info(f"⏳ Match {mid} deadline passed. Match is now LOCKED.")
                _selection_cooldown[mid] = True 

def process_match_end(match_id):
    """
    Match deadline nikalne ke baad points calculate karta hai aur admin ko notify karta hai.
    """
    global MATCHES
    logging.info(f"🚀 Starting point calculation for match: {match_id}")
    
    try:
        # Fetch all player live stats for this match
        player_live_scores_map = db.db_get_all_player_scores(match_id)
        
        if calculate_all_points(match_id, player_live_scores_map):
            db.db_mark_points_calculated(match_id) # DB mein mark karein
            MATCHES[match_id]['points_calculated'] = True # Memory cache mein update karein
            bot.send_message(ADMIN_ID, f"✅ <b>Points Calculated for Match: {html.escape(MATCHES[match_id]['name'])}</b>", parse_mode='HTML')
            logging.info(f"✅ Points calculation completed for match: {match_id}")
        else:
            bot.send_message(ADMIN_ID, f"❌ <b>Error in point calculation for Match: {html.escape(MATCHES[match_id]['name'])}</b>", parse_mode='HTML')
            logging.error(f"❌ Error in point calculation for match: {match_id}")
    except Exception as e:
        logging.error(f"Error in process_match_end for {match_id}: {e}")
        if ADMIN_ID:
            try:
                bot.send_message(ADMIN_ID, f"❌ <b>Critical Error in process_match_end:</b> {html.escape(str(e))}", parse_mode='HTML')
            except: pass

# ===================================================
# FEATURE 3: Re-engagement Notification (3 Day Inactive)
# ===================================================
def send_reengagement_notifications():
    """Sends re-engagement messages to inactive users."""
    inactive_users = db.db_get_inactive_users(days=3)
    for user in inactive_users:
        uid = user['user_id']
        first_name = user['first_name']
        try:
            text = (
                f"👋 *Hey {first_name}, wapas aao!* 👋\n\n"
                "Aapko miss kar rahe hain! Naye matches aur bade prizes aapka intezaar kar rahe hain.\n\n"
                "Jaldi se bot par wapas aao aur apni team banao! 🏏"
            )
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("🏆 CONTESTS DEKHO", callback_data="contest_list"))
            bot.send_message(uid, text, reply_markup=markup, parse_mode='Markdown')
            db.db_mark_reminder_sent(None, uid, 'reengagement') # match_id is None for re-engagement
            time.sleep(0.05)
        except Exception as e:
            logging.warning(f"Failed to send re-engagement to {uid}: {e}")

# ===================================================
# POINTS CALCULATION SYSTEM
# ===================================================

def calculate_all_points(match_id, player_scores):
    """
    Calculates points for all teams in a given match.
    Args:
        match_id (str): The ID of the match.
        player_scores (dict): A dictionary of {'Player Name': total_points_from_stats}.
    """
    try:
        with db.get_db() as conn:
            # 1. Fetch all paid teams and their entry amounts
            conn.execute("""
                SELECT t.*, l.amount as entry_fee 
                FROM TEAMS t 
                JOIN LEDGER l ON l.reference_id LIKE 'DEBIT_MATCH_' || t.match_id || '_' || t.team_num || '_%'
                WHERE t.match_id = %s AND t.is_paid = 1
            """, (match_id,))
            all_paid_teams = conn.fetchall()
            
            if not all_paid_teams:
                logging.info(f"No paid teams to calculate for match {match_id}")
                return True

            # 2. Points update logic (First pass)
            for row in all_paid_teams:
                uid, tnum = row['user_id'], row['team_num']
                team_data = json.loads(row['team_players'])
                total_pts = 0
                for role in ROLES:
                    for p in team_data.get(role, []):
                        p_pts = player_scores.get(p, 0)
                        mult = 2.0 if p == row['captain'] else 1.5 if p == row['vice_captain'] else 1.0
                        total_pts += p_pts * mult
                
                conn.execute("UPDATE TEAMS SET points = %s WHERE user_id = %s AND match_id = %s AND team_num = %s", 
                             (total_pts, uid, match_id, tnum))
                row['points'] = total_pts # Update local object for ranking

            # 3. Group by Contest (Entry Fee) and Reward
            # We find distinct entry fees joined for this match
            entry_fees = set(abs(int(r['entry_fee'])) for r in all_paid_teams)
            
            for fee in entry_fees:
                # Filter teams belonging to this contest tier
                contest_teams = [r for r in all_paid_teams if abs(int(r['entry_fee'])) == fee]
                contest_teams.sort(key=lambda x: x['points'], reverse=True)
                
                # Get Prize Breakdown for this tier
                config = db.db_get_contest_config(match_id, fee)
                slots = config['max_slots'] if config else 50
                bd = ui.get_prize_breakdown(fee, slots)
                
                for index, res in enumerate(contest_teams):
                    rank = index + 1
                    prize_amt = 0
                    
                    if rank == 1: prize_amt = bd['1st']
                    elif rank == 2: prize_amt = bd['2nd']
                    elif rank == 3: prize_amt = bd['3rd']
                    elif 4 <= rank <= 10: prize_amt = bd['4-10']
                    elif 11 <= rank <= bd['winners']: prize_amt = bd['bottom']
                    
                    prize_text = f"₹{prize_amt}" if prize_amt > 0 else "₹0"
                    
                    # Credit Prize
                    if prize_amt > 0:
                        ref_id = f"PRIZE_{match_id}_{fee}_{rank}_{res['user_id']}_{res['team_num']}"
                        process_payment_success(res['user_id'], prize_amt, ref_id, conn=conn)
                        
                        congrats_msg = (
                            f"🎊 *CONGRATS!*\n\n"
                            f"Match: `{MATCHES[match_id]['name']}`\n"
                            f"Rank: *#{rank}* (Team {res['team_num']})\n"
                            f"Points: *{res['points']}*\n"
                            f"Prize: *{prize_text}* 💰\n\n"
                            f"Balance aapke wallet mein add kar diya gaya hai!"
                        )
                        try: bot.send_message(res['user_id'], congrats_msg, parse_mode='Markdown')
                        except: pass
                    else:
                        fail_msg = (
                            f"📉 *Match Ended*\n\n"
                            f"Match: `{MATCHES[match_id]['name']}`\n"
                            f"Rank: *#{rank}* (Team {res['team_num']})\n"
                            f"Points: *{res['points']}*\n\n"
                            "Better luck next time!"
                        )
                        try: bot.send_message(res['user_id'], fail_msg, parse_mode='Markdown')
                        except: pass

                    # Sheets Sync
                    sheets.sync_wrapper({
                        "contest_date": datetime.now().strftime('%Y-%m-%d'),
                        "user_id": res['user_id'],
                        "points": res['points'],
                        "rank": rank,
                        "prize": prize_text
                    }, "RESULTS")
                
        return True
    except Exception as e:
        logging.error(f"Points calculation error: {e}")
        return False

@bot.message_handler(commands=['add_match'])
def cmd_add_match(msg):
    if not is_admin(msg.from_user.id): return
    text = (
        "🆕 *ADD NEW MATCH*\n\n"
        "Niche diye gaye format mein details bhejein:\n"
        "`match_id | Name | Type | YYYY-MM-DD HH:MM`\n\n"
        "Example:\n"
        "`m1 | CSK vs MI | IPL T20 | 2026-05-01 19:30`"
    )
    sent = bot.send_message(msg.chat.id, text, parse_mode='Markdown')
    bot.register_next_step_handler(sent, process_match_input)

def process_match_input(msg):
    if not is_admin(msg.from_user.id): return
    try:
        parts = [p.strip() for p in msg.text.split("|")]
        if len(parts) < 4:
            bot.reply_to(msg, "❌ Format galat hai! Example: `m5 | RR vs PBKS | IPL T20 | 2026-04-24 19:30`")
            return
        
        mid, name, m_type, deadline_str = parts[0], parts[1], parts[2], parts[3]
        # Validate date
        datetime.strptime(deadline_str, '%Y-%m-%d %H:%M')
        
        db.db_add_match(mid, name, m_type, deadline_str)
        PLAYERS_CACHE.pop(mid, None) # Clear players cache for this match
        sync_matches_from_db() # Refresh memory cache
        uid = str(msg.from_user.id)
        ADMIN_MATCH_CONTEXT[uid] = mid # Remember match context
        ADMIN_MATCH_CONTEXT[uid + "_wizard"] = True # Start setup wizard

        bot.reply_to(msg, (
            f"✅ *Match Added: {name}*\n\n"
            "Ab is match ke liye *Players* bhein.\n"
            "Format: `Name | Role | Desig | Team`\n"
            "Example: `Virat Kohli | bat | c | RCB`"
        ), parse_mode='Markdown')
        bot.register_next_step_handler(msg, process_player_addition)
    except Exception as e:
        bot.reply_to(msg, f"❌ Error: {e}\nFormat check karein: `YYYY-MM-DD HH:MM`")

@bot.message_handler(commands=['add_player'])
def cmd_add_player(msg):
    if not is_admin(msg.from_user.id): return
    active_mid = ADMIN_MATCH_CONTEXT.get(str(msg.from_user.id), "m1")
    help_text = (
        "👤 *QUICK ADD PLAYER*\n\n"
        "👉 *Standard Format:*\n"
        "`Name | Role | Designation | Team`\n\n"
        "✅ *Example:*\n"
        "`Virat Kohli | bat | c | RCB`\n"
        "`MS Dhoni | wk | vc | CSK`\n\n"
        "👉 *Format 1 (Bulk with Match ID):*\n"
        f"`{active_mid}` (Pehli line)\n"
        "`Player Name | role` (Baaki lines)\n\n"
        "👉 *Format 2 (Single):*\n"
        f"`{active_mid} | Name | role`\n\n"
        "Roles: `bat, wk, ar, bowl, sub`"
    )
    sent = bot.send_message(msg.chat.id, help_text, parse_mode='Markdown')
    bot.register_next_step_handler(sent, process_player_addition)

def process_player_addition(msg):
    if not is_admin(msg.from_user.id): return
    uid = str(msg.from_user.id)
    try:
        raw_text = msg.text.strip()
        lines = raw_text.split('\n')
        added_players = []
        failed_players = []
        
        # Smart Detection: If first line is just a match_id
        default_mid = ADMIN_MATCH_CONTEXT.get(uid)
        if len(lines) > 0 and "|" not in lines[0] and "," not in lines[0]:
            default_mid = lines[0].strip()
            lines = lines[1:]
            ADMIN_MATCH_CONTEXT[uid] = default_mid # Update context

        # Combine remaining text and handle comma-separated horizontal format
        remaining_text = "\n".join(lines)
        if ',' in remaining_text:
            entries = [item.strip() for item in remaining_text.split(',')]
        else:
            entries = [line.strip() for line in lines]
        
        # Shorthand mapping for easy typing
        role_map = {
            'w': 'wk', 'keeper': 'wk', 'keep': 'wk',
            'ball': 'bowl', 'baller': 'bowl', 'bowler': 'bowl',
            'all': 'ar', 'allrounder': 'ar', 'ar': 'ar',
            'bat': 'bat', 'batsman': 'bat',
            's': 'sub', 'sub': 'sub', 'substitute': 'sub'
        }

        for entry in entries:
            if not entry: continue
            parts = [p.strip() for p in entry.split("|")]
            designation = ""
            
            if len(parts) == 5: # mid | name | role | desig | team
                mid, name, role, designation, team = parts[0], parts[1], parts[2].lower(), parts[3].lower(), parts[4].upper()
            elif len(parts) == 4: 
                if parts[0] in MATCHES: # mid | name | role | team
                    mid, name, role, team = parts[0], parts[1], parts[2].lower(), parts[3].upper()
                elif default_mid: # name | role | desig | team
                    mid, name, role, designation, team = default_mid, parts[0], parts[1].lower(), parts[2].lower(), parts[3].upper()
            elif len(parts) == 3 and default_mid: # name | role | team
                mid, name, role, team = default_mid, parts[0], parts[1].lower(), parts[2].upper()
            elif len(parts) == 2 and default_mid: # name | role (Legacy)
                mid, name, role, team = default_mid, parts[0], parts[1].lower(), "N/A"
            else:
                failed_players.append(f"❌ Format error: `{entry}`")
                continue

            # Apply shorthand role mapping
            role = role_map.get(role, role)

            if role not in ROLES:
                failed_players.append(f"❌ Galat role `{role}`: `{name}`")
                continue
                
            try:
                db.db_add_player(mid, name, role, team, designation)
                PLAYERS_CACHE.pop(mid, None) # Invalidate cache
                added_players.append(f"✅ {name} ({role.upper()} - {team})")
            except Exception as e:
                if "unique constraint" in str(e).lower():
                    failed_players.append(f"⚠️ {name} (Pehle se hai)")
                else:
                    failed_players.append(f"❌ {name}: {str(e)}")
        
        response_text = f"📊 *IMPORT STATUS ({mid})*\n━━━━━━━━━━━━━━\n"
        if added_players:
            response_text += "\n".join(added_players)
        if failed_players:
            response_text += "\n\n*Nahi huye:*\n" + "\n".join(failed_players)
            
        # Navigation buttons after import
        markup = types.InlineKeyboardMarkup()
        markup.add(
            types.InlineKeyboardButton("👥 View Players", callback_data=f"adm_m_view_{mid}"),
            types.InlineKeyboardButton("🏆 Setup Contests", callback_data="adm_nav_home")
        )
        bot.send_message(msg.chat.id, response_text, reply_markup=markup, parse_mode='Markdown')

        # Wizard Flow: Check if we should proceed to contest setup
        if ADMIN_MATCH_CONTEXT.get(uid + "_wizard"):
            bot.send_message(msg.chat.id, "✅ *Players Sync Ho Gaye!*\n\nAb **🥇 MEGA Contest** setup karein.\nFormat: `fee | slots | comm%` (e.g. `100 | 50 | 10`)", parse_mode='Markdown')
            bot.register_next_step_handler(msg, process_mega_setup)

    except Exception as e:
        bot.reply_to(msg, f"❌ Error: {e}")

def process_mega_setup(msg):
    if not is_admin(msg.from_user.id): return
    uid = str(msg.from_user.id)
    mid = ADMIN_MATCH_CONTEXT.get(uid)
    
    if msg.text.lower() == 'skip':
        bot.send_message(msg.chat.id, "⏭️ Mega skipped. Ab **🥈 MEDIUM Contest** bhein (`fee | slots`):", parse_mode='Markdown')
        bot.register_next_step_handler(msg, process_medium_setup)
        return

    try:
        parts = [p.strip() for p in msg.text.split("|")]
        fee, slots = int(parts[0]), int(parts[1])
        comm = int(parts[2]) if len(parts) > 2 else None
        
        db.db_set_contest_config(mid, fee, slots)
        bd = ui.get_prize_breakdown(fee, slots, custom_comm=comm)
        
        txt = (f"✅ *MEGA Contest Set!*\n\n💰 Total Collection: ₹{bd['collection']}\n✂️ *Admin Cut ({bd['comm_pct']}%): ₹{bd['commission_amt']}*\n🎁 *Batega (Pool): ₹{bd['pool']}*\n\n"
               f"✨ Winners: {bd['winners']} | 🥇 Rank 1: ₹{bd['1st']}\n\n"
               f"Ab **🥈 MEDIUM Contest** details bhein (`fee | slots`) ya `skip` likhein:")
        bot.send_message(msg.chat.id, txt, parse_mode='Markdown')
        bot.register_next_step_handler(msg, process_medium_setup)
    except:
        bot.reply_to(msg, "❌ Invalid format. Use `fee | slots` (e.g. `100 | 50`)")
        bot.register_next_step_handler(msg, process_mega_setup)

def process_medium_setup(msg):
    if not is_admin(msg.from_user.id): return
    uid = str(msg.from_user.id)
    mid = ADMIN_MATCH_CONTEXT.get(uid)

    if msg.text.lower() == 'skip':
        bot.send_message(msg.chat.id, "⏭️ Medium skipped. Ab **🥉 SMALL Contest** bhein (`fee | slots`):", parse_mode='Markdown')
        bot.register_next_step_handler(msg, process_small_setup)
        return

    try:
        parts = [p.strip() for p in msg.text.split("|")]
        fee, slots = int(parts[0]), int(parts[1])
        comm = int(parts[2]) if len(parts) > 2 else None

        db.db_set_contest_config(mid, fee, slots)
        bd = ui.get_prize_breakdown(fee, slots, custom_comm=comm)
        
        txt = (f"✅ *MEDIUM Contest Set!*\n\n💰 Total Collection: ₹{bd['collection']}\n✂️ *Admin Cut ({bd['comm_pct']}%): ₹{bd['commission_amt']}*\n🎁 *Batega (Pool): ₹{bd['pool']}*\n\n"
               f"✨ Winners: {bd['winners']} | 🥇 Rank 1: ₹{bd['1st']}\n\n"
               f"Ab **🥉 SMALL Contest** details bhein (`fee | slots`) ya `skip` likhein:")
        bot.send_message(msg.chat.id, txt, parse_mode='Markdown')
        bot.register_next_step_handler(msg, process_small_setup)
    except:
        bot.reply_to(msg, "❌ Invalid format. Use `fee | slots` (e.g. `50 | 100`)")
        bot.register_next_step_handler(msg, process_medium_setup)

def process_small_setup(msg):
    if not is_admin(msg.from_user.id): return
    uid = str(msg.from_user.id)
    mid = ADMIN_MATCH_CONTEXT.get(uid)
    ADMIN_MATCH_CONTEXT.pop(uid + "_wizard", None) # Wizard complete

    if msg.text.lower() == 'skip':
        bot.send_message(msg.chat.id, "✅ *Match Setup Complete!* Sabhi updates live hain.")
        return

    try:
        parts = [p.strip() for p in msg.text.split("|")]
        fee, slots = int(parts[0]), int(parts[1])
        db.db_set_contest_config(mid, fee, slots)
        bot.send_message(msg.chat.id, "✅ *SMALL Contest Set!*\n\n🚀 *Match Setup Complete!* Match ab users ke liye dashboard par live hai.")
    except:
        bot.reply_to(msg, "❌ Invalid format. Use `fee | slots` (e.g. `20 | 200`)")
        bot.register_next_step_handler(msg, process_small_setup)

@bot.message_handler(commands=['list_players'])
def cmd_list_players(msg):
    if not is_admin(msg.from_user.id): return
    parts = msg.text.split()
    mid = parts[1] if len(parts) > 1 else ADMIN_MATCH_CONTEXT.get(str(msg.from_user.id), "m1")
    
    ADMIN_MATCH_CONTEXT[str(msg.from_user.id)] = mid 
    players_data = get_players(mid)
    
    markup = types.InlineKeyboardMarkup(row_width=2)
    text = f"👥 *INTERACTIVE SQUAD: {mid}*\n\nPlayer par tap karein use delete karne ke liye:\n━━━━━━━━━━━━━━━━━━━━"
    
    found = False
    for role in ROLES:
        p_list = players_data.get(role, [])
        if p_list:
            found = True
            markup.add(types.InlineKeyboardButton(f"🔹 {ROLE_NAMES[role].upper()} 🔹", callback_data="ignore"))
            row = []
            for p in p_list:
                # Dictionary data structure ke hisaab se update kiya
                p_name = p['name']
                p_display = p['display']
                row.append(types.InlineKeyboardButton(f"🗑️ {p_display}", callback_data=f"adm_p_vdel_{mid}_{p_name.replace(' ', '_')}"))
                if len(row) == 2:
                    markup.row(*row)
                    row = []
            if row: markup.row(*row)
            
    if not found:
        bot.send_message(msg.chat.id, f"❌ Match `{mid}` mein koi players nahi mile.")
        return

    markup.add(types.InlineKeyboardButton("🔙 Back to Match Control", callback_data=f"adm_ctrl_{mid}"))
    bot.send_message(msg.chat.id, text, reply_markup=markup, parse_mode='Markdown')

@bot.message_handler(commands=['my_matches'])
def cmd_my_matches(msg):
    """F10: Admin command to view and manage all matches in the system"""
    if not is_admin(msg.from_user.id): return
    
    if not MATCHES:
        bot.send_message(msg.chat.id, "❌ No matches found in system.")
        return

    bot.send_message(msg.chat.id, "📋 *ALL MATCHES*", parse_mode='Markdown')
    now = get_now()
    
    for mid, info in MATCHES.items():
        deadline = info['deadline']
        player_count = db.db_get_player_count(mid)
        
        # Status Logic
        if now > deadline:
            status = "🔒 LOCKED"
        else:
            delta = deadline - now
            total_sec = delta.total_seconds()
            
            # Format time left string
            if delta.days > 0:
                time_str = f"{delta.days}d {delta.seconds//3600}h"
            else:
                time_str = f"{delta.seconds//3600}h {(delta.seconds//60)%60}m"
                
            if total_sec < 6 * 3600:
                status = f"🟢 OPEN ({time_str} left)"
            else:
                status = f"🟡 UPCOMING ({time_str} left)"

        count_display = f"{player_count}" + (" ⚠️" if player_count == 0 else "")
        deadline_display = deadline.strftime('%d %b %Y • %I:%M %p')

        match_text = (
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"🆔 ID: `{mid}`\n"
            f"🏏 Match: *{info['name']}*\n"
            f"📅 Type: {info['type']}\n"
            f"⏰ Deadline: {deadline_display}\n"
            f"⌛ Status: {status}\n"
            f"👥 Players Added: {count_display}\n"
            "━━━━━━━━━━━━━━━━━━━━"
        )
        
        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            types.InlineKeyboardButton("➕ Add Players", callback_data=f"adm_m_add_{mid}"),
            types.InlineKeyboardButton("👥 View Squad", callback_data=f"adm_m_view_{mid}")
        )
        markup.row(
            types.InlineKeyboardButton("✏️ Edit Role", callback_data=f"adm_p_edit_{mid}"),
            types.InlineKeyboardButton("❌ Remove Player", callback_data=f"adm_p_del_{mid}")
        )
        markup.add(
            types.InlineKeyboardButton("🗑️ Delete Contest", callback_data=f"adm_p_delcont_{mid}"),
            types.InlineKeyboardButton("🔥 DELETE MATCH", callback_data=f"adm_m_del_{mid}")
        )
        bot.send_message(msg.chat.id, match_text, reply_markup=markup, parse_mode='Markdown')

    bot.send_message(msg.chat.id, f"Total: {len(MATCHES)} matches")

@bot.message_handler(commands=['delete_player'])
def cmd_delete_player(msg):
    if not is_admin(msg.from_user.id): return
    help_text = (
        "🗑️ *DELETE PLAYER*\n\n"
        "Format: `match_id | player_name`\n"
        "Example: `m3 | Rohit Sharma`"
    )
    sent = bot.send_message(msg.chat.id, help_text, parse_mode='Markdown')
    bot.register_next_step_handler(sent, process_player_deletion)

def process_player_deletion(msg):
    if not is_admin(msg.from_user.id): return
    try:
        parts = [p.strip() for p in msg.text.split("|")]
        if len(parts) < 2:
            bot.reply_to(msg, "❌ Format: `mid | Name`")
            return
        mid, name = parts[0], parts[1]
        db.db_delete_player(mid, name)
        PLAYERS_CACHE.pop(mid, None) # Invalidate cache
        bot.reply_to(msg, f"🗑️ Player `{name}` removed from match `{mid}`.", parse_mode='Markdown')
    except Exception as e:
        bot.reply_to(msg, f"❌ Error: {e}")

@bot.message_handler(commands=['update_points', 'up'])
def cmd_update_points(msg):
    """
    Fast Update:
    1. /up Kohli 50 (Uses active match)
    2. /up m1 Kohli 50
    3. /up m1 | Kohli:50, Rohit:20 (Bulk)
    """
    if not is_admin(msg.from_user.id):
        return

    uid = str(msg.from_user.id)
    text = msg.text.replace('/update_points', '').replace('/up', '').strip()
    
    if not text:
        bot.reply_to(msg, "⚡ *Quick Update:* `/up PlayerName Score` (e.g. `/up Kohli 50`)", parse_mode='Markdown')
        return

    try:
        if "|" in text: # Bulk format
            mid_part, scores_part = text.split("|")
            mid = mid_part.strip()
            scores = {p.split(':')[0].strip(): float(p.split(':')[1].strip()) for p in scores_part.split(',')}
        else: # Simple space-separated format
            parts = text.split()
            if parts[0] in MATCHES: # Format: /up m1 Kohli 50
                mid = parts[0]
                player = " ".join(parts[1:-1])
                score = float(parts[-1])
            else: # Format: /up Kohli 50 (Uses context)
                mid = ADMIN_MATCH_CONTEXT.get(uid, "m1")
                player = " ".join(parts[:-1])
                score = float(parts[-1])
            scores = {player: score}
        
        if calculate_all_points(mid, scores):
            bot.reply_to(msg, f"✅ Points updated for Match `{mid}`!")
        else:
            bot.reply_to(msg, "❌ Error calculating points.")
    except Exception as e:
        bot.reply_to(msg, "⚠️ Usage: `/up m1 | Kohli:50, Rohit:20`")

@bot.message_handler(commands=['edit_player_role'])
def cmd_edit_role(msg):
    if not is_admin(msg.from_user.id): return
    help_text = "✏️ *EDIT PLAYER ROLE*\n\nFormat: `mid | Name | new_role` \nExample: `m1 | Rohit Sharma | bat`"
    sent = bot.send_message(msg.chat.id, help_text, parse_mode='Markdown')
    bot.register_next_step_handler(sent, process_role_edit)

def process_role_edit(msg):
    try:
        parts = [p.strip() for p in msg.text.split("|")]
        mid, name, role = parts[0], parts[1], parts[2].lower()
        if role in ROLES:
            db.db_add_player(mid, name, role) # Re-uses ON CONFLICT update
            PLAYERS_CACHE.pop(mid, None)
            bot.reply_to(msg, f"✅ `{name}` ka role update hokar `{role.upper()}` ho gaya hai.")
        else:
            bot.reply_to(msg, "❌ Invalid Role!")
    except:
        bot.reply_to(msg, "❌ Format: `mid | Name | role` use karein.")

# Helper callbacks for UI buttons
def process_role_edit_callback(msg):
    mid = ADMIN_MATCH_CONTEXT.get(str(msg.from_user.id))
    if not mid: return
    try:
        parts = [p.strip() for p in msg.text.split("|")]
        name, role = parts[0], parts[1].lower()
        if role in ROLES:
            db.db_add_player(mid, name, role)
            PLAYERS_CACHE.pop(mid, None)
            bot.reply_to(msg, f"✅ `{name}` ka role update hokar `{role.upper()}` ho gaya hai.")
        else: bot.reply_to(msg, "❌ Invalid Role!")
    except: bot.reply_to(msg, "❌ Format: `Name | role` use karein.")

def process_player_deletion_callback(msg):
    mid = ADMIN_MATCH_CONTEXT.get(str(msg.from_user.id))
    if not mid: return
    try:
        name = msg.text.strip()
        db.db_delete_player(mid, name)
        PLAYERS_CACHE.pop(mid, None)
        bot.reply_to(msg, f"🗑️ Player `{name}` removed from match `{mid}`.")
    except: bot.reply_to(msg, "❌ Error removing player.")

def process_delete_contest_callback(msg):
    mid = ADMIN_MATCH_CONTEXT.get(str(msg.from_user.id))
    if not mid: return
    try:
        fee = int(msg.text.strip())
        db.db_delete_contest(mid, fee)
        bot.reply_to(msg, f"✅ Match `{mid}` se ₹{fee} wala contest delete ho gaya!")
    except:
        bot.reply_to(msg, "❌ Invalid Fee! Sirf number bhein (e.g. 100)")

# ===================================================
# START BOT
# ===================================================


def reminder_worker():
    last_reengagement_date = None
    while True:
        try:
            send_prematch_reminders()
            # FEATURE 3: Re-engagement Notification (3 Day Inactive)
            today = get_now().date()
            if last_reengagement_date != today:
                send_reengagement_notifications()
                last_reengagement_date = today
        except Exception as e:
            logging.error(f"Worker error: {e}")
        time.sleep(300)  # Har 5 minute

reminder_thread = threading.Thread(target=reminder_worker, daemon=True)
reminder_thread.start()

if __name__ == "__main__":
    if os.getenv('RENDER'):
        logging.info("🚀 Starting in WEBHOOK mode (Production)...")
        server.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
    else:
        logging.info("🤖 Starting in POLLING mode (Local)...")
        logging.info("🧹 Clearing webhook for local polling...")
        bot.remove_webhook()
        time.sleep(1)
        while True:
            try:
                bot.infinity_polling(
                    skip_pending=True,
                    timeout=30,
                    long_polling_timeout=30,
                    interval=0
                )
            except Exception as e:
                logging.error(f"Polling Error: {e}")
                time.sleep(3)
