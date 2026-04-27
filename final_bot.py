import os
import telebot
from telebot import types
import json
from datetime import datetime, timedelta
import time
import re
from contextlib import contextmanager
import logging

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
    'sub': (1, 4)
}
ROLE_NAMES = {'bat': 'Batsmen', 'wk': 'Wicketkeepers', 'ar': 'All-rounders', 'bowl': 'Bowlers', 'sub': 'Substitute'}
NEXT_ROLES = {'bat': 'wk', 'wk': 'ar', 'ar': 'bowl', 'bowl': 'sub'}
DB_FILE = "crickteam11.db"
MIN_WITHDRAWAL = 200
MATCHES = {}

# Load environment variables from .env file (for local development)
load_dotenv()

# 1. Token aur Admin ID - Priority to environment variables
TOKEN = os.getenv('BOT_TOKEN', '').strip()
ADMIN_ID = os.getenv('ADMIN_ID', '').strip()

# 2. Webhook Host detection
WEBHOOK_HOST = os.getenv('WEBHOOK_URL') or os.getenv('RENDER_EXTERNAL_URL') or ""

if not TOKEN or not ADMIN_ID:
    logging.error("❌ CRITICAL: BOT_TOKEN or ADMIN_ID is missing!")
    raise ValueError("Missing essential Environment Variables.")

# Bot Identity Check
try:
    bot_user = telebot.TeleBot(TOKEN).get_me()
    logging.info(f"🤖 Bot Connected: @{bot_user.username} (ID: {bot_user.id})")
except Exception as e:
    logging.error(f"❌ Invalid Bot Token: {e}")
    raise

def get_now():
    """Returns current time in IST (Indian Standard Time)"""
    if IST:
        return datetime.now(IST).replace(tzinfo=None)
    return datetime.now() + timedelta(hours=5, minutes=30)

def get_payment_channel():
    return db.db_get_setting('PAYMENT_CHANNEL_ID', os.getenv('PAYMENT_CHANNEL_ID', ADMIN_ID))

PAYMENT_UPI = os.getenv('PAYMENT_UPI', "amankumar8879@ibl")

# Flask Server Setup
server = Flask(__name__)
bot = telebot.TeleBot(TOKEN)

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
        try:
            json_string = request.get_data().decode('utf-8')
            logging.info(f"📩 Incoming Update ID: {json.loads(json_string).get('update_id')}")
            update = telebot.types.Update.de_json(json_string)
            if update:
                bot.process_new_updates([update])
            return '', 200
        except Exception as e:
            logging.error(f"❌ Webhook Error: {e}")
            return '', 200 # Telegram ko 200 bhein taaki wo retry na kare
    else:
        abort(403)

def get_support_handle():
    return db.db_get_setting('SUPPORT_HANDLE', 'crick_support001')

def get_channel_handle():
    return db.db_get_setting('CHANNEL_HANDLE', 'crick_channel001')

def is_admin(user_id):
    """Checks if the given user_id is the authorized administrator"""
    return str(user_id) == str(ADMIN_ID)

def sync_matches_from_db():
    """Database se matches load karke global MATCHES dict mein dalta hai"""
    global MATCHES
    
    # 1. First, pull new matches from Google Sheets and update Local DB
    sheet_matches = sheets.get_all_rows_safe("MATCHES")
    if sheet_matches:
        for m in sheet_matches:
            db.db_add_match(m['match_id'], m['name'], m['type'], m['deadline'])

    # 2. Then load everything from Local DB to Bot Memory
    db_matches = db.db_get_matches()
    for m in db_matches:
        try:
            MATCHES[m['match_id']] = {
                'name': m['name'],
                'type': m['type'],
                'deadline': datetime.strptime(m['deadline'], '%Y-%m-%d %H:%M').replace(tzinfo=None)
            }
        except Exception as e:
            logging.error(f"Error parsing match {m['match_id']}: {e}")

# Memory cache to make UI interaction lightning fast
PLAYERS_CACHE = {}

def get_players(match_id):
    """Database se players fetch karke role-wise dictionary return karega (With Cache)"""
    if match_id in PLAYERS_CACHE:
        return PLAYERS_CACHE[match_id]
        
    db_players = db.db_get_players_by_match(match_id)
    formatted_data = {r: [] for r in ROLES}
    
    for p in db_players:
        role = p.get('role', '').lower()
        if role in formatted_data:
            formatted_data[role].append(p['player_name'])
    
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
    """Checks if the match deadline has passed"""
    deadline = MATCHES.get(match_id, {}).get('deadline', datetime.now())
    return get_now() > deadline

def get_time_left(match_id='m1'):
    """Returns countdown string until lock"""
    match_info = MATCHES.get(match_id)
    if not match_info: return "N/A"
    deadline = match_info.get('deadline', datetime.now())
    delta = deadline - get_now()
    if delta.total_seconds() <= 0: return "LOCKED 🔒"
    return f"{delta.days}d {delta.seconds//3600}h {(delta.seconds//60)%60}m"

# ===================================================
# INITIALIZATION
# ===================================================
db.init_db()
db.run_migrations()
sync_matches_from_db()

def setup_webhook():
    """Sets up the webhook for production environments"""
    if WEBHOOK_HOST:
        clean_host = WEBHOOK_HOST.strip().strip('/')
        if not clean_host.startswith('http'):
            clean_host = f"https://{clean_host}"
        
        webhook_url = f"{clean_host}/bot-webhook"
        logging.info(f"⚙️ Webhook Sync: Target URL is {webhook_url}")
        
        try:
            current_info = bot.get_webhook_info()
            if not current_info.url or current_info.url.strip('/') != webhook_url.strip('/'):
                bot.remove_webhook()
                time.sleep(1)
                bot.set_webhook(url=webhook_url, drop_pending_updates=True, allowed_updates=["message", "callback_query"])
                logging.info(f"🚀 Webhook successfully set to: {webhook_url}")
            else:
                logging.info("✅ Webhook already configured correctly.")
        except Exception as e:
            logging.error(f"❌ Webhook Setup Error: {e}")

# Trigger Webhook Setup during module load for Gunicorn
if os.getenv('RENDER') or (WEBHOOK_HOST and len(WEBHOOK_HOST) > 10):
    if not WEBHOOK_HOST and os.getenv('RENDER_EXTERNAL_URL'):
        WEBHOOK_HOST = os.getenv('RENDER_EXTERNAL_URL')
    
    setup_webhook()

# Only run webhook setup if NOT in local testing mode
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
    """Persist to SQLite and invalidate cache for consistency"""
    try:
        with db.get_db() as conn:
            team_json = json.dumps({k: team_data.get(k, []) for k in ROLES})
            conn.execute("""
                INSERT INTO TEAMS (user_id, match_id, team_num, team_players, captain, vice_captain, team_saved, is_paid) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id, match_id, team_num) 
                DO UPDATE SET team_players = EXCLUDED.team_players, captain = EXCLUDED.captain, 
                              vice_captain = EXCLUDED.vice_captain, team_saved = EXCLUDED.team_saved, is_paid = EXCLUDED.is_paid
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
    logging.info(f"🚀 /start received from {message.from_user.id}")
    uid = str(message.from_user.id)

    # Check if this is a brand new user
    existing_user = db.db_get_user(uid)
    is_new_registration = existing_user is None

    # 🔗 Referral Detection (Format: /start ref12345)
    referrer = None
    if len(message.text.split()) > 1:
        ref_data = message.text.split()[1]
        if ref_data.startswith('ref'):
            potential_ref = ref_data.replace('ref', '')
            if potential_ref.isdigit() and potential_ref != uid:
                referrer = potential_ref

    db.db_create_user(uid, message.from_user.username, message.from_user.first_name)
    
    # Sheets sync
    sheets.sync_wrapper({
        "user_id": uid,
        "username": message.from_user.username or "N/A",
        "first_name": message.from_user.first_name or "N/A",
        "paid": 0,
        "balance": 0,
        "joined_date": get_now().strftime('%Y-%m-%d %H:%M:%S')
    }, "USERS")

    if referrer and is_new_registration:
        with db.get_db() as conn:
            conn.execute("UPDATE USERS SET referred_by = %s WHERE user_id = %s AND referred_by IS NULL", (referrer, uid))
    
    db.db_update_last_seen(uid)
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add("🏆 CONTEST", "💰 WALLET", "⚾ MY TEAM", "👥 LEADERBOARD", "📊 STATS", "ℹ️ HELP")

    # Channel Link Logic
    c_handle = get_channel_handle().replace('@', '').strip()
    c_url = f"https://t.me/{c_handle}"

    inline_markup = types.InlineKeyboardMarkup()
    inline_markup.add(types.InlineKeyboardButton("📢 JOIN OFFICIAL CHANNEL", url=c_url))
    inline_markup.add(types.InlineKeyboardButton("📋 COPY USERNAME", callback_data=f"copy_channel_handle_{c_handle}"))

    brief = (
        f"🏏 <b>Welcome to CrickTEAM11, {message.from_user.first_name}!</b> 🚀\n\n"
        "Aap India ke sabse <b>TRUSTED</b> aur <b>HIGH-PAYOUT</b> platform par hain! \n\n"
        "🔥 <b>Hum kyu behtar hain?</b>\n"
        "• Dream11 apps 30% commission lete hain, par hum <b>10% se bhi kam</b>! 📉\n"
        "• Hum <b>90% se zyada Prize Pool</b> wapas winners ko dete hain! 🤑\n"
        "• <b>5,000+ Users</b> roz Hazaro jeet rahe hain! ✅\n"
        "• <b>Fast Payout:</b> UPI par sirf 10 min mein! ⚡\n\n"
        "📋 <b>Kaise Khele:</b>\n"
        "1️⃣ ⚾ <b>MY TEAM</b> mein 11 best players chunein.\n"
        "2️⃣ Captain (2x) & Vice-Captain (1.5x) chunein.\n"
        "3️⃣ 🏆 <b>CONTEST</b> join karein aur jeetna shuru karein!\n\n"
        "⚠️ <b>Niyam (Rules):</b>\n"
        "• Match start hote hi team lock ho jayegi.\n"
        f"• Min Withdrawal: ₹{MIN_WITHDRAWAL}\n"
        "• /rules se scoring system check karein.\n\n"
        "❓ Madad ke liye /help bhein ya niche button dabayein 👇"
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
    parts = call.data.split("_")
    match_id = parts[2]
    page = int(parts[3]) if len(parts) > 3 else 1
    uid = str(call.from_user.id)
    
    # Immediate feedback to avoid "laggy" button feel
    bot.answer_callback_query(call.id)

    markup = types.InlineKeyboardMarkup(row_width=5)
    buttons = []
    
    # Paginated slots (10 per page)
    start_idx = (page - 1) * 10 + 1
    end_idx = start_idx + 10
    
    for i in range(start_idx, end_idx):
        t = db_get_team(uid, match_id, i)
        label = f"T{i}✅" if t and t.get('team_saved') else f"T{i}"
        cb = f"view_team_{match_id}_{i}" if t and t.get('team_saved') else f"nav_bat_{match_id}_{i}"
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
        
        for player in get_players(match_id)[role]:
            status = "✅" if player in selected else "⬜"
            callback = f"sel_{match_id}_{team_num}_{role}_{player.replace(' ', '_')}"
            markup.add(types.InlineKeyboardButton(f"{status} {player}", callback_data=callback))
        
        nav_row = []
        if role != 'bat':
            for r, next_r in NEXT_ROLES.items():
                if next_r == role:
                    nav_row.append(types.InlineKeyboardButton("⬅️ Back", callback_data=f"nav_{r}_{match_id}_{team_num}"))
                    break
        
        if role == 'bowl':
            nav_row.append(types.InlineKeyboardButton("✅ SAVE TEAM", callback_data=f"team_save_{match_id}_{team_num}"))
        else:
            next_role = NEXT_ROLES[role]
            nav_row.append(types.InlineKeyboardButton("➡️ Next", callback_data=f"nav_{next_role}_{match_id}_{team_num}"))
        
        markup.row(*nav_row)
        
        time_left = get_time_left(match_id)
        role_min, role_max = ROLE_LIMITS[role]
        
        # UX: Clear Progress Indicators
        step = 1 if role == 'bat' else 2 if role == 'wk' else 3 if role == 'ar' else 4
        text = f"🏏 *Match:* {MATCHES[match_id]['name']} (T{team_num})\n"
        text += f"📝 *Step {step}/4: Select {ROLE_NAMES[role]}*\n\n"
        text += f"✅ Selected: {len(selected)}/{role_max} (Min: {role_min})\n"
        text += f"🏟 Total Squad: {total}/11\n"
        text += f"⏳ Deadline: {time_left}"

        if message_id:
            bot.edit_message_text(text, chat_id, message_id, reply_markup=markup, parse_mode='Markdown')
        else:
            bot.send_message(chat_id, text, reply_markup=markup, parse_mode='Markdown')
    except Exception as e:
        logging.error(f"Error in show_player_selection: {e}")
        bot.send_message(chat_id, f"❌ Error: {str(e)[:100]}")

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
    
    main_11 = get_total_players(team)
    if main_11 != 11:
        bot.answer_callback_query(call.id, f"❌ Starting 11 mein 11 players hone chahiye (Abhi: {main_11})")
        return
    
    for role, (r_min, r_max) in ROLE_LIMITS.items():
        count = len(team.get(role, []))
        if not (r_min <= count <= r_max):
            bot.answer_callback_query(call.id, f"❌ {ROLE_NAMES[role]} must be between {r_min}-{r_max}!")
            return
    
    # Strict C/VC Validation before final save
    if not team.get('captain') or not team.get('vice_captain'):
        bot.answer_callback_query(call.id, "⚠️ Please select Captain & VC first!", show_alert=True)
        # Redirect to C/VC menu directly
        callback_cv_menu(call)
        return

    team['team_saved'] = 1
    db_save_team(uid, team, match_id, team_num) # Persistent Save

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

    summary = f"🎉 *TEAM {team_num} SAVED!*\n\n"
    for role in ROLES:
        players = team.get(role, [])
        if players:
            summary += f"*{ROLE_NAMES[role]}:* {len(players)}\n"
    
    c = team.get('captain')
    vc = team.get('vice_captain')
    summary += f"\n👑 C: {c if c else '❌'}\n⭐ VC: {vc if vc else '❌'}"
    
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(types.InlineKeyboardButton("👑 SELECT CAPTAIN/VC", callback_data=f"set_cv_menu_{match_id}_{team_num}"))
    markup.add(types.InlineKeyboardButton("🔙 BACK TO SLOTS", callback_data=f"team_slots_{match_id}"))
    
    bot.edit_message_text(summary, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='Markdown')
    bot.answer_callback_query(call.id, "✅ Team Saved!")

@bot.callback_query_handler(func=lambda call: call.data.startswith("view_team_"))
def callback_view_team(call):
    parts = call.data.split("_")
    match_id, team_num = parts[2], int(parts[3])
    uid = str(call.from_user.id)
    
    team = db_get_team(uid, match_id, team_num)
    if not team:
        bot.answer_callback_query(call.id, "Team not found!")
        return

    text = f"⚾ *Team {team_num} Summary - {MATCHES[match_id]['name']}*\n\n"
    for role in ROLES:
        players = team.get(role, [])
        if players:
            text += f"*{ROLE_NAMES[role]}:* {', '.join(players)}\n"
    
    text += f"\n👑 C: {team.get('captain', '❌')}\n⭐ VC: {team.get('vice_captain', '❌')}"
    text += f"\n💰 Paid: {'✅ YES' if team.get('is_paid') else '❌ NO'}"

    markup = types.InlineKeyboardMarkup(row_width=1)
    if not is_match_locked(match_id):
        markup.add(types.InlineKeyboardButton("✏️ EDIT TEAM", callback_data=f"nav_bat_{match_id}_{team_num}"))
    markup.add(types.InlineKeyboardButton("🔙 BACK TO SLOTS", callback_data=f"team_slots_{match_id}"))
    
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
    role = parts[1]
    match_id = parts[2]
    team_num = int(parts[3])
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
    
    all_players = []
    for role in ['bat', 'wk', 'ar', 'bowl']:
        all_players.extend(team.get(role, []))
    
    markup = types.InlineKeyboardMarkup(row_width=2)
    for p in all_players:
        markup.add(
            types.InlineKeyboardButton(f"👑 C: {p}", callback_data=f"cv_{match_id}_{team_num}_c_{p.replace(' ', '_')}"),
            types.InlineKeyboardButton(f"⭐ VC: {p}", callback_data=f"cv_{match_id}_{team_num}_vc_{p.replace(' ', '_')}")
        )
    markup.add(types.InlineKeyboardButton("🔙 BACK", callback_data=f"team_save_{match_id}_{team_num}"))
    
    bot.edit_message_text("🎯 *Select Captain (2x) and Vice-Captain (1.5x)*", 
                         call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='Markdown')

@bot.callback_query_handler(func=lambda call: call.data.startswith("cv_"))
def callback_set_cv(call):
    uid = str(call.from_user.id)
    parts = call.data.split("_")
    match_id = parts[1]
    team_num = int(parts[2])
    type_cv = parts[3] # 'c' or 'vc'
    name = "_".join(parts[4:]).replace('_', ' ')
    
    team = db_get_team(uid, match_id, team_num)
    if type_cv == 'c':
        team['captain'] = name
    else:
        team['vice_captain'] = name
    
    db_save_team(uid, team, match_id, team_num)
    bot.answer_callback_query(call.id, f"{'Captain' if type_cv=='c' else 'VC'} set to {name}")
    
    # Manually rebuilding a fake call to trigger team_save view
    call.data = f"team_save_{match_id}_{team_num}"
    callback_team_save(call)

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
    
    text = "🏆 *UPCOMING MATCHES*\n\nSelect a match to join contests:"
    if not has_any_team:
        text += "\n\n⚠️ *IMP:* Aapne abhi tak koi team nahi banayi hai.\n👉 Pehle /myteam command se team banayein, phir contest join karein."
        markup.add(types.InlineKeyboardButton("⚾ CREATE TEAM NOW", callback_data="cmd_my_team_nav"))

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
    markup = types.InlineKeyboardMarkup(row_width=4)
    buttons = []
    found_any = False
    for i in range(1, 51):
        t = db_get_team(uid, match_id, i)
        if t and t.get('team_saved'):
            found_any = True
            # Indicate if already paid for this slot
            label = f"T{i} 💳" if t.get('is_paid') else f"T{i}"
            buttons.append(types.InlineKeyboardButton(label, callback_data=f"confirm_join_{match_id}_{i}_{fee}"))
    
    if not found_any:
        bot.answer_callback_query(call.id, "❌ No saved teams for this match! Use 'MY TEAM' first.", show_alert=True)
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
    
    info = MATCHES.get(mid)
    stats = db.get_contest_stats(mid)
    user_summary = db.get_user_match_summary(uid, mid)
    has_team = db_has_saved_team(uid, mid)
    time_left = get_time_left(mid)
    
    # Dynamically fetch all configured contests for this match
    configs = db.db_get_all_contest_configs(mid)

    markup, text = ui.match_dashboard_render(mid, info, stats, user_summary, time_left, configs)
    bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='Markdown')

@bot.message_handler(commands=['set_contest_size'])
def cmd_set_contest_size(msg):
    if not is_admin(msg.from_user.id): return
    help_txt = "📏 *SET CONTEST SIZE*\n\nFormat: `match_id | entry_fee | max_slots`\nExample: `m1 | 100 | 200`"
    sent = bot.send_message(msg.chat.id, help_txt, parse_mode='Markdown')
    bot.register_next_step_handler(sent, process_contest_size)

def process_contest_size(msg):
    try:
        parts = [p.strip() for p in msg.text.split("|")]
        mid, fee, slots = parts[0], int(parts[1]), int(parts[2])
        db.db_set_contest_config(mid, fee, slots)
        bot.reply_to(msg, f"✅ *Contest Configured!*\nMatch: `{mid}`\nFee: ₹{fee}\nMax Slots: {slots}\n\nAb users ko 70% winners wala breakup dikhega.")
    except Exception as e:
        bot.reply_to(msg, "❌ Error! Use format: `mid | fee | slots`")

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
        amount = user_deposit_amount.get(uid, 100)
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
    user_deposit_amount[uid] = amount
    user_active_match[uid] = context

    pay_msg = (
        "💳 *PAYMENT VERIFICATION METHOD*\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"📱 *UPI ID:* `{PAYMENT_UPI}`\n"
        f"💰 *Payable Amount:* `₹{amount}`\n"
        f"🆔 *Order ID:* `{order_id}`\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "✨ *Aapke paas 2 options hain:*\n\n"
        "1️⃣ *UTR ID (Fastest):* ⚡\n"
        "Payment ke baad 12-digit ka **UTR Number** yahan chat mein likhein. System turant check karke balance add kar dega. ✅\n\n"
        "2️⃣ *SCREENSHOT (Manual):* ⏳\n"
        "Screenshot bhejne par Admin manual check karega (5-10 mins). Photo niche bhein.\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "❓ *UTR Kaise Milega?*\n"
        "App ki 'History' ya 'Transaction Details' mein 12-digit ka *UTR/Ref No.* dekhein.\n\n"
        "📝 *Note:* GPay/PhonePe ke 'Add Note' ya 'Message' section mein Order ID `{order_id}` zaroor likhein."
    )
    if match_id != "wallet":
        pay_msg += f"\n\n🏏 Match: {MATCHES[match_id]['name']}\n⚾ Team Slot: {team_num}"
    
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
                bot.reply_to(msg, f"❌ *No Active Order:* Pehle 'ADD MONEY' par click karein aur ₹{user_deposit_amount.get(uid, '')} pay karein, uske baad UTR bhein.\n\n⚠️ Caution: Galat UTR par ID block ho sakti hai. (Attempts left: {3-failed_count})")
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
                conn.execute("UPDATE PAYMENT_INTENTS SET status='completed' WHERE id=%s", (intent['id'],))
                bot.reply_to(msg, f"✅ *UTR VERIFIED!*\n\n₹{intent['amount']} added to wallet.\nRef: {utr}", parse_mode='Markdown')
                
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
        conn.execute("SELECT amount, id FROM PAYMENTS WHERE user_id=%s AND match_id=%s AND status='pending' ORDER BY timestamp DESC", (uid, mid))
        pay_row = conn.fetchone()
        if not pay_row:
            bot.edit_message_caption(caption="❌ No pending request found.", chat_id=call.message.chat.id, message_id=call.message.message_id)
            return

        amount = pay_row['amount']
        ref = f"MANUAL_{pay_row['id']}_{int(time.time())}"
        
        success, _ = process_payment_success(uid, amount, ref, f"{mid}_{tnum}", conn=conn)
        if success:
            conn.execute("UPDATE PAYMENTS SET status='completed' WHERE id=%s", (pay_row['id'],))
            bot.send_message(uid, f"🎉 *PAYMENT APPROVED!*\n₹{amount} credited to ledger.", parse_mode='Markdown')
            bot.edit_message_caption(caption=f"✅ APPROVED (₹{amount})", chat_id=call.message.chat.id, message_id=call.message.message_id)

@bot.callback_query_handler(func=lambda call: call.data.startswith("reject_"))
def callback_reject(call):
    uid = call.data.split("_")[1]
    bot.answer_callback_query(call.id)
    
    try:
        with db.get_db() as conn:
            conn.execute("UPDATE PAYMENTS SET status='rejected' WHERE user_id=%s AND status='pending'", (uid,))
        
        user = db.db_get_user(uid)
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("💳 TRY AGAIN", callback_data="init_deposit"))

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

    text = f"💰 *WALLET*\n\n"
    text += f"👤 *User:* {user['first_name']}\n"
    text += f"💵 *Available Balance:* ₹{balance}\n"
    text += (
        f"\n🎁 *REFER & EARN*\n"
        f"Doston ko invite karein aur payein *₹10 Bonus*! 💸\n"
        f"_(Condition: Dost pehla contest join kare)_\n\n"
        f"🔗 *Link:* `{ref_link}`"
    )

    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(types.InlineKeyboardButton("➕ ADD MONEY", callback_data="init_deposit"))
    if balance >= MIN_WITHDRAWAL:
        markup.add(types.InlineKeyboardButton("🏧 WITHDRAW MONEY", callback_data="req_withdraw"))
    else:
        text += f"\n\n_⚠️ Min Withdrawal: ₹{MIN_WITHDRAWAL}_"

    markup.add(types.InlineKeyboardButton("🚀 SHARE LINK", switch_inline_query=f"India's best Fantasy Bot! 🏏 Join & Win: {ref_link}"))
    
    bot.send_message(msg.chat.id, text, reply_markup=markup, parse_mode='Markdown')

@bot.callback_query_handler(func=lambda call: call.data == "init_deposit")
def callback_init_deposit(call):
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("❌ CANCEL PAYMENT", callback_data="payment_cancel"))
    msg = bot.send_message(call.message.chat.id, "💰 *Enter amount* to add to your wallet:\n(Min: ₹10, Max: ₹50,000)", reply_markup=markup, parse_mode='Markdown')
    bot.register_next_step_handler(msg, process_deposit_input)

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
        # Input string split: "binod@oksbi 500" -> ["binod@oksbi", "500"]
        parts = msg.text.split()
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

        bot.reply_to(msg, f"✅ *Request Submitted!*\n💰 Amount: ₹{amount}\n🏦 UPI: `{upi_id}`\n\nAdmin 10-15 minute mein verify karke paise bhej dega aur payment ka screenshot yahi share karega.")

        # Admin notification with buttons
        admin_markup = types.InlineKeyboardMarkup()
        admin_markup.add(
            types.InlineKeyboardButton("✅ APPROVE", callback_data=f"wd_approve_{req_id}"),
            types.InlineKeyboardButton("❌ REJECT", callback_data=f"wd_reject_{req_id}")
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
        if not req or req['status'] != 'pending': 
            bot.answer_callback_query(call.id, "Already processed!")
            return

        if action == "approve":
            # Wallet se amount debit (minus) karein
            ref = f"WD_REF_{req_id}"
            conn.execute("INSERT INTO LEDGER (user_id, amount, type, reference_id, timestamp) VALUES (%s, %s, 'DEBIT', %s, %s)",
                         (req['user_id'], -req['amount'], ref, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            conn.execute("UPDATE WITHDRAWALS SET status='approved' WHERE id=%s", (req_id,))
            bot.edit_message_text(f"✅ Approved: ₹{req['amount']} to {req['user_id']}", get_payment_channel(), call.message.message_id)
            bot.send_message(req['user_id'], f"✅ *Withdrawal Success!*\n₹{req['amount']} aapke UPI `{req['upi_id']}` par bhej diye gaye hain. Admin abhi screenshot *Support Channel* par share kar raha hai.")
        else:
            conn.execute("UPDATE WITHDRAWALS SET status='rejected' WHERE id=%s", (req_id,))
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
    s_handle = get_support_handle().replace('@', '').replace('\\', '').strip()
    c_handle = get_channel_handle().replace('@', '').replace('\\', '').strip()
    
    support_url = f"https://t.me/{s_handle}"
    channel_url = f"https://t.me/{c_handle}"

    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("📢 Main Channel", url=channel_url))
    markup.add(types.InlineKeyboardButton("📸 Support & Screenshots", url=support_url))
    
    help_text = (
        "❓ <b>HELP CENTER</b>\n\n"
        "⚾ /myteam - Build/Edit Team\n"
        "🏆 /contest - Join Matches\n"
        "💰 /wallet - Deposit & Payout\n"
        "📊 /stats - Your Points\n"
        "📜 /rules - Scoring System\n"
        "📸 <b>Note:</b> Winner payout ke screenshots Support Channel par milenge.\n\n"
        f"📞 Support ID: <code>@{s_handle}</code>\n"
        f"📢 Channel ID: <code>@{c_handle}</code>\n\n"
        "☝️ <i>Upar diye gaye ID par tap karke copy karein. Agar link kaam na kare toh Telegram search mein paste karein.</i>"
    )
    bot.send_message(msg.chat.id, help_text, reply_markup=markup, parse_mode='HTML', disable_web_page_preview=True)

@bot.message_handler(commands=['set_handle'])
def cmd_set_handle(msg):
    if str(msg.from_user.id) != ADMIN_ID: return

    # ⚡ Check for one-line command (e.g. /set_handle PAYMENT_ID | -100...)
    if "|" in msg.text:
        msg.text = re.sub(r'^/\w+\s*', '', msg.text) # Strip command prefix
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

def process_handle_setting(msg):
    """Admin input handle karne ke liye jo Support ya Channel badalta hai"""
    try:
        if "|" not in msg.text:
            return bot.reply_to(msg, "❌ Invalid Format! Use `TYPE | VALUE`")
        parts = [p.strip() for p in msg.text.split("|")]
        # Proper Cleanup: Remove @ and backslashes in one go
        h_type, value = parts[0].upper(), parts[1].replace("@", "").replace("\\", "").strip()
        if h_type == "SUPPORT":
            db.db_set_setting('SUPPORT_HANDLE', value)
            bot.reply_to(msg, f"✅ Support handle updated to: @{value}")
        elif h_type == "CHANNEL":
            db.db_set_setting('CHANNEL_HANDLE', value)
            bot.reply_to(msg, f"✅ Channel handle updated to: @{value}")
        elif h_type == "PAYMENT_ID":
            db.db_set_setting('PAYMENT_CHANNEL_ID', value)
            bot.reply_to(msg, f"✅ Payment Verification Channel ID updated to: {value}")
        else:
            bot.reply_to(msg, "❌ Invalid Type! Use `SUPPORT`, `CHANNEL`, or `PAYMENT_ID`.")
    except:
        bot.reply_to(msg, "❌ Error! Format: `TYPE | HANDLE`")

@bot.message_handler(commands=['rules'])
def cmd_rules(msg):
    """Users ko scoring system samjhane ke liye"""
    rules = (
        "📊 *CRICKTEAM11 SCORING SYSTEM*\n\n"
        "🏏 Run: +1 | 4s: +4 | 6s: +6\n"
        "⚽ Wicket: +25 | Maiden: +10\n"
        "👑 C: 2x | VC: 1.5x\n\n"
        "🚫 *Match Lock:* Match start hone par team edit nahi hogi.\n"
        f"🏧 *Withdrawal:* Minimum ₹{MIN_WITHDRAWAL}"
    )
    bot.send_message(msg.chat.id, rules, parse_mode='Markdown')

# ===================================================
# DEBUG COMMANDS
# ===================================================

@bot.message_handler(commands=['test_sync'])
def cmd_test_sync(msg):
    """Google Sheets connection verify karne ke liye"""
    if str(msg.from_user.id) != ADMIN_ID:
        return
    bot.send_message(msg.chat.id, "🧪 Testing Google Sheets sync... Check console and your Sheet.")
    sheets.sync_wrapper({
        "user_id": str(msg.from_user.id),
        "username": msg.from_user.username or "N/A",
        "first_name": "SyncTest_User",
        "paid": 0,
        "balance": 999,
        "joined_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }, "USERS")

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

def handle_selection(call):
    uid = str(call.from_user.id)
    parts = call.data.split("_")
    match_id, team_num, role = parts[1], int(parts[2]), parts[3]
    player_name = " ".join(parts[4:])
    cache_key = (uid, match_id, team_num)

    if is_match_locked(match_id):
        bot.answer_callback_query(call.id, "🚫 Match is Locked!", show_alert=True)
        return

    # Answer immediately with NO text to make the interface feel instant
    bot.answer_callback_query(call.id)

    # ⚡ CACHE UPDATE ONLY - NO DATABASE WRITE
    team = db_get_team(uid, match_id, team_num) # Hydrates cache if empty
    if not team: team = {k: [] for k in ROLES}
    
    selected = team.get(role, [])
    _, role_max = ROLE_LIMITS[role]

    if player_name in selected:
        selected.remove(player_name)
    else:
        if role != 'sub' and get_total_players(team) >= 11:
            return

        if len(selected) >= role_max:
            return
        selected.append(player_name)

    # Update memory cache
    temp_team_cache[cache_key] = team
    # Refresh UI instantly
    show_player_selection(call.message.chat.id, uid, role, match_id, team_num, call.message.message_id)

@bot.callback_query_handler(func=lambda call: True)
def callback_catchall(call):
    # Route Admin commands
    if call.data.startswith("adm_"):
        if not is_admin(call.from_user.id):
            bot.answer_callback_query(call.id, "🚫 Unauthorized!", show_alert=True)
            return
        import admin_app # Lazy import to avoid circular dependency
        admin_app.handle_admin_nav(call, bot)
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
        bot.send_message(msg.chat.id, text, reply_markup=markup, parse_mode='Markdown')
    except Exception as e:
        logging.error(f"Admin help error: {e}")

@bot.message_handler(commands=['download_db'])
def cmd_download_db(msg):
    if not is_admin(msg.from_user.id):
        return
    try:
        if os.path.exists(DB_FILE):
            with open(DB_FILE, 'rb') as f:
                bot.send_document(msg.chat.id, f, caption=f"📂 Database Backup: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            bot.reply_to(msg, "❌ Database file not found!")
    except Exception as e:
        logging.error(f"Error downloading DB: {e}")

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
    
    broadcast_text = msg.text
    if not broadcast_text:
        bot.send_message(ADMIN_ID, "❌ Broadcast message cannot be empty.")
        return

    bot.send_message(ADMIN_ID, "🚀 Starting broadcast... This may take a while.")
    
    success_count = 0
    fail_count = 0
    with db.get_db() as conn:
        conn.execute("SELECT user_id FROM USERS")
        users = conn.fetchall()
        for user_row in users:
            try:
                bot.send_message(user_row['user_id'], broadcast_text, parse_mode='Markdown')
                success_count += 1
                time.sleep(0.05) # Small delay to avoid hitting Telegram API limits
            except telebot.apihelper.ApiTelegramException as e:
                logging.warning(f"Failed to send broadcast to user {user_row['user_id']}: {e}")
                fail_count += 1
    
    bot.send_message(ADMIN_ID, f"✅ Broadcast finished!\n\nSent to {success_count} users.\nFailed for {fail_count} users (likely blocked the bot).")

# ===================================================
# POINTS CALCULATION SYSTEM
# ===================================================

# This function is primarily for manual /up command or final calculation
def calculate_all_points(match_id, player_scores):
    """
    match_id: Specific match target
    player_scores: Dictionary of {'Player Name': score}
    """
    try:
        with db.get_db() as conn:
            conn.execute("SELECT * FROM TEAMS WHERE match_id = %s", (match_id,))
            teams_rows = conn.fetchall()
            results = []
            
            for row in teams_rows:
                uid = row['user_id']
                team_data = json.loads(row['team_players'])
                captain = row['captain']
                vice_captain = row['vice_captain']
                
                total_pts = 0
                # Har category ke players ke points jodo
                # Include 'sub' in point calculation
                for role in ['bat', 'wk', 'ar', 'bowl', 'sub']:
                    for p in team_data.get(role, []):
                        p_pts = player_scores.get(p, 0) # This comes from manual input
                        if p == captain:
                            total_pts += p_pts * scoring.CAPTAIN_MULTIPLIER  # Captain 2x
                        elif p == vice_captain:
                            total_pts += p_pts * scoring.VC_MULTIPLIER # VC 1.5x
                        else:
                            total_pts += p_pts
                
                # DB mein update karein
                conn.execute("UPDATE TEAMS SET points = %s WHERE user_id = %s AND match_id = %s AND team_num = %s", (total_pts, uid, row['match_id'], row['team_num']))
                results.append({'user_id': uid, 'points': total_pts})
            
            # Ranking calculate karein (Sorted by points)
            results.sort(key=lambda x: x['points'], reverse=True)
            
            for index, res in enumerate(results):
                rank = index + 1
                prize = "₹2000" if rank == 1 else "₹800" if rank == 2 else "₹400" if rank == 3 else "₹0"
                
                # Results Sheet mein sync karein
                sheets.sync_wrapper({
                    "contest_date": datetime.now().strftime('%Y-%m-%d'),
                    "user_id": res['user_id'],
                    "points": res['points'],
                    "rank": rank,
                    "prize": prize
                }, "RESULTS")
                
                # Notify User (Optional: only for top ranks to avoid spam)
                if rank <= 3:
                    bot.send_message(res['user_id'], f"🎊 *CONGRATS!*\n\nAapka rank *#{rank}* hai with *{res['points']}* points!\nPrize: {prize}\n\n📸 *Note:* Payout hote hi payment ka screenshot yahi share kiya jayega.", parse_mode='Markdown')
                
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
        "`ID | Name | Type | YYYY-MM-DD HH:MM`\n\n"
        "Example:\n"
        "`m5 | RR vs PBKS | IPL T20 | 2026-04-24 19:30`"
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
        PLAYERS_CACHE.pop(mid, None) # Clear cache for this match
        sync_matches_from_db() # Refresh memory cache
        
        bot.reply_to(msg, f"✅ *Match Added Successfully!*\n\n🏏 {name}\n⏰ Deadline: {deadline_str}\n\n⚠️ *Note:* Ab `/add_player` command use karke players add karein.")
    except Exception as e:
        bot.reply_to(msg, f"❌ Error: {e}\nFormat check karein: `YYYY-MM-DD HH:MM`")

@bot.message_handler(commands=['add_player'])
def cmd_add_player(msg):
    if not is_admin(msg.from_user.id): return
    help_text = (
        "👤 *ADD NEW PLAYER*\n\n"
        "Format: `match_id | player_name | role`\n"
        "Roles: `bat, wk, ar, bowl, sub`\n\n"
        "Example: `m3 | Rohit Sharma | bat`"
        "\n\n*Multiple players add karne ke liye, har player ko nayi line mein likhein:*\n"
        "`m5 | Player1 | bat\nm5 | Player2 | bowl\nm5 | Player3 | wk`"
    )
    sent = bot.send_message(msg.chat.id, help_text, parse_mode='Markdown')
    bot.register_next_step_handler(sent, process_player_addition)

def process_player_addition(msg):
    if not is_admin(msg.from_user.id): return
    try:
        lines = msg.text.strip().split('\n')
        added_players = []
        failed_players = []
        
        for line in lines:
            parts = [p.strip() for p in line.split("|")]
            if len(parts) < 3:
                failed_players.append(f"❌ Format galat: `{line}`")
                continue
                
            mid, name, role = parts[0], parts[1], parts[2].lower()
            if role not in ROLES:
                failed_players.append(f"❌ Galat role `{role}`: `{name}`")
                continue
                
            try:
                db.db_add_player(mid, name, role)
                PLAYERS_CACHE.pop(mid, None) # Invalidate cache
                added_players.append(f"✅ {name} ({role.upper()})")
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
            
        bot.send_message(msg.chat.id, response_text, parse_mode='Markdown')
    except Exception as e:
        bot.reply_to(msg, f"❌ Error: {e}")

@bot.message_handler(commands=['list_players'])
def cmd_list_players(msg):
    if not is_admin(msg.from_user.id): return
    parts = msg.text.split()
    if len(parts) < 2:
        bot.reply_to(msg, "Usage: `/list_players m1`", parse_mode='Markdown')
        return
        
    mid = parts[1]
    players = get_players(mid)
    
    text = f"📋 *PLAYER LIST - {mid}*\n━━━━━━━━━━━━━━━━━━━━\n"
    found = False
    for role in ROLES:
        p_list = players.get(role, [])
        if p_list:
            found = True
            text += f"\n*{ROLE_NAMES[role].upper()}:*\n• " + "\n• ".join(p_list) + "\n"
            
    if not found:
        text = f"❌ No players found for match ID: `{mid}`"
        
    bot.send_message(msg.chat.id, text, parse_mode='Markdown')

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
    """Short Method: /up m1 | Player1:10, Player2:20"""
    if not is_admin(msg.from_user.id):
        return

    try:
        input_data = msg.text.split(maxsplit=1)[1]
        if "|" not in input_data:
            bot.reply_to(msg, "⚠️ Use: `/up match_id | P1:10, P2:20`")
            return
            
        mid_part, scores_part = input_data.split("|")
        mid = mid_part.strip()
        
        if mid not in MATCHES:
            bot.reply_to(msg, f"❌ Match ID `{mid}` valid nahi hai! Pehle match check karein.")
            return
            
        scores = {p.split(':')[0].strip(): float(p.split(':')[1].strip()) for p in scores_part.split(',')}
        
        if calculate_all_points(mid, scores):
            bot.reply_to(msg, f"✅ Points updated for Match `{mid}`!")
        else:
            bot.reply_to(msg, "❌ Error calculating points.")
    except Exception as e:
        bot.reply_to(msg, "⚠️ Usage: `/up m1 | Kohli:50, Rohit:20`")

# ===================================================
# START BOT
# ===================================================

if __name__ == "__main__":
    # Local development ke liye polling use karein
    bot.remove_webhook()
    logging.info("🔄 Local Dev Detected: Starting Polling Mode...")
    bot.infinity_polling()
