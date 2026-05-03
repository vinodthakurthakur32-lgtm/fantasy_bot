import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv # Added for local .env support
import json
import logging
import secrets
from contextlib import contextmanager
from datetime import datetime, timedelta

# Load environment variables from .env file (for local development)
load_dotenv()

# Render provides the database URL in an environment variable
DATABASE_URL = os.getenv("DATABASE_URL")

# Simple In-memory cache for settings to improve speed
_settings_cache = {}
_manual_prizes_cache = {}

@contextmanager
def get_db():
    if not DATABASE_URL:
        logging.error("❌ DATABASE_URL is not set. Check your .env file.")
        raise ValueError("DATABASE_URL environment variable is missing.")
    # Connect to PostgreSQL
    try:
        # Explicitly setting sslmode=require for Supabase
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor, sslmode='require')
    except psycopg2.OperationalError as e:
        if "could not translate host name" in str(e):
            logging.error("❌ DNS Error: Cannot find the database host. Is your Supabase project PAUSED?")
            raise ConnectionError("Supabase project might be paused. Please check your Supabase dashboard.") from e
        else:
            logging.error(f"❌ Failed to connect to the database: {e}")
            raise
    try:
        with conn.cursor() as cur:
            yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def init_db():
    with get_db() as c:
        # 1. Create Tables (if not exist)
        c.execute('''CREATE TABLE IF NOT EXISTS USERS (
            user_id VARCHAR(255) PRIMARY KEY, username TEXT, first_name TEXT, joined_date TEXT,
            referred_by VARCHAR(255) DEFAULT NULL,
            last_seen TEXT DEFAULT 'N/A', 
            paid INTEGER DEFAULT 0,
            total_added NUMERIC DEFAULT 0,
            total_used NUMERIC DEFAULT 0,
            is_flagged INTEGER DEFAULT 0
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS TEAMS (
            user_id VARCHAR(255), match_id VARCHAR(255), team_players TEXT, captain TEXT, vice_captain TEXT,
            team_saved INTEGER DEFAULT 0, team_num INTEGER DEFAULT 1,
            is_paid INTEGER DEFAULT 0, points NUMERIC DEFAULT 0,
            PRIMARY KEY (user_id, match_id, team_num)
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS PAYMENTS (
            id SERIAL PRIMARY KEY, user_id VARCHAR(255), amount INTEGER,
            match_id VARCHAR(255), upi_txn_id TEXT, timestamp TEXT, status VARCHAR(50) DEFAULT 'pending'
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS PAYMENT_INTENTS (
            id SERIAL PRIMARY KEY, order_id VARCHAR(255), user_id VARCHAR(255), amount INTEGER,
            match_context TEXT, status TEXT DEFAULT 'pending', created_at TEXT, expires_at TEXT
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS USED_UTR (
            utr VARCHAR(255) PRIMARY KEY, user_id VARCHAR(255), amount INTEGER, timestamp TEXT
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS LEDGER (
            id SERIAL PRIMARY KEY, user_id VARCHAR(255), amount NUMERIC,
            type VARCHAR(50), reference_id VARCHAR(255) UNIQUE, timestamp TEXT
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS MATCHES_LIST (
            match_id VARCHAR(255) PRIMARY KEY, name TEXT, type TEXT, deadline TEXT,
            points_calculated INTEGER DEFAULT 0, manual_lock INTEGER DEFAULT 0,
            live_link TEXT
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS PLAYERS (
            id SERIAL PRIMARY KEY,
            match_id VARCHAR(255),
            player_name TEXT,
            role TEXT,
            team TEXT DEFAULT 'N/A',
            designation TEXT DEFAULT '',
            UNIQUE(match_id, player_name, team)
        )''')
        c.execute("CREATE INDEX IF NOT EXISTS idx_match_players ON PLAYERS(match_id)")
        c.execute('''CREATE TABLE IF NOT EXISTS FAILED_UTR_LOGS (
            user_id VARCHAR(255), utr TEXT, timestamp TEXT
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS WITHDRAWALS (
            id SERIAL PRIMARY KEY, user_id VARCHAR(255), amount NUMERIC,
            upi_id TEXT, status VARCHAR(50) DEFAULT 'pending', timestamp TEXT
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS CONTEST_CONFIG (
            match_id VARCHAR(255), entry_fee INTEGER, max_slots INTEGER,
            PRIMARY KEY (match_id, entry_fee)
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS MANUAL_PRIZES (
            match_id VARCHAR(255),
            entry_fee INTEGER,
            r1 INTEGER,
            r2 INTEGER,
            r3 INTEGER,
            r4_10 INTEGER,
            bottom INTEGER,
            winners_count INTEGER,
            PRIMARY KEY (match_id, entry_fee)
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS SETTINGS (
            key VARCHAR(255) PRIMARY KEY,
            value TEXT
        )''')
        # Track individual player performance per match
        c.execute('''CREATE TABLE IF NOT EXISTS PLAYER_LIVE_STATS (
            match_id VARCHAR(255),
            player_name TEXT,
            runs INTEGER DEFAULT 0,
            fours INTEGER DEFAULT 0,
            sixes INTEGER DEFAULT 0,
            wickets INTEGER DEFAULT 0,
            PRIMARY KEY (match_id, player_name)
        )''')
        # Audit log for every scoring event
        c.execute('''CREATE TABLE IF NOT EXISTS MATCH_EVENTS (
            id SERIAL PRIMARY KEY,
            match_id VARCHAR(255),
            player_name TEXT,
            event_type TEXT,
            points_awarded NUMERIC,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        # FIX 3: Add USER_STATE table for persistence
        c.execute('''CREATE TABLE IF NOT EXISTS USER_STATE (
            user_id VARCHAR(255),
            key VARCHAR(255),
            value TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (user_id, key)
        )''')

        # FEATURE 1: Add REMINDERS table
        c.execute('''CREATE TABLE IF NOT EXISTS REMINDERS (
            id SERIAL PRIMARY KEY,
            match_id TEXT,
            user_id TEXT,
            reminder_type TEXT,
            sent_at TEXT
        )''')
        # FEATURE 4: Add SUPPORT_TICKETS table
        c.execute('''CREATE TABLE IF NOT EXISTS SUPPORT_TICKETS (
            id SERIAL PRIMARY KEY,
            user_id TEXT,
            issue TEXT,
            status TEXT DEFAULT 'open',
            created_at TEXT,
            resolved_at TEXT
        )''')

def db_set_setting(key, value):
    with get_db() as c:
        c.execute("""
            INSERT INTO SETTINGS (key, value) VALUES (%s, %s)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        """, (key, str(value)))
    # Invalidate cache
    if key in _settings_cache:
        del _settings_cache[key]

def db_get_setting(key, default=None):
    if key in _settings_cache:
        return _settings_cache[key]
    with get_db() as c:
        c.execute("SELECT value FROM SETTINGS WHERE key=%s", (key,))
        row = c.fetchone()
        val = row['value'] if row else default
        _settings_cache[key] = val
        return val

def db_set_contest_config(mid, fee, slots, c_type='J'):
    with get_db() as c:
        c.execute("""
            INSERT INTO CONTEST_CONFIG (match_id, entry_fee, max_slots, contest_type) VALUES (%s, %s, %s, %s)
            ON CONFLICT (match_id, entry_fee) DO UPDATE SET max_slots = EXCLUDED.max_slots, contest_type = EXCLUDED.contest_type
        """, (mid, fee, slots, c_type))

def db_get_contest_config(mid, fee):
    with get_db() as c:
        c.execute("SELECT * FROM CONTEST_CONFIG WHERE match_id=%s AND entry_fee=%s", (mid, fee))
        return c.fetchone()

def db_set_manual_prizes(mid, fee, r1, r2, r3, r4_10, bottom, winners):
    with get_db() as c:
        c.execute("""
            INSERT INTO MANUAL_PRIZES (match_id, entry_fee, r1, r2, r3, r4_10, bottom, winners_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (match_id, entry_fee) DO UPDATE SET
            r1=EXCLUDED.r1, r2=EXCLUDED.r2, r3=EXCLUDED.r3, r4_10=EXCLUDED.r4_10, 
            bottom=EXCLUDED.bottom, winners_count=EXCLUDED.winners_count
        """, (mid, fee, r1, r2, r3, r4_10, bottom, winners))
    # Invalidate cache
    cache_key = f"{mid}_{fee}"
    if cache_key in _manual_prizes_cache:
        del _manual_prizes_cache[cache_key]

def db_get_manual_prizes(mid, fee):
    cache_key = f"{mid}_{fee}"
    if cache_key in _manual_prizes_cache:
        return _manual_prizes_cache[cache_key]
    with get_db() as c:
        c.execute("SELECT * FROM MANUAL_PRIZES WHERE match_id=%s AND entry_fee=%s", (mid, fee))
        res = c.fetchone()
        _manual_prizes_cache[cache_key] = res
        return res

def db_get_recent_users_stats(limit=10):
    """Fetches recently joined users and their team counts without file download"""
    with get_db() as c:
        c.execute("""
            SELECT u.user_id, u.first_name, u.username, u.joined_date,
            (SELECT COUNT(*) FROM TEAMS WHERE user_id = u.user_id AND team_saved=1) as team_count
            FROM USERS u
            ORDER BY u.joined_date DESC LIMIT %s
        """, (limit,))
        return c.fetchall()

def db_delete_contest(mid, fee):
    """Admin can remove a specific contest configuration"""
    with get_db() as c:
        c.execute("DELETE FROM CONTEST_CONFIG WHERE match_id=%s AND entry_fee=%s", (mid, fee))

def db_get_all_contest_configs(mid):
    with get_db() as c:
        c.execute("SELECT * FROM CONTEST_CONFIG WHERE match_id=%s", (mid,))
        return c.fetchall()

def db_cleanup_unpaid_teams(match_id):
    """Deletes all teams for a match that were never paid/joined"""
    with get_db() as c:
        c.execute("DELETE FROM TEAMS WHERE match_id=%s AND is_paid=0", (match_id,))
    logging.info(f"🧹 Cleanup: Unpaid teams removed for match {match_id}")

def db_add_match(mid, name, m_type, deadline, points_calculated=0, manual_lock=0):
    with get_db() as c:
        c.execute("""
            INSERT INTO MATCHES_LIST (match_id, name, type, deadline, points_calculated, manual_lock) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (match_id) DO UPDATE SET name = EXCLUDED.name, type = EXCLUDED.type, deadline = EXCLUDED.deadline, points_calculated = EXCLUDED.points_calculated, manual_lock = EXCLUDED.manual_lock
        """, (mid, name, m_type, deadline, points_calculated, manual_lock))

def db_get_matches():
    with get_db() as c:
        c.execute("SELECT * FROM MATCHES_LIST")
        return c.fetchall()

def run_migrations():
    with get_db() as c:
        migrations = {
            "USERS": [
                ("last_seen", "TEXT DEFAULT 'N/A'"),
                ("paid", "INTEGER DEFAULT 0"),
                ("is_flagged", "INTEGER DEFAULT 0"),
                ("referred_by", "VARCHAR(255) DEFAULT NULL")
            ],
            "PAYMENT_INTENTS": [
                ("order_id", "VARCHAR(255)")
            ],
            "TEAMS": [
                ("is_paid", "INTEGER DEFAULT 0"),
                ("points", "INTEGER DEFAULT 0")
            ],
            "MATCHES_LIST": [
                ("points_calculated", "INTEGER DEFAULT 0"), 
                ("manual_lock", "INTEGER DEFAULT 0"),
                ("live_link", "TEXT")
            ],
            "PLAYERS": [("team", "TEXT DEFAULT 'N/A'"), ("designation", "TEXT DEFAULT ''")],
            "CONTEST_CONFIG": [("contest_type", "VARCHAR(50) DEFAULT 'J'")]
        }
        for table, cols in migrations.items():
            c.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s", (table.lower(),))
            existing = [row['column_name'] for row in c.fetchall()]
            for col_name, col_def in cols:
                if col_name not in existing:
                    c.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_def}")
        
        # Constraints check for Postgres
        try:
            c.execute("ALTER TABLE PLAYERS DROP CONSTRAINT IF EXISTS players_match_id_player_name_key")
            c.execute("ALTER TABLE PLAYERS DROP CONSTRAINT IF EXISTS players_match_id_player_name_team_key")
            
            c.execute("DELETE FROM PLAYERS a USING PLAYERS b WHERE a.id < b.id AND a.match_id = b.match_id AND a.player_name = b.player_name AND COALESCE(a.team, 'N/A') = COALESCE(b.team, 'N/A')")
            c.execute("ALTER TABLE PLAYERS ADD CONSTRAINT players_match_id_player_name_team_key UNIQUE (match_id, player_name, team)")
        except Exception as e:
            logging.warning(f"Constraint migration info: {e}")

    logging.info("✅ Database Migrations completed.")

def db_get_user(user_id):
    with get_db() as c:
        c.execute("SELECT * FROM USERS WHERE user_id=%s", (str(user_id),))
        return c.fetchone()

def db_register_user_optimized(user_id, username, first_name):
    """
    Optimized user registration: Single connection check, create, and update last_seen.
    Returns (is_new_user, user_data)
    """
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    uid_str = str(user_id)
    with get_db() as c:
        # 1. Check if user exists
        c.execute("SELECT * FROM USERS WHERE user_id=%s", (uid_str,))
        user = c.fetchone()
        
        if not user:
            # 2. Create new user
            c.execute("""
                INSERT INTO USERS (user_id, username, first_name, joined_date, last_seen, is_flagged) 
                VALUES (%s, %s, %s, %s, %s, 0)
            """, (uid_str, username, first_name, now, now))
            # Fetch the newly created user
            c.execute("SELECT * FROM USERS WHERE user_id=%s", (uid_str,))
            return True, c.fetchone()
        else:
            # 3. Just update last_seen
            c.execute("UPDATE USERS SET last_seen = %s WHERE user_id = %s", (now, uid_str))
            return False, user

def db_reward_referrer(referrer_id, new_user_id, amount=10):
    """Referrer ke ledger mein bonus credit karta hai agar wo already rewarded nahi hai"""
    with get_db() as c:
        ref_id = f"REF_BONUS_{new_user_id}"
        # Idempotency check: Ek user ke referral ka bonus ek hi baar milna chahiye
        c.execute("SELECT id FROM LEDGER WHERE reference_id=%s", (ref_id,))
        exists = c.fetchone()
        if not exists:
            c.execute(
                "INSERT INTO LEDGER (user_id, amount, type, reference_id, timestamp) VALUES (%s, %s, 'CREDIT', %s, %s)",
                (str(referrer_id), amount, ref_id, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            )
            return True
        return False

def db_update_last_seen(user_id):
    with get_db() as c:
        c.execute("UPDATE USERS SET last_seen = %s WHERE user_id = %s", 
                     (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), str(user_id)))

def db_log_failed_utr(user_id, utr):
    with get_db() as c:
        c.execute("INSERT INTO FAILED_UTR_LOGS (user_id, utr, timestamp) VALUES (%s, %s, %s)",
                     (str(user_id), utr, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

def db_get_failed_utr_count(user_id):
    with get_db() as c:
        c.execute("SELECT COUNT(*) as cnt FROM FAILED_UTR_LOGS WHERE user_id=%s", (str(user_id),))
        row = c.fetchone()
        return row['cnt'] if row and 'cnt' in row else row[0] if row else 0

# FIX 3: Helper functions for user state persistence
def db_set_user_state(user_id, key, value):
    with get_db() as c:
        c.execute("""
            INSERT INTO USER_STATE (user_id, key, value) VALUES (%s, %s, %s)
            ON CONFLICT (user_id, key) DO UPDATE SET value = EXCLUDED.value, updated_at = CURRENT_TIMESTAMP
        """, (str(user_id), key, str(value)))

def db_get_user_state(user_id, key):
    with get_db() as c:
        c.execute("SELECT value FROM USER_STATE WHERE user_id=%s AND key=%s", (str(user_id), key))
        row = c.fetchone()
        return row['value'] if row else None

def db_flag_user(user_id, status=1):
    with get_db() as c:
        c.execute("UPDATE USERS SET is_flagged = %s WHERE user_id = %s", (status, str(user_id)))

def db_get_all_user_teams(user_id, match_id):
    """Ek hi baar mein user ki saari teams fetch karta hai (Performance optimization)"""
    try:
        with get_db() as c:
            c.execute("SELECT * FROM TEAMS WHERE user_id=%s AND match_id=%s", (str(user_id), match_id))
            return c.fetchall()
    except Exception as e:
        logging.error(f"Error fetching all teams: {e}")
        return []

def db_get_team_internal(user_id, match_id='m1', team_num=1):
    try:
        with get_db() as c:
            c.execute("SELECT * FROM TEAMS WHERE user_id=%s AND match_id=%s AND team_num=%s", (str(user_id), match_id, team_num))
            row = c.fetchone()
            if row:
                data = json.loads(row['team_players']) if row['team_players'] else {}
                data['captain'] = row['captain']
                data['vice_captain'] = row['vice_captain']
                data['team_saved'] = row['team_saved']
                data['is_paid'] = row['is_paid']
                data['points'] = row['points']
                return data
            return None
    except Exception as e:
        logging.error(f"Error getting team internal: {e}")
        return None

def db_get_team_status(user_id, match_id, team_num):
    """Returns the status of a specific team slot"""
    with get_db() as c:
        c.execute(
            "SELECT team_saved, is_paid FROM TEAMS WHERE user_id=%s AND match_id=%s AND team_num=%s",
            (str(user_id), match_id, team_num))
        row = c.fetchone()
        if not row: return "empty"
        if row['team_saved'] and row['is_paid']: return "paid"
        if row['team_saved']: return "unpaid"
        return "empty"

def db_create_order(user_id, amount, context="wallet"):
    order_id = f"ORD-{secrets.token_hex(4).upper()}"
    now = datetime.now()
    expires = now + timedelta(minutes=60)
    with get_db() as c:
        c.execute(
            "INSERT INTO PAYMENT_INTENTS (order_id, user_id, amount, match_context, created_at, expires_at) VALUES (%s, %s, %s, %s, %s, %s)",
            (order_id, str(user_id), amount, context, now.strftime('%Y-%m-%d %H:%M:%S'), expires.strftime('%Y-%m-%d %H:%M:%S'))
        )
    return order_id

def db_get_order(order_id):
    with get_db() as c:
        c.execute("SELECT * FROM PAYMENT_INTENTS WHERE order_id=%s", (order_id,))
        return c.fetchone()

def get_admin_stats():
    """Calculates real-time summary for Dashboard"""
    with get_db() as c:
        now = datetime.now()
        today = now.strftime('%Y-%m-%d')
        five_mins_ago = (now - timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')
        
        c.execute("SELECT COUNT(*) as cnt FROM USERS"); total = c.fetchone()['cnt']
        c.execute("SELECT COUNT(*) as cnt FROM USERS WHERE last_seen > %s", (five_mins_ago,)); active = c.fetchone()['cnt']
        c.execute("SELECT COUNT(*) as cnt FROM USERS WHERE joined_date LIKE %s", (f"{today}%",)); new_today = c.fetchone()['cnt']
        c.execute("SELECT COUNT(DISTINCT user_id) as cnt FROM TEAMS WHERE is_paid=1"); paid = c.fetchone()['cnt']
        c.execute("SELECT COUNT(*) as cnt FROM USERS WHERE is_flagged=1"); flagged = c.fetchone()['cnt']
        
        conv_rate = (paid / total * 100) if total > 0 else 0
        
        return {
            "total": total, "active": active, "new": new_today, 
            "paid": paid, "conv": round(conv_rate, 1), "flagged": flagged
        }

def db_get_match_financials(match_id):
    """Calculates total collection and stats based on actual paid entries in Ledger"""
    with get_db() as c:
        # Group actual participation from Ledger by Entry Fee to avoid "Mixing"
        search_pattern = f"DEBIT_MATCH_{match_id}_%"
        c.execute("""
            SELECT ABS(amount) as fee, SUM(ABS(amount)) as collection, COUNT(*) as entries
            FROM LEDGER 
            WHERE type='DEBIT' AND reference_id LIKE %s
            GROUP BY ABS(amount)
        """, (search_pattern,))
        ledger_stats = c.fetchall()
        
        # Fetch configs to match slots information
        c.execute("SELECT entry_fee, max_slots, contest_type FROM CONTEST_CONFIG WHERE match_id=%s", (match_id,))
        configs = {float(cfg['entry_fee']): (cfg['max_slots'], cfg.get('contest_type', 'J')) for cfg in c.fetchall()}
        
        contests = []
        total_col = 0
        total_ent = 0
        for row in ledger_stats:
            f = float(row['fee'])
            slots_info = configs.get(f, (0, 'J'))
            slots, c_type = slots_info
            contests.append({
                "fee": f,
                "collection": float(row['collection']),
                "entries": row['entries'],
                "max_slots": slots,
                "type": c_type
            })
            total_col += float(row['collection'])
            total_ent += row['entries']
            
        return {
            "total_collection": total_col,
            "total_entries": total_ent,
            "contests": contests
        }

def get_contest_stats(match_id, entry_fee=100):
    """Fetches real-time participation stats for a match dashboard"""
    with get_db() as c:
        # Count actual entries from LEDGER instead of just team slots
        search_pattern = f"DEBIT_MATCH_{match_id}_%"
        c.execute("SELECT COUNT(*) as count FROM LEDGER WHERE reference_id LIKE %s AND ABS(amount) = %s", (search_pattern, entry_fee))
        real_joined = c.fetchone()['count'] or 0

        fake_base = int(db_get_setting('FAKE_PARTICIPANTS_BASE', 0))
        c.execute("SELECT max_slots FROM CONTEST_CONFIG WHERE match_id=%s AND entry_fee=%s", (match_id, entry_fee))
        cfg = c.fetchone()
        max_slots = cfg['max_slots'] if cfg else 50 # Default 50 slots
        
        # Check for manual prize pool override
        c.execute("SELECT * FROM MANUAL_PRIZES WHERE match_id=%s AND entry_fee=%s", (match_id, entry_fee))
        manual = c.fetchone()
        if manual:
            prize_pool = manual['r1'] + manual['r2'] + manual['r3'] + (manual['r4_10'] * 7) + (manual['bottom'] * (manual['winners_count'] - 10))
        else:
            # Fallback to automatic 90% payout logic
            prize_pool = max_slots * entry_fee * 0.9
            
        # Agar fake_base 0 hai toh asli data dikhao, warna marketing logic lagao
        if fake_base > 0:
            total_joined = min(real_joined + fake_base, max_slots) 
        else:
            total_joined = real_joined
            
        return {"joined": total_joined, "max_slots": max_slots, "prize_pool": int(prize_pool)}

def get_user_match_summary(user_id, match_id):
    """Calculates user-specific stats for a specific match"""
    with get_db() as c:
        c.execute("SELECT team_num, team_saved, is_paid FROM TEAMS WHERE user_id=%s AND match_id=%s", 
                             (str(user_id), match_id))
        teams = c.fetchall()
        saved = [t['team_num'] for t in teams if t['team_saved'] == 1]
        paid = [t['team_num'] for t in teams if t['is_paid'] == 1]
        incomplete = [t['team_num'] for t in teams if t['team_saved'] == 0]
        return {"saved": saved, "paid": paid, "incomplete": incomplete}

def get_funnel_data():
    """Calculates user journey drop-offs"""
    with get_db() as c:
        c.execute("SELECT COUNT(*) FROM USERS"); total = c.fetchone()['count']
        # Users who at least opened the selection UI (temp_team_cache or TEAMS entry)
        c.execute("SELECT COUNT(DISTINCT user_id) FROM TEAMS"); started_team = c.fetchone()['count']
        c.execute("SELECT COUNT(DISTINCT user_id) FROM TEAMS WHERE team_saved=1"); saved_team = c.fetchone()['count']
        c.execute("SELECT COUNT(DISTINCT user_id) FROM TEAMS WHERE is_paid=1"); paid_contest = c.fetchone()['count']
        
        return [total, started_team, saved_team, paid_contest]

def get_referral_analytics():
    with get_db() as c:
        c.execute("""
            SELECT referred_by, COUNT(*) as count 
            FROM USERS WHERE referred_by IS NOT NULL 
            GROUP BY referred_by ORDER BY count DESC LIMIT 5
        """)
        return c.fetchall()

def get_fraud_list():
    with get_db() as c:
        # Sirf flagged users ya suspicious high activity dikhayein
        c.execute("""
            SELECT u.user_id, u.first_name, u.username, 
            (SELECT COUNT(*) FROM USED_UTR WHERE user_id = u.user_id) as utr_count
            FROM USERS u 
            WHERE u.is_flagged = 1
            ORDER BY utr_count DESC LIMIT 10
        """)
        return c.fetchall()

def db_get_wallet_balance(user_id):
    with get_db() as c:
        c.execute("SELECT SUM(amount) as bal FROM LEDGER WHERE user_id=%s", (str(user_id),))
        row = c.fetchone()
        return float(row['bal']) if row and row['bal'] else 0

def get_live_ranks(match_id):
    with get_db() as c:
        c.execute("""
            SELECT u.username, u.first_name, t.points 
            FROM TEAMS t 
            JOIN USERS u ON t.user_id = u.user_id 
            WHERE t.match_id = %s 
            ORDER BY t.points DESC 
            LIMIT 10
        """, (match_id,))
        return c.fetchall()

def db_add_player(match_id, name, role, team='N/A', designation=''):
    with get_db() as c:
        c.execute(
            """INSERT INTO PLAYERS (match_id, player_name, role, team, designation) VALUES (%s, %s, %s, %s, %s)
               ON CONFLICT (match_id, player_name, team) DO UPDATE SET role = EXCLUDED.role, designation = EXCLUDED.designation""",
            (match_id, name, role, team, designation)
        )

def db_get_players_by_match(match_id):
    """Fetches the squad (names and roles) for a given match."""
    with get_db() as c:
        c.execute("SELECT player_name, role, team FROM PLAYERS WHERE match_id=%s", (match_id,))
        return c.fetchall()

def db_get_player_count(match_id):
    """Returns the total number of players added to a specific match"""
    with get_db() as c:
        c.execute("SELECT COUNT(*) as cnt FROM PLAYERS WHERE match_id=%s", (match_id,))
        row = c.fetchone()
        return row['cnt'] if row else 0

def db_delete_player(match_id, name):
    with get_db() as c:
        c.execute(
            "DELETE FROM PLAYERS WHERE match_id=%s AND player_name=%s",
            (match_id, name)
        )

def db_get_user_payment_history(user_id, limit=10):
    """Fetches list of all payment attempts (screenshots) by a user"""
    with get_db() as c:
        c.execute(
            "SELECT amount, status, timestamp FROM PAYMENTS WHERE user_id=%s ORDER BY timestamp DESC LIMIT %s",
            (str(user_id), limit))
        return c.fetchall()

# ADD 1: Get transaction history from LEDGER
def db_get_transaction_history(user_id, limit=10):
    with get_db() as c:
        c.execute("""
            SELECT type, amount, reference_id, timestamp 
            FROM LEDGER WHERE user_id=%s 
            ORDER BY timestamp DESC LIMIT %s
        """, (str(user_id), limit))
        return c.fetchall()

# ADD 3: Get Referral Stats
def db_get_referral_stats(user_id):
    with get_db() as c:
        c.execute("SELECT COUNT(*) as total FROM USERS WHERE referred_by=%s", (str(user_id),))
        total = c.fetchone()['total']
        c.execute("SELECT SUM(amount) as bonus FROM LEDGER WHERE user_id=%s AND reference_id LIKE 'REF_BONUS_%%'", (str(user_id),))
        bonus = c.fetchone()['bonus'] or 0
        return {"total": total, "bonus": bonus}

# ADD 4: Update withdrawal status stage
def db_update_withdrawal_status(req_id, status):
    with get_db() as c:
        c.execute("UPDATE WITHDRAWALS SET status=%s WHERE id=%s", (status, req_id))

# FEATURE 1: Reminder System Helpers
def db_get_users_without_team(match_id):
    with get_db() as c:
        c.execute("""
            SELECT user_id FROM USERS 
            WHERE user_id NOT IN (SELECT user_id FROM TEAMS WHERE match_id=%s AND team_saved=1)
        """, (match_id,))
        return [row['user_id'] for row in c.fetchall()]

def db_get_users_unpaid_team(match_id):
    with get_db() as c:
        c.execute("""
            SELECT user_id FROM TEAMS 
            WHERE match_id=%s AND team_saved=1 AND is_paid=0
        """, (match_id,))
        return [row['user_id'] for row in c.fetchall()]

def db_mark_reminder_sent(match_id, user_id, reminder_type):
    with get_db() as c:
        c.execute("""
            INSERT INTO REMINDERS (match_id, user_id, reminder_type, sent_at) 
            VALUES (%s, %s, %s, %s)
        """, (match_id, user_id, reminder_type, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

def db_was_reminder_sent(match_id, user_id, reminder_type):
    with get_db() as c:
        c.execute("""
            SELECT 1 FROM REMINDERS 
            WHERE match_id=%s AND user_id=%s AND reminder_type=%s
        """, (match_id, user_id, reminder_type))
        return c.fetchone() is not None

# FEATURE 3: Re-engagement Notification Helpers
def db_get_inactive_users(days=3):
    with get_db() as c:
        three_days_ago = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
        seven_days_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
        c.execute("""
            SELECT user_id, first_name FROM USERS 
            WHERE last_seen < %s 
            AND user_id NOT IN (
                SELECT DISTINCT user_id FROM REMINDERS 
                WHERE reminder_type='reengagement' AND sent_at > %s
            )
        """, (three_days_ago, seven_days_ago))
        return c.fetchall()

# FEATURE 4: Support Ticket System Helpers
def db_create_ticket(user_id, issue):
    with get_db() as c:
        c.execute("INSERT INTO SUPPORT_TICKETS (user_id, issue, created_at) VALUES (%s, %s, %s) RETURNING id",
                  (user_id, issue, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        return c.fetchone()['id']

def db_resolve_ticket(ticket_id):
    with get_db() as c:
        c.execute("UPDATE SUPPORT_TICKETS SET status='resolved', resolved_at=%s WHERE id=%s",
                  (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), ticket_id))

# FEATURE 5: Live Rank Helpers
def db_get_user_rank(user_id, match_id):
    with get_db() as c:
        c.execute("""
            SELECT rank FROM (
                SELECT user_id, points,
                RANK() OVER (ORDER BY points DESC) as rank
                FROM TEAMS WHERE match_id=%s AND is_paid=1
            ) ranked WHERE user_id=%s
        """, (match_id, user_id))
        return c.fetchone()

def db_get_match_participant_count(match_id):
    with get_db() as c:
        search_pattern = f"DEBIT_MATCH_{match_id}_%"
        c.execute("SELECT COUNT(*) as cnt FROM LEDGER WHERE reference_id LIKE %s", (search_pattern,))
        row = c.fetchone()
        if not row: return 0
        return row.get('cnt', 0) if isinstance(row, dict) else row[0]

def db_get_all_player_scores(match_id):
    """Returns a dictionary of all players and their current total points"""
    with get_db() as c:
        c.execute("""
            SELECT player_name, (runs * 1 + fours * 4 + sixes * 6 + wickets * 25) as pts 
            FROM PLAYER_LIVE_STATS WHERE match_id=%s
        """, (match_id,))
        rows = c.fetchall()
        return {r['player_name']: r['pts'] for r in rows}

def db_get_player_live_stats_map(match_id):
    """Returns a dictionary of player_name -> full stats record"""
    with get_db() as c:
        c.execute("SELECT * FROM PLAYER_LIVE_STATS WHERE match_id=%s", (match_id,))
        rows = c.fetchall()
        return {r['player_name']: r for r in rows}

def db_mark_points_calculated(match_id):
    """Marks a match as having its points calculated"""
    with get_db() as c:
        c.execute("UPDATE MATCHES_LIST SET points_calculated = 1 WHERE match_id = %s", (match_id,))

def db_set_manual_lock(match_id, status):
    """0: Auto, 1: Force Lock, -1: Force Unlock"""
    with get_db() as c:
        c.execute("UPDATE MATCHES_LIST SET manual_lock = %s WHERE match_id = %s", (status, match_id))

def db_set_player_stats_absolute(match_id, player_name, runs=None, wickets=None):
    """Directly sets total runs/wickets instead of incrementing"""
    with get_db() as c:
        if runs is not None:
            c.execute("INSERT INTO PLAYER_LIVE_STATS (match_id, player_name, runs) VALUES (%s,%s,%s) ON CONFLICT(match_id, player_name) DO UPDATE SET runs = EXCLUDED.runs", (match_id, player_name, runs))
        if wickets is not None:
            c.execute("INSERT INTO PLAYER_LIVE_STATS (match_id, player_name, wickets) VALUES (%s,%s,%s) ON CONFLICT(match_id, player_name) DO UPDATE SET wickets = EXCLUDED.wickets", (match_id, player_name, wickets))
    return True

def db_set_live_link(match_id, link):
    with get_db() as c:
        c.execute("UPDATE MATCHES_LIST SET live_link = %s WHERE match_id = %s", (link, match_id))
    return True

def db_get_team_joined_contests(user_id, match_id, team_num):
    """Ek team ne is match mein kaunse fees wale contests join kiye hain"""
    with get_db() as c:
        # reference_id pattern: DEBIT_MATCH_{mid}_{tnum}_{timestamp}
        search_pattern = f"DEBIT_MATCH_{match_id}_{team_num}_%"
        c.execute("SELECT ABS(amount) as fee FROM LEDGER WHERE user_id=%s AND reference_id LIKE %s", (str(user_id), search_pattern))
        rows = c.fetchall()
        return [int(r['fee']) for r in rows]

def db_get_all_user_data(user_id):
    """Fetches all available data for a specific user from various tables."""
    user_data = {}
    with get_db() as c:
        # 1. User Profile
        c.execute("SELECT * FROM USERS WHERE user_id=%s", (user_id,))
        user_data['profile'] = c.fetchone()

        # 2. Wallet Balance
        c.execute("SELECT SUM(amount) as balance FROM LEDGER WHERE user_id=%s", (user_id,))
        bal_row = c.fetchone()
        user_data['wallet_balance'] = float(bal_row['balance']) if bal_row and bal_row['balance'] is not None else 0.0

        # 3. Transaction History (Ledger)
        c.execute("SELECT * FROM LEDGER WHERE user_id=%s ORDER BY timestamp DESC", (user_id,))
        user_data['ledger_history'] = c.fetchall()

        # 4. Teams
        c.execute("SELECT * FROM TEAMS WHERE user_id=%s ORDER BY match_id, team_num", (user_id,))
        user_data['teams'] = c.fetchall()

        # 5. Payment Intents
        c.execute("SELECT * FROM PAYMENT_INTENTS WHERE user_id=%s ORDER BY created_at DESC", (user_id,))
        user_data['payment_intents'] = c.fetchall()

        # 6. Payment Records (Screenshots/UTR)
        c.execute("SELECT * FROM PAYMENTS WHERE user_id=%s ORDER BY timestamp DESC", (user_id,))
        user_data['payments'] = c.fetchall()

        # 7. Withdrawal Requests
        c.execute("SELECT * FROM WITHDRAWALS WHERE user_id=%s ORDER BY timestamp DESC", (user_id,))
        user_data['withdrawals'] = c.fetchall()

        # 8. Match Results (History)
        c.execute("SELECT * FROM USER_RESULTS WHERE user_id=%s ORDER BY timestamp DESC", (user_id,))
        user_data['match_results'] = c.fetchall()

        # 9. Support Tickets
        c.execute("SELECT * FROM SUPPORT_TICKETS WHERE user_id=%s ORDER BY created_at DESC", (user_id,))
        user_data['support_tickets'] = c.fetchall()

    return user_data

def db_get_match_audit_data(match_id):
    """Audit ke liye match se judi saari financial aur team activity nikalta hai"""
    with get_db() as c:
        # 1. Total Debits (Users ne kitna pay kiya)
        search_debit = f"DEBIT_MATCH_{match_id}_%"
        c.execute("SELECT ABS(SUM(amount)) as total_in, COUNT(*) as entry_count FROM LEDGER WHERE reference_id LIKE %s AND type='DEBIT'", (search_debit,))
        debit_stats = c.fetchone()

        # 2. Total Credits (Bot ne kitna prize baanta)
        search_credit = f"PRIZE_{match_id}_%"
        c.execute("SELECT SUM(amount) as total_out, COUNT(*) as winner_count FROM LEDGER WHERE reference_id LIKE %s AND type='CREDIT'", (search_credit,))
        credit_stats = c.fetchone()

        # 3. Discrepancy check: Teams marked as paid vs Ledger entries
        c.execute("SELECT COUNT(*) as paid_teams FROM TEAMS WHERE match_id=%s AND is_paid=1", (match_id,))
        paid_teams_count = c.fetchone()['paid_teams']

        return {
            "in": float(debit_stats['total_in'] or 0),
            "entries": debit_stats['entry_count'] or 0,
            "out": float(credit_stats['total_out'] or 0),
            "winners": credit_stats['winner_count'] or 0,
            "db_paid_teams": paid_teams_count
        }

def db_get_match_prizes(match_id):
    """Ek match ke saare baante huye prizes ki list nikalta hai reversal ke liye"""
    with get_db() as c:
        search_pattern = f"PRIZE_{match_id}_%"
        c.execute("SELECT user_id, amount, reference_id FROM LEDGER WHERE reference_id LIKE %s AND type='CREDIT'", (search_pattern,))
        return c.fetchall()

def db_reset_match_status(match_id):
    """Match ko dubara active karta hai taaki points/result phir se set ho sakein"""
    with get_db() as c:
        c.execute("UPDATE MATCHES_LIST SET points_calculated = 0 WHERE match_id = %s", (match_id,))

def db_get_user_results(user_id):
    """User ke purane matches ke results nikalta hai join karke"""
    with get_db() as c:
        c.execute("""
            SELECT r.*, m.name as match_name 
            FROM USER_RESULTS r
            LEFT JOIN MATCHES_LIST m ON r.match_id = m.match_id
            WHERE r.user_id = %s 
            ORDER BY r.timestamp DESC
        """, (str(user_id),))
        return c.fetchall()
