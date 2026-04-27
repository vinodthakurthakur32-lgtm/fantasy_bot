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
            is_paid INTEGER DEFAULT 0, points INTEGER DEFAULT 0,
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
            match_id VARCHAR(255) PRIMARY KEY, name TEXT, type TEXT, deadline TEXT
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS PLAYERS (
            id SERIAL PRIMARY KEY,
            match_id VARCHAR(255),
            player_name TEXT,
            role TEXT,
            UNIQUE(match_id, player_name)
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

def db_set_setting(key, value):
    with get_db() as c:
        c.execute("""
            INSERT INTO SETTINGS (key, value) VALUES (%s, %s)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        """, (key, str(value)))

def db_get_setting(key, default=None):
    with get_db() as c:
        c.execute("SELECT value FROM SETTINGS WHERE key=%s", (key,))
        row = c.fetchone()
        return row['value'] if row else default

def db_set_contest_config(mid, fee, slots):
    with get_db() as c:
        c.execute("""
            INSERT INTO CONTEST_CONFIG (match_id, entry_fee, max_slots) VALUES (%s, %s, %s)
            ON CONFLICT (match_id, entry_fee) DO UPDATE SET max_slots = EXCLUDED.max_slots
        """, (mid, fee, slots))

def db_get_contest_config(mid, fee):
    with get_db() as c:
        c.execute("SELECT * FROM CONTEST_CONFIG WHERE match_id=%s AND entry_fee=%s", (mid, fee))
        return c.fetchone()

def db_get_all_contest_configs(mid):
    with get_db() as c:
        c.execute("SELECT * FROM CONTEST_CONFIG WHERE match_id=%s", (mid,))
        return c.fetchall()

def db_add_match(mid, name, m_type, deadline):
    with get_db() as c:
        c.execute("""
            INSERT INTO MATCHES_LIST (match_id, name, type, deadline) VALUES (%s, %s, %s, %s)
            ON CONFLICT (match_id) DO UPDATE SET name = EXCLUDED.name, type = EXCLUDED.type, deadline = EXCLUDED.deadline
        """, (mid, name, m_type, deadline))

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
            ]
        }
        for table, cols in migrations.items():
            c.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s", (table.lower(),))
            existing = [row['column_name'] for row in c.fetchall()]
            for col_name, col_def in cols:
                if col_name not in existing:
                    c.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_def}")
    logging.info("✅ Database Migrations completed.")

def db_get_user(user_id):
    with get_db() as c:
        c.execute("SELECT * FROM USERS WHERE user_id=%s", (str(user_id),))
        return c.fetchone()

def db_create_user(user_id, username, first_name):
    with get_db() as c:
        c.execute("""
            INSERT INTO USERS (user_id, username, first_name, joined_date, last_seen, is_flagged) 
            VALUES (%s, %s, %s, %s, %s, 0)
            ON CONFLICT (user_id) DO NOTHING
        """, (str(user_id), username, first_name, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        c.execute("UPDATE USERS SET last_seen = %s WHERE user_id = %s", 
                     (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), str(user_id)))

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

def db_flag_user(user_id, status=1):
    with get_db() as c:
        c.execute("UPDATE USERS SET is_flagged = %s WHERE user_id = %s", (status, str(user_id)))

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
        
        c.execute("SELECT COUNT(*) FROM USERS"); total = c.fetchone()['count']
        c.execute("SELECT COUNT(*) FROM USERS WHERE last_seen > %s", (five_mins_ago,)); active = c.fetchone()['count']
        c.execute("SELECT COUNT(*) FROM USERS WHERE joined_date LIKE %s", (f"{today}%",)); new_today = c.fetchone()['count']
        c.execute("SELECT COUNT(DISTINCT user_id) FROM TEAMS WHERE is_paid=1"); paid = c.fetchone()['count']
        c.execute("SELECT COUNT(*) FROM USERS WHERE is_flagged=1"); flagged = c.fetchone()['count']
        
        conv_rate = (paid / total * 100) if total > 0 else 0
        
        return {
            "total": total, "active": active, "new": new_today, 
            "paid": paid, "conv": round(conv_rate, 1), "flagged": flagged
        }

def get_contest_stats(match_id):
    """Fetches real-time participation stats for a match dashboard"""
    with get_db() as c:
        c.execute("SELECT COUNT(*) FROM TEAMS WHERE match_id=%s AND is_paid=1", (match_id,))
        total_joined = c.fetchone()['count']
        # Mock prize pool logic - in production, this would be in a CONTESTS table
        prize_pool = total_joined * 100 * 0.9 # 90% payout
        return {"joined": total_joined, "prize_pool": int(prize_pool)}

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

def db_add_player(match_id, name, role):
    with get_db() as c:
        c.execute(
            "INSERT INTO PLAYERS (match_id, player_name, role) VALUES (%s, %s, %s)",
            (match_id, name, role)
        )

def db_get_players_by_match(match_id):
    """Fetches the squad (names and roles) for a given match."""
    with get_db() as c:
        c.execute("SELECT player_name, role FROM PLAYERS WHERE match_id=%s", (match_id,))
        return c.fetchall()

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