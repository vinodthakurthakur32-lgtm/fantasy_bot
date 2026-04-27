import gspread
import logging
import threading
import time
import os
import json
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials

SHEET_ID = "1e5YbzdgM2-orRa04sWtrgZiVXP8rql6JNZFZ-jcMVw4"

sheets_lock = threading.RLock()
_sheets_spreadsheet = None
_worksheet_cache = {}

SHEET_STRUCTURES = {
    "USERS": ["user_id", "username", "paid", "entry_amount", "joined_date"],
    "TEAMS": ["user_id", "team_players", "captain", "vice_captain"],
    "PAYMENTS": ["user_id", "amount", "upi_txn_id", "timestamps", "status"],
    "RESULTS": ["contest_date", "user_id", "points", "rank", "prize"],
    "MATCHES": ["match_id", "name", "type", "deadline"],
}

# Load env variables
load_dotenv()


def init_sheets():
    global _sheets_spreadsheet

    if _sheets_spreadsheet:
        return _sheets_spreadsheet

    with sheets_lock:
        try:
            scopes = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ]

            raw_creds = os.getenv("GOOGLE_CREDENTIALS")
            creds_info = None

            if raw_creds:
                try:
                    # Clean the string: handle potential wrapping quotes from Render dashboard
                    cleaned_raw = raw_creds.strip()
                    if (cleaned_raw.startswith('"') and cleaned_raw.endswith('"')) or \
                       (cleaned_raw.startswith("'") and cleaned_raw.endswith("'")):
                        cleaned_raw = cleaned_raw[1:-1]
                    
                    creds_info = json.loads(cleaned_raw)
                except Exception as e:
                    logging.error(f"❌ Failed to parse GOOGLE_CREDENTIALS: {e}")
                    # Fallback: Agar environment variable invalid hai toh local file check karein
                    if os.path.exists("credentials.json"):
                        with open("credentials.json", "r") as f:
                            creds_info = json.load(f)
                    else:
                        return None
            elif os.path.exists("credentials.json"):
                try:
                    with open("credentials.json", "r") as f:
                        creds_info = json.load(f)
                    logging.info("✅ Loading Google credentials from local credentials.json file.")
                except Exception as e:
                    logging.error(f"❌ Error reading local credentials.json: {e}")
                    return None
            else:
                logging.error("❌ GOOGLE_CREDENTIALS environment variable or credentials.json file is missing!")
                return None
            
            # Ensure private key has correct newline characters for JWT signing.
            # We handle both single-escaped and double-escaped newlines.
            if "private_key" in creds_info:
                # Robust cleaning: handling different ways Render/Docker might escape newlines
                pk = creds_info["private_key"]
                if isinstance(pk, str):
                    # Handle multiple levels of escaping that happen in Env Variables
                    pk = pk.replace("\\n", "\n")
                    pk = pk.replace("\\\\n", "\n")
                    pk = pk.strip().strip("'").strip('"')
                creds_info["private_key"] = pk

            creds = Credentials.from_service_account_info(
                creds_info,
                scopes=scopes
            )

            gc = gspread.authorize(creds)
            _sheets_spreadsheet = gc.open_by_key(SHEET_ID)

            logging.info("✅ Google Sheets initialized successfully.")
            return _sheets_spreadsheet

        except Exception as e:
            logging.error(f"❌ GSheets Init Error: {e}")
            return None


def safe_api_call(func, *args, **kwargs):
    for _ in range(5):
        try:
            with sheets_lock:
                return func(*args, **kwargs)
        except Exception:
            time.sleep(1)
    return None


def get_or_create_sheet(sh, sheet_name, headers):
    global _worksheet_cache
    sheet_name = sheet_name.upper()

    if sheet_name in _worksheet_cache:
        return _worksheet_cache[sheet_name]

    with sheets_lock:
        existing = [ws.title.upper() for ws in sh.worksheets()]

        if sheet_name in existing:
            sheet = sh.worksheet(sheet_name)
        else:
            sheet = sh.add_worksheet(
                title=sheet_name,
                rows="1000",
                cols=len(headers)
            )
            sheet.append_row(headers)

        _worksheet_cache[sheet_name] = sheet
        return sheet


def format_players(data):
    if isinstance(data, dict):
        all_p = []
        for r in ['bat', 'wk', 'ar', 'bowl', 'sub']:
            all_p.extend(data.get(r, []))
        return ",".join(filter(None, all_p))
    return str(data)


def append_row_safe(sheet, headers, data_dict):
    with sheets_lock:
        mapping = {
            "team_players": "players",
            "timestamps": "timestamp",
            "entry_amount": "balance"
        }

        row = []
        for h in headers:
            val = data_dict.get(h)
            if val is None:
                val = data_dict.get(mapping.get(h), "")

            if h == "players":
                val = format_players(val)

            row.append(str(val).strip())

        all_rows = safe_api_call(sheet.get_all_values)

        unique_id_map = {
            "USERS": [0],
            "PAYMENTS": [0, 2],
            "TEAMS": [0]
        }

        keys_to_check = unique_id_map.get(sheet.title.upper(), [0])

        row_index = -1

        if all_rows and len(all_rows) > 1:
            for idx, existing_row in enumerate(all_rows[1:], start=2):
                if all(
                    str(existing_row[k]) == str(row[k])
                    for k in keys_to_check
                    if k < len(existing_row)
                ):
                    row_index = idx
                    break

        if row_index != -1:
            if sheet.title.upper() == "PAYMENTS":
                return

            range_label = f"A{row_index}:{chr(64 + len(headers))}{row_index}"
            safe_api_call(sheet.update, range_label, [row])
        else:
            safe_api_call(sheet.append_row, row)


def sync_to_sheets(user_data, sheet_type="USERS"):
    headers = SHEET_STRUCTURES.get(sheet_type.upper())
    sh = init_sheets()

    if sh:
        sheet = get_or_create_sheet(sh, sheet_type, headers)
        if sheet:
            append_row_safe(sheet, headers, user_data)


def sync_wrapper(user_data, sheet_type):
    threading.Thread(
        target=sync_to_sheets,
        args=(user_data, sheet_type),
        daemon=True
    ).start()


def get_all_rows_safe(sheet_type):
    headers = SHEET_STRUCTURES.get(sheet_type.upper())
    sh = init_sheets()

    if sh:
        sheet = get_or_create_sheet(sh, sheet_type, headers)
        return safe_api_call(sheet.get_all_records)

    return []
