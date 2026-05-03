import html
import time
from datetime import datetime, timedelta
from telebot import types
import db

def get_loading_render(progress):
    fill = int(progress / 10)
    bar = "█" * fill + "▒" * (10 - fill)
    return f"⏳ *Loading System Components...*\n\n`{bar}` {progress}%"

def fake_animate(bot, chat_id, message_id):
    for p in [20, 50, 80, 100]:
        try:
            bot.edit_message_text(get_loading_render(p), chat_id, message_id, parse_mode='Markdown')
            time.sleep(0.3)
        except: pass

def home_screen_markup(matches):
    markup = types.InlineKeyboardMarkup(row_width=1)
    for mid, info in matches.items():
        markup.add(types.InlineKeyboardButton(f"🏏 {info['name']} (Live)", callback_data=f"app_match_{mid}"))
    
    markup.row(
        types.InlineKeyboardButton("💰 Wallet", callback_data="app_wallet"),
        types.InlineKeyboardButton("🏆 Ranks", callback_data="app_global_ranks"),
        types.InlineKeyboardButton("📜 My Results", callback_data="my_results")
    )
    return markup, "📱 *CRICK-TEAM11 DASHBOARD*\n\nSelect a live match to view real-time scoring and your standing."

def match_screen_markup(match_id, match_name, ranks):
    markup = types.InlineKeyboardMarkup(row_width=2)
    
    rank_text = "🏆 *LIVE LEADERBOARD*\n"
    if not ranks:
        rank_text += "_No points recorded yet._"
    for i, r in enumerate(ranks[:5], 1):
        medal = "🥇" if i==1 else "🥈" if i==2 else "🥉" if i==3 else "🔹"
        rank_text += f"{medal} {r['first_name']} - {r['points']} pts\n"

    markup.add(
        types.InlineKeyboardButton("🔄 Refresh Score", callback_data=f"app_match_{match_id}"),
        types.InlineKeyboardButton("🏠 Home", callback_data="app_home")
    )
    
    body = f"🏟 *MATCH:* {match_name}\n\n{rank_text}"
    return markup, body

def lock_screen_markup():
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("🏠 Return Home", callback_data="app_home"))
    return markup, "🔒 *MATCH LOCKED*\n\nThe deadline has passed. Team editing is disabled. Live points are being calculated."

def admin_match_finance_render(match_id, match_name, fin_data):
    comm_pct = float(db.db_get_setting('PRIZE_COMMISSION', 18))
    
    res = (
        f"💰 *FINANCIAL SUMMARY: {match_name}*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
    )
    
    for con in fin_data['contests']:
        fee = con['fee']
        col = con['collection']
        ent = con['entries']
        c_type = con.get('type', 'J')
        
        # Match logic: Saver has 10% commission, others use global setting
        actual_comm = 10.0 if c_type == 'S' else comm_pct
        cut = (col * actual_comm) / 100
        pool = col - cut
        type_label = "⚡ JACKPOT" if c_type == 'J' else "🛡️ TEAM SAVER"
        
        res += (
            f"{type_label} *₹{int(fee)}*\n"
            f"👥 Entries: `{ent}` | 📈 Collection: `₹{col}`\n"
            f"✂️ Cut ({actual_comm}%): `₹{round(cut, 2)}`\n"
            f"🎁 Prize Pool: `₹{round(pool, 2)}`\n"
            f"--------------------\n"
        )

    res += (
        f"📊 *TOTAL OVERALL*\n"
        f"💰 Collection: `₹{fin_data['total_collection']}`\n"
        f"👥 Total Entries: `{fin_data['total_entries']}`\n"
        f"━━━━━━━━━━━━━━━━━━━━"
    )

    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("🔄 Refresh", callback_data=f"adm_fin_{match_id}"))
    markup.add(types.InlineKeyboardButton("🔙 Back to Menu", callback_data="adm_nav_home"))
    return markup, res

def admin_dashboard_home(stats, matches):
    """Admin Dashboard ka main menu dikhata hai"""
    markup = types.InlineKeyboardMarkup(row_width=2)
    fraud_btn_text = f"⚠️ Fraud Alerts ({stats['flagged']})" if stats['flagged'] > 0 else "⚠️ Fraud Alerts"
    markup.add(
        types.InlineKeyboardButton("📊 Funnel", callback_data="adm_nav_funnel"),
        types.InlineKeyboardButton("👥 Recent Users", callback_data="adm_nav_recent"),
        types.InlineKeyboardButton("🔗 Referrals", callback_data="adm_nav_refs"),
        types.InlineKeyboardButton(fraud_btn_text, callback_data="adm_nav_fraud"),
        types.InlineKeyboardButton("🏆 Leaderboard", callback_data="adm_nav_lead"),
        types.InlineKeyboardButton("🔍 User Search", callback_data="adm_nav_get_user"),
    )
    markup.add(
        types.InlineKeyboardButton("📤 Data Backup", callback_data="adm_export_data"),
        types.InlineKeyboardButton("🛠 Setup Guide", callback_data="adm_nav_help"),
        types.InlineKeyboardButton("🔄 Refresh Data", callback_data="adm_nav_home")
    )
    # Add match control buttons
    if matches:
        markup.add(types.InlineKeyboardButton("━━━━━━━━━━━━━━", callback_data="ignore"))
        for mid, info in matches.items():
            # ⚡ Skip settled matches to keep the admin panel clean
            if info.get('points_calculated'):
                continue
                
            markup.row(
                types.InlineKeyboardButton(f"🎮 Control: {info['name']}", callback_data=f"adm_ctrl_{mid}"),
                types.InlineKeyboardButton("💰 Finance", callback_data=f"adm_fin_{mid}")
            )
            
    markup.add(types.InlineKeyboardButton("🔙 EXIT ADMIN", callback_data="app_home"))
    
    text = (
        "📊 <b>ADMIN DASHBOARD</b>\n"
        "━━━━━━━━━━━━━━\n"
        f"👥 Users: <code>{stats['total']}</code> | 🟢 Live: <code>{stats['active']}</code>\n"
        f"🆕 Today: <code>{stats['new']}</code> | 🚨 Fraud: <code>{stats['flagged']}</code>\n"
        "━━━━━━━━━━━━━━\n"
        f"💳 Paid: <code>{stats['paid']}</code> | 📈 Conv: <code>{stats['conv']}%</code>\n"
        "━━━━━━━━━━━━━━\n"
        "🔄 <i>Click Refresh for updates</i>"
    )
    return markup, text

def admin_help_render():
    """Admin help guide return karta hai"""
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("🔙 BACK TO DASHBOARD", callback_data="adm_nav_home"))
    
    text = """
🛠 <b>ADMIN CONTROL CENTER</b>
━━━━━━━━━━━━━━━━━━━━

🚀 <b>QUICK ACTIONS</b>
• <code>/admin_panel</code> - Main Dashboard
• <code>/broadcast</code> - Sabhi ko message bhein
• <code>/export_data</code> - Pura data backup lein

🏏 <b>MATCH SETUP FLOW (Step-by-Step)</b>
1️⃣ <code>/add_match</code> - Naya match details banayein
2️⃣ <code>/add_player</code> - Players add karein (RR vs DC)
   <i>Format: Name | Role | Desig | Team</i>
• <code>/set_live_link</code> - <b>(NEW)</b> Streaming link set karein
   <i>Ex: m1 | https://aapka-streaming-link.com</i>
3️⃣ <code>/setup_contests</code> - <b>Unlimited Jackpot/Saver</b> set karein
   <i>Format: fee | slots | J/S (J=Jackpot, S=Saver)</i>

🏆 <b>CONTESTS &amp; PLAYER MANAGEMENT</b>
• <code>/delete_contest</code> - Particular contest hatayein
• <code>/set_manual_prizes</code> - <b>(NEW)</b> Custom prize set karein
  <i>Format: mid | fee | R1 | R2 | R3 | R4-10 | Bottom | Winners</i>
• <code>/set_contest_size</code> - Contest modify <i>(mid | fee | slots | J/S)</i>
• <code>/set_prize_config</code> - Global commission/payout set karein
• <code>/my_matches</code> - Dashboard se match/player control karein (Buttons)
• <code>/list_players</code> - <b>(NEW)</b> Interactive Squad (Click karke delete karein)
• <code>/edit_player_role</code> - Player ka role badlein
• <code>/delete_player</code> - Squad se player nikalne ke liye

📈 <b>LIVE SCORING (REAL-TIME)</b>
• <code>/up</code> - Fast point update (Ex: <code>/up Kohli 50</code>)
• Admin Dashboard mein <b>Match Control</b> se 🔒 <b>LOCK / UNLOCK</b> karein.
• <code>/myrank</code> - User rank check karein

⚙️ <b>ADVANCED SETTINGS</b>
• <code>/set_fake_count</code> - <b>(HOT)</b> Display participants badhayein
• <code>/set_handle</code> - Support/Channel links update karein
•ar <code>/rules</code> - Point system update karein
• <code>/get_user_data</code> - User ka kacha-chittha nikalein
• <code>/audit_match</code> - 🛡️ <b>Fairness Audit</b> (Match end ke baad)
• <code>/rollback_match</code> - ⚠️ <b>Emergency:</b> Prizes wapas lein
• <code>/clear_database</code> - ⚠️ Pura data saaf karein
━━━━━━━━━━━━━━━━━━━━
💡 <i>Naya match setup karne ke liye Step 1, 2, 3 follow karein.</i>"""
    return markup, text

def admin_funnel_render(funnel_counts):
    steps = ["Start", "Team Init", "Team Save", "Payment"]
    max_val = funnel_counts[0] if funnel_counts[0] > 0 else 1
    
    res = "📈 *USER CONVERSION FUNNEL*\n━━━━━━━━━━━━━━━━━━━━\n"
    for i, count in enumerate(funnel_counts):
        perc = int((count / max_val) * 100)
        bar_len = int(perc / 10)
        bar = "🟩" * bar_len + "⬜" * (10 - bar_len)
        res += f"*{steps[i]}*\n`{bar}` {perc}%\n(Count: {count})\n\n"
    
    drop_off = 100 - int((funnel_counts[-1] / max_val) * 100) if max_val > 0 else 0
    res += f"📉 *Overall Drop-off:* `{drop_off}%`"
    
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("🔙 Back", callback_data="adm_nav_home"))
    return markup, res

def admin_fraud_render(fraud_list):
    res = "⚠️ *FRAUD DETECTION PANEL*\n━━━━━━━━━━━━━━━━━━━━\n"
    markup = types.InlineKeyboardMarkup(row_width=2)
    
    if not fraud_list:
        res += "✅ No high-risk users detected."
    else:
        for user in fraud_list:
            risk_icon = "🔴" if user['utr_count'] > 10 else "🟡"
            res += f"{risk_icon} *{user['first_name']}*\n`ID: {user['user_id']}`\nStatus: Flagged 🚩\n\n"
            markup.row(
                types.InlineKeyboardButton(f"🚫 Block", callback_data=f"adm_act_block_{user['user_id']}"),
                types.InlineKeyboardButton(f"✅ Clear Flag", callback_data=f"adm_act_unflag_{user['user_id']}")
            )
            
    markup.add(types.InlineKeyboardButton("🔄 REFRESH LIST", callback_data="adm_nav_fraud"))
    markup.add(types.InlineKeyboardButton("🔙 Back to Menu", callback_data="adm_nav_home"))
    return markup, res

def admin_referral_render(top_refs):
    res = "🔗 *REFERRAL INTELLIGENCE*\n━━━━━━━━━━━━━━━━━━━━\n"
    if not top_refs:
        res += "No referral data available."
    else:
        for i, ref in enumerate(top_refs, 1):
            uid = ref.get('referred_by', ref.get('username', 'N/A')) if isinstance(ref, dict) else ref[0]
            count = ref.get('count', ref.get('points', 0)) if isinstance(ref, dict) else ref[1]
            res += f"{i}. `ID:{uid}` ➔ *{count}*\n"
            
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("🔙 Back", callback_data="adm_nav_home"))
    return markup, res

def payment_instructions_render(order_id, amount, upi_id):
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(
        types.InlineKeyboardButton(f"📋 Copy UPI: {upi_id}", callback_data=f"copy_upi_{upi_id}"),
        types.InlineKeyboardButton("✅ I HAVE PAID", callback_data=f"paid_confirm_{order_id}"),
        types.InlineKeyboardButton("❌ CANCEL ORDER", callback_data="app_home")
    )
    text = f"""
💳 *SECURE PAYMENT*
━━━━━━━━━━━━━━
💰 Amount: *₹{amount}*
🆔 Order ID: `{order_id}`
━━━━━━━━━━━━━━
1️⃣ Upar di gayi UPI ID par payment karein.
2️⃣ **Payment ho jane ke baad**, uska Screenshot ya 12-digit UTR number yahan niche bhejein. ⚡
3️⃣ Verification hote hi aapka balance update ho jayega.
━━━━━━━━━━━━━━
⚠️ Expiry: 60 mins
"""
    return markup, text

def contest_list_render(matches):
    markup = types.InlineKeyboardMarkup(row_width=1)
    now = datetime.now()
    
    res = "🏆 *MATCHES*\n\n👉 *Next:* Select a match to join contests\n━━━━━━━━━━━━━━━━━━━━\n"
    
    for mid, info in matches.items():
        deadline = info['deadline']
        
        # Logic moved here to avoid circular import with final_bot
        m_lock = info.get('manual_lock', 0)
        if m_lock == 1: 
            is_locked = True
        elif m_lock == -1: 
            is_locked = False
        else:
            is_locked = datetime.now() > deadline
            
        time_left_delta = info['deadline'] - now

        status_icon = "🔒" if is_locked else "⏳"

        day_tag = "Today" if deadline.date() == now.date() else deadline.strftime('%d %b')
        time_str = deadline.strftime('%I:%M %p')
        
        btn_text = f"{status_icon} {info['name']}"
        markup.add(types.InlineKeyboardButton(btn_text, callback_data=f"show_match_{mid}"))

    res += "\n⚠️ *No team?* \n👉 Pehle team banao taaki Battle join kar sako."
    return markup, res

def get_prize_breakdown(fee, slots, custom_comm=None, match_id=None, contest_type='J'):
    """Calculates distribution where all winners get >= fee and top ranks get surplus"""
    if match_id:
        manual = db.db_get_manual_prizes(match_id, fee)
        if manual:
            # Manual prizes override automatic logic, type doesn't matter much here
            collection = fee * slots
            pool = manual['r1'] + manual['r2'] + manual['r3'] + (manual['r4_10'] * 7) + (manual['bottom'] * (manual['winners_count'] - 10))
            return {
                "collection": collection,
                "commission_amt": collection - pool,
                "comm_pct": round(((collection - pool) / collection) * 100, 1) if collection > 0 else 0,
                "pool": pool,
                "winners": manual['winners_count'],
                "1st": manual['r1'],
                "2nd": manual['r2'],
                "3rd": manual['r3'],
                "4-10": manual['r4_10'],
                "bottom": manual['bottom'],
                "bottom_range": f"11-{manual['winners_count']}"
            }

    # Fetch dynamic settings from DB with defaults
    if custom_comm is not None:
        comm_val = float(custom_comm)
    elif contest_type == 'S': # Logic based on explicit type
        comm_val = 10.0 # 10% Admin Cut for maximum user happiness
    else: # Mini-Jackpot (>=₹30) and Mega (>=₹100)
        comm_val = float(db.db_get_setting('PRIZE_COMMISSION', 18)) # Global setting ya default 18%
    
    if contest_type == 'J':
        # ⚡ JACKPOT: High Risk, High Reward
        # Winner kam, par jo jeetega wo bada jackpot le jayega
        win_pct = 25
        r1_pct, r2_pct, r3_pct, r4_pct, r5_pct = 70, 20, 10, 0, 0
    else:
        # 🛡️ SAVER: Low Risk, Maximum Refund (75% Winners)
        # Focus on returning entry fee to max users
        win_pct = 75
        r1_pct, r2_pct, r3_pct, r4_pct, r5_pct = 30, 15, 10, 5, 5

    # Platform commission logic
    commission_multiplier = (100 - comm_val) / 100
    collection = fee * slots
    pool = int(collection * commission_multiplier)
    # Ensure at least 1 winner if any users joined
    winners_count = max(1 if slots > 0 else 0, int(slots * (win_pct / 100)))

    # Step 1: Guarantee every winner gets at least their entry fee back
    total_base_cost = winners_count * fee
    
    # 🛡️ FIX: Agar pool total base cost se kam hai, toh winners kam karo
    # Taaki admin commission (18%) hamesha safe rahe.
    while total_base_cost > pool and winners_count > 1:
        winners_count -= 1
        total_base_cost = winners_count * fee

    surplus = max(0, pool - total_base_cost)
    
    top_total_pct = r1_pct + r2_pct + r3_pct + r4_pct + r5_pct
    remaining_pct = max(0, 100 - top_total_pct)

    # 🏆 Professional Scaling: Share remaining surplus among ALL winners after Rank 5
    others_count = max(0, winners_count - 5)
    
    # If no others, redistribute remaining surplus to Rank 1 to make jackpot bigger
    if others_count == 0 and winners_count > 0:
        r1_pct += remaining_pct
        remaining_pct = 0

    rest_surplus_share = int((surplus * (remaining_pct / 100)) / others_count) if others_count > 0 else 0

    prizes = {
        "1st": (fee if winners_count >= 1 else 0) + int(surplus * (r1_pct / 100)),
        "2nd": (fee if winners_count >= 2 else 0) + int(surplus * (r2_pct / 100)),
        "3rd": (fee if winners_count >= 3 else 0) + int(surplus * (r3_pct / 100)),
        "4th": (fee if winners_count >= 4 else 0) + int(surplus * (r4_pct / 100)),
        "5th": (fee if winners_count >= 5 else 0) + int(surplus * (r5_pct / 100)),
        "others": (fee if others_count > 0 else 0) + rest_surplus_share
    }
    return {
        "collection": collection, "commission_amt": collection - pool, "comm_pct": comm_val,
        "pool": pool, "winners": winners_count, 
        "1st": prizes["1st"], "2nd": prizes["2nd"], "3rd": prizes["3rd"],
        "4th": prizes["4th"], "5th": prizes["5th"], "6-10": prizes["others"],
        "bottom": prizes["others"], "bottom_range": f"6-{winners_count}"
    }

def prize_breakdown_render(match_id, fee, slots, contest_type='J'):
    breakdown = get_prize_breakdown(fee, slots, match_id=match_id, contest_type=contest_type)
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("🔙 Back to Match", callback_data=f"show_match_{match_id}"))
    
    # Determine Label
    c_type_label = "JACKPOT" if contest_type == 'J' else "TEAM SAVER"
    win_p = 25 if contest_type == 'J' else 75
    payout_pct = 90 if contest_type == 'S' else 82

    # Catchy labels
    r1_label = "🏆 JACKPOT" if fee >= 30 else "🥇 CHAMPION"
    r2_label = "🥈 STAR"

    # Build rank lines based on contest type
    rank_lines = (
        f"{r1_label}: *Rank 1* ➔ ₹{breakdown['1st']}\n"
        f"{r2_label}: *Rank 2* ➔ ₹{breakdown['2nd']}\n"
        f"🥉 *Rank 3* ➔ ₹{breakdown['3rd']}\n"
        f"🏅 *Rank 4* ➔ ₹{breakdown['4th']}\n"
        f"🏅 *Rank 5* ➔ ₹{breakdown['5th']}\n"
        f"🎖 *Rank 6-10* ➔ ₹{breakdown['6-10']} (Each)\n"
    )

    text = (
        f"🏆 *{c_type} BATTLE BREAKUP (₹{fee})*\n"
        f"👥 Total Slots: {slots} | 💰 Pool: ₹{breakdown['pool']}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{rank_lines}"
        f"💸 *Rank {breakdown['bottom_range']}:* ₹{breakdown['bottom']} (Full Refund)\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"✨ *Total Winners:* {breakdown['winners']} ({win_p}% of participants)\n"
        f"⚠️ *Note:* Prize Pool {payout_pct}% logic par hai. Slots full na hone par amount adjust ho jayega."
    )
    return markup, text

def match_dashboard_render(match_id, info, stats, user_summary, time_left, contest_configs=None, entry_fee=100):
    markup = types.InlineKeyboardMarkup(row_width=2)
    
    deadline = info['deadline']
    today = datetime.now().date()
    tomorrow = today + timedelta(days=1)
    
    if deadline.date() == today: day_tag = "Today 📅"
    elif deadline.date() == tomorrow: day_tag = "Tomorrow 🗓"
    else: day_tag = deadline.strftime('%d %b')

    deadline_time = deadline.strftime('%I:%M %p')
    
    avail_spots = stats['max_slots'] - stats['joined']
    
    live_link = info.get('live_link')
    entry_text = "🏆 *Available Battles:*"
    if live_link:
        markup.add(types.InlineKeyboardButton("📺 WATCH MATCH LIVE", url=live_link))
    live_text = "📺 Match is LIVE! Niche button se dekhein.\n" if live_link else ""

    if contest_configs:
        for cfg in contest_configs:
            fee = cfg['entry_fee']
            c_type = cfg.get('contest_type', 'J')
            
            if c_type == 'J': label = f"⚡ JACKPOT ₹{fee}"
            else: label = f"🛡️ SAVER ₹{fee}"

            markup.row(types.InlineKeyboardButton(label, callback_data=f"join_{match_id}_{fee}"),
                       types.InlineKeyboardButton("📋 Breakup", callback_data=f"breakup_{match_id}_{fee}"))
    else:
        entry_text = "❌ No active battles found"
    
    if not user_summary['saved']:
        markup.add(types.InlineKeyboardButton("🏏 PEHLE TEAM BANAO", callback_data=f"team_slots_{match_id}_1"))
    else:
        markup.add(types.InlineKeyboardButton("⚾ MY TEAMS", callback_data=f"team_slots_{match_id}_1"))

    markup.add(
        types.InlineKeyboardButton("📊 Leaderboard", callback_data=f"app_match_{match_id}"),
        types.InlineKeyboardButton("🏏 Player Stats", callback_data=f"show_player_stats_{match_id}")
    )
    markup.add(types.InlineKeyboardButton("🔙 Match List", callback_data="contest_list"))

    text = f"""
🏏 *{info['name']}*
📅 {day_tag} • Deadline: {deadline_time}
⏰ Time Left: {time_left}
━━━━━━━━━━━━━━━━━━━━
💰 *Prize Pool: ₹{stats['prize_pool']}*
{live_text}
{entry_text}

👥 {stats['joined']}/{stats['max_slots']} spots filled
✅ {avail_spots} spots available
━━━━━━━━━━━━━━━━━━━━
👉 *Next: Team banao aur BATTLE join karo!*"""
    return markup, text

def player_stats_render(match_id, match_name, stats, point_system):
    res = f"📊 *PLAYER LIVE STATS: {match_name}*\n━━━━━━━━━━━━━━━━━━━━\n"
    if not stats:
        res += "_No stats recorded yet. Points update as soon as events occur._"
    else:
        for p in stats:
            pts = (p['runs'] * point_system.get('run', 1) + 
                   p['fours'] * point_system.get('four', 4) + 
                   p['sixes'] * point_system.get('six', 6) + 
                   p['wickets'] * point_system.get('wicket', 25))
            res += f"👤 *{p['player_name']}*\n"
            res += f"└ {p['runs']} runs | {p['fours']}x4 | {p['sixes']}x6 | {p['wickets']} wkts\n"
            res += f"⭐ *Points:* `{int(pts)}` \n\n"
            
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("🔄 Refresh Score", callback_data=f"show_player_stats_{match_id}"),
        types.InlineKeyboardButton("🔙 Back to Match", callback_data=f"show_match_{match_id}")
    )
    return markup, res

def contest_selection_render(match_id, match_name):
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(
        types.InlineKeyboardButton("🏅 Mega Battle (₹100) - 💎 High Prize", callback_data=f"sel_team_{match_id}_100"),
        types.InlineKeyboardButton("🥈 Mid Battle (₹50) - 🔥 Low Comp", callback_data=f"sel_team_{match_id}_50"),
        types.InlineKeyboardButton("🥉 Small Battle (₹20) - 🔰 Beginner", callback_data=f"sel_team_{match_id}_20"),
        types.InlineKeyboardButton("🔙 Back to Match", callback_data=f"show_match_{match_id}")
    )
    text = f"🏆 *{match_name}* - Battle Selection\n\nChoose an entry level to compete. Each battle has different prize pools and competition levels."
    return markup, text

def team_points_breakdown_render(match_id, team_num, team_data, player_stats_map):
    res = f"📊 *TEAM PERFORMANCE | T{team_num}*\n"
    res += "━━━━━━━━━━━━━━━━━━━━\n"
    total = 0
    
    # Professional Role Mapping with Emojis
    role_info = {
        'wk': ('🧤', 'WICKETKEEPERS'),
        'bat': ('🏏', 'BATSMEN'),
        'ar': ('🌟', 'ALL-ROUNDERS'),
        'bowl': ('🥎', 'BOWLERS'),
        'sub': ('🔄', 'IMPACT PLAYERS')
    }

    for role in ['wk', 'bat', 'ar', 'bowl', 'sub']:
        p_list = team_data.get(role, [])
        if not p_list: continue
        
        icon, label = role_info[role]
        res += f"\n{icon} *{label}*\n"
        
        for p in p_list:
            stats = player_stats_map.get(p, {'runs': 0, 'fours': 0, 'sixes': 0, 'wickets': 0})
            raw_pts = (stats['runs'] * 1 + stats['fours'] * 4 + stats['sixes'] * 6 + stats['wickets'] * 25)
            
            mult = 1.0
            tag = ""
            if p == team_data.get('captain'): mult, tag = 2.0, " (C) 👑"
            elif p == team_data.get('vice_captain'): mult, tag = 1.5, " (VC) ⭐"
            
            p_final = int(raw_pts * mult)
            total += p_final
            
            res += f"👤 `{p}`{tag}\n"
            res += f"└ ⚡ *{p_final} pts* — ({stats['runs']}R | {stats['wickets']}W)\n"
            
    res += "\n━━━━━━━━━━━━━━━━━━━━\n"
    res += f"🏆 *TOTAL SCORE: {total} PTS*"
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("🔙 Back to Team", callback_data=f"view_team_{match_id}_{team_num}"))
    return markup, res

def team_slot_picker_render(user_id, match_id, fee, db_helper):
    markup = types.InlineKeyboardMarkup(row_width=4)
    res = f"⚾ *JOIN BATTLE (₹{fee})*\n━━━━━━━━━━━━━━━━━━━━\n"
    res += "Select a team slot to join with:\n\n"
    
    buttons = []
    for i in range(1, 11): # Showing first 10 slots
        status = db_helper(user_id, match_id, i)
        if status in ["paid", "unpaid"]:
            # Saved teams are always selectable for re-joining
            icon = "✅" if status == "paid" else "💾"
            cb = f"confirm_join_{match_id}_{i}_{fee}"
        else:
            icon, cb = "⚪", f"nav_bat_{match_id}_{i}"
        
        buttons.append(types.InlineKeyboardButton(f"T{i}{icon}", callback_data=cb))
        
    markup.add(*buttons)
    markup.add(types.InlineKeyboardButton("🔙 Back to Selection", callback_data=f"choose_contest_{match_id}"))
    
    res += "✅ Paid & Ready\n❌ Not Paid (Select to Join)\n⚪ Empty (Select to Create)"
    return markup, res

def team_view_render(match_id, match_name, team_num, team, is_locked, joined_fees=None):
    """Professional team preview with icons and organized layout"""
    if joined_fees:
        # List joined contests like: ₹100, ₹100, ₹50
        contests_str = ", ".join([f"₹{f}" for f in joined_fees])
        status = f"✅ JOINED: {contests_str}"
    else:
        status = "⚠️ UNPAID (Not in Battle)"
    
    res = f"📋 *TEAM PREVIEW | T{team_num}*\n"
    res += f"🏟 *Match:* {match_name}\n"
    res += f"💳 *Status:* {status}\n"
    res += "━━━━━━━━━━━━━━━━━━━━\n"

    # Role icons for professional look
    icons = {'wk': '🧤', 'bat': '🏏', 'ar': '⭐', 'bowl': '🥎', 'sub': '🔄'}

    for role in ['wk', 'bat', 'ar', 'bowl', 'sub']:
        p_list = team.get(role, [])
        if not p_list: continue

        role_label = "IMPACT PLAYER" if role == 'sub' else role.upper()
        res += f"\n{icons.get(role, '👤')} *{role_label}*\n"
        
        for p in p_list:
            tag = ""
            if p == team.get('captain'): tag = " (C) 👑"
            elif p == team.get('vice_captain'): tag = " (VC) ⭐"
            res += f" ├ `{p}`{tag}\n"

    res += "\n━━━━━━━━━━━━━━━━━━━━\n"
    res += "👉 _Match shuru hone par live points yahan dikhenge._"

    markup = types.InlineKeyboardMarkup(row_width=1)
    if not is_locked:
        markup.add(types.InlineKeyboardButton("✏️ EDIT SQUAD", callback_data=f"nav_bat_{match_id}_{team_num}"))
        markup.add(types.InlineKeyboardButton("👑 SET CAPTAIN / VC", callback_data=f"set_cv_menu_{match_id}_{team_num}"))
        
        if not team.get('is_paid'):
            markup.add(types.InlineKeyboardButton("🚀 JOIN BATTLE NOW", callback_data=f"show_match_{match_id}"))
            markup.add(types.InlineKeyboardButton("🗑️ DELETE TEAM", callback_data=f"del_team_ask_{match_id}_{team_num}"))

    markup.add(
        types.InlineKeyboardButton("📊 POINTS BREAKDOWN", callback_data=f"pts_break_{match_id}_{team_num}"),
        types.InlineKeyboardButton("🔄 REFRESH", callback_data=f"view_team_{match_id}_{team_num}"),
        types.InlineKeyboardButton("🔙 BACK TO SLOTS", callback_data=f"team_slots_{match_id}")
    )
    
    return markup, res

def user_results_list_render(results):
    """Professional view for completed matches history"""
    markup = types.InlineKeyboardMarkup(row_width=1)
    res = "📜 *MY MATCH HISTORY*\n\nAapne jo matches khele hain unka result yahan hai:\n━━━━━━━━━━━━━━━━━━━━\n"
    
    if not results:
        res += "\n_Abhi tak koi completed match nahi mila._"
    else:
        for r in results:
            # Prize emoji logic
            p_icon = "💰" if "₹0" not in r['prize'] else "📉"
            res += (
                f"🏟 *{r['match_name']}*\n"
                f"📅 {r['timestamp'][:10]} | Rank: *#{r['rank']}*\n"
                f"⭐ Points: `{r['points']}` | {p_icon} Won: *{r['prize']}*\n"
                "────────────────────\n"
            )

    markup.add(types.InlineKeyboardButton("🔄 Refresh", callback_data="my_results"))
    markup.add(types.InlineKeyboardButton("🏠 Back to Home", callback_data="app_home"))
    return markup, res

def transaction_item_render(item):
    """Renders a single transaction item in a professional, human-readable format."""
    sign = "✅ +" if item['type'] == 'CREDIT' else "❌ -"
    amount = abs(item['amount'])
    date_time = datetime.strptime(item['timestamp'], '%Y-%m-%d %H:%M:%S').strftime('%d-%m %H:%M')
    ref_id = item['reference_id']
    
    description = "Unknown Transaction"
    icon = "❓"

    if ref_id.startswith("PRIZE_"):
        parts = ref_id.split('_')
        # PRIZE_{match_id}_{fee}_{rank}_{user_id}_{team_num}
        match_id = parts[1]
        fee = parts[2]
        rank = parts[3]
        # Fetch contest type from DB if possible, otherwise default
        cfg = db.db_get_contest_config(match_id, fee)
        c_type = cfg.get('contest_type', 'J') if cfg else ('J' if int(fee) >= 30 else 'S')
        type_name = "JACKPOT" if c_type == 'J' else "TEAM SAVER"
        description = f"🏆 Prize: {type_name} (₹{fee}, Rank #{rank})"
        icon = "💰"
    elif ref_id.startswith("DEBIT_MATCH_"):
        parts = ref_id.split('_')
        # DEBIT_MATCH_{mid}_{tnum}_{ref_id}
        match_id = parts[2]
        team_num = parts[3]
        description = f"🏏 Contest Join: {match_id} (T{team_num})"
        icon = "⚔️"
    elif ref_id.startswith("MANUAL_"):
        description = "➕ Manual Deposit (Admin)"
        icon = "🧑‍💻"
    elif ref_id.startswith("UTR_"):
        description = f"⬆️ UPI Deposit (UTR: {ref_id[4:]})"
        icon = "💳"
    elif ref_id.startswith("REF_BONUS_"):
        description = "🎁 Referral Bonus"
        icon = "🤝"
    elif ref_id.startswith("WD_REF_"):
        description = "⬇️ Withdrawal"
        icon = "💸"

    return f"`{date_time}` | {sign}₹{amount} | {icon} {description}"

def audit_report_render(match_id, match_name, audit):
    """Generates a professional fairness report for admin"""
    # Consistency check
    is_fair = (audit['entries'] == audit['db_paid_teams'])
    status_icon = "✅ PASS" if is_fair else "⚠️ DISCREPANCY"
    
    # Admin profit calculation
    profit = audit['in'] - audit['out']
    
    res = (
        f"🛡️ *MATCH AUDIT REPORT*\n"
        f"🏟 Match: `{match_name}`\n"
        f"🆔 ID: `{match_id}`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 *FINANCIAL CHECK:*\n"
        f"📥 Total Collected: `₹{audit['in']}`\n"
        f"📤 Total Distributed: `₹{audit['out']}`\n"
        f"📈 Retained Profit: `₹{round(profit, 2)}`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👥 *INTEGRITY CHECK:*\n"
        f"📑 Ledger Entries: `{audit['entries']}`\n"
        f"⚾ Paid Team Slots: `{audit['db_paid_teams']}`\n"
        f"🏁 Winners Paid: `{audit['winners']}`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 *Verdict:* {status_icon}\n"
    )
    if not is_fair:
        res += "\n🚨 *ALERT:* Ledger aur Team count match nahi kar rahe. Kuch users ko free entry mili ho sakti hai!"
    
    return res
