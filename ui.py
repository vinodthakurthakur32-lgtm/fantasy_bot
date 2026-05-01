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
        types.InlineKeyboardButton("🏆 Ranks", callback_data="app_global_ranks")
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
    comm_pct = float(db.db_get_setting('PRIZE_COMMISSION', 10))
    total_collection = fin_data['collection']
    admin_cut = (total_collection * comm_pct) / 100
    prize_pool = total_collection - admin_cut
    
    res = (
        f"💰 *FINANCIAL SUMMARY: {match_name}*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📈 Total Collection: `₹{total_collection}`\n"
        f"✂️ Admin Cut ({comm_pct}%): `₹{admin_cut}`\n"
        f"🎁 Total Prize Pool: `₹{prize_pool}`\n"
        f"👥 Paid Entries: `{fin_data['entries']}`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🏆 *PRIZE BREAKDOWN (Per Contest Type):*\n"
    )
    
    for cfg in fin_data['configs']:
        bd = get_prize_breakdown(cfg['entry_fee'], cfg['max_slots'], match_id=match_id)
        res += (
            f"\n📍 *Contest ₹{cfg['entry_fee']} ({cfg['max_slots']} slots):*\n"
            f"🥇 1st: ₹{bd['1st']} | 🥈 2nd: ₹{bd['2nd']}\n"
            f"🥉 3rd: ₹{bd['3rd']} | 🏅 4-10: ₹{bd['4-10']}\n"
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
        types.InlineKeyboardButton("🔗 Referrals", callback_data="adm_nav_refs"),
        types.InlineKeyboardButton(fraud_btn_text, callback_data="adm_nav_fraud"),
        types.InlineKeyboardButton("🏆 Leaderboard", callback_data="adm_nav_lead"),
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
   <i>Tip: Horizontal bhein (Name|w, Name|bat)</i>
3️⃣ <code>/setup_contests</code> - Ek sath Mega/Med/Small set karein

🏆 <b>CONTESTS &amp; PLAYER MANAGEMENT</b>
• <code>/delete_contest</code> - Particular contest hatayein
• <code>/set_manual_prizes</code> - <b>(NEW)</b> Custom prize set karein
  <i>Format: mid | fee | R1 | R2 | R3 | R4-10 | Bottom | Winners</i>
• <code>/set_contest_size</code> - Single contest modify karein
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
• <code>/rules</code> - Point system update karein
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
            res += f"{i}. `ID:{ref[0]}` ➔ *{ref[1]} Invites*\n"
            
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

    res += "\n⚠️ *No team?* \n👉 Pehle team banao niche buttons se."
    return markup, res

def get_prize_breakdown(fee, slots, custom_comm=None, match_id=None):
    """Calculates distribution where all winners get >= fee and top ranks get surplus"""
    if match_id:
        manual = db.db_get_manual_prizes(match_id, fee)
        if manual:
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
    comm_val = float(custom_comm) if custom_comm is not None else float(db.db_get_setting('PRIZE_COMMISSION', 10))
    win_pct = float(db.db_get_setting('PRIZE_WINNERS_PCT', 70))
    r1_pct = float(db.db_get_setting('PRIZE_R1_PCT', 35))
    r2_pct = float(db.db_get_setting('PRIZE_R2_PCT', 20))
    r3_pct = float(db.db_get_setting('PRIZE_R3_PCT', 12))

    # Platform commission logic
    commission_multiplier = (100 - comm_val) / 100
    collection = fee * slots
    pool = int(collection * commission_multiplier)
    winners_count = int(slots * (win_pct / 100)) # Custom % winners

    # Step 1: Guarantee every winner gets at least their entry fee back
    total_base_cost = winners_count * fee
    
    # Safety check: if pool is too small for winner %, reduce winner count
    if total_base_cost > pool:
        winners_count = pool // fee
        total_base_cost = winners_count * fee

    # Step 2: Calculate Surplus (extra money above the entry fee refunds)
    surplus = pool - total_base_cost
    
    top3_total_pct = r1_pct + r2_pct + r3_pct
    remaining_pct = max(0, 100 - top3_total_pct)

    prizes = {
        "1st": fee + int(surplus * (r1_pct / 100)),
        "2nd": fee + int(surplus * (r2_pct / 100)),
        "3rd": fee + int(surplus * (r3_pct / 100)),
        "4-10": fee + int((surplus * (remaining_pct / 100)) / 7)
    }
    return {
        "collection": collection, "commission_amt": collection - pool, "comm_pct": comm_val,
        "pool": pool, "winners": winners_count, 
        "1st": prizes["1st"], "2nd": prizes["2nd"], "3rd": prizes["3rd"],
        "4-10": prizes["4-10"], "bottom": fee, "bottom_range": f"11-{winners_count}"
    }

def prize_breakdown_render(match_id, fee, slots):
    breakdown = get_prize_breakdown(fee, slots, match_id=match_id)
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("🔙 Back to Match", callback_data=f"show_match_{match_id}"))
    
    text = (
        f"🏆 *PRIZE BREAKUP (₹{fee} Contest)*\n"
        f"👥 Total Slots: {slots} | 💰 Pool: ₹{breakdown['pool']}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🥇 *Rank 1:* ₹{breakdown['1st']}\n"
        f"🥈 *Rank 2:* ₹{breakdown['2nd']}\n"
        f"🥉 *Rank 3:* ₹{breakdown['3rd']}\n"
        f"🏅 *Rank 4-10:* ₹{breakdown['4-10']} each\n"
        f"🎖 *Rank {breakdown['bottom_range']}:* ₹{breakdown['bottom']} (Refund)\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"✨ *Total Winners:* {breakdown['winners']} (70% of slots)\n"
        f"⚠️ _Note: Prize pool calculation slots full hone par based hai._"
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
    
    if contest_configs:
        for cfg in contest_configs:
            fee = cfg['entry_fee']
            
            # Custom labeling based on entry level
            if fee >= 100: label = f"🥇 Mega ₹{fee}"
            elif fee >= 50: label = f"🥈 Medium ₹{fee}"
            else: label = f"🥉 Small ₹{fee}"

            markup.row(types.InlineKeyboardButton(label, callback_data=f"join_{match_id}_{fee}"),
                       types.InlineKeyboardButton("📋 Breakup", callback_data=f"breakup_{match_id}_{fee}"))
    else:
        markup.row(types.InlineKeyboardButton("🏅 Join Mega ₹100", callback_data=f"join_{match_id}_100"),
                   types.InlineKeyboardButton("📋 Breakup", callback_data=f"breakup_{match_id}_100"))
    
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
🎯 Entry: ₹{entry_fee}

👥 {stats['joined']}/{stats['max_slots']} spots filled
✅ {avail_spots} spots available
━━━━━━━━━━━━━━━━━━━━
👉 *Next: Team banao aur contest join karo!*"""
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
        types.InlineKeyboardButton("🏅 Mega Contest (₹100) - 💎 High Prize", callback_data=f"sel_team_{match_id}_100"),
        types.InlineKeyboardButton("🥈 Mid Contest (₹50) - 🔥 Low Comp", callback_data=f"sel_team_{match_id}_50"),
        types.InlineKeyboardButton("🥉 Small Contest (₹20) - 🔰 Beginner", callback_data=f"sel_team_{match_id}_20"),
        types.InlineKeyboardButton("🔙 Back to Match", callback_data=f"show_match_{match_id}")
    )
    text = f"🏆 *{match_name}* - Contest Selection\n\nChoose an entry level to compete. Each contest has different prize pools and competition levels."
    return markup, text

def team_points_breakdown_render(match_id, team_num, team_data, player_stats_map):
    res = f"📊 *TEAM PERFORMANCE (T{team_num})*\n━━━━━━━━━━━━━━━━━━━━\n"
    total = 0
    for role in ['bat', 'wk', 'ar', 'bowl', 'sub']:
        p_list = team_data.get(role, [])
        if not p_list: continue
        role_label = "IMPACT/SUB" if role == 'sub' else role.upper()
        res += f"\n*{role_label}*\n"
        for p in p_list:
            stats = player_stats_map.get(p, {'runs': 0, 'fours': 0, 'sixes': 0, 'wickets': 0})
            raw_pts = (stats['runs'] * 1 + stats['fours'] * 4 + stats['sixes'] * 6 + stats['wickets'] * 25)
            
            mult = 1.0
            tag = ""
            if p == team_data.get('captain'): mult, tag = 2.0, "(C)"
            elif p == team_data.get('vice_captain'): mult, tag = 1.5, "(VC)"
            
            p_final = int(raw_pts * mult)
            total += p_final
            res += f"👤 {p} {tag}\n"
            res += f"└ {stats['runs']} R | {stats['wickets']} W | `{p_final} pts`\n"
            
    res += f"━━━━━━━━━━━━━━━━━━━━\n⭐ *Total Points: {total}*"
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("🔙 Back to Team", callback_data=f"view_team_{match_id}_{team_num}"))
    return markup, res

def team_slot_picker_render(user_id, match_id, fee, db_helper):
    markup = types.InlineKeyboardMarkup(row_width=4)
    res = f"⚾ *JOIN CONTEST (₹{fee})*\n━━━━━━━━━━━━━━━━━━━━\n"
    res += "Select a team slot to join with:\n\n"
    
    buttons = []
    for i in range(1, 11): # Showing first 10 slots
        status = db_helper(user_id, match_id, i)
        if status == "paid":
            icon, cb = "✅", f"already_joined"
        elif status == "unpaid":
            icon, cb = "❌", f"final_join_{match_id}_{i}_{fee}"
        else:
            icon, cb = "⚪", f"nav_bat_{match_id}_{i}"
        
        buttons.append(types.InlineKeyboardButton(f"T{i}{icon}", callback_data=cb))
        
    markup.add(*buttons)
    markup.add(types.InlineKeyboardButton("🔙 Back to Selection", callback_data=f"choose_contest_{match_id}"))
    
    res += "✅ Paid & Ready\n❌ Not Paid (Select to Join)\n⚪ Empty (Select to Create)"
    return markup, res
