import time
from datetime import datetime
from telebot import types

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

def admin_dashboard_home(stats, matches):
    markup = types.InlineKeyboardMarkup(row_width=2)
    fraud_btn_text = f"⚠️ Fraud Alerts ({stats['flagged']})" if stats['flagged'] > 0 else "⚠️ Fraud Alerts"
    markup.add(
        types.InlineKeyboardButton("📊 Funnel", callback_data="adm_nav_funnel"),
        types.InlineKeyboardButton("🔗 Referrals", callback_data="adm_nav_refs"),
        types.InlineKeyboardButton(fraud_btn_text, callback_data="adm_nav_fraud"),
        types.InlineKeyboardButton("🏆 Leaderboard", callback_data="adm_nav_lead"),
        types.InlineKeyboardButton("❓ Commands Help", callback_data="adm_nav_help"),
        types.InlineKeyboardButton("🔄 Refresh Data", callback_data="adm_nav_home")
    )
    # Add match control buttons
    markup.add(types.InlineKeyboardButton("--- Match Controls ---", callback_data="ignore_match_control_header"))
    for mid, info in matches.items():
        markup.add(types.InlineKeyboardButton(f"🎮 Control: {info['name']}", callback_data=f"adm_ctrl_{mid}"))
    markup.add(types.InlineKeyboardButton("🔙 EXIT ADMIN", callback_data="app_home"))
    
    text = (
        "📊 *ADMIN DASHBOARD*\n"
        "━━━━━━━━━━━━━━\n"
        f"👥 Users: `{stats['total']}` | 🟢 Live: `{stats['active']}`\n"
        f"🆕 Today: `{stats['new']}` | 🚨 Fraud: `{stats['flagged']}`\n"
        "━━━━━━━━━━━━━━\n"
        f"💳 Paid: `{stats['paid']}` | 📈 Conv: `{stats['conv']}%`\n"
        "━━━━━━━━━━━━━━\n"
        "🔄 _Click Refresh for updates_"
    )
    return markup, text

def admin_help_render():
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("🔙 BACK TO DASHBOARD", callback_data="adm_nav_home"))
    text = (
        "🛠 *ADMIN CONTROL CENTER*\n"
        "━━━━━━━━━━━━━━\n"
        "📌 *Essential Commands:*\n"
        "• `/add_match` - Add new match details\n"
        "• `/add_player` - Add players to match\n"
        "• `/set_contest_size` - Prize pool setup\n"
        "• `/update_points` - Live score updates\n"
        "• `/set_handle` - Update Links (Format: `TYPE | HANDLE`)\n"
        "• `/broadcast` - Send message to all\n"
        "• `/download_db` - Database backup\n\n"
        "⚖️ *System Rules:*\n"
        "• *Scoring:* Run:1 | 4s:4 | 6s:6 | Wkt:25\n"
        "• *Multipliers:* Captain 2x | VC 1.5x\n"
        "• *Withdrawal:* Minimum ₹200\n"
        "• *Fee:* 10% Platform Commission\n"
        "━━━━━━━━━━━━━━\n"
        "💡 _In commands ko direct chat mein type karein._"
    )
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
    text = (
        "💳 *SECURE PAYMENT*\n"
        "━━━━━━━━━━━━━━\n"
        f"🆔 ID: `{order_id}` | 💰 Amt: *₹{amount}*\n"
        "━━━━━━━━━━━━━━\n"
        "✅ *Steps:*\n"
        "1. Upar di gayi UPI par pay karein.\n"
        "2. **12-digit UTR** yahan bhein. (Fastest) ⚡\n"
        "3. Ya screenshot upload karein. ⏳\n"
        "━━━━━━━━━━━━━━\n"
        "⚠️ _Expiry: 60 mins_"
    )
    return markup, text

def contest_list_render(matches):
    markup = types.InlineKeyboardMarkup(row_width=1)
    now = datetime.now()
    
    res = "🏆 *UPCOMING CONTESTS*\n━━━━━━━━━━━━━━━━━━━━\n"
    
    for mid, info in matches.items():
        deadline = info['deadline']
        is_locked = now > deadline
        
        status_icon = "🔒" if is_locked else "⏳"
        if is_locked and (now - deadline).seconds < 14400: # 4 hours live window
            status_icon = "🟢"
            
        time_str = info['deadline'].strftime('%H:%M')
        btn_text = f"{status_icon} {info['name']} | {time_str}"
        markup.add(types.InlineKeyboardButton(btn_text, callback_data=f"show_match_{mid}"))
        
    res += "_Select a match to view Prize Pools and Join._"
    return markup, res

def get_prize_breakdown(fee, slots, commission_pct=10):
    """Calculates distribution where ~70% of players win"""
    # Platform commission set to 10%
    commission_multiplier = (100 - commission_pct) / 100
    collection = fee * slots
    pool = int(collection * commission_multiplier) 
    winners_count = int(slots * 0.7) # 70% winners

    # 70% Winners Logic: 
    # Ranks 11 to (70% of slots) get their entry fee back.
    # Top 10 share the surplus.
    
    refund_winners = max(0, winners_count - 10)
    refund_total = refund_winners * fee
    
    # Surplus calculation with safety check
    surplus_pool = max(100, pool - refund_total)
    
    # Distribution of surplus among Top 10
    # (These values are relative percentages of the top pool)
    prizes = {
        "1st": int(surplus_pool * 0.35),
        "2nd": int(surplus_pool * 0.20),
        "3rd": int(surplus_pool * 0.12),
        "4-10": int((surplus_pool * 0.33) / 7)
    }

    return {
        "pool": pool, "winners": winners_count,
        "1st": prizes["1st"], "2nd": prizes["2nd"], "3rd": prizes["3rd"],
        "4-10": prizes["4-10"], "bottom": fee, "bottom_range": f"11-{winners_count}"
    }

def match_dashboard_render(match_id, info, stats, user_summary, time_left, contest_configs=None):
    markup = types.InlineKeyboardMarkup(row_width=2)
    
    prize_breakdown_text = ""
    if contest_configs:
        prize_breakdown_text = "🏆 *PRIZES:*\n"
        for cfg in contest_configs:
            bd = get_prize_breakdown(cfg['entry_fee'], cfg['max_slots'])
            prize_breakdown_text += f"💰 ₹{cfg['entry_fee']} -> 🥇 ₹{bd['1st']} | 🥈 ₹{bd['2nd']} | ✅ 11-{bd['winners']}: ₹{bd['bottom']}\n"

    # Contest Entry Buttons
    if contest_configs:
        for cfg in contest_configs:
            fee = cfg['entry_fee']
            markup.add(types.InlineKeyboardButton(f"🏆 Join Contest (₹{fee})", callback_data=f"join_{match_id}_{fee}"))
    else:
        markup.add(types.InlineKeyboardButton("🏅 Mega ₹100", callback_data=f"join_{match_id}_100"))
    
    # Logic for One-Click Join or Create
    if not user_summary['saved']:
        markup.add(types.InlineKeyboardButton("⚾ CREATE YOUR FIRST TEAM", callback_data=f"team_slots_{match_id}_1"))
    else:
        markup.add(types.InlineKeyboardButton("📋 My Teams / Status", callback_data=f"team_slots_{match_id}_1"))

    markup.add(
        types.InlineKeyboardButton("🏆 Leaderboard", callback_data=f"app_match_{match_id}"),
        types.InlineKeyboardButton("🔙 Back", callback_data="contest_list")
    )

    # User Status Block
    u_status = f"✅ {len(user_summary['paid'])} Paid | 📝 {len(user_summary['saved'])} Saved"
    if user_summary['incomplete']:
        u_status += f" | ⚠️ {len(user_summary['incomplete'])} Incomplete"

    text = (
        f"🏏 *{info['name']}*\n"
        f"👥 Joined: `{stats['joined']}` | ⏳ `{time_left}`\n"
        "━━━━━━━━━━━━━━\n"
        f"{prize_breakdown_text}\n"
        f"👤 *My Status:* {u_status}\n"
        "━━━━━━━━━━━━━━"
    )
    return markup, text

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