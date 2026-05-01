import html
from telebot import types
import db, ui
from telebot.apihelper import ApiTelegramException

def admin_main_markup(matches):
    markup = types.InlineKeyboardMarkup(row_width=1)
    for mid, info in matches.items():
        markup.add(types.InlineKeyboardButton(f"🎮 Control: {info['name']}", callback_data=f"adm_ctrl_{mid}"))
    return markup

def admin_event_markup(match_id, players, active_role='bat', is_locked=False, stats_map=None):
    markup = types.InlineKeyboardMarkup(row_width=4)
    
    # 🔒 Manual Lock Toggle Button
    lock_btn_text = "🔓 UNLOCK MATCH (Deadline Ignore)" if is_locked else "🔒 FORCE LOCK (Stop Entries)"
    lock_action = f"adm_toggle_lock_{match_id}_{'unlock' if is_locked else 'lock'}"
    markup.row(types.InlineKeyboardButton(lock_btn_text, callback_data=lock_action))

    # 1. Role Filter Buttons (Panel ko chhota rakhne ke liye)
    role_btns = []
    for r in ['bat', 'wk', 'ar', 'bowl', 'sub']:
        label = f"» {r.upper()} «" if r == active_role else r.upper()
        role_btns.append(types.InlineKeyboardButton(label, callback_data=f"adm_filter_{match_id}_{r}"))
    markup.row(*role_btns)

    # 2. Only show players of active role
    if active_role in players:
        for p_obj in players.get(active_role, []):
            # Clean name aur Display name ko alag kiya
            p_name = p_obj['name']
            p_display = p_obj['display']
            # Fetch current stats for the button label
            stats = stats_map.get(p_name, {'runs': 0, 'wickets': 0}) if stats_map else {'runs': 0, 'wickets': 0}
            stats_label = f"({stats['runs']}R, {stats['wickets']}W)"
            
            # Row 1: Player Full Name + Current Score
            markup.row(types.InlineKeyboardButton(f"👤 {p_display} {stats_label}", callback_data="ignore"))
            # Row 2: Incremental buttons
            markup.row(
                types.InlineKeyboardButton("+1 R", callback_data=f"evt_{match_id}_{p_name.replace(' ', '_')}_run"),
                types.InlineKeyboardButton("+4 R", callback_data=f"evt_{match_id}_{p_name.replace(' ', '_')}_four"),
                types.InlineKeyboardButton("+6 R", callback_data=f"evt_{match_id}_{p_name.replace(' ', '_')}_six"),
                types.InlineKeyboardButton("+1 W", callback_data=f"evt_{match_id}_{p_name.replace(' ', '_')}_wicket")
            )

    # Match locked hai toh Result declare karne ka option dein
    if is_locked:
        markup.row(types.InlineKeyboardButton("🏁 DECLARE FINAL RESULT & PAY WINNERS", callback_data=f"adm_settle_{match_id}"))

    # Bulk Update Option
    markup.row(types.InlineKeyboardButton("📝 BULK TOTALS (Chunk Update)", callback_data=f"adm_bulk_up_{match_id}"))
    
    markup.add(types.InlineKeyboardButton("🔙 Back to Dashboard", callback_data="adm_nav_home"))
    return markup

def handle_admin_nav(call, bot):
    import final_bot # Local import to prevent circular dependency errors
    nav = call.data
    chat_id = call.message.chat.id
    mid = call.message.message_id
    
    if nav == "adm_nav_home":
        bot.answer_callback_query(call.id)
        stats = db.get_admin_stats()
        markup, text = ui.admin_dashboard_home(stats, final_bot.MATCHES)
        bot.edit_message_text(text, chat_id, mid, reply_markup=markup, parse_mode='HTML')

    elif nav == "adm_nav_lead":
        bot.answer_callback_query(call.id)
        rows = final_bot.get_leaderboard(10)
        markup, text = ui.admin_referral_render(rows) # Re-using referral style for leaderboard
        bot.edit_message_text(text, chat_id, mid, reply_markup=markup, parse_mode='HTML')
        
    elif nav == "adm_export_data":
        bot.answer_callback_query(call.id, "Generating Backup...")
        final_bot.cmd_export_data(call.message)
        
    elif nav == "adm_nav_funnel":
        bot.answer_callback_query(call.id)
        data = db.get_funnel_data()
        markup, text = ui.admin_funnel_render(data)
        bot.edit_message_text(text, chat_id, mid, reply_markup=markup, parse_mode='HTML')

    elif nav == "adm_nav_fraud":
        bot.answer_callback_query(call.id)
        data = db.get_fraud_list()
        markup, text = ui.admin_fraud_render(data)
        bot.edit_message_text(text, chat_id, mid, reply_markup=markup, parse_mode='HTML')

    elif nav.startswith("adm_fin_"):
        bot.answer_callback_query(call.id)
        match_id = nav.split("_")[2]
        match_name = final_bot.MATCHES.get(match_id, {}).get('name', match_id)
        
        fin_data = db.db_get_match_financials(match_id)
        markup, text = ui.admin_match_finance_render(match_id, match_name, fin_data)
        bot.edit_message_text(text, chat_id, mid, reply_markup=markup, parse_mode='HTML')

    elif nav == "adm_nav_refs":
        data = db.get_referral_analytics()
        markup, text = ui.admin_referral_render(data)
        bot.edit_message_text(text, chat_id, mid, reply_markup=markup, parse_mode='HTML')

    elif nav.startswith("adm_ctrl_"):
        bot.answer_callback_query(call.id)
        # Robust parsing for match_id with underscores
        match_id = "_".join(nav.split("_")[2:])
        
        players_data = final_bot.get_players(match_id) # Get players for this match
        markup = admin_event_markup(match_id, players_data, is_locked=final_bot.is_match_locked(match_id))
        match_name = final_bot.MATCHES.get(match_id, {}).get('name', match_id)
        text = f"🎮 <b>LIVE SCORING: {html.escape(str(match_name))}</b>\n\nSelect player and event to update points."
        bot.edit_message_text(text, chat_id, mid, reply_markup=markup, parse_mode='HTML')

    elif nav.startswith("adm_filter_"):
        bot.answer_callback_query(call.id)
        parts = nav.split("_")
        # Handle potential underscores in match_id
        role = parts[-1]
        match_id = "_".join(parts[2:-1])
        
        players_data = final_bot.get_players(match_id)
        markup = admin_event_markup(match_id, players_data, active_role=role, is_locked=final_bot.is_match_locked(match_id))
        match_name = final_bot.MATCHES.get(match_id, {}).get('name', match_id)
        try:
            bot.edit_message_reply_markup(chat_id, mid, reply_markup=markup)
        except ApiTelegramException as e:
            if "message is not modified" in e.description:
                pass # Ignore if no changes were made to the UI
            else: raise e

    elif nav.startswith("adm_settle_"):
        match_id = nav.split("_")[2]
        bot.answer_callback_query(call.id, "Settling Match & Distributing Prizes...", show_alert=True)
        final_bot.process_match_end(match_id)

    elif nav == "adm_nav_help":
        bot.answer_callback_query(call.id)
        markup, text = ui.admin_help_render()
        bot.edit_message_text(text, chat_id, mid, reply_markup=markup, parse_mode='HTML', disable_web_page_preview=True)
