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
    # Added 'cv' virtual role to filters to quickly find captains
    for r in ['bat', 'wk', 'ar', 'bowl', 'sub', 'cv']:
        label = f"» {r.upper()} «" if r == active_role else r.upper()
        role_btns.append(types.InlineKeyboardButton(label, callback_data=f"adm_filter_{match_id}_{r}"))
    markup.row(*role_btns[:3]) # Split into two rows for cleaner UI
    markup.row(*role_btns[3:])

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
                types.InlineKeyboardButton("+1 R", callback_data=f"evt|{match_id}|{p_name}|run"),
                types.InlineKeyboardButton("+4 R", callback_data=f"evt|{match_id}|{p_name}|four"),
                types.InlineKeyboardButton("+6 R", callback_data=f"evt|{match_id}|{p_name}|six"),
                types.InlineKeyboardButton("+1 W", callback_data=f"evt|{match_id}|{p_name}|wicket")
            )

    # Match locked hai toh Result declare karne ka option dein
    if is_locked:
        markup.row(types.InlineKeyboardButton("🏁 DECLARE FINAL RESULT & PAY WINNERS", callback_data=f"adm_settle_ask_{match_id}"))
    
    # 🔄 MASTER SYNC (Fix all errors)
    markup.row(types.InlineKeyboardButton("🔄 RE-SYNC ALL POINTS (Magic Fix)", callback_data=f"adm_resync_{match_id}"))
    
    markup.row(types.InlineKeyboardButton("🌧️ ABANDON MATCH (Refund All)", callback_data=f"adm_refund_ask_{match_id}"))

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

    elif nav == "adm_nav_recent":
        bot.answer_callback_query(call.id)
        users = db.db_get_recent_users_stats(10)
        text = "👥 <b>RECENT USERS & ACTIVITY</b>\n\n"
        if not users:
            text += "No recent users found."
        for u in users:
            uname = f"@{u['username']}" if u['username'] else "N/A"
            text += (f"👤 <b>{html.escape(u['first_name'])}</b> ({uname})\n"
                     f"🆔 <code>{u['user_id']}</code>\n"
                     f"📅 Joined: {u['joined_date'][5:16]}\n"
                     f"⚾ Teams: <code>{u['team_count']}</code>\n\n")
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("🔙 Back", callback_data="adm_nav_home"))
        bot.edit_message_text(text, chat_id, mid, reply_markup=markup, parse_mode='HTML')

    elif nav == "adm_nav_get_user":
        bot.answer_callback_query(call.id)
        final_bot.cmd_get_user_data(call.message, admin_id=call.from_user.id)
    
    elif nav.startswith("adm_bulk_up_"):
        match_id = nav.split("_")[3]
        bot.answer_callback_query(call.id)
        sent = bot.send_message(chat_id, f"📝 <b>BULK TOTALS: {match_id}</b>\n\nFormat: <code>Player Name | Runs | Wickets</code>\nMultiple lines bhein.\n\n<i>Note: Ye purane score ko overwrite kar dega.</i>", parse_mode='HTML')
        bot.register_next_step_handler(sent, final_bot.process_bulk_scoring, match_id)
        
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
        stats_map = db.db_get_player_live_stats_map(match_id)
        markup = admin_event_markup(match_id, players_data, is_locked=final_bot.is_match_locked(match_id), stats_map=stats_map)
        match_name = final_bot.MATCHES.get(match_id, {}).get('name', match_id)
        text = f"🎮 <b>LIVE SCORING: {html.escape(str(match_name))}</b>\n\nSelect player and event to update points."
        bot.edit_message_text(text, chat_id, mid, reply_markup=markup, parse_mode='HTML')

    elif nav.startswith("adm_resync_"):
        match_id = nav.split("_")[2]
        bot.answer_callback_query(call.id, "⏳ Syncing all teams...", show_alert=False)
        if scoring.recalculate_match_points(match_id):
            bot.answer_callback_query(call.id, "✅ All team points synced and corrected!", show_alert=True)
        else:
            bot.answer_callback_query(call.id, "❌ Sync failed!", show_alert=True)

    elif nav.startswith("adm_filter_"):
        bot.answer_callback_query(call.id)
        parts = nav.split("_")
        # Handle potential underscores in match_id
        role = parts[-1]
        match_id = "_".join(parts[2:-1])
        
        players_data = final_bot.get_players(match_id)
        stats_map = db.db_get_player_live_stats_map(match_id)
        markup = admin_event_markup(match_id, players_data, active_role=role, is_locked=final_bot.is_match_locked(match_id), stats_map=stats_map)
        match_name = final_bot.MATCHES.get(match_id, {}).get('name', match_id)
        try:
            bot.edit_message_reply_markup(chat_id, mid, reply_markup=markup)
        except ApiTelegramException as e:
            if "message is not modified" in e.description:
                pass # Ignore if no changes were made to the UI
            else: raise e

    elif nav.startswith("adm_settle_ask_"):
        match_id = nav.split("_")[3]
        bot.answer_callback_query(call.id)
        markup = types.InlineKeyboardMarkup()
        markup.add(
            types.InlineKeyboardButton("🔥 YES, CONFIRM & PAY", callback_data=f"adm_settle_confirm_{match_id}"),
            types.InlineKeyboardButton("❌ NO, CANCEL", callback_data=f"adm_ctrl_{match_id}")
        )
        bot.edit_message_text(f"⚠️ <b>CONFIRM SETTLEMENT</b>\n\nMatch: <code>{match_id}</code>\n\nKya aap pakka result declare karke winners ko pay karna chahte hain? Yeh process wapas nahi liya ja sakta.", 
                             chat_id, mid, reply_markup=markup, parse_mode='HTML')

    elif nav.startswith("adm_settle_confirm_"):
        match_id = nav.split("_")[3]
        bot.answer_callback_query(call.id, "🚀 Settle shuru ho raha hai...", show_alert=True)
        final_bot.process_match_end(match_id)

    elif nav.startswith("adm_refund_ask_"):
        match_id = nav.split("_")[3]
        bot.answer_callback_query(call.id)
        markup = types.InlineKeyboardMarkup()
        markup.add(
            types.InlineKeyboardButton("💰 YES, REFUND EVERYONE", callback_data=f"adm_refund_confirm_{match_id}"),
            types.InlineKeyboardButton("❌ NO, CANCEL", callback_data=f"adm_ctrl_{match_id}")
        )
        bot.edit_message_text(f"⚠️ <b>ABANDON MATCH & REFUND</b>\n\nMatch: <code>{match_id}</code>\n\nKya aap pakka is match ke saare users ka paisa wapas karna chahte hain?", 
                             chat_id, mid, reply_markup=markup, parse_mode='HTML')

    elif nav.startswith("adm_refund_confirm_"):
        match_id = nav.split("_")[3]
        bot.answer_callback_query(call.id, "💸 Refund process shuru...", show_alert=True)
        final_bot.process_match_refund(match_id)

    elif nav == "adm_nav_help":
        bot.answer_callback_query(call.id)
        markup, text = ui.admin_help_render()
        bot.edit_message_text(text, chat_id, mid, reply_markup=markup, parse_mode='HTML', disable_web_page_preview=True)
