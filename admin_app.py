from telebot import types
import db, ui
import final_bot # To access get_players and MATCHES
from telebot.apihelper import ApiTelegramException

def admin_main_markup(matches):
    markup = types.InlineKeyboardMarkup(row_width=1)
    for mid, info in matches.items():
        markup.add(types.InlineKeyboardButton(f"🎮 Control: {info['name']}", callback_data=f"adm_ctrl_{mid}"))
    return markup

def admin_event_markup(match_id, players, active_role='bat'):
    markup = types.InlineKeyboardMarkup(row_width=4)
    
    # 1. Role Filter Buttons (Panel ko chhota rakhne ke liye)
    role_btns = []
    for r in ['bat', 'wk', 'ar', 'bowl']:
        label = f"» {r.upper()} «" if r == active_role else r.upper()
        role_btns.append(types.InlineKeyboardButton(label, callback_data=f"adm_filter_{match_id}_{r}"))
    markup.row(*role_btns)

    # 2. Only show players of active role
    if active_role in players:
        for p_name in players[active_role]:
            p_short = p_name.split()[-1] # Surname only for buttons
            markup.row(
                types.InlineKeyboardButton(f"{p_short}", callback_data="ignore"),
                types.InlineKeyboardButton("+1", callback_data=f"evt_{match_id}_{p_name}_run"),
                types.InlineKeyboardButton("+4", callback_data=f"evt_{match_id}_{p_name}_four"),
                types.InlineKeyboardButton("+6", callback_data=f"evt_{match_id}_{p_name}_six"),
                types.InlineKeyboardButton("WK", callback_data=f"evt_{match_id}_{p_name}_wicket")
            )

    markup.add(types.InlineKeyboardButton("🔙 Back to Dashboard", callback_data="adm_nav_home"))
    return markup

def handle_admin_nav(call, bot):
    nav = call.data
    chat_id = call.message.chat.id
    mid = call.message.message_id
    
    if nav == "adm_nav_home":
        bot.answer_callback_query(call.id)
        stats = db.get_admin_stats()
        markup, text = ui.admin_dashboard_home(stats)
        bot.edit_message_text(text, chat_id, mid, reply_markup=markup, parse_mode='Markdown')
        
    elif nav == "adm_nav_funnel":
        bot.answer_callback_query(call.id)
        data = db.get_funnel_data()
        markup, text = ui.admin_funnel_render(data)
        bot.edit_message_text(text, chat_id, mid, reply_markup=markup, parse_mode='Markdown')

    elif nav == "adm_nav_fraud":
        bot.answer_callback_query(call.id)
        data = db.get_fraud_list()
        markup, text = ui.admin_fraud_render(data)
        bot.edit_message_text(text, chat_id, mid, reply_markup=markup, parse_mode='Markdown')

    elif nav.startswith("adm_fin_"):
        bot.answer_callback_query(call.id)
        match_id = nav.split("_")[2]
        match_name = final_bot.MATCHES.get(match_id, {}).get('name', match_id)
        
        fin_data = db.db_get_match_financials(match_id)
        markup, text = ui.admin_match_finance_render(match_id, match_name, fin_data)
        bot.edit_message_text(text, chat_id, mid, reply_markup=markup, parse_mode='Markdown')

    elif nav == "adm_nav_refs":
        data = db.get_referral_analytics()
        markup, text = ui.admin_referral_render(data)
        bot.edit_message_text(text, chat_id, mid, reply_markup=markup, parse_mode='Markdown')

    elif nav.startswith("adm_ctrl_"):
        bot.answer_callback_query(call.id)
        match_id = nav.split("_")[2]
        
        players_data = final_bot.get_players(match_id) # Get players for this match
        markup = admin_event_markup(match_id, players_data)
        match_name = final_bot.MATCHES.get(match_id, {}).get('name', match_id)
        text = f"🎮 *LIVE SCORING: {match_name}*\n\nSelect player and event to update points."
        bot.edit_message_text(text, chat_id, mid, reply_markup=markup, parse_mode='Markdown')

    elif nav.startswith("adm_filter_"):
        bot.answer_callback_query(call.id)
        parts = nav.split("_")
        match_id, role = parts[2], parts[3]
        
        players_data = final_bot.get_players(match_id)
        markup = admin_event_markup(match_id, players_data, active_role=role)
        match_name = final_bot.MATCHES.get(match_id, {}).get('name', match_id)
        try:
            bot.edit_message_reply_markup(chat_id, mid, reply_markup=markup)
        except ApiTelegramException as e:
            if "message is not modified" in e.description:
                pass # Ignore if no changes were made to the UI
            else: raise e

    elif nav == "adm_nav_help":
        bot.answer_callback_query(call.id)
        markup, text = ui.admin_help_render()
        bot.edit_message_text(text, chat_id, mid, reply_markup=markup, parse_mode='Markdown')
