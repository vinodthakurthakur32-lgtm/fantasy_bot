import json
import db

POINT_SYSTEM = {
    'run': 1,
    'four': 4,
    'six': 6,
    'wicket': 25,
    'maiden': 10 # Assuming maiden over points
}
CAPTAIN_MULTIPLIER = 2.0
VC_MULTIPLIER = 1.5

def update_match_event(match_id, player_name, event_type):
    points = POINT_SYSTEM.get(event_type, 0)
    
    with db.get_db() as conn:
        # 1. Update Player Stats
        col = "runs" if event_type == 'run' else event_type + "s" if event_type in ['four', 'six'] else "wickets"
        conn.execute(f"""
            INSERT INTO PLAYER_LIVE_STATS (match_id, player_name, {col}) 
            VALUES (%s, %s, 1)
            ON CONFLICT(match_id, player_name) DO UPDATE SET {col} = PLAYER_LIVE_STATS.{col} + 1
        """, (match_id, player_name))

        # 2. Log Event
        conn.execute("INSERT INTO MATCH_EVENTS (match_id, player_name, event_type, points_awarded) VALUES (%s,%s,%s,%s)",
                     (match_id, player_name, event_type, points))

        # 3. Recalculate ALL teams for this match
        conn.execute("SELECT user_id, team_num, team_players, captain, vice_captain FROM TEAMS WHERE match_id=%s", (match_id,))
        teams = conn.fetchall()
        
        for team in teams:
            players_data = json.loads(team['team_players'])
            raw_team_names = []
            for role in players_data:
                # Ensure we strip any team info or tags from stored team data for matching
                raw_team_names.extend([str(p).split(' (')[0].strip() for p in players_data[role]])
            
            if player_name in raw_team_names:
                # Multiply if Captain/VC
                multiplier = 1.0
                raw_c = str(team['captain']).split(' (')[0].strip() if team['captain'] else ""
                raw_vc = str(team['vice_captain']).split(' (')[0].strip() if team['vice_captain'] else ""
                if player_name == raw_c: multiplier = CAPTAIN_MULTIPLIER
                elif player_name == raw_vc: multiplier = VC_MULTIPLIER
                
                final_points = points * multiplier
                conn.execute("UPDATE TEAMS SET points = points + %s WHERE user_id=%s AND match_id=%s AND team_num=%s",
                             (final_points, team['user_id'], match_id, team['team_num']))
                
                # ⚡ Cache Invalidation: Invalidate the team cache in final_bot so latest points are shown
                from final_bot import temp_team_cache
                cache_key = (str(team['user_id']), match_id, int(team['team_num']))
                if cache_key in temp_team_cache:
                    del temp_team_cache[cache_key]
    return True
