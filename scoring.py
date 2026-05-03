import json
import logging
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

def recalculate_match_points(match_id):
    """
    Nuclear Option: Purani saari galtiyon ko theek karta hai. 
    PLAYER_LIVE_STATS se fresh points uthakar har team ka score zero se calculate karta hai.
    """
    try:
        stats_map = db.db_get_player_live_stats_map(match_id)
        
        def get_p_pts(name):
            s = stats_map.get(name, {'runs': 0, 'fours': 0, 'sixes': 0, 'wickets': 0})
            return (s['runs'] * POINT_SYSTEM['run'] + 
                    s['fours'] * POINT_SYSTEM['four'] + 
                    s['sixes'] * POINT_SYSTEM['six'] + 
                    s['wickets'] * POINT_SYSTEM['wicket'])

        with db.get_db() as conn:
            conn.execute("SELECT user_id, team_num, team_players, captain, vice_captain FROM TEAMS WHERE match_id=%s AND is_paid=1", (match_id,))
            teams = conn.fetchall()
            
            for t in teams:
                players_data = json.loads(t['team_players'])
                t_total = 0
                for role in players_data:
                    for p_full in players_data[role]:
                        p_name = str(p_full).split(' (')[0].strip()
                        p_pts = get_p_pts(p_name)
                        
                        # Multiplier check
                        mult = 1.0
                        if p_full == t['captain']: mult = CAPTAIN_MULTIPLIER
                        elif p_full == t['vice_captain']: mult = VC_MULTIPLIER
                        
                        t_total += (p_pts * mult)
                
                conn.execute("UPDATE TEAMS SET points = %s WHERE user_id=%s AND match_id=%s AND team_num=%s", 
                             (t_total, t['user_id'], match_id, t['team_num']))
        return True
    except Exception as e:
        logging.error(f"Recalculate error: {e}")
        return False
