import json
import time
from kafka import KafkaProducer
from nba_api.live.nba.endpoints import scoreboard, boxscore, playbyplay

last_stats = {}

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def get_active_game_ids():
    games = scoreboard.ScoreBoard()
    game_data = games.get_dict().get("scoreboard", {}).get("games", [])
    return [game['gameId'] for game in game_data if game['gameStatusText'] != 'Final']



def fetch_game_events(game_id):
    try:
        pbp = playbyplay.PlayByPlay(game_id=game_id)
        with open('data.json', 'w') as f:
            json.dump(pbp.get_dict()["game"], f)
        plays = pbp.get_dict()["game"].get("actions", [])

        for play in plays:
            # Extract relevant information from each play
            description = play.get("description")
            player_name = play.get("playerName")
            team_name = play.get("teamTricode")
            time_remaining = play.get("clock")
            quarter = play.get("period")

            event_type = "other"  # Default event type
            if description:
                if " 3PT" in description:
                    event_type = "made_3pt" if "makes" in description.lower() else "missed_3pt"
                elif " 2PT" in description:
                    event_type = "made_2pt" if "makes" in description.lower() else "missed_2pt"
                elif "Free Throw" in description:
                    event_type = "made_ft" if "makes" in description.lower() else "missed_ft"
                elif "TURNOVER" in description:
                    event_type = "turnover"
                elif "BLOCK" in description:
                    event_type = "block"
                elif "REBOUND" in description:
                    event_type = "rebound"
                elif "FOUL" in description:
                    event_type = "foul"
                elif "STEAL" in description:
                    event_type = "steal"
                elif "TIMEOUT" in description:
                    event_type = "timeout"
                elif "lost ball" in description:
                    event_type = "turnover" #addded
                elif "offensive REBOUND" in description:
                    event_type = "rebound_offensive" #added
                elif "defensive REBOUND" in description:
                    event_type = "rebound_defensive" #added
  
            total_seconds = 0
            if time_remaining:
                try:
                    m_index = time_remaining.find('M')
                    s_index = time_remaining.find('S')
                    minutes = int(time_remaining[m_index-2:m_index])
                    seconds = int(time_remaining[m_index+1:s_index-3])
                    total_seconds = minutes * 60 + seconds
                except ValueError:
                    print(f"Error converting time_remaining: {time_remaining} for game_id: {game_id}")
                    total_seconds = 0
            # Create a payload for the Kafka message
            payload = {
                "game_id": game_id,
                "event_type": event_type,
                "description": description,
                "player_name": player_name,
                "team_name": team_name,
                "time_remaining": total_seconds,
                "quarter": quarter
            }

            producer.send('nba_live_events', payload)
            print(f"[Producer] Sent: {payload}")

    except Exception as e:
        print(f"[Error] {e}")



def run():
    print("[NBA API Producer] Starting live feed...")
    while True:
        game_ids = get_active_game_ids()
        for game_id in game_ids:
            fetch_game_events(game_id)
        time.sleep(15)

if __name__ == "__main__":
    run()
