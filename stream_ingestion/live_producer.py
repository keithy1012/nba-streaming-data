# stream_producer/nba_api_producer.py
import json
import time
from kafka import KafkaProducer
from nba_api.live.nba.endpoints import scoreboard, boxscore

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
        bs = boxscore.BoxScore(game_id=game_id)
        home_team = bs.get_dict()["game"]["homeTeam"]["teamName"]
        away_team = bs.get_dict()["game"]["awayTeam"]["teamName"]
        home_players = bs.get_dict()["game"]["homeTeam"]["players"]
        away_players = bs.get_dict()["game"]["awayTeam"]["players"]
        for player in home_players:
            player["teamTricode"] = home_team
        for player in away_players:
            player["teamTricode"] = away_team

        players = home_players + away_players

        print(len(players))
        for player in players:
            player_name = player["name"]
            team = player["teamTricode"]
            stats = player["statistics"]

            # Get previous stats
            prev = last_stats.get(player_name, {
                "points": 0,
                "fieldGoalsMade": 0,
                "fieldGoalsAttempted": 0,
                "threePointersMade": 0,
                "threePointersAttempted": 0,
                "freeThrowsMade": 0,
                "freeThrowsAttempted": 0,
                "reboundsOffensive": 0,
                "reboundsDefensive": 0,
                "turnovers": 0,
                "blocks": 0
            })

            events = []
            if stats["fieldGoalsMade"] > prev["fieldGoalsMade"]:
                new_3pt = stats["threePointersMade"] - prev["threePointersMade"]
                if new_3pt > 0:
                    events += ["made_3pt"] * new_3pt
                else:
                    events += ["made_2pt"] * (stats["fieldGoalsMade"] - prev["fieldGoalsMade"])
            if stats["fieldGoalsAttempted"] > prev["fieldGoalsAttempted"]:
                missed = (
                    stats["fieldGoalsAttempted"] - prev["fieldGoalsAttempted"]
                    - (stats["fieldGoalsMade"] - prev["fieldGoalsMade"])
                )
                events += ["missed_2pt"] * missed  # Approximation

            if stats["threePointersAttempted"] > prev["threePointersAttempted"]:
                missed_3pt = (
                    stats["threePointersAttempted"] - prev["threePointersAttempted"]
                    - (stats["threePointersMade"] - prev["threePointersMade"])
                )
                events += ["missed_3pt"] * missed_3pt

            if stats["freeThrowsMade"] > prev["freeThrowsMade"]:
                events += ["made_ft"] * (stats["freeThrowsMade"] - prev["freeThrowsMade"])
            if stats["freeThrowsAttempted"] > prev["freeThrowsAttempted"]:
                missed_ft = (
                    stats["freeThrowsAttempted"] - prev["freeThrowsAttempted"]
                    - (stats["freeThrowsMade"] - prev["freeThrowsMade"])
                )
                events += ["missed_ft"] * missed_ft

            if stats["turnovers"] > prev["turnovers"]:
                events += ["turnover"] * (stats["turnovers"] - prev["turnovers"])
            if stats["blocks"] > prev["blocks"]:
                events += ["block"] * (stats["blocks"] - prev["blocks"])
            if stats["reboundsOffensive"] > prev["reboundsOffensive"]:
                events += ["rebound_offensive"] * (stats["reboundsOffensive"] - prev["reboundsOffensive"])
            if stats["reboundsDefensive"] > prev["reboundsDefensive"]:
                events += ["rebound_defensive"] * (stats["reboundsDefensive"] - prev["reboundsDefensive"])

            for event_type in events:
                payload = {
                    "player": player_name,
                    "team": team,
                    "event": event_type
                }
                producer.send('nba_live_events', payload)
                print(f"[Producer] Sent: {payload}")

            last_stats[player_name] = stats

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
