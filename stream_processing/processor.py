# stream_processing/processor.py
import json
import threading
from collections import defaultdict
from kafka import KafkaConsumer
from flask import Flask, jsonify
import psycopg2
from datetime import datetime
import xgboost as xgb
import numpy as np

model = xgb.XGBClassifier()
model.load_model("model/win_predictor.json")

#import redis

app = Flask(__name__)

# Connect to Redis (for in-game storage)
# r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# PostgreSQL connection
conn = psycopg2.connect(
    dbname="nba_stats",
    user="nba",
    password="nba",
    host="localhost",
    port=5432
)
cursor = conn.cursor()

score = defaultdict(int)
player_shots = defaultdict(lambda: {"made": 0, "attempted": 0})
player_stats = defaultdict(lambda: {
    "turnovers": 0,
    "blocks": 0,
    "rebounds_total": 0,
    "rebounds_offensive": 0,
    "rebounds_defensive": 0
})
event_counts = defaultdict(lambda: defaultdict(int))

consumer = KafkaConsumer(
    'nba_live_events',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='nba-feature-processor',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

event_points = {
    "made_2pt": 2,
    "made_3pt": 3,
    "made_ft": 1
}

shooting_events = {"made_2pt", "made_3pt", "missed_2pt", "missed_3pt"}
supported_events = {
    "turnover", "block", "rebound_offensive", "rebound_defensive",
    "made_2pt", "missed_2pt", "made_3pt", "missed_3pt", "made_ft"
}
team_names = set()
def consume_events():
    print("[Processor] Starting feature extractor...")
    team_stats = defaultdict(lambda: {
        "turnovers": 0,
        "blocks": 0,
        "rebounds_offensive": 0,
        "rebounds_defensive": 0,
        "made_2pt": 0,
        "missed_2pt": 0,
        "made_3pt": 0,
        "missed_3pt": 0,
        "made_ft": 0,
        "missed_ft":0,
        "seconds_left": 0, 
        "period": 0
    })
    for message in consumer:
        print(message)
        event = message.value
        event_type = event.get("event")
        if event_type == "GAME_END":
            print("GAME END")
            cursor.execute("TRUNCATE player_stats, team_scores, player_shots, win_probabilities")
            break
        team = event.get("team")
        team_names.add(team)
        player = event.get("player")
        now = datetime.utcnow()

        if not team or event_type not in supported_events:
            continue

        event_counts[team][event_type] += 1

        if event_type in event_points:
            score[team] += event_points[event_type]

        if event_type == "turnover":
            team_stats[team]["turnovers"] += 1
        elif event_type == "block":
            team_stats[team]["blocks"] += 1
        elif event_type == "rebound_offensive":
            team_stats[team]["rebounds_offensive"] += 1
        elif event_type == "rebound_defensive":
            team_stats[team]["rebounds_defensive"] += 1
        elif event_type == "made_2pt":
            team_stats[team]["made_2pt"] += 1
        elif event_type == "missed_2pt":
            team_stats[team]["missed_2pt"] += 1
        elif event_type == "made_3pt":
            team_stats[team]["made_3pt"] += 1
        elif event_type == "missed_3pt":
            team_stats[team]["missed_3pt"] += 1
        elif event_type == "made_ft":
            team_stats[team]["made_ft"] += 1
        elif event_type == "missed_ft":
            team_stats[team]["missed_ft"] += 1

        if player:
            if event_type in shooting_events:
                player_shots[player]["attempted"] += 1
                if "made" in event_type:
                    player_shots[player]["made"] += 1

            if event_type == "turnover":
                player_stats[player]["turnovers"] += 1
            elif event_type == "block":
                player_stats[player]["blocks"] += 1
            elif event_type == "rebound_offensive":
                player_stats[player]["rebounds_offensive"] += 1
                player_stats[player]["rebounds_total"] += 1
            elif event_type == "rebound_defensive":
                player_stats[player]["rebounds_defensive"] += 1
                player_stats[player]["rebounds_total"] += 1

        # Insert into PostgreSQL
        cursor.execute("""
            INSERT INTO team_scores (timestamp, team, score)
            VALUES (%s, %s, %s)
        """, (now, team, score[team]))

        if player and event_type in shooting_events:
            cursor.execute("""
                INSERT INTO player_shots (timestamp, player, made, attempted)
                VALUES (%s, %s, %s, %s)
            """, (
                now, player,
                player_shots[player]["made"],
                player_shots[player]["attempted"]
            ))

        if player and event_type in {"turnover", "block", "rebound_offensive", "rebound_defensive"}:
            stats = player_stats[player]
            cursor.execute("""
                INSERT INTO player_stats (
                    timestamp, player,
                    turnovers, blocks,
                    rebounds_offensive, rebounds_defensive, rebounds_total
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                now, player,
                stats["turnovers"], stats["blocks"],
                stats["rebounds_offensive"], stats["rebounds_defensive"], stats["rebounds_total"]
            ))
        team_one, team_two = list(team_names)[0], list(team_names)[1]
        team_one_score = team_stats[team_one]['made_2pt'] * 2 + team_stats[team_one]['made_3pt'] * 3 + team_stats[team_one]['made_ft']
        team_two_score = team_stats[team_two]['made_2pt'] * 2 + team_stats[team_two]['made_3pt'] * 3 + team_stats[team_two]['made_ft']
        team_one_fga = team_stats[team_one]['made_2pt'] + team_stats[team_one]['made_3pt'] + team_stats[team_one]['made_ft'] + team_stats[team_one]['missed_2pt'] + team_stats[team_one]['missed_3pt'] + team_stats[team_one]['missed_ft']
        team_two_fga = team_stats[team_two]['made_2pt'] + team_stats[team_two]['made_3pt'] + team_stats[team_two]['made_ft'] + team_stats[team_two]['missed_2pt'] + team_stats[team_two]['missed_3pt'] + team_stats[team_two]['missed_ft']
        team_one_fgm = team_stats[team_one]['made_2pt'] + team_stats[team_one]['made_3pt'] + team_stats[team_one]['made_ft']
        team_two_fgm = team_stats[team_two]['made_2pt'] + team_stats[team_two]['made_3pt'] + team_stats[team_two]['made_ft']
        team_one_fg3a = team_stats[team_one]['made_3pt'] + team_stats[team_one]['missed_3pt']
        team_two_fg3a = team_stats[team_two]['made_3pt'] + team_stats[team_two]['missed_3pt']
        team_one_fta = team_stats[team_one]['made_ft'] + team_stats[team_one]['missed_ft']
        team_two_fta = team_stats[team_two]['made_ft'] + team_stats[team_two]['missed_ft']
        
        features = [
            0,#team_stats[list(team_names)[0]]['wins'],
            0,#team_stats[team_names[0]]['losses'],
            0,#team_stats[team_names[1]]['wins'],
            0,#team_stats[team_names[1]]['losses'],
            team_stats[team_one]['seconds_left'],
            team_stats[team_one]['period'],
            team_one_score,
            team_two_score,
            team_one_score - team_two_score,
            team_one_fgm / team_one_fga if team_one_fga else 0,
            team_stats[team_one]['made_3pt'] / team_one_fg3a if team_one_fg3a else 0,
            team_stats[team_one]['made_ft'] / team_one_fta if team_one_fta else 0,
            team_stats[team_one]['turnovers'],
            team_stats[team_one]['rebounds_offensive'] + team_stats[team_one]['rebounds_defensive'],

            team_two_fgm / team_two_fga if team_two_fga else 0,
            team_stats[team_two]['made_3pt'] / team_two_fg3a if team_two_fg3a else 0,
            team_stats[team_two]['made_ft'] / team_two_fta if team_two_fta else 0,
            team_stats[team_two]['turnovers'],
            team_stats[team_two]['rebounds_offensive'] + team_stats[team_two]['rebounds_defensive'],
            0,  # score_diff_momentum placeholder
            0,  # points_scored_last_2min_home
            0   # points_scored_last_2min_away
        ]

        win_prob = float(model.predict_proba(np.array(features).reshape(1, -1))[0][1])  # prob home wins
        print(win_prob)

        team_stats = defaultdict(lambda: {
            "turnovers": 0,
            "blocks": 0,
            "rebounds_offensive": 0,
            "rebounds_defensive": 0,
            "made_2pt": 0,
            "missed_2pt": 0,
            "made_3pt": 0,
            "missed_3pt": 0,
            "made_ft": 0,
            "missed_ft":0,
            "seconds_left": 0, 
            "period": 0
        })
        
        cursor.execute("""
            INSERT INTO win_probabilities (timestamp, home_team, away_team, home_score, away_score, home_win_prob)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (datetime.now(), str(team_one), str(team_two), int(team_one_score), int(team_two_score), win_prob))


        conn.commit()

# Flask routes (optional API layer)
@app.route("/api/score")
def get_score():
    return jsonify(dict(score))

@app.route("/api/player_shots")
def get_player_shots():
    return jsonify(dict(player_shots))

@app.route("/api/player_stats")
def get_player_stats():
    return jsonify(dict(player_stats))

@app.route("/api/win_probability")
def get_win_prob():
    cursor.execute("SELECT * FROM win_probabilities ORDER BY timestamp DESC LIMIT 1")
    row = cursor.fetchone()
    return jsonify({
        "timestamp": row[0], "home_team": row[1], "away_team": row[2],
        "home_score": row[3], "away_score": row[4],
        "home_win_prob": row[5]
    })

if __name__ == '__main__':
    threading.Thread(target=consume_events, daemon=True).start()
    app.run(debug=True, port=5000)
