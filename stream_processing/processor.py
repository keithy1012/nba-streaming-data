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
    "seconds_left": 0, 
    "period": 0
})

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
    for message in consumer:
        event = message.value
        event_type = event.get("event")
        if event_type == "GAME_END":
            print("GAME END")
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

        features = [
            team_stats[team_names[0]]['wins'],
            team_stats[team_names[0]]['losses'],
            team_stats[team_names[1]]['wins'],
            team_stats[team_names[1]]['losses'],
            team_stats['seconds_left'],
            team_stats['period'],
            team_stats[team_names[0]]['score'],
            team_stats[team_names[1]]['score'],
            team_stats[team_names[0]]['score'] - team_stats[team_names[1]]['score'],
            team_stats[team_names[0]]['fgm'] / team_stats[team_names[0]]['fga'] if team_stats[team_names[0]]['fga'] else 0,
            team_stats[team_names[0]]['fg3m'] / team_stats[team_names[0]]['fg3a'] if team_stats[team_names[0]]['fg3a'] else 0,
            team_stats[team_names[0]]['ftm'] / team_stats[team_names[0]]['fta'] if team_stats[team_names[0]]['fta'] else 0,
            team_stats[team_names[0]]['to'],
            team_stats[team_names[0]]['reb'],
            team_stats[team_names[1]]['fgm'] / team_stats[team_names[1]]['fga'] if team_stats[team_names[1]]['fga'] else 0,
            team_stats[team_names[1]]['fg3m'] / team_stats[team_names[1]]['fg3a'] if team_stats[team_names[1]]['fg3a'] else 0,
            team_stats[team_names[1]]['ftm'] / team_stats[team_names[1]]['fta'] if team_stats[team_names[1]]['fta'] else 0,
            team_stats[team_names[1]]['to'],
            team_stats[team_names[1]]['reb'],
            0,  # score_diff_momentum placeholder
            0,  # points_scored_last_2min_home
            0   # points_scored_last_2min_away
        ]

        win_prob = model.predict_proba(np.array(features).reshape(1, -1))[0][1]  # prob home wins

        cursor.execute("""
            INSERT INTO win_predictions (timestamp, home_team_score, away_team_score, home_win_probability)
            VALUES (%s, %s, %s, %s)
        """, (now, team_stats[team_names[0]]['score'], team_stats[team_names[1]]['score'], win_prob))


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
