# stream_processing/processor.py
import json
from collections import defaultdict
from kafka import KafkaConsumer

# Initialize consumer
consumer = KafkaConsumer(
    'nba_live_events',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='nba-feature-processor',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# In-memory trackers
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

print("[Processor] Starting feature extractor...")

for message in consumer:
    event = message.value
    team = event.get("team")
    player = event.get("player")
    event_type = event.get("event")

    if team and event_type:
        event_counts[team][event_type] += 1

        if event_type in event_points:
            score[team] += event_points[event_type]

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

        print(f"[Game Update] Score: {dict(score)}")
        print(f"[Event] {event_type} by {player or 'N/A'} ({team})")
        print(f"[Team Totals] {dict(event_counts)}")
        print(f"[Player Shooting] {dict(player_shots)}")
        print(f"[Player Other Stats] {dict(player_stats)}\n")