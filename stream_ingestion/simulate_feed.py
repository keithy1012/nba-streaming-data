# simulator/simulate_feed.py
import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def simulate_game_stream():
    plays = [
        {"event": "made_2pt", "player": "Curry", "team": "GSW"},
        {"event": "made_3pt", "player": "Edwards", "team": "TIM"},
        {"event": "turnover", "player": "Edwards", "team": "TIM"},
        {"event": "foul", "player": "Gobert", "team": "Team TIM"},
        {"event": "made_3pt", "player": "Curry", "team": "GSW"},
        {"event": "timeout", "team": "Team TIM"},
        {"event": "GAME_END"}
    ]

    while True:
        play = random.choice(plays)
        play["timestamp"] = datetime.utcnow().isoformat()
        print("Sending to Kafka:", play)
        producer.send("nba_live_events", play)
        time.sleep(1)

if __name__ == "__main__":
    simulate_game_stream()
