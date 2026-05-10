# Phase 3 — Kafka Producer
# Reads twitter_training.csv row-by-row and publishes each tweet to the Kafka 'tweets' topic.
# Implementation added in Phase 3.

import argparse
import json
import os
import signal
import sys
import time

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from config import KAFKA_BROKER, KAFKA_TOPIC, DEFAULT_DELAY_MS, LOG_EVERY_N

# Resolve dataset path relative to this script's location
DATASET_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "ML-PySpark-Model", "datasets", "twitter_training.csv"
)
COLS = ["tweet_id", "entity", "sentiment", "text"]

_producer = None


#  Graceful shutdown 
def _shutdown(sig, frame):
    print("\n[Producer] Shutting down — flushing pending messages...")
    if _producer:
        _producer.flush()
        _producer.close()
    print("[Producer] Closed cleanly.")
    sys.exit(0)

signal.signal(signal.SIGINT,  _shutdown)
signal.signal(signal.SIGTERM, _shutdown)


# Kafka connection with startup retry 
def _connect(broker, max_retries=10, wait_sec=3):
    global _producer
    for attempt in range(1, max_retries + 1):
        try:
            _producer = KafkaProducer(
                bootstrap_servers=broker,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",          # wait for broker acknowledgement
                retries=3,           # retry failed sends up to 3 times
            )
            print(f"[Producer] Connected to Kafka → {broker}")
            return _producer
        except NoBrokersAvailable:
            print(f"[Producer] Kafka not ready (attempt {attempt}/{max_retries}) — retrying in {wait_sec}s...")
            time.sleep(wait_sec)

    print("[Producer] ERROR: Could not connect after max retries. Is 'docker-compose up -d' running?")
    sys.exit(1)


#  Main streaming loop 
def main(delay_ms: int):
    producer  = _connect(KAFKA_BROKER)
    delay_sec = delay_ms / 1000.0

    df = pd.read_csv(DATASET_PATH, header=None, names=COLS, dtype=str)
    df = df.dropna(subset=["text", "sentiment"])
    total = len(df)

    print(f"[Producer] Loaded {total} tweets.")
    print(f"[Producer] Topic: '{KAFKA_TOPIC}' | Delay: {delay_ms}ms/msg | "
          f"ETA: ~{(total * delay_ms) // 60000} min\n")

    for i, row in enumerate(df.itertuples(index=False), start=1):
        message = {
            "tweet_id":  row.tweet_id,
            "entity":    row.entity,
            "sentiment": row.sentiment,   
            "text":      row.text,
        }
        producer.send(KAFKA_TOPIC, value=message)

        if i % LOG_EVERY_N == 0:
            print(
                f"[Producer] {i:>6}/{total} sent | "
                f"Entity: {row.entity:<15} | Sentiment: {row.sentiment}"
            )

        time.sleep(delay_sec)

    producer.flush()
    print(f"\n[Producer] Complete — {total} messages sent to topic '{KAFKA_TOPIC}'.")
    producer.close()


#  Entry point
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Streams twitter_training.csv into Kafka.")
    parser.add_argument(
        "--delay",
        type=int,
        default=DEFAULT_DELAY_MS,
        help=f"Milliseconds between messages (default: {DEFAULT_DELAY_MS}). "
             "Use --delay 10 for fast testing, --delay 100 for demos.",
    )
    args = parser.parse_args()
    main(args.delay)
