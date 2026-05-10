# Central config — imported by producer.py, consumer.py, and Django views.
# Change values here once; all components pick them up automatically.

# ── Kafka ──────────────────────────────────────────────────────────────────
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC  = "tweets"

# ── MongoDB ────────────────────────────────────────────────────────────────
MONGO_URI        = "mongodb://localhost:27017"
MONGO_DB         = "twitter_sentiment"
MONGO_COLLECTION = "tweets"

# ── Producer defaults ──────────────────────────────────────────────────────
DEFAULT_DELAY_MS = 100   # ms between messages (override with --delay flag)
LOG_EVERY_N      = 100   # print progress every N messages

# ── Consumer (Spark Streaming) defaults ────────────────────────────────────
SPARK_BATCH_INTERVAL = 5   # seconds between micro-batches
# MODEL_PATH is resolved dynamically inside consumer.py via Path(__file__) —
# avoids relative-path pitfalls with PySpark on Windows.
