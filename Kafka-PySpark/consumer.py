# Phase 4 — Spark Streaming Consumer
# Reads tweets from Kafka, runs Spark MLlib model + VADER, writes enriched docs to MongoDB.
import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, udf, regexp_replace, lower, trim
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)
from pyspark.ml import PipelineModel
from pymongo import MongoClient



from config import (
    KAFKA_BROKER, KAFKA_TOPIC,
    MONGO_URI, MONGO_DB, MONGO_COLLECTION,
    SPARK_BATCH_INTERVAL,
)

os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['PATH']        = r'C:\hadoop\bin;' + os.environ.get('PATH', '')

# Force PySpark workers to use the SAME Python interpreter as the driver.
# Without this, Windows workers may launch with system Python (e.g. 3.13),
# while the driver runs in the conda env (3.11) → socket reset / version mismatch.
os.environ['PYSPARK_PYTHON']        = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# ── Resolve absolute paths (Windows-safe) ─────────────────────────────────
SCRIPT_DIR = Path(__file__).parent

MODEL_PATH = str(
    (SCRIPT_DIR.parent / "ML-PySpark-Model" / "saved_models" / "spark_lr_pipeline").resolve()
).replace("\\", "/")

CHECKPOINT_PATH = str(
    (SCRIPT_DIR / "checkpoints").resolve()
).replace("\\", "/")


# ── SparkSession (Kafka package downloaded automatically on first run) ────
# spark.hadoop.io.native.lib.available=false → disables Hadoop native I/O
# (avoids UnsatisfiedLinkError from hadoop.dll version mismatch on Windows)
spark = (
    SparkSession.builder
    .appName("TwitterSentimentConsumer")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
    .config("spark.hadoop.io.native.lib.available", "false")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print(f"[Consumer] Spark version: {spark.version}")


# ── Load saved Spark MLlib pipeline ───────────────────────────────────────
print(f"[Consumer] Loading model from: {MODEL_PATH}")
model = PipelineModel.load(MODEL_PATH)

# Captured here so the label_from_prediction UDF can use it
LABELS = list(model.stages[0].labels)
print(f"[Consumer] Label mapping: {dict(enumerate(LABELS))}")


# ── Schema for incoming Kafka JSON messages ───────────────────────────────
tweet_schema = StructType([
    StructField("tweet_id",  StringType(), True),
    StructField("entity",    StringType(), True),
    StructField("sentiment", StringType(), True),  # ground-truth label from CSV (not used)
    StructField("text",      StringType(), True),
])


# ── UDFs ──────────────────────────────────────────────────────────────────
@udf(DoubleType())
def extract_confidence(probability_vector):
    """Highest probability across classes = the model's confidence in its prediction."""
    return float(max(probability_vector.toArray()))


@udf(StringType())
def label_from_prediction(prediction):
    """Map numeric prediction back to original label name (Positive/Negative/...)."""
    idx = int(prediction)
    return LABELS[idx] if 0 <= idx < len(LABELS) else "Unknown"


VADER_SCHEMA = StructType([
    StructField("sentiment_vader", StringType(), False),
    StructField("vader_compound",  DoubleType(), False),
])

@udf(VADER_SCHEMA)
def vader_score(text):
    """VADER imported locally so each Spark executor loads it independently."""
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    if text is None or len(text.strip()) == 0:
        return ("Neutral", 0.0)
    compound = SentimentIntensityAnalyzer().polarity_scores(text)["compound"]
    if compound >= 0.05:
        label = "Positive"
    elif compound <= -0.05:
        label = "Negative"
    else:
        label = "Neutral"
    return (label, float(compound))


# ── Read stream from Kafka ────────────────────────────────────────────────
print(f"[Consumer] Reading stream → topic '{KAFKA_TOPIC}' @ {KAFKA_BROKER}")
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)


# ── Parse JSON, preserve original text, apply training-time text cleaning ─
parsed = (
    raw.selectExpr("CAST(value AS STRING) AS json_str")
       .select(from_json("json_str", tweet_schema).alias("d"))
       .select("d.*")
       .withColumn("original_text", col("text"))
       .withColumn("text", regexp_replace(col("text"), r"http\S+", ""))
       .withColumn("text", regexp_replace(col("text"), r"@\w+", ""))
       .withColumn("text", regexp_replace(col("text"), r"[^a-zA-Z\s]", ""))
       .withColumn("text", trim(lower(col("text"))))
)


# ── Apply Spark model on cleaned text ─────────────────────────────────────
predictions = model.transform(parsed)


# ── Enrich with confidence + VADER + timestamp ────────────────────────────
enriched = (
    predictions
    .withColumn("sentiment_spark",  label_from_prediction(col("prediction")))
    .withColumn("confidence_spark", extract_confidence(col("probability")))
    .withColumn("vader",            vader_score(col("original_text")))
    .withColumn("sentiment_vader",  col("vader.sentiment_vader"))
    .withColumn("vader_compound",   col("vader.vader_compound"))
    .withColumn("timestamp",        current_timestamp())
    .select(
        "tweet_id",
        "entity",
        col("original_text").alias("text"),
        col("sentiment").alias("ground_truth"),  # original CSV label, used by dashboard for accuracy
        "sentiment_spark",
        "confidence_spark",
        "sentiment_vader",
        "vader_compound",
        "timestamp",
    )
)


# ── MongoDB sink via foreachBatch (PyMongo) ───────────────────────────────
def write_to_mongo(batch_df, batch_id):
    count = batch_df.count()
    if count == 0:
        return
    client = MongoClient(MONGO_URI)
    try:
        coll = client[MONGO_DB][MONGO_COLLECTION]
        records = [row.asDict() for row in batch_df.collect()]
        coll.insert_many(records)
        print(f"[Consumer] Batch {batch_id}: wrote {count:>4} docs → {MONGO_DB}.{MONGO_COLLECTION}")
    finally:
        client.close()


# ── Start streaming query ─────────────────────────────────────────────────
query = (
    enriched.writeStream
    .foreachBatch(write_to_mongo)
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime=f"{SPARK_BATCH_INTERVAL} seconds")
    .start()
)

print(f"[Consumer] Streaming started. Trigger: {SPARK_BATCH_INTERVAL}s")
print(f"[Consumer] Checkpoint: {CHECKPOINT_PATH}")
print("[Consumer] Press Ctrl+C to stop.\n")

query.awaitTermination()
