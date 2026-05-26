"""
Smart Grid — PySpark Structured Streaming (Local version)
==========================================================
Reads from local Kafka, applies Medallion architecture,
writes Delta Lake tables to local folders.

Run:
    python spark_streaming/stream_processor_local.py

Requirements:
    pip install pyspark delta-spark

Make sure Kafka is running first:
    cd kafka && docker compose up -d
And the simulator is running in a separate terminal:
    python iot_simulator/simulator.py
"""

import os

# ── Windows / Hadoop setup (must happen before PySpark starts the JVM) ─────────
if not os.environ.get("HADOOP_HOME"):
    os.environ["HADOOP_HOME"] = r"C:\hadoop"

# Add HADOOP_HOME\bin to PATH so Java's System.loadLibrary("hadoop") finds
# hadoop.dll — PySpark 3.5.x does not add this automatically on Windows.
_hadoop_bin = os.path.join(os.environ["HADOOP_HOME"], "bin")
if _hadoop_bin.lower() not in os.environ.get("PATH", "").lower():
    os.environ["PATH"] = _hadoop_bin + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, BooleanType,
)
from pyspark.sql.functions import (
    from_json, col, window, avg, max, sum, count,
    current_timestamp, when, lit, to_timestamp,
)
from delta import configure_spark_with_delta_pip

# ── Configuration ──────────────────────────────────────────────────────────────
KAFKA_BROKER  = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC   = "raw-meter-readings"

# Kafka SQL connector — Spark 3.5.x uses Scala 2.12
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"

# Local Delta Lake output paths — forward slashes required by Hadoop on Windows
_base       = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))
BASE_PATH   = _base.replace("\\", "/")
BRONZE_PATH = f"{BASE_PATH}/bronze/meter_readings"
SILVER_PATH = f"{BASE_PATH}/silver/meter_readings"
GOLD_PATH   = f"{BASE_PATH}/gold/consumption_agg"
CHECKPOINT  = f"{BASE_PATH}/checkpoints"

# ── Create output folders ──────────────────────────────────────────────────────
for path in [BRONZE_PATH, SILVER_PATH, GOLD_PATH, CHECKPOINT]:
    os.makedirs(path, exist_ok=True)

# ── Spark Session ──────────────────────────────────────────────────────────────
print("Starting Spark session (downloading JARs on first run — ~1 min)...")

builder = (
    SparkSession.builder
        .appName("SmartGridStream")
        .master("local[*]")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT)
        .config("spark.sql.streaming.minBatchesToRetain", "2")
        .config("spark.sql.shuffle.partitions", "4")
)

spark = configure_spark_with_delta_pip(
    builder, extra_packages=[KAFKA_PACKAGE]
).getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark session ready\n")

# ── Message schema ─────────────────────────────────────────────────────────────
meter_schema = StructType([
    StructField("meter_id",    StringType(),  True),
    StructField("timestamp",   StringType(),  True),
    StructField("kwh_reading", DoubleType(),  True),
    StructField("voltage",     DoubleType(),  True),
    StructField("frequency",   DoubleType(),  True),
    StructField("grid_zone",   StringType(),  True),
    StructField("meter_type",  StringType(),  True),
    StructField("is_anomaly",  BooleanType(), True),
])

# ── Read from Kafka ────────────────────────────────────────────────────────────
print(f"Connecting to Kafka at {KAFKA_BROKER}, topic={KAFKA_TOPIC}...")

raw_stream = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
)

# ── Bronze — raw parsed records ────────────────────────────────────────────────
bronze_stream = (
    raw_stream
        .selectExpr("CAST(value AS STRING) as json_value", "timestamp as kafka_ts")
        .select(
            from_json(col("json_value"), meter_schema).alias("data"),
            col("kafka_ts"),
        )
        .select("data.*", "kafka_ts")
        .withColumn("event_timestamp",  to_timestamp(col("timestamp")))
        .withColumn("ingest_timestamp", current_timestamp())
)

bronze_query = (
    bronze_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT}/bronze")
        .partitionBy("grid_zone")
        .start(BRONZE_PATH)
)
print(f"Bronze → {BRONZE_PATH}")

# ── Silver — validated records ─────────────────────────────────────────────────
silver_stream = (
    bronze_stream
        .filter(col("meter_id").isNotNull())
        .filter(col("kwh_reading").isNotNull())
        .filter(col("kwh_reading") >= 0)
        .filter(col("voltage").between(200, 260))
        .filter(col("frequency").between(49, 51))
        .withColumn("is_valid",            lit(True))
        .withColumn("processed_timestamp", current_timestamp())
)

silver_query = (
    silver_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT}/silver")
        .partitionBy("grid_zone")
        .start(SILVER_PATH)
)
print(f"Silver → {SILVER_PATH}")

# ── Gold — 5-min window aggregations ──────────────────────────────────────────
# NOTE: in append mode, gold results only appear after watermark passes the
# window (window_end + 2 min watermark). Run for at least 7 minutes to see rows.
gold_stream = (
    silver_stream
        .withWatermark("event_timestamp", "2 minutes")
        .groupBy(
            window(col("event_timestamp"), "5 minutes"),
            col("grid_zone"),
            col("meter_type"),
        )
        .agg(
            avg("kwh_reading").alias("avg_kwh"),
            max("kwh_reading").alias("max_kwh"),
            sum("kwh_reading").alias("total_kwh"),
            count("meter_id").alias("meter_count"),
            sum(when(col("is_anomaly"), 1).otherwise(0)).alias("anomaly_count"),
            avg("voltage").alias("avg_voltage"),
            avg("frequency").alias("avg_frequency"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("grid_zone"),
            col("meter_type"),
            col("avg_kwh"),
            col("max_kwh"),
            col("total_kwh"),
            col("meter_count"),
            col("anomaly_count"),
            col("avg_voltage"),
            col("avg_frequency"),
        )
)

gold_query = (
    gold_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT}/gold")
        .start(GOLD_PATH)
)
print(f"Gold  → {GOLD_PATH}")
print("\nAll 3 layers running. Waiting for data from Kafka...")
print("Press Ctrl+C to stop (run for 7+ min to see Gold rows).\n")

# ── Wait ───────────────────────────────────────────────────────────────────────
try:
    spark.streams.awaitAnyTermination(timeout=600)   # 10-min max; Ctrl+C exits sooner
except KeyboardInterrupt:
    print("\nStopping streams...")
finally:
    for q in spark.streams.active:
        q.stop()

    # ── Summary ────────────────────────────────────────────────────────────────
    print("\nFinal row counts:")
    try:
        b = spark.read.format("delta").load(BRONZE_PATH).count()
        s = spark.read.format("delta").load(SILVER_PATH).count()
        g = spark.read.format("delta").load(GOLD_PATH).count()
        print(f"  Bronze : {b:,}")
        print(f"  Silver : {s:,}")
        print(f"  Gold   : {g:,}  (0 is expected if run < 7 min)")

        if g > 0:
            print("\nSample Gold aggregations:")
            spark.read.format("delta").load(GOLD_PATH) \
                .orderBy("window_start", "grid_zone") \
                .show(10, truncate=False)
    except Exception:
        print("  (no data written yet — run longer next time)")

    spark.stop()
    print("\nDone.")
