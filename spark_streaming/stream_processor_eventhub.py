# Smart Grid — PySpark Structured Streaming
# ==========================================
# Reads from Azure Event Hubs, applies Medallion architecture
# (Bronze → Silver → Gold), writes Delta Lake to Azure ADLS Gen2.
#
# Run in Azure Databricks — paste each cell separately.
#
# Before running, set environment variables in your Databricks cluster:
#   Compute → your cluster → Edit → Advanced options → Environment variables:
#     EVENTHUB_CONN_STR = your-event-hubs-connection-string
#     AZURE_STORAGE_KEY = your-storage-account-key


# ── CELL 1 — Configuration ────────────────────────────────────────────────────
import os

# Azure Event Hubs
EVENTHUB_NAMESPACE = "smartgrid-eventhub"
EVENTHUB_NAME      = "raw-meter-readings"
EVENTHUB_CONN_STR  = os.environ.get("EVENTHUB_CONN_STR")

# Azure ADLS Gen2
STORAGE_ACCOUNT    = "smartgridziba"
STORAGE_KEY        = os.environ.get("AZURE_STORAGE_KEY")

# Validate secrets are set
if not EVENTHUB_CONN_STR:
    raise ValueError("EVENTHUB_CONN_STR environment variable is not set")
if not STORAGE_KEY:
    raise ValueError("AZURE_STORAGE_KEY environment variable is not set")

# ADLS paths
BRONZE_PATH = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/meter_readings"
SILVER_PATH = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/meter_readings"
GOLD_PATH   = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net/consumption_agg"

print("✅ Configuration loaded from environment variables")
print(f"   Event Hub : {EVENTHUB_NAMESPACE}/{EVENTHUB_NAME}")
print(f"   Storage   : {STORAGE_ACCOUNT}")


# ── CELL 2 — Connect Databricks to Azure ADLS Gen2 ───────────────────────────
spark.conf.set(
    f"fs.azure.account.auth.type.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    "SharedKey"
)
spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    STORAGE_KEY
)

try:
    dbutils.fs.ls(f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/")
    print("✅ Connected to Azure ADLS Gen2")
except Exception as e:
    print(f"❌ Connection failed: {e}")


# ── CELL 3 — Event Hubs connection config for Spark ──────────────────────────
eventhub_config = {
    "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs \
        .EventHubsUtils.encrypt(spark.sparkContext, EVENTHUB_CONN_STR),
    "eventhubs.consumerGroup":    "$Default",
    "eventhubs.startingPosition": '{"offset": "@latest", "seqNo": -1, "enqueuedTime": null, "isInclusive": true}',
}

print("✅ Event Hubs config ready")


# ── CELL 4 — Define message schema ────────────────────────────────────────────
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, BooleanType
)
from pyspark.sql.functions import (
    from_json, col, window, avg, max, sum, count,
    current_timestamp, when, lit, to_timestamp
)

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

print("✅ Schema defined")


# ── CELL 5 — Read stream from Event Hubs ─────────────────────────────────────
raw_stream = (
    spark.readStream
        .format("eventhubs")
        .options(**eventhub_config)
        .load()
)

bronze_stream = (
    raw_stream
        .selectExpr("CAST(body AS STRING) as json_value", "enqueuedTime as event_time")
        .select(
            from_json(col("json_value"), meter_schema).alias("data"),
            col("event_time")
        )
        .select("data.*", "event_time")
        .withColumn("event_timestamp",  to_timestamp(col("timestamp")))
        .withColumn("ingest_timestamp", current_timestamp())
)

print("✅ Bronze stream defined — reading from Event Hubs")


# ── CELL 6 — Write Bronze to ADLS ────────────────────────────────────────────
bronze_query = (
    bronze_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{BRONZE_PATH}/_checkpoint")
        .option("mergeSchema", "true")
        .partitionBy("grid_zone")
        .start(BRONZE_PATH)
)

print(f"✅ Bronze → {BRONZE_PATH}")


# ── CELL 7 — Silver layer (cleaned + validated) ───────────────────────────────
silver_stream = (
    bronze_stream
        .filter(col("meter_id").isNotNull())
        .filter(col("kwh_reading").isNotNull())
        .filter(col("kwh_reading") >= 0)
        .filter(col("voltage").between(200, 260))
        .filter(col("frequency").between(49, 51))
        .withColumn("is_valid", lit(True))
        .withColumn("processed_timestamp", current_timestamp())
)

silver_query = (
    silver_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{SILVER_PATH}/_checkpoint")
        .partitionBy("grid_zone")
        .start(SILVER_PATH)
)

print(f"✅ Silver → {SILVER_PATH}")


# ── CELL 8 — Gold layer (5-minute window aggregations) ───────────────────────
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
        .option("checkpointLocation", f"{GOLD_PATH}/_checkpoint")
        .start(GOLD_PATH)
)

print(f"✅ Gold → {GOLD_PATH}")
print("\n🎉 All 3 layers running. Check Azure containers in 2–3 minutes.")


# ── CELL 9 — Verify data (run after 3–5 minutes) ─────────────────────────────
bronze_df = spark.read.format("delta").load(BRONZE_PATH)
silver_df = spark.read.format("delta").load(SILVER_PATH)
gold_df   = spark.read.format("delta").load(GOLD_PATH)

print(f"Bronze rows : {bronze_df.count():,}")
print(f"Silver rows : {silver_df.count():,}")
print(f"Gold rows   : {gold_df.count():,}")

print("\n📊 Gold layer — consumption by zone:")
gold_df.orderBy("window_start", "grid_zone").show(10, truncate=False)
