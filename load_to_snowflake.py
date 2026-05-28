"""
Load Silver Delta Lake data into Snowflake RAW.METER_READINGS
Run once to populate Snowflake for the dbt pipeline.

Usage:
    python load_to_snowflake.py
"""

import os
import glob
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# ── Config ─────────────────────────────────────────────────────────────────────
ACCOUNT   = "bxhqszl-ur89326"
USER      = "ZIBAZNGNH"
PASSWORD  = os.environ.get("SNOWFLAKE_PASSWORD")   # set this before running
WAREHOUSE = "SMART_GRID_WH"
DATABASE  = "SMART_GRID_DB"
SCHEMA    = "RAW"
TABLE     = "METER_READINGS"

SILVER_PATH = os.path.join(
    os.path.dirname(__file__), "data", "silver", "meter_readings"
)

# ── Read all Silver parquet files ───────────────────────────────────────────────
print("Reading Silver parquet files...")
parquet_files = glob.glob(os.path.join(SILVER_PATH, "**", "*.parquet"), recursive=True)
if not parquet_files:
    raise FileNotFoundError(f"No parquet files found in {SILVER_PATH}")

df = pd.read_parquet(SILVER_PATH, engine="pyarrow")

# Normalise column names to uppercase (Snowflake default)
df.columns = [c.upper() for c in df.columns]

# Drop Delta Lake internal columns if present
for col in ["__NULL_DASK_INDEX__"]:
    if col in df.columns:
        df = df.drop(columns=[col])

print(f"Loaded {len(df):,} rows, columns: {list(df.columns)}")

# ── Connect to Snowflake ────────────────────────────────────────────────────────
print("Connecting to Snowflake...")
conn = snowflake.connector.connect(
    account=ACCOUNT,
    user=USER,
    password=PASSWORD,
    warehouse=WAREHOUSE,
    database=DATABASE,
    schema=SCHEMA,
)

cur = conn.cursor()

# Create table if it doesn't exist
print(f"Creating table {DATABASE}.{SCHEMA}.{TABLE} if needed...")
cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        METER_ID          VARCHAR,
        TIMESTAMP         VARCHAR,
        KWH_READING       FLOAT,
        VOLTAGE           FLOAT,
        FREQUENCY         FLOAT,
        GRID_ZONE         VARCHAR,
        METER_TYPE        VARCHAR,
        IS_ANOMALY        BOOLEAN,
        KAFKA_TS          TIMESTAMP_NTZ,
        EVENT_TIMESTAMP   TIMESTAMP_NTZ,
        INGEST_TIMESTAMP  TIMESTAMP_NTZ,
        IS_VALID          BOOLEAN,
        PROCESSED_TIMESTAMP TIMESTAMP_NTZ
    )
""")

# ── Upload ──────────────────────────────────────────────────────────────────────
print(f"Uploading {len(df):,} rows to {DATABASE}.{SCHEMA}.{TABLE}...")
success, nchunks, nrows, _ = write_pandas(
    conn, df, TABLE,
    database=DATABASE, schema=SCHEMA,
    auto_create_table=False,
    quote_identifiers=False,
)

print(f"Done. success={success}, chunks={nchunks}, rows_uploaded={nrows:,}")
cur.close()
conn.close()
