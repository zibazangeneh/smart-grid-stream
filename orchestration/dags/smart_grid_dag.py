"""
Smart Grid — Airflow DAG
========================
Orchestrates the full pipeline hourly:
  1. Check Kafka health (raw-meter-readings topic reachable)
  2. Run PySpark streaming job for 5 minutes — writes Bronze/Silver/Gold
  3. Load Silver Delta Lake → Snowflake RAW schema
  4. dbt build — run all models + tests

In production this would use:
  - KafkaConsumerLagSensor (Confluent / MSK)
  - DatabricksRunNowOperator for step 2
  - SnowflakeOperator for step 3

Run locally:
  cd orchestration
  docker compose up airflow-init
  docker compose up -d
  # UI: http://localhost:8081  admin / admin
"""

import os
import socket
import subprocess
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ── Constants ──────────────────────────────────────────────────────────────────
KAFKA_BROKER  = os.environ.get("KAFKA_BROKER", "host.docker.internal:9092")
DBT_PROJECT   = "/opt/smart_grid_dbt"
DBT_PROFILES  = "/opt/airflow/dbt_profiles"
LOAD_SCRIPT   = "/opt/load_to_snowflake.py"

default_args = {
    "owner":            "ziba",
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

# ── Task functions ─────────────────────────────────────────────────────────────

def check_kafka_health(**context):
    """Verify Kafka broker is reachable before starting the pipeline."""
    host, port = KAFKA_BROKER.split(":")
    try:
        with socket.create_connection((host, int(port)), timeout=5):
            print(f"Kafka reachable at {KAFKA_BROKER}")
    except (socket.timeout, ConnectionRefusedError) as e:
        raise RuntimeError(f"Kafka not reachable at {KAFKA_BROKER}: {e}")


def load_silver_to_snowflake(**context):
    """Load Silver Delta Lake parquet files into Snowflake RAW.METER_READINGS."""
    result = subprocess.run(
        [sys.executable, LOAD_SCRIPT],
        capture_output=True,
        text=True,
        env={**os.environ},
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"load_to_snowflake.py failed:\n{result.stderr}")


# ── DAG ────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="smart_grid_pipeline",
    default_args=default_args,
    description="Smart Grid hourly pipeline: Kafka → Spark → Snowflake → dbt",
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["smart-grid", "streaming", "dbt"],
) as dag:

    # 1 — Kafka health check
    check_kafka = PythonOperator(
        task_id="check_kafka_health",
        python_callable=check_kafka_health,
        doc_md="TCP check that Kafka broker is reachable before starting the pipeline.",
    )

    # 2 — PySpark streaming (5-minute batch)
    # In production: DatabricksRunNowOperator pointing to a job cluster
    run_spark = BashOperator(
        task_id="run_spark_streaming",
        bash_command=(
            "timeout 300 python /opt/spark_streaming/stream_processor_local.py; "
            "exit_code=$?; [ $exit_code -eq 124 ] && exit 0 || exit $exit_code"
        ),
        doc_md=(
            "Run PySpark Structured Streaming for 5 minutes. "
            "Writes Bronze → Silver → Gold Delta Lake tables. "
            "In production: DatabricksRunNowOperator."
        ),
    )

    # 3 — Load Silver → Snowflake
    load_snowflake = PythonOperator(
        task_id="load_silver_to_snowflake",
        python_callable=load_silver_to_snowflake,
        doc_md="Export Silver Delta parquet files and COPY INTO Snowflake RAW.METER_READINGS.",
    )

    # 4 — dbt build (models + tests in one command)
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"dbt build "
            f"--project-dir {DBT_PROJECT} "
            f"--profiles-dir {DBT_PROFILES} "
            f"--target prod"
        ),
        doc_md="Run all dbt models and tests. Fails the DAG if any test fails.",
    )

    # ── Pipeline order ─────────────────────────────────────────────────────────
    check_kafka >> run_spark >> load_snowflake >> dbt_build
