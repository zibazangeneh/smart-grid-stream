"""
Smart Grid — Airflow DAG
========================
Orchestrates the full pipeline hourly:
  1. Check Kafka / Event Hubs lag
  2. Trigger PySpark streaming job on Databricks
  3. Wait for ADLS Gold layer write
  4. Run dbt transformations
  5. Run dbt tests
  6. Alert on failure

Week 4 TODO: complete this DAG
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    "owner":            "ziba",
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": True,
}

with DAG(
    dag_id="smart_grid_pipeline",
    default_args=default_args,
    description="Smart Grid real-time pipeline — hourly",
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["smart-grid", "streaming", "dbt"],
) as dag:

    # TODO Week 4: implement each task
    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command="dbt run --project-dir /opt/smart_grid_dbt --target prod",
    )

    test_dbt = BashOperator(
        task_id="test_dbt",
        bash_command="dbt test --project-dir /opt/smart_grid_dbt --target prod",
    )

    run_dbt >> test_dbt
