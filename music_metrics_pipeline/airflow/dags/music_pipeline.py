from __future__ import annotations


# ============================
# Standard library imports
# ============================
from datetime import datetime, timedelta
import os
import sys

# ============================
# Airflow imports
# ============================
from airflow import DAG

try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python import PythonOperator  # type: ignore

from airflow.operators.bash import BashOperator # type: ignore

# ============================
# Path bootstrap
# ============================
PROJECT_PATH = "/opt/airflow/project"
if PROJECT_PATH not in sys.path:
    sys.path.insert(0, PROJECT_PATH)


# ============================
# Local project imports
# ============================

from s3_calls.call_plays import run as plays_run
from ingestion.clients.openmeteo_client import run as openmeteo_run


# ============================
# DAG defaults
# ============================
default_args = {
    "owner": "you",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ============================
# DAG definition
# ============================
with DAG(
    dag_id="ingest_plays_openmeteo_bigquery",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="0 */6 * * *",
    catchup=False,
    tags=["bronze", "processed", "bigquery", "dbt"],
) as dag:

    # -----------------
    # S3 ingestion callables
    # -----------------
    def _s3_plays(**context) -> str:
        return plays_run()

    def _openmeteo_to_s3(ti, **context) -> str:
        airports_key = ti.xcom_pull(task_ids="airports_to_s3")
        return openweather_run(airports_s3_key=airports_key)

    # -----------------
    # S3 ingestion tasks
    # -----------------
    adb_to_s3 = PythonOperator(
        task_id="adb_to_s3",
        python_callable=_adb_to_s3,
    )

    airports_to_s3 = PythonOperator(
        task_id="airports_to_s3",
        python_callable=_airports_to_s3,
    )

    openweather_to_s3 = PythonOperator(
        task_id="openweather_to_s3",
        python_callable=_openweather_to_s3,
    )

    
    # -----------------
    # dbt tasks
    # -----------------

    DBT_PROJECT_DIR = "/opt/airflow/project/dbt/flight_weather_dbt"
    DBT_PROFILES_DIR = "/opt/airflow/dbt_profiles"

    dbt_build_silver = BashOperator(
        task_id="dbt_build_silver",
        bash_command=f"""
        set -euo pipefail
        export DBT_PROFILES_DIR="{DBT_PROFILES_DIR}"
        cd "{DBT_PROJECT_DIR}"
        dbt --version
        dbt build --select silver
        """,
    )

    dbt_test_freshness = BashOperator(
        task_id="dbt_test_freshness",
        bash_command=f"""
        set -euo pipefail
        export DBT_PROFILES_DIR="{DBT_PROFILES_DIR}"
        cd "{DBT_PROJECT_DIR}"
        dbt test --select tag:freshness
        """,
    )


    dbt_build_gold = BashOperator(
        task_id="dbt_build_gold",
        bash_command=f"""
        set -euo pipefail
        export DBT_PROFILES_DIR="{DBT_PROFILES_DIR}"
        cd "{DBT_PROJECT_DIR}"
        dbt --version
        dbt build --select gold
        """,
    )

 
    # -----------------
    # Dependencies
    # -----------------
    adb_to_s3 >> airports_to_s3 >> openweather_to_s3
    openweather_to_s3 >> dbt_build_silver >> dbt_test_freshness >> dbt_build_gold