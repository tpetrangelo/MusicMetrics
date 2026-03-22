from __future__ import annotations
import pandas as pd

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

from ingestion.call_plays import run as plays_run
from ingestion.clients.openmeteo_client import run as openmeteo_run
from ingestion.geo_buckets import run as geobuckets_run

# ============================
# DAG defaults
# ============================
default_args = {
    "owner": "you",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


# ============================
# DAG definition
# ============================
with DAG(
    dag_id="ingest_plays_openmeteo_bigquery",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="0 5 * * *",
    catchup=False,
    tags=["bronze", "processed", "bigquery", "dbt"],
) as dag:


    def _s3_plays(**context) -> str:
        date = context["ds"]
        return plays_run(date)

    def _geobuckets_to_s3(ti, **context) -> str:
        date = context["ds"]
        plays_key = ti.xcom_pull(task_ids="fetch_s3_plays")
        return geobuckets_run(plays_key=plays_key, play_date=date)

    def _openmeteo_to_s3(ti, **context) -> str:
        date = context["ds"]
        geobuckets_key = ti.xcom_pull(task_ids="make_geobuckets")
        return openmeteo_run(geobuckets_key=geobuckets_key, play_date=date)


    fetch_s3_plays = PythonOperator(
        task_id="fetch_s3_plays",
        python_callable=_s3_plays,
    )
    
    make_geobuckets = PythonOperator(
        task_id = "geobuckets_to_s3",
        python_callable = _geobuckets_to_s3
    )

    openmeteo_to_s3 = PythonOperator(
        task_id="openmeteo_to_s3",
        python_callable=_openmeteo_to_s3,
    )
    
    # -----------------
    # dbt tasks
    # -----------------

    DBT_PROJECT_DIR = "/opt/airflow/project/dbt/play_weather_dbt"
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
    fetch_s3_plays >> make_geobuckets >> openmeteo_to_s3 #>> dbt_build_silver >> dbt_test_freshness >> dbt_build_gold