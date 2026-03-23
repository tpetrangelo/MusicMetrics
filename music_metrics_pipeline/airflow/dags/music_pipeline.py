from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime, timedelta

from ingestion.call_plays import run as plays_run
from ingestion.geo_buckets import run as geobuckets_run
from ingestion.clients.openmeteo_client import run as openmeteo_run
from gcp.ingest.ingest_plays import run as ingest_plays_run
from gcp.ingest.ingest_weather import run as ingest_weather_run

from dbt.cli.main import dbtRunner, dbtRunnerResult

from app.utils.callbacks import on_failure, on_success

from airflow import DAG
from datetime import datetime

default_args = {
    "owner": "music_metrics",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure,
}

DBT_PROJECT_DIR = "/opt/airflow/MusicMetrics/music_metrics_pipeline/dbt/music_metrics"
DBT_PROFILES_DIR = "/opt/airflow/MusicMetrics/music_metrics_pipeline/dbt/music_metrics"

with DAG(
    dag_id="music_metrics_pipeline",
    description="Daily music play ingestion, weather enrichment, and dbt transformation",
    schedule_interval="0 5 * * *",
    on_success_callback = on_success,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["music_metrics"],
) as dag:

    # ── S3 Ingestion ──────────────────────────────────────────

    def _s3_plays(**context) -> str:
        date = context["ds"]
        return plays_run(date)

    def _geobuckets_to_s3(ti, **context) -> str:
        date = context["ds"]
        plays_key = ti.xcom_pull(task_ids="fetch_s3_plays")
        return geobuckets_run(plays_key=plays_key, play_date=date)

    def _openmeteo_to_s3(ti, **context) -> str:
        date = context["ds"]
        geobuckets_key = ti.xcom_pull(task_ids="geobuckets_to_s3")
        return openmeteo_run(geobuckets_key=geobuckets_key, play_date=date)

    # ── BigQuery Ingestion ────────────────────────────────────

    def _ingest_plays_to_bq(ti, **context) -> None:
        plays_key = ti.xcom_pull(task_ids="fetch_s3_plays")
        ingest_plays_run(plays_key=plays_key)

    def _ingest_weather_to_bq(ti, **context) -> None:
        weather_key = ti.xcom_pull(task_ids="openmeteo_to_s3")
        ingest_weather_run(weather_key=weather_key)

    # ── dbt ───────────────────────────────────────────────────

    def _run_dbt(**context):
        dbt = dbtRunner()
        args = [
            "run",
            "--project-dir", DBT_PROJECT_DIR,
            "--profiles-dir", DBT_PROFILES_DIR,
        ]
        res: dbtRunnerResult = dbt.invoke(args)
        if not res.success:
            raise Exception(f"dbt run failed: {res.exception}")

    def _test_dbt(**context):
        dbt = dbtRunner()
        args = [
            "test",
            "--project-dir", DBT_PROJECT_DIR,
            "--profiles-dir", DBT_PROFILES_DIR,
        ]
        res: dbtRunnerResult = dbt.invoke(args)
        if not res.success:
            raise Exception(f"dbt test failed: {res.exception}")

    # ── Operators ─────────────────────────────────────────────

    fetch_s3_plays = PythonOperator(
        task_id="fetch_s3_plays",
        python_callable=_s3_plays,
    )

    geobuckets_to_s3 = PythonOperator(
        task_id="geobuckets_to_s3",
        python_callable=_geobuckets_to_s3,
    )

    openmeteo_to_s3 = PythonOperator(
        task_id="openmeteo_to_s3",
        python_callable=_openmeteo_to_s3,
    )

    ingest_plays_to_bq = PythonOperator(
        task_id="ingest_plays_to_bq",
        python_callable=_ingest_plays_to_bq,
    )

    ingest_weather_to_bq = PythonOperator(
        task_id="ingest_weather_to_bq",
        python_callable=_ingest_weather_to_bq,
    )

    run_dbt = PythonOperator(
        task_id="run_dbt",
        python_callable=_run_dbt,
    )

    test_dbt = PythonOperator(
        task_id="test_dbt",
        python_callable=_test_dbt,
    )

    # ── Dependencies ──────────────────────────────────────────

    fetch_s3_plays >> geobuckets_to_s3 >> openmeteo_to_s3

    fetch_s3_plays >> ingest_plays_to_bq
    openmeteo_to_s3 >> ingest_weather_to_bq

    [ingest_plays_to_bq, ingest_weather_to_bq] >> run_dbt >> test_dbt