import json
import boto3
from datetime import datetime
from app.config import (
    BIGQUERY_PROJECT,
    BIGQUERY_DATASET_GOLD,
    AWS_SES_REGION,
    AWS_SES_FROM_EMAIL,
    AWS_SES_TO_EMAIL,
)

from app.utils.bq_io import get_bq_client

if not BIGQUERY_PROJECT:
    raise RuntimeError("BIGQUERY_PROJECT not set")

if not BIGQUERY_DATASET_GOLD:
    raise RuntimeError("BIGQUERY_DATASET_GOLD not set")

if not AWS_SES_REGION:
    raise RuntimeError("AWS_SES_REGION not set")

if not AWS_SES_FROM_EMAIL:
    raise RuntimeError("AWS_SES_FROM_EMAIL not set")

if not AWS_SES_TO_EMAIL:
    raise RuntimeError("AWS_SES_TO_EMAIL not set")



LOG_TABLE = "dag_run_log"

GOLD_TABLES = [
    "mart_plays_by_artist",
    "mart_plays_by_weather",
    "mart_plays_by_location",
    "mart_new_album_rotation",
]


# ── Clients ───────────────────────────────────────────────────────────────────

def _ses_client():
    return boto3.client("ses", region_name=AWS_SES_REGION)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _send_email(subject: str, body: str) -> None:
    _ses_client().send_email(
        Source=AWS_SES_FROM_EMAIL,
        Destination={"ToAddresses": [AWS_SES_TO_EMAIL]},
        Message={
            "Subject": {"Data": subject},
            "Body":    {"Text": {"Data": body}},
        },
    )


def _get_row_counts() -> dict:
    client = get_bq_client()
    counts = {}
    for table in GOLD_TABLES:
        full = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET_GOLD}.{table}"
        result = client.query(f"SELECT COUNT(*) AS n FROM `{full}`").result()
        counts[table] = list(result)[0].n
    return counts


def _write_log(
    dag_id: str,
    run_id: str,
    status: str,
    task_failed: str = None,
    row_counts: dict = None,
) -> None:
    client = get_bq_client()
    full_log = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET_GOLD}.{LOG_TABLE}"
    rows = [{
        "dag_id":      dag_id,
        "run_id":      run_id,
        "status":      status,
        "task_failed": task_failed,
        "row_counts":  json.dumps(row_counts or {}),
        "ran_at":      datetime.utcnow().isoformat(),
    }]
    errors = client.insert_rows_json(full_log, rows)
    if errors:
        print(f"[callbacks] BigQuery log insert errors: {errors}")


# ── Callbacks ─────────────────────────────────────────────────────────────────

def on_failure(context) -> None:
    dag_id  = context["dag"].dag_id
    run_id  = context["run_id"]
    task_id = context["task_instance"].task_id
    log_url = context["task_instance"].log_url
    exc     = str(context.get("exception", "No exception captured"))

    _write_log(
        dag_id=dag_id,
        run_id=run_id,
        status="failure",
        task_failed=task_id,
    )

    body = (
        f"DAG:       {dag_id}\n"
        f"Run ID:    {run_id}\n"
        f"Failed at: {task_id}\n"
        f"Error:     {exc}\n"
        f"Logs:      {log_url}\n"
        f"Time:      {datetime.utcnow()} UTC"
    )
    _send_email(subject=f"DAG failed: {dag_id}", body=body)


def on_success(context) -> None:
    dag_id   = context["dag"].dag_id
    run_id   = context["run_id"]
    end_date = context["dag_run"].end_date
    start    = context["dag_run"].start_date
    duration = (end_date - start) if (end_date and start) else "unknown"

    try:
        counts = _get_row_counts()
    except Exception as e:
        counts = {"error": str(e)}

    _write_log(dag_id=dag_id, run_id=run_id, status="success", row_counts=counts)

    counts_str = "\n".join(
        f"  {tbl:<35} {n:>8} rows" for tbl, n in counts.items()
    )
    body = (
        f"DAG:       {dag_id}\n"
        f"Run ID:    {run_id}\n"
        f"Duration:  {duration}\n"
        f"Finished:  {datetime.utcnow()} UTC\n\n"
        f"Gold table row counts:\n{counts_str}\n"
    )
    _send_email(subject=f"DAG succeeded: {dag_id}", body=body)