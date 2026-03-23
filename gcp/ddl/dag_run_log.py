import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from google.cloud import bigquery
from app.utils.bq_io import get_bq_client
from app.config import BIGQUERY_PROJECT, BIGQUERY_DATASET_GOLD

if not BIGQUERY_PROJECT:
    raise RuntimeError("BIGQUERY_PROJECT not set")

if not BIGQUERY_DATASET_GOLD:
    raise RuntimeError("BIGQUERY_DATASET_GOLD not set")


SCHEMA = [
    bigquery.SchemaField("dag_id",      "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("run_id",      "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("status",      "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("task_failed", "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("row_counts",  "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("ran_at",      "TIMESTAMP", mode="REQUIRED"),
]


def create_table() -> None:
    client = get_bq_client()
    table_ref = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET_GOLD}.dag_run_log"
    table = bigquery.Table(table_ref, schema=SCHEMA)
    table = client.create_table(table, exists_ok=True)
    print(f"Table {table_ref} created/verified")


if __name__ == "__main__":
    create_table()