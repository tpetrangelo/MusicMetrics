from google.cloud import bigquery
from app.utils.bq_io import get_bq_client
from app.config import BIGQUERY_PROJECT, BIGQUERY_DATASET_BRONZE

SCHEMA = [
    bigquery.SchemaField("id",           "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("track_id",     "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("track_name",   "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("artist_name",  "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("album_name",   "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("duration_ms",  "INTEGER",   mode="NULLABLE"),
    bigquery.SchemaField("played_at",    "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("release_date", "DATE",      mode="NULLABLE"),
    bigquery.SchemaField("latitude",     "FLOAT",     mode="NULLABLE"),
    bigquery.SchemaField("longitude",    "FLOAT",     mode="NULLABLE"),
    bigquery.SchemaField("source",       "STRING",    mode="NULLABLE"),
]

def create_table() -> None:
    client = get_bq_client()
    table_ref = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET_BRONZE}.plays"
    table = bigquery.Table(table_ref, schema=SCHEMA)
    table = client.create_table(table, exists_ok=True)
    print(f"Table {table_ref} created/verified")

if __name__ == "__main__":
    create_table()