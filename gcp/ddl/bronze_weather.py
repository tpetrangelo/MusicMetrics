import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from google.cloud import bigquery
from app.utils.bq_io import get_bq_client
from app.config import BIGQUERY_PROJECT, BIGQUERY_DATASET_BRONZE

SCHEMA = [
    bigquery.SchemaField("lat_bucket",       "FLOAT",   mode="REQUIRED"),
    bigquery.SchemaField("lon_bucket",       "FLOAT",   mode="REQUIRED"),
    bigquery.SchemaField("date",             "DATE",    mode="REQUIRED"),
    bigquery.SchemaField("hour",             "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("time",             "STRING",  mode="NULLABLE"),
    bigquery.SchemaField("temperature_f",    "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("apparent_temp_f",  "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("precipitation_in", "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("weather_code",     "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("wind_speed_mph",   "FLOAT",   mode="NULLABLE"),
]

def create_table() -> None:
    client = get_bq_client()
    table_ref = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET_BRONZE}.weather"
    table = bigquery.Table(table_ref, schema=SCHEMA)
    table = client.create_table(table, exists_ok=True)
    print(f"Table {table_ref} created/verified")

if __name__ == "__main__":
    create_table()