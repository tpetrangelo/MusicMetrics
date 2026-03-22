import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from google.cloud import bigquery
from google.oauth2 import service_account
from app.config import GOOGLE_APPLICATION_CREDENTIALS, BIGQUERY_PROJECT

import pandas as pd

def get_bq_client() -> bigquery.Client:
    credentials = service_account.Credentials.from_service_account_file(
        GOOGLE_APPLICATION_CREDENTIALS,
        scopes=["https://www.googleapis.com/auth/bigquery"]
    )
    return bigquery.Client(project=BIGQUERY_PROJECT, credentials=credentials)


def load_df_to_bq(
    df: pd.DataFrame,
    dataset: str,
    table: str,
    schema: list,
    write_disposition: str = "WRITE_APPEND"
) -> None:
    client = get_bq_client()
    table_ref = f"{BIGQUERY_PROJECT}.{dataset}.{table}"

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=write_disposition,
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()

    print(f"Loaded {len(df)} rows to {table_ref}")


if __name__ == "__main__":
    client = get_bq_client()
    print(f"Connected to BigQuery project: {client.project}")