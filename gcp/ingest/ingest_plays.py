import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import pandas as pd
from app.utils.bq_io import load_df_to_bq
from app.utils.s3_io import read_json_from_s3_prefix
from app.config import BIGQUERY_DATASET_BRONZE
from gcp.ddl.bronze_plays import SCHEMA


def ingest_plays(plays_key: str) -> None:
    print(f"Reading plays from {plays_key}")
    df = read_json_from_s3_prefix(plays_key)

    if df.empty:
        print("No plays found — skipping")
        return

    # Cast types to match schema
    df["played_at"]    = pd.to_datetime(df["played_at"], utc=True)
    df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce").dt.date
    df["duration_ms"]  = df["duration_ms"].astype("Int64")
    df["latitude"]     = df["latitude"].astype(float)
    df["longitude"]    = df["longitude"].astype(float)
    df["source"]       = "ios"

    print(f"Loading {len(df)} plays to BigQuery bronze.plays")
    load_df_to_bq(
        df=df,
        dataset=BIGQUERY_DATASET_BRONZE,
        table="plays",
        schema=SCHEMA,
        write_disposition="WRITE_TRUNCATE"
    )


def run(plays_key: str) -> None:
    ingest_plays(plays_key=plays_key)


if __name__ == "__main__":
    plays_key = "s3://music-metrics/bronze/plays/dt=2026-03-21/"
    run(plays_key=plays_key)