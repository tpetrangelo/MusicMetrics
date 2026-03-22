import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import awswrangler as wr

from datetime import datetime
from app.config import AWS_S3_BUCKET, AWS_S3_GEOBUCKET_SOURCE
from app.utils.consolidate_geo_buckets import bucket_location_time
from app.utils.s3_io import read_json_from_s3_prefix, write_json_to_s3

BUCKET = AWS_S3_BUCKET
SOURCE = AWS_S3_GEOBUCKET_SOURCE

def make_geobuckets(*, plays_key: str, play_date: str) -> str:


    prior_day = datetime.strptime(play_date, "%Y-%m-%d").date()  

    df = read_json_from_s3_prefix(plays_key)

    geobucket_df = df.copy()

    consolidated_geobucket_df = bucket_location_time(geobucket_df)

    geobucket_s3_path = f"s3://{BUCKET}/processed/{SOURCE}/dt={prior_day}.json"

    write_json_to_s3(consolidated_geobucket_df.to_dict(orient="records"), geobucket_s3_path)
    
    return  geobucket_s3_path


def run(plays_key: str, play_date: str) -> str:
    return make_geobuckets(plays_key=plays_key, play_date=play_date)


if __name__ == "__main__":
    # Local test run
    plays_key = "s3://music-metrics/bronze/plays/dt=2026-03-21/"
    play_date = "2026-03-21"
    print(run(plays_key,play_date))