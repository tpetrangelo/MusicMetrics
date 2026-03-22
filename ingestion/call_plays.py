
import pandas as pd
from datetime import date, datetime


import sys
import os
sys.path.insert(0, '/workspaces/MusicMetrics')

from app.config import AWS_S3_BUCKET, AWS_S3_PLAYS_SOURCE

BUCKET = AWS_S3_BUCKET
SOURCE = AWS_S3_PLAYS_SOURCE


def _fetch_prior_day_plays(*,play_date: str) -> pd.DataFrame:
    prior_day = datetime.strptime(play_date, "%Y-%m-%d").date()
    plays_path = f"s3://{BUCKET}/bronze/{SOURCE}/dt={prior_day}"
    return  plays_path


def run(dag_date: str) -> str:
    return _fetch_prior_day_plays(play_date=dag_date)



if __name__ == "__main__":
    # Local test run
    run()
    