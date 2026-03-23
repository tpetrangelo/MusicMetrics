import pandas as pd
from datetime import datetime

from app.config import AWS_S3_BUCKET, AWS_S3_PLAYS_SOURCE

if not AWS_S3_BUCKET:
    raise RuntimeError("AWS_S3_BUCKET not set")
else:
    BUCKET = AWS_S3_BUCKET    

if not AWS_S3_PLAYS_SOURCE:
    raise RuntimeError("AWS_S3_PLAYS_SOURCE not set")
else:
    AWS_SOURCE = AWS_S3_PLAYS_SOURCE 

def _fetch_prior_day_plays(*,play_date: str) -> pd.DataFrame:
    prior_day = datetime.strptime(play_date, "%Y-%m-%d").date()
    plays_path = f"s3://{BUCKET}/bronze/{AWS_SOURCE}/dt={prior_day}"
    return  plays_path


def run(dag_date: str) -> str:
    return _fetch_prior_day_plays(play_date=dag_date)



if __name__ == "__main__":
    # Local test run
    run()
    