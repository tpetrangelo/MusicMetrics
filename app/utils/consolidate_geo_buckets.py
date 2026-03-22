import pandas as pd
from datetime import datetime


def bucket_location_time(df: pd.DataFrame) -> pd.DataFrame:

    bucketed = df.copy()

    # Spatial bucketing — 0.1 degrees ≈ 7 miles
    bucketed["lat_bucket"] = bucketed["latitude"].round(1)
    bucketed["lon_bucket"] = bucketed["longitude"].round(1)

    # Temporal bucketing — floor to nearest 30 minutes
    #Unix timestamp in milliseconds, update to readable format for openmeteo

    bucketed["time_bucket"] = (
        pd.to_datetime(bucketed["played_at"], utc=True)
        .dt.floor("h")
        .dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    )

    return (
        bucketed[["lat_bucket", "lon_bucket", "time_bucket"]]
        .dropna()
        .drop_duplicates()
        .reset_index(drop=True)
    )

def bucket_location(df: pd.DataFrame) -> pd.DataFrame:

    bucketed = df.copy()

    # Spatial bucketing — 0.1 degrees ≈ 7 miles
    bucketed["lat_bucket"] = bucketed["lat_bucket"]
    bucketed["lon_bucket"] = bucketed["lon_bucket"]

    return (
        bucketed[["lat_bucket", "lon_bucket"]]
        .dropna()
        .drop_duplicates()
        .reset_index(drop=True)
    )