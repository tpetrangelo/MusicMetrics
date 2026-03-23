import pandas as pd
from app.utils.bq_io import load_df_to_bq
from app.utils.s3_io import read_json_from_s3_file
from app.config import BIGQUERY_DATASET_BRONZE
from gcp.ddl.bronze_weather import SCHEMA


def ingest_weather(weather_key: str) -> None:
    print(f"Reading weather from {weather_key}")
    df = read_json_from_s3_file(weather_key)

    if df.empty:
        print("No weather data found — skipping")
        return
    
    # Cast types to match schema
    df["lat_bucket"]        = df["lat_bucket"].astype(float)
    df["lon_bucket"]        = df["lon_bucket"].astype(float)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["hour"]              = df["hour"].astype("Int64")
    df["temperature_f"]     = df["temperature_f"].astype(float)
    df["apparent_temp_f"]   = df["apparent_temp_f"].astype(float)
    df["precipitation_in"]  = df["precipitation_in"].astype(float)
    df["weather_code"]      = df["weather_code"].astype("Int64")
    df["wind_speed_mph"]    = df["wind_speed_mph"].astype(float)

    print(f"Loading {len(df)} rows to BigQuery bronze.weather")
    load_df_to_bq(
        df=df,
        dataset=BIGQUERY_DATASET_BRONZE,
        table="weather",
        schema=SCHEMA,
        write_disposition="WRITE_TRUNCATE"
    )


def run(weather_key: str) -> None:
    ingest_weather(weather_key=weather_key)


if __name__ == "__main__":
    weather_key = "s3://music-metrics/bronze/openmeteo/dt=2026-03-22.json"
    run(weather_key=weather_key)