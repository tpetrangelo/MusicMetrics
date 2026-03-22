import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import pandas as pd
import awswrangler as wr
import requests

from datetime import datetime
from app.config import AWS_S3_BUCKET, AWS_S3_WEATHER_SOURCE
from app.utils.consolidate_geo_buckets import bucket_location
from app.utils.s3_io import read_json_from_s3_file, write_json_to_s3

BUCKET = AWS_S3_BUCKET
SOURCE = AWS_S3_WEATHER_SOURCE


def results_to_df(results: list[dict]) -> pd.DataFrame:
    rows = []
    for result in results:
        lat = result["lat_bucket"]
        lon = result["lon_bucket"]
        hourly = result["hourly"]
        
        # Each index = one hour (0-23)
        for hour in range(len(hourly["temperature_2m"])):
            rows.append({
                "lat_bucket":          lat,
                "lon_bucket":          lon,
                "hour":                hour,
                "time":                hourly["time"][hour],
                "temperature_f":       hourly["temperature_2m"][hour],
                "apparent_temp_f":     hourly["apparent_temperature"][hour],
                "precipitation_in":    hourly["precipitation"][hour],
                "weather_code":        hourly["weather_code"][hour],
                "wind_speed_mph":      hourly["wind_speed_10m"][hour],
            })
    
    return pd.DataFrame(rows)

def call_openmeteo(*, lat: float, lon: float, play_date: str) -> dict:
    response = requests.get(
        "https://historical-forecast-api.open-meteo.com/v1/forecast",
        params={
            "latitude":           lat,
            "longitude":          lon,
            "start_date":         play_date,
            "end_date":           play_date,
            "hourly":             "temperature_2m,precipitation,apparent_temperature,weather_code,wind_speed_10m",
            "temperature_unit":   "fahrenheit",
            "wind_speed_unit":    "mph",
            "precipitation_unit": "inch",
        },
        timeout=10
    )
    return response.json()
    


def get_openmeteo_weather(*, geobuckets_key: str, play_date: str):

    prior_day = datetime.strptime(play_date, "%Y-%m-%d").date()  
    df = read_json_from_s3_file(geobuckets_key)   #wr.s3.read_json(path=geobuckets_key, dataset = True, orient="records")

    geobucket_df = df.copy()
    
    consolidated_geobucket_df = bucket_location(geobucket_df)

    results = []

    for _, row in consolidated_geobucket_df.iterrows():
        data = call_openmeteo(
            lat=row["lat_bucket"],
            lon=row["lon_bucket"],
            play_date=play_date
        )

        results.append({
            "lat_bucket": row["lat_bucket"],
            "lon_bucket": row["lon_bucket"],
            "hourly":     data["hourly"]
        })

    weather_df = results_to_df(results=results)

    openmeteo_s3_path = f"s3://{BUCKET}/bronze/{SOURCE}/dt={prior_day}"
    
    write_json_to_s3(weather_df.to_dict(orient="records"),openmeteo_s3_path)
    
    return  openmeteo_s3_path





def run(*, geobuckets_key: str, play_date: str) -> str:
    return get_openmeteo_weather(geobuckets_key=geobuckets_key, play_date=play_date)


if __name__ == "__main__":
    geobuckets_key = "s3://music-metrics/processed/geobuckets/dt=2026-03-21.json"
    play_date = "2026-03-21"
    print(run(geobuckets_key=geobuckets_key, play_date=play_date))
    