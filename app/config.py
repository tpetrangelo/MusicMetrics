import os
from dotenv import load_dotenv

load_dotenv()  # loads .env into environment

OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")

AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET")
AWS_S3_PLAYS_SOURCE = os.getenv("AWS_S3_PLAYS_SOURCE")
AWS_S3_WEATHER_SOURCE = os.getenv("AWS_S3_WEATHER_SOURCE")

if not AWS_S3_BUCKET:
    raise RuntimeError("AWS_S3_BUCKET not set")

if not AWS_S3_PLAYS_SOURCE:
    raise RuntimeError("AWS_S3_PLAYS_SOURCE not set")

if not AWS_S3_WEATHER_SOURCE:
    raise RuntimeError("AWS_S3_WEATHER_SOURCE not set")