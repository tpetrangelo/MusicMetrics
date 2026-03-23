import os
from dotenv import load_dotenv

load_dotenv()  # loads .env into environment

OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")

AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET")
AWS_S3_PLAYS_SOURCE = os.getenv("AWS_S3_PLAYS_SOURCE")
AWS_S3_GEOBUCKET_SOURCE = os.getenv("AWS_S3_GEOBUCKET_SOURCE")
AWS_S3_WEATHER_SOURCE = os.getenv("AWS_S3_WEATHER_SOURCE")

AWS_SES_REGION = os.getenv("AWS_SES_REGION")
AWS_SES_FROM_EMAIL=os.getenv("AWS_SES_FROM_EMAIL")
AWS_SES_TO_EMAIL=os.getenv("AWS_SES_TO_EMAIL")

GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
BIGQUERY_PROJECT = os.getenv("BIGQUERY_PROJECT")
BIGQUERY_DATASET_BRONZE = os.getenv("BIGQUERY_DATASET_BRONZE")
BIGQUERY_DATASET_SILVER = os.getenv("BIGQUERY_DATASET_SILVER")
BIGQUERY_DATASET_GOLD = os.getenv("BIGQUERY_DATASET_GOLD")

if not AWS_S3_BUCKET:
    raise RuntimeError("AWS_S3_BUCKET not set")

if not AWS_S3_PLAYS_SOURCE:
    raise RuntimeError("AWS_S3_PLAYS_SOURCE not set")

if not AWS_S3_GEOBUCKET_SOURCE:
    raise RuntimeError("AWS_S3_GEOBUCKET_SOURCE not set")

if not AWS_S3_WEATHER_SOURCE:
    raise RuntimeError("AWS_S3_WEATHER_SOURCE not set")

if not AWS_SES_REGION:
    raise RuntimeError("AWS_SES_REGION not set")

if not AWS_SES_FROM_EMAIL:
    raise RuntimeError("AWS_SES_FROM_EMAIL not set")

if not AWS_SES_TO_EMAIL:
    raise RuntimeError("AWS_SES_TO_EMAIL not set")

if not BIGQUERY_PROJECT:
    raise RuntimeError("BIGQUERY_PROJECT not set")
