# MusicMetrics

A personal music analytics pipeline that captures Apple Music listening history enriched with GPS location and real-time weather data. Every play event — track, artist, album, coordinates, and atmospheric conditions — is ingested, stored, transformed, and made available for analysis.

Link to MusicMetrics dashboard [MusicMetrics](https://pages.github.com/https://musicmetrics-i6fxmgvgvbxwf4bxl3fspn.streamlit.app/).

---

## Architecture

```
iPhone (iOS App)
  MusicKit + CoreLocation
  Observes SystemMusicPlayer for track changes
  Captures GPS on every track commit (75% listen threshold)
        |
        | HTTPS POST
        v
AWS API Gateway + Lambda
  FastAPI application (containerized via Docker + ECR)
  Validates and writes raw JSON to S3
        |
        v
AWS S3 (Raw / Bronze Layer)
  s3://music-metrics/plays/ios/YYYY/MM/DD/{track_id}_{timestamp}.json
        |
        | Airflow DAG @ 5am UTC
        v
Coordinate Bucketing (geo_buckets.py)
  Groups plays into ~7 mile spatial grids (0.1 degree rounding)
  Minimizes downstream API calls
        |
        v
Open-Meteo API (Historical Weather)
  One API call per unique location per day
  Returns full 24-hour hourly weather for each location bucket
  Written to s3://music-metrics/weather/YYYY/MM/DD/
        |
        v
Google BigQuery
  bronze.plays       <- raw play events
  bronze.weather     <- hourly weather per location bucket
        |
        | dbt
        v
  silver.stg_plays             <- cleaned, deduplicated
  silver.stg_weather           <- cleaned weather
  silver.silver_plays_enriched <- plays joined with weather + weather codes
        |
        v
  gold.mart_plays_by_artist
  gold.mart_plays_by_weather
  gold.mart_plays_by_location
  gold.mart_new_album_rotation
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data Collection | Swift iOS app (MusicKit, CoreLocation) |
| Ingestion API | FastAPI, AWS Lambda, API Gateway |
| Containerization | Docker, AWS ECR |
| Raw Storage | AWS S3 |
| Orchestration | Apache Airflow on EC2 |
| Weather Enrichment | Open-Meteo API (free, no key required) |
| Data Warehouse | Google BigQuery |
| Transformation | dbt (dbt-bigquery) |
| Secrets Management | AWS Secrets Manager |
| Infrastructure | EC2, EventBridge (scheduled start/stop) |

---

## iOS App

The iOS app is a lightweight background data collector. It does not replace Apple Music — it runs silently alongside it.

- Observes `SystemMusicPlayer` via a 3-second poll timer
- Commits a play event only after 75% of the track's duration has elapsed
- Handles pause/resume by banking accumulated listen time
- Captures GPS via CoreLocation on every commit
- Buffers events locally in case of network failure, retries on next launch
- Stays alive in the background via the Location Updates background mode

The app POSTs a single JSON payload per play to the API Gateway endpoint. It has no knowledge of S3, BigQuery, or dbt.

```json
{
  "track_id": "abc123",
  "track_name": "Pyramid Song",
  "artist_name": "Radiohead",
  "album_name": "Amnesiac",
  "duration_ms": 294000,
  "played_at": "2026-03-21T14:32:11Z",
  "release_date": "2001-06-04",
  "latitude": 42.2476,
  "longitude": -71.1746
}
```

---

## Pipeline

### Ingestion (FastAPI on Lambda)

- Receives POST requests from the iOS app at `/plays/ios`
- Validates the payload using Pydantic
- Writes raw JSON to S3 partitioned by date
- Source is inferred from the route, not from the payload

### Orchestration (Airflow DAG)

The DAG runs at 5am UTC daily, processing the previous day's data.

```
fetch_s3_plays
    |
    |-----> geobuckets_to_s3
    |             |
    |             v
    |       openmeteo_to_s3
    |             |
    |    --------------------
    |    |                  |
    v    v                  v
ingest_plays_to_bq   ingest_weather_to_bq
         |                  |
         --------------------
                  |
                  v
              run_dbt
                  |
                  v
             test_dbt
```

Tasks run in parallel where dependencies allow. `ingest_plays_to_bq` and `ingest_weather_to_bq` run concurrently since they are independent. dbt runs only after both loads complete.

### Coordinate Bucketing

Rather than calling the weather API once per play event, plays are grouped into spatial and temporal buckets before the API call:

- **Spatial**: Coordinates are rounded to 1 decimal place (~7 mile grid)
- **Result**: If 40 songs were played from home today, that is one API call, not 40

### Weather Enrichment

Open-Meteo returns full 24-hour hourly data per location per day. Each row in `bronze.weather` represents one hour at one location bucket. The dbt silver model joins plays to weather on `lat_bucket + lon_bucket + hour`.

### dbt Models

**Staging**
- `stg_plays` — deduplicates on `id`, casts types, derives `lat_bucket`, `lon_bucket`, `hour`
- `stg_weather` — casts types, renames `date` to `weather_date`

**Silver**
- `silver_plays_enriched` — joins `stg_plays` + `stg_weather` + `weather_codes` seed

**Gold**
- `mart_plays_by_artist` — total plays, minutes listened, most common weather and time of day per artist
- `mart_plays_by_weather` — play counts and minutes by weather category and temperature bucket
- `mart_plays_by_location` — play counts per coordinate bucket, top artist per location
- `mart_new_album_rotation` — days until first play and total plays per album since release

**Seed**
- `weather_codes` — WMO weather code reference table with description, category, and severity (0-5)

---

## Data Quality

24 dbt tests run after every pipeline execution:

- `not_null` on all required fields across staging, silver, and gold models
- `unique` on `id` in staging and silver
- `accepted_values` on `source` field (`ios`, `lastfm`)

---

## Infrastructure

### EC2

Airflow runs on a `t3.small` EC2 instance with 2GB swap configured to prevent OOM failures. The instance is scheduled to start at 11:50pm UTC and stop at 12:10am UTC via EventBridge rules, keeping monthly compute costs minimal.

### Lambda

The FastAPI ingestion service runs as a container image on Lambda. Cold starts are acceptable since the iOS app has a 30-second timeout and buffers failed requests locally. At 20-80 plays per day, Lambda costs are effectively zero.

### Secrets

GCP service account credentials are stored in AWS Secrets Manager and retrieved at runtime by the BigQuery client. No credentials are stored in the repository or on disk.

---

## Notifications

The pipeline sends automated email notifications via AWS SES on every DAG run.

- **Success**: Fires when `test_dbt` completes, includes gold table row counts and run duration
- **Failure**: Fires on any task failure, includes the failed task name, error message, and Airflow log URL

Every run is also logged to `gold.dag_run_log` in BigQuery for run history and debugging.

---

## Setup

### Prerequisites

- AWS account with permissions for Lambda, S3, EC2, ECR, Secrets Manager, EventBridge
- GCP account with BigQuery enabled
- Apple Developer account (for iOS app sideloading)
- Xcode (for building and deploying the iOS app)
- Docker

### Environment Variables

See `.env.example` for all required variables. Key variables:

```
AWS_S3_BUCKET
AWS_S3_PLAYS_SOURCE
AWS_S3_GEOBUCKET_SOURCE
AWS_S3_OPENWEATHER_SOURCE
BIGQUERY_PROJECT
BIGQUERY_DATASET_BRONZE
BIGQUERY_DATASET_SILVER
BIGQUERY_DATASET_GOLD
GOOGLE_APPLICATION_CREDENTIALS
GCP_PRIVATE_KEY_ID
GCP_PRIVATE_KEY
GCP_CLIENT_EMAIL
GCP_CLIENT_ID
```

### Deploy Ingestion API

```bash
./deploy.sh
```

### Deploy Airflow

```bash
~/deploy_airflow.sh
```

### Run dbt

```bash
dbt seed
dbt run
dbt test
```

---

## Project Structure

```
MusicMetrics/
  app/
    config.py              <- environment variable loading
    models.py              <- shared Pydantic models
    gcp/
      ddl/                 <- BigQuery table definitions
      ingest/              <- BigQuery load functions
    utils/
      bq_io.py             <- BigQuery client + load utility
      s3_io.py             <- S3 read/write utilities
      secrets.py           <- AWS Secrets Manager client
      consolidate_geo_buckets.py <- spatial bucketing logic
  docker/
    airflow/
      Dockerfile
      requirements.txt
  gcp/
    ddl/                   <- BigQuery DDL scripts
    ingest/                <- BigQuery ingestion scripts
  ingestion/
    call_plays.py          <- reads plays from S3
    geo_buckets.py         <- buckets coordinates, writes to S3
    clients/
      openmeteo_client.py  <- fetches weather, writes to S3
  music_metrics_ios/       <- Xcode project (Swift)
  music_metrics_pipeline/
    airflow/
      dags/
        music_pipeline.py  <- main Airflow DAG
    dbt/
      music_metrics/
        models/
          staging/
          silver/
          marts/
        seeds/
          weather_codes.csv
  streamlit/
    app.py
    requirements.txt
  setup.py
  deploy.sh
```