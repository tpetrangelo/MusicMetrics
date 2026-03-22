#!/bin/bash
set -e

AIRFLOW_DIR=~/airflow
REPO_DIR=~/MusicMetrics

echo "Pulling latest changes from GitHub..."
cd $REPO_DIR
git fetch origin
git reset --hard origin/main

echo "Copying DAG to Airflow..."
cp $REPO_DIR/music_metrics_pipeline/airflow/dags/music_pipeline.py $AIRFLOW_DIR/dags/

echo "Installing dependencies..."
pip install -r $REPO_DIR/docker/airflow/requirements.txt

echo "Running dbt seed..."
docker exec airflow_airflow-scheduler_1 /bin/bash -c "/home/airflow/.local/bin/dbt seed --project-dir /opt/airflow/MusicMetrics/music_metrics_pipeline/dbt/music_metrics --profiles-dir /opt/airflow/MusicMetrics/music_metrics_pipeline/dbt/music_metrics 2>&1"

echo "Restarting Airflow..."
cd $AIRFLOW_DIR
docker-compose down
docker-compose up -d

echo "Done"