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

echo "Rebuilding Docker image..."
cd $AIRFLOW_DIR
docker-compose down
docker-compose build --no-cache

echo "Initializing Airflow..."
docker-compose up airflow-init

echo "Starting Airflow..."
docker-compose up -d

echo "Done"