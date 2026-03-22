#!/bin/bash
set -e

AIRFLOW_DIR=~/airflow
REPO_DIR=~/MusicMetrics

echo "Pulling latest changes from GitHub..."
cd $REPO_DIR
git pull

echo "Restarting Airflow..."
cd $AIRFLOW_DIR
docker-compose down
docker-compose up -d

echo "Done! Airflow is updated and running."