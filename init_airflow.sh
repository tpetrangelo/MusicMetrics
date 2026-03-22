#!/bin/bash
set -e

AIRFLOW_DIR=~/airflow

echo "Initializing Airflow database..."
cd $AIRFLOW_DIR
docker-compose down
docker-compose up airflow-init
docker-compose up -d

echo "Airflow initialized and running."