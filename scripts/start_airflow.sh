#!/bin/bash
cd ~/airflow
docker-compose up -d
echo "Waiting for containers to initialize..."
sleep 30
docker-compose stop airflow-webserver
echo "Webserver stopped, scheduler running"