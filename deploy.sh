#!/bin/bash

set -e  # stop on any error

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION="us-east-1"
IMAGE_NAME="music-metrics-ingest"
ECR_URI="$ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$IMAGE_NAME"

echo "🔨 Building image..."
docker build -t $IMAGE_NAME -f music_metrics_pipeline/fastapi/Dockerfile .

echo "🔐 Logging into ECR..."
docker login --username AWS --password-stdin $ECR_URI <<< $(aws ecr get-login-password --region $REGION)

echo "🏷️ Tagging image..."
docker tag $IMAGE_NAME:latest $ECR_URI:latest

echo "🚀 Pushing to ECR..."
docker push $ECR_URI:latest

echo "⚡ Updating Lambda..."
aws lambda update-function-code \
  --function-name $IMAGE_NAME \
  --image-uri $ECR_URI:latest \
  --region $REGION \
  --output text > /dev/null

echo "✅ Done! Lambda is updated."