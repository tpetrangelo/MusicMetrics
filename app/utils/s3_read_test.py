import boto3
import json

s3 = boto3.client("s3")
# strip s3:// and split into bucket/key
obj = s3.get_object(Bucket="music-metrics", Key="processed/geobuckets/dt=2026-03-21.json")
print(json.loads(obj["Body"].read()))