from __future__ import annotations

from dataclasses import dataclass

import json
import boto3
import pandas as pd

from app.config import AWS_S3_BUCKET

BUCKET = AWS_S3_BUCKET

@dataclass(frozen=True)
class S3Location:
    bucket: str
    key: str


def write_json_to_s3(data: list[dict], s3_path: str) -> None:
    key = strip_s3_uri(s3_path, BUCKET)
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(data, default=str),
        ContentType="application/json"
    )

def read_json_from_s3_prefix(s3_path: str) -> pd.DataFrame:
    key = strip_s3_uri(s3_path, BUCKET)
    s3 = boto3.client("s3")
    
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=key)
    
    if "Contents" not in response:
        raise ValueError(f"No files found at {s3_path}")
    
    all_records = []
    for obj in response["Contents"]:
        file = s3.get_object(Bucket=BUCKET, Key=obj["Key"])
        data = json.loads(file["Body"].read())
        if isinstance(data, list):
            all_records.extend(data)
        else:
            all_records.append(data)
    
    return pd.DataFrame(all_records)


def read_json_from_s3_file(s3_path: str) -> pd.DataFrame:
    key = strip_s3_uri(s3_path, BUCKET)
    s3 = boto3.client("s3")
    file = s3.get_object(Bucket=BUCKET, Key=key)
    body = file["Body"].read()
    data = json.loads(body)
    return pd.DataFrame(data)

def strip_s3_uri(s3_path: str, bucket: str) -> str:

    return s3_path.replace(f"s3://{bucket}/", "")