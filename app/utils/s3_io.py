from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional
from app.models import Item

import json
import boto3

@dataclass(frozen=True)
class S3Location:
    bucket: str
    key: str


def _s3_client():
    # Centralize this so you can later add config (region, retries, etc.)
    return boto3.client("s3")

def upload_parquet_to_s3(
    *,
    bucket: str,
    key: str,
    data: Item,
    content_type: str = "application/json",
    metadata: Optional[dict[str, str]] = None,
) -> None:

    meta = {"ingest_ts_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
    if metadata:
        meta.update({str(k): str(v) for k, v in metadata.items()})

    s3 = _s3_client()
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data.model_dump(), default=str),
        ContentType=content_type,
        Metadata=meta,
    )