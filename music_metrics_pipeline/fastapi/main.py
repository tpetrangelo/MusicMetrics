from fastapi import FastAPI, BackgroundTasks
from datetime import datetime, UTC, timezone


from app.config import AWS_S3_BUCKET, AWS_S3_PLAYS_SOURCE
from app.utils.s3_keys import build_raw_key
from app.utils.s3_io import upload_parquet_to_s3  
from app.models import Item

from mangum import Mangum # type: ignore

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BUCKET = AWS_S3_BUCKET
AWS_SOURCE = AWS_S3_PLAYS_SOURCE

    
app = FastAPI()
handler = Mangum(app)

def write_to_s3(item: Item, source: str):

    ts = datetime.now(timezone.utc)
    plays_key = build_raw_key(AWS_SOURCE, ts, ext="json", play_source = source)

    upload_parquet_to_s3(
        bucket=BUCKET,
        key=plays_key,
        data=item,
        metadata={"source": AWS_SOURCE},
    )



@app.post("/plays/ios")
async def create_entry_ios(item:Item, background_tasks: BackgroundTasks):
    background_tasks.add_task(write_to_s3, item, source = "ios")
    logger.info(f"Received play: {item}")
    return {"status": "ok"}


@app.post("/plays/lastfm")
async def create_entry_lastfm(item:Item, background_tasks: BackgroundTasks):
    background_tasks.add_task(write_to_s3, item, source = "lastfm")
    logger.info(f"Received play: {item}")
    return {"status": "ok"}