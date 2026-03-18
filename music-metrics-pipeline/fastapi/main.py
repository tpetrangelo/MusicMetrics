from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel,Field,field_validator
from datetime import date, datetime
from typing import Annotated, Optional
from dataclasses import dataclass

import logging
import boto3

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

Milliseconds = Annotated[int, Field(ge=0)]


class Item(BaseModel):
    artist_name: str
    track_name: str
    album_name: str
    duration_ms: Milliseconds
    track_id: str
    id: str
    release_date: Optional[date] = None

    @field_validator("release_date", mode="before")
    def parse_release_date(cls,v):
        if v is None:
            return v
        if isinstance(v,int): #year
            return date(v,1,1)
        if isinstance(v,str) and len(v) == 4: # "YYYY"
            return date(int(v),1,1)
        return v

    played_at: datetime
    latitude: Optional[float] = Field(ge=-90, le=90)
    longitude: Optional[float] = Field(ge=-180, le=180)
    

@dataclass(frozen=True)
class S3Location:
    bucket: str
    key:str

def _s3_client():
    return boto3.client('s3')

app = FastAPI()


def write_to_s3(item: Item, source: str):
    s3 = _s3_client
    s3.put_object

@app.post("/plays/ios")
async def create_entry(item:Item, background_tasks: BackgroundTasks):
    background_tasks.add_task(write_to_s3, item, source = "ios")
    logger.info(f"Received play: {item}")
    return {"status": "ok"}


@app.post("/plays/lastfm")
async def create_entry(item:Item, background_tasks: BackgroundTasks):
    background_tasks.add_task(write_to_s3, item, source = "lastfm")
    logger.info(f"Received play: {item}")
    return {"status": "ok"}