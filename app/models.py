from pydantic import BaseModel, Field, field_validator
from datetime import date, datetime
from typing import Annotated, Optional

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