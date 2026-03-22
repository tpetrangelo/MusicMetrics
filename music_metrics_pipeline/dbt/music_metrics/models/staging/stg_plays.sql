SELECT
    id,
    track_id,
    track_name,
    artist_name,
    album_name,
    duration_ms,
    TIMESTAMP(played_at) AS played_at,
    DATE(release_date)   AS release_date,
    latitude,
    longitude,
    source,
    ROUND(latitude, 1)   AS lat_bucket,
    ROUND(longitude, 1)  AS lon_bucket,
    EXTRACT(HOUR FROM played_at) AS hour
FROM {{ source('bronze', 'plays') }}