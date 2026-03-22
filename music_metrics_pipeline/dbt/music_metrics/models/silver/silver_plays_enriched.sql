SELECT
    p.id,
    p.track_id,
    p.track_name,
    p.artist_name,
    p.album_name,
    p.duration_ms,
    p.played_at,
    p.release_date,
    p.latitude,
    p.longitude,
    p.source,
    p.lat_bucket,
    p.lon_bucket,
    p.hour,

    -- Weather
    w.temperature_f,
    w.apparent_temp_f,
    w.precipitation_in,
    w.wind_speed_mph,
    w.weather_code,

    -- Weather description from seed
    wc.description      AS weather_description,
    wc.category         AS weather_category,
    wc.severity         AS weather_severity,

    -- Time dimensions
    DATE(p.played_at)                                   AS play_date,
    EXTRACT(DAYOFWEEK FROM p.played_at)                 AS day_of_week,
    EXTRACT(MONTH FROM p.played_at)                     AS month,
    CASE
        WHEN EXTRACT(HOUR FROM p.played_at) BETWEEN 5  AND 11 THEN 'morning'
        WHEN EXTRACT(HOUR FROM p.played_at) BETWEEN 12 AND 16 THEN 'afternoon'
        WHEN EXTRACT(HOUR FROM p.played_at) BETWEEN 17 AND 20 THEN 'evening'
        ELSE 'night'
    END                                                 AS time_of_day,

    -- Days since release
    DATE_DIFF(DATE(p.played_at), p.release_date, DAY)  AS days_since_release

FROM {{ ref('stg_plays') }} p
LEFT JOIN {{ ref('stg_weather') }} w
    ON  p.lat_bucket = w.lat_bucket
    AND p.lon_bucket = w.lon_bucket
    AND p.hour       = w.hour
    AND DATE(p.played_at) = w.date
LEFT JOIN {{ ref('weather_codes') }} wc
    ON w.weather_code = wc.weather_code