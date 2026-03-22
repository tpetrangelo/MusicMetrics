SELECT
    lat_bucket,
    lon_bucket,
    DATE(date)      AS date,
    hour,
    time,
    temperature_f,
    apparent_temp_f,
    precipitation_in,
    weather_code,
    wind_speed_mph
FROM {{ source('bronze', 'weather') }}