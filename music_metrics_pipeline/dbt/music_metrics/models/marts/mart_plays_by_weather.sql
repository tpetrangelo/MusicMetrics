SELECT
    weather_category,
    weather_description,
    weather_severity,
    ROUND(temperature_f / 10) * 10                  AS temp_bucket_f,
    COUNT(*)                                        AS play_count,
    ROUND(SUM(duration_ms) / 60000.0, 2)           AS minutes_listened,
    COUNT(DISTINCT artist_name)                     AS unique_artists
FROM {{ ref('silver_plays_enriched') }}
WHERE weather_category IS NOT NULL
GROUP BY 1, 2, 3, 4
ORDER BY 5 DESC