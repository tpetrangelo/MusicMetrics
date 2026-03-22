SELECT
    lat_bucket,
    lon_bucket,
    COUNT(*)                                        AS play_count,
    ROUND(SUM(duration_ms) / 60000.0, 2)           AS minutes_listened,
    COUNT(DISTINCT artist_name)                     AS unique_artists,
    APPROX_TOP_COUNT(artist_name, 1)[OFFSET(0)].value       AS top_artist,
    ROUND(AVG(temperature_f), 1)                   AS avg_temp_f
FROM {{ ref('silver_plays_enriched') }}
WHERE lat_bucket IS NOT NULL
GROUP BY 1, 2
ORDER BY 3 DESC