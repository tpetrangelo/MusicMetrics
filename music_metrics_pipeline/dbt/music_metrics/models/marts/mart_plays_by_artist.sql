SELECT
    artist_name,
    COUNT(*)                                        AS total_plays,
    COUNT(DISTINCT track_id)                        AS unique_tracks,
    ROUND(SUM(duration_ms) / 60000.0, 2)           AS total_minutes,
    ROUND(AVG(temperature_f), 1)                   AS avg_temp_f,
    APPROX_TOP_COUNT(weather_category, 1)[OFFSET(0)].value  AS most_common_weather,
    APPROX_TOP_COUNT(time_of_day, 1)[OFFSET(0)].value       AS most_common_time_of_day,
    MIN(played_at)                                  AS first_played,
    MAX(played_at)                                  AS last_played
FROM {{ ref('silver_plays_enriched') }}
GROUP BY 1
ORDER BY 2 DESC