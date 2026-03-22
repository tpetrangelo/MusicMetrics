SELECT
    artist_name,
    album_name,
    release_date,
    MIN(days_since_release)                         AS days_until_first_play,
    COUNT(*)                                        AS total_plays,
    ROUND(SUM(duration_ms) / 60000.0, 2)           AS total_minutes,
    MAX(days_since_release)                         AS days_in_rotation
FROM {{ ref('silver_plays_enriched') }}
WHERE release_date IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY total_plays DESC