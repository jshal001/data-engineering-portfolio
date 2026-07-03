WITH PopularityGroups AS (
        SELECT percentile_approx(popularity, 0.10) AS p10,
               percentile_approx(popularity, 0.90) AS p90
        FROM {{ ref('spotifytracks_silver') }} 
    ),
    Grouped AS (
        SELECT CASE WHEN popularity <= p.p10 THEN 'low'
        WHEN popularity >= p.p90 THEN 'high'
        ELSE 'mid'
        END AS popularity_group,
        s.danceability,
        s.energy,
        s.loudness,
        s.acousticness,
        s.valence,
        s.tempo
        FROM PopularityGroups p CROSS JOIN {{ ref('spotifytracks_silver') }}  AS s
        )
    SELECT 
        popularity_group,
        COUNT(*) AS track_count,
        AVG(danceability) AS avg_danceability,
        AVG(energy) AS avg_energy,
        AVG(loudness) AS avg_loudness,
        AVG(acousticness) AS avg_acousticness,
        AVG(valence) AS avg_valence,
        AVG(tempo) AS avg_tempo
    FROM Grouped
    WHERE popularity_group != 'mid'
    GROUP BY popularity_group