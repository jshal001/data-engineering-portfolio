SELECT
    track_id,
    artists,
    album_name,
    track_name,
    track_genre,
    CAST(popularity AS INT) AS popularity,
    CAST(duration_ms AS INT) AS duration_ms,
    CASE 
    WHEN LOWER(explicit) = 'true' THEN TRUE
    WHEN LOWER(explicit) = 'false' THEN FALSE
    ELSE NULL
    END AS explicit,
    CAST(danceability AS DOUBLE) AS danceability,
    CAST(energy AS DOUBLE) AS energy,
    CAST(key AS INT) AS key,
    CAST(loudness AS DOUBLE) AS loudness,
    CAST(mode AS INT) AS mode,
    CAST(speechiness AS DOUBLE) AS speechiness,
    CAST(acousticness AS DOUBLE) AS acousticness,
    CAST(instrumentalness AS DOUBLE) AS instrumentalness,
    CAST(liveness AS DOUBLE) AS liveness,
    CAST(valence AS DOUBLE) AS valence,
    CAST(tempo AS DOUBLE) AS tempo,
    CAST(time_signature AS INT) AS time_signature
FROM {{ source('spotify_pipeline','spotifytracks_bronze') }}