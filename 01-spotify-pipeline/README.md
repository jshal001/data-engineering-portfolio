# Spotify Tracks Data Pipeline

## Business Question
Do audio features like energy, loudness, danceability, and tempo meaningfully separate
popular tracks from unpopular ones — or is popularity driven by factors beyond the music itself?

## Finding
When comparing the top 10% most popular tracks against the bottom 10%, differences in audio
features exist but are modest. Loudness and acousticness show the clearest separation. 
Popular tracks tend to be louder and less acoustic. However, effect sizes are small across
all features, suggesting audio characteristics are weak predictors of popularity on Spotify.

## Pipeline Architecture

```
Kaggle CSV
    ↓
Volume (raw file landing zone)
    ↓
Bronze — raw ingestion, all columns as STRING, no transformations
    ↓
Silver — typed, cleaned, index column dropped
    ↓
Gold — top/bottom 10% popularity groups compared across audio features
```

## Layer Details

- **Bronze** — raw Spotify tracks data loaded via COPY INTO with explicit STRING schema.
  No transformations applied. Preserves the source exactly as it arrived.
- **Silver** — columns cast to appropriate types (INT, DOUBLE, BOOLEAN). Index column
  dropped. All rows retained for downstream use.
- **Gold** — tracks split into high (p90+) and low (p10-) popularity groups using
  percentile thresholds derived from the data. Average audio features computed per group
  and compared to answer the business question.

## Tools Used

- **Databricks** — notebook execution and Delta Lake storage
- **Delta Lake** — ACID transactions, time travel, schema enforcement
- **PySpark** — bronze ingestion and DataFrame operations
- **Spark SQL** — silver transformations and gold aggregations
- **Unity Catalog** — catalog, schema, and volume management

## Dataset

Spotify Tracks dataset from Kaggle (~114k tracks across multiple genres).
Contains audio features including danceability, energy, loudness, tempo,
valence, acousticness, instrumentalness, and popularity scores.
