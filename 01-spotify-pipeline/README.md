# Spotify Tracks Data Pipeline

## Business Question
Which music genres have the highest energy and danceability scores, 
and how has this changed across decades?

## Pipeline Layers
- **Bronze** — raw Spotify tracks data loaded as-is into a Delta table
- **Silver** — cleaned, typed, and enriched data with nulls removed 
  and decade column added
- **Gold** — aggregated metrics by genre and decade answering 
  the business question

## Tools Used
- Databricks — notebook execution and Delta Lake storage
- Lakeflow Jobs — pipeline orchestration
- Delta Lake — ACID transactions, time travel, MERGE INTO
- Python — ingestion and transformation logic
- SQL — Gold layer aggregations
