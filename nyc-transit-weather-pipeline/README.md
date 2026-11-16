# NYC Transit Delay vs Weather Pipeline

Does bad weather slow down the NYC subway, and by how much?

A medallion pipeline on Databricks that measures how long trains take on fixed stretches of track compared with their usual time, and joins every measurement to the weather at that hour.

**Status:** in progress

## Approach

Routes don't always run the same path, so whole trips aren't comparable. Instead:

1. Measure how long each train takes across **5 consecutive stops**.
2. Compare it with the **median** for that segment, hour, day type, and season.
3. The difference is **excess**. 2+ minutes counts as delayed.
4. Join the **weather** for that hour.

## Planned architecture

```
MTA GTFS-realtime (poller) ──┐
subwaydata.nyc archive ──────┼──> UC Volume ──> Bronze ──> Silver ──> Gold ──> Postgres
NOAA weather (Central Park) ─┘
```

- **Bronze:** raw files, append-only
- **Silver:** one clean shape for both subway sources, plus hourly weather
- **Gold:** one row per train per 5-stop segment, with excess and weather
- **Serving:** Postgres, loaded via JDBC staging table and upsert
- **Orchestration:** Airflow triggering Databricks Jobs

## Data sources

| Source | Role |
|---|---|
| MTA GTFS-realtime | Live train positions, every ~30s, from the poller start date forward |
| [subwaydata.nyc](https://subwaydata.nyc) | Daily archive of the same feeds: 12-month backfill |
| NOAA historical hourly, Central Park | Weather for every hour |
