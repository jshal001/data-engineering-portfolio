# Design: NYC Transit Delay vs Weather Pipeline

## Question

Do trains move slower when the weather is bad, and by how much?

## Sources

| Source | Role |
|---|---|
| MTA GTFS-realtime (8 feeds, protobuf, ~30s) | Live subway data, via a poller |
| [subwaydata.nyc](https://subwaydata.nyc) daily archive | 12-month subway backfill |
| api.weather.gov, Central Park (KNYC) | Preliminary hourly weather |
| NOAA NCEI historical hourly, Central Park | Final, quality-controlled weather; backfill and corrections |

Each service date uses one subway source: the archive before the poller's first full day, the poller after.

## Architecture

```
MTA poller ───────┐
Archive ──────────┼──> landing Volume ──> Bronze ──> Silver ──> Gold ──> Postgres (Lakebase)
NOAA / weather.gov┘
```

- Airflow triggers Databricks Jobs.
- The archive and weather downloaders run as Databricks jobs. The poller runs on a separate always-on host, since polling every 30 seconds as a Databricks job would run continuously.
- Every stage reads and writes only the landing Volume or Unity Catalog tables, with catalog and schema passed as parameters, so any stage can change where or how it runs without affecting the others.

## Environments

```
nyc_transit_prod
├── landing
│   ├── files            downloaded files, unmodified (the only copy)
│   └── checkpoints      Auto Loader state
├── bronze
├── silver
└── gold

nyc_transit_dev
├── landing
│   ├── files            test runs of ingestion code only
│   └── checkpoints      one folder per owner, separate from prod's
├── <developer>_*        each developer's own schemas
└── pr_<number>_*        one set per open pull request
```

- Only deployed jobs write to prod, as a service principal. Developers have read-only access to prod.
- Dev reads prod's landing files; it never shares prod's checkpoints.

## Bronze

Append-only, unaltered copies of the landed files, queryable as Delta tables. The archive downloader extracts each daily `.tar.xz` and lands its CSVs gzipped, which Auto Loader reads directly.

| Table | Loaded with | Schema changes |
|---|---|---|
| `mta_snapshots` | Auto Loader (`binaryFile`) | Fixed |
| `archive_trips`, `archive_stop_times` | Auto Loader, from gzipped CSVs, all strings | `rescue`: logged in backfill, halts daily runs |
| `weather_final` | COPY INTO | Fixed |
| `weather_preliminary` | Auto Loader | `rescue`, halts |

## Silver

Runs once per service day after it ends; each run replaces that day's partition. The 12-month backfill runs one month at a time (~7M stop rows per month).

- **`trips`:** one row per `trip_uid`, with an `is_rerouted` flag.
- **`stop_times`:** one row per trip per stop. `last_observed_ts` and `marked_past_ts` bracket when the train passed; their gap measures timing precision.
- **Poller path (optional):** collapses snapshots into the archive's shape.

### Weather: incremental MERGE

`weather_hourly` (key: station + UTC hour) is loaded with `MERGE INTO`. Recent hours arrive as preliminary values and are corrected when NOAA publishes final values.

1. Deduplicate the batch deterministically (final over preliminary, routine report over specials, latest observation).
2. Insert new hours; update only final-over-preliminary, or same quality and newer. Preliminary never overwrites final.
3. Log inserted and updated counts.

## Gold

**`segment_traversals`:** one row per train per 5-stop segment. All traversals are kept, so delay rates can be computed.

- Segments, not trips: routes vary (branches, express vs local, reroutes), but the track between stations doesn't.
- Segment time from observed timestamps, not predictions.
- Baseline: median (`percentile_approx`) per segment, hour, day type, and season, from non-rerouted trips. Computed one season at a time after the backfill (~74M traversals per year).
- **Excess** over baseline is the main metric; **delayed** = 2+ minutes.
- Joined to the weather for that hour, with `weather_status` (preliminary or final).

## Data quality

Each run logs row counts, null and duplicate rates, schema changes, and MERGE counts.

| Check | On failure |
|---|---|
| Duplicate or null keys | Halt |
| Unexpected schema change | Halt (logged during backfill) |
| Imprecise timing | Quarantine row |
| Unusual row counts, missing weather | Warn |

Thresholds are derived from observed distributions.

## CI/CD

- Protected `main`; per-project required checks.
- **CI:** pytest, then dbt slim CI (changed models only) into per-PR schemas in dev, deferring to prod. PR schemas dropped on close.
- **CD:** Asset Bundle deploy and `dbt build` to prod.

## Serving

Gold is written over JDBC from Databricks into a staging table in Lakebase (Databricks-managed Postgres), then upserted into the target in one transaction.

## Constraints and fallbacks

| Item | Here | In production / fallback |
|---|---|---|
| Compute | Serverless only (Free Edition) | Job clusters for scheduled work; Photon only where measured to help |
| Usage quota | Daily compute cap: incremental daily runs, monthly backfill chunks, per-season baseline | Not a constraint on a paid workspace |
| Poller host | Laptop, uploading to the Volume | Always-on container service |
| Weather MERGE | Preliminary + final sources | Final only, if it runs over its timebox |

## Limitations

- One weather station.
- Disrupted trains drop out of segments, which can understate the effect.
- Measures "slower than usual," not lateness against the timetable.
