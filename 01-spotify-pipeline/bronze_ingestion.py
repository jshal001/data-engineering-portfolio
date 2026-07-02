"""
Ingests raw Spotify tracks CSV from a Unity Catalog volume into a
Bronze Delta table

Usage (Databricks):
    %run ./bronze_ingestion
    run_bronze_ingestion()

Usage (local / CI):
    python bronze_ingestion.py
"""

import logging
from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
CATALOG = "workspace"
SCHEMA  = "spotify_pipeline"
VOLUME  = "spotify_tracks"
TABLE   = "spotifytracks_bronze"

BRONZE_SCHEMA = """
    _c0              STRING,
    track_id         STRING,
    artists          STRING,
    album_name       STRING,
    track_name       STRING,
    popularity       STRING,
    duration_ms      STRING,
    explicit         STRING,
    danceability     STRING,
    energy           STRING,
    key              STRING,
    loudness         STRING,
    mode             STRING,
    speechiness      STRING,
    acousticness     STRING,
    instrumentalness STRING,
    liveness         STRING,
    valence          STRING,
    tempo            STRING,
    time_signature   STRING,
    track_genre      STRING
"""

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Functions
# ---------------------------------------------------------------------------

def get_spark() -> SparkSession:
    """Return the active SparkSession (or create one locally for testing)."""
    return SparkSession.builder.getOrCreate()


def create_schema_and_volume(spark: SparkSession, catalog: str, schema: str, volume: str) -> None:
    """Create the Unity Catalog schema and volume if they do not already exist."""
    log.info("Creating schema and volume if not exists...")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")
    log.info("Schema and volume ready.")


def drop_table(spark: SparkSession, catalog: str, schema: str, table: str) -> None:
    """Drop the bronze table so it can be recreated cleanly on each full refresh."""
    full_name = f"{catalog}.{schema}.{table}"
    log.info(f"Dropping table {full_name} if it exists...")
    spark.sql(f"DROP TABLE IF EXISTS {full_name}")
    log.info(f"Table {full_name} dropped.")


def create_bronze_table(spark: SparkSession, catalog: str, schema: str, table: str) -> None:
    """Create an empty Bronze Delta table with an explicit all-STRING schema."""
    full_name = f"{catalog}.{schema}.{table}"
    log.info(f"Creating bronze table {full_name}...")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_name}
        (
            {BRONZE_SCHEMA}
        )
    """)
    log.info(f"Bronze table {full_name} created.")


def copy_into_bronze(spark: SparkSession, catalog: str, schema: str, table: str, volume: str) -> int:
    """
    Load CSV files from the volume into the Bronze table using COPY INTO.

    Returns the number of rows inserted.
    """
    full_name   = f"{catalog}.{schema}.{table}"
    volume_path = f"/Volumes/{catalog}/{schema}/{volume}"

    log.info(f"Running COPY INTO {full_name} from {volume_path}...")
    result_df = spark.sql(f"""
        COPY INTO {full_name}
        FROM '{volume_path}'
        FILEFORMAT = CSV
        FORMAT_OPTIONS (
            'header' = 'true',
            'quote'  = '"',
            'escape' = '"'
        )
    """)

    rows_inserted = result_df.collect()[0]["num_inserted_rows"]
    log.info(f"COPY INTO complete — {rows_inserted:,} rows inserted into {full_name}.")
    return rows_inserted


def verify_bronze(spark: SparkSession, catalog: str, schema: str, table: str) -> int:
    """Return the total row count of the bronze table as a sanity check."""
    full_name = f"{catalog}.{schema}.{table}"
    count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {full_name}").collect()[0]["cnt"]
    log.info(f"Verification — {full_name} contains {count:,} rows.")
    return count


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_bronze_ingestion(
    catalog: str = CATALOG,
    schema:  str = SCHEMA,
    volume:  str = VOLUME,
    table:   str = TABLE,
) -> None:
    """
    End-to-end bronze ingestion:
      1. Create schema + volume (idempotent)
      2. Drop + recreate bronze table (full refresh)
      3. COPY INTO from volume
      4. Verify row count
    """
    spark = get_spark()

    create_schema_and_volume(spark, catalog, schema, volume)
    drop_table(spark, catalog, schema, table)
    create_bronze_table(spark, catalog, schema, table)
    rows = copy_into_bronze(spark, catalog, schema, table, volume)
    total = verify_bronze(spark, catalog, schema, table)

    if rows != total:
        msg = (
        f"Row count mismatch: COPY INTO inserted {rows:,} "
        f"but table contains {total:,}. "
        "Check for pre-existing data or duplicate loads."
        )
        log.error(msg)  
        raise ValueError(msg)

    log.info("Bronze ingestion complete and verified.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run_bronze_ingestion()