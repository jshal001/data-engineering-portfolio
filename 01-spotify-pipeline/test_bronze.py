"""
test_bronze.py

Unit tests for bronze_ingestion.py
Each test mocks SparkSession so no real cluster is needed.
"""

import pytest
from unittest.mock import MagicMock, call
from pyspark.sql import Row
from bronze_ingestion import (
    verify_bronze,
    copy_into_bronze,
    drop_table,
    create_bronze_table,
    create_schema_and_volume,
    run_bronze_ingestion,
)


def test_create_bronze_table():
    """create_bronze_table should create an empty bronze table with the correct schema -- does not return anything"""
    mock_spark = MagicMock()
    create_bronze_table(mock_spark, "workspace", "spotify_pipeline", "spotifytracks_bronze")
    call_args = mock_spark.sql.call_args[0][0]

    assert "CREATE TABLE IF NOT EXISTS workspace.spotify_pipeline.spotifytracks_bronze" in call_args
    assert "track_id" in call_args
    assert "track_genre" in call_args 



# ---------------------------------------------------------------------------
# verify_bronze
# ---------------------------------------------------------------------------

def test_verify_bronze_returns_correct_count():
    """verify_bronze() should extract cnt from the query result and return it."""
    # Arrange
    mock_spark = MagicMock()
    fake_row = Row(cnt=114000)
    mock_spark.sql.return_value.collect.return_value = [fake_row]

    # Act
    result = verify_bronze(mock_spark, "workspace", "spotify_pipeline", "spotifytracks_bronze")

    # Assert
    assert result == 114000


def test_verify_bronze_queries_correct_table():
    """verify_bronze() should query the fully qualified table name."""
    # Arrange
    mock_spark = MagicMock()
    fake_row = Row(cnt=10)
    mock_spark.sql.return_value.collect.return_value = [fake_row]

    # Act
    verify_bronze(mock_spark, "workspace", "spotify_pipeline", "spotifytracks_bronze")

    # Assert
    mock_spark.sql.assert_called_once_with(
        "SELECT COUNT(*) AS cnt FROM workspace.spotify_pipeline.spotifytracks_bronze"
    )


# ---------------------------------------------------------------------------
# copy_into_bronze
# ---------------------------------------------------------------------------

def test_copy_into_bronze_returns_rows_inserted():
    """copy_into_bronze() should extract num_inserted_rows and return it."""
    # Arrange
    mock_spark = MagicMock()
    fake_row = Row(num_inserted_rows=114000)
    mock_spark.sql.return_value.collect.return_value = [fake_row]

    # Act
    result = copy_into_bronze(mock_spark, "workspace", "spotify_pipeline", "spotifytracks_bronze", "spotify_tracks")

    # Assert
    assert result == 114000


# ---------------------------------------------------------------------------
# drop_table
# ---------------------------------------------------------------------------

def test_drop_table_executes_correct_sql():
    """drop_table() should call DROP TABLE IF EXISTS with the correct table name."""
    # Arrange
    mock_spark = MagicMock()

    # Act
    drop_table(mock_spark, "workspace", "spotify_pipeline", "spotifytracks_bronze")

    # Assert
    mock_spark.sql.assert_called_once_with(
        "DROP TABLE IF EXISTS workspace.spotify_pipeline.spotifytracks_bronze"
    )


# ---------------------------------------------------------------------------
# create_schema_and_volume
# ---------------------------------------------------------------------------

def test_create_schema_and_volume_executes_correct_sql():
    """create_schema_and_volume() should call CREATE SCHEMA and CREATE VOLUME."""
    # Arrange
    mock_spark = MagicMock()

    # Act
    create_schema_and_volume(mock_spark, "workspace", "spotify_pipeline", "spotify_tracks")

    # Assert
    assert mock_spark.sql.call_count == 2
    mock_spark.sql.assert_any_call("CREATE SCHEMA IF NOT EXISTS workspace.spotify_pipeline")
    mock_spark.sql.assert_any_call("CREATE VOLUME IF NOT EXISTS workspace.spotify_pipeline.spotify_tracks")


# ---------------------------------------------------------------------------
# run_bronze_ingestion — orchestrator
# ---------------------------------------------------------------------------

def test_run_bronze_ingestion_raises_on_row_mismatch(mocker):
    """run_bronze_ingestion() should raise ValueError when rows inserted != table count."""
    # Arrange
    mocker.patch("bronze_ingestion.get_spark", return_value=MagicMock())
    mocker.patch("bronze_ingestion.create_schema_and_volume")
    mocker.patch("bronze_ingestion.drop_table")
    mocker.patch("bronze_ingestion.create_bronze_table")
    mocker.patch("bronze_ingestion.copy_into_bronze", return_value=114000)
    mocker.patch("bronze_ingestion.verify_bronze", return_value=50000)  # mismatch

    # Act + Assert
    with pytest.raises(ValueError, match="Row count mismatch"):
        run_bronze_ingestion()


def test_run_bronze_ingestion_succeeds_when_counts_match(mocker):
    """run_bronze_ingestion() should complete without error when counts match."""
    # Arrange
    mocker.patch("bronze_ingestion.get_spark", return_value=MagicMock())
    mocker.patch("bronze_ingestion.create_schema_and_volume")
    mocker.patch("bronze_ingestion.drop_table")
    mocker.patch("bronze_ingestion.create_bronze_table")
    mocker.patch("bronze_ingestion.copy_into_bronze", return_value=114000)
    mocker.patch("bronze_ingestion.verify_bronze", return_value=114000)  # match

    # Act + Assert — should not raise
    run_bronze_ingestion()