"""Unit tests for MongoDB Spark connector behavior."""
import json
from unittest.mock import MagicMock, call, patch

import pytest

from src.connectors.mongodb_connector import MongoDBConnector
from src.ingestors.generic_ingestor import GenericIngestor


@pytest.mark.unit
class TestMongoDBConnector:
    def _connector(self):
        spark = MagicMock()
        reader = MagicMock()
        reader.option.return_value = reader
        dataframe = MagicMock()
        reader.load.return_value = dataframe
        spark.read.format.return_value = reader
        connector = MongoDBConnector(
            spark=spark,
            connection_uri="mongodb://secret-uri",
            database="app",
            default_collection="events",
        )
        return connector, reader, dataframe

    def test_reads_configured_database_and_collection(self):
        connector, reader, dataframe = self._connector()

        with patch("src.connectors.mongodb_connector.F.col") as spark_col:
            result = connector.read_table("logical_events", columns=["event_id"])

        assert result is dataframe.select.return_value
        spark_col.assert_called_once_with("event_id")
        assert reader.option.call_args_list[:3] == [
            call("connection.uri", "mongodb://secret-uri"),
            call("database", "app"),
            call("collection", "logical_events"),
        ]

    def test_uses_collection_override_and_aggregation_pipeline(self):
        connector, reader, _ = self._connector()
        pipeline = [{"$project": {"event_id": 1}}]

        connector.read_table(
            "events",
            read_options={"aggregation_pipeline": pipeline, "sampleSize": 100},
            filter_query='{"status": "active"}',
        )

        assert (
            "aggregation.pipeline",
            json.dumps([{"$match": {"status": "active"}}, *pipeline]),
        ) in [call.args for call in reader.option.call_args_list]
        assert ("sampleSize", "100") in [
            call.args for call in reader.option.call_args_list
        ]

    def test_rejects_non_json_filter(self):
        connector, _, _ = self._connector()

        with pytest.raises(ValueError, match="must be a JSON object"):
            connector.read_table("events", filter_query="status = 'active'")

    def test_incremental_load_applies_watermark_filter(self):
        connector, _, dataframe = self._connector()

        with patch("src.connectors.mongodb_connector.F.col") as spark_col, patch(
            "src.connectors.mongodb_connector.F.lit"
        ) as spark_lit:
            predicate = object()
            spark_col.return_value.__gt__.return_value = predicate
            result = connector.read_incremental(
                "events", "updated_at", "2024-01-01T00:00:00Z"
            )

        assert result is dataframe.filter.return_value
        dataframe.filter.assert_called_once_with(predicate)
        spark_col.assert_called_once_with("updated_at")
        spark_lit.assert_called_once_with("2024-01-01T00:00:00Z")

    def test_missing_collection_is_reported(self):
        connector = MongoDBConnector(
            spark=MagicMock(),
            connection_uri="mongodb://secret-uri",
            database="app",
        )

        with pytest.raises(ValueError, match="collection must be configured"):
            connector.test_connection()


@pytest.mark.unit
class TestMongoDBIngestionConfig:
    def test_ingestor_uses_configured_collection(self):
        connector = MagicMock()
        ingestor = GenericIngestor(
            spark=MagicMock(),
            connector=connector,
            catalog_name="bronze",
            bronze_schema="raw",
            adls_base_path="abfss://bronze@storage",
        )
        table_config = {
            "table_name": "event_records",
            "collection": "events",
            "target_table": "events",
            "load_mode": "full",
            "read_options": {"sampleSize": 100},
        }

        ingestor._load_full(table_config)

        connector.read_table.assert_called_once_with(
            "events", {"sampleSize": 100}, None, None
        )
