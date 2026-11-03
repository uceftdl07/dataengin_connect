"""Unit tests for Kafka batch reading and previous-day ingestion."""
from datetime import date
from unittest.mock import MagicMock, call, patch

import pytest

from src.connectors.kafka_batch_connector import KafkaBatchConnector
from src.ingestors.kafka_daily_ingestor import KafkaDailyIngestor


@pytest.mark.unit
class TestKafkaBatchConnector:
    def test_reads_topic_from_earliest_to_latest(self):
        spark = MagicMock()
        reader = MagicMock()
        reader.option.return_value = reader
        spark.read.format.return_value = reader
        connector = KafkaBatchConnector(
            spark,
            "broker:9093",
            {"kafka.security.protocol": "SASL_SSL"},
        )

        connector.read_topic("events", {"minPartitions": 4})

        assert reader.option.call_args_list == [
            call("kafka.bootstrap.servers", "broker:9093"),
            call("subscribe", "events"),
            call("startingOffsets", "earliest"),
            call("endingOffsets", "latest"),
            call("kafka.security.protocol", "SASL_SSL"),
            call("minPartitions", "4"),
        ]
        reader.load.assert_called_once_with()

    def test_rejects_offset_overrides(self):
        connector = KafkaBatchConnector(MagicMock(), "broker:9093")

        with pytest.raises(ValueError, match="startingOffsets"):
            connector.read_topic("events", {"startingOffsets": "latest"})


@pytest.mark.unit
class TestKafkaDailyIngestor:
    class _Column:
        def cast(self, *_):
            return self

        def alias(self, *_):
            return self

        def getField(self, *_):
            return self

        def isNull(self):
            return self

        def __eq__(self, _):
            return self

    @staticmethod
    def _topic_config():
        return {
            "topic": "events",
            "target_table": "events",
            "event_timestamp_field": "event_timestamp",
            "schema": {
                "type": "struct",
                "fields": [
                    {
                        "name": "event_id",
                        "type": "string",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "event_timestamp",
                        "type": "timestamp",
                        "nullable": True,
                        "metadata": {},
                    },
                ],
            },
        }

    def test_filters_by_event_date_and_uses_kafka_timestamp_fallback(self):
        spark = MagicMock()
        raw = MagicMock()
        parsed = MagicMock()
        parsed_filter = MagicMock()
        parsed_filter.count.return_value = 0
        parsed.filter.return_value = parsed_filter
        parsed.select.return_value = MagicMock()
        events = parsed.select.return_value
        events.withColumn.return_value = events
        events.drop.return_value = events
        undated_events = MagicMock()
        undated_events.count.return_value = 0
        daily_events = MagicMock()
        daily_events.persist.return_value = daily_events
        daily_events.count.return_value = 2
        events.filter.side_effect = [undated_events, daily_events]
        raw.select.return_value = parsed
        connector = MagicMock()
        connector.read_topic.return_value = raw
        writer = daily_events.write
        writer.format.return_value = writer
        writer.mode.return_value = writer
        writer.option.return_value = writer
        ingestor = KafkaDailyIngestor(spark, connector, "bronze", "raw", "UTC")

        with patch(
            "src.ingestors.kafka_daily_ingestor.F.col", return_value=self._Column()
        ), patch(
            "src.ingestors.kafka_daily_ingestor.F.from_json",
            return_value=self._Column(),
        ), patch(
            "src.ingestors.kafka_daily_ingestor.F.coalesce",
            return_value=self._Column(),
        ), patch(
            "src.ingestors.kafka_daily_ingestor.F.to_date",
            return_value=self._Column(),
        ), patch(
            "src.ingestors.kafka_daily_ingestor.F.lit",
            return_value=self._Column(),
        ):
            result = ingestor.ingest_topic(self._topic_config(), date(2026, 11, 2))

        assert result["rows_loaded"] == 2
        writer.option.assert_called_once_with(
            "replaceWhere",
            "to_date(`event_timestamp`) = DATE '2026-11-02'",
        )
        writer.saveAsTable.assert_called_once_with("bronze.raw.events")
        daily_events.unpersist.assert_called_once_with()
        parsed.unpersist.assert_called_once_with()

    def test_rejects_malformed_json(self):
        parsed = MagicMock()
        parsed_filter = MagicMock()
        parsed_filter.count.return_value = 1
        parsed.filter.return_value = parsed_filter
        raw = MagicMock()
        raw.select.return_value = parsed
        connector = MagicMock()
        connector.read_topic.return_value = raw
        ingestor = KafkaDailyIngestor(MagicMock(), connector, "bronze", "raw", "UTC")

        with patch(
            "src.ingestors.kafka_daily_ingestor.F.col", return_value=self._Column()
        ), patch(
            "src.ingestors.kafka_daily_ingestor.F.from_json",
            return_value=self._Column(),
        ):
            with pytest.raises(
                ValueError, match="do not match the configured JSON schema"
            ):
                ingestor.ingest_topic(self._topic_config(), date(2026, 11, 2))
        parsed.unpersist.assert_called_once_with()

    def test_requires_a_timestamp_field_in_schema(self):
        ingestor = KafkaDailyIngestor(MagicMock(), MagicMock(), "bronze", "raw", "UTC")
        config = self._topic_config()
        config["event_timestamp_field"] = "missing"

        with pytest.raises(ValueError, match="is not in the configured schema"):
            ingestor.ingest_topic(config, date(2026, 11, 2))

    def test_rejects_unsafe_target_identifiers(self):
        ingestor = KafkaDailyIngestor(MagicMock(), MagicMock(), "bronze", "raw", "UTC")

        with pytest.raises(ValueError, match="simple SQL identifiers"):
            ingestor._target_table_name("events; DROP TABLE users")
