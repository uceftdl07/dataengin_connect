"""Daily, idempotent Kafka-to-Delta batch ingestion."""
import logging
import re
from datetime import date
from typing import Any, Dict

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, TimestampType


class KafkaDailyIngestor:
    """Parse JSON Kafka messages and replace one event-date slice in Delta."""

    _IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

    def __init__(
        self,
        spark: SparkSession,
        connector,
        catalog_name: str,
        bronze_schema: str,
        timezone: str,
    ):
        self.spark = spark
        self.connector = connector
        self.catalog_name = catalog_name
        self.bronze_schema = bronze_schema
        self.timezone = timezone
        self.spark.conf.set("spark.sql.session.timeZone", timezone)
        self.logger = logging.getLogger(self.__class__.__name__)

    def ingest_topic(
        self, topic_config: Dict[str, Any], run_date: date
    ) -> Dict[str, Any]:
        topic = topic_config["topic"]
        target_table = topic_config["target_table"]
        timestamp_field = topic_config["event_timestamp_field"]
        schema = self._parse_schema(topic_config["schema"])

        if timestamp_field not in schema.fieldNames():
            raise ValueError(
                f"event_timestamp_field '{timestamp_field}' is not in the "
                f"configured schema for topic '{topic}'"
            )
        if not self._IDENTIFIER.fullmatch(timestamp_field):
            raise ValueError("event_timestamp_field must be a simple field name")
        if not isinstance(schema[timestamp_field].dataType, TimestampType):
            raise ValueError(
                f"Schema field '{timestamp_field}' must have type 'timestamp'"
            )

        full_table_name = self._target_table_name(target_table)
        raw = self.connector.read_topic(topic, topic_config.get("source_options"))
        parsed = raw.select(
            F.from_json(F.col("value").cast("string"), schema).alias("_payload"),
            F.col("timestamp").alias("_kafka_timestamp"),
        )
        parsed.persist()
        daily_events = None
        try:
            malformed_count = parsed.filter(F.col("_payload").isNull()).count()
            if malformed_count:
                raise ValueError(
                    f"Topic '{topic}' contains {malformed_count} message(s) "
                    "that do not match the configured JSON schema"
                )

            payload_columns = [
                F.col("_payload").getField(field_name).alias(field_name)
                for field_name in schema.fieldNames()
            ]
            events = parsed.select(*payload_columns, "_kafka_timestamp")
            events = events.withColumn(
                timestamp_field,
                F.coalesce(
                    F.col(timestamp_field),
                    F.col("_kafka_timestamp").cast("timestamp"),
                ),
            ).drop("_kafka_timestamp")

            undated_count = events.filter(F.col(timestamp_field).isNull()).count()
            if undated_count:
                raise ValueError(
                    f"Topic '{topic}' contains {undated_count} message(s) without "
                    f"a usable '{timestamp_field}' or Kafka timestamp"
                )

            daily_events = events.filter(
                F.to_date(F.col(timestamp_field))
                == F.lit(run_date.isoformat()).cast("date")
            ).persist()
            rows_loaded = daily_events.count()
            replace_predicate = (
                f"to_date(`{timestamp_field}`) = DATE '{run_date.isoformat()}'"
            )

            # Delta replaceWhere atomically replaces just this date, including on retries.
            (
                daily_events.write.format("delta")
                .mode("overwrite")
                .option("replaceWhere", replace_predicate)
                .saveAsTable(full_table_name)
            )
        finally:
            if daily_events is not None:
                daily_events.unpersist()
            parsed.unpersist()

        self.logger.info(
            "Ingested %s rows from topic %s for %s into %s",
            rows_loaded,
            topic,
            run_date,
            full_table_name,
        )
        return {
            "table_name": topic,
            "target_table": full_table_name,
            "load_mode": "daily_replace",
            "rows_loaded": rows_loaded,
            "status": "success",
        }

    @staticmethod
    def _parse_schema(schema_config: Dict[str, Any]) -> StructType:
        if not isinstance(schema_config, dict) or schema_config.get("type") != "struct":
            raise ValueError("Kafka topic 'schema' must be a Spark struct schema")
        try:
            return StructType.fromJson(schema_config)
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"Invalid Kafka JSON schema: {exc}") from exc

    def _target_table_name(self, target_table: str) -> str:
        if not isinstance(target_table, str):
            raise ValueError("Target table must be a simple SQL identifier")
        parts = [self.catalog_name, self.bronze_schema, target_table]
        if not all(self._IDENTIFIER.fullmatch(part) for part in parts):
            raise ValueError(
                "Catalog, schema, and target table must be simple SQL identifiers"
            )
        return ".".join(parts)
