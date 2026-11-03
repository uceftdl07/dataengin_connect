"""Batch connector for reading a bounded Kafka offset range."""
import logging
from typing import Dict, Optional

from pyspark.sql import DataFrame, SparkSession


class KafkaBatchConnector:
    """Read all retained records currently available for a Kafka topic."""

    _RESERVED_OPTIONS = {
        "subscribe",
        "subscribepattern",
        "assign",
        "startingOffsets",
        "endingOffsets",
        "kafka.bootstrap.servers",
    }

    def __init__(
        self,
        spark: SparkSession,
        bootstrap_servers: str,
        kafka_options: Optional[Dict[str, str]] = None,
    ):
        if not bootstrap_servers:
            raise ValueError("Kafka connector requires bootstrap_servers")
        if any(not isinstance(option, str) for option in (kafka_options or {})):
            raise ValueError("Kafka option names must be strings")
        if "kafka.bootstrap.servers" in {
            option.lower() for option in (kafka_options or {})
        }:
            raise ValueError(
                "Configure Kafka brokers with bootstrap_servers, not kafka_options"
            )
        self.spark = spark
        self.bootstrap_servers = bootstrap_servers
        self.kafka_options = kafka_options or {}
        self.logger = logging.getLogger(self.__class__.__name__)

    def read_topic(
        self, topic: str, source_options: Optional[Dict[str, str]] = None
    ) -> DataFrame:
        """Read from earliest retained offsets through the current latest offsets."""
        source_options = source_options or {}
        conflicts = {
            option
            for option in source_options
            if isinstance(option, str)
            and option.lower()
            in {reserved.lower() for reserved in self._RESERVED_OPTIONS}
        }
        invalid_options = [
            option for option in source_options if not isinstance(option, str)
        ]
        if invalid_options:
            raise ValueError("Kafka source option names must be strings")
        if conflicts:
            raise ValueError(
                "Kafka source_options cannot override: " + ", ".join(sorted(conflicts))
            )
        if not topic:
            raise ValueError("Kafka topic name cannot be empty")

        reader = (
            self.spark.read.format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap_servers)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
        )

        for option, value in self.kafka_options.items():
            reader = reader.option(option, value)
        for option, value in source_options.items():
            reader = reader.option(option, str(value))

        self.logger.info("Reading retained Kafka records from topic %s", topic)
        return reader.load()
