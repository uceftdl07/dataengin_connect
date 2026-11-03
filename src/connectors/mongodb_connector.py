"""
MongoDB connector using the MongoDB Spark Connector.
"""
import json
import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


class MongoDBConnector:
    """Read MongoDB collections into Spark DataFrames."""

    def __init__(
        self,
        spark: SparkSession,
        connection_uri: str,
        database: str,
        default_collection: Optional[str] = None,
    ):
        self.spark = spark
        self.connection_uri = connection_uri
        self.database = database
        self.default_collection = default_collection
        self.logger = logging.getLogger(self.__class__.__name__)

    def _collection_name(self, collection: Optional[str]) -> str:
        collection_name = collection or self.default_collection
        if not collection_name:
            raise ValueError(
                "A MongoDB collection must be configured in connector_config.yml "
                "or tables_config.yml"
            )
        return collection_name

    def read_table(
        self,
        table_name: str,
        read_options: Optional[Dict[str, Any]] = None,
        filter_query: Optional[str] = None,
        columns: Optional[List[str]] = None,
    ) -> DataFrame:
        """Read a collection; filter_query, when provided, is a JSON $match document."""
        options = dict(read_options or {})
        pipeline = options.pop("aggregation_pipeline", [])

        if not isinstance(pipeline, list) or any(
            not isinstance(stage, dict) for stage in pipeline
        ):
            raise ValueError(
                "'aggregation_pipeline' must be a list of MongoDB pipeline stages"
            )

        if filter_query:
            try:
                match_filter = json.loads(filter_query)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    "'filter_query' for MongoDB must be a JSON object"
                ) from exc
            if not isinstance(match_filter, dict):
                raise ValueError("'filter_query' for MongoDB must be a JSON object")
            pipeline = [{"$match": match_filter}, *pipeline]

        reader = (
            self.spark.read.format("mongodb")
            .option("connection.uri", self.connection_uri)
            .option("database", self.database)
            .option("collection", self._collection_name(table_name))
        )

        for key, value in options.items():
            reader = reader.option(key, str(value))

        if pipeline:
            reader = reader.option("aggregation.pipeline", json.dumps(pipeline))

        dataframe = reader.load()
        if columns:
            dataframe = dataframe.select(*[F.col(column) for column in columns])
        return dataframe

    def read_incremental(
        self,
        table_name: str,
        incremental_column: str,
        last_value: Optional[Union[str, datetime, date, int]],
        read_options: Optional[Dict[str, Any]] = None,
        filter_query: Optional[str] = None,
        columns: Optional[List[str]] = None,
    ) -> DataFrame:
        """Read rows whose incremental column is greater than the saved watermark."""
        dataframe = self.read_table(table_name, read_options, filter_query, columns)
        if last_value is None:
            return dataframe
        return dataframe.filter(F.col(incremental_column) > F.lit(last_value))

    def get_max_value(self, table_name: str, column_name: str) -> Optional[Any]:
        """Return the maximum value of a field in a collection."""
        result = (
            self.read_table(table_name)
            .agg(F.max(F.col(column_name)).alias("max_value"))
            .first()
        )
        return (
            result["max_value"] if result and result["max_value"] is not None else None
        )

    def test_connection(self, collection: Optional[str] = None) -> bool:
        """Validate MongoDB connectivity by attempting to read one document."""
        collection_name = self._collection_name(collection)
        try:
            self.read_table(collection_name).limit(1).count()
        except Exception as exc:
            raise ConnectionError(
                f"MongoDB connection test failed ({type(exc).__name__})"
            ) from None
        return True
