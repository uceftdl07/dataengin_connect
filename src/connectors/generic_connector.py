"""
Generic SQL Server connector via JDBC.
Production-ready with error handling and logging.
"""
from typing import Dict, Any, Optional, Union
from pyspark.sql import SparkSession, DataFrame
from datetime import datetime, date
import logging


class GenericConnector:
    """Generic connector for SQL Server via JDBC."""
    
    def __init__(
        self,
        spark: SparkSession,
        jdbc_url: str,
        jdbc_properties: Dict[str, str],
        jdbc_options: Dict[str, Any]
    ):

        self.spark = spark
        self.jdbc_url = jdbc_url
        self.jdbc_properties = jdbc_properties
        self.jdbc_options = jdbc_options
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def read_table(
        self,
        table_name: str,
        read_options: Optional[Dict[str, Any]] = None,
        filter_query: Optional[str] = None
    ) -> DataFrame:

        # Merge properties and options
        merged_properties = {**self.jdbc_properties}
        
        if read_options:
            for key, value in read_options.items():
                if key not in ['numPartitions', 'partitionColumn', 'lowerBound', 'upperBound']:
                    merged_properties[key] = str(value)
        
        # Build query
        if filter_query:
            query = f"(SELECT * FROM {table_name} WHERE {filter_query}) AS filtered_table"
        else:
            query = table_name
        
        self.logger.info(f"Reading from SQL Server: {query}")
        
        # JDBC read
        reader = self.spark.read.format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("dbtable", query)
        
        # Add properties
        for key, value in merged_properties.items():
            reader = reader.option(key, value)
        
        # Add partitioning options if present
        if read_options:
            if 'numPartitions' in read_options:
                reader = reader.option("numPartitions", int(read_options['numPartitions']))
            
            # Column-based partitioning (for large tables)
            if all(k in read_options for k in ['partitionColumn', 'lowerBound', 'upperBound']):
                reader = reader.option("partitionColumn", read_options['partitionColumn']) \
                               .option("lowerBound", int(read_options['lowerBound'])) \
                               .option("upperBound", int(read_options['upperBound']))
        
        return reader.load()
    
    def read_incremental(
        self,
        table_name: str,
        incremental_column: str,
        last_value: Optional[Union[str, datetime, date, int]],
        read_options: Optional[Dict[str, Any]] = None,
        filter_query: Optional[str] = None
    ) -> DataFrame:
        """
        Read new data since last extraction.
        
        Args:
            table_name: Table name
            incremental_column: Reference column (timestamp, date, ID)
            last_value: Last extracted value (None for first run)
            read_options: Read options
            filter_query: Additional WHERE filter
        
        Returns:
            DataFrame with new data
        """
        if last_value is None:
            # First run: load all data
            self.logger.info(f"First incremental load for {table_name} - loading all data")
            return self.read_table(table_name, read_options, filter_query)
        
        # Build incremental filter
        # Handle different types
        if isinstance(last_value, (datetime, date)):
            incremental_filter = f"{incremental_column} > '{last_value}'"
        elif isinstance(last_value, str):
            incremental_filter = f"{incremental_column} > '{last_value}'"
        else:
            incremental_filter = f"{incremental_column} > {last_value}"
        
        self.logger.info(f"Incremental load: {incremental_filter}")
        
        # Combine with existing filter if present
        if filter_query:
            combined_filter = f"({filter_query}) AND ({incremental_filter})"
        else:
            combined_filter = incremental_filter
        
        return self.read_table(table_name, read_options, combined_filter)
    
    def get_max_value(
        self,
        table_name: str,
        column_name: str
    ) -> Optional[Any]:
        """
        Get max value of a column (for incremental tracking).
        
        Args:
            table_name: Table name
            column_name: Column name
        
        Returns:
            Max value or None if table is empty
        """
        query = f"(SELECT MAX({column_name}) as max_value FROM {table_name}) AS max_query"
        
        self.logger.info(f"Getting max value for {column_name} from {table_name}")
        
        df = self.spark.read.format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("dbtable", query)
        
        for key, value in self.jdbc_properties.items():
            df = df.option(key, value)
        
        result = df.load().collect()
        
        if result and result[0]['max_value'] is not None:
            max_val = result[0]['max_value']
            self.logger.info(f"Max value found: {max_val}")
            return max_val
        
        self.logger.info("No max value found (table empty or column NULL)")
        return None
    
    def test_connection(self) -> bool:
        """
        Test SQL Server connection.
        
        Returns:
            True if connection OK
        
        Raises:
            ConnectionError: If connection fails
        """
        try:
            self.logger.info(f"Testing connection to {self.jdbc_url}")
            
            query = "(SELECT 1 AS test_col) AS test_query"
            
            df = self.spark.read.format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("dbtable", query)
            
            for key, value in self.jdbc_properties.items():
                df = df.option(key, value)
            
            df.load().count()
            
            self.logger.info("Connection test successful")
            return True
        
        except Exception as e:
            self.logger.error(f"Connection test failed: {str(e)}")
            raise ConnectionError(f"Failed to connect to SQL Server: {str(e)}")
