"""
Generic API ingestor (REST APIs → ADLS / Unity Catalog).
Production-ready for Databricks with proper JSON handling.
"""
from typing import Dict, Any, List, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable
import logging
import time
import json


class GenericApiIngestor:
    """Generic ingestion from REST APIs to ADLS / Unity Catalog."""
    
    def __init__(
        self,
        spark: SparkSession,
        catalog_name: str,
        bronze_schema: str,
        adls_base_path: str
    ):
        """
        Initialize the API ingestor.
        
        Args:
            spark: SparkSession
            catalog_name: Unity Catalog catalog name
            bronze_schema: Bronze layer schema name
            adls_base_path: ADLS base path
        """
        self.spark = spark
        self.catalog_name = catalog_name
        self.bronze_schema = bronze_schema
        self.adls_base_path = adls_base_path.rstrip('/')
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def ingest_api(
        self,
        api_config: Dict[str, Any],
        api_connector,
        global_options: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Ingest an API according to its configuration.
        
        Args:
            api_config: API configuration
            api_connector: GenericApiConnector instance
            global_options: Global domain options
        
        Returns:
            Dict with ingestion stats
        """
        api_name = api_config['api_name']
        target_table = api_config['target_table']
        load_mode = api_config.get('load_mode', 'full')
        
        self.logger.info(f"Starting API ingestion: {api_name} (mode: {load_mode})")
        
        start_time = time.time()
        
        try:
            # Fetch data from API
            raw_data = api_connector.fetch_all_data()
            
            if not raw_data:
                self.logger.warning(f"No data returned from API {api_name}")
                return {
                    'api_name': api_name,
                    'target_table': target_table,
                    'status': 'success',
                    'rows_loaded': 0,
                    'duration_seconds': round(time.time() - start_time, 2),
                    'message': 'No data available'
                }
            
            self.logger.info(f"Fetched {len(raw_data)} records from API")
            
            # Convert to DataFrame
            df = self._convert_to_dataframe(raw_data)
            
            # Filter incremental data if needed
            if load_mode == 'incremental':
                df = self._filter_incremental(df, api_config)
            
            # Check if DataFrame is empty after filtering
            if df.rdd.isEmpty():
                self.logger.warning(f"No new data to ingest for {api_name}")
                return {
                    'api_name': api_name,
                    'target_table': target_table,
                    'load_mode': load_mode,
                    'rows_loaded': 0,
                    'duration_seconds': round(time.time() - start_time, 2),
                    'status': 'success',
                    'message': 'No new data to ingest'
                }
            
            # Add metadata
            df = self._add_metadata_columns(df, api_config, global_options)
            
            # Add derived columns
            df = self._add_derived_columns(df, api_config)
            
            # Write data
            rows_written = self._write_data(df, api_config, load_mode)
            
            duration = time.time() - start_time
            
            self.logger.info(
                f"API ingestion completed for {api_name}: "
                f"{rows_written:,} rows in {duration:.2f}s"
            )
            
            return {
                'api_name': api_name,
                'target_table': target_table,
                'load_mode': load_mode,
                'rows_loaded': rows_written,
                'duration_seconds': round(duration, 2),
                'status': 'success',
                'message': None
            }
        
        except Exception as e:
            self.logger.error(f"Failed to ingest API {api_name}: {str(e)}", exc_info=True)
            
            error_handling = global_options.get('error_handling', {})
            on_error = error_handling.get('on_error', 'fail')
            
            if on_error == 'fail':
                raise
            elif on_error == 'skip':
                return {
                    'api_name': api_name,
                    'target_table': target_table,
                    'status': 'skipped',
                    'error': str(e)
                }
            elif on_error == 'continue':
                return {
                    'api_name': api_name,
                    'target_table': target_table,
                    'status': 'failed',
                    'error': str(e)
                }
    
    def _convert_to_dataframe(self, raw_data: List[Dict[str, Any]]) -> DataFrame:
        """
        Convert JSON data to Spark DataFrame.
        
        Args:
            raw_data: List of JSON dicts
        
        Returns:
            DataFrame
        """
        self.logger.info(f"Converting {len(raw_data)} records to DataFrame")
        
        # Convert to JSON strings
        json_strings = [json.dumps(record) for record in raw_data]
        
        # Create RDD then DataFrame
        rdd = self.spark.sparkContext.parallelize(json_strings)
        df = self.spark.read.json(rdd)
        
        self.logger.info(f"DataFrame created with {df.count()} rows, {len(df.columns)} columns")
        
        return df
    
    def _filter_incremental(
        self,
        df: DataFrame,
        api_config: Dict[str, Any]
    ) -> DataFrame:
        """
        Filter data for incremental ingestion.
        
        Args:
            df: Source DataFrame
            api_config: API configuration
        
        Returns:
            Filtered DataFrame
        """
        incremental_field = api_config.get('incremental_field')
        
        if not incremental_field:
            self.logger.warning("Incremental mode but no incremental_field specified")
            return df
        
        target_table = api_config['target_table']
        
        # Get last value
        last_value = self._get_last_incremental_value(target_table, incremental_field)
        
        if last_value is None:
            # First run
            self.logger.info("First incremental run - loading all data")
            return df
        
        # Filter new data
        incremental_type = api_config.get('incremental_type', 'timestamp')
        
        self.logger.info(
            f"Filtering incremental data where {incremental_field} > {last_value}"
        )
        
        if incremental_type in ['timestamp', 'date']:
            df_filtered = df.filter(F.col(incremental_field) > F.lit(last_value))
        else:
            # For integer or other types
            df_filtered = df.filter(F.col(incremental_field) > last_value)
        
        new_count = df_filtered.count()
        self.logger.info(f"Filtered to {new_count} new records")
        
        return df_filtered
    
    def _get_last_incremental_value(
        self,
        target_table: str,
        incremental_field: str
    ) -> Optional[Any]:
        """Get last incremental value from target table."""
        full_table_name = f"{self.catalog_name}.{self.bronze_schema}.{target_table}"
        
        try:
            if not self.spark.catalog.tableExists(full_table_name):
                self.logger.info(f"Table {full_table_name} does not exist - first run")
                return None
            
            df = self.spark.table(full_table_name)
            result = df.agg(F.max(incremental_field).alias("max_value")).collect()
            max_value = result[0]['max_value'] if result else None
            
            if max_value is not None:
                self.logger.info(f"Last incremental value for {target_table}: {max_value}")
            else:
                self.logger.info(f"Table {full_table_name} is empty")
            
            return max_value
        
        except Exception as e:
            self.logger.warning(
                f"Could not retrieve last incremental value for {full_table_name}: {str(e)}"
            )
            return None
    
    def _add_metadata_columns(
        self,
        df: DataFrame,
        api_config: Dict[str, Any],
        global_options: Dict[str, Any]
    ) -> DataFrame:
        """Add metadata columns."""
        if not global_options.get('add_metadata_columns', False):
            return df
        
        metadata_columns = global_options.get('metadata_columns', [])
        
        for col_config in metadata_columns:
            col_name = col_config['name']
            expression = col_config['expression']
            
            # Variable substitution
            expression = expression.replace('${api.api_name}', f"'{api_config['api_name']}'")
            
            try:
                df = df.withColumn(col_name, F.expr(expression))
            except Exception as e:
                self.logger.warning(
                    f"Failed to add metadata column {col_name}: {str(e)}. Skipping."
                )
        
        return df
    
    def _add_derived_columns(
        self,
        df: DataFrame,
        api_config: Dict[str, Any]
    ) -> DataFrame:
        """Add derived columns."""
        derived_columns = api_config.get('derived_columns', {})
        
        for col_name, expression in derived_columns.items():
            try:
                df = df.withColumn(col_name, F.expr(expression))
            except Exception as e:
                self.logger.warning(
                    f"Failed to add derived column {col_name}: {str(e)}. Skipping."
                )
        
        return df
    
    def _write_data(
        self,
        df: DataFrame,
        api_config: Dict[str, Any],
        load_mode: str
    ) -> int:
        """Write data to ADLS and/or Unity Catalog."""
        write_mode = api_config.get('write_mode', 'unity_catalog')
        
        # Cache DataFrame
        df.cache()
        row_count = df.count()
        self.logger.info(f"Writing {row_count:,} rows (write_mode: {write_mode})")
        
        try:
            if write_mode in ['adls', 'both']:
                self._write_to_adls(df, api_config, load_mode)
            
            if write_mode in ['unity_catalog', 'both']:
                self._write_to_unity_catalog(df, api_config, load_mode)
        finally:
            df.unpersist()
        
        return row_count
    
    def _write_to_adls(
        self,
        df: DataFrame,
        api_config: Dict[str, Any],
        load_mode: str
    ):
        """Write as Parquet to ADLS."""
        adls_path = api_config.get('adls_path')
        
        if not adls_path:
            raise ValueError("adls_path not specified")
        
        full_path = f"{self.adls_base_path}/{adls_path}"
        partition_by = api_config.get('partition_by', [])
        
        self.logger.info(f"Writing to ADLS: {full_path}")
        
        writer = df.write.format("parquet") \
            .mode("overwrite" if load_mode == 'full' else "append")
        
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        
        writer.save(full_path)
        self.logger.info("ADLS write completed")
    
    def _write_to_unity_catalog(
        self,
        df: DataFrame,
        api_config: Dict[str, Any],
        load_mode: str
    ):
        """Write to Unity Catalog."""
        target_table = api_config['target_table']
        full_table_name = f"{self.catalog_name}.{self.bronze_schema}.{target_table}"
        
        partition_by = api_config.get('partition_by', [])
        table_exists = self.spark.catalog.tableExists(full_table_name)
        
        if load_mode == 'full':
            self.logger.info(f"Writing FULL load to UC: {full_table_name}")
            
            writer = df.write.format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .option("delta.autoOptimize.optimizeWrite", "true") \
                .option("delta.autoOptimize.autoCompact", "true")
            
            if partition_by:
                writer = writer.partitionBy(*partition_by)
            
            writer.saveAsTable(full_table_name)
            self.logger.info("UC write completed (full overwrite)")
        
        elif load_mode == 'incremental':
            if not table_exists:
                self.logger.info(f"Creating UC table: {full_table_name}")
                
                writer = df.write.format("delta") \
                    .mode("overwrite") \
                    .option("delta.autoOptimize.optimizeWrite", "true") \
                    .option("delta.autoOptimize.autoCompact", "true")
                
                if partition_by:
                    writer = writer.partitionBy(*partition_by)
                
                writer.saveAsTable(full_table_name)
                self.logger.info("UC table created")
            
            else:
                # Table exists: merge or append
                merge_key = api_config.get('merge_key')
                
                if merge_key:
                    self._merge_incremental(df, api_config, full_table_name)
                else:
                    self.logger.info(f"Appending to UC: {full_table_name}")
                    
                    df.write.format("delta") \
                        .mode("append") \
                        .option("delta.autoOptimize.optimizeWrite", "true") \
                        .option("delta.autoOptimize.autoCompact", "true") \
                        .saveAsTable(full_table_name)
                    
                    self.logger.info("UC append completed")
    
    def _merge_incremental(
        self,
        df: DataFrame,
        api_config: Dict[str, Any],
        full_table_name: str
    ):
        """Merge upsert for incremental."""
        merge_key = api_config.get('merge_key')
        
        if not merge_key:
            raise ValueError("merge_key required for MERGE operation")
        
        self.logger.info(f"Merging into UC: {full_table_name}")
        
        delta_table = DeltaTable.forName(self.spark, full_table_name)
        
        if isinstance(merge_key, str):
            merge_keys = [merge_key]
        else:
            merge_keys = list(merge_key)
        
        merge_condition = " AND ".join([
            f"target.{key} = source.{key}" for key in merge_keys
        ])
        
        self.logger.info(f"Merge condition: {merge_condition}")
        
        delta_table.alias("target") \
            .merge(df.alias("source"), merge_condition) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
        
        self.logger.info(f"Merge completed for {full_table_name}")
