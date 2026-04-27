"""
Generic SQL Server ingestor (full & incremental loads).
Production-ready for Databricks with proper Delta Lake handling.
"""
from dataclasses import dataclass, asdict
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable
import logging
import time


@dataclass
class IngestResult:
    """Typed container for the result of a single table ingestion."""
    table_name: str
    target_table: Optional[str] = None
    load_mode: Optional[str] = None
    rows_loaded: Optional[int] = None
    duration_seconds: Optional[float] = None
    status: str = "unknown"
    message: Optional[str] = None
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class GenericIngestor:
    """Generic ingestion from SQL Server to ADLS / Unity Catalog."""

    # Default retry configuration
    DEFAULT_MAX_RETRIES: int = 3
    DEFAULT_BACKOFF_BASE: int = 2  # seconds; wait = base^attempt

    def __init__(
        self,
        spark: SparkSession,
        connector,
        catalog_name: str,
        bronze_schema: str,
        adls_base_path: str,
        max_retries: int = DEFAULT_MAX_RETRIES,
        watermark_table: str = "_watermarks",
    ):
        """
        Initialize the ingestor.

        Args:
            spark: SparkSession
            connector: GenericConnector instance
            catalog_name: Unity Catalog catalog name
            bronze_schema: Bronze layer schema name
            adls_base_path: ADLS base path (abfss://...)
            max_retries: Max attempts for transient failures (default 3)
            watermark_table: Name of the dedicated watermark tracking table
                             stored at {catalog_name}.{bronze_schema}.{watermark_table}.
                             Set to None to disable and fall back to reading
                             max() from the target table directly.
        """
        self.spark = spark
        self.connector = connector
        self.catalog_name = catalog_name
        self.bronze_schema = bronze_schema
        self.adls_base_path = adls_base_path.rstrip('/')
        self.max_retries = max_retries
        self.watermark_table = watermark_table
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def ingest_table(
        self,
        table_config: Dict[str, Any],
        global_options: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Ingest a table according to its configuration.

        Args:
            table_config: Table configuration
            global_options: Global domain options

        Returns:
            Dict (from IngestResult) with ingestion stats (rows_loaded, duration, etc.)
        """
        table_name = table_config['table_name']
        target_table = table_config['target_table']
        load_mode = table_config.get('load_mode', 'full')

        self.logger.info(f"Starting ingestion for table: {table_name} (mode: {load_mode})")

        start_time = time.time()

        try:
            # Load data with retry for transient failures
            if load_mode == 'full':
                df = self._execute_with_retry(self._load_full, table_config)
            elif load_mode == 'incremental':
                df = self._execute_with_retry(self._load_incremental, table_config)
            else:
                raise ValueError(f"Unsupported load mode: {load_mode}")

            # Check if DataFrame is empty
            if df.rdd.isEmpty():
                self.logger.warning(f"No data to ingest for {table_name}")
                return IngestResult(
                    table_name=table_name,
                    target_table=target_table,
                    load_mode=load_mode,
                    rows_loaded=0,
                    duration_seconds=round(time.time() - start_time, 2),
                    status='success',
                    message='No data to ingest',
                ).to_dict()

            # Add metadata columns
            df = self._add_metadata_columns(df, table_config, global_options)

            # Add derived columns (for partitioning)
            df = self._add_derived_columns(df, table_config)

            # Write data
            rows_written = self._execute_with_retry(self._write_data, df, table_config, load_mode)

            duration = time.time() - start_time

            # Persist watermark after successful incremental write
            if load_mode == 'incremental':
                incremental_column = table_config.get('incremental_column')
                if incremental_column:
                    self._save_watermark(target_table, incremental_column, df)

            self.logger.info(
                f"Ingestion completed for {table_name}: "
                f"{rows_written:,} rows in {duration:.2f}s"
            )

            return IngestResult(
                table_name=table_name,
                target_table=target_table,
                load_mode=load_mode,
                rows_loaded=rows_written,
                duration_seconds=round(duration, 2),
                status='success',
            ).to_dict()

        except Exception as e:
            self.logger.error(f"Failed to ingest table {table_name}: {str(e)}", exc_info=True)

            # Error handling based on global_options
            error_handling = global_options.get('error_handling', {})
            on_error = error_handling.get('on_error', 'fail')

            if on_error == 'fail':
                raise
            elif on_error == 'skip':
                self.logger.warning(f"Skipping table {table_name} due to error")
                return IngestResult(
                    table_name=table_name, target_table=target_table,
                    status='skipped', error=str(e),
                ).to_dict()
            elif on_error == 'continue':
                self.logger.warning(f"Continuing despite error on {table_name}")
                return IngestResult(
                    table_name=table_name, target_table=target_table,
                    status='failed', error=str(e),
                ).to_dict()

    # ------------------------------------------------------------------
    # Retry helper
    # ------------------------------------------------------------------

    def _execute_with_retry(self, func, *args, **kwargs):
        """
        Execute *func* with exponential back-off retry on any exception.

        Uses self.max_retries and DEFAULT_BACKOFF_BASE.
        Wait between attempts: DEFAULT_BACKOFF_BASE ^ attempt  (2s, 4s, 8s, …)
        """
        last_exc: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            try:
                return func(*args, **kwargs)
            except Exception as exc:
                last_exc = exc
                if attempt < self.max_retries:
                    wait = self.DEFAULT_BACKOFF_BASE ** attempt
                    self.logger.warning(
                        f"Attempt {attempt}/{self.max_retries} failed "
                        f"({type(exc).__name__}: {exc}). Retrying in {wait}s…"
                    )
                    time.sleep(wait)
                else:
                    self.logger.error(
                        f"All {self.max_retries} attempts failed for {func.__name__}."
                    )
        raise last_exc  # type: ignore[misc]

    # ------------------------------------------------------------------
    # Watermark persistence
    # ------------------------------------------------------------------

    @property
    def _watermark_full_table(self) -> Optional[str]:
        if not self.watermark_table:
            return None
        return f"{self.catalog_name}.{self.bronze_schema}.{self.watermark_table}"

    def _ensure_watermark_table(self):
        """Create the watermark tracking table if it doesn't exist."""
        full_name = self._watermark_full_table
        if not full_name:
            return
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {full_name} (
                target_table    STRING NOT NULL,
                column_name     STRING NOT NULL,
                last_value      STRING,
                updated_at      TIMESTAMP
            )
            USING DELTA
        """)

    def _save_watermark(self, target_table: str, column_name: str, df: DataFrame):
        """
        Persist the max value of *column_name* in *df* to the watermark table.
        No-op when watermark_table is disabled.
        """
        full_name = self._watermark_full_table
        if not full_name:
            return
        try:
            self._ensure_watermark_table()
            max_val = df.agg(F.max(column_name).cast("string").alias("v")).collect()[0]["v"]
            if max_val is None:
                return
            self.spark.sql(f"""
                MERGE INTO {full_name} AS t
                USING (SELECT
                    '{target_table}'        AS target_table,
                    '{column_name}'         AS column_name,
                    '{max_val}'             AS last_value,
                    current_timestamp()     AS updated_at
                ) AS s
                ON t.target_table = s.target_table AND t.column_name = s.column_name
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)
            self.logger.info(
                f"Watermark saved: {target_table}.{column_name} = {max_val}"
            )
        except Exception as exc:
            self.logger.warning(f"Could not save watermark for {target_table}: {exc}")
    
    def _load_full(self, table_config: Dict[str, Any]) -> DataFrame:
        """Load all data from table."""
        table_name = table_config['table_name']
        read_options = table_config.get('read_options', {})
        filter_query = table_config.get('filter_query')
        
        self.logger.info(f"Loading full data from {table_name}")
        return self.connector.read_table(table_name, read_options, filter_query)
    
    def _load_incremental(self, table_config: Dict[str, Any]) -> DataFrame:
        """Load new data (incremental)."""
        table_name = table_config['table_name']
        target_table = table_config['target_table']
        incremental_column = table_config.get('incremental_column')
        
        if not incremental_column:
            raise ValueError(f"incremental_column not specified for table {table_name}")
        
        # Get last extracted value
        last_value = self._get_last_incremental_value(target_table, incremental_column)
        
        if last_value:
            self.logger.info(
                f"Loading incremental data from {table_name} "
                f"where {incremental_column} > {last_value}"
            )
        else:
            self.logger.info(f"First incremental load for {table_name} - loading all data")
        
        read_options = table_config.get('read_options', {})
        filter_query = table_config.get('filter_query')
        
        return self.connector.read_incremental(
            table_name,
            incremental_column,
            last_value,
            read_options,
            filter_query
        )
    
    def _get_last_incremental_value(
        self,
        target_table: str,
        incremental_column: str
    ) -> Optional[Any]:
        """
        Get last value of incremental column.

        Priority:
        1. Dedicated watermark table (if configured) — survives target table rebuilds.
        2. MAX() of the incremental column in the target Delta table (fallback).

        Returns:
            Last value or None if table doesn't exist / is empty
        """
        # 1. Watermark table
        full_wm = self._watermark_full_table
        if full_wm and self.spark.catalog.tableExists(full_wm):
            try:
                result = self.spark.sql(f"""
                    SELECT last_value FROM {full_wm}
                    WHERE target_table = '{target_table}'
                      AND column_name  = '{incremental_column}'
                """).collect()
                if result and result[0]["last_value"] is not None:
                    raw = result[0]["last_value"]
                    self.logger.info(
                        f"Watermark (from table) for {target_table}.{incremental_column}: {raw}"
                    )
                    return raw
            except Exception as exc:
                self.logger.warning(f"Could not read watermark table: {exc}")

        # 2. Fallback: read from target Delta table
        full_table_name = f"{self.catalog_name}.{self.bronze_schema}.{target_table}"

        try:
            if not self.spark.catalog.tableExists(full_table_name):
                self.logger.info(f"Table {full_table_name} does not exist - first run")
                return None

            df = self.spark.table(full_table_name)
            result = df.agg(F.max(incremental_column).alias("max_value")).collect()
            max_value = result[0]['max_value'] if result else None

            if max_value is not None:
                self.logger.info(
                    f"Last incremental value (from target table) "
                    f"for {target_table}: {max_value}"
                )
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
        table_config: Dict[str, Any],
        global_options: Dict[str, Any]
    ) -> DataFrame:
        """Add metadata columns to DataFrame."""
        if not global_options.get('add_metadata_columns', False):
            return df
        
        metadata_columns = global_options.get('metadata_columns', [])
        
        for col_config in metadata_columns:
            col_name = col_config['name']
            expression = col_config['expression']
            
            # Variable substitution
            expression = expression.replace('${table.table_name}', f"'{table_config['table_name']}'")
            
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
        table_config: Dict[str, Any]
    ) -> DataFrame:
        """Add derived columns (for partitioning)."""
        derived_columns = table_config.get('derived_columns', {})
        
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
        table_config: Dict[str, Any],
        load_mode: str
    ) -> int:
        """
        Write data to ADLS and/or Unity Catalog.
        
        Args:
            df: DataFrame to write
            table_config: Table configuration
            load_mode: Load mode (full or incremental)
        
        Returns:
            Number of rows written
        """
        write_mode = table_config.get('write_mode', 'unity_catalog')
        
        # Cache DataFrame to avoid recomputation
        df.cache()
        row_count = df.count()
        self.logger.info(f"Writing {row_count:,} rows (write_mode: {write_mode})")
        
        try:
            if write_mode in ['adls', 'both']:
                self._write_to_adls(df, table_config, load_mode)
            
            if write_mode in ['unity_catalog', 'both']:
                self._write_to_unity_catalog(df, table_config, load_mode)
        finally:
            df.unpersist()
        
        return row_count
    
    def _write_to_adls(
        self,
        df: DataFrame,
        table_config: Dict[str, Any],
        load_mode: str
    ):
        """Write data as Parquet to ADLS."""
        adls_path = table_config.get('adls_path')
        
        if not adls_path:
            raise ValueError("adls_path not specified for ADLS write")
        
        # Full path
        full_path = f"{self.adls_base_path}/{adls_path}"
        
        partition_by = table_config.get('partition_by', [])
        
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
        table_config: Dict[str, Any],
        load_mode: str
    ):
        """Write data to Unity Catalog (Delta format)."""
        target_table = table_config['target_table']
        full_table_name = f"{self.catalog_name}.{self.bronze_schema}.{target_table}"
        
        partition_by = table_config.get('partition_by', [])
        
        # Check if table exists
        table_exists = self.spark.catalog.tableExists(full_table_name)
        
        if load_mode == 'full':
            # Full load: overwrite
            self.logger.info(f"Writing FULL load to UC table: {full_table_name}")
            
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
                # First write: create table
                self.logger.info(f"Creating UC table (first incremental): {full_table_name}")
                
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
                merge_key = table_config.get('merge_key')
                
                if merge_key:
                    # MERGE (upsert)
                    self._merge_incremental(df, table_config, full_table_name)
                else:
                    # Simple APPEND
                    self.logger.info(f"Appending incremental data to: {full_table_name}")
                    
                    df.write.format("delta") \
                        .mode("append") \
                        .option("delta.autoOptimize.optimizeWrite", "true") \
                        .option("delta.autoOptimize.autoCompact", "true") \
                        .saveAsTable(full_table_name)
                    
                    self.logger.info("UC append completed")
    
    def _merge_incremental(
        self,
        df: DataFrame,
        table_config: Dict[str, Any],
        full_table_name: str
    ):
        """
        Perform MERGE for incremental ingestion (upsert).
        
        Args:
            df: DataFrame to merge
            table_config: Table configuration
            full_table_name: Full UC table name
        """
        merge_key = table_config.get('merge_key')
        
        if not merge_key:
            raise ValueError("merge_key required for MERGE operation")
        
        self.logger.info(f"Merging incremental data into: {full_table_name}")
        
        # Get Delta table
        delta_table = DeltaTable.forName(self.spark, full_table_name)
        
        # Build merge condition
        if isinstance(merge_key, str):
            merge_keys = [merge_key]
        else:
            merge_keys = list(merge_key)
        
        merge_condition = " AND ".join([
            f"target.{key} = source.{key}" for key in merge_keys
        ])
        
        self.logger.info(f"Merge condition: {merge_condition}")
        
        # Perform merge
        delta_table.alias("target") \
            .merge(
                df.alias("source"),
                merge_condition
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
        
        self.logger.info(f"Merge completed for {full_table_name}")
