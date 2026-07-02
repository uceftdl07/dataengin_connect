# Databricks notebook source
# MAGIC %md
# MAGIC # SQL Server Ingestion - Production
# MAGIC 
# MAGIC Generic ingestion from SQL Server to ADLS / Unity Catalog.
# MAGIC 
# MAGIC **Parameters:**
# MAGIC - `domain_name`: Domain (mv, wolf, etc.)
# MAGIC - `catalog_name`: Unity Catalog catalog
# MAGIC - `bronze_schema`: Bronze schema
# MAGIC - `adls_base_path`: ADLS base path

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parameters

# COMMAND ----------

dbutils.widgets.text("domain_name", "mv", "Domain Name")
dbutils.widgets.text("catalog_name", "bronze", "Catalog Name")
dbutils.widgets.text("bronze_schema", "raw", "Bronze Schema")
dbutils.widgets.text("adls_base_path", "abfss://bronze@yourstorageaccount.dfs.core.windows.net", "ADLS Base Path")

# COMMAND ----------

domain_name = dbutils.widgets.get("domain_name")
catalog_name = dbutils.widgets.get("catalog_name")
bronze_schema = dbutils.widgets.get("bronze_schema")
adls_base_path = dbutils.widgets.get("adls_base_path")

print(f"Domain: {domain_name}")
print(f"Target: {catalog_name}.{bronze_schema}")
print(f"ADLS: {adls_base_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Setup

# COMMAND ----------

import sys
import os
import logging
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('main_ingestion')

# Add project to path
# The bundle name is "jdbc-ingestion-engine" (see databricks.yml).
# Candidate paths are checked in order; the first existing one wins.
_candidate_roots = [
    "/Workspace/.bundle/jdbc-ingestion-engine",                      # bundle deployment root
    os.path.abspath(os.path.join(os.getcwd(), "..")),                # notebook one level up
    os.path.abspath(os.getcwd()),                                     # current dir
]

project_root = next(
    (p for p in _candidate_roots if os.path.isdir(os.path.join(p, "src"))),
    _candidate_roots[1]  # safe default
)

logger.info(f"Project root: {project_root}")

if project_root not in sys.path:
    sys.path.insert(0, project_root)

# COMMAND ----------

# Verify imports work
try:
    from src.utils.config_loader import ConfigLoader
    from src.utils.secret_manager import SecretManager
    from src.connectors.generic_connector import GenericConnector
    from src.ingestors.generic_ingestor import GenericIngestor
    logger.info(" All imports successful")
except Exception as e:
    logger.error(f" Import failed: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Load Configuration

# COMMAND ----------

try:
    config_loader = ConfigLoader(project_root, domain_name)
    
    tables_config = config_loader.load_tables_config()
    connector_config = config_loader.load_connector_config()
    global_options = config_loader.get_global_table_options()
    
    enabled_tables = config_loader.get_enabled_tables()
    
    logger.info(f" Configuration loaded successfully")
    logger.info(f"  Domain: {tables_config['domain']}")
    logger.info(f"  Description: {tables_config.get('description', 'N/A')}")
    logger.info(f"  Tables to ingest: {len(enabled_tables)}")
    
    for table in enabled_tables:
        logger.info(f"    - {table['table_name']} ({table['load_mode']})")

except Exception as e:
    logger.error(f" Configuration loading failed: {str(e)}")
    dbutils.notebook.exit(f"FAILED: Configuration error - {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Initialize Connector

# COMMAND ----------

try:
    secret_manager = SecretManager(dbutils)
    
    # Get credentials
    credentials = secret_manager.get_sqlserver_credentials(connector_config['connector'])
    
    # Build JDBC connection
    jdbc_url = secret_manager.build_jdbc_url(
        credentials,
        connector_config['connector']['jdbc_options']
    )
    
    jdbc_properties = secret_manager.build_jdbc_properties(
        credentials,
        connector_config['connector']['jdbc_options']
    )
    
    # Create connector
    connector = GenericConnector(
        spark=spark,
        jdbc_url=jdbc_url,
        jdbc_properties=jdbc_properties,
        jdbc_options=connector_config['connector']['jdbc_options']
    )
    
    # Test connection
    logger.info("Testing SQL Server connection...")
    connector.test_connection()
    logger.info(" SQL Server connection successful")

except Exception as e:
    logger.error(f" Connector initialization failed: {str(e)}")
    dbutils.notebook.exit(f"FAILED: Connection error - {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Initialize Ingestor

# COMMAND ----------

ingestor = GenericIngestor(
    spark=spark,
    connector=connector,
    catalog_name=catalog_name,
    bronze_schema=bronze_schema,
    adls_base_path=adls_base_path
)

logger.info(" Ingestor initialized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Create Schema

# COMMAND ----------

try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{bronze_schema}")
    logger.info(f" Schema {catalog_name}.{bronze_schema} ready")
except Exception as e:
    logger.warning(f"Schema creation warning: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Ingest Tables

# COMMAND ----------

results = []
total_start = time.time()

for i, table_config in enumerate(enabled_tables, 1):
    table_name = table_config['table_name']
    
    logger.info(f"\n{'='*80}")
    logger.info(f"Processing table {i}/{len(enabled_tables)}: {table_name}")
    logger.info(f"{'='*80}")
    
    try:
        result = ingestor.ingest_table(table_config, global_options)
        results.append(result)
        
        if result['status'] == 'success':
            logger.info(f" {table_name}: {result['rows_loaded']:,} rows in {result['duration_seconds']}s")
        else:
            logger.warning(f"⚠ {table_name}: {result['status']}")
    
    except Exception as e:
        logger.error(f" {table_name} failed: {str(e)}")
        
        error_handling = global_options.get('error_handling', {})
        if error_handling.get('on_error', 'fail') == 'fail':
            dbutils.notebook.exit(f"FAILED: {table_name} - {str(e)}")
        
        results.append({
            'table_name': table_name,
            'target_table': table_config.get('target_table'),
            'status': 'failed',
            'error': str(e)
        })

total_duration = time.time() - total_start

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Report

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

# Ordered field list — single source of truth for both schema and row extraction
_RESULT_FIELDS = [
    ("table_name",       StringType()),
    ("target_table",     StringType()),
    ("load_mode",        StringType()),
    ("rows_loaded",      LongType()),
    ("duration_seconds", DoubleType()),
    ("status",           StringType()),
    ("message",          StringType()),
    ("error",            StringType()),
]

schema = StructType([StructField(name, dtype, True) for name, dtype in _RESULT_FIELDS])

results_data = [
    tuple(r.get(name) for name, _ in _RESULT_FIELDS)
    for r in results
]

results_df = spark.createDataFrame(results_data, schema)

print("\n" + "="*80)
print("INGESTION SUMMARY")
print("="*80)
display(results_df)

# COMMAND ----------

# Stats
total_tables = len(results)
successful = len([r for r in results if r['status'] == 'success'])
failed = len([r for r in results if r['status'] == 'failed'])
skipped = len([r for r in results if r['status'] == 'skipped'])
total_rows = sum([r.get('rows_loaded', 0) for r in results if r.get('rows_loaded')])

print(f"\n{'='*80}")
print(f"FINAL STATS - Domain: {domain_name}")
print(f"{'='*80}")
print(f"Total tables processed: {total_tables}")
print(f"   Successful: {successful}")
print(f"   Failed: {failed}")
print(f"  ⊘ Skipped: {skipped}")
print(f"Total rows ingested: {total_rows:,}")
print(f"Total duration: {total_duration:.2f}s")
print(f"{'='*80}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Exit

# COMMAND ----------

if failed > 0:
    dbutils.notebook.exit(f"COMPLETED WITH FAILURES: {failed} failures out of {total_tables} tables")
else:
    dbutils.notebook.exit(f"SUCCESS: {total_rows:,} rows from {successful} tables in {total_duration:.2f}s")
