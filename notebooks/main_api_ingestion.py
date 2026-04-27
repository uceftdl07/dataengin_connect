# Databricks notebook source
# MAGIC %md
# MAGIC # API Ingestion - Production
# MAGIC 
# MAGIC Generic ingestion from REST APIs to ADLS / Unity Catalog.
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
logger = logging.getLogger('main_api_ingestion')

# Add project to path
workspace_path = "/Workspace/.bundle/sqlserver-api-ingestion-framework"

if os.path.exists(workspace_path):
    project_root = workspace_path
else:
    current_dir = os.getcwd()
    project_root = os.path.abspath(os.path.join(current_dir, ".."))

logger.info(f"Project root: {project_root}")

if project_root not in sys.path:
    sys.path.insert(0, project_root)

# COMMAND ----------

# Verify imports
try:
    from src.utils.config_loader import ConfigLoader
    from src.utils.secret_manager import SecretManager
    from src.connectors.generic_api_connector import GenericApiConnector
    from src.ingestors.generic_api_ingestor import GenericApiIngestor
    logger.info("✓ All imports successful")
except Exception as e:
    logger.error(f"✗ Import failed: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Load Configuration

# COMMAND ----------

try:
    config_loader = ConfigLoader(project_root, domain_name)
    
    apis_config = config_loader.load_apis_config()
    connector_config = config_loader.load_connector_config()
    global_options = config_loader.get_global_api_options()
    
    enabled_apis = config_loader.get_enabled_apis()
    
    logger.info(f"  Configuration loaded successfully")
    logger.info(f"  Domain: {apis_config['domain']}")
    logger.info(f"  Description: {apis_config.get('description', 'N/A')}")
    logger.info(f"  APIs to ingest: {len(enabled_apis)}")
    
    for api in enabled_apis:
        logger.info(f"    - {api['api_name']} ({api['load_mode']})")

except Exception as e:
    logger.error(f"  Configuration loading failed: {str(e)}")
    dbutils.notebook.exit(f"FAILED: Configuration error - {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Initialize Ingestor

# COMMAND ----------

ingestor = GenericApiIngestor(
    spark=spark,
    catalog_name=catalog_name,
    bronze_schema=bronze_schema,
    adls_base_path=adls_base_path
)

logger.info("✓ Ingestor initialized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Create Schema

# COMMAND ----------

try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{bronze_schema}")
    logger.info(f"✓ Schema {catalog_name}.{bronze_schema} ready")
except Exception as e:
    logger.warning(f"Schema creation warning: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Ingest APIs

# COMMAND ----------

secret_manager = SecretManager(dbutils)
results = []
total_start = time.time()

for i, api_config in enumerate(enabled_apis, 1):
    api_name = api_config['api_name']
    
    logger.info(f"\n{'='*80}")
    logger.info(f"Processing API {i}/{len(enabled_apis)}: {api_name}")
    logger.info(f"{'='*80}")
    
    try:
        # Get API credentials
        api_secrets = connector_config.get('api_secrets', {})
        credentials = secret_manager.get_api_credentials(api_name, api_secrets)
        
        # Create API connector
        api_connector = GenericApiConnector(
            api_config=api_config,
            credentials=credentials
        )
        
        # Ingest
        result = ingestor.ingest_api(api_config, api_connector, global_options)
        results.append(result)
        
        if result['status'] == 'success':
            logger.info(f"✓ {api_name}: {result['rows_loaded']:,} rows in {result['duration_seconds']}s")
        else:
            logger.warning(f"⚠ {api_name}: {result['status']}")
    
    except Exception as e:
        logger.error(f"✗ {api_name} failed: {str(e)}")
        
        error_handling = global_options.get('error_handling', {})
        if error_handling.get('on_error', 'fail') == 'fail':
            dbutils.notebook.exit(f"FAILED: {api_name} - {str(e)}")
        
        results.append({
            'api_name': api_name,
            'target_table': api_config.get('target_table'),
            'status': 'failed',
            'error': str(e)
        })

total_duration = time.time() - total_start

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Report

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

# Create results DataFrame
schema = StructType([
    StructField("api_name", StringType(), True),
    StructField("target_table", StringType(), True),
    StructField("load_mode", StringType(), True),
    StructField("rows_loaded", LongType(), True),
    StructField("duration_seconds", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("message", StringType(), True),
    StructField("error", StringType(), True)
])

results_data = []
for r in results:
    results_data.append((
        r.get('api_name'),
        r.get('target_table'),
        r.get('load_mode'),
        r.get('rows_loaded'),
        r.get('duration_seconds'),
        r.get('status'),
        r.get('message'),
        r.get('error')
    ))

results_df = spark.createDataFrame(results_data, schema)

print("\n" + "="*80)
print("API INGESTION SUMMARY")
print("="*80)
display(results_df)

# COMMAND ----------

# Stats
total_apis = len(results)
successful = len([r for r in results if r['status'] == 'success'])
failed = len([r for r in results if r['status'] == 'failed'])
skipped = len([r for r in results if r['status'] == 'skipped'])
total_rows = sum([r.get('rows_loaded', 0) for r in results if r.get('rows_loaded')])

print(f"\n{'='*80}")
print(f"FINAL STATS - Domain: {domain_name}")
print(f"{'='*80}")
print(f"Total APIs processed: {total_apis}")
print(f"  ✓ Successful: {successful}")
print(f"  ✗ Failed: {failed}")
print(f"  ⊘ Skipped: {skipped}")
print(f"Total rows ingested: {total_rows:,}")
print(f"Total duration: {total_duration:.2f}s")
print(f"{'='*80}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Exit

# COMMAND ----------

if failed > 0:
    dbutils.notebook.exit(f"COMPLETED WITH FAILURES: {failed} failures out of {total_apis} APIs")
else:
    dbutils.notebook.exit(f"SUCCESS: {total_rows:,} rows from {successful} APIs in {total_duration:.2f}s")
