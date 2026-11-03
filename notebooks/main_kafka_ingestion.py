# Databricks notebook source
# MAGIC %md
# MAGIC # Daily Kafka Topic Ingestion
# MAGIC
# MAGIC Reads retained Kafka records, parses JSON, selects the requested event
# MAGIC date, and atomically replaces that date in a Unity Catalog Delta table.

# COMMAND ----------

dbutils.widgets.text("domain_name", "kafka_example", "Kafka Domain")
dbutils.widgets.text("catalog_name", "bronze", "Catalog Name")
dbutils.widgets.text("bronze_schema", "raw", "Bronze Schema")
dbutils.widgets.text("run_date", "", "Event Date (YYYY-MM-DD; default: yesterday)")

domain_name = dbutils.widgets.get("domain_name")
catalog_name = dbutils.widgets.get("catalog_name")
bronze_schema = dbutils.widgets.get("bronze_schema")
run_date_parameter = dbutils.widgets.get("run_date").strip()

# COMMAND ----------

import logging
import os
import re
import sys
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("main_kafka_ingestion")

candidate_roots = [
    "/Workspace/.bundle/jdbc-ingestion-engine",
    os.path.abspath(os.path.join(os.getcwd(), "..")),
    os.path.abspath(os.getcwd()),
]
project_root = next(
    (path for path in candidate_roots if os.path.isdir(os.path.join(path, "src"))),
    candidate_roots[1],
)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.connectors.kafka_batch_connector import KafkaBatchConnector
from src.ingestors.kafka_daily_ingestor import KafkaDailyIngestor
from src.utils.config_loader import ConfigLoader
from src.utils.secret_manager import SecretManager

# COMMAND ----------

try:
    config_loader = ConfigLoader(project_root, domain_name)
    kafka_config = config_loader.load_kafka_config()
    connector_config = config_loader.load_connector_config()["connector"]
    topics = config_loader.get_enabled_kafka_topics()
    timezone = kafka_config.get("timezone", "Europe/Paris")

    if run_date_parameter:
        run_date = date.fromisoformat(run_date_parameter)
    else:
        run_date = datetime.now(ZoneInfo(timezone)).date() - timedelta(days=1)

    if connector_config.get("type", "").lower() != "kafka":
        raise ValueError("connector_config.yml must declare connector.type: kafka")
    if not connector_config.get("bootstrap_servers"):
        raise ValueError("Kafka connector requires 'bootstrap_servers'")
    if not all(
        re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier)
        for identifier in (catalog_name, bronze_schema)
    ):
        raise ValueError("Catalog and schema must be simple SQL identifiers")

    spark.conf.set("spark.sql.session.timeZone", timezone)
    logger.info(
        "Kafka domain=%s, event_date=%s, timezone=%s, topics=%d",
        domain_name,
        run_date,
        timezone,
        len(topics),
    )
except Exception as exc:
    logger.error("Kafka configuration failed: %s", exc)
    dbutils.notebook.exit(f"FAILED: Kafka configuration - {exc}")

# COMMAND ----------

try:
    secret_manager = SecretManager(dbutils)
    kafka_options = secret_manager.get_kafka_options(connector_config)
    connector = KafkaBatchConnector(
        spark=spark,
        bootstrap_servers=connector_config["bootstrap_servers"],
        kafka_options=kafka_options,
    )
    ingestor = KafkaDailyIngestor(
        spark=spark,
        connector=connector,
        catalog_name=catalog_name,
        bronze_schema=bronze_schema,
        timezone=timezone,
    )
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{bronze_schema}")
except Exception as exc:
    logger.error("Kafka connector initialization failed: %s", exc)
    dbutils.notebook.exit(f"FAILED: Kafka connection configuration - {exc}")

# COMMAND ----------

results = []
error_handling = kafka_config.get("global_options", {}).get("error_handling", {})

for topic_config in topics:
    try:
        result = ingestor.ingest_topic(topic_config, run_date)
        results.append(result)
        logger.info(
            "Ingested topic %s: %s rows into %s",
            topic_config["topic"],
            result["rows_loaded"],
            result["target_table"],
        )
    except Exception as exc:
        logger.exception("Failed to ingest Kafka topic %s", topic_config["topic"])
        if error_handling.get("on_error", "fail") == "fail":
            dbutils.notebook.exit(
                f"FAILED: Kafka topic {topic_config['topic']} - {exc}"
            )
        results.append(
            {
                "table_name": topic_config["topic"],
                "target_table": topic_config.get("target_table"),
                "load_mode": "daily_replace",
                "rows_loaded": 0,
                "status": "failed",
                "error": str(exc),
            }
        )

# COMMAND ----------

for result in results:
    print(
        "{table_name} -> {target_table}: {rows_loaded} rows ({status})".format(**result)
    )

failures = [result for result in results if result["status"] != "success"]
if failures:
    dbutils.notebook.exit(
        f"COMPLETED WITH FAILURES: {len(failures)} of {len(results)} topics failed"
    )
dbutils.notebook.exit(
    f"SUCCESS: {sum(result['rows_loaded'] for result in results)} rows "
    f"from {len(results)} topics for {run_date}"
)
