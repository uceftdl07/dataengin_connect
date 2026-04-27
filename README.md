# MTE Engine — SQL Server & API Ingestion Framework

A **metadata-driven, production-ready** ingestion framework for loading data from SQL Server and REST APIs into Databricks (ADLS + Unity Catalog / Delta Lake).

Zero code required to add a new source — everything is driven by YAML configuration files.

---

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Quick Start](#quick-start)
- [Configuration Reference](#configuration-reference)
- [Deployment](#deployment)
- [Running Jobs](#running-jobs)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)
- [Best Practices](#best-practices)

---

## Features

### SQL Server Ingestion
| Capability | Details |
|---|---|
| Load modes | `full` (overwrite) · `incremental` (append or MERGE) |
| Parallel reads | Configurable `numPartitions` + `partitionColumn` |
| Filtering | Per-table `filter_query` (WHERE clause passthrough) |
| Upsert | Delta MERGE controlled by `merge_key` |
| Dual write | ADLS Parquet and/or Unity Catalog Delta simultaneously |
| Retry | Exponential back-off (configurable `max_retries`) |
| Watermarks | Dedicated `_watermarks` Delta table — survives target table rebuilds |

### REST API Ingestion
| Capability | Details |
|---|---|
| Auth | `bearer` · `api_key` · `basic` · `oauth2_client_credentials` |
| Pagination | `offset` · `page` · `cursor` · `link_header` |
| Rate limiting | `requests_per_second` + burst |
| Retry | Exponential back-off on configurable status codes |
| Incremental | Track by timestamp / date / ID field |
| Response parsing | Dot-separated `response_path` (e.g. `data.customers`) |

### Platform
- **100% YAML config** — add a table or API without touching Python
- **Multi-domain** — each domain is fully isolated; not every domain uses both ingestion types
- **Multi-environment** — `dev` / `staging` / `prod` via Databricks Asset Bundles
- **Typed results** — `IngestResult` dataclass for consistent reporting
- **Structured logging** throughout every layer
- **Unit-tested** with PySpark local mode + `responses` HTTP mocking

### Domain Overview

| Domain | SQL Server | REST APIs | Notes |
|--------|:----------:|:---------:|-------|
| `mv` | ✓ | ✗ | Transactional & reference tables only |
| `wolf` | ✓ | ✓ | Analytics tables + external analytics/reporting APIs |

---

## Architecture

```
┌─────────────────────┐     ┌─────────────────────┐
│  main_ingestion.py  │     │main_api_ingestion.py │
│  (Databricks NB)    │     │  (Databricks NB)     │
└────────┬────────────┘     └──────────┬───────────┘
         │                             │
         ▼                             ▼
┌─────────────────────┐     ┌─────────────────────┐
│  GenericIngestor    │     │  GenericApiIngestor  │
│  · full / incr.     │     │  · full / incr.      │
│  · retry w/ backoff │     │  · retry w/ backoff  │
│  · watermark table  │     │  · pagination        │
└────────┬────────────┘     └──────────┬───────────┘
         │                             │
         ▼                             ▼
┌─────────────────────┐     ┌─────────────────────┐
│  GenericConnector   │     │ GenericApiConnector  │
│  (JDBC / PySpark)   │     │ (requests + auth)    │
└────────┬────────────┘     └──────────┬───────────┘
         │                             │
         ▼                             ▼
┌─────────────────────────────────────────────────┐
│           ADLS (Parquet)  ·  Unity Catalog (Δ)  │
└─────────────────────────────────────────────────┘

       ConfigLoader ──▶ config/domains/<domain>/*.yml
       SecretManager ──▶ Databricks Secret Scopes
```

---

## Project Structure

```
MTE_Engine/
├── databricks.yml                      # Asset Bundle — name & environments
├── resources/
│   ├── jobs_mv.yml                     # MV job only
│   ├── jobs_wolf.yml                   # WOLF job only
│   └── jobs_all_domains.yml            # Cross-domain orchestration job
├── config/
│   └── domains/
│       ├── mv/
│       │   ├── tables_config.yml       # SQL Server tables for domain MV
│       │   ├── apis_config.yml         # REST APIs for domain MV
│       │   └── connector_config.yml    # Secret scopes & JDBC options
│       └── wolf/
│           ├── tables_config.yml
│           ├── apis_config.yml
│           └── connector_config.yml
├── src/
│   ├── connectors/
│   │   ├── generic_connector.py        # SQL Server JDBC connector
│   │   └── generic_api_connector.py    # REST API connector (auth + pagination)
│   ├── ingestors/
│   │   ├── generic_ingestor.py         # SQL → ADLS / Unity Catalog
│   │   └── generic_api_ingestor.py     # API → ADLS / Unity Catalog
│   └── utils/
│       ├── config_loader.py            # YAML config loader
│       └── secret_manager.py           # Databricks secrets helper
├── notebooks/
│   ├── main_ingestion.py               # Entry point — SQL Server
│   └── main_api_ingestion.py           # Entry point — REST APIs
├── tests/
│   ├── conftest.py                     # Shared fixtures (Spark, mocks, configs)
│   └── unit/
│       ├── test_api_connector.py
│       ├── test_config_loader.py
│       └── test_secret_manager.py
├── requirements.txt                    # Runtime deps (pyyaml, requests)
├── requirements-test.txt               # Test deps (pytest, pyspark, responses…)
└── setup.py
```

---

## Quick Start

### Prerequisites

- Python ≥ 3.9
- Databricks CLI v0.200+ (`pip install databricks-cli`)
- Access to a Databricks workspace (Azure / AWS / GCP)

### 1. Clone and install

```bash
git clone <repo-url> MTE_Engine
cd MTE_Engine

# Runtime deps only
pip install -r requirements.txt

# + test deps for local development
pip install -r requirements-test.txt
```

### 2. Create Databricks Secret Scopes

#### SQL Server — domain MV

```bash
databricks secrets create-scope --scope sqlserver-mv

databricks secrets put --scope sqlserver-mv --key hostname  --string-value "sql-server.database.windows.net"
databricks secrets put --scope sqlserver-mv --key port      --string-value "1433"
databricks secrets put --scope sqlserver-mv --key database  --string-value "MV_DB"
databricks secrets put --scope sqlserver-mv --key username  --string-value "db_user"
databricks secrets put --scope sqlserver-mv --key password  --string-value "YOUR_PASSWORD"
```

> The MV domain uses **SQL Server only** — no API secret scopes are needed.

#### SQL Server — domain WOLF

```bash
databricks secrets create-scope --scope sqlserver-wolf

databricks secrets put --scope sqlserver-wolf --key hostname  --string-value "wolf-server.database.windows.net"
databricks secrets put --scope sqlserver-wolf --key port      --string-value "1433"
databricks secrets put --scope sqlserver-wolf --key database  --string-value "WOLF_DB"
databricks secrets put --scope sqlserver-wolf --key username  --string-value "db_user"
databricks secrets put --scope sqlserver-wolf --key password  --string-value "YOUR_PASSWORD"
```

#### REST APIs — domain WOLF

```bash
# Bearer token (analytics_api)
databricks secrets create-scope --scope api-wolf-analytics_api
databricks secrets put --scope api-wolf-analytics_api --key bearer_token --string-value "YOUR_TOKEN"

# API Key (reporting_api)
databricks secrets create-scope --scope api-wolf-reporting_api
databricks secrets put --scope api-wolf-reporting_api --key api_key --string-value "YOUR_API_KEY"
```

### 3. Configure your workspace

Edit `databricks.yml`:

```yaml
bundle:
  name: sqlserver-api-ingestion-framework

targets:
  dev:
    workspace:
      host: https://adb-<your-workspace-id>.azuredatabricks.net
```

### 4. Deploy and run

```bash
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run   -t dev ingestion_mv
```

---

## Configuration Reference

### tables_config.yml

Every table is a YAML entry under `tables:`. The full set of options:

```yaml
domain: mv

tables:
  # ── Full load ────────────────────────────────────────────────
  - table_name: "dbo.Products"        # Source table (schema.table)
    target_table: "products"          # Target Delta table name
    load_mode: "full"                 # "full" | "incremental"
    enabled: true                     # Set false to skip

    filter_query: "IsActive = 1"      # Optional WHERE clause

    write_mode: "unity_catalog"       # "unity_catalog" | "adls" | "both"
    adls_path: "products/full"        # Path within adls_base_path (for "adls"/"both")

    partition_by: null                # List of column names, or null

    read_options:
      numPartitions: 2
      fetchsize: 5000

  # ── Incremental load with MERGE ──────────────────────────────
  - table_name: "dbo.Orders"
    target_table: "orders"
    load_mode: "incremental"
    enabled: true

    incremental_column: "OrderDate"   # Watermark column
    incremental_type: "timestamp"     # "timestamp" | "date" | "integer"
    merge_key: "OrderID"              # Upsert key — omit for simple APPEND

    write_mode: "unity_catalog"
    adls_path: "orders/incremental"

    partition_by:
      - "order_year"
      - "order_month"

    derived_columns:                  # Computed columns added before write
      order_year:  "year(OrderDate)"
      order_month: "month(OrderDate)"

    read_options:
      numPartitions: 8
      fetchsize: 50000
      partitionColumn: "OrderID"      # Enables parallel JDBC reads
      lowerBound: "1"
      upperBound: "10000000"

  # ── Composite merge key ───────────────────────────────────────
  - table_name: "warehouse.Inventory"
    target_table: "inventory"
    load_mode: "incremental"
    enabled: true

    incremental_column: "LastUpdateTimestamp"
    merge_key:                        # List for composite keys
      - "WarehouseID"
      - "ProductID"

    write_mode: "unity_catalog"
    adls_path: "inventory/incremental"
    partition_by:
      - "warehouse_id"
    derived_columns:
      warehouse_id: "WarehouseID"

    read_options:
      numPartitions: 6
      fetchsize: 30000

global_options:
  add_metadata_columns: true
  metadata_columns:
    - name: "_ingestion_timestamp"
      type: "timestamp"
      expression: "current_timestamp()"
    - name: "_source_domain"
      type: "string"
      expression: "'mv'"

  error_handling:
    on_error: "fail"    # "fail" | "skip" | "continue"
```

### apis_config.yml

```yaml
domain: mv

apis:
  - api_name: "orders_api"
    target_table: "api_orders"
    enabled: true

    base_url: "https://api.orders-system.com"
    endpoint: "/api/v1/orders"
    http_method: "GET"               # "GET" | "POST"

    # ── Auth ─────────────────────────────────────────────────
    auth_type: "bearer"              # "bearer" | "api_key" | "basic" | "oauth2_client_credentials"
    # For oauth2_client_credentials:
    # auth_config:
    #   token_url: "https://auth.example.com/token"
    #   scope: "read:orders"
    # For api_key:
    # auth_config:
    #   api_key_header: "X-API-Key"

    # ── Pagination ────────────────────────────────────────────
    pagination:
      type: "cursor"                 # "offset" | "page" | "cursor" | "link_header"
      page_size: 200
      page_size_param: "per_page"
      cursor_param: "cursor"
      cursor_response_path: "pagination.next_cursor"
      max_pages: 500

    query_params:
      status: "completed,shipped"

    headers:
      Accept: "application/json"

    # ── Rate limiting ─────────────────────────────────────────
    rate_limit:
      requests_per_second: 5
      burst: 10

    # ── Retry ─────────────────────────────────────────────────
    retry_config:
      max_retries: 5
      retry_delay_seconds: 10
      backoff_factor: 2
      retry_on_status: [429, 500, 503]

    timeout_seconds: 60

    # ── Response parsing ──────────────────────────────────────
    response_path: "orders"          # Dot-path to the records array

    # ── Incremental ───────────────────────────────────────────
    load_mode: "incremental"
    incremental_field: "updated_at"
    incremental_type: "timestamp"
    merge_key: "order_id"

    write_mode: "unity_catalog"
    adls_path: "api/orders_api/incremental"
    partition_by:
      - "order_date"
    derived_columns:
      order_date: "to_date(updated_at)"
```

### connector_config.yml

There is one file per domain. For a **SQL Server-only domain** (like MV), no `api_secrets` section is needed:

```yaml
domain: mv

connector:
  type: "sqlserver"
  secret_scope: "sqlserver-mv"        # Databricks Secret Scope name
  secret_keys:
    hostname: "hostname"
    port: "port"
    database: "database"
    username: "username"
    password: "password"
  jdbc_options:
    driver: "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    encrypt: "true"
    trustServerCertificate: "false"
    loginTimeout: "30"
    fetchsize: "10000"
    isolationLevel: "READ_UNCOMMITTED"
```

For a **mixed domain** (SQL Server + APIs, like WOLF), add an `api_secrets` section:

```yaml
domain: wolf

connector:
  type: "sqlserver"
  secret_scope: "sqlserver-wolf"
  secret_keys:
    hostname: "hostname"
    port: "port"
    database: "database"
    username: "username"
    password: "password"
  jdbc_options:
    driver: "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    encrypt: "true"
    trustServerCertificate: "false"
    fetchsize: "10000"
    isolationLevel: "READ_UNCOMMITTED"

api_secrets:
  analytics_api:
    secret_scope: "api-wolf-analytics_api"
    secret_keys:
      bearer_token: "bearer_token"
  reporting_api:
    secret_scope: "api-wolf-reporting_api"
    secret_keys:
      api_key: "api_key"
```

---

## Deployment

The bundle defines three environments in `databricks.yml`:

| Target | Mode | Catalog | Schedule |
|--------|------|---------|----------|
| `dev` | development | `dev_bronze` | manual |
| `staging` | development | `staging_bronze` | manual |
| `prod` | production | `bronze` | daily (02:00 CET) |

```bash
# Validate bundle syntax
databricks bundle validate

# Deploy to an environment
databricks bundle deploy -t dev
databricks bundle deploy -t staging
databricks bundle deploy -t prod
```

> **Important**: splitting files in `resources/` improves organization, but it does **not** change deployment granularity. A Databricks Asset Bundle deploys **all included resources** from `resources/*.yml`. If you want true **domain-only deployment**, you need separate bundles (for example one bundle for `mv` and one for `wolf`) or separate bundle roots in CI/CD.

---

## Running Jobs

Three jobs are defined across the domain-specific files in `resources/`:

| Job key | Domain | Sources | Schedule |
|---------|--------|---------|----------|
| `ingestion_mv` | MV | SQL Server only | Daily 02:00 CET |
| `ingestion_wolf` | WOLF | SQL Server + REST APIs | Daily 03:00 CET |
| `ingestion_all_domains` | All | SQL Server + REST APIs | Sundays 01:00 CET (paused) |

**`ingestion_mv`** runs a single task:
1. `ingest_sqlserver_mv` — `main_ingestion.py` with `domain_name=mv`

**`ingestion_wolf`** runs two tasks in sequence:
1. `ingest_sqlserver_wolf` — `main_ingestion.py` with `domain_name=wolf`
2. `ingest_apis_wolf` — `main_api_ingestion.py` with `domain_name=wolf` (depends on task 1)

```bash
# Trigger manually
databricks bundle run -t dev ingestion_mv
databricks bundle run -t prod ingestion_wolf
databricks bundle run -t prod ingestion_all_domains
```

### Adding a New Domain

1. Create the config directory:
```bash
mkdir -p config/domains/new_domain
# Copy and edit the three YAML files
cp config/domains/mv/*.yml config/domains/new_domain/
```

2. Update `domain:` keys in all three YAMLs to `new_domain`.

3. Create secret scopes for the new domain.

4. Add a new resource file such as `resources/jobs_new_domain.yml` (copy `resources/jobs_mv.yml` or `resources/jobs_wolf.yml` and adapt it).

5. Deploy:
```bash
databricks bundle deploy -t dev
databricks bundle run   -t dev ingestion_new_domain
```

---

## Watermark Persistence

Incremental loads track their progress in a dedicated `_watermarks` Delta table located at:

```
{catalog_name}.{bronze_schema}._watermarks
```

| Column | Description |
|--------|-------------|
| `target_table` | Name of the destination table |
| `column_name` | Incremental column (e.g. `OrderDate`) |
| `last_value` | Last successfully ingested value |
| `updated_at` | Timestamp of last update |

This means watermarks **survive target table drops and rebuilds**. On each run:
1. Read watermark from `_watermarks` table (if it exists)
2. Fall back to `MAX(incremental_column)` from the target table
3. After a successful write, upsert the new watermark into `_watermarks`

To disable the watermark table and always fall back to reading from the target table, set `watermark_table=None` when instantiating `GenericIngestor`.

---

## Testing

### Run all unit tests
```bash
pytest tests/ -v
```

### Run with coverage
```bash
pytest tests/ --cov=src --cov-report=term-missing
```

### Run a specific test class
```bash
pytest tests/unit/test_api_connector.py -v
```

### Test structure
```
tests/
├── conftest.py              # Shared fixtures: SparkSession, mock_dbutils,
│                            #   sample_api_config, sample_table_config, …
└── unit/
    ├── test_api_connector.py    # GenericApiConnector — auth, pagination, retry
    ├── test_config_loader.py    # ConfigLoader — YAML loading & validation
    └── test_secret_manager.py   # SecretManager — credentials & JDBC building
```

> **Note**: `pyspark` and `delta-spark` are test-only dependencies. On Databricks they are provided by the cluster runtime.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `Secret not found: scope/key` | Missing or misnamed secret | `databricks secrets list --scope YOUR_SCOPE` |
| `Domain path not found` | Wrong `domain_name` parameter | Verify folder at `config/domains/<domain_name>` exists |
| `Table not found` | Wrong catalog / schema | Check `catalog_name` and `bronze_schema` job parameters |
| `Connection timeout` | Firewall or wrong hostname/port | Check network rules; verify `connector_config.yml` |
| `HTTP 401 Unauthorized` | Expired or wrong credentials | Rotate and re-upload the secret |
| `HTTP 429 Too Many Requests` | Rate limit exceeded | Lower `requests_per_second`; increase `retry_delay_seconds` |
| `Import failed` in notebook | Wrong `project_root` path | The notebook auto-detects the path; check that `src/` is accessible |

### Inspect secrets
```bash
databricks secrets list-scopes
databricks secrets list --scope sqlserver-mv
```

### Check job run logs
1. Databricks UI → **Workflows** → **Jobs**
2. Click the job → click the latest run
3. Click each task to view the driver log

---

## Best Practices

1. **Always test in `dev` before deploying to `prod`**
2. **Use `incremental` mode** for tables > 1 M rows — avoids full rewrites
3. **Set `partition_by` on date columns** for time-series to speed up downstream queries
4. **Use `merge_key` only when updates are expected** — plain APPEND is faster
5. **Configure `max_pages`** on API pagination to guard against runaway loops
6. **Set `on_error: "continue"`** in `global_options` during initial onboarding to discover all failures at once
7. **Rotate secrets regularly** and update them in Databricks Secret Scopes
8. **Set email notifications** in the relevant resource file under `resources/` for production jobs

---

## Reference Tables

### Load Mode Behaviour

| `load_mode` | `merge_key` | Write behaviour |
|-------------|------------|-----------------|
| `full` | — | Delta `overwrite` (schema replaced) |
| `incremental` | absent | Delta `append` |
| `incremental` | set | Delta `MERGE` (upsert) |

### Write Mode

| `write_mode` | Writes to ADLS | Writes to Unity Catalog |
|---|:---:|:---:|
| `unity_catalog` | ✗ | ✓ |
| `adls` | ✓ | ✗ |
| `both` | ✓ | ✓ |

### API Auth Types

| `auth_type` | Required secret keys |
|---|---|
| `bearer` | `bearer_token` |
| `api_key` | `api_key` |
| `basic` | `username`, `password` |
| `oauth2_client_credentials` | `client_id`, `client_secret` |

### API Pagination Types

| `type` | Key parameters |
|---|---|
| `offset` | `page_size_param`, `offset_param` |
| `page` | `page_size_param`, `page_param` |
| `cursor` | `cursor_param`, `cursor_response_path` |
| `link_header` | (uses RFC 5988 `Link` header automatically) |

---

**Bundle name**: `sqlserver-api-ingestion-framework`  
**Version**: 1.1.0  
**Last updated**: April 2026  
**Maintained by**: Data Engineering Team
