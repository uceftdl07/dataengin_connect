# MTE Engine — JDBC, MongoDB, Kafka & API Ingestion Framework

A **metadata-driven** ingestion framework for loading data from SQL Server/JDBC, MongoDB, Kafka and REST APIs into Databricks (ADLS + Unity Catalog / Delta Lake).

Zero code required to add a new source — everything is driven by YAML configuration files.

---

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Quick Start](#quick-start)
- [Detailed ingestion guide (French)](docs/INGESTION_GUIDE_FR.md)
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

### MongoDB Ingestion
| Capability | Details |
|---|---|
| Source selection | Database and default collection in `connector_config.yml`; per-table collection overrides in `tables_config.yml` |
| Load modes | `full` and `incremental` using a configured watermark field |
| Filtering | MongoDB `$match` JSON in `filter_query`; optional aggregation stages in `read_options.aggregation_pipeline` |
| Destination | Same ADLS Parquet and/or Unity Catalog Delta output as SQL Server |
| Credentials | MongoDB URI retrieved from a Databricks Secret Scope |

### Kafka Previous-Day Ingestion
| Capability | Details |
|---|---|
| Execution | Scheduled daily batch job; optional `run_date` widget for backfills |
| Parsing | JSON payload parsed using a Spark schema declared per topic in `kafka_config.yml` |
| Event date | Configured payload timestamp first; Kafka record timestamp if that field is null |
| Idempotency | Atomic Delta `replaceWhere` for the requested event date, without changing other dates |
| Targets | Creates a Delta table when absent, or replaces that date in an existing Delta table with a matching schema |
| Authentication | Kafka consumer options and secret references in `connector_config.yml` / Databricks Secret Scopes |

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
│ GenericConnector    │     │ GenericApiConnector  │
│ (JDBC / PySpark)    │     │ (requests + auth)    │
└────────┬────────────┘     └──────────┬───────────┘
         │                             │
   MongoDBConnector                    │
   (Mongo Spark)                       │
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
├── databricks.yml                      # Asset Bundle — name, `domains` list & environments
├── resources/
│   ├── jdbc_ingestion.job.yml          # Shared SQL Server / MongoDB job
│   └── kafka_ingestion.job.yml         # Scheduled previous-day Kafka job
├── config/
│   └── domains/
│       ├── mv/
│       │   ├── tables_config.yml       # SQL Server tables for domain MV
│       │   ├── apis_config.yml         # REST APIs for domain MV
│       │   └── connector_config.yml    # Secret scopes & JDBC options
│       ├── wolf/
│       │   ├── tables_config.yml
│       │   ├── apis_config.yml
│       │   └── connector_config.yml
│       └── kafka_example/
│           ├── connector_config.yml    # Brokers, Kafka options & secret references
│           └── kafka_config.yml        # Topics, payload schemas & target tables
├── src/
│   ├── connectors/
│   │   ├── generic_connector.py        # SQL Server JDBC connector
│   │   ├── mongodb_connector.py        # MongoDB Spark connector
│   │   ├── kafka_batch_connector.py    # Bounded Kafka offset-range reader
│   │   └── generic_api_connector.py    # REST API connector (auth + pagination)
│   ├── ingestors/
│   │   ├── generic_ingestor.py         # SQL / Mongo → ADLS / Unity Catalog
│   │   ├── generic_api_ingestor.py     # API → ADLS / Unity Catalog
│   │   └── kafka_daily_ingestor.py     # Previous event date → Delta
│   └── utils/
│       ├── config_loader.py            # YAML config loader
│       └── secret_manager.py           # Databricks secrets helper
├── notebooks/
│   ├── main_ingestion.py               # Entry point — SQL Server
│   ├── main_kafka_ingestion.py         # Entry point — previous-day Kafka
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
  name: jdbc-ingestion-engine

targets:
  dev:
    workspace:
      host: https://adb-<your-workspace-id>.azuredatabricks.net
```

### 4. Deploy and run

```bash
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run   -t dev jdbc_ingestion
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

    columns:                          # Optional: only ingest these columns
      - "ProductID"                   #   (SELECT pushdown). Omit for all columns.
      - "ProductName"
      - "UnitPrice"

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

#### Column selection (`columns`)

Add an optional `columns:` list to any table to let the source owner pick exactly
which columns are ingested. The projection is **pushed down to the source query**
(`SELECT col1, col2 FROM ...`), so only the requested columns travel over JDBC —
better performance and no unwanted PII columns.

- **Omitted** → all columns are read (`SELECT *`), unchanged default behavior.
- **Validation** → each name must be a plain SQL identifier (optionally
  `schema.col` / `alias.col`); anything else is rejected to prevent SQL injection.
- **Technical columns auto-included** → if you restrict `columns`, the engine adds
  `incremental_column` and `merge_key` automatically when missing (they are needed
  for the incremental filter and the MERGE upsert), logging each addition.
- **derived_columns caveat** → a `derived_columns` expression (e.g.
  `year(OrderDate)`) references a *source* column; if you use `columns`, list that
  source column explicitly, otherwise the derived/partition column cannot be built.

```yaml
- table_name: "dbo.Orders"
  target_table: "orders"
  load_mode: "incremental"
  incremental_column: "OrderDate"     # auto-added to the projection if omitted
  merge_key: "OrderID"                # auto-added to the projection if omitted
  columns:
    - "OrderID"
    - "Amount"
    - "OrderDate"                     # keep it: referenced by derived_columns below
  derived_columns:
    order_year: "year(OrderDate)"
```

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

There is one file per domain. SQL Server domains use the JDBC settings below; no
`api_secrets` section is needed unless the domain also uses APIs:

```yaml
domain: mv

connector:
  type: "sqlserver"
  auth_type: "sql"                    # sql | service_principal | managed_identity | access_token
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

For MongoDB, store the connection URI in a Databricks Secret Scope. The database
and default collection are configuration values; a collection can be overridden
for an individual table. The MongoDB Spark Connector is installed by the job.

```yaml
domain: mongo_app

connector:
  type: "mongodb"
  database: "application"
  collection: "events"                 # Default collection for connection test
  secret_scope: "mongodb-app"
  uri_secret_key: "connection_uri"
```

In `tables_config.yml`, use the collection as the source, or set `collection`
explicitly when `table_name` is a logical identifier:

```yaml
domain: mongo_app

tables:
  - table_name: "event_records"
    collection: "events"               # Overrides connector.collection
    target_table: "events"
    load_mode: "incremental"
    incremental_column: "updated_at"
    write_mode: "unity_catalog"
    filter_query: '{"status": "active"}'   # Optional MongoDB $match document
    read_options:
      aggregation_pipeline:
        - $project:
            _id: 0
            event_id: 1
            updated_at: 1
            status: 1

global_options:
  add_metadata_columns: true
```

The job uses the MongoDB Spark Connector `10.4.1` with Spark 3.5 / Scala 2.12.
Add the domain to the `domains` variable in `databricks.yml` to run it through
the shared ingestion job.

Create the scope and store the URI there (not in the YAML file):

```bash
databricks secrets create-scope --scope mongodb-app
databricks secrets put --scope mongodb-app --key connection_uri --string-value "mongodb+srv://<user>:<password>@<cluster>/?retryWrites=true&w=majority"
```

### Kafka daily ingestion

The Kafka job reads the topic's retained offsets (`earliest` to the current
`latest`) and filters records by event date. This ensures late-arriving messages
whose payload timestamp is yesterday are not missed, but means the topic must
retain the period you need and the job scans retained records on each run.
`event_timestamp_field` must be a top-level `timestamp` in the configured JSON
schema. If its value is null, the Kafka record timestamp is used. The job
defaults to yesterday in the configured timezone (`Europe/Paris` in the example);
set `run_date` to `YYYY-MM-DD` for a backfill.

The output table must be a Delta table if it already exists. Its schema must
match the configured JSON schema. The job can also create the table if it does
not exist. Reruns atomically replace only the requested event date using Delta
`replaceWhere`, leaving rows from other dates unchanged.

```yaml
# config/domains/kafka_example/connector_config.yml
domain: kafka_example
connector:
  type: kafka
  bootstrap_servers: "broker1.example.com:9093,broker2.example.com:9093"
  secret_scope: "kafka-example"
  options:
    kafka.security.protocol: "SASL_SSL"
    kafka.sasl.mechanism: "PLAIN"
  secret_options:
    kafka.sasl.jaas.config: "sasl_jaas_config"
```

```yaml
# config/domains/kafka_example/kafka_config.yml
domain: kafka_example
timezone: "Europe/Paris"
topics:
  - topic: "events"
    target_table: "events"
    event_timestamp_field: "event_timestamp"
    schema:
      type: "struct"
      fields:
        - name: "event_id"
          type: "string"
          nullable: true
          metadata: {}
        - name: "event_timestamp"
          type: "timestamp"
          nullable: true
          metadata: {}
        - name: "event_type"
          type: "string"
          nullable: true
          metadata: {}
```

Store the complete JAAS configuration as a secret instead of putting credentials
in YAML, for example in secret scope `kafka-example`, key `sasl_jaas_config`.
Add the domain to `kafka_domains` in `databricks.yml` (and the relevant target)
to enable it. The daily job is deployed paused; unpause it after setting the
broker, secret, target catalog/schema, and domain list. The Kafka Spark connector
is provisioned by the job for Spark 3.5 / Scala 2.12.

```yaml
# databricks.yml (default list or target-specific variables)
kafka_domains:
  - name: "kafka_example"
```

#### JDBC Authentication (`auth_type`)

Authentication is chosen **per data source** via `auth_type`, so a heterogeneous
fleet of JDBC databases can each use the right strategy without any code change.

| `auth_type` | How it authenticates | Secret required? | Where it applies |
|-------------|----------------------|:----------------:|------------------|
| `sql` *(default)* | SQL login — `username` / `password` from the secret scope | Yes (SQL password) | Any JDBC DB (SQL Server on-prem, PostgreSQL, MySQL, Oracle…) |
| `service_principal` | Entra ID SP — `authentication=ActiveDirectoryServicePrincipal`, `user=client_id`, `password=client_secret` | Yes (SP **client secret**) | Azure SQL DB / MI / Synapse |
| `managed_identity` | Entra ID managed identity of the Databricks compute — `authentication=ActiveDirectoryManagedIdentity` | **No — zero secret** | Azure SQL DB / MI / Synapse |
| `access_token` | Pre-generated Entra token via `accessToken` (obtained from the SP with MSAL) | Yes (SP client secret to mint the token) | Azure SQL DB / MI / Synapse |

> **"Service principal instead of secrets" — the honest answer:** only
> `managed_identity` is truly secret-free, and it requires the Databricks compute
> to run under an Entra **managed identity** (e.g. via an Access Connector) that is
> mapped to a DB user with the right grants. `service_principal` and
> `access_token` do use the SP, but you still store its **client secret** in a
> scope — you're replacing a SQL password with the SP secret, not removing secrets.
> Entra-based auth only works on **Azure SQL / MI / Synapse**; a plain on-prem SQL
> Server or a non-Azure engine (PostgreSQL, Oracle…) stays on `sql`.

**`secret_keys` per strategy:**

```yaml
# service_principal (Azure SQL) — SP client id + secret
connector:
  type: "sqlserver"
  auth_type: "service_principal"
  secret_scope: "sqlserver-mydb"
  secret_keys:
    hostname: "hostname"
    port: "port"
    database: "database"
    client_id: "sp_client_id"
    client_secret: "sp_client_secret"

# managed_identity (Azure SQL) — ZERO secret; client_id only for a user-assigned MI
connector:
  type: "sqlserver"
  auth_type: "managed_identity"
  secret_scope: "sqlserver-mydb"
  secret_keys:
    hostname: "hostname"
    port: "port"
    database: "database"
    # client_id: "mi_client_id"   # omit for a system-assigned managed identity

# access_token (Azure SQL) — token minted at runtime from the SP (needs the `msal` library)
connector:
  type: "sqlserver"
  auth_type: "access_token"
  secret_scope: "sqlserver-mydb"
  secret_keys:
    hostname: "hostname"
    port: "port"
    database: "database"
    client_id: "sp_client_id"
    client_secret: "sp_client_secret"
    tenant_id: "tenant_id"
```

For a **mixed domain** (SQL Server + APIs, like WOLF), add an `api_secrets` section:

```yaml
domain: wolf

connector:
  type: "sqlserver"
  auth_type: "sql"
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
| `dev` | development | `dev_bronze` | JDBC manual; Kafka daily job paused |
| `staging` | development | `staging_bronze` | JDBC manual; Kafka daily job paused |
| `prod` | production | `bronze` | JDBC daily (02:00); Kafka daily job paused |

```bash
# Validate bundle syntax
databricks bundle validate

# Deploy to an environment
databricks bundle deploy -t dev
databricks bundle deploy -t staging
databricks bundle deploy -t prod
```

> **Note**: a Databricks Asset Bundle deploys **all included resources** from `resources/*.yml`. The JDBC/Mongo job fans out over `domains`; the Kafka daily job fans out over `kafka_domains` and is deployed paused.

### CI/CD (GitHub Actions)

Two workflows under `.github/workflows/` automate validation and deployment. Auth
uses **OAuth M2M service principals** (one per environment) — no PATs.

| Workflow | Trigger | Action |
|----------|---------|--------|
| `ci.yml` | Pull request → `master` (or manual) | Unit tests (PySpark) + `bundle validate -t dev` |
| `deploy.yml` | Push to `master` | Deploy to `staging` |
| `deploy.yml` | GitHub **Release** published | Deploy to `prod` (approval-gated) |
| `deploy.yml` | Manual `workflow_dispatch` | Deploy to the chosen target (`dev` / `staging` / `prod`) |

**Setup (one-time):**

1. Create a Databricks **service principal per environment** and grant it OAuth
   (M2M) access + workspace/catalog permissions.
2. In GitHub → **Settings → Environments**, create `dev`, `staging`, `production`.
   Add **required reviewers** on `production` to gate prod deploys with approval.
3. For each environment, add the secrets:
   - `DATABRICKS_CLIENT_ID`
   - `DATABRICKS_CLIENT_SECRET`

   The workspace **host** is read from the matching target in `databricks.yml`
   (replace the placeholder `adb-...` hosts with your real workspace URLs).

**Promotion flow:** open a PR (CI validates) → merge to `master` (auto-deploy to
staging) → publish a Release (deploy to prod after approval).


---

## Running Jobs

The engine defines **one generic, reusable job** in `resources/jdbc_ingestion.job.yml`:

| Job key | Sources | Fan-out | Schedule |
|---------|---------|---------|----------|
| `jdbc_ingestion` | JDBC only | one run of `main_ingestion.py` per entry in `domains` (parallel) | Daily 02:00 CET |

The single `ingest_jdbc_domains` task is a **`for_each`** over the `domains` variable
(defined in `databricks.yml`). For each domain it runs `main_ingestion.py` with
`domain_name` and `adls_base_path` taken from that domain entry. The notebook then
loads `config/domains/<domain>/` to connect to the JDBC database and writes — per
each table's `write_mode` — Parquet files to ADLS and/or Unity Catalog Delta tables.

```bash
# Trigger manually (runs every domain in the `domains` list)
databricks bundle run -t dev jdbc_ingestion
databricks bundle run -t prod jdbc_ingestion
```

### Onboarding a New JDBC Source

Each developer onboards a **new data source with their own config file** — no code
and no new job are required. The same generic `jdbc_ingestion` job picks it up.

1. Create the domain config directory (this is *your* config file set):
```bash
mkdir -p config/domains/my_source
cp config/domains/mv/tables_config.yml     config/domains/my_source/
cp config/domains/mv/connector_config.yml  config/domains/my_source/
```

2. Edit the two YAMLs:
   - Set `domain: my_source` in both files.
   - In `connector_config.yml`, set the JDBC `driver`, `secret_scope` and `secret_keys`
     for your database. The built-in JDBC URL and authentication currently target
     SQL Server; other JDBC databases (for example PostgreSQL, Oracle or MySQL)
     require adapting the connector URL, driver and authentication before use.
   - In `tables_config.yml`, declare the tables to ingest and, per table, the
     `write_mode` (`adls`, `unity_catalog`, or `both`).

3. Create the secret scope holding your DB credentials:
```bash
databricks secrets create-scope --scope sqlserver-my_source
databricks secrets put --scope sqlserver-my_source --key hostname --string-value "..."
databricks secrets put --scope sqlserver-my_source --key username --string-value "..."
databricks secrets put --scope sqlserver-my_source --key password --string-value "..."
# + port, database
```

4. Register the domain in `databricks.yml` by adding one entry to the `domains`
   variable (in the `default:` block and/or the relevant target):
```yaml
variables:
  domains:
    default:
      - name: "my_source"
        adls_base_path: "abfss://my-source-bronze@yourstorageaccount.dfs.core.windows.net"
```

5. Deploy — the existing job now ingests your source automatically:
```bash
databricks bundle deploy -t dev
databricks bundle run   -t dev jdbc_ingestion
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

**Bundle name**: `jdbc-ingestion-engine`  
**Version**: 1.1.0  
**Last updated**: April 2026  
**Maintained by**: Data Engineering Team
