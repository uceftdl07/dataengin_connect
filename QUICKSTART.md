# ⚡ Quick Start - 10 Minutes to First Ingestion

Get your first pipeline running in 10 minutes.

## Prerequisites (2 minutes)

```bash
# 1. Install Databricks CLI
pip install databricks-cli

# 2. Configure CLI
databricks configure
# Enter your workspace URL and token
```

## Setup (5 minutes)

### 1. Create ONE Secret Scope

```bash
# For SQL Server (domain MV)
databricks secrets create-scope --scope sqlserver-mv

databricks secrets put --scope sqlserver-mv --key hostname
# Enter: your-server.database.windows.net

databricks secrets put --scope sqlserver-mv --key port
# Enter: 1433

databricks secrets put --scope sqlserver-mv --key database
# Enter: YourDatabase

databricks secrets put --scope sqlserver-mv --key username
# Enter: db_user

databricks secrets put --scope sqlserver-mv --key password
# Enter: your_password
```

### 2. Update Config

Edit `databricks.yml`:

```yaml
targets:
  dev:
    workspace:
      host: https://adb-3059738143768593.13.azuredatabricks.net/
```

Edit `config/domains/mv/tables_config.yml` - keep only ONE table for testing:

```yaml
tables:
  - table_name: "dbo.YourSmallTable"  # Replace with actual table
    target_table: "test_table"
    load_mode: "full"
    enabled: true
    write_mode: "unity_catalog"
    adls_path: "test/full"
    read_options:
      numPartitions: 2
      fetchsize: 1000
```

Set all other tables to `enabled: false`.

## Deploy & Run (3 minutes)

```bash
# Navigate to project
cd jdbc-ingestion-engine

# Validate
databricks bundle validate

# Deploy
databricks bundle deploy -t dev

# Run
databricks bundle run -t dev jdbc_ingestion
```

## Verify

```sql
-- In Databricks SQL editor
USE CATALOG dev_bronze;
USE SCHEMA raw_dev;

SHOW TABLES;

SELECT * FROM test_table LIMIT 10;
SELECT COUNT(*) FROM test_table;
```

## Next Steps

Once this works:

1. **Enable more tables**: Set `enabled: true` in `tables_config.yml`
2. **Add APIs**: Configure in `apis_config.yml` with secrets
3. **Add incremental**: Set `load_mode: "incremental"`
4. **Deploy to prod**: `databricks bundle deploy -t prod`

## Troubleshooting

### "Secret not found"
```bash
databricks secrets list --scope sqlserver-mv
```
If empty, recreate secrets (step 1).

### "Connection refused"
- Check SQL Server firewall allows Databricks
- Verify hostname/port
- Test credentials manually

### "Table not found" in Unity Catalog
```sql
CREATE CATALOG IF NOT EXISTS dev_bronze;
CREATE SCHEMA IF NOT EXISTS dev_bronze.raw_dev;
```

## Success Indicators

✅ Job shows as "Succeeded" in Databricks UI  
✅ Table appears in Unity Catalog  
✅ Row count > 0  
✅ `_ingestion_timestamp` column present

## Full Documentation

- [README.md](README.md) - Complete guide
- [DEPLOYMENT.md](DEPLOYMENT.md) - Detailed deployment
- [PRE_DEPLOYMENT_CHECKLIST.md](PRE_DEPLOYMENT_CHECKLIST.md) - Validation

---

**Time to first success**: ~10 minutes
