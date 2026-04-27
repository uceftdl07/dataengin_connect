# 🚀 Deployment Guide

Complete step-by-step guide for deploying the SQL Server & API Ingestion Framework to Databricks.

## Prerequisites

- [ ] Databricks workspace (Azure Databricks)
- [ ] Unity Catalog enabled
- [ ] ADLS Gen2 storage account configured
- [ ] Databricks CLI installed
- [ ] Access to create Secret Scopes
- [ ] SQL Server databases accessible from Databricks
- [ ] API credentials for external APIs

## Step 1: Install Databricks CLI

```bash
# Install
pip install databricks-cli

# Verify
databricks --version
```

## Step 2: Configure Databricks CLI

```bash
databricks configure

# You'll be prompted for:
# - Databricks Host (e.g., https://adb-1234567890123456.7.azuredatabricks.net)
# - Token (create in User Settings → Access Tokens)
```

Verify connection:

```bash
databricks workspace ls /
```

## Step 3: Update Configuration

### 3.1 Edit `databricks.yml`

Update workspace URLs for each environment:

```yaml
targets:
  dev:
    workspace:
      host: https://adb-YOUR-DEV-WORKSPACE.7.azuredatabricks.net
  
  staging:
    workspace:
      host: https://adb-YOUR-STAGING-WORKSPACE.7.azuredatabricks.net
  
  prod:
    workspace:
      host: https://adb-YOUR-PROD-WORKSPACE.7.azuredatabricks.net
```

### 3.2 Update ADLS Paths

In `databricks.yml`, update:

```yaml
variables:
  adls_base_path:
    default: "abfss://bronze@YOUR_STORAGE_ACCOUNT.dfs.core.windows.net"

targets:
  dev:
    variables:
      adls_base_path: "abfss://bronze-dev@YOUR_STORAGE_ACCOUNT.dfs.core.windows.net"
```

### 3.3 Update Email Notifications

In the relevant job file under `resources/` (for example `resources/jobs_mv.yml` or `resources/jobs_wolf.yml`), update email addresses:

```yaml
email_notifications:
  on_failure:
    - "your-team@yourcompany.com"
```

## Step 4: Create Secret Scopes

### 4.1 SQL Server Secrets (Domain MV)

```bash
# Create scope
databricks secrets create-scope --scope sqlserver-mv

# Add secrets
databricks secrets put --scope sqlserver-mv --key hostname
# Enter: your-sqlserver.database.windows.net

databricks secrets put --scope sqlserver-mv --key port
# Enter: 1433

databricks secrets put --scope sqlserver-mv --key database
# Enter: YourDatabase

databricks secrets put --scope sqlserver-mv --key username
# Enter: your_username

databricks secrets put --scope sqlserver-mv --key password
# Enter: your_password
```

### 4.2 SQL Server Secrets (Domain WOLF)

```bash
databricks secrets create-scope --scope sqlserver-wolf

databricks secrets put --scope sqlserver-wolf --key hostname
databricks secrets put --scope sqlserver-wolf --key port
databricks secrets put --scope sqlserver-wolf --key database
databricks secrets put --scope sqlserver-wolf --key username
databricks secrets put --scope sqlserver-wolf --key password
```

### 4.3 API Secrets (Domain MV)

#### Customer API (OAuth2)

```bash
databricks secrets create-scope --scope api-mv-customer_api

databricks secrets put --scope api-mv-customer_api --key oauth_client_id
# Enter: your_client_id

databricks secrets put --scope api-mv-customer_api --key oauth_client_secret
# Enter: your_client_secret
```

#### Orders API (Bearer)

```bash
databricks secrets create-scope --scope api-mv-orders_api

databricks secrets put --scope api-mv-orders_api --key bearer_token
# Enter: your_bearer_token
```

#### Inventory API (API Key)

```bash
databricks secrets create-scope --scope api-mv-inventory_api

databricks secrets put --scope api-mv-inventory_api --key api_key
# Enter: your_api_key
```

### 4.4 API Secrets (Domain WOLF)

```bash
# Analytics API
databricks secrets create-scope --scope api-wolf-analytics_api
databricks secrets put --scope api-wolf-analytics_api --key bearer_token

# Reporting API
databricks secrets create-scope --scope api-wolf-reporting_api
databricks secrets put --scope api-wolf-reporting_api --key api_key
```

### 4.5 Verify Secrets

```bash
# List all scopes
databricks secrets list-scopes

# List keys in a scope (values are hidden)
databricks secrets list --scope sqlserver-mv
```

## Step 5: Validate Configuration

```bash
cd sqlserver-api-ingestion-framework

# Validate bundle
databricks bundle validate

# Expected output: "Validation OK!"
```

If validation fails, check:
- YAML syntax in all config files
- File paths in `databricks.yml`
- All required fields present

## Step 6: Deploy to Dev

```bash
# Deploy
databricks bundle deploy -t dev

# Expected output:
# Uploading bundle files to /Workspace/.bundle/...
# Deploying resources...
# Success!
```

Verify deployment:

```bash
# List deployed jobs
databricks jobs list | grep "Ingestion"
```

## Step 7: Test Run (Dev)

### 7.1 Test SQL Server Ingestion

```bash
databricks bundle run -t dev ingestion_mv
```

Monitor in Databricks UI:
1. Go to **Workflows** → **Jobs**
2. Find `[dev] Ingestion MV Domain`
3. Click on the running job
4. View logs and progress

### 7.2 Test API Ingestion

```bash
databricks bundle run -t dev ingestion_wolf
```

### 7.3 Verify Data

```sql
-- In Databricks SQL editor
USE CATALOG dev_bronze;
USE SCHEMA raw_dev;

SHOW TABLES;

-- Check row counts
SELECT COUNT(*) FROM customers;
SELECT COUNT(*) FROM orders;
SELECT COUNT(*) FROM api_customers;
```

## Step 8: Troubleshooting Dev Deployment

### Issue: "Secret not found"

```bash
# Verify secret exists
databricks secrets list --scope sqlserver-mv

# Recreate if missing
databricks secrets put --scope sqlserver-mv --key hostname
```

### Issue: "Connection refused" (SQL Server)

Check:
- SQL Server firewall allows Databricks IPs
- Hostname/port correct
- Credentials valid

Test connection separately:
```python
# In Databricks notebook
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:sqlserver://YOUR_HOST:1433;databaseName=YOUR_DB") \
    .option("dbtable", "(SELECT 1 as test) as t") \
    .option("user", "USERNAME") \
    .option("password", "PASSWORD") \
    .load()

display(df)
```

### Issue: "Table not found"

```sql
-- Verify catalog/schema exists
SHOW CATALOGS;
SHOW SCHEMAS IN dev_bronze;

-- Create if missing
CREATE CATALOG IF NOT EXISTS dev_bronze;
CREATE SCHEMA IF NOT EXISTS dev_bronze.raw_dev;
```

### Issue: "API 401 Unauthorized"

- Regenerate API credentials
- Check token expiration
- Verify correct secret scope name

## Step 9: Deploy to Staging

Once dev is working:

```bash
# Deploy to staging
databricks bundle deploy -t staging

# Test
databricks bundle run -t staging ingestion_mv
databricks bundle run -t staging ingestion_wolf
```

Verify in staging:

```sql
USE CATALOG staging_bronze;
USE SCHEMA raw_staging;

SHOW TABLES;
SELECT COUNT(*) FROM customers;
```

## Step 10: Deploy to Production

### 10.1 Pre-Production Checklist

- [ ] Dev deployment successful
- [ ] Staging deployment successful
- [ ] All tests passed
- [ ] Production secrets created
- [ ] Production workspace configured
- [ ] Schedules reviewed
- [ ] Notifications configured
- [ ] Runbook prepared

### 10.2 Create Production Secrets

```bash
# Repeat Step 4 for production
databricks secrets create-scope --scope sqlserver-mv
# ... etc
```

### 10.3 Deploy to Prod

```bash
# Deploy
databricks bundle deploy -t prod

# DO NOT run immediately - verify jobs first
```

### 10.4 Verify Jobs Before Running

1. Go to Databricks UI → Workflows → Jobs
2. Verify job configurations:
   - Cluster sizes appropriate
   - Schedules correct
   - Notifications set
   - Tags present

### 10.5 First Production Run (Manual)

```bash
# Run manually first to verify
databricks bundle run -t prod ingestion_mv
```

Monitor closely:
- Check logs for errors
- Verify data landed correctly
- Check row counts match expectations

### 10.6 Enable Schedules

Once manual run succeeds:

1. Go to job in UI
2. Click "Edit"
3. Change schedule from PAUSED to UNPAUSED
4. Save

Or update the relevant file in `resources/`:

```yaml
schedule:
  pause_status: "UNPAUSED"  # Changed from PAUSED
```

Then redeploy:

```bash
databricks bundle deploy -t prod
```

## Step 11: Post-Deployment

### 11.1 Set Up Monitoring

Create alerts for:
- Job failures (email)
- Data freshness (check last ingestion timestamp)
- Row count anomalies

### 11.2 Document Operations

Create runbook with:
- How to add new tables/APIs
- Troubleshooting steps
- Escalation procedures
- Maintenance windows

### 11.3 Set Up Data Quality Checks

```sql
-- Example: Check for NULL values
SELECT COUNT(*) as null_count
FROM bronze.raw.customers
WHERE customer_id IS NULL;

-- Example: Check data freshness
SELECT MAX(_ingestion_timestamp) as last_ingestion
FROM bronze.raw.orders;
```

## Rollback Procedure

If production deployment fails:

### Option 1: Redeploy Previous Version

```bash
# Checkout previous version
git checkout <previous-commit>

# Redeploy
databricks bundle deploy -t prod
```

### Option 2: Pause Jobs

```bash
# Manually pause jobs in UI
# Or update databricks.yml and redeploy
```

### Option 3: Manual Intervention

If data corruption occurred:
1. Stop all jobs
2. Restore from backup (if available)
3. Re-run ingestion from last good state

## Maintenance

### Weekly
- Review job logs for errors
- Check data freshness
- Verify row counts

### Monthly
- Rotate secrets
- Review and optimize queries
- Check cluster utilization
- Review storage costs

### Quarterly
- Update dependencies
- Review and update documentation
- Disaster recovery test

## Additional Resources

### Databricks CLI Commands

```bash
# List jobs
databricks jobs list

# Get job details
databricks jobs get --job-id JOB_ID

# List runs for a job
databricks jobs runs list --job-id JOB_ID

# Get run output
databricks jobs runs get-output --run-id RUN_ID
```

### Unity Catalog Commands

```sql
-- Grant permissions
GRANT SELECT ON TABLE bronze.raw.customers TO `data-analysts`;

-- Revoke permissions
REVOKE SELECT ON TABLE bronze.raw.customers FROM `data-analysts`;

-- Show grants
SHOW GRANTS ON TABLE bronze.raw.customers;
```

## Support

For issues during deployment:

1. Check logs in Databricks UI
2. Review this guide
3. Check README.md troubleshooting section
4. Contact: data-engineering@yourcompany.com

## Appendix: Environment Variables

Optional: Use environment variables for sensitive values:

```bash
# Set environment variables
export DATABRICKS_HOST="https://..."
export DATABRICKS_TOKEN="..."

# CLI will use these automatically
databricks bundle deploy
```

## Checklist Summary

### Pre-Deployment
- [ ] Databricks CLI installed and configured
- [ ] All secrets created
- [ ] Configuration files updated
- [ ] Bundle validated

### Dev Deployment
- [ ] Deployed successfully
- [ ] Jobs run successfully
- [ ] Data verified in Unity Catalog

### Staging Deployment
- [ ] Deployed successfully
- [ ] Full test cycle completed
- [ ] Performance acceptable

### Prod Deployment
- [ ] All secrets created
- [ ] Manual run successful
- [ ] Schedules configured
- [ ] Monitoring set up
- [ ] Team notified

---

**Last Updated**: February 2026  
**Version**: 1.0.0
