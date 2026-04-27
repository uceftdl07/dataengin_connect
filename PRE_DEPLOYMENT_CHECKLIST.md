# ✅ Pre-Deployment Checklist

Complete this checklist before deploying to any environment.

## Configuration Files

### databricks.yml
- [ ] Workspace URLs correct for each environment
- [ ] ADLS paths accessible
- [ ] Catalog names match Unity Catalog
- [ ] Email addresses valid

### config/domains/*/tables_config.yml
- [ ] All `table_name` values exist in SQL Server
- [ ] `incremental_column` exists and is indexed
- [ ] `merge_key` uniquely identifies rows
- [ ] `filter_query` has valid SQL syntax

### config/domains/*/apis_config.yml
- [ ] All `base_url` values are accessible
- [ ] `auth_type` matches API requirements
- [ ] `pagination.type` matches API
- [ ] `response_path` correctly extracts data

## Secret Scopes

### SQL Server
- [ ] Scope exists: `databricks secrets list-scopes | grep sqlserver`
- [ ] Has all keys: hostname, port, database, username, password
- [ ] Credentials tested and valid

### APIs
- [ ] Scope exists for each API
- [ ] Has required auth keys (bearer_token, api_key, etc.)
- [ ] Tokens not expired

## Connectivity

### SQL Server
- [ ] Firewall allows Databricks IPs
- [ ] Can connect from Databricks notebook
- [ ] User has SELECT permission

### APIs
- [ ] Accessible from Databricks
- [ ] Credentials valid
- [ ] Rate limits known and configured

## Unity Catalog

- [ ] Target catalogs exist
- [ ] Permissions allow CREATE TABLE
- [ ] Schemas exist or will auto-create

```sql
SHOW CATALOGS;
SHOW GRANTS ON CATALOG dev_bronze;
```

## Validation

```bash
# Validate bundle
databricks bundle validate
# Expected: "Validation OK!"
```

## Testing (Dev Only)

- [ ] Deploy to dev: `databricks bundle deploy -t dev`
- [ ] Run one table: `databricks bundle run -t dev ingestion_mv`
- [ ] Verify data in Unity Catalog
- [ ] Check logs for errors
- [ ] Verify row counts

## Sign-Off

**Deployed by**: ________________  
**Date**: ________________  
**Approved by**: ________________  
**Environment**: dev / staging / **prod**

## Deployment Commands

```bash
# Dev
databricks bundle deploy -t dev
databricks bundle run -t dev ingestion_mv

# Staging  
databricks bundle deploy -t staging

# Production
databricks bundle deploy -t prod
```

---

**Version**: 1.0.0
