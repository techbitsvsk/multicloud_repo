# Operational Runbook – Iceberg Multi-Cloud Pipeline

## 1. Monitoring & Alerting

### AWS Glue
- **CloudWatch Metrics**: Glue publishes `glue.driver.*` and `glue.ALL.*` metrics automatically when `--enable-metrics true` is set.
- **CloudWatch Logs**: Continuous logging enabled via `--enable-continuous-cloudwatch-log true`. Pipeline JSON logs appear in `/aws-glue/jobs/output/`.
- **Alerts**: Create CloudWatch Alarms on `glue.driver.aggregate.numFailedTasks > 0` and job duration exceeding 2× baseline.

### Microsoft Fabric
- **Spark Monitoring Hub**: Navigate to Fabric Workspace → Monitor → Apache Spark applications. View stage-level metrics, Spark UI, and driver logs.
- **Fabric Audit Logs**: Available via Microsoft Purview compliance portal. Track who queried which tables and when.
- **Alerts**: Use Power Automate flows triggered by notebook failure status.

### On-Prem / Local
- **Spark UI**: Available at `http://localhost:4040` during job execution.
- **Log Files**: JSON logs written to stdout; redirect to file for persistence:
  ```powershell
  spark-submit spark_job.py 2>&1 | Tee-Object -FilePath pipeline.log
  ```

## 2. Retry & Failure Handling

The pipeline has built-in retry logic (configurable via `max_retries` / `retry_delay_seconds` in config):
- Each phase (bronze, silver, gold) is independently retried.
- Idempotency is ensured via `createOrReplace()` – re-runs produce identical results.
- On terminal failure, the job exits with code 1 and logs a full stack trace.

**Manual recovery**: If a phase fails after all retries, fix the root cause and re-run the entire job. Because each phase is idempotent, completed phases will simply overwrite with the same data.

## 3. Schema Evolution

Iceberg supports schema evolution natively. To add a column:

```sql
ALTER TABLE iceberg.silver.orders ADD COLUMN new_col STRING;
```

Rules:
- Adding columns: always safe (old data returns NULL for new columns).
- Renaming columns: supported, but downstream consumers must update.
- Dropping columns: supported but destructive – take a snapshot first.
- Type promotion (e.g. `int` → `long`): supported for widening only.

After schema changes, update the `ORDERS_SCHEMA` / `LINEITEM_SCHEMA` in `spark_job.py` to match.

## 4. Iceberg Table Maintenance

### Compaction (rewrite small files)

```sql
-- From Spark SQL / Athena / Fabric:
CALL iceberg.system.rewrite_data_files(table => 'iceberg.silver.orders');
CALL iceberg.system.rewrite_data_files(table => 'iceberg.silver.lineitem');
```

**Schedule**: Run compaction weekly or when average file size drops below 64 MB.

### Expire Snapshots

```sql
CALL iceberg.system.expire_snapshots(
  table => 'iceberg.gold.revenue_by_order_date',
  older_than => TIMESTAMP '2025-01-01 00:00:00',
  retain_last => 5
);
```

**Schedule**: Run monthly. Keep at least 5 snapshots for time-travel queries.

### Remove Orphan Files

```sql
CALL iceberg.system.remove_orphan_files(
  table => 'iceberg.silver.orders',
  older_than => TIMESTAMP '2025-01-01 00:00:00'
);
```

**Caution**: Only run this after confirming no concurrent writers.

## 5. Cost Considerations

| Platform | Primary Cost Drivers | Optimisation |
|----------|---------------------|-------------|
| AWS Glue | DPU-hours, S3 storage, S3 requests | Use G.1X workers, minimise shuffle, compact files |
| Fabric   | CU-seconds (Spark), OneLake storage | Use starter pools, avoid idle notebooks |
| On-Prem  | Hardware, network, MinIO storage | Right-size Spark executors, S3A `fs.s3a.multipart.size=64M` |

## 6. Migrations & Rollbacks

### Rolling back a bad pipeline run
Iceberg time-travel makes this straightforward:

```sql
-- Find snapshots
SELECT * FROM iceberg.silver.orders.snapshots;

-- Rollback to a known-good snapshot
CALL iceberg.system.rollback_to_snapshot('iceberg.silver.orders', <snapshot_id>);
```

### Migrating between platforms
The Iceberg Hadoop catalog stores all metadata alongside data files. To migrate:

1. Copy the entire warehouse directory (e.g. `s3://bucket/iceberg/` → `abfss://container@account.dfs.core.windows.net/iceberg/`).
2. Update `ICEBERG_WAREHOUSE` env var to point to the new location.
3. Validate with `SELECT count(*) FROM iceberg.gold.top_customers`.

## 7. Access Control Quick Reference

### Grant analyst read access

**AWS (Lake Formation CLI)**:
```bash
aws lakeformation grant-permissions \
  --principal DataLakePrincipalIdentifier=arn:aws:iam::ACCOUNT:role/iceberg-pipeline-dev-data-analyst \
  --permissions SELECT \
  --resource '{"Table":{"DatabaseName":"gold","TableWildcard":{}}}'
```

**Azure (CLI)**:
```bash
az role assignment create \
  --assignee-object-id <analyst-group-oid> \
  --role "Storage Blob Data Reader" \
  --scope /subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.Storage/storageAccounts/<sa>
```

**On-Prem (Spark SQL)**:
```sql
GRANT SELECT ON iceberg.gold TO analyst1;
```

### Revoke access

**AWS**: Replace `grant-permissions` with `revoke-permissions` using the same arguments.  
**Azure**: `az role assignment delete --assignee-object-id <oid> --role "Storage Blob Data Reader" --scope <scope>`  
**On-Prem**: `REVOKE SELECT ON iceberg.gold FROM analyst1;`

## 8. Security Checklist

- [ ] No hardcoded secrets in code or IaC
- [ ] S3 / ADLS encryption at rest enabled (KMS / Microsoft-managed keys)
- [ ] TLS 1.2+ enforced for all connections
- [ ] VPC endpoints (AWS) / private endpoints (Azure) configured for production
- [ ] CloudTrail / Fabric audit logging enabled
- [ ] IAM roles / managed identities used (no long-lived access keys)
- [ ] MinIO TLS enabled in production (not in local dev)
- [ ] Lake Formation / OneLake item permissions enforced for column/row filtering

## 9. Acceptance Criteria Checklist

- [ ] Bronze tables exist and row counts match raw CSV files
- [ ] Silver `O_TOTALPRICE` is `decimal(15,2)`, not `string`
- [ ] Silver tables are partitioned by year
- [ ] Silver has no NULL primary keys
- [ ] Gold `revenue_by_order_date` returns positive revenue per date
- [ ] Gold `top_customers` is sorted by revenue descending
- [ ] Athena can run `SELECT * FROM iceberg.gold.revenue_by_order_date LIMIT 10`
- [ ] Fabric Warehouse can query via Lakehouse shortcut
- [ ] On-prem MinIO smoke test passes (`pytest tests/test_smoke.py`)
