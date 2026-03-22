# Iceberg Multi-Cloud Pipeline – Complete Deliverable

---

## SECTION 1: High-Level Approach

### Architecture

A single PySpark job (`spark_job.py`) implements a **Bronze → Silver → Gold** medallion architecture on Apache Iceberg tables. The job is platform-agnostic: a thin factory layer (`spark_factory.py`) creates the correct `SparkSession` for each runtime (AWS Glue, Microsoft Fabric, local Spark + MinIO). The Iceberg **Hadoop catalog** is used everywhere because it stores metadata alongside data files on the object store — changing only the `ICEBERG_WAREHOUSE` URI switches between S3, ABFS, and S3A/MinIO with zero code changes.

### Components Per Platform

#### AWS
- **Compute**: Glue 4.0 ETL job (Spark 3.3, Iceberg bundled via `--datalake-formats=iceberg`)
- **Storage**: S3 (raw bucket + Iceberg warehouse bucket), KMS encryption
- **Catalog**: Iceberg Hadoop catalog on S3 (not Glue Data Catalog — avoids Glue-specific lock-in)
- **Query**: Athena v3 queries Iceberg tables via `CREATE TABLE ... LOCATION 's3://...'`
- **Governance**: Lake Formation grants, IAM roles per persona, CloudTrail audit
- **IaC**: Terraform (`terraform/aws/main.tf`)

#### Microsoft Fabric / Azure
- **Compute**: Fabric Data Engineering notebook (Spark 3.5)
- **Storage**: ADLS Gen2 (OneLake-backed), abfss:// paths
- **Catalog**: Iceberg Hadoop catalog on ADLS Gen2 (Fabric Lakehouse shortcuts for discovery)
- **Query**: Fabric Warehouse via Lakehouse SQL endpoint or Spark SQL
- **Governance**: Entra ID groups, Storage RBAC, OneLake item permissions
- **IaC**: Bicep (`bicep/azure.bicep`) + manual Fabric workspace steps

#### On-Prem / Local
- **Compute**: `spark-submit` with `local[*]` master
- **Storage**: MinIO (S3-compatible), S3A Hadoop filesystem
- **Catalog**: Iceberg Hadoop catalog on MinIO
- **Query**: Spark SQL shell or any JDBC client
- **Governance**: MinIO IAM policies, OS/LDAP groups, Spark SQL GRANT
- **Setup**: `scripts/minio_setup.sh`

### File Manifest

```
spark_job.py              Main pipeline (bronze/silver/gold)
spark_factory.py          SparkSession factory per runtime
config.py                 Environment-driven configuration
utils/logging.py          JSON structured logging + metrics
terraform/aws/main.tf     AWS infrastructure (S3, IAM, Glue, Lake Formation)
bicep/azure.bicep         Azure infrastructure (ADLS Gen2, RBAC)
scripts/minio_setup.sh    MinIO bucket/user/policy provisioning
tests/test_smoke.py       Pytest smoke tests
tests/sample_data/        Sample orders.csv & lineitem.csv
env.example               Environment variable template
runbooks/operational_notes.md  Monitoring, maintenance, security runbook
```

---

## SECTION 2: Code (see individual files)

All files are provided separately. Key design notes:

### spark_factory.py
- **AWS Glue**: Initialises `GlueContext` for bookmark support, returns underlying `SparkSession`. Iceberg conf applied via `spark.conf.set()`.
- **Fabric**: Assumes `%%configure` cell already ran. Validates the Iceberg catalog is registered. Does NOT re-create the session.
- **Local**: Builds `SparkSession` with S3A filesystem pointed at MinIO, pulls Iceberg + hadoop-aws JARs from Maven.

### spark_job.py
- **Bronze**: Reads pipe-delimited TPC-H CSVs with explicit schemas (no inference). Uses `writeTo().createOrReplace()` for idempotency.
- **Silver**: Casts strings to `DecimalType`/`DateType`, adds `order_year`/`ship_year` partition columns, derives `revenue` on lineitem, filters null keys, adds `_ingested_at` audit timestamp.
- **Gold**: Two aggregate tables:
  - `revenue_by_order_date` – daily revenue, line count, distinct order count, partitioned by year.
  - `top_customers` – customer spend, order count, date range, ordered by revenue desc.
- Each phase wrapped in `with_retry()` for transient failure resilience.

### Fabric %%configure Block (paste as first cell in notebook)

```json
%%configure -f
{
  "conf": {
    "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
    "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.iceberg.type": "hadoop",
    "spark.sql.catalog.iceberg.warehouse": "abfss://<container>@<storage>.dfs.core.windows.net/iceberg/",
    "spark.sql.defaultCatalog": "iceberg",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
  }
}
```

**Fabric caveats**:
1. `%%configure` must be the **first executable cell**. It is a notebook directive, not Python.
2. Replace `<container>` and `<storage>` with your actual ADLS Gen2 values.
3. Fabric Spark pools default to Delta – Iceberg requires the explicit JAR.
4. Workspace identity must have **Storage Blob Data Contributor** on the storage account.
5. OneLake shortcuts using passthrough identity do NOT work for Iceberg writes today – use direct abfss:// paths.
6. Check your Fabric capacity SKU supports custom JAR packages (F2+ recommended).

### How to Run

#### AWS Glue

```bash
# 1. Upload scripts to S3
aws s3 cp spark_job.py s3://BUCKET/scripts/spark_job.py
aws s3 cp spark_factory.py s3://BUCKET/scripts/spark_factory.py
aws s3 cp config.py s3://BUCKET/scripts/config.py
aws s3 cp utils/ s3://BUCKET/scripts/utils/ --recursive

# 2. Upload raw data
aws s3 cp tests/sample_data/ s3://RAW_BUCKET/tpch/ --recursive

# 3. Run the Glue job
aws glue start-job-run --job-name iceberg-pipeline-dev-job
```

#### Fabric Notebook

1. Create a Fabric workspace and Lakehouse.
2. Upload `orders.csv` and `lineitem.csv` to the Lakehouse Files area under `tpch/`.
3. Create a new notebook attached to the Lakehouse.
4. Paste the `%%configure` block as Cell 1.
5. In Cell 2, set environment variables and paste the pipeline code:
   ```python
   import os
   os.environ["RUNTIME"] = "fabric"
   os.environ["ICEBERG_WAREHOUSE"] = "abfss://container@storage.dfs.core.windows.net/iceberg/"
   os.environ["ICEBERG_CATALOG"] = "iceberg"
   os.environ["RAW_SOURCE_PATH"] = "abfss://container@storage.dfs.core.windows.net/raw/tpch/"
   os.environ["PERSONA"] = "data_engineer"
   
   # Paste contents of config.py, spark_factory.py, utils/logging.py, spark_job.py
   # Or use %run magic to reference uploaded .py files
   ```
6. Run all cells.

#### Local Spark + MinIO (Windows PowerShell)

```powershell
# 1. Start MinIO (Docker or binary)
docker run -d --name minio -p 9000:9000 -p 9001:9001 `
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin `
  minio/minio server /data --console-address ":9001"

# 2. Set up buckets and upload sample data (Git Bash or WSL)
bash scripts/minio_setup.sh

# 3. Set environment variables
$env:RUNTIME = "local"
$env:ICEBERG_WAREHOUSE = "s3a://iceberg-warehouse/iceberg/"
$env:ICEBERG_CATALOG = "iceberg"
$env:RAW_SOURCE_PATH = "s3a://raw-data/tpch/"
$env:S3_ENDPOINT = "http://localhost:9000"
$env:AWS_ACCESS_KEY_ID = "minioadmin"
$env:AWS_SECRET_ACCESS_KEY = "minioadmin"
$env:PERSONA = "data_engineer"

# 4. Run the pipeline
spark-submit `
  --master "local[*]" `
  --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262" `
  spark_job.py

# NOTE on Windows: If spark-submit is not on PATH, use the full path:
#   C:\spark\bin\spark-submit.cmd ...
# Ensure JAVA_HOME and SPARK_HOME are set. Spark 3.5 requires Java 11 or 17.
```

### Access Management & Persona Mapping

| Persona | AWS (IAM + Lake Formation) | Azure/Fabric (Entra + RBAC) | On-Prem (MinIO) |
|---------|---------------------------|----------------------------|-----------------|
| `data_engineer` | Full S3 read/write, Glue catalog CRUD, Lake Formation ALL on all DBs | Storage Blob Data Contributor, Fabric workspace Member | MinIO `data-engineer` policy (full access), Spark SQL `GRANT ALL` |
| `data_analyst` | S3 read-only on iceberg bucket, Athena query, Lake Formation SELECT on gold | Storage Blob Data Reader, Fabric workspace Viewer | MinIO `data-analyst` policy (read iceberg-warehouse), Spark SQL `GRANT SELECT ON gold` |
| `platform_admin` | Full S3, IAM, Lake Formation admin, Glue admin | Owner on storage account, Fabric workspace Admin | MinIO `platform-admin` policy (full), OS root/sudo |

### Security & Best Practices

- **No hardcoded secrets**: AWS uses IAM roles; Azure uses managed identity; MinIO credentials come from env vars only.
- **Encryption at rest**: AWS KMS (see `main.tf`), Azure Storage Service Encryption (Microsoft-managed or CMK via Key Vault).
- **Encryption in transit**: S3/ADLS enforce HTTPS. MinIO should enable TLS in production (`minio server --certs-dir`).
- **Network controls**: AWS – S3 VPC endpoints; Azure – private endpoints + service firewall; MinIO – bind to private IP.
- **Audit**: CloudTrail (AWS), Fabric audit logs (Azure), MinIO audit webhook.
- **Fine-grained access (AWS)**: Lake Formation supports column-level and row-level filtering on Iceberg tables registered in the Glue Data Catalog. For the Hadoop catalog used here, enforce access at the S3 path level or register tables in Glue catalog for Lake Formation FGAC.
- **Fine-grained access (Fabric)**: OneLake item permissions control Read/ReadWrite per Lakehouse item. Column/row security requires Fabric Warehouse T-SQL security policies.

---

## SECTION 3: Validation Steps

### 3.1 Unit / Integration Tests (all platforms)

```powershell
# Windows PowerShell – ensure MinIO is running and env vars are set (see above)
python -m pytest tests/test_smoke.py -v
```

Expected output:
```
tests/test_smoke.py::TestBronze::test_orders_exists PASSED
tests/test_smoke.py::TestBronze::test_lineitem_exists PASSED
tests/test_smoke.py::TestBronze::test_orders_columns PASSED
tests/test_smoke.py::TestSilver::test_orders_typed PASSED
tests/test_smoke.py::TestSilver::test_orders_partitioned PASSED
tests/test_smoke.py::TestSilver::test_lineitem_revenue PASSED
tests/test_smoke.py::TestSilver::test_silver_no_null_keys PASSED
tests/test_smoke.py::TestGold::test_revenue_by_date_exists PASSED
tests/test_smoke.py::TestGold::test_top_customers_exists PASSED
tests/test_smoke.py::TestGold::test_revenue_positive PASSED
tests/test_smoke.py::TestGold::test_top_customer_ordering PASSED
tests/test_smoke.py::TestSQLQueries::test_athena_style_gold_query PASSED
tests/test_smoke.py::TestSQLQueries::test_top_10_customers PASSED
```

### 3.2 End-to-End: AWS (Athena)

After the Glue job completes, query in Athena:

```sql
-- Register the Iceberg table (one-time, if not using Glue Data Catalog)
CREATE TABLE gold_revenue
  WITH (table_type = 'ICEBERG', location = 's3://BUCKET/iceberg/gold/revenue_by_order_date')
AS SELECT 1;  -- dummy; Athena reads existing Iceberg metadata

-- Or query directly if using the Iceberg connector:
SELECT order_date, total_revenue, order_count
FROM iceberg.gold.revenue_by_order_date
WHERE order_year = 1996
ORDER BY total_revenue DESC
LIMIT 10;
```

**Expected**: Rows with positive `total_revenue` for 1996 dates.

### 3.3 End-to-End: Fabric Warehouse

1. In the Fabric Lakehouse, click **New SQL Endpoint**.
2. The Iceberg tables appear under the Lakehouse Tables if shortcuts are configured.
3. Run:
   ```sql
   SELECT TOP 10 customer_key, total_revenue
   FROM iceberg.gold.top_customers
   ORDER BY total_revenue DESC;
   ```

### 3.4 End-to-End: Local MinIO

```powershell
# After pipeline completes, query via spark-sql shell:
spark-sql `
  --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262" `
  --conf "spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog" `
  --conf "spark.sql.catalog.iceberg.type=hadoop" `
  --conf "spark.sql.catalog.iceberg.warehouse=s3a://iceberg-warehouse/iceberg/" `
  --conf "spark.sql.defaultCatalog=iceberg" `
  --conf "spark.hadoop.fs.s3a.endpoint=http://localhost:9000" `
  --conf "spark.hadoop.fs.s3a.access.key=minioadmin" `
  --conf "spark.hadoop.fs.s3a.secret.key=minioadmin" `
  --conf "spark.hadoop.fs.s3a.path.style.access=true" `
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem"

-- Then in the spark-sql shell:
SELECT order_year, SUM(total_revenue) AS rev
FROM gold.revenue_by_order_date
GROUP BY order_year
ORDER BY order_year;

-- Expected (sample data): one or more rows with positive rev values.

SELECT customer_key, total_revenue, order_count
FROM gold.top_customers
LIMIT 5;

-- Expected: 5 rows sorted by total_revenue DESC, all positive.
```

### 3.5 Validation Assertions (sample data)

With the provided 10-row `orders.csv` and 13-row `lineitem.csv`:

| Check | Expected |
|-------|----------|
| `bronze.orders` row count | 10 |
| `bronze.lineitem` row count | 13 |
| `silver.orders` O_TOTALPRICE dtype | `decimal(15,2)` |
| `silver.lineitem` has `revenue` column | Yes |
| `gold.revenue_by_order_date` total_revenue | > 0 for all rows |
| `gold.top_customers` sorted desc | Yes |

---

## SECTION 4: Destroy / Cleanup

### AWS

```bash
# Terraform destroy (removes S3, IAM, Glue, Lake Formation)
cd terraform/aws
terraform destroy -var="account_id=123456789012" -auto-approve

# If Terraform state is lost, manual cleanup:
aws s3 rb s3://iceberg-pipeline-dev-raw-123456789012 --force
aws s3 rb s3://iceberg-pipeline-dev-iceberg-123456789012 --force
aws glue delete-job --job-name iceberg-pipeline-dev-job
aws iam delete-role-policy --role-name iceberg-pipeline-dev-glue-job-role --policy-name iceberg-pipeline-dev-glue-job-policy
aws iam delete-role --role-name iceberg-pipeline-dev-glue-job-role
# Repeat for data-engineer, data-analyst, platform-admin roles
aws kms schedule-key-deletion --key-id KEY_ID --pending-window-in-days 7
```

### Azure / Fabric

```bash
# Delete the resource group (removes storage, RBAC assignments)
az group delete --name rg-iceberg-pipeline --yes --no-wait

# Delete Entra groups
az ad group delete --group grp-data-engineer
az ad group delete --group grp-data-analyst
az ad group delete --group grp-platform-admin

# Fabric workspace (manual):
# Navigate to Fabric portal → Workspace Settings → Remove workspace
# Or via Power BI REST API:
# Invoke-RestMethod -Method DELETE -Uri "https://api.powerbi.com/v1.0/myorg/groups/WORKSPACE_ID" -Headers @{Authorization="Bearer $token"}
```

### On-Prem / Local

```powershell
# Stop and remove MinIO container
docker stop minio; docker rm minio

# Or if running as a binary, stop the process and delete data:
Remove-Item -Recurse -Force C:\minio-data

# Remove downloaded JARs (optional)
Remove-Item -Recurse -Force $HOME\.ivy2\cache\org.apache.iceberg
Remove-Item -Recurse -Force $HOME\.ivy2\cache\org.apache.hadoop
```

### Data-Only Cleanup (keep infrastructure)

```sql
-- From Spark SQL on any platform:
DROP TABLE IF EXISTS iceberg.gold.top_customers PURGE;
DROP TABLE IF EXISTS iceberg.gold.revenue_by_order_date PURGE;
DROP TABLE IF EXISTS iceberg.silver.lineitem PURGE;
DROP TABLE IF EXISTS iceberg.silver.orders PURGE;
DROP TABLE IF EXISTS iceberg.bronze.lineitem PURGE;
DROP TABLE IF EXISTS iceberg.bronze.orders PURGE;
DROP NAMESPACE IF EXISTS iceberg.gold;
DROP NAMESPACE IF EXISTS iceberg.silver;
DROP NAMESPACE IF EXISTS iceberg.bronze;
```

The `PURGE` keyword deletes underlying data files (not just metadata).
