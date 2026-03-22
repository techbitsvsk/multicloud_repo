# Local Development Guide (Windows)

Complete step-by-step guide to run and test the Iceberg pipeline on a Windows machine
using local Spark + MinIO.

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Install Java 17](#2-install-java-17)
3. [Install Apache Spark 3.5](#3-install-apache-spark-35)
4. [Install Python & Dependencies](#4-install-python--dependencies)
5. [Install Docker Desktop](#5-install-docker-desktop)
6. [Install MinIO Client (mc)](#6-install-minio-client-mc)
7. [Start MinIO](#7-start-minio)
8. [Create Buckets & Upload Sample Data](#8-create-buckets--upload-sample-data)
9. [Configure Environment Variables](#9-configure-environment-variables)
10. [Run the Pipeline](#10-run-the-pipeline)
11. [Run Tests](#11-run-tests)
12. [Query Results Interactively](#12-query-results-interactively)
13. [Troubleshooting](#13-troubleshooting)
14. [Cleanup](#14-cleanup)

---

## 1. Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Java (JDK) | 11 or 17 | Spark runtime |
| Apache Spark | 3.5.x | Distributed compute engine |
| Python | 3.9 – 3.12 | PySpark driver |
| Docker Desktop | any | Runs MinIO container |
| MinIO Client (mc) | latest | Creates buckets, uploads data |
| Git | any | Clone the repo |

> **Disk space**: ~1.5 GB for Spark + Maven cache on first run.
> **RAM**: 4 GB minimum free for `local[*]` Spark.

---

## 2. Install Java 17

**Option A — winget (recommended)**:
```powershell
winget install Microsoft.OpenJDK.17
```

**Option B — manual download**:
Download from https://learn.microsoft.com/en-us/java/openjdk/download and install.

**Set JAVA_HOME** (if not auto-set):
```powershell
# Find install path
Get-Command java | Select-Object -ExpandProperty Source
# Typically: C:\Program Files\Microsoft\jdk-17.x.x\

# Set permanently (run as Admin):
[System.Environment]::SetEnvironmentVariable("JAVA_HOME", "C:\Program Files\Microsoft\jdk-17.0.13+11", "Machine")

# Or for current session only:
$env:JAVA_HOME = "C:\Program Files\Microsoft\jdk-17.0.13+11"
```

**Verify**:
```powershell
java -version
# Expected: openjdk version "17.x.x"
```

---

## 3. Install Apache Spark 3.5

### 3a. Download

Go to https://spark.apache.org/downloads.html and download:
- Spark release: **3.5.3**
- Package type: **Pre-built for Apache Hadoop 3.3 and later**

Or via PowerShell:
```powershell
# Download (adjust version if needed)
$sparkVersion = "3.5.3"
$hadoopVersion = "3"
$url = "https://archive.apache.org/dist/spark/spark-$sparkVersion/spark-$sparkVersion-bin-hadoop$hadoopVersion.tgz"
Invoke-WebRequest -Uri $url -OutFile "spark-$sparkVersion-bin-hadoop$hadoopVersion.tgz"
```

### 3b. Extract

```powershell
# Extract to C:\spark (or your preferred location)
tar -xzf spark-3.5.3-bin-hadoop3.tgz
Move-Item spark-3.5.3-bin-hadoop3 C:\spark
```

### 3c. Set environment variables

```powershell
# Permanently (run as Admin):
[System.Environment]::SetEnvironmentVariable("SPARK_HOME", "C:\spark", "Machine")
[System.Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\spark", "Machine")

# Add to PATH:
$currentPath = [System.Environment]::GetEnvironmentVariable("Path", "Machine")
[System.Environment]::SetEnvironmentVariable("Path", "$currentPath;C:\spark\bin", "Machine")
```

> **Restart your terminal** after setting Machine-level variables.

### 3d. Hadoop winutils (required on Windows)

Spark on Windows needs `winutils.exe`:

```powershell
# Create hadoop bin directory
New-Item -ItemType Directory -Force -Path C:\spark\bin

# Download winutils for Hadoop 3.3
Invoke-WebRequest `
  -Uri "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/winutils.exe" `
  -OutFile "C:\spark\bin\winutils.exe"

Invoke-WebRequest `
  -Uri "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/hadoop.dll" `
  -OutFile "C:\spark\bin\hadoop.dll"
```

### 3e. Verify

```powershell
spark-submit --version
# Expected: version 3.5.3, Using Scala version 2.12.x

pyspark --version
# Expected: same version
```

---

## 4. Install Python & Dependencies

```powershell
# Install Python (if not present)
winget install Python.Python.3.12

# Verify
python --version   # 3.9 – 3.12

# Create virtual environment (recommended)
cd iceberg-pipeline
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# Install dependencies
pip install -r requirements.txt
```

**requirements.txt contents**:
```
pyspark==3.5.3
pytest>=7.0,<9.0
```

> **Important**: The PySpark version must match your Spark installation (3.5.x).

---

## 5. Install Docker Desktop

Download and install from https://docs.docker.com/desktop/install/windows-install/

```powershell
# Verify
docker --version
docker run hello-world
```

> If you prefer running MinIO natively (no Docker), see [Section 7 Alternative](#7b-alternative-minio-binary-no-docker).

---

## 6. Install MinIO Client (mc)

**Option A — winget**:
```powershell
winget install MinIO.Client
```

**Option B — direct download**:
```powershell
Invoke-WebRequest -Uri "https://dl.min.io/client/mc/release/windows-amd64/mc.exe" -OutFile "$HOME\mc.exe"
# Add $HOME to PATH or move mc.exe to a directory on PATH
```

**Verify**:
```powershell
mc --version
```

---

## 7. Start MinIO

### 7a. Docker (recommended)

```powershell
docker run -d --name minio `
  -p 9000:9000 `
  -p 9001:9001 `
  -e MINIO_ROOT_USER=minioadmin `
  -e MINIO_ROOT_PASSWORD=minioadmin `
  minio/minio server /data --console-address ":9001"
```

**Verify**:
- API endpoint: http://localhost:9000
- Web console: http://localhost:9001 (login: minioadmin / minioadmin)

```powershell
# Check container is running
docker ps --filter name=minio
```

### 7b. Alternative: MinIO Binary (no Docker)

```powershell
# Download server binary
Invoke-WebRequest `
  -Uri "https://dl.min.io/server/minio/release/windows-amd64/minio.exe" `
  -OutFile "C:\minio\minio.exe"

# Create data directory
New-Item -ItemType Directory -Force -Path C:\minio-data

# Start server (new terminal)
$env:MINIO_ROOT_USER = "minioadmin"
$env:MINIO_ROOT_PASSWORD = "minioadmin"
C:\minio\minio.exe server C:\minio-data --console-address ":9001"
```

---

## 8. Create Buckets & Upload Sample Data

### 8a. Using bash script (Git Bash or WSL)

```bash
bash scripts/minio_setup.sh
```

This creates buckets, uploads sample CSVs, creates IAM users and policies.

### 8b. Manual steps (PowerShell only, no bash needed)

```powershell
# Configure mc alias
mc alias set local http://localhost:9000 minioadmin minioadmin

# Create buckets
mc mb local/raw-data
mc mb local/iceberg-warehouse

# Upload sample TPC-H CSVs
mc cp tests/sample_data/orders.csv local/raw-data/tpch/orders.csv
mc cp tests/sample_data/lineitem.csv local/raw-data/tpch/lineitem.csv

# Verify uploads
mc ls local/raw-data/tpch/
# Expected:
#   [DATE] 995B  orders.csv
#   [DATE] 1.7K  lineitem.csv
```

### 8c. Create personas (optional — for access-control testing)

```powershell
# Data Engineer policy (full access)
@'
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["s3:*"],
    "Resource": ["arn:aws:s3:::raw-data/*","arn:aws:s3:::raw-data","arn:aws:s3:::iceberg-warehouse/*","arn:aws:s3:::iceberg-warehouse"]
  }]
}
'@ | Set-Content -Path $env:TEMP\policy_engineer.json

mc admin policy create local data-engineer $env:TEMP\policy_engineer.json
mc admin user add local engineer1 "Eng1neer!Pass"
mc admin policy attach local data-engineer --user engineer1

# Data Analyst policy (read-only on iceberg)
@'
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["s3:GetObject","s3:ListBucket"],
    "Resource": ["arn:aws:s3:::iceberg-warehouse/*","arn:aws:s3:::iceberg-warehouse"]
  }]
}
'@ | Set-Content -Path $env:TEMP\policy_analyst.json

mc admin policy create local data-analyst $env:TEMP\policy_analyst.json
mc admin user add local analyst1 "An@lyst!Pass"
mc admin policy attach local data-analyst --user analyst1
```

---

## 9. Configure Environment Variables

```powershell
$env:RUNTIME               = "local"
$env:ICEBERG_WAREHOUSE     = "s3a://iceberg-warehouse/iceberg/"
$env:ICEBERG_CATALOG       = "iceberg"
$env:RAW_SOURCE_PATH       = "s3a://raw-data/tpch/"
$env:S3_ENDPOINT           = "http://localhost:9000"
$env:AWS_ACCESS_KEY_ID     = "minioadmin"
$env:AWS_SECRET_ACCESS_KEY = "minioadmin"
$env:PERSONA               = "data_engineer"
```

> **Tip**: Put these in a `set_env.ps1` script and dot-source it: `. .\set_env.ps1`

Create `set_env.ps1`:
```powershell
# set_env.ps1 — dot-source this: . .\set_env.ps1
$env:RUNTIME               = "local"
$env:ICEBERG_WAREHOUSE     = "s3a://iceberg-warehouse/iceberg/"
$env:ICEBERG_CATALOG       = "iceberg"
$env:RAW_SOURCE_PATH       = "s3a://raw-data/tpch/"
$env:S3_ENDPOINT           = "http://localhost:9000"
$env:AWS_ACCESS_KEY_ID     = "minioadmin"
$env:AWS_SECRET_ACCESS_KEY = "minioadmin"
$env:PERSONA               = "data_engineer"
Write-Host "Environment variables set for local MinIO" -ForegroundColor Green
```

---

## 10. Run the Pipeline

```powershell
# Ensure you are in the project root directory
cd iceberg-pipeline

# Set env vars (if not already)
. .\set_env.ps1

# Run
spark-submit `
  --master "local[*]" `
  --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262" `
  spark_job.py
```

**First run** downloads ~150 MB of JARs from Maven Central (~2 min on broadband).
Cached in `~/.ivy2/` afterwards.

**Expected output** (JSON structured logs):
```
{"ts":"...","level":"INFO","message":"Pipeline starting"}
{"ts":"...","level":"INFO","message":"Config: runtime=local, warehouse=s3a://iceberg-warehouse/iceberg/, ..."}
{"ts":"...","level":"INFO","message":"SparkSession created"}
{"ts":"...","level":"INFO","message":"START  PHASE 1 – BRONZE"}
{"ts":"...","level":"INFO","message":"Bronze: iceberg.bronze.orders – 10 rows in 12.3s"}
{"ts":"...","level":"INFO","message":"Bronze: iceberg.bronze.lineitem – 13 rows in 8.1s"}
{"ts":"...","level":"INFO","message":"START  PHASE 2 – SILVER"}
...
{"ts":"...","level":"INFO","message":"START  PHASE 3 – GOLD"}
...
{"ts":"...","level":"INFO","message":"Pipeline completed successfully"}
```

---

## 11. Run Tests

```powershell
# Ensure env vars are set (same as step 9)
. .\set_env.ps1

python -m pytest tests/test_smoke.py -v
```

**Expected output** (all 13 pass):
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

13 passed
```

---

## 12. Query Results Interactively

Launch Spark SQL shell connected to MinIO:

```powershell
spark-sql `
  --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262" `
  --conf "spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog" `
  --conf "spark.sql.catalog.iceberg.type=hadoop" `
  --conf "spark.sql.catalog.iceberg.warehouse=s3a://iceberg-warehouse/iceberg/" `
  --conf "spark.sql.defaultCatalog=iceberg" `
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" `
  --conf "spark.hadoop.fs.s3a.endpoint=http://localhost:9000" `
  --conf "spark.hadoop.fs.s3a.access.key=minioadmin" `
  --conf "spark.hadoop.fs.s3a.secret.key=minioadmin" `
  --conf "spark.hadoop.fs.s3a.path.style.access=true" `
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" `
  --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false"
```

**Sample queries**:

```sql
-- List all databases
SHOW NAMESPACES;
-- Expected: bronze, gold, silver

-- Bronze row counts
SELECT count(*) FROM bronze.orders;     -- 10
SELECT count(*) FROM bronze.lineitem;   -- 13

-- Silver type verification
DESCRIBE silver.orders;
-- O_TOTALPRICE should be decimal(15,2)

-- Gold: Revenue by year
SELECT order_year, SUM(total_revenue) AS rev, SUM(order_count) AS orders
FROM gold.revenue_by_order_date
GROUP BY order_year
ORDER BY order_year;

-- Gold: Top 5 customers
SELECT customer_key, total_revenue, order_count
FROM gold.top_customers
LIMIT 5;

-- Iceberg metadata: snapshots
SELECT * FROM gold.revenue_by_order_date.snapshots;

-- Iceberg time-travel (read previous version)
SELECT * FROM gold.top_customers VERSION AS OF <snapshot_id>;
```

---

## 13. Troubleshooting

### "java.lang.ClassNotFoundException: org.apache.hadoop.fs.s3a.S3AFileSystem"
Maven JARs not downloaded. Check internet connectivity and re-run. If behind a proxy:
```powershell
$env:JAVA_OPTS = "-Dhttp.proxyHost=proxy.corp -Dhttp.proxyPort=8080"
```

### "Connection refused" to MinIO
Verify MinIO is running:
```powershell
docker ps --filter name=minio     # Docker
# or
Test-NetConnection localhost -Port 9000   # Any
```

### "winutils.exe not found" / "Could not locate executable null\bin\winutils.exe"
`HADOOP_HOME` is not set. See [Section 3d](#3d-hadoop-winutils-required-on-windows).

### Spark hangs on "Ivy Default Cache"
First-time JAR resolution can be slow. Wait 2–3 minutes. If stuck, delete the cache:
```powershell
Remove-Item -Recurse -Force $HOME\.ivy2\cache
```

### "AnalysisException: Cannot create table ... already exists"
This is safe — the pipeline is idempotent. If you need a clean start:
```powershell
mc rm --recursive --force local/iceberg-warehouse/
```

### Python version mismatch with PySpark
Ensure the Python running `spark-submit` matches the `pip install pyspark` environment:
```powershell
python -c "import sys; print(sys.executable)"
$env:PYSPARK_PYTHON = (python -c "import sys; print(sys.executable)")
```

---

## 14. Cleanup

```powershell
# Stop and remove MinIO
docker stop minio
docker rm minio

# Delete MinIO data (if using binary)
Remove-Item -Recurse -Force C:\minio-data

# Remove Maven cache (optional, saves ~150 MB)
Remove-Item -Recurse -Force $HOME\.ivy2\cache\org.apache.iceberg
Remove-Item -Recurse -Force $HOME\.ivy2\cache\org.apache.hadoop
Remove-Item -Recurse -Force $HOME\.ivy2\cache\com.amazonaws

# Delete pipeline Iceberg data only (keep buckets)
mc rm --recursive --force local/iceberg-warehouse/iceberg/

# Or nuke everything
mc rb --force local/raw-data
mc rb --force local/iceberg-warehouse
```
