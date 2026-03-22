# Multi-Cloud Pipeline

A single PySpark pipeline that ingests TPC-H `orders` and `lineitem` datasets through
**Bronze → Silver → Gold** Iceberg tables. Runs unchanged on **AWS Glue**, **Microsoft Fabric**,
and **local Spark + MinIO**.

---

## Quick Start (Local — 5 minutes)

**Prerequisites**: Java 17, Spark 3.5, Python 3.9+, Docker.

```powershell
pip install -r requirements.txt

# 2. Start MinIO
docker run -d --name minio -p 9000:9000 -p 9001:9001 `
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin `
  minio/minio server /data --console-address ":9001"

# 3. Create buckets and upload sample data
mc alias set local http://localhost:9000 minioadmin minioadmin
mc mb local/raw-data
mc mb local/iceberg-warehouse
mc cp tests/sample_data/orders.csv local/raw-data/tpch/orders.csv
mc cp tests/sample_data/lineitem.csv local/raw-data/tpch/lineitem.csv

# 4. Set environment variables  (copy env.example → .env and edit, or dot-source the helper)
. .\set_env.bat        # CMD
# . .\set_env.ps1      # PowerShell

# 5. Run pipeline
spark-submit --master "local[*]" `
  --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262" `
  spark_job.py

# 6. Run tests
python -m pytest tests/test_smoke.py -v
```

> First run downloads ~150 MB of JARs from Maven (~2 min). Cached afterwards.

For detailed install steps (Java, Spark, winutils, troubleshooting) see **[docs/LOCAL_SETUP.md](docs/LOCAL_SETUP.md)**.

---

## How It Works

### Single Codebase, Three Runtimes

```
                    ┌──────────────┐
                    │  spark_job.py │  ← Platform-agnostic pipeline logic
                    │  (bronze →    │
                    │   silver →    │
                    │   gold)       │
                    └──────┬───────┘
                           │
                    ┌──────▼───────┐
                    │spark_factory  │  ← Builds the right SparkSession
                    │   .py        │     per RUNTIME env var
                    └──┬───┬───┬──┘
                       │   │   │
              ┌────────┘   │   └────────┐
              ▼            ▼            ▼
        ┌──────────┐ ┌──────────┐ ┌──────────┐
        │ AWS Glue │ │ Fabric   │ │ Local    │
        │ GlueCtx  │ │ %%config │ │ MinIO    │
        │ S3       │ │ abfss:// │ │ S3A      │
        └──────────┘ └──────────┘ └──────────┘
```

The **Iceberg Hadoop catalog** stores table metadata as files alongside the data.
Changing `ICEBERG_WAREHOUSE` from `s3://` → `abfss://` → `s3a://` is all that's needed
to switch platforms.

### Pipeline Phases

| Phase | Input | Output | Key Operations |
|-------|-------|--------|---------------|
| **Bronze** | Raw CSVs (pipe-delimited) | `iceberg.bronze.orders`, `iceberg.bronze.lineitem` | Schema-enforced ingest, idempotent `createOrReplace` |
| **Silver** | Bronze tables | `iceberg.silver.orders`, `iceberg.silver.lineitem` | Type casting, null filtering, audit columns, partitioning |
| **Gold** | Silver tables | `iceberg.gold.revenue_by_order_date`, `iceberg.gold.top_customers` | Joins, aggregations, business metrics |

---

## Project Structure

```
iceberg-pipeline/
├── .gitignore
├── README.md                    ← You are here
├── requirements.txt             Python dependencies
├── env.example                  Environment variable template  (commit this)
├── set_env.bat                  CMD env helper        (gitignored — contains credentials)
│
├── spark_job.py                 Main pipeline (bronze / silver / gold)
├── spark_factory.py             SparkSession factory per runtime
├── config.py                    Environment-driven configuration
├── utils/
│   ├── __init__.py
│   └── logging.py               JSON structured logging + metrics
│
├── docs/
│   ├── LOCAL_SETUP.md           Local / Windows dev guide
│   ├── AWS_SETUP.md             AWS deployment guide
│   └── AZURE_FABRIC_SETUP.md   Azure / Fabric deployment guide
│
├── terraform/aws/main.tf        S3, IAM, Glue, Lake Formation
├── bicep/azure.bicep            ADLS Gen2, Entra groups, RBAC
├── scripts/minio_setup.sh       MinIO buckets, users, policies
│
├── tests/
│   ├── test_smoke.py            13 pytest smoke tests
│   └── sample_data/
│       ├── orders.csv           10-row TPC-H sample
│       └── lineitem.csv         13-row TPC-H sample
│
├── runbooks/
│   └── operational_notes.md     Monitoring, Iceberg maintenance, security
└── DELIVERABLE.md               Full architecture doc
```

---

## Platform Deployment Guides

| Platform | Guide | What's covered |
|----------|-------|---------------|
| **Local (Windows)** | [docs/LOCAL_SETUP.md](docs/LOCAL_SETUP.md) | Java, Spark, MinIO install, `spark-submit`, tests |
| **AWS** | [docs/AWS_SETUP.md](docs/AWS_SETUP.md) | Terraform deploy, Glue job, Athena queries, IAM |
| **Azure / Fabric** | [docs/AZURE_FABRIC_SETUP.md](docs/AZURE_FABRIC_SETUP.md) | Bicep deploy, Fabric notebook, shortcuts, Power BI |

> **Start with Local** — verify the pipeline works on your machine before deploying to cloud.

---

## Cleanup

```powershell
# Local
docker stop minio; docker rm minio

# AWS
cd terraform/aws
terraform destroy -var="account_id=YOUR_ACCOUNT" -auto-approve

# Azure
az group delete --name rg-iceberg-pipeline --yes
# Fabric workspace: delete via portal
```

---

## Further Reading

- [Full Deliverable](DELIVERABLE.md) — Architecture, code notes, validation matrix, destroy steps
- [Operational Runbook](runbooks/operational_notes.md) — Monitoring, Iceberg maintenance, schema evolution
