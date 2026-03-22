# AWS Deployment Guide

Complete step-by-step guide to deploy and run the Iceberg pipeline on AWS using
Glue 5.0, S3, Lake Formation, and Athena.

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Architecture Overview](#2-architecture-overview)
3. [Deploy Infrastructure with Terraform](#3-deploy-infrastructure-with-terraform)
4. [Upload Pipeline Code to S3](#4-upload-pipeline-code-to-s3)
5. [Upload Raw Data to S3](#5-upload-raw-data-to-s3)
6. [Run the Glue Job](#6-run-the-glue-job)
7. [Query Iceberg Tables via Athena](#7-query-iceberg-tables-via-athena)
8. [Access Management & Personas](#8-access-management--personas)
9. [Lake Formation Fine-Grained Access Control](#9-lake-formation-fine-grained-access-control)
10. [Security Configuration](#10-security-configuration)
11. [Monitoring & Logging](#11-monitoring--logging)
12. [Cost Estimation](#12-cost-estimation)
13. [Troubleshooting](#13-troubleshooting)
14. [Destroy Resources](#14-destroy-resources)

---

## 1. Prerequisites

### Tools

| Tool | Version | Install |
|------|---------|---------|
| AWS CLI | v2 | `winget install Amazon.AWSCLI` |
| Terraform | >= 1.5 | `winget install Hashicorp.Terraform` |
| Git | any | `winget install Git.Git` |

### AWS Account

- An AWS account with admin privileges (or scoped permissions to create IAM, S3, Glue, Lake Formation, KMS).
- AWS CLI configured:
  ```powershell
  aws configure
  # Enter: Access Key, Secret Key, Region (eu-west-2), Output (json)
  ```

### Verify

```powershell
aws sts get-caller-identity    # Confirms credentials work
terraform --version            # >= 1.5
aws --version                  # v2.x
```

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                        AWS Account                          │
│                                                             │
│  ┌──────────┐    ┌──────────────────┐    ┌──────────────┐  │
│  │ S3 Bucket│    │   AWS Glue 4.0   │    │   S3 Bucket  │  │
│  │  (raw)   │───▶│  spark_job.py    │───▶│  (iceberg)   │  │
│  │          │    │  GlueContext +   │    │              │  │
│  │ tpch/    │    │  Iceberg runtime │    │ bronze/      │  │
│  │  orders  │    └──────────────────┘    │ silver/      │  │
│  │  lineitem│             │              │ gold/        │  │
│  └──────────┘             │              └──────┬───────┘  │
│                           │                     │          │
│                    ┌──────▼───────┐      ┌──────▼───────┐  │
│                    │  CloudWatch  │      │   Athena v3  │  │
│                    │  Logs/Metrics│      │  (SQL query) │  │
│                    └──────────────┘      └──────────────┘  │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │               Lake Formation                         │   │
│  │  data_engineer: ALL on bronze/silver/gold            │   │
│  │  data_analyst:  SELECT on gold                       │   │
│  │  platform_admin: Super + ALL                         │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  ┌──────────┐                                              │
│  │ KMS Key  │  SSE-KMS encryption on both S3 buckets       │
│  └──────────┘                                              │
└─────────────────────────────────────────────────────────────┘
```

### What Terraform Creates

| Resource | Purpose |
|----------|---------|
| `aws_s3_bucket.raw` | Raw TPC-H source files |
| `aws_s3_bucket.iceberg` | Iceberg warehouse (bronze/silver/gold tables) |
| `aws_kms_key.data_key` | SSE-KMS encryption for both buckets |
| `aws_iam_role.glue_job` | Glue execution role (S3 + KMS + Glue catalog + CloudWatch) |
| `aws_iam_role.data_engineer` | Human role – full pipeline access |
| `aws_iam_role.data_analyst` | Human role – read-only S3 + Athena |
| `aws_iam_role.platform_admin` | Human role – full admin |
| `aws_lakeformation_resource` | Register S3 iceberg bucket with Lake Formation |
| `aws_lakeformation_permissions` | Grant engineer ALL, analyst SELECT on gold |
| `aws_glue_job.pipeline` | Glue ETL job definition (Glue 5.0 / Spark 3.5, Iceberg) |

---

## 3. Deploy Infrastructure with Terraform

```powershell
cd terraform\aws

# Get your AWS account ID
$ACCOUNT_ID = (aws sts get-caller-identity --query Account --output text)
echo "Account: $ACCOUNT_ID"

# Initialise Terraform
terraform init

# Preview changes
terraform plan -var="account_id=$ACCOUNT_ID"

# Deploy
terraform apply -var="account_id=$ACCOUNT_ID"
```

**Terraform outputs** (save these):
```
raw_bucket     = "iceberg-pipeline-dev-raw-123456789012"
iceberg_bucket = "iceberg-pipeline-dev-iceberg-123456789012"
glue_job_name  = "iceberg-pipeline-dev-job"
kms_key_arn    = "arn:aws:kms:eu-west-2:123456789012:key/xxxx"
glue_role_arn  = "arn:aws:iam::123456789012:role/iceberg-pipeline-dev-glue-job-role"
```

### Optional: Customise Variables

Create `terraform.tfvars`:
```hcl
aws_region  = "us-east-1"      # Change region
account_id  = "123456789012"
project     = "my-pipeline"     # Change naming prefix
environment = "staging"
```

---

## 4. Upload Pipeline Code to S3

The Glue job expects the Python files at the `script_location` configured in Terraform.
Use the provided helper script — it creates the zip with correct Unix path separators
(Windows `Compress-Archive` writes backslash paths that break on Linux/Glue):

**Command Prompt:**
```cmd
cd d:\projects\cloud_agnostic_project
set RAW_BUCKET=<your raw bucket name>

scripts\deploy_aws.bat
```

**Manual steps (PowerShell):**
```powershell
cd d:\projects\cloud_agnostic_project
$RAW_BUCKET = (cd terraform\aws; terraform output -raw raw_bucket; cd ..\..)

# Build zip with correct path separators (do NOT use Compress-Archive — backslash issue)
python -c "
import zipfile, os
with zipfile.ZipFile('pipeline_deps.zip', 'w', zipfile.ZIP_DEFLATED) as z:
    for f in ['config.py', 'spark_factory.py']:
        z.write(f, f)
    for root, dirs, files in os.walk('utils'):
        dirs[:] = [d for d in dirs if d != '__pycache__']
        for fn in files:
            if not fn.endswith('.pyc'):
                full = os.path.join(root, fn)
                z.write(full, full.replace(os.sep, '/'))
"

aws s3 cp spark_job.py      "s3://$RAW_BUCKET/scripts/spark_job.py"
aws s3 cp pipeline_deps.zip "s3://$RAW_BUCKET/scripts/pipeline_deps.zip"
```

> `--extra-py-files` is already set in `main.tf` `default_arguments` — no manual
> `aws glue update-job` needed after a `terraform apply`.

---

## 5. Upload Raw Data to S3

### Sample data (testing)

```powershell
$RAW_BUCKET = terraform output -raw raw_bucket

aws s3 cp tests\sample_data\orders.csv "s3://$RAW_BUCKET/tpch/orders.csv"
aws s3 cp tests\sample_data\lineitem.csv "s3://$RAW_BUCKET/tpch/lineitem.csv"

# Verify
aws s3 ls "s3://$RAW_BUCKET/tpch/"
```

### Full TPC-H data (production)

Generate with the TPC-H dbgen tool or download pre-generated CSVs:
```powershell
# Example: upload large files
aws s3 cp /path/to/tpch_10gb/ "s3://$RAW_BUCKET/tpch/" --recursive
```

---

## 6. Run the Glue Job

### Via CLI

**PowerShell:**
```powershell
$JOB_NAME = (cd terraform\aws; terraform output -raw glue_job_name)

$RUN_ID = (aws glue start-job-run --job-name $JOB_NAME --query 'JobRunId' --output text)
echo "Job run ID: $RUN_ID"

aws glue get-job-run --job-name $JOB_NAME --run-id $RUN_ID --query 'JobRun.{State:JobRunState,Error:ErrorMessage}' --output table
```

**Command Prompt:**
```cmd
set JOB_NAME=iceberg-pipeline-dev-job

for /f "tokens=*" %i in ('aws glue start-job-run --job-name %JOB_NAME% --query "JobRunId" --output text') do set RUN_ID=%i
echo Job run ID: %RUN_ID%

aws glue get-job-run --job-name %JOB_NAME% --run-id %RUN_ID% --query "JobRun.{State:JobRunState,Error:ErrorMessage}" --output table
```

**Poll until complete (Command Prompt):**
```cmd
:loop
aws glue get-job-run --job-name %JOB_NAME% --run-id %RUN_ID% --query "JobRun.JobRunState" --output text
timeout /t 15 /nobreak >nul
goto loop
```

### Via Console

1. Go to **AWS Glue Console** → **ETL Jobs**.
2. Find `iceberg-pipeline-dev-job`.
3. Click **Run**.
4. Monitor under the **Runs** tab.

### Expected Duration

| Data Size | Workers | Duration |
|-----------|---------|----------|
| Sample (23 rows) | 2 × G.1X | ~2 min |
| TPC-H 1 GB | 2 × G.1X | ~5 min |
| TPC-H 10 GB | 5 × G.1X | ~15 min |

### Glue Job Parameters

The Terraform definition sets these defaults. Override per-run:

```powershell
aws glue start-job-run --job-name $JOB_NAME --arguments '{
  "--RUNTIME": "aws_glue",
  "--ICEBERG_WAREHOUSE": "s3://ICEBERG_BUCKET/iceberg/",
  "--RAW_SOURCE_PATH": "s3://RAW_BUCKET/tpch/",
  "--PERSONA": "data_engineer"
}'
```

---

## 7. Query Iceberg Tables via Athena

### 7a. Set up Athena workgroup (one-time)

```powershell
$ICEBERG_BUCKET = terraform output -raw iceberg_bucket

aws athena create-work-group `
  --name iceberg-pipeline `
  --configuration "ResultConfiguration={OutputLocation=s3://$ICEBERG_BUCKET/athena-results/}"
```

### 7b. Register Iceberg tables in Athena

Athena v3 supports Iceberg tables. Register them by pointing at the warehouse:

```sql
-- Run in Athena console or via CLI

-- Create database references
CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS gold;

-- Register gold tables (Athena reads Iceberg metadata from S3)
-- Option 1: Using Iceberg table type
CREATE TABLE gold.revenue_by_order_date (
    order_date DATE,
    order_year INT,
    order_month INT,
    total_revenue DECIMAL(38,2),
    line_count BIGINT,
    order_count BIGINT
)
LOCATION 's3://ICEBERG_BUCKET/iceberg/gold/revenue_by_order_date'
TBLPROPERTIES ('table_type' = 'ICEBERG');

CREATE TABLE gold.top_customers (
    customer_key BIGINT,
    total_revenue DECIMAL(38,2),
    order_count BIGINT,
    first_order_date DATE,
    last_order_date DATE
)
LOCATION 's3://ICEBERG_BUCKET/iceberg/gold/top_customers'
TBLPROPERTIES ('table_type' = 'ICEBERG');
```

### 7c. Run queries

```sql
-- Revenue by year
SELECT order_year,
       SUM(total_revenue) AS yearly_revenue,
       SUM(order_count)   AS total_orders
FROM gold.revenue_by_order_date
GROUP BY order_year
ORDER BY order_year;

-- Top 10 customers
SELECT customer_key, total_revenue, order_count
FROM gold.top_customers
LIMIT 10;

-- Iceberg time travel (Athena v3)
SELECT * FROM gold.top_customers FOR SYSTEM_TIME AS OF TIMESTAMP '2025-01-15 00:00:00';

-- Iceberg snapshot history
SELECT * FROM gold."top_customers$snapshots";
```

### Via CLI

```powershell
$QUERY = "SELECT order_year, SUM(total_revenue) AS rev FROM gold.revenue_by_order_date GROUP BY order_year"

$EXECUTION_ID = (aws athena start-query-execution `
  --query-string $QUERY `
  --work-group iceberg-pipeline `
  --query-execution-context "Database=gold" `
  --query 'QueryExecutionId' --output text)

# Wait for completion
aws athena get-query-execution --query-execution-id $EXECUTION_ID --query 'QueryExecution.Status.State'

# Get results
aws athena get-query-results --query-execution-id $EXECUTION_ID
```

---

## 8. Access Management & Personas

### IAM Roles Created by Terraform

| Role | ARN Pattern | Purpose |
|------|-------------|---------|
| `iceberg-pipeline-dev-glue-job-role` | `arn:aws:iam::ACCT:role/...` | Glue job execution |
| `iceberg-pipeline-dev-data-engineer` | `arn:aws:iam::ACCT:role/...` | Full pipeline access |
| `iceberg-pipeline-dev-data-analyst` | `arn:aws:iam::ACCT:role/...` | Read-only on gold |
| `iceberg-pipeline-dev-platform-admin` | `arn:aws:iam::ACCT:role/...` | Full admin |

### Assume a persona role (testing)

```powershell
# Assume data_analyst role
$CREDS = (aws sts assume-role `
  --role-arn "arn:aws:iam::${ACCOUNT_ID}:role/iceberg-pipeline-dev-data-analyst" `
  --role-session-name "test-analyst" | ConvertFrom-Json)

$env:AWS_ACCESS_KEY_ID     = $CREDS.Credentials.AccessKeyId
$env:AWS_SECRET_ACCESS_KEY = $CREDS.Credentials.SecretAccessKey
$env:AWS_SESSION_TOKEN     = $CREDS.Credentials.SessionToken

# This should work (read-only)
aws s3 ls "s3://$ICEBERG_BUCKET/iceberg/gold/"

# This should FAIL (no write access)
aws s3 cp test.txt "s3://$ICEBERG_BUCKET/iceberg/gold/test.txt"
# Expected: An error occurred (AccessDenied)
```

### Data Analyst IAM Policy (created by Terraform)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3ReadOnly",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::iceberg-pipeline-dev-iceberg-*",
        "arn:aws:s3:::iceberg-pipeline-dev-iceberg-*/*"
      ]
    },
    {
      "Sid": "AthenaQuery",
      "Effect": "Allow",
      "Action": [
        "athena:StartQueryExecution",
        "athena:GetQueryExecution",
        "athena:GetQueryResults"
      ],
      "Resource": "*"
    },
    {
      "Sid": "KMSDecrypt",
      "Effect": "Allow",
      "Action": ["kms:Decrypt", "kms:DescribeKey"],
      "Resource": ["<kms_key_arn>"]
    }
  ]
}
```

---

## 9. Lake Formation Fine-Grained Access Control

### Important Caveat

This pipeline uses the **Iceberg Hadoop catalog** (metadata stored as files in S3) rather
than the **Glue Data Catalog**. Lake Formation FGAC (column/row filtering) requires tables
registered in the Glue Data Catalog.

**Two options**:

| Approach | Pros | Cons |
|----------|------|------|
| **A) Register in Glue Catalog post-pipeline** | Full Lake Formation FGAC | Extra registration step; dual-catalog maintenance |
| **B) Path-level IAM** | Simple; no Glue Catalog needed | No column/row filtering |

### Option A: Register Iceberg tables in Glue Catalog

```powershell
# Register the Iceberg S3 location with Lake Formation
aws lakeformation register-resource `
  --resource-arn "arn:aws:s3:::$ICEBERG_BUCKET" `
  --use-service-linked-role

# Create Glue database
aws glue create-database --database-input '{"Name":"gold"}'

# Create Glue table pointing at Iceberg
aws glue create-table --database-name gold --table-input '{
  "Name": "revenue_by_order_date",
  "StorageDescriptor": {
    "Location": "s3://'"$ICEBERG_BUCKET"'/iceberg/gold/revenue_by_order_date",
    "InputFormat": "org.apache.iceberg.mr.hive.HiveIcebergInputFormat",
    "OutputFormat": "org.apache.iceberg.mr.hive.HiveIcebergOutputFormat",
    "SerdeInfo": {"SerializationLibrary": "org.apache.iceberg.mr.hive.HiveIcebergSerDe"}
  },
  "TableType": "EXTERNAL_TABLE",
  "Parameters": {"table_type": "ICEBERG"}
}'
```

### Grant column-level access (Option A only)

```powershell
# Grant analyst SELECT on only order_year and total_revenue columns
aws lakeformation grant-permissions `
  --principal "DataLakePrincipalIdentifier=arn:aws:iam::${ACCOUNT_ID}:role/iceberg-pipeline-dev-data-analyst" `
  --permissions "SELECT" `
  --resource '{
    "TableWithColumns": {
      "DatabaseName": "gold",
      "Name": "revenue_by_order_date",
      "ColumnNames": ["order_year", "total_revenue"]
    }
  }'
```

### Revoke access

```powershell
aws lakeformation revoke-permissions `
  --principal "DataLakePrincipalIdentifier=arn:aws:iam::${ACCOUNT_ID}:role/iceberg-pipeline-dev-data-analyst" `
  --permissions "SELECT" `
  --resource '{"Table": {"DatabaseName": "gold", "Name": "revenue_by_order_date"}}'
```

---

## 10. Security Configuration

### Encryption

Terraform enables SSE-KMS on both buckets with automatic key rotation. For additional control:

```powershell
# Enforce encryption in transit via bucket policy (add to Terraform or apply manually)
aws s3api put-bucket-policy --bucket $RAW_BUCKET --policy '{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "DenyInsecureTransport",
    "Effect": "Deny",
    "Principal": "*",
    "Action": "s3:*",
    "Resource": ["arn:aws:s3:::'"$RAW_BUCKET"'/*"],
    "Condition": {"Bool": {"aws:SecureTransport": "false"}}
  }]
}'
```

### VPC Endpoints (production)

```hcl
# Add to main.tf for production
resource "aws_vpc_endpoint" "s3" {
  vpc_id       = var.vpc_id
  service_name = "com.amazonaws.${var.aws_region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [var.route_table_id]
}

resource "aws_vpc_endpoint" "glue" {
  vpc_id              = var.vpc_id
  service_name        = "com.amazonaws.${var.aws_region}.glue"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = var.subnet_ids
  security_group_ids  = [var.sg_id]
  private_dns_enabled = true
}
```

### CloudTrail Audit

```powershell
# Verify CloudTrail is logging S3 data events
aws cloudtrail get-event-selectors --trail-name management-trail
```

---

## 11. Monitoring & Logging

### CloudWatch Logs

```powershell
# View Glue job logs
aws logs describe-log-groups --log-group-name-prefix "/aws-glue/jobs"

# Tail latest logs
aws logs tail "/aws-glue/jobs/output" --follow
```

### CloudWatch Metrics

The Glue job has `--enable-metrics true` set. Key metrics:
- `glue.driver.aggregate.numCompletedTasks`
- `glue.driver.aggregate.numFailedTasks`
- `glue.ALL.s3.filesystem.read_bytes`

### Create alarm for failures

```powershell
aws cloudwatch put-metric-alarm `
  --alarm-name "iceberg-pipeline-failures" `
  --metric-name "glue.driver.aggregate.numFailedTasks" `
  --namespace "Glue" `
  --statistic Sum `
  --period 300 `
  --threshold 1 `
  --comparison-operator GreaterThanOrEqualToThreshold `
  --evaluation-periods 1 `
  --alarm-actions "arn:aws:sns:eu-west-2:${ACCOUNT_ID}:alerts-topic"
```

---

## 12. Cost Estimation

| Component | Unit Cost | Sample Run (23 rows) | TPC-H 10 GB |
|-----------|-----------|---------------------|-------------|
| Glue ETL (G.1X × 2 workers) | $0.44/DPU-hour | ~$0.02 (2 min) | ~$0.22 (15 min) |
| S3 Storage | $0.023/GB/month | < $0.01 | ~$0.25/month |
| S3 Requests (PUT/GET) | $0.005/1000 | < $0.01 | ~$0.05 |
| KMS | $1/key/month + $0.03/10K requests | ~$1.00 | ~$1.00 |
| Athena | $5/TB scanned | < $0.01 | ~$0.05/query |
| **Total (one-off run)** | | **~$1.03** | **~$1.52** |

---

## 13. Troubleshooting

### "Access Denied" on S3

```powershell
# Check Glue role has S3 permissions
aws iam simulate-principal-policy `
  --policy-source-arn (terraform output -raw glue_role_arn) `
  --action-names s3:GetObject `
  --resource-arns "arn:aws:s3:::$RAW_BUCKET/tpch/orders.csv"
```

### "ClassNotFoundException: org.apache.iceberg..."
Ensure `--datalake-formats=iceberg` is set in Glue job parameters (Terraform default includes it).

### Glue job stays in "RUNNING" forever
Check CloudWatch logs for OOM. Increase `worker_type` to `G.2X` or add workers.

### Lake Formation "Insufficient permissions"
```powershell
# Grant caller as Lake Formation admin first
aws lakeformation put-data-lake-settings --data-lake-settings '{
  "DataLakeAdmins": [{"DataLakePrincipalIdentifier": "arn:aws:iam::'"$ACCOUNT_ID"':role/ADMIN_ROLE"}]
}'
```

---

## 14. Destroy Resources

### Full teardown

```powershell
cd terraform\aws
terraform destroy -var="account_id=$ACCOUNT_ID" -auto-approve
```

### Manual cleanup (if Terraform state lost)

```powershell
# Empty and delete buckets
aws s3 rm "s3://$RAW_BUCKET" --recursive
aws s3 rb "s3://$RAW_BUCKET"
aws s3 rm "s3://$ICEBERG_BUCKET" --recursive
aws s3 rb "s3://$ICEBERG_BUCKET"

# Delete Glue job
aws glue delete-job --job-name iceberg-pipeline-dev-job

# Delete IAM roles (must delete policies first)
foreach ($role in @("glue-job-role","data-engineer","data-analyst","platform-admin")) {
    $fullName = "iceberg-pipeline-dev-$role"
    # List and delete inline policies
    $policies = (aws iam list-role-policies --role-name $fullName --query 'PolicyNames' --output text)
    foreach ($p in $policies.Split()) {
        aws iam delete-role-policy --role-name $fullName --policy-name $p
    }
    aws iam delete-role --role-name $fullName
}

# Schedule KMS key deletion
$KEY_ID = terraform output -raw kms_key_arn
aws kms schedule-key-deletion --key-id $KEY_ID --pending-window-in-days 7

# Delete Athena workgroup
aws athena delete-work-group --work-group iceberg-pipeline --recursive-delete-option

# Deregister Lake Formation resource
aws lakeformation deregister-resource --resource-arn "arn:aws:s3:::$ICEBERG_BUCKET"
```
