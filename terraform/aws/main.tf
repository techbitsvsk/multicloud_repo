# ──────────────────────────────────────────────────────────────────────────────
# terraform/aws/main.tf – Minimal-but-complete AWS infrastructure
#
# Creates: S3 buckets (raw + iceberg), KMS key, IAM roles (personas + Glue),
#          Lake Formation registration & grants, Glue job definition.
#
# Usage:
#   cd terraform/aws
#   terraform init
#   terraform plan -var="account_id=123456789012"
#   terraform apply -var="account_id=123456789012"
# ──────────────────────────────────────────────────────────────────────────────

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
}

provider "aws" {
  region = var.aws_region
}

# ── Variables ────────────────────────────────────────────────────────────────

variable "aws_region"  { default = "eu-west-2" }
variable "account_id"  { type = string }
variable "project"     { default = "iceberg-pipeline" }
variable "environment" { default = "dev" }

locals {
  prefix = "${var.project}-${var.environment}"
  tags   = { Project = var.project, Environment = var.environment, ManagedBy = "terraform" }
}

# ── KMS key for encryption at rest ───────────────────────────────────────────

resource "aws_kms_key" "data_key" {
  description             = "Encryption key for ${local.prefix} data buckets"
  deletion_window_in_days = 14
  enable_key_rotation     = true
  tags                    = local.tags
}

resource "aws_kms_alias" "data_key" {
  name          = "alias/${local.prefix}-data"
  target_key_id = aws_kms_key.data_key.key_id
}

# ── S3 Buckets ───────────────────────────────────────────────────────────────

resource "aws_s3_bucket" "raw" {
  bucket        = "${local.prefix}-raw-${var.account_id}"
  force_destroy = true
  tags          = local.tags
}

resource "aws_s3_bucket" "iceberg" {
  bucket        = "${local.prefix}-iceberg-${var.account_id}"
  force_destroy = true
  tags          = local.tags
}

# Encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "raw_enc" {
  bucket = aws_s3_bucket.raw.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.data_key.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "iceberg_enc" {
  bucket = aws_s3_bucket.iceberg.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.data_key.arn
    }
    bucket_key_enabled = true
  }
}

# Block public access
resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "iceberg" {
  bucket                  = aws_s3_bucket.iceberg.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── IAM: Glue Job Execution Role ────────────────────────────────────────────

data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_job" {
  name               = "${local.prefix}-glue-job-role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
  tags               = local.tags
}

data "aws_iam_policy_document" "glue_job_policy" {
  # S3 access (least privilege: raw=read, iceberg=read+write)
  statement {
    sid       = "S3RawRead"
    actions   = ["s3:GetObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.raw.arn, "${aws_s3_bucket.raw.arn}/*"]
  }
  statement {
    sid     = "S3IcebergReadWrite"
    actions = [
      "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
      "s3:ListBucket", "s3:GetBucketLocation"
    ]
    resources = [aws_s3_bucket.iceberg.arn, "${aws_s3_bucket.iceberg.arn}/*"]
  }
  # KMS
  statement {
    sid       = "KMSUsage"
    actions   = ["kms:Decrypt", "kms:Encrypt", "kms:GenerateDataKey*", "kms:DescribeKey"]
    resources = [aws_kms_key.data_key.arn]
  }
  # Glue catalog (for Iceberg metadata operations)
  statement {
    sid     = "GlueCatalog"
    actions = [
      "glue:GetDatabase", "glue:CreateDatabase",
      "glue:GetTable", "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable",
      "glue:GetPartitions", "glue:BatchCreatePartition",
    ]
    resources = ["*"]
  }
  # CloudWatch Logs
  statement {
    sid       = "Logs"
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["arn:aws:logs:${var.aws_region}:${var.account_id}:*"]
  }
}

resource "aws_iam_role_policy" "glue_job" {
  name   = "${local.prefix}-glue-job-policy"
  role   = aws_iam_role.glue_job.id
  policy = data.aws_iam_policy_document.glue_job_policy.json
}

# ── IAM: Persona Roles ──────────────────────────────────────────────────────

# Data Engineer – full pipeline access
resource "aws_iam_role" "data_engineer" {
  name               = "${local.prefix}-data-engineer"
  assume_role_policy = data.aws_iam_policy_document.human_assume.json
  tags               = local.tags
}

# Data Analyst – read-only on silver/gold
resource "aws_iam_role" "data_analyst" {
  name               = "${local.prefix}-data-analyst"
  assume_role_policy = data.aws_iam_policy_document.human_assume.json
  tags               = local.tags
}

# Platform Admin – full admin
resource "aws_iam_role" "platform_admin" {
  name               = "${local.prefix}-platform-admin"
  assume_role_policy = data.aws_iam_policy_document.human_assume.json
  tags               = local.tags
}

data "aws_iam_policy_document" "human_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${var.account_id}:root"]
    }
  }
}

# Analyst policy: read-only S3 + Athena
data "aws_iam_policy_document" "analyst_policy" {
  statement {
    sid       = "S3ReadOnly"
    actions   = ["s3:GetObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.iceberg.arn, "${aws_s3_bucket.iceberg.arn}/*"]
  }
  statement {
    sid       = "AthenaQuery"
    actions   = ["athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults"]
    resources = ["*"]
  }
  statement {
    sid       = "KMSDecrypt"
    actions   = ["kms:Decrypt", "kms:DescribeKey"]
    resources = [aws_kms_key.data_key.arn]
  }
}

resource "aws_iam_role_policy" "analyst" {
  name   = "${local.prefix}-analyst-policy"
  role   = aws_iam_role.data_analyst.id
  policy = data.aws_iam_policy_document.analyst_policy.json
}

# ── LakeFormation Admin (Terraform caller) ───────────────────────────────────

data "aws_caller_identity" "current" {}

locals {
  caller_role_name = regex("assumed-role/([^/]+)/", data.aws_caller_identity.current.arn)[0]
}

data "aws_iam_role" "terraform_caller" {
  name = local.caller_role_name
}

resource "aws_lakeformation_data_lake_settings" "this" {
  admins = [data.aws_iam_role.terraform_caller.arn]
}

# ── Glue Catalog Databases ───────────────────────────────────────────────────

resource "aws_glue_catalog_database" "bronze" {
  name = "bronze"
}

resource "aws_glue_catalog_database" "gold" {
  name = "gold"
}

# ── Lake Formation ───────────────────────────────────────────────────────────

resource "aws_lakeformation_resource" "iceberg_bucket" {
  arn      = aws_s3_bucket.iceberg.arn
  role_arn = aws_iam_role.glue_job.arn
}

# Grant data_engineer full access
resource "aws_lakeformation_permissions" "engineer_all" {
  principal   = aws_iam_role.data_engineer.arn
  permissions = ["ALL"]
  database {
    name = aws_glue_catalog_database.bronze.name
  }
  depends_on = [aws_glue_catalog_database.bronze, aws_lakeformation_data_lake_settings.this]
}

# Grant data_analyst SELECT on gold
resource "aws_lakeformation_permissions" "analyst_gold_select" {
  principal   = aws_iam_role.data_analyst.arn
  permissions = ["SELECT"]
  table {
    database_name = aws_glue_catalog_database.gold.name
    wildcard      = true
  }
  depends_on = [aws_glue_catalog_database.gold, aws_lakeformation_data_lake_settings.this]
}

# ── Glue Job Definition ─────────────────────────────────────────────────────

resource "aws_glue_job" "pipeline" {
  name     = "${local.prefix}-job"
  role_arn = aws_iam_role.glue_job.arn

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.raw.id}/scripts/spark_job.py"
    python_version  = "3"
  }

  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 60

  default_arguments = {
    "--extra-py-files"               = "s3://${aws_s3_bucket.raw.id}/scripts/pipeline_deps.zip"
    "--datalake-formats"             = "iceberg"
    "--conf"                         = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    "--RUNTIME"                      = "aws_glue"
    "--ICEBERG_WAREHOUSE"            = "s3://${aws_s3_bucket.iceberg.id}/iceberg/"
    "--ICEBERG_CATALOG"              = "iceberg"
    "--RAW_SOURCE_PATH"              = "s3://${aws_s3_bucket.raw.id}/tpch/"
    "--PERSONA"                      = "data_engineer"
    "--additional-python-modules"    = ""
    "--enable-metrics"               = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-language"                 = "python"
  }

  tags = local.tags
}

# ── Outputs ──────────────────────────────────────────────────────────────────

output "raw_bucket"     { value = aws_s3_bucket.raw.id }
output "iceberg_bucket" { value = aws_s3_bucket.iceberg.id }
output "glue_job_name"  { value = aws_glue_job.pipeline.name }
output "kms_key_arn"    { value = aws_kms_key.data_key.arn }
output "glue_role_arn"  { value = aws_iam_role.glue_job.arn }
