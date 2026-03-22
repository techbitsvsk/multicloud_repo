#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# scripts/minio_setup.sh – Provision MinIO buckets, users, and policies.
#
# Prerequisites:
#   • MinIO server running (docker or binary)
#   • mc (MinIO Client) installed and on PATH
#
# Quick-start (Docker):
#   docker run -d --name minio \
#     -p 9000:9000 -p 9001:9001 \
#     -e MINIO_ROOT_USER=minioadmin \
#     -e MINIO_ROOT_PASSWORD=minioadmin \
#     minio/minio server /data --console-address ":9001"
#
# Windows (PowerShell) – download binary:
#   Invoke-WebRequest -Uri "https://dl.min.io/server/minio/release/windows-amd64/minio.exe" -OutFile minio.exe
#   $env:MINIO_ROOT_USER="minioadmin"; $env:MINIO_ROOT_PASSWORD="minioadmin"
#   .\minio.exe server C:\minio-data --console-address ":9001"
#
# Then run this script (Git Bash / WSL on Windows):
#   bash scripts/minio_setup.sh
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail

MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
ALIAS="local"

echo "▶ Configuring mc alias '${ALIAS}' → ${MINIO_ENDPOINT}"
mc alias set "${ALIAS}" "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"

# ── Buckets ──────────────────────────────────────────────────────────────────

for BUCKET in raw-data iceberg-warehouse; do
  if mc ls "${ALIAS}/${BUCKET}" &>/dev/null; then
    echo "  Bucket '${BUCKET}' already exists."
  else
    mc mb "${ALIAS}/${BUCKET}"
    echo "  Created bucket '${BUCKET}'."
  fi
done

# ── Upload sample TPC-H CSVs (if present) ───────────────────────────────────

SAMPLE_DIR="$(dirname "$0")/../tests/sample_data"
if [ -d "${SAMPLE_DIR}" ]; then
  echo "▶ Uploading sample CSVs from ${SAMPLE_DIR}"
  mc cp "${SAMPLE_DIR}/orders.csv"   "${ALIAS}/raw-data/tpch/orders.csv"
  mc cp "${SAMPLE_DIR}/lineitem.csv" "${ALIAS}/raw-data/tpch/lineitem.csv"
else
  echo "⚠ No sample_data directory found at ${SAMPLE_DIR}. Skipping upload."
fi

# ── Policies (least privilege) ───────────────────────────────────────────────

echo "▶ Creating access policies"

# Data Engineer: full access to both buckets
cat > /tmp/policy_engineer.json <<'POLICY'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:*"],
      "Resource": [
        "arn:aws:s3:::raw-data/*",
        "arn:aws:s3:::raw-data",
        "arn:aws:s3:::iceberg-warehouse/*",
        "arn:aws:s3:::iceberg-warehouse"
      ]
    }
  ]
}
POLICY
mc admin policy create "${ALIAS}" data-engineer /tmp/policy_engineer.json

# Data Analyst: read-only on iceberg-warehouse
cat > /tmp/policy_analyst.json <<'POLICY'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::iceberg-warehouse/*",
        "arn:aws:s3:::iceberg-warehouse"
      ]
    }
  ]
}
POLICY
mc admin policy create "${ALIAS}" data-analyst /tmp/policy_analyst.json

# Platform Admin: full access
cat > /tmp/policy_admin.json <<'POLICY'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:*"],
      "Resource": ["arn:aws:s3:::*"]
    }
  ]
}
POLICY
mc admin policy create "${ALIAS}" platform-admin /tmp/policy_admin.json

# ── Users ────────────────────────────────────────────────────────────────────

echo "▶ Creating users and attaching policies"

create_user() {
  local user=$1 pass=$2 policy=$3
  mc admin user add "${ALIAS}" "${user}" "${pass}" 2>/dev/null || true
  mc admin policy attach "${ALIAS}" "${policy}" --user "${user}"
  echo "  User '${user}' → policy '${policy}'"
}

create_user "engineer1"  "Eng1neer!Pass"  "data-engineer"
create_user "analyst1"   "An@lyst!Pass"   "data-analyst"
create_user "admin1"     "Adm!n1Pass"     "platform-admin"

# ── LDAP / local group mapping example (commented) ──────────────────────────
# If MinIO is configured with LDAP (--idp-ldap-server-url), you map groups:
#   mc admin policy attach local data-analyst --group "cn=analysts,ou=groups,dc=corp,dc=local"
#   mc admin policy attach local data-engineer --group "cn=engineers,ou=groups,dc=corp,dc=local"

echo ""
echo "✅  MinIO setup complete."
echo "    Console:  http://localhost:9001"
echo "    Endpoint: ${MINIO_ENDPOINT}"
echo ""
echo "    Spark SQL GRANT examples (run inside spark-sql shell):"
echo "      -- Analyst: read-only on gold"
echo "      GRANT SELECT ON iceberg.gold TO analyst1;"
echo "      -- Engineer: full access"
echo "      GRANT ALL ON iceberg.bronze TO engineer1;"
echo "      GRANT ALL ON iceberg.silver TO engineer1;"
echo "      GRANT ALL ON iceberg.gold   TO engineer1;"
