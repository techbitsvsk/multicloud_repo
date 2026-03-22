# Azure / Microsoft Fabric Deployment Guide

Complete step-by-step guide to deploy and run the Iceberg pipeline on Microsoft Fabric
with ADLS Gen2 storage, Entra ID governance, and Fabric Warehouse queries.

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Architecture Overview](#2-architecture-overview)
3. [What Bicep Can and Cannot Do](#3-what-bicep-can-and-cannot-do)
4. [Phase 1: Deploy Azure Infrastructure (Bicep)](#4-phase-1-deploy-azure-infrastructure-bicep)
5. [Phase 2: Create Entra ID Groups](#5-phase-2-create-entra-id-groups)
6. [Phase 3: Create Fabric Workspace & Lakehouse (Manual)](#6-phase-3-create-fabric-workspace--lakehouse-manual)
7. [Phase 4: Upload Raw Data](#7-phase-4-upload-raw-data)
8. [Phase 5: Create and Run the Notebook](#8-phase-5-create-and-run-the-notebook)
9. [Query Iceberg Tables via Fabric Warehouse](#9-query-iceberg-tables-via-fabric-warehouse)
10. [Access Management & Personas](#10-access-management--personas)
11. [OneLake & Lakehouse Shortcuts](#11-onelake--lakehouse-shortcuts)
12. [Security Configuration](#12-security-configuration)
13. [Fabric Caveats & Limitations](#13-fabric-caveats--limitations)
14. [Monitoring & Audit](#14-monitoring--audit)
15. [Cost Estimation](#15-cost-estimation)
16. [Troubleshooting](#16-troubleshooting)
17. [Destroy Resources](#17-destroy-resources)

---

## 1. Prerequisites

### Tools

| Tool | Version | Install |
|------|---------|---------|
| Azure CLI | >= 2.60 | `winget install Microsoft.AzureCLI` |
| PowerShell | 7+ | `winget install Microsoft.PowerShell` |
| Git | any | `winget install Git.Git` |

### Azure / Fabric

- Azure subscription with Contributor access.
- Microsoft Fabric capacity (F2 or higher recommended). Trial capacity works for testing.
- Fabric workspace with Spark compute enabled (any capacity SKU).
- Entra ID (Azure AD) permissions to create groups and role assignments.

### Verify

```powershell
az version                       # >= 2.60
az login                         # Interactive login
az account show --query name     # Confirm correct subscription
```

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                      Azure Subscription                             │
│                                                                     │
│  ┌────────────────────────────────────────────────────┐             │
│  │   ADLS Gen2 Storage Account (Bicep-managed)        │             │
│  │                                                    │             │
│  │  Container: raw/          Container: iceberg/      │             │
│  │    tpch/orders.csv          bronze/                │             │
│  │    tpch/lineitem.csv        silver/                │             │
│  │                             gold/                  │             │
│  └─────────────────┬──────────────────────────────────┘             │
│                    │                                                │
│  ┌─────────────────▼──────────────────────────────────┐             │
│  │        Microsoft Fabric (Manual setup)              │             │
│  │                                                     │             │
│  │  Workspace ──▶ Lakehouse ──▶ Notebook               │             │
│  │                    │           (%%configure +        │             │
│  │                    │            spark_job.py)        │             │
│  │                    │                                 │             │
│  │              SQL Endpoint ◀── Iceberg gold tables    │             │
│  └─────────────────────────────────────────────────────┘             │
│                                                                     │
│  ┌─────────────────────────────────────────────────────┐             │
│  │  Entra ID Groups (az ad group)                      │             │
│  │    grp-data-engineer  →  Blob Contributor           │             │
│  │    grp-data-analyst   →  Blob Reader                │             │
│  │    grp-platform-admin →  Blob Contributor + Owner   │             │
│  └─────────────────────────────────────────────────────┘             │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 3. What Bicep Can and Cannot Do

This is critical to understand before deploying.

### Bicep CREATES (automated)

| Resource | What It Does |
|----------|-------------|
| ADLS Gen2 Storage Account | HNS-enabled storage for raw and Iceberg data |
| Blob containers (`raw`, `iceberg`) | Separate containers for source and warehouse |
| Storage encryption (Microsoft-managed keys) | Encryption at rest |
| Network ACLs (default deny) | Firewall rules — add your VNet |
| RBAC role assignments | Blob Contributor/Reader for Entra groups |

### Bicep CANNOT CREATE (manual steps required)

| Resource | Why | Workaround |
|----------|-----|-----------|
| **Fabric Workspace** | No ARM/Bicep resource provider. Fabric is a SaaS service, not an Azure resource. | Create via Fabric portal or Power BI REST API |
| **Fabric Lakehouse** | Same — SaaS layer above Azure | Create via Fabric portal |
| **Fabric Notebook** | Same | Create via portal, paste code |
| **Fabric Capacity** | Can be created via Bicep (`Microsoft.Fabric/capacities`) but typically pre-exists | Create via portal or `az resource create` |
| **OneLake Shortcuts** | Fabric-specific API, not ARM | Create via portal or Fabric REST API |
| **Workspace Identity** | Fabric-managed | Enable via workspace settings |

### Optional: Fabric Capacity via Bicep

If you need to create a Fabric capacity programmatically (separate from this pipeline):

```bicep
resource fabricCapacity 'Microsoft.Fabric/capacities@2023-11-01' = {
  name: 'iceberg-pipeline-capacity'
  location: location
  sku: { name: 'F2', tier: 'Fabric' }
  properties: {
    administration: {
      members: ['admin@yourdomain.com']
    }
  }
}
```

> **Cost warning**: Fabric capacities bill continuously while active. Use F2 for dev (~$262/month) or pause when idle.

---

## 4. Phase 1: Deploy Azure Infrastructure (Bicep)

### 4a. Create resource group

```powershell
$LOCATION = "uksouth"
$RG = "rg-iceberg-pipeline"
$STORAGE_ACCOUNT = "sticebergpipeline"    # Must be globally unique, lowercase, no hyphens

az group create --name $RG --location $LOCATION
```

### 4b. Deploy Bicep template

```powershell
cd bicep

az deployment group create `
  --resource-group $RG `
  --template-file azure.bicep `
  --parameters storageAccountName=$STORAGE_ACCOUNT
```

**Output**:
```
storageAccountId    = /subscriptions/.../storageAccounts/sticebergpipeline
rawContainerPath    = abfss://raw@sticebergpipeline.dfs.core.windows.net/
icebergWarehousePath = abfss://iceberg@sticebergpipeline.dfs.core.windows.net/iceberg/
```

Save the `icebergWarehousePath` — you need it for the notebook.

### 4c. Allow network access (dev only)

The Bicep template defaults to `Deny` for network ACLs. For development, allow your IP:

```powershell
$MY_IP = (Invoke-RestMethod -Uri "https://api.ipify.org")

az storage account network-rule add `
  --resource-group $RG `
  --account-name $STORAGE_ACCOUNT `
  --ip-address $MY_IP
```

For production, add VNet rules instead:
```powershell
az storage account network-rule add `
  --resource-group $RG `
  --account-name $STORAGE_ACCOUNT `
  --vnet-name my-vnet `
  --subnet my-subnet
```

---

## 5. Phase 2: Create Entra ID Groups

```powershell
# Create groups
$ENGINEER_GROUP = (az ad group create `
  --display-name "grp-data-engineer" `
  --mail-nickname "grp-data-engineer" `
  --query id --output tsv)

$ANALYST_GROUP = (az ad group create `
  --display-name "grp-data-analyst" `
  --mail-nickname "grp-data-analyst" `
  --query id --output tsv)

$ADMIN_GROUP = (az ad group create `
  --display-name "grp-platform-admin" `
  --mail-nickname "grp-platform-admin" `
  --query id --output tsv)

echo "Engineer group: $ENGINEER_GROUP"
echo "Analyst group:  $ANALYST_GROUP"
echo "Admin group:    $ADMIN_GROUP"
```

### Assign RBAC roles (if not done via Bicep)

Re-deploy Bicep with group IDs, or assign manually:

```powershell
$SCOPE = (az storage account show -n $STORAGE_ACCOUNT -g $RG --query id -o tsv)

# Engineer: Storage Blob Data Contributor (read + write)
az role assignment create `
  --assignee-object-id $ENGINEER_GROUP `
  --assignee-principal-type Group `
  --role "Storage Blob Data Contributor" `
  --scope $SCOPE

# Analyst: Storage Blob Data Reader (read-only)
az role assignment create `
  --assignee-object-id $ANALYST_GROUP `
  --assignee-principal-type Group `
  --role "Storage Blob Data Reader" `
  --scope $SCOPE

# Platform Admin: Owner (full control + RBAC management)
az role assignment create `
  --assignee-object-id $ADMIN_GROUP `
  --assignee-principal-type Group `
  --role "Owner" `
  --scope $SCOPE
```

### Add users to groups

```powershell
# Get your own user object ID
$MY_ID = (az ad signed-in-user show --query id -o tsv)

# Add yourself to engineers group
az ad group member add --group "grp-data-engineer" --member-id $MY_ID

# Add another user to analysts
$USER_ID = (az ad user show --id "analyst@yourdomain.com" --query id -o tsv)
az ad group member add --group "grp-data-analyst" --member-id $USER_ID
```

---

## 6. Phase 3: Create Fabric Workspace & Lakehouse (Manual)

These steps **cannot be automated via Bicep/ARM**. They require the Fabric portal or
the Power BI REST API.

### 6a. Create Fabric Workspace

1. Go to https://app.fabric.microsoft.com
2. Click **Workspaces** (left nav) → **+ New workspace**
3. Name: `iceberg-pipeline-dev`
4. Under **Advanced** → **License mode**: select your Fabric capacity (F2+)
5. Click **Apply**

**Via Power BI REST API** (alternative):
```powershell
$token = (az account get-access-token --resource "https://analysis.windows.net/powerbi/api" --query accessToken -o tsv)

$body = @{
    name = "iceberg-pipeline-dev"
} | ConvertTo-Json

Invoke-RestMethod `
  -Method POST `
  -Uri "https://api.powerbi.com/v1.0/myorg/groups" `
  -Headers @{ Authorization = "Bearer $token"; "Content-Type" = "application/json" } `
  -Body $body
```

Save the workspace ID (visible in the URL: `https://app.fabric.microsoft.com/groups/{WORKSPACE_ID}`).

### 6b. Create Lakehouse

1. In the workspace, click **+ New** → **Lakehouse**
2. Name: `iceberg_lakehouse`
3. Click **Create**

### 6c. Enable Workspace Identity (required for storage access)

1. Go to **Workspace Settings** → **General** → **Workspace identity**
2. Toggle **Enable workspace identity**
3. Copy the workspace identity's application/object ID

Grant the workspace identity access to your storage:
```powershell
$WORKSPACE_IDENTITY_OID = "<object-id-from-step-above>"

az role assignment create `
  --assignee-object-id $WORKSPACE_IDENTITY_OID `
  --assignee-principal-type ServicePrincipal `
  --role "Storage Blob Data Contributor" `
  --scope (az storage account show -n $STORAGE_ACCOUNT -g $RG --query id -o tsv)
```

---

## 7. Phase 4: Upload Raw Data

### Option A: Azure CLI

```powershell
# Upload sample data to raw container
az storage blob upload `
  --account-name $STORAGE_ACCOUNT `
  --container-name raw `
  --file tests\sample_data\orders.csv `
  --name tpch/orders.csv `
  --auth-mode login

az storage blob upload `
  --account-name $STORAGE_ACCOUNT `
  --container-name raw `
  --file tests\sample_data\lineitem.csv `
  --name tpch/lineitem.csv `
  --auth-mode login

# Verify
az storage blob list `
  --account-name $STORAGE_ACCOUNT `
  --container-name raw `
  --prefix "tpch/" `
  --auth-mode login `
  --output table
```

### Option B: Upload via Fabric Lakehouse UI

1. Open the Lakehouse in Fabric
2. Click **Files** → **Upload** → **Upload files**
3. Create a `tpch` folder, upload `orders.csv` and `lineitem.csv`

> If using this method, `RAW_SOURCE_PATH` should reference the Lakehouse Files path
> instead: `abfss://<workspace-id>@onelake.dfs.fabric.microsoft.com/<lakehouse-id>/Files/tpch/`

---

## 8. Phase 5: Create and Run the Notebook

### 8a. Create Notebook

1. In the Fabric workspace, click **+ New** → **Notebook**
2. Name: `iceberg_pipeline`
3. Attach it to the Lakehouse (top-right: **Lakehouse** → select `iceberg_lakehouse`)

### 8b. Assign the correct RBAC role on the storage account

> **Common mistake:** The Azure `Contributor` role is a *control plane* role — it grants
> rights to manage the storage account resource but **not** to read or write blob/ADLS data.
> Spark's Hadoop filesystem layer hits the data plane and gets a 403
> (`AccessDeniedException: This request is not authorized to perform this operation`).
>
> You need **Storage Blob Data Contributor** (data plane) on the storage account or container.

1. Azure Portal → your storage account (`sticebergpipeline`) → **Access Control (IAM)**
2. **+ Add role assignment**
3. Role: **Storage Blob Data Contributor**
4. Assign access to: **Managed identity**
5. Select: your Fabric workspace's managed identity
   - Found under: Fabric workspace → **Workspace settings** → **Identity** tab — copy the Object ID
   - In the role assignment picker, filter managed identities by that Object ID
6. Save — allow 1–2 minutes to propagate before re-running the notebook

### 8c. Cell 1: %%configure (MUST be first cell)

The `%%configure` cell must also declare the ABFS OAuth settings so Spark knows to
use the workspace managed identity for the external storage account.
Without these, Spark attempts an unauthenticated request and gets a 403 even if RBAC is correct.

```
%%configure -f
{
  "sessionTimeoutInSeconds": 3600,
  "conf": {
    "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
    "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.iceberg.type": "hadoop",
    "spark.sql.catalog.iceberg.warehouse": "abfss://iceberg@sticebergpipeline.dfs.core.windows.net/iceberg/",
    "spark.sql.defaultCatalog": "iceberg",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.hadoop.fs.azure.account.key.sticebergpipeline.dfs.core.windows.net": "<storage-account-key>"
  }
}
```

> **Replace** `sticebergpipeline` with your actual storage account name and `<storage-account-key>` with Key 1 from the storage account.
>
> `sessionTimeoutInSeconds` is a **top-level Fabric property** (outside `"conf"`). Fabric does
> NOT use `livy.server.session.timeout` — that is a different Spark platform convention.
> Default idle timeout in Fabric is 20 minutes; `3600` = 1 hour.
>
> You can also adjust timeout without restarting via the session status indicator
> (bottom-left of the notebook) or at workspace level:
> **Workspace Settings → Data Engineering/Science → Spark settings → Jobs tab**.

### 8c. Cell 2: Set environment variables

```python
import os

os.environ["RUNTIME"]            = "fabric"
os.environ["ICEBERG_WAREHOUSE"]  = "abfss://iceberg@sticebergpipeline.dfs.core.windows.net/iceberg/"
os.environ["ICEBERG_CATALOG"]    = "iceberg"
os.environ["RAW_SOURCE_PATH"]    = "abfss://raw@sticebergpipeline.dfs.core.windows.net/tpch/"
os.environ["PERSONA"]            = "data_engineer"

# Azure-specific (for auditing, not used by Spark directly)
os.environ["AZURE_STORAGE_ACCOUNT"] = "sticebergpipeline"
os.environ["AZURE_CONTAINER"]       = "iceberg"
```

### 8d. Upload the Python files to Lakehouse Files

> **Fabric vs Databricks:** In Microsoft Fabric, `%run` only executes other **Fabric notebooks**.
> It **cannot** run `.py` files from Lakehouse Files — attempting this raises:
> `MagicUsageError: Cannot read notebook Files/...`
> Use `sys.path` + normal Python imports instead (shown below).

1. In your Lakehouse, go to **Files** and create a `pipeline/` folder.
2. Inside `pipeline/`, create a `utils/` subfolder.
3. Upload files:
   - `pipeline/config.py`
   - `pipeline/spark_factory.py`
   - `pipeline/spark_job.py`
   - `pipeline/utils/__init__.py`
   - `pipeline/utils/logging.py`

### 8e. Cell 3: Verify the upload path (run this first)

Before importing, confirm the files are visible and the path is correct:

```python
import os

PIPELINE_PATH = "/lakehouse/default/Files/pipeline"

# Check the path exists
if not os.path.isdir(PIPELINE_PATH):
    raise RuntimeError(
        f"Path not found: {PIPELINE_PATH}\n"
        "Check that:\n"
        "  1. This notebook has a default Lakehouse attached "
        "(Explorer panel → Add Lakehouse → select your lakehouse → tick 'Set as default')\n"
        "  2. You uploaded the files to the 'pipeline/' folder inside Lakehouse Files\n"
        f"  Current /lakehouse/default/Files contents: {os.listdir('/lakehouse/default/Files')}"
    )

# List what was found
print("Files found in pipeline/:")
for root, dirs, files in os.walk(PIPELINE_PATH):
    level = root.replace(PIPELINE_PATH, "").count(os.sep)
    print("  " + "  " * level + os.path.basename(root) + "/")
    for f in files:
        print("  " + "  " * (level + 1) + f)
```

Expected output:
```
Files found in pipeline/:
  pipeline/
    config.py
    spark_factory.py
    spark_job.py
    utils/
      __init__.py
      logging.py
```

### 8f. Cell 4: Add files to driver AND executors

> **Why two steps?** `sys.path.insert` only affects the **driver** process.
> Spark executor workers are separate processes that don't inherit the driver's path —
> they raise `Spark_User_UserApp_ModuleNotFoundError` for any module they can't find.
> `sc.addPyFile()` ships files to every executor before tasks run.

```python
import sys
import zipfile

PIPELINE_PATH = "/lakehouse/default/Files/pipeline"

# ── Driver: add pipeline root so imports resolve locally ──────────────────────
sys.path.insert(0, PIPELINE_PATH)

# ── Executors: ship individual modules ────────────────────────────────────────
sc = spark.sparkContext
for _mod in ["config.py", "spark_factory.py", "spark_job.py"]:
    sc.addPyFile(f"{PIPELINE_PATH}/{_mod}")

# utils/ is a package (has __init__.py) — must be zipped for addPyFile
_zip = "/tmp/pipeline_utils.zip"
with zipfile.ZipFile(_zip, "w") as zf:
    for _f in ["__init__.py", "logging.py"]:
        zf.write(f"{PIPELINE_PATH}/utils/{_f}", f"utils/{_f}")
sc.addPyFile(_zip)
```

### 8g. Cell 5: Set environment variables (as above in Cell 2)

### 8h. Cell 6: Run the pipeline

```python
from spark_job import main
main()
```

### 8i. Run All

Click **Run All** or execute cells sequentially. Expected runtime: 2–5 minutes for sample data.

---

## 9. Query Iceberg Tables via Fabric Warehouse

### Option A: Spark SQL (in notebook)

Add a new cell after the pipeline runs:

Always qualify with the catalog name (`iceberg.`) — without it, Spark resolves against the
Lakehouse Delta catalog and raises `TABLE_OR_VIEW_NOT_FOUND`:

```python
spark.sql("SELECT order_year, SUM(total_revenue) AS rev FROM iceberg.gold.revenue_by_order_date GROUP BY order_year ORDER BY order_year").show()
spark.sql("SELECT customer_key, total_revenue FROM iceberg.gold.top_customers LIMIT 10").show()
```

### Option B: Lakehouse SQL Endpoint

1. In the Lakehouse, click **SQL analytics endpoint** (top-right toggle)
2. The Iceberg tables should appear under Tables if properly registered
3. Run:
```sql
SELECT TOP 10 customer_key, total_revenue
FROM gold.top_customers
ORDER BY total_revenue DESC;
```

### Option C: Create Fabric Warehouse Views

If the Iceberg tables don't appear automatically in the SQL endpoint (common with Hadoop catalog), create external references:

1. Create a new **Warehouse** item in the workspace
2. Use a Spark notebook to register tables in the Lakehouse catalog:
```python
# This makes tables visible in the SQL endpoint
spark.sql("""
  CREATE OR REPLACE VIEW default.gold_revenue AS
  SELECT * FROM iceberg.gold.revenue_by_order_date
""")
```

---

## 10. Access Management

### Minimum required assignments for your user account

Two role assignments are needed — one on Azure storage (data plane), one on the Fabric workspace:

**1. Storage account — Azure Portal**

| Assignment | Where | Role |
|-----------|-------|------|
| Your user or Entra group | `sticebergpipeline` → Access Control (IAM) | **Storage Blob Data Contributor** |

> **Common mistake:** The `Contributor` role is a control-plane role — it manages the storage
> account resource but does **not** grant read/write access to blobs or ADLS paths.
> Spark's ABFS driver operates at the data plane and gets a 403 without this role.

**2. Fabric workspace — Fabric portal**

| Assignment | Where | Role |
|-----------|-------|------|
| Your user or Entra group | Workspace → Manage access | **Member** (run notebooks, edit) or **Admin** |

### Recommendation: use Entra groups for team access

Assigning roles directly to individual users does not scale. For any team setup:

1. **Azure Portal → Microsoft Entra ID → Groups → New group**
   - Create e.g. `grp-iceberg-engineers`
   - Add team members to the group

2. **Assign the group** (not individuals) to:
   - `sticebergpipeline` storage → Storage Blob Data Contributor
   - Fabric workspace → Member

3. Onboarding a new team member then requires only adding them to the group —
   no role assignment changes needed.

---

## 11. OneLake Shortcuts → Power BI

Shortcuts let the Fabric Lakehouse reference Iceberg tables in ADLS Gen2 **without copying data**.
OneLake automatically virtualises the Iceberg metadata as Delta (`_delta_log/`), making the tables
visible to the SQL analytics endpoint and Power BI.

### How it works

```
spark_job.py writes Iceberg
        ↓
abfss://iceberg@sticebergpipeline.dfs.core.windows.net/iceberg/gold/<table>/
  ├── metadata/   ← Iceberg metadata
  └── data/       ← Parquet files
        ↓
Lakehouse shortcut (Tables section)
  OneLake converts Iceberg → Delta (_delta_log/ generated automatically)
        ↓
SQL Analytics Endpoint   →   T-SQL queries
        ↓
Direct Lake Semantic Model  →  Power BI reports
```

### Prerequisites

Before creating shortcuts, verify the storage account has **Hierarchical Namespaces** enabled.
This is required for ADLS Gen2 shortcuts — without it, shortcut creation fails silently.

```powershell
az storage account show -n sticebergpipeline -g $RG --query "kind,isHnsEnabled" -o table
```

`isHnsEnabled` must be `true`. The Bicep template in this repo sets this automatically.
If it is `false`, it cannot be enabled on an existing account without a migration — create a new
storage account with HNS enabled and re-run the pipeline.

### Step 1 — Create one shortcut per gold table

Each shortcut must point to an **individual table folder** (the folder containing `metadata/`
and `data/`). Pointing at a namespace folder (e.g. `/iceberg/gold/`) does not work.

> **Important:** Create shortcuts under the **Tables** section of the Lakehouse, not Files.
> Only Tables-section shortcuts are auto-discovered by the SQL endpoint and Power BI.

For each gold table (`revenue_by_order_date`, `top_customers`):

1. Lakehouse → **Tables** → **… menu** → **New shortcut**
2. Source: **Azure Data Lake Storage Gen2**
3. Connection: select or create a connection to `sticebergpipeline`
   - URL: `https://sticebergpipeline.dfs.core.windows.net`
   - Authentication: **Organizational account** (your Entra identity — must have Storage Blob Data Reader or Contributor on the storage)
4. Navigate to the table folder:
   - `iceberg/iceberg/gold/revenue_by_order_date`
   - `iceberg/iceberg/gold/top_customers`
5. Shortcut name: `revenue_by_order_date` / `top_customers` (no spaces, Latin chars only)
6. Click **Create**

### Step 2 — Verify Iceberg → Delta conversion

After creating the shortcut, OneLake converts the Iceberg metadata automatically.

Right-click the shortcut → **View files** — you should see:
```
revenue_by_order_date/
├── _delta_log/                ← generated by OneLake (confirms conversion succeeded)
├── latest_conversion_log.txt  ← check this if the table icon does not appear
├── metadata/
└── data/
```

If `_delta_log/` is missing, open `latest_conversion_log.txt` for the failure reason.
Common causes: unsupported partition transforms (`bucket`, `truncate`, `void`) or
multiple metadata snapshots from a DROP/recreate cycle.

### Step 3 — Query via SQL analytics endpoint

Switch the Lakehouse to **SQL analytics endpoint** (top-right toggle).
The shortcut tables appear automatically:

```sql
SELECT TOP 100 order_year, rev
FROM dbo.revenue_by_order_date
ORDER BY order_year;

SELECT TOP 10 customer_key, total_revenue
FROM dbo.top_customers
ORDER BY total_revenue DESC;
```

### Step 4 — Create a Power BI semantic model

1. In the Lakehouse (Lakehouse view, not SQL endpoint) → **New semantic model**
2. Name it e.g. `iceberg_gold_model`
3. Select the shortcut tables: `revenue_by_order_date`, `top_customers`
4. Click **Confirm** — this creates a **Direct Lake** semantic model (no data copy, live query)
5. Open the semantic model → **Open data model** to add relationships/measures if needed

### Step 5 — Build a Power BI report

1. From the semantic model page → **Create report**
2. Or in Power BI Desktop: **Get data → Microsoft Fabric → Semantic models** → select `iceberg_gold_model`
3. Build visuals against the live Iceberg data

### Shortcut limitations

| Limitation | Detail | Mitigation |
|-----------|--------|-----------|
| **Read-only** | Cannot write through a shortcut | Write via `abfss://` in Spark; shortcut is for reading only |
| **Passthrough identity** | Shortcut uses your Entra user, not a service principal | Ensure your user has Storage Blob Data Reader on the container |
| **No private endpoints** | Shortcut fails if storage is VNet-locked | Add Fabric service tag to storage firewall |
| **Unsupported partition transforms** | `bucket[N]`, `truncate[W]`, `void` block conversion | Avoid these transforms when writing Iceberg via Spark |
| **Same region required** | OneLake shortcut target must be in the same Azure region | Deploy storage in the same region as the Fabric capacity |
| **Schema changes lag** | Column add/rename may not reflect until a data write occurs | Trigger a pipeline re-run after schema changes |

---

## 12. Security Configuration

### Encryption

- **At rest**: ADLS Gen2 encryption is enabled by default (Microsoft-managed keys). For CMK:
  ```powershell
  # Create Key Vault and key first, then:
  az storage account update `
    --name $STORAGE_ACCOUNT `
    --resource-group $RG `
    --encryption-key-source Microsoft.Keyvault `
    --encryption-key-vault "https://my-keyvault.vault.azure.net" `
    --encryption-key-name "storage-key"
  ```

- **In transit**: Enforced — Bicep template sets `supportsHttpsTrafficOnly: true` and `minimumTlsVersion: TLS1_2`.

### Private Endpoints (production)

```powershell
# Create private endpoint for storage
az network private-endpoint create `
  --name pe-storage `
  --resource-group $RG `
  --vnet-name my-vnet `
  --subnet my-subnet `
  --private-connection-resource-id (az storage account show -n $STORAGE_ACCOUNT -g $RG --query id -o tsv) `
  --group-ids blob `
  --connection-name pe-storage-conn

# Private DNS zone
az network private-dns zone create --resource-group $RG --name "privatelink.blob.core.windows.net"
```

### Diagnostic Logging

```powershell
# Enable storage analytics logging
az monitor diagnostic-settings create `
  --name "storage-diagnostics" `
  --resource (az storage account show -n $STORAGE_ACCOUNT -g $RG --query id -o tsv) `
  --logs '[{"category":"StorageRead","enabled":true},{"category":"StorageWrite","enabled":true}]' `
  --metrics '[{"category":"Transaction","enabled":true}]' `
  --workspace "<log-analytics-workspace-id>"
```

---

## 13. Fabric Caveats & Limitations

| Caveat | Detail | Mitigation |
|--------|--------|-----------|
| **%%configure must be Cell 1** | Notebook-level Spark directive — not Python. Any cell above it causes it to be silently ignored. | Always first cell, no exceptions. |
| **Session must be restarted after %%configure changes** | %%configure is applied only at session startup. Editing and re-running the cell in a live session has no effect. | Session → Stop session → Run all. |
| **sessionTimeoutInSeconds not livy.server.session.timeout** | Fabric does not use the standard Livy conf key for idle timeout. | Use `"sessionTimeoutInSeconds": 3600` as a top-level %%configure key (outside `"conf"`). Default is 20 min. |
| **`%run` cannot run .py files** | In Fabric, `%run` runs other Fabric notebooks only. Pointing it at a `.py` file in Lakehouse Files raises `MagicUsageError`. | Use `sys.path.insert` + `sc.addPyFile` instead (see section 8). |
| **sys.path.insert is driver-only** | Executors are separate processes — modules not found on driver path raise `Spark_User_UserApp_ModuleNotFoundError` on workers. | Use `sc.addPyFile()` for `.py` modules and zip packages for sub-packages. |
| **spark.stop() kills the notebook session** | Calling `spark.stop()` in notebook context destroys the shared SparkContext — subsequent cells raise `Cannot call methods on a stopped SparkContext`. | Only call `spark.stop()` in local/standalone mode. The pipeline guards this with `if cfg.is_local`. |
| **Contributor ≠ Storage Blob Data Contributor** | `Contributor` is a control-plane role — no data-plane access. ABFS 403 even with `Contributor` assigned. | Assign **Storage Blob Data Contributor** on the storage account or container. |
| **ABFS auth config required for external storage** | Without `fs.azure.account.key.*` or OAuth config in %%configure, Spark sends unauthenticated requests to external ADLS accounts. | Add auth config to %%configure (see section 8c). |
| **Catalog queries need fully-qualified names** | `gold.table` resolves against the Lakehouse Delta catalog, not Iceberg. Raises `TABLE_OR_VIEW_NOT_FOUND`. | Always use `iceberg.gold.table_name`. |
| **Custom JARs need capacity SKU F2+** | Starter/Trial capacities may reject `spark.jars.packages`. | Upgrade to F2 or higher. |
| **Fabric defaults to Delta, not Iceberg** | Lakehouse natively understands Delta. Iceberg requires explicit JAR + catalog config. | Use %%configure to load Iceberg runtime. |
| **Spark pool startup time** | First cell execution takes 30–90 seconds while the session initialises. | Normal; subsequent cells are fast. |
| **Iceberg not auto-visible in SQL endpoint** | HadoopCatalog stores metadata in files — the SQL endpoint cannot read it directly. | Create shortcuts (section 11) or register Delta views. |
| **Shortcuts are read-only** | Cannot write Iceberg tables through a shortcut path. | Write via `abfss://` in Spark, shortcut for reading/Power BI only. |

---

## 14. Monitoring & Audit

### Spark Monitoring Hub

1. In Fabric workspace → **Monitor** → **Apache Spark applications**
2. View: job duration, stages, tasks, executor metrics, driver logs

### Fabric Audit Logs

1. Go to **Microsoft Purview compliance portal** → **Audit**
2. Search for Fabric activities: `RunNotebook`, `ReadTable`, `ViewReport`

### Azure Storage Logs

```powershell
# View recent storage operations
az monitor activity-log list `
  --resource-group $RG `
  --start-time (Get-Date).AddDays(-7).ToString("yyyy-MM-dd") `
  --query "[?contains(resourceId, '$STORAGE_ACCOUNT')]" `
  --output table
```

---

## 15. Cost Estimation

| Component | Unit Cost | Sample Run | Notes |
|-----------|-----------|-----------|-------|
| ADLS Gen2 Storage | $0.018/GB/month | < $0.01 | Hot tier |
| ADLS Transactions | $0.0065/10K ops | < $0.01 | |
| Fabric Capacity (F2) | ~$0.36/hour | ~$0.06 (10 min) | Pause when idle! |
| Fabric Capacity (F64) | ~$5.76/hour | ~$0.96 (10 min) | Production |
| Entra ID | Free (P1 for advanced) | $0 | Groups are free |
| **Total (dev, sample data)** | | **~$0.10** | Excluding capacity reservation |

> **Cost tip**: Pause Fabric capacity when not in use. In the Azure portal → Fabric Capacity → **Pause**.

---

## 16. Troubleshooting

### 403 AccessDeniedException on ADLS

Two independent checks:

**A — RBAC role**
```powershell
az role assignment list \
  --assignee (az ad signed-in-user show --query id -o tsv) \
  --scope (az storage account show -n $STORAGE_ACCOUNT -g $RG --query id -o tsv) \
  --output table
```
Must show **Storage Blob Data Contributor** (not just Contributor).

**B — ABFS auth config in %%configure**
```python
# In a notebook cell — confirm key landed in Spark conf
print(spark.conf.get(
    "spark.hadoop.fs.azure.account.key.sticebergpipeline.dfs.core.windows.net",
    "NOT SET"
))
```
If `NOT SET`: session was not restarted after editing %%configure. Stop session → Run all.

**C — Storage firewall**
```powershell
az storage account network-rule list --account-name $STORAGE_ACCOUNT -g $RG --output table
```
If `defaultAction: Deny` — enable **"Allow Azure services on the trusted services list"** in
Azure Portal → storage account → Networking.

### MagicUsageError: Cannot read notebook Files/...

`%run` in Fabric runs other Fabric notebooks only — it cannot execute `.py` files from
Lakehouse Files. Use `sys.path.insert` + `sc.addPyFile()` (see section 8f).

### ModuleNotFoundError / Spark_User_UserApp_ModuleNotFoundError

- `ModuleNotFoundError` on the driver: file not uploaded or wrong path. Run the diagnostic cell (section 8e).
- `Spark_User_UserApp_ModuleNotFoundError` on executors: `sys.path` is driver-only.
  Use `sc.addPyFile()` for modules and zip the `utils/` package (see section 8f).

### Cannot call methods on a stopped SparkContext

`spark.stop()` was called — either explicitly or by the pipeline. The pipeline only calls
`spark.stop()` when `RUNTIME=local`. If you see this error in Fabric:
- Check `RUNTIME` is set to `fabric` (not `local`) in Cell 2
- Do not call `spark.stop()` anywhere in notebook cells

### TABLE_OR_VIEW_NOT_FOUND on gold tables

The Iceberg catalog name is missing from the query. Always fully qualify:
```python
spark.sql("SELECT * FROM iceberg.gold.revenue_by_order_date").show()
#                         ↑ required catalog prefix
```

### java.net.URISyntaxException: Relative path in absolute URI

Warehouse URI has no path component — e.g. `abfss://iceberg@account.dfs.core.windows.net`
instead of `abfss://iceberg@account.dfs.core.windows.net/iceberg/`.
Fix: ensure the warehouse in %%configure ends with a path and trailing slash.

### IllegalStateException: Promise already completed

Session is in a broken state — usually from %%configure running against an already-started session.
Fix: Session → Stop session (wait for grey) → Run all cells from Cell 1.

### Session times out mid-run (Cannot call methods on stopped SparkContext)

Default Fabric idle timeout is 20 minutes. Add `"sessionTimeoutInSeconds": 3600` as a
top-level key in %%configure (outside `"conf"`). Alternatively adjust at workspace level:
**Workspace Settings → Data Engineering/Science → Spark settings → Jobs tab**.

### "Iceberg catalog not registered"

%%configure was not the first cell, or session was not restarted after editing it.
Session → Stop session → confirm %%configure is Cell 1 → Run all.

### Shortcut fails to create or table icon does not appear

1. Verify **Hierarchical Namespaces** is enabled on the storage account:
   ```powershell
   az storage account show -n sticebergpipeline -g $RG --query isHnsEnabled
   ```
   Must be `true`. Cannot be enabled post-creation without migration.

2. Confirm the shortcut is in the **Tables** section (not Files) — only Tables-section
   shortcuts are virtualized as Delta and visible to the SQL endpoint and Power BI.

3. Check `latest_conversion_log.txt` inside the shortcut folder for the failure reason.
   Common causes: unsupported partition transforms (`bucket`, `truncate`, `void`), or
   multiple metadata snapshots from a DROP/recreate cycle.

### Notebook runs slowly or fails to start

- Check Fabric capacity is not paused (Azure Portal → Fabric Capacity → Resume)
- Spark pool startup is normal 30–90 s for the first cell
- Custom JARs (`spark.jars.packages`) require capacity SKU F2 or higher

---

## 17. Destroy Resources

### Azure Infrastructure (Bicep-managed)

```powershell
# Delete the entire resource group (storage + RBAC + everything)
az group delete --name $RG --yes --no-wait
```

### Entra ID Groups

```powershell
az ad group delete --group "grp-data-engineer"
az ad group delete --group "grp-data-analyst"
az ad group delete --group "grp-platform-admin"
```

### Fabric Resources (manual)

**Option A: Portal**
1. Go to https://app.fabric.microsoft.com
2. Open workspace `iceberg-pipeline-dev`
3. **Workspace Settings** → **General** → **Remove this workspace**

**Option B: Power BI REST API**
```powershell
$token = (az account get-access-token --resource "https://analysis.windows.net/powerbi/api" --query accessToken -o tsv)

Invoke-RestMethod `
  -Method DELETE `
  -Uri "https://api.powerbi.com/v1.0/myorg/groups/$WORKSPACE_ID" `
  -Headers @{ Authorization = "Bearer $token" }
```

### Pause Fabric Capacity (to stop billing without destroying)

```powershell
# Find capacity resource ID
$CAP_ID = (az resource list --resource-type "Microsoft.Fabric/capacities" --query "[0].id" -o tsv)

# Pause
az resource invoke-action --action suspend --ids $CAP_ID

# Resume later
az resource invoke-action --action resume --ids $CAP_ID
```

### Data-only cleanup (keep infrastructure)

```python
# In a Fabric notebook:
spark.sql("DROP TABLE IF EXISTS iceberg.gold.top_customers PURGE")
spark.sql("DROP TABLE IF EXISTS iceberg.gold.revenue_by_order_date PURGE")
spark.sql("DROP TABLE IF EXISTS iceberg.silver.lineitem PURGE")
spark.sql("DROP TABLE IF EXISTS iceberg.silver.orders PURGE")
spark.sql("DROP TABLE IF EXISTS iceberg.bronze.lineitem PURGE")
spark.sql("DROP TABLE IF EXISTS iceberg.bronze.orders PURGE")
spark.sql("DROP NAMESPACE IF EXISTS iceberg.gold")
spark.sql("DROP NAMESPACE IF EXISTS iceberg.silver")
spark.sql("DROP NAMESPACE IF EXISTS iceberg.bronze")
```

Or via Azure CLI:
```powershell
# Delete Iceberg data files from ADLS
az storage blob delete-batch `
  --account-name $STORAGE_ACCOUNT `
  --source iceberg `
  --pattern "iceberg/*" `
  --auth-mode login
```
