"""
config.py – Centralised, immutable configuration for the Iceberg multi-cloud pipeline.

Design decisions
────────────────
1.  All tunables come from environment variables so the same artefact runs
    everywhere (Glue, Fabric notebook, local spark-submit) without code changes.
2.  A frozen dataclass gives type safety and prevents accidental mutation.
3.  Validation happens at import time so a mis-configured job fails fast with a
    clear message rather than dying mid-pipeline.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional


# ── helpers ──────────────────────────────────────────────────────────────────

def _require(name: str) -> str:
    """Return env var value or raise with an actionable message."""
    val = os.environ.get(name, "").strip()
    if not val:
        raise EnvironmentError(
            f"Required environment variable '{name}' is not set. "
            f"See env.example for the full list."
        )
    return val


def _optional(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


# ── configuration dataclass ──────────────────────────────────────────────────

@dataclass(frozen=True)
class PipelineConfig:
    """Immutable pipeline configuration built from environment variables."""

    # Core
    runtime: str            # aws_glue | fabric | local
    iceberg_warehouse: str  # Warehouse root URI
    iceberg_catalog: str    # Spark SQL catalog name
    raw_source_path: str    # Path to raw CSVs / Parquet
    persona: str            # data_engineer | data_analyst | platform_admin

    # AWS
    aws_region: str = "eu-west-2"
    kms_key_arn: str = ""
    s3_endpoint: str = ""   # MinIO override

    # Azure / Fabric
    azure_storage_account: str = ""
    azure_container: str = ""
    key_vault_uri: str = ""
    fabric_workspace_id: str = ""

    # MinIO (local only)
    minio_root_user: str = "minioadmin"
    minio_root_password: str = "minioadmin"

    # Derived / constants
    bronze_db: str = "bronze"
    silver_db: str = "silver"
    gold_db: str = "gold"

    # Tables
    source_tables: tuple = ("orders", "lineitem")

    # Retry settings
    max_retries: int = 3
    retry_delay_seconds: int = 5

    @property
    def is_aws(self) -> bool:
        return self.runtime == "aws_glue"

    @property
    def is_fabric(self) -> bool:
        return self.runtime == "fabric"

    @property
    def is_local(self) -> bool:
        return self.runtime == "local"


def load_config() -> PipelineConfig:
    """Build a PipelineConfig from the current environment."""
    runtime = _require("RUNTIME")
    if runtime not in ("aws_glue", "fabric", "local"):
        raise ValueError(
            f"RUNTIME must be one of aws_glue, fabric, local. Got: '{runtime}'"
        )

    return PipelineConfig(
        runtime=runtime,
        iceberg_warehouse=_require("ICEBERG_WAREHOUSE"),
        iceberg_catalog=_optional("ICEBERG_CATALOG", "iceberg"),
        raw_source_path=_require("RAW_SOURCE_PATH"),
        persona=_optional("PERSONA", "data_engineer"),
        aws_region=_optional("AWS_REGION", "eu-west-2"),
        kms_key_arn=_optional("KMS_KEY_ARN"),
        s3_endpoint=_optional("S3_ENDPOINT"),
        azure_storage_account=_optional("AZURE_STORAGE_ACCOUNT"),
        azure_container=_optional("AZURE_CONTAINER"),
        key_vault_uri=_optional("KEY_VAULT_URI"),
        fabric_workspace_id=_optional("FABRIC_WORKSPACE_ID"),
        minio_root_user=_optional("MINIO_ROOT_USER", "minioadmin"),
        minio_root_password=_optional("MINIO_ROOT_PASSWORD", "minioadmin"),
    )
