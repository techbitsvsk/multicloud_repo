"""
spark_factory.py – SparkSession factory that abstracts platform differences.

Design decisions
────────────────
1.  The factory returns a plain SparkSession in every case.  On AWS Glue we
    still initialise GlueContext (required for bookmarks / metrics) but hand
    back its underlying SparkSession so the pipeline code is 100 % portable.
2.  All Iceberg catalog config is set here via Spark conf – the Hadoop catalog
    type works identically over S3, ABFS, and S3A/MinIO because the underlying
    Hadoop FileSystem implementation handles the protocol differences.
3.  On Fabric the %%configure cell must be run *before* this factory is called
    (notebooks execute %%configure at session startup).  The factory therefore
    only validates that the expected catalog already exists.
4.  Secrets / credentials are never passed through config values – they come
    from IAM roles (AWS), managed identity (Azure), or env vars (local MinIO).
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from config import PipelineConfig

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


# ── Public API ───────────────────────────────────────────────────────────────

def create_spark_session(cfg: PipelineConfig) -> "SparkSession":
    """
    Return a fully configured SparkSession with Iceberg catalog registered.

    The caller passes a PipelineConfig; the factory chooses the right builder
    path depending on cfg.runtime.
    """
    if cfg.is_aws:
        return _build_glue_session(cfg)
    elif cfg.is_fabric:
        return _build_fabric_session(cfg)
    elif cfg.is_local:
        return _build_local_session(cfg)
    else:
        raise ValueError(f"Unknown runtime: {cfg.runtime}")


# ── Private builders ─────────────────────────────────────────────────────────

def _iceberg_conf(cfg: PipelineConfig) -> dict:
    """Return Spark conf entries common to every runtime."""
    cat = cfg.iceberg_catalog
    return {
        f"spark.sql.catalog.{cat}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{cat}.type": "hadoop",
        f"spark.sql.catalog.{cat}.warehouse": cfg.iceberg_warehouse,
        "spark.sql.defaultCatalog": cat,
        "spark.sql.extensions": (
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        ),
        # Write settings for performance
        "spark.sql.iceberg.handle-timestamp-without-timezone": "true",
    }


def _build_glue_session(cfg: PipelineConfig) -> "SparkSession":
    """
    AWS Glue path.

    Glue 4.0+ ships Iceberg runtime on the classpath when you set
    --datalake-formats=iceberg in the job parameters.  We still set the
    catalog conf explicitly so it matches our naming convention.

    GlueContext is initialised for job-bookmark support; we return its
    underlying SparkSession.
    """
    import sys
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    # Glue injects --JOB_NAME via the service
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])

    sc = SparkContext.getOrCreate()
    glue_ctx = GlueContext(sc)
    spark = glue_ctx.spark_session

    # Apply Iceberg catalog config. Skip static configs that Glue already sets
    # at JVM startup via --conf / --datalake-formats and cannot be overridden.
    _STATIC = {"spark.sql.extensions"}
    for k, v in _iceberg_conf(cfg).items():
        if k not in _STATIC:
            spark.conf.set(k, v)

    # Initialise Glue Job for bookmarks / metrics
    job = Job(glue_ctx)
    job.init(args["JOB_NAME"], args)

    # Stash job reference so the main script can call job.commit()
    spark._glue_job = job  # type: ignore[attr-defined]

    return spark


def _build_fabric_session(cfg: PipelineConfig) -> "SparkSession":
    """
    Microsoft Fabric path.

    In a Fabric Data Engineering notebook the SparkSession already exists
    (created by the %%configure cell or the platform default).  We retrieve
    the active session and validate that our Iceberg catalog is registered.

    IMPORTANT – Fabric caveats:
    ───────────────────────────
    •  The %%configure cell MUST appear as the *first* executable cell in the
       notebook.  It is a notebook-level directive, not Python code.
    •  Fabric Spark pools run Spark 3.5 with Delta as the default catalog.
       To use Iceberg you must supply the iceberg-spark-runtime JAR via
       spark.jars.packages in %%configure.
    •  OneLake paths use abfss:// – the storage account is the Fabric
       workspace's backing ADLS Gen2 account.
    •  Workspace identity (managed identity) must have Storage Blob Data
       Contributor on the container.  There is no service-principal passthrough
       via shortcuts today; use direct abfss paths.
    •  Some Fabric runtimes cap custom JARs – verify your Fabric capacity SKU.
    """
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    # Validate catalog is registered (it should be via %%configure)
    cat = cfg.iceberg_catalog
    cat_class = spark.conf.get(f"spark.sql.catalog.{cat}", "")
    if "iceberg" not in cat_class.lower():
        raise RuntimeError(
            f"Iceberg catalog '{cat}' is not registered in the Spark session. "
            f"Ensure the %%configure cell in the notebook sets "
            f"spark.sql.catalog.{cat} to the Iceberg SparkCatalog class."
        )

    return spark


def _build_local_session(cfg: PipelineConfig) -> "SparkSession":
    """
    Local / on-prem path (MinIO + local Spark).

    We build a full SparkSession with:
    •  S3A filesystem pointed at MinIO via S3_ENDPOINT.
    •  Path-style access (required for MinIO).
    •  Iceberg runtime JAR pulled from Maven (requires internet on first run,
       then cached in ~/.ivy2).
    •  Credentials from env vars (MINIO_ROOT_USER / MINIO_ROOT_PASSWORD or
       AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY).
    """
    from pyspark.sql import SparkSession

    # Iceberg + AWS bundle JARs (Spark 3.5 / Scala 2.12)
    ICEBERG_VERSION = "1.5.0"
    SPARK_COMPAT = "3.5"
    SCALA_COMPAT = "2.12"
    packages = (
        f"org.apache.iceberg:iceberg-spark-runtime-{SPARK_COMPAT}_{SCALA_COMPAT}:{ICEBERG_VERSION},"
        f"org.apache.hadoop:hadoop-aws:3.3.4,"
        f"com.amazonaws:aws-java-sdk-bundle:1.12.262"
    )

    builder = (
        SparkSession.builder
        .master("local[*]")
        .appName("iceberg-pipeline-local")
        .config("spark.jars.packages", packages)
        # S3A / MinIO filesystem config
        .config("spark.hadoop.fs.s3a.endpoint", cfg.s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key",
                os.environ.get("AWS_ACCESS_KEY_ID", cfg.minio_root_user))
        .config("spark.hadoop.fs.s3a.secret.key",
                os.environ.get("AWS_SECRET_ACCESS_KEY", cfg.minio_root_password))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Driver memory for local dev
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
    )

    # Apply Iceberg catalog conf
    for k, v in _iceberg_conf(cfg).items():
        builder = builder.config(k, v)

    return builder.getOrCreate()
