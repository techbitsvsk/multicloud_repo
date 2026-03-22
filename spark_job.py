"""
spark_job.py – Unified Bronze → Silver → Gold Iceberg pipeline.

Runs unchanged on AWS Glue, Microsoft Fabric, and local Spark + MinIO.
Platform differences are isolated in spark_factory.py; this file contains
only pure Spark/Iceberg logic.

Design decisions
────────────────
•  Idempotency:  Every write uses CREATE TABLE IF NOT EXISTS + MERGE INTO
   (or INSERT OVERWRITE by partition) so re-runs produce the same result.
•  Retries:      Each phase is wrapped in a retry loop (configurable).
•  Logging:      JSON-structured logs via utils/logging.py.
•  Metrics:      Row counts and durations are recorded per phase/table.

TPC-H tables used
──────────────────
•  orders   – O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE,
              O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT
•  lineitem – L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER,
              L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX,
              L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE,
              L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT
"""

from __future__ import annotations

import os
import sys
import time
import traceback
from typing import Callable

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    DateType, DecimalType, IntegerType, LongType, StringType, StructField, StructType,
)

from config import PipelineConfig, load_config
from spark_factory import create_spark_session
from utils.logging import MetricsCollector, get_logger, timed

# ── Logger & metrics ─────────────────────────────────────────────────────────
log = get_logger()
metrics = MetricsCollector()


# ═══════════════════════════════════════════════════════════════════════════════
# SCHEMAS – explicit schemas for raw CSV ingest (never rely on inference)
# ═══════════════════════════════════════════════════════════════════════════════

ORDERS_SCHEMA = StructType([
    StructField("O_ORDERKEY",      LongType(),    False),
    StructField("O_CUSTKEY",       LongType(),    False),
    StructField("O_ORDERSTATUS",   StringType(),  True),
    StructField("O_TOTALPRICE",    StringType(),  True),   # cast in silver
    StructField("O_ORDERDATE",     StringType(),  True),   # cast in silver
    StructField("O_ORDERPRIORITY", StringType(),  True),
    StructField("O_CLERK",         StringType(),  True),
    StructField("O_SHIPPRIORITY",  IntegerType(), True),
    StructField("O_COMMENT",       StringType(),  True),
])

LINEITEM_SCHEMA = StructType([
    StructField("L_ORDERKEY",      LongType(),    False),
    StructField("L_PARTKEY",       LongType(),    True),
    StructField("L_SUPPKEY",       LongType(),    True),
    StructField("L_LINENUMBER",    IntegerType(), False),
    StructField("L_QUANTITY",      StringType(),  True),   # cast in silver
    StructField("L_EXTENDEDPRICE", StringType(),  True),   # cast in silver
    StructField("L_DISCOUNT",      StringType(),  True),   # cast in silver
    StructField("L_TAX",           StringType(),  True),   # cast in silver
    StructField("L_RETURNFLAG",    StringType(),  True),
    StructField("L_LINESTATUS",    StringType(),  True),
    StructField("L_SHIPDATE",      StringType(),  True),   # cast in silver
    StructField("L_COMMITDATE",    StringType(),  True),   # cast in silver
    StructField("L_RECEIPTDATE",   StringType(),  True),   # cast in silver
    StructField("L_SHIPINSTRUCT",  StringType(),  True),
    StructField("L_SHIPMODE",      StringType(),  True),
    StructField("L_COMMENT",       StringType(),  True),
])

TABLE_SCHEMAS = {
    "orders":   ORDERS_SCHEMA,
    "lineitem": LINEITEM_SCHEMA,
}


# ═══════════════════════════════════════════════════════════════════════════════
# RETRY HELPER
# ═══════════════════════════════════════════════════════════════════════════════

def with_retry(fn: Callable, description: str, cfg: PipelineConfig):
    """Execute fn() with up to cfg.max_retries attempts."""
    for attempt in range(1, cfg.max_retries + 1):
        try:
            return fn()
        except Exception as exc:
            log.warning(
                f"Attempt {attempt}/{cfg.max_retries} failed for '{description}': {exc}"
            )
            if attempt == cfg.max_retries:
                log.error(f"All {cfg.max_retries} attempts failed for '{description}'")
                raise
            time.sleep(cfg.retry_delay_seconds)


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 1 – BRONZE  (raw ingest into Iceberg)
# ═══════════════════════════════════════════════════════════════════════════════

def ingest_bronze(spark: SparkSession, cfg: PipelineConfig):
    """
    Read raw CSVs (pipe-delimited TPC-H format) from RAW_SOURCE_PATH and write
    them as-is into Iceberg bronze tables.

    Idempotency: Uses CREATE TABLE IF NOT EXISTS + INSERT OVERWRITE so
    re-running does not duplicate data.
    """
    cat = cfg.iceberg_catalog
    db = cfg.bronze_db

    # Ensure bronze namespace exists
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cat}.{db}")

    for table_name in cfg.source_tables:
        def _ingest(tbl=table_name):
            start = time.monotonic()
            source_path = f"{cfg.raw_source_path}{tbl}.csv"
            log.info(f"Bronze: reading {source_path}", extra={"phase": "bronze", "table": tbl})

            schema = TABLE_SCHEMAS[tbl]
            df = (
                spark.read
                .option("header", "true")
                .option("delimiter", "|")
                .option("mode", "PERMISSIVE")
                .schema(schema)
                .csv(source_path)
            )

            fqn = f"{cat}.{db}.{tbl}"

            # CREATE TABLE IF NOT EXISTS (idempotent)
            # We use df.writeTo which handles both creation and overwrite.
            log.info(f"Bronze: writing {fqn}", extra={"phase": "bronze", "table": tbl})

            df.writeTo(fqn).using("iceberg").createOrReplace()

            row_count = spark.table(fqn).count()
            elapsed = time.monotonic() - start
            metrics.record("bronze", tbl, row_count, elapsed)
            log.info(
                f"Bronze: {fqn} – {row_count} rows in {elapsed:.1f}s",
                extra={"phase": "bronze", "table": tbl, "rows": row_count},
            )

        with_retry(_ingest, f"bronze.{table_name}", cfg)


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 2 – SILVER  (clean, cast, partition)
# ═══════════════════════════════════════════════════════════════════════════════

def transform_silver(spark: SparkSession, cfg: PipelineConfig):
    """
    Read bronze, apply type casts, add audit columns, partition, and write to
    silver Iceberg tables.
    """
    cat = cfg.iceberg_catalog
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cat}.{cfg.silver_db}")

    # ── orders ──
    def _silver_orders():
        start = time.monotonic()
        tbl = "orders"
        bronze_fqn = f"{cat}.{cfg.bronze_db}.{tbl}"
        silver_fqn = f"{cat}.{cfg.silver_db}.{tbl}"

        df = (
            spark.table(bronze_fqn)
            .withColumn("O_TOTALPRICE", F.col("O_TOTALPRICE").cast(DecimalType(15, 2)))
            .withColumn("O_ORDERDATE",  F.to_date(F.col("O_ORDERDATE"), "yyyy-MM-dd"))
            .withColumn("order_year",   F.year(F.col("O_ORDERDATE")))
            .withColumn("order_month",  F.month(F.col("O_ORDERDATE")))
            # Audit columns
            .withColumn("_ingested_at", F.current_timestamp())
            # Drop rows with null keys (data quality gate)
            .filter(F.col("O_ORDERKEY").isNotNull())
        )

        # Partitioned by order_year for efficient pruning
        df.writeTo(silver_fqn).using("iceberg").partitionedBy("order_year").createOrReplace()

        row_count = spark.table(silver_fqn).count()
        elapsed = time.monotonic() - start
        metrics.record("silver", tbl, row_count, elapsed)
        log.info(
            f"Silver: {silver_fqn} – {row_count} rows",
            extra={"phase": "silver", "table": tbl, "rows": row_count},
        )

    # ── lineitem ──
    def _silver_lineitem():
        start = time.monotonic()
        tbl = "lineitem"
        bronze_fqn = f"{cat}.{cfg.bronze_db}.{tbl}"
        silver_fqn = f"{cat}.{cfg.silver_db}.{tbl}"

        df = (
            spark.table(bronze_fqn)
            .withColumn("L_QUANTITY",      F.col("L_QUANTITY").cast(DecimalType(15, 2)))
            .withColumn("L_EXTENDEDPRICE", F.col("L_EXTENDEDPRICE").cast(DecimalType(15, 2)))
            .withColumn("L_DISCOUNT",      F.col("L_DISCOUNT").cast(DecimalType(15, 2)))
            .withColumn("L_TAX",           F.col("L_TAX").cast(DecimalType(15, 2)))
            .withColumn("L_SHIPDATE",      F.to_date(F.col("L_SHIPDATE"), "yyyy-MM-dd"))
            .withColumn("L_COMMITDATE",    F.to_date(F.col("L_COMMITDATE"), "yyyy-MM-dd"))
            .withColumn("L_RECEIPTDATE",   F.to_date(F.col("L_RECEIPTDATE"), "yyyy-MM-dd"))
            .withColumn("ship_year",       F.year(F.col("L_SHIPDATE")))
            # Revenue metric for downstream gold
            .withColumn(
                "revenue",
                F.col("L_EXTENDEDPRICE") * (F.lit(1) - F.col("L_DISCOUNT")),
            )
            .withColumn("_ingested_at", F.current_timestamp())
            .filter(F.col("L_ORDERKEY").isNotNull())
        )

        df.writeTo(silver_fqn).using("iceberg").partitionedBy("ship_year").createOrReplace()

        row_count = spark.table(silver_fqn).count()
        elapsed = time.monotonic() - start
        metrics.record("silver", tbl, row_count, elapsed)
        log.info(
            f"Silver: {silver_fqn} – {row_count} rows",
            extra={"phase": "silver", "table": tbl, "rows": row_count},
        )

    with_retry(_silver_orders, "silver.orders", cfg)
    with_retry(_silver_lineitem, "silver.lineitem", cfg)


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 3 – GOLD  (business aggregates)
# ═══════════════════════════════════════════════════════════════════════════════

def build_gold(spark: SparkSession, cfg: PipelineConfig):
    """
    Build gold-layer business aggregate tables:
      1. revenue_by_order_date – daily revenue with order count
      2. top_customers         – top N customers by total spend
    """
    cat = cfg.iceberg_catalog
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cat}.{cfg.gold_db}")

    # ── Gold 1: Revenue by order date ──
    def _gold_revenue():
        start = time.monotonic()
        tbl = "revenue_by_order_date"
        gold_fqn = f"{cat}.{cfg.gold_db}.{tbl}"

        orders  = spark.table(f"{cat}.{cfg.silver_db}.orders")
        lineitem = spark.table(f"{cat}.{cfg.silver_db}.lineitem")

        df = (
            orders.alias("o")
            .join(
                lineitem.alias("li"),
                F.col("o.O_ORDERKEY") == F.col("li.L_ORDERKEY"),
                "inner",
            )
            .groupBy(
                F.col("o.O_ORDERDATE").alias("order_date"),
                F.col("o.order_year"),
                F.col("o.order_month"),
            )
            .agg(
                F.sum("li.revenue").alias("total_revenue"),
                F.count("li.L_ORDERKEY").alias("line_count"),
                F.countDistinct("o.O_ORDERKEY").alias("order_count"),
            )
            .orderBy("order_date")
        )

        df.writeTo(gold_fqn).using("iceberg").partitionedBy("order_year").createOrReplace()

        row_count = spark.table(gold_fqn).count()
        elapsed = time.monotonic() - start
        metrics.record("gold", tbl, row_count, elapsed)
        log.info(
            f"Gold: {gold_fqn} – {row_count} rows",
            extra={"phase": "gold", "table": tbl, "rows": row_count},
        )

    # ── Gold 2: Top customers ──
    def _gold_top_customers():
        start = time.monotonic()
        tbl = "top_customers"
        gold_fqn = f"{cat}.{cfg.gold_db}.{tbl}"

        orders  = spark.table(f"{cat}.{cfg.silver_db}.orders")
        lineitem = spark.table(f"{cat}.{cfg.silver_db}.lineitem")

        df = (
            orders.alias("o")
            .join(
                lineitem.alias("li"),
                F.col("o.O_ORDERKEY") == F.col("li.L_ORDERKEY"),
                "inner",
            )
            .groupBy(F.col("o.O_CUSTKEY").alias("customer_key"))
            .agg(
                F.sum("li.revenue").alias("total_revenue"),
                F.countDistinct("o.O_ORDERKEY").alias("order_count"),
                F.min("o.O_ORDERDATE").alias("first_order_date"),
                F.max("o.O_ORDERDATE").alias("last_order_date"),
            )
            .orderBy(F.desc("total_revenue"))
        )

        df.writeTo(gold_fqn).using("iceberg").createOrReplace()

        row_count = spark.table(gold_fqn).count()
        elapsed = time.monotonic() - start
        metrics.record("gold", tbl, row_count, elapsed)
        log.info(
            f"Gold: {gold_fqn} – {row_count} rows",
            extra={"phase": "gold", "table": tbl, "rows": row_count},
        )

    with_retry(_gold_revenue, "gold.revenue_by_order_date", cfg)
    with_retry(_gold_top_customers, "gold.top_customers", cfg)


# ═══════════════════════════════════════════════════════════════════════════════
# ENTRYPOINT
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    _inject_glue_args()
    log.info("Pipeline starting")
    cfg = load_config()
    log.info(
        f"Config: runtime={cfg.runtime}, warehouse={cfg.iceberg_warehouse}, "
        f"catalog={cfg.iceberg_catalog}, persona={cfg.persona}",
        extra={"runtime": cfg.runtime},
    )

    spark = create_spark_session(cfg)
    log.info("SparkSession created")

    try:
        with timed("PHASE 1 – BRONZE", log):
            ingest_bronze(spark, cfg)

        with timed("PHASE 2 – SILVER", log):
            transform_silver(spark, cfg)

        with timed("PHASE 3 – GOLD", log):
            build_gold(spark, cfg)

        # Print metrics summary
        metrics.log_summary(log)

        # Glue commit (no-op on other runtimes)
        if cfg.is_aws and hasattr(spark, "_glue_job"):
            spark._glue_job.commit()  # type: ignore[attr-defined]
            log.info("Glue job committed")

        log.info("Pipeline completed successfully")

    except Exception:
        log.error(f"Pipeline failed:\n{traceback.format_exc()}")
        sys.exit(1)
    finally:
        if cfg.is_local:
            # Only stop the session when running as a standalone script (local/MinIO).
            # On Fabric the SparkSession is shared with the notebook kernel — stopping it
            # kills the kernel and makes subsequent cells fail with "stopped SparkContext".
            # On AWS Glue, GlueContext manages its own lifecycle.
            spark.stop()


def _inject_glue_args() -> None:
    """
    On AWS Glue, job parameters (--KEY value) are passed via sys.argv, not as
    environment variables.  Inject them into os.environ so load_config() can
    read them with os.environ.get() regardless of runtime.
    """
    try:
        from awsglue.utils import getResolvedOptions  # noqa: F401 – confirms Glue runtime
    except ImportError:
        return  # Not running on Glue

    i = 1
    while i < len(sys.argv) - 1:
        arg = sys.argv[i]
        if arg.startswith("--"):
            key = arg[2:]
            val = sys.argv[i + 1]
            if not val.startswith("--") and key not in os.environ:
                os.environ[key] = val
            i += 2
        else:
            i += 1


if __name__ == "__main__":
    _inject_glue_args()
    main()
