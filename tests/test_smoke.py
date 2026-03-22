"""
tests/test_smoke.py – Unit & integration smoke tests for the Iceberg pipeline.

Run locally:
  # From the project root (PowerShell):
  $env:RUNTIME="local"
  $env:ICEBERG_WAREHOUSE="s3a://iceberg-warehouse/iceberg/"
  $env:ICEBERG_CATALOG="iceberg"
  $env:RAW_SOURCE_PATH="s3a://raw-data/tpch/"
  $env:S3_ENDPOINT="http://localhost:9000"
  python -m pytest tests/test_smoke.py -v

  # Or using pytest from Bash / Git Bash:
  RUNTIME=local ICEBERG_WAREHOUSE=s3a://iceberg-warehouse/iceberg/ \\
  ICEBERG_CATALOG=iceberg RAW_SOURCE_PATH=s3a://raw-data/tpch/ \\
  S3_ENDPOINT=http://localhost:9000 \\
  python -m pytest tests/test_smoke.py -v

These tests require:
  • MinIO running on localhost:9000
  • Sample CSVs uploaded to s3a://raw-data/tpch/ (see scripts/minio_setup.sh)
  • PySpark + Iceberg runtime available
"""

from __future__ import annotations

import os
import sys

import pytest

# Ensure project root is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config import load_config
from spark_factory import create_spark_session


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def cfg():
    """Load pipeline config from environment."""
    return load_config()


@pytest.fixture(scope="session")
def spark(cfg):
    """Create a SparkSession that lives for the entire test session."""
    session = create_spark_session(cfg)
    yield session
    session.stop()


@pytest.fixture(scope="session", autouse=True)
def run_pipeline(spark, cfg):
    """Run the full pipeline once before all tests."""
    from spark_job import ingest_bronze, transform_silver, build_gold
    ingest_bronze(spark, cfg)
    transform_silver(spark, cfg)
    build_gold(spark, cfg)


# ── Bronze Tests ─────────────────────────────────────────────────────────────

class TestBronze:
    def test_orders_exists(self, spark, cfg):
        cat = cfg.iceberg_catalog
        df = spark.table(f"{cat}.bronze.orders")
        assert df.count() > 0, "Bronze orders table should have rows"

    def test_lineitem_exists(self, spark, cfg):
        cat = cfg.iceberg_catalog
        df = spark.table(f"{cat}.bronze.lineitem")
        assert df.count() > 0, "Bronze lineitem table should have rows"

    def test_orders_columns(self, spark, cfg):
        cat = cfg.iceberg_catalog
        df = spark.table(f"{cat}.bronze.orders")
        expected = {"O_ORDERKEY", "O_CUSTKEY", "O_ORDERSTATUS", "O_TOTALPRICE",
                    "O_ORDERDATE", "O_ORDERPRIORITY", "O_CLERK", "O_SHIPPRIORITY", "O_COMMENT"}
        assert expected.issubset(set(df.columns))


# ── Silver Tests ─────────────────────────────────────────────────────────────

class TestSilver:
    def test_orders_typed(self, spark, cfg):
        """O_TOTALPRICE should be decimal after silver transformation."""
        cat = cfg.iceberg_catalog
        df = spark.table(f"{cat}.silver.orders")
        dtype = dict(df.dtypes)["O_TOTALPRICE"]
        assert "decimal" in dtype, f"Expected decimal, got {dtype}"

    def test_orders_partitioned(self, spark, cfg):
        """Silver orders should have order_year column (used for partitioning)."""
        cat = cfg.iceberg_catalog
        df = spark.table(f"{cat}.silver.orders")
        assert "order_year" in df.columns

    def test_lineitem_revenue(self, spark, cfg):
        """Silver lineitem should have derived revenue column."""
        cat = cfg.iceberg_catalog
        df = spark.table(f"{cat}.silver.lineitem")
        assert "revenue" in df.columns

    def test_silver_no_null_keys(self, spark, cfg):
        """Silver should have no null order keys."""
        cat = cfg.iceberg_catalog
        orders = spark.table(f"{cat}.silver.orders")
        null_count = orders.filter("O_ORDERKEY IS NULL").count()
        assert null_count == 0, f"Found {null_count} null order keys in silver"


# ── Gold Tests ───────────────────────────────────────────────────────────────

class TestGold:
    def test_revenue_by_date_exists(self, spark, cfg):
        cat = cfg.iceberg_catalog
        df = spark.table(f"{cat}.gold.revenue_by_order_date")
        assert df.count() > 0

    def test_top_customers_exists(self, spark, cfg):
        cat = cfg.iceberg_catalog
        df = spark.table(f"{cat}.gold.top_customers")
        assert df.count() > 0

    def test_revenue_positive(self, spark, cfg):
        """Total revenue should be positive."""
        cat = cfg.iceberg_catalog
        df = spark.table(f"{cat}.gold.revenue_by_order_date")
        from pyspark.sql import functions as F
        total = df.agg(F.sum("total_revenue")).collect()[0][0]
        assert total is not None and float(total) > 0, f"Total revenue should be > 0, got {total}"

    def test_top_customer_ordering(self, spark, cfg):
        """Top customers should be ordered by revenue descending."""
        cat = cfg.iceberg_catalog
        df = spark.table(f"{cat}.gold.top_customers").limit(10).collect()
        revenues = [float(row["total_revenue"]) for row in df]
        assert revenues == sorted(revenues, reverse=True), "Top customers not sorted desc"


# ── SQL Query Validation (mirrors Athena / Fabric Warehouse queries) ─────────

class TestSQLQueries:
    def test_athena_style_gold_query(self, spark, cfg):
        """Simulate the kind of query an analyst would run in Athena."""
        cat = cfg.iceberg_catalog
        result = spark.sql(f"""
            SELECT order_year, 
                   SUM(total_revenue) AS yearly_revenue,
                   SUM(order_count)   AS yearly_orders
            FROM {cat}.gold.revenue_by_order_date
            GROUP BY order_year
            ORDER BY order_year
        """).collect()
        assert len(result) > 0, "Yearly revenue query returned no rows"
        # Every year's revenue should be positive
        for row in result:
            assert float(row["yearly_revenue"]) > 0

    def test_top_10_customers(self, spark, cfg):
        cat = cfg.iceberg_catalog
        result = spark.sql(f"""
            SELECT customer_key, total_revenue, order_count
            FROM {cat}.gold.top_customers
            LIMIT 10
        """).collect()
        assert len(result) > 0
        assert len(result) <= 10
