"""
utils/logging.py – Structured logging & lightweight metrics for the pipeline.

Design decisions
────────────────
1.  JSON-structured logs so CloudWatch / Azure Monitor / ELK can parse them.
2.  A tiny MetricsCollector records row counts and durations; on AWS Glue this
    would feed CloudWatch custom metrics; on Fabric it writes to notebook output;
    locally it prints a summary.
3.  No external dependencies beyond the stdlib – keep the deployment surface tiny.
"""

from __future__ import annotations

import json
import logging
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Optional


# ── JSON formatter ───────────────────────────────────────────────────────────

class JsonFormatter(logging.Formatter):
    """Emit each log record as a single JSON line."""

    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info and record.exc_info[0] is not None:
            payload["exception"] = self.formatException(record.exc_info)
        # Merge any extra fields attached via `extra={"phase": "bronze", ...}`
        for key in ("phase", "table", "rows", "duration_s", "runtime"):
            val = getattr(record, key, None)
            if val is not None:
                payload[key] = val
        return json.dumps(payload, default=str)


def get_logger(name: str = "iceberg_pipeline") -> logging.Logger:
    """Return a logger that writes JSON to stdout (captured by Glue/Fabric)."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


# ── Lightweight metrics collector ────────────────────────────────────────────

class MetricsCollector:
    """Accumulates row-count and timing metrics across pipeline phases."""

    def __init__(self):
        self._metrics: Dict[str, Dict[str, Any]] = {}

    def record(self, phase: str, table: str, row_count: int, duration_s: float):
        key = f"{phase}.{table}"
        self._metrics[key] = {
            "phase": phase,
            "table": table,
            "rows": row_count,
            "duration_s": round(duration_s, 3),
        }

    def summary(self) -> Dict[str, Dict[str, Any]]:
        return dict(self._metrics)

    def log_summary(self, logger: logging.Logger):
        logger.info("Pipeline metrics summary", extra={"phase": "summary"})
        for key, m in self._metrics.items():
            logger.info(
                f"  {key}: {m['rows']} rows in {m['duration_s']}s",
                extra=m,
            )


@contextmanager
def timed(label: str, logger: logging.Logger):
    """Context manager that logs elapsed wall-clock time."""
    start = time.monotonic()
    logger.info(f"START  {label}")
    try:
        yield
    finally:
        elapsed = round(time.monotonic() - start, 3)
        logger.info(f"FINISH {label} ({elapsed}s)", extra={"duration_s": elapsed})
