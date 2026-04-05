"""
Structured JSON logging configuration for the payment stream pipeline.

All pipeline components obtain a logger via ``get_logger``.  In production
the JSON output is ingested by a log aggregator (e.g. Datadog, Splunk).
In development ``LOG_FORMAT=text`` produces human-readable output.
"""

from __future__ import annotations

import logging
import os
import sys

import structlog

# Group 4 — "How does real-time fraud detection work?"
# Purpose: The most interesting part of the project.

# src/streaming/fraud_rules.py — Three fraud detection rules:
# Velocity: Card used more than 5 times in 60 seconds? Flag it.
# Amount Anomaly: Transaction 3x higher than that card's average? Flag it.
# Geo-Impossibility: Same card used in two different countries within 30 minutes? Flag it.
# src/streaming/aggregations.py — Real-time summaries: how much is each merchant processing right now? What % of a bank's transactions are crossing borders?
# src/streaming/stream_processor.py — The conductor. Starts Spark, reads incoming data, runs the fraud rules and aggregations simultaneously, writes results to files.

def configure_logging() -> None:
    """Configure structlog with processors appropriate for the current env.

    Call once at the entry-point of each job before obtaining any loggers.
    """
    log_level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_name, logging.INFO)
    log_format = os.getenv("LOG_FORMAT", "json").lower()

    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
    ]

    if log_format == "json":
        renderer: structlog.types.Processor = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty())

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        processor=renderer,
        foreign_pre_chain=shared_processors,
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers = [handler]
    root_logger.setLevel(log_level)


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Return a bound structlog logger for ``name``."""
    return structlog.get_logger(name)
