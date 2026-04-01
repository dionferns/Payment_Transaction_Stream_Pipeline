"""
Generator configuration — loaded once at startup from the pipeline YAML
and environment variable overrides.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional

import yaml


@dataclass
class GeneratorConfig:
    """Runtime parameters for the transaction generator."""

    tps: int = 1000
    duration_seconds: int = 3600
    batch_size: int = 500
    output_mode: str = "file"   # "file" | "kafka"
    timezone: str = "UTC"

    # Kafka settings (only used when output_mode == "kafka")
    kafka_bootstrap_servers: str = ""
    kafka_topic: str = "payment.transactions.raw"

    # File output path (only used when output_mode == "file")
    landing_zone_path: str = "./data/landing"

    # Throughput distribution: hour (int) -> fraction of peak TPS
    hourly_tps_distribution: dict[int, float] = field(default_factory=dict)

    # Transaction type -> probability weight
    transaction_type_weights: dict[str, float] = field(
        default_factory=lambda: {
            "purchase": 0.82,
            "refund": 0.10,
            "chargeback": 0.03,
            "p2p": 0.05,
        }
    )

    # Country -> probability weight
    country_weights: dict[str, float] = field(default_factory=dict)

    # Country -> ISO 4217 currency code
    currency_map: dict[str, str] = field(default_factory=dict)

    cross_border_rate: float = 0.18


def load_config(config_path: Optional[str] = None) -> GeneratorConfig:
    """Load GeneratorConfig from pipeline_config.yaml, then apply env overrides.

    Parameters
    ----------
    config_path:
        Explicit path to ``pipeline_config.yaml``.  Defaults to
        ``config/pipeline_config.yaml`` relative to the working directory.
    """
    path = config_path or os.path.join(
        os.path.dirname(__file__), "..", "..", "config", "pipeline_config.yaml"
    )
    path = os.path.abspath(path)

    with open(path) as fh:
        raw = yaml.safe_load(fh)

    gen = raw.get("generator", {})

    cfg = GeneratorConfig(
        tps=int(os.getenv("GENERATOR_TPS", gen.get("tps", 1000))),
        duration_seconds=int(
            os.getenv("GENERATOR_DURATION_SECONDS", gen.get("duration_seconds", 3600))
        ),
        batch_size=int(
            os.getenv("GENERATOR_BATCH_SIZE", gen.get("batch_size", 500))
        ),
        output_mode=os.getenv("GENERATOR_OUTPUT_MODE", gen.get("output_mode", "file")),
        timezone=gen.get("timezone", "UTC"),
        kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", ""),
        kafka_topic=os.getenv(
            "KAFKA_TOPIC_TRANSACTIONS", "payment.transactions.raw"
        ),
        landing_zone_path=os.getenv("LANDING_ZONE_PATH", "./data/landing"),
        hourly_tps_distribution={
            int(k): v
            for k, v in gen.get("hourly_tps_distribution", {}).items()
        },
        transaction_type_weights=gen.get(
            "transaction_type_weights",
            {"purchase": 0.82, "refund": 0.10, "chargeback": 0.03, "p2p": 0.05},
        ),
        country_weights=gen.get("country_weights", {}),
        currency_map=gen.get("currency_map", {}),
        cross_border_rate=float(gen.get("cross_border_rate", 0.18)),
    )
    return cfg
