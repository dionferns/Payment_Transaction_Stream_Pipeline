"""
Synthetic payment transaction generator.

Produces a continuous stream of realistic payment events at configurable
throughput, with:
  - Hourly TPS variance mimicking real payment network traffic patterns.
  - Plausible merchant names drawn from a weighted category distribution.
  - Geographic spread matching approximate global card-present volumes.
  - A small fraction of intentionally "anomalous" transactions to exercise
    the fraud detection rules in the streaming layer.

Usage (CLI)::

    python -m src.generator.transaction_generator \\
        --tps 500 \\
        --duration 60 \\
        --output-mode file

Usage (import)::

    from src.generator.transaction_generator import TransactionGenerator
    gen = TransactionGenerator(config)
    for batch in gen.generate():
        write_batch(batch)
"""

from __future__ import annotations

import hashlib
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Generator, Iterator

import numpy as np
from faker import Faker

from src.generator.config import GeneratorConfig, load_config
from src.generator.schemas import (
    PaymentTransaction,
    TransactionBatch,
    TransactionType,
)
from src.utils.logging_config import configure_logging, get_logger

logger = get_logger(__name__)

fake = Faker()
Faker.seed(42)
random.seed(42)
np.random.seed(42)


# ---------------------------------------------------------------------------
# Static reference data
# ---------------------------------------------------------------------------

# ISO 18245 MCC codes — category label pairs (a realistic subset)
MCC_CODES: list[tuple[str, str]] = [
    ("5411", "Grocery Stores"),
    ("5812", "Eating Places, Restaurants"),
    ("5541", "Service Stations"),
    ("5912", "Drug Stores, Pharmacies"),
    ("5310", "Discount Stores"),
    ("5999", "Miscellaneous Retail"),
    ("4111", "Transportation"),
    ("7011", "Hotels & Lodging"),
    ("5944", "Jewelry Stores"),
    ("5734", "Computer Software Stores"),
    ("5045", "Computers, Peripherals & Software"),
    ("5651", "Family Clothing Stores"),
    ("5661", "Shoe Stores"),
    ("5200", "Home Supply Stores"),
    ("4814", "Telecommunication Services"),
    ("5815", "Digital Goods"),
    ("7841", "Video Tape Rental Stores"),
    ("5300", "Wholesale Clubs"),
    ("5511", "Car Dealers"),
    ("5621", "Women's Clothing Stores"),
]

MCC_WEIGHTS = [0.14, 0.12, 0.08, 0.07, 0.07, 0.06, 0.06, 0.05,
               0.04, 0.04, 0.04, 0.04, 0.03, 0.03, 0.03, 0.03,
               0.03, 0.03, 0.02, 0.05]

# Approximate mean purchase amounts per MCC (USD)
MCC_AMOUNT_PARAMS: dict[str, tuple[float, float]] = {
    "5411": (85.0, 40.0),
    "5812": (38.0, 22.0),
    "5541": (65.0, 20.0),
    "5912": (42.0, 18.0),
    "5310": (120.0, 60.0),
    "5999": (55.0, 30.0),
    "4111": (25.0, 15.0),
    "7011": (220.0, 120.0),
    "5944": (350.0, 200.0),
    "5734": (75.0, 35.0),
    "5045": (450.0, 250.0),
    "5651": (95.0, 45.0),
    "5661": (85.0, 40.0),
    "5200": (130.0, 60.0),
    "4814": (60.0, 25.0),
    "5815": (12.0, 8.0),
    "7841": (4.0, 1.5),
    "5300": (180.0, 80.0),
    "5511": (25000.0, 8000.0),
    "5621": (90.0, 40.0),
}

# A pool of synthetic merchant IDs (generated once)
_MERCHANT_POOL: list[str] = [f"MER{i:07d}" for i in range(1, 5001)]
_ACQUIRER_POOL: list[str] = [f"ACQ{i:04d}" for i in range(1, 51)]
_ISSUER_POOL: list[str] = [f"ISS{i:04d}" for i in range(1, 201)]

# Response codes and their approximate probability for normal transactions
RESPONSE_CODES = ["00", "51", "05", "54", "59", "03", "62", "61"]
RESPONSE_WEIGHTS = [0.94, 0.025, 0.015, 0.008, 0.004, 0.004, 0.003, 0.001]


class TransactionGenerator:
    """Generates synthetic payment transactions according to a ``GeneratorConfig``.

    Parameters
    ----------
    config:
        Generator runtime configuration.  If ``None``, loaded from the default
        ``pipeline_config.yaml``.
    """

    def __init__(self, config: GeneratorConfig | None = None) -> None:
        self.config = config or load_config()
        self._countries = list(self.config.country_weights.keys())
        self._country_probs = list(self.config.country_weights.values())
        # Normalise weights to sum to 1
        total = sum(self._country_probs)
        self._country_probs = [p / total for p in self._country_probs]

        self._txn_types = list(self.config.transaction_type_weights.keys())
        self._txn_type_probs = list(self.config.transaction_type_weights.values())
        txn_total = sum(self._txn_type_probs)
        self._txn_type_probs = [p / txn_total for p in self._txn_type_probs]

        # Small pool of "hot" cards to produce velocity anomalies
        self._hot_cards: list[str] = [self._random_card_hash() for _ in range(10)]

    # -----------------------------------------------------------------------
    # Public interface
    # -----------------------------------------------------------------------

    def generate(self) -> Iterator[TransactionBatch]:
        """Yield TransactionBatches for the configured duration.

        The generator adapts TPS to the hourly distribution so that generated
        data has realistic intra-day patterns.
        """
        start = time.monotonic()
        deadline = start + self.config.duration_seconds
        batch_buf: list[PaymentTransaction] = []
        batch_count = 0

        logger.info(
            "generator_start",
            tps=self.config.tps,
            duration_seconds=self.config.duration_seconds,
            output_mode=self.config.output_mode,
        )

        while time.monotonic() < deadline:
            now = datetime.now(tz=timezone.utc)
            effective_tps = self._effective_tps(now.hour)
            interval = 1.0 / max(effective_tps, 1)

            txn = self._make_transaction(now)
            batch_buf.append(txn)

            if len(batch_buf) >= self.config.batch_size:
                batch_count += 1
                yield TransactionBatch(
                    transactions=batch_buf,
                    batch_id=str(uuid.uuid4()),
                    generated_at=now,
                    target_date=now.strftime("%Y-%m-%d"),
                    target_hour=now.hour,
                )
                batch_buf = []

            time.sleep(interval)

        # Flush remaining
        if batch_buf:
            now = datetime.now(tz=timezone.utc)
            yield TransactionBatch(
                transactions=batch_buf,
                batch_id=str(uuid.uuid4()),
                generated_at=now,
                target_date=now.strftime("%Y-%m-%d"),
                target_hour=now.hour,
            )

        logger.info("generator_complete", batches_yielded=batch_count)

    def generate_sample(self, n: int = 1000) -> list[dict]:
        """Generate ``n`` transactions as dicts — useful for seeding tests."""
        now = datetime.now(tz=timezone.utc)
        return [self._make_transaction(now).to_dict() for _ in range(n)]

    # -----------------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------------

    def _effective_tps(self, hour: int) -> int:
        fraction = self.config.hourly_tps_distribution.get(hour, 1.0)
        return max(1, int(self.config.tps * fraction))

    def _make_transaction(self, ts: datetime) -> PaymentTransaction:
        mcc, _ = random.choices(MCC_CODES, weights=MCC_WEIGHTS, k=1)[0]
        txn_type = random.choices(self._txn_types, weights=self._txn_type_probs, k=1)[0]
        card_country = random.choices(self._countries, weights=self._country_probs, k=1)[0]

        is_cross_border = random.random() < self.config.cross_border_rate
        if is_cross_border:
            merchant_country = random.choices(
                [c for c in self._countries if c != card_country],
                k=1,
            )[0]
        else:
            merchant_country = card_country

        currency = self.config.currency_map.get(merchant_country, "USD")
        amount = self._sample_amount(mcc, txn_type)
        response_code = random.choices(RESPONSE_CODES, weights=RESPONSE_WEIGHTS, k=1)[0]

        # Occasionally use a "hot" card to trigger velocity fraud rules
        if random.random() < 0.005:
            card_hash = random.choice(self._hot_cards)
        else:
            card_hash = self._random_card_hash()

        return PaymentTransaction(
            transaction_id=str(uuid.uuid4()),
            card_hash=card_hash,
            merchant_id=random.choice(_MERCHANT_POOL),
            merchant_category_code=mcc,
            amount=amount,
            currency=currency,
            timestamp=ts,
            transaction_type=TransactionType(txn_type),
            country_code=merchant_country,
            acquirer_id=random.choice(_ACQUIRER_POOL),
            issuer_id=random.choice(_ISSUER_POOL),
            response_code=response_code,
            is_cross_border=is_cross_border,
            merchant_name=fake.company(),
            terminal_id=f"TERM{random.randint(1000, 9999)}",
            auth_code=fake.lexify("??????").upper() if response_code == "00" else None,
        )

    @staticmethod
    def _random_card_hash() -> str:
        # Simulate a SHA-256 hash of a PAN (16-digit card number)
        pan = "".join([str(random.randint(0, 9)) for _ in range(16)])
        return hashlib.sha256(pan.encode()).hexdigest()

    @staticmethod
    def _sample_amount(mcc: str, txn_type: str) -> Decimal:
        mean, std = MCC_AMOUNT_PARAMS.get(mcc, (50.0, 25.0))
        raw = max(0.01, np.random.normal(mean, std))
        # Round to 2 decimal places
        amount = Decimal(str(round(raw, 2)))
        if txn_type in ("refund", "chargeback"):
            # Partial refund — typically 60-100% of original
            amount = amount * Decimal(str(round(random.uniform(0.6, 1.0), 2)))
            amount = -abs(amount)
        return amount


# ---------------------------------------------------------------------------
# File / Kafka output
# ---------------------------------------------------------------------------

def write_batch_to_file(batch: TransactionBatch, landing_path: str) -> None:
    """Append a batch as newline-delimited JSON to the hourly landing partition."""
    out_dir = Path(landing_path) / "transactions" / f"date={batch.target_date}" / f"hour={batch.target_hour:02d}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{batch.batch_id}.json"
    with open(out_file, "w") as fh:
        for txn in batch.transactions:
            fh.write(json.dumps(txn.to_dict()) + "\n")
    logger.debug(
        "batch_written",
        path=str(out_file),
        records=len(batch.transactions),
    )


def write_batch_to_kafka(batch: TransactionBatch, bootstrap_servers: str, topic: str) -> None:
    """Produce a batch to a Kafka topic."""
    try:
        from confluent_kafka import Producer  # type: ignore[import]
    except ImportError as exc:
        raise RuntimeError(
            "confluent-kafka is required for Kafka output. "
            "Install it with: pip install confluent-kafka"
        ) from exc

    producer = Producer({"bootstrap.servers": bootstrap_servers})
    for txn in batch.transactions:
        producer.produce(
            topic,
            key=txn.card_hash.encode(),
            value=json.dumps(txn.to_dict()).encode(),
        )
    producer.flush()
    logger.debug(
        "batch_produced_to_kafka",
        topic=topic,
        records=len(batch.transactions),
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """CLI entry point — run the generator until duration is exhausted."""
    import argparse

    configure_logging()

    parser = argparse.ArgumentParser(description="Payment transaction generator")
    parser.add_argument("--tps", type=int, default=None)
    parser.add_argument("--duration", type=int, default=None)
    parser.add_argument("--output-mode", choices=["file", "kafka"], default=None)
    parser.add_argument("--config", default=None, help="Path to pipeline_config.yaml")
    args = parser.parse_args()

    cfg = load_config(args.config)
    if args.tps:
        cfg.tps = args.tps
    if args.duration:
        cfg.duration_seconds = args.duration
    if args.output_mode:
        cfg.output_mode = args.output_mode

    gen = TransactionGenerator(cfg)

    for batch in gen.generate():
        if cfg.output_mode == "kafka":
            write_batch_to_kafka(batch, cfg.kafka_bootstrap_servers, cfg.kafka_topic)
        else:
            write_batch_to_file(batch, cfg.landing_zone_path)


if __name__ == "__main__":
    main()
