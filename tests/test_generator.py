"""
Unit tests for the transaction generator.
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.generator.config import GeneratorConfig
from src.generator.schemas import PaymentTransaction, TransactionType
from src.generator.transaction_generator import (
    TransactionGenerator,
    write_batch_to_file,
)


def _minimal_config() -> GeneratorConfig:
    return GeneratorConfig(
        tps=10,
        duration_seconds=1,
        batch_size=5,
        output_mode="file",
        hourly_tps_distribution={h: 1.0 for h in range(24)},
        transaction_type_weights={"purchase": 0.82, "refund": 0.10, "chargeback": 0.03, "p2p": 0.05},
        country_weights={"US": 0.5, "GB": 0.5},
        currency_map={"US": "USD", "GB": "GBP"},
        cross_border_rate=0.1,
    )


class TestTransactionSchema:
    def test_generated_transaction_is_valid(self):
        gen = TransactionGenerator(_minimal_config())
        now = datetime.now(tz=timezone.utc)
        txn = gen._make_transaction(now)
        assert isinstance(txn, PaymentTransaction)
        assert txn.transaction_id
        assert txn.card_hash
        assert len(txn.merchant_category_code) == 4

    def test_purchase_has_positive_amount(self):
        gen = TransactionGenerator(_minimal_config())
        now = datetime.now(tz=timezone.utc)
        for _ in range(50):
            txn = gen._make_transaction(now)
            if txn.transaction_type == TransactionType.PURCHASE:
                assert txn.amount > 0

    def test_refund_has_negative_amount(self):
        gen = TransactionGenerator(_minimal_config())
        now = datetime.now(tz=timezone.utc)
        refunds = []
        for _ in range(200):
            txn = gen._make_transaction(now)
            if txn.transaction_type == TransactionType.REFUND:
                refunds.append(txn)
        # We generated 200 transactions so should have at least a few refunds
        for r in refunds:
            assert r.amount < 0

    def test_currency_matches_country(self):
        gen = TransactionGenerator(_minimal_config())
        now = datetime.now(tz=timezone.utc)
        for _ in range(50):
            txn = gen._make_transaction(now)
            if not txn.is_cross_border:
                expected = _minimal_config().currency_map.get(txn.country_code)
                if expected:
                    assert txn.currency == expected


class TestGenerateSample:
    def test_returns_correct_count(self):
        gen = TransactionGenerator(_minimal_config())
        sample = gen.generate_sample(100)
        assert len(sample) == 100

    def test_records_are_dicts_with_required_fields(self):
        gen = TransactionGenerator(_minimal_config())
        sample = gen.generate_sample(10)
        required = {"transaction_id", "card_hash", "merchant_id", "amount", "timestamp"}
        for record in sample:
            assert required.issubset(set(record.keys()))

    def test_amounts_serialised_as_strings(self):
        """Amounts must be strings in the JSON output to preserve decimal precision."""
        gen = TransactionGenerator(_minimal_config())
        sample = gen.generate_sample(10)
        for record in sample:
            assert isinstance(record["amount"], str)


class TestWriteBatchToFile:
    def test_creates_partitioned_output(self, tmp_path):
        from datetime import timezone
        from src.generator.schemas import TransactionBatch

        gen = TransactionGenerator(_minimal_config())
        now = datetime.now(tz=timezone.utc)
        txns = [gen._make_transaction(now) for _ in range(5)]
        batch = TransactionBatch(
            transactions=txns,
            batch_id="test-batch-001",
            generated_at=now,
            target_date="2024-01-15",
            target_hour=10,
        )
        write_batch_to_file(batch, str(tmp_path))
        output_file = tmp_path / "transactions" / "date=2024-01-15" / "hour=10" / "test-batch-001.json"
        assert output_file.exists()
        lines = output_file.read_text().strip().split("\n")
        assert len(lines) == 5
        # Each line must be valid JSON
        for line in lines:
            record = json.loads(line)
            assert "transaction_id" in record
