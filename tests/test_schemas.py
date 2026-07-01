"""tests de validacion de schemas pydantic"""
from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from src.schemas import CanonicalEvent, CdcEnvelope, Operation


def test_envelope_requires_business_key_non_empty():
    """business_key no puede ser vacia o whitespace"""
    with pytest.raises(ValidationError):
        CdcEnvelope(
            event_id="a",
            event_timestamp=datetime(2026, 6, 1, tzinfo=timezone.utc),
            source_table="orders",
            business_key="   ",
            operation=Operation.INSERT,
        )


def test_envelope_accepts_valid_operation():
    """las 3 operaciones son validas"""
    for op in ["insert", "update", "delete"]:
        env = CdcEnvelope(
            event_id="a",
            event_timestamp=datetime(2026, 6, 1, tzinfo=timezone.utc),
            source_table="orders",
            business_key="order-1",
            operation=op,
        )
        assert env.operation.value == op


def test_envelope_rejects_unknown_operation():
    """operaciones desconocidas fallan la validacion"""
    with pytest.raises(ValidationError):
        CdcEnvelope(
            event_id="a",
            event_timestamp=datetime(2026, 6, 1, tzinfo=timezone.utc),
            source_table="orders",
            business_key="order-1",
            operation="upsert",
        )


def test_canonical_event_serialization_roundtrip():
    """to_bq_row produce dict con las claves esperadas"""
    ev = CanonicalEvent(
        event_id="a",
        event_timestamp=datetime(2026, 6, 1, 10, 0, tzinfo=timezone.utc),
        ingested_at=datetime(2026, 6, 1, 10, 1, tzinfo=timezone.utc),
        source_table="orders",
        business_key="order-1",
        operation=Operation.INSERT,
        payload_json=json.dumps({"x": 1}),
        schema_version=1,
        partition_date="2026-06-01",
    )
    row = ev.to_bq_row()
    assert set(row.keys()) == {
        "event_id",
        "event_timestamp",
        "ingested_at",
        "source_table",
        "business_key",
        "operation",
        "payload_json",
        "payload_before_json",
        "schema_version",
        "partition_date",
    }
    assert row["operation"] == "insert"
    assert row["partition_date"] == "2026-06-01"
