"""tests unitarios de las transforms usando apache_beam.testing

corren en DirectRunner en memoria, sin dependencias externas
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from src.pipeline.transforms import (
    DecodeEnvelope,
    KeyByBusinessKey,
    ToCanonicalEvent,
    dedup_pipeline,
)
from src.schemas import CdcEnvelope, Operation


def _envelope_bytes(
    business_key: str,
    event_id: str,
    ts: datetime,
    operation: str = "insert",
    payload: dict | None = None,
) -> bytes:
    """helper para armar bytes de un envelope"""
    obj = {
        "event_id": event_id,
        "event_timestamp": ts.isoformat(),
        "ingested_at": ts.isoformat(),
        "source_table": "orders",
        "business_key": business_key,
        "operation": operation,
        "payload": payload or {"id": business_key, "amount": 100},
        "schema_version": 1,
    }
    return json.dumps(obj).encode("utf-8")


def test_decode_ok_yields_envelope():
    """un mensaje valido decodifica a CdcEnvelope"""
    msg = _envelope_bytes(
        "order-1", "evt-1", datetime(2026, 6, 1, 10, 0, tzinfo=timezone.utc)
    )
    with TestPipeline() as p:
        outputs = p | beam.Create([msg]) | beam.ParDo(DecodeEnvelope()).with_outputs(
            "errors", main="ok"
        )
        assert_that(
            outputs.ok | beam.Map(lambda e: e.business_key),
            equal_to(["order-1"]),
            label="ok",
        )


def test_decode_bad_json_goes_to_errors():
    """mensaje mal formado va al side output errors, no tira excepcion"""
    with TestPipeline() as p:
        outputs = (
            p
            | beam.Create([b"not-a-json"])
            | beam.ParDo(DecodeEnvelope()).with_outputs("errors", main="ok")
        )
        assert_that(outputs.ok, equal_to([]), label="ok_empty")
        assert_that(
            outputs.errors | beam.Map(lambda d: "raw" in d),
            equal_to([True]),
            label="err_has_raw",
        )


def test_dedup_keeps_latest_by_event_timestamp():
    """dado el mismo business_key, gana el event_timestamp mas reciente"""
    envs = [
        CdcEnvelope(
            event_id="a",
            event_timestamp=datetime(2026, 6, 1, 10, 0, tzinfo=timezone.utc),
            source_table="orders",
            business_key="order-1",
            operation=Operation.INSERT,
            payload={"v": 1},
        ),
        CdcEnvelope(
            event_id="b",
            event_timestamp=datetime(2026, 6, 1, 10, 5, tzinfo=timezone.utc),
            source_table="orders",
            business_key="order-1",
            operation=Operation.UPDATE,
            payload={"v": 2},
        ),
    ]
    with TestPipeline() as p:
        result = (
            p
            | beam.Create(envs)
            | beam.ParDo(KeyByBusinessKey())
            | beam.GroupByKey()
            | beam.ParDo(_UnwrapGroupThenDedup())
        )
        assert_that(
            result | beam.Map(lambda e: e.event_id), equal_to(["b"]),
        )


class _UnwrapGroupThenDedup(beam.DoFn):
    """helper para el test: aplica dedup sin windowing"""

    def process(self, element):
        from src.pipeline.transforms import DedupByEventTimestamp

        # invocacion manual del DoFn dentro de un DoFn para no depender del window
        dedup = DedupByEventTimestamp()
        yield from dedup.process(element)


def test_to_canonical_derives_partition_date():
    """la particion se deriva de event_timestamp, no de ingested_at"""
    env = CdcEnvelope(
        event_id="a",
        event_timestamp=datetime(2026, 5, 20, 23, 59, tzinfo=timezone.utc),
        source_table="orders",
        business_key="order-1",
        operation=Operation.INSERT,
        payload={"x": 1},
    )
    with TestPipeline() as p:
        result = p | beam.Create([env]) | beam.ParDo(ToCanonicalEvent())
        assert_that(result | beam.Map(lambda e: e.partition_date), equal_to(["2026-05-20"]))
