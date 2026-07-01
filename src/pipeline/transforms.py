"""DoFns reutilizables por streaming y batch

cada transform es idempotente y testeable por separado
las metricas de beam se emiten con Metrics para trazabilidad en dataflow
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Iterable

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.transforms.window import FixedWindows

from src.schemas import CanonicalEvent, CdcEnvelope, Operation


# metricas beam emitidas al dataflow monitoring
_M_DECODE_OK = Metrics.counter("pipeline", "envelopes_decoded_ok")
_M_DECODE_FAIL = Metrics.counter("pipeline", "envelopes_decoded_fail")
_M_DEDUP_KEPT = Metrics.counter("pipeline", "events_dedup_kept")
_M_DEDUP_DROPPED = Metrics.counter("pipeline", "events_dedup_dropped")
_M_INVALID_TS = Metrics.counter("pipeline", "events_invalid_timestamp")


class DecodeEnvelope(beam.DoFn):
    """decodifica bytes desde pub/sub a CdcEnvelope

    en falla emite al side output 'errors' con el bytes raw y el motivo
    en vez de tirar la excepcion, para que el pipeline no muera
    """

    def process(self, message: bytes) -> Iterable[CdcEnvelope]:
        try:
            payload = json.loads(message.decode("utf-8"))
            env = CdcEnvelope.model_validate(payload)
            _M_DECODE_OK.inc()
            yield env
        except Exception as e:
            _M_DECODE_FAIL.inc()
            yield beam.pvalue.TaggedOutput(
                "errors",
                {
                    "raw": message.decode("utf-8", errors="replace"),
                    "error": str(e),
                    "ts": datetime.now(timezone.utc).isoformat(),
                },
            )


class KeyByBusinessKey(beam.DoFn):
    """emite (business_key, envelope) para agrupar por natural key"""

    def process(self, env: CdcEnvelope) -> Iterable[tuple[str, CdcEnvelope]]:
        yield (env.business_key, env)


class DedupByEventTimestamp(beam.DoFn):
    """dentro de una ventana, se queda con el envelope mas reciente por business_key

    resuelve last-write-wins sin importar el orden de llegada
    conflictos: si dos eventos comparten event_timestamp exacto, gana el event_id
    lexicograficamente mayor para determinismo
    """

    def process(
        self, element: tuple[str, Iterable[CdcEnvelope]]
    ) -> Iterable[CdcEnvelope]:
        _, envelopes = element
        winner: CdcEnvelope | None = None
        dropped = 0
        for env in envelopes:
            if winner is None:
                winner = env
                continue
            if env.event_timestamp > winner.event_timestamp or (
                env.event_timestamp == winner.event_timestamp
                and env.event_id > winner.event_id
            ):
                dropped += 1
                winner = env
            else:
                dropped += 1
        if winner is not None:
            _M_DEDUP_KEPT.inc()
            _M_DEDUP_DROPPED.inc(dropped)
            yield winner


class ToCanonicalEvent(beam.DoFn):
    """convierte CdcEnvelope validado a CanonicalEvent listo para bigquery

    materializa la fecha de particion y serializa el payload como json string
    """

    def process(self, env: CdcEnvelope) -> Iterable[CanonicalEvent]:
        # ingested_at por defecto: ahora
        ingested = env.ingested_at or datetime.now(timezone.utc)
        # particion por fecha del evento, no de carga
        try:
            partition = env.event_timestamp.date().isoformat()
        except Exception:
            _M_INVALID_TS.inc()
            return
        yield CanonicalEvent(
            event_id=env.event_id,
            event_timestamp=env.event_timestamp,
            ingested_at=ingested,
            source_table=env.source_table,
            business_key=env.business_key,
            operation=Operation(env.operation),
            payload_json=json.dumps(env.payload, ensure_ascii=False, sort_keys=True),
            payload_before_json=(
                json.dumps(env.payload_before, ensure_ascii=False, sort_keys=True)
                if env.payload_before is not None
                else None
            ),
            schema_version=env.schema_version,
            partition_date=partition,
        )


class ToBQRow(beam.DoFn):
    """convierte CanonicalEvent a dict compatible con WriteToBigQuery"""

    def process(self, ev: CanonicalEvent) -> Iterable[dict]:
        yield ev.to_bq_row()


def dedup_pipeline(pcoll, window_seconds: int):
    """helper que arma la sub-pipeline de dedup por business key

    aplica una ventana fija, agrupa por key, y elige el envelope mas reciente
    """
    return (
        pcoll
        | "KeyByBusinessKey" >> beam.ParDo(KeyByBusinessKey())
        | "Window" >> beam.WindowInto(FixedWindows(window_seconds))
        | "GroupByKey" >> beam.GroupByKey()
        | "DedupByEventTimestamp" >> beam.ParDo(DedupByEventTimestamp())
    )
