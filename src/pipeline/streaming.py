"""pipeline streaming Pub/Sub → BigQuery

lee cdc events desde una subscription, decodifica y valida el envelope,
deduplica por business key en una ventana temporal, materializa a
CanonicalEvent y escribe a bigquery con streaming inserts

falla ruidosa: los eventos invalidos se sinkean a un dead-letter queue
en vez de tirar la excepcion, el pipeline sigue vivo
"""
from __future__ import annotations

import argparse
import logging

import apache_beam as beam
from apache_beam.io.gcp.bigquery import (
    BigQueryDisposition,
    WriteToBigQuery,
)
from apache_beam.io.gcp.pubsub import ReadFromPubSub, WriteToPubSub
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

from src.config import get_settings
from src.logging_setup import configure_logging
from src.pipeline.transforms import (
    DecodeEnvelope,
    ToBQRow,
    ToCanonicalEvent,
    dedup_pipeline,
)


# schema bigquery para la tabla raw destino
# se declara aca por explicitud, en produccion es equivalente al tf del infra
_BQ_SCHEMA = {
    "fields": [
        {"name": "event_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "event_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "ingested_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "source_table", "type": "STRING", "mode": "REQUIRED"},
        {"name": "business_key", "type": "STRING", "mode": "REQUIRED"},
        {"name": "operation", "type": "STRING", "mode": "REQUIRED"},
        {"name": "payload_json", "type": "STRING", "mode": "REQUIRED"},
        {"name": "payload_before_json", "type": "STRING", "mode": "NULLABLE"},
        {"name": "schema_version", "type": "INT64", "mode": "REQUIRED"},
        {"name": "partition_date", "type": "DATE", "mode": "REQUIRED"},
    ]
}


def build_pipeline(pipeline: beam.Pipeline, settings) -> None:
    """arma el grafo del pipeline sobre el objeto Pipeline provisto"""

    # lectura desde pub/sub con soporte para ack en success
    events = (
        pipeline
        | "ReadFromPubSub"
        >> ReadFromPubSub(subscription=settings.pubsub_subscription_full).with_output_types(bytes)
    )

    # decodificacion con side output de errores
    decoded = events | "DecodeEnvelope" >> beam.ParDo(DecodeEnvelope()).with_outputs(
        "errors", main="ok"
    )

    # branch de errores a un topic de dead-letter
    # en produccion, alertas + retry manual desde ahi
    _ = (
        decoded.errors
        | "SerializeError" >> beam.Map(lambda d: str(d).encode("utf-8"))
        | "WriteDLQ"
        >> WriteToPubSub(
            topic=f"projects/{settings.gcp_project}/topics/cdc-events-dlq"
        )
    )

    # dedup en ventana temporal, materializar y escribir
    _ = (
        decoded.ok
        | "Dedup" >> beam.ptransform_fn(dedup_pipeline)(settings.dedup_window_seconds)
        | "ToCanonical" >> beam.ParDo(ToCanonicalEvent())
        | "ToBQRow" >> beam.ParDo(ToBQRow())
        | "WriteToBigQuery"
        >> WriteToBigQuery(
            table=settings.bq_raw_table_ref,
            schema=_BQ_SCHEMA,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            method=WriteToBigQuery.Method.STREAMING_INSERTS,
            additional_bq_parameters={
                "timePartitioning": {
                    "type": "DAY",
                    "field": "event_timestamp",
                },
                "clustering": {
                    "fields": ["source_table", "business_key"],
                },
            },
        )
    )


def run(argv: list[str] | None = None) -> None:
    """entry point invocable desde CLI y desde tests"""
    configure_logging()
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(description="cdc streaming pipeline")
    parser.add_argument("--runner", default=None, help="beam runner override")
    parser.add_argument(
        "--streaming",
        action="store_true",
        help="fuerza modo streaming",
    )
    known, pipeline_args = parser.parse_known_args(argv)

    settings = get_settings()
    if known.runner:
        settings.runner = known.runner  # type: ignore[assignment]

    options = PipelineOptions(pipeline_args)
    options.view_as(StandardOptions).runner = settings.runner
    options.view_as(StandardOptions).streaming = known.streaming or settings.streaming

    logger.info(
        "pipeline_start",
        extra={
            "runner": settings.runner,
            "streaming": options.view_as(StandardOptions).streaming,
            "subscription": settings.pubsub_subscription_full,
            "table": settings.bq_raw_table_ref,
        },
    )

    with beam.Pipeline(options=options) as p:
        build_pipeline(p, settings)


if __name__ == "__main__":
    run()
