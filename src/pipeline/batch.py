"""pipeline batch para backfills historicos

reutiliza los mismos DoFns que el streaming, cambia solo la fuente y el
sink de errores. usa lectura desde gcs (archivos jsonl exportados desde la
fuente) y escribe a bigquery con load jobs en vez de streaming inserts
"""
from __future__ import annotations

import argparse
import logging

import apache_beam as beam
from apache_beam.io.gcp.bigquery import (
    BigQueryDisposition,
    WriteToBigQuery,
)
from apache_beam.io.textio import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

from src.config import get_settings
from src.logging_setup import configure_logging
from src.pipeline.streaming import _BQ_SCHEMA
from src.pipeline.transforms import (
    DecodeEnvelope,
    ToBQRow,
    ToCanonicalEvent,
    dedup_pipeline,
)


def build_pipeline(
    pipeline: beam.Pipeline,
    settings,
    input_glob: str,
    error_output_prefix: str,
) -> None:
    """arma el grafo batch usando load jobs a bigquery

    load jobs son mas baratos que streaming inserts para volumenes grandes
    y no consumen la cuota de streaming
    """
    lines = pipeline | "ReadFromText" >> ReadFromText(input_glob)

    decoded = (
        lines
        | "ToBytes" >> beam.Map(lambda s: s.encode("utf-8"))
        | "DecodeEnvelope" >> beam.ParDo(DecodeEnvelope()).with_outputs(
            "errors", main="ok"
        )
    )

    _ = decoded.errors | "WriteErrors" >> WriteToText(
        error_output_prefix, file_name_suffix=".jsonl"
    )

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
            method=WriteToBigQuery.Method.FILE_LOADS,
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
    configure_logging()
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(description="cdc batch backfill pipeline")
    parser.add_argument(
        "--input", required=True, help="glob gs:// o filesystem con archivos jsonl"
    )
    parser.add_argument(
        "--error_output_prefix",
        default="/tmp/cdc-batch-errors",
        help="prefix para archivos con eventos que no se pudieron parsear",
    )
    known, pipeline_args = parser.parse_known_args(argv)

    settings = get_settings()

    options = PipelineOptions(pipeline_args)
    options.view_as(StandardOptions).runner = settings.runner
    options.view_as(StandardOptions).streaming = False

    logger.info(
        "batch_pipeline_start",
        extra={
            "input": known.input,
            "runner": settings.runner,
            "table": settings.bq_raw_table_ref,
        },
    )

    with beam.Pipeline(options=options) as p:
        build_pipeline(p, settings, known.input, known.error_output_prefix)


if __name__ == "__main__":
    run()
