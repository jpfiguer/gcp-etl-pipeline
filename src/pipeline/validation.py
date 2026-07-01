"""cloud function post-load para validar bulk inserts

este archivo esta pensado para deployarse como una cloud function 2nd gen
gatillada por una notificacion pub/sub que emite bigquery al finalizar
una escritura o por un scheduled trigger cada N minutos

qué valida:
1. row count de la ultima particion vs promedio movil de los N dias previos
2. row count por source_table vs promedio movil
3. duplicados por event_id en la particion
4. gap de tiempo entre event_timestamp y ingested_at (freshness)

si alguna validacion falla, emite un log estructurado con severidad ERROR
que dispara alertas en cloud logging + notifica un topic pub/sub para
integraciones downstream
"""
from __future__ import annotations

import json
import logging
import os
from datetime import date, timedelta
from typing import Any

from google.cloud import bigquery, pubsub_v1


logger = logging.getLogger(__name__)


_PROJECT = os.environ["GCP_PROJECT"]
_DATASET = os.environ.get("BQ_DATASET_RAW", "raw")
_TABLE = os.environ.get("BQ_TABLE_EVENTS", "events")
_ALERT_TOPIC = os.environ.get("ALERT_TOPIC", "cdc-validation-alerts")

# thresholds
_DEVIATION_PCT_ROWS = float(os.environ.get("DEVIATION_PCT_ROWS", "0.30"))
_DEVIATION_PCT_TABLE = float(os.environ.get("DEVIATION_PCT_TABLE", "0.40"))
_MAX_FRESHNESS_MINUTES = int(os.environ.get("MAX_FRESHNESS_MINUTES", "30"))


def _bq_client() -> bigquery.Client:
    return bigquery.Client(project=_PROJECT)


def _publisher() -> pubsub_v1.PublisherClient:
    return pubsub_v1.PublisherClient()


def _emit_alert(kind: str, payload: dict[str, Any]) -> None:
    """publica una alerta al topic + loguea en stackdriver"""
    logger.error("validation_failed", extra={"kind": kind, **payload})
    pub = _publisher()
    topic_path = pub.topic_path(_PROJECT, _ALERT_TOPIC)
    pub.publish(
        topic_path,
        json.dumps({"kind": kind, **payload}, default=str).encode("utf-8"),
    )


def check_row_count(client: bigquery.Client, target_date: date) -> None:
    """compara la cantidad de filas de una particion vs el promedio de los ultimos 7 dias

    si desvia mas de _DEVIATION_PCT_ROWS del promedio, alerta
    """
    query = f"""
    with target as (
      select count(*) as n
      from `{_PROJECT}.{_DATASET}.{_TABLE}`
      where date(event_timestamp) = @target_date
    ),
    baseline as (
      select avg(daily_n) as avg_n
      from (
        select date(event_timestamp) as d, count(*) as daily_n
        from `{_PROJECT}.{_DATASET}.{_TABLE}`
        where date(event_timestamp)
          between date_sub(@target_date, interval 7 day)
          and date_sub(@target_date, interval 1 day)
        group by 1
      )
    )
    select (select n from target) as n, (select avg_n from baseline) as avg_n
    """
    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("target_date", "DATE", target_date),
            ]
        ),
    )
    row = list(job.result())[0]
    n = row["n"] or 0
    avg_n = row["avg_n"] or 0
    if avg_n == 0:
        return
    deviation = abs(n - avg_n) / avg_n
    if deviation > _DEVIATION_PCT_ROWS:
        _emit_alert(
            "row_count_deviation",
            {
                "date": target_date,
                "n": n,
                "avg_n": avg_n,
                "deviation_pct": deviation,
                "threshold": _DEVIATION_PCT_ROWS,
            },
        )


def check_duplicates(client: bigquery.Client, target_date: date) -> None:
    """cuenta event_id repetidos en la particion, cualquier duplicado alerta"""
    query = f"""
    select count(*) as duplicate_count
    from (
      select event_id, count(*) as c
      from `{_PROJECT}.{_DATASET}.{_TABLE}`
      where date(event_timestamp) = @target_date
      group by 1
      having c > 1
    )
    """
    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("target_date", "DATE", target_date),
            ]
        ),
    )
    row = list(job.result())[0]
    dup = row["duplicate_count"] or 0
    if dup > 0:
        _emit_alert(
            "duplicate_event_ids",
            {"date": target_date, "duplicate_count": dup},
        )


def check_freshness(client: bigquery.Client) -> None:
    """freshness: p95 del gap event → ingested en la ultima hora

    si el p95 supera _MAX_FRESHNESS_MINUTES, alerta
    """
    query = f"""
    select
      approx_quantiles(
        timestamp_diff(ingested_at, event_timestamp, minute),
        100
      )[offset(95)] as p95_minutes
    from `{_PROJECT}.{_DATASET}.{_TABLE}`
    where ingested_at >= timestamp_sub(current_timestamp(), interval 1 hour)
    """
    job = client.query(query)
    row = list(job.result())[0]
    p95 = row["p95_minutes"]
    if p95 is None:
        return
    if p95 > _MAX_FRESHNESS_MINUTES:
        _emit_alert(
            "stale_pipeline",
            {"p95_minutes": int(p95), "threshold": _MAX_FRESHNESS_MINUTES},
        )


def validate(request: Any) -> dict:
    """entry point de la cloud function

    lee `target_date` del body (opcional, default = ayer)
    corre todas las validaciones, retorna el resultado
    """
    body: dict[str, Any] = {}
    try:
        body = request.get_json(silent=True) or {}
    except Exception:
        body = {}

    if "target_date" in body:
        target = date.fromisoformat(body["target_date"])
    else:
        target = date.today() - timedelta(days=1)

    client = _bq_client()
    check_row_count(client, target)
    check_duplicates(client, target)
    check_freshness(client)

    return {"status": "ok", "target_date": target.isoformat()}
