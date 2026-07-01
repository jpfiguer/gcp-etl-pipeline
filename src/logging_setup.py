"""configura logging estructurado para toda la app

en desarrollo, texto legible por humanos
en produccion (dataflow, cloud functions), json compatible con cloud logging
"""
from __future__ import annotations

import logging
import os
import sys

import structlog


def configure_logging() -> None:
    """setup unico e idempotente

    detecta el entorno via env var LOG_FORMAT:
      - json (default en containers): salida en json compatible con cloud logging
      - console: color y timestamps legibles para dev local
    """
    log_format = os.environ.get("LOG_FORMAT", "json").lower()
    level = os.environ.get("LOG_LEVEL", "INFO").upper()

    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.format_exc_info,
    ]

    if log_format == "console":
        processors = shared_processors + [
            structlog.dev.ConsoleRenderer(colors=True),
        ]
    else:
        processors = shared_processors + [
            structlog.processors.EventRenamer("message"),
            structlog.processors.JSONRenderer(),
        ]

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level, logging.INFO)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )

    # sincroniza logging estandar con structlog para que apache beam y otros
    # loguen con el mismo formato
    logging.basicConfig(
        format="%(message)s",
        level=getattr(logging, level, logging.INFO),
        stream=sys.stdout,
    )
