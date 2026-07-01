"""settings de la aplicacion via pydantic-settings

lee de env vars y opcionalmente de un .env file
todas las settings tienen defaults sensatos para desarrollo local
"""
from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """configuracion global del pipeline"""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # gcp
    gcp_project: str = Field(default="my-project", description="gcp project id")
    gcp_region: str = Field(default="us-central1", description="gcp region para dataflow")

    # pub/sub
    pubsub_topic: str = Field(default="cdc-events", description="topic con eventos cdc")
    pubsub_subscription: str = Field(
        default="cdc-events-pipeline",
        description="subscription para el pipeline streaming",
    )

    # bigquery
    bq_dataset_raw: str = Field(default="raw", description="dataset staging")
    bq_dataset_bronze: str = Field(default="bronze", description="dataset limpieza")
    bq_dataset_silver: str = Field(default="silver", description="dataset reglas de negocio")
    bq_dataset_gold: str = Field(default="gold", description="dataset data marts")
    bq_table_events: str = Field(default="events", description="tabla destino raw")

    # dedup window
    dedup_window_seconds: int = Field(
        default=600,
        description="ventana temporal para deduplicacion por business key",
    )

    # runtime
    runner: Literal["DirectRunner", "DataflowRunner"] = Field(
        default="DirectRunner",
        description="beam runner: DirectRunner en local, DataflowRunner en prod",
    )
    streaming: bool = Field(
        default=True, description="modo streaming vs batch"
    )
    log_level: str = Field(default="INFO", description="nivel de log")

    # dataflow specifico
    dataflow_staging: str = Field(
        default="", description="gs:// staging path, requerido en DataflowRunner"
    )
    dataflow_temp: str = Field(
        default="", description="gs:// temp path, requerido en DataflowRunner"
    )
    dataflow_service_account: str = Field(
        default="", description="service account para dataflow workers"
    )

    @property
    def pubsub_topic_full(self) -> str:
        """path completo del topic pub/sub"""
        return f"projects/{self.gcp_project}/topics/{self.pubsub_topic}"

    @property
    def pubsub_subscription_full(self) -> str:
        """path completo de la subscription"""
        return (
            f"projects/{self.gcp_project}/subscriptions/{self.pubsub_subscription}"
        )

    @property
    def bq_raw_table_ref(self) -> str:
        """referencia completa a la tabla raw destino"""
        return f"{self.gcp_project}:{self.bq_dataset_raw}.{self.bq_table_events}"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """singleton de settings, cacheado con lru_cache"""
    return Settings()
