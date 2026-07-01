"""schemas pydantic para envelopes cdc y filas destino

el pipeline decodea el envelope crudo de pub/sub, valida contra CdcEnvelope
y produce una CanonicalEvent lista para escribir a bigquery
"""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator


class Operation(str, Enum):
    """tipo de operacion cdc"""

    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"


class CdcEnvelope(BaseModel):
    """envelope generico estilo debezium/datastream

    el pipeline no asume un formato especifico de fuente, solo requiere
    estos campos minimos para desambiguar y particionar
    """

    event_id: str = Field(..., description="uuid unico del evento cdc")
    event_timestamp: datetime = Field(
        ..., description="cuando ocurrio el cambio en la fuente"
    )
    ingested_at: Optional[datetime] = Field(
        default=None, description="cuando llego al bus (opcional)"
    )
    source_table: str = Field(..., description="tabla de origen")
    business_key: str = Field(
        ..., description="natural key de la fila afectada"
    )
    operation: Operation = Field(..., description="tipo de operacion")
    payload: dict[str, Any] = Field(
        default_factory=dict, description="valor completo post cambio"
    )
    payload_before: Optional[dict[str, Any]] = Field(
        default=None, description="valor previo, solo en updates y deletes"
    )
    schema_version: int = Field(
        default=1, description="version del schema del envelope"
    )

    @field_validator("business_key")
    @classmethod
    def _business_key_non_empty(cls, v: str) -> str:
        """la business key no puede ser vacia o whitespace"""
        v = v.strip()
        if not v:
            raise ValueError("business_key vacia no es valida")
        return v


class CanonicalEvent(BaseModel):
    """fila destino en bigquery raw.events

    el orden de las columnas y sus nombres se preservan al escribir
    """

    event_id: str
    event_timestamp: datetime
    ingested_at: datetime
    source_table: str
    business_key: str
    operation: Operation
    payload_json: str = Field(
        ..., description="payload serializado como json string"
    )
    payload_before_json: Optional[str] = Field(
        default=None, description="payload_before serializado"
    )
    schema_version: int
    partition_date: str = Field(
        ..., description="fecha de particion YYYY-MM-DD derivada de event_timestamp"
    )

    def to_bq_row(self) -> dict[str, Any]:
        """convierte a dict compatible con WriteToBigQuery"""
        return {
            "event_id": self.event_id,
            "event_timestamp": self.event_timestamp.isoformat(),
            "ingested_at": self.ingested_at.isoformat(),
            "source_table": self.source_table,
            "business_key": self.business_key,
            "operation": self.operation.value,
            "payload_json": self.payload_json,
            "payload_before_json": self.payload_before_json,
            "schema_version": self.schema_version,
            "partition_date": self.partition_date,
        }
