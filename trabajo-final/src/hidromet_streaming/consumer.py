"""Consumidor analítico incremental con modelo de serving idempotente en memoria."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from typing import Any

import pandas as pd
from confluent_kafka import Consumer, KafkaError

from hidromet_streaming.config import Settings
from hidromet_streaming.transforms import is_empty_pane


@dataclass
class AggregateStore:
    """Aplicar revisiones (panes) por upsert y exponer DataFrames listos para análisis."""

    records: dict[str, dict[str, Any]] = field(default_factory=dict)
    messages_seen: int = 0
    revisions: int = 0

    def upsert(self, aggregate: dict[str, Any]) -> bool:
        aggregate_id = aggregate["aggregate_id"]
        current = self.records.get(aggregate_id)
        self.messages_seen += 1
        if current is not None:
            # Un pane viejo que llega después de uno más nuevo (reintento fuera
            # de orden) no debe pisar la revisión más reciente.
            if int(current.get("pane_index", -1)) > int(aggregate.get("pane_index", -1)):
                return False
            # Al expirar una ventana, el runner puede emitir un pane de cierre
            # sin lecturas con el mismo pane_index: no trae información nueva.
            if is_empty_pane(aggregate) and not is_empty_pane(current):
                return False
            self.revisions += 1
        self.records[aggregate_id] = aggregate
        return True

    def frame(self, metric_type: str | None = None) -> pd.DataFrame:
        rows = list(self.records.values())
        if metric_type is not None:
            rows = [row for row in rows if row.get("metric_type") == metric_type]
        frame = pd.DataFrame(rows)
        if frame.empty:
            return frame
        for column in ("window_start", "window_end"):
            frame[column] = pd.to_datetime(frame[column], utc=True)
        return frame.sort_values(["window_start", "dimension_name"])

    def station_frame(self) -> pd.DataFrame:
        return self.frame("estacion_10min")

    def basin_frame(self) -> pd.DataFrame:
        return self.frame("subcuenca_1h")

    def alerts_frame(self) -> pd.DataFrame:
        return self.frame("alerta")

    def summary(self) -> dict[str, int]:
        return {
            "messages_seen": self.messages_seen,
            "logical_aggregates": len(self.records),
            "revisions": self.revisions,
            "station_windows": len(self.station_frame()),
            "basin_windows": len(self.basin_frame()),
            "alerts": len(self.alerts_frame()),
        }


def build_consumer(
    settings: Settings,
    *,
    group_id: str | None = None,
    offset_reset: str = "earliest",
) -> Consumer:
    """Consumidor del tópico de agregados para una vista en memoria.

    La vista (`AggregateStore`) vive en memoria, así que cada sesión tiene que
    reconstruirla leyendo el tópico desde el principio. Por eso, si no se pasa
    un `group_id`, se usa uno nuevo por sesión y no se confirman offsets: con
    un grupo fijo y offsets confirmados, Kafka ignora `auto.offset.reset` y la
    vista arrancaría vacía.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": group_id or f"hidromet-vista-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": offset_reset,
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([settings.aggregates_topic])
    return consumer


def poll_into_store(
    consumer: Consumer,
    store: AggregateStore,
    *,
    max_messages: int = 500,
    timeout_seconds: float = 1.0,
) -> dict[str, int]:
    accepted = 0
    errors = 0
    for message in consumer.consume(num_messages=max_messages, timeout=timeout_seconds):
        if message.error():
            if message.error().code() != KafkaError._PARTITION_EOF:
                errors += 1
            continue
        try:
            aggregate = json.loads(message.value().decode())
            accepted += int(store.upsert(aggregate))
        except (json.JSONDecodeError, KeyError, UnicodeDecodeError):
            errors += 1
    return {"accepted": accepted, "errors": errors, **store.summary()}
