"""Configuración compartida por notebooks, procesos de línea de comandos y tests."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

RAW_TOPIC = "ema.lecturas.v1"
AGGREGATES_TOPIC = "ema.agregados.v1"
DLQ_TOPIC = "ema.lecturas.dlq.v1"


@dataclass(frozen=True)
class Settings:
    """Parámetros de ejecución con valores por defecto pensados para Docker Compose."""

    kafka_bootstrap_servers: str = "kafka:9092"
    raw_topic: str = RAW_TOPIC
    aggregates_topic: str = AGGREGATES_TOPIC
    dlq_topic: str = DLQ_TOPIC
    window_seconds: int = 600  # ventana principal: 10 minutos (cadencia de las EMAS)
    hourly_window_seconds: int = 3600  # ventana de subcuenca: 1 hora
    allowed_lateness_seconds: int = 1200  # 20 minutos (ver Tarea 2)
    early_firing_seconds: int = 10
    alert_precip_10min_mm: float = 10.0  # lluvia intensa: >= 10 mm en 10 min
    alert_gust_ms: float = 20.0  # ráfaga fuerte: >= 20 m/s (72 km/h)
    parallelism: int = 2
    job_endpoint: str = "beam-job-server:8099"

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            raw_topic=os.getenv("KAFKA_RAW_TOPIC", RAW_TOPIC),
            aggregates_topic=os.getenv("KAFKA_AGGREGATES_TOPIC", AGGREGATES_TOPIC),
            dlq_topic=os.getenv("KAFKA_DLQ_TOPIC", DLQ_TOPIC),
            window_seconds=int(os.getenv("WINDOW_SECONDS", "600")),
            hourly_window_seconds=int(os.getenv("HOURLY_WINDOW_SECONDS", "3600")),
            allowed_lateness_seconds=int(os.getenv("ALLOWED_LATENESS_SECONDS", "1200")),
            early_firing_seconds=int(os.getenv("EARLY_FIRING_SECONDS", "10")),
            alert_precip_10min_mm=float(os.getenv("ALERT_PRECIP_10MIN_MM", "10")),
            alert_gust_ms=float(os.getenv("ALERT_GUST_MS", "20")),
            parallelism=int(os.getenv("BEAM_PARALLELISM", "2")),
            job_endpoint=os.getenv("BEAM_JOB_ENDPOINT", "beam-job-server:8099"),
        )


def project_root() -> Path:
    """Raíz del proyecto, tanto desde el código fuente como desde una wheel instalada."""
    configured = os.getenv("HIDROMET_LAB_ROOT")
    if configured:
        return Path(configured).resolve()
    return Path(__file__).resolve().parents[2]


def processed_dataset_path() -> Path:
    default = project_root() / "data/processed/emas_10min.parquet"
    return Path(os.getenv("HIDROMET_DATASET_PATH", default))


def stations_path() -> Path:
    default = project_root() / "data/processed/estaciones.csv"
    return Path(os.getenv("HIDROMET_STATIONS_PATH", default))
