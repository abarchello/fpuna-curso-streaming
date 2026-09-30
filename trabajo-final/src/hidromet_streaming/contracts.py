"""Contratos estables de eventos y agregados para la telemetría hidro-meteorológica.

El contrato de entrada es una **lectura de estación meteorológica automática
(EMA)**: un registro cada 10 minutos por estación con las variables medidas.
El contrato de salida (agregados) se documenta en `transforms.py`.
"""

from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

EVENT_TYPE = "ema.lectura"
SCHEMA_VERSION = 1

# Rangos físicos plausibles para el Alto Paraná. Un valor fuera de rango no se
# corrige: el evento va a la DLQ para diagnóstico del sensor.
PHYSICAL_RANGES: dict[str, tuple[float, float]] = {
    "precip_mm": (0.0, 120.0),  # en 10 minutos
    "temp_c": (-10.0, 50.0),
    "hr_pct": (0.0, 100.0),
    "presion_hpa": (930.0, 1060.0),
    "viento_ms": (0.0, 60.0),
    "viento_dir_deg": (0.0, 360.0),
    "rafaga_ms": (0.0, 80.0),
    "radiacion_wm2": (0.0, 1500.0),
}
VARIABLES = tuple(PHYSICAL_RANGES)
# Sólo la precipitación es obligatoria: una red real no mide todas las
# variables en todas las estaciones. Las demás pueden venir en `null`.
REQUIRED_VARIABLES = ("precip_mm",)


def iso_utc(value: datetime | str) -> str:
    """Normalizar a ISO-8601 UTC con sufijo `Z`."""
    parsed = parse_utc(value) if isinstance(value, str) else value
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")


def parse_utc(raw_value: str) -> datetime:
    """Convertir un timestamp ISO-8601 (con `Z` u offset) a datetime UTC aware."""
    if not isinstance(raw_value, str) or not raw_value.strip():
        raise ValueError(f"timestamp inválido: {raw_value!r}")
    normalized = raw_value.strip()
    if normalized.endswith(("Z", "z")):
        normalized = normalized[:-1] + "+00:00"
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        raise ValueError(f"timestamp sin zona horaria: {raw_value!r}")
    return parsed.astimezone(UTC)


def _finite(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


@dataclass(frozen=True)
class StationReading:
    """Una lectura de 10 minutos de una estación meteorológica automática."""

    schema_version: int
    event_id: str
    event_type: str
    event_time: str
    station_id: str
    station_name: str
    subcuenca: str
    lat: float
    lon: float
    seq: int
    precip_mm: float
    temp_c: float | None
    hr_pct: float | None
    presion_hpa: float | None
    viento_ms: float | None
    viento_dir_deg: float | None
    rafaga_ms: float | None
    radiacion_wm2: float | None
    source: str
    flag_qc: str = "OK"

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def make_event_id(station_id: str, event_time: datetime | str) -> str:
    """Identificador determinista: la misma lectura reenviada produce el mismo id."""
    stamp = parse_utc(event_time) if isinstance(event_time, str) else event_time.astimezone(UTC)
    return f"ema-{station_id}-{stamp:%Y%m%dT%H%M%SZ}"


def reading_from_row(row: dict[str, Any], *, source: str) -> StationReading:
    """Construir una lectura validada a partir de una fila del dataset procesado."""
    event_time = iso_utc(row["event_time"])
    station_id = str(row["station_id"])
    values: dict[str, float | None] = {}
    for name in VARIABLES:
        parsed = _finite(row.get(name))
        if parsed is None and name in REQUIRED_VARIABLES:
            raise ValueError(f"{station_id} {event_time}: variable {name} ausente o no numérica")
        values[name] = None if parsed is None else round(parsed, 1)
    return StationReading(
        schema_version=SCHEMA_VERSION,
        event_id=make_event_id(station_id, event_time),
        event_type=EVENT_TYPE,
        event_time=event_time,
        station_id=station_id,
        station_name=str(row.get("station_name", station_id)),
        subcuenca=str(row.get("subcuenca", "desconocida")),
        lat=float(row.get("lat", 0.0)),
        lon=float(row.get("lon", 0.0)),
        seq=int(row.get("seq", 0)),
        source=source,
        flag_qc=str(row.get("flag_qc", "OK")),
        **values,
    )


def encode_event(event: StationReading | dict[str, Any]) -> bytes:
    payload = event.as_dict() if isinstance(event, StationReading) else event
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()


def decode_event(payload: bytes | str) -> dict[str, Any]:
    """Decodificar y validar un evento; levanta ValueError si viola el contrato."""
    decoded = json.loads(payload.decode() if isinstance(payload, bytes) else payload)
    required = {"event_id", "event_type", "event_time", "station_id", *REQUIRED_VARIABLES}
    missing = required.difference(decoded)
    if missing:
        raise ValueError(f"faltan campos obligatorios: {sorted(missing)}")
    if decoded["event_type"] != EVENT_TYPE:
        raise ValueError(f"event_type no soportado: {decoded['event_type']}")
    parse_utc(decoded["event_time"])
    for name, (low, high) in PHYSICAL_RANGES.items():
        raw = decoded.get(name)
        if raw is None and name not in REQUIRED_VARIABLES:
            continue
        value = _finite(raw)
        if value is None or not (low <= value <= high):
            raise ValueError(f"{name}={raw!r} fuera del rango físico [{low}, {high}]")
    return decoded
