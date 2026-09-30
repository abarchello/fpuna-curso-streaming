"""Transformaciones Beam reutilizables para la analítica hidro-meteorológica.

Contrato de salida (un registro JSON por pane, clave Kafka = `aggregate_id`):

| campo | contenido |
|---|---|
| `aggregate_id` | `metric_type|window_start|dimension_id` (clave idempotente) |
| `metric_type` | `estacion_10min`, `subcuenca_1h` o `alerta` |
| `window_start`, `window_end` | límites de la ventana en tiempo de evento (ISO UTC) |
| `dimension_id`, `dimension_name`, `subcuenca` | estación o subcuenca |
| `pane_index`, `pane_timing`, `is_first`, `is_last` | metadatos del pane (revisiones) |
| métricas | ver `StationStatsCombineFn` / `BasinStatsCombineFn` |

Cada pane trae el total **acumulado** de la ventana (modo ACCUMULATING); el
consumidor reemplaza por `aggregate_id` en lugar de sumar.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.coders import StrUtf8Coder
from apache_beam.transforms import trigger, window
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.userstate import SetStateSpec, TimerSpec, on_timer
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.windowed_value import PaneInfoTiming

from hidromet_streaming.config import Settings
from hidromet_streaming.contracts import decode_event, parse_utc


class ParseEvent(beam.DoFn):
    """Decodificar y validar JSON; lo inválido sale por una salida etiquetada (DLQ)."""

    INVALID = "invalid"

    def process(self, element: tuple[bytes, bytes]):
        key, payload = element
        try:
            event = decode_event(payload)
            event["kafka_key"] = key.decode(errors="replace") if key else None
            yield event
        except (ValueError, TypeError, json.JSONDecodeError) as error:
            yield pvalue.TaggedOutput(
                self.INVALID,
                {"error": str(error), "payload": payload.decode(errors="replace")},
            )


def assign_event_timestamp(event: dict[str, Any]) -> TimestampedValue:
    """El timestamp del dominio es `event_time` (cuándo midió el sensor)."""
    return TimestampedValue(event, parse_utc(event["event_time"]).timestamp())


class DeduplicateReadings(beam.DoFn):
    """Eliminar `event_id` repetidos dentro de cada estación y ventana.

    Estado por clave (`station_id`) y ventana; un timer de watermark limpia el
    conjunto al vencer `fin de ventana + lateness`, cuando ya ningún duplicado
    podría alterar un agregado. Sin expiración el estado crecería sin cota.
    """

    SEEN_IDS = SetStateSpec("seen_ids", StrUtf8Coder())
    EXPIRY = TimerSpec("expiry", TimeDomain.WATERMARK)

    def __init__(self, allowed_lateness_seconds: int) -> None:
        super().__init__()
        self.allowed_lateness_seconds = allowed_lateness_seconds

    def process(
        self,
        element: tuple[str, dict[str, Any]],
        seen_ids=beam.DoFn.StateParam(SEEN_IDS),
        window_param=beam.DoFn.WindowParam,
        expiry=beam.DoFn.TimerParam(EXPIRY),
    ):
        _station, event = element
        event_id = str(event["event_id"])
        if event_id in set(seen_ids.read()):
            return
        seen_ids.add(event_id)
        expiry.set(window_param.end + self.allowed_lateness_seconds)
        yield element

    @on_timer(EXPIRY)
    def expire(self, seen_ids=beam.DoFn.StateParam(SEEN_IDS)):
        seen_ids.clear()


class StationStatsCombineFn(beam.CombineFn):
    """Métricas aditivas/incrementales de una estación en una ventana.

    El acumulador es un diccionario de sumas, extremos y conteos **por
    variable**, de modo que Beam pueda combinar parciales en cada worker antes
    del shuffle (combiner lifting) y que una variable ausente en una estación
    (valor `null`) simplemente no cuente. El promedio se calcula al extraer.
    """

    SUM_VARS = ("temp_c", "hr_pct", "presion_hpa", "viento_ms", "radiacion_wm2")

    def create_accumulator(self):
        return {
            "n": 0,
            "precip": 0.0,
            "sums": dict.fromkeys(self.SUM_VARS, 0.0),
            "counts": dict.fromkeys(self.SUM_VARS, 0),
            "temp_min": float("inf"),
            "temp_max": float("-inf"),
            "gust_max": float("-inf"),
        }

    def add_input(self, acc, event):
        acc = {**acc, "sums": dict(acc["sums"]), "counts": dict(acc["counts"])}
        acc["n"] += 1
        acc["precip"] += float(event.get("precip_mm") or 0.0)
        for name in self.SUM_VARS:
            value = event.get(name)
            if value is not None:
                acc["sums"][name] += float(value)
                acc["counts"][name] += 1
        temp = event.get("temp_c")
        if temp is not None:
            acc["temp_min"] = min(acc["temp_min"], float(temp))
            acc["temp_max"] = max(acc["temp_max"], float(temp))
        gust = event.get("rafaga_ms")
        if gust is not None:
            acc["gust_max"] = max(acc["gust_max"], float(gust))
        return acc

    def merge_accumulators(self, accumulators):
        merged = self.create_accumulator()
        for acc in accumulators:
            merged["n"] += acc["n"]
            merged["precip"] += acc["precip"]
            for name in self.SUM_VARS:
                merged["sums"][name] += acc["sums"][name]
                merged["counts"][name] += acc["counts"][name]
            merged["temp_min"] = min(merged["temp_min"], acc["temp_min"])
            merged["temp_max"] = max(merged["temp_max"], acc["temp_max"])
            merged["gust_max"] = max(merged["gust_max"], acc["gust_max"])
        return merged

    def extract_output(self, acc):
        if acc["n"] == 0:
            return {"n_lecturas": 0, "precip_mm": 0.0}

        def mean(name, digits):
            count = acc["counts"][name]
            return round(acc["sums"][name] / count, digits) if count else None

        def finite(value, digits):
            return round(value, digits) if value not in (float("inf"), float("-inf")) else None

        return {
            "n_lecturas": acc["n"],
            "precip_mm": round(acc["precip"], 1),
            "temp_media_c": mean("temp_c", 1),
            "temp_min_c": finite(acc["temp_min"], 1),
            "temp_max_c": finite(acc["temp_max"], 1),
            "hr_media_pct": mean("hr_pct", 0),
            "presion_media_hpa": mean("presion_hpa", 1),
            "viento_medio_ms": mean("viento_ms", 1),
            "rafaga_max_ms": finite(acc["gust_max"], 1),
            "radiacion_media_wm2": mean("radiacion_wm2", 0),
        }


class BasinStatsCombineFn(beam.CombineFn):
    """Precipitación areal de una subcuenca: promedio de los totales por estación.

    El acumulador guarda un diccionario `station_id -> (suma, n)`: sumar la
    lluvia de todas las estaciones no tiene sentido físico; sí lo tiene el
    promedio de los acumulados por estación (lluvia media areal) y el máximo.
    """

    def create_accumulator(self):
        return {}

    def add_input(self, acc, event):
        station = str(event["station_id"])
        total, n = acc.get(station, (0.0, 0))
        acc = dict(acc)
        acc[station] = (total + float(event.get("precip_mm") or 0.0), n + 1)
        return acc

    def merge_accumulators(self, accumulators):
        merged: dict[str, tuple[float, int]] = {}
        for acc in accumulators:
            for station, (total, n) in acc.items():
                t0, n0 = merged.get(station, (0.0, 0))
                merged[station] = (t0 + total, n0 + n)
        return merged

    def extract_output(self, acc):
        if not acc:
            return {"n_estaciones": 0, "precip_media_mm": 0.0, "precip_max_mm": 0.0}
        totals = {s: t for s, (t, _) in acc.items()}
        return {
            "n_estaciones": len(totals),
            "n_lecturas": sum(n for _, n in acc.values()),
            "precip_media_mm": round(sum(totals.values()) / len(totals), 1),
            "precip_max_mm": round(max(totals.values()), 1),
            "estacion_max": max(totals, key=totals.get),
        }


def _iso_z(timestamp) -> str:
    """Timestamp de Beam -> ISO-8601 UTC con sufijo Z (mismo formato que los eventos)."""
    return datetime.fromtimestamp(float(timestamp), tz=UTC).isoformat().replace("+00:00", "Z")


class FormatAggregate(beam.DoFn):
    """Adjuntar metadatos de ventana y pane, y construir la clave idempotente."""

    def __init__(self, metric_type: str) -> None:
        super().__init__()
        self.metric_type = metric_type

    def process(
        self,
        element,
        window_param=beam.DoFn.WindowParam,
        pane_info=beam.DoFn.PaneInfoParam,
    ):
        dimension, metrics = element
        start = _iso_z(window_param.start)
        end = _iso_z(window_param.end)
        dimension_id, dimension_name, subcuenca = dimension
        yield {
            "schema_version": 1,
            "aggregate_id": f"{self.metric_type}|{start}|{dimension_id}",
            "metric_type": self.metric_type,
            "window_start": start,
            "window_end": end,
            "dimension_id": str(dimension_id),
            "dimension_name": dimension_name,
            "subcuenca": subcuenca,
            "pane_index": pane_info.index,
            "pane_timing": PaneInfoTiming.to_string(pane_info.timing),
            "is_first": pane_info.is_first,
            "is_last": pane_info.is_last,
            **metrics,
        }


def _windowed(events, settings: Settings, *, seconds: int, label: str, streaming_triggers: bool):
    kwargs: dict[str, Any] = {
        "windowfn": window.FixedWindows(seconds),
        "allowed_lateness": settings.allowed_lateness_seconds,
        "accumulation_mode": trigger.AccumulationMode.ACCUMULATING,
    }
    if streaming_triggers:
        kwargs["trigger"] = trigger.AfterWatermark(
            early=trigger.AfterProcessingTime(settings.early_firing_seconds),
            late=trigger.AfterCount(1),
        )
    return events | label >> beam.WindowInto(**kwargs)


def detect_alerts(aggregate: dict[str, Any], settings: Settings):
    """Derivar alertas de un agregado de estación de 10 minutos."""
    conditions = []
    if float(aggregate.get("precip_mm") or 0.0) >= settings.alert_precip_10min_mm:
        conditions.append(("lluvia_intensa", "precip_mm", settings.alert_precip_10min_mm))
    if float(aggregate.get("rafaga_max_ms") or 0.0) >= settings.alert_gust_ms:
        conditions.append(("rafaga_fuerte", "rafaga_max_ms", settings.alert_gust_ms))
    for tipo, variable, umbral in conditions:
        yield {
            **{
                k: aggregate[k]
                for k in (
                    "schema_version",
                    "window_start",
                    "window_end",
                    "dimension_id",
                    "dimension_name",
                    "subcuenca",
                    "pane_index",
                    "pane_timing",
                    "is_first",
                    "is_last",
                )
            },
            "aggregate_id": (
                f"alerta|{aggregate['window_start']}|{aggregate['dimension_id']}|{tipo}"
            ),
            "metric_type": "alerta",
            "tipo": tipo,
            "variable": variable,
            "umbral": umbral,
            "valor": aggregate[variable],
            "n_lecturas": aggregate.get("n_lecturas", 0),
        }


def build_analytics(events, settings: Settings, *, streaming_triggers: bool = True):
    """Construir agregados por estación (10 min), por subcuenca (1 h) y alertas."""
    timestamped = events | "Asignar tiempo de evento" >> beam.Map(assign_event_timestamp)

    ten_min = _windowed(
        timestamped,
        settings,
        seconds=settings.window_seconds,
        label="Ventanas fijas de 10 min",
        streaming_triggers=streaming_triggers,
    )
    deduplicated = (
        ten_min
        | "Clave por estación" >> beam.Map(lambda e: (e["station_id"], e))
        | "Deduplicar por event_id"
        >> beam.ParDo(DeduplicateReadings(settings.allowed_lateness_seconds))
        | "Quitar clave" >> beam.Map(lambda kv: kv[1])
    )

    station_stats = (
        deduplicated
        | "Clave (estación, nombre, subcuenca)"
        >> beam.Map(lambda e: ((e["station_id"], e["station_name"], e["subcuenca"]), e))
        | "Combinar métricas por estación" >> beam.CombinePerKey(StationStatsCombineFn())
        | "Formatear agregados de estación" >> beam.ParDo(FormatAggregate("estacion_10min"))
    )

    alerts = station_stats | "Detectar alertas" >> beam.FlatMap(detect_alerts, settings)

    basin_stats = (
        deduplicated
        | "Re-ventanear a 1 h"
        >> beam.WindowInto(
            window.FixedWindows(settings.hourly_window_seconds),
            allowed_lateness=settings.allowed_lateness_seconds,
            accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
            **(
                {
                    "trigger": trigger.AfterWatermark(
                        early=trigger.AfterProcessingTime(settings.early_firing_seconds),
                        late=trigger.AfterCount(1),
                    )
                }
                if streaming_triggers
                else {}
            ),
        )
        | "Clave por subcuenca"
        >> beam.Map(lambda e: ((e["subcuenca"], e["subcuenca"], e["subcuenca"]), e))
        | "Combinar precipitación areal" >> beam.CombinePerKey(BasinStatsCombineFn())
        | "Formatear agregados de subcuenca" >> beam.ParDo(FormatAggregate("subcuenca_1h"))
    )

    return (station_stats, alerts, basin_stats) | "Unir registros analíticos" >> beam.Flatten()


def aggregate_to_kafka_record(aggregate: dict[str, Any]) -> tuple[bytes, bytes]:
    return (
        aggregate["aggregate_id"].encode(),
        json.dumps(aggregate, sort_keys=True, separators=(",", ":")).encode(),
    )


def invalid_to_kafka_record(record: dict[str, Any]) -> tuple[bytes, bytes]:
    return (b"invalid", json.dumps(record, sort_keys=True, separators=(",", ":")).encode())
