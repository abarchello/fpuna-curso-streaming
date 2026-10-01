"""Reproducir (replay) lecturas históricas de estaciones como un stream en Kafka.

El replay conserva las distancias entre `event_time` de las lecturas, pero las
entrega aceleradas por un factor configurable. Además reproduce las tres
patologías reales de la telemetría:

- **duplicados**: el datalogger reenvía una lectura sin ACK (mismo `event_id`);
- **atraso por enlace**: cada estación tiene un perfil (`fibra`, `gprs`,
  `satelital`) que retiene sus lecturas cierto tiempo lógico antes de
  publicarlas, generando desorden y datos tardíos respecto del watermark;
- **atraso real**: si el dataset trae `arrival_time` (la hora en que la
  estación envió la lectura), esa lectura se publica con su atraso real en
  lugar del simulado;
- **jitter**: variación aleatoria en el instante de publicación.

La clave Kafka es `station_id`: las lecturas de una estación conservan el
orden de publicación dentro de su partición.
"""

from __future__ import annotations

import argparse
import heapq
import json
import random
import signal
import time
from collections.abc import Iterable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event

import pandas as pd
from confluent_kafka import Producer

from hidromet_streaming.config import Settings, processed_dataset_path
from hidromet_streaming.contracts import (
    StationReading,
    encode_event,
    make_event_id,
    parse_utc,
    reading_from_row,
)

# Atraso lógico (segundos) que impone cada tipo de enlace: (mínimo, máximo).
LINK_DELAY_SECONDS: dict[str, tuple[int, int]] = {
    "fibra": (2, 20),
    "gprs": (30, 900),  # hasta 15 min: ráfagas al recuperar cobertura
    "satelital": (600, 2100),  # 10–35 min: supera a veces la lateness de 20 min
}


def load_readings(
    path: Path, *, max_readings: int | None = None, source: str | None = None
) -> tuple[list[StationReading], dict[str, str]]:
    """Cargar el dataset procesado como lecturas ordenadas por tiempo de evento."""
    frame = pd.read_parquet(path).sort_values(["event_time", "station_id"])
    if max_readings is not None:
        frame = frame.head(max_readings)
    links = {}
    if "enlace" in frame:
        links = dict(zip(frame["station_id"], frame["enlace"], strict=False))
    label = source
    if label is None:
        manifest = path.parent / "manifest.json"
        if manifest.exists():
            label = json.loads(manifest.read_text(encoding="utf-8")).get("source")
    label = label or "dataset-procesado"
    readings = [reading_from_row(row, source=label) for row in frame.to_dict(orient="records")]
    return readings, links


def load_arrival_delays(path: Path, *, max_readings: int | None = None) -> dict[str, float]:
    """Atraso real de llegada (segundos) por `event_id`, si el dataset trae `arrival_time`."""
    frame = pd.read_parquet(path).sort_values(["event_time", "station_id"])
    if max_readings is not None:
        frame = frame.head(max_readings)
    if "arrival_time" not in frame:
        return {}
    known = frame[frame["arrival_time"].notna()]
    arrival = pd.to_datetime(known["arrival_time"], utc=True)
    event = pd.to_datetime(known["event_time"], utc=True)
    delays = (arrival - event).dt.total_seconds().clip(lower=0)
    return {
        make_event_id(station, stamp): float(delay)
        for station, stamp, delay in zip(known["station_id"], event, delays, strict=True)
    }


def shift_event_time(
    reading: StationReading, *, source_start: datetime, target_start: datetime
) -> StationReading:
    """Desplazar el tiempo de evento para que el replay empiece 'ahora'."""
    original = parse_utc(reading.event_time)
    shifted = target_start + (original - source_start)
    return replace(reading, event_time=shifted.astimezone(UTC).isoformat().replace("+00:00", "Z"))


class StationReplay:
    """Productor determinista y controlable, apto para notebook o CLI."""

    def __init__(
        self,
        producer: Producer,
        *,
        topic: str,
        speedup: float = 60.0,
        duplicate_rate: float = 0.0,
        jitter_seconds: float = 0.0,
        link_delays: dict[str, str] | None = None,
        arrival_delays: dict[str, float] | None = None,
        simulate_links: bool = True,
        shift_to_now: bool = True,
        seed: int = 7,
    ) -> None:
        if speedup <= 0:
            raise ValueError("speedup debe ser positivo")
        self.producer = producer
        self.topic = topic
        self.speedup = speedup
        self.duplicate_rate = duplicate_rate
        self.jitter_seconds = jitter_seconds
        self.link_delays = link_delays or {}
        self.arrival_delays = arrival_delays or {}
        self.simulate_links = simulate_links
        # Con fechas originales (pasadas) y publicación de una vez, el watermark
        # de KafkaIO sigue al event_time mientras haya backlog y las ventanas
        # cierran en orden: es la forma de ver ON_TIME y LATE en una demo corta.
        self.shift_to_now = shift_to_now
        self.random = random.Random(seed)
        self.stop_event = Event()
        self.sent = 0
        self.duplicates = 0
        self.delayed = 0

    def stop(self) -> None:
        self.stop_event.set()

    def _logical_delay(self, reading: StationReading) -> float:
        if not self.simulate_links:
            return 0.0
        if reading.event_id in self.arrival_delays:
            return self.arrival_delays[reading.event_id]
        low, high = LINK_DELAY_SECONDS.get(
            self.link_delays.get(reading.station_id, "fibra"), (0, 0)
        )
        return float(self.random.uniform(low, high))

    def _produce(self, reading: StationReading) -> None:
        timestamp_ms = int(parse_utc(reading.event_time).timestamp() * 1000)
        self.producer.produce(
            self.topic,
            key=reading.station_id.encode(),
            value=encode_event(reading),
            timestamp=timestamp_ms,
        )
        self.producer.poll(0)
        self.sent += 1

    def schedule(
        self, readings: Iterable[StationReading]
    ) -> list[tuple[float, int, StationReading]]:
        """Calcular el instante lógico de publicación de cada lectura.

        Retorna una lista ordenada por instante de publicación (segundos desde
        el inicio del replay). Como cada enlace impone un atraso distinto, el
        orden de publicación difiere del orden de tiempo de evento: ese es el
        desorden que el pipeline tiene que absorber.
        """
        materialized = list(readings)
        if not materialized:
            return []
        source_start = parse_utc(materialized[0].event_time)
        heap: list[tuple[float, int, StationReading]] = []
        for index, reading in enumerate(materialized):
            offset = (parse_utc(reading.event_time) - source_start).total_seconds()
            delay = self._logical_delay(reading)
            if delay > 60:
                self.delayed += 1
            heapq.heappush(heap, (offset + delay, index, reading))
        return [heapq.heappop(heap) for _ in range(len(heap))]

    def replay(
        self, readings: Iterable[StationReading], *, realtime: bool = True
    ) -> dict[str, int]:
        schedule = self.schedule(readings)
        if not schedule:
            return {"events": 0, "duplicates": 0, "delayed": 0}
        first_event_time = min(parse_utc(item[2].event_time) for item in schedule)
        target_start = datetime.now(UTC)
        wall_start = time.monotonic()
        for publish_offset, _, original in schedule:
            if self.stop_event.is_set():
                break
            shifted = (
                shift_event_time(original, source_start=first_event_time, target_start=target_start)
                if self.shift_to_now
                else original
            )
            jitter = self.random.uniform(0, self.jitter_seconds) if self.jitter_seconds else 0.0
            due = wall_start + publish_offset / self.speedup + jitter
            if realtime:
                while not self.stop_event.is_set() and (remaining := due - time.monotonic()) > 0:
                    time.sleep(min(remaining, 0.1))
            self._produce(shifted)
            if self.random.random() < self.duplicate_rate:
                self._produce(shifted)
                self.duplicates += 1
        self.producer.flush(10)
        return {"events": self.sent, "duplicates": self.duplicates, "delayed": self.delayed}


def build_producer(bootstrap_servers: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "ema-replay-producer",
            "enable.idempotence": True,
            "acks": "all",
            "compression.type": "snappy",
        }
    )


def logical_span(readings: list[StationReading]) -> timedelta:
    if not readings:
        return timedelta(0)
    stamps = [parse_utc(r.event_time) for r in readings]
    return max(stamps) - min(stamps)


def main() -> None:
    settings = Settings.from_env()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", type=Path, default=processed_dataset_path())
    parser.add_argument("--max-readings", type=int)
    parser.add_argument("--speedup", type=float, default=60)
    parser.add_argument("--duplicate-rate", type=float, default=0.02)
    parser.add_argument("--jitter-seconds", type=float, default=0)
    parser.add_argument("--no-link-delays", action="store_true", help="publicar sin atrasos")
    parser.add_argument(
        "--simulated-delays",
        action="store_true",
        help="usar el atraso simulado por enlace aunque el dataset traiga arrival_time",
    )
    parser.add_argument(
        "--no-realtime", action="store_true", help="publicar todo de una vez, sin esperar"
    )
    parser.add_argument(
        "--keep-event-time",
        action="store_true",
        help="conservar las fechas originales en lugar de desplazar la primera a ahora",
    )
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()
    readings, links = load_readings(args.dataset, max_readings=args.max_readings)
    arrival_delays = (
        {}
        if args.simulated_delays
        else load_arrival_delays(args.dataset, max_readings=args.max_readings)
    )
    replay = StationReplay(
        build_producer(settings.kafka_bootstrap_servers),
        topic=settings.raw_topic,
        speedup=args.speedup,
        duplicate_rate=args.duplicate_rate,
        jitter_seconds=args.jitter_seconds,
        link_delays=links,
        arrival_delays=arrival_delays,
        simulate_links=not args.no_link_delays,
        shift_to_now=not args.keep_event_time,
        seed=args.seed,
    )
    signal.signal(signal.SIGTERM, lambda *_: replay.stop())
    signal.signal(signal.SIGINT, lambda *_: replay.stop())
    print(
        json.dumps(
            {
                "readings": len(readings),
                "real_arrival_delays": len(arrival_delays),
                "logical_span_hours": logical_span(readings).total_seconds() / 3600,
            }
        )
    )
    result = replay.replay(readings, realtime=not args.no_realtime)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
