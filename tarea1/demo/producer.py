"""Productor de demostración: simula lecturas de 4 estaciones cada 10 minutos.

El tiempo está acelerado (1 s real = 10 min simulados). Tanto `event_time`
como `arrival_time` salen del mismo reloj simulado, así el atraso que muestra
el consumidor tiene sentido. Inyecta:
- una lectura duplicada (mismo event_id) cada 15 pasos, en ITA07;
- una lectura atrasada (llega 3 pasos, o sea 30 min, después de su event_time)
  cada 11 pasos, en ITA23.
"""

from __future__ import annotations

import json
import random
import time
from datetime import UTC, datetime, timedelta

from kafka import KafkaProducer

TOPIC = "ema.lecturas.raw"
STATIONS = ["ITA01", "ITA07", "ITA12", "ITA23"]
STEP = timedelta(minutes=10)


def iso(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def make_event(station: str, event_time: datetime, seq: int) -> dict:
    return {
        "event_id": f"ema-{station}-{event_time:%Y%m%dT%H%M%SZ}",
        "event_type": "ema.lectura",
        "schema_version": 1,
        "station_id": station,
        "cuenca": "parana-alto",
        "event_time": iso(event_time),
        # enlace bueno: llega entre 5 y 20 s después de medir
        "arrival_time": iso(event_time + timedelta(seconds=random.randint(5, 20))),
        "seq": seq,
        "variables": {
            "precip_mm": round(max(0.0, random.gauss(0.6, 1.5)), 1),
            "temp_c": round(random.gauss(25, 3), 1),
            "hr_pct": random.randint(55, 98),
            "presion_hpa": round(random.gauss(1008, 3), 1),
            "viento_ms": round(abs(random.gauss(3, 2)), 1),
        },
        "producer": "demo-producer",
    }


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        acks="all",
        enable_idempotence=True,
        key_serializer=lambda k: k.encode(),
        value_serializer=lambda v: json.dumps(v).encode(),
    )
    sim_time = datetime.now(UTC).replace(second=0, microsecond=0)
    seq = {s: 0 for s in STATIONS}
    delayed: list[tuple[int, dict]] = []
    tick = 0
    print(f"Publicando en {TOPIC} (Ctrl+C para detener)")
    try:
        while True:
            tick += 1
            for station in STATIONS:
                seq[station] += 1
                event = make_event(station, sim_time, seq[station])
                if tick % 11 == 0 and station == "ITA23":
                    delayed.append((tick + 3, event))  # llega tarde
                    continue
                producer.send(TOPIC, key=station, value=event)
                if tick % 15 == 0 and station == "ITA07":
                    producer.send(TOPIC, key=station, value=event)  # duplicado
            for due, event in [d for d in delayed if d[0] <= tick]:
                event["arrival_time"] = iso(sim_time + timedelta(seconds=30))
                producer.send(TOPIC, key=event["station_id"], value=event)
                delayed.remove((due, event))
            producer.flush()
            print(f"tick {tick:4d} · event_time simulado {sim_time:%H:%M}")
            sim_time += STEP
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        producer.close()


if __name__ == "__main__":
    main()
