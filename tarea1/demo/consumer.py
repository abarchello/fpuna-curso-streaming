"""Consumidor de demostración: imprime cada lectura con su partición y atraso."""

from __future__ import annotations

import json
from datetime import datetime

from kafka import KafkaConsumer

TOPIC = "ema.lecturas.raw"


def parse(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def main() -> None:
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers="localhost:9092",
        group_id="demo-consumer",
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode()),
    )
    seen: set[str] = set()
    print(f"Leyendo {TOPIC} (Ctrl+C para detener)")
    for msg in consumer:
        e = msg.value
        delay = (parse(e["arrival_time"]) - parse(e["event_time"])).total_seconds()
        flag = "DUPLICADO" if e["event_id"] in seen else ""
        seen.add(e["event_id"])
        print(
            f"p{msg.partition:02d} off={msg.offset:<6} {e['station_id']} seq={e['seq']:<5}"
            f" event={e['event_time']} atraso={delay:>7.0f}s"
            f" precip={e['variables']['precip_mm']:>5} {flag}"
        )


if __name__ == "__main__":
    main()
