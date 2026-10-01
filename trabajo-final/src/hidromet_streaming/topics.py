"""Inspeccionar los tres tópicos y resumir lo que pasó, sólo con conteos.

Lee los tópicos de punta a punta con un consumidor propio (grupo nuevo, sin
confirmar offsets) y muestra lo que hace falta para verificar una corrida:
cuántas lecturas entraron, cuántas llegaron fuera de orden, cuántos
`event_id` se repitieron, qué panes salieron y qué quedó en la DLQ.

    python -m hidromet_streaming.topics
    python -m hidromet_streaming.topics --dlq     # los mensajes rechazados, con su motivo
    docker compose exec analytics-notebook python -m hidromet_streaming.topics
"""

from __future__ import annotations

import argparse
import json
import uuid
from collections import Counter
from typing import Any

from confluent_kafka import Consumer, TopicPartition

from hidromet_streaming.config import Settings


def read_topic(settings: Settings, topic: str) -> list[tuple[int, int, bytes]]:
    """Todos los mensajes del tópico como (partición, timestamp_ms, valor), en orden de offset."""
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": f"hidromet-inspeccion-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    metadata = consumer.list_topics(topic, timeout=10).topics[topic]
    messages: list[tuple[int, int, bytes]] = []
    for partition in sorted(metadata.partitions):
        handle = TopicPartition(topic, partition, 0)
        consumer.assign([handle])
        _low, high = consumer.get_watermark_offsets(handle, timeout=10)
        read = 0
        while read < high:
            message = consumer.poll(5)
            if message is None:
                break
            if message.error():
                continue
            messages.append((partition, message.timestamp()[1], message.value()))
            read += 1
    consumer.close()
    return messages


def summarize_input(messages: list[tuple[int, int, bytes]]) -> dict[str, Any]:
    """Desorden por partición (timestamp menor al del mensaje previo) y `event_id` repetidos."""
    by_partition: dict[int, dict[str, int]] = {}
    previous: dict[int, int] = {}
    ids: Counter[str] = Counter()
    for partition, timestamp, value in messages:
        stats = by_partition.setdefault(partition, {"mensajes": 0, "fuera_de_orden": 0})
        stats["mensajes"] += 1
        if partition in previous and timestamp < previous[partition]:
            stats["fuera_de_orden"] += 1
        previous[partition] = timestamp
        try:
            ids[str(json.loads(value).get("event_id"))] += 1
        except (ValueError, AttributeError):
            ids["<no es JSON>"] += 1
    return {
        "mensajes": len(messages),
        "por_particion": {str(p): by_partition[p] for p in sorted(by_partition)},
        "event_id_repetidos": sum(1 for count in ids.values() if count > 1),
    }


def summarize_output(messages: list[tuple[int, int, bytes]]) -> dict[str, Any]:
    """Panes por tipo y momento, ventanas con más de una lectura y alertas."""
    aggregates = [json.loads(value) for _, _, value in messages]
    panes = Counter(f"{a['metric_type']}/{a['pane_timing']}" for a in aggregates)
    latest: dict[str, dict[str, Any]] = {}
    for aggregate in aggregates:
        if aggregate["metric_type"] != "estacion_10min":
            continue
        current = latest.get(aggregate["aggregate_id"])
        if current is None or aggregate["pane_index"] >= current["pane_index"]:
            latest[aggregate["aggregate_id"]] = aggregate
    alerts = {a["aggregate_id"]: a for a in aggregates if a["metric_type"] == "alerta"}
    return {
        "mensajes": len(aggregates),
        "claves_distintas": len({a["aggregate_id"] for a in aggregates}),
        "panes": dict(sorted(panes.items())),
        "ventanas_de_estacion": len(latest),
        "ventanas_con_mas_de_una_lectura": sum(1 for a in latest.values() if a["n_lecturas"] > 1),
        "panes_vacios": sum(1 for a in aggregates if a.get("n_lecturas") == 0),
        "alertas": dict(Counter(a.get("tipo") for a in alerts.values())),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dlq", action="store_true", help="mostrar los mensajes de la DLQ")
    args = parser.parse_args()
    settings = Settings.from_env()
    dlq = read_topic(settings, settings.dlq_topic)
    if args.dlq:
        for _, _, value in dlq:
            print(json.dumps(json.loads(value), indent=2, ensure_ascii=False))
        print(f"{len(dlq)} mensaje(s) en {settings.dlq_topic}")
        return
    summary = {
        settings.raw_topic: summarize_input(read_topic(settings, settings.raw_topic)),
        settings.aggregates_topic: summarize_output(
            read_topic(settings, settings.aggregates_topic)
        ),
        settings.dlq_topic: {
            "mensajes": len(dlq),
            "motivos": [json.loads(value).get("error") for _, _, value in dlq][-3:],
        },
    }
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
