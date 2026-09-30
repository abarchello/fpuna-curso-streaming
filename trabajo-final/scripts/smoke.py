"""Prueba de humo acotada en Docker: Kafka -> Beam/Flink -> Kafka."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import uuid

from confluent_kafka.admin import AdminClient, NewTopic

from hidromet_streaming.config import Settings, processed_dataset_path
from hidromet_streaming.consumer import AggregateStore, build_consumer, poll_into_store
from hidromet_streaming.producer import StationReplay, build_producer, load_readings


def main() -> None:
    suffix = uuid.uuid4().hex[:8]
    raw_topic = f"ema.smoke.events.{suffix}"
    aggregate_topic = f"ema.smoke.aggregates.{suffix}"
    os.environ["KAFKA_RAW_TOPIC"] = raw_topic
    os.environ["KAFKA_AGGREGATES_TOPIC"] = aggregate_topic
    settings = Settings.from_env()
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    futures = admin.create_topics([NewTopic(raw_topic, 4, 1), NewTopic(aggregate_topic, 4, 1)])
    for future in futures.values():
        future.result(timeout=20)

    readings, links = load_readings(processed_dataset_path(), max_readings=120)
    replay = StationReplay(
        build_producer(settings.kafka_bootstrap_servers),
        topic=raw_topic,
        speedup=10_000,
        duplicate_rate=0.05,
        link_delays=links,
    )
    produced = replay.replay(readings, realtime=False)
    if produced["events"] < 120:
        raise RuntimeError(f"se esperaban al menos 120 eventos, se produjeron {produced}")

    subprocess.run(
        [
            sys.executable,
            "-m",
            "hidromet_streaming.pipeline",
            "--group-id",
            f"smoke-beam-{suffix}",
            "--max-num-records",
            str(produced["events"]),
            "--max-read-time",
            "30",
        ],
        check=True,
        timeout=300,
        env=os.environ,
    )

    consumer = build_consumer(settings, group_id=f"smoke-dashboard-{suffix}")
    store = AggregateStore()
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline and not store.records:
        poll_into_store(consumer, store, max_messages=1000, timeout_seconds=1)
    consumer.close()
    if not store.records:
        raise RuntimeError("the aggregate topic remained empty")
    print(json.dumps({"produced": produced, "consumed": store.summary()}, indent=2))


if __name__ == "__main__":
    main()
