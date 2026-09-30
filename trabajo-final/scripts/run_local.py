"""Ejecutar la analítica completa en local (DirectRunner, batch), sin Kafka ni Flink.

Sirve para validar el pipeline y producir evidencia (agregados, alertas) con la
misma lógica de `transforms.build_analytics` que corre en Flink. En batch el
watermark salta a +inf, por lo que no hay descarte por lateness: es el
"resultado de referencia" contra el cual comparar la ejecución en streaming.

    uv run python scripts/run_local.py --max-readings 3000
"""

from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path

import apache_beam as beam
import pandas as pd
from apache_beam.options.pipeline_options import PipelineOptions

from hidromet_streaming.config import Settings, processed_dataset_path, project_root
from hidromet_streaming.contracts import encode_event
from hidromet_streaming.producer import StationReplay, load_arrival_delays, load_readings
from hidromet_streaming.transforms import ParseEvent, build_analytics


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", type=Path, default=processed_dataset_path())
    parser.add_argument("--max-readings", type=int)
    parser.add_argument("--duplicate-rate", type=float, default=0.02)
    parser.add_argument(
        "--simulated-delays",
        action="store_true",
        help="usar el atraso simulado por enlace aunque el dataset traiga arrival_time",
    )
    parser.add_argument(
        "--output", type=Path, default=project_root() / "data/processed/agregados_local.parquet"
    )
    args = parser.parse_args()

    settings = Settings.from_env()
    readings, links = load_readings(args.dataset, max_readings=args.max_readings)
    arrival_delays = (
        {}
        if args.simulated_delays
        else load_arrival_delays(args.dataset, max_readings=args.max_readings)
    )

    # Reutilizamos el planificador del productor para obtener el orden de
    # publicación (con atrasos por enlace) y los duplicados, sin Kafka.
    class _Collector:
        def __init__(self) -> None:
            self.records: list[tuple[bytes, bytes]] = []

        def produce(self, _topic, key, value, timestamp):  # firma de confluent_kafka.Producer
            self.records.append((key, value))

        def poll(self, _timeout):
            return 0

        def flush(self, _timeout):
            return 0

    collector = _Collector()
    replay = StationReplay(
        collector,
        topic="local",
        speedup=1e9,
        duplicate_rate=args.duplicate_rate,
        link_delays=links,
        arrival_delays=arrival_delays,
        seed=7,
    )
    # Orden de publicación con atrasos por enlace, conservando las fechas
    # originales (no se desplazan a "ahora" como en el replay real).
    for _, _, reading in replay.schedule(readings):
        collector.records.append((reading.station_id.encode(), encode_event(reading)))
        if replay.random.random() < args.duplicate_rate:
            collector.records.append((reading.station_id.encode(), encode_event(reading)))
            replay.duplicates += 1
    stats = {
        "events": len(collector.records),
        "duplicates": replay.duplicates,
        "delayed": replay.delayed,
        "real_arrival_delays": len(arrival_delays),
    }
    # Un evento corrupto para ejercitar la DLQ.
    collector.records.append((b"ITA99", b'{"event_id": "roto", "event_type": "ema.lectura"}'))
    collector.records.append((b"ITA01", encode_event({**readings[0].as_dict(), "temp_c": 99.0})))

    with tempfile.TemporaryDirectory() as tmp:
        prefix = str(Path(tmp) / "agg")
        dlq_prefix = str(Path(tmp) / "dlq")
        with beam.Pipeline(options=PipelineOptions(["--runner=DirectRunner"])) as pipeline:
            parsed = (
                pipeline
                | beam.Create(collector.records)
                | beam.ParDo(ParseEvent()).with_outputs(ParseEvent.INVALID, main="valid")
            )
            (
                build_analytics(parsed.valid, settings, streaming_triggers=False)
                | "solo pane final" >> beam.Filter(lambda r: r["is_last"])
                | "json" >> beam.Map(json.dumps)
                | "escribir agregados" >> beam.io.WriteToText(prefix, shard_name_template="")
            )
            (
                parsed.invalid
                | "json dlq" >> beam.Map(json.dumps)
                | "escribir dlq" >> beam.io.WriteToText(dlq_prefix, shard_name_template="")
            )
        rows = [json.loads(line) for line in Path(prefix).read_text().splitlines() if line.strip()]
        dlq = [
            json.loads(line) for line in Path(dlq_prefix).read_text().splitlines() if line.strip()
        ]

    frame = pd.DataFrame(rows)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(args.output, index=False)
    summary = {
        "readings": len(readings),
        "published": stats,
        "dlq_records": len(dlq),
        "aggregates": int(len(frame)),
        "by_metric_type": frame["metric_type"].value_counts().to_dict() if not frame.empty else {},
        "alerts": (
            frame[frame["metric_type"] == "alerta"][
                ["dimension_id", "window_start", "tipo", "valor"]
            ]
            .head(10)
            .to_dict(orient="records")
            if not frame.empty and (frame["metric_type"] == "alerta").any()
            else []
        ),
        "output": str(args.output),
    }
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
