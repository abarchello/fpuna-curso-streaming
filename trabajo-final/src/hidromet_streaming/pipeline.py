"""Pipeline Apache Beam Kafka → Kafka ejecutado por Flink (PortableRunner)."""

from __future__ import annotations

import argparse
import json
import os

import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka, WriteToKafka, default_io_expansion_service
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.typehints import KV

from hidromet_streaming.config import Settings
from hidromet_streaming.transforms import (
    ParseEvent,
    aggregate_to_kafka_record,
    build_analytics,
    invalid_to_kafka_record,
)


def java_kafka_expansion_service():
    """Ejecutar las etapas Java de KafkaIO como procesos dentro de los TaskManagers."""
    return default_io_expansion_service(
        append_args=[
            "--defaultEnvironmentType=PROCESS",
            '--defaultEnvironmentConfig={"command":"/opt/apache/beam/boot"}',
        ]
    )


def pipeline_options(settings: Settings, *, job_name: str) -> PipelineOptions:
    return PipelineOptions(
        [
            "--runner=PortableRunner",
            f"--job_endpoint={settings.job_endpoint}",
            "--environment_type=PROCESS",
            '--environment_config={"command":"/opt/fpuna-lab/python-sdk/boot"}',
            "--streaming",
            f"--parallelism={settings.parallelism}",
            f"--job_name={job_name}",
            "--experiments=use_sdf_read",
        ]
    )


def build_pipeline(
    pipeline: beam.Pipeline,
    settings: Settings,
    *,
    group_id: str,
    max_num_records: int | None = None,
    max_read_time: int | None = None,
):
    expansion_service = java_kafka_expansion_service()
    read_kwargs = {}
    if max_num_records is not None:
        read_kwargs["max_num_records"] = max_num_records
    if max_read_time is not None:
        read_kwargs["max_read_time"] = max_read_time
    raw = pipeline | "Leer lecturas de Kafka" >> ReadFromKafka(
        consumer_config={
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "true",
        },
        topics=[settings.raw_topic],
        # El productor fija el timestamp Kafka = event_time; el watermark de la
        # fuente se deriva de él.
        timestamp_policy=ReadFromKafka.create_time_policy,
        expansion_service=expansion_service,
        **read_kwargs,
    )
    parsed = raw | "Decodificar y validar" >> beam.ParDo(ParseEvent()).with_outputs(
        ParseEvent.INVALID, main="valid"
    )
    aggregates = build_analytics(parsed.valid, settings)
    (
        aggregates
        | "Codificar agregados"
        >> beam.Map(aggregate_to_kafka_record).with_output_types(KV[bytes, bytes])
        | "Escribir agregados en Kafka"
        >> WriteToKafka(
            producer_config={
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "enable.idempotence": "true",
                "acks": "all",
            },
            topic=settings.aggregates_topic,
            expansion_service=expansion_service,
        )
    )
    (
        parsed.invalid
        | "Codificar inválidos"
        >> beam.Map(invalid_to_kafka_record).with_output_types(KV[bytes, bytes])
        | "Escribir DLQ en Kafka"
        >> WriteToKafka(
            producer_config={"bootstrap.servers": settings.kafka_bootstrap_servers},
            topic=settings.dlq_topic,
            expansion_service=expansion_service,
        )
    )
    return aggregates, parsed.invalid


def run(
    *,
    group_id: str = "beam-hidromet-v1",
    max_num_records: int | None = None,
    max_read_time: int | None = None,
):
    settings = Settings.from_env()
    options = pipeline_options(settings, job_name=os.getenv("BEAM_JOB_NAME", "hidromet-analytics"))
    pipeline = beam.Pipeline(options=options)
    build_pipeline(
        pipeline,
        settings,
        group_id=group_id,
        max_num_records=max_num_records,
        max_read_time=max_read_time,
    )
    result = pipeline.run()
    result.wait_until_finish()
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--group-id", default="beam-hidromet-v1")
    parser.add_argument("--max-num-records", type=int)
    parser.add_argument("--max-read-time", type=int)
    args = parser.parse_args()
    result = run(
        group_id=args.group_id,
        max_num_records=args.max_num_records,
        max_read_time=args.max_read_time,
    )
    print(json.dumps({"state": str(result.state)}))


if __name__ == "__main__":
    main()
