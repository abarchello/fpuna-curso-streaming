import apache_beam as beam
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream as BeamTestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue

from hidromet_streaming.config import Settings
from hidromet_streaming.transforms import (
    DeduplicateReadings,
    FormatAggregate,
    ParseEvent,
    StationStatsCombineFn,
    build_analytics,
    detect_alerts,
)


def reading(
    station: str,
    event_time: str,
    *,
    precip: float,
    temp: float = 25.0,
    gust: float = 5.0,
    subcuenca: str = "monday",
) -> dict:
    return {
        "event_id": f"ema-{station}-{event_time}",
        "event_type": "ema.lectura",
        "event_time": event_time,
        "station_id": station,
        "station_name": station,
        "subcuenca": subcuenca,
        "precip_mm": precip,
        "temp_c": temp,
        "hr_pct": 70,
        "presion_hpa": 1005.0,
        "viento_ms": 3.0,
        "viento_dir_deg": 120,
        "rafaga_ms": gust,
        "radiacion_wm2": 300,
    }


def test_combine_fn_tracks_sums_and_extremes():
    fn = StationStatsCombineFn()
    acc = fn.create_accumulator()
    acc = fn.add_input(acc, reading("A", "2023-10-28T14:00:00Z", precip=1.0, temp=20.0, gust=4.0))
    acc = fn.add_input(acc, reading("A", "2023-10-28T14:10:00Z", precip=2.5, temp=24.0, gust=9.0))
    out = fn.extract_output(fn.merge_accumulators([acc, fn.create_accumulator()]))
    assert out["n_lecturas"] == 2
    assert out["precip_mm"] == 3.5
    assert out["temp_media_c"] == 22.0
    assert out["temp_min_c"] == 20.0 and out["temp_max_c"] == 24.0
    assert out["rafaga_max_ms"] == 9.0


def test_batch_pipeline_deduplicates_and_aggregates_by_station_and_basin():
    settings = Settings(
        window_seconds=600, hourly_window_seconds=3600, allowed_lateness_seconds=1200
    )
    repeated = reading("ITA01", "2023-10-28T14:02:00Z", precip=4.0)
    events = [
        repeated,
        repeated,  # duplicado exacto: no debe sumar
        reading("ITA01", "2023-10-28T14:05:00Z", precip=6.0, gust=22.0),
        reading("ITA02", "2023-10-28T14:03:00Z", precip=2.0),
        reading("ITA02", "2023-10-28T14:15:00Z", precip=1.0),  # otra ventana de 10 min
    ]
    with BeamTestPipeline() as pipeline:
        # En batch, ACCUMULATING + allowed_lateness > 0 emite dos panes por
        # ventana (on-time y el de cierre, con is_last=True). Se verifica el
        # pane final: es el que un consumidor usaría para informes cerrados.
        output = build_analytics(
            pipeline | beam.Create(events), settings, streaming_triggers=False
        ) | "pane final" >> beam.Filter(lambda r: r["is_last"])
        station_rows = (
            output
            | "solo estación" >> beam.Filter(lambda r: r["metric_type"] == "estacion_10min")
            | "proyectar estación"
            >> beam.Map(
                lambda r: (r["dimension_id"], r["window_start"], r["n_lecturas"], r["precip_mm"])
            )
        )
        basin_rows = (
            output
            | "solo cuenca" >> beam.Filter(lambda r: r["metric_type"] == "subcuenca_1h")
            | "proyectar cuenca"
            >> beam.Map(
                lambda r: (
                    r["dimension_id"],
                    r["n_estaciones"],
                    r["precip_media_mm"],
                    r["precip_max_mm"],
                )
            )
        )
        alert_rows = (
            output
            | "solo alertas" >> beam.Filter(lambda r: r["metric_type"] == "alerta")
            | "proyectar alertas" >> beam.Map(lambda r: (r["dimension_id"], r["tipo"], r["valor"]))
        )
        assert_that(
            station_rows,
            equal_to(
                [
                    ("ITA01", "2023-10-28T14:00:00Z", 2, 10.0),
                    ("ITA02", "2023-10-28T14:00:00Z", 1, 2.0),
                    ("ITA02", "2023-10-28T14:10:00Z", 1, 1.0),
                ]
            ),
            label="estaciones",
        )
        # media areal: ITA01 = 10, ITA02 = 3 -> 6.5; máximo 10
        assert_that(basin_rows, equal_to([("monday", 2, 6.5, 10.0)]), label="cuenca")
        assert_that(
            alert_rows,
            equal_to([("ITA01", "lluvia_intensa", 10.0), ("ITA01", "rafaga_fuerte", 22.0)]),
            label="alertas",
        )


def test_parse_event_routes_invalid_payloads_to_dlq():
    with BeamTestPipeline() as pipeline:
        parsed = (
            pipeline
            | beam.Create([(b"ITA01", b"{no es json"), (b"ITA01", b'{"event_id": "x"}')])
            | beam.ParDo(ParseEvent()).with_outputs(ParseEvent.INVALID, main="valid")
        )
        assert_that(parsed.valid, equal_to([]), label="valid")
        assert_that(
            parsed.invalid | beam.Map(lambda r: "error" in r),
            equal_to([True, True]),
            label="invalid",
        )


def test_detect_alerts_uses_thresholds():
    settings = Settings(alert_precip_10min_mm=10.0, alert_gust_ms=20.0)
    base = {
        "schema_version": 1,
        "window_start": "w0",
        "window_end": "w1",
        "dimension_id": "ITA01",
        "dimension_name": "Hernandarias",
        "subcuenca": "monday",
        "pane_index": 0,
        "pane_timing": "ON_TIME",
        "is_first": True,
        "is_last": False,
        "precip_mm": 12.0,
        "rafaga_max_ms": 8.0,
        "n_lecturas": 1,
    }
    alerts = list(detect_alerts(base, settings))
    assert [a["tipo"] for a in alerts] == ["lluvia_intensa"]
    assert alerts[0]["aggregate_id"] == "alerta|w0|ITA01|lluvia_intensa"
    assert list(detect_alerts({**base, "precip_mm": 0.0}, settings)) == []


T0 = 1_700_000_400  # múltiplo de 600 s: inicio de una ventana de 10 min


def _ts(station: str, offset: int, precip: float) -> TimestampedValue:
    return TimestampedValue(
        {"station_id": station, "event_id": f"{station}-{offset}", "precip_mm": precip},
        T0 + offset,
    )


def test_streaming_late_reading_within_lateness_revises_the_window():
    """Con TestStream: pane ON_TIME y luego un pane LATE acumulado."""
    stream = (
        BeamTestStream()
        .advance_watermark_to(T0)
        .add_elements([_ts("ITA01", 60, 1.0), _ts("ITA01", 300, 2.0)])
        .advance_watermark_to(T0 + 601)  # cierra la ventana: ON_TIME
        .add_elements([_ts("ITA01", 480, 4.0)])  # llega tarde, dentro de 20 min
        .advance_watermark_to_infinity()
    )
    options = StandardOptions(streaming=True)
    with BeamTestPipeline(options=options) as pipeline:
        output = (
            pipeline
            | stream
            | beam.WindowInto(
                beam.window.FixedWindows(600),
                trigger=beam.transforms.trigger.AfterWatermark(
                    late=beam.transforms.trigger.AfterCount(1)
                ),
                accumulation_mode=beam.transforms.trigger.AccumulationMode.ACCUMULATING,
                allowed_lateness=1200,
            )
            | beam.Map(lambda e: (e["station_id"], e))
            | beam.ParDo(DeduplicateReadings(1200))
            | beam.Map(lambda kv: (kv[0], kv[1]["precip_mm"]))
            | beam.CombinePerKey(sum)
        )
        assert_that(output, equal_to([("ITA01", 3.0), ("ITA01", 7.0)]))


def test_streaming_duplicate_is_ignored_by_stateful_dedup():
    stream = (
        BeamTestStream()
        .advance_watermark_to(T0)
        .add_elements([_ts("ITA01", 60, 1.0)])
        .add_elements([_ts("ITA01", 60, 1.0)])  # mismo event_id
        .advance_watermark_to_infinity()
    )
    options = StandardOptions(streaming=True)
    with BeamTestPipeline(options=options) as pipeline:
        output = (
            pipeline
            | stream
            | beam.WindowInto(beam.window.FixedWindows(600))
            | beam.Map(lambda e: (e["station_id"], e))
            | beam.ParDo(DeduplicateReadings(1200))
            | beam.Map(lambda kv: (kv[0], kv[1]["precip_mm"]))
            | beam.CombinePerKey(sum)
        )
        assert_that(output, equal_to([("ITA01", 1.0)]))


DIMENSION = ("ITA01", "Hernandarias", "monday")


def test_format_aggregate_labels_panes_by_name():
    """El contrato de salida publica EARLY/ON_TIME/LATE, no el número interno."""
    stream = (
        BeamTestStream()
        .advance_watermark_to(T0)
        .add_elements([_ts("ITA01", 60, 1.0)])
        .advance_watermark_to(T0 + 601)  # pane ON_TIME
        .add_elements([_ts("ITA01", 480, 4.0)])  # pane LATE
        .advance_watermark_to_infinity()
    )
    options = StandardOptions(streaming=True)
    with BeamTestPipeline(options=options) as pipeline:
        output = (
            pipeline
            | stream
            | beam.WindowInto(
                beam.window.FixedWindows(600),
                trigger=beam.transforms.trigger.AfterWatermark(
                    late=beam.transforms.trigger.AfterCount(1)
                ),
                accumulation_mode=beam.transforms.trigger.AccumulationMode.ACCUMULATING,
                allowed_lateness=1200,
            )
            | beam.Map(lambda e: (DIMENSION, {"precip_mm": e["precip_mm"]}))
            | beam.CombinePerKey(lambda values: {"precip_mm": sum(v["precip_mm"] for v in values)})
            | beam.ParDo(FormatAggregate("precip_10min"))
            | beam.Map(lambda row: (row["pane_timing"], row["pane_index"], row["precip_mm"]))
        )
        assert_that(output, equal_to([("ON_TIME", 0, 1.0), ("LATE", 1, 5.0)]))


def test_streaming_reading_beyond_lateness_is_dropped():
    """Una lectura que llega después de fin de ventana + 20 min no cambia el total."""
    stream = (
        BeamTestStream()
        .advance_watermark_to(T0)
        .add_elements([_ts("ITA23", 60, 1.0)])
        # fin de la ventana (T0 + 600) más 1200 s de lateness: la ventana expira
        .advance_watermark_to(T0 + 600 + 1200 + 1)
        .add_elements([_ts("ITA23", 120, 3.8)])  # como ITA23-1420 en la Tarea 2
        .advance_watermark_to_infinity()
    )
    options = StandardOptions(streaming=True)
    with BeamTestPipeline(options=options) as pipeline:
        output = (
            pipeline
            | stream
            | beam.WindowInto(
                beam.window.FixedWindows(600),
                trigger=beam.transforms.trigger.AfterWatermark(
                    late=beam.transforms.trigger.AfterCount(1)
                ),
                accumulation_mode=beam.transforms.trigger.AccumulationMode.ACCUMULATING,
                allowed_lateness=1200,
            )
            | beam.Map(lambda e: (e["station_id"], e))
            | beam.ParDo(DeduplicateReadings(1200))
            | beam.Map(lambda kv: (kv[0], kv[1]["precip_mm"]))
            | beam.CombinePerKey(sum)
        )
        assert_that(output, equal_to([("ITA23", 1.0)]))
