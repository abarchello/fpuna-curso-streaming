"""Pruebas en modo streaming con TestStream.

Complementan la suite provista. `TestStream` me deja mover el watermark a
mano, así que puedo mostrar cosas que un pipeline batch no deja ver:

1. un evento que llega después del cierre de la ventana, pero dentro de
   `allowed_lateness`, produce un pane LATE que revisa el total;
2. un evento que llega después de `window_end + allowed_lateness` se
   descarta y no produce pane;
3. un duplicado dentro del mismo comercio no cambia el total;
4. un evento que llega desordenado suma en la ventana de su `event_time`;
5. el mismo `event_id` en dos comercios cuenta dos veces (estado aislado
   por clave);
6. el timer de expiración borra el estado de deduplicación cuando el
   watermark lo alcanza.
"""

from __future__ import annotations

import apache_beam as beam
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream as BeamTestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue

WINDOW_SECONDS = 60
LATENESS_SECONDS = 120
T0 = 1_800_000_000  # inicio de una ventana de 60 s alineada al epoch


def _event(event_id: str, merchant_id: str, seconds_from_t0: int, amount: int):
    return TimestampedValue(
        {"event_id": event_id, "merchant_id": merchant_id, "amount": amount},
        T0 + seconds_from_t0,
    )


def _with_pane(keyed_total, pane=beam.DoFn.PaneInfoParam):
    merchant_id, total = keyed_total
    timing = {0: "EARLY", 1: "ON_TIME", 2: "LATE", 3: "UNKNOWN"}[pane.timing]
    return (merchant_id, total, timing)


def _streaming_pipeline() -> BeamTestPipeline:
    options = StandardOptions(streaming=True)
    return BeamTestPipeline(options=options)


def test_late_event_within_lateness_produces_late_pane(solution):
    stream = (
        BeamTestStream()
        .advance_watermark_to(T0)
        .add_elements([_event("p-1", "m-a", 5, 10), _event("p-2", "m-a", 42, 20)])
        # El watermark cruza el fin de la ventana: se dispara el pane ON_TIME.
        .advance_watermark_to(T0 + WINDOW_SECONDS + 1)
        # Llega un dato con event_time dentro de la ventana ya cerrada, pero
        # antes de window_end + allowed_lateness: pane LATE (revisión).
        .add_elements([_event("p-3", "m-a", 30, 5)])
        .advance_watermark_to_infinity()
    )

    with _streaming_pipeline() as pipeline:
        output = (
            pipeline
            | stream
            | solution.build_trigger_policy(
                window_seconds=WINDOW_SECONDS,
                allowed_lateness_seconds=LATENESS_SECONDS,
            )
            | beam.Map(lambda e: (e["merchant_id"], e["amount"]))
            | beam.CombinePerKey(sum)
            | beam.Map(_with_pane)
        )
        # ACCUMULATING: el pane late trae el total completo (30 + 5), no el delta.
        assert_that(output, equal_to([("m-a", 30, "ON_TIME"), ("m-a", 35, "LATE")]))


def test_event_beyond_lateness_is_dropped(solution):
    stream = (
        BeamTestStream()
        .advance_watermark_to(T0)
        .add_elements([_event("p-1", "m-a", 5, 10)])
        # El watermark supera window_end + allowed_lateness: la ventana expira.
        .advance_watermark_to(T0 + WINDOW_SECONDS + LATENESS_SECONDS + 1)
        .add_elements([_event("p-9", "m-a", 50, 999)])
        .advance_watermark_to_infinity()
    )

    with _streaming_pipeline() as pipeline:
        output = (
            pipeline
            | stream
            | solution.build_trigger_policy(
                window_seconds=WINDOW_SECONDS,
                allowed_lateness_seconds=LATENESS_SECONDS,
            )
            | beam.Map(lambda e: (e["merchant_id"], e["amount"]))
            | beam.CombinePerKey(sum)
            | beam.Map(_with_pane)
        )
        assert_that(output, equal_to([("m-a", 10, "ON_TIME")]))


def test_stateful_dedup_in_streaming_ignores_duplicate(solution):
    stream = (
        BeamTestStream()
        .advance_watermark_to(T0)
        .add_elements([_event("p-1", "m-a", 5, 10)])
        .add_elements([_event("p-1", "m-a", 5, 10)])  # duplicado exacto
        .advance_watermark_to(T0 + WINDOW_SECONDS + 1)
        .advance_watermark_to_infinity()
    )

    with _streaming_pipeline() as pipeline:
        output = (
            pipeline
            | stream
            | beam.WindowInto(beam.window.FixedWindows(WINDOW_SECONDS))
            | beam.Map(lambda e: (e["merchant_id"], e))
            | beam.ParDo(solution.DeduplicatePayments(LATENESS_SECONDS))
            | beam.Map(lambda kv: (kv[0], kv[1]["amount"]))
            | beam.CombinePerKey(sum)
        )
        assert_that(output, equal_to([("m-a", 10)]))


def _with_window(keyed_total, window=beam.DoFn.WindowParam):
    merchant_id, total = keyed_total
    return (merchant_id, int(window.start) - T0, total)


def test_out_of_order_event_in_streaming_lands_in_its_event_time_window(solution):
    stream = (
        BeamTestStream()
        .advance_watermark_to(T0)
        # Primero llega un pago de la segunda ventana [T0+60, T0+120)...
        .add_elements([_event("p-2", "m-a", 70, 20)])
        # ...y después uno de la primera ventana [T0, T0+60). Llega desordenado,
        # pero el watermark todavía no cerró su ventana.
        .add_elements([_event("p-1", "m-a", 10, 10)])
        .advance_watermark_to_infinity()
    )

    with _streaming_pipeline() as pipeline:
        output = (
            pipeline
            | stream
            | solution.build_trigger_policy(
                window_seconds=WINDOW_SECONDS,
                allowed_lateness_seconds=LATENESS_SECONDS,
            )
            | beam.Map(lambda e: (e["merchant_id"], e["amount"]))
            | beam.CombinePerKey(sum)
            | beam.Map(_with_window)
        )
        # Cada pago suma en la ventana de su event_time, no en la de llegada.
        assert_that(output, equal_to([("m-a", 0, 10), ("m-a", 60, 20)]))


def test_stateful_dedup_in_streaming_keeps_merchants_isolated(solution):
    stream = (
        BeamTestStream()
        .advance_watermark_to(T0)
        # El mismo event_id en dos comercios distintos: son dos pagos distintos.
        .add_elements([_event("p-1", "m-a", 5, 10), _event("p-1", "m-b", 6, 7)])
        # El reenvío de p-1 en m-a sí es un duplicado.
        .add_elements([_event("p-1", "m-a", 5, 10)])
        .advance_watermark_to_infinity()
    )

    with _streaming_pipeline() as pipeline:
        output = (
            pipeline
            | stream
            | beam.WindowInto(beam.window.FixedWindows(WINDOW_SECONDS))
            | beam.Map(lambda e: (e["merchant_id"], e))
            | beam.ParDo(solution.DeduplicatePayments(LATENESS_SECONDS))
            | beam.Map(lambda kv: (kv[0], kv[1]["amount"]))
            | beam.CombinePerKey(sum)
        )
        assert_that(output, equal_to([("m-a", 10), ("m-b", 7)]))


def test_expiry_timer_clears_dedup_state_in_streaming(solution):
    """El timer de expiración borra el estado cuando el watermark lo alcanza.

    Para hacerlo visible, la ventana acepta datos tardíos durante 300 s pero
    el DoFn expira su estado a los 0 s. Después del timer, el mismo event_id
    vuelve a pasar: por eso en el notebook el timer se arma en
    `window_end + allowed_lateness` y no antes.
    """
    stream = (
        BeamTestStream()
        .advance_watermark_to(T0)
        .add_elements([_event("p-1", "m-a", 5, 10)])
        # El watermark pasa window_end: vence el timer y se limpia seen_ids.
        .advance_watermark_to(T0 + WINDOW_SECONDS + 1)
        .add_elements([_event("p-1", "m-a", 5, 10)])
        .advance_watermark_to_infinity()
    )

    with _streaming_pipeline() as pipeline:
        output = (
            pipeline
            | stream
            | beam.WindowInto(
                beam.window.FixedWindows(WINDOW_SECONDS),
                allowed_lateness=300,
            )
            | beam.Map(lambda e: (e["merchant_id"], e))
            | beam.ParDo(solution.DeduplicatePayments(allowed_lateness_seconds=0))
            | beam.Map(lambda kv: kv[1]["event_id"])
        )
        assert_that(output, equal_to(["p-1", "p-1"]))
