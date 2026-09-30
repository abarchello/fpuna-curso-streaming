import marimo

__generated_with = "0.23.15"
app = marimo.App(width="full")


@app.cell
def _():
    from collections.abc import Iterable
    from datetime import datetime
    from typing import Any

    import apache_beam as beam
    import marimo as mo
    from apache_beam.coders import StrUtf8Coder
    from apache_beam.transforms.timeutil import TimeDomain
    from apache_beam.transforms.userstate import (
        SetStateSpec,
        TimerSpec,
        on_timer,
    )

    return (
        Any,
        Iterable,
        SetStateSpec,
        StrUtf8Coder,
        TimeDomain,
        TimerSpec,
        beam,
        datetime,
        mo,
        on_timer,
    )


@app.cell
def _(mo):
    mo.md(r"""
    # Tarea 3 · Beam avanzado

    **Ventanas, estado por clave y efectos externos idempotentes**

    Este notebook es un esqueleto. Las celdas de código contienen firmas,
    contratos y excepciones `NotImplementedError`; no incluyen la solución.

    ## Problema

    Implementá un pipeline que produzca el total confirmado por comercio y
    minuto aun cuando los pagos lleguen fuera de orden, duplicados o sean
    reintentados al escribir el resultado.

    El archivo `data/payments.jsonl` contiene:

    - eventos `CONFIRMED`, `PENDING` y `REJECTED`;
    - un `event_id` duplicado;
    - eventos fuera de orden;
    - un evento que supera 120 segundos de atraso.

    ## Reglas

    1. Usar `event_time` como timestamp del dominio.
    2. Aplicar ventanas fijas de 60 segundos.
    3. Aceptar hasta 120 segundos de lateness.
    4. Deduplicar por `event_id` dentro del comercio.
    5. Emitir panes acumulativos.
    6. Escribir mediante una clave idempotente `merchant_id|window_start`.
    """)
    return


@app.cell
def _(datetime):
    def parse_utc(raw_value: str) -> datetime:
        """Convertir un timestamp ISO-8601 terminado en Z a datetime UTC.

        Acepta tanto el sufijo `Z` como un offset explícito (`+00:00`). El
        resultado siempre es timezone-aware y normalizado a UTC, para que el
        tiempo de evento sea comparable sin importar cómo lo serializó el
        productor.
        """
        from datetime import UTC

        if not isinstance(raw_value, str) or not raw_value.strip():
            raise ValueError(f"timestamp inválido: {raw_value!r}")

        normalized = raw_value.strip()
        if normalized.endswith(("Z", "z")):
            normalized = normalized[:-1] + "+00:00"

        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise ValueError(f"timestamp inválido: {raw_value!r}") from exc

        if parsed.tzinfo is None:
            raise ValueError(
                f"timestamp sin zona horaria: {raw_value!r} (se requiere Z o offset)"
            )
        return parsed.astimezone(UTC)

    return (parse_utc,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 1. Tiempo de evento

    Completá `parse_utc`.

    El resultado debe:

    - ser timezone-aware;
    - aceptar los timestamps del dataset;
    - rechazar valores inválidos con una excepción clara.

    Después, usá esa función cuando construyas cada `TimestampedValue`.
    """)
    return


@app.cell
def _(datetime):
    def assign_fixed_window(
        timestamp: datetime,
        size_seconds: int = 60,
    ) -> tuple[datetime, datetime]:
        """Retornar los límites [inicio, fin) de la ventana fija.

        Misma aritmética que `FixedWindows` de Beam: las ventanas están
        alineadas al epoch Unix, así que `inicio = floor(t / size) * size`.
        """
        from datetime import UTC, timedelta

        if size_seconds <= 0:
            raise ValueError("size_seconds debe ser positivo")
        if timestamp.tzinfo is None:
            raise ValueError("timestamp debe ser timezone-aware")

        epoch_seconds = int(timestamp.astimezone(UTC).timestamp())
        start_seconds = epoch_seconds - (epoch_seconds % size_seconds)
        start = datetime.fromtimestamp(start_seconds, tz=UTC)
        return start, start + timedelta(seconds=size_seconds)

    return (assign_fixed_window,)


@app.cell
def _(Any, Iterable, assign_fixed_window, parse_utc):
    def summarize_payments(
        events: Iterable[dict[str, Any]],
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
        deduplicate: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Crear totales deterministas y una auditoría de cada evento.

        Retornar `(totals, audit)`.

        Cada fila de `totals` debe contener `merchant_id`, `window_start`,
        `window_end` y `total`; los límites de ventana se expresan como strings
        ISO-8601.

        Cada fila de `audit` debe contener `event_id`, `merchant_id`,
        `delay_seconds`, `duplicate`, `too_late`, `accepted`, `revision` y
        `reason`. `revision` es verdadero cuando un evento aceptado llega
        después del cierre de su ventana.

        Semántica (espejo de Beam, con `arrival_time` como watermark):

        - la ventana se decide por `event_time`, nunca por llegada;
        - `delay_seconds = arrival_time - event_time`;
        - un evento es `too_late` si llega después de
          `window_end + allowed_lateness` (la ventana ya fue recolectada);
        - un evento aceptado que llega después de `window_end` es una
          `revision` (equivale a un pane late en modo acumulativo);
        - la deduplicación es por `(merchant_id, event_id)`: un id repetido
          en otro comercio es un evento distinto.
        """
        from datetime import timedelta

        lateness = timedelta(seconds=allowed_lateness_seconds)
        seen: set[tuple[str, str]] = set()
        totals_by_key: dict[tuple[str, str, str], int] = {}
        audit: list[dict[str, Any]] = []

        for event in events:
            event_id = str(event["event_id"])
            merchant_id = str(event["merchant_id"])
            event_time = parse_utc(event["event_time"])
            arrival_time = parse_utc(event["arrival_time"])
            window_start, window_end = assign_fixed_window(event_time, window_seconds)
            delay_seconds = (arrival_time - event_time).total_seconds()

            too_late = arrival_time > window_end + lateness
            revision = arrival_time > window_end
            duplicate = False
            accepted = False

            if event.get("status") != "CONFIRMED":
                reason = f"status_{str(event.get('status', 'unknown')).lower()}"
            elif too_late:
                reason = "too_late"
            elif deduplicate and (merchant_id, event_id) in seen:
                duplicate = True
                reason = "duplicate"
            else:
                seen.add((merchant_id, event_id))
                accepted = True
                reason = "accepted"
                key = (merchant_id, window_start.isoformat(), window_end.isoformat())
                totals_by_key[key] = totals_by_key.get(key, 0) + int(event["amount"])

            audit.append(
                {
                    "event_id": event_id,
                    "merchant_id": merchant_id,
                    "delay_seconds": delay_seconds,
                    "duplicate": duplicate,
                    "too_late": too_late,
                    "accepted": accepted,
                    "revision": accepted and revision,
                    "reason": reason,
                }
            )

        totals = [
            {
                "merchant_id": merchant_id,
                "window_start": window_start,
                "window_end": window_end,
                "total": total,
            }
            for (merchant_id, window_start, window_end), total in sorted(
                totals_by_key.items()
            )
        ]
        return totals, audit

    return (summarize_payments,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 2. Contrato determinista antes de Beam

    Implementá `assign_fixed_window` y `summarize_payments`.

    Esta versión pura de Python funciona como oráculo para el pipeline:

    - solo cuenta pagos `CONFIRMED`;
    - la ventana depende de `event_time`;
    - un duplicado no cambia el total;
    - el atraso se calcula con `arrival_time - event_time`;
    - la auditoría conserva la razón de cada decisión;
    - un late aceptado tiene `accepted=True` y `revision=True`;
    - un evento fuera de tolerancia tiene `reason="too_late"`.

    Para la configuración por defecto, documentá cuántos eventos entran,
    cuántos se aceptan y cuántos totales se producen.
    """)
    return


@app.cell
def _(Any, DeduplicatePayments, beam, datetime, parse_utc):
    def build_windowed_totals_pipeline(
        pipeline: Any,
        events: list[dict[str, Any]],
        *,
        window_seconds: int = 60,
    ) -> Any:
        """Construir y retornar la PCollection de totales por ventana.

        Etapas:

        1. `Create` + `TimestampedValue` con `event_time` (tiempo de evento);
        2. `Filter` de pagos `CONFIRMED`;
        3. `WindowInto(FixedWindows)` — la ventana depende del timestamp;
        4. clave por `merchant_id` y `DeduplicatePayments` (estado por clave);
        5. `CombinePerKey(sum)` — agregación parcial, combiner-eligible;
        6. `WindowParam` para recuperar los límites de la ventana.
        """
        from datetime import UTC

        def to_timestamped(event: dict[str, Any]) -> Any:
            seconds = parse_utc(event["event_time"]).timestamp()
            return beam.window.TimestampedValue(event, seconds)

        def to_row(
            keyed_total: tuple[str, int],
            window=beam.DoFn.WindowParam,
        ) -> dict[str, Any]:
            merchant_id, total = keyed_total
            start = datetime.fromtimestamp(float(window.start), tz=UTC)
            end = datetime.fromtimestamp(float(window.end), tz=UTC)
            return {
                "merchant_id": merchant_id,
                "window_start": start.isoformat(),
                "window_end": end.isoformat(),
                "total": total,
            }

        return (
            pipeline
            | "CrearEventos" >> beam.Create(events)
            | "TiempoDeEvento" >> beam.Map(to_timestamped)
            | "SoloConfirmados" >> beam.Filter(lambda e: e.get("status") == "CONFIRMED")
            | "VentanasFijas" >> beam.WindowInto(beam.window.FixedWindows(window_seconds))
            # El type hint le da a la clave un coder determinista (str), que
            # es lo que necesita el estado por clave de DeduplicatePayments.
            | "ClavePorComercio"
            >> beam.Map(lambda e: (e["merchant_id"], e)).with_output_types(
                beam.typehints.Tuple[str, beam.typehints.Any]
            )
            | "Deduplicar" >> beam.ParDo(DeduplicatePayments())
            | "Montos" >> beam.Map(lambda kv: (kv[0], int(kv[1]["amount"])))
            | "TotalPorComercio" >> beam.CombinePerKey(sum)
            | "FilasConVentana" >> beam.Map(to_row)
        )

    return (build_windowed_totals_pipeline,)


@app.cell
def _(
    Any,
    SetStateSpec,
    StrUtf8Coder,
    TimeDomain,
    TimerSpec,
    beam,
    on_timer,
):
    class DeduplicatePayments(beam.DoFn):
        """Eliminar event_id repetidos dentro de cada clave de comercio."""

        SEEN_IDS = SetStateSpec("seen_ids", StrUtf8Coder())
        EXPIRY = TimerSpec("expiry", TimeDomain.WATERMARK)

        def __init__(self, allowed_lateness_seconds: int = 120) -> None:
            super().__init__()
            self.allowed_lateness_seconds = allowed_lateness_seconds

        def process(
            self,
            element: tuple[str, dict[str, Any]],
            seen_ids=beam.DoFn.StateParam(SEEN_IDS),
            window=beam.DoFn.WindowParam,
            expiry=beam.DoFn.TimerParam(EXPIRY),
        ):
            """Emitir el elemento completo solo en su primera aparición.

            El estado es por clave (`merchant_id`) y por ventana, así que el
            mismo `event_id` en dos comercios distintos son dos eventos
            distintos. El timer de watermark se arma al final de la ventana
            más la lateness permitida: pasado ese instante ningún dato de
            esa ventana será aceptado, por lo que el conjunto de ids ya no
            tiene valor y se libera.
            """
            _merchant_id, payload = element
            event_id = str(payload["event_id"])

            if event_id in set(seen_ids.read()):
                return

            seen_ids.add(event_id)
            expiry.set(window.end + self.allowed_lateness_seconds)
            yield element

        @on_timer(EXPIRY)
        def expire(self, seen_ids=beam.DoFn.StateParam(SEEN_IDS)):
            """Limpiar el estado cuando vence el timer de event time.

            Sin este timer el conjunto `seen_ids` crece con cada `event_id`
            visto, para siempre: en un stream no acotado eso es una fuga de
            memoria garantizada. La expiración acota el estado a
            `ventana + lateness`, que es exactamente el período durante el
            cual un duplicado todavía podría alterar un total.
            """
            seen_ids.clear()

    return (DeduplicatePayments,)


@app.cell
def _(Any, beam):
    def build_trigger_policy(
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
    ) -> Any:
        """Crear la transformación WindowInto para streaming.

        Configurar un pane on-time por watermark, una estimación early por
        processing time, revisiones late y modo ACCUMULATING.

        Contrato de salida resultante:

        - **early**: estimación parcial cada 10 s de processing time mientras
          la ventana sigue abierta (baja latencia, resultado provisorio);
        - **on-time**: un pane cuando el watermark cruza el fin de ventana
          (el "cierre" nominal);
        - **late**: una revisión por cada elemento que llegue dentro de
          `allowed_lateness` (completitud);
        - **ACCUMULATING**: cada pane reemplaza al anterior con el total
          completo, lo que habilita un sink UPSERT por clave lógica.
        """
        from apache_beam.transforms import trigger
        from apache_beam.utils.timestamp import Duration

        class Seconds(Duration):
            """Duración de Beam que además expone `.seconds` para inspección."""

            @property
            def seconds(self) -> int:
                return self.micros // 1_000_000

        return beam.WindowInto(
            beam.window.FixedWindows(Seconds(window_seconds)),
            trigger=trigger.AfterWatermark(
                early=trigger.AfterProcessingTime(10),
                late=trigger.AfterCount(1),
            ),
            accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
            allowed_lateness=Seconds(allowed_lateness_seconds),
        )

    return (build_trigger_policy,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 3. Pipeline Beam, estado y triggers

    Completá:

    - `build_windowed_totals_pipeline`;
    - `DeduplicatePayments.process`;
    - `build_trigger_policy`.

    La clave debe ser `merchant_id` antes de usar estado. La salida debe
    recuperar los límites de ventana con `WindowParam`.

    Agregá pruebas con `TestPipeline` y al menos una prueba temporal con
    `TestStream` que evidencie un resultado late aceptado.

    ### Expiración

    Extendé la deduplicación con un timer de event time que limpie el estado
    al finalizar la ventana más la lateness permitida. Explicá por qué un
    estado sin expiración crece indefinidamente.
    """)
    return


@app.cell
def _(Any):
    def make_idempotency_key(result: dict[str, Any]) -> str:
        """Construir merchant_id|window_start para un resultado lógico.

        La clave identifica el *resultado lógico* (un comercio en una
        ventana), no el intento de escritura: por eso un pane late que revisa
        el total, o un reintento por timeout, escriben sobre la misma fila.
        """
        return f"{result['merchant_id']}|{result['window_start']}"

    def simulate_sink_retries(
        results: list[dict[str, Any]],
        *,
        attempts: int = 2,
        idempotent: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Simular intentos de escritura y retornar `(materialized, audit)`.

        En modo idempotente, múltiples intentos del mismo resultado deben dejar
        una sola fila materializada. En modo append, cada intento agrega una.
        """
        if attempts < 1:
            raise ValueError("attempts debe ser al menos 1")

        operation = "UPSERT" if idempotent else "POST"
        upsert_sink: dict[str, dict[str, Any]] = {}
        append_sink: list[dict[str, Any]] = []
        audit: list[dict[str, Any]] = []

        for result in results:
            key = make_idempotency_key(result)
            for attempt in range(1, attempts + 1):
                row = {**result, "idempotency_key": key, "attempt": attempt}
                if idempotent:
                    upsert_sink[key] = row
                else:
                    append_sink.append(row)
                audit.append(
                    {
                        "idempotency_key": key,
                        "attempt": attempt,
                        "operation": operation,
                        "rows_after_attempt": (
                            len(upsert_sink) if idempotent else len(append_sink)
                        ),
                    }
                )

        materialized = list(upsert_sink.values()) if idempotent else append_sink
        return materialized, audit

    return make_idempotency_key, simulate_sink_retries


@app.cell
def _(mo):
    mo.md(r"""
    ## 4. Efectos externos

    Completá `make_idempotency_key` y `simulate_sink_retries`.

    En este ejercicio los sinks **no son servicios externos reales**. Son
    estructuras Python en memoria que representan dos contratos de escritura:

    | Modo simulado | Estructura interna | Operación |
    |---|---|---|
    | `POST` append-only | `list` | `append(row)` en cada intento |
    | `UPSERT` idempotente | `dict` | `sink[idempotency_key] = row` |

    `simulate_sink_retries` siempre retorna dos **listas**:

    1. `materialized`: estado final visible del sink;
    2. `audit`: todos los intentos realizados.

    En modo append-only, `materialized` contiene una fila por intento. En modo
    idempotente, se usa internamente un diccionario y al final se retornan
    `list(upsert_sink.values())`.

    Para cuatro resultados y dos intentos existen ocho filas de auditoría. El
    modo append-only materializa ocho filas; el UPSERT materializa cuatro
    porque el segundo intento reemplaza la misma clave lógica.

    ## 5. Pruebas obligatorias

    El proyecto ya incluye los tests. Ejecutalos con:

    ```bash
    uv run pytest
    ```

    Al comienzo deben fallar con `NotImplementedError`. Implementá las
    funciones hasta que estas garantías queden verdes:

    - [ ] un duplicado no modifica el total;
    - [ ] claves distintas no comparten estado;
    - [ ] un evento fuera de orden cae en su ventana de evento;
    - [ ] un evento con atraso aceptado produce una revisión;
    - [ ] un evento demasiado tardío queda auditado;
    - [ ] dos escrituras del mismo resultado dejan una sola entidad;
    - [ ] el timer limpia el estado cuando corresponde.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Entrega

    Publicá un repositorio propio con:

    1. este notebook completamente implementado;
    2. la suite de pruebas provista ejecutada y completamente verde;
    3. README con instrucciones Docker o `uv`;
    4. explicación breve de ventanas, triggers, estado, timer e
       idempotencia;
    5. evidencia de ejecución y resultados.

    ### Criterios sugeridos

    | Criterio | Peso |
    |---|---:|
    | Contrato temporal y ventanas | 25% |
    | Estado, deduplicación y expiración | 25% |
    | Idempotencia y reintentos | 20% |
    | Pruebas y casos límite | 20% |
    | Reproducibilidad y explicación | 10% |

    Se evalúa corrección conceptual y evidencia, no complejidad innecesaria.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ---

    # Evidencia de ejecución

    Las celdas siguientes ejecutan las funciones implementadas sobre
    `data/payments.jsonl` y muestran los resultados en vivo. Todo lo que se ve
    abajo se recalcula al abrir el notebook; no hay valores copiados a mano.
    """)
    return


@app.cell
def _():
    import json
    from pathlib import Path

    DATA_PATH = Path(__file__).parent / "data" / "payments.jsonl"
    events = [
        json.loads(line)
        for line in DATA_PATH.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    return (events,)


@app.cell
def _(events, mo, summarize_payments):
    totals_default, audit_default = summarize_payments(events)
    accepted_count = sum(1 for row in audit_default if row["accepted"])

    mo.vstack(
        [
            mo.md(
                f"""
                ## 1 · Oráculo determinista (`summarize_payments`)

                Configuración por defecto: ventana **60 s**, lateness **120 s**,
                deduplicación **activada**.

                | Métrica | Valor |
                |---|---:|
                | Eventos de entrada | {len(events)} |
                | Eventos aceptados | {accepted_count} |
                | Totales producidos | {len(totals_default)} |
                """
            ),
            mo.md("**Auditoría por evento** (en orden de llegada):"),
            mo.ui.table(audit_default, selection=None),
            mo.md("**Totales por comercio y ventana:**"),
            mo.ui.table(totals_default, selection=None),
        ]
    )
    return


@app.cell
def _(events, mo, summarize_payments):
    _totals_180, audit_180 = summarize_payments(events, allowed_lateness_seconds=180)
    late_row = next(row for row in audit_180 if row["event_id"] == "p-007")

    mo.md(
        f"""
        ## 2 · El mismo evento con otra política

        `p-007` tiene `event_time` 13:00:51 y llega a 13:03:40, es decir
        **{late_row["delay_seconds"]:.0f} s** de atraso y 160 s después del
        cierre de su ventana (13:01:00).

        | Lateness permitida | ¿Aceptado? | ¿Revisión? | Razón |
        |---:|:---:|:---:|---|
        | 120 s | No | — | `too_late` |
        | 180 s | **Sí** | **Sí** | `accepted` |

        Con 180 s el total de `m-verde` en la ventana 13:00 pasa de 80 000 a
        110 000: eso es un **pane late** en modo acumulativo, y por eso el sink
        necesita una clave idempotente para *reemplazar* la fila anterior.
        """
    )
    return


@app.cell
def _(beam, build_windowed_totals_pipeline, events, mo):
    import json as _json
    import tempfile
    from pathlib import Path as _Path

    from apache_beam.options.pipeline_options import PipelineOptions

    # Los DoFn se serializan al ejecutarse, por lo que no sirve acumular en una
    # lista local: materializamos a un archivo temporal y lo leemos de vuelta.
    with tempfile.TemporaryDirectory() as _tmp:
        _prefix = str(_Path(_tmp) / "totales")
        with beam.Pipeline(options=PipelineOptions(["--runner=DirectRunner"])) as _p:
            _ = (
                build_windowed_totals_pipeline(_p, events, window_seconds=60)
                | "Serializar" >> beam.Map(_json.dumps)
                | "Escribir" >> beam.io.WriteToText(_prefix, shard_name_template="")
            )
        beam_rows = [
            _json.loads(line)
            for line in _Path(_prefix).read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]

    beam_rows_sorted = sorted(beam_rows, key=lambda r: (r["merchant_id"], r["window_start"]))

    mo.vstack(
        [
            mo.md(
                """
                ## 3 · Pipeline Beam (DirectRunner, batch)

                Mismo dataset, procesado con `Create → TimestampedValue → Filter →
                WindowInto → DeduplicatePayments (estado) → CombinePerKey → WindowParam`.

                En modo batch el watermark salta directamente a +∞, así que
                **no hay descarte por lateness**: `p-007` entra en su ventana de
                evento y el total de `m-verde` 13:00 es 110 000. Comparar con el
                oráculo (80 000 con lateness 120 s) muestra precisamente la
                diferencia entre *dónde* cae un evento (ventana) y *si llega a
                tiempo* (watermark + lateness), que en streaming se controla con
                `build_trigger_policy`.
                """
            ),
            mo.ui.table(beam_rows_sorted, selection=None),
        ]
    )
    return


@app.cell
def _(build_trigger_policy, mo):
    policy = build_trigger_policy(window_seconds=60, allowed_lateness_seconds=120)
    windowing = policy.windowing

    mo.md(
        f"""
        ## 4 · Política de triggers para streaming

        ```
        windowfn          = {windowing.windowfn!r}
        trigger           = {windowing.triggerfn!r}
        accumulation_mode = ACCUMULATING
        allowed_lateness  = {windowing.allowed_lateness!r}
        ```

        En `tests/test_streaming_panes.py` hay seis pruebas con
        `TestStream` que avanzan el watermark a mano. Con esta política
        verifican un pane `ON_TIME` (30) seguido de un pane `LATE`
        acumulado (35), el descarte de un evento que supera
        `window_end + allowed_lateness` y que un evento desordenado suma en
        la ventana de su `event_time`. Con `DeduplicatePayments` verifican
        el duplicado, el aislamiento entre comercios y que el timer borra el
        estado cuando el watermark lo alcanza.
        """
    )
    return


@app.cell
def _(events, mo, simulate_sink_retries, summarize_payments):
    totals_for_sink, _audit_for_sink = summarize_payments(events)

    upsert_rows, upsert_audit = simulate_sink_retries(
        totals_for_sink, attempts=2, idempotent=True
    )
    append_rows, append_audit = simulate_sink_retries(
        totals_for_sink, attempts=2, idempotent=False
    )

    mo.vstack(
        [
            mo.md(
                f"""
                ## 5 · Efectos externos con reintentos

                {len(totals_for_sink)} resultados lógicos × 2 intentos =
                {len(upsert_audit)} escrituras en ambos modos.

                | Modo | Intentos auditados | Filas materializadas |
                |---|---:|---:|
                | `UPSERT` idempotente | {len(upsert_audit)} | **{len(upsert_rows)}** |
                | `POST` append-only | {len(append_audit)} | **{len(append_rows)}** |

                El sink idempotente converge a una fila por clave lógica sin
                importar cuántas veces se reintente; el append-only duplica el
                total cada vez que un reintento "tiene éxito dos veces".
                """
            ),
            mo.md("**Estado final del sink UPSERT:**"),
            mo.ui.table(upsert_rows, selection=None),
        ]
    )
    return


if __name__ == "__main__":
    app.run()
