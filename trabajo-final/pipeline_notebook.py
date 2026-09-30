import marimo

__generated_with = "0.23.15"
app = marimo.App(width="full")


@app.cell
def _():
    import sys

    import marimo as mo

    from hidromet_streaming.config import Settings, project_root
    from hidromet_streaming.process_control import ManagedProcess

    return ManagedProcess, Settings, mo, project_root, sys


@app.cell
def _(mo):
    mo.md(r"""
    # 2 · Pipeline: Kafka → Beam → Kafka

    Este notebook es el **plano de control**. Al iniciar el job, Python
    construye el grafo Beam y lo envía al Job Server; Flink distribuye sus
    etapas entre TaskManagers. KafkaIO se ejecuta con el SDK Java y nuestras
    funciones analíticas con workers Python.

    ```text
    Kafka (ema.lecturas.v1, 4 particiones)
      → KafkaIO (Java) → decodificar/validar → DLQ para inválidos
      → tiempo de evento → ventanas fijas 10 min (early / on-time / late)
      → clave station_id → DeduplicateReadings (estado + timer)
      → CombinePerKey(StationStatsCombineFn) ─┬→ alertas (umbral)
      → re-ventaneo 1 h → clave subcuenca → BasinStatsCombineFn
      → KafkaIO (Java) → ema.agregados.v1 (clave = aggregate_id)
    ```
    """)
    return


@app.cell
def _(Settings):
    pipeline_settings = Settings.from_env()
    return (pipeline_settings,)


@app.cell
def _(mo, pipeline_settings):
    mo.vstack(
        [
            mo.md("## Contrato de ejecución"),
            mo.ui.table(
                [
                    {"parámetro": "Particiones Kafka", "valor": 4},
                    {"parámetro": "Paralelismo Beam/Flink", "valor": pipeline_settings.parallelism},
                    {
                        "parámetro": "Ventana de estación",
                        "valor": f"{pipeline_settings.window_seconds} s",
                    },
                    {
                        "parámetro": "Ventana de subcuenca",
                        "valor": f"{pipeline_settings.hourly_window_seconds} s",
                    },
                    {
                        "parámetro": "Lateness permitida",
                        "valor": f"{pipeline_settings.allowed_lateness_seconds} s",
                    },
                    {
                        "parámetro": "Pane early cada",
                        "valor": f"{pipeline_settings.early_firing_seconds} s (processing time)",
                    },
                    {
                        "parámetro": "Umbral lluvia intensa",
                        "valor": f"{pipeline_settings.alert_precip_10min_mm} mm / 10 min",
                    },
                    {
                        "parámetro": "Umbral ráfaga fuerte",
                        "valor": f"{pipeline_settings.alert_gust_ms} m/s",
                    },
                    {"parámetro": "Runner", "valor": "PortableRunner → Flink"},
                ],
                selection=None,
            ),
            mo.callout(
                mo.md(
                    "La UI de Flink está disponible en "
                    "[localhost:8081](http://localhost:8081): subtasks, slots, "
                    "backpressure, watermarks por operador y checkpoints."
                ),
                kind="info",
            ),
        ]
    )
    return


@app.cell
def _(mo):
    group_id = mo.ui.text(value="beam-hidromet-v1", label="Consumer group del pipeline")
    start_pipeline = mo.ui.run_button(label="▶ Enviar job a Flink")
    stop_pipeline = mo.ui.run_button(label="■ Cancelar job local")
    refresh_pipeline = mo.ui.run_button(label="↻ Actualizar log")
    mo.vstack(
        [
            group_id,
            mo.hstack([start_pipeline, stop_pipeline, refresh_pipeline], justify="start"),
        ]
    )
    return group_id, refresh_pipeline, start_pipeline, stop_pipeline


@app.cell
def _(ManagedProcess):
    beam_process = ManagedProcess()
    return (beam_process,)


@app.cell
def _(beam_process, group_id, project_root, start_pipeline, stop_pipeline, sys):
    if stop_pipeline.value:
        beam_process.stop()
    if start_pipeline.value and not beam_process.running:
        beam_process.start(
            [sys.executable, "-m", "hidromet_streaming.pipeline", "--group-id", group_id.value],
            log_path=project_root() / "tmp/pipeline.log",
        )
    return


@app.cell
def _(beam_process, mo, refresh_pipeline):
    refresh_pipeline.value
    pipeline_status = "🟢 job activo" if beam_process.running else "⚪ sin proceso local"
    pipeline_log = beam_process.tail(45) or "El pipeline aún no fue iniciado."
    mo.vstack(
        [
            mo.md(f"## Estado: {pipeline_status}"),
            mo.callout(mo.md(f"```text\n{pipeline_log}\n```"), kind="neutral"),
        ]
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Decisiones de la transformación

    | Decisión | Por qué |
    |---|---|
    | `event_time` como timestamp | El acumulado de lluvia es una magnitud física del intervalo en que cayó, no del momento en que llegó el dato. |
    | Ventanas fijas de 10 min | Coinciden con la cadencia de las EMAS: cada ventana contiene una lectura por estación. |
    | `AfterWatermark(early=10 s, late=1)` + ACCUMULATING | Estimación provisoria para el dashboard, resultado nominal al cerrar, revisiones por datos tardíos; cada pane trae el total completo. |
    | Lateness 20 min | Cubre a las estaciones satelitales en operación normal; acota el estado a 30 min por ventana. |
    | Dedup con estado + timer | `event_id` determinista por estación y ventana; el timer libera el estado en `fin + lateness`. |
    | `CombinePerKey` con CombineFn | Agregación parcial en cada worker antes del shuffle; el promedio se calcula al extraer. |
    | Precipitación areal por subcuenca | Promedio de los acumulados por estación (no la suma): tiene significado hidrológico. |
    | Clave `metric_type|window_start|dimension_id` | Un pane nuevo reemplaza al anterior en el consumidor; el replay es seguro. |
    | DLQ | Lecturas malformadas o fuera de rango físico no contaminan las ventanas y quedan para diagnóstico. |
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Replay y reprocesamiento

    Como la salida es idempotente por clave lógica, se puede reprocesar el log
    con otra versión del pipeline sin borrar nada:

    ```bash
    docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
      --bootstrap-server kafka:9092 --group beam-hidromet-v2 \
      --topic ema.lecturas.v1 --reset-offsets --to-earliest --execute
    ```

    Luego enviar el job con el grupo `beam-hidromet-v2` desde este notebook.
    Las ventanas recalculadas sobreescriben las anteriores en el consumidor.

    ## Experimentar con paralelismo

    ```bash
    BEAM_PARALLELISM=3 docker compose up -d --scale taskmanager=3 \
      taskmanager beam-job-server
    ```

    Cuatro particiones → como máximo cuatro lectores útiles. La clave
    `station_id` reparte 12 estaciones en 4 particiones. El hash no asegura
    3 por partición (alguna puede quedar con más), pero como todas emiten a la
    misma cadencia no aparecen claves calientes.
    """)
    return


if __name__ == "__main__":
    app.run()
