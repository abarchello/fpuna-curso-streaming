import marimo

__generated_with = "0.23.15"
app = marimo.App(width="full")


@app.cell
def _():
    import json
    import sys

    import altair as alt
    import marimo as mo
    import pandas as pd

    from hidromet_streaming.config import processed_dataset_path, project_root
    from hidromet_streaming.process_control import ManagedProcess

    return ManagedProcess, alt, json, mo, pd, processed_dataset_path, project_root, sys


@app.cell
def _(mo):
    mo.md(r"""
    # 1 · Productor: de la historia de las estaciones a un stream

    Las estaciones meteorológicas automáticas (EMAS) registran cada **10
    minutos** precipitación, temperatura, humedad, presión, viento y radiación.
    Este notebook toma un período histórico (o sintético), desplaza la primera
    lectura al presente y publica el resto en Kafka respetando las distancias
    de tiempo de evento, aceleradas por un factor configurable.

    La clave Kafka es `station_id`: las lecturas de una estación conservan el
    orden de publicación dentro de una partición. Cada estación tiene un perfil
    de enlace (`fibra`, `gprs`, `satelital`) que retiene sus lecturas cierto
    tiempo antes de publicarlas: así aparecen el **desorden** y los **datos
    tardíos** que el pipeline tiene que absorber.
    """)
    return


@app.cell
def _(mo, processed_dataset_path):
    # Cada dataset preparado vive en data/processed/ o en una subcarpeta (--output).
    processed_dir = processed_dataset_path().parent
    candidates = sorted(processed_dir.rglob("*.parquet")) or [processed_dataset_path()]
    candidates = [p for p in candidates if not p.name.startswith("agregados")] or candidates
    dataset_options = {str(p.relative_to(processed_dir)): str(p) for p in candidates}
    default_choice = processed_dataset_path().name
    dataset_choice = mo.ui.dropdown(
        options=dataset_options,
        value=default_choice if default_choice in dataset_options else next(iter(dataset_options)),
        label="Dataset a reproducir",
    )
    dataset_choice
    return (dataset_choice,)


@app.cell
def _(dataset_choice, json, pd):
    from pathlib import Path

    dataset_path = Path(dataset_choice.value)
    readings = pd.read_parquet(dataset_path).sort_values(["event_time", "station_id"])
    manifest_path = dataset_path.parent / "manifest.json"
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    real_arrivals = int(readings["arrival_time"].notna().sum()) if "arrival_time" in readings else 0
    return dataset_path, manifest, readings, real_arrivals


@app.cell
def _(alt, manifest, mo, readings, real_arrivals):
    precip_by_station = (
        readings.groupby(["station_id", "station_name", "enlace"], as_index=False)["precip_mm"]
        .sum()
        .rename(columns={"precip_mm": "precip_total_mm"})
    )
    source_chart = (
        alt.Chart(precip_by_station)
        .mark_bar(color="#2a78d6")
        .encode(
            x=alt.X("station_name:N", title="Estación", sort="-y"),
            y=alt.Y("precip_total_mm:Q", title="Precipitación total del período (mm)"),
            tooltip=["station_id", "station_name", "enlace", "precip_total_mm"],
        )
        .properties(height=260)
    )
    mo.vstack(
        [
            mo.md(
                f"""
                ## Datos que serán reproducidos

                **{len(readings):,} lecturas** de **{readings.station_id.nunique()} estaciones**
                (fuente: `{manifest.get("source", "?")}`)
                `{readings.event_time.min()}` → `{readings.event_time.max()}`.
                Lecturas con hora de envío real (`arrival_time`): **{real_arrivals:,}**;
                las demás usan el atraso simulado por tipo de enlace.
                """
            ),
            source_chart,
            mo.ui.table(
                readings[
                    [
                        "event_time",
                        "station_id",
                        "station_name",
                        "enlace",
                        "precip_mm",
                        "temp_c",
                        "hr_pct",
                        "viento_ms",
                        "rafaga_ms",
                    ]
                ].head(12),
                selection=None,
            ),
        ]
    )
    return


@app.cell
def _(mo):
    speedup = mo.ui.slider(10, 600, value=60, step=10, label="Aceleración")
    max_readings = mo.ui.number(100, 50_000, value=3_000, step=100, label="Máximo de lecturas")
    duplicate_rate = mo.ui.slider(
        0.0, 0.10, value=0.02, step=0.01, label="Probabilidad de duplicado"
    )
    jitter = mo.ui.slider(0.0, 3.0, value=0.0, step=0.25, label="Jitter de llegada (s)")
    link_delays = mo.ui.checkbox(value=True, label="Atrasos de llegada (reales o por enlace)")
    start_producer = mo.ui.run_button(label="▶ Iniciar replay")
    stop_producer = mo.ui.run_button(label="■ Detener replay")
    refresh_log = mo.ui.run_button(label="↻ Actualizar estado")
    mo.vstack(
        [
            mo.md(
                """
                ## Controles de la simulación

                A `60×`, diez minutos del dataset transcurren en diez segundos de
                pared. Los duplicados reutilizan el mismo `event_id`, de modo que
                Beam los reconoce como reintentos del mismo evento lógico. Con el
                atraso por enlace activado, las estaciones satelitales llegan
                entre 10 y 35 minutos (lógicos) tarde: algunas superan la
                lateness de 20 minutos y quedan fuera de su ventana. Las
                lecturas que traen su hora de envío real se publican con ese
                atraso, que a veces es de horas.
                """
            ),
            mo.hstack([speedup, max_readings, duplicate_rate, jitter], widths="equal"),
            mo.hstack([link_delays, start_producer, stop_producer, refresh_log], justify="start"),
        ]
    )
    return (
        duplicate_rate,
        jitter,
        link_delays,
        max_readings,
        refresh_log,
        speedup,
        start_producer,
        stop_producer,
    )


@app.cell
def _(ManagedProcess):
    producer_process = ManagedProcess()
    return (producer_process,)


@app.cell
def _(
    dataset_path,
    duplicate_rate,
    jitter,
    link_delays,
    max_readings,
    producer_process,
    project_root,
    speedup,
    start_producer,
    stop_producer,
    sys,
):
    if stop_producer.value:
        producer_process.stop()
    if start_producer.value and not producer_process.running:
        command = [
            sys.executable,
            "-m",
            "hidromet_streaming.producer",
            "--dataset",
            str(dataset_path),
            "--max-readings",
            str(int(max_readings.value)),
            "--speedup",
            str(float(speedup.value)),
            "--duplicate-rate",
            str(float(duplicate_rate.value)),
            "--jitter-seconds",
            str(float(jitter.value)),
        ]
        if not link_delays.value:
            command.append("--no-link-delays")
        producer_process.start(command, log_path=project_root() / "tmp/producer.log")
    return


@app.cell
def _(mo, producer_process, refresh_log):
    refresh_log.value
    status = "🟢 ejecutándose" if producer_process.running else "⚪ detenido"
    log_text = producer_process.tail(30) or "Todavía no hay salida del productor."
    mo.vstack(
        [
            mo.md(f"## Estado: {status}"),
            mo.md(
                """
                `confluent-kafka` usa un productor idempotente con `acks=all`:
                evita duplicados creados por reintentos internos del cliente,
                pero no los duplicados lógicos que introduce la fuente (el
                datalogger que reenvía una lectura). Son problemas distintos y
                se resuelven en capas distintas.
                """
            ),
            mo.callout(mo.md(f"```text\n{log_text}\n```"), kind="neutral"),
        ]
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Qué observar en Kafka

    ```bash
    docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server kafka:9092 --describe --topic ema.lecturas.v1

    docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
      --bootstrap-server kafka:9092 --topic ema.lecturas.v1 \
      --property print.key=true --property print.timestamp=true --max-messages 5
    ```

    El timestamp Kafka de cada mensaje es el `event_time` de la lectura (no el
    de publicación): el pipeline usa la política `create_time` de KafkaIO para
    derivar el watermark de ese campo.
    """)
    return


if __name__ == "__main__":
    app.run()
