import marimo

__generated_with = "0.23.15"
app = marimo.App(width="full")


@app.cell
def _():
    import altair as alt
    import marimo as mo

    from hidromet_streaming.config import Settings
    from hidromet_streaming.consumer import AggregateStore, build_consumer, poll_into_store

    return AggregateStore, Settings, alt, build_consumer, mo, poll_into_store


@app.cell
def _(mo):
    mo.md(r"""
    # 3 · Consumidor analítico: del changelog a una vista operativa

    El tópico `ema.agregados.v1` conserva resultados por ventana y pane. Este
    notebook no recalcula lecturas: consume revisiones y hace **upsert** por
    `aggregate_id`, ignorando panes viejos que lleguen fuera de orden. Al
    reiniciarlo reproduce Kafka desde `earliest` y reconstruye la vista.
    """)
    return


@app.cell
def _(AggregateStore, Settings, build_consumer):
    analytics_settings = Settings.from_env()
    aggregate_store = AggregateStore()
    aggregate_consumer = build_consumer(analytics_settings)
    return aggregate_consumer, aggregate_store


@app.cell
def _(mo):
    refresh_stream = mo.ui.refresh(options=["1s", "2s", "5s", "10s"], default_interval="2s")
    max_messages = mo.ui.slider(10, 2000, value=500, step=10, label="Mensajes por poll")
    mo.hstack([refresh_stream, max_messages], justify="start")
    return max_messages, refresh_stream


@app.cell
def _(aggregate_consumer, aggregate_store, max_messages, poll_into_store, refresh_stream):
    refresh_stream.value
    poll_result = poll_into_store(
        aggregate_consumer,
        aggregate_store,
        max_messages=int(max_messages.value),
        timeout_seconds=0.2,
    )
    station_frame = aggregate_store.station_frame()
    basin_frame = aggregate_store.basin_frame()
    alerts_frame = aggregate_store.alerts_frame()
    return alerts_frame, basin_frame, poll_result, station_frame


@app.cell
def _(mo, poll_result):
    mo.hstack(
        [
            mo.stat(poll_result["messages_seen"], label="Panes leídos"),
            mo.stat(poll_result["logical_aggregates"], label="Agregados lógicos"),
            mo.stat(poll_result["revisions"], label="Revisiones aplicadas"),
            mo.stat(poll_result["station_windows"], label="Estación × ventana"),
            mo.stat(poll_result["alerts"], label="Alertas"),
            mo.stat(poll_result["errors"], label="Errores del poll"),
        ],
        widths="equal",
    )
    return


@app.cell
def _(alt, mo, station_frame):
    if station_frame.empty:
        precip_view = mo.callout(
            mo.md("Todavía no hay agregados. Iniciá primero el pipeline y el productor."),
            kind="warn",
        )
    else:
        precip_view = (
            alt.Chart(station_frame)
            .mark_bar()
            .encode(
                x=alt.X("window_start:T", title="Inicio de ventana (tiempo de evento)"),
                y=alt.Y("precip_mm:Q", title="Precipitación en 10 min (mm)"),
                color=alt.Color("subcuenca:N", title="Subcuenca"),
                row=alt.Row("dimension_name:N", title=None, header=alt.Header(labelAngle=0)),
                tooltip=[
                    "dimension_name",
                    "window_start:T",
                    "precip_mm",
                    "n_lecturas",
                    "pane_timing",
                    "pane_index",
                ],
            )
            .properties(height=48, width=900)
        )
    mo.vstack([mo.md("## Precipitación por estación y ventana de 10 min"), precip_view])
    return


@app.cell
def _(alt, basin_frame, mo):
    if basin_frame.empty:
        basin_view = mo.md("La precipitación areal por subcuenca aparecerá con la primera hora.")
    else:
        basin_view = (
            alt.Chart(basin_frame)
            .mark_line(point=True)
            .encode(
                x=alt.X("window_end:T", title="Fin de ventana horaria"),
                y=alt.Y("precip_media_mm:Q", title="Precipitación media areal (mm/h)"),
                color=alt.Color("dimension_name:N", title="Subcuenca"),
                tooltip=[
                    "dimension_name",
                    "window_end:T",
                    "precip_media_mm",
                    "precip_max_mm",
                    "estacion_max",
                    "n_estaciones",
                ],
            )
            .properties(height=320)
        )
    mo.vstack([mo.md("## Precipitación areal por subcuenca (1 h)"), basin_view])
    return


@app.cell
def _(alt, mo, station_frame):
    if station_frame.empty:
        temp_view = mo.md("Esperando ventanas de temperatura…")
    else:
        temp_view = (
            alt.Chart(station_frame)
            .mark_line()
            .encode(
                x=alt.X("window_start:T", title="Inicio de ventana"),
                y=alt.Y("temp_media_c:Q", title="Temperatura media (°C)", scale=alt.Scale(zero=False)),
                color=alt.Color("dimension_name:N", title="Estación"),
                tooltip=["dimension_name", "window_start:T", "temp_media_c", "hr_media_pct"],
            )
            .properties(height=300)
        )
    mo.vstack([mo.md("## Temperatura media por estación"), temp_view])
    return


@app.cell
def _(alerts_frame, mo):
    mo.vstack(
        [
            mo.md("## Alertas emitidas"),
            mo.ui.table(
                alerts_frame[
                    ["window_start", "dimension_name", "subcuenca", "tipo", "valor", "umbral", "pane_timing"]
                ].sort_values("window_start", ascending=False),
                selection=None,
                pagination=True,
            )
            if not alerts_frame.empty
            else mo.md("Sin alertas hasta ahora."),
        ]
    )
    return


@app.cell
def _(mo, station_frame):
    mo.vstack(
        [
            mo.md("## Registros materializados (estación × ventana)"),
            mo.ui.table(station_frame.tail(200), selection=None, pagination=True)
            if not station_frame.empty
            else mo.md("Sin filas todavía."),
            mo.callout(
                mo.md(
                    "Este notebook es una capa de *serving* efímera. Kafka sigue siendo la "
                    "fuente durable; en producción el changelog se materializaría en una base "
                    "de series temporales (p. ej. TimescaleDB) con `UPSERT` por `aggregate_id`."
                ),
                kind="info",
            ),
        ]
    )
    return


if __name__ == "__main__":
    app.run()
