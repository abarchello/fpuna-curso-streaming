from hidromet_streaming.consumer import AggregateStore


def aggregate(pane_index: int, precip: float, metric_type: str = "estacion_10min") -> dict:
    return {
        "aggregate_id": f"{metric_type}|2023-10-28T14:00:00Z|ITA01",
        "metric_type": metric_type,
        "window_start": "2023-10-28T14:00:00Z",
        "window_end": "2023-10-28T14:10:00Z",
        "dimension_id": "ITA01",
        "dimension_name": "Hernandarias",
        "subcuenca": "monday",
        "pane_index": pane_index,
        "precip_mm": precip,
    }


def test_store_upserts_revisions_and_ignores_stale_panes():
    store = AggregateStore()
    assert store.upsert(aggregate(0, 1.0)) is True
    assert store.upsert(aggregate(2, 3.5)) is True  # revisión (pane late)
    assert store.upsert(aggregate(1, 2.0)) is False  # pane viejo fuera de orden
    assert len(store.records) == 1
    assert store.station_frame().iloc[0]["precip_mm"] == 3.5
    summary = store.summary()
    assert summary["messages_seen"] == 3
    assert summary["revisions"] == 1
    assert summary["station_windows"] == 1
