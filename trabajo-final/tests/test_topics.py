import json

from hidromet_streaming.topics import summarize_input, summarize_output


def reading(event_id: str) -> bytes:
    return json.dumps({"event_id": event_id}).encode()


def test_input_summary_counts_disorder_per_partition_and_repeated_ids():
    messages = [
        (0, 1000, reading("a")),
        (0, 2000, reading("b")),
        (0, 1500, reading("c")),  # llegó después de uno con timestamp mayor
        (0, 2000, reading("b")),  # reenvío del mismo evento
        (1, 5000, reading("d")),
        (1, 4000, reading("e")),
    ]
    summary = summarize_input(messages)
    assert summary["mensajes"] == 6
    assert summary["por_particion"]["0"] == {"mensajes": 4, "fuera_de_orden": 1}
    assert summary["por_particion"]["1"] == {"mensajes": 2, "fuera_de_orden": 1}
    assert summary["event_id_repetidos"] == 1


def test_output_summary_keeps_the_latest_pane_of_each_window():
    def pane(index: int, timing: str, count: int) -> bytes:
        return json.dumps(
            {
                "aggregate_id": "estacion_10min|w|ITA01",
                "metric_type": "estacion_10min",
                "pane_index": index,
                "pane_timing": timing,
                "n_lecturas": count,
            }
        ).encode()

    alert = json.dumps(
        {
            "aggregate_id": "alerta|w|ITA01",
            "metric_type": "alerta",
            "pane_index": 0,
            "pane_timing": "ON_TIME",
            "tipo": "lluvia_intensa",
        }
    ).encode()
    summary = summarize_output(
        [(0, 1, pane(0, "EARLY", 1)), (0, 2, pane(1, "ON_TIME", 1)), (1, 3, alert)]
    )
    assert summary["panes"] == {
        "alerta/ON_TIME": 1,
        "estacion_10min/EARLY": 1,
        "estacion_10min/ON_TIME": 1,
    }
    assert summary["ventanas_de_estacion"] == 1
    assert summary["ventanas_con_mas_de_una_lectura"] == 0
    assert summary["panes_vacios"] == 0
    assert summary["alertas"] == {"lluvia_intensa": 1}
