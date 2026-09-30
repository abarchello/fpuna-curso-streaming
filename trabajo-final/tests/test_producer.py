import pandas as pd

from hidromet_streaming.producer import StationReplay, load_arrival_delays, load_readings


def test_replay_uses_the_real_arrival_delay_when_the_dataset_has_it(tmp_path):
    t0 = pd.Timestamp("2026-08-01T10:00:00Z")
    dataset = pd.DataFrame(
        {
            "station_id": ["ITA01", "ITA02"],
            "event_time": [t0, t0 + pd.Timedelta(minutes=10)],
            "precip_mm": [0.4, 1.2],
            "enlace": ["fibra", "fibra"],
            # la primera lectura llegó una hora después de medida; la segunda no trae envío
            "arrival_time": [t0 + pd.Timedelta(hours=1), pd.NaT],
        }
    )
    path = tmp_path / "emas_10min.parquet"
    dataset.to_parquet(path)

    readings, links = load_readings(path, source="test")
    delays = load_arrival_delays(path)
    assert delays == {"ema-ITA01-20260801T100000Z": 3600.0}

    replay = StationReplay(None, topic="t", link_delays=links, arrival_delays=delays, seed=1)
    schedule = replay.schedule(readings)

    # la de ITA02 (fibra, segundos de atraso) se publica antes que la de ITA01, medida antes
    assert [reading.station_id for _, _, reading in schedule] == ["ITA02", "ITA01"]
    assert schedule[1][0] == 3600.0
    assert replay.delayed == 1
