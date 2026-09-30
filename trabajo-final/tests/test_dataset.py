from datetime import UTC, datetime

import numpy as np
import pandas as pd

from hidromet_streaming.contracts import PHYSICAL_RANGES
from hidromet_streaming.dataset import (
    STATIONS,
    disaggregate_to_10min,
    from_ftp_long,
    prepare_dataset,
    synthetic_station,
)


def test_synthetic_generator_is_deterministic_and_physical():
    start = datetime(2023, 10, 28, tzinfo=UTC)
    a = synthetic_station(STATIONS[0], start, 1, seed=42)
    b = synthetic_station(STATIONS[0], start, 1, seed=42)
    pd.testing.assert_frame_equal(a, b)
    assert len(a) == 144
    for name, (low, high) in PHYSICAL_RANGES.items():
        assert a[name].between(low, high).all(), name
    assert (a["rafaga_ms"] >= a["viento_ms"]).all()


def test_disaggregation_conserves_hourly_precipitation():
    times = pd.date_range("2023-10-28T00:00Z", periods=4, freq="h")
    hourly = pd.DataFrame(
        {
            "time": times,
            "temp_c": [20.0, 21.0, 23.0, 24.0],
            "hr_pct": [80, 78, 70, 65],
            "precip_mm": [0.0, 12.3, 4.0, 0.0],
            "presion_hpa": [1008.0, 1007.5, 1007.0, 1006.5],
            "viento_ms": [2.0, 3.0, 4.0, 3.5],
            "viento_dir_deg": [120, 130, 140, 150],
            "rafaga_ms": [4.0, 7.0, 9.0, 6.0],
            "radiacion_wm2": [0, 50, 200, 400],
        }
    )
    ten_min = disaggregate_to_10min(hourly, seed=1)
    assert len(ten_min) == 19  # 3 h completas + el instante final
    by_hour = ten_min.groupby(ten_min["event_time"].dt.floor("h"))["precip_mm"].sum()
    assert np.isclose(by_hour.iloc[0], 0.0)
    assert np.isclose(by_hour.iloc[1], 12.3, atol=0.05)
    assert np.isclose(by_hour.iloc[2], 4.0, atol=0.05)
    assert ten_min["temp_c"].between(19.0, 25.0).all()


def test_csv_source_clips_period_and_takes_metadata_from_catalog(tmp_path, monkeypatch):
    monkeypatch.setenv("HIDROMET_LAB_ROOT", str(tmp_path))
    monkeypatch.delenv("HIDROMET_DATASET_PATH", raising=False)
    monkeypatch.delenv("HIDROMET_STATIONS_PATH", raising=False)
    export = tmp_path / "export.csv"
    export.write_text(
        "codigo,fecha,lluvia,nombre_en_origen\n"
        "ITA01,2024-06-30T23:50:00Z,9.9,no-se-usa\n"  # antes del período
        "ITA01,2024-07-01T00:00:00Z,0.4,no-se-usa\n"
        "ITA07,2024-07-01T00:00:00Z,,no-se-usa\n"  # sin precipitación: se descarta
        "ITA07,2024-07-02T23:50:00Z,1.2,no-se-usa\n"  # último instante del día de --end
        "ITA07,2024-07-03T00:00:00Z,5.0,no-se-usa\n",  # después del período
        encoding="utf-8",
    )
    column_map = {"station_id": "codigo", "event_time": "fecha", "precip_mm": "lluvia"}

    manifest = prepare_dataset(
        source="csv", csv_file=export, column_map=column_map, start="2024-07-01", end="2024-07-02"
    )

    assert manifest["readings"] == 2
    assert manifest["dropped_incomplete_rows"] == 1
    assert manifest["period_filter"] == {
        "start": "2024-07-01",
        "end": "2024-07-02",
        "rows_outside_period": 2,
    }
    assert manifest["precip_total_mm_by_station"] == {"ITA01": 0.4, "ITA07": 1.2}
    stations = pd.read_csv(tmp_path / "data/processed/estaciones.csv")
    by_id = stations.set_index("station_id")
    assert by_id.loc["ITA01", "station_name"] == "Hernandarias"
    assert by_id.loc["ITA07", "subcuenca"] == "margen-derecha-centro"
    dataset = pd.read_parquet(tmp_path / "data/processed/emas_10min.parquet")
    assert dataset["temp_c"].isna().all()  # el export sólo trae lluvia


def test_ftp_long_export_applies_qc_and_keeps_first_arrival(tmp_path):
    t0 = datetime(2026, 8, 1, 10, 0)
    t1 = datetime(2026, 8, 1, 10, 10)
    sent = datetime(2026, 8, 1, 10, 2)
    resent = datetime(2026, 8, 1, 10, 30)
    rows = [
        # estación con hora de envío (un archivo por transmisión)
        ("6000000001", t0, "0003", "0.4", sent),
        ("6000000001", t0, "0008", "21.5", sent),
        ("6000000001", t0, "0013", "[01]", sent),  # código de estado del sensor
        ("6000000001", t0, "0010", "900.0", sent),  # presión fuera de rango
        ("6000000001", t0, "0003", "0.4", resent),  # la misma medición, reenviada
        ("6000000001", t1, "0003", "1.2", datetime(2026, 8, 1, 10, 11)),
        ("6000000001", t1, "0013", "-1.2", datetime(2026, 8, 1, 10, 11)),  # offset nocturno
        ("6000000001", t1, "0032", "12.7", datetime(2026, 8, 1, 10, 11)),  # batería: se ignora
        # estación de archivos diarios, sin hora de envío
        ("6000000002", t0, "0003", "0.0", None),
        ("6000000002", t0, "0010", "999", None),  # centinela
        ("6000000002", t0, "0002", "-3", None),  # dirección imposible
        ("6000000009", t0, "0003", "5.0", None),  # fuera del mapa: se ignora
    ]
    path = tmp_path / "largo.parquet"
    pd.DataFrame(
        rows, columns=["estacion", "ts", "sensor", "valor_texto", "enviado_en"]
    ).to_parquet(path)

    frame, info = from_ftp_long(path, {"6000000001": "ITA01", "6000000002": "ITA02"})

    assert len(frame) == 3
    by_key = frame.set_index(["station_id", frame["event_time"].dt.strftime("%H:%M")])
    first = by_key.loc[("ITA01", "10:00")]
    assert first["precip_mm"] == 0.4 and first["temp_c"] == 21.5
    assert np.isnan(first["radiacion_wm2"]) and np.isnan(first["presion_hpa"])
    assert first["flag_qc"] == "QC:presion_hpa"
    assert str(first["arrival_time"]) == "2026-08-01 10:02:00+00:00"  # el primer envío
    second = by_key.loc[("ITA01", "10:10")]
    assert second["radiacion_wm2"] == 0.0 and second["flag_qc"] == "OK"
    daily = by_key.loc[("ITA02", "10:00")]
    assert np.isnan(daily["presion_hpa"]) and np.isnan(daily["viento_dir_deg"])
    assert daily["flag_qc"] == "QC:viento_dir_deg"
    assert pd.isna(daily["arrival_time"])
    assert info["non_numeric_values"] == 1
    assert info["sentinel_values"] == 1
    assert info["night_radiation_set_to_zero"] == 1
    assert info["qc_nulled_by_variable"] == {"presion_hpa": 1, "viento_dir_deg": 1}
    assert info["readings_with_arrival_time"] == 2
