from datetime import UTC, datetime

import numpy as np
import pandas as pd

from hidromet_streaming.contracts import PHYSICAL_RANGES
from hidromet_streaming.dataset import (
    STATIONS,
    disaggregate_to_10min,
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
