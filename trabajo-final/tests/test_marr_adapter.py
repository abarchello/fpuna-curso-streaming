"""Adaptador del export largo de MARR.CE, probado con un fixture sintético mínimo.

El fixture reproduce el *formato* del export (separador `;`, decimales con coma,
fecha local dd/mm/aaaa, centinela -999,99, filas manuales) con valores
inventados, para que la prueba no dependa del dataset real.
"""

from pathlib import Path

import numpy as np

from hidromet_streaming.contracts import decode_event, encode_event, reading_from_row
from hidromet_streaming.dataset import from_marr_long

HEADER = (
    "Fecha y hora de medicion;Observaciones;Id Origen;Nombre Variable;Tipo Medicion;"
    "Tipo Variable;Unidad Medida;Encargado Nombre;Codigo Estacion;Nombre Estacion;"
    "Tipo Estacion;Fuente Estacion;Latitud Estacion;Longitud Estacion;Origen Dato;"
    "Fecha Pronostico;Nombres de medidas;Valor Medicion"
)


def row(stamp, variable, unit, code, name, lat, value, tipo="auto"):
    return (
        f"{stamp};;1;{variable};{tipo};Climatologica;{unit};;{code};{name};Meteorologica;"
        f"X;{lat};-54,6;PGY;;Valor medicion;{value}"
    )


def write_fixture(path: Path) -> None:
    lines = [
        HEADER,
        row("01/09/2025 10:00:00", "Precipitación", "mm", "6000000001", "Norte", "-24,1", "0,4"),
        row(
            "01/09/2025 10:00:00",
            "Temperatura del Aire",
            "°C",
            "6000000001",
            "Norte",
            "-24,1",
            "21,5",
        ),
        row(
            "01/09/2025 10:00:00",
            "Evapotranspiración",
            "mm",
            "6000000001",
            "Norte",
            "-24,1",
            "-999,99",
        ),
        row("01/09/2025 10:10:00", "Precipitación", "mm", "6000000001", "Norte", "-24,1", "1,2"),
        row(
            "01/09/2025 10:10:00", "Precipitación", "mm", "6000000001", "Norte", "-24,1", "1,2"
        ),  # dup
        row("01/09/2025 10:00:00", "Precipitación", "mm", "6000000002", "Sur", "-25,5", "0"),
        row(
            "01/09/2025 10:00:00",
            "Temperatura del Aire",
            "°C",
            "6000000002",
            "Sur",
            "-25,5",
            "-999,99",
        ),
        row(
            "01/09/2025 00:00:00",
            "Evaporación (Tanque)",
            "mm",
            "6000000002",
            "Sur",
            "-25,5",
            "4,5",
            "manual",
        ),
    ]
    path.write_text("\ufeff" + "\r\n".join(lines) + "\r\n", encoding="utf-8")


def test_marr_long_export_is_pivoted_to_project_schema(tmp_path):
    path = tmp_path / "export.csv"
    write_fixture(path)
    frame, info = from_marr_long(path)

    assert info["duplicated_rows_in_source"] == 1
    assert info["variables_present"] == ["precip_mm", "temp_c"]
    assert set(frame["station_id"]) == {"MARR0001", "MARR0002"}
    assert set(frame["subcuenca"]) == {"embalse-norte", "embalse-sur"}

    norte = frame[frame["station_id"] == "MARR0001"].sort_values("event_time")
    assert len(norte) == 2
    # hora local 10:00 (UTC-3) -> 13:00 UTC
    assert str(norte.iloc[0]["event_time"]) == "2025-09-01 13:00:00+00:00"
    assert norte.iloc[0]["precip_mm"] == 0.4 and norte.iloc[0]["temp_c"] == 21.5
    assert norte.iloc[1]["precip_mm"] == 1.2 and np.isnan(norte.iloc[1]["temp_c"])
    assert np.isnan(norte.iloc[0]["hr_pct"])  # variable ausente en el export

    sur = frame[frame["station_id"] == "MARR0002"].iloc[0]
    assert sur["precip_mm"] == 0.0 and np.isnan(sur["temp_c"])  # -999,99 -> sin dato


def test_reading_with_missing_optional_variables_round_trips(tmp_path):
    path = tmp_path / "export.csv"
    write_fixture(path)
    frame, _ = from_marr_long(path)
    record = frame[frame["station_id"] == "MARR0001"].sort_values("event_time").iloc[1].to_dict()
    reading = reading_from_row(record, source="test")
    decoded = decode_event(encode_event(reading))
    assert decoded["precip_mm"] == 1.2
    assert decoded["temp_c"] is None and decoded["hr_pct"] is None
