import json

import pytest

from hidromet_streaming.contracts import decode_event, encode_event, make_event_id, reading_from_row


def base_row(**overrides):
    row = {
        "station_id": "ITA01",
        "station_name": "Hernandarias",
        "subcuenca": "margen-derecha-sur",
        "lat": -25.4,
        "lon": -54.62,
        "seq": 7,
        "event_time": "2023-10-28T14:10:00Z",
        "precip_mm": 3.2,
        "temp_c": 27.4,
        "hr_pct": 81,
        "presion_hpa": 1004.6,
        "viento_ms": 4.8,
        "viento_dir_deg": 135,
        "rafaga_ms": 9.1,
        "radiacion_wm2": 512,
    }
    row.update(overrides)
    return row


def test_event_id_is_deterministic():
    assert make_event_id("ITA01", "2023-10-28T14:10:00Z") == "ema-ITA01-20231028T141000Z"
    assert make_event_id("ITA01", "2023-10-28T11:10:00-03:00") == "ema-ITA01-20231028T141000Z"


def test_round_trip_preserves_contract():
    reading = reading_from_row(base_row(), source="test")
    decoded = decode_event(encode_event(reading))
    assert decoded["event_id"] == "ema-ITA01-20231028T141000Z"
    assert decoded["event_type"] == "ema.lectura"
    assert decoded["precip_mm"] == 3.2
    assert decoded["subcuenca"] == "margen-derecha-sur"


def test_decode_rejects_missing_fields_and_out_of_range_values():
    reading = reading_from_row(base_row(), source="test").as_dict()
    del reading["precip_mm"]
    with pytest.raises(ValueError, match="faltan campos"):
        decode_event(json.dumps(reading))

    bad = reading_from_row(base_row(temp_c=72.0), source="test").as_dict()
    with pytest.raises(ValueError, match="fuera del rango"):
        decode_event(json.dumps(bad))


def test_decode_rejects_naive_timestamps():
    payload = reading_from_row(base_row(), source="test").as_dict()
    payload["event_time"] = "2023-10-28T14:10:00"
    with pytest.raises(ValueError, match="zona horaria"):
        decode_event(json.dumps(payload))
