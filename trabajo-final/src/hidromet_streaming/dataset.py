"""Preparar el dataset de lecturas de 10 minutos que el productor reproduce.

Cuatro fuentes, todas escriben el mismo esquema en `data/processed/emas_10min.parquet`:

- ``openmeteo``: descarga datos horarios reales (reanálisis ERA5 vía la API
  pública de Open-Meteo, sin clave) para cada estación de la red y los
  desagrega a 10 minutos. La precipitación horaria se reparte entre los seis
  intervalos de forma determinista, conservando el total; el resto de las
  variables se interpola.
- ``sintetico``: generador estocástico determinista (sin red). Produce
  frentes de lluvia que cruzan la red de estaciones, ciclo diurno de
  temperatura y radiación, y ráfagas asociadas a la lluvia.
- ``csv``: adaptador genérico para un export *ancho* (una columna por
  variable) con un mapeo de columnas JSON.
- ``marr``: adaptador para el export *largo* del sistema de mediciones
  meteorológicas de la División de Embalse (MARR.CE) de Itaipú Binacional:
  una fila por (estación, instante, variable), separador ``;``, decimales con
  coma, fechas ``dd/mm/aaaa`` en hora local. Se pivotea a una fila por
  (estación, instante).

Con ``csv`` y ``marr``, ``--start`` y ``--end`` recortan el export a ese
período (fechas UTC, con el día de ``--end`` incluido). Los datos de esas dos
fuentes no van en el repositorio.

Ejecutar:

    python -m hidromet_streaming.dataset --source openmeteo --start 2023-10-28 --end 2023-11-04
    python -m hidromet_streaming.dataset --source sintetico --days 3
    python -m hidromet_streaming.dataset --source csv --file export.csv --column-map mapa.json
    python -m hidromet_streaming.dataset --source marr --file export.csv --start 2025-09-01
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from hidromet_streaming.config import processed_dataset_path, project_root, stations_path
from hidromet_streaming.contracts import PHYSICAL_RANGES, VARIABLES

OPEN_METEO_ARCHIVE = "https://archive-api.open-meteo.com/v1/archive"
HOURLY_VARIABLES = {
    "temperature_2m": "temp_c",
    "relative_humidity_2m": "hr_pct",
    "precipitation": "precip_mm",
    "surface_pressure": "presion_hpa",
    "wind_speed_10m": "viento_ms",
    "wind_direction_10m": "viento_dir_deg",
    "wind_gusts_10m": "rafaga_ms",
    "shortwave_radiation": "radiacion_wm2",
}
STEP = timedelta(minutes=10)
STEPS_PER_HOUR = 6

# Fechas por defecto: episodio de lluvias intensas en el Alto Paraná durante
# El Niño 2023 (fines de octubre / inicios de noviembre). Cambiar con --start/--end.
DEFAULT_START = "2023-10-28"
DEFAULT_END = "2023-11-04"


@dataclass(frozen=True)
class Station:
    station_id: str
    station_name: str
    subcuenca: str
    lat: float
    lon: float
    enlace: str  # fibra | gprs | satelital: define el perfil de atraso en el replay


# Red de ejemplo en el área de influencia del embalse (margen derecha). Las
# coordenadas son aproximadas y públicas; los nombres son de localidades, no de
# estaciones reales.
STATIONS: tuple[Station, ...] = (
    Station("ITA01", "Hernandarias", "margen-derecha-sur", -25.40, -54.62, "fibra"),
    Station("ITA02", "Ciudad del Este", "margen-derecha-sur", -25.51, -54.61, "fibra"),
    Station("ITA03", "Presidente Franco", "monday", -25.56, -54.60, "fibra"),
    Station("ITA04", "Minga Guazú", "monday", -25.49, -54.75, "gprs"),
    Station("ITA05", "Yguazú", "acaray", -25.47, -55.00, "gprs"),
    Station("ITA06", "Itakyry", "acaray", -24.93, -55.20, "gprs"),
    Station("ITA07", "San Alberto", "margen-derecha-centro", -24.97, -54.90, "gprs"),
    Station("ITA08", "Mbaracayú", "margen-derecha-centro", -24.55, -54.50, "gprs"),
    Station("ITA09", "Nueva Esperanza", "margen-derecha-centro", -24.48, -54.83, "satelital"),
    Station("ITA10", "Katueté", "margen-derecha-norte", -24.25, -54.75, "satelital"),
    Station("ITA11", "Salto del Guairá", "margen-derecha-norte", -24.06, -54.31, "satelital"),
    Station("ITA12", "Santa Rita", "nacunday", -25.80, -55.07, "gprs"),
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


# --------------------------------------------------------------------------- #
# Fuente 1: Open-Meteo (ERA5) horario -> 10 minutos
# --------------------------------------------------------------------------- #


def fetch_openmeteo_hourly(station: Station, start: str, end: str, *, cache: Path) -> pd.DataFrame:
    """Descargar (o reutilizar del cache) las series horarias de una estación."""
    cache.mkdir(parents=True, exist_ok=True)
    target = cache / f"openmeteo_{station.station_id}_{start}_{end}.json"
    if not target.exists() or target.stat().st_size == 0:
        query = urllib.parse.urlencode(
            {
                "latitude": station.lat,
                "longitude": station.lon,
                "start_date": start,
                "end_date": end,
                "hourly": ",".join(HOURLY_VARIABLES),
                "wind_speed_unit": "ms",
                "timezone": "UTC",
            }
        )
        request = urllib.request.Request(
            f"{OPEN_METEO_ARCHIVE}?{query}", headers={"User-Agent": "fpuna-streaming-tp/1.0"}
        )
        with urllib.request.urlopen(request, timeout=120) as response:
            target.write_bytes(response.read())
    payload = json.loads(target.read_text(encoding="utf-8"))
    hourly = payload["hourly"]
    frame = pd.DataFrame({"time": pd.to_datetime(hourly["time"], utc=True)})
    for source_name, target_name in HOURLY_VARIABLES.items():
        frame[target_name] = pd.to_numeric(pd.Series(hourly[source_name]), errors="coerce")
    return frame


def _split_hour_precip(total_mm: float, rng: np.random.Generator) -> np.ndarray:
    """Repartir la lluvia de una hora en seis intervalos conservando el total.

    Usa pesos Dirichlet concentrados: la lluvia convectiva del Alto Paraná se
    concentra en pocos intervalos, no cae uniforme. Redondea a 0,1 mm (paso de
    un pluviómetro de cangilones) y ajusta el residuo al intervalo mayor.
    """
    if total_mm <= 0:
        return np.zeros(STEPS_PER_HOUR)
    weights = rng.dirichlet(np.full(STEPS_PER_HOUR, 0.6))
    parts = np.round(weights * total_mm, 1)
    residual = round(total_mm - parts.sum(), 1)
    parts[int(np.argmax(parts))] += residual
    return np.clip(parts, 0.0, None)


def disaggregate_to_10min(hourly: pd.DataFrame, *, seed: int) -> pd.DataFrame:
    """Convertir series horarias en lecturas de 10 minutos.

    - variables de estado (temperatura, humedad, presión, viento, radiación):
      interpolación lineal entre horas consecutivas más un ruido pequeño;
    - precipitación: reparto determinista de cada total horario (ver arriba).
    """
    hourly = hourly.sort_values("time").reset_index(drop=True)
    rng = np.random.default_rng(seed)
    times = pd.date_range(hourly["time"].iloc[0], hourly["time"].iloc[-1], freq="10min", tz=UTC)
    out = pd.DataFrame({"event_time": times})
    hourly_indexed = hourly.set_index("time")
    for name in ("temp_c", "hr_pct", "presion_hpa", "viento_ms", "rafaga_ms", "radiacion_wm2"):
        series = hourly_indexed[name].reindex(times).interpolate(limit_direction="both")
        noise_scale = {
            "temp_c": 0.15,
            "hr_pct": 0.8,
            "presion_hpa": 0.1,
            "viento_ms": 0.25,
            "rafaga_ms": 0.4,
            "radiacion_wm2": 8.0,
        }[name]
        out[name] = (series.to_numpy() + rng.normal(0, noise_scale, len(times))).round(1)
    direction = hourly_indexed["viento_dir_deg"].reindex(times).ffill().bfill()
    out["viento_dir_deg"] = ((direction.to_numpy() + rng.normal(0, 8, len(times))) % 360).round(0)

    precip = np.zeros(len(times))
    hour_of = pd.Series(times).dt.floor("h")
    for hour, total in hourly_indexed["precip_mm"].fillna(0.0).items():
        mask = (hour_of == hour).to_numpy()
        n = int(mask.sum())
        if n == 0:
            continue
        parts = _split_hour_precip(float(total), rng)[:n]
        precip[mask] = parts
    out["precip_mm"] = precip.round(1)
    return _clip_physical(out)


def _clip_physical(frame: pd.DataFrame) -> pd.DataFrame:
    for name, (low, high) in PHYSICAL_RANGES.items():
        frame[name] = frame[name].clip(low, high)
    frame["hr_pct"] = frame["hr_pct"].round(0)
    frame["rafaga_ms"] = np.maximum(frame["rafaga_ms"], frame["viento_ms"]).round(1)
    frame["radiacion_wm2"] = frame["radiacion_wm2"].clip(lower=0).round(0)
    return frame


# --------------------------------------------------------------------------- #
# Fuente 2: generador sintético (sin red)
# --------------------------------------------------------------------------- #


def synthetic_station(station: Station, start: datetime, days: int, *, seed: int) -> pd.DataFrame:
    """Lecturas sintéticas plausibles para una estación durante `days` días."""
    rng = np.random.default_rng(seed)
    n = days * 24 * STEPS_PER_HOUR
    times = pd.date_range(start, periods=n, freq="10min", tz=UTC)
    hours = (times.hour + times.minute / 60).to_numpy() - 3  # hora local aprox. (UTC-3)
    diurnal = np.sin((hours - 9) / 24 * 2 * np.pi)  # mínimo ~6 h local, máximo ~15 h local
    lat_offset = (station.lat + 25.0) * 0.8  # más cálido hacia el norte
    temp = 24.0 + lat_offset + 5.5 * diurnal + rng.normal(0, 0.3, n)

    # Frentes de lluvia: 2–3 por semana, se desplazan de SO a NE con ~1 h de
    # retraso entre subcuencas; intensidad decae con el tiempo.
    precip = np.zeros(n)
    front_rng = np.random.default_rng(seed // 1000)  # compartido por toda la red
    n_fronts = max(1, int(round(days * 0.45)))
    for _ in range(n_fronts):
        start_idx = int(front_rng.integers(0, max(1, n - 12 * STEPS_PER_HOUR)))
        delay = int(round(((station.lat + 25.9) + (station.lon + 55.3)) * 3))  # hacia el NE
        duration = int(front_rng.integers(2, 7)) * STEPS_PER_HOUR
        peak = float(front_rng.uniform(4, 22))
        for k in range(duration):
            idx = start_idx + delay + k
            if 0 <= idx < n:
                shape = math.exp(-((k - duration * 0.3) ** 2) / (2 * (duration * 0.25) ** 2))
                precip[idx] += max(0.0, rng.gamma(2.0, peak * shape / 2.0 + 1e-3) * 0.5)
    precip = np.round(precip, 1)

    wet = precip > 0
    hr = np.clip(62 - 12 * diurnal + 25 * wet + rng.normal(0, 3, n), 20, 100).round(0)
    presion = 1010 - 2.5 * diurnal - 4 * wet + rng.normal(0, 0.4, n)
    viento = np.clip(2.5 + 1.5 * np.abs(diurnal) + 3.0 * wet + rng.normal(0, 0.6, n), 0, None)
    rafaga = viento * rng.uniform(1.3, 2.4, n) + 6.0 * wet * (precip > 8)
    direccion = (135 + 60 * diurnal + rng.normal(0, 20, n)) % 360
    radiacion = np.clip(950 * np.sin((hours - 6) / 12 * np.pi), 0, None) * (1 - 0.8 * wet)
    temp = temp - 3.0 * wet

    frame = pd.DataFrame(
        {
            "event_time": times,
            "precip_mm": precip,
            "temp_c": temp.round(1),
            "hr_pct": hr,
            "presion_hpa": presion.round(1),
            "viento_ms": viento.round(1),
            "viento_dir_deg": direccion.round(0),
            "rafaga_ms": rafaga.round(1),
            "radiacion_wm2": radiacion.round(0),
        }
    )
    return _clip_physical(frame)


# --------------------------------------------------------------------------- #
# Fuente 3: export CSV de estaciones reales
# --------------------------------------------------------------------------- #


def from_csv(path: Path, column_map: dict[str, str], *, tz: str = "UTC") -> pd.DataFrame:
    """Adaptar un export real al esquema del proyecto.

    `column_map` asocia cada campo del esquema (`station_id`, `event_time`, las
    variables de `contracts.VARIABLES` y, si el export los trae, los metadatos
    `station_name`, `subcuenca`, `lat`, `lon` y `enlace`) con el nombre de
    columna del export. Sólo la precipitación es obligatoria: las demás
    variables ausentes quedan en NaN, y las filas sin precipitación se
    descartan dejando constancia en el manifiesto.
    """
    raw = pd.read_csv(path)
    frame = pd.DataFrame()
    frame["station_id"] = raw[column_map["station_id"]].astype(str)
    stamps = pd.to_datetime(raw[column_map["event_time"]], errors="coerce")
    if stamps.dt.tz is None:
        stamps = stamps.dt.tz_localize(tz)
    frame["event_time"] = stamps.dt.tz_convert(UTC)
    for name in VARIABLES:
        column = column_map.get(name)
        frame[name] = pd.to_numeric(raw[column], errors="coerce") if column in raw else np.nan
    for name in ("station_name", "subcuenca", "enlace"):
        column = column_map.get(name)
        if column in raw:
            frame[name] = raw[column].astype(str)
    for name in ("lat", "lon"):
        column = column_map.get(name)
        if column in raw:
            frame[name] = pd.to_numeric(raw[column], errors="coerce")
    return frame


def clip_period(frame: pd.DataFrame, start: str | None, end: str | None) -> pd.DataFrame:
    """Recortar un export al período [start, end]: fechas UTC, con el día de `end` incluido."""
    if start:
        frame = frame[frame["event_time"] >= pd.Timestamp(start, tz="UTC")]
    if end:
        frame = frame[frame["event_time"] < pd.Timestamp(end, tz="UTC") + pd.Timedelta(days=1)]
    return frame


# --------------------------------------------------------------------------- #
# Fuente 4: export largo de MARR.CE (Itaipú Binacional)
# --------------------------------------------------------------------------- #

MARR_VARIABLES = {
    "Precipitación": "precip_mm",
    "Temperatura del Aire": "temp_c",
    "Humedad Relativa": "hr_pct",
    "Presión Atmosférica": "presion_hpa",
    "Velocidad del Viento": "viento_ms",
    "Dirección del Viento": "viento_dir_deg",
    "Ráfaga": "rafaga_ms",
    "Radiación Solar": "radiacion_wm2",
}
MARR_MISSING = -999.0
MARR_LOCAL_TZ = "America/Asuncion"


def from_marr_long(
    path: Path, *, tz: str = MARR_LOCAL_TZ
) -> tuple[pd.DataFrame, dict[str, object]]:
    """Pivotear el export largo de MARR.CE al esquema de 10 minutos del proyecto.

    Decisiones:

    - sólo mediciones automáticas (``Tipo Medicion == auto``); las manuales
      (evaporación de tanque/Piche) son diarias y no pertenecen al stream;
    - el centinela ``-999,99`` es "sin dato" y se convierte en ``NaN``;
    - las variables ausentes en el export quedan en ``NaN`` (el contrato sólo
      exige precipitación);
    - las filas repetidas (misma estación, instante y variable) se conservan
      **una** vez aquí porque el productor las vuelve a inyectar de forma
      controlada; su cantidad se registra en el manifiesto como evidencia de
      que el fenómeno existe en la fuente real;
    - la subcuenca se asigna por banda de latitud (norte / centro / sur del
      embalse), porque el export no informa la subcuenca.
    """
    raw = pd.read_csv(path, sep=";", encoding="utf-8-sig", dtype=str)
    raw.columns = [c.strip() for c in raw.columns]
    auto = raw[raw["Tipo Medicion"].str.strip().str.lower() == "auto"].copy()
    auto["variable"] = auto["Nombre Variable"].str.strip().map(MARR_VARIABLES)
    auto = auto.dropna(subset=["variable"])
    auto["value"] = pd.to_numeric(
        auto["Valor Medicion"].str.replace(",", ".", regex=False), errors="coerce"
    )
    auto.loc[auto["value"] <= MARR_MISSING, "value"] = np.nan
    stamps = pd.to_datetime(auto["Fecha y hora de medicion"], dayfirst=True, errors="coerce")
    auto["event_time"] = stamps.dt.tz_localize(
        tz, ambiguous="NaT", nonexistent="NaT"
    ).dt.tz_convert(UTC)
    auto = auto.dropna(subset=["event_time"])
    auto["station_id"] = "MARR" + auto["Codigo Estacion"].str.strip().str[-4:]

    duplicated_rows = int(auto.duplicated(["station_id", "event_time", "variable"]).sum())
    dedup = auto.drop_duplicates(["station_id", "event_time", "variable"], keep="first")
    wide = dedup.pivot_table(
        index=["station_id", "event_time"], columns="variable", values="value", aggfunc="first"
    ).reset_index()
    for name in VARIABLES:
        if name not in wide:
            wide[name] = np.nan

    meta = (
        dedup.groupby("station_id")
        .agg(
            station_name=("Nombre Estacion", "first"),
            lat=("Latitud Estacion", "first"),
            lon=("Longitud Estacion", "first"),
        )
        .reset_index()
    )
    for column in ("lat", "lon"):
        meta[column] = pd.to_numeric(
            meta[column].str.replace(",", ".", regex=False), errors="coerce"
        )
    meta["subcuenca"] = pd.cut(
        meta["lat"],
        bins=[-90, -25.3, -24.6, 90],
        labels=["embalse-sur", "embalse-centro", "embalse-norte"],
    ).astype(str)
    # El export no informa el tipo de enlace: se asignan perfiles rotativos
    # (fibra / gprs / satelital) para que el replay reproduzca atraso y desorden.
    profiles = ["fibra", "gprs", "satelital"]
    meta = meta.sort_values("station_id").reset_index(drop=True)
    meta["enlace"] = [profiles[i % len(profiles)] for i in range(len(meta))]
    frame = wide.merge(meta, on="station_id", how="left")

    variables_present = sorted(v for v in MARR_VARIABLES.values() if frame[v].notna().any())
    info = {
        "rows_in_export": int(len(raw)),
        "auto_rows": int(len(auto)),
        "duplicated_rows_in_source": duplicated_rows,
        "missing_sentinel_rows": int(
            (
                pd.to_numeric(
                    raw["Valor Medicion"].str.replace(",", ".", regex=False), errors="coerce"
                )
                <= MARR_MISSING
            ).sum()
        ),
        "variables_present": variables_present,
        "stations": meta["station_id"].tolist(),
        "source_timezone": tz,
    }
    return frame, info


# --------------------------------------------------------------------------- #
# Orquestación
# --------------------------------------------------------------------------- #


def _attach_station_metadata(frame: pd.DataFrame) -> pd.DataFrame:
    meta = pd.DataFrame([asdict(s) for s in STATIONS])
    if "station_name" in frame:  # la fuente ya trae sus metadatos (p. ej. MARR.CE)
        merged = frame.copy()
        for column in ("station_name", "subcuenca", "enlace", "lat", "lon"):
            if column not in merged:
                merged[column] = np.nan
    else:
        merged = frame.merge(meta, on="station_id", how="left")
    merged["station_name"] = merged["station_name"].fillna(merged["station_id"])
    merged["subcuenca"] = merged["subcuenca"].fillna("desconocida")
    merged["enlace"] = merged["enlace"].fillna("gprs")
    merged["lat"] = merged["lat"].fillna(0.0)
    merged["lon"] = merged["lon"].fillna(0.0)
    merged = merged.sort_values(["event_time", "station_id"]).reset_index(drop=True)
    merged["seq"] = merged.groupby("station_id").cumcount() + 1
    merged["flag_qc"] = "OK"
    return merged


def prepare_dataset(
    *,
    source: str = "sintetico",
    start: str | None = None,
    end: str | None = None,
    days: int = 3,
    csv_file: Path | None = None,
    column_map: dict[str, str] | None = None,
    seed: int = 20260924,
) -> dict[str, object]:
    root = project_root()
    frames: list[pd.DataFrame] = []
    dropped = 0
    outside_period = 0
    extra: dict[str, object] = {}
    if source == "openmeteo":
        for i, station in enumerate(STATIONS):
            hourly = fetch_openmeteo_hourly(
                station, start or DEFAULT_START, end or DEFAULT_END, cache=root / "data/cache"
            )
            ten_min = disaggregate_to_10min(hourly, seed=seed + i)
            ten_min["station_id"] = station.station_id
            frames.append(ten_min)
    elif source == "sintetico":
        start_dt = datetime.fromisoformat(start or DEFAULT_START).replace(tzinfo=UTC)
        for i, station in enumerate(STATIONS):
            frame = synthetic_station(station, start_dt, days, seed=seed * 1000 + i)
            frame["station_id"] = station.station_id
            frames.append(frame)
    elif source == "csv":
        if csv_file is None or column_map is None:
            raise ValueError("--file y --column-map son obligatorios con --source csv")
        frame = from_csv(csv_file, column_map)
        before = len(frame)
        frame = frame.dropna(subset=["event_time", "precip_mm"])
        dropped = before - len(frame)
        frames.append(frame)
    elif source == "marr":
        if csv_file is None:
            raise ValueError("--file es obligatorio con --source marr")
        frame, extra = from_marr_long(csv_file)
        before = len(frame)
        frame = frame.dropna(subset=["event_time", "precip_mm"])
        dropped = before - len(frame)
        frames.append(frame)
    else:
        raise ValueError(f"fuente desconocida: {source}")
    if source in ("csv", "marr") and (start or end):
        clipped = clip_period(frames[0], start, end)
        outside_period = len(frames[0]) - len(clipped)
        if clipped.empty:
            raise ValueError(f"el export no tiene lecturas entre {start} y {end}")
        frames = [clipped]

    dataset = _attach_station_metadata(pd.concat(frames, ignore_index=True))
    output = processed_dataset_path()
    output.parent.mkdir(parents=True, exist_ok=True)
    dataset.to_parquet(output, index=False)
    (
        dataset[["station_id", "station_name", "subcuenca", "lat", "lon", "enlace"]]
        .drop_duplicates("station_id")
        .sort_values("station_id")
        .to_csv(stations_path(), index=False)
    )

    manifest = {
        "source": source,
        "selection_start": str(dataset["event_time"].min()),
        "selection_end": str(dataset["event_time"].max()),
        "stations": int(dataset["station_id"].nunique()),
        "readings": int(len(dataset)),
        "dropped_incomplete_rows": dropped,
        "precip_total_mm_by_station": {
            k: round(float(v), 1)
            for k, v in dataset.groupby("station_id")["precip_mm"].sum().items()
        },
        "max_precip_10min_mm": round(float(dataset["precip_mm"].max()), 1),
        "seed": seed,
        "prepared_at": datetime.now(UTC).isoformat(),
    }
    if source == "openmeteo":
        manifest["upstream"] = OPEN_METEO_ARCHIVE
        manifest["license"] = "Open-Meteo (CC BY 4.0) sobre reanálisis ERA5 (Copernicus)"
    if source in ("csv", "marr") and csv_file is not None:
        manifest["csv_sha256"] = sha256(csv_file)
        manifest["csv_name"] = csv_file.name
        if start or end:
            manifest["period_filter"] = {
                "start": start,
                "end": end,
                "rows_outside_period": outside_period,
            }
    if source == "marr":
        manifest.update(extra)
        manifest["citation"] = (
            "Itaipú Binacional, División de Embalse (MARR.CE): mediciones meteorológicas "
            "automáticas de estaciones del área del embalse."
        )
    (output.parent / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source", choices=["openmeteo", "sintetico", "csv", "marr"], default="sintetico"
    )
    parser.add_argument(
        "--start",
        help=f"fecha inicial; por defecto {DEFAULT_START} (openmeteo, sintetico) o todo el export",
    )
    parser.add_argument(
        "--end",
        help=f"fecha final incluida; por defecto {DEFAULT_END} (openmeteo) o todo el export",
    )
    parser.add_argument("--days", type=int, default=3, help="sólo para --source sintetico")
    parser.add_argument("--file", type=Path)
    parser.add_argument("--column-map", type=Path, help="JSON campo_esquema -> columna_export")
    parser.add_argument("--seed", type=int, default=20260924)
    parser.add_argument(
        "--skip-if-exists",
        action="store_true",
        help="no regenerar si ya existe un dataset procesado (p. ej. de --source marr)",
    )
    parser.add_argument(
        "--fallback-sintetico",
        action="store_true",
        help="si la descarga falla, generar datos sintéticos en lugar de abortar",
    )
    args = parser.parse_args()
    column_map = json.loads(args.column_map.read_text()) if args.column_map else None
    existing = processed_dataset_path()
    if args.skip_if_exists and existing.exists() and existing.stat().st_size > 0:
        manifest_path = existing.parent / "manifest.json"
        current = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
        print(
            json.dumps(
                {"skipped": True, "existing": str(existing), "source": current.get("source")}
            )
        )
        return
    try:
        manifest = prepare_dataset(
            source=args.source,
            start=args.start,
            end=args.end,
            days=args.days,
            csv_file=args.file,
            column_map=column_map,
            seed=args.seed,
        )
    except (OSError, ValueError, KeyError) as error:
        if not args.fallback_sintetico or args.source == "sintetico":
            raise
        print(json.dumps({"warning": f"{type(error).__name__}: {error}", "fallback": "sintetico"}))
        manifest = prepare_dataset(
            source="sintetico", start=args.start, days=args.days, seed=args.seed
        )
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
