"""Simulación de la política temporal de la Tarea 2.

Genera, de forma determinista, las tablas de evidencia que se incluyen en el
documento: secuencia de eventos (tiempo de evento vs. llegada), ventana
asignada, progreso del watermark, panes early / on-time / late y eventos
descartados. Ejecutar:

    python simulacion.py            # imprime las tablas en Markdown
    python simulacion.py --json     # imprime el resultado como JSON

No requiere Apache Beam: el objetivo es razonar sobre la política, no
implementarla. La semántica reproduce la de Beam:

- ventanas fijas alineadas al epoch, asignadas por tiempo de evento;
- watermark heurístico: `max(event_time visto) - holgura`, monótono;
- pane early cada N segundos de tiempo de procesamiento mientras la ventana
  esté abierta y haya datos nuevos;
- pane on-time cuando el watermark supera el fin de ventana;
- pane late por cada elemento que llega después del on-time y antes de
  `fin + allowed_lateness`;
- modo ACCUMULATING: cada pane emite el total completo de la ventana.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

WINDOW = timedelta(minutes=10)
ALLOWED_LATENESS = timedelta(minutes=20)
WATERMARK_SLACK = timedelta(minutes=2)  # holgura heurística del watermark
EARLY_EVERY = timedelta(minutes=5)  # pane early por processing time

T0 = datetime(2026, 9, 24, 14, 0, tzinfo=UTC)


def t(minutes: float) -> datetime:
    return T0 + timedelta(minutes=minutes)


@dataclass
class Event:
    event_id: str
    station: str
    event_time: datetime
    arrival_time: datetime
    precip_mm: float
    note: str = ""


# Secuencia observada (orden de llegada). Cuatro estaciones con distinta
# calidad de enlace: ITA01 y ITA07 (fibra), ITA12 (GPRS, ráfagas), ITA23
# (satelital, muy atrasada). Las lecturas nominales son cada 10 min.
EVENTS: list[Event] = [
    Event("ITA01-1400", "ITA01", t(0), t(0.2), 0.0),
    Event("ITA07-1400", "ITA07", t(0), t(0.3), 0.4),
    Event("ITA01-1410", "ITA01", t(10), t(10.2), 1.2),
    Event("ITA07-1410", "ITA07", t(10), t(10.3), 2.8),
    Event("ITA12-1400", "ITA12", t(0), t(11.0), 0.6, "llega en una ráfaga GPRS, 11 min después de medir"),
    Event("ITA12-1410", "ITA12", t(10), t(11.0), 3.1, "viene en la misma ráfaga"),
    Event("ITA01-1420", "ITA01", t(20), t(20.2), 4.5),
    Event("ITA07-1420", "ITA07", t(20), t(20.4), 5.0),
    Event("ITA07-1410", "ITA07", t(10), t(20.9), 2.8, "el datalogger la reenvía (duplicado)"),
    Event("ITA12-1420", "ITA12", t(20), t(22.5), 4.0),
    Event("ITA01-1430", "ITA01", t(30), t(30.2), 6.1),
    Event("ITA07-1430", "ITA07", t(30), t(30.3), 7.4),
    Event("ITA23-1400", "ITA23", t(0), t(31.0), 0.2, "llega por satélite, todavía dentro de la tolerancia"),
    Event("ITA23-1410", "ITA23", t(10), t(31.0), 2.2, "llega por satélite, todavía dentro de la tolerancia"),
    Event("ITA12-1430", "ITA12", t(30), t(32.0), 6.8),
    Event("ITA01-1440", "ITA01", t(40), t(40.2), 3.0),
    Event("ITA07-1440", "ITA07", t(40), t(40.3), 2.1),
    Event("ITA12-1440", "ITA12", t(40), t(41.5), 2.9),
    Event("ITA01-1450", "ITA01", t(50), t(50.2), 0.9),
    Event("ITA07-1450", "ITA07", t(50), t(50.3), 0.5),
    Event("ITA23-1430", "ITA23", t(30), t(52.0), 5.5, "llega por satélite, todavía dentro de la tolerancia"),
    Event("ITA01-1500", "ITA01", t(60), t(60.2), 0.0),
    Event("ITA07-1500", "ITA07", t(60), t(60.3), 0.0),
    Event("ITA23-1420", "ITA23", t(20), t(61.0), 3.8, "llega por satélite cuando su ventana ya expiró"),
]


def window_of(ts: datetime) -> tuple[datetime, datetime]:
    epoch = int(ts.timestamp())
    size = int(WINDOW.total_seconds())
    start = datetime.fromtimestamp(epoch - epoch % size, tz=UTC)
    return start, start + WINDOW


@dataclass
class WindowState:
    start: datetime
    end: datetime
    seen_ids: set[str] = field(default_factory=set)
    total: float = 0.0
    count: int = 0
    on_time_fired: bool = False
    early_due: datetime | None = None  # timer de processing time pendiente
    dirty: bool = False


def fmt(ts: datetime) -> str:
    return ts.strftime("%H:%M:%S") if ts.second else ts.strftime("%H:%M")


def simulate() -> dict:
    windows: dict[datetime, WindowState] = {}
    watermark = datetime.min.replace(tzinfo=UTC)
    max_event_time = datetime.min.replace(tzinfo=UTC)
    audit: list[dict] = []
    panes: list[dict] = []

    def fire(ws: WindowState, timing: str, at: datetime) -> None:
        panes.append(
            {
                "window": f"[{fmt(ws.start)}, {fmt(ws.end)})",
                "timing": timing,
                "fired_at": fmt(at),
                "total_mm": round(ws.total, 1),
                "n": ws.count,
            }
        )
        ws.dirty = False

    def fire_due_timers(now: datetime) -> None:
        """Dispara los timers early cuyo plazo de processing time venció."""
        for w in sorted(windows.values(), key=lambda w: w.start):
            while w.early_due is not None and w.early_due <= now and not w.on_time_fired:
                fire(w, "EARLY", w.early_due)
                w.early_due = w.early_due + EARLY_EVERY if w.dirty else None

    def advance_watermark(now: datetime) -> None:
        nonlocal watermark
        candidate = max_event_time - WATERMARK_SLACK
        if candidate > watermark:
            watermark = candidate
        # on-time: ventanas cuyo fin quedó por debajo del watermark
        for ws in sorted(windows.values(), key=lambda w: w.start):
            if not ws.on_time_fired and watermark >= ws.end:
                ws.on_time_fired = True
                fire(ws, "ON_TIME", now)

    for ev in EVENTS:
        now = ev.arrival_time
        fire_due_timers(now)
        start, end = window_of(ev.event_time)
        ws = windows.setdefault(start, WindowState(start, end))
        delay = (ev.arrival_time - ev.event_time).total_seconds() / 60

        # 1) ¿la ventana ya expiró? (watermark > fin + lateness)
        if watermark > ws.end + ALLOWED_LATENESS:
            decision, reason = "descartado", "too_late"
        elif ev.event_id in ws.seen_ids:
            decision, reason = "descartado", "duplicado"
        else:
            ws.seen_ids.add(ev.event_id)
            ws.total += ev.precip_mm
            ws.count += 1
            ws.dirty = True
            if ws.on_time_fired:
                decision, reason = "aceptado", "late (revisión)"
                fire(ws, "LATE", now)
            else:
                decision, reason = "aceptado", "on-time"

        max_event_time = max(max_event_time, ev.event_time)
        advance_watermark(now)

        # 2) armar el timer early: AfterProcessingTime(5 min) cuenta desde el
        #    primer elemento del pane actual
        if decision == "aceptado" and not ws.on_time_fired and ws.early_due is None:
            ws.early_due = now + EARLY_EVERY

        audit.append(
            {
                "event_id": ev.event_id,
                "station": ev.station,
                "event_time": fmt(ev.event_time),
                "arrival_time": fmt(ev.arrival_time),
                "delay_min": round(delay, 1),
                "window": f"[{fmt(start)}, {fmt(end)})",
                "watermark_after": fmt(watermark) if watermark > T0 - timedelta(days=1) else "-",
                "decision": decision,
                "reason": reason,
                "note": ev.note,
            }
        )

    # cierre del stream: el watermark avanza a infinito
    close_at = EVENTS[-1].arrival_time + timedelta(minutes=1)
    fire_due_timers(close_at)
    for ws in sorted(windows.values(), key=lambda w: w.start):
        if not ws.on_time_fired:
            ws.on_time_fired = True
            fire(ws, "ON_TIME (cierre)", close_at)

    finals = [
        {
            "window": f"[{fmt(ws.start)}, {fmt(ws.end)})",
            "total_mm": round(ws.total, 1),
            "n": ws.count,
            "estaciones": ws.count,
        }
        for ws in sorted(windows.values(), key=lambda w: w.start)
    ]
    return {"audit": audit, "panes": panes, "finals": finals}


def md_table(rows: list[dict], cols: list[tuple[str, str]]) -> str:
    head = "| " + " | ".join(h for _, h in cols) + " |"
    sep = "|" + "|".join("---" for _ in cols) + "|"
    body = [
        "| " + " | ".join(str(r.get(k, "")) for k, _ in cols) + " |" for r in rows
    ]
    return "\n".join([head, sep, *body])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    result = simulate()
    if args.json:
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return

    print("## Secuencia de eventos\n")
    print(
        md_table(
            result["audit"],
            [
                ("event_id", "event_id"),
                ("event_time", "t. evento"),
                ("arrival_time", "t. llegada"),
                ("delay_min", "atraso (min)"),
                ("window", "ventana"),
                ("watermark_after", "watermark tras procesar"),
                ("decision", "decisión"),
                ("reason", "razón"),
                ("note", "nota"),
            ],
        )
    )
    print("\n## Panes emitidos\n")
    print(
        md_table(
            result["panes"],
            [
                ("window", "ventana"),
                ("timing", "pane"),
                ("fired_at", "emitido a las"),
                ("total_mm", "total acumulado (mm)"),
                ("n", "lecturas"),
            ],
        )
    )
    print("\n## Resultado final por ventana\n")
    print(
        md_table(
            result["finals"],
            [("window", "ventana"), ("total_mm", "total (mm)"), ("n", "lecturas")],
        )
    )


if __name__ == "__main__":
    main()
