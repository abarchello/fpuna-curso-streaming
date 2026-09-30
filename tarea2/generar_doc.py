"""Genera README.md a partir de README.template.md y de la simulación.

Todas las tablas numéricas del documento salen de `simulacion.py`, de modo que
el texto y la evidencia no puedan desincronizarse. Ejecutar:

    python generar_doc.py
"""

from __future__ import annotations

from collections import defaultdict
from datetime import timedelta
from pathlib import Path

from simulacion import ALLOWED_LATENESS, EVENTS, WATERMARK_SLACK, fmt, md_table, simulate, window_of

HERE = Path(__file__).parent


def alternatives() -> list[dict]:
    """Totales por ventana bajo políticas alternativas, para la sección 6."""
    # Deduplicación siempre activa (misma regla en todas las políticas).
    seen: set[str] = set()
    unique = []
    for ev in EVENTS:
        if ev.event_id in seen:
            continue
        seen.add(ev.event_id)
        unique.append(ev)

    def totals(key_fn, accept_fn):
        acc: dict = defaultdict(float)
        max_et = None
        for ev in unique:
            max_et = ev.event_time if max_et is None else max(max_et, ev.event_time)
            wm = max_et - WATERMARK_SLACK
            if accept_fn(ev, wm):
                acc[key_fn(ev)] += ev.precip_mm
        return acc

    def et_window(ev):
        return window_of(ev.event_time)[0]

    def pt_window(ev):
        return window_of(ev.arrival_time)[0]

    ours = totals(et_window, lambda ev, wm: wm <= window_of(ev.event_time)[1] + ALLOWED_LATENESS)
    zero = totals(et_window, lambda ev, wm: wm <= window_of(ev.event_time)[1])
    infinite = totals(et_window, lambda ev, wm: True)
    proc = totals(pt_window, lambda ev, wm: True)

    starts = sorted(set(ours) | set(zero) | set(infinite) | set(proc))
    rows = []
    for s in starts:
        rows.append(
            {
                "window": f"[{fmt(s)}, {fmt(s + timedelta(minutes=10))})",
                "ours": f"{ours.get(s, 0.0):.1f}",
                "zero": f"{zero.get(s, 0.0):.1f}",
                "inf": f"{infinite.get(s, 0.0):.1f}",
                "proc": f"{proc.get(s, 0.0):.1f}",
            }
        )
    return rows


def main() -> None:
    result = simulate()
    events_table = md_table(
        result["audit"],
        [
            ("event_id", "event_id"),
            ("event_time", "t. evento"),
            ("arrival_time", "t. llegada"),
            ("delay_min", "atraso (min)"),
            ("window", "ventana asignada"),
            ("watermark_after", "watermark"),
            ("reason", "decisión"),
        ],
    )
    notes = "\n".join(
        f"- `{r['event_id']}`: {r['note']}." for r in result["audit"] if r["note"]
    )
    panes_table = md_table(
        result["panes"],
        [
            ("window", "ventana"),
            ("timing", "pane"),
            ("fired_at", "emitido a las"),
            ("total_mm", "acumulado (mm)"),
            ("n", "lecturas"),
        ],
    )
    finals_table = md_table(
        result["finals"],
        [("window", "ventana"), ("total_mm", "total final (mm)"), ("n", "lecturas")],
    )
    alt_table = md_table(
        alternatives(),
        [
            ("window", "ventana"),
            ("ours", "propuesta (evento, lateness 20)"),
            ("zero", "lateness 0"),
            ("inf", "lateness ∞"),
            ("proc", "ventanas por t. de llegada"),
        ],
    )

    n_total = len(result["audit"])
    n_acc = sum(1 for r in result["audit"] if r["decision"] == "aceptado")
    n_late = sum(1 for r in result["audit"] if r["reason"].startswith("late"))
    n_dup = sum(1 for r in result["audit"] if r["reason"] == "duplicado")
    n_too = sum(1 for r in result["audit"] if r["reason"] == "too_late")
    n_panes = len(result["panes"])
    n_early = sum(1 for p in result["panes"] if p["timing"] == "EARLY")
    n_latep = sum(1 for p in result["panes"] if p["timing"] == "LATE")

    template = (HERE / "README.template.md").read_text(encoding="utf-8")
    doc = (
        template.replace("{{TABLA_EVENTOS}}", events_table)
        .replace("{{NOTAS_EVENTOS}}", notes)
        .replace("{{TABLA_PANES}}", panes_table)
        .replace("{{TABLA_FINAL}}", finals_table)
        .replace("{{TABLA_ALTERNATIVAS}}", alt_table)
        .replace("{{N_TOTAL}}", str(n_total))
        .replace("{{N_ACC}}", str(n_acc))
        .replace("{{N_LATE}}", str(n_late))
        .replace("{{N_DUP}}", str(n_dup))
        .replace("{{N_TOO}}", str(n_too))
        .replace("{{N_PANES}}", str(n_panes))
        .replace("{{N_EARLY}}", str(n_early))
        .replace("{{N_LATEP}}", str(n_latep))
    )
    (HERE / "README.md").write_text(doc, encoding="utf-8")
    print("OK -> README.md")


if __name__ == "__main__":
    main()
