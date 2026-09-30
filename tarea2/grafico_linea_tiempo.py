"""Figura: tiempo de evento vs. tiempo de llegada, con ventanas y watermark.

Genera `img/linea_tiempo.png` a partir de la misma simulación que produce las
tablas del documento. Ejecutar: `python grafico_linea_tiempo.py`.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402

from simulacion import ALLOWED_LATENESS, EVENTS, T0, WATERMARK_SLACK, WINDOW  # noqa: E402

OUT = Path(__file__).parent / "img" / "linea_tiempo.png"

# Marcadores por decisión (forma + color, nunca color solo).
STYLE = {
    "on-time": dict(marker="o", color="#2a78d6", label="aceptado (on-time)"),
    "late": dict(marker="^", color="#eda100", label="aceptado tarde (revisión)"),
    "duplicado": dict(marker="s", color="#52514e", label="descartado: duplicado"),
    "too_late": dict(marker="X", color="#d03b3b", label="descartado: too late"),
}


def classify() -> list[tuple[object, str]]:
    """Replica la decisión de `simulacion.simulate` evento por evento."""
    from simulacion import simulate

    audit = simulate()["audit"]
    out = []
    for ev, row in zip(EVENTS, audit, strict=True):
        if row["reason"] == "duplicado":
            kind = "duplicado"
        elif row["reason"] == "too_late":
            kind = "too_late"
        elif row["reason"].startswith("late"):
            kind = "late"
        else:
            kind = "on-time"
        out.append((ev, kind))
    return out


def main() -> None:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(11, 6.2), dpi=150)
    fig.patch.set_facecolor("#fcfcfb")
    ax.set_facecolor("#fcfcfb")

    t_end = T0 + timedelta(minutes=70)

    # Bandas de ventana (eje y = tiempo de evento)
    w = T0
    shade = True
    while w < T0 + timedelta(minutes=70):
        if shade:
            ax.axhspan(w, w + WINDOW, color="#f0efec", zorder=0)
        shade = not shade
        w += WINDOW

    # Diagonal: llegada == evento (atraso cero)
    ax.plot([T0, t_end], [T0, t_end], color="#c3c2b7", lw=1, ls="--", zorder=1)
    ax.annotate("sin atraso", (t_end, t_end), xytext=(-52, -14),
                textcoords="offset points", color="#52514e", fontsize=8)

    # Watermark heurístico: max(event_time) - holgura, evaluado en cada llegada
    wm_x, wm_y, max_et = [], [], None
    for ev in EVENTS:
        max_et = ev.event_time if max_et is None else max(max_et, ev.event_time)
        wm_x.append(ev.arrival_time)
        wm_y.append(max_et - WATERMARK_SLACK)
    ax.step(wm_x, wm_y, where="post", color="#4a3aa7", lw=2, zorder=2,
            label="watermark (máx. t. evento − 2 min)")
    # Límite de expiración: watermark + lateness
    ax.step(wm_x, [y - ALLOWED_LATENESS for y in wm_y], where="post",
            color="#4a3aa7", lw=1.2, ls=":", zorder=2,
            label="watermark − lateness (20 min): ventanas por debajo expiran")

    for ev, kind in classify():
        st = STYLE[kind]
        ax.scatter(ev.arrival_time, ev.event_time, s=64, marker=st["marker"],
                   color=st["color"], edgecolor="#fcfcfb", linewidth=1.2, zorder=3)
    for kind, st in STYLE.items():
        ax.scatter([], [], s=64, marker=st["marker"], color=st["color"], label=st["label"])

    # Etiquetas selectivas
    for ev, kind in classify():
        if kind in ("late", "too_late", "duplicado") or ev.station == "ITA12" and ev.event_id.endswith("1400"):
            ax.annotate(ev.event_id, (ev.arrival_time, ev.event_time), xytext=(6, -3),
                        textcoords="offset points", fontsize=7.5, color="#52514e")

    fmt = mdates.DateFormatter("%H:%M")
    ax.xaxis.set_major_formatter(fmt)
    ax.yaxis.set_major_formatter(fmt)
    ax.xaxis.set_major_locator(mdates.MinuteLocator(byminute=range(0, 60, 10)))
    ax.yaxis.set_major_locator(mdates.MinuteLocator(byminute=range(0, 60, 10)))
    ax.set_xlim(T0 - timedelta(minutes=2), t_end)
    ax.set_ylim(T0 - timedelta(minutes=5), T0 + timedelta(minutes=66))
    ax.set_xlabel("Tiempo de llegada (processing time)")
    ax.set_ylabel("Tiempo de evento (bandas = ventanas fijas de 10 min)")
    ax.set_title("Eventos desordenados y tardíos frente al watermark", loc="left",
                 fontsize=12, color="#0b0b0b")
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    for spine in ("left", "bottom"):
        ax.spines[spine].set_color("#c3c2b7")
    ax.grid(True, color="#e6e5e1", lw=0.6)
    ax.legend(loc="upper left", fontsize=8, frameon=False)
    fig.tight_layout()
    fig.savefig(OUT)
    print(f"OK -> {OUT}")


if __name__ == "__main__":
    main()
