"""Visualize candidate cluster definitions over price, for eyeball comparison.

python engine/cluster_viz.py outputs/grid_replica_nne_2026-07-27.json
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(sys.executable).parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent))

from daimon_runtime import setup_plot
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta

from signals import load_days, detect, cluster_trades

setup_plot()


def s2d(n):
    return datetime(1899, 12, 30) + timedelta(days=float(n))


DEFS = [
    ("D1: strict col-A color runs", dict(method="d1", min_len=2)),
    ("D2: col-A runs, tolerate 1 opposite day", dict(method="d2", tolerance=2, min_len=3)),
    ("D3: row-score hysteresis (>=+6 in, <=0 out)", dict(method="d3", enter=6.0, exit=0.0, min_len=3)),
]


def main(path):
    days = load_days(path)
    dates = [s2d(d["date"]) for d in days]
    closes = [d["close"] for d in days]

    fig, axes = plt.subplots(len(DEFS) + 1, 1, figsize=(13, 11), sharex=True,
                             gridspec_kw={"height_ratios": [1.2] + [1] * len(DEFS)})

    # top: raw color signals
    ax = axes[0]
    ax.plot(dates, closes, color="black", lw=1.2)
    for i, d in enumerate(days):
        col = {"green": "#2ca02c", "red": "#d62728"}.get(d["a_family"])
        if col:
            ax.axvspan(dates[i], dates[min(i + 1, len(dates) - 1)], color=col, alpha=0.25)
    ax.set_title(f"{Path(path).stem} — top: col-A daily color; "
                 f"below: candidate cluster definitions", fontsize=11)

    for ax, (label, kw) in zip(axes[1:], DEFS):
        ax.plot(dates, closes, color="black", lw=1.2)
        clusters = detect(days, **kw)
        trades = cluster_trades(days, clusters)
        for c, t in zip(clusters, trades):
            col = "#2ca02c" if c["side"] == 1 else "#d62728"
            ax.axvspan(dates[c["start"]], dates[min(c["end"] - 1, len(dates) - 1)],
                       color=col, alpha=0.30)
            mk = "^" if c["side"] == 1 else "v"
            ax.plot(dates[c["start"]], t["entry"], marker=mk, color=col,
                    markersize=9, markeredgecolor="black")
            ax.plot(dates[c["end"] - 1], t["exit"], marker="o", color=col,
                    markersize=7, markeredgecolor="black")
        total = sum(t["ret"] for t in trades)
        wins = sum(1 for t in trades if t["ret"] > 0)
        ax.set_ylabel(label.split(":")[0], fontsize=9)
        ax.set_title(f"{label}   |   {len(trades)} clusters, {wins} winning, "
                     f"sum of returns {total:+.1%}", fontsize=10, loc="left")

    for ax in axes:
        ax.grid(alpha=0.25)
    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
    fig.tight_layout()
    out = Path("outputs") / f"clusters_{Path(path).stem}.png"
    fig.savefig(out, dpi=160, bbox_inches="tight")
    print("saved", out)


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "outputs/grid_replica_nne_2026-07-27.json")
