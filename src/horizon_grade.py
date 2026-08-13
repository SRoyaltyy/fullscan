"""Horizon grader — grade multi-timeframe calls (3d/1w/2w/1m) once enough
trading days have passed.

Predict runs (general + sector) record `horizon_calls` in their scoreboard
entry: {"HORIZON_1W": {"trading_days": 5, "direction": "up",
"magnitude_band": "mild", "confidence": 0.55}, ...}. Outcome runs record
`actual_close` per (date, topic) every day. This module walks the
scoreboard, finds calls whose T+h close now exists, and grades them:

    actual % change = close(T+h) / close(T) - 1
    direction hit   = predicted direction == actual direction
                      (flat threshold ±0.3% on the whole window)
    magnitude hit   = predicted band == actual band, with the band ladder
                      scaled by sqrt(h) — a "mild" month is bigger than a
                      "mild" day

Results are written back into each entry under `horizon_grades` (idempotent
— already-graded keys are skipped) and summarized in
03_scoreboard/HORIZON_BOARD.md.

No LLM, no external data — everything comes from the scoreboard's own
close history. CLI: python -m src.horizon_grade [--date YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
import math
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from . import config, scoreboard

HORIZON_LABELS = {"HORIZON_3D": "3d", "HORIZON_1W": "1w",
                  "HORIZON_2W": "2w", "HORIZON_1M": "1m"}
FLAT_PCT = 0.3           # |pct| below this over the whole window = flat
BAND_LADDER = (0.3, 1.0, 2.0)   # daily mild/notable/severe cutoffs


def _band(pct: float, days: int) -> str:
    scale = math.sqrt(max(days, 1))
    a = abs(pct)
    if a >= BAND_LADDER[2] * scale:
        return "severe"
    if a >= BAND_LADDER[1] * scale:
        return "notable"
    if a >= BAND_LADDER[0] * scale:
        return "mild"
    return "flat"


def _direction(pct: float) -> str:
    if pct > FLAT_PCT:
        return "up"
    if pct < -FLAT_PCT:
        return "down"
    return "flat"


def grade_board(board: dict) -> tuple[int, dict]:
    """Grade every matured horizon call. Returns (n_newly_graded, summary)."""
    # close series per topic: {topic: [(date, close), ...]} sorted by date
    series: dict[str, list[tuple[str, float]]] = {}
    for r in board.get("runs", []):
        if r.get("actual_close") is None:
            continue
        series.setdefault(r["topic"], []).append((r["date"], r["actual_close"]))
    for topic in series:
        series[topic].sort()

    graded = 0
    summary: dict[str, dict] = {}   # topic -> horizon_key -> stats

    for r in board.get("runs", []):
        calls = r.get("horizon_calls") or {}
        if not calls or r.get("actual_close") is None:
            continue
        topic = r["topic"]
        dates = series.get(topic, [])
        date_list = [d for d, _ in dates]
        if r["date"] not in date_list:
            continue
        idx = date_list.index(r["date"])
        grades = r.setdefault("horizon_grades", {})

        for key, hc in calls.items():
            if key in grades:
                continue
            h = int(hc.get("trading_days") or 0)
            if h <= 0 or idx + h >= len(dates):
                continue  # not matured yet
            t_close = dates[idx][1]
            h_date, h_close = dates[idx + h]
            if not t_close:
                continue
            pct = (h_close / t_close - 1) * 100
            adir = _direction(pct)
            aband = _band(pct, h)
            grades[key] = {
                "label": HORIZON_LABELS.get(key, key),
                "target_date": h_date,
                "actual_pct": round(pct, 2),
                "actual_direction": adir,
                "actual_magnitude_band": aband,
                "direction_hit": adir == hc.get("direction"),
                "magnitude_hit": aband == hc.get("magnitude_band"),
                "predicted_direction": hc.get("direction"),
                "predicted_magnitude_band": hc.get("magnitude_band"),
                "confidence": hc.get("confidence"),
            }
            graded += 1

    # rebuild summary from ALL graded calls (not just new ones)
    for r in board.get("runs", []):
        for key, g in (r.get("horizon_grades") or {}).items():
            st = summary.setdefault(r["topic"], {}).setdefault(
                key, {"n": 0, "dir_hits": 0, "mag_hits": 0, "pct_sum": 0.0})
            st["n"] += 1
            st["dir_hits"] += 1 if g.get("direction_hit") else 0
            st["mag_hits"] += 1 if g.get("magnitude_hit") else 0
            st["pct_sum"] += g.get("actual_pct") or 0.0

    return graded, summary


def write_board(date_str: str, summary: dict, board: dict) -> str:
    L = [f"# Horizon board — multi-timeframe prediction grades", "",
         f"Updated: {date_str}. Calls are graded at T+h trading days using "
         f"the scoreboard's own close history. Magnitude bands scale by "
         f"√h (a 'mild' month ≈ ±1.4%, a 'severe' month ≈ ±9.2%).", ""]

    L += ["## Hit rates by topic × horizon", "",
          "| Topic | Horizon | Graded | Dir hit | Mag hit | Avg actual % |",
          "|---|---|---|---|---|---|"]
    for topic in sorted(summary):
        for key in ("HORIZON_3D", "HORIZON_1W", "HORIZON_2W", "HORIZON_1M"):
            st = summary[topic].get(key)
            if not st or not st["n"]:
                continue
            L.append(f"| {topic} | {HORIZON_LABELS[key]} | {st['n']} | "
                     f"{st['dir_hits'] / st['n'] * 100:.0f}% "
                     f"({st['dir_hits']}/{st['n']}) | "
                     f"{st['mag_hits'] / st['n'] * 100:.0f}% "
                     f"({st['mag_hits']}/{st['n']}) | "
                     f"{st['pct_sum'] / st['n']:+.2f}% |")
    L.append("")

    L += ["## Recently graded calls", "",
          "| Date | Topic | Horizon | Call | Actual | Dir | Mag |",
          "|---|---|---|---|---|---|---|"]
    recent = []
    for r in board.get("runs", []):
        for key, g in (r.get("horizon_grades") or {}).items():
            recent.append((g.get("target_date", ""), r["date"], r["topic"],
                           HORIZON_LABELS.get(key, key), g))
    recent.sort(reverse=True)
    for _, date, topic, hlabel, g in recent[:25]:
        L.append(f"| {date} | {topic} | {hlabel} | "
                 f"{g['predicted_direction']}/{g['predicted_magnitude_band']} | "
                 f"{g['actual_pct']:+.2f}% ({g['actual_direction']}/"
                 f"{g['actual_magnitude_band']}) | "
                 f"{'✅' if g['direction_hit'] else '❌'} | "
                 f"{'✅' if g['magnitude_hit'] else '❌'} |")
    L.append("")

    pending = sum(1 for r in board.get("runs", [])
                  for key in (r.get("horizon_calls") or {})
                  if key not in (r.get("horizon_grades") or {}))
    L.append(f"*{pending} calls still maturing (T+h close not recorded yet).*")
    L.append("")

    path = os.path.join("03_scoreboard", "HORIZON_BOARD.md")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(L) + "\n")
    return path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    args = ap.parse_args()
    date_str = (args.date
                or datetime.now(ZoneInfo(config.TZ)).date().isoformat())

    board = scoreboard.load()
    graded, summary = grade_board(board)
    scoreboard.save(board)
    path = write_board(date_str, summary, board)
    total = sum(st["n"] for t in summary.values() for st in t.values())
    print(f"[horizon_grade] {date_str}: {graded} newly graded, "
          f"{total} total graded -> {path}")


if __name__ == "__main__":
    main()
