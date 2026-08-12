"""Birds-eye HIT% board across all dates — general market + 11 sectors.

Reads 03_scoreboard/scoreboard.json (no LLM).

CLI:
  python -m src.hit_board

Writes:
  03_scoreboard/HIT_BOARD.md
  03_scoreboard/hit_board.json
"""
from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo

from . import config, scoreboard
from .sector_taxonomy import FINVIZ_SECTORS, SECTOR_ETFS


def _hit_stats(rows: list[dict]) -> dict:
    graded = [r for r in rows if r.get("direction_hit") is not None]
    dir_hits = sum(1 for r in graded if r.get("direction_hit") is True)
    mag_graded = [r for r in graded if r.get("magnitude_hit") is not None]
    mag_hits = sum(1 for r in mag_graded if r.get("magnitude_hit") is True)
    n = len(graded)
    return {
        "n_graded": n,
        "n_predicts": sum(1 for r in rows if r.get("predicted_direction")),
        "dir_hits": dir_hits,
        "dir_hit_pct": (100.0 * dir_hits / n) if n else None,
        "mag_hits": mag_hits,
        "mag_n": len(mag_graded),
        "mag_hit_pct": (100.0 * mag_hits / len(mag_graded)) if mag_graded else None,
    }


def _pct(v) -> str:
    if v is None:
        return "—"
    return f"{v:.1f}%"


def _hit_cell(v) -> str:
    if v is True:
        return "HIT"
    if v is False:
        return "MISS"
    return "—"


def build() -> dict:
    board = scoreboard.load()
    runs = board.get("runs") or []

    general = [r for r in runs if r.get("topic") == "general"]
    by_date_general = {r["date"]: r for r in general if r.get("date")}

    sector_runs = [r for r in runs if str(r.get("topic", "")).startswith("sector:")]
    # date -> sector -> row
    by_date_sector: dict[str, dict[str, dict]] = defaultdict(dict)
    for r in sector_runs:
        topic = r.get("topic") or ""
        sector = topic.split("sector:", 1)[-1]
        d = r.get("date")
        if d and sector:
            by_date_sector[d][sector] = r

    all_dates = sorted(
        set(by_date_general.keys()) | set(by_date_sector.keys())
    )

    # Per-date sector rollup
    sector_by_date = []
    for d in all_dates:
        rows = list(by_date_sector.get(d, {}).values())
        st = _hit_stats(rows)
        sector_by_date.append({"date": d, **st, "n_sectors": len(rows)})

    # Per-sector across dates
    per_sector = []
    for sector in FINVIZ_SECTORS:
        rows = []
        for d in all_dates:
            r = by_date_sector.get(d, {}).get(sector)
            if r:
                rows.append(r)
        st = _hit_stats(rows)
        per_sector.append({
            "sector": sector,
            "etf": SECTOR_ETFS.get(sector, ""),
            **st,
        })

    general_stats = _hit_stats(general)
    sector_all_stats = _hit_stats(sector_runs)

    return {
        "generated_at": datetime.now(ZoneInfo(config.TZ)).isoformat(),
        "dates": all_dates,
        "general": {
            "overall": general_stats,
            "by_date": [
                {
                    "date": d,
                    "predicted_direction": (by_date_general.get(d) or {}).get("predicted_direction"),
                    "predicted_magnitude_band": (by_date_general.get(d) or {}).get("predicted_magnitude_band"),
                    "total_score": (by_date_general.get(d) or {}).get("total_score"),
                    "actual_pct_change": (by_date_general.get(d) or {}).get("actual_pct_change"),
                    "actual_direction": (by_date_general.get(d) or {}).get("actual_direction"),
                    "direction_hit": (by_date_general.get(d) or {}).get("direction_hit"),
                    "magnitude_hit": (by_date_general.get(d) or {}).get("magnitude_hit"),
                }
                for d in all_dates
            ],
        },
        "sectors": {
            "overall": sector_all_stats,
            "by_date": sector_by_date,
            "per_sector": per_sector,
            "matrix": {
                d: {
                    s: {
                        "dir": (by_date_sector.get(d, {}).get(s) or {}).get("predicted_direction"),
                        "hit": (by_date_sector.get(d, {}).get(s) or {}).get("direction_hit"),
                        "pct": (by_date_sector.get(d, {}).get(s) or {}).get("actual_pct_change"),
                    }
                    for s in FINVIZ_SECTORS
                }
                for d in all_dates
            },
        },
    }


def to_markdown(payload: dict) -> str:
    g = payload["general"]["overall"]
    s = payload["sectors"]["overall"]
    L = [
        "# HIT Board — general + sectors (all dates)",
        "",
        f"Generated: **{payload['generated_at']}**",
        "",
        "Source: `03_scoreboard/scoreboard.json` (graded runs only count when `direction_hit` is set).",
        "",
        "## Overall HIT%",
        "",
        "| Book | Direction HIT% | n graded | Mag HIT% | n mag |",
        "|------|----------------|----------|----------|-------|",
        f"| **General (SPX-style)** | **{_pct(g.get('dir_hit_pct'))}** | {g.get('n_graded')} | "
        f"{_pct(g.get('mag_hit_pct'))} | {g.get('mag_n')} |",
        f"| **All sector calls** | **{_pct(s.get('dir_hit_pct'))}** | {s.get('n_graded')} | "
        f"{_pct(s.get('mag_hit_pct'))} | {s.get('mag_n')} |",
        "",
        "## General market — by date",
        "",
        "| Date | Pred dir | Mag | Score | Actual % | Actual dir | Dir | Mag |",
        "|------|----------|-----|-------|----------|------------|-----|-----|",
    ]
    for r in payload["general"]["by_date"]:
        L.append(
            f"| {r['date']} | {r.get('predicted_direction') or '—'} | "
            f"{r.get('predicted_magnitude_band') or '—'} | "
            f"{r.get('total_score') if r.get('total_score') is not None else '—'} | "
            f"{r.get('actual_pct_change') if r.get('actual_pct_change') is not None else '—'} | "
            f"{r.get('actual_direction') or '—'} | "
            f"{_hit_cell(r.get('direction_hit'))} | {_hit_cell(r.get('magnitude_hit'))} |"
        )

    L += [
        "",
        "## Sectors — HIT% by date (11 names rolled up)",
        "",
        "| Date | n sectors | Dir HIT% | hits/graded | Mag HIT% |",
        "|------|-----------|----------|-------------|----------|",
    ]
    for r in payload["sectors"]["by_date"]:
        L.append(
            f"| {r['date']} | {r.get('n_sectors')} | **{_pct(r.get('dir_hit_pct'))}** | "
            f"{r.get('dir_hits')}/{r.get('n_graded')} | {_pct(r.get('mag_hit_pct'))} |"
        )

    L += [
        "",
        "## Sectors — HIT% by sector (across dates)",
        "",
        "| Sector | ETF | Dir HIT% | hits/graded | Mag HIT% |",
        "|--------|-----|----------|-------------|----------|",
    ]
    for r in payload["sectors"]["per_sector"]:
        L.append(
            f"| {r['sector']} | {r.get('etf')} | **{_pct(r.get('dir_hit_pct'))}** | "
            f"{r.get('dir_hits')}/{r.get('n_graded')} | {_pct(r.get('mag_hit_pct'))} |"
        )

    # Compact matrix last few dates only if many
    dates = payload["dates"]
    show = dates[-10:] if len(dates) > 10 else dates
    L += [
        "",
        f"## Sector matrix (dir hit) — last {len(show)} dates",
        "",
        "HIT / MISS / — (no grade). Actual % in parentheses when graded.",
        "",
    ]
    # header
    hdr = "| Sector | " + " | ".join(show) + " |"
    sep = "|--------|" + "|".join(["------" for _ in show]) + "|"
    L.append(hdr)
    L.append(sep)
    matrix = payload["sectors"]["matrix"]
    for sector in FINVIZ_SECTORS:
        cells = []
        for d in show:
            cell = matrix.get(d, {}).get(sector) or {}
            h = cell.get("hit")
            pct = cell.get("pct")
            mark = _hit_cell(h)
            if h is not None and pct is not None:
                cells.append(f"{mark} ({pct:+.1f}%)" if isinstance(pct, (int, float)) else mark)
            else:
                pred = cell.get("dir")
                cells.append(mark if mark != "—" else (pred or "—"))
        L.append(f"| {sector} | " + " | ".join(cells) + " |")

    L += [
        "",
        "## Files",
        "",
        "- This board: `03_scoreboard/HIT_BOARD.md`",
        "- JSON: `03_scoreboard/hit_board.json`",
        "- Per-day sector snapshot: `01_daily/sectors/<date>/_BOARD.md`",
        "",
    ]
    return "\n".join(L)


def write() -> tuple[str, str]:
    payload = build()
    out_dir = config.SCOREBOARD_DIR if hasattr(config, "SCOREBOARD_DIR") else os.path.join(
        config.ROOT, "03_scoreboard"
    )
    os.makedirs(out_dir, exist_ok=True)
    md_path = os.path.join(out_dir, "HIT_BOARD.md")
    js_path = os.path.join(out_dir, "hit_board.json")
    with open(md_path, "w", encoding="utf-8") as fh:
        fh.write(to_markdown(payload))
    with open(js_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, ensure_ascii=False)
    print(f"[hit-board] {md_path}")
    return md_path, js_path


def main() -> None:
    argparse.ArgumentParser(description="Build multi-date HIT board").parse_args()
    write()


if __name__ == "__main__":
    main()
