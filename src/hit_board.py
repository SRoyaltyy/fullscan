"""Birds-eye HIT% board across all dates — general market + 11 sectors.

Reads 03_scoreboard/scoreboard.json (no LLM).

Headline direction HIT% counts only runs that made a real prediction
(predicted_direction set). Pipeline blanks (no predict file) are listed
separately and do not dilute model accuracy.

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


def _has_predict(r: dict) -> bool:
    d = r.get("predicted_direction")
    return bool(d) and str(d).strip().lower() not in ("", "none", "null")


def _hit_stats(rows: list[dict], *, require_predict: bool = True) -> dict:
    """Direction/mag accuracy.

    require_predict=True (default): only rows with a real predicted_direction
    and direction_hit set. Pipeline blanks excluded from HIT%.
    """
    if require_predict:
        pool = [r for r in rows if _has_predict(r)]
    else:
        pool = list(rows)

    graded = [r for r in pool if r.get("direction_hit") is not None]
    dir_hits = sum(1 for r in graded if r.get("direction_hit") is True)
    mag_graded = [r for r in graded if r.get("magnitude_hit") is not None]
    mag_hits = sum(1 for r in mag_graded if r.get("magnitude_hit") is True)
    n = len(graded)

    blanks = [r for r in rows if not _has_predict(r) and r.get("date")]
    # graded-without-predict (legacy auto-MISS)
    auto_miss = [
        r for r in rows
        if not _has_predict(r) and r.get("direction_hit") is False
    ]

    return {
        "n_graded": n,
        "n_predicts": sum(1 for r in rows if _has_predict(r)),
        "dir_hits": dir_hits,
        "dir_hit_pct": (100.0 * dir_hits / n) if n else None,
        "mag_hits": mag_hits,
        "mag_n": len(mag_graded),
        "mag_hit_pct": (100.0 * mag_hits / len(mag_graded)) if mag_graded else None,
        "n_pipeline_blank": len(blanks),
        "pipeline_blank_dates": sorted({r["date"] for r in blanks if r.get("date")}),
        "n_auto_miss_no_predict": len(auto_miss),
        "auto_miss_dates": sorted({r["date"] for r in auto_miss if r.get("date")}),
    }


def _pct(v) -> str:
    if v is None:
        return "—"
    return f"{v:.1f}%"


def _hit_cell(v, *, has_pred: bool = True) -> str:
    if not has_pred:
        return "NO_PRED"
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
    by_date_sector: dict[str, dict[str, dict]] = defaultdict(dict)
    for r in sector_runs:
        topic = r.get("topic") or ""
        sector = topic.split("sector:", 1)[-1]
        d = r.get("date")
        if d and sector:
            by_date_sector[d][sector] = r

    all_dates = sorted(set(by_date_general.keys()) | set(by_date_sector.keys()))

    sector_by_date = []
    for d in all_dates:
        rows = list(by_date_sector.get(d, {}).values())
        st = _hit_stats(rows, require_predict=True)
        sector_by_date.append({"date": d, **st, "n_sectors": len(rows)})

    per_sector = []
    for sector in FINVIZ_SECTORS:
        rows = []
        for d in all_dates:
            r = by_date_sector.get(d, {}).get(sector)
            if r:
                rows.append(r)
        st = _hit_stats(rows, require_predict=True)
        per_sector.append({
            "sector": sector,
            "etf": SECTOR_ETFS.get(sector, ""),
            **st,
        })

    general_stats = _hit_stats(general, require_predict=True)
    general_legacy = _hit_stats(general, require_predict=False)
    sector_all_stats = _hit_stats(sector_runs, require_predict=True)

    return {
        "generated_at": datetime.now(ZoneInfo(config.TZ)).isoformat(),
        "dates": all_dates,
        "general": {
            "overall": general_stats,
            "legacy_all_graded": general_legacy,
            "by_date": [
                {
                    "date": d,
                    "has_predict": _has_predict(by_date_general.get(d) or {}),
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
                        "has_predict": _has_predict(by_date_sector.get(d, {}).get(s) or {}),
                    }
                    for s in FINVIZ_SECTORS
                }
                for d in all_dates
            },
        },
    }


def to_markdown(payload: dict) -> str:
    g = payload["general"]["overall"]
    g_legacy = payload["general"].get("legacy_all_graded") or {}
    s = payload["sectors"]["overall"]

    blank_dates = g.get("pipeline_blank_dates") or []
    auto_miss = g.get("auto_miss_dates") or []

    L = [
        "# HIT Board — general + sectors (all dates)",
        "",
        f"Generated: **{payload['generated_at']}**",
        "",
        "Source: `03_scoreboard/scoreboard.json`.",
        "",
        "**HIT% rule:** only runs with a real `predicted_direction` count. "
        "Days with no predict file are **pipeline blanks** — listed separately, "
        "not counted as model MISS.",
        "",
        "## Overall HIT% (model calls only)",
        "",
        "| Book | Direction HIT% | hits / graded | Mag HIT% | n mag |",
        "|------|----------------|---------------|----------|-------|",
        f"| **General (SPX-style)** | **{_pct(g.get('dir_hit_pct'))}** | "
        f"{g.get('dir_hits')}/{g.get('n_graded')} | "
        f"{_pct(g.get('mag_hit_pct'))} | {g.get('mag_n')} |",
        f"| **All sector calls** | **{_pct(s.get('dir_hit_pct'))}** | "
        f"{s.get('dir_hits')}/{s.get('n_graded')} | "
        f"{_pct(s.get('mag_hit_pct'))} | {s.get('mag_n')} |",
        "",
        "### Pipeline blanks (general) — excluded from HIT%",
        "",
    ]
    if blank_dates:
        L.append(
            f"- No `predicted_direction`: **{', '.join(blank_dates)}** "
            f"(n={len(blank_dates)})"
        )
        if auto_miss:
            L.append(
                f"- Of those, legacy scoreboard still marked direction_hit=false: "
                f"**{', '.join(auto_miss)}** — ops failure, not model error"
            )
        L.append(
            f"- If blanks were counted as MISS (old method): "
            f"**{_pct(g_legacy.get('dir_hit_pct'))}** "
            f"({g_legacy.get('dir_hits')}/{g_legacy.get('n_graded')})"
        )
    else:
        L.append("- None — every general row has a prediction.")

    L += [
        "",
        "## General market — by date",
        "",
        "| Date | Pred dir | Mag | Score | Actual % | Actual dir | Dir | Mag |",
        "|------|----------|-----|-------|----------|------------|-----|-----|",
    ]
    for r in payload["general"]["by_date"]:
        hp = r.get("has_predict", True)
        L.append(
            f"| {r['date']} | {r.get('predicted_direction') or '—'} | "
            f"{r.get('predicted_magnitude_band') or '—'} | "
            f"{r.get('total_score') if r.get('total_score') is not None else '—'} | "
            f"{r.get('actual_pct_change') if r.get('actual_pct_change') is not None else '—'} | "
            f"{r.get('actual_direction') or '—'} | "
            f"{_hit_cell(r.get('direction_hit'), has_pred=hp)} | "
            f"{_hit_cell(r.get('magnitude_hit'), has_pred=hp)} |"
        )

    L += [
        "",
        "## Sectors — HIT% by date (model calls only)",
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

    dates = payload["dates"]
    show = dates[-10:] if len(dates) > 10 else dates
    L += [
        "",
        f"## Sector matrix (dir hit) — last {len(show)} dates",
        "",
        "HIT / MISS / NO_PRED / — . Actual % when graded.",
        "",
    ]
    hdr = "| Sector | " + " | ".join(show) + " |"
    sep = "|--------|" + "|".join(["------" for _ in show]) + "|"
    L.append(hdr)
    L.append(sep)
    matrix = payload["sectors"]["matrix"]
    for sector in FINVIZ_SECTORS:
        cells = []
        for d in show:
            cell = matrix.get(d, {}).get(sector) or {}
            hp = cell.get("has_predict", True)
            h = cell.get("hit")
            pct = cell.get("pct")
            mark = _hit_cell(h, has_pred=hp)
            if hp and h is not None and isinstance(pct, (int, float)):
                cells.append(f"{mark} ({pct:+.1f}%)")
            else:
                pred = cell.get("dir")
                cells.append(mark if mark not in ("—",) else (pred or "—"))
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
    out_dir = os.path.dirname(config.SCOREBOARD_JSON) or "03_scoreboard"
    os.makedirs(out_dir, exist_ok=True)
    md_path = os.path.join(out_dir, "HIT_BOARD.md")
    js_path = os.path.join(out_dir, "hit_board.json")
    with open(md_path, "w", encoding="utf-8") as fh:
        fh.write(to_markdown(payload))
    with open(js_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, ensure_ascii=False)
    print(f"[hit-board] {md_path}")
    g = payload["general"]["overall"]
    print(
        f"[hit-board] general model HIT%={_pct(g.get('dir_hit_pct'))} "
        f"({g.get('dir_hits')}/{g.get('n_graded')}); "
        f"pipeline blanks={g.get('n_pipeline_blank')}"
    )
    return md_path, js_path


def main() -> None:
    argparse.ArgumentParser(description="Build multi-date HIT board").parse_args()
    write()


if __name__ == "__main__":
    main()
