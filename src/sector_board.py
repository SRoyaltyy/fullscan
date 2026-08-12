"""Birds-eye board for all 11 sector predicts/outcomes in one page.

CLI:
  python -m src.sector_board [--date YYYY-MM-DD] [--stage predict|outcome|both]

Writes:
  01_daily/sectors/<date>/_BOARD.md
  01_daily/sectors/<date>/_board.json
"""
from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime
from zoneinfo import ZoneInfo

from . import config, scoreboard
from .sector_taxonomy import FINVIZ_SECTORS, SECTOR_ETFS


def _slug(sector: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", sector.lower()).strip("_")


def _topic(sector: str) -> str:
    return f"sector:{sector}"


def _entry(board: dict, date_str: str, sector: str) -> dict | None:
    topic = _topic(sector)
    for r in board.get("runs", []):
        if r.get("date") == date_str and r.get("topic") == topic:
            return r
    return None


def _md_exists(date_str: str, sector: str, kind: str) -> bool:
    path = os.path.join(config.DAILY_SECTORS, date_str, f"{_slug(sector)}_{kind}.md")
    return os.path.isfile(path)


def _fmt(v, nd=2):
    if v is None:
        return "—"
    if isinstance(v, float):
        return f"{v:.{nd}f}"
    return str(v)


def _hit(v) -> str:
    if v is True:
        return "HIT"
    if v is False:
        return "MISS"
    return "—"


def build(date_str: str) -> dict:
    board = scoreboard.load()
    rows = []
    for sector in FINVIZ_SECTORS:
        e = _entry(board, date_str, sector) or {}
        rows.append({
            "sector": sector,
            "etf": SECTOR_ETFS.get(sector, ""),
            "predicted_direction": e.get("predicted_direction"),
            "predicted_magnitude_band": e.get("predicted_magnitude_band"),
            "total_score": e.get("total_score"),
            "confidence_score": e.get("confidence_score"),
            "multiplier": e.get("multiplier"),
            "divergence_flagged": e.get("divergence_flagged"),
            "actual_pct_change": e.get("actual_pct_change"),
            "actual_direction": e.get("actual_direction"),
            "direction_hit": e.get("direction_hit"),
            "magnitude_hit": e.get("magnitude_hit"),
            "has_predict_md": _md_exists(date_str, sector, "predict"),
            "has_outcome_md": _md_exists(date_str, sector, "outcome"),
            "components": e.get("components") or {},
        })
    return {
        "date": date_str,
        "generated_at": datetime.now(ZoneInfo(config.TZ)).isoformat(),
        "sectors": rows,
    }


def _lead_lag(rows: list[dict]) -> tuple[list, list, list]:
    leads, lags, flat = [], [], []
    for r in rows:
        d = (r.get("predicted_direction") or "").lower()
        if d == "up":
            leads.append(r)
        elif d == "down":
            lags.append(r)
        else:
            flat.append(r)
    leads.sort(key=lambda x: (x.get("total_score") is not None, x.get("total_score") or -999), reverse=True)
    lags.sort(key=lambda x: (x.get("total_score") is not None, x.get("total_score") or 999))
    return leads, lags, flat


def to_markdown(payload: dict) -> str:
    date_str = payload["date"]
    rows = payload["sectors"]
    leads, lags, flat = _lead_lag(rows)

    graded = [r for r in rows if r.get("direction_hit") is not None]
    hits = sum(1 for r in graded if r.get("direction_hit") is True)
    n_pred = sum(1 for r in rows if r.get("predicted_direction") is not None)

    L = [
        f"# Sector Board — {date_str}",
        "",
        f"Generated: **{payload.get('generated_at')}** ({config.TZ})",
        "",
        "Birds-eye of all 11 Finviz sectors. Individual write-ups live next to this file "
        f"(`technology_predict.md`, etc.).",
        "",
        "## Summary",
        "",
        f"- Predicts present: **{n_pred}/11**",
        f"- Outcomes graded: **{len(graded)}/11**",
        f"- Direction hits (when graded): **{hits}/{len(graded) if graded else 0}**",
        f"- Predicted up / down / flat-or-missing: "
        f"**{len(leads)}** / **{len(lags)}** / **{len(flat)}**",
        "",
        "## Full table",
        "",
        "| Sector | ETF | Dir | Mag | Score | Conf | Actual% | Dir hit | Mag hit | MD |",
        "|--------|-----|-----|-----|-------|------|---------|---------|---------|----|",
    ]
    for r in rows:
        md = []
        if r.get("has_predict_md"):
            md.append("P")
        if r.get("has_outcome_md"):
            md.append("O")
        L.append(
            f"| {r['sector']} | {r['etf']} | {_fmt(r.get('predicted_direction'), 0)} | "
            f"{_fmt(r.get('predicted_magnitude_band'), 0)} | {_fmt(r.get('total_score'))} | "
            f"{_fmt(r.get('confidence_score'))} | {_fmt(r.get('actual_pct_change'))} | "
            f"{_hit(r.get('direction_hit'))} | {_hit(r.get('magnitude_hit'))} | "
            f"{''.join(md) or '—'} |"
        )

    L += ["", "## Predicted leaders (up)", ""]
    if not leads:
        L.append("_None_")
    else:
        for r in leads[:5]:
            L.append(
                f"- **{r['sector']}** ({r['etf']}): score={_fmt(r.get('total_score'))}, "
                f"mag={_fmt(r.get('predicted_magnitude_band'), 0)}, conf={_fmt(r.get('confidence_score'))}"
            )

    L += ["", "## Predicted laggards (down)", ""]
    if not lags:
        L.append("_None_")
    else:
        for r in lags[:5]:
            L.append(
                f"- **{r['sector']}** ({r['etf']}): score={_fmt(r.get('total_score'))}, "
                f"mag={_fmt(r.get('predicted_magnitude_band'), 0)}, conf={_fmt(r.get('confidence_score'))}"
            )

    if graded:
        L += ["", "## Graded calls (post-outcome)", ""]
        for r in graded:
            L.append(
                f"- **{r['sector']}**: pred {r.get('predicted_direction')} → "
                f"actual {_fmt(r.get('actual_pct_change'))}% ({r.get('actual_direction')}) — "
                f"dir {_hit(r.get('direction_hit'))}, mag {_hit(r.get('magnitude_hit'))}"
            )

    L += [
        "",
        "## Files",
        "",
        f"- Board JSON: `01_daily/sectors/{date_str}/_board.json`",
        f"- Per sector: `01_daily/sectors/{date_str}/<slug>_predict.md` / `_outcome.md`",
        "",
    ]
    return "\n".join(L)


def write(date_str: str) -> tuple[str, str]:
    payload = build(date_str)
    out_dir = os.path.join(config.DAILY_SECTORS, date_str)
    os.makedirs(out_dir, exist_ok=True)
    md_path = os.path.join(out_dir, "_BOARD.md")
    js_path = os.path.join(out_dir, "_board.json")
    with open(md_path, "w", encoding="utf-8") as fh:
        fh.write(to_markdown(payload))
    with open(js_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, ensure_ascii=False)
    print(f"[sector-board] {md_path}")
    return md_path, js_path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--stage", default="both",
                    help="predict|outcome|both (informational; always rebuilds from scoreboard)")
    args = ap.parse_args()
    date_str = args.date or datetime.now(ZoneInfo(config.TZ)).date().isoformat()
    write(date_str)


if __name__ == "__main__":
    main()
