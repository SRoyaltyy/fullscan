"""Sector OUTCOME — grade one sector's morning prediction vs sector ETF close.

CLI:
  python -m src.run_sector_outcome [--date YYYY-MM-DD] [--sectors Technology]
"""
from __future__ import annotations

import argparse
import os
import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from . import compute_sector_scores, config, deepseek_client, scoreboard
from .sector_memory import topic_for
from .sector_taxonomy import FINVIZ_SECTORS, SECTOR_ETFS


def _slug(sector: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", sector.lower()).strip("_")


def _read(path: str) -> str:
    try:
        with open(path, encoding="utf-8") as fh:
            return fh.read()
    except OSError:
        return "(missing)"


def _etf_actual(etf: str, date_str: str) -> dict:
    """Open/close % for etf and SPY on date_str (UTC-ish daily bars)."""
    out = {"etf": etf, "pct": None, "spy_pct": None, "rel": None,
           "open": None, "close": None}
    try:
        import socket

        import yfinance as yf
        start = (datetime.fromisoformat(date_str) - timedelta(days=5)).date().isoformat()
        end = (datetime.fromisoformat(date_str) + timedelta(days=3)).date().isoformat()
        # threads=True + no socket timeout hung Post-Close ALL before learn.
        prev_to = socket.getdefaulttimeout()
        socket.setdefaulttimeout(30)
        try:
            data = yf.download([etf, "SPY"], start=start, end=end,
                               progress=False, threads=False)
        finally:
            socket.setdefaulttimeout(prev_to)
        if data is None or data.empty:
            return out
        if hasattr(data.columns, "levels"):
            close = data["Close"]
            opn = data["Open"] if "Open" in data.columns.get_level_values(0) else close
        else:
            close, opn = data, data
        # find row for date_str
        for sym, key in ((etf, "pct"), ("SPY", "spy_pct")):
            if sym not in close.columns:
                continue
            s = close[sym].dropna()
            row = None
            for idx in s.index:
                if str(idx.date()) == date_str:
                    row = idx
                    break
            if row is None:
                continue
            # prior close
            loc = list(s.index).index(row)
            if loc == 0:
                continue
            prev = s.iloc[loc - 1]
            cur = s.iloc[loc]
            out[key] = float(cur / prev - 1) * 100.0
            if sym == etf:
                out["close"] = float(cur)
                try:
                    out["open"] = float(opn[sym].loc[row])
                except Exception:
                    out["open"] = None
        if out["pct"] is not None and out["spy_pct"] is not None:
            out["rel"] = out["pct"] - out["spy_pct"]
    except Exception as e:  # noqa: BLE001
        out["error"] = str(e)
    return out


def run_one(sector: str, date_str: str) -> None:
    etf = SECTOR_ETFS[sector]
    slug = _slug(sector)
    out_dir = os.path.join(config.DAILY_SECTORS, date_str)
    existing = os.path.join(out_dir, f"{slug}_outcome.md")
    if os.path.isfile(existing) and os.path.getsize(existing) >= 200:
        print(f"[sector-outcome] skip {sector}: outcome already on disk")
        return
    predict_md = _read(os.path.join(out_dir, f"{slug}_predict.md"))
    if predict_md == "(missing)":
        print(f"[sector-outcome] skip {sector}: no predict file")
        return
    config.require_llm()

    actual = _etf_actual(etf, date_str)
    with open(os.path.join(config.GROUNDING, "sector_outcome_prompt.md"),
              encoding="utf-8") as fh:
        prompt = fh.read()

    user_msg = (
        f"DATE: {date_str}\nSECTOR: {sector}\nETF: {etf}\n\n"
        f"=== MORNING SECTOR PREDICTION ===\n{predict_md}\n\n"
        f"=== ACTUALS (deterministic) ===\n"
        f"ETF_PCT: {actual.get('pct')}\nSPY_PCT: {actual.get('spy_pct')}\n"
        f"REL_PCT: {actual.get('rel')}\nOPEN: {actual.get('open')} "
        f"CLOSE: {actual.get('close')}\n\n"
        "Execute the sector post-session review now."
    )

    text = deepseek_client.chat(
        [{"role": "system", "content": prompt},
         {"role": "user", "content": user_msg}],
        model=config.MODEL_OUTCOME,
        tools=True,
        max_tokens=8000,
        transcript_path=os.path.join(
            "01_daily/_transcripts", f"{date_str}_sector_{slug}_outcome.json"),
        trace_path=os.path.join(out_dir, f"{slug}_outcome_trace.md"),
        stage_label=f"SECTOR OUTCOME {sector} {date_str}",
    )

    path = os.path.join(out_dir, f"{slug}_outcome.md")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(f"# Sector Outcome — {sector} — {date_str}\n\n")
        fh.write(f"Actuals: {actual}\n\n")
        fh.write(text)

    board = scoreboard.load()
    entry = scoreboard.get_or_create(board, date_str, topic_for(sector))
    pct = actual.get("pct")
    if pct is not None:
        grade = compute_sector_scores.grade(
            entry.get("predicted_direction") or "flat",
            entry.get("predicted_magnitude_band") or "flat",
            float(pct),
        )
        entry.update({
            "actual_open": actual.get("open"),
            "actual_close": actual.get("close"),
            "actual_pct_change": float(pct),
            "actual_direction": grade["actual_direction"],
            "actual_magnitude_band": grade["actual_magnitude_band"],
            "direction_hit": grade["direction_hit"],
            "magnitude_hit": grade["magnitude_hit"],
            "spy_pct_change": actual.get("spy_pct"),
            "rel_pct_change": actual.get("rel"),
        })
        scoreboard.save(board)
        print(f"[sector-outcome] {sector}: ETF {pct}% dir_hit={grade['direction_hit']}")
    else:
        print(f"[sector-outcome] {sector}: no ETF actuals ({actual})")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--sectors", default=None)
    args = ap.parse_args()
    date_str = args.date or datetime.now(ZoneInfo(config.TZ)).date().isoformat()
    sectors = ([s.strip() for s in args.sectors.split(",") if s.strip()]
               if args.sectors else list(FINVIZ_SECTORS))
    for sector in sectors:
        if sector not in FINVIZ_SECTORS:
            raise SystemExit(f"unknown sector {sector}")
        print(f"\n======== SECTOR OUTCOME: {sector} ========\n")
        try:
            run_one(sector, date_str)
        except Exception as e:  # noqa: BLE001
            print(f"[sector-outcome] WARN {sector}: {e}")


if __name__ == "__main__":
    main()
