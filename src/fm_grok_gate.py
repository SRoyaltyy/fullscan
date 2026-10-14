"""Refuse Factor Mine's daily rows unless Grok wrote the day's general predict.

PENDING CYRUS (2026-10-14). Same two-part rule as the h1 seal
(research/hot_n4_clean_v4/forward/h1_grok_gate.py, shared via
``reason_from``). For session D, Factor Mine rows may be written only when
BOTH hold on origin/main:
  * 01_daily/<D>_preopen_status.json has ``grok_ok`` == true, and
  * 01_daily/general/<D>_predict_trace.md names a Grok model.

Wrapper only. No ENGINE_SHA256-pinned file is touched (factor_mine.py,
factor_mine_book.py, ticker_lookback.py ...). ``install()`` wraps
``strategy_tickets.recipe_strats`` (the step that turns the day's look into
Factor Mine buy/sell rows). On refusal every factor_mine row is emitted with
no buys and no sells, status ``missing_grok`` and the reason as its note, the
same fail-closed shape as a missing required input: nothing to trade, and
strategy-status shows MISSING naming the reason. Only session D's fresh build
is affected; rows already sealed for past days are never rewritten (the
09:30 ticket lock and past_day_lock still apply as before).
"""
from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
STATUS = "missing_grok"
# First session the rule applies to. Earlier sessions keep the rule they
# were sealed under (presence only); nothing before this date is rewritten
# or re-judged.
FROM = "2026-10-15"


def _blob(root: Path, rel: str) -> str | None:
    from src.strategy_status_publish import blob_on_main
    raw = blob_on_main(root, rel)
    return None if raw is None else raw.decode("utf-8", "replace")


def reason(day: str, root: Path = ROOT) -> str | None:
    """None when Grok wrote D's predict (on origin/main); else why FM refuses."""
    if str(day) < FROM:
        return None
    from research.hot_n4_clean_v4.forward.h1_grok_gate import (reason_from,
                                                                status_rel,
                                                                trace_rel)
    return reason_from(
        day, _blob(root, status_rel(day)), _blob(root, trace_rel(day)),
        "main", "Factor Mine", f"No Factor Mine rows written for {day}.",
    )


def refuse_rows(rows: list[dict], day: str, why: str) -> list[dict]:
    """Same names, no buys or sells, status missing_grok, reason as note."""
    from src import strategy_tickets as st
    out = []
    for r in rows or []:
        if r.get("family") != "factor_mine":
            out.append(r)
            continue
        out.append(st._entry(r.get("name") or "factor_mine", "factor_mine", day,
                             [], [], status=STATUS, note=why[:200]))
    if not out:
        out = [st._entry("factor_mine", "factor_mine", day, [], [],
                         status=STATUS, note=why[:200])]
    return out


def install(root: Path = ROOT) -> None:
    from src import strategy_tickets as st
    if getattr(st.recipe_strats, "_fm_grok_gate", False):
        return
    original = st.recipe_strats

    def recipe_strats(date: str, look_out: dict | None = None) -> list[dict]:
        rows = original(date, look_out=look_out)
        why = reason(str(date), root)
        if why is None:
            return rows
        print(f"[fm-grok-gate] REFUSING Factor Mine rows for {date}: {why}", flush=True)
        print(f"::error::Factor Mine seal refused for {date}: {why}", flush=True)
        return refuse_rows(rows, str(date), why)

    recipe_strats._fm_grok_gate = True
    for k, v in vars(original).items():
        setattr(recipe_strats, k, v)
    st.recipe_strats = recipe_strats
