"""Rank the 34 formulas on the primary clean book through 2026-09-11.

Fresh $10,000 at each preregistered start. Does not read a forward
session and does not write returns/.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.factor_mine_score_search_v3.protocol import (  # noqa: E402
    FREEZE,
    GRID,
    LUCK_N,
    OBSERVE_STARTS,
    RANK_STARTS,
    TUNE,
    load_drop,
    load_inputs,
    prereg_fingerprint,
    rank_formulas,
    required_cells,
    score_formula,
)
from research.factor_mine_score_search_v3.walk import (  # noqa: E402
    assert_tune_bars,
    board_tickers,
    fee_fns,
    jump_flags,
    load_tape,
    prepare_days,
    run_formula,
)
from research.factor_mine_score_search_v3.protocol import window_stats  # noqa: E402

PUBLIC = (
    "best", "closed", "compound", "days_entered", "days_traded", "down",
    "entries", "ex_best", "flat", "too_few", "up", "up_share", "win_rate",
)


def _public(stats: dict) -> dict:
    return {key: stats[key] for key in PUBLIC}


def _cell_key(start: str, top_n: int) -> str:
    return f"{start}|{top_n}"


def main() -> None:
    if FREEZE.exists():
        raise SystemExit("freeze already written")
    assert_tune_bars("clean")
    inputs = load_inputs()
    dropped = set(load_drop()["dropped"])
    print("jump check clean through 2026-09-11", flush=True)
    clean_tape = load_tape("clean", through=TUNE[-1])
    names = board_tickers(inputs, clean_tape, TUNE[-1])
    clean_jumps = jump_flags(clean_tape, names, TUNE[-1], dropped)
    if clean_jumps:
        sample = ", ".join(f"{row['ticker']} {row['date']}" for row in clean_jumps[:8])
        raise SystemExit(f"IRONCLAD 26: clean tape halted, {len(clean_jumps)} unexplained jumps, including {sample}")
    print("jump check yahoo through 2026-09-11", flush=True)
    yahoo_tape = load_tape("yahoo", through=TUNE[-1])
    yahoo_names = board_tickers(inputs, yahoo_tape, TUNE[-1])
    yahoo_jumps = jump_flags(yahoo_tape, yahoo_names, TUNE[-1], dropped)
    yahoo = {
        "halted": bool(yahoo_jumps),
        "jumps": yahoo_jumps[:12],
        "n": len(yahoo_jumps),
    }
    if yahoo_jumps:
        print(f"yahoo halted {len(yahoo_jumps)}", flush=True)
    days = prepare_days(inputs, clean_tape, TUNE, "primary")
    if [day["session"] for day in days] != list(TUNE):
        raise SystemExit("tune sessions")
    index = {day["session"]: pos for pos, day in enumerate(days)}
    futu, _flat = fee_fns()
    starts = list(RANK_STARTS) + list(OBSERVE_STARTS)
    ranked = []
    cells = {}
    for formula in GRID:
        packed: dict[tuple[str, int], dict] = {}
        stored = {}
        for top_n in (2, 4, 8):
            for start in starts:
                sub = days[index[start]:]
                book = run_formula(sub, formula, top_n, futu)
                sessions = [day["session"] for day in sub]
                stats = _public(window_stats(book, sessions, tuple(sessions)))
                packed[(start, top_n)] = stats
                stored[_cell_key(start, top_n)] = stats
        summary = score_formula(packed)
        summary["id"] = formula["id"]
        ranked.append(summary)
        cells[formula["id"]] = stored
        print(formula["id"], summary["eligible"], summary["rank_key"], flush=True)
    order = rank_formulas(ranked)
    payload = {
        "best": order[0],
        "cells": cells,
        "fingerprint": prereg_fingerprint(),
        "luck_n": LUCK_N,
        "order": order,
        "passers": [row["id"] for row in ranked if row["eligible"]],
        "rank": ranked,
        "required": [[start, top_n] for start, top_n in required_cells()],
        "through": TUNE[-1],
        "top10": order[:10],
        "top20": order[:20],
        "yahoo_tune": yahoo,
    }
    # Passers follow rank order.
    payload["passers"] = [fid for fid in order if fid in set(payload["passers"])]
    FREEZE.parent.mkdir(parents=True, exist_ok=True)
    FREEZE.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"best {order[0]} passers {len(payload['passers'])}", flush=True)
    print(f"wrote {FREEZE}", flush=True)


if __name__ == "__main__":
    main()
