"""Rank the 34 formulas on clean bars through 2026-09-11.

Does not read a forward session and does not write returns/.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.factor_mine_score_search_v1.protocol import (  # noqa: E402
    FREEZE,
    GRID,
    TUNE,
    load_inputs,
    mean,
    prereg_fingerprint,
    rank_formulas,
)
from research.factor_mine_score_search_v1.walk import (  # noqa: E402
    assert_tune_bars,
    fee_fns,
    load_tape,
    prepare_days,
    run_formula,
)
from research.factor_mine_score_search_v1.protocol import window_stats  # noqa: E402


def _public(stats: dict) -> dict:
    return {key: stats[key] for key in (
        "best", "closed", "compound", "days_entered", "down", "entries",
        "ex_best", "flat", "too_few", "up", "win_rate",
    )}


def main() -> None:
    if FREEZE.exists():
        raise SystemExit("freeze already written")
    assert_tune_bars("clean")
    inputs = load_inputs()
    print("loading clean bars through 2026-09-11", flush=True)
    tape = load_tape("clean", through=TUNE[-1])
    days = prepare_days(inputs, tape, TUNE)
    if [day["session"] for day in days] != list(TUNE):
        raise SystemExit("tune sessions")
    futu, _flat = fee_fns()
    ranked = []
    clean = {}
    for formula in GRID:
        by_x = {}
        compounds = []
        exes = []
        for top_n in (2, 4, 8):
            book = run_formula(days, formula, top_n, futu)
            stats = window_stats(book, list(TUNE), TUNE)
            by_x[str(top_n)] = _public(stats)
            compounds.append(stats["compound"])
            if stats["ex_best"] is not None:
                exes.append(stats["ex_best"])
        clean[formula["id"]] = by_x
        ranked.append({
            "id": formula["id"],
            "mean_compound": mean(compounds),
            "mean_ex": mean(exes),
        })
        print(formula["id"], round(ranked[-1]["mean_compound"], 4), flush=True)
    order = rank_formulas(ranked)
    payload = {
        "clean_tune": clean,
        "fingerprint": prereg_fingerprint(),
        "order": order,
        "rank": ranked,
        "through": TUNE[-1],
    }
    FREEZE.parent.mkdir(parents=True, exist_ok=True)
    FREEZE.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"best {order[0]}", flush=True)
    print(f"wrote {FREEZE}", flush=True)


if __name__ == "__main__":
    main()
