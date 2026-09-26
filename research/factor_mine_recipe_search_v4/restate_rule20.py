"""Write a new rule-20 reading of the frozen forward window.

Does not rewrite FORWARD.json, REPORT.md, the daily return files, or the freeze.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.factor_mine_recipe_search_v4.bars import CleanStore  # noqa: E402
from research.factor_mine_recipe_search_v4.engine import walk  # noqa: E402
from research.factor_mine_recipe_search_v4.forward import _slice  # noqa: E402
from research.factor_mine_recipe_search_v4.metrics import summarize  # noqa: E402
from research.factor_mine_recipe_search_v4.protocol import (  # noqa: E402
    FORWARD,
    INPUTS,
    MIN_TRADES,
    REJECT_JOINT,
    RETURNS,
    SESSIONS,
    candidates,
)
from src.paper_trade import load_fees  # noqa: E402

OUT_JSON = RETURNS / "RULE20_RESTATE.json"
OUT_MD = RETURNS / "RULE20_RESTATE.md"
# Excel's independent recompute, without the best stock, one decimal percent.
EXCEL_EX_BEST = {
    "union_hot_n4_h1_time__w0": 4.2,
    "union_hot_n4_h1__w0": 5.6,
    "union_hot_n4_holdup__w0": 11.2,
    "union_hot_n4_h1_nonews__w0": 5.0,
}
FROZEN_PAIRS = (
    ("union_hot_score_h3__w0", "union_hot_score_h3_holdup__w0"),
    ("union_hot_score_h3__w1", "union_hot_score_h3_holdup__w1"),
)


def _pct(value) -> str:
    if value is None:
        return ""
    return f"{100.0 * float(value):.2f}%"


def _one_decimal(value: float) -> float:
    return round(100.0 * float(value) + 1e-12, 1)


def _identical_h3() -> dict:
    sessions = sorted(path.stem for path in RETURNS.glob("2026-*.json"))
    out = {}
    for left, right in FROZEN_PAIRS:
        diffs = []
        for session in sessions:
            doc = json.loads((RETURNS / f"{session}.json").read_text(encoding="utf-8"))
            if doc["recipes"][left] != doc["recipes"][right]:
                diffs.append(session)
        out[f"{left}={right}"] = {"diffs": diffs, "sessions": len(sessions)}
    return out


def _status(stat: dict) -> str:
    if stat["n"] < MIN_TRADES or stat["joint"] is None:
        return "unproven"
    if stat["joint"] < REJECT_JOINT:
        return "rejected"
    return "not_rejected"


def _report(rows: list[dict], identical: dict) -> str:
    lines = [
        "# Rule 20 restatement, forward window 2026-09-14 through 2026-09-25",
        "",
        "This file is a new reading. It does not replace `FORWARD.json` or `REPORT.md`.",
        "",
        "The committed without-best-stock, without-CYPH, without-GLND, and without-INDP figures for this window started the book at $10,000. The continuous book already had whatever equity the 2026-09-11 close had reached. Keep-held remains the Futubull figure. The window return itself (the compounded daily ratios) was already measured from that prior equity, so the compound column is unchanged. Only the dollar drops were wrong.",
        "",
        "Who carries forward is unchanged. A reject still uses the trade count and the joint of win rate and up-day share. Rule 20 is not that test.",
        "",
        "| id | status | compound | ex-best corrected | old ex-best | CYPH out | GLND out | INDP out | best |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in rows:
        lines.append(
            "| `{id}` | {status} | {comp} | {ex} | {old} | {cyph} | {glnd} | {indp} | {best} |".format(
                id=row["id"],
                status=row["status"],
                comp=_pct(row["compound"]),
                ex=_pct(row["ex_best"]),
                old=_pct(row["old_ex_best"]),
                cyph=_pct(row["named"]["CYPH"]),
                glnd=_pct(row["named"]["GLND"]),
                indp=_pct(row["named"]["INDP"]),
                best=row["ex_best_ticker"] or "",
            )
        )
    lines.extend([
        "",
        "Excel's corrected without-best-stock figures, which this reading matches to one decimal percent: `union_hot_n4_h1_time__w0` +4.2%, `union_hot_n4_h1__w0` +5.6%, `union_hot_n4_holdup__w0` +11.2%, `union_hot_n4_h1_nonews__w0` +5.0%.",
        "",
        "## Holdup does nothing on the h3 rows",
        "",
        "`union_hot_score_h3` and `union_hot_score_h3_holdup` write the same daily series under both weather settings. The committed return files have no differing session.",
        "",
    ])
    for key, info in identical.items():
        lines.append(f"- `{key}`: {info['sessions']} sessions, {len(info['diffs'])} differences")
    lines.extend([
        "",
        "Holdup, on an up morning that is not a hard-red sit, sets a new lot's minimum hold to the greater of the recipe hold and 2 sessions. The h3 recipe already holds for 3 sessions, so that floor stays 3. No lot sells on a different day. The weather gate only sits new buys on a hard-red morning. It does not change this floor. Holdup does change an h1 recipe, where the floor can rise from 1 session to 2.",
        "",
        "## `union_hot_n4_holdup__w0` carries because it was not rejected",
        "",
        "`union_hot_n4_holdup__w0` cleared 2 of the 3 Monday tuning starts. It is not a passer, and it has no rank key. It is on the frozen top 20, so the forward window checked it. The window left it not rejected (joint 53.33% on 30 trades). It carries from 2026-09-28 for that reason.",
        "",
    ])
    return "\n".join(lines)


def main() -> None:
    committed_path = RETURNS / "FORWARD.json"
    committed = json.loads(committed_path.read_text(encoding="utf-8"))
    by_id = {row["id"]: row for row in committed["results"]}
    specs = {spec["id"]: spec for spec in candidates()}
    fees = load_fees()
    payload = json.loads(INPUTS.read_text(encoding="utf-8"))
    store = CleanStore()
    price = lambda ticker, session, which: (  # noqa: E731
        store.session_open(ticker, session) if which == "open" else store.session_close(ticker, session)
    )
    days = [
        {"session": session, "s": payload["dates"][session]["s"], "rows": payload["dates"][session]["rows"]}
        for session in SESSIONS
    ]
    identical = _identical_h3()
    if any(info["diffs"] for info in identical.values()):
        raise SystemExit(f"h3 holdup series differ {identical}")
    rows = []
    for old in committed["results"]:
        spec = specs[old["id"]]
        book = _slice(walk(days, spec, fees, price, "keep_held"))
        if book.get("start_equity") is None:
            raise SystemExit("slice has no prior equity")
        stat = summarize(book)
        status = _status(stat)
        if status != old["status"]:
            raise SystemExit(f"status changed {old['id']} {old['status']} -> {status}")
        if abs(stat["compound"] - old["compound"]) > 1e-9:
            raise SystemExit(f"compound changed {old['id']}")
        if stat["n"] != old["n"]:
            raise SystemExit(f"trade count changed {old['id']}")
        contributed = 0.0
        if stat["ex_best_ticker"]:
            for bucket in book["pnl_by_day"].values():
                contributed += float(bucket.get(stat["ex_best_ticker"]) or 0.0)
        if contributed > 0 and not stat["ex_best"] < stat["compound"]:
            raise SystemExit(f"positive best still raises {old['id']}")
        prior = float(book["start_equity"])
        if abs(prior - 10_000.0) < 1.0 and old["id"] in EXCEL_EX_BEST:
            raise SystemExit(f"{old['id']} still starts at CAPITAL")
        rows.append({
            "compound": stat["compound"],
            "ex_best": stat["ex_best"],
            "ex_best_ticker": stat["ex_best_ticker"],
            "id": old["id"],
            "n": stat["n"],
            "named": stat["named"],
            "old_ex_best": old["ex_best"],
            "old_named": old["named"],
            "prior_equity": prior,
            "status": status,
        })
        print(old["id"], _pct(stat["ex_best"]), "prior", round(prior, 2), flush=True)
    for recipe_id, expect in EXCEL_EX_BEST.items():
        got = _one_decimal(next(row["ex_best"] for row in rows if row["id"] == recipe_id))
        if got != expect:
            raise SystemExit(f"excel {recipe_id} {got} != {expect}")
    carry = [row["id"] for row in rows if row["status"] != "rejected"]
    old_carry = [row["id"] for row in committed["results"] if row["status"] != "rejected"]
    if carry != old_carry:
        raise SystemExit("carry list changed")
    body = {
        "carry_unchanged": carry,
        "excel_ex_best_pct": EXCEL_EX_BEST,
        "h3_holdup_identical": identical,
        "note": (
            "Rule 20 drops for 2026-09-14 through 2026-09-25 now start from the "
            "2026-09-11 close. Status, compound, and who carries forward are unchanged."
        ),
        "results": rows,
        "source_forward": "research/factor_mine_recipe_search_v4/returns/FORWARD.json",
    }
    if OUT_JSON.exists() or OUT_MD.exists():
        raise SystemExit("restatement files already exist")
    OUT_JSON.write_text(json.dumps(body, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    OUT_MD.write_text(_report(rows, identical), encoding="utf-8")
    # The committed forward record stays byte-for-byte.
    if json.loads(committed_path.read_text(encoding="utf-8")) != committed:
        raise SystemExit("FORWARD.json changed")
    print("rule20 restatement wrote", len(rows), flush=True)


if __name__ == "__main__":
    main()
