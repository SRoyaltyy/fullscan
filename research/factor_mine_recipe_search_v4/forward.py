"""Reject-only check from 2026-09-14. Requires the tune freeze. Does not re-rank."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.factor_mine_recipe_search_v4.bars import CleanStore  # noqa: E402
from research.factor_mine_recipe_search_v4.engine import walk  # noqa: E402
from research.factor_mine_recipe_search_v4.metrics import summarize  # noqa: E402
from research.factor_mine_recipe_search_v4.protocol import (  # noqa: E402
    FORWARD,
    FREEZE,
    INPUTS,
    MIN_TRADES,
    REJECT_JOINT,
    RETURNS,
    SESSIONS,
    TUNE,
    candidates,
    file_sha256,
)
from src.paper_trade import load_fees  # noqa: E402


def _slice(book: dict) -> dict:
    daily = [day for day in book["daily"] if day["session"] in FORWARD]
    # Rebase the forward slice as its own compound from the equity entering 09-14.
    if not daily:
        return book
    # summarize() compounds daily ret, which is already a ratio. Slicing the
    # ratios is the forward return of the continuous book. Closed trades are
    # those whose sell date is in the forward window.
    closed = [trade for trade in book["closed"] if trade["exit"] in set(FORWARD)]
    pnl = {day: bucket for day, bucket in book["pnl_by_day"].items() if day in set(FORWARD)}
    first = {}
    for trade in closed:
        first.setdefault(trade["ticker"], trade["entry"])
    return {"closed": closed, "daily": daily, "first": first, "pnl_by_day": pnl}


def main() -> None:
    if not FREEZE.is_file():
        raise SystemExit("freeze missing; the forward check cannot run")
    freeze = json.loads(FREEZE.read_text(encoding="utf-8"))
    if freeze.get("through") != TUNE[-1]:
        raise SystemExit("freeze is not the tune lock")
    fees = load_fees()
    payload = json.loads(INPUTS.read_text(encoding="utf-8"))
    store = CleanStore()
    price_fn = lambda ticker, session, which: (  # noqa: E731
        store.session_open(ticker, session) if which == "open" else store.session_close(ticker, session)
    )
    days = [
        {"session": session, "s": payload["dates"][session]["s"], "rows": payload["dates"][session]["rows"]}
        for session in SESSIONS
    ]
    by_id = {spec["id"]: spec for spec in candidates()}
    wanted = list(dict.fromkeys(freeze["passers"] + freeze["top10"] + freeze["top20"]))
    results = []
    day_rows: dict[str, dict] = {}
    # Daily series for every combination, not only the frozen set.
    for spec in candidates():
        keep = walk(days, spec, fees, price_fn, "keep_held")
        renew = walk(days, spec, fees, price_fn, "renew")
        for left, right in zip(keep["daily"], renew["daily"]):
            if left["session"] not in FORWARD:
                continue
            bucket = day_rows.setdefault(left["session"], {
                "session": left["session"],
                "study": "factor_mine_recipe_search_v4",
                "recipes": {},
            })
            bucket["recipes"][spec["id"]] = {
                "ret_flat_15bp": round(left["ret_15"], 8),
                "ret_flat_15bp_renew": round(right["ret_15"], 8),
                "ret_futubull": round(left["ret"], 8),
                "ret_futubull_renew": round(right["ret"], 8),
            }
        if spec["id"] not in wanted:
            continue
        stat = summarize(_slice(keep))
        renew_stat = summarize(_slice(renew))
        if stat["n"] < MIN_TRADES or stat["joint"] is None:
            status = "unproven"
        elif stat["joint"] < REJECT_JOINT:
            status = "rejected"
        else:
            status = "not_rejected"
        results.append({
            "id": spec["id"],
            "n": stat["n"],
            "renew_compound": renew_stat["compound"],
            "status": status,
            "tune_rank_key": next(row["rank_key"] for row in freeze["rows"] if row["id"] == spec["id"]),
            **{key: stat[key] for key in (
                "compound", "compound_15", "down", "ex_best", "flat", "joint",
                "under_3", "up", "up_share", "win_rate", "named",
            )},
        })
        print(spec["id"], status, flush=True)
    manifest = RETURNS / "manifest.jsonl"
    for session in FORWARD:
        path = RETURNS / f"{session}.json"
        if path.exists():
            raise SystemExit(f"{session} already written")
        body = json.dumps(day_rows[session], sort_keys=True).encode("utf-8")
        path.write_bytes(body + b"\n")
        with manifest.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({
                "session": session,
                "sha256": file_sha256(path),
                "study": "factor_mine_recipe_search_v4",
            }, sort_keys=True) + "\n")
    out = {
        "carry_from": "2026-09-28",
        "note": "Fewer than 30 closed trades cannot prove. A joint under 0.5 with at least 30 closed trades rejects. Nothing in this window is marked proven.",
        "results": results,
        "unchanged": {
            "passers": freeze["passers"],
            "top10": freeze["top10"],
            "top20": freeze["top20"],
            "verdict": freeze["verdict"],
        },
    }
    (RETURNS / "FORWARD.json").write_text(json.dumps(out, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    lines = [
        "",
        "## Forward check 2026-09-14 through 2026-09-25",
        "",
        "This window can only reject a frozen recipe. Under 30 closed trades is unproven. The frozen list is unchanged. Recipes that are not rejected and not unproven are still not proven; the record carries from 2026-09-28.",
        "",
        "| id | status | compound | renew compound | win rate | up share | trades | joint |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in results:
        lines.append(
            "| `{id}` | {status} | {comp:.2%} | {renew:.2%} | {win} | {up} | {n} | {joint} |".format(
                id=row["id"],
                status=row["status"],
                comp=row["compound"],
                renew=row["renew_compound"],
                win="" if row["win_rate"] is None else f"{row['win_rate']:.2%}",
                up="" if row["up_share"] is None else f"{row['up_share']:.2%}",
                n=row["n"],
                joint="" if row["joint"] is None else f"{row['joint']:.2%}",
            )
        )
    lines.append("")
    report = RETURNS / "REPORT.md"
    report.write_text(report.read_text(encoding="utf-8") + "\n".join(lines), encoding="utf-8")
    print("forward wrote", len(results), flush=True)


if __name__ == "__main__":
    main()
