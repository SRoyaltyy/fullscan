"""Reject-only check from 2026-09-14. Requires the tune freeze. Does not re-rank."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.concentration_cap_v1.checks import iwm_return, jump_check, random4_mean  # noqa: E402
from research.concentration_cap_v1.engine import walk  # noqa: E402
from research.concentration_cap_v1.metrics import slice_book, window_row  # noqa: E402
from research.concentration_cap_v1.protocol import (  # noqa: E402
    CAPITAL,
    FORWARD,
    FORWARD_FIRST,
    FREEZE,
    LUCK_N,
    MIN_TRADES,
    PREREG,
    REJECT_JOINT,
    RETURNS,
    SESSIONS,
    TUNE,
    candidates,
    fingerprint_sha256,
)
from research.factor_mine_recipe_search_v4.bars import CleanStore  # noqa: E402
from research.factor_mine_recipe_search_v4.protocol import INPUTS  # noqa: E402
from src.forward_shadow_v1 import canon, recipe_fingerprint, validate_winner  # noqa: E402
from src.paper_trade import load_fees  # noqa: E402

HOOK = ROOT / "research/forward_shadow_v1/hooks/v4_winners.json"


def _pct(value) -> str:
    if value is None:
        return ""
    return f"{100.0 * float(value):.2f}%"


def _days(payload: dict, sessions) -> list[dict]:
    out = []
    for session in sessions:
        row = payload["dates"][session]
        out.append({"session": session, "s": row["s"], "rows": row["rows"]})
    return out


def _price(store):
    def price(ticker: str, session: str, which: str):
        if which == "open":
            return store.session_open(ticker, session)
        if which == "close":
            return store.session_close(ticker, session)
        raise SystemExit(which)
    return price


def _tickers(payload: dict) -> set[str]:
    names = {"IWM"}
    for session in SESSIONS:
        for row in payload["dates"][session]["rows"]:
            names.add(row["ticker"])
    return names


def _match(got, exp, label: str) -> None:
    if isinstance(exp, float) or isinstance(got, float):
        if exp is None or got is None:
            if exp != got:
                raise SystemExit(f"tune prefix drift {label}")
            return
        if abs(float(got) - float(exp)) > 1e-8:
            raise SystemExit(f"tune prefix drift {label} {got} vs {exp}")
        return
    if got != exp:
        raise SystemExit(f"tune prefix drift {label} {got} vs {exp}")


def _hook_recipe(spec: dict) -> dict:
    name = "fwd_ccap_" + spec["id"]
    recipe = {
        "cap_cash": "sit",
        "earn_news": False,
        "exit_when": dict(spec.get("exit_when") or {}),
        "forbid": dict(spec.get("forbid") or {}),
        "hold": int(spec["hold"]),
        "name": name,
        "rank": spec["rank"],
        "require": dict(spec.get("require") or {}),
        "s_boost": spec.get("s_boost") or "none",
        "sell": spec.get("sell") or "list",
        "side": "long",
        "skip_first": False,
        "source_id": spec["base_id"],
        "source_name": spec["source_name"],
        "study": "concentration_cap_v1",
        "top_n": int(spec["top_n"]),
        "universe": "union",
        "weather": bool(spec["weather"]),
    }
    if spec.get("weight_cap") is not None:
        recipe["weight_cap"] = float(spec["weight_cap"])
    return recipe


def _winner(spec: dict, prereg: str) -> dict:
    recipe = _hook_recipe(spec)
    winner = {
        "fingerprint_commit": prereg,
        "fingerprint_sha256": recipe_fingerprint(recipe),
        "first_session": FORWARD_FIRST,
        "name": recipe["name"],
        "recipe": recipe,
    }
    validate_winner(winner, last_session=None, commit_day="2026-09-26")
    return winner


def _write_hook(winners: list[dict]) -> None:
    doc = json.loads(HOOK.read_text(encoding="utf-8"))
    existing = list(doc.get("winners") or [])
    ours = [row for row in existing if str(row.get("name") or "").startswith("fwd_ccap_")]
    others = [row for row in existing if not str(row.get("name") or "").startswith("fwd_ccap_")]
    if ours:
        if [canon(row) for row in ours] != [canon(row) for row in winners]:
            raise SystemExit("v4 hook already has a different concentration_cap carry")
        return
    if not winners:
        return
    doc["winners"] = others + winners
    HOOK.write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")


def _report(freeze: dict, tune_rows: list[dict], forward_rows: list[dict], iwm_tune, iwm_p2) -> str:
    by_fwd = {row["id"]: row for row in forward_rows}
    carry = [row["id"] for row in forward_rows if row["carry"]]
    lines = [
        "# concentration_cap_v1",
        "",
        "Research only. Every session scored here is `designed_after`. No real money follows from it.",
        "",
        f"Prereg commit `{freeze['prereg_sha']}`. Fingerprint `{freeze['fingerprint_sha256']}`.",
        f"Luck N is {LUCK_N}. That is 45 tries plus 21,536 plus 260 from concentration_screen_v1.",
        "Leftover cash from a trim sits in cash. A trim does not add shares to a name that is already held.",
        f"Tune IWM { _pct(iwm_tune) }. P2 IWM { _pct(iwm_p2) }. P2 starts at the 2026-09-11 close.",
        "The width-4 cap-none P2 return without the top stock matches the corrected v4 keep-held reading: `union_hot_n4_h1__w0` +5.62%, `union_hot_n4_holdup__w0` +11.25%, `union_hot_n4_h1_nonews__w0` +5.04%.",
        "",
        "## Frozen and not rejected",
        "",
    ]
    if not carry:
        lines.append("None. No recipe was appended to the forward hook.")
        lines.append("")
    else:
        lines.append("| id | worst joint | tune return | tune ex-top3 | P2 return | P2 ex-top3 | P2 trades | P2 win rate |")
        lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
        tune_by = {row["id"]: row for row in tune_rows}
        for ident in carry:
            tune = tune_by[ident]
            fwd = by_fwd[ident]
            lines.append(
                f"| `{ident}` | {_pct(tune['rank_key'])} | {_pct(tune['tune']['compound'])} | "
                f"{_pct(tune['tune']['ex_top3'])} | {_pct(fwd['p2']['compound'])} | "
                f"{_pct(fwd['p2']['ex_top3'])} | {fwd['p2']['n']} | {_pct(fwd['p2']['win_rate'])} |"
            )
        lines.append("")
        lines.append("Each of these is appended to `research/forward_shadow_v1/hooks/v4_winners.json` as `fwd_ccap_` plus the id. `first_session` is 2026-09-28.")
        lines.append("")
    lines.extend([
        "## Tune, 2026-08-13 through 2026-09-11",
        "",
        "| id | passer | worst joint | return | 15bp | ex top 1 | ex top 3 | ex top 5 | best share | tickers | trades | win rate | RANDOM4 |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ])
    for row in tune_rows:
        tune = row["tune"]
        lines.append(
            f"| `{row['id']}` | {row['passer']} | {_pct(row['rank_key'])} | {_pct(tune['compound'])} | "
            f"{_pct(tune['compound_15'])} | {_pct(tune['ex_top1'])} | {_pct(tune['ex_top3'])} | "
            f"{_pct(tune['ex_top5'])} | {_pct(tune['profit_share'])} | {tune['distinct']} | "
            f"{tune['n']} | {_pct(tune['win_rate'])} | {_pct(row.get('random4'))} |"
        )
    lines.extend([
        "",
        "## P2, 2026-09-14 through 2026-09-25",
        "",
        "A row that was not frozen cannot be rejected and cannot be carried. Rejected means a frozen passer with at least 30 P2 trades and a joint under 0.5.",
        "",
        "| id | frozen | rejected | return | 15bp | ex top 1 | ex top 3 | ex top 5 | best share | tickers | trades | win rate | RANDOM4 |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ])
    for row in forward_rows:
        p2 = row["p2"]
        lines.append(
            f"| `{row['id']}` | {row['frozen']} | {row['rejected']} | {_pct(p2['compound'])} | "
            f"{_pct(p2['compound_15'])} | {_pct(p2['ex_top1'])} | {_pct(p2['ex_top3'])} | "
            f"{_pct(p2['ex_top5'])} | {_pct(p2['profit_share'])} | {p2['distinct']} | "
            f"{p2['n']} | {_pct(p2['win_rate'])} | {_pct(row.get('random4'))} |"
        )
    lines.extend([
        "",
        "RANDOM4 is the mean of 1000 draws, seed 20260813, 4 names, the recipe's hold, sell, holdup, weather, and weight cap. IWM is buy-and-hold on that window after the Futubull entry fee.",
        f"Keep bar is at least {MIN_TRADES} closed trades and a win rate above 55%. It does not add or remove a frozen row.",
        "",
    ])
    return "\n".join(lines)


def main() -> None:
    if not FREEZE.is_file():
        raise SystemExit("freeze missing; the forward check cannot run")
    freeze = json.loads(FREEZE.read_text(encoding="utf-8"))
    if freeze.get("through") != TUNE[-1]:
        raise SystemExit("freeze is not the tune lock")
    text = PREREG.read_text(encoding="utf-8")
    if freeze.get("fingerprint_sha256") != fingerprint_sha256(text):
        raise SystemExit("freeze fingerprint does not match the prereg")
    tune_doc = json.loads((RETURNS / "TUNE.json").read_text(encoding="utf-8"))
    tune_by = {row["id"]: row for row in tune_doc["rows"]}
    fees = load_fees()
    payload = json.loads(INPUTS.read_text(encoding="utf-8"))
    store = CleanStore()
    jump_check(store, _tickers(payload), FORWARD[-1])
    price = _price(store)
    full_days = _days(payload, SESSIONS)
    p2_days = _days(payload, FORWARD)
    iwm_p2 = iwm_return(store, list(FORWARD), fees)
    specs = {spec["id"]: spec for spec in candidates()}
    if set(tune_by) != set(specs):
        raise SystemExit("tune file is not the 45")
    random_cache: dict = {}
    forward_rows = []
    for spec in candidates():
        book = walk(full_days, spec, fees, price)
        prior = [day for day in book["daily"] if day["session"] in set(TUNE)]
        if not prior or prior[-1]["session"] != TUNE[-1]:
            raise SystemExit("tune prefix missing")
        tune_slice = slice_book(book, TUNE, CAPITAL)
        got = window_row(tune_slice)
        exp = tune_by[spec["id"]]["tune"]
        for key in ("compound", "ex_top1", "ex_top3", "ex_top5", "n", "win_rate"):
            _match(got[key], exp[key], f"{spec['id']} {key}")
        p2 = window_row(slice_book(book, FORWARD, float(prior[-1]["equity"])))
        frozen = spec["id"] in set(freeze["passers"])
        rejected = bool(frozen and p2["n"] >= MIN_TRADES and p2["joint"] is not None and p2["joint"] < REJECT_JOINT)
        cache_key = (
            spec["hold"], spec["s_boost"], bool(spec["weather"]), spec["sell"], spec.get("weight_cap"),
        )
        if cache_key not in random_cache:
            random_cache[cache_key] = random4_mean(spec, p2_days, fees, price)
        forward_rows.append({
            "carry": bool(frozen and not rejected),
            "frozen": frozen,
            "id": spec["id"],
            "p2": p2,
            "p2_end_equity": p2["end_equity"],
            "p2_start_equity": p2["start_equity"],
            "random4": random_cache[cache_key],
            "rejected": rejected,
        })
    carry_ids = [row["id"] for row in forward_rows if row["carry"]]
    winners = [_winner(specs[ident], freeze["prereg_sha"]) for ident in carry_ids]
    _write_hook(winners)
    body = {
        "carry": carry_ids,
        "fingerprint_sha256": freeze["fingerprint_sha256"],
        "iwm_p2": iwm_p2,
        "iwm_tune": tune_doc.get("iwm_tune"),
        "luck_n": LUCK_N,
        "prereg_sha": freeze["prereg_sha"],
        "rejected": [row["id"] for row in forward_rows if row["rejected"]],
        "rows": forward_rows,
        "study": "concentration_cap_v1",
        "through": FORWARD[-1],
    }
    RETURNS.mkdir(parents=True, exist_ok=True)
    (RETURNS / "FORWARD.json").write_text(json.dumps(body, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    report = _report(freeze, tune_doc["rows"], forward_rows, tune_doc.get("iwm_tune"), iwm_p2)
    (RETURNS / "REPORT.md").write_text(report, encoding="utf-8")
    print(f"carry {len(carry_ids)} rejected {sum(1 for row in forward_rows if row['rejected'])}")
    for ident in carry_ids:
        print(ident)


if __name__ == "__main__":
    main()
