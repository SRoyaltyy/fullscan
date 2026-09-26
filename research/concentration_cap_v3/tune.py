"""Score through 2026-09-11 and freeze passers. Does not read a later session."""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.concentration_cap_v1.checks import (  # noqa: E402
    clip_store,
    iwm_return,
    jump_check,
    random4_mean,
)
from research.concentration_cap_v1.engine import walk  # noqa: E402
from research.concentration_cap_v1.metrics import window_row  # noqa: E402
from research.concentration_cap_v3.metrics import gross_share  # noqa: E402
from research.concentration_cap_v3.protocol import (  # noqa: E402
    FORWARD,
    FREEZE,
    PREREG,
    RETURNS,
    STARTS,
    TUNE,
    candidates,
    dependence,
    fingerprint_sha256,
    share_line_passes,
    top3_passes,
)
from research.factor_mine_recipe_search_v4.bars import CleanStore  # noqa: E402
from research.factor_mine_recipe_search_v4.protocol import INPUTS  # noqa: E402
from src.paper_trade import load_fees  # noqa: E402


def assert_tune_only(sessions: list[str]) -> None:
    for session in sessions:
        if session in FORWARD or session > TUNE[-1]:
            raise SystemExit(f"tune loaded {session}")


def is_passer(tune: dict, starts: list[dict]) -> bool:
    """Monday joints, the 20% dependence line, and R_-3 above zero."""
    joints = [item["joint"] for item in starts if item["joint"] is not None]
    if len(joints) != len(STARTS):
        return False
    return bool(
        share_line_passes(tune.get("compound"), tune.get("ex_top1"))
        and top3_passes(tune.get("ex_top3"))
    )


def load_tune_payload() -> dict:
    payload = json.loads(INPUTS.read_text(encoding="utf-8"))
    dates = payload["dates"]
    for session in list(dates):
        if session > TUNE[-1]:
            del dates[session]
    assert_tune_only(list(dates))
    return payload


def days_for(payload: dict, sessions: tuple[str, ...] | list[str]) -> list[dict]:
    assert_tune_only(list(sessions))
    out = []
    for session in sessions:
        row = payload["dates"][session]
        out.append({"session": session, "s": row["s"], "rows": row["rows"]})
    return out


def price_of(store):
    def price(ticker: str, session: str, which: str):
        if session > TUNE[-1]:
            raise SystemExit(f"tune price {session}")
        if which == "open":
            return store.session_open(ticker, session)
        if which == "close":
            return store.session_close(ticker, session)
        raise SystemExit(which)
    return price


def board_tickers(payload: dict, sessions: list[str]) -> set[str]:
    names = {"IWM"}
    for session in sessions:
        for row in payload["dates"][session]["rows"]:
            names.add(row["ticker"])
    return names


def annotate(book: dict) -> dict:
    row = window_row(book)
    row["dependence"] = dependence(row["compound"], row["ex_top1"])
    row["gross_share"] = gross_share(book)
    return row


def _rank_key(row: dict):
    return (
        -(row["rank_key"] if row["rank_key"] is not None else -1.0),
        -(row["mean_joint"] if row["mean_joint"] is not None else -1.0),
        row["id"],
    )


def score_tune(payload: dict, store, fees: dict) -> dict:
    sessions = list(TUNE)
    assert_tune_only(sessions)
    jump_check(store, board_tickers(payload, sessions), TUNE[-1])
    price = price_of(store)
    continuous_days = days_for(payload, sessions)
    start_days = {start: days_for(payload, [day for day in sessions if day >= start]) for start in STARTS}
    iwm = iwm_return(store, sessions, fees)
    random_cache: dict = {}
    rows = []
    for spec in candidates():
        book = walk(continuous_days, spec, fees, price)
        tune = annotate(book)
        starts = []
        for start in STARTS:
            part = window_row(walk(start_days[start], spec, fees, price))
            starts.append({
                "joint": part["joint"],
                "n": part["n"],
                "start": start,
                "up_share": part["up_share"],
                "win_rate": part["win_rate"],
            })
        joints = [item["joint"] for item in starts if item["joint"] is not None]
        passer = is_passer(tune, starts)
        rank_key = min(joints) if passer else None
        mean_joint = (sum(joints) / len(joints)) if joints else None
        cache_key = (
            spec["hold"], spec["s_boost"], bool(spec["weather"]), spec["sell"],
            spec.get("weight_cap"),
        )
        if cache_key not in random_cache:
            random_cache[cache_key] = random4_mean(spec, continuous_days, fees, price)
        rows.append({
            "base_id": spec["base_id"],
            "id": spec["id"],
            "mean_joint": mean_joint,
            "passer": passer,
            "random4": random_cache[cache_key],
            "rank_key": rank_key,
            "starts": starts,
            "tune": tune,
            "weight_cap": spec.get("weight_cap"),
            "width": spec["top_n"],
        })
    passers = [row for row in rows if row["passer"]]
    passers.sort(key=_rank_key)
    return {
        "iwm_tune": iwm,
        "passers": [row["id"] for row in passers],
        "rows": rows,
    }


def prereg_sha() -> str:
    proc = subprocess.run(
        ["git", "log", "-1", "--format=%H", "--", "research/concentration_cap_v3/PREREG.md"],
        cwd=ROOT, check=True, capture_output=True, text=True,
    )
    return proc.stdout.strip()


def main() -> None:
    payload = load_tune_payload()
    store = CleanStore()
    clip_store(store, TUNE[-1])
    fees = load_fees()
    scored = score_tune(payload, store, fees)
    text = PREREG.read_text(encoding="utf-8")
    freeze = {
        "fingerprint_sha256": fingerprint_sha256(text),
        "passers": scored["passers"],
        "prereg_sha": prereg_sha(),
        "study": "concentration_cap_v3",
        "through": TUNE[-1],
    }
    FREEZE.parent.mkdir(parents=True, exist_ok=True)
    FREEZE.write_text(json.dumps(freeze, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    RETURNS.mkdir(parents=True, exist_ok=True)
    body = {
        "freeze": freeze,
        "iwm_tune": scored["iwm_tune"],
        "rows": scored["rows"],
        "study": "concentration_cap_v3",
        "through": TUNE[-1],
    }
    (RETURNS / "TUNE.json").write_text(json.dumps(body, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"passers {len(scored['passers'])}")
    for item in scored["passers"]:
        print(item)


if __name__ == "__main__":
    main()
