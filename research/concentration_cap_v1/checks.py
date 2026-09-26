"""Jump gate and the two baselines. The tune caller clips dates first."""
from __future__ import annotations

import json
import random

from research.concentration_cap_v1.engine import walk
from research.concentration_cap_v1.protocol import (
    CAPITAL,
    RANDOM_DRAWS,
    RANDOM_N,
    RANDOM_SEED,
    ROOT,
    SPLIT_SHA256,
    file_sha256,
)
from research.factor_mine_recipe_search_v4.metrics import compound
from src.paper_trade import order_fees


def jump_check(store, tickers: set[str], last: str) -> None:
    path = ROOT / "research/breadth_rank_v1c/bars/splits.json"
    if file_sha256(path) != SPLIT_SHA256:
        raise SystemExit("splits sha")
    raw = json.loads(path.read_text(encoding="utf-8"))
    splits: dict[str, list] = {}
    for row in raw:
        splits.setdefault(row["ticker"], []).append((row["date"], float(row["split"])))
    for ticker in sorted(tickers):
        tape = store.tapes.get(ticker)
        if not tape:
            continue
        prev_close = None
        for idx, day in enumerate(tape["date"]):
            if day > last:
                break
            opx = tape["open"][idx]
            cpx = tape["close"][idx]
            ratios = []
            if prev_close and opx == opx and prev_close > 0 and opx > 0:
                ratios.append(opx / prev_close)
            if opx == opx and cpx == cpx and opx > 0 and cpx > 0:
                ratios.append(cpx / opx)
            for ratio in ratios:
                if ratio > 3.0 or ratio < (1.0 / 3.0):
                    ok = False
                    for date, split in splits.get(ticker, []):
                        if date != day or split <= 0:
                            continue
                        for target in (split, 1.0 / split):
                            if abs(ratio - target) / target <= 0.25:
                                ok = True
                    if not ok:
                        raise SystemExit(f"jump halt {ticker} {day} {ratio}")
            if cpx == cpx and cpx > 0:
                prev_close = cpx


def clip_store(store, last: str) -> None:
    for tape in store.tapes.values():
        keep = [i for i, day in enumerate(tape["date"]) if day <= last]
        for key in ("date", "open", "high", "low", "close"):
            tape[key] = [tape[key][i] for i in keep]
    store.dates = tuple(day for day in store.dates if day <= last)


def iwm_return(store, sessions: list[str], fees: dict) -> float | None:
    if not sessions:
        return None
    first = sessions[0]
    opx = store.session_open("IWM", first)
    if opx is None:
        return None
    shares = int((CAPITAL - float(order_fees(1, opx, "buy", fees))) // opx)
    while shares > 0 and shares * opx + float(order_fees(shares, opx, "buy", fees)) > CAPITAL + 1e-6:
        shares -= 1
    if shares < 1:
        return None
    fee = float(order_fees(shares, opx, "buy", fees))
    cash = CAPITAL - shares * opx - fee
    prev = CAPITAL
    rets = []
    for session in sessions:
        cpx = store.session_close("IWM", session)
        if cpx is None:
            return None
        equity = cash + shares * cpx
        rets.append(equity / prev - 1.0)
        prev = equity
    return compound(rets)


def random4_mean(spec: dict, days: list[dict], fees: dict, price) -> float:
    recipe = {
        "earn_news": False,
        "exit_when": {},
        "forbid": {},
        "hold": spec["hold"],
        "id": "RANDOM4",
        "name": "RANDOM4",
        "rank": None,
        "require": {},
        "s_boost": spec["s_boost"],
        "sell": spec["sell"],
        "skip_first": False,
        "top_n": RANDOM_N,
        "universe": "union",
        "weather": spec["weather"],
        "weight_cap": spec.get("weight_cap"),
    }
    compounds = []
    for draw in range(RANDOM_DRAWS):
        rng = random.Random(RANDOM_SEED + draw)
        sampled = []
        for day in days:
            pool = list(day["rows"])
            k = min(RANDOM_N, len(pool))
            picked = rng.sample(pool, k) if k else []
            sampled.append({"session": day["session"], "s": day["s"], "rows": picked})
        book = walk(sampled, recipe, fees, price, )
        compounds.append(compound([day["ret"] for day in book["daily"]]))
    return sum(compounds) / len(compounds) if compounds else 0.0
