"""Score through 2026-09-11 and freeze. Does not read a later session."""
from __future__ import annotations

import json
import random
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.factor_mine_recipe_search_v4.bars import CleanStore  # noqa: E402
from research.factor_mine_recipe_search_v4.engine import assert_fills, walk  # noqa: E402
from research.factor_mine_recipe_search_v4.metrics import summarize  # noqa: E402
from research.factor_mine_recipe_search_v4.protocol import (  # noqa: E402
    BASE_ID,
    CAPITAL,
    FORWARD,
    FREEZE,
    INPUTS,
    LUCK_N,
    MIN_TRADES,
    PICKED_ID,
    RANDOM_DRAWS,
    RANDOM_N,
    RANDOM_SEED,
    RETURNS,
    SPLIT_SHA256,
    STARTS,
    TUNE,
    candidates,
    file_sha256,
)
from src.paper_trade import load_fees, order_fees  # noqa: E402


def _days(payload: dict, store: CleanStore, sessions: tuple[str, ...]) -> list[dict]:
    out = []
    for session in sessions:
        if session in FORWARD or session > TUNE[-1]:
            raise SystemExit(f"tune loaded {session}")
        row = payload["dates"][session]
        out.append({"session": session, "s": row["s"], "rows": row["rows"]})
    return out


def _price(store: CleanStore):
    def price(ticker: str, session: str, which: str):
        if which == "open":
            return store.session_open(ticker, session)
        if which == "close":
            return store.session_close(ticker, session)
        raise SystemExit(which)
    return price


def _jump_check(store: CleanStore, tickers: set[str]) -> None:
    raw = json.loads((ROOT / "research/breadth_rank_v1c/bars/splits.json").read_text(encoding="utf-8"))
    if file_sha256(ROOT / "research/breadth_rank_v1c/bars/splits.json") != SPLIT_SHA256:
        raise SystemExit("splits sha")
    splits = {}
    for row in raw:
        splits.setdefault(row["ticker"], []).append((row["date"], float(row["split"])))
    last = TUNE[-1]
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


def _iwm(store: CleanStore, sessions: list[str], fees: dict) -> float | None:
    first = sessions[0]
    opx = store.session_open("IWM", first)
    if opx is None:
        return None
    fee = float(order_fees(1, opx, "buy", fees))
    shares = int((CAPITAL - fee) // opx)
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
    from research.factor_mine_recipe_search_v4.metrics import compound
    return compound(rets)


def _random_mean(spec: dict, days: list[dict], fees: dict, price) -> float:
    from research.factor_mine_recipe_search_v4.metrics import compound
    recipe = {
        "earn_news": False,
        "exit_when": spec["exit_when"],
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
        book = walk(sampled, recipe, fees, price, "keep_held")
        compounds.append(compound([day["ret"] for day in book["daily"]]))
    return sum(compounds) / len(compounds) if compounds else 0.0


def _rank_row(stats: list[dict]) -> dict:
    joints = [row["joint"] for row in stats if row["joint"] is not None]
    cleared = len(joints)
    passer = cleared == len(stats) and len(stats) == len(STARTS)
    rank_key = min(joints) if passer else None
    mean_joint = (sum(joints) / len(joints)) if joints else None
    return {
        "cleared": cleared,
        "mean_joint": mean_joint,
        "passer": passer,
        "rank_key": rank_key,
    }


def _sort_key(row: dict):
    return (
        0 if row["passer"] else 1,
        -(row["rank_key"] if row["rank_key"] is not None else -1),
        -(row["mean_joint"] if row["mean_joint"] is not None else -1),
        -row["cleared"],
        row["id"],
    )


def _pct(value) -> str:
    if value is None:
        return ""
    return f"{100.0 * float(value):.2f}%"


def _report(rows: list[dict], iwm: dict) -> str:
    lines = [
        "# factor_mine_recipe_search_v4 tune",
        "",
        "Ranked through 2026-09-11 only. Keep-held is the Futubull figure. Renew is the old sell-then-rebuy figure beside it. One try covers both.",
        "",
        f"Luck N is {LUCK_N}. A table sorted for reading is the freeze, not a live record.",
        "",
        "| rank | id | passer | rank key | tune compound | renew compound | win rate | up share | trades | <30 | ex-best | under $3 | CYPH out | GLND out | INDP out | RANDOM4 | IWM |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for index, row in enumerate(rows, start=1):
        cont = row["continuous"]
        lines.append(
            "| {rank} | `{id}` | {passer} | {key} | {comp} | {renew} | {win} | {up} | {n} | {flag} | {ex} | {under} | {cyph} | {glnd} | {indp} | {rnd} | {iwm} |".format(
                rank=index,
                id=row["id"],
                passer="yes" if row["passer"] else "",
                key=_pct(row["rank_key"]),
                comp=_pct(cont["compound"]),
                renew=_pct(row["renew_continuous"]["compound"]),
                win=_pct(cont["win_rate"]),
                up=_pct(cont["up_share"]),
                n=cont["n"],
                flag="yes" if cont["n"] < MIN_TRADES else "",
                ex=_pct(cont["ex_best"]),
                under=_pct(cont["under_3"]),
                cyph=_pct(cont["named"]["CYPH"]),
                glnd=_pct(cont["named"]["GLND"]),
                indp=_pct(cont["named"]["INDP"]),
                rnd=_pct(row["random4"]),
                iwm=_pct(iwm.get("continuous")),
            )
        )
    lines.append("")
    lines.append(f"Verdict: {rows[0]['verdict'] if rows else 'keep base'}.")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    assert_fills()
    fees = load_fees()
    payload = json.loads(INPUTS.read_text(encoding="utf-8"))
    if list(payload["dates"]) != list(TUNE) + list(FORWARD):
        raise SystemExit("input sessions")
    store = CleanStore()
    tickers = {"IWM"}
    for session in TUNE:
        for row in payload["dates"][session]["rows"]:
            tickers.add(row["ticker"])
    _jump_check(store, tickers)
    price = _price(store)
    full = _days(payload, store, TUNE)
    by_start = {start: [day for day in full if day["session"] >= start] for start in STARTS}
    specs = candidates()
    books = {}
    for spec in specs:
        books[spec["id"]] = {
            "keep": {
                "continuous": walk(full, spec, fees, price, "keep_held"),
                "starts": {start: walk(by_start[start], spec, fees, price, "keep_held") for start in STARTS},
            },
            "renew": {
                "continuous": walk(full, spec, fees, price, "renew"),
                "starts": {start: walk(by_start[start], spec, fees, price, "renew") for start in STARTS},
            },
        }
        print(spec["id"], flush=True)
    random_cache = {}
    iwm = {
        "continuous": _iwm(store, list(TUNE), fees),
        **{start: _iwm(store, [day["session"] for day in by_start[start]], fees) for start in STARTS},
    }
    ranked = []
    for spec in specs:
        keep = books[spec["id"]]["keep"]
        renew = books[spec["id"]]["renew"]
        start_stats = [summarize(keep["starts"][start]) for start in STARTS]
        head = _rank_row(start_stats)
        shape = (
            spec["hold"], spec["sell"], spec["s_boost"],
            json.dumps(spec["exit_when"], sort_keys=True), bool(spec["weather"]),
        )
        if ("continuous",) + shape not in random_cache:
            random_cache[("continuous",) + shape] = _random_mean(spec, full, fees, price)
            for start in STARTS:
                random_cache[(start,) + shape] = _random_mean(spec, by_start[start], fees, price)
            print("random", shape[0], shape[1], spec["weather"], flush=True)
        ranked.append({
            "already_picked": spec["id"] == PICKED_ID,
            "base": spec["id"] == BASE_ID,
            "continuous": summarize(keep["continuous"]),
            "id": spec["id"],
            "random4": random_cache[("continuous",) + shape],
            "random4_starts": {start: random_cache[(start,) + shape] for start in STARTS},
            "renew_continuous": summarize(renew["continuous"]),
            "starts": {start: summarize(keep["starts"][start]) for start in STARTS},
            "renew_starts": {start: summarize(renew["starts"][start]) for start in STARTS},
            **head,
        })
    ranked.sort(key=_sort_key)
    base = next(row for row in ranked if row["id"] == BASE_ID)
    beater = None
    for row in ranked:
        if row["id"] == PICKED_ID or not row["passer"]:
            continue
        if base["passer"] and row["rank_key"] is not None and base["rank_key"] is not None:
            if row["rank_key"] > base["rank_key"]:
                beater = row["id"]
                break
        elif row["passer"] and not base["passer"]:
            beater = row["id"]
            break
    verdict = "keep base" if beater is None else f"{beater} beats the base"
    for row in ranked:
        row["verdict"] = verdict
    RETURNS.mkdir(parents=True, exist_ok=True)
    by_session: dict[str, dict] = {}
    for spec in specs:
        keep = books[spec["id"]]["keep"]["continuous"]["daily"]
        renew = books[spec["id"]]["renew"]["continuous"]["daily"]
        for left, right in zip(keep, renew):
            bucket = by_session.setdefault(left["session"], {
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
    manifest = RETURNS / "manifest.jsonl"
    for session in TUNE:
        body = json.dumps(by_session[session], sort_keys=True).encode("utf-8")
        path = RETURNS / f"{session}.json"
        path.write_bytes(body + b"\n")
        digest = file_sha256(path)
        with manifest.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({
                "session": session, "sha256": digest, "study": "factor_mine_recipe_search_v4",
            }, sort_keys=True) + "\n")
    start_dir = RETURNS / "starts"
    start_dir.mkdir(parents=True, exist_ok=True)
    for spec in specs:
        payload_s = {"id": spec["id"], "fills": {}}
        for label in ("keep", "renew"):
            payload_s["fills"][label] = {
                start: [round(day["ret"], 8) for day in books[spec["id"]][label]["starts"][start]["daily"]]
                for start in STARTS
            }
        (start_dir / f"{spec['id']}.json").write_text(
            json.dumps(payload_s, indent=2, sort_keys=True) + "\n", encoding="utf-8",
        )
    frozen = [row["id"] for row in ranked]
    freeze = {
        "base": BASE_ID,
        "luck_n": LUCK_N,
        "passers": [row["id"] for row in ranked if row["passer"]],
        "picked_already": PICKED_ID,
        "rows": [
            {
                "cleared": row["cleared"],
                "id": row["id"],
                "mean_joint": row["mean_joint"],
                "passer": row["passer"],
                "rank_key": row["rank_key"],
                "tune_compound": row["continuous"]["compound"],
                "tune_compound_renew": row["renew_continuous"]["compound"],
            }
            for row in ranked
        ],
        "through": TUNE[-1],
        "top10": frozen[:10],
        "top20": frozen[:20],
        "verdict": verdict,
    }
    FREEZE.parent.mkdir(parents=True, exist_ok=True)
    FREEZE.write_text(json.dumps(freeze, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (RETURNS / "REPORT.md").write_text(_report(ranked, iwm), encoding="utf-8")
    (RETURNS / "RESULTS.json").write_text(json.dumps({
        "iwm": iwm,
        "ranked": ranked,
        "verdict": verdict,
    }, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(verdict, flush=True)


if __name__ == "__main__":
    main()
