"""Score factor_mine_avg_v1. The preregistration is not edited.

Writes research/factor_mine_avg_v1/returns/ once. A second write needs
FACTOR_MINE_AVG_RERUN=1 and still fails CI if a committed result changes.
"""
from __future__ import annotations

import hashlib
import json
import os
import random
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.factor_mine_avg_v1.protocol import (  # noqa: E402
    AFTER,
    BAR_PIN,
    BEFORE,
    RANDOM4_DRAWS,
    RANDOM4_N,
    RANDOM4_SEED,
    REPORT,
    RETURNS,
    SESSIONS,
    WINDOWS,
    best_ticker,
    compound,
    day_counts,
    ex_best_compound,
    load_drop,
    mean,
    median,
    prereg_fingerprint,
    universe,
)
from src.lever_search_proof import required_roles  # noqa: E402
from src.lever_search_score import (  # noqa: E402
    _first_blob,
    _git_blob,
    _row,
    _stock_lists,
    load_proof_rows,
    role_usable,
    walk_recipe,
)
from src.paper_trade import load_fees, order_fees  # noqa: E402

SUMMARY = RETURNS / "summary.json"


class Bars:
    """Pinned Yahoo bars. Feature reads stop the day before the session."""

    def __init__(self, frame) -> None:
        from src.lever_search_score import Store

        self._feat: dict = {}
        self.dates = tuple(sorted(frame["date"].unique()))
        self.tapes: dict[str, dict] = {}
        for ticker, group in frame.groupby("ticker", sort=False):
            group = group.sort_values("date")
            self.tapes[str(ticker)] = {
                "date": group["date"].tolist(),
                "open": group["open"].to_numpy(dtype=float),
                "high": group["high"].to_numpy(dtype=float),
                "low": group["low"].to_numpy(dtype=float),
                "close": group["close"].to_numpy(dtype=float),
                "volume": group["volume"].to_numpy(dtype=float),
            }
        # Reuse the pinned feature and fill rules without a second copy.
        self._methods = Store

    def session_open(self, ticker: str, session: str):
        return self._methods.session_open(self, ticker, session)

    def session_close(self, ticker: str, session: str):
        return self._methods.session_close(self, ticker, session)

    def feature(self, ticker: str, session: str) -> dict:
        return self._methods.feature(self, ticker, session)


class Hide:
    """Same bars, with the dropped names unable to fill or mark."""

    def __init__(self, inner: Bars, banned: set[str]) -> None:
        self.inner = inner
        self.banned = banned

    def session_open(self, ticker: str, session: str):
        if ticker in self.banned:
            return None
        return self.inner.session_open(ticker, session)

    def session_close(self, ticker: str, session: str):
        if ticker in self.banned:
            return None
        return self.inner.session_close(ticker, session)

    def feature(self, ticker: str, session: str) -> dict:
        if ticker in self.banned:
            return {"ok": False}
        return self.inner.feature(ticker, session)


def _pct(value) -> str:
    if value is None:
        return "n/a"
    return f"{float(value) * 100:.2f}%"


def _num(value, digits: int = 2) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.{digits}f}"


def _load_frame():
    import pandas as pd

    path = ROOT / BAR_PIN["path"]
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    if digest != BAR_PIN["sha256"]:
        raise SystemExit(f"bar sha {digest} != {BAR_PIN['sha256']}")
    frame = pd.read_parquet(path, columns=["date", "ticker", "open", "high", "low", "close", "volume"])
    frame["date"] = pd.to_datetime(frame["date"]).dt.strftime("%Y-%m-%d")
    frame["ticker"] = frame["ticker"].astype(str)
    return frame


def _header_fingerprint() -> str:
    for line in (ROOT / "research/factor_mine_avg_v1/PREREG.md").read_text(encoding="utf-8").splitlines():
        if line.startswith("- fingerprint_sha256:"):
            return line.split(":", 1)[1].strip()
    raise SystemExit("fingerprint header missing")


def _stock_rows(store: Bars, proof: dict, session: str, banned: set[str]) -> list[dict]:
    if not role_usable(proof, session, "stock_book"):
        return []
    meta = _first_blob(proof, session, "stock_book")
    if not meta:
        return []
    names, _flatten = _stock_lists(_git_blob(meta["blob_sha"]))
    spy = store.feature("SPY", session)
    spy_ret5 = float(spy["ret_5"]) if spy.get("ok") and spy.get("ret_5") is not None else None
    rows = []
    rank = 0
    for ticker in names:
        if ticker in banned:
            continue
        rows.append(_row(store, session, ticker, ["stock_book"], rank, {}, {}, None, spy_ret5))
        rank += 1
    return rows


def _search_ok(recipe: dict, proof: dict) -> dict[str, bool]:
    roles = required_roles(recipe)
    return {day: all(role_usable(proof, day, role) for role in roles) for day in SESSIONS}


def _pnl_by_day(book: dict) -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = {}
    for ticker, by_day in (book.get("ticker_pnl") or {}).items():
        for session, pnl in by_day.items():
            out.setdefault(session, {})[ticker] = float(pnl)
    return out


def _closed(days: list[dict], fees: dict, window: tuple[str, ...]) -> list[float]:
    lots: dict[str, tuple[int, float, float]] = {}
    keep = set(window)
    out = []
    for day in days:
        for fill in day["fills"]:
            ticker = fill["ticker"]
            shares = int(fill["shares"])
            px = float(fill["price"])
            if fill["side"] == "BUY":
                lots[ticker] = (shares, px, order_fees(shares, px, "buy", fees))
                continue
            if fill["side"] != "SELL":
                continue
            buy = lots.pop(ticker, None)
            if buy is None or day["session"] not in keep:
                continue
            b_shares, b_px, b_fee = buy
            cost = b_shares * b_px + b_fee
            proceeds = shares * px - order_fees(shares, px, "sell", fees)
            pnl = proceeds - cost
            out.append(pnl / cost if cost else 0.0)
    return out


def _window_stats(book: dict, fees: dict, window: tuple[str, ...], sessions: tuple[str, ...]) -> dict:
    ret_by = {day["session"]: float(day["ret_futubull"]) for day in book["days"]}
    returns = [ret_by[session] for session in sessions]
    check = [ret_by[session] for session in window]
    pnl = _pnl_by_day(book)
    totals: dict[str, float] = {}
    for session in window:
        for ticker, value in pnl.get(session, {}).items():
            totals[ticker] = totals.get(ticker, 0.0) + value
    best = best_ticker(totals, book.get("first_entry") or {})
    trades = _closed(book["days"], fees, window)
    up, down, flat = day_counts(check)
    return {
        "best": best,
        "compound": compound(check) if check else 0.0,
        "down": down,
        "ex_best": ex_best_compound(list(sessions), list(window), returns, pnl, best),
        "flat": flat,
        "n_closed": len(trades),
        "pnl": totals,
        "up": up,
        "win_rate": (sum(1 for value in trades if value > 0) / len(trades)) if trades else None,
    }


def _blank_recipe(name: str, hold: int, top_n: int) -> dict:
    return {
        "name": name,
        "universe": "union",
        "hold": hold,
        "side": "long",
        "top_n": top_n,
        "require": {},
        "forbid": {},
        "rank": None,
        "exit_when": {},
    }


def _pick_rows(session: str, names: list[str]) -> list[dict]:
    return [
        {"date": session, "ticker": ticker, "sources": ["union"], "src_rank": i}
        for i, ticker in enumerate(names)
    ]


def _pool(rows: list[dict], store, session: str) -> list[str]:
    out = []
    for row in rows:
        ticker = row["ticker"]
        px = store.session_open(ticker, session)
        if px is None or px <= 0:
            continue
        out.append(ticker)
    return out


def _iwm(store, fees: dict) -> dict:
    recipe = _blank_recipe("iwm", 10000, 1)
    rows = {}
    for session in SESSIONS:
        if store.session_open("IWM", session) is None:
            raise SystemExit(f"IWM missing {session}")
        rows[session] = _pick_rows(session, ["IWM"])
    ok = {session: True for session in SESSIONS}
    return walk_recipe(recipe, list(SESSIONS), rows, store, ok, fees)


def _random4(store, fees: dict, pools: dict[str, list[str]]) -> list[dict]:
    recipe = _blank_recipe("random4", 1, 4)
    ok = {session: True for session in SESSIONS}
    books = []
    for draw_i in range(RANDOM4_DRAWS):
        rng = random.Random(RANDOM4_SEED + draw_i)
        rows = {}
        for session in SESSIONS:
            pool = list(pools.get(session) or [])
            k = min(RANDOM4_N, len(pool))
            picks = rng.sample(pool, k) if k else []
            rows[session] = _pick_rows(session, picks)
        books.append(walk_recipe(recipe, list(SESSIONS), rows, store, ok, fees))
        if draw_i % 200 == 0:
            print(f"random4 {draw_i}", flush=True)
    return books


def _agg(stats: list[dict]) -> dict:
    compounds = [row["compound"] for row in stats]
    ups = [row["up"] for row in stats]
    downs = [row["down"] for row in stats]
    flats = [row["flat"] for row in stats]
    ex = [row["ex_best"] for row in stats if row["ex_best"] is not None]
    wins = [row["win_rate"] for row in stats if row["win_rate"] is not None]
    return {
        "compound_mean": mean(compounds),
        "compound_median": median(compounds),
        "down_mean": mean(downs),
        "down_median": median(downs),
        "ex_best_mean": mean(ex),
        "ex_best_median": median(ex),
        "flat_mean": mean(flats),
        "flat_median": median(flats),
        "n": len(stats),
        "positive_share": (sum(1 for value in compounds if value > 0) / len(compounds)) if compounds else None,
        "up_mean": mean(ups),
        "up_median": median(ups),
        "win_omitted": len(stats) - len(wins),
        "win_rate_mean": mean(wins),
        "win_rate_median": median(wins),
    }


def _share(stats: list[dict], names: set[str]) -> dict:
    dropped = 0.0
    total = 0.0
    ratios = []
    for row in stats:
        part = sum(value for ticker, value in row["pnl"].items() if ticker in names)
        whole = sum(row["pnl"].values())
        dropped += part
        total += whole
        if whole != 0.0:
            ratios.append(part / whole)
    return {
        "dropped_pnl": dropped,
        "mean_share": mean(ratios),
        "n_with_pnl": len(ratios),
        "share_of_sum": (dropped / total) if total != 0.0 else None,
        "total_pnl": total,
    }


def _bench(books: list[dict], fees: dict, window: tuple[str, ...]) -> dict:
    compounds = [_window_stats(book, fees, window, SESSIONS)["compound"] for book in books]
    return {"compound_mean": mean(compounds), "compound_median": median(compounds), "n": len(compounds)}


def _render(payload: dict) -> str:
    lines = [
        "# factor_mine_avg_v1",
        "",
        "study: factor_mine_avg_v1",
        "",
        f"Preregistration fingerprint `{payload['fingerprint']}`.",
        "Every session is `designed_after`. The study was created 2026-09-26.",
        "",
        f"Universe: {payload['n_recipes']} long top-N ranked buys from the #357 110 that also sit in the #336 book.",
        "",
        "| recipe | rank | top N | hold |",
        "| --- | --- | ---: | ---: |",
    ]
    for recipe in payload["recipes"]:
        lines.append(f"| `{recipe['name']}` | {recipe['rank']} | {recipe['top_n']} | {recipe['hold']} |")
    lines += [
        "",
        f"Bars: `{BAR_PIN['path']}` sha256 `{BAR_PIN['sha256']}`.",
        f"Drop list: #362 `{payload['clean_commit']}`, cleaned snapshot sha256 `{payload['cleaned_parquet_sha256']}`.",
        f"Version (a) removes {payload['n_dropped']} names. {payload['n_dropped_in_pin']} of them have bars on this pin.",
        "Version (b) keeps them. Flagged matched splits: ALP, NFE, TNMG, WCT.",
        "The 52-name open/previous-close subset is inside the 77. YAAS is in the 77 and outside the 52.",
        "",
    ]
    for window, label in (("before_0914", "Before 2026-09-14, through 2026-09-11"), ("from_0914", "2026-09-14 through 2026-09-25")):
        lines += [f"## {label}", ""]
        lines.append("| version | compound mean | compound median | up mean/median | down mean/median | flat mean/median | win mean | win median | win omitted | ex-best mean | ex-best median | positive share |")
        lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
        for version in ("a", "b"):
            row = payload["windows"][window][version]["recipes"]
            lines.append(
                "| {version} | {cmean} | {cmed} | {up} | {down} | {flat} | {wmean} | {wmed} | {omit} | {exmean} | {exmed} | {pos} |".format(
                    version=version,
                    cmean=_pct(row["compound_mean"]),
                    cmed=_pct(row["compound_median"]),
                    up=f"{_num(row['up_mean'])} / {_num(row['up_median'])}",
                    down=f"{_num(row['down_mean'])} / {_num(row['down_median'])}",
                    flat=f"{_num(row['flat_mean'])} / {_num(row['flat_median'])}",
                    wmean=_pct(row["win_rate_mean"]),
                    wmed=_pct(row["win_rate_median"]),
                    omit=row["win_omitted"],
                    exmean=_pct(row["ex_best_mean"]),
                    exmed=_pct(row["ex_best_median"]),
                    pos=_pct(row["positive_share"]),
                )
            )
        gap = payload["windows"][window]["gap"]
        lines += [
            "",
            f"Mean compound (b) minus (a): {_pct(gap['compound_mean_b_minus_a'])}.",
            f"Median compound (b) minus (a): {_pct(gap['compound_median_b_minus_a'])}.",
            f"P&L share of all {payload['n_dropped']} dropped names on (b): {_pct(gap['dropped_77']['share_of_sum'])} of summed ticker dollars ({_num(gap['dropped_77']['dropped_pnl'], 2)} / {_num(gap['dropped_77']['total_pnl'], 2)}). Equal-weight mean of per-recipe shares: {_pct(gap['dropped_77']['mean_share'])}.",
            f"P&L share of the 52-name subset on (b): {_pct(gap['dropped_52']['share_of_sum'])}. Equal-weight mean of per-recipe shares: {_pct(gap['dropped_52']['mean_share'])}.",
            f"P&L share of ALP, NFE, TNMG, WCT on (b): {_pct(gap['matched_splits']['share_of_sum'])}. Equal-weight mean of per-recipe shares: {_pct(gap['matched_splits']['mean_share'])}.",
            "",
            "| benchmark | version | compound mean | compound median | up | down | flat |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
        ]
        for version in ("a", "b"):
            iwm = payload["windows"][window][version]["iwm"]
            rnd = payload["windows"][window][version]["random4"]
            lines.append(
                f"| IWM | {version} | {_pct(iwm['compound'])} |  | {iwm['up']} | {iwm['down']} | {iwm['flat']} |"
            )
            lines.append(
                f"| RANDOM4 | {version} | {_pct(rnd['compound_mean'])} | {_pct(rnd['compound_median'])} |  |  |  |"
            )
        lines.append("")
        lines += [
            "Per recipe, compound then ex-best, version (a) then (b). Order is the locked name order.",
            "",
            "| recipe | a compound | a ex-best | a win | b compound | b ex-best | b win |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
        by_name = {row["name"]: row for row in payload["windows"][window]["per_recipe"]}
        for recipe in payload["recipes"]:
            row = by_name[recipe["name"]]
            lines.append(
                "| `{name}` | {ac} | {ax} | {aw} | {bc} | {bx} | {bw} |".format(
                    name=recipe["name"],
                    ac=_pct(row["a"]["compound"]),
                    ax=_pct(row["a"]["ex_best"]),
                    aw=_pct(row["a"]["win_rate"]),
                    bc=_pct(row["b"]["compound"]),
                    bx=_pct(row["b"]["ex_best"]),
                    bw=_pct(row["b"]["win_rate"]),
                )
            )
        lines.append("")
    lines += [
        "IWM is one buy at the 2026-08-13 open, marked at each close, still held on 2026-09-25. The buy fee is in the path. There is no exit fee.",
        "RANDOM4 is 1000 draws, seed 20260813 plus the draw index, 4 names, hold 1, from that day's stock_book list. Version (a) draws after the 77 names are removed.",
        "A closed trade wins when its Futubull round trip is positive. Lots still open on 2026-09-25 are not in the win rate.",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    if _header_fingerprint() != prereg_fingerprint():
        raise SystemExit("prereg fingerprint mismatch")
    if REPORT.exists() and os.environ.get("FACTOR_MINE_AVG_RERUN") != "1":
        raise SystemExit("results already written")
    drop = load_drop()
    banned = set(drop["dropped"])
    recipes = universe()
    print("loading bars", flush=True)
    store = Bars(_load_frame())
    hidden = Hide(store, banned)
    present = sorted(name for name in banned if name in store.tapes)
    print(f"tickers {len(store.tapes)} dropped present {len(present)}", flush=True)
    proof = load_proof_rows()
    rows_b = {}
    rows_a = {}
    for session in SESSIONS:
        print(f"rows {session}", flush=True)
        rows_b[session] = _stock_rows(store, proof, session, set())
        rows_a[session] = _stock_rows(store, proof, session, banned)
        print(f"  b {len(rows_b[session])} a {len(rows_a[session])}", flush=True)
    fees = load_fees()
    books = []
    for recipe in recipes:
        ok = _search_ok(recipe, proof)
        print(f"walk {recipe['name']}", flush=True)
        books.append({
            "a": walk_recipe(recipe, list(SESSIONS), rows_a, hidden, ok, fees),
            "b": walk_recipe(recipe, list(SESSIONS), rows_b, store, ok, fees),
            "recipe": recipe,
        })
    print("iwm", flush=True)
    iwm = {"a": _iwm(hidden, fees), "b": _iwm(store, fees)}
    pools = {
        "a": {session: _pool(rows_a[session], hidden, session) for session in SESSIONS},
        "b": {session: _pool(rows_b[session], store, session) for session in SESSIONS},
    }
    print("random4 a", flush=True)
    random_a = _random4(hidden, fees, pools["a"])
    print("random4 b", flush=True)
    random_b = _random4(store, fees, pools["b"])
    names52 = set(drop["open_prev_close_52"])
    matched = set(drop["matched_splits"])
    windows = {}
    for key, window in WINDOWS:
        per = []
        stats = {"a": [], "b": []}
        for item in books:
            row = {"name": item["recipe"]["name"]}
            for version in ("a", "b"):
                got = _window_stats(item[version], fees, window, SESSIONS)
                stats[version].append(got)
                row[version] = {
                    "compound": got["compound"],
                    "ex_best": got["ex_best"],
                    "win_rate": got["win_rate"],
                }
            per.append(row)
        gap_b = _agg(stats["b"])
        gap_a = _agg(stats["a"])
        windows[key] = {
            "a": {
                "iwm": _window_stats(iwm["a"], fees, window, SESSIONS),
                "random4": _bench(random_a, fees, window),
                "recipes": gap_a,
            },
            "b": {
                "iwm": _window_stats(iwm["b"], fees, window, SESSIONS),
                "random4": _bench(random_b, fees, window),
                "recipes": gap_b,
            },
            "gap": {
                "compound_mean_b_minus_a": gap_b["compound_mean"] - gap_a["compound_mean"],
                "compound_median_b_minus_a": gap_b["compound_median"] - gap_a["compound_median"],
                "dropped_52": _share(stats["b"], names52),
                "dropped_77": _share(stats["b"], banned),
                "matched_splits": _share(stats["b"], matched),
            },
            "per_recipe": per,
        }
    payload = {
        "clean_commit": drop["source_commit"],
        "cleaned_parquet_sha256": drop["cleaned_parquet_sha256"],
        "fingerprint": prereg_fingerprint(),
        "n_dropped": len(banned),
        "n_dropped_in_pin": len(present),
        "n_recipes": len(recipes),
        "recipes": [
            {"hold": recipe["hold"], "name": recipe["name"], "rank": recipe["rank"], "top_n": recipe["top_n"]}
            for recipe in recipes
        ],
        "windows": windows,
    }
    RETURNS.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    SUMMARY.write_text(raw, encoding="utf-8")
    REPORT.write_text(_render(payload), encoding="utf-8")
    print(f"wrote {REPORT}", flush=True)


if __name__ == "__main__":
    main()
