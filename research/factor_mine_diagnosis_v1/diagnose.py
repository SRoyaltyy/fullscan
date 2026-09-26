"""Score factor_mine_diagnosis_v1. The preregistration is not edited.

No recipe and no leaf is selected for trading.
"""
from __future__ import annotations

import hashlib
import io
import json
import subprocess
import sys
from bisect import bisect_left
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.factor_mine_diagnosis_v1.engine import assert_engine_agreement, walk  # noqa: E402
from research.factor_mine_diagnosis_v1.protocol import (  # noqa: E402
    CLEAN_BLOB,
    CLEAN_SHA256,
    DROP_TOGGLES,
    FORWARD,
    MIN_TRADES,
    N_GRID,
    RANDOM_DRAWS,
    REPORT,
    RETURNS,
    SESSIONS,
    TUNE,
    YAHOO_PATH,
    YAHOO_SHA256,
    active_parts,
    build_orders,
    compound,
    day_counts,
    feature_bins,
    grow_tree,
    interaction_gain,
    leaf_matches,
    load_drop,
    load_inputs,
    load_recipes,
    mean,
    pair_drops,
    prior_feature_dates,
    top_leaves,
)
from src.paper_trade import load_fees  # noqa: E402


class Tape:
    def __init__(self, frame) -> None:
        self.rows: dict[str, dict] = {}
        for ticker, group in frame.groupby("ticker", sort=False):
            group = group.sort_values("date")
            self.rows[str(ticker)] = {
                "close": [None if value != value else float(value) for value in group["close"].tolist()],
                "date": group["date"].tolist(),
                "open": [None if value != value else float(value) for value in group["open"].tolist()],
                "volume": [None if value != value else float(value) for value in group["volume"].tolist()],
            }

    def _idx(self, ticker: str, session: str) -> int | None:
        tape = self.rows.get(ticker)
        if not tape:
            return None
        idx = bisect_left(tape["date"], session)
        if idx >= len(tape["date"]) or tape["date"][idx] != session:
            return None
        return idx

    def price(self, ticker: str, session: str, which: str) -> float | None:
        tape = self.rows.get(ticker)
        idx = self._idx(ticker, session)
        if tape is None or idx is None:
            return None
        value = tape[which][idx]
        if value is None or value <= 0:
            return None
        return float(value)

    def prior_volume(self, ticker: str, session: str) -> float | None:
        tape = self.rows.get(ticker)
        if not tape:
            return None
        idx = bisect_left(tape["date"], session)
        if idx <= 0:
            return None
        prior = tape["date"][idx - 1]
        prior_feature_dates([prior], session)
        value = tape["volume"][idx - 1]
        if value is None or value <= 0:
            return None
        return float(value)


def _frame(kind: str):
    import pandas as pd

    if kind == "clean":
        raw = subprocess.check_output(["git", "cat-file", "blob", CLEAN_BLOB], cwd=ROOT)
        if hashlib.sha256(raw).hexdigest() != CLEAN_SHA256:
            raise SystemExit("clean bar sha")
        frame = pd.read_parquet(io.BytesIO(raw))
    elif kind == "yahoo":
        path = ROOT / YAHOO_PATH
        if hashlib.sha256(path.read_bytes()).hexdigest() != YAHOO_SHA256:
            raise SystemExit("yahoo bar sha")
        frame = pd.read_parquet(path, columns=["date", "ticker", "open", "high", "low", "close", "volume"])
    else:
        raise SystemExit(kind)
    frame = frame.copy()
    frame["date"] = pd.to_datetime(frame["date"]).dt.strftime("%Y-%m-%d")
    frame["ticker"] = frame["ticker"].astype(str)
    frame = frame.loc[frame["date"] <= SESSIONS[-1]]
    return frame


def prepare(inputs: dict, tape: Tape) -> list[dict]:
    seen: dict[str, int] = {}
    days = []
    for session in SESSIONS:
        block = inputs["dates"][session]
        rows = []
        known = 0
        outcome = []
        for src in block["rows"]:
            ticker = src["ticker"]
            seen[ticker] = seen.get(ticker, 0) + 1
            if src.get("last_green"):
                known += 1
            opx = tape.price(ticker, session, "open")
            cpx = tape.price(ticker, session, "close")
            if opx and cpx:
                outcome.append(cpx / opx - 1.0)
            feat = dict(src)
            feat["prior_volume"] = tape.prior_volume(ticker, session)
            row = dict(src)
            row["days_on_list"] = seen[ticker]
            row["feat"] = feat
            row["open"] = opx
            row["prior_volume"] = feat["prior_volume"]
            rows.append(row)
        iwm_o = tape.price("IWM", session, "open")
        iwm_c = tape.price("IWM", session, "close")
        days.append({
            "breadth_known": (known / len(rows)) if rows else None,
            "breadth_outcome": mean(outcome),
            "iwm": None if not iwm_o or not iwm_c else iwm_c / iwm_o - 1.0,
            "rows": rows,
            "s": block.get("s"),
            "session": session,
        })
    return days


def full_config(recipe: dict) -> dict:
    return {
        "exit_when": recipe.get("exit_when") or {},
        "forbid": recipe.get("forbid") or {},
        "hold": int(recipe["hold"]),
        "name": recipe["name"],
        "rank": recipe.get("rank"),
        "rank_mode": "recipe",
        "require": recipe.get("require") or {},
        "s_boost": recipe.get("s_boost") or "none",
        "sell": recipe.get("sell") or "list",
        "side": recipe.get("side") or "long",
        "size": "leftover",
        "top_n": int(recipe["top_n"]),
        "universe": recipe.get("universe") or "union",
        "weather": True,
    }


def drop_config(recipe: dict, off: set[str], top_n: int | None = None) -> dict:
    rec = full_config(recipe)
    if top_n is not None:
        rec["top_n"] = int(top_n)
    if "must_have" in off:
        rec["require"] = {}
    if "must_not" in off:
        rec["forbid"] = {}
    if "sort" in off:
        rec["rank_mode"] = "random_sort"
    if "weather" in off:
        rec["weather"] = False
    if "hold_rule" in off:
        rec["sell"] = "time"
        rec["s_boost"] = "none"
        rec["exit_when"] = {}
    return rec


def subset_config(recipe: dict, on: set[str]) -> dict:
    rec = full_config(recipe)
    rec["universe"] = recipe["universe"] if "list_source" in on else "union"
    rec["require"] = recipe["require"] if "must_have" in on else {}
    rec["forbid"] = recipe["forbid"] if "must_not" in on else {}
    if "sort" in on:
        rec["rank"] = recipe.get("rank")
        rec["rank_mode"] = "recipe"
    else:
        rec["rank"] = None
        rec["rank_mode"] = "list"
    if "hold_rule" in on:
        rec["hold"] = int(recipe["hold"])
        rec["sell"] = recipe.get("sell") or "list"
        rec["s_boost"] = recipe.get("s_boost") or "none"
        rec["exit_when"] = recipe.get("exit_when") or {}
    else:
        rec["hold"] = 1
        rec["sell"] = "time"
        rec["s_boost"] = "none"
        rec["exit_when"] = {}
    rec["weather"] = "weather" in on
    return rec


def random_config(recipe: dict, draw: int) -> dict:
    rec = full_config(recipe)
    rec["universe"] = "union"
    rec["require"] = {}
    rec["forbid"] = {}
    rec["rank"] = None
    rec["rank_mode"] = "sample"
    rec["draw"] = draw
    rec["top_n"] = 4
    rec["name"] = "RANDOM4"
    return rec


def _key(config: dict) -> str:
    return json.dumps(config, sort_keys=True, default=str)


def window_stats(book: dict, window: tuple[str, ...], dropped: set[str]) -> dict:
    by = {day["session"]: day for day in book["daily"]}
    rets = [float(by[session]["ret"]) for session in window]
    rets_15 = [float(by[session]["ret_15"]) for session in window]
    up, down, flat = day_counts(rets)
    check = set(window)
    closed = [trade for trade in book["closed"] if trade["exit"] in check]
    wins = [trade for trade in closed if trade["win"]]
    totals: dict[str, float] = {}
    for session in window:
        for ticker, value in book["pnl_by_day"].get(session, {}).items():
            totals[ticker] = totals.get(ticker, 0.0) + float(value)
    net = sum(totals.values())
    drop_dollars = sum(value for ticker, value in totals.items() if ticker in dropped)
    yaas = float(totals.get("YAAS") or 0.0)
    splits = sum(float(totals.get(name) or 0.0) for name in ("ALP", "NFE", "TNMG", "WCT"))
    bands = {"lt3": 0.0, "m3_10": 0.0, "ge10": 0.0, "missing": 0.0}
    gross = 0.0
    for trade in closed:
        px = trade["entry_px"]
        band = "missing" if px is None else ("lt3" if px < 3 else "m3_10" if px < 10 else "ge10")
        bands[band] += float(trade["pnl"])
        if trade["pnl"] > 0:
            gross += float(trade["pnl"])
    ranked = sorted(totals, key=lambda ticker: (-totals[ticker], book["first"].get(ticker, "9999-99-99"), ticker))
    top = []
    for ticker in ranked[:5]:
        win_gross = sum(float(trade["pnl"]) for trade in closed if trade["ticker"] == ticker and trade["pnl"] > 0)
        top.append({
            "gross_share": (win_gross / gross) if gross else None,
            "net": totals[ticker],
            "net_share": (totals[ticker] / net) if net else None,
            "ticker": ticker,
        })
    return {
        "bands": bands,
        "band_net": {key: (value / net) if net else None for key, value in bands.items()},
        "best": [ranked[:k] for k in (1, 3, 5)],
        "closed": len(closed),
        "compound": compound(rets),
        "compound_15": compound(rets_15),
        "down": down,
        "drop_dollars": drop_dollars,
        "drop_share": (drop_dollars / net) if net else None,
        "ex": {str(k): _ex(book, list(SESSIONS), window, ranked[:k]) for k in (1, 3, 5)},
        "first_dollars": sum(float(by[session]["first_dollars"]) for session in window),
        "flat": flat,
        "gap": sum(float(by[session]["gap"]) for session in window),
        "later_dollars": sum(float(by[session]["later_dollars"]) for session in window),
        "net": net,
        "realized": sum(float(trade["pnl"]) for trade in closed),
        "session_dollars": sum(float(by[session]["session_dollars"]) for session in window),
        "splits_dollars": splits,
        "too_few": len(closed) < MIN_TRADES,
        "top": top,
        "up": up,
        "win_rate": (len(wins) / len(closed)) if closed else None,
        "yaas_dollars": yaas,
    }


def _ex(book: dict, sessions: list[str], window: tuple[str, ...], names: list[str]) -> float | None:
    if not window or not names:
        return None
    by = {day["session"]: float(day["ret"]) for day in book["daily"]}
    equity = 10000.0
    out = []
    check = set(window)
    remove = set(names)
    for session in sessions:
        start = equity
        end = start * (1.0 + by[session])
        if session in check and start:
            removed = sum(float(book["pnl_by_day"].get(session, {}).get(ticker) or 0.0) for ticker in remove)
            out.append((end - removed) / start - 1.0)
        equity = end
    return compound(out) if out else None


def _round(value, digits: int = 4):
    if value is None:
        return None
    return round(float(value), digits)


def _pct(value) -> str:
    if value is None:
        return "—"
    return f"{100.0 * float(value):.2f}%"


def _num(value) -> str:
    if value is None:
        return "—"
    return f"{float(value):.2f}"


def _flag(row: dict) -> str:
    return "yes" if row.get("too_few") else ""


def run_cached(days, fees, price, cache: dict, config: dict, *, keep: bool) -> dict:
    key = _key(config)
    hit = cache.get(key)
    if hit is None:
        book = walk(days, config, fees, price)
        for trade in book["closed"]:
            trade["recipe"] = config["name"]
            trade["hold"] = int(config["hold"])
        hit = book
        cache[key] = book if keep else {"daily": book["daily"], "closed": book["closed"], "first": book["first"], "pnl_by_day": book["pnl_by_day"], "open": book["open"]}
    return cache[key]


def _trade_rows(trades: list[dict]) -> list[dict]:
    rows = []
    for trade in trades:
        bins = feature_bins(
            trade["row"], entry_px=trade["entry_px"],
            days_on_list=int(trade["days_on_list"]), score=trade.get("s"),
        )
        rows.append({
            "bins": bins, "entry": trade["entry"], "hold": trade.get("hold"),
            "recipe": trade.get("recipe"), "ret": float(trade["ret"]), "win": bool(trade["win"]),
        })
    return rows


def _leaf_random(leaf_rows: list[dict], random_daily: dict[str, dict[str, float]]) -> dict:
    names = {row.get("recipe") for row in leaf_rows}
    entries = sorted({row["entry"] for row in leaf_rows})
    if len(names) == 1 and next(iter(names)) in random_daily:
        key = next(iter(names))
    else:
        key = "hold1"
    series = random_daily.get(key) or {}
    vals = [series[day] for day in entries if day in series]
    return {"key": key, "mean": mean(vals), "n_days": len(vals)}


def describe_combos(trades: list[dict], forward_trades: list[dict], random_daily: dict) -> dict:
    tune_rows = _trade_rows(trades)
    forward_rows = _trade_rows(forward_trades)
    out = {"forward_tree": {}, "joint": {}, "tune_tree": {}}
    for depth in (2, 3):
        leaves = grow_tree(tune_rows, depth)
        forward_hits = []
        for leaf in top_leaves(leaves):
            matched = [row for row in forward_rows if leaf_matches(row["bins"], leaf["conds"])]
            forward_hits.append({
                "conds": leaf["conds"],
                "forward_mean": mean([row["ret"] for row in matched]),
                "forward_n": len(matched),
                "forward_too_few": len(matched) < MIN_TRADES,
                "forward_win": (sum(1 for row in matched if row["win"]) / len(matched)) if matched else None,
                "parts": _parts(tune_rows, leaf["conds"]),
                "random": _leaf_random(
                    [row for row in tune_rows if leaf_matches(row["bins"], leaf["conds"])],
                    random_daily,
                ),
                "tune_mean": leaf["mean_ret"],
                "tune_n": leaf["n"],
                "tune_win": leaf["win_rate"],
            })
        out["tune_tree"][str(depth)] = {
            "flagged": sum(1 for leaf in leaves if leaf["flag_lt_30"]),
            "leaves": len(leaves),
            "top": forward_hits,
        }
        grown = grow_tree(forward_rows, depth)
        out["forward_tree"][str(depth)] = {
            "flagged": sum(1 for leaf in grown if leaf["flag_lt_30"]),
            "leaves": len(grown),
            "top": [
                {
                    "conds": leaf["conds"],
                    "mean": leaf["mean_ret"],
                    "n": leaf["n"],
                    "win_rate": leaf["win_rate"],
                }
                for leaf in top_leaves(grown)
            ],
        }
    out["joint"] = {
        "tune": _joints(tune_rows),
        "forward": _joints(forward_rows),
    }
    return out


def _parts(rows: list[dict], conds: list[dict]) -> list[dict]:
    found = []
    for cond in conds:
        matched = [row for row in rows if leaf_matches(row["bins"], [cond])]
        wins = sum(1 for row in matched if row["win"])
        found.append({
            "cond": cond,
            "mean": mean([row["ret"] for row in matched]),
            "n": len(matched),
            "too_few": len(matched) < MIN_TRADES,
            "win_rate": (wins / len(matched)) if matched else None,
        })
    return found


def _joints(rows: list[dict]) -> dict:
    specs = {
        "price_weather_news": ("price_band", "weather", "news"),
        "price_cond_ret5": ("price_band", "cond_band", "ret5_band"),
    }
    out = {}
    for name, keys in specs.items():
        buckets: dict[tuple, list] = {}
        for row in rows:
            key = tuple(row["bins"][feature] for feature in keys)
            buckets.setdefault(key, []).append(row)
        cells = []
        flagged = 0
        for key, group in sorted(buckets.items()):
            if len(group) < MIN_TRADES:
                flagged += 1
                continue
            wins = sum(1 for row in group if row["win"])
            cells.append({
                "key": list(key),
                "mean": mean([row["ret"] for row in group]),
                "n": len(group),
                "win_rate": wins / len(group),
            })
        cells.sort(key=lambda cell: (-abs(cell["win_rate"] - 0.5), -cell["n"]))
        out[name] = {"cells": cells[:20], "flagged_lt_30": flagged, "shown": len(cells[:20])}
    return out


def winner_table(trades: list[dict]) -> dict:
    winners = [trade for trade in trades if trade["win"]]
    losers = [trade for trade in trades if trade["pnl"] < 0]
    return {
        "losers": _profile(losers),
        "n_losers": len(losers),
        "n_winners": len(winners),
        "winners": _profile(winners),
    }


def _profile(trades: list[dict]) -> dict:
    if not trades:
        return {}
    def avg(fn):
        vals = [fn(trade) for trade in trades]
        vals = [value for value in vals if value is not None]
        return mean(vals)
    tones = Counter()
    for trade in trades:
        boxes = (trade["row"].get("boxes") or {})
        for key, value in boxes.items():
            tones[f"{key}:{value}"] += 1
    common = tones.most_common(8)
    return {
        "cameras": [{"tone": key, "share": count / len(trades)} for key, count in common],
        "candle_capture": sum(1 for trade in trades if trade["row"].get("candle_capture")) / len(trades),
        "candle_score": avg(lambda trade: trade["row"].get("candle_score")),
        "cond_net": avg(lambda trade: (trade["row"].get("cond_good") or 0) - (trade["row"].get("cond_bad") or 0)),
        "days_on_list": avg(lambda trade: trade["days_on_list"]),
        "earn_react": sum(1 for trade in trades if trade["row"].get("erd_earn_react")) / len(trades),
        "entry_px": avg(lambda trade: trade["entry_px"]),
        "hot": avg(lambda trade: trade["row"].get("ohlc_hot_score")),
        "news_good": sum(1 for trade in trades if str(trade["row"].get("news_box") or "") == "good") / len(trades),
        "prior_volume": avg(lambda trade: trade["row"].get("prior_volume") if "prior_volume" in trade["row"] else None),
        "ret_5": avg(lambda trade: trade["row"].get("ohlc_ret_5")),
        "weather_s": avg(lambda trade: trade.get("s")),
    }


def day_conditions(book: dict, days: list[dict], window: tuple[str, ...]) -> dict:
    by = {day["session"]: day for day in book["daily"]}
    meta = {day["session"]: day for day in days}
    groups = {"up": [], "down": [], "flat": []}
    for session in window:
        ret = float(by[session]["ret"])
        label = "up" if ret > 0 else "down" if ret < 0 else "flat"
        groups[label].append(meta[session])
    out = {}
    for label, rows in groups.items():
        out[label] = {
            "breadth_known": mean([row["breadth_known"] for row in rows if row["breadth_known"] is not None]),
            "breadth_outcome": mean([row["breadth_outcome"] for row in rows if row["breadth_outcome"] is not None]),
            "iwm": mean([row["iwm"] for row in rows if row["iwm"] is not None]),
            "iwm_missing": sum(1 for row in rows if row["iwm"] is None),
            "n": len(rows),
            "s": mean([row["s"] for row in rows if row["s"] is not None]),
        }
    return out


def render(payload: dict) -> str:
    lines = [
        "# factor_mine_diagnosis_v1",
        "",
        "Diagnosis only. No recipe and no combination is selected for trading.",
        "",
        "The cash book runs from 2026-08-13 through 2026-09-25 and does not reset on 2026-09-14. Futubull fees are the main figure. Flat 15bp uses the same share counts.",
        "",
        "A row with fewer than 30 closed trades is flagged and kept.",
        "",
    ]
    lines.append("## Baseline")
    lines.append("")
    lines.append("| recipe | tape | window | compound | flat 15bp | win rate | closed | up | down | flat | <30 | ex-best |")
    lines.append("| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: |")
    for row in payload["baseline"]:
        lines.append(
            f"| `{row['name']}` | {row['tape']} | {row['window']} | {_pct(row['compound'])} | {_pct(row['compound_15'])} | {_pct(row['win_rate'])} | {row['closed']} | {row['up']} | {row['down']} | {row['flat']} | {_flag(row)} | {_pct(row['ex'].get('1'))} |"
        )
    lines.append("")
    lines.append("## Profit source")
    lines.append("")
    lines.append("| recipe | tape | window | top name | top share of net | under $3 | $3–$10 | $10+ | without best 1 | best 3 | best 5 | 76 share | <30 |")
    lines.append("| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |")
    for row in payload["baseline"]:
        top = row["top"][0]["ticker"] if row["top"] else "—"
        share = row["top"][0]["net_share"] if row["top"] else None
        lines.append(
            f"| `{row['name']}` | {row['tape']} | {row['window']} | {top} | {_pct(share)} | {_pct(row['band_net'].get('lt3'))} | {_pct(row['band_net'].get('m3_10'))} | {_pct(row['band_net'].get('ge10'))} | {_pct(row['ex'].get('1'))} | {_pct(row['ex'].get('3'))} | {_pct(row['ex'].get('5'))} | {_pct(row['drop_share'])} | {_flag(row)} |"
        )
    lines.append("")
    lines.append("YAAS is not in the 76. Its dollars are in the machine-readable results, apart from that share. ALP, NFE, TNMG, and WCT are the matched splits inside the 76.")
    lines.append("")
    lines.append("## Timing")
    lines.append("")
    lines.append("| recipe | tape | window | overnight $ | 09:30-to-close $ | first-day $ | later-day $ |")
    lines.append("| --- | --- | --- | ---: | ---: | ---: | ---: |")
    for row in payload["baseline"]:
        lines.append(
            f"| `{row['name']}` | {row['tape']} | {row['window']} | {_num(row['gap'])} | {_num(row['session_dollars'])} | {_num(row['first_dollars'])} | {_num(row['later_dollars'])} |"
        )
    lines.append("")
    lines.append("Up, down, and flat days, and the market around them, are in the results file under `conditions`. Morning S and last-green breadth are knowable at 09:30. IWM open-to-close and list open-to-close are outcomes. Yahoo IWM is missing after the pin ends.")
    lines.append("")
    lines.append("## Drop-one contribution")
    lines.append("")
    lines.append("Contribution is the full book minus the book with that part removed. A negative contribution means the book did better without the part.")
    lines.append("")
    lines.append("| recipe | tape | window | part | compound contribution | win-rate contribution | dropped closed | <30 |")
    lines.append("| --- | --- | --- | --- | ---: | ---: | ---: | --- |")
    for row in payload["drop_one"]:
        lines.append(
            f"| `{row['name']}` | {row['tape']} | {row['window']} | {row['part']} | {_pct(row['compound_delta'])} | {_pct(row['win_delta'])} | {row['closed']} | {_flag(row)} |"
        )
    lines.append("")
    lines.append("## Pair interactions")
    lines.append("")
    lines.append("Gain = combined effect minus the two single effects. Zero means the two removals added. This is not a ranking.")
    lines.append("")
    lines.append("| recipe | tape | window | pair | compound gain | win-rate gain |")
    lines.append("| --- | --- | --- | --- | ---: | ---: |")
    for row in payload["pairs"]:
        lines.append(
            f"| `{row['name']}` | {row['tape']} | {row['window']} | {row['pair']} | {_pct(row['compound_gain'])} | {_pct(row['win_gain'])} |"
        )
    lines.append("")
    lines.append("## Build-up")
    lines.append("")
    lines.append("Each subset of a recipe's own parts is walked once from the bare mixed list. Permutations reuse those walks. No order is chosen. The full grid is in `RESULTS.json` under `build`.")
    lines.append("")
    lines.append("| recipe | tape | window | parts on | compound | win rate | closed | <30 |")
    lines.append("| --- | --- | --- | --- | ---: | ---: | ---: | --- |")
    for row in payload["build_ends"]:
        lines.append(
            f"| `{row['name']}` | {row['tape']} | {row['window']} | {row['parts']} | {_pct(row['compound'])} | {_pct(row['win_rate'])} | {row['closed']} | {_flag(row)} |"
        )
    lines.append("")
    lines.append("## N")
    lines.append("")
    lines.append("N is 4, 8, or 12, with every other part left on.")
    lines.append("")
    lines.append("| recipe | tape | window | N | compound | win rate | closed | <30 |")
    lines.append("| --- | --- | --- | ---: | ---: | ---: | ---: | --- |")
    for row in payload["n_grid"]:
        lines.append(
            f"| `{row['name']}` | {row['tape']} | {row['window']} | {row['n']} | {_pct(row['compound'])} | {_pct(row['win_rate'])} | {row['closed']} | {_flag(row)} |"
        )
    lines.append("")
    lines.append("## RANDOM4 from the same morning list")
    lines.append("")
    lines.append("| recipe | tape | window | recipe compound | random mean | random median | recipe win | <30 |")
    lines.append("| --- | --- | --- | ---: | ---: | ---: | ---: | --- |")
    for row in payload["random"]:
        lines.append(
            f"| `{row['name']}` | {row['tape']} | {row['window']} | {_pct(row['recipe'])} | {_pct(row['mean'])} | {_pct(row['median'])} | {_pct(row['win_rate'])} | {_flag(row)} |"
        )
    lines.append("")
    lines.append("## Winners versus losers at 09:30")
    lines.append("")
    lines.append("Means are entry-morning fields on closed trades. Float is not on the earliest board, so it is omitted. Prior volume is the last bar before the session.")
    lines.append("")
    lines.append("| recipe pool | tape | window | side | n | entry px | ret 5 | cond | hot | days on list | news good | earn | S |")
    lines.append("| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for row in payload["profiles"]:
        for side in ("winners", "losers"):
            prof = row[side]
            lines.append(
                f"| all subjects | {row['tape']} | {row['window']} | {side} | {row['n_'+side]} | {_num(prof.get('entry_px'))} | {_num(prof.get('ret_5'))} | {_num(prof.get('cond_net'))} | {_num(prof.get('hot'))} | {_num(prof.get('days_on_list'))} | {_pct(prof.get('news_good'))} | {_pct(prof.get('earn_react'))} | {_num(prof.get('weather_s'))} |"
            )
    lines.append("")
    lines.append("## Combo descriptions")
    lines.append("")
    lines.append("Trees and joint buckets describe trades the books already took. A leaf under 30 trades is flagged and is not listed as a combo. The table order is distance from a 50% win rate. That order is for reading. It selects nothing. Tune leaves are scored again on forward trades. A separate forward tree is a description of that window only.")
    lines.append("")
    for tape, block in payload["combos"].items():
        for depth, tree in block["tune_tree"].items():
            lines.append(f"### {tape} tune tree depth {depth}")
            lines.append("")
            lines.append(f"Leaves {tree['leaves']}, flagged under 30: {tree['flagged']}.")
            lines.append("")
            lines.append("| conditions | tune n | tune win | tune mean | forward n | forward win | forward mean | forward <30 |")
            lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |")
            for leaf in tree["top"]:
                text = "; ".join(f"{c['feature']} {c['op']} {c['value']}" for c in leaf["conds"])
                lines.append(
                    f"| {text} | {leaf['tune_n']} | {_pct(leaf['tune_win'])} | {_pct(leaf['tune_mean'])} | {leaf['forward_n']} | {_pct(leaf['forward_win'])} | {_pct(leaf['forward_mean'])} | {'yes' if leaf['forward_too_few'] else ''} |"
                )
            lines.append("")
            if tree["top"]:
                lines.append("Each condition alone, on the tune trade pool, and the random-book mean on the leaf's entry dates:")
                lines.append("")
                lines.append("| leaf | part | n | win | mean | <30 | random mean |")
                lines.append("| --- | --- | ---: | ---: | ---: | --- | ---: |")
                for leaf in tree["top"]:
                    label = "; ".join(f"{c['feature']}{c['op']}{c['value']}" for c in leaf["conds"])
                    for part in leaf["parts"]:
                        cond = part["cond"]
                        lines.append(
                            f"| {label} | {cond['feature']} {cond['op']} {cond['value']} | {part['n']} | {_pct(part['win_rate'])} | {_pct(part['mean'])} | {'yes' if part['too_few'] else ''} | {_pct(leaf['random']['mean'])} |"
                        )
                lines.append("")
    lines.append("Joint buckets with at least 30 trades are in `RESULTS.json` under `combos`. Cells under 30 are counted and not listed as combos.")
    lines.append("")
    return "\n".join(lines) + "\n"


def main() -> None:
    assert_engine_agreement()
    inputs = load_inputs()
    recipes = load_recipes()
    drop = set(load_drop()["dropped"])
    fees = load_fees()
    payload = {
        "baseline": [],
        "build": [],
        "build_ends": [],
        "combos": {},
        "conditions": [],
        "drop_one": [],
        "n_grid": [],
        "note": "Diagnosis only. No recipe and no combination is selected for trading.",
        "pairs": [],
        "profiles": [],
        "random": [],
    }
    for kind in ("clean", "yahoo"):
        print(f"tape {kind}", flush=True)
        tape = Tape(_frame(kind))
        days = prepare(inputs, tape)

        def price(ticker: str, session: str, which: str, _tape=tape):
            return _tape.price(ticker, session, which)

        cache: dict = {}
        books = {}
        for recipe in recipes:
            print(f"  {recipe['name']}", flush=True)
            books[recipe["name"]] = run_cached(days, fees, price, cache, full_config(recipe), keep=True)
        for window_name, window in (("tune", TUNE), ("forward", FORWARD)):
            for recipe in recipes:
                stats = window_stats(books[recipe["name"]], window, drop)
                stats.update({"name": recipe["name"], "tape": kind, "window": window_name})
                payload["baseline"].append(stats)
                payload["conditions"].append({
                    "groups": day_conditions(books[recipe["name"]], days, window),
                    "name": recipe["name"],
                    "tape": kind,
                    "window": window_name,
                })
        # ablations
        single = {}
        for recipe in recipes:
            single[recipe["name"]] = {}
            for part in DROP_TOGGLES:
                book = run_cached(days, fees, price, cache, drop_config(recipe, {part}), keep=False)
                single[recipe["name"]][part] = book
                for window_name, window in (("tune", TUNE), ("forward", FORWARD)):
                    base = next(row for row in payload["baseline"] if row["name"] == recipe["name"] and row["tape"] == kind and row["window"] == window_name)
                    stats = window_stats(book, window, drop)
                    win_delta = None if base["win_rate"] is None or stats["win_rate"] is None else base["win_rate"] - stats["win_rate"]
                    payload["drop_one"].append({
                        "closed": stats["closed"],
                        "compound_delta": base["compound"] - stats["compound"],
                        "name": recipe["name"],
                        "part": part,
                        "tape": kind,
                        "too_few": stats["too_few"],
                        "win_delta": win_delta,
                        "window": window_name,
                    })
            for left, right in pair_drops():
                both = run_cached(days, fees, price, cache, drop_config(recipe, {left, right}), keep=False)
                for window_name, window in (("tune", TUNE), ("forward", FORWARD)):
                    base = next(row for row in payload["baseline"] if row["name"] == recipe["name"] and row["tape"] == kind and row["window"] == window_name)
                    only_i = window_stats(single[recipe["name"]][left], window, drop)
                    only_j = window_stats(single[recipe["name"]][right], window, drop)
                    pair = window_stats(both, window, drop)
                    win_gain = None
                    if None not in (base["win_rate"], only_i["win_rate"], only_j["win_rate"], pair["win_rate"]):
                        win_gain = interaction_gain(base["win_rate"], only_i["win_rate"], only_j["win_rate"], pair["win_rate"])
                    payload["pairs"].append({
                        "compound_gain": interaction_gain(base["compound"], only_i["compound"], only_j["compound"], pair["compound"]),
                        "name": recipe["name"],
                        "pair": f"{left}+{right}",
                        "tape": kind,
                        "win_gain": win_gain,
                        "window": window_name,
                    })
            for number in N_GRID:
                book = run_cached(days, fees, price, cache, drop_config(recipe, set(), top_n=number), keep=False)
                for window_name, window in (("tune", TUNE), ("forward", FORWARD)):
                    stats = window_stats(book, window, drop)
                    payload["n_grid"].append({
                        "closed": stats["closed"],
                        "compound": stats["compound"],
                        "n": number,
                        "name": recipe["name"],
                        "tape": kind,
                        "too_few": stats["too_few"],
                        "win_rate": stats["win_rate"],
                        "window": window_name,
                    })
            parts = active_parts(recipe)
            subset_stats = {}
            for mask in range(1 << len(parts)):
                on = {parts[index] for index in range(len(parts)) if mask & (1 << index)}
                book = run_cached(days, fees, price, cache, subset_config(recipe, on), keep=False)
                label = ",".join(part for part in parts if part in on) or "bare"
                for window_name, window in (("tune", TUNE), ("forward", FORWARD)):
                    stats = window_stats(book, window, drop)
                    subset_stats[(label, window_name)] = stats
                    if label in {"bare", ",".join(parts)}:
                        payload["build_ends"].append({
                            "closed": stats["closed"],
                            "compound": stats["compound"],
                            "name": recipe["name"],
                            "parts": label,
                            "tape": kind,
                            "too_few": stats["too_few"],
                            "win_rate": stats["win_rate"],
                            "window": window_name,
                        })
            for order in build_orders(parts):
                chain = []
                chosen: list[str] = []
                for part in order:
                    chosen.append(part)
                    label = ",".join(item for item in parts if item in set(chosen))
                    chain.append({
                        "forward": subset_stats[(label, "forward")]["compound"],
                        "parts": label,
                        "tune": subset_stats[(label, "tune")]["compound"],
                    })
                payload["build"].append({
                    "chain": chain,
                    "name": recipe["name"],
                    "order": list(order),
                    "tape": kind,
                })
        # random, grouped by exit config
        groups: dict[str, dict] = {}
        for recipe in recipes:
            cfg = random_config(recipe, 0)
            cfg.pop("draw")
            groups.setdefault(_key(cfg), full_config(recipe))
        random_books: dict[str, list] = {key: [] for key in groups}
        hold1 = full_config(recipes[0])
        hold1["hold"] = 1
        hold1["s_boost"] = "none"
        hold1["side"] = "long"
        hold1["sell"] = "list"
        print(f"  random groups {len(groups)}", flush=True)
        for key, recipe in groups.items():
            for draw in range(RANDOM_DRAWS):
                book = walk(days, random_config(recipe, draw), fees, price)
                random_books[key].append([float(day["ret"]) for day in book["daily"]])
        # hold-1 long reference if not already in the groups
        hold1_paths = None
        for key, recipe in groups.items():
            probe = random_config(recipe, 0)
            if probe["hold"] == 1 and probe["side"] == "long" and probe["s_boost"] == "none":
                hold1_paths = random_books[key]
                break
        if hold1_paths is None:
            hold1_paths = []
            for draw in range(RANDOM_DRAWS):
                book = walk(days, random_config(hold1, draw), fees, price)
                hold1_paths.append([float(day["ret"]) for day in book["daily"]])
        by_session = {session: index for index, session in enumerate(SESSIONS)}
        for recipe in recipes:
            cfg = random_config(recipe, 0)
            cfg.pop("draw")
            paths = random_books[_key(cfg)]
            for window_name, window in (("tune", TUNE), ("forward", FORWARD)):
                compounds = []
                for path in paths:
                    compounds.append(compound([path[by_session[session]] for session in window]))
                compounds.sort()
                base = next(row for row in payload["baseline"] if row["name"] == recipe["name"] and row["tape"] == kind and row["window"] == window_name)
                mid = len(compounds) // 2
                median = compounds[mid] if len(compounds) % 2 else (compounds[mid - 1] + compounds[mid]) / 2
                payload["random"].append({
                    "mean": mean(compounds),
                    "median": median,
                    "name": recipe["name"],
                    "recipe": base["compound"],
                    "tape": kind,
                    "too_few": base["too_few"],
                    "win_rate": base["win_rate"],
                    "window": window_name,
                })
        # combos from pooled baseline trades
        tune_trades = []
        forward_trades = []
        for recipe in recipes:
            for trade in books[recipe["name"]]["closed"]:
                if trade["exit"] in set(TUNE):
                    tune_trades.append(trade)
                elif trade["exit"] in set(FORWARD):
                    forward_trades.append(trade)
        # random daily mean for leaf comparison: matched is not one series; use hold-1 and also each recipe later if needed
        random_daily = {"hold1": {}}
        for index, session in enumerate(SESSIONS):
            random_daily["hold1"][session] = mean([path[index] for path in hold1_paths])
        for recipe in recipes:
            cfg = random_config(recipe, 0)
            cfg.pop("draw")
            paths = random_books[_key(cfg)]
            random_daily[recipe["name"]] = {
                session: mean([path[index] for path in paths])
                for index, session in enumerate(SESSIONS)
            }
        payload["combos"][kind] = describe_combos(tune_trades, forward_trades, random_daily)
        for window_name, pool in (("tune", tune_trades), ("forward", forward_trades)):
            prof = winner_table(pool)
            # prior volume lives on the day row, copied onto feat only if we stored it.
            payload["profiles"].append({
                "losers": prof["losers"],
                "n_losers": prof["n_losers"],
                "n_winners": prof["n_winners"],
                "tape": kind,
                "window": window_name,
                "winners": prof["winners"],
            })
        del cache
        del books
    RETURNS.mkdir(parents=True, exist_ok=True)
    # Slim the baseline rows for JSON: drop nothing essential, but top/ex stay.
    (RETURNS / "RESULTS.json").write_text(json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8")
    REPORT.write_text(render(payload), encoding="utf-8")
    print(f"wrote {REPORT}", flush=True)


if __name__ == "__main__":
    main()
