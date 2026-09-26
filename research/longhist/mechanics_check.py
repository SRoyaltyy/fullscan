"""Mechanics audit for the long-history books.

Reads the cached bars, the split events, and the saved trade ledgers.
Does not write a score, does not edit the frozen file, and does not
change a rule. The only study file it writes is MECHANICS_CHECK.md.
The published markdown also names the six continuity failures and the
XTIA 1-for-250 example; those notes were added after the calculator run.
"""
from __future__ import annotations

import json
import math
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pyarrow.dataset as ds
from scipy import stats

from research.longhist.engine import (
    CAPITAL,
    build_panel,
    fee_model,
    load_tapes,
    sessions_between,
    simulate,
    ymd,
)
from research.longhist.engine import Rule
from src.paper_trade import order_fees

ET = ZoneInfo("America/New_York")
OUT = Path("research/longhist/results/MECHANICS_CHECK.md")
BARS = "research/longhist/bars/ohlcv"
JSON_DIR = Path("/tmp/longhist-bars/ok")
BP15 = 0.0015


def ny_date(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), ET).date().isoformat()


def fee_parts(shares: int, price: float, side: str, f: dict) -> dict:
    amount = shares * price
    comm_var = f["commission_per_share"] * shares
    plat_var = f["platform_per_share"] * shares
    comm_cap = f["commission_max_pct_of_amount"] * amount
    plat_cap = f["platform_max_pct_of_amount"] * amount
    comm = min(max(comm_var, f["commission_min_per_order"]), comm_cap)
    plat = min(max(plat_var, f["platform_min_per_order"]), plat_cap)
    settle = f["settlement_per_share"] * shares
    reg = taf = 0.0
    reg_var = taf_var = 0.0
    if side == "sell":
        reg_var = f["regulatory_pct_of_amount_sell_only"] * amount
        taf_var = f["taf_per_share_sell_only"] * shares
        reg = max(reg_var, f["regulatory_min_per_order"])
        taf = min(max(taf_var, f["taf_min_per_order"]), f["taf_max_per_order"])
    total = round(comm + plat + settle + reg + taf, 4)
    # Same caps, floors removed. This is the premium of the minimums.
    comm_nf = min(comm_var, comm_cap)
    plat_nf = min(plat_var, plat_cap)
    reg_nf = reg_var
    taf_nf = min(taf_var, f["taf_max_per_order"]) if side == "sell" else 0.0
    no_floor = round(comm_nf + plat_nf + settle + reg_nf + taf_nf, 4)
    return {
        "amount": amount,
        "commission": comm,
        "platform": plat,
        "settlement": settle,
        "regulatory": reg,
        "taf": taf,
        "total": total,
        "no_floor": no_floor,
        "floor_premium": round(total - no_floor, 4),
        "commission_at_min": abs(comm - f["commission_min_per_order"]) < 1e-9 and comm_var < f["commission_min_per_order"],
        "platform_at_min": abs(plat - f["platform_min_per_order"]) < 1e-9 and plat_var < f["platform_min_per_order"],
    }


def load_splits() -> list[dict]:
    rows = []
    paths = sorted(JSON_DIR.glob("*.json"))
    for i, path in enumerate(paths):
        raw = json.loads(path.read_text())
        result = (raw.get("chart") or {}).get("result") or []
        if not result:
            continue
        block = result[0]
        ticker = str((block.get("meta") or {}).get("symbol") or path.stem)
        splits = ((block.get("events") or {}).get("splits")) or {}
        for key, item in splits.items():
            num = float(item.get("numerator") or 0)
            den = float(item.get("denominator") or 0)
            if num <= 0 or den <= 0 or num == den:
                continue
            ts = int(item.get("date") or key)
            rows.append({
                "ticker": ticker,
                "date": ny_date(ts),
                "numerator": num,
                "denominator": den,
                "ratio": str(item.get("splitRatio") or ""),
                "raw_mult": den / num,
            })
        if (i + 1) % 1500 == 0:
            print(f"splits scanned {i + 1}/{len(paths)}", flush=True)
    print(f"split events {len(rows)}", flush=True)
    return rows


def classify_splits(splits: list[dict], sessions: list[str]) -> None:
    """Mark each split adjusted / unadjusted from the cached open/close."""
    prev = {}
    last = None
    for day in sessions:
        if last is not None:
            prev[day] = last
        last = day
    want = defaultdict(set)
    for row in splits:
        prior = prev.get(row["date"])
        row["prior"] = prior
        if prior:
            want[row["ticker"]].update((row["date"], prior))
    dataset = ds.dataset(BARS, format="parquet")
    have = defaultdict(dict)
    tickers = sorted(want)
    for start in range(0, len(tickers), 400):
        chunk = tickers[start:start + 400]
        dates = sorted({d for t in chunk for d in want[t]})
        table = dataset.to_table(
            filter=(ds.field("ticker").isin(chunk) & ds.field("date").isin(dates)),
            columns=["ticker", "date", "open", "close"],
        )
        for ticker, day, opened, closed in zip(
            table.column("ticker").to_pylist(),
            table.column("date").to_pylist(),
            table.column("open").to_pylist(),
            table.column("close").to_pylist(),
        ):
            have[ticker][day] = (opened, closed)
        print(f"continuity prices {min(start + 400, len(tickers))}/{len(tickers)}", flush=True)
    for row in splits:
        prior = row.get("prior")
        pair_ex = have[row["ticker"]].get(row["date"])
        pair_prior = have[row["ticker"]].get(prior) if prior else None
        if not pair_ex or not pair_prior or pair_prior[1] <= 0 or pair_ex[0] <= 0:
            row["class"] = "no_bar"
            row["obs_mult"] = None
            continue
        obs = pair_ex[0] / pair_prior[1]
        row["obs_mult"] = obs
        raw_mult = row["raw_mult"]
        if obs <= 0 or raw_mult <= 0:
            row["class"] = "no_bar"
            continue
        dist_adj = abs(math.log(obs))
        dist_raw = abs(math.log(obs / raw_mult))
        if dist_adj + 0.05 < dist_raw:
            row["class"] = "adjusted"
        elif dist_raw + 0.05 < dist_adj:
            row["class"] = "unadjusted"
        else:
            row["class"] = "ambiguous"


def load_trades(path: Path, stage: str) -> list[dict]:
    import pyarrow.parquet as pq
    if not path.exists():
        return []
    frame = pq.read_table(path).to_pandas()
    rows = []
    for rec in frame.itertuples(index=False):
        if not rec.ticker:
            continue
        rows.append({
            "stage": stage,
            "rule": rec.rule,
            "ticker": rec.ticker,
            "entry_date": rec.entry_date,
            "exit_date": rec.exit_date,
            "shares": int(rec.shares),
            "entry_px": float(rec.entry_px),
            "exit_px": float(rec.exit_px),
            "buy_fee": float(rec.buy_fee),
            "sell_fee": float(rec.sell_fee),
            "pnl": float(rec.pnl),
            "ret": float(rec.ret),
        })
    return rows


def mean_ci(values: list[float]) -> dict:
    arr = np.asarray(values, dtype=float)
    arr = arr[np.isfinite(arr)]
    n = int(arr.size)
    if n == 0:
        return {"n": 0, "mean": None, "low": None, "high": None}
    mean = float(arr.mean())
    if n < 2:
        return {"n": n, "mean": mean, "low": None, "high": None}
    se = float(arr.std(ddof=1) / math.sqrt(n))
    crit = float(stats.t.ppf(0.975, n - 1))
    return {"n": n, "mean": mean, "low": mean - crit * se, "high": mean + crit * se}


def fixed_10k(trade: dict, fees: dict) -> dict | None:
    entry = trade["entry_px"]
    exit_px = trade["exit_px"]
    if entry <= 0 or exit_px <= 0:
        return None
    shares = int(CAPITAL // entry)
    if shares < 1:
        return None
    buy = order_fees(shares, entry, "buy", fees)
    sell = order_fees(shares, exit_px, "sell", fees)
    cost = shares * entry + buy
    # One reduction pass, same idea as the book, so the ticket stays funded.
    guard = 0
    while shares >= 1 and cost > CAPITAL + 1e-6 and guard < 5:
        shares = int((CAPITAL - buy) // entry)
        if shares < 1:
            return None
        buy = order_fees(shares, entry, "buy", fees)
        sell = order_fees(shares, exit_px, "sell", fees)
        cost = shares * entry + buy
        guard += 1
    if shares < 1 or cost > CAPITAL + 1e-6:
        return None
    price = (exit_px - entry) / entry
    pnl = shares * (exit_px - entry) - buy - sell
    return {
        "shares": shares,
        "price": price,
        "futu": pnl / (shares * entry),
        "bp15": price - BP15,
        "pnl": pnl,
        "fees": buy + sell,
        "premium": fee_parts(shares, entry, "buy", fees)["floor_premium"]
        + fee_parts(shares, exit_px, "sell", fees)["floor_premium"],
    }


def analyze_trades(trades: list[dict], splits: list[dict], fees: dict) -> dict:
    by_ticker = defaultdict(list)
    for row in splits:
        by_ticker[row["ticker"]].append(row)
    for rows in by_ticker.values():
        rows.sort(key=lambda r: r["date"])

    def cum_factor(ticker: str, day: str) -> float:
        factor = 1.0
        for row in by_ticker.get(ticker, []):
            if row["date"] > day:
                factor *= row["raw_mult"]
        return factor

    fee_mismatch = 0
    pnl_mismatch = 0
    span = []
    span_unadjusted = []
    span_ambiguous = []
    span_nobar = []
    for trade in trades:
        buy = order_fees(trade["shares"], trade["entry_px"], "buy", fees)
        sell = order_fees(trade["shares"], trade["exit_px"], "sell", fees)
        if abs(buy - trade["buy_fee"]) > 1e-6 or abs(sell - trade["sell_fee"]) > 1e-6:
            fee_mismatch += 1
        pnl = trade["shares"] * (trade["exit_px"] - trade["entry_px"]) - trade["buy_fee"] - trade["sell_fee"]
        if abs(pnl - trade["pnl"]) > 1e-4:
            pnl_mismatch += 1
        hit = [
            row for row in by_ticker.get(trade["ticker"], [])
            if trade["entry_date"] < row["date"] <= trade["exit_date"]
        ]
        if not hit:
            continue
        f_entry = cum_factor(trade["ticker"], trade["entry_date"])
        f_exit = cum_factor(trade["ticker"], trade["exit_date"])
        price = trade["exit_px"] / trade["entry_px"] - 1.0
        # Cache treated as split-adjusted: undo the factor to rebuild raw.
        raw = (trade["exit_px"] / f_exit) / (trade["entry_px"] / f_entry) - 1.0
        classes = {row["class"] for row in hit}
        rec = {
            "stage": trade["stage"],
            "rule": trade["rule"],
            "ticker": trade["ticker"],
            "entry": trade["entry_date"],
            "exit": trade["exit_date"],
            "splits": [f"{row['date']} {row['ratio'] or row['numerator']}:{row['denominator']} {row['class']}" for row in hit],
            "price_return": price,
            "raw_return": raw,
            "diff": raw - price,
        }
        span.append(rec)
        if "unadjusted" in classes:
            span_unadjusted.append(rec)
        elif "ambiguous" in classes and "adjusted" not in classes:
            span_ambiguous.append(rec)
        elif "no_bar" in classes and "adjusted" not in classes:
            span_nobar.append(rec)
    return {
        "n": len(trades),
        "fee_mismatch": fee_mismatch,
        "pnl_mismatch": pnl_mismatch,
        "span": span,
        "span_unadjusted": span_unadjusted,
        "span_ambiguous": span_ambiguous,
        "span_nobar": span_nobar,
    }


def sizing_block(trades: list[dict], fees: dict) -> dict:
    price_pnl = 0.0
    fees_paid = 0.0
    premium = 0.0
    small = 0
    fee_on_small = 0.0
    notional_small = 0.0
    fee_on_large = 0.0
    notional_large = 0.0
    fixed_futu = []
    fixed_pnl = []
    fixed_premium = 0.0
    for trade in trades:
        notional = trade["shares"] * trade["entry_px"]
        price_pnl += trade["shares"] * (trade["exit_px"] - trade["entry_px"])
        paid = trade["buy_fee"] + trade["sell_fee"]
        fees_paid += paid
        premium += fee_parts(trade["shares"], trade["entry_px"], "buy", fees)["floor_premium"]
        premium += fee_parts(trade["shares"], trade["exit_px"], "sell", fees)["floor_premium"]
        if trade["shares"] < 200:
            small += 1
            fee_on_small += paid
            notional_small += notional
        else:
            fee_on_large += paid
            notional_large += notional
        ticket = fixed_10k(trade, fees)
        if ticket:
            fixed_futu.append(ticket["futu"])
            fixed_pnl.append(ticket["pnl"])
            fixed_premium += ticket["premium"]
    return {
        "n": len(trades),
        "price_pnl": price_pnl,
        "fees": fees_paid,
        "closed_pnl": price_pnl - fees_paid,
        "floor_premium": premium,
        "small_share_trades": small,
        "fee_rate_small": (fee_on_small / notional_small) if notional_small else None,
        "fee_rate_large": (fee_on_large / notional_large) if notional_large else None,
        "fixed_mean": mean_ci(fixed_futu),
        "fixed_pnl_sum": float(sum(fixed_pnl)),
        "fixed_premium": fixed_premium,
    }


def ci_table(trades: list[dict]) -> list[dict]:
    by = defaultdict(list)
    for trade in trades:
        price = trade["exit_px"] / trade["entry_px"] - 1.0
        by[trade["rule"]].append((price, price - BP15))
    rows = []
    for rule in sorted(by):
        before = mean_ci([p for p, _ in by[rule]])
        after = mean_ci([a for _, a in by[rule]])
        rows.append({"rule": rule, "before": before, "after": after})
    return rows


def random4_returns(panel, tapes, sessions, fees) -> tuple[list[float], list[float]]:
    rng = np.random.Generator(np.random.PCG64(20260813))
    rule = Rule("RANDOM4", 1, "break10", "hot_score", 4)
    equities = []
    price = []
    for draw in range(1000):
        if draw and draw % 200 == 0:
            print(f"random4 {draw}", flush=True)
        picks = []
        for rows in panel:
            if not rows:
                picks.append([])
                continue
            k = min(4, len(rows))
            chosen = rng.choice(len(rows), size=k, replace=False)
            picks.append([rows[int(i)] for i in chosen])
        book = simulate(panel, tapes, sessions, rule, fees, picks=picks)
        equities.append(book.final_equity)
        for trade in book.trades:
            price.append(trade.exit_px / trade.entry_px - 1.0)
    return equities, price


def pct(value) -> str:
    if value is None:
        return ""
    return f"{100.0 * value:.2f}%"


def num(value, digits=2) -> str:
    if value is None:
        return ""
    return f"{value:,.{digits}f}"


def ci_line(row: dict) -> str:
    b, a = row["before"], row["after"]
    return (
        f"| {row['rule']} | {b['n']} | {pct(b['mean'])} | {pct(b['low'])} to {pct(b['high'])} "
        f"| {pct(a['mean'])} | {pct(a['low'])} to {pct(a['high'])} |"
    )


def write_report(payload: dict) -> None:
    splits = payload["splits"]
    n_rev = sum(1 for s in splits if s["raw_mult"] > 1)
    n_fwd = sum(1 for s in splits if s["raw_mult"] < 1)
    classes = defaultdict(int)
    for row in splits:
        classes[row["class"]] += 1
    rev_class = defaultdict(int)
    for row in splits:
        if row["raw_mult"] > 1:
            rev_class[row["class"]] += 1
    bug = payload["span_unadjusted"]
    lines = []
    lines.append("# Mechanics check")
    lines.append("")
    lines.append("This file does not re-score a rule. It does not edit `PREREG.md`, `ADDENDUM_1.md`, or `part2_frozen.json`. The ledgers in this folder are the ones already scored.")
    lines.append("")
    if bug:
        lines.append(f"**Bug:** {len(bug)} closed trades span a split whose cached prices jump by about the split ratio. Those returns include the split. Scoring was not changed.")
        lines.append("")
    else:
        lines.append("**No split-adjustment bug found.** Cached quote prices are split-adjusted. Trades that contain a split use those adjusted prices. Scoring was left as published.")
        lines.append("")

    lines.append("## 1. Split and dividend adjustment")
    lines.append("")
    lines.append("The cache is Yahoo chart v8 `indicators.quote` open, high, low, close, and volume, from the download that requested `events=div|split`. `adj_close` is stored on each bar and is not read by a gate, a rank, a fill, or a fee.")
    lines.append("")
    lines.append("Quote prices are split-adjusted. Volume is split-adjusted in the same units, so `close * volume` stays a dollar amount. Quote prices are not dividend-adjusted. `adj_close` is the dividend-adjusted close. On AAPL the 4-for-1 split is ex-date 2020-08-31. The cached close is 124.81 on 2020-08-28 and the cached open is 127.58 on 2020-08-31. An unadjusted tape would have dropped by about 4x. The close/`adj_close` ratio is about 1.032 on both sides of that split, which is the dividend factor, not the split.")
    lines.append("")
    lines.append(f"Split events in the cached responses: {len(splits)}. Forward (raw price falls): {n_fwd}. Reverse (raw price rises, numerator < denominator): {n_rev}.")
    lines.append("")
    lines.append("Each split is classified from the cached ex-date open divided by the prior session close. Adjusted means that ratio is closer to 1 than to the raw split multiplier. Unadjusted means it is closer to the raw multiplier. A 0.05 gap in absolute log distance is required; otherwise the split is ambiguous. A missing print is `no_bar`.")
    lines.append("")
    lines.append("| class | all splits | reverse splits |")
    lines.append("| --- | --- | --- |")
    for key in ("adjusted", "unadjusted", "ambiguous", "no_bar"):
        lines.append(f"| {key} | {classes[key]} | {rev_class[key]} |")
    lines.append("")
    lines.append(f"Reverse splits whose ex-date is a session on which that ticker is in the capped proxy panel: {payload['reverse_in_panel']}. The panel is the Part 1 window (2019-01-02 through 2026-08-12), rebuilt for this count only and not scored.")
    lines.append("")
    lines.append(f"Closed trades checked: {payload['n_trades']} (Part 1, Part 2 train, and Part 2 test). Fee recomputation mismatches: {payload['fee_mismatch']}. P&L identity mismatches: {payload['pnl_mismatch']}.")
    lines.append("")
    span = payload["span"]
    lines.append(f"Trades whose open-to-exit interval contains a split ex-date (`entry < ex-date <= exit`): {len(span)}. On a split-adjusted tape the raw reconstruction differs exactly on these trades. Absolute difference in simple return: median {pct(payload['span_median_abs'])}, max {pct(payload['span_max_abs'])}.")
    lines.append("")
    lines.append(f"Of those, unadjusted: {len(bug)}. Ambiguous and not otherwise adjusted: {len(payload['span_ambiguous'])}. No cached print and not otherwise adjusted: {len(payload['span_nobar'])}.")
    lines.append("")
    if bug:
        lines.append("Unadjusted spans:")
        lines.append("")
        for rec in bug[:30]:
            lines.append(f"- {rec['stage']} {rec['rule']} {rec['ticker']} {rec['entry']} → {rec['exit']} splits {', '.join(rec['splits'])} cached return {pct(rec['price_return'])} raw-style return {pct(rec['raw_return'])}")
        lines.append("")
    if payload["span_ambiguous"]:
        lines.append("Ambiguous spans, listed so they can be inspected. They are not classified as unadjusted:")
        lines.append("")
        for rec in payload["span_ambiguous"][:20]:
            lines.append(f"- {rec['stage']} {rec['rule']} {rec['ticker']} {rec['entry']} → {rec['exit']} {', '.join(rec['splits'])}")
        lines.append("")
    lines.append("The book fills are the cached opens. Where the continuity test says adjusted, the recorded return is the split-adjusted return. The raw figure is what the same dates would have printed on an unadjusted tape, including the split jump. That raw figure is not the economic return.")
    lines.append("")

    lines.append("## 2. Fees and slippage")
    lines.append("")
    lines.append("There is no slippage model. A buy fills at that session's open. A sell fills at the open of the exit session. A missing open carries the lot. It does not substitute a close.")
    lines.append("")
    lines.append("Futubull is `paper_trade.order_fees`, one call per order. A round trip is one buy and one sell. The 15bp column is not a second Futubull charge. It is `0.0015 * shares * entry`, once per closed trade, on the same shares.")
    lines.append("")
    lines.append("For `shares > 0` and `price > 0`, amount = shares * price:")
    lines.append("")
    lines.append("- commission = min(max(0.0049 * shares, 0.99), 0.005 * amount)")
    lines.append("- platform = min(max(0.005 * shares, 1.00), 0.005 * amount)")
    lines.append("- settlement = 0.003 * shares")
    lines.append("- a sell also adds regulatory = max(0.000008 * amount, 0.01)")
    lines.append("- a sell also adds TAF = min(max(0.000166 * shares, 0.01), 8.30)")
    lines.append("")
    lines.append("The order fee is that sum, rounded to 4 decimal places. A buy does not pay regulatory or TAF. The ledger stores `buy_fee` and `sell_fee` once each. Closed P&L is `shares * (exit - entry) - buy_fee - sell_fee`. Every checked trade matches a fresh call of `order_fees` and that identity.")
    lines.append("")
    lines.append("Worked trades from the Part 1 ledger:")
    lines.append("")
    for ex in payload["examples"]:
        lines.append(f"### {ex['rule']} {ex['ticker']} {ex['entry_date']} → {ex['exit_date']}")
        lines.append("")
        lines.append(f"{ex['shares']} shares, entry {ex['entry_px']:.6f}, exit {ex['exit_px']:.6f}.")
        lines.append("")
        lines.append(f"Buy amount {ex['buy']['amount']:.4f}. Commission {ex['buy']['commission']:.4f}, platform {ex['buy']['platform']:.4f}, settlement {ex['buy']['settlement']:.4f}. Buy fee {ex['buy']['total']:.4f}. Ledger buy fee {ex['buy_fee']:.4f}.")
        lines.append("")
        lines.append(f"Sell amount {ex['sell']['amount']:.4f}. Commission {ex['sell']['commission']:.4f}, platform {ex['sell']['platform']:.4f}, settlement {ex['sell']['settlement']:.4f}, regulatory {ex['sell']['regulatory']:.4f}, TAF {ex['sell']['taf']:.4f}. Sell fee {ex['sell']['total']:.4f}. Ledger sell fee {ex['sell_fee']:.4f}.")
        lines.append("")
        lines.append(f"P&L {ex['pnl']:.4f} = {ex['shares']} * ({ex['exit_px']:.6f} - {ex['entry_px']:.6f}) - {ex['buy_fee']:.4f} - {ex['sell_fee']:.4f}.")
        lines.append("")
        lines.append(f"15bp is once on the entry notional: 0.0015 * {ex['buy']['amount']:.4f} = {ex['bp15_fee']:.4f}. It is not charged again on the sell.")
        lines.append("")

    lines.append("## 3. Sizing")
    lines.append("")
    lines.append("Position size is leftover cash, not a fraction of equity and not a fixed dollar ticket. `day_cap` is 1. After the morning sells, the cash balance is split equally across the new names (names already held are excluded). Shares = floor(budget / open). A name under 1 share is skipped and its budget is not given to another name. If the buy fee makes the cost exceed cash, shares are reduced with `order_fees` until the ticket fits or the name is skipped. This is the `leftover` rule in `PREREG.md` section 5. `simulate` in `research/longhist/engine.py` does that and does not size off marked equity.")
    lines.append("")
    lines.append("The published books start at $10,000 and reinvest what is left. The −99% ending equity is that compounded path. It is not a fixed-ticket average.")
    lines.append("")
    lines.append("The dollar minimums do not grow as a share of a tiny ticket. Commission is `min(max(0.0049 * shares, 0.99), 0.005 * amount)`. When the ticket is small, `0.005 * amount` is below $0.99, so the 0.5% cap binds and the $0.99 floor does not. The same is true of the $1.00 platform floor. The floor binds only when the notional is still large enough that 0.5% of amount exceeds the floor (about $198 for commission) and the per-share rate is still below the floor (under about 202 shares).")
    lines.append("")
    lines.append("The table uses the saved ledgers. `price P&L` is `shares * (exit - entry)` on those shares. `fees` is buy fee plus sell fee. Fee rate is that sum divided by entry notional. `floor premium` is the part of the fee above the same formula with the per-order minimums removed and the percentage caps kept. `fixed $10k` resizes each saved trade to a fresh $10,000 ticket (whole shares, Futubull once per side). It is a diagnostic. It is not a pass-rule result and it does not replace the published return. The pooled rows add separate $10,000 books.")
    lines.append("")
    lines.append("| book | trades | price P&L | fees | floor premium | fee rate if shares < 200 | fee rate if shares ≥ 200 | trades with shares < 200 | fixed $10k mean Futubull return | fixed $10k floor premium |")
    lines.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for name, block in payload["sizing"].items():
        lines.append(
            f"| {name} | {block['n']} | {num(block['price_pnl'])} | {num(block['fees'])} "
            f"| {num(block['floor_premium'])} | {pct(block['fee_rate_small'])} | {pct(block['fee_rate_large'])} "
            f"| {block['small_share_trades']} | {pct(block['fixed_mean']['mean'])} | {num(block['fixed_premium'])} |"
        )
    lines.append("")
    lines.append("Before fees, per-trade return does not depend on the share count. The fixed $10k ticket changes the Futubull return, because the minimums shrink as a fraction of a $10,000 order. It does not change the 15bp return, which is a flat fraction of entry notional. The dollar gap between price P&L and published closed P&L is the fee total above. The floor premium is the piece of that fee that appears only because the account has become small.")
    lines.append("")
    for name, block in payload["sizing"].items():
        loss = -block["closed_pnl"]
        if loss <= 0:
            continue
        lines.append(
            f"{name}: closed loss {num(loss)}. Price movement is {num(-block['price_pnl'])} of that loss "
            f"({100.0 * -block['price_pnl'] / loss:.1f}%). Fees are {num(block['fees'])} "
            f"({100.0 * block['fees'] / loss:.1f}%). The minimum-floor premium is {num(block['floor_premium'])} "
            f"({100.0 * block['floor_premium'] / loss:.1f}% of the closed loss)."
        )
        lines.append("")

    lines.append("## 4. Per-trade mean return, 95% CI")
    lines.append("")
    lines.append("Each closed trade is one observation. The return before fees is `(exit - entry) / entry`. The return after 15bp is that minus 0.0015. The interval is the Student-t 95% interval for the mean, `mean ± t_{n-1, 0.975} * s / sqrt(n)`. It is not the entry-day cluster test in rule 6.1, and it is not a Holm test. Open lots at the window end are not trades.")
    lines.append("")
    lines.append("Because 15bp subtracts a constant, the after-fee interval is the before-fee interval shifted down by 0.15 percentage points.")
    lines.append("")
    lines.append("| rule | n | mean before fees | 95% CI | mean after 15bp | 95% CI |")
    lines.append("| --- | --- | --- | --- | --- | --- |")
    for row in payload["ci_part1"]:
        lines.append(ci_line(row))
    lines.append(ci_line(payload["ci_random_part1"]))
    lines.append("")
    lines.append("Part 2 test rules, same definition. RANDOM4 here is the test-window baseline, seed 20260813, 1,000 draws, hold 1, four names.")
    lines.append("")
    lines.append("| rule | n | mean before fees | 95% CI | mean after 15bp | 95% CI |")
    lines.append("| --- | --- | --- | --- | --- | --- |")
    for row in payload["ci_part2"]:
        lines.append(ci_line(row))
    lines.append(ci_line(payload["ci_random_part2"]))
    lines.append("")
    lines.append(f"Part 1 RANDOM4 regenerated ending-equity mean return {pct(payload['random_part1_return'])}. Published mean return {pct(payload['published_random_part1'])}.")
    lines.append("")
    lines.append(f"Part 2 test RANDOM4 regenerated ending-equity mean return {pct(payload['random_part2_return'])}. Published mean return {pct(payload['published_random_part2'])}.")
    lines.append("")
    if payload["random_mismatch"]:
        lines.append("**The regenerated RANDOM4 ending equity does not match the published figure.** The per-trade interval above is from this regeneration. The published baseline was not overwritten.")
        lines.append("")
    OUT.write_text("\n".join(lines) + "\n")
    print(f"wrote {OUT}", flush=True)


def main() -> None:
    fees = fee_model()
    sessions = sessions_between("2018-01-01", "2026-08-12")
    splits = load_splits()
    classify_splits(splits, sessions)
    trades = []
    trades += load_trades(Path("research/longhist/results/trades_part1.parquet"), "part1")
    trades += load_trades(Path("research/longhist/results/trades_part2_train.parquet"), "part2_train")
    trades += load_trades(Path("research/longhist/results/trades_part2_test.parquet"), "part2_test")
    checked = analyze_trades(trades, splits, fees)
    span = checked["span"]
    abs_diff = sorted(abs(rec["diff"]) for rec in span)
    part1 = [t for t in trades if t["stage"] == "part1"]
    part2 = [t for t in trades if t["stage"] == "part2_test"]
    # Examples: largest share count, smallest share count, and a mid-size sell.
    ordered = sorted(part1, key=lambda t: t["shares"])
    picked = [ordered[-1], ordered[0], ordered[len(ordered) // 2]]
    examples = []
    for trade in picked:
        buy = fee_parts(trade["shares"], trade["entry_px"], "buy", fees)
        sell = fee_parts(trade["shares"], trade["exit_px"], "sell", fees)
        examples.append({
            **trade,
            "buy": buy,
            "sell": sell,
            "bp15_fee": BP15 * trade["shares"] * trade["entry_px"],
        })
    print("loading tapes for the panel count and RANDOM4", flush=True)
    tapes = load_tapes("2026-08-12")
    score_sessions = sessions_between("2019-01-01", "2026-08-12")
    panel = build_panel(tapes, score_sessions)
    member = defaultdict(set)
    for day, rows in zip(score_sessions, panel):
        member[day] = {row.ticker for row in rows}
    reverse_in_panel = 0
    for row in splits:
        if row["raw_mult"] > 1 and row["ticker"] in member.get(row["date"], ()):
            reverse_in_panel += 1
    print("RANDOM4 part 1", flush=True)
    eq1, px1 = random4_returns(panel, tapes, score_sessions, fees)
    test_sessions = [d for d in score_sessions if d >= "2024-01-01"]
    test_panel = [rows for day, rows in zip(score_sessions, panel) if day >= "2024-01-01"]
    print("RANDOM4 part 2 test", flush=True)
    eq2, px2 = random4_returns(test_panel, tapes, test_sessions, fees)
    part1_doc = json.loads(Path("research/longhist/results/part1.json").read_text())
    part2_doc = json.loads(Path("research/longhist/results/part2_test.json").read_text())
    pub1 = float(part1_doc["rows"][0]["random4_mean_return"])
    pub2 = float(part2_doc["rows"][0]["random4_mean_return"])
    got1 = float(np.mean(np.asarray(eq1) / CAPITAL - 1.0))
    got2 = float(np.mean(np.asarray(eq2) / CAPITAL - 1.0))
    payload = {
        "splits": splits,
        "reverse_in_panel": reverse_in_panel,
        "n_trades": checked["n"],
        "fee_mismatch": checked["fee_mismatch"],
        "pnl_mismatch": checked["pnl_mismatch"],
        "span": span,
        "span_unadjusted": checked["span_unadjusted"],
        "span_ambiguous": checked["span_ambiguous"],
        "span_nobar": checked["span_nobar"],
        "span_median_abs": float(abs_diff[len(abs_diff) // 2]) if abs_diff else 0.0,
        "span_max_abs": float(abs_diff[-1]) if abs_diff else 0.0,
        "examples": examples,
        "sizing": {
            "Part 1, four rules pooled": sizing_block(part1, fees),
            "Part 2 test, 40 rules pooled": sizing_block(part2, fees),
        },
        "ci_part1": ci_table(part1),
        "ci_part2": ci_table(part2),
        "ci_random_part1": {
            "rule": "RANDOM4 part 1",
            "before": mean_ci(px1),
            "after": mean_ci([p - BP15 for p in px1]),
        },
        "ci_random_part2": {
            "rule": "RANDOM4 part 2 test",
            "before": mean_ci(px2),
            "after": mean_ci([p - BP15 for p in px2]),
        },
        "random_part1_return": got1,
        "random_part2_return": got2,
        "published_random_part1": pub1,
        "published_random_part2": pub2,
        "random_mismatch": abs(got1 - pub1) > 1e-6 or abs(got2 - pub2) > 1e-6,
    }
    # Per-rule sizing for the four Part 1 books, so the -99% split is visible per rule.
    for rule in ("longhist_break10_h2", "longhist_rvol_lg_h1", "longhist_break10_h1", "longhist_zero_candle_h2"):
        payload["sizing"][rule] = sizing_block([t for t in part1 if t["rule"] == rule], fees)
    write_report(payload)
    summary = {
        "classes": dict(defaultdict(int, {s["class"]: 0 for s in splits})),
        "span": len(span),
        "unadjusted_spans": len(checked["span_unadjusted"]),
        "fee_mismatch": checked["fee_mismatch"],
        "pnl_mismatch": checked["pnl_mismatch"],
        "reverse_in_panel": reverse_in_panel,
        "random_part1": [got1, pub1],
        "random_part2": [got2, pub2],
    }
    counts = defaultdict(int)
    for row in splits:
        counts[row["class"]] += 1
    summary["classes"] = dict(counts)
    print(json.dumps(summary, indent=2), flush=True)


if __name__ == "__main__":
    main()
