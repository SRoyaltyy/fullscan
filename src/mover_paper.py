"""Mover paper trading — simulate actually trading the mover lookback calls.

Takes the stamped calls from 03_scoreboard/mover_lookback_action.json,
re-applies the `gated` preset (featured + SPY down-streak SELL veto, all
09:30-knowable), keeps each day's top-N calls by conviction, and runs a
cash-accounted paper book:

- entry at the call date's 09:30 open, exit at the next session's close
  (1-session hold)
- per-trade notional = equity * pct (default 5%)
- Futubull fees via src.paper_trade.order_fees
- SELL side is a short: skip when open < $5 (hard-to-borrow proxy),
  1%/yr borrow fee prorated over the hold
- trades are SKIPPED (and logged) when cash / buying power is insufficient —
  held positions tie up capital, so not every theoretical call fills

Outputs:
  data/mover_paper/trades.csv        one row per filled trade
  data/mover_paper/skipped.csv       one row per skipped call (with reason)
  data/mover_paper/equity_curve.csv  daily mark-to-market equity
  data/mover_paper/state.json        headline stats
  03_scoreboard/MOVER_PAPER.md       human summary
  dashboard/mover-paper/index.html   standalone dashboard page
"""
from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PAYLOAD = ROOT / "03_scoreboard" / "mover_lookback_action.json"
OUT_DIR = ROOT / "data" / "mover_paper"
MD_OUT = ROOT / "03_scoreboard" / "MOVER_PAPER.md"
HTML_OUT = ROOT / "dashboard" / "mover-paper" / "index.html"

BORROW_MIN_PRICE = 5.0          # hard-to-borrow proxy below $5
BORROW_ANNUAL = 0.01            # 1%/yr borrow fee, prorated by hold days
DEFAULT_CAPITAL = 100_000.0
DEFAULT_TOP_N = 10
DEFAULT_PCT = 0.05
GATED_PRESET = "gated"


# ------------------------------------------------------------ payload I/O --
def load_payload(path: Path = PAYLOAD) -> dict:
    if not path.is_file():
        raise SystemExit(f"[mover-paper] missing payload: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def gated_calls(payload: dict) -> list[dict]:
    """Replay the `gated` preset over the stamped rows.

    The gate only vetoes (SELL -> HOLD when SPY is in a down-streak), so
    replaying over the already-called rows reproduces exactly what the gated
    preset would have called — rows the featured preset never called stay
    uncalled either way.
    """
    from src import lookback_action as act

    regime = payload.get("regime") or {}
    params = act.preset_params(GATED_PRESET)
    params["_regime"] = regime
    out = []
    for row in payload.get("called_rows") or []:
        packed = act.action_call(row, params=params)
        if packed["action"] not in ("BUY", "SELL"):
            continue  # gated veto turned this into HOLD — no trade
        rec = dict(row)
        rec["gated_action"] = packed["action"]
        rec["gated_reason"] = packed["reason"]
        rec["conviction"] = act.conviction(row, packed)
        out.append(rec)
    return out


# --------------------------------------------------------------- pricing --
def _bar(ticker: str, date: str) -> dict:
    """Session bar from the OHLC store (empty dict when unavailable)."""
    try:
        from src import ticker_lookback as tl
        return tl.session_bar(ticker, date) or {}
    except Exception:
        return {}


def _exit_close(row: dict, entry_close: float) -> tuple[str | None, float | None]:
    """Next-session close: bar store first, else close * (1 + fwd_1d)."""
    hz = row.get("horizon_dates") or {}
    nxt = hz.get("1d")
    if nxt:
        c = (_bar(row.get("ticker") or "", nxt) or {}).get("close")
        if c:
            return nxt, float(c)
    fwd = (row.get("price_changes") or {}).get("1d")
    if fwd is not None and entry_close:
        try:
            return nxt, entry_close * (1.0 + float(fwd) / 100.0)
        except (TypeError, ValueError):
            pass
    return None, None


# --------------------------------------------------------------- the sim --
def run_sim(calls: list[dict], capital: float = DEFAULT_CAPITAL,
            top_n: int = DEFAULT_TOP_N, pct: float = DEFAULT_PCT) -> dict:
    from src import paper_trade as pt

    fees = pt.load_fees()
    by_day: dict[str, list[dict]] = defaultdict(list)
    for r in calls:
        by_day[r.get("date")].append(r)

    cash = capital
    open_pos: list[dict] = []      # filled, not yet exited
    trades: list[dict] = []
    skipped: list[dict] = []
    curve: list[dict] = []

    dates = sorted(by_day)
    for date in dates:
        day_calls = by_day[date]
        day_calls.sort(key=lambda r: -(r.get("conviction") or 0))

        # ---- mark-to-market helper --------------------------------------
        def equity_now(px_date: str) -> float:
            eq = cash
            for p in open_pos:
                last = p.get("last_px") or p["entry_px"]
                c = (_bar(p["ticker"], px_date) or {}).get("close")
                if c:
                    last = p["last_px"] = float(c)
                mv = p["shares"] * last
                eq += mv if p["side"] == "BUY" else -mv
            return eq

        # ---- exits first (positions held from a prior session) ----------
        still_open = []
        for p in open_pos:
            if p["exit_date"] == date:
                bar = _bar(p["ticker"], date)
                px = (bar or {}).get("close") or p.get("last_px") \
                    or p["entry_px"]
                px = float(px)
                if p["side"] == "BUY":
                    fee = pt.order_fees(p["shares"], px, "sell", fees)
                    cash += p["shares"] * px - fee
                    pnl = p["shares"] * (px - p["entry_px"]) \
                        - p["fee_in"] - fee
                else:
                    fee = pt.order_fees(p["shares"], px, "buy", fees)
                    cash -= p["shares"] * px + fee
                    pnl = p["shares"] * (p["entry_px"] - px) \
                        - p["fee_in"] - fee
                p.update({"exit_px": px, "fee_out": fee, "pnl": round(pnl, 2),
                          "ret_pct": round(100 * pnl / max(p["notional"], 1), 2)})
                trades.append(p)
            else:
                still_open.append(p)
        open_pos = still_open

        # ---- entries: top-N per side ------------------------------------
        taken = {"BUY": 0, "SELL": 0}
        for r in day_calls:
            side = r["gated_action"]
            ticker = r.get("ticker") or ""
            if taken[side] >= top_n:
                skipped.append({"date": date, "ticker": ticker, "side": side,
                                "conviction": r.get("conviction"),
                                "reason": "outside top-N conviction cut"})
                continue
            bar = r.get("session_bar") or _bar(ticker, date)
            op = (bar or {}).get("open")
            cl = (bar or {}).get("close")
            if not op or not cl:
                skipped.append({"date": date, "ticker": ticker, "side": side,
                                "conviction": r.get("conviction"),
                                "reason": "no session bar (price data missing)"})
                continue
            op, cl = float(op), float(cl)
            if side == "SELL" and op < BORROW_MIN_PRICE:
                skipped.append({"date": date, "ticker": ticker, "side": side,
                                "conviction": r.get("conviction"),
                                "reason": f"open ${op:.2f} < ${BORROW_MIN_PRICE:.0f} "
                                          f"hard-to-borrow screen"})
                continue
            exit_date, exit_px = _exit_close(r, cl)
            if exit_px is None:
                skipped.append({"date": date, "ticker": ticker, "side": side,
                                "conviction": r.get("conviction"),
                                "reason": "no next-session close (unsettleable)"})
                continue

            eq = equity_now(date)
            notional = round(eq * pct, 2)
            shares = int(notional // op)
            if shares <= 0:
                skipped.append({"date": date, "ticker": ticker, "side": side,
                                "conviction": r.get("conviction"),
                                "reason": "position size < 1 share"})
                continue
            notional = shares * op
            if side == "BUY":
                fee_in = pt.order_fees(shares, op, "buy", fees)
                cost = notional + fee_in
                if cost > cash:
                    skipped.append({"date": date, "ticker": ticker,
                                    "side": side,
                                    "conviction": r.get("conviction"),
                                    "reason": f"insufficient cash "
                                              f"(${cost:,.0f} > ${cash:,.0f})"})
                    continue
                cash -= cost
            else:
                # short: Reg-T-ish guard — equity must cover 2x notional
                if eq < 2 * notional:
                    skipped.append({"date": date, "ticker": ticker,
                                    "side": side,
                                    "conviction": r.get("conviction"),
                                    "reason": f"insufficient margin "
                                              f"(equity ${eq:,.0f} < 2x "
                                              f"notional ${notional:,.0f})"})
                    continue
                hold_days = 1
                borrow = notional * BORROW_ANNUAL * hold_days / 365.0
                fee_in = pt.order_fees(shares, op, "sell", fees) + borrow
                cash += notional - fee_in
            taken[side] += 1
            open_pos.append({
                "date": date, "ticker": ticker, "side": side,
                "shares": shares, "entry_px": op, "notional": round(notional, 2),
                "fee_in": round(fee_in, 2), "exit_date": exit_date,
                "exit_px_planned": round(exit_px, 4),
                "conviction": r.get("conviction"),
                "reason": r.get("gated_reason"),
                "last_px": cl,
            })

        # ---- end-of-day mark ---------------------------------------------
        curve.append({"date": date, "cash": round(cash, 2),
                      "equity": round(equity_now(date), 2),
                      "open": len(open_pos)})

    # force-close anything still open at the last known price
    for p in open_pos:
        px = float(p.get("last_px") or p["entry_px"])
        fee = pt.order_fees(p["shares"], px,
                            "sell" if p["side"] == "BUY" else "buy", fees)
        sign = 1 if p["side"] == "BUY" else -1
        pnl = sign * p["shares"] * (px - p["entry_px"]) - p["fee_in"] - fee
        cash += (p["shares"] * px - fee) if p["side"] == "BUY" \
            else -(p["shares"] * px + fee)
        p.update({"exit_px": px, "fee_out": fee, "pnl": round(pnl, 2),
                  "ret_pct": round(100 * pnl / max(p["notional"], 1), 2),
                  "reason": (p.get("reason") or "") + " [force-closed]"})
        trades.append(p)

    return {"capital": capital, "top_n": top_n, "pct": pct,
            "trades": trades, "skipped": skipped, "curve": curve,
            "final_equity": round(cash, 2)}


# ---------------------------------------------------------------- stats --
def stats(sim: dict) -> dict:
    trades = sim["trades"]
    curve = sim["curve"]
    cap = sim["capital"]
    final = curve[-1]["equity"] if curve else sim["final_equity"]
    pnls = [t["pnl"] for t in trades]
    wins = [p for p in pnls if p > 0]
    by_side = {}
    for side in ("BUY", "SELL"):
        sp = [t["pnl"] for t in trades if t["side"] == side]
        by_side[side] = {
            "n": len(sp),
            "hit": round(sum(1 for p in sp if p > 0) / len(sp), 3) if sp else None,
            "pnl": round(sum(sp), 2),
        }
    peak, max_dd = cap, 0.0
    for pt_ in curve:
        peak = max(peak, pt_["equity"])
        max_dd = max(max_dd, (peak - pt_["equity"]) / peak)
    return {
        "n_trades": len(trades),
        "n_skipped": len(sim["skipped"]),
        "hit": round(len(wins) / len(pnls), 3) if pnls else None,
        "total_pnl": round(sum(pnls), 2),
        "total_ret_pct": round(100 * (final - cap) / cap, 2),
        "final_equity": round(final, 2),
        "max_dd_pct": round(100 * max_dd, 2),
        "avg_win": round(sum(wins) / len(wins), 2) if wins else None,
        "avg_loss": (round(sum(p for p in pnls if p <= 0)
                           / max(len(pnls) - len(wins), 1), 2)
                     if pnls else None),
        "by_side": by_side,
        "n_days": len(curve),
    }


# ---------------------------------------------------------------- output --
def write_outputs(sim: dict, st: dict, payload: dict) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    HTML_OUT.parent.mkdir(parents=True, exist_ok=True)

    trade_cols = ["date", "ticker", "side", "shares", "entry_px", "exit_px",
                  "exit_date", "notional", "fee_in", "fee_out", "pnl",
                  "ret_pct", "conviction", "reason"]
    with open(OUT_DIR / "trades.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=trade_cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(sim["trades"])
    with open(OUT_DIR / "skipped.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(
            fh, fieldnames=["date", "ticker", "side", "conviction", "reason"],
            extrasaction="ignore")
        w.writeheader()
        w.writerows(sim["skipped"])
    with open(OUT_DIR / "equity_curve.csv", "w", newline="",
              encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=["date", "cash", "equity", "open"])
        w.writeheader()
        w.writerows(sim["curve"])
    state = {"generated_at": datetime.now().isoformat(timespec="seconds"),
             "asof": payload.get("to_date"), "params": {
                 "capital": sim["capital"], "top_n": sim["top_n"],
                 "pct": sim["pct"], "preset": GATED_PRESET}, **st}
    (OUT_DIR / "state.json").write_text(json.dumps(state, indent=2),
                                        encoding="utf-8")
    _write_md(sim, st, payload)
    _write_html(sim, st, payload)


def _write_md(sim: dict, st: dict, payload: dict) -> None:
    bs = st["by_side"]
    L = [
        "# Mover paper trading",
        "",
        f"_Generated {datetime.now().isoformat(timespec='seconds')} — "
        f"calls {payload.get('from_date')} → {payload.get('to_date')}_",
        "",
        "Trades the **gated** mover calls (featured preset + SPY "
        "down-streak ≥ 3 SELL veto), top "
        f"{sim['top_n']} per side by conviction, "
        f"{sim['pct']:.0%} of equity per trade, 1-session hold "
        "(09:30 open → next session close), Futubull fees, shorts screened "
        f"below ${BORROW_MIN_PRICE:.0f} with a {BORROW_ANNUAL:.0%}/yr borrow "
        "charge. Trades that don't fit the cash / margin available are "
        "**skipped and logged** — this is a cash-accounted book, not a "
        "theoretical fill-everything tally.",
        "",
        "## Headline",
        "",
        f"| Start capital | Final equity | Return | Max DD | Trades | "
        f"Skipped | Win rate |",
        f"|---:|---:|---:|---:|---:|---:|---:|",
        f"| ${sim['capital']:,.0f} | ${st['final_equity']:,.2f} | "
        f"**{st['total_ret_pct']}%** | {st['max_dd_pct']}% | "
        f"{st['n_trades']} | {st['n_skipped']} | "
        f"{round(100 * (st['hit'] or 0), 1)}% |",
        "",
        "| Side | Trades | Win rate | P&L |",
        "|---|---:|---:|---:|",
        f"| BUY (long) | {bs['BUY']['n']} | "
        f"{round(100 * (bs['BUY']['hit'] or 0), 1)}% | ${bs['BUY']['pnl']:,.2f} |",
        f"| SELL (short) | {bs['SELL']['n']} | "
        f"{round(100 * (bs['SELL']['hit'] or 0), 1)}% | "
        f"${bs['SELL']['pnl']:,.2f} |",
        "",
        f"Full detail: `data/mover_paper/trades.csv`, "
        f"`skipped.csv`, `equity_curve.csv`. Dashboard: "
        f"`dashboard/mover-paper/index.html`.",
        "",
        "## Last 20 filled trades",
        "",
        "| Date | Ticker | Side | Shares | Entry 09:30 | Exit close | "
        "P&L | Ret | Why |",
        "|---|---|---|---:|---:|---:|---:|---:|---|",
    ]
    for t in sim["trades"][-20:]:
        L.append(
            f"| {t['date']} | `{t['ticker']}` | {t['side']} | {t['shares']} | "
            f"${t['entry_px']:.2f} | ${t.get('exit_px') or 0:.2f} | "
            f"${t.get('pnl') or 0:,.2f} | {t.get('ret_pct') or 0}% | "
            f"{str(t.get('reason') or '—').replace('|', '/')[:80]} |")
    L.append("")
    MD_OUT.write_text("\n".join(L) + "\n", encoding="utf-8")


def _write_html(sim: dict, st: dict, payload: dict) -> None:
    import html as _html
    curve = sim["curve"]
    # inline SVG equity curve
    svg = ""
    if len(curve) > 1:
        W, H, P = 960, 260, 34
        xs = [i for i in range(len(curve))]
        ys = [c["equity"] for c in curve]
        lo, hi = min(ys + [sim["capital"]]), max(ys + [sim["capital"]])
        rng = (hi - lo) or 1.0

        def X(i):
            return P + (W - 2 * P) * i / (len(curve) - 1)

        def Y(v):
            return H - P - (H - 2 * P) * (v - lo) / rng

        pts = " ".join(f"{X(i):.1f},{Y(v):.1f}" for i, v in zip(xs, ys))
        base = Y(sim["capital"])
        svg = (
            f"<svg viewBox='0 0 {W} {H}' width='100%' height='{H}'>"
            f"<line x1='{P}' y1='{base:.1f}' x2='{W - P}' y2='{base:.1f}' "
            f"stroke='#5b6b8c' stroke-dasharray='4 4'/>"
            f"<polyline points='{pts}' fill='none' stroke='#4ade80' "
            f"stroke-width='2'/>"
            f"<text x='{P}' y='{Y(hi) - 6:.1f}' fill='#9cabc9' "
            f"font-size='12'>${hi:,.0f}</text>"
            f"<text x='{P}' y='{Y(lo) + 14:.1f}' fill='#9cabc9' "
            f"font-size='12'>${lo:,.0f}</text>"
            f"<text x='{W - P}' y='{base - 6:.1f}' fill='#9cabc9' "
            f"font-size='12' text-anchor='end'>start "
            f"${sim['capital']:,.0f}</text></svg>")
    rows = []
    for t in sim["trades"]:
        cls = "good" if (t.get("pnl") or 0) > 0 else "bad"
        rows.append(
            f"<tr><th>{t['date']}</th><td>{_html.escape(t['ticker'])}</td>"
            f"<td>{t['side']}</td><td>{t['shares']}</td>"
            f"<td>${t['entry_px']:.2f}</td>"
            f"<td>${t.get('exit_px') or 0:.2f}</td>"
            f"<td class='{cls}'>${t.get('pnl') or 0:,.2f}</td>"
            f"<td class='{cls}'>{t.get('ret_pct') or 0}%</td>"
            f"<td>{t.get('conviction') or 0:.1f}</td>"
            f"<td class='why'>{_html.escape(str(t.get('reason') or '—'))}</td>"
            f"</tr>")
    sk = []
    for s in sim["skipped"]:
        sk.append(
            f"<tr><th>{s['date']}</th><td>{_html.escape(str(s['ticker']))}</td>"
            f"<td>{s['side']}</td><td>{(s.get('conviction') or 0):.1f}</td>"
            f"<td class='why'>{_html.escape(str(s.get('reason')))}</td></tr>")
    bs = st["by_side"]
    HTML_OUT.write_text(f"""<!doctype html>
<html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Mover paper trading</title>
<style>
:root{{--bg:#0b1020;--card:#131b31;--line:#2b3552;--text:#edf2ff;--muted:#9cabc9}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--text);font:15px/1.45 system-ui}}
main{{max-width:1180px;margin:auto;padding:16px}}h1,h2{{margin:.4em 0}}
.muted{{color:var(--muted)}}
.cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin:14px 0}}
.card{{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:12px}}
.card b{{display:block;font-size:22px;margin-top:4px}}
.sheet{{overflow-x:auto;border:1px solid var(--line);border-radius:12px;margin:14px 0}}
table{{border-collapse:separate;border-spacing:0;width:100%;background:var(--card)}}
th,td{{padding:7px 8px;text-align:center;border-bottom:1px solid var(--line);white-space:nowrap}}
thead th{{position:sticky;top:0;background:#17213a}}
tbody th{{background:#17213a;text-align:left}}
td.good{{color:#4ade80}}td.bad{{color:#f87171}}
td.why{{text-align:left;white-space:normal;max-width:340px;font-size:12px}}
</style></head><body><main>
<h1>Mover paper trading</h1>
<p class="muted">Gated mover calls (featured + SPY down-streak veto) ·
top {sim['top_n']}/side by conviction · {sim['pct']:.0%} equity per trade ·
1-session hold · Futubull fees · cash-accounted (unfittable trades skipped).</p>
<div class="cards">
<div class="card">Final equity<b>${st['final_equity']:,.0f}</b></div>
<div class="card">Return<b>{st['total_ret_pct']}%</b></div>
<div class="card">Max drawdown<b>{st['max_dd_pct']}%</b></div>
<div class="card">Trades<b>{st['n_trades']}</b></div>
<div class="card">Skipped<b>{st['n_skipped']}</b></div>
<div class="card">Win rate<b>{round(100 * (st['hit'] or 0), 1)}%</b></div>
<div class="card">BUY P&L<b>${bs['BUY']['pnl']:,.0f}</b></div>
<div class="card">SELL P&L<b>${bs['SELL']['pnl']:,.0f}</b></div>
</div>
{svg}
<h2>Filled trades</h2>
<div class="sheet"><table>
<thead><tr><th>Date</th><th>Ticker</th><th>Side</th><th>Shares</th>
<th>Entry 09:30</th><th>Exit close</th><th>P&L</th><th>Ret</th>
<th>Conviction</th><th>Why</th></tr></thead>
<tbody>{''.join(rows)}</tbody></table></div>
<h2>Skipped calls</h2>
<div class="sheet"><table>
<thead><tr><th>Date</th><th>Ticker</th><th>Side</th><th>Conviction</th>
<th>Why skipped</th></tr></thead>
<tbody>{''.join(sk)}</tbody></table></div>
</main></body></html>""", encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--payload", default=str(PAYLOAD))
    ap.add_argument("--capital", type=float, default=DEFAULT_CAPITAL)
    ap.add_argument("--top-n", type=int, default=DEFAULT_TOP_N)
    ap.add_argument("--pct", type=float, default=DEFAULT_PCT)
    args = ap.parse_args()
    payload = load_payload(Path(args.payload))
    calls = gated_calls(payload)
    print(f"[mover-paper] {len(calls)} gated calls "
          f"({len(payload.get('called_rows') or [])} stamped)")
    sim = run_sim(calls, capital=args.capital, top_n=args.top_n,
                  pct=args.pct)
    st = stats(sim)
    write_outputs(sim, st, payload)
    print(f"[mover-paper] {st['n_trades']} trades, {st['n_skipped']} skipped, "
          f"equity ${st['final_equity']:,.0f} ({st['total_ret_pct']}%), "
          f"hit {st['hit']}, maxDD {st['max_dd_pct']}%")


if __name__ == "__main__":
    main()
