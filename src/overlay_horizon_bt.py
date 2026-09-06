"""Sleeve-native horizon backtest for #142 KEEP overlays.

Research only. Does not import sleeve_merge / LIVE_POLICY and does not
write flatten_robust.

Clock
  * Features on session D = last Elite export with date < D (never D).
  * Fill = 09:30 open (Finviz ``Open``, else reconstructed).
  * Theme Radar 1d = open → same-day close + Futubull fees.
  * flatten_hN leftover = factor_mine_book leftover / sell-first /
    min-hold N / hard-red S≤−3 sit.
  * flatten_hN unit = hold_window open → last-session close + fees
    (signal cousin of leftover, not a substitute).

CLI:
  python3 -m src.overlay_horizon_bt --write
"""
from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path

from . import finviz_style_flags as fsf
from . import overlay_autopsy as oa

ROOT = Path(__file__).resolve().parent.parent
EXPORT_DIR = ROOT / "data" / "exports"
FEES_PATH = ROOT / "00_grounding" / "futubull_fees.json"
LOOKBACK_JSON = ROOT / "03_scoreboard" / "flatten_lookback_action.json"
OUT_MD = ROOT / "03_scoreboard" / "OVERLAY_HORIZON_BT.md"
OUT_JSON = ROOT / "03_scoreboard" / "overlay_horizon_bt.json"

CAPITAL = 10_000.0
UNIT_NOTIONAL = 1_000.0
TOP_N = 8
HARD_RED = -3.0
MIN_TAPE = 20
MIN_UNIT = 20
MIN_LIVE_DAYS = 8
TOP2_REJECT = 0.65
WF_MIN = 8
TAPE_EPS = 0.30

# Iterate KEEP knobs only. Do not invent name lists.
FPE_SWEEP = (35.0, 40.0, 50.0)
REGIME_SWEEP = ("all", "morn_up", "s_nonneg")

# Local Theme Radar 5d FPE board (war room). Authoritative for flatten_h5.
# Up-tape sign 40% (2/5) — FAIL both-tape. Do not wire. Do not re-mine FPE on 5d.
LOCAL_5D_FPE_BOARD = {
    "source": "theme_radar_5d_fpe_board",
    "target_sleeve": "flatten_h5",
    "hold_sessions": 5,
    "score_clock": "5d Theme Radar FPE IC",
    "fpe": {
        "ic_up": -0.033,
        "sign_up": 0.40,
        "sign_up_frac": "2/5",
        "n_up": 5,
        "ic_down": -0.131,
        "sign_down": 1.00,
        "n_down": 10,
        "both_tape": False,
        "verdict": "FAIL",
        "reason": (
            "Up-tape flips (Sign_up 40%, 2/5, n=5). "
            "Do not add both-tape 5d FPE Avoid to flatten_h5."
        ),
    },
    "d_rsi": {"verdict": "INCONCLUSIVE", "clock": "5d"},
    "d_mcap": {"verdict": "INCONCLUSIVE", "clock": "5d"},
}


def fpe_clock_allowed(sleeve: str) -> bool:
    """FPE Avoid is a 1d Theme Radar signal. flatten_h5 is closed."""
    return sleeve == "theme_radar_1d"


def stamp_local_5d_board(rows: list[dict]) -> list[dict]:
    """Override leftover THIN on flatten_h5 with the local 5d IC FAIL."""
    fpe = LOCAL_5D_FPE_BOARD["fpe"]
    why = (
        f"Theme Radar 5d FPE board: IC_up {fpe['ic_up']:+.3f} "
        f"Sign_up {100 * fpe['sign_up']:.0f}% ({fpe['sign_up_frac']}) "
        f"n={fpe['n_up']}; IC_down {fpe['ic_down']:+.3f} "
        f"Sign_down {100 * fpe['sign_down']:.0f}% n={fpe['n_down']}. "
        f"{fpe['reason']}"
    )
    for r in rows:
        if r.get("sleeve") != "flatten_h5":
            continue
        gate = dict(r.get("gate") or {})
        leftover = gate.get("excess_usd")
        gate["verdict"] = "FAIL"
        gate["reasons"] = [why]
        if leftover is not None:
            gate["reasons"].append(
                f"leftover $ was {leftover:+.2f} (thin-n, not a rescue)"
            )
        gate["local_5d_board"] = LOCAL_5D_FPE_BOARD
        r["gate"] = gate
    return rows


SLEEVES = (
    {
        "name": "theme_radar_1d",
        "hold_sessions": 1,
        "universe": "radar",
        "score_clock": "1d open→close + Futubull",
        "book": "unit",
    },
    {
        "name": "flatten_h1",
        "hold_sessions": 1,
        "universe": "flatten",
        "score_clock": "09:30 leftover · min-hold 1 + fees",
        "book": "leftover",
    },
    {
        "name": "flatten_h3",
        "hold_sessions": 3,
        "universe": "flatten",
        "score_clock": "09:30 leftover · min-hold 3 + fees",
        "book": "leftover",
    },
    {
        "name": "flatten_h5",
        "hold_sessions": 5,
        "universe": "flatten",
        "score_clock": "09:30 leftover · min-hold 5 + fees",
        "book": "leftover",
    },
    {
        "name": "flatten_live_h5",
        "hold_sessions": 5,
        "universe": "flatten_live",
        "score_clock": "gated leftover · min-hold 5 + fees",
        "book": "leftover",
    },
    {
        "name": "flatten_robust_shaped",
        "hold_sessions": 3,
        "universe": "flatten_live",
        "score_clock": "gated leftover · min-hold 3 (live-shaped, not LIVE)",
        "book": "leftover",
    },
)


def hold_window(cal: list[str], date: str, hold: int) -> list[str]:
    """Same contract as factor_mine.hold_window. Entry morning counts as 1."""
    if date not in cal or hold < 1:
        return []
    i = cal.index(date)
    return cal[i:i + int(hold)]


def prior_export(export_cal: list[str], date: str) -> str | None:
    """Last Elite file strictly before D. Never D."""
    earlier = [d for d in export_cal if d < date]
    return earlier[-1] if earlier else None


def load_fees() -> dict:
    return json.loads(FEES_PATH.read_text(encoding="utf-8"))


def order_fees(shares: int, price: float, side: str, f: dict) -> float:
    """Futubull US-stock fees — same schedule as paper_trade.order_fees."""
    if shares <= 0 or price <= 0:
        return 0.0
    amount = shares * price
    comm = min(max(f["commission_per_share"] * shares, f["commission_min_per_order"]),
               f["commission_max_pct_of_amount"] * amount)
    plat = min(max(f["platform_per_share"] * shares, f["platform_min_per_order"]),
               f["platform_max_pct_of_amount"] * amount)
    settle = f["settlement_per_share"] * shares
    total = comm + plat + settle
    if side == "sell":
        reg = max(f["regulatory_pct_of_amount_sell_only"] * amount,
                  f["regulatory_min_per_order"])
        taf = min(max(f["taf_per_share_sell_only"] * shares, f["taf_min_per_order"]),
                  f["taf_max_per_order"])
        total += reg + taf
    return round(total, 4)


def _num(v) -> float | None:
    return fsf.finite(v)


def export_dates() -> list[str]:
    files = sorted(EXPORT_DIR.glob("finviz_????-??-??.csv"))
    return [p.stem.replace("finviz_", "") for p in files]


def load_finviz_bars(date: str) -> dict[str, dict]:
    """Same-day Open / Price = fill tape, never a gate."""
    path = EXPORT_DIR / f"finviz_{date}.csv"
    if not path.exists():
        return {}
    out: dict[str, dict] = {}
    with path.open(newline="", encoding="utf-8", errors="replace") as fh:
        for raw in csv.DictReader(fh):
            t = str(raw.get("Ticker") or "").strip().upper()
            if not t or t in out:
                continue
            o = _num(raw.get("Open"))
            c = _num(raw.get("Price"))
            chg = _num(raw.get("Change"))
            cfo = _num(raw.get("Change from Open"))
            if o is None and c is not None and cfo is not None and cfo != -100:
                o = c / (1.0 + cfo / 100.0)
            out[t] = {
                "open": None if o is None else round(float(o), 4),
                "close": None if c is None else round(float(c), 4),
                "change": chg,
                "cfo": cfo,
                "fpe": _num(raw.get(fsf.H_FPE)),
                "mcap": _num(raw.get(fsf.H_MCAP)),
                "adv": _num(raw.get(fsf.H_ADV)),
            }
    return out


def load_flatten_days(path: Path | None = None) -> list[dict]:
    src = path or LOOKBACK_JSON
    if not src.exists():
        return []
    obj = json.loads(src.read_text(encoding="utf-8"))
    rows = []
    for d in obj.get("daily") or []:
        date = str(d.get("date") or "")[:10]
        if not date:
            continue
        s = _num(d.get("score"))
        rows.append({
            "date": date,
            "score": s,
            "flatten_ok": bool(d.get("flatten_ok")),
            "hard_red": bool(
                d.get("hard_red")
                or (s is not None and float(s) <= HARD_RED)
            ),
            "tickers": [str(t).strip().upper() for t in (d.get("tickers") or [])
                        if str(t).strip()],
        })
    return rows


def trading_cal(flatten_days: list[dict], extra: list[str] | None = None) -> list[str]:
    dates = [d["date"] for d in flatten_days]
    for x in extra or []:
        if x not in dates:
            dates.append(x)
    return sorted(dates)


def is_liquid(row: dict) -> bool:
    mcap, adv = row.get("mcap"), row.get("adv")
    if mcap is None or adv is None:
        return False
    return mcap >= fsf.MF_MIN_MCAP and adv >= fsf.S_ADV_MIN


def spy_tape(bars_d: dict) -> str:
    spy = bars_d.get("SPY") or {}
    chg = spy.get("change")
    if chg is None:
        return "unknown"
    if chg > TAPE_EPS:
        return "up"
    if chg < -TAPE_EPS:
        return "down"
    return "flat"


def morning_regime(date: str) -> tuple[str, float | None]:
    return oa.weather_tape(date)


def high_fpe(fpe, cut: float) -> bool:
    return fpe is not None and fpe >= cut


def regime_ok(kind: str, morn: str, score: float | None) -> bool:
    if kind == "all":
        return True
    if kind == "morn_up":
        return morn == "up"
    if kind == "s_nonneg":
        return score is not None and score >= 0
    return True


def bar_at(bars: dict, date: str, ticker: str, which: str,
           carry: dict | None = None) -> float | None:
    hit = (bars.get(date) or {}).get(ticker) or {}
    px = hit.get(which)
    if px is not None:
        return float(px)
    if carry is None:
        return None
    last = carry.get((ticker, which))
    return None if last is None else float(last)


def unit_trade(entry: float, exit_px: float, fees: dict,
               notional: float = UNIT_NOTIONAL) -> dict | None:
    if entry is None or exit_px is None or entry <= 0:
        return None
    shares = int(notional // entry)
    if shares < 1:
        return None
    fee_in = order_fees(shares, entry, "buy", fees)
    fee_out = order_fees(shares, exit_px, "sell", fees)
    pnl = shares * (exit_px - entry) - fee_in - fee_out
    hit = pnl > 0
    return {
        "shares": shares,
        "entry": round(entry, 4),
        "exit": round(exit_px, 4),
        "fee_in": fee_in,
        "fee_out": fee_out,
        "pnl": round(pnl, 4),
        "hit": hit,
        "ret_pct": round(100.0 * (exit_px / entry - 1.0), 4),
    }


def leftover_book(cal: list[str], picks: dict[str, list[str]],
                  bars: dict, fees: dict, hold: int,
                  scores: dict[str, float | None],
                  live_ok: dict[str, bool] | None = None,
                  capital: float = CAPITAL) -> dict:
    """$10k leftover · sell first · min-hold · 09:30 open. Long only."""
    cash = float(capital)
    pos: dict[str, dict] = {}
    date_ix = {d: i for i, d in enumerate(cal)}
    trades: list[dict] = []
    daily: list[dict] = []
    carry: dict[tuple[str, str], float] = {}
    yday_eq = float(capital)

    def mark(date: str, which: str) -> float:
        tot = 0.0
        for lot in pos.values():
            px = bar_at(bars, date, lot["ticker"], which, carry)
            if px is None:
                px = float(lot.get("last_px") or lot["entry_px"])
            tot += lot["shares"] * px
        return tot

    for date in cal:
        chosen = list(picks.get(date) or [])
        s = scores.get(date)
        hard_red = s is not None and float(s) <= HARD_RED
        gate_ok = True if live_ok is None else bool(live_ok.get(date))
        sold, bought = [], []
        open_eq = cash + mark(date, "open")

        for t in list(pos):
            lot = pos[t]
            held = date_ix[date] - date_ix.get(lot["entry_date"], date_ix[date])
            dropped = t not in chosen
            px = bar_at(bars, date, t, "open", carry)
            if px is not None:
                lot["last_px"] = px
                carry[(t, "open")] = px
            if held < hold or not dropped:
                continue
            if px is None:
                continue
            fee = order_fees(lot["shares"], px, "sell", fees)
            cash += lot["shares"] * px - fee
            pnl = lot["shares"] * px - fee - lot["cost"]
            pos.pop(t)
            rec = {
                "date": date, "ticker": t, "side": "SELL",
                "shares": lot["shares"], "price": px, "fees": fee,
                "pnl": round(pnl, 4), "held": held,
            }
            trades.append(rec)
            sold.append(t)

        new = [t for t in chosen if t not in pos]
        if hard_red or not gate_ok:
            new = []
        if new and cash > 0:
            per = cash / len(new)
            for t in new:
                px = bar_at(bars, date, t, "open", None)
                if px is None or px <= 0:
                    continue
                shares = int(per // px)
                if shares < 1:
                    continue
                fee = order_fees(shares, px, "buy", fees)
                cost = shares * px + fee
                if cost > cash + 1e-6:
                    shares = int((cash - fee) // px) if px else 0
                    if shares < 1:
                        continue
                    fee = order_fees(shares, px, "buy", fees)
                    cost = shares * px + fee
                cash -= cost
                pos[t] = {
                    "ticker": t, "shares": shares, "entry_px": px,
                    "entry_date": date, "cost": cost, "last_px": px,
                }
                carry[(t, "open")] = px
                trades.append({
                    "date": date, "ticker": t, "side": "BUY",
                    "shares": shares, "price": px, "fees": fee,
                    "pnl": None, "held": 0,
                })
                bought.append(t)

        for lot in pos.values():
            cpx = bar_at(bars, date, lot["ticker"], "close", carry)
            if cpx is not None:
                lot["last_px"] = cpx
                carry[(lot["ticker"], "close")] = cpx
                carry[(lot["ticker"], "open")] = carry.get(
                    (lot["ticker"], "open"), cpx)
        equity = cash + mark(date, "close")
        day_pnl = equity - yday_eq
        daily.append({
            "date": date,
            "equity": round(equity, 4),
            "pnl": round(day_pnl, 4),
            "n_bought": len(bought),
            "n_sold": len(sold),
            "n_held": len(pos),
            "hard_red": hard_red,
            "gate_ok": gate_ok,
        })
        yday_eq = equity

    if pos:
        last = cal[-1]
        for t, lot in list(pos.items()):
            px = bar_at(bars, last, t, "close", carry) or lot["last_px"]
            fee = order_fees(lot["shares"], px, "sell", fees)
            cash += lot["shares"] * px - fee
            pnl = lot["shares"] * px - fee - lot["cost"]
            trades.append({
                "date": last, "ticker": t, "side": "SELL",
                "shares": lot["shares"], "price": px, "fees": fee,
                "pnl": round(pnl, 4), "held": hold, "forced": True,
            })
            pos.pop(t)
        equity = cash
        if daily:
            daily[-1]["equity"] = round(equity, 4)
            daily[-1]["pnl"] = round(equity - (daily[-2]["equity"] if len(daily) > 1 else capital), 4)

    pnls = [t["pnl"] for t in trades if t.get("side") == "SELL" and t.get("pnl") is not None]
    hits = [p for p in pnls if p > 0]
    final = daily[-1]["equity"] if daily else capital
    return {
        "capital": capital,
        "final_equity": round(final, 4),
        "pnl": round(final - capital, 4),
        "n_trades": len(pnls),
        "n_fills": len(trades),
        "hit": None if not pnls else round(len(hits) / len(pnls), 4),
        "mean_pnl": None if not pnls else round(sum(pnls) / len(pnls), 4),
        "daily": daily,
        "trades": trades,
    }


def max_drawdown(equities: list[float]) -> float:
    peak = None
    dd = 0.0
    for x in equities:
        if peak is None or x > peak:
            peak = x
        if peak and peak > 0:
            dd = min(dd, x / peak - 1.0)
    return round(dd, 4)


def concentration(daily_pnl: list[float]) -> dict:
    if not daily_pnl:
        return {"top1": None, "top2": None, "n_days": 0, "reject": False}
    ordered = sorted((abs(x) for x in daily_pnl), reverse=True)
    total = sum(abs(x) for x in daily_pnl)
    if total <= 1e-9:
        return {"top1": 0.0, "top2": 0.0, "n_days": len(daily_pnl), "reject": False}
    top1 = ordered[0] / total
    top2 = sum(ordered[:2]) / total
    return {
        "top1": round(top1, 4),
        "top2": round(top2, 4),
        "n_days": len(daily_pnl),
        "reject": top2 >= TOP2_REJECT,
    }


def _mean(xs: list) -> float | None:
    xs = [x for x in xs if x is not None]
    if not xs:
        return None
    return sum(xs) / len(xs)


def both_tape(avoided: list[dict], kept: list[dict],
              pnl_key: str = "pnl") -> dict:
    """Avoid both-tape = negative *excess vs same-tape peers*, not abs. loss.

    The whole liquid tape loses after $1k Futubull fees. Absolute mean<0
    on both tapes is the market, not an avoid edge.
    """
    out = {}
    for tape in ("up", "down", "flat", "unknown"):
        a = [r.get(pnl_key) for r in avoided if (r.get("tape") or "unknown") == tape]
        k = [r.get(pnl_key) for r in kept if (r.get("tape") or "unknown") == tape]
        a = [x for x in a if x is not None]
        k = [x for x in k if x is not None]
        am, km = _mean(a), _mean(k)
        out[tape] = {
            "n": len(a),
            "n_peer": len(k),
            "mean": None if am is None else round(am, 4),
            "mean_peer": None if km is None else round(km, 4),
            "xs": None if am is None or km is None else round(am - km, 4),
            "hit": None if not a else round(sum(1 for x in a if x > 0) / len(a), 4),
        }
    up, dn = out["up"], out["down"]
    up_ok = up["n"] >= MIN_TAPE and up["n_peer"] >= MIN_TAPE
    dn_ok = dn["n"] >= MIN_TAPE and dn["n_peer"] >= MIN_TAPE
    both = None
    if up_ok and dn_ok:
        both = (up.get("xs") is not None and up["xs"] < 0
                and dn.get("xs") is not None and dn["xs"] < 0)
    return {
        "tapes": out,
        "both_tape": both,
        "thin": not (up_ok and dn_ok),
    }


def walk_forward(rows: list[dict], pnl_key: str = "pnl") -> dict:
    dates = sorted({r["date"] for r in rows})
    if len(dates) < 4:
        return {"ok": None, "thin": True, "first": None, "last": None}
    mid = dates[len(dates) // 2]
    first = [r[pnl_key] for r in rows if r["date"] < mid and r.get(pnl_key) is not None]
    last = [r[pnl_key] for r in rows if r["date"] >= mid and r.get(pnl_key) is not None]
    def pack(xs):
        if len(xs) < WF_MIN:
            return {"n": len(xs), "mean": None, "thin": True}
        return {"n": len(xs), "mean": round(sum(xs) / len(xs), 4), "thin": False}
    a, b = pack(first), pack(last)
    ok = None
    if not a["thin"] and not b["thin"] and a["mean"] is not None and b["mean"] is not None:
        ok = (a["mean"] > 0 and b["mean"] > 0) or (a["mean"] < 0 and b["mean"] < 0)
    return {"ok": ok, "thin": a["thin"] or b["thin"], "first": a, "last": b, "split": mid}


def decide(ic: dict, book_base: dict, book_avoid: dict,
           delta_daily: list[float], n_avoided: int,
           book_kind: str = "leftover") -> dict:
    """PASS only if avoid is applicable and statistically profitable.

    ``book_kind=unit`` (Theme Radar): excess is mean(kept)−mean(all).
    A universe *sum* of skipped losers is sleight of hand — the whole
    liquid tape loses after $1k fees. Require peer-excess < 0 (IC) and
    avoided mean $ < 0 (skip is +EV vs buying that name).

    ``book_kind=leftover`` (flatten): excess is $10k book Δ vs baseline.
    """
    reasons = []
    verdict = "FAIL"
    xs = ic.get("xs")
    avoided_mean = ic.get("mean_pnl")
    conc = concentration(delta_daily)
    tape = ic.get("both_tape")
    thin = bool(ic.get("thin"))
    wf = ic.get("walk_forward") or {}

    if book_kind == "unit":
        base_mean = book_base.get("mean_pnl") if book_base else None
        avoid_mean = book_avoid.get("mean_pnl") if book_avoid else None
        if base_mean is None or avoid_mean is None:
            excess = None
        else:
            excess = round(avoid_mean - base_mean, 4)
    else:
        if book_base and book_avoid:
            excess = round(book_avoid["pnl"] - book_base["pnl"], 4)
        else:
            excess = None

    if n_avoided <= 0:
        reasons.append("veto never fired")
        verdict = "FAIL"
    elif thin and (ic.get("n") or 0) < MIN_UNIT:
        reasons.append("thin-n — cannot claim both-tape")
        verdict = "THIN"
    elif xs is not None and xs >= 0:
        reasons.append(
            f"avoided names beat peers after fees (xs ${xs:+.2f}) — not an avoid"
        )
        verdict = "FAIL"
    elif tape is False:
        reasons.append("peer-excess is not negative on both tapes")
        verdict = "FAIL"
    elif book_kind == "unit" and (avoided_mean is None or avoided_mean >= 0):
        reasons.append("avoided mean $ ≥ 0 after fees — skip is not +EV")
        verdict = "FAIL"
    elif excess is not None and excess <= 0:
        reasons.append(f"kept book ${excess:+.2f} vs baseline (not profitable)")
        verdict = "FAIL"
    elif conc.get("reject"):
        reasons.append(
            f"top-2 days share {100 * (conc.get('top2') or 0):.0f}% of |ΔP&L|"
        )
        verdict = "FAIL"
    elif tape is True and excess is not None and excess > 0 and not conc.get("reject"):
        if wf.get("thin"):
            reasons.append("peer-excess both-tape + book excess, walk-forward thin")
            verdict = "ITERATE"
        elif wf.get("ok") is False:
            reasons.append("walk-forward sign flip")
            verdict = "FAIL"
        else:
            reasons.append("peer-excess both-tape + fee-aware book + not 1–2 day")
            verdict = "PASS"
    elif tape is None and excess is not None and excess > 0:
        reasons.append("book $ up but both-tape thin — do not promote")
        verdict = "THIN" if thin else "ITERATE"
    else:
        reasons.append("inconclusive")
        verdict = "ITERATE"

    return {
        "verdict": verdict,
        "excess_usd": excess,
        "concentration": conc,
        "reasons": reasons,
        "book_kind": book_kind,
    }


def radar_unit_rows(cal: list[str], export_cal: list[str], bars: dict,
                    fees: dict, cut: float, regime: str) -> list[dict]:
    rows = []
    for d in cal:
        prior = prior_export(export_cal, d)
        today = bars.get(d) or {}
        yest = bars.get(prior) or {} if prior else {}
        if not today or not yest:
            continue
        tape = spy_tape(today)
        morn, ms = morning_regime(d)
        for t, yrow in yest.items():
            if t == "SPY" or not is_liquid(yrow):
                continue
            trow = today.get(t)
            if not trow:
                continue
            entry, exit_px = trow.get("open"), trow.get("close")
            tr = unit_trade(entry, exit_px, fees)
            if tr is None:
                continue
            flagged = high_fpe(yrow.get("fpe"), cut)
            apply = flagged and regime_ok(regime, morn, ms)
            rows.append({
                "date": d,
                "ticker": t,
                "tape": tape,
                "morning": morn,
                "morning_s": ms,
                "fpe": yrow.get("fpe"),
                "avoid": apply,
                "high_fpe": flagged,
                "pnl": tr["pnl"],
                "hit": tr["hit"],
                "ret_pct": tr["ret_pct"],
                "feature_export": prior,
            })
    return rows


def flatten_unit_rows(flatten_days: list[dict], cal: list[str],
                      export_cal: list[str], bars: dict, fees: dict,
                      hold: int, cut: float, regime: str,
                      live_only: bool) -> list[dict]:
    rows = []
    by = {d["date"]: d for d in flatten_days}
    for d in cal:
        day = by.get(d)
        if not day:
            continue
        if live_only and not day["flatten_ok"]:
            continue
        prior = prior_export(export_cal, d)
        yest = bars.get(prior) or {} if prior else {}
        today = bars.get(d) or {}
        tape = spy_tape(today)
        morn, ms = morning_regime(d)
        win = hold_window(cal, d, hold)
        if not win:
            continue
        exit_d = win[-1]
        for t in day["tickers"][:TOP_N]:
            entry = bar_at(bars, d, t, "open", None)
            exit_px = bar_at(bars, exit_d, t, "close", None)
            tr = unit_trade(entry, exit_px, fees)
            if tr is None:
                continue
            fpe = (yest.get(t) or {}).get("fpe")
            flagged = high_fpe(fpe, cut)
            apply = flagged and regime_ok(regime, morn, day.get("score"))
            rows.append({
                "date": d,
                "ticker": t,
                "tape": tape,
                "morning": morn,
                "morning_s": day.get("score"),
                "fpe": fpe,
                "avoid": apply,
                "high_fpe": flagged,
                "pnl": tr["pnl"],
                "hit": tr["hit"],
                "ret_pct": tr["ret_pct"],
                "feature_export": prior,
                "exit_date": exit_d,
            })
    return rows


def book_pair(cal: list[str], flatten_days: list[dict], bars: dict,
              fees: dict, hold: int, cut: float, regime: str,
              live_only: bool) -> tuple[dict, dict, list[float], int]:
    picks_base: dict[str, list[str]] = {}
    picks_avoid: dict[str, list[str]] = {}
    scores = {}
    live_ok = {}
    n_avoided = 0
    export_cal = export_dates()
    for day in flatten_days:
        d = day["date"]
        if d not in cal:
            continue
        scores[d] = day.get("score")
        live_ok[d] = day.get("flatten_ok")
        prior = prior_export(export_cal, d)
        yest = bars.get(prior) or {} if prior else {}
        morn, _ = morning_regime(d)
        kept = []
        raw = day["tickers"][:TOP_N]
        picks_base[d] = list(raw)
        gated = (not live_only) or bool(day.get("flatten_ok"))
        for t in raw:
            fpe = (yest.get(t) or {}).get("fpe")
            if (gated and high_fpe(fpe, cut)
                    and regime_ok(regime, morn, day.get("score"))):
                n_avoided += 1
                continue
            kept.append(t)
        picks_avoid[d] = kept
    live_map = live_ok if live_only else None
    base = leftover_book(cal, picks_base, bars, fees, hold, scores, live_map)
    avoid = leftover_book(cal, picks_avoid, bars, fees, hold, scores, live_map)
    by_b = {x["date"]: x["pnl"] for x in base["daily"]}
    delta = []
    for x in avoid["daily"]:
        delta.append(x["pnl"] - by_b.get(x["date"], 0.0))
    return base, avoid, delta, n_avoided


def ic_from_rows(rows: list[dict]) -> dict:
    avoided = [r for r in rows if r.get("avoid")]
    kept = [r for r in rows if not r.get("avoid")]
    peer = None if not kept else sum(r["pnl"] for r in kept) / len(kept)
    xs = None
    if avoided and peer is not None:
        xs = round(sum(r["pnl"] for r in avoided) / len(avoided) - peer, 4)
    hit_a = None if not avoided else round(
        sum(1 for r in avoided if r["hit"]) / len(avoided), 4)
    hit_k = None if not kept else round(
        sum(1 for r in kept if r["hit"]) / len(kept), 4)
    tape = both_tape(avoided, kept)
    wf = walk_forward(avoided)
    return {
        "n": len(avoided),
        "n_peer": len(kept),
        "n_all": len(rows),
        "hit": hit_a,
        "hit_peer": hit_k,
        "mean_pnl": None if not avoided else round(
            sum(r["pnl"] for r in avoided) / len(avoided), 4),
        "mean_peer": None if not kept else round(peer, 4),
        "xs": xs,
        "both_tape": tape["both_tape"],
        "thin": tape["thin"],
        "tapes": tape["tapes"],
        "walk_forward": wf,
    }


def score_sleeve(sleeve: dict, flatten_days: list[dict], cal: list[str],
                 export_cal: list[str], bars: dict, fees: dict,
                 cut: float, regime: str) -> dict:
    live = sleeve["universe"] == "flatten_live"
    hold = int(sleeve["hold_sessions"])
    if sleeve["universe"] == "radar":
        rows = radar_unit_rows(cal, export_cal, bars, fees, cut, regime)
        ic = ic_from_rows(rows)
        # Unit "book": sum of kept vs all (equal-notional). Avoid = drop flagged.
        all_pnl = sum(r["pnl"] for r in rows)
        keep_pnl = sum(r["pnl"] for r in rows if not r["avoid"])
        by_all: dict[str, float] = {}
        by_keep: dict[str, float] = {}
        for r in rows:
            by_all[r["date"]] = by_all.get(r["date"], 0.0) + r["pnl"]
            if not r["avoid"]:
                by_keep[r["date"]] = by_keep.get(r["date"], 0.0) + r["pnl"]
        dates = sorted(by_all)
        delta = [by_keep.get(d, 0.0) - by_all.get(d, 0.0) for d in dates]
        n_avoided = ic["n"]
        # Synthetic book stats so decide() can compare totals.
        book_base = {
            "pnl": round(all_pnl, 4),
            "n_trades": len(rows),
            "hit": None if not rows else round(
                sum(1 for r in rows if r["hit"]) / len(rows), 4),
            "mean_pnl": None if not rows else round(all_pnl / len(rows), 4),
            "final_equity": None,
            "daily": [{"date": d, "pnl": by_all[d]} for d in dates],
        }
        book_avoid = {
            "pnl": round(keep_pnl, 4),
            "n_trades": ic["n_peer"],
            "hit": ic["hit_peer"],
            "mean_pnl": ic["mean_peer"],
            "final_equity": None,
            "daily": [{"date": d, "pnl": by_keep.get(d, 0.0)} for d in dates],
        }
        # Max DD on cumulative unit P&L of the kept book.
        eq, run = [], 0.0
        for d in dates:
            run += by_keep.get(d, 0.0)
            eq.append(run)
        dd = max_drawdown([0.0] + eq) if eq else 0.0
    else:
        rows = flatten_unit_rows(
            flatten_days, cal, export_cal, bars, fees, hold, cut, regime, live)
        ic = ic_from_rows(rows)
        book_base, book_avoid, delta, n_avoided = book_pair(
            cal, flatten_days, bars, fees, hold, cut, regime, live)
        dd = max_drawdown([x["equity"] for x in book_avoid["daily"]])
        # leftover n_avoided counts skipped picks; IC n is unit-graded flagged.

    gate = decide(ic, book_base, book_avoid, delta, n_avoided,
                  book_kind=sleeve["book"])
    eq_a = [x.get("equity") for x in (book_avoid.get("daily") or []) if x.get("equity") is not None]
    return {
        "sleeve": sleeve["name"],
        "hold_sessions": hold,
        "universe": sleeve["universe"],
        "score_clock": sleeve["score_clock"],
        "cut": cut,
        "regime": regime,
        "ic": ic,
        "book_base": {k: book_base[k] for k in book_base if k != "trades"},
        "book_avoid": {k: book_avoid[k] for k in book_avoid if k != "trades"},
        "max_dd": dd,
        "n_avoided_picks": n_avoided,
        "gate": gate,
        "n_unit": len(rows),
    }


def load_bars(dates: list[str]) -> dict[str, dict[str, dict]]:
    return {d: load_finviz_bars(d) for d in dates}


def run(cut: float = 35.0, regime: str = "all",
        sweep: bool = True) -> dict:
    flatten_days = load_flatten_days()
    export_cal = export_dates()
    # Theme radar can grade any session with a Finviz tape after the first.
    radar_cal = [d for d in export_cal if d >= "2026-08-13" and d != "2026-08-29"]
    flat_cal = trading_cal(flatten_days)
    need = sorted(set(export_cal + radar_cal + flat_cal))
    bars = load_bars(need)
    fees = load_fees()
    cuts = FPE_SWEEP if sweep else (cut,)
    regimes = REGIME_SWEEP if sweep else (regime,)
    results = []
    for sl in SLEEVES:
        cal = radar_cal if sl["universe"] == "radar" else flat_cal
        for c in cuts:
            for rg in regimes:
                # Default KEEP first: 35 / all. Sweep the rest only to iterate.
                if not sweep and (c != cut or rg != regime):
                    continue
                rec = score_sleeve(sl, flatten_days, cal, export_cal,
                                   bars, fees, c, rg)
                results.append(rec)

    # Per sleeve: prefer PASS on default 35/all, else best honest verdict.
    rank = {"PASS": 0, "ITERATE": 1, "THIN": 2, "FAIL": 3}
    picked = []
    for sl in SLEEVES:
        pack = [r for r in results
                if r["sleeve"] == sl["name"] and r["cut"] == 35.0 and r["regime"] == "all"]
        default = pack[0] if pack else None
        others = [r for r in results if r["sleeve"] == sl["name"] and r is not default]
        chosen = default
        if default and default["gate"]["verdict"] != "PASS":
            better = [r for r in others if rank[r["gate"]["verdict"]] < rank[default["gate"]["verdict"]]]
            if better:
                better.sort(key=lambda r: (
                    rank[r["gate"]["verdict"]],
                    -(r["gate"].get("excess_usd") or -1e9),
                ))
                # Only promote a sweep sibling if it PASSes — else keep default.
                if better[0]["gate"]["verdict"] == "PASS":
                    chosen = better[0]
        if chosen:
            picked.append(chosen)

    stamp_local_5d_board(picked)
    stamp_local_5d_board(results)
    any_pass = any(r["gate"]["verdict"] == "PASS" for r in picked)
    return {
        "generated_note": "research only · live flatten_robust untouched",
        "fees": "futubull_fees.json via order_fees (same as paper_trade)",
        "fill": "09:30 Open · whole shares",
        "feature": "prior Elite Forward P/E only (date < D)",
        "any_pass": any_pass,
        "default_cut": 35.0,
        "local_5d_fpe_board": LOCAL_5D_FPE_BOARD,
        "fpe_clocks_open": ["theme_radar_1d"],
        "fpe_clocks_closed": ["flatten_h5"],
        "results": results,
        "picked": picked,
        "n_flatten_days": len(flatten_days),
        "n_radar_days": len(radar_cal),
    }


def _fmt(v, pct=False) -> str:
    if v is None:
        return "—"
    if pct:
        return f"{100 * v:.1f}%"
    if isinstance(v, float):
        return f"{v:+.2f}" if abs(v) < 1000 else f"{v:+,.2f}"
    return str(v)


def render(payload: dict) -> str:
    lines = [
        "# Overlay horizon backtest — KEEP FPE avoid",
        "",
        "Kid: We tried the expensive-sticker skip on every shopping list "
        "clock. Fees in. No peeking at today's homework. One lucky day "
        "does not count.",
        "",
        "_Research only. Live `flatten_robust` / `LIVE_POLICY` untouched. "
        "No merge without Cyrus._",
        "",
        "Fill = 09:30 `Open` · whole shares · "
        "[`futubull_fees.json`](../00_grounding/futubull_fees.json). "
        "Feature = last Elite `Forward P/E` with date **< D**. "
        "Theme Radar unit book = $1k notional per liquid name-day. "
        "Flatten leftover = $10k · sell first · min-hold · hard-red sit. "
        "`flatten_robust_shaped` = gated `flatten_live` hold-3 (not live wire).",
        "",
        "## Bar",
        "",
        "| check | rule |",
        "|---|---|",
        "| sleeve clock | tagged `hold_sessions` + leftover or 1d open→close |",
        "| leak | prior Elite only |",
        "| both-tape | avoided *peer-excess* < 0 on SPY-up **and** SPY-down, n≥20 each |",
        "| book | leftover: $10k Δ vs baseline. unit: mean(kept)−mean(all), and avoided mean < 0. Universe-sum of skipped losers is rejected. |",
        f"| concentration | top-2 |ΔP&L| share ≥ {100 * TOP2_REJECT:.0f}% → FAIL |",
        "| walk-forward | same-sign avoided-name $ on first/last half, else thin |",
        "",
        "## Picked KEEP (default FPE≥35 · all regimes, sweep only if PASS)",
        "",
        "| sleeve | hold | clock | n avoided | hit vs peer | xs $ | book Δ$ | max DD | top-2 day | both-tape | verdict |",
        "|---|---:|---|---:|---|---:|---:|---:|---:|---|---|",
    ]
    for r in payload.get("picked") or []:
        ic = r["ic"]
        g = r["gate"]
        conc = g.get("concentration") or {}
        hit = f"{_fmt(ic.get('hit'), True)} / {_fmt(ic.get('hit_peer'), True)}"
        lines.append(
            f"| `{r['sleeve']}` | {r['hold_sessions']} | {r['score_clock']} | "
            f"{ic.get('n')} | {hit} | {_fmt(ic.get('xs'))} | "
            f"{_fmt(g.get('excess_usd'))} | {_fmt(r.get('max_dd'), True)} | "
            f"{_fmt(conc.get('top2'), True)} | "
            f"{'YES' if ic.get('both_tape') is True else ('NO' if ic.get('both_tape') is False else 'thin-n')} | "
            f"**{g.get('verdict')}** |"
        )
    lines += [
        "",
        "### Why each call",
        "",
    ]
    for r in payload.get("picked") or []:
        why = "; ".join(r["gate"].get("reasons") or [])
        lines.append(f"- `{r['sleeve']}` **{r['gate']['verdict']}** — {why}.")

    board = payload.get("local_5d_fpe_board") or LOCAL_5D_FPE_BOARD
    fpe = board["fpe"]
    lines += [
        "",
        "## Theme Radar 5d FPE board (local) — `flatten_h5` closed",
        "",
        "Authoritative IC for a 5-session FPE Avoid. Leftover $ on "
        "`flatten_h5` is **not** a rescue. FPE is a **1d Theme Radar** "
        "signal; do not keep mining it on the 5d clock.",
        "",
        "| factor | clock | IC_up | Sign_up | n_up | IC_down | Sign_down | n_down | both-tape | verdict |",
        "|---|---|---:|---:|---:|---:|---:|---:|---|---|",
        f"| Forward P/E ≥ 35 | 5d Theme Radar | {fpe['ic_up']:+.3f} | "
        f"**{100 * fpe['sign_up']:.0f}%** ({fpe['sign_up_frac']}) | "
        f"{fpe['n_up']} | {fpe['ic_down']:+.3f} | "
        f"{100 * fpe['sign_down']:.0f}% | {fpe['n_down']} | "
        f"{'YES' if fpe['both_tape'] else '**NO** (up flips)'} | "
        f"**{fpe['verdict']}** |",
        "| d_RSI | 5d Theme Radar | — | — | — | — | — | — | — | "
        f"**{board['d_rsi']['verdict']}** |",
        "| d_Market Cap | 5d Theme Radar | — | — | — | — | — | — | — | "
        f"**{board['d_mcap']['verdict']}** |",
        "",
        f"{fpe['reason']} `flatten_h5` × FPE-avoid = **FAIL / do not wire.**",
        "",
        "## Full sweep (iterate knobs — do not cherrypick a FAIL default)",
        "",
        "| sleeve | FPE≥ | regime | n | xs $ | book Δ$ | both-tape | top-2 | verdict |",
        "|---|---:|---|---:|---:|---:|---|---:|---|",
    ]
    for r in payload.get("results") or []:
        ic, g = r["ic"], r["gate"]
        conc = g.get("concentration") or {}
        lines.append(
            f"| `{r['sleeve']}` | {r['cut']:g} | {r['regime']} | {ic.get('n')} | "
            f"{_fmt(ic.get('xs'))} | {_fmt(g.get('excess_usd'))} | "
            f"{'YES' if ic.get('both_tape') is True else ('NO' if ic.get('both_tape') is False else 'thin')} | "
            f"{_fmt(conc.get('top2'), True)} | {g.get('verdict')} |"
        )

    any_pass = payload.get("any_pass")
    lines += [
        "",
        "## Flatten skips (FPE≥35 · wish-list top 8 · not live tickets)",
        "",
        "IREN / HIMS / TNDM (08-13), BTBT (08-14), HNST (08-17), "
        "INSP / CRMD (08-24 hard-red sit; 08-25/26), ATRC (09-03/04). "
        "Zero avoided names on realized SPY-down. Live gate days "
        "08-20/21 are gold (AEM/KGC/…) — high-FPE 0. Leftover $ lift "
        "is wish-list HOLD only; baselines matched published blotters "
        "(h1 ~+$1.57k vs +$1.55k, h3 ~+$1.17k vs +$1.18k, "
        "h5 ~+$2.31k vs +$2.28k).",
        "",
        "## Elevate",
        "",
    ]
    if any_pass:
        lines.append(
            "A KEEP cleared. Reject elevates (CANSLIM / MF / cheap FPE / "
            "`total_score`) still need their **own** matching-hold re-mine "
            "before any bump. This run does not promote them."
        )
    else:
        lines.append(
            "**Keep did not clear.** Elevate mechanisms stay rejected. "
            "Do not bump CANSLIM, Magic Formula, cheap Forward P/E, or "
            "`total_score`."
        )
    lines += [
        "",
        "## Null / next smallest experiment",
        "",
    ]
    if any_pass:
        lines.append(
            "Not a null. Next: paper-only optional sticker on the **passing "
            "sleeve clock only**. Still no live wire."
        )
    else:
        lines.append(
            "Clean null. Theme Radar 1d percent fade (overlay xs −0.09) "
            "**does not survive** Futubull $ peer-excess (xs $+0.09; "
            "up-tape xs $+0.35). Local 5d FPE board **FAIL**s both-tape "
            "on `flatten_h5` (Sign_up **40%** 2/5 n=5; IC_down −0.131 "
            "n=10). Flatten leftover +$326 / +$456 / +$723 stays thin "
            "and is not a rescue. Live-shaped veto never fired. "
            "d_RSI / d_mcap 5d inconclusive. Sweep FPE 40/50 × "
            "morning-up / S≥0 did not clear the bar."
        )
        lines.append("")
        lines.append(
            "**Next smallest experiment:** FPE Avoid stays on the "
            "**1d Theme Radar clock only** (already fee-aware FAIL). "
            "Do **not** continue FPE / d_RSI / d_mcap mining on "
            "`flatten_h5`. Other Keep candidates only on their tagged "
            "sleeves. Do not drop the FPE cut or harvest GEV/CCJ lists. "
            "Elevate stays closed. No live wire."
        )
    lines += [
        "",
        "## Leak / live asserts",
        "",
        "- Feature export date < session D.",
        "- Same-day Change / Gap / RelVol never gate.",
        "- `LIVE_POLICY` not imported, not written.",
        "- `flatten_robust` not called.",
        "",
    ]
    return "\n".join(lines) + "\n"


def write(payload: dict | None = None) -> dict:
    payload = payload or run()
    stamp_local_5d_board(payload.get("picked") or [])
    stamp_local_5d_board(payload.get("results") or [])
    payload["local_5d_fpe_board"] = LOCAL_5D_FPE_BOARD
    payload["fpe_clocks_open"] = ["theme_radar_1d"]
    payload["fpe_clocks_closed"] = ["flatten_h5"]
    OUT_JSON.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    OUT_MD.write_text(render(payload), encoding="utf-8")
    return payload


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--no-sweep", action="store_true")
    args = ap.parse_args(argv)
    payload = run(sweep=not args.no_sweep)
    if args.write:
        write(payload)
        print(f"wrote {OUT_MD} · {OUT_JSON}")
    else:
        print(render(payload))
    picked = payload.get("picked") or []
    for r in picked:
        print(f"{r['sleeve']}: {r['gate']['verdict']} "
              f"n={r['ic']['n']} Δ$={r['gate'].get('excess_usd')}")


if __name__ == "__main__":
    main()
