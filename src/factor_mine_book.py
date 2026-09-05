"""Cash-accounted blotter for factor-mine recipes.

The first mine compounded equal-weight pick returns. That is **not**
the paper / live book:

  * $10k, whole shares, Futubull fees
  * leftover cash split equally among *new* names
  * sell first (cash freed), then buy
  * min-hold = recipe hold (trading sessions)
  * fill at the 09:30 open
  * hard-red morning S ≤ −3: sit, no new buys (same as flatten_robust)
  * skip if leftover cannot buy 1 share, or no open

This module is the book that the Action MD and dashboard blotter use.
It does not change live flatten_robust.
"""
from __future__ import annotations

import json
from pathlib import Path

from . import factor_mine as fm
from . import paper_trade as pt
from . import sleeve_merge as sm
from . import ticker_lookback as tl

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "03_scoreboard" / "factor_mine"
OUT_INDEX = ROOT / "03_scoreboard" / "FACTOR_MINE_ACTION.md"
DAILY_MD = ROOT / "01_daily" / "factor_mine_action.md"
HARD_RED = -3.0
UNIVERSES = ("auto", "union", "flatten", "probable", "yday_gainer", "ohlc_hot")
HOLDS = ("auto", "1", "3", "5")
GATES = (
    "auto", "none", "vol_g", "news_g", "white", "coil_off", "join_g",
    "last_green", "blue", "news_present", "join_present", "ab_g",
)
RANKS = ("auto", "none", "hot_score", "cond", "w_hot_cond", "w_hot_candle",
         "ret_5", "candle_score")
SIDES = ("auto", "long", "short")
TOP_NS = ("auto", "4", "8", "12")
EXITS = ("auto", "none", "alarm", "last_red", "news_bad")
BOOK_RULES = {
    "capital": fm.CAPITAL,
    "day_cap": 1.0,
    "hard_red": HARD_RED,
    "hard_red_no_new": True,
    "sell_first": True,
    "fill": "open",
}


def morning_s(regime: dict | None, date: str):
    g = (regime or {}).get(date) or {}
    v = g.get("predict_score")
    try:
        return None if v is None else float(v)
    except (TypeError, ValueError):
        return None


def load_regime() -> dict:
    try:
        return (sm.load_payload() or {}).get("regime") or {}
    except Exception:
        return {}


def _px(ticker: str, date: str, which: str, bars: dict | None):
    bar = fm._bar(ticker, date, bars)
    return fm._finite(bar.get(which)) or fm._finite(bar.get("close"))


def _tone_glyph(v: str) -> str:
    return {"good": "🟢", "neutral": "🟡", "bad": "🔴"}.get(str(v), "⬛")


def camera_stamp(boxes: dict | None) -> str:
    bits = []
    for k in fm.CAMERAS:
        v = str((boxes or {}).get(k) or "missing")
        if v == "missing":
            continue
        bits.append(f"{k}{_tone_glyph(v)}")
    return " ".join(bits) or "—"


def why_buy(rec: dict, row: dict) -> str:
    bits = [rec.get("note") or rec["name"]]
    req = rec.get("require") or {}
    if req:
        bits.append("gate " + ",".join(f"{k}={v}" for k, v in req.items()))
    if rec.get("rank"):
        bits.append(f"rank {rec['rank']}")
    src = ",".join(row.get("sources") or [])
    if src:
        bits.append(f"list {src}")
    if row.get("blue"):
        bits.append("🔵")
    if row.get("zero_red"):
        bits.append("⚪")
    ret5 = row.get("ohlc_ret_5")
    if ret5 is not None:
        bits.append(f"ret5={float(ret5):+.1f}")
    return "; ".join(bits)


def why_sell(ticker: str, held: int, min_hold: int, early: bool,
             exit_when: dict | None, dropped: bool) -> str:
    if early:
        if (exit_when or {}).get("alarm"):
            return f"exit 🚨 after {held} sess"
        if (exit_when or {}).get("last_red"):
            return f"exit last-red after {held} sess"
        if (exit_when or {}).get("news") == "bad":
            return f"exit news🔴 after {held} sess"
        return f"condition exit after {held} sess"
    if dropped:
        return f"dropped from list after {held} sess (min {min_hold})"
    return f"sold after {held} sess"


def recipes_from_action(*, universe="auto", hold="auto", gate="auto",
                        rank="auto", side="auto", top_n="auto",
                        exit="auto", auto_tweak=True) -> list[dict]:
    """Filter the systematic grid; auto dims stay swept.

    ``auto_tweak`` adds one-knob neighbors so a custom dropdown still
    explores nearby holds / gates / ranks without a second click.
    """
    base = fm.build_recipes()

    def gate_name(rec: dict) -> str:
        req = rec.get("require") or {}
        if not req:
            return "none"
        if req.get("vol") == "good" and len(req) == 1:
            return "vol_g"
        if req.get("news") == "good" and len(req) == 1:
            return "news_g"
        if req.get("zero_red"):
            return "white"
        if "ret_5_max" in req and "rvol_max" in req and not req.get("last_green"):
            return "coil_off"
        if req.get("join") == "good" and len(req) == 1:
            return "join_g"
        if req.get("last_green") and len(req) == 1:
            return "last_green"
        if req.get("blue") and len(req) == 1:
            return "blue"
        if req.get("news_present"):
            return "news_present"
        if req.get("join_present"):
            return "join_present"
        if req.get("ab") == "good" and len(req) == 1:
            return "ab_g"
        return "other"

    def exit_name(rec: dict) -> str:
        ex = rec.get("exit_when") or {}
        if ex.get("alarm"):
            return "alarm"
        if ex.get("last_red"):
            return "last_red"
        if ex.get("news") == "bad":
            return "news_bad"
        return "none"

    def keep(rec, *, uni, h, g, rk, sd, tn, ex) -> bool:
        if uni != "auto" and rec["universe"] != uni:
            return False
        if h != "auto" and int(rec["hold"]) != int(h):
            return False
        if g != "auto" and gate_name(rec) != g:
            return False
        if rk != "auto":
            have = rec.get("rank") or "none"
            if have != rk:
                return False
        if sd != "auto" and rec["side"] != sd:
            return False
        if tn != "auto" and int(rec["top_n"]) != int(tn):
            return False
        if ex != "auto" and exit_name(rec) != ex:
            return False
        return True

    kept = [r for r in base if keep(
        r, uni=universe, h=hold, g=gate, rk=rank, sd=side, tn=top_n, ex=exit)]
    if auto_tweak:
        extras = []
        if hold != "auto":
            extras += [r for r in base if keep(
                r, uni=universe, h="auto", g=gate, rk=rank, sd=side,
                tn=top_n, ex=exit)]
        if gate != "auto":
            extras += [r for r in base if keep(
                r, uni=universe, h=hold, g="auto", rk=rank, sd=side,
                tn=top_n, ex=exit)]
        if rank != "auto":
            extras += [r for r in base if keep(
                r, uni=universe, h=hold, g=gate, rk="auto", sd=side,
                tn=top_n, ex=exit)]
        seen = {r["name"] for r in kept}
        for r in extras:
            if r["name"] not in seen:
                kept.append(r)
                seen.add(r["name"])
    return kept or list(base)


def simulate_book(panel: dict, rec: dict, *, bars=None, fees=None,
                  regime=None, rules=None, start: str | None = None) -> dict:
    """Walk one recipe as a $10k paper sleeve. Sell first, then buy."""
    rules = {**BOOK_RULES, **(rules or {})}
    fees = fees if fees is not None else pt.load_fees()
    cal_all = list(panel.get("session_dates") or [])
    cal = [d for d in cal_all if not start or d >= start]
    by_date = panel.get("by_date") or {}
    row_index = {(r["date"], r["ticker"]): r for r in (panel.get("rows") or [])}
    cash = float(rules["capital"])
    pos: dict[str, dict] = {}
    trades: list[dict] = []
    skips: list[dict] = []
    daily: list[dict] = []
    date_ix = {d: i for i, d in enumerate(cal)}
    min_hold = int(rec["hold"])
    side = rec.get("side") or "long"
    day_cap = float(rules["day_cap"])

    def mark(date: str, which: str) -> float:
        tot = 0.0
        for lot in pos.values():
            px = _px(lot["ticker"], date, which, bars)
            if px is None:
                px = lot.get("last_px") or lot["entry_px"]
            tot += lot["shares"] * float(px)
        return tot

    for date in cal:
        s = morning_s(regime, date)
        hard_red = (rules.get("hard_red_no_new")
                    and s is not None and float(s) <= float(rules["hard_red"]))
        chosen = fm.pick_day(by_date.get(date) or [], rec)
        tset = {r["ticker"] for r in chosen}
        sold, bought, held_names = [], [], []
        day_why = []

        for t in list(pos):
            lot = pos[t]
            held = date_ix[date] - date_ix.get(lot["entry_date"], date_ix[date])
            row = row_index.get((date, t)) or {}
            early = fm.should_exit(row, rec.get("exit_when"))
            dropped = t not in tset
            if not early and not (dropped and held >= min_hold):
                if dropped and held < min_hold:
                    skips.append({
                        "date": date, "ticker": t, "kind": "min_hold",
                        "reason": f"dropped but min-hold {held}/{min_hold} sess — no sell",
                    })
                held_names.append(t)
                continue
            px = _px(t, date, "open", bars)
            if px is None:
                skips.append({"date": date, "ticker": t, "kind": "no_price",
                              "reason": "no 09:30 open — carry"})
                held_names.append(t)
                continue
            reason = why_sell(t, held, min_hold, early,
                              rec.get("exit_when"), dropped)
            fee = pt.order_fees(lot["shares"], px, "sell" if side == "long" else "buy", fees)
            if side == "long":
                proceeds = lot["shares"] * px - fee
                cash += proceeds
                pnl = proceeds - lot["cost"]
            else:
                cost_cover = lot["shares"] * px + fee
                cash -= cost_cover
                pnl = lot["notional"] - cost_cover - lot.get("fee_in", 0)
            pos.pop(t)
            rec_t = {
                "date": date, "ticker": t, "side": "SELL" if side == "long" else "COVER",
                "shares": lot["shares"], "price": round(px, 4), "fees": fee,
                "cash_after": round(cash, 2),
                "pnl": round(pnl, 2),
                "reason": reason,
                "held": held,
                "cameras": camera_stamp(row.get("boxes")),
            }
            trades.append(rec_t)
            sold.append(rec_t)
            day_why.append(f"SELL {t} ({reason})")

        new = [r for r in chosen if r["ticker"] not in pos]
        if hard_red:
            for r in new:
                skips.append({
                    "date": date, "ticker": r["ticker"], "kind": "hard_red",
                    "reason": f"hard-red S={s:+.2f} sit; no new buys",
                })
            if new:
                day_why.append(f"hard-red S={s:+.2f} sit; no new buys")
            new = []

        if new and cash > 0:
            room = cash * day_cap
            per = room / len(new)
            for row in new:
                t = row["ticker"]
                px = _px(t, date, "open", bars)
                reason = why_buy(rec, row)
                if px is None:
                    skips.append({"date": date, "ticker": t, "kind": "no_price",
                                  "reason": "no 09:30 open"})
                    continue
                shares = int(per // px)
                if shares < 1:
                    skips.append({
                        "date": date, "ticker": t, "kind": "cash",
                        "reason": f"leftover split {per:.2f} < 1 share @ {px:.2f}",
                    })
                    continue
                fee_side = "buy" if side == "long" else "sell"
                fee = pt.order_fees(shares, px, fee_side, fees)
                if side == "long":
                    cost = shares * px + fee
                    if cost > cash + 1e-6:
                        shares = int((cash - fee) // px) if px else 0
                        if shares < 1:
                            skips.append({
                                "date": date, "ticker": t, "kind": "cash",
                                "reason": f"cash {cash:.2f} < 1 share @ {px:.2f}",
                            })
                            continue
                        fee = pt.order_fees(shares, px, "buy", fees)
                        cost = shares * px + fee
                    cash -= cost
                    lot = {
                        "ticker": t, "shares": shares, "entry_px": px,
                        "entry_date": date, "cost": cost, "fee_in": fee,
                        "notional": shares * px, "last_px": px, "reason": reason,
                    }
                else:
                    notional = shares * px
                    eq_now = cash + mark(date, "open")
                    if eq_now < 2 * notional:
                        skips.append({
                            "date": date, "ticker": t, "kind": "cash",
                            "reason": f"short cover {2*notional:.0f} > equity {eq_now:.0f}",
                        })
                        continue
                    fee = pt.order_fees(shares, px, "sell", fees)
                    cash += notional - fee
                    lot = {
                        "ticker": t, "shares": shares, "entry_px": px,
                        "entry_date": date, "cost": fee, "fee_in": fee,
                        "notional": notional, "last_px": px, "reason": reason,
                    }
                pos[t] = lot
                rec_t = {
                    "date": date, "ticker": t,
                    "side": "BUY" if side == "long" else "SHORT",
                    "shares": shares, "price": round(px, 4), "fees": fee,
                    "cash_after": round(cash, 2),
                    "pnl": None,
                    "reason": reason,
                    "held": 0,
                    "cameras": camera_stamp(row.get("boxes")),
                }
                trades.append(rec_t)
                bought.append(rec_t)
                day_why.append(f"{rec_t['side']} {t} x{shares} @ {px:.2f}")
                held_names.append(t)
        for t in pos:
            if t not in held_names:
                held_names.append(t)

        stock = mark(date, "close")
        equity = cash + stock
        daily.append({
            "date": date,
            "s": None if s is None else round(s, 2),
            "hard_red": hard_red,
            "n": len(chosen),
            "cash": round(cash, 2),
            "stock": round(stock, 2),
            "equity": round(equity, 2),
            "bought": [b["ticker"] for b in bought],
            "sold": [x["ticker"] for x in sold],
            "held": list(pos.keys()),
            "skipped": [k["ticker"] for k in skips if k["date"] == date],
            "why": "; ".join(day_why) or (
                f"hard-red sit S={s:+.2f}" if hard_red else
                ("hold " + ",".join(pos.keys()) if pos else "flat cash")
            ),
            "made_money": False,
        })

    for i, d in enumerate(daily):
        prev = float(rules["capital"]) if i == 0 else daily[i - 1]["equity"]
        d["mean"] = None if prev <= 0 else round(100.0 * (d["equity"] / prev - 1.0), 4)
        d["made_money"] = bool(d["mean"] is not None and d["mean"] > 0)

    equity = [float(rules["capital"])] + [d["equity"] for d in daily]
    total_ret = round(100.0 * (equity[-1] / rules["capital"] - 1.0), 3) if equity else 0.0
    closed = [t for t in trades if t.get("pnl") is not None]
    wins = [t for t in closed if (t.get("pnl") or 0) > 0]
    losses = [t for t in closed if (t.get("pnl") or 0) < 0]
    return {
        "name": rec["name"],
        "rules": {k: rules[k] for k in BOOK_RULES},
        "cash": round(cash, 2),
        "n_open": len(pos),
        "open": [
            {"ticker": t, "shares": p["shares"], "entry_date": p["entry_date"],
             "entry_px": p["entry_px"], "reason": p.get("reason")}
            for t, p in pos.items()
        ],
        "n_trades": len(trades),
        "n_skips": len(skips),
        "n_closed": len(closed),
        "n_wins": len(wins),
        "n_losses": len(losses),
        "realized": round(sum(t.get("pnl") or 0 for t in closed), 2),
        "total_ret_pct": total_ret,
        "final_equity": equity[-1] if equity else rules["capital"],
        "equity": [round(x, 2) for x in equity],
        "daily": daily,
        "trades": trades,
        "skips": skips,
        "win_rate": None if not closed else round(len(wins) / len(closed), 4),
        "avg_win_pct": None if not wins else round(
            sum(100 * t["pnl"] / max((t.get("price") or 1) * t["shares"], 1)
                for t in wins) / len(wins), 3),
        "avg_loss_pct": None if not losses else round(
            sum(100 * t["pnl"] / max((t.get("price") or 1) * t["shares"], 1)
                for t in losses) / len(losses), 3),
    }


def replay_starts(panel: dict, rec: dict, **kw) -> list[dict]:
    cal = list(panel.get("session_dates") or [])
    out = []
    for start in cal:
        book = simulate_book(panel, rec, start=start, **kw)
        out.append({
            "start": start,
            "return_pct": book["total_ret_pct"],
            "made_money": book["total_ret_pct"] > 0,
            "n_sessions": len(book.get("daily") or []),
            "final_equity": book["final_equity"],
        })
    return out


def attach_book(stats: dict, book: dict, starts: list[dict]) -> dict:
    """Overwrite money fields with the cash book; keep pick capture stats."""
    days = [d for d in book.get("daily") or [] if d.get("mean") is not None]
    n_green = sum(1 for s in starts if s["made_money"])
    start_rets = [s["return_pct"] for s in starts]
    median_start = (round(float(sorted(start_rets)[len(start_rets) // 2]), 3)
                    if start_rets else None)
    means = [d["mean"] for d in days]
    pothole_pct = max(means) if means else None
    pothole_date = None
    if means:
        pothole_date = next(d["date"] for d in days if d["mean"] == pothole_pct)
    reliable = (
        (stats.get("n_graded") or 0) >= fm.MIN_GRADED
        and len(starts) >= fm.MIN_STARTS
        and len(days) >= fm.MIN_DAYS
    )
    stats = dict(stats)
    stats["signal_ret_pct"] = stats.get("total_ret_pct")
    stats["book"] = True
    stats["total_ret_pct"] = book["total_ret_pct"]
    stats["final_equity"] = book["final_equity"]
    stats["equity"] = book["equity"]
    stats["daily"] = book["daily"]
    stats["starts"] = starts
    stats["start_n"] = len(starts)
    stats["start_green"] = n_green
    stats["start_rate"] = None if not starts else round(n_green / len(starts), 4)
    stats["median_start_pct"] = median_start
    stats["pothole_date"] = pothole_date
    stats["pothole_pct"] = None if pothole_pct is None else round(float(pothole_pct), 3)
    stats["profitable_day_rate"] = None if not days else round(
        sum(1 for d in days if d["made_money"]) / len(days), 4)
    stats["book_win_rate"] = book.get("win_rate")
    stats["book_n_trades"] = book.get("n_trades")
    stats["book_n_skips"] = book.get("n_skips")
    stats["book_realized"] = book.get("realized")
    if book.get("avg_win_pct") is not None:
        stats["avg_win_pct"] = book["avg_win_pct"]
    if book.get("avg_loss_pct") is not None:
        stats["avg_loss_pct"] = book["avg_loss_pct"]
    stats["reliable"] = reliable
    stats["effectiveness"] = fm._effectiveness(
        stats.get("win_rate"), stats.get("profitable_day_rate"),
        stats.get("start_rate"), stats.get("gainer_rate"),
        stats.get("loser_rate"), stats.get("payoff"),
        stats.get("total_ret_pct"), median_start, pothole_pct, reliable,
    )
    return stats


def render_recipe_md(rec: dict, stats: dict, book: dict) -> str:
    lines = [
        f"# Factor mine action — `{rec['name']}`",
        "",
        f"_Book rules: $10k · whole shares · Futubull fees · leftover cash "
        f"split on new names · sell first · min-hold **{rec['hold']}** sessions · "
        f"fill 09:30 open · hard-red S≤{HARD_RED:g} sit. "
        f"Live `flatten_robust` is not changed._",
        "",
        f"Side **{rec['side']}** · universe `{rec['universe']}` · "
        f"top {rec['top_n']} · rank `{rec.get('rank') or 'list'}` · "
        f"{rec.get('note') or ''}",
        "",
        f"Cash book **{stats.get('total_ret_pct'):+.2f}%** "
        f"(${stats.get('final_equity'):,.0f}) · "
        f"signal-only (no cash/fees) was "
        f"{stats.get('signal_ret_pct'):+.2f}%. "
        f"Starts YES **{stats.get('start_green')}/{stats.get('start_n')}**. "
        f"Fills {book.get('n_trades')} · skips {book.get('n_skips')} · "
        f"realized ${book.get('realized'):+.2f}.",
        "",
        "## Each session",
        "",
        "| Date | S | Hard-red | Cash | Stock | Equity | Bought | Sold | Skipped | Why |",
        "|---|---:|---|---:|---:|---:|---|---|---|---|",
    ]
    for d in book.get("daily") or []:
        s = d.get("s")
        lines.append(
            f"| {d['date']} | {('—' if s is None else f'{s:+.2f}')} | "
            f"{'yes' if d.get('hard_red') else 'no'} | "
            f"${d['cash']:,.2f} | ${d['stock']:,.2f} | ${d['equity']:,.2f} | "
            f"{', '.join(d.get('bought') or []) or '—'} | "
            f"{', '.join(d.get('sold') or []) or '—'} | "
            f"{', '.join(d.get('skipped') or []) or '—'} | "
            f"{str(d.get('why') or '—').replace('|', '/')} |"
        )
    lines += [
        "",
        "## Fills (what was bought / sold)",
        "",
        "| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Why | Cameras |",
        "|---|---|---|---:|---:|---:|---:|---:|---|---|",
    ]
    for t in book.get("trades") or []:
        pnl = t.get("pnl")
        lines.append(
            f"| {t['date']} 09:30 ET | **{t['side']}** | `{t['ticker']}` | "
            f"{t['shares']} | ${t['price']:.2f} | ${t['fees']:.2f} | "
            f"{'—' if pnl is None else f'${pnl:+.2f}'} | "
            f"${t['cash_after']:,.2f} | "
            f"{str(t.get('reason') or '—').replace('|', '/')} | "
            f"{t.get('cameras') or '—'} |"
        )
    skips = book.get("skips") or []
    if skips:
        lines += [
            "",
            "## Not taken",
            "",
            "| Date | Ticker | Kind | Why |",
            "|---|---|---|---|",
        ]
        for k in skips:
            lines.append(
                f"| {k['date']} | `{k['ticker']}` | {k['kind']} | "
                f"{str(k.get('reason') or '—').replace('|', '/')} |"
            )
    open_pos = book.get("open") or []
    if open_pos:
        lines += [
            "",
            "## Still open (marked at last close)",
            "",
            "| Ticker | Shares | Entry | Why |",
            "|---|---:|---|---|",
        ]
        for p in open_pos:
            lines.append(
                f"| `{p['ticker']}` | {p['shares']} | "
                f"{p['entry_date']} @ ${p['entry_px']:.2f} | "
                f"{str(p.get('reason') or '—').replace('|', '/')} |"
            )
    return "\n".join(lines) + "\n"


def write_action_mds(payload: dict, stats: list[dict], books: dict,
                     featured: list[str]) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    recs = {r["name"]: r for r in (payload.get("recipes") or [])}
    index = [
        f"# Factor mine action — {payload.get('from_date')} → {payload.get('to_date')}",
        "",
        "Cash-accounted blotters for the leak-free 09:30 recipes. "
        "Same rules as the paper sleeve book: **$10k, whole shares, "
        "Futubull fees, leftover cash split, sell first, min-hold, "
        "09:30 open fills, hard-red S≤−3 sit**. "
        "Signal-only percentages in the research table are **not** fills.",
        "",
        f"Phone: `dashboard/factor-mine/index.html`. "
        f"Sister: [flatten lookback](../dashboard/flatten-lookback/) · "
        f"[sleeve merge](../dashboard/sleeve-merge/).",
        "",
        "Live `flatten_robust` is not changed.",
        "",
        "## Featured books",
        "",
        "| Strategy | Book % | Signal-only % | Starts YES | Fills | Skips | MD |",
        "|---|---:|---:|---:|---:|---:|---|",
    ]
    for name in featured:
        s = next((x for x in stats if x["name"] == name), None)
        b = books.get(name) or {}
        if not s:
            continue
        md_name = f"{name}.md"
        (OUT_DIR / md_name).write_text(
            render_recipe_md(recs.get(name) or {"name": name, "hold": s.get("hold"),
                                                "side": s.get("side"),
                                                "universe": s.get("universe"),
                                                "top_n": s.get("top_n"),
                                                "rank": s.get("rank"),
                                                "note": s.get("note")},
                             s, b),
            encoding="utf-8",
        )
        index.append(
            f"| `{name}` | {s.get('total_ret_pct'):+.2f} | "
            f"{s.get('signal_ret_pct'):+.2f} | "
            f"{s.get('start_green')}/{s.get('start_n')} | "
            f"{b.get('n_trades') or 0} | {b.get('n_skips') or 0} | "
            f"[{md_name}](factor_mine/{md_name}) |"
        )
    OUT_INDEX.write_text("\n".join(index) + "\n", encoding="utf-8")
    DAILY_MD.write_text(
        OUT_INDEX.read_text(encoding="utf-8"), encoding="utf-8")
