"""09:30 BUY/SELL on the liquid-mover universe — not top gainers.

For every session since the dashboard start (2026-08-13):

  * take names knowable at 09:30: last completed Finviz tape
    with mcap > $100M, ADV > 500K, and ATR% ≥ 2.5
  * paint ticker lookback (cameras / setups / hall pass)
  * stamp BUY / SELL / NO BUY / HOLD
  * grade on forward +1d / +3d / +1w (never same-day Change%)

Same-day Change% does not pick the universe. A name does not have
to close as a gainer. Quiet large-caps that compressed (late-August
CVS) fail the ATR% gate on last night's tape.

CLI: python -m src.mover_lookback_action --write
"""
from __future__ import annotations

import argparse
import html
import json
from collections import Counter
from datetime import datetime
from pathlib import Path

from . import gainer_asof as ga
from . import gainer_lookback_action as gla
from . import lookback_action as act
from . import ticker_lookback as tl
from . import ticker_lookback_run as run
from . import ticker_lookback_setups as setups

ROOT = Path(__file__).resolve().parent.parent
OUT_MD = ROOT / "03_scoreboard" / "MOVER_LOOKBACK_ACTION.md"
OUT_JSON = ROOT / "03_scoreboard" / "mover_lookback_action.json"
OUT_HTML = ROOT / "dashboard" / "mover-lookback" / "index.html"
DAILY_MD = ROOT / "01_daily" / "mover_lookback_action.md"

START = ga.START
HORIZONS = act.HORIZONS
CALLED = ("BUY", "SELL", "NO BUY")


def universe_tape(sess: dict) -> str:
    """Finviz date knowable at 09:30 on this session.

    Prefer the prior session's export. If that file is missing (first
    tape in the archive), use this session's file — only then.
    """
    prior = sess.get("prior_date")
    if prior and (tl.EXPORT_DIR / f"finviz_{prior}.csv").is_file():
        return prior
    return sess["date"]


def collect_movers(from_date: str = START, to_date: str | None = None,
                   min_mcap_m: float = tl.RANDOM_MIN_MCAP_M,
                   min_avg_vol_k: float = tl.RANDOM_MIN_AVG_VOL_K,
                   min_atr_pct: float = tl.MIN_ATR_PCT) -> dict:
    idx = tl.build_index()
    sessions = [
        s for s in idx["sessions"]
        if s["date"] >= from_date and (not to_date or s["date"] <= to_date)
    ]
    by_date: dict[str, list[str]] = {}
    tape_of: dict[str, str] = {}
    keys: set[tuple[str, str]] = set()
    names: set[str] = set()
    for sess in sessions:
        date = sess["date"]
        tape = universe_tape(sess)
        uni = tl.liquid_universe(
            asof=tape, min_mcap_m=min_mcap_m,
            min_avg_vol_k=min_avg_vol_k, min_atr_pct=min_atr_pct,
        )
        by_date[date] = uni
        tape_of[date] = tape
        for t in uni:
            keys.add((date, t))
            names.add(t)
    return {
        "from_date": from_date,
        "to_date": to_date or (sessions[-1]["date"] if sessions else from_date),
        "min_mcap_m": min_mcap_m,
        "min_avg_vol_k": min_avg_vol_k,
        "min_atr_pct": min_atr_pct,
        "n_sessions": len(sessions),
        "n_mover_days": len(keys),
        "n_tickers": len(names),
        "by_date": by_date,
        "tape_of": tape_of,
        "keys": keys,
        "tickers": sorted(names),
        "session_dates": [s["date"] for s in sessions],
    }


def paint_movers(meta: dict, from_date: str, to_date: str | None) -> dict:
    names = meta["tickers"]
    if not names:
        return {"generated_at": datetime.now(tl.ET).isoformat(), "names": []}
    payload = run.scan_tickers(names, from_date=from_date, to_date=to_date)
    setups.attach_setups(payload)
    return payload


def day_change_pct(ticker: str, date: str, dates: list[str] | None = None):
    """Prior close → this session close (outcome). Not an Action input."""
    dates = dates if dates is not None else tl.session_dates()
    prior = None
    key = str(date or "")[:10]
    try:
        i = dates.index(key)
        if i > 0:
            prior = dates[i - 1]
    except ValueError:
        prior = None
    cur = tl.session_bar(ticker, date)
    prev = tl.session_bar(ticker, prior) if prior else {}
    c, p = cur.get("close"), prev.get("close")
    if not c or not p:
        return None
    return round(100.0 * (float(c) / float(p) - 1.0), 3)


def _stamp_row(row: dict, params: dict, dates: list[str]) -> dict:
    packed = act.action_call(row, params=params)
    row["action_call"] = packed["action"]
    row["action_reason"] = packed["reason"]
    row["action_stamp"] = act.session_stamp(row.get("date"), act.OPEN_CLOCK)
    row["action_label"] = act.format_action(packed["action"], row.get("date"))
    row["hits"] = act.grade_call(packed["action"], gla._fwd(row))
    if not row.get("session_bar"):
        row["session_bar"] = tl.session_bar(row.get("ticker"), row.get("date"))
    if not row.get("horizon_dates"):
        row["horizon_dates"] = tl.horizon_dates(row.get("date"), dates)
    if not row.get("condition"):
        row["condition"] = tl.general_condition(row.get("boxes") or {})
    row["cond_tally"] = act.cond_tally(row)
    row["conviction"] = act.conviction(row, packed)
    if row.get("day_change") is None:
        row["day_change"] = day_change_pct(
            row.get("ticker") or "", row.get("date") or "", dates)
    return row


# ------------------------------------------------------- regime context --
GENERAL_DIR = ROOT / "01_daily" / "general"


def _predict_snapshot(date: str):
    """Premarket general predict for `date` (published ~05:55 ET, knowable
    at the 09:30 open). Returns (direction, score) or (None, None)."""
    import re
    p = GENERAL_DIR / f"{date}_predict.md"
    if not p.is_file():
        return None, None
    try:
        txt = p.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None, None
    m = re.search(r"Prediction:\s*(UP|DOWN|FLAT).*?total score\s*(-?[\d.]+)",
                  txt)
    if not m:
        m2 = re.search(r"Prediction:\s*(UP|DOWN|FLAT)", txt)
        return (m2.group(1), None) if m2 else (None, None)
    return m.group(1), float(m.group(2))


def _spy_closes(dates: list[str]) -> dict[str, float]:
    """SPY close per session from the OHLC store; yfinance fallback."""
    out = {}
    for d in dates:
        c = (tl.session_bar("SPY", d) or {}).get("close")
        if c:
            out[d] = float(c)
    if not out:
        try:
            import yfinance as yf
            df = yf.download("SPY", start=dates[0], end=None,
                             progress=False, auto_adjust=True)
            if df is not None and not df.empty:
                closes = df["Close"]
                for d in dates:
                    try:
                        v = closes.loc[:d].iloc[-1]
                        out[d] = float(v.iloc[0] if hasattr(v, "iloc") else v)
                    except Exception:
                        continue
        except Exception:
            pass
    return out


def regime_context(dates: list[str]) -> dict[str, dict]:
    """Per-session regime knowable at 09:30 ET:
    spy_down_streak = consecutive down SPY closes BEFORE that session;
    predict dir/score = that morning's premarket general predict.
    """
    closes = _spy_closes(dates)
    ctx = {}
    prev_close = None
    streak = 0
    for d in sorted(dates):
        pdir, pscore = _predict_snapshot(d)
        ctx[d] = {
            "spy_down_streak": streak,          # strictly prior sessions
            "predict_dir": pdir,
            "predict_score": pscore,
            "spy_close": closes.get(d),
        }
        c = closes.get(d)
        if c is not None:
            streak = streak + 1 if (prev_close is not None
                                    and c < prev_close) else 0
            prev_close = c
    return ctx


# ------------------------------------------------------------ leaderboard --
def _signed_ret(r: dict, h: str):
    v = (gla._fwd(r) or {}).get(h)
    try:
        v = None if v is None else float(v)
    except (TypeError, ValueError):
        return None
    if v is None:
        return None
    return -v if r.get("action_call") == "SELL" else v


def _ret_stats(rets: list[float]) -> dict:
    if not rets:
        return {"n": 0, "hit": None, "mean": None}
    return {
        "n": len(rets),
        "hit": round(sum(1 for x in rets if x > 0) / len(rets), 3),
        "mean": round(sum(rets) / len(rets), 3),
    }


def build_leaderboard(called_rows: list[dict], top_n: int = 10) -> dict:
    """Per-day top-N calls by conviction vs all calls — the tradeable cut."""
    by_day: dict[str, list[dict]] = {}
    for r in called_rows:
        by_day.setdefault(r.get("date"), []).append(r)
    days = []
    agg: dict[str, dict[str, list]] = {
        s: {"top": {h: [] for h in act.HORIZONS},
            "all": {h: [] for h in act.HORIZONS}}
        for s in ("BUY", "SELL")
    }
    for date in sorted(by_day):
        for side in ("BUY", "SELL"):
            calls = [r for r in by_day[date] if r.get("action_call") == side]
            if not calls:
                continue
            calls.sort(key=lambda r: -(r.get("conviction") or 0))
            top = calls[:top_n]
            days.append({
                "date": date, "side": side, "n_calls": len(calls),
                "top": [{
                    "ticker": r.get("ticker"),
                    "conviction": r.get("conviction"),
                    "reason": r.get("action_reason"),
                    "cond": r.get("cond_tally"),
                    "fwd": {h: (gla._fwd(r) or {}).get(h)
                            for h in act.HORIZONS},
                    "hits": r.get("hits"),
                } for r in top],
            })
            for h in act.HORIZONS:
                agg[side]["top"][h] += [x for x in
                                        (_signed_ret(r, h) for r in top)
                                        if x is not None]
                agg[side]["all"][h] += [x for x in
                                        (_signed_ret(r, h) for r in calls)
                                        if x is not None]
    summary = {
        side: {
            "top": {h: _ret_stats(agg[side]["top"][h]) for h in act.HORIZONS},
            "all": {h: _ret_stats(agg[side]["all"][h]) for h in act.HORIZONS},
        } for side in ("BUY", "SELL")
    }
    return {"top_n": top_n, "days": days, "summary": summary}


def walk(from_date: str = START, to_date: str | None = None,
         preset: str | None = None) -> dict:
    meta = collect_movers(from_date=from_date, to_date=to_date)
    payload = paint_movers(meta, meta["from_date"], meta["to_date"])
    by_card = gla._day_index(payload)
    mover_rows = []
    for date, tickers in meta["by_date"].items():
        tape = (meta.get("tape_of") or {}).get(date)
        for t in tickers:
            card = by_card.get((date, t))
            if not card or card.get("class") == "no_data":
                continue
            rec = dict(card)
            rec["universe_tape"] = tape
            mover_rows.append(rec)

    default_name = preset or act.default_preset_name()
    regime = regime_context(meta["session_dates"])
    sweeps = {}
    for name in act.PRESETS:
        params = act.preset_params(name)
        params["_regime"] = regime
        sweeps[name] = {
            "mover_days": gla._score_rows([dict(r) for r in mover_rows], params),
        }

    chosen = act.preset_params(default_name)
    chosen["_regime"] = regime
    dates = meta["session_dates"]
    for row in mover_rows:
        _stamp_row(row, chosen, dates)

    called_rows = [r for r in mover_rows if r.get("action_call") in CALLED]
    leaderboard = build_leaderboard(called_rows)
    daily = []
    by_day: dict[str, list[dict]] = {}
    for r in mover_rows:
        by_day.setdefault(r.get("date"), []).append(r)
    for date in meta["session_dates"]:
        rows = by_day.get(date) or []
        counts = Counter(r.get("action_call") for r in rows)
        scored = gla._score_rows([dict(r) for r in rows], chosen)
        daily.append({
            "date": date,
            "tape": (meta.get("tape_of") or {}).get(date),
            "n": len(rows),
            "n_buy": counts.get("BUY", 0),
            "n_sell": counts.get("SELL", 0),
            "n_no_buy": counts.get("NO BUY", 0),
            "n_hold": counts.get("HOLD", 0),
            "catch_1d": (scored.get("catch") or {}).get("1d"),
            "buy_1d": (scored.get("buy_catch") or {}).get("1d"),
            "sell_1d": (scored.get("sell_catch") or {}).get("1d"),
            "pnl_1d": (scored.get("mean_pnl") or {}).get("1d"),
        })

    recall_buy = sum(1 for r in mover_rows if r.get("action_call") == "BUY")
    return {
        "generated_at": datetime.now(tl.ET).isoformat(),
        "asof": "09:30_et",
        "method": "mover_lookback_action",
        "from_date": meta["from_date"],
        "to_date": meta["to_date"],
        "min_mcap_m": meta["min_mcap_m"],
        "min_avg_vol_k": meta["min_avg_vol_k"],
        "min_atr_pct": meta["min_atr_pct"],
        "n_sessions": meta["n_sessions"],
        "n_mover_days": len(mover_rows),
        "n_tickers": meta["n_tickers"],
        "n_called": len(called_rows),
        "preset": default_name,
        "recall_buy": recall_buy,
        "recall_buy_rate": (
            None if not mover_rows else round(recall_buy / len(mover_rows), 3)
        ),
        "sweeps": sweeps,
        "chosen": sweeps.get(default_name) or {},
        "daily": daily,
        "called_rows": called_rows,
        "regime": regime,
        "leaderboard": leaderboard,
        "lookback": {
            "generated_at": payload.get("generated_at"),
            "n_names": len(payload.get("names") or []),
        },
        "session_dates": meta["session_dates"],
        "tape_of": meta["tape_of"],
        "by_date_n": {d: len(v) for d, v in meta["by_date"].items()},
    }


def _row_md(r: dict) -> str:
    hits = r.get("hits") or {}
    bar = r.get("session_bar") or {}
    hz = r.get("horizon_dates") or {}
    fwd = gla._fwd(r) or {}
    day_chg = r.get("day_change")
    return (
        f"| {act.session_stamp(r.get('date'), act.OPEN_CLOCK)} | "
        f"{gla.hits_cell(hits)} | `{r.get('ticker')}` | "
        f"{act.format_price(bar.get('close'), r.get('date'), act.CLOSE_CLOCK)} | "
        f"{act.format_price(bar.get('open'), r.get('date'), act.OPEN_CLOCK)} | "
        f"{gla._pct_text(day_chg, r.get('date'), act.CLOSE_CLOCK)} | "
        f"{gla._oc_text(bar.get('close_open_pct'), r.get('date'))} | "
        f"{r.get('cond_tally') or act.cond_tally(r)} | "
        f"**{r.get('action_label') or act.format_action(r.get('action_call'), r.get('date'))}** | "
        f"{str(r.get('action_reason') or '—').replace('|', '/')} | "
        f"{setups.setup_labels(r) or '—'} | "
        f"{gla._pct_text(fwd.get('1d'), hz.get('1d'), act.CLOSE_CLOCK)} | "
        f"{gla._pct_text(fwd.get('3d'), hz.get('3d'), act.CLOSE_CLOCK)} | "
        f"{gla._pct_text(fwd.get('1w'), hz.get('1w'), act.CLOSE_CLOCK)} |"
    )


def render_markdown(payload: dict) -> str:
    sweeps = payload.get("sweeps") or {}
    chosen = payload.get("preset") or "featured"
    L = [
        "# Mover lookback action",
        "",
        f"_Generated {payload.get('generated_at')}_",
        "",
        f"Universe: names knowable at **09:30 ET** — last completed "
        f"Finviz tape with mcap > ${payload.get('min_mcap_m'):.0f}M, "
        f"ADV > {payload.get('min_avg_vol_k'):.0f}K, "
        f"ATR% ≥ {payload.get('min_atr_pct')}. "
        f"**{payload.get('from_date')}** → **{payload.get('to_date')}**. "
        f"{payload.get('n_mover_days')} mover-days · "
        f"{payload.get('n_tickers')} names · "
        f"{payload.get('n_sessions')} sessions.",
        "",
        "**Not top gainers.** Same-day Change% does not pick the list. "
        "Action is that date **09:30 ET** from cameras / setups / hall "
        "pass only. +1d / +3d / +1w are later **16:00 ET** closes. "
        "Catch = BUY and the forward move is up, or SELL and it is down. "
        "**pnl 1d** is signed (BUY keeps +1d, SELL flips it).",
        "",
        f"Default preset **`{chosen}`**. BUY rate: "
        f"**{gla._pct(payload.get('recall_buy_rate'))}** "
        f"({payload.get('recall_buy')}/{payload.get('n_mover_days')}). "
        f"The table lists BUY / SELL / NO BUY only "
        f"({payload.get('n_called')} called). HOLD is in the sweep.",
        "",
        "## Preset sweep",
        "",
        "Paint the mover universe once, score each rule set. "
        "**Mover-days** = every name that cleared liquidity + ATR% "
        "that morning.",
        "",
        "| Preset | n | BUY | SELL | NO BUY | HOLD | "
        "catch 1d | BUY 1d | SELL 1d | catch 3d | catch 1w | 1d+3d | pnl 1d |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for name, block in sweeps.items():
        s = (block or {}).get("mover_days") or {}
        mark = " ←" if name == chosen else ""
        L.append(
            f"| `{name}`{mark} | {s.get('n') or 0} | "
            f"{s.get('n_buy') or 0} | {s.get('n_sell') or 0} | "
            f"{s.get('n_no_buy') or 0} | {s.get('n_hold') or 0} | "
            f"{gla._pct((s.get('catch') or {}).get('1d'))} "
            f"({(s.get('catch_hit') or {}).get('1d') or 0}/"
            f"{(s.get('catch_n') or {}).get('1d') or 0}) | "
            f"{gla._pct((s.get('buy_catch') or {}).get('1d'))} | "
            f"{gla._pct((s.get('sell_catch') or {}).get('1d'))} | "
            f"{gla._pct((s.get('catch') or {}).get('3d'))} | "
            f"{gla._pct((s.get('catch') or {}).get('1w'))} | "
            f"{gla._pct(s.get('aligned_1d_3d'))} | "
            f"{gla._ret((s.get('mean_pnl') or {}).get('1d'))} |"
        )
    L += [
        "",
        "## How the default call is made",
        "",
        "1. A featured **fade** setup (`first crack`, `🚨+heat🔴`) → **SELL**.",
        "2. Hall pass **blocked** → **NO BUY**.",
        "3. Hall pass standard / group leader / catalyst / probable → **BUY**.",
        "4. Else a featured **long** setup with 1d edge ≥ the preset cut "
        "(vol+AB, vol+gen🔴, vol+join🔴, 🔵+heat) → **BUY**.",
        "5. Else **HOLD**.",
        "",
        "## Each session (universe mix)",
        "",
        "| Date 09:30 ET | Tape | n | BUY | SELL | NO BUY | HOLD | "
        "catch 1d | BUY 1d | SELL 1d | pnl 1d |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for d in payload.get("daily") or []:
        L.append(
            f"| {act.session_stamp(d.get('date'), act.OPEN_CLOCK)} | "
            f"{d.get('tape') or '—'} | {d.get('n') or 0} | "
            f"{d.get('n_buy') or 0} | {d.get('n_sell') or 0} | "
            f"{d.get('n_no_buy') or 0} | {d.get('n_hold') or 0} | "
            f"{gla._pct(d.get('catch_1d'))} | {gla._pct(d.get('buy_1d'))} | "
            f"{gla._pct(d.get('sell_1d'))} | {gla._ret(d.get('pnl_1d'))} |"
        )
    regime = payload.get("regime") or {}
    if regime:
        L += [
            "",
            "## Regime gate inputs (knowable at 09:30 ET)",
            "",
            "SPY down-streak counts **prior** closes only — today's close is "
            "never used. The **`gated`** preset in the sweep blocks SELL when "
            "the streak reaches 3 (exhaustion: fading into a washed-out tape "
            "is what blew up 2026-09-02).",
            "",
            "| Date | Predict 05:55 ET | Score | SPY down-streak (prior) |",
            "|---|---|---:|---:|",
        ]
        for d in sorted(regime):
            r = regime[d] or {}
            L.append(
                f"| {act.session_stamp(d, act.OPEN_CLOCK)} | "
                f"{r.get('predict_dir') or '—'} | "
                f"{gla._ret(r.get('predict_score'))} | "
                f"{r.get('spy_down_streak') or 0} |"
            )
    lb = payload.get("leaderboard") or {}
    if lb:
        summ = lb.get("summary") or {}
        L += [
            "",
            f"## Conviction leaderboard (top {lb.get('top_n') or 10} per side)",
            "",
            "Calls ranked by conviction (long-setup edge + lane bonus + "
            "condition boxes). **Top** = the names you'd actually size into; "
            "**all** = every call that day.",
            "",
            "| Side | Cut | hit 1d | mean 1d | hit 3d | mean 3d | "
            "hit 1w | mean 1w |",
            "|---|---|---:|---:|---:|---:|---:|---:|",
        ]
        for side in ("BUY", "SELL"):
            for cut in ("top", "all"):
                s = (summ.get(side) or {}).get(cut) or {}
                L.append(
                    f"| {side} | {cut} | "
                    f"{gla._pct((s.get('1d') or {}).get('hit'))} | "
                    f"{gla._ret((s.get('1d') or {}).get('mean'))} | "
                    f"{gla._pct((s.get('3d') or {}).get('hit'))} | "
                    f"{gla._ret((s.get('3d') or {}).get('mean'))} | "
                    f"{gla._pct((s.get('1w') or {}).get('hit'))} | "
                    f"{gla._ret((s.get('1w') or {}).get('mean'))} |"
                )
        recent = (lb.get("days") or [])[-8:]
        if recent:
            L += ["", "### Recent top calls", ""]
            for blk in reversed(recent):
                L.append(
                    f"**{act.session_stamp(blk.get('date'), act.OPEN_CLOCK)} "
                    f"{blk.get('side')}** ({blk.get('n_calls')} calls): "
                    + " · ".join(
                        f"`{t.get('ticker')}` {(t.get('conviction') or 0):.1f}"
                        for t in (blk.get("top") or [])[:5]
                    )
                )
    L += [
        "",
        "## Called mornings (BUY / SELL / NO BUY)",
        "",
        "| Date 09:30 ET | Hits 1d/3d/1w | Ticker | Close 16:00 ET | "
        "Open 09:30 ET | Δ close 16:00 ET | o→c 09:30→16:00 | Cond | "
        "Action 09:30 ET | Why | Setups | "
        "+1d 16:00 ET | +3d 16:00 ET | +1w 16:00 ET |",
        "|---|---|---|---:|---:|---:|---:|---|---|---|---|---:|---:|---:|",
    ]
    rows = sorted(
        payload.get("called_rows") or [],
        key=lambda r: (
            str(r.get("date") or ""),
            {"SELL": 0, "BUY": 1, "NO BUY": 2}.get(r.get("action_call"), 9),
            str(r.get("ticker") or ""),
        ),
    )
    L.extend(_row_md(r) for r in rows)
    L += [
        "",
        "_Universe tape = last completed Finviz before the open (ATR% / "
        "mcap / ADV). Action = that date **09:30 ET**. Hits = 1d/3d/1w "
        "catch ✅/❌. 🟢 up · 🟡 flat · 🔴 down. Δ / o→c / forwards are "
        "outcomes after the 09:30 stamp._",
        "",
    ]
    return "\n".join(L) + "\n"


def _row_html(r: dict) -> str:
    tone = act.action_tone(r.get("action_call") or "")
    hits = r.get("hits") or {}
    bar = r.get("session_bar") or {}
    hz = r.get("horizon_dates") or {}
    fwd = gla._fwd(r) or {}
    day_chg = r.get("day_change")

    def pct_td(pct, text) -> str:
        cls = html.escape(act.ret_tone(pct))
        return f"<td class='{cls}'>{html.escape(text)}</td>"

    return (
        f"<tr><th>{html.escape(act.session_stamp(r.get('date'), act.OPEN_CLOCK))}</th>"
        f"<td class='hits'>{html.escape(gla.hits_cell(hits))}</td>"
        f"<td>{html.escape(str(r.get('ticker') or ''))}</td>"
        f"<td>{html.escape(act.format_price(bar.get('close'), r.get('date'), act.CLOSE_CLOCK))}</td>"
        f"<td>{html.escape(act.format_price(bar.get('open'), r.get('date'), act.OPEN_CLOCK))}</td>"
        f"{pct_td(day_chg, gla._pct_text(day_chg, r.get('date'), act.CLOSE_CLOCK))}"
        f"{pct_td(bar.get('close_open_pct'), gla._oc_text(bar.get('close_open_pct'), r.get('date')))}"
        f"<td>{html.escape(str(r.get('cond_tally') or act.cond_tally(r)))}</td>"
        f"<td class='{tone}'>{html.escape(str(r.get('action_label') or act.format_action(r.get('action_call'), r.get('date'))))}</td>"
        f"<td class='why'>{html.escape(str(r.get('action_reason') or '—'))}</td>"
        f"<td>{html.escape(setups.setup_labels(r) or '—')}</td>"
        f"{pct_td(fwd.get('1d'), gla._pct_text(fwd.get('1d'), hz.get('1d'), act.CLOSE_CLOCK))}"
        f"{pct_td(fwd.get('3d'), gla._pct_text(fwd.get('3d'), hz.get('3d'), act.CLOSE_CLOCK))}"
        f"{pct_td(fwd.get('1w'), gla._pct_text(fwd.get('1w'), hz.get('1w'), act.CLOSE_CLOCK))}"
        "</tr>"
    )


def render_html(payload: dict) -> str:
    chosen = payload.get("preset") or "featured"
    sweeps = payload.get("sweeps") or {}
    sweep_rows = []
    for name, block in sweeps.items():
        s = (block or {}).get("mover_days") or {}
        cls = "chosen" if name == chosen else ""
        sweep_rows.append(
            f"<tr class='{cls}'><td><code>{html.escape(name)}</code></td>"
            f"<td>{s.get('n') or 0}</td><td>{s.get('n_buy') or 0}</td>"
            f"<td>{s.get('n_sell') or 0}</td><td>{s.get('n_no_buy') or 0}</td>"
            f"<td>{s.get('n_hold') or 0}</td>"
            f"<td>{html.escape(gla._pct((s.get('catch') or {}).get('1d')))}</td>"
            f"<td>{html.escape(gla._pct((s.get('buy_catch') or {}).get('1d')))}</td>"
            f"<td>{html.escape(gla._pct((s.get('sell_catch') or {}).get('1d')))}</td>"
            f"<td>{html.escape(gla._pct((s.get('catch') or {}).get('3d')))}</td>"
            f"<td>{html.escape(gla._ret((s.get('mean_pnl') or {}).get('1d')))}</td></tr>"
        )
    daily_rows = []
    for d in payload.get("daily") or []:
        daily_rows.append(
            f"<tr><th>{html.escape(act.session_stamp(d.get('date'), act.OPEN_CLOCK))}</th>"
            f"<td>{html.escape(str(d.get('tape') or '—'))}</td>"
            f"<td>{d.get('n') or 0}</td><td>{d.get('n_buy') or 0}</td>"
            f"<td>{d.get('n_sell') or 0}</td><td>{d.get('n_no_buy') or 0}</td>"
            f"<td>{d.get('n_hold') or 0}</td>"
            f"<td>{html.escape(gla._pct(d.get('catch_1d')))}</td>"
            f"<td>{html.escape(gla._pct(d.get('buy_1d')))}</td>"
            f"<td>{html.escape(gla._pct(d.get('sell_1d')))}</td>"
            f"<td>{html.escape(gla._ret(d.get('pnl_1d')))}</td></tr>"
        )
    called = sorted(
        payload.get("called_rows") or [],
        key=lambda r: (
            str(r.get("date") or ""),
            {"SELL": 0, "BUY": 1, "NO BUY": 2}.get(r.get("action_call"), 9),
            str(r.get("ticker") or ""),
        ),
    )
    body = [_row_html(r) for r in called]
    lb_rows = []
    lb = payload.get("leaderboard") or {}
    summ = lb.get("summary") or {}
    for side in ("BUY", "SELL"):
        for cut in ("top", "all"):
            s = (summ.get(side) or {}).get(cut) or {}
            lb_rows.append(
                f"<tr><td>{side}</td><td>{cut}</td>"
                f"<td>{html.escape(gla._pct((s.get('1d') or {}).get('hit')))}</td>"
                f"<td>{html.escape(gla._ret((s.get('1d') or {}).get('mean')))}</td>"
                f"<td>{html.escape(gla._pct((s.get('3d') or {}).get('hit')))}</td>"
                f"<td>{html.escape(gla._ret((s.get('3d') or {}).get('mean')))}</td>"
                f"<td>{html.escape(gla._pct((s.get('1w') or {}).get('hit')))}</td>"
                f"<td>{html.escape(gla._ret((s.get('1w') or {}).get('mean')))}</td></tr>"
            )
    lb_section = ""
    if lb_rows:
        lb_section = (
            f"<h2>Conviction leaderboard (top {html.escape(str(lb.get('top_n') or 10))} per side)</h2>"
            "<div class='sheet'><table class='mix'>"
            "<thead><tr><th>Side</th><th>Cut</th><th>hit 1d</th><th>mean 1d</th>"
            "<th>hit 3d</th><th>mean 3d</th><th>hit 1w</th><th>mean 1w</th></tr></thead>"
            f"<tbody>{''.join(lb_rows)}</tbody></table></div>"
        )
    return f"""<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Mover lookback action</title>
<style>
:root{{--bg:#0b1020;--card:#131b31;--line:#2b3552;--text:#edf2ff;--muted:#9cabc9}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--text);font:15px/1.45 system-ui}}
main{{max-width:1280px;margin:auto;padding:16px}}h1,h2{{margin:.4em 0}}
.muted{{color:var(--muted)}}
.sheet{{overflow-x:auto;border:1px solid var(--line);border-radius:12px;margin:16px 0}}
table{{border-collapse:separate;border-spacing:0;min-width:1600px;width:100%;background:var(--card)}}
table.mix{{min-width:960px}}
th,td{{padding:8px 7px;text-align:center;border-bottom:1px solid var(--line);white-space:nowrap}}
thead th{{position:sticky;top:0;background:#17213a;z-index:2}}
thead th:nth-child(2){{left:13.5rem;z-index:3}}
tbody th{{position:sticky;left:0;background:#17213a;text-align:left;z-index:1}}
td.hits{{position:sticky;left:13.5rem;background:#17213a;font-weight:700;z-index:1;box-shadow:1px 0 0 var(--line)}}
td.good{{background:#123d2c}}td.bad{{background:#4b2028}}td.neutral{{background:#473e1d}}
td.why{{text-align:left;white-space:normal;max-width:280px;font-size:12px}}
tr.chosen td{{outline:1px solid #eab308}}
@media(max-width:600px){{main{{padding:8px}}th,td{{padding:8px 6px;font-size:13px}}}}
</style></head><body><main>
<h1>Mover lookback action</h1>
<p>Names knowable at <b>09:30 ET</b> — last completed Finviz tape with
mcap &gt; ${html.escape(str(payload.get('min_mcap_m')))}M,
ADV &gt; {html.escape(str(payload.get('min_avg_vol_k')))}K,
ATR% ≥ {html.escape(str(payload.get('min_atr_pct')))}
({html.escape(str(payload.get('from_date')))} → {html.escape(str(payload.get('to_date')))}).
<b>Not top gainers</b> — same-day Change% does not pick the list.
Action is that date 09:30 ET. 🟢 up · 🟡 flat · 🔴 down. Hits = 1d/3d/1w catch.</p>
<p class="muted">BUY rate:
<b>{html.escape(gla._pct(payload.get('recall_buy_rate')))}</b>
· {html.escape(str(payload.get('n_mover_days')))} mover-days
· {html.escape(str(payload.get('n_called')))} called
· default <code>{html.escape(str(chosen))}</code></p>
<h2>Preset sweep (mover mornings)</h2>
<div class="sheet"><table class="mix">
<thead><tr><th>Preset</th><th>n</th><th>BUY</th><th>SELL</th><th>NO BUY</th><th>HOLD</th><th>catch 1d</th><th>BUY 1d</th><th>SELL 1d</th><th>catch 3d</th><th>pnl 1d</th></tr></thead>
<tbody>{''.join(sweep_rows)}</tbody></table></div>
<h2>Each session</h2>
<div class="sheet"><table class="mix">
<thead><tr><th>Date 09:30 ET</th><th>Tape</th><th>n</th><th>BUY</th><th>SELL</th><th>NO BUY</th><th>HOLD</th><th>catch 1d</th><th>BUY 1d</th><th>SELL 1d</th><th>pnl 1d</th></tr></thead>
<tbody>{''.join(daily_rows)}</tbody></table></div>
{lb_section}
<h2>Called mornings (BUY / SELL / NO BUY)</h2>
<div class="sheet"><table>
<thead><tr><th>Date 09:30 ET</th><th>Hits 1d/3d/1w</th><th>Ticker</th><th>Close 16:00 ET</th><th>Open 09:30 ET</th><th>Δ close 16:00 ET</th><th>o→c 09:30→16:00</th><th>Cond</th><th>Action 09:30 ET</th><th>Why</th><th>Setups</th><th>+1d 16:00 ET</th><th>+3d 16:00 ET</th><th>+1w 16:00 ET</th></tr></thead>
<tbody>{''.join(body)}</tbody></table></div>
</main></body></html>"""


def _slim_called(r: dict) -> dict:
    return {
        "date": r.get("date"),
        "ticker": r.get("ticker"),
        "universe_tape": r.get("universe_tape"),
        "session_bar": r.get("session_bar"),
        "horizon_dates": r.get("horizon_dates"),
        "condition": r.get("condition"),
        "cond_tally": r.get("cond_tally"),
        "boxes": r.get("boxes"),
        "conviction": r.get("conviction"),
        "action_call": r.get("action_call"),
        "action_label": r.get("action_label"),
        "action_stamp": r.get("action_stamp"),
        "action_reason": r.get("action_reason"),
        "lane": r.get("lane"),
        "lane_label": r.get("lane_label"),
        "setups": [
            {"id": s.get("id"), "short": s.get("short"),
             "verdict": s.get("verdict"), "edge_1d": s.get("edge_1d")}
            for s in (r.get("setups") or [])
        ],
        "price_changes": r.get("price_changes") or r.get("forward_returns"),
        "day_change": r.get("day_change"),
        "hits": r.get("hits"),
    }


def write(payload: dict) -> dict:
    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    DAILY_MD.parent.mkdir(parents=True, exist_ok=True)
    OUT_HTML.parent.mkdir(parents=True, exist_ok=True)
    slim = {k: v for k, v in payload.items() if k != "called_rows"}
    slim["called_rows"] = [_slim_called(r) for r in payload.get("called_rows") or []]
    md = render_markdown(payload)
    OUT_MD.write_text(md, encoding="utf-8")
    DAILY_MD.write_text(md, encoding="utf-8")
    OUT_JSON.write_text(json.dumps(slim, indent=2, default=str), encoding="utf-8")
    OUT_HTML.write_text(render_html(payload), encoding="utf-8")
    print(f"[mover-lookback-action] wrote {OUT_MD}")
    print(f"[mover-lookback-action] wrote {OUT_JSON}")
    print(f"[mover-lookback-action] phone {OUT_HTML}")
    return payload


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-date", default=START)
    ap.add_argument("--to-date", default="")
    ap.add_argument("--preset", default="", help="featured|strict|setups|lane|loose")
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args()
    payload = walk(
        from_date=args.from_date,
        to_date=args.to_date or None,
        preset=args.preset or None,
    )
    if args.write:
        write(payload)
    print(render_markdown(payload).split("## Called mornings")[0])


if __name__ == "__main__":
    main()
