"""Compact 09:30 pack so the dashboard can replay any cash-start.

The 8-13 blotter is the Python book. Later starts need the same
per-name marks; shipping 161 × 17 full books blows GitHub's payload
budget. This pack is the rows + open/close tape the browser (and
tests) use to walk the same leftover / fee / min-hold rules.
"""
from __future__ import annotations

import json
from pathlib import Path

from . import factor_mine as fm
from . import factor_mine_book as fmb
from . import paper_trade as pt
from . import ticker_lookback as tl

ROOT = Path(__file__).resolve().parent.parent
FEE_PATH = ROOT / "00_grounding" / "futubull_fees.json"

ROW_KEEP = (
    "date", "ticker", "sources", "src_rank", "boxes",
    "blue", "alarm", "zero_red", "last_green", "last_red",
    "candle_capture", "candle_score",
    "ohlc_ret_5", "ohlc_rvol", "ohlc_hot_score", "ohlc_nr7", "ohlc_break_10",
    "erd_earn_react", "erd_days_since_E", "erd_days_since_R",
    "erd_flag_E", "erd_flag_R",
    "e_pol", "e_label", "r_pol", "r_label",
    "cond_good", "cond_bad",
)


def _num_fees() -> dict:
    raw = json.loads(FEE_PATH.read_text(encoding="utf-8")) if FEE_PATH.is_file() else {}
    out = {}
    for k, v in (raw or {}).items():
        if k.startswith("_") or k in ("currency", "paper_account"):
            continue
        if isinstance(v, (int, float)):
            out[k] = v
    if not out:
        fees = pt.load_fees()
        for k, v in (fees or {}).items():
            if isinstance(v, (int, float)):
                out[k] = v
    return out


def pack_rets(xs: list) -> dict:
    """Win rate + mean for a list of horizon %."""
    xs = [float(v) for v in xs if v is not None]
    if not xs:
        return {"n": 0, "win": None, "mean": None}
    return {
        "n": len(xs),
        "win": round(sum(1 for v in xs if v > 0) / len(xs), 4),
        "mean": round(sum(xs) / len(xs), 3),
    }


def hit_tally(looks_by_date: dict, hard_dates) -> dict:
    """BUY / NO / SIT profitable-hit rates, plus n_neg ≤2 vs ≥3."""
    hard = set(hard_dates or [])
    buckets = {"buy": [], "no": [], "sit": []}
    neg = {"le2": [], "ge3": []}
    for date, looks in (looks_by_date or {}).items():
        sit = date in hard
        for x in looks or []:
            ret = x.get("ret")
            if ret is None:
                continue
            take = "sit" if sit else ("buy" if x.get("buy") else "no")
            buckets[take].append(ret)
            (neg["ge3"] if int(x.get("n_neg") or 0) >= 3 else neg["le2"]).append(ret)
    return {
        "buy": pack_rets(buckets["buy"]),
        "no": pack_rets(buckets["no"]),
        "sit": pack_rets(buckets["sit"]),
        "n_neg_le2": pack_rets(neg["le2"]),
        "n_neg_ge3": pack_rets(neg["ge3"]),
    }


def build_sim_pack(panel: dict) -> dict:
    """Rows + tape + fee schedule. No same-day Change%."""
    from . import factor_mine_probe as fmp
    panel = fm.rehydrate_panel(panel)
    fmp.attach_erd_polarity(panel)
    cal = list(panel.get("session_dates") or [])
    rows = []
    tickers: set[str] = set()
    for r in panel.get("rows") or []:
        t = fm._tick(r.get("ticker"))
        d = r.get("date")
        if not t or not d:
            continue
        tickers.add(t)
        slim = {k: r.get(k) for k in ROW_KEEP}
        slim["ticker"] = t
        rows.append(slim)
    tape: dict[str, dict] = {}
    for t in tickers:
        for d in cal:
            bar = tl.session_bar(t, d) or {}
            o = fm._finite(bar.get("open"))
            c = fm._finite(bar.get("close"))
            if o is None and c is None:
                continue
            tape.setdefault(t, {})[d] = [
                None if o is None else round(float(o), 4),
                None if c is None else round(float(c), 4),
            ]
    for r in panel.get("rows") or []:
        t = fm._tick(r.get("ticker"))
        d = r.get("date")
        o = fm._finite(r.get("open"))
        c = fm._finite(r.get("close"))
        if not t or not d or (o is None and c is None):
            continue
        tape.setdefault(t, {})[d] = [
            None if o is None else round(float(o), 4),
            None if c is None else round(float(c), 4),
        ]
    regime = fmb.load_regime()
    scores = {}
    for d in cal:
        s = fmb.morning_s(regime, d)
        if s is not None:
            scores[d] = round(float(s), 2)
    return {
        "dates": cal,
        "fees": _num_fees(),
        "s": scores,
        "hard_red": fmb.HARD_RED,
        "good_s": fmb.GOOD_S,
        "more_names": fmb.MORE_NAMES,
        "sizeup": fmb.SIZEUP,
        "cut_los": fmb.CUT_LOS,
        "trail_off": fmb.TRAIL_OFF,
        "borrow_annual": fmb.BORROW_ANNUAL,
        "capital": fm.CAPITAL,
        "rows": rows,
        "tape": tape,
    }


def rank_score(row: dict, rec: dict) -> float:
    how = rec.get("rank")
    hot = fm._finite(row.get("ohlc_hot_score")) or 0.0
    candle = fm._finite(row.get("candle_score")) or 0.0
    cond = int(row.get("cond_good") or 0) - int(row.get("cond_bad") or 0)
    if how == "hot_score":
        return round(float(hot), 4)
    if how == "candle_score":
        return round(float(candle), 4)
    if how == "ret_5":
        return round(float(fm._finite(row.get("ohlc_ret_5")) or 0.0), 4)
    if how == "cond":
        return float(cond)
    if how == "w_hot_cond":
        return round(0.6 * hot + 0.4 * max(cond, 0), 4)
    if how == "w_hot_candle":
        return round(0.6 * hot + 0.4 * candle, 4)
    src = 99 if row.get("src_rank") is None else int(row["src_rank"])
    return float(100 - src)


def horizon_pct(panel: dict, rec: dict, ticker: str, date: str,
                bars=None) -> float | None:
    """Hold-H % from the 09:30 open. Missing later bars carry the last print."""
    cal = list(panel.get("session_dates") or [])
    row_index = {(r["date"], r["ticker"]): r for r in (panel.get("rows") or [])}
    ret = fm.hold_return(
        ticker, date, rec["hold"], cal, rec.get("side") or "long",
        rec.get("exit_when"), row_index, bars=bars,
    )
    if ret is not None:
        return round(float(ret), 3)
    win = fm.hold_window(cal, date, int(rec["hold"]))
    if not win:
        return None
    entry_bar = fm._bar(ticker, date, bars)
    entry = fm._finite(entry_bar.get("open")) or fm._finite(entry_bar.get("close"))
    if entry is None or entry == 0:
        return None
    exit_px = None
    for d in reversed(win):
        bar = fm._bar(ticker, d, bars)
        exit_px = fm._finite(bar.get("close")) or fm._finite(bar.get("open"))
        if exit_px is not None:
            break
    if exit_px is None or exit_px == 0:
        return None
    out = 100.0 * (float(exit_px) / float(entry) - 1.0)
    if (rec.get("side") or "long") == "short":
        out = -out
    return round(out, 3)


def look_day(panel: dict, rec: dict, date: str, *, bars=None,
             regime=None) -> list[dict]:
    """Names this recipe looked at on ``date``, ranked, with horizon %."""
    panel = fm.rehydrate_panel(panel)
    by_date = panel.get("by_date") or {}
    uni = rec.get("universe") or "union"
    looked = []
    for r in by_date.get(date) or []:
        srcs = set(r.get("sources") or [])
        if uni != "union" and uni not in srcs:
            continue
        looked.append(r)
    passed = [r for r in looked if fm.matches(r, rec)]
    passed.sort(key=lambda r: fm.rank_key(r, rec))
    s = fmb.morning_s(regime if regime is not None else fmb.load_regime(), date)
    hard = bool(s is not None and float(s) <= float(fmb.HARD_RED))
    top_n = int(rec.get("top_n") or fm.TOP_N_DEFAULT)
    if (s is not None and float(s) >= fmb.GOOD_S and not hard
            and (rec.get("s_boost") or "none") in ("more_names", "both")):
        top_n += fmb.MORE_NAMES
    out = []
    seen = set()
    for i, r in enumerate(passed, 1):
        seen.add(r["ticker"])
        ret = horizon_pct(panel, rec, r["ticker"], date, bars=bars)
        boxes = r.get("boxes") or {}
        n_neg = sum(1 for v in boxes.values() if v == "bad")
        if r.get("alarm"):
            n_neg += 1
        out.append({
            "ticker": r["ticker"],
            "rank": i,
            "score": rank_score(r, rec),
            "pass": True,
            "buy": (not hard) and i <= top_n,
            "ret": ret,
            "n_neg": n_neg,
            "src_rank": r.get("src_rank"),
        })
    rest = [r for r in looked if r["ticker"] not in seen]
    rest.sort(key=lambda r: fm.rank_key(r, rec))
    for r in rest:
        ret = horizon_pct(panel, rec, r["ticker"], date, bars=bars)
        boxes = r.get("boxes") or {}
        n_neg = sum(1 for v in boxes.values() if v == "bad")
        if r.get("alarm"):
            n_neg += 1
        out.append({
            "ticker": r["ticker"],
            "rank": None,
            "score": rank_score(r, rec),
            "pass": False,
            "buy": False,
            "ret": None if ret is None else round(float(ret), 3),
            "n_neg": n_neg,
            "src_rank": r.get("src_rank"),
        })
    return out
