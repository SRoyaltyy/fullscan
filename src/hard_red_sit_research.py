"""Hard-red sit experiment — research only.

Manager lock / clock split (do not invert):

  **A short-only on hard-red** — pre-open policy, decided before 09:30.
  Excel may pre-filter *which* shorts are eligible via open-knowable
  CLEAR / letters only (FQ / ER / EP / AH / FR / DF_lag1). Excel does
  **not** own the sit. Deep-corr is not waited on.

  **B long dip-scoop open−X%** — **intraday**, after 09:30. Open is
  known. Trigger is first touch of open−X% on same-day OHLC (session
  low as the daily first-hit proxy). Close is the *grade* only — never
  the trigger.

Live control remains full sit (S≤−3 blocks long and short).

KEEP bar: ≥30 fires where possible, >55% after Futubull fees, both tapes
(Webull combo + flatten/io) if data allows. Thin n is a KILL, not a wink.

Does **not** change flatten_robust live buy or Webull live policy.
Default ``simulate_shared`` / ``size_combo_tickets`` stay full sit.

CLI: python -m src.hard_red_sit_research --write
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

from . import book_era
from . import combo_broker as cb
from . import factor_mine as fm
from . import factor_mine_book as fmb
from . import factor_mine_combo as fmc
from . import flatten_lookback_action as fla
from . import paper_trade as pt
from . import sleeve_merge as sm
from . import ticker_lookback as tl
from . import walkforward_factor_mine as wf

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "03_scoreboard" / "hard_red_sit"
OUT_MD = ROOT / "03_scoreboard" / "HARD_RED_SIT.md"
OUT_JSON = OUT_DIR / "hard_red_sit.json"

LIVE_COMBO = cb.PAPER_COMBO
ASOF = "2026-09-14"
WINDOW_START = book_era.DASHBOARD_START
DIP_GRID = fmc.DIP_GRID
KEEP_MIN_FIRES = 30
KEEP_WIN = 0.55
CAPITAL = fm.CAPITAL
FLATTEN_HOLD = 3  # flatten_robust io_hold = 3d

# 2026-09-14 dry lists from the war room (also reconstructed when rows exist).
WEBULL_0914_LONGS = ("INDP", "GPRO", "VERI", "HUT")
WEBULL_0914_SHORTS = ("BKV",)
FLATTEN_0914 = ("CVE", "BG", "NVT", "DK")

# Clock-clean Excel letter panel. Open-knowable only — no same-row H/I.
LETTER_CSV = ROOT / "excel_bot" / "research" / "excel_clear_letter_panel.csv"
OPEN_LETTERS = ("FQ", "ER", "EP", "AH", "FR", "DF_lag1")
# Stretch letters used as a short *eligibility* hint (confirmed as LONG
# avoids on the Yahoo analog). Not a short CLEAR. Deep-corr not run.
SHORT_STRETCH = ("FQ", "ER", "EP")
_LETTER_INDEX: dict | None = None


def clock_bar(ticker: str, date: str, bars=None) -> dict:
    """Official regular-session OHLC. Never Gap / last / same-day Finviz Price."""
    t = fm._tick(ticker)
    bar = fm._bar(t, date, bars) if t else {}
    return {
        "open": fm._finite(bar.get("open")),
        "high": fm._finite(bar.get("high")),
        "low": fm._finite(bar.get("low")),
        "close": fm._finite(bar.get("close")),
        "src": bar.get("src"),
    }


def yahoo_session_overlay(tickers: list[str], date: str) -> dict:
    """Regular-session Yahoo bar when parquet has not landed that date.

    Open / high / low are the printed regular session. Close is the
    last print Yahoo has — **not** a 16:00 mark if the session is still
    open. Research overlay only; does not write ``ohlc.parquet``.
    """
    names = sorted({fm._tick(t) for t in tickers if fm._tick(t)})
    if not names:
        return {}
    try:
        from src.price_store import _yf_download
    except Exception:
        return {}
    # yfinance end is exclusive; +1 calendar day is enough.
    y, m, d = (int(x) for x in date.split("-"))
    from datetime import date as _date, timedelta
    end = (_date(y, m, d) + timedelta(days=2)).isoformat()
    try:
        df = _yf_download(names, date, end)
    except (Exception, SystemExit):
        return {}
    if df is None or getattr(df, "empty", True):
        return {}
    out = {}
    try:
        recs = df.to_dict(orient="records")
    except Exception:
        return {}
    for rec in recs:
        t = fm._tick(rec.get("ticker"))
        d0 = str(rec.get("date") or "")[:10]
        if not t or d0 != date:
            continue
        out[(t, date)] = {
            "open": fm._finite(rec.get("open")),
            "high": fm._finite(rec.get("high")),
            "low": fm._finite(rec.get("low")),
            "close": fm._finite(rec.get("close")),
            "src": "yahoo_session",
        }
    return out


def load_letter_index(path: Path | None = None) -> dict:
    """Clock-clean CLEAR letter panel: date+ticker → FQ/ER/EP/AH/FR/DF_lag1."""
    global _LETTER_INDEX
    if _LETTER_INDEX is not None and path is None:
        return _LETTER_INDEX
    p = path or LETTER_CSV
    out: dict = {}
    if not p.is_file():
        if path is None:
            _LETTER_INDEX = out
        return out
    import csv
    with p.open(encoding="utf-8", newline="") as fh:
        for rec in csv.DictReader(fh):
            d = str(rec.get("date") or "")[:10]
            t = fm._tick(rec.get("ticker"))
            if not d or not t:
                continue
            out[(d, t)] = {k: rec.get(k) for k in OPEN_LETTERS}
    if path is None:
        _LETTER_INDEX = out
    return out


def _prior_ohlc_bars(ticker: str, date: str, n: int = 20) -> list[dict]:
    """Completed session bars strictly before ``date``. No same-row peek."""
    t = fm._tick(ticker)
    if not t:
        return []
    try:
        df = tl._ohlc_bars()
    except Exception:
        return []
    if df is None or getattr(df, "empty", True):
        return []
    try:
        sub = df.xs(t, level=1)
    except Exception:
        return []
    rows = []
    try:
        recs = sub.reset_index().to_dict(orient="records")
    except Exception:
        return []
    for rec in recs:
        d = str(rec.get("date") or "")[:10]
        if not d or d >= date:
            continue
        o = fm._finite(rec.get("open"))
        c = fm._finite(rec.get("close"))
        if o is None or c is None:
            continue
        h = fm._finite(rec.get("high"))
        low = fm._finite(rec.get("low"))
        rows.append({
            "date": d, "o": o,
            "h": h if h is not None else c,
            "l": low if low is not None else c,
            "c": c,
            "v": float(rec.get("volume") or rec.get("vol") or 0),
        })
    rows.sort(key=lambda r: r["date"])
    return rows[-n:]


def _compute_letters(ticker: str, date: str) -> dict | None:
    """Open-knowable letters from prior bars + excel_open_features.

    Passes ``today_open=None`` so J is blank — stretch letters FQ/ER/EP
    do not need today's open. Never feeds today's H/L/C.
    """
    prior = _prior_ohlc_bars(ticker, date)
    if len(prior) < 6:
        return None
    if any(b.get("date") >= date for b in prior):
        raise ValueError(f"LEAK abort: same-row bar in prior {ticker} {date}")
    try:
        import sys
        eng = str(ROOT / "excel_bot" / "engine")
        if eng not in sys.path:
            sys.path.insert(0, eng)
        from excel_open_features import open_features
    except Exception:
        return None
    xl = open_features(prior, None)
    if xl.get("same_row_df") or xl.get("same_row_bb") or xl.get("same_row_bq"):
        raise ValueError(f"LEAK abort: same-row DF/BB/BQ {ticker} {date}")
    return {
        "FQ": xl.get("FQ"),
        "ER": xl.get("ER"),
        "EP": xl.get("EP"),
        "AH": xl.get("AH"),
        "FR": xl.get("FR"),
        "DF_lag1": xl.get("df") or "None",
        "_src": "open_features",
        "_prior_date": prior[-1].get("date"),
    }


def excel_letters(date: str, ticker: str, index: dict | None = None) -> dict:
    """Open-knowable CLEAR letters for one name-day. No close-knowable fields."""
    t = fm._tick(ticker)
    idx = index if index is not None else load_letter_index()
    rec = (idx or {}).get((date, t))
    src = "panel"
    if rec is None:
        rec = _compute_letters(t, date) or {}
        src = rec.get("_src") or "missing"
    out = {"date": date, "ticker": t, "src": src}
    for k in OPEN_LETTERS:
        out[k] = rec.get(k)
    banned = ("H", "I", "close", "core_score", "M")
    for k in banned:
        if k in out and k not in OPEN_LETTERS:
            raise ValueError(f"LEAK abort: close-knowable {k} on letter row")
    return out


def _num_letter(v):
    if v is None or v == "" or v == "None":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def short_letter_eligible(letters: dict | None) -> dict:
    """Pre-filter only. Excel does not own the sit.

    Eligible if an open-knowable stretch letter is on (FQ=1 / ER=1 /
    EP≥0.03). Those letters CLEAR as *long avoids* on the Yahoo analog
    — here they only mark “yesterday already ran,” a fade hint for the
    short kid. Missing letters → ``unknown`` (do not invent a sit).
    Deep-corr is not run; this is not a short CLEAR.
    """
    letters = letters or {}
    fq = _num_letter(letters.get("FQ"))
    er = _num_letter(letters.get("ER"))
    ep = _num_letter(letters.get("EP"))
    have = any(v is not None for v in (fq, er, ep))
    hits = []
    if fq is not None and fq >= 1:
        hits.append("FQ")
    if er is not None and er >= 1:
        hits.append("ER")
    if ep is not None and ep >= 0.03:
        hits.append("EP")
    if not have:
        status = "unknown"
        ok = True  # pass-through — do not invent a block
    else:
        status = "eligible" if hits else "ineligible"
        ok = bool(hits)
    return {
        "ok": ok,
        "status": status,
        "hits": hits,
        "letters": {k: letters.get(k) for k in OPEN_LETTERS},
        "owns_sit": False,
    }


def stamp_excel_shorts(fires: list[dict], index: dict | None = None) -> list[dict]:
    out = []
    for f in fires:
        rec = dict(f)
        letters = excel_letters(f.get("date") or "", f.get("ticker") or "",
                                index=index)
        gate = short_letter_eligible(letters)
        rec["excel"] = gate
        rec["excel_src"] = letters.get("src")
        out.append(rec)
    return out


def after_fee_pnl(shares: int, entry: float, exit_px: float, *,
                  side: str, fees) -> float:
    """Round-trip $ after Futubull fees. Shorts pay borrow is skipped
    (same-day / few-day holds); entry+exit commissions still apply."""
    shares = int(shares)
    if shares < 1 or entry is None or exit_px is None:
        return 0.0
    if side == "short":
        fee_in = pt.order_fees(shares, entry, "sell", fees)
        fee_out = pt.order_fees(shares, exit_px, "buy", fees)
        return (shares * entry - fee_in) - (shares * exit_px + fee_out)
    fee_in = pt.order_fees(shares, entry, "buy", fees)
    fee_out = pt.order_fees(shares, exit_px, "sell", fees)
    return (shares * exit_px - fee_out) - (shares * entry + fee_in)


def hard_red_dates(cal: list[str], regime: dict | None) -> list[dict]:
    out = []
    for d in cal:
        s = fmb.morning_s(regime, d)
        if s is not None and float(s) <= float(fmb.HARD_RED):
            out.append({"date": d, "s": round(float(s), 3)})
    return out


def combo_recs(recipes: list[dict] | None = None) -> tuple[dict, list[dict]]:
    spec = cb.combo_spec(LIVE_COMBO)
    rec_by = {r["name"]: r for r in (recipes or fm.build_recipes())}
    return spec, fmc.recs_for_spec(spec, rec_by)


def extend_panel(panel: dict, date: str, rows: list[dict] | None = None) -> dict:
    """Append a leak-free look-row session (09-14) when the baked panel stops early."""
    cal = list(panel.get("session_dates") or [])
    if date in cal:
        return panel
    if rows is None:
        try:
            rows = cb.build_look_rows(date)
        except Exception:
            rows = []
    rows = [dict(r, date=r.get("date") or date) for r in (rows or [])]
    new_cal = sorted(set(cal) | {date})
    all_rows = list(panel.get("rows") or []) + rows
    by_date = {k: list(v) for k, v in (panel.get("by_date") or {}).items()}
    if rows:
        by_date[date] = rows
    out = dict(panel)
    out.update({
        "session_dates": new_cal,
        "rows": all_rows,
        "by_date": by_date,
        "from_date": new_cal[0] if new_cal else panel.get("from_date"),
        "to_date": new_cal[-1] if new_cal else date,
        "n_sessions": len(new_cal),
        "n_rows": len(all_rows),
        "extended": date,
    })
    return out


def simulate_combo(panel: dict, recs: list[dict], spec: dict, *,
                   bars, fees, regime, mode: str = fmc.HARD_RED_SIT,
                   dip_pct: float | None = None) -> dict:
    return fmc.simulate_shared(
        panel, recs, spec["weights"], bars=bars, fees=fees, regime=regime,
        net=spec.get("net") or "priority",
        name=f"{LIVE_COMBO}:{mode}"
             + (f":x{dip_pct:g}" if dip_pct is not None else ""),
        hard_red_mode=mode, dip_pct=dip_pct,
    )


def _close_px_for(book: dict, fill: dict, bars) -> tuple[float | None, str]:
    """Exit print for a hard-red fill: matching SELL/COVER, else last close."""
    t = fill.get("ticker")
    entry = fill.get("date")
    want = "SELL" if fill.get("side") == "BUY" else "COVER"
    for tr in book.get("trades") or []:
        if (tr.get("ticker") == t and tr.get("side") == want
                and tr.get("date") >= entry and tr.get("price") is not None):
            return float(tr["price"]), "closed"
    cal = [d.get("date") for d in book.get("daily") or [] if d.get("date")]
    last = cal[-1] if cal else entry
    px = fmb._px(t, last, "close", bars)
    if px is not None:
        return float(px), "mark"
    return None, "no_exit"


def hard_red_fires(book: dict, *, bars=None, fees=None,
                   side: str | None = None) -> list[dict]:
    """BUY/SHORT fills that printed on a hard-red morning, with after-fee P&L."""
    red = {d["date"] for d in book.get("daily") or [] if d.get("hard_red")}
    fees = fees if fees is not None else pt.load_fees()
    out = []
    for tr in book.get("trades") or []:
        if tr.get("date") not in red:
            continue
        if tr.get("side") not in ("BUY", "SHORT"):
            continue
        kid = "long" if tr["side"] == "BUY" else "short"
        if side and kid != side:
            continue
        exit_px, how = _close_px_for(book, tr, bars)
        pnl = None
        if exit_px is not None and tr.get("price") is not None:
            pnl = round(after_fee_pnl(
                int(tr.get("shares") or 0), float(tr["price"]), exit_px,
                side=kid, fees=fees), 2)
        out.append({
            "date": tr["date"],
            "ticker": tr.get("ticker"),
            "side": kid,
            "shares": tr.get("shares"),
            "entry": tr.get("price"),
            "exit": None if exit_px is None else round(exit_px, 4),
            "exit_how": how,
            "pnl": pnl,
            "owner": tr.get("owner"),
            "win": None if pnl is None else bool(pnl > 0),
        })
    return out


def fire_stats(fires: list[dict]) -> dict:
    graded = [f for f in fires if f.get("pnl") is not None]
    wins = [f for f in graded if (f.get("pnl") or 0) > 0]
    n = len(fires)
    n_g = len(graded)
    return {
        "n_fires": n,
        "n_graded": n_g,
        "n_wins": len(wins),
        "win_rate": None if not n_g else round(len(wins) / n_g, 4),
        "pnl": round(sum(float(f.get("pnl") or 0) for f in graded), 2),
        "fires": fires,
    }


def book_row(book: dict, fires: dict, *, label: str, mode: str,
             dip_pct=None) -> dict:
    return {
        "label": label,
        "mode": mode,
        "dip_pct": dip_pct,
        "name": book.get("name"),
        "book_pct": book.get("total_ret_pct"),
        "final_equity": book.get("final_equity"),
        "n_trades": book.get("n_trades"),
        "n_closed": book.get("n_closed"),
        "book_win_rate": book.get("win_rate"),
        "realized": book.get("realized"),
        "audit_ok": bool((book.get("audit") or {}).get("ok")),
        "hard_red_fires": fires.get("n_fires"),
        "hard_red_win_rate": fires.get("win_rate"),
        "hard_red_pnl": fires.get("pnl"),
        "hard_red_n_graded": fires.get("n_graded"),
        "hard_red_n_wins": fires.get("n_wins"),
    }


def horizon_exit(cal: list[str], date: str, hold: int, ticker: str,
                 bars=None):
    """Close of the hold-th session including entry (3d → entry+2)."""
    if date not in cal:
        bar = clock_bar(ticker, date, bars)
        return bar.get("close"), date, "same_or_missing"
    i = cal.index(date)
    j = min(i + max(int(hold) - 1, 0), len(cal) - 1)
    exit_d = cal[j]
    px = clock_bar(ticker, exit_d, bars).get("close")
    how = "horizon_close" if j > i else "same_day_close"
    if px is None:
        px = clock_bar(ticker, exit_d, bars).get("open")
        how = "horizon_open" if px is not None else "no_exit"
    return px, exit_d, how


def flatten_woulds(cal: list[str], *, payload=None, books=None,
                   pol=None, book_map=None) -> list[dict]:
    """Would-have .io names on each hard-red session. No live tickets."""
    payload = payload if payload is not None else sm.load_payload()
    books = books if books is not None else sm.list_books()
    pol = dict(pol or sm.live_policy())
    book_map = book_map if book_map is not None else sm.load_book_map(books)
    full_cal = sm.session_calendar(payload, books)
    use_cal = [d for d in (full_cal or cal) if d >= (cal[0] if cal else "")]
    out = []
    for date in cal:
        try:
            plan = fla.flatten_day_targets(
                date, payload=payload, books=books, pol=pol,
                book_map=book_map, cal=use_cal)
        except Exception:
            continue
        if not plan.get("hard_red"):
            continue
        names = [fm._tick(t) for t in (plan.get("tickers") or []) if t]
        out.append({
            "date": date,
            "s": plan.get("score"),
            "route": plan.get("route"),
            "tickers": names,
            "io_picks": [fm._tick(t) for t in (plan.get("io_picks") or [])],
        })
    return out


def flatten_name_days(woulds: list[dict], cal: list[str], *,
                      bars=None, fees=None, dip_pct: float | None = None,
                      mode: str = fmc.HARD_RED_SIT) -> list[dict]:
    """One fire per flatten/io would-have that a mode would have taken."""
    fees = fees if fees is not None else pt.load_fees()
    fires = []
    for day in woulds:
        date = day["date"]
        for t in day.get("tickers") or []:
            if mode == fmc.HARD_RED_SIT:
                continue
            if mode == fmc.HARD_RED_SHORT_ONLY:
                # Flatten .io is a long book — no short kid to un-sit.
                continue
            bar = clock_bar(t, date, bars)
            o, low = bar.get("open"), bar.get("low")
            if o is None:
                fires.append({
                    "date": date, "ticker": t, "side": "long",
                    "shares": 1, "entry": None, "exit": None,
                    "exit_how": "no_open", "pnl": None, "win": None,
                    "owner": "flatten_io", "kind": "no_open",
                })
                continue
            if dip_pct is None:
                continue
            fill, kind = fmc.dip_limit_px(o, low, dip_pct)
            if fill is None:
                continue
            exit_px, exit_d, how = horizon_exit(
                cal, date, FLATTEN_HOLD, t, bars)
            pnl = None
            if exit_px is not None:
                pnl = round(after_fee_pnl(
                    1, fill, float(exit_px), side="long", fees=fees), 4)
            fires.append({
                "date": date, "ticker": t, "side": "long",
                "shares": 1, "entry": round(fill, 4),
                "exit": None if exit_px is None else round(float(exit_px), 4),
                "exit_date": exit_d, "exit_how": how,
                "pnl": pnl,
                "win": None if pnl is None else bool(pnl > 0),
                "owner": "flatten_io",
                "kind": kind,
                "open": o, "low": low, "dip_pct": dip_pct,
            })
    return fires


def pick_best_x(grid_rows: list[dict]) -> dict | None:
    """Best X by after-fee win%, then n fires, then P&L. Thin still ranked."""
    ranked = []
    for row in grid_rows:
        wr = row.get("hard_red_win_rate")
        ranked.append((
            wr if wr is not None else -1.0,
            int(row.get("hard_red_fires") or 0),
            float(row.get("hard_red_pnl") or 0),
            -float(row.get("dip_pct") or 99),
            row,
        ))
    if not ranked:
        return None
    ranked.sort(reverse=True)
    return ranked[0][-1]


def decide_verdict(*, n_fires: int, win_rate, tapes_n: dict,
                   wf_ok: bool | None, label: str) -> dict:
    """KEEP only if the published bar is actually met.

    ≥30 fires, >55% after fees, both tapes when both can fire, and
    walk-forward OOS still clears the win bar when a fold exists.
    Thin n is KILL — not a live-policy wink.
    """
    wr = None if win_rate is None else float(win_rate)
    n = int(n_fires or 0)
    tape_notes = []
    usable_tapes = 0
    for tape, tn in (tapes_n or {}).items():
        tf = int((tn or {}).get("n_fires") or 0)
        tw = (tn or {}).get("win_rate")
        tape_notes.append(f"{tape} n={tf} win="
                          f"{'—' if tw is None else f'{100*tw:.0f}%'}")
        if tf > 0:
            usable_tapes += 1
    both = usable_tapes >= 2
    thin = n < KEEP_MIN_FIRES
    miss_win = wr is None or wr <= KEEP_WIN
    if thin:
        verdict = "KILL"
        why = (
            f"KILL {label}: thin n={n} fires (bar ≥{KEEP_MIN_FIRES}). "
            f"After-fee win "
            f"{'—' if wr is None else f'{100*wr:.1f}%'}. "
            + "; ".join(tape_notes)
            + ". Do not change live sit on this sample."
        )
    elif miss_win:
        verdict = "KILL"
        why = (
            f"KILL {label}: {n} fires but after-fee win "
            f"{100*wr:.1f}% ≤ {100*KEEP_WIN:.0f}%. "
            + "; ".join(tape_notes)
        )
    elif not both and "flatten" in (tapes_n or {}) and "webull" in (tapes_n or {}):
        # One tape could not fire (flatten has no short kid, or no scoops).
        verdict = "KILL"
        why = (
            f"KILL {label}: {n} fires and after-fee win {100*wr:.1f}% "
            f"but only one tape actually fired ({'; '.join(tape_notes)}). "
            "KEEP needs both tapes or an honest single-tape note that "
            "the other tape cannot apply."
        )
    elif wf_ok is False:
        verdict = "KILL"
        why = (
            f"KILL {label}: full-sample after-fee win {100*wr:.1f}% on "
            f"{n} fires, but walk-forward OOS missed the >55% bar."
        )
    else:
        verdict = "KEEP"
        why = (
            f"KEEP {label}: {n} fires, after-fee win {100*wr:.1f}% "
            f"(>{100*KEEP_WIN:.0f}%). " + "; ".join(tape_notes)
            + ("" if wf_ok is None else " Walk-forward OOS still clears.")
        )
    return {
        "label": verdict,
        "keep": verdict == "KEEP",
        "why": why,
        "n_fires": n,
        "win_rate": wr,
        "thin": thin,
        "both_tapes": both,
        "wf_ok": wf_ok,
        "tapes": tapes_n,
    }


def _wf_ok(oos_fires: list[dict]) -> bool | None:
    st = fire_stats(oos_fires)
    if st["n_fires"] == 0:
        return None
    wr = st["win_rate"]
    return bool(st["n_fires"] >= KEEP_MIN_FIRES and wr is not None
                and wr > KEEP_WIN)


def walkforward_combo(panel: dict, recs: list[dict], spec: dict, *,
                      bars, fees, regime) -> dict:
    cal = list(panel.get("session_dates") or [])
    folds = wf.make_folds(cal)
    rows = []
    oos_short: list[dict] = []
    oos_by_x: dict[float, list[dict]] = {x: [] for x in DIP_GRID}
    picked = []
    for spec_f in folds:
        is_p = wf.slice_panel(panel, end=spec_f["is_dates"][-1])
        oos_p = wf.slice_panel(
            panel, start=spec_f["oos_dates"][0],
            end=spec_f["oos_dates"][-1])
        is_grid = []
        for x in DIP_GRID:
            bk = simulate_combo(
                is_p, recs, spec, bars=bars, fees=fees, regime=regime,
                mode=fmc.HARD_RED_DIP_SCOOP, dip_pct=x)
            fr = fire_stats(hard_red_fires(
                bk, bars=bars, fees=fees, side="long"))
            is_grid.append(book_row(
                bk, fr, label=f"IS scoop {x:g}%",
                mode=fmc.HARD_RED_DIP_SCOOP, dip_pct=x))
        best = pick_best_x(is_grid)
        pick_x = None if not best else best.get("dip_pct")
        short_oos = simulate_combo(
            oos_p, recs, spec, bars=bars, fees=fees, regime=regime,
            mode=fmc.HARD_RED_SHORT_ONLY)
        oos_short.extend(hard_red_fires(
            short_oos, bars=bars, fees=fees, side="short"))
        fold_x = {}
        for x in DIP_GRID:
            bk = simulate_combo(
                oos_p, recs, spec, bars=bars, fees=fees, regime=regime,
                mode=fmc.HARD_RED_DIP_SCOOP, dip_pct=x)
            fires = hard_red_fires(bk, bars=bars, fees=fees, side="long")
            oos_by_x[x].extend(fires)
            fold_x[str(x)] = fire_stats(fires)
        picked.append({
            "cutoff": spec_f["cutoff"],
            "oos": f"{spec_f['oos_dates'][0]} → {spec_f['oos_dates'][-1]}",
            "picked_x": pick_x,
            "is_best": None if not best else {
                k: best.get(k) for k in (
                    "dip_pct", "hard_red_fires", "hard_red_win_rate",
                    "hard_red_pnl", "book_pct")
            },
            "oos_short": fire_stats(hard_red_fires(
                short_oos, bars=bars, fees=fees, side="short")),
            "oos_x": {k: {kk: vv for kk, vv in v.items() if kk != "fires"}
                      for k, v in fold_x.items()},
        })
        rows.append(spec_f)
    return {
        "n_folds": len(folds),
        "folds": picked,
        "oos_short": fire_stats(oos_short),
        "oos_x": {str(x): fire_stats(oos_by_x[x]) for x in DIP_GRID},
        "wf_short_ok": _wf_ok(oos_short),
        "wf_x_ok": {str(x): _wf_ok(oos_by_x[x]) for x in DIP_GRID},
    }


def _pin_would(date: str, tickers: list[str], side: str) -> list[dict]:
    return [{"date": date, "ticker": t, "kid_side": side,
             "sleeve": "pinned_war_room"} for t in tickers]


def counterfactual_0914(*, panel: dict, recs: list[dict], spec: dict,
                        bars, fees, regime, flatten_days: list[dict],
                        look_rows: list[dict] | None = None) -> dict:
    """What 2026-09-14 would have done under A (pre-open short) / B (intraday scoop)."""
    s = fmb.morning_s(regime, ASOF)
    rows = look_rows
    if rows is None:
        try:
            rows = cb.build_look_rows(ASOF)
        except Exception:
            rows = (panel.get("by_date") or {}).get(ASOF) or []
    would = []
    seen = set()
    for rec in recs:
        for r in fm.pick_day(rows or [], rec):
            t = fm._tick(r.get("ticker"))
            if not t or t in seen:
                continue
            seen.add(t)
            would.append({
                "ticker": t,
                "sleeve": rec["name"],
                "kid_side": rec.get("side") or "long",
                "clock": "09:30 ET",
                "source": "look",
            })
    pinned = (_pin_would(ASOF, list(WEBULL_0914_LONGS), "long")
              + _pin_would(ASOF, list(WEBULL_0914_SHORTS), "short"))
    for p in pinned:
        t = p["ticker"]
        if t in seen:
            continue
        seen.add(t)
        would.append({**p, "source": "war_room_pin"})
    flat_names = []
    for d in flatten_days:
        if d.get("date") == ASOF:
            flat_names = list(d.get("tickers") or [])
    if not flat_names:
        flat_names = list(FLATTEN_0914)
    for t in FLATTEN_0914:
        if t not in flat_names:
            flat_names.append(t)
    need = [w["ticker"] for w in would] + list(flat_names)
    overlay = yahoo_session_overlay(need, ASOF)
    if overlay:
        bars = dict(bars or {})
        for k, v in overlay.items():
            cur = bars.get(k) or {}
            if fm._finite(cur.get("open")) is None:
                bars[k] = v

    def _name_card(t: str, side: str, sleeve: str) -> dict:
        bar = clock_bar(t, ASOF, bars)
        o, low, c = bar.get("open"), bar.get("low"), bar.get("close")
        scoops = {}
        for x in DIP_GRID:
            fill, kind = fmc.dip_limit_px(o, low, x)
            pnl_sd = None
            if fill is not None and c is not None:
                pnl_sd = round(after_fee_pnl(
                    1, fill, c, side=side, fees=fees), 4)
            scoops[str(x)] = {
                "kind": kind,
                "fill": None if fill is None else round(fill, 4),
                "same_day_pnl": pnl_sd,
                "win": None if pnl_sd is None else bool(pnl_sd > 0),
            }
        short_pnl = None
        if side == "short" and o is not None and c is not None:
            short_pnl = round(after_fee_pnl(1, o, c, side="short", fees=fees), 4)
        letters = excel_letters(ASOF, t)
        gate = short_letter_eligible(letters) if side == "short" else None
        return {
            "ticker": t, "side": side, "sleeve": sleeve,
            "open": o, "low": low, "close": c,
            "px_src": bar.get("src") or ("parquet" if o is not None else None),
            "close_is_last": bar.get("src") == "yahoo_session",
            "dip_from_open_pct": (
                None if o is None or low is None or o <= 0
                else round(100.0 * (o - low) / o, 3)
            ),
            "same_day_oc_pnl_short": short_pnl,
            "scoops": scoops,
            "excel": gate,
            "letters": {k: letters.get(k) for k in OPEN_LETTERS},
            "letter_src": letters.get("src"),
            "no_price": o is None,
        }

    webull = [_name_card(w["ticker"], w["kid_side"], w.get("sleeve") or "")
              for w in would]
    flatten = [_name_card(t, "long", "flatten_io") for t in flat_names]
    return {
        "date": ASOF,
        "s": None if s is None else round(float(s), 3),
        "hard_red": s is not None and float(s) <= float(fmb.HARD_RED),
        "live": (
            f"combo {LIVE_COMBO} + flatten_robust sat every new lot "
            f"(S={s})"
        ),
        "webull_would": webull,
        "flatten_would": flatten,
        "note": (
            "Clock split: (A) short-only is a pre-open policy call — "
            "shorts fire at the clock-clean 09:30 open if S≤−3. Excel "
            "may pre-filter those names via open-knowable letters "
            "(FQ/ER/EP stretch); it does not own the sit. Deep-corr "
            "was not run. (B) long scoop is *intraday* after 09:30: "
            "trigger is first touch of open−X% (session low as the "
            "daily proxy). Close / last grades the fire — it does not "
            "trigger it. 09-14 Yahoo close is last-so-far, not 16:00. "
            "Flatten's live card used last-close for DK; scoop uses "
            "the official 09:30 open."
        ),
        "yahoo_overlay_n": len(overlay),
    }


def _pct(v) -> str:
    return "—" if v is None else f"{100.0 * float(v):.1f}%"


def _n(v) -> str:
    return "—" if v is None else f"{float(v):+.2f}"


def _pxs(v, n: int = 2) -> str:
    if v is None:
        return "—"
    try:
        return f"{float(v):.{n}f}"
    except (TypeError, ValueError):
        return "—"


def scoreboard_bars(date: str) -> dict[str, dict]:
    """Clock-clean 09:30 cards already on HARD_RED_SIT.json. Research only."""
    if not OUT_JSON.is_file():
        return {}
    try:
        data = json.loads(OUT_JSON.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    cf = data.get("counterfactual_0914") or {}
    if str(cf.get("date") or "") != date:
        return {}
    out: dict[str, dict] = {}
    for row in list(cf.get("webull_would") or []) + list(cf.get("flatten_would") or []):
        t = str(row.get("ticker") or "").upper()
        if t and t not in out:
            out[t] = row
    return out


def per_sleeve_from_tickets(payload: dict) -> dict:
    """Per-sleeve RESEARCH cards from open-pack tickets. Not a wire."""
    date = str(payload.get("clock_legal_for") or payload.get("date") or "")
    sleeves = []
    n_sit = 0
    n_named = 0
    for name, rec in (payload.get("strategies") or {}).items():
        if not isinstance(rec, dict):
            continue
        if not (rec.get("sit") or rec.get("hard_red")):
            continue
        n_sit += 1
        res = rec.get("research") or {}
        shorts = list(res.get("short_only") or [])
        longs = list(res.get("dip_scoop") or [])
        if shorts or longs:
            n_named += 1
        sleeves.append({
            "name": name,
            "family": rec.get("family"),
            "sit": True,
            "hard_red": bool(rec.get("hard_red")),
            "side": rec.get("side") or "long",
            "s": rec.get("s") or rec.get("why"),
            "short_only": shorts,
            "dip_scoop": longs,
            "tag": "RESEARCH",
            "live_sit": True,
        })
    return {
        "tag": "RESEARCH",
        "date": date,
        "live_sit": True,
        "keep_bar_unchanged": True,
        "n_sit": n_sit,
        "n_named": n_named,
        "grid": list(DIP_GRID),
        "note": (
            "Per-sleeve paper counterfactuals on looked names. "
            "Live sit stays default. KEEP bar unchanged. "
            "(A) short-only fires the short kid at the 09:30 open. "
            "(B) dip-scoop longs wait for open−X% (session low / Elite "
            "live). Close does not trigger."
        ),
        "sleeves": sleeves,
    }


def render_per_sleeve_md(block: dict) -> list[str]:
    """Markdown table: each sit sleeve's (A) shorts + (B) scoop grid."""
    if not block:
        return []
    grid = list(block.get("grid") or DIP_GRID)
    lines = [
        "## RESEARCH per sleeve (paper, not a wire)",
        "",
        f"**{block.get('date') or ''}** — `{block.get('n_named') or 0}` "
        f"sit sleeves with looked names / "
        f"`{block.get('n_sit') or 0}` sit sleeves total. "
        "Live policy sits. KEEP bar unchanged. "
        "Label: **RESEARCH**.",
        "",
        str(block.get("note") or ""),
        "",
        "| Sleeve | Side | (A) short-only @ open | (B) dip-scoop X% |",
        "|---|---|---|---|",
    ]
    for rec in block.get("sleeves") or []:
        shorts = rec.get("short_only") or []
        longs = rec.get("dip_scoop") or []
        if not shorts and not longs:
            continue
        a_bits = []
        for r in shorts:
            a_bits.append(
                f"{r.get('ticker')} @{_pxs(r.get('open'), 3)}"
            )
        b_bits = []
        for r in longs:
            hits = []
            for x in grid:
                sc = (r.get("scoops") or {}).get(str(x)) or {}
                if sc.get("kind") == "scoop":
                    hits.append(f"{x:g}")
            hit = ("scoop " + ",".join(hits) + "%") if hits else "miss"
            b_bits.append(f"{r.get('ticker')} {hit}")
        lines.append(
            f"| `{rec.get('name')}` | {rec.get('side') or ''} | "
            f"{'; '.join(a_bits) or '—'} | "
            f"{'; '.join(b_bits) or '—'} |"
        )
    lines += [
        "",
        "Scoop trigger = official open + session low (Elite live only "
        "when the low has not printed). Close / last / Theme Radar "
        "never trigger. #236 KILL of global short-only / dip-scoop "
        "stands — this table is display/paper only.",
        "",
    ]
    return lines


def write_per_sleeve(block: dict, *, write: bool = True) -> dict:
    """Merge per-sleeve RESEARCH into HARD_RED_SIT.md + JSON.

    Does not re-run the 22-day KEEP/KILL experiment and does not
    change flatten_robust / Webull live sit.
    """
    data: dict = {}
    if OUT_JSON.is_file():
        try:
            data = json.loads(OUT_JSON.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            data = {}
    data["per_sleeve"] = block
    if write:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        OUT_JSON.write_text(json.dumps(data, indent=2), encoding="utf-8")
        OUT_MD.write_text(render_md(data), encoding="utf-8")
    return data


def _plain_0914(cf: dict) -> list[str]:
    lines = [
        f"On **{cf.get('date')}** morning S was "
        f"**{cf.get('s')}** (hard-red). Live policy `{LIVE_COMBO}` "
        f"(#232) and `flatten_robust` sat every new lot.",
        "",
    ]
    web = cf.get("webull_would") or []
    longs = [r for r in web if r.get("side") == "long"]
    shorts = [r for r in web if r.get("side") == "short"]
    def _long_bit(r: dict) -> str:
        if r.get("no_price"):
            return f"{r['ticker']} (no 09:30 open)"
        dip = r.get("dip_from_open_pct")
        hits = []
        for x, sc in (r.get("scoops") or {}).items():
            if (sc or {}).get("kind") == "scoop":
                hits.append(f"{x}%")
        hit = ("would scoop at " + ", ".join(hits)) if hits else "no X on the grid hit"
        last = r.get("close")
        last_s = "" if last is None else (
            f", last {_pxs(last, 3)}"
            + (" (not 16:00)" if r.get("close_is_last") else "")
        )
        return (
            f"{r['ticker']} (open {_pxs(r.get('open'), 3)}, low −"
            f"{'—' if dip is None else f'{dip:.2f}'}% — {hit}{last_s})"
        )

    if longs:
        lines.append("Webull longs that sat: " + "; ".join(_long_bit(r) for r in longs) + ".")
    if shorts:
        bits = []
        for r in shorts:
            if r.get("no_price"):
                bits.append(f"{r['ticker']} (no 09:30 open)")
                continue
            last = r.get("close")
            how = "last" if r.get("close_is_last") else "close"
            pnl = _n(r.get("same_day_oc_pnl_short"))
            xl = r.get("excel") or {}
            xl_s = (
                f", Excel {xl.get('status')}"
                + (f" ({','.join(xl.get('hits') or [])})" if xl.get("hits") else "")
            )
            bits.append(
                f"{r['ticker']} (short at open {_pxs(r.get('open'))} → "
                f"{how} {_pxs(last)}; 1-share after-fee {pnl}, "
                f"fee-dominated, not cash-book size{xl_s})"
            )
        lines.append(
            "Webull shorts that sat (the short kid): "
            + "; ".join(bits) + "."
        )
    flat = cf.get("flatten_would") or []
    if flat:
        lines.append(
            "Flatten/io would-haves that sat: "
            + "; ".join(_long_bit(r) for r in flat) + "."
        )
    lines.append("")
    lines.append(str(cf.get("note") or ""))
    return lines


def render_md(payload: dict) -> str:
    v_s = payload.get("verdict_short") or {}
    v_sx = payload.get("verdict_short_excel") or {}
    v_x = payload.get("verdict_x") or {}
    best = payload.get("best_x") or {}
    cf = payload.get("counterfactual_0914") or {}
    red = payload.get("hard_red_days") or []
    lines = [
        "# Hard-red sit experiment",
        "",
        f"**A short-only (pre-open): {v_s.get('label') or 'KILL'}.** "
        f"**B dip-scoop X={best.get('dip_pct') if best else '—'}% "
        f"(intraday): {v_x.get('label') or 'KILL'}.**",
        "",
        "Research only. Live `flatten_robust` buys and Webull "
        f"`{LIVE_COMBO}` stay on full hard-red sit. This board does "
        "not wire either idea. Excel does not own the sit. Deep-corr "
        "was not waited on.",
        "",
        "## Clock split (Manager lock)",
        "",
        "**A short-only** is a **pre-open** policy decision, made "
        "before 09:30 when morning S is already ≤−3. The short kid "
        "may fire at the official open. Excel may pre-filter "
        "short-eligible names via open-knowable CLEAR letters only "
        "(`FQ` / `ER` / `EP` stretch = yesterday already ran). Those "
        "letters CLEAR as *long avoids* on the Yahoo analog — they "
        "are a fade hint here, **not** a short CLEAR. Missing letters "
        "pass through. Excel does **not** own the sit.",
        "",
        "**B long dip-scoop** is **intraday**, after 09:30. Open is "
        "known. The trigger is first touch of open−X% on same-day "
        "OHLC (session low = daily first-hit proxy). Close / last / "
        "Gap / Finviz Price never trigger. Close only grades after "
        "fees.",
        "",
        "## Board — what would have happened",
        "",
        str(v_s.get("why") or ""),
        "",
        str(v_x.get("why") or ""),
        "",
        *_plain_0914(cf),
        "",
        *render_per_sleeve_md(payload.get("per_sleeve") or {}),
        f"Window `{payload.get('from_date')} → {payload.get('to_date')}` "
        f"({payload.get('n_sessions')} sessions). Hard-red mornings "
        f"(S≤{fmb.HARD_RED:g}): **{len(red)}** — "
        + (", ".join(f"`{r['date']}` S={r['s']}" for r in red) or "none")
        + ".",
        "",
        "Book% on the continuous $10k combo path is **not** the KEEP "
        "bar. Scoop / short-only books look richer because those extra "
        "lots stay held into later non-red days. KEEP only grades the "
        "hard-red **fires** after Futubull fees. A fat Book% with a "
        "coin-flip hard-red win rate is still KILL.",
        "",
        "## KEEP bar",
        "",
        f"Need **≥{KEEP_MIN_FIRES} fires** where the tape can print "
        f"them, **> {100*KEEP_WIN:.0f}% after Futubull fees**, and "
        "**both tapes** (Webull combo + flatten/io) when that tape can "
        "actually fire. Walk-forward mines X on hidden windows; a "
        "full-sample winner that dies OOS is KILL. Thin n is KILL.",
        "",
        "After-fee caveat: every graded fire pays the Futubull US "
        "round-trip. Scoop **trigger** is open + session low only "
        "(first touch of open−X%). Close is the grade, not the "
        "trigger. Daily OHLC cannot prove the print happened after "
        "09:30, so scoop P&L is slightly optimistic. A missing 09:30 "
        "open is a skip — never Gap, last, or prior close. 09-14 may "
        "lack a 16:00 mark; those rows stay ungraded.",
        "",
        "## Webull combo tape",
        "",
        f"Cash book is the audited `simulate_shared` ledger on "
        f"`{LIVE_COMBO}` (short news🔴 ∩ MACD-up + hot-4, shared "
        f"50/50, $10k, whole shares, sell first). Live sit is the "
        f"control. (A) is pre-open short-only. (B) is the after-09:30 "
        f"open−X% scoop (low touches the limit; close grades).",
        "",
        "| Variant | Mode | Book% | Hard-red fires | After-fee win | "
        "Hard-red $ | Book win | Audit |",
        "|---|---|---:|---:|---:|---:|---:|---|",
    ]
    for row in payload.get("webull_rows") or []:
        x = row.get("dip_pct")
        mode = row.get("mode")
        if x is not None:
            mode = f"{mode} {x:g}%"
        lines.append(
            f"| {row.get('label')} | `{mode}` | "
            f"{_n(row.get('book_pct'))} | {row.get('hard_red_fires') or 0} | "
            f"{_pct(row.get('hard_red_win_rate'))} | "
            f"{_n(row.get('hard_red_pnl'))} | "
            f"{_pct(row.get('book_win_rate'))} | "
            f"{'PASS' if row.get('audit_ok') else 'FAIL'} |"
        )
    lines += [
        "",
        "## Flatten / .io tape",
        "",
        "Long book only (`flatten_robust` has no short kid). Live sit "
        "and (A) short-only print **zero** new lots — same as live. "
        "(B) scoops the hard-red would-have .io names after open−X%, "
        f"1 share each, exit at the {FLATTEN_HOLD}d horizon close. "
        "Trigger still uses open+low only.",
        "",
        "| Variant | Hard-red fires | After-fee win | After-fee $ |",
        "|---|---:|---:|---:|",
    ]
    for row in payload.get("flatten_rows") or []:
        x = row.get("dip_pct")
        lab = row.get("label")
        if x is not None:
            lab = f"{lab} {x:g}%"
        lines.append(
            f"| {lab} | {row.get('n_fires') or 0} | "
            f"{_pct(row.get('win_rate'))} | {_n(row.get('pnl'))} |"
        )
    wf = payload.get("walkforward") or {}
    lines += [
        "",
        "## Walk-forward (mine X, freeze, score hidden)",
        "",
        f"{wf.get('n_folds') or 0} folds — same cut as "
        f"`walkforward_factor_mine` (first cutoff 2026-08-20, step 4, "
        f"forward 4). Each cutoff picks the IS scoop X with the best "
        f"after-fee hard-red win%, then scores that logic on the hidden "
        f"window. Short-only has nothing to mine; OOS fires are pooled.",
        "",
        "| Cutoff | Hidden | IS best X | IS win | OOS short n/win | "
        + " | ".join(f"OOS {x:g}%" for x in DIP_GRID) + " |",
        "|---|---|---:|---:|---:|" + "---:|" * len(DIP_GRID),
    ]
    for f in wf.get("folds") or []:
        ib = f.get("is_best") or {}
        sh = f.get("oos_short") or {}
        xs = f.get("oos_x") or {}
        bits = []
        for x in DIP_GRID:
            st = xs.get(str(x)) or xs.get(x) or {}
            bits.append(f"{st.get('n_fires') or 0}/{_pct(st.get('win_rate'))}")
        lines.append(
            f"| `{f.get('cutoff')}` | {f.get('oos')} | "
            f"{ib.get('dip_pct') if ib else '—'} | "
            f"{_pct(ib.get('hard_red_win_rate'))} | "
            f"{sh.get('n_fires') or 0}/{_pct(sh.get('win_rate'))} | "
            + " | ".join(bits) + " |"
        )
    oos_s = wf.get("oos_short") or {}
    lines += [
        "",
        f"Pooled OOS short-only: n={oos_s.get('n_fires') or 0} "
        f"win={_pct(oos_s.get('win_rate'))} $={_n(oos_s.get('pnl'))}.",
        "",
        "Pooled OOS scoop by X:",
        "",
        "| X | n | After-fee win | $ |",
        "|---:|---:|---:|---:|",
    ]
    for x in DIP_GRID:
        st = (wf.get("oos_x") or {}).get(str(x)) or {}
        lines.append(
            f"| {x:g}% | {st.get('n_fires') or 0} | "
            f"{_pct(st.get('win_rate'))} | {_n(st.get('pnl'))} |"
        )
    lines += [
        "",
        "## Gate (do not change live)",
        "",
        "Hard-red S≤−3 currently blocks **long and short** new lots in:",
        "",
        "1. `src/combo_broker.py` `size_combo_tickets` — Webull paper "
        f"`{LIVE_COMBO}` tickets (`hard_red` → skip every kid).",
        "2. `src/factor_mine_combo.py` `simulate_shared` — the cash "
        "book used to grade that combo.",
        "3. `src/factor_mine_book.py` `simulate_book` / "
        "`flatten_robust` `hard_red_no_new` — flatten/io sits new buys.",
        "",
        "This experiment adds opt-in `hard_red_mode` "
        "(`sit` / `short_only` / `dip_scoop` / `short_and_scoop`) "
        "with default **sit**. Live callers do not pass a mode.",
        "",
        "## KEEP / KILL",
        "",
        f"**A short-only (pre-open): {v_s.get('label')}.** {v_s.get('why')}",
        "",
        f"**A · Excel letter pre-filter: {v_sx.get('label') or 'KILL'}.** "
        f"{v_sx.get('why') or ''}",
        "",
        f"**B dip-scoop X={best.get('dip_pct') if best else '—'}% "
        f"(intraday): {v_x.get('label')}.** {v_x.get('why')}",
        "",
        "Do not merge a live policy change from this PR.",
        "",
    ]
    return "\n".join(lines)


def _slim_fires(st: dict, limit: int = 80) -> dict:
    out = {k: v for k, v in st.items() if k != "fires"}
    out["fires"] = list(st.get("fires") or [])[:limit]
    return out


def run(*, from_date: str = WINDOW_START, to_date: str | None = ASOF,
        write: bool = False, panel: dict | None = None, bars=None,
        fees=None, regime=None, flatten_days: list[dict] | None = None,
        look_rows_0914: list[dict] | None = None,
        skip_look: bool = False) -> dict:
    recipes = fm.build_recipes()
    spec, recs = combo_recs(recipes)
    if panel is None:
        panel = fm.load_or_build_panel(from_date, None)
        if (not skip_look and to_date
                and to_date not in (panel.get("session_dates") or [])):
            panel = extend_panel(panel, to_date)
    cal = [d for d in (panel.get("session_dates") or [])
           if d >= from_date and (not to_date or d <= to_date)]
    if bars is None:
        bars = wf.preload_bars(panel)
        # Official lows for flatten names / 09-14 pins that may sit
        # outside the combo panel.
        extra = set(FLATTEN_0914) | set(WEBULL_0914_LONGS) | set(WEBULL_0914_SHORTS)
        for t in extra:
            for d in cal:
                if (t, d) not in bars:
                    bars[(t, d)] = clock_bar(t, d, None)
    fees = fees if fees is not None else pt.load_fees()
    if regime is None:
        try:
            regime = fmb.load_regime()
        except Exception:
            regime = {}
    red = hard_red_dates(cal, regime)
    webull_rows = []
    books = {}

    sit = simulate_combo(
        panel, recs, spec, bars=bars, fees=fees, regime=regime,
        mode=fmc.HARD_RED_SIT)
    sit_f = fire_stats(hard_red_fires(sit, bars=bars, fees=fees))
    webull_rows.append(book_row(
        sit, sit_f, label="live sit (control)", mode=fmc.HARD_RED_SIT))
    books["sit"] = sit

    short = simulate_combo(
        panel, recs, spec, bars=bars, fees=fees, regime=regime,
        mode=fmc.HARD_RED_SHORT_ONLY)
    short_raw = hard_red_fires(short, bars=bars, fees=fees, side="short")
    letter_idx = load_letter_index()
    short_stamped = stamp_excel_shorts(short_raw, index=letter_idx)
    short_f = fire_stats(short_stamped)
    short_xl = fire_stats([
        f for f in short_stamped
        if (f.get("excel") or {}).get("ok")
        and (f.get("excel") or {}).get("status") != "ineligible"
    ])
    # unknown letters pass through; ineligible drop.
    webull_rows.append(book_row(
        short, short_f, label="(A) short-only · pre-open",
        mode=fmc.HARD_RED_SHORT_ONLY))
    webull_rows.append(book_row(
        short, short_xl,
        label="(A) short-only · Excel letter pre-filter",
        mode="short_only_excel"))
    books["short_only"] = short

    grid = []
    for x in DIP_GRID:
        bk = simulate_combo(
            panel, recs, spec, bars=bars, fees=fees, regime=regime,
            mode=fmc.HARD_RED_DIP_SCOOP, dip_pct=x)
        fr = fire_stats(hard_red_fires(
            bk, bars=bars, fees=fees, side="long"))
        row = book_row(
            bk, fr, label=f"(B) scoop {x:g}% · after 09:30",
            mode=fmc.HARD_RED_DIP_SCOOP, dip_pct=x)
        webull_rows.append(row)
        grid.append(row)
        books[f"scoop_{x:g}"] = bk

    if flatten_days is None:
        try:
            flatten_days = flatten_woulds(cal)
        except Exception:
            flatten_days = []
    flat_rows = [
        {"label": "live sit (control)", "mode": fmc.HARD_RED_SIT,
         **fire_stats([])},
        {"label": "(A) short-only (N/A — flatten has no short kid)",
         "mode": fmc.HARD_RED_SHORT_ONLY, **fire_stats([])},
    ]
    flat_grid = []
    for x in DIP_GRID:
        fires = flatten_name_days(
            flatten_days, cal, bars=bars, fees=fees,
            dip_pct=x, mode=fmc.HARD_RED_DIP_SCOOP)
        st = fire_stats(fires)
        row = {"label": "(B) scoop", "mode": fmc.HARD_RED_DIP_SCOOP,
               "dip_pct": x, **st}
        flat_rows.append(row)
        flat_grid.append(row)

    best_x = pick_best_x(grid)
    best_flat = pick_best_x([
        {"dip_pct": r.get("dip_pct"),
         "hard_red_fires": r.get("n_fires"),
         "hard_red_win_rate": r.get("win_rate"),
         "hard_red_pnl": r.get("pnl")}
        for r in flat_grid
    ])

    wf_payload = walkforward_combo(
        panel, recs, spec, bars=bars, fees=fees, regime=regime)

    # Short-only: flatten cannot fire. Treat that as an honest
    # single-tape case (not a both-tape fail) — the other tape has
    # no short kid.
    short_tapes = {
        "webull": {k: short_f.get(k) for k in
                   ("n_fires", "win_rate", "pnl", "n_graded")},
        "flatten": {"n_fires": 0, "win_rate": None, "pnl": 0,
                    "n_graded": 0, "note": "no short kid"},
    }
    v_short = decide_verdict(
        n_fires=short_f.get("n_fires") or 0,
        win_rate=short_f.get("win_rate"),
        tapes_n=short_tapes,
        wf_ok=wf_payload.get("wf_short_ok"),
        label="A short-only (pre-open)",
    )
    v_short_xl = decide_verdict(
        n_fires=short_xl.get("n_fires") or 0,
        win_rate=short_xl.get("win_rate"),
        tapes_n={"webull": {k: short_xl.get(k) for k in
                            ("n_fires", "win_rate", "pnl")}},
        wf_ok=None,
        label="A short-only · Excel letter pre-filter",
    )
    # Single-tape exception: flatten cannot apply. Re-grade without
    # demanding both tapes, but keep thin / win / WF gates.
    if v_short["label"] == "KILL" and not v_short["thin"] and (
            short_f.get("win_rate") or 0) > KEEP_WIN and (
            (short_f.get("n_fires") or 0) >= KEEP_MIN_FIRES):
        if wf_payload.get("wf_short_ok") is not False:
            v_short = decide_verdict(
                n_fires=short_f.get("n_fires") or 0,
                win_rate=short_f.get("win_rate"),
                tapes_n={"webull": short_tapes["webull"]},
                wf_ok=wf_payload.get("wf_short_ok"),
                label="hard-red short-only (Webull tape only; flatten has no short kid)",
            )

    bx = None if not best_x else best_x.get("dip_pct")
    web_best = best_x or {}
    flat_for_x = next((r for r in flat_grid if r.get("dip_pct") == bx), None)
    if flat_for_x is None and best_flat:
        flat_for_x = next((r for r in flat_grid
                           if r.get("dip_pct") == best_flat.get("dip_pct")),
                          None)
    x_tapes = {
        "webull": {k: web_best.get(k) for k in
                   ("hard_red_fires", "hard_red_win_rate", "hard_red_pnl")},
        "flatten": {k: (flat_for_x or {}).get(k) for k in
                    ("n_fires", "win_rate", "pnl")},
    }
    # Normalize keys
    x_tapes = {
        "webull": {
            "n_fires": web_best.get("hard_red_fires") or 0,
            "win_rate": web_best.get("hard_red_win_rate"),
            "pnl": web_best.get("hard_red_pnl"),
        },
        "flatten": {
            "n_fires": (flat_for_x or {}).get("n_fires") or 0,
            "win_rate": (flat_for_x or {}).get("win_rate"),
            "pnl": (flat_for_x or {}).get("pnl"),
        },
    }
    n_x = (x_tapes["webull"]["n_fires"] + x_tapes["flatten"]["n_fires"])
    # Pooled win across tapes (equal name-day / fill).
    w_n = (web_best.get("hard_red_n_graded") or 0) + (
        (flat_for_x or {}).get("n_graded") or 0)
    w_w = (web_best.get("hard_red_n_wins") or 0) + (
        (flat_for_x or {}).get("n_wins") or 0)
    pooled_wr = None if not w_n else round(w_w / w_n, 4)
    wf_x = None
    if bx is not None:
        wf_x = (wf_payload.get("wf_x_ok") or {}).get(str(bx))
    v_x = decide_verdict(
        n_fires=n_x,
        win_rate=pooled_wr,
        tapes_n=x_tapes,
        wf_ok=wf_x,
        label=f"B dip-scoop X={bx if bx is not None else '—'}% (intraday)",
    )

    cf = counterfactual_0914(
        panel=panel, recs=recs, spec=spec, bars=bars, fees=fees,
        regime=regime, flatten_days=flatten_days,
        look_rows=look_rows_0914,
    )
    payload = {
        "generated_at": datetime.now(tl.ET).isoformat(),
        "live_untouched": ["flatten_robust", LIVE_COMBO],
        "combo": LIVE_COMBO,
        "from_date": cal[0] if cal else from_date,
        "to_date": cal[-1] if cal else to_date,
        "n_sessions": len(cal),
        "hard_red_n": len(red),
        "hard_red_days": red,
        "keep_bar": {"min_fires": KEEP_MIN_FIRES, "win": KEEP_WIN},
        "dip_grid": list(DIP_GRID),
        "webull_rows": webull_rows,
        "flatten_rows": [
            {k: v for k, v in r.items() if k != "fires"} for r in flat_rows
        ],
        "flatten_days": flatten_days,
        "best_x": None if not best_x else {
            k: best_x.get(k) for k in (
                "dip_pct", "hard_red_fires", "hard_red_win_rate",
                "hard_red_pnl", "book_pct", "label")
        },
        "best_x_flatten": best_flat,
        "walkforward": {
            "n_folds": wf_payload.get("n_folds"),
            "folds": wf_payload.get("folds"),
            "oos_short": _slim_fires(wf_payload.get("oos_short") or {}),
            "oos_x": {k: _slim_fires(v)
                      for k, v in (wf_payload.get("oos_x") or {}).items()},
            "wf_short_ok": wf_payload.get("wf_short_ok"),
            "wf_x_ok": wf_payload.get("wf_x_ok"),
        },
        "clock_split": {
            "A": "short-only · pre-open policy · Excel letters may filter",
            "B": "long dip-scoop · after 09:30 · open known, close grades",
            "excel_owns_sit": False,
            "deep_corr": "not waited",
            "open_letters": list(OPEN_LETTERS),
            "short_stretch": list(SHORT_STRETCH),
        },
        "verdict_short": v_short,
        "verdict_short_excel": v_short_xl,
        "excel_short": {
            k: short_xl.get(k) for k in
            ("n_fires", "n_graded", "n_wins", "win_rate", "pnl")
        },
        "verdict_x": v_x,
        "counterfactual_0914": cf,
        "sit_hard_red_fires": sit_f.get("n_fires"),
        "gate": {
            "combo_broker": "size_combo_tickets hard_red → skip long+short",
            "simulate_shared": "hard_red → no new lots (default sit)",
            "flatten_robust": "hard_red_no_new sits .io / mover buys",
        },
    }
    if write:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        slim = dict(payload)
        # Drop full trade blotters; keep scoreboard + 09-14 cards.
        OUT_JSON.write_text(json.dumps(slim, indent=2), encoding="utf-8")
        OUT_MD.write_text(render_md(payload), encoding="utf-8")
        # Mirror next to other research boards.
        (ROOT / "03_scoreboard" / "HARD_RED_SIT.md").write_text(
            render_md(payload), encoding="utf-8")
    return payload


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="Hard-red short-only + dip-scoop research board "
                    "(does not change live policy).")
    ap.add_argument("--from-date", default=WINDOW_START)
    ap.add_argument("--to-date", default=ASOF)
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--skip-look", action="store_true",
                    help="do not rebuild 09-14 look rows")
    args = ap.parse_args(argv)
    payload = run(
        from_date=args.from_date, to_date=args.to_date or None,
        write=args.write, skip_look=args.skip_look)
    vs = payload.get("verdict_short") or {}
    vx = payload.get("verdict_x") or {}
    print(
        f"[hard-red-sit] sessions={payload.get('n_sessions')} "
        f"hard-red={payload.get('hard_red_n')} "
        f"short={vs.get('label')} x={vx.get('label')} "
        f"best_x={(payload.get('best_x') or {}).get('dip_pct')}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
