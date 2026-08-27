"""Re-rank one day's book with NO weights.

Standalone score = simple average of the six raw layers
(join, sector, general, news, AB, peer). No mid-opp, no rebound,
no persist penalty, no heat scale.

Tape score = average of AB + peer only (the name-specific pair).

Same buy gates as the live ranker so the 15-name lists are comparable.

  python -m src.lookback_unweighted --date 2026-08-20 --top 15
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from .book_learn import _load_panel, _select_buys, load_frame
from .book_lookback import BOOK_DIR, DAILY, SCORE, _jload, _parse_date, _tick
from .stock_book import MAX_OPP_MCAP_M, MIN_OPP_MCAP_M

LAYERS = ("s_join", "s_sector", "s_general", "s_news", "s_ab", "s_peer")
YF_H = {"1d": 1, "2d": 2, "3d": 3, "1w": 5}


def _num(x, nd=2) -> str:
    try:
        return f"{float(x):+.{nd}f}"
    except (TypeError, ValueError):
        return "—"


def _fmt(v) -> str:
    return "n/a" if v is None else f"{v:+.1f}%"


def _yf_forwards(date: str, tickers: list[str]) -> dict[str, dict[str, float | None]]:
    tickers = sorted({_tick(t) for t in tickers if _tick(t)})
    blank = {h: None for h in YF_H}
    out = {t: dict(blank) for t in tickers}
    if not tickers:
        return out
    try:
        import yfinance as yf
    except ImportError:
        return out
    start = datetime.fromisoformat(date).date()
    end = start + timedelta(days=18)
    try:
        data = yf.download(
            tickers=tickers,
            start=start.isoformat(),
            end=(end + timedelta(days=1)).isoformat(),
            group_by="ticker",
            auto_adjust=True,
            threads=True,
            progress=False,
        )
    except Exception as e:
        print(f"[unweighted] yfinance failed: {e}")
        return out
    if data is None or data.empty:
        return out

    def closes_of(t: str) -> pd.Series:
        try:
            if isinstance(data.columns, pd.MultiIndex):
                if t not in data.columns.get_level_values(0):
                    return pd.Series(dtype=float)
                return data[t]["Close"].dropna()
            return data["Close"].dropna() if len(tickers) == 1 else pd.Series(dtype=float)
        except Exception:
            return pd.Series(dtype=float)

    for t in tickers:
        close = closes_of(t)
        if close.empty or len(close) < 2:
            continue
        entry = float(close.iloc[0])
        if not entry:
            continue
        for h, n in YF_H.items():
            if len(close) > n:
                out[t][h] = float(close.iloc[n]) / entry - 1.0
            elif h == "1w" and len(close) >= 2:
                out[t][h] = float(close.iloc[-1]) / entry - 1.0
    return out


def _mean_fwd(rows: list[dict], h: str) -> float | None:
    vals = []
    for r in rows:
        v = (r.get("fwd") or {}).get(h)
        if v is not None:
            vals.append(float(v))
    return float(np.mean(vals)) if vals else None


def _row_from_frame(df: pd.DataFrame, i: int, standalone: float, tape: float) -> dict:
    r = df.iloc[i]
    return {
        "ticker": str(r["Ticker"]),
        "standalone": round(float(standalone), 4),
        "tape": round(float(tape), 4),
        "book": None if pd.isna(r.get("score_1d")) else round(float(r.get("score_1d")), 4),
        "s_join": float(r.get("s_join") or 0),
        "s_sector": float(r.get("s_sector") or 0),
        "s_general": float(r.get("s_general") or 0),
        "s_news": float(r.get("s_news") or 0),
        "s_ab": float(r.get("s_ab") or 0),
        "s_peer": float(r.get("s_peer") or 0),
        "s_opp": float(r.get("s_opp") or 0),
        "size": r.get("size"),
        "sector": r.get("sector"),
        "fwd": {},
    }


def run(date: str | None = None, top_n: int = 15) -> dict:
    date = _parse_date(date)
    df = load_frame(date)
    if df is None or df.empty:
        raise SystemExit(f"no stock book frame for {date}")

    for c in LAYERS:
        if c not in df.columns:
            df[c] = 0.0
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    if "s_opp" not in df.columns:
        df["s_opp"] = 0.0
    if "score_1d" not in df.columns:
        df["score_1d"] = np.nan

    standalone = df[list(LAYERS)].mean(axis=1).to_numpy(dtype=float)
    tape = df[["s_ab", "s_peer"]].mean(axis=1).to_numpy(dtype=float)
    df = df.copy()
    df["standalone"] = standalone
    df["tape"] = tape

    stand_idx = _select_buys(df, standalone, top_n)
    tape_idx = _select_buys(df, tape, top_n)

    book = _jload("data", "stock_book", f"{date}_stock_book.json") or {}
    live_buys = [
        _tick(r.get("ticker"))
        for r in ((book.get("books") or {}).get("1d") or {}).get("buy") or []
    ]
    live_set = set(live_buys)

    stand_rows = [_row_from_frame(df, i, standalone[i], tape[i]) for i in stand_idx]
    tape_rows = [_row_from_frame(df, i, standalone[i], tape[i]) for i in tape_idx]

    live_rows = []
    by_t = {str(t).upper(): i for i, t in enumerate(df["Ticker"].astype(str))}
    for t in live_buys:
        i = by_t.get(t)
        if i is None:
            continue
        live_rows.append(_row_from_frame(df, i, standalone[i], tape[i]))

    need = [r["ticker"] for r in stand_rows + tape_rows + live_rows]
    # also rank where SLS-like names land
    watch = ["SLS", "ARCT", "CYPH", "ASST", "BTDR", "VIRT", "FIGR", "ELF", "AUPH"]
    watch_rows = []
    for t in watch:
        i = by_t.get(t)
        if i is None:
            continue
        watch_rows.append(_row_from_frame(df, i, standalone[i], tape[i]))
        need.append(t)

    yf = _yf_forwards(date, need)
    for rows in (stand_rows, tape_rows, live_rows, watch_rows):
        for r in rows:
            got = yf.get(r["ticker"]) or {}
            r["fwd"] = {
                h: (None if got.get(h) is None else round(float(got[h]) * 100, 2))
                for h in YF_H
            }

    stand_set = {r["ticker"] for r in stand_rows}
    tape_set = {r["ticker"] for r in tape_rows}
    payload = {
        "date": date,
        "definition": {
            "standalone": "mean(join, sector, general, news, AB, peer) — no weights, no opp/rebound",
            "tape": "mean(AB, peer) — name-specific only",
            "gates": f"same as live: skip micro/<${MIN_OPP_MCAP_M:.0f}M, max 4 large, 4/sector, 3/industry",
        },
        "live_buy": live_rows,
        "standalone_buy": stand_rows,
        "tape_buy": tape_rows,
        "watch": watch_rows,
        "overlap_standalone_vs_live": sorted(stand_set & live_set),
        "entered_standalone": sorted(stand_set - live_set),
        "dropped_standalone": sorted(live_set - stand_set),
        "overlap_tape_vs_live": sorted(tape_set & live_set),
        "entered_tape": sorted(tape_set - live_set),
        "dropped_tape": sorted(live_set - tape_set),
        "avg_fwd": {
            "live": {h: _mean_fwd(live_rows, h) for h in YF_H},
            "standalone": {h: _mean_fwd(stand_rows, h) for h in YF_H},
            "tape": {h: _mean_fwd(tape_rows, h) for h in YF_H},
        },
    }

    # universe ranks for watch names
    order_stand = np.argsort(-standalone)
    order_tape = np.argsort(-tape)
    rank_stand = {str(df.iloc[i]["Ticker"]): n for n, i in enumerate(order_stand, 1)}
    rank_tape = {str(df.iloc[i]["Ticker"]): n for n, i in enumerate(order_tape, 1)}
    # gated rank = position among names that pass the $400M walk
    for r in watch_rows:
        r["rank_standalone_raw"] = rank_stand.get(r["ticker"])
        r["rank_tape_raw"] = rank_tape.get(r["ticker"])
        mcap = 0.0
        i = by_t.get(r["ticker"])
        if i is not None:
            try:
                mcap = float(df.iloc[i].get("market_cap_m") or 0)
            except (TypeError, ValueError):
                mcap = 0.0
        r["mcap"] = mcap
        r["gated"] = str(r.get("size") or "").lower() == "micro" or mcap < MIN_OPP_MCAP_M

    out_js = BOOK_DIR / f"{date}_unweighted.json"
    BOOK_DIR.mkdir(parents=True, exist_ok=True)
    out_js.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")

    block = render(payload)
    for path in (SCORE / "BOOK_LOOKBACK.md", DAILY / f"{date}_lookback.md"):
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("# Book lookback — " + date + "\n\n" + block, encoding="utf-8")
            continue
        text = path.read_text(encoding="utf-8")
        if "## Unweighted book" in text:
            pre, _mid, rest = text.partition("## Unweighted book")
            cut = rest.find("\n## Files")
            tail = rest[cut:] if cut >= 0 else ""
            text = pre.rstrip() + "\n\n" + block + tail.lstrip("\n")
        elif "## Files" in text:
            text = text.replace("## Files", block + "## Files", 1)
        else:
            text = text.rstrip() + "\n\n" + block
        path.write_text(text, encoding="utf-8")
    print(block)
    print(f"[unweighted] wrote {out_js}")
    return payload


def _avg_cell(d: dict | None, h: str) -> str:
    if not d or d.get(h) is None:
        return "n/a"
    return f"{d[h]:+.2f}%"


def _table(rows: list[dict], score_key: str) -> list[str]:
    L = [
        "| # | Ticker | stand | tape | book | join | AB | peer | opp | 1d | 1w | size |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for i, r in enumerate(rows, 1):
        fp = r.get("fwd") or {}
        L.append(
            f"| {i} | {r['ticker']} | {r['standalone']:+.3f} | {r['tape']:+.3f} | "
            f"{_num(r.get('book'), 3)} | {_num(r.get('s_join'))} | "
            f"{_num(r.get('s_ab'))} | {_num(r.get('s_peer'))} | {_num(r.get('s_opp'))} | "
            f"{_fmt(fp.get('1d'))} | {_fmt(fp.get('1w'))} | {r.get('size') or '?'} |"
        )
    return L


def render(p: dict) -> str:
    avg = p.get("avg_fwd") or {}
    L = [
        "## Unweighted book — same day, weights stripped",
        "",
        p["definition"]["standalone"] + ".",
        p["definition"]["tape"] + ".",
        p["definition"]["gates"] + ".",
        "",
        "| Book | names kept vs live | entered | dropped | avg 1d | avg 1w |",
        "|------|--------------------|---------|---------|--------|--------|",
        f"| live weighted | {len(p.get('live_buy') or [])} | — | — | "
        f"{_avg_cell(avg.get('live'), '1d')} | {_avg_cell(avg.get('live'), '1w')} |",
        f"| standalone equal-mean | overlap {len(p.get('overlap_standalone_vs_live') or [])} | "
        f"{', '.join(p.get('entered_standalone') or []) or '—'} | "
        f"{', '.join(p.get('dropped_standalone') or []) or '—'} | "
        f"{_avg_cell(avg.get('standalone'), '1d')} | {_avg_cell(avg.get('standalone'), '1w')} |",
        f"| tape AB+peer only | overlap {len(p.get('overlap_tape_vs_live') or [])} | "
        f"{', '.join(p.get('entered_tape') or []) or '—'} | "
        f"{', '.join(p.get('dropped_tape') or []) or '—'} | "
        f"{_avg_cell(avg.get('tape'), '1d')} | {_avg_cell(avg.get('tape'), '1w')} |",
        "",
        "### Live weighted BUY (what actually printed)",
        "",
        *_table(p.get("live_buy") or [], "book"),
        "",
        "### Standalone BUY (equal mean of 6 layers, same gates)",
        "",
        *_table(p.get("standalone_buy") or [], "standalone"),
        "",
        "### Tape BUY (mean of AB + peer only, same gates)",
        "",
        *_table(p.get("tape_buy") or [], "tape"),
        "",
        "### Watch names — where they sit with no weights",
        "",
        "| Ticker | stand | tape | book | raw stand rank | raw tape rank | gated out? | 1d | 1w |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for r in p.get("watch") or []:
        fp = r.get("fwd") or {}
        L.append(
            f"| {r['ticker']} | {r['standalone']:+.3f} | {r['tape']:+.3f} | "
            f"{_num(r.get('book'), 3)} | {r.get('rank_standalone_raw') or '—'} | "
            f"{r.get('rank_tape_raw') or '—'} | "
            f"{'yes' if r.get('gated') else 'no'} | {_fmt(fp.get('1d'))} | {_fmt(fp.get('1w'))} |"
        )
    L.append("")
    return "\n".join(L) + "\n"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--top", type=int, default=15)
    args = ap.parse_args()
    run(date=args.date, top_n=args.top)


if __name__ == "__main__":
    main()
