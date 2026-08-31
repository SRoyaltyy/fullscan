"""What the 15-name book would be if it filled from the green pile first.

  python -m src.lookback_green --date 2026-08-20 --top 15
  python -m src.lookback_green --all
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from .book_learn import _fwd_returns, _load_panel, _select_buys, load_frame
from .book_lookback import BOOK_DIR, SCORE, _jload, _parse_date, _tick
from .green_pile import GREEN_MIN, attach_ranks, green_mask

YF_H = {"1d": 1, "2d": 2, "3d": 3, "1w": 5}
SUMMARY = SCORE / "BOOK_GREEN.md"


def _fmt(v) -> str:
    return "n/a" if v is None else f"{v:+.1f}%"


def _mean(rows, h):
    vals = [float(r["fwd"][h]) for r in rows if (r.get("fwd") or {}).get(h) is not None]
    return float(np.mean(vals)) if vals else None


def _yf(date: str, tickers: list[str]) -> dict:
    tickers = sorted({_tick(t) for t in tickers if _tick(t)})
    out = {t: {h: None for h in YF_H} for t in tickers}
    if not tickers:
        return out
    try:
        import yfinance as yf
    except ImportError:
        return out
    start = datetime.fromisoformat(date).date()
    try:
        data = yf.download(
            tickers=tickers, start=start.isoformat(),
            end=(start + timedelta(days=19)).isoformat(),
            group_by="ticker", auto_adjust=True, threads=True, progress=False,
        )
    except Exception as e:
        print(f"[green] yfinance {date}: {e}")
        return out
    if data is None or data.empty:
        return out

    def closes(t):
        try:
            if isinstance(data.columns, pd.MultiIndex):
                if t not in data.columns.get_level_values(0):
                    return pd.Series(dtype=float)
                return data[t]["Close"].dropna()
            return data["Close"].dropna() if len(tickers) == 1 else pd.Series(dtype=float)
        except Exception:
            return pd.Series(dtype=float)

    for t in tickers:
        c = closes(t)
        if len(c) < 2:
            continue
        entry = float(c.iloc[0])
        if not entry:
            continue
        for h, n in YF_H.items():
            if len(c) > n:
                out[t][h] = float(c.iloc[n]) / entry - 1.0
    return out


def _row(df, i, pile: bool) -> dict:
    r = df.iloc[i]
    return {
        "ticker": str(r["Ticker"]),
        "book": None if pd.isna(r.get("score_1d")) else round(float(r.get("score_1d")), 3),
        "s_join": float(r.get("s_join") or 0),
        "s_general": float(r.get("s_general") or 0),
        "s_ab": float(r.get("s_ab") or 0),
        "s_peer": float(r.get("s_peer") or 0),
        "s_opp": float(r.get("s_opp") or 0),
        "size": None if pd.isna(r.get("size")) else r.get("size"),
        "sector": None if pd.isna(r.get("sector")) else r.get("sector"),
        "in_pile": bool(pile),
        "fwd": {},
    }


def run(date: str | None = None, top_n: int = 15, panel=None) -> dict:
    date = _parse_date(date)
    df = load_frame(date)
    if df is None or df.empty:
        raise SystemExit(f"no frame for {date}")
    df = attach_ranks(df)
    pile = green_mask(df)
    df = df.copy()
    df["green"] = pile.to_numpy()
    n_pile = int(pile.sum())
    green_df = df.loc[pile].reset_index(drop=True)
    if green_df.empty:
        green_picks: list[int] = []
    else:
        col = "green_rank" if "green_rank" in green_df.columns else "score_1d"
        gscore = pd.to_numeric(green_df[col], errors="coerce").fillna(-999).to_numpy()
        green_picks = _select_buys(green_df, gscore, top_n)

    book = _jload("data", "stock_book", f"{date}_stock_book.json") or {}
    live = [_tick(r.get("ticker")) for r in ((book.get("books") or {}).get("1d") or {}).get("buy") or []]
    by_t = {str(t).upper(): i for i, t in enumerate(df["Ticker"].astype(str))}
    green_rows = [_row(green_df, i, True) for i in green_picks]
    live_rows = []
    for t in live:
        i = by_t.get(t)
        if i is None:
            continue
        live_rows.append(_row(df, i, bool(df.iloc[i]["green"])))

    need = [r["ticker"] for r in green_rows + live_rows]
    fwd = _yf(date, need)
    if panel is not None:
        for h, n in YF_H.items():
            try:
                rets = _fwd_returns(panel, date, n)
            except Exception:
                rets = None
            if rets is None:
                continue
            for r in green_rows + live_rows:
                if r["fwd"].get(h) is None and r["ticker"] in rets.index and pd.notna(rets[r["ticker"]]):
                    fwd.setdefault(r["ticker"], {})[h] = float(rets[r["ticker"]])
    for rows in (green_rows, live_rows):
        for r in rows:
            got = fwd.get(r["ticker"]) or {}
            r["fwd"] = {h: (None if got.get(h) is None else round(float(got[h]) * 100, 2)) for h in YF_H}

    gset = {r["ticker"] for r in green_rows}
    lset = set(live)
    payload = {
        "date": date,
        "n_pile": n_pile,
        "n_universe": int(len(df)),
        "pile_used": n_pile >= GREEN_MIN,
        "live_buy": live_rows,
        "green_buy": green_rows,
        "overlap": sorted(gset & lset),
        "entered": sorted(gset - lset),
        "dropped": sorted(lset - gset),
        "avg_fwd": {
            "live": {h: _mean(live_rows, h) for h in YF_H},
            "green": {h: _mean(green_rows, h) for h in YF_H},
        },
    }
    out = BOOK_DIR / f"{date}_green.json"
    BOOK_DIR.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    print(f"[green] {date} pile={n_pile} used={payload['pile_used']} overlap={len(payload['overlap'])} entered={payload['entered']}")
    return payload


def _avg(d, h):
    if not d or d.get(h) is None:
        return "n/a"
    return f"{d[h]:+.2f}%"


def _table(rows):
    L = ["| # | Ticker | book | join | gen | AB | peer | opp | 1d | 1w | pile |",
         "|---|---|---|---|---|---|---|---|---|---|---|"]
    for i, r in enumerate(rows, 1):
        fp = r.get("fwd") or {}
        L.append(
            f"| {i} | {r['ticker']} | {r.get('book') if r.get('book') is not None else '—'} | "
            f"{r['s_join']:+.2f} | {r['s_general']:+.2f} | {r['s_ab']:+.2f} | {r['s_peer']:+.2f} | "
            f"{r['s_opp']:+.2f} | {_fmt(fp.get('1d'))} | {_fmt(fp.get('1w'))} | "
            f"{'yes' if r.get('in_pile') else 'no'} |"
        )
    return L


def render_all(days: list[dict]) -> str:
    L = [
        "# Green-pile book — every date",
        "",
        "Fill the 15 from names where join + AB + peer are all green,",
        "sector/news not red, relvol not red (< 0.7) when printed. Rank by green_rank.",
        "Same $400M / sector caps. Thin pile (< 8 liquid) → skip / fallback.",
        "",
        "| Date | pile | used? | live 1d | green 1d | live 1w | green 1w | overlap | entered |",
        "|------|------|-------|---------|----------|---------|----------|---------|---------|",
    ]
    for d in days:
        avg = d.get("avg_fwd") or {}
        L.append(
            f"| {d['date']} | {d.get('n_pile')} | {'yes' if d.get('pile_used') else 'no'} | "
            f"{_avg(avg.get('live'), '1d')} | {_avg(avg.get('green'), '1d')} | "
            f"{_avg(avg.get('live'), '1w')} | {_avg(avg.get('green'), '1w')} | "
            f"{len(d.get('overlap') or [])}/15 | {', '.join(d.get('entered') or []) or '—'} |"
        )
    L += ["", "## Per day", ""]
    for d in days:
        L += [f"### {d['date']}", "",
              f"live dropped: {', '.join(d.get('dropped') or []) or '—'}",
              "", "**live**", "", *_table(d.get("live_buy") or []),
              "", "**green-pile 15**", "", *_table(d.get("green_buy") or []), ""]
    return "\n".join(L) + "\n"


def run_all(top_n: int = 15) -> list[dict]:
    dates = sorted({p.name[:10] for p in BOOK_DIR.glob("????-??-??_stock_book.csv")})
    panel = _load_panel()
    days = []
    for d in dates:
        try:
            days.append(run(date=d, top_n=top_n, panel=panel))
        except SystemExit as e:
            print(f"[green] skip {d}: {e}")
    SCORE.mkdir(parents=True, exist_ok=True)
    SUMMARY.write_text(render_all(days), encoding="utf-8")
    print(f"[green] wrote {SUMMARY}")
    return days


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--top", type=int, default=15)
    ap.add_argument("--all", action="store_true")
    args = ap.parse_args()
    if args.all:
        run_all(top_n=args.top)
    else:
        run(date=args.date, top_n=args.top)


if __name__ == "__main__":
    main()
