"""Re-rank the book with NO weights.

Standalone score = simple average of the six raw layers
(join, sector, general, news, AB, peer). No mid-opp, no rebound,
no persist penalty, no heat scale.

Tape score = average of AB + peer only (the name-specific pair).

Same buy gates as the live ranker so the 15-name lists are comparable.

  python -m src.lookback_unweighted --date 2026-08-20 --top 15
  python -m src.lookback_unweighted --all --top 15
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from .book_learn import _fwd_returns, _load_panel, _select_buys, load_frame
from .book_lookback import BOOK_DIR, DAILY, SCORE, _jload, _parse_date, _tick
from .stock_book import MIN_OPP_MCAP_M

LAYERS = ("s_join", "s_sector", "s_general", "s_news", "s_ab", "s_peer")
YF_H = {"1d": 1, "2d": 2, "3d": 3, "1w": 5}
SUMMARY = SCORE / "BOOK_UNWEIGHTED.md"


def _num(x, nd=2) -> str:
    try:
        return f"{float(x):+.{nd}f}"
    except (TypeError, ValueError):
        return "—"


def _fmt(v) -> str:
    return "n/a" if v is None else f"{v:+.1f}%"


def _book_dates() -> list[str]:
    return sorted({p.name[:10] for p in BOOK_DIR.glob("????-??-??_stock_book.csv")})


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
        print(f"[unweighted] yfinance failed {date}: {e}")
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


def _panel_forwards(panel, date: str, tickers: list[str]) -> dict[str, dict[str, float | None]]:
    tickers = [_tick(t) for t in tickers if _tick(t)]
    out = {t: {h: None for h in YF_H} for t in tickers}
    if panel is None:
        return out
    for h, n in YF_H.items():
        try:
            rets = _fwd_returns(panel, date, n)
        except Exception:
            rets = None
        if rets is None:
            continue
        for t in tickers:
            if t in rets.index and pd.notna(rets[t]):
                out[t][h] = float(rets[t])
    return out


def _merge_fwd(*maps: dict) -> dict[str, dict[str, float | None]]:
    out: dict[str, dict[str, float | None]] = {}
    for m in maps:
        for t, rec in (m or {}).items():
            cur = out.setdefault(t, {h: None for h in YF_H})
            for h in YF_H:
                if cur[h] is None and rec.get(h) is not None:
                    cur[h] = rec[h]
    return out


def _mean_fwd(rows: list[dict], h: str) -> float | None:
    vals = [float(r["fwd"][h]) for r in rows if (r.get("fwd") or {}).get(h) is not None]
    return float(np.mean(vals)) if vals else None


def _hit_rate(rows: list[dict], h: str) -> float | None:
    vals = [float(r["fwd"][h]) for r in rows if (r.get("fwd") or {}).get(h) is not None]
    if not vals:
        return None
    return float(sum(1 for v in vals if v > 0) / len(vals))


def _row_from_frame(df: pd.DataFrame, i: int, standalone: float, tape: float) -> dict:
    r = df.iloc[i]
    book = r.get("score_1d")
    try:
        book_v = None if book is None or pd.isna(book) else round(float(book), 4)
    except (TypeError, ValueError):
        book_v = None
    return {
        "ticker": str(r["Ticker"]),
        "standalone": round(float(standalone), 4),
        "tape": round(float(tape), 4),
        "book": book_v,
        "s_join": float(r.get("s_join") or 0),
        "s_sector": float(r.get("s_sector") or 0),
        "s_general": float(r.get("s_general") or 0),
        "s_news": float(r.get("s_news") or 0),
        "s_ab": float(r.get("s_ab") or 0),
        "s_peer": float(r.get("s_peer") or 0),
        "s_opp": float(r.get("s_opp") or 0),
        "size": None if pd.isna(r.get("size")) else r.get("size"),
        "sector": None if pd.isna(r.get("sector")) else r.get("sector"),
        "fwd": {},
    }


def _live_buys(date: str, df: pd.DataFrame, top_n: int) -> list[str]:
    book = _jload("data", "stock_book", f"{date}_stock_book.json") or {}
    live = [
        _tick(r.get("ticker"))
        for r in ((book.get("books") or {}).get("1d") or {}).get("buy") or []
    ]
    if live:
        return [t for t in live if t]
    if "score_1d" in df.columns:
        score = pd.to_numeric(df["score_1d"], errors="coerce").fillna(-999).to_numpy()
        idx = _select_buys(df, score, top_n)
        return [str(df.iloc[i]["Ticker"]) for i in idx]
    return []


def run(date: str | None = None, top_n: int = 15, write_lookback_md: bool = True,
        panel=None) -> dict:
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
    live_buys = _live_buys(date, df, top_n)
    live_set = set(live_buys)

    stand_rows = [_row_from_frame(df, i, standalone[i], tape[i]) for i in stand_idx]
    tape_rows = [_row_from_frame(df, i, standalone[i], tape[i]) for i in tape_idx]
    by_t = {str(t).upper(): i for i, t in enumerate(df["Ticker"].astype(str))}
    live_rows = []
    for t in live_buys:
        i = by_t.get(t)
        if i is None:
            continue
        live_rows.append(_row_from_frame(df, i, standalone[i], tape[i]))

    need = [r["ticker"] for r in stand_rows + tape_rows + live_rows]
    watch = ["SLS", "ARCT", "CYPH", "ASST", "BTDR", "VIRT", "FIGR", "ELF", "AUPH"]
    watch_rows = []
    for t in watch:
        i = by_t.get(t)
        if i is None:
            continue
        watch_rows.append(_row_from_frame(df, i, standalone[i], tape[i]))
        need.append(t)

    fwd = _merge_fwd(_panel_forwards(panel, date, need), _yf_forwards(date, need))
    for rows in (stand_rows, tape_rows, live_rows, watch_rows):
        for r in rows:
            got = fwd.get(r["ticker"]) or {}
            r["fwd"] = {
                h: (None if got.get(h) is None else round(float(got[h]) * 100, 2))
                for h in YF_H
            }

    stand_set = {r["ticker"] for r in stand_rows}
    tape_set = {r["ticker"] for r in tape_rows}
    payload = {
        "date": date,
        "n_universe": int(len(df)),
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
        "hit_rate": {
            "live": {h: _hit_rate(live_rows, h) for h in YF_H},
            "standalone": {h: _hit_rate(stand_rows, h) for h in YF_H},
            "tape": {h: _hit_rate(tape_rows, h) for h in YF_H},
        },
    }

    order_stand = np.argsort(-standalone)
    order_tape = np.argsort(-tape)
    rank_stand = {str(df.iloc[i]["Ticker"]): n for n, i in enumerate(order_stand, 1)}
    rank_tape = {str(df.iloc[i]["Ticker"]): n for n, i in enumerate(order_tape, 1)}
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

    if write_lookback_md:
        _write_lookback_block(date, payload)
    print(f"[unweighted] {date} live={len(live_rows)} stand={len(stand_rows)} tape={len(tape_rows)}")
    return payload


def _write_lookback_block(date: str, payload: dict) -> None:
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


def _avg_cell(d: dict | None, h: str) -> str:
    if not d or d.get(h) is None:
        return "n/a"
    return f"{d[h]:+.2f}%"


def _pct_cell(d: dict | None, h: str) -> str:
    if not d or d.get(h) is None:
        return "n/a"
    return f"{100 * d[h]:.0f}%"


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


def _grand_mean(days: list[dict], book: str, h: str) -> float | None:
    vals = []
    for d in days:
        v = ((d.get("avg_fwd") or {}).get(book) or {}).get(h)
        if v is not None:
            vals.append(float(v))
    return float(np.mean(vals)) if vals else None


def render_all(days: list[dict]) -> str:
    L = [
        "# Unweighted ranking — every book date",
        "",
        "Same method as the 20 Aug experiment, run on every `*_stock_book.csv`.",
        "",
        "- **live** = weighted book the ranker actually printed (1d BUY, top 15, gates on)",
        "- **standalone** = equal mean of join/sector/gen/news/AB/peer. No weights, no opp.",
        "- **tape** = mean(AB, peer) only.",
        "",
        "Returns are close-to-close after the signal date. `n/a` = that horizon has not traded yet.",
        "",
        "## Scoreboard",
        "",
        "| Date | live 1d | stand 1d | tape 1d | live 1w | stand 1w | tape 1w | stand overlap | tape overlap |",
        "|------|---------|----------|---------|---------|----------|---------|---------------|--------------|",
    ]
    for d in days:
        avg = d.get("avg_fwd") or {}
        L.append(
            f"| {d['date']} | {_avg_cell(avg.get('live'), '1d')} | "
            f"{_avg_cell(avg.get('standalone'), '1d')} | {_avg_cell(avg.get('tape'), '1d')} | "
            f"{_avg_cell(avg.get('live'), '1w')} | {_avg_cell(avg.get('standalone'), '1w')} | "
            f"{_avg_cell(avg.get('tape'), '1w')} | "
            f"{len(d.get('overlap_standalone_vs_live') or [])}/15 | "
            f"{len(d.get('overlap_tape_vs_live') or [])}/15 |"
        )
    L += [
        "",
        "## Mean across dates that have that horizon",
        "",
        "| Book | avg 1d | avg 1w | 1d win rate | 1w win rate |",
        "|------|--------|--------|-------------|-------------|",
    ]
    for book, label in (("live", "live weighted"), ("standalone", "standalone equal-mean"), ("tape", "tape AB+peer")):
        wr1 = []
        wrw = []
        for d in days:
            h1 = ((d.get("hit_rate") or {}).get(book) or {}).get("1d")
            hw = ((d.get("hit_rate") or {}).get(book) or {}).get("1w")
            if h1 is not None:
                wr1.append(h1)
            if hw is not None:
                wrw.append(hw)
        L.append(
            f"| {label} | {_fmt(_grand_mean(days, book, '1d'))} | "
            f"{_fmt(_grand_mean(days, book, '1w'))} | "
            f"{(f'{100*float(np.mean(wr1)):.0f}%' if wr1 else 'n/a')} | "
            f"{(f'{100*float(np.mean(wrw)):.0f}%' if wrw else 'n/a')} |"
        )
    L += ["", "## Per-day lists", ""]
    for d in days:
        L += [
            f"### {d['date']}",
            "",
            f"standalone entered: {', '.join(d.get('entered_standalone') or []) or '—'}",
            f"standalone dropped: {', '.join(d.get('dropped_standalone') or []) or '—'}",
            "",
            "**live**",
            "",
            *_table(d.get("live_buy") or [], "book"),
            "",
            "**standalone**",
            "",
            *_table(d.get("standalone_buy") or [], "standalone"),
            "",
            "**tape**",
            "",
            *_table(d.get("tape_buy") or [], "tape"),
            "",
        ]
    return "\n".join(L) + "\n"


def run_all(top_n: int = 15) -> list[dict]:
    dates = _book_dates()
    if not dates:
        raise SystemExit("no data/stock_book/*_stock_book.csv")
    panel = _load_panel()
    days = []
    for d in dates:
        try:
            days.append(run(date=d, top_n=top_n, write_lookback_md=False, panel=panel))
        except SystemExit as e:
            print(f"[unweighted] skip {d}: {e}")
    SCORE.mkdir(parents=True, exist_ok=True)
    text = render_all(days)
    SUMMARY.write_text(text, encoding="utf-8")
    print(text[:4000])
    print(f"[unweighted] wrote {SUMMARY} for {len(days)} dates: {dates}")
    return days


def main() -> None:
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
