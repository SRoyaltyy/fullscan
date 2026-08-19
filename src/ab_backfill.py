"""Point-in-time daily AB checklist backfill.

Each asof day includes:
  - Part A/B1 score (OHLC always; B1 when Finviz export exists)
  - Peer 5d RS / beat% / peers advancing (price_store × Correlations)
  - Industry 5d median (members from latest export roster)
  - Sector board Dir/Score (nearest _BOARD.md <= asof; coverage from ~late Jul 2026)
  - Forward 1d/3d/1w/2m 🟢🔴 favorability

CLI:
  python -m src.ab_backfill --months 24 --ticker BB
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from . import config
from . import price_store as ps
from . import ab_checklist as ab
from . import ab_context_daily as ctx

ROOT = Path(__file__).resolve().parent.parent
EXPORT_DIR = ROOT / "data" / "exports"
OUT_DIR = ROOT / "data" / "ab_backfill"
INS_PANEL = ROOT / "data" / "insider" / "history" / "monthly_panel.parquet"
INS_PANEL_CSV = ROOT / "data" / "insider" / "history" / "monthly_panel.csv"
ET = ZoneInfo(config.TZ)

HORIZONS = {"1d": 1, "3d": 3, "1w": 5, "2m": 42}


def _load_exports() -> list[tuple[str, pd.DataFrame]]:
    files = sorted(EXPORT_DIR.glob("finviz_????-??-??.csv"))
    out = []
    for f in files:
        d = f.stem.replace("finviz_", "")
        df = pd.read_csv(f, low_memory=False)
        tcol = "Ticker" if "Ticker" in df.columns else df.columns[0]
        df["Ticker"] = df[tcol].astype(str).str.strip().str.upper()
        df = df.drop_duplicates("Ticker", keep="first")
        out.append((d, df))
    return out


def _export_asof(exports, asof: str):
    cand = [(d, df) for d, df in exports if d <= asof]
    if not cand:
        return None, None
    return cand[-1]


def _prior_export_row(exports, asof: str, ticker: str):
    cand = [(d, df) for d, df in exports if d < asof]
    if not cand:
        return None, None
    d, df = cand[-1]
    hit = df[df["Ticker"] == ticker]
    if hit.empty:
        return None, d
    return hit.iloc[0], d


def _form4_asof(panel: pd.DataFrame, ticker: str, asof: str) -> dict:
    if panel is None or panel.empty:
        return {"flag": 0, "val": "no_panel"}
    month = (pd.Timestamp(asof).to_period("M") - 1).strftime("%Y-%m")
    p = panel[panel["ticker"].astype(str).str.upper() == ticker.upper()]
    p = p[p["month"] <= month]
    if p.empty:
        return {"flag": 0, "val": f"no_form4_month<={month}"}
    row = p.sort_values("month").iloc[-1]
    net = float(row.get("net_value", np.nan))
    delta = float(row.get("net_delta", np.nan)) if "net_delta" in row.index else np.nan
    flag = 0
    if np.isfinite(delta):
        flag = 1 if delta > 0 else (-1 if delta < 0 else 0)
    elif np.isfinite(net):
        flag = 1 if net > 0 else (-1 if net < 0 else 0)
    return {"flag": flag, "val": f"month={row['month']} net={net} delta={delta}"}


def _empty_b1() -> dict:
    return {"profitable": False, "prior_export_date": None}


def _setup_flag(a: dict, b: dict) -> tuple[int, str]:
    if not a.get("ok"):
        return 0, "no_ohlc"
    prof = bool(b.get("profitable"))
    rsi = a.get("rsi")
    sec = a.get("sections") or {}
    near_floor = bool(sec.get("ok") and (sec.get("rising_lows") or sec.get("flatish")))
    near_low_px = False
    if sec.get("ok") and sec.get("lows"):
        floor = min(sec["lows"])
        px = a.get("price")
        if np.isfinite(px) and floor > 0 and (px / floor - 1.0) <= 0.08:
            near_low_px = True
    pair = a.get("pair") or {}
    body_ok = np.isfinite(pair.get("body_rg", np.nan)) and pair["body_rg"] >= 1.0
    vol_ok = (not np.isfinite(pair.get("vol_rg", np.nan))) or pair["vol_rg"] >= 1.0
    oversold = np.isfinite(rsi) and rsi < 30
    if prof and oversold and (near_floor or near_low_px) and body_ok and vol_ok:
        return 1, "SETUP_GOOD"
    return 0, "SETUP_NO"


def _forward_multi(ohlc, asof: str, entry: float) -> dict:
    out: dict = {}
    for name, bars in HORIZONS.items():
        out[f"mp_{name}"] = np.nan
        out[f"ml_{name}"] = np.nan
        out[f"end_{name}"] = np.nan
        out[f"fav_{name}"] = np.nan
        out[f"up_{name}"] = np.nan
        out[f"bars_{name}"] = 0
    if ohlc is None or ohlc.empty or not np.isfinite(entry) or entry <= 0:
        return out
    df = ohlc.copy()
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)
    df.columns = [c.lower() for c in df.columns]
    fut_all = df[df.index > pd.Timestamp(asof)]
    if fut_all.empty:
        return out
    high_col = "high" if "high" in fut_all.columns else "close"
    low_col = "low" if "low" in fut_all.columns else "close"
    for name, bars in HORIZONS.items():
        fut = fut_all.head(bars)
        n = len(fut)
        if n == 0:
            continue
        hi = float(fut[high_col].astype(float).max())
        lo = float(fut[low_col].astype(float).min())
        end = float(fut["close"].astype(float).iloc[-1])
        mp = hi / entry - 1.0
        ml = lo / entry - 1.0
        er = end / entry - 1.0
        out[f"mp_{name}"] = mp
        out[f"ml_{name}"] = ml
        out[f"end_{name}"] = er
        out[f"bars_{name}"] = n
        out[f"fav_{name}"] = 1.0 if mp > abs(ml) else 0.0
        out[f"up_{name}"] = 1.0 if er > 0 else 0.0
    return out


def _dot(fav) -> str:
    if fav is None or (isinstance(fav, float) and not np.isfinite(fav)):
        return "⚪"
    return "🟢" if float(fav) >= 0.5 else "🔴"


def _sec_dot(direction) -> str:
    if direction == "up":
        return "🟢"
    if direction == "down":
        return "🔴"
    return "⚪"


def _score_cell(score) -> str:
    try:
        s = int(score)
    except Exception:
        return "⚪—"
    if s > 0:
        return f"🟢 **{s:+d}**"
    if s < 0:
        return f"🔴 **{s:+d}**"
    return f"⚪ **{s:+d}**"


def _p_chip(val, pos_label: str, neg_label: str) -> str:
    try:
        v = int(val)
    except Exception:
        return "⚪"
    if v > 0:
        return f"🟢{pos_label}"
    if v < 0:
        return f"🔴{neg_label}"
    return "⚪"


def _label_colored(r) -> str:
    parts = [
        _p_chip(r.get("P01"), "LEAD", "LAG"),
        _p_chip(r.get("P02"), "peers↑", "peers↓"),
        _p_chip(r.get("P03"), "ind↑", "ind↓"),
        _p_chip(r.get("P04"), "sec↑", "sec↓"),
    ]
    return " ".join(parts)


def _ensure_price_coverage(start: str, end: str, ticker: str | None, peer_tickers: list[str]) -> None:
    store = ps._load_store()
    need_days = max((pd.Timestamp(end) - pd.Timestamp(start)).days + 150, 400)
    want = set()
    if ticker:
        want.add(ticker.upper())
    want.update(p.upper() for p in peer_tickers)
    # always bootstrap requested names for peer RS
    names = sorted(want) if want else None
    if len(store):
        dmin = pd.to_datetime(store["date"]).min().date().isoformat()
        have = set(store["ticker"].astype(str).str.upper().unique())
        missing = [t for t in (names or []) if t not in have]
        if dmin <= start and not missing:
            print(f"[backfill] price store OK from {dmin}; peers present")
            return
        print(f"[backfill] extending bootstrap days={need_days} missing_peers={missing[:8]}")
    else:
        print(f"[backfill] empty store — bootstrap days={need_days}")
    if names:
        # bootstrap ticker + peers in chunks
        ps.bootstrap(days=need_days, tickers=names[:1], resume=True)
        if len(names) > 1:
            ps.bootstrap(days=need_days, tickers=names[1:], resume=True)
    else:
        ps.bootstrap(days=need_days, tickers=None, resume=True)


def _streaks(series: pd.Series) -> pd.Series:
    sign = (series > 0).astype(int)
    out, run, prev = [], 0, None
    for s in sign:
        if prev is None or s != prev:
            run = 1
        else:
            run += 1
        out.append(run if s == 1 else -run)
        prev = s
    return pd.Series(out, index=series.index)


def _pattern_audit(out: pd.DataFrame) -> list[str]:
    lines = [
        "## Pattern correlation audit",
        "",
        "🟢 = max upside > |max downside| over horizon (long from asof close).",
        "",
    ]
    if out is None or out.empty:
        lines.append("_no rows_")
        return lines
    df = out.copy().sort_values(["Ticker", "asof_date"]).reset_index(drop=True)
    parts = []
    for t, g in df.groupby("Ticker", sort=False):
        g = g.copy()
        g["score_delta"] = g["score"].diff()
        g["streak"] = _streaks(g["score"])
        parts.append(g)
    df = pd.concat(parts, ignore_index=True)
    horizons = ["1d", "3d", "1w", "2m"]

    def rate(mask, h):
        col = f"fav_{h}"
        sub = df.loc[mask, col].dropna()
        if len(sub) < 5:
            return None, len(sub)
        return float((sub >= 0.5).mean()), len(sub)

    lines += ["### A. Score level", "", "| bucket | n | 1d | 3d | 1w | 2m |", "|--------|--:|---:|---:|---:|---:|"]
    for (lo, hi), lab in zip([(-99, -1), (0, 2), (3, 5), (6, 99)], ["≤-1", "0-2", "3-5", "≥6"]):
        m = (df["score"] >= lo) & (df["score"] <= hi)
        cells = []
        for h in horizons:
            r, _ = rate(m, h)
            cells.append(f"{100*r:.0f}%" if r is not None else "—")
        lines.append(f"| {lab} | {int(m.sum())} | " + " | ".join(cells) + " |")

    lines += ["", "### B. Context labels vs 1w/2m favorability", "",
              "| label contains | n | 1w 🟢% | 2m 🟢% |",
              "|----------------|--:|-------:|-------:|"]
    for lab in ("LEAD", "LAG", "peers↑", "peers↓", "ind↑", "ind↓", "sec↑", "sec↓"):
        m = df["context_label"].astype(str).str.contains(lab, regex=False)
        r1, _ = rate(m, "1w")
        r2, _ = rate(m, "2m")
        lines.append(
            f"| {lab} | {int(m.sum())} | "
            f"{100*r1:.0f}%" if r1 is not None else f"| {lab} | {int(m.sum())} | — | — |"
        )
        if r1 is not None:
            lines[-1] = (
                f"| {lab} | {int(m.sum())} | "
                f"{100*r1:.0f}% | {100*r2:.0f}% |" if r2 is not None else
                f"| {lab} | {int(m.sum())} | {100*r1:.0f}% | — |"
            )

    lines += ["", "### C. Base rates", ""]
    cells = []
    for h in horizons:
        r, n = rate(pd.Series(True, index=df.index), h)
        cells.append(f"{h}:{100*r:.0f}%(n={n})" if r is not None else f"{h}:—")
    lines.append("- " + " · ".join(cells))
    return lines


def run(
    start: str | None = None,
    end: str | None = None,
    months: int | None = None,
    ticker: str | None = None,
) -> pd.DataFrame:
    exports = _load_exports()
    exp_dates = [d for d, _ in exports]
    first_exp = exp_dates[0] if exp_dates else None
    last_exp = exp_dates[-1] if exp_dates else None

    end = end or (last_exp or datetime.now(ET).date().isoformat())
    if start is None:
        m = 6 if months is None else int(months)
        start = (pd.Timestamp(end) - pd.DateOffset(months=m)).date().isoformat()

    corr_map = ctx.load_corr_map()
    boards = ctx.load_sector_boards()
    board_dates = [d for d, _ in boards]
    print(f"[backfill] requested {start} → {end}")
    print(f"[backfill] exports n={len(exports)} {first_exp}→{last_exp}")
    print(f"[backfill] sector boards n={len(boards)} {board_dates[0] if board_dates else '—'}→{board_dates[-1] if board_dates else '—'}")
    print(f"[backfill] corr map tickers={len(corr_map):,}")

    peer_list = corr_map.get(ticker.upper(), []) if ticker else []
    # also bootstrap a sample of industry peers for industry median
    sector_name, industry_name = (None, None)
    if ticker:
        sector_name, industry_name = ctx.ticker_sector_industry_from_latest_export(ticker)
    ind_members = ctx.industry_members_from_latest_export()
    ind_tickers = ind_members.get(industry_name or "", [])[:40]

    _ensure_price_coverage(start, end, ticker, peer_list + ind_tickers)

    store = ps._load_store()
    if not len(store):
        raise SystemExit("[backfill] empty price store")

    all_days = sorted(pd.to_datetime(store["date"]).unique())
    days = [d.date().isoformat() for d in all_days if start <= d.date().isoformat() <= end]
    if not days:
        raise SystemExit("[backfill] no sessions in window")

    form4 = None
    if INS_PANEL.exists():
        form4 = pd.read_parquet(INS_PANEL)
    elif INS_PANEL_CSV.exists():
        form4 = pd.read_csv(INS_PANEL_CSV)

    store = store.copy()
    store["date"] = pd.to_datetime(store["date"])
    store["ticker"] = store["ticker"].astype(str).str.upper()
    groups = {t: g.set_index("date").sort_index() for t, g in store.groupby("ticker")}

    if ticker:
        tickers = [ticker.upper()]
    else:
        latest_df = exports[-1][1] if exports else None
        if latest_df is not None:
            tickers = ab._filter_liquid(latest_df)["Ticker"].astype(str).str.upper().tolist()
        else:
            tickers = sorted(groups.keys())

    print(f"[backfill] sessions={len(days)} ({days[0]}→{days[-1]}) names={len(tickers)}")
    if ticker:
        print(f"[backfill] {ticker}: sector={sector_name} industry={industry_name} peers={len(peer_list)}")

    rows = []
    n_ohlc_only = 0
    n_with_export = 0
    for i, asof in enumerate(days, 1):
        exp_date, exp_df = _export_asof(exports, asof)
        exp_idx = exp_df.set_index("Ticker") if exp_df is not None else None

        for t in tickers:
            g = groups.get(t)
            if g is None:
                continue
            ohlc_to = g[g.index <= pd.Timestamp(asof)]
            if ohlc_to.empty or len(ohlc_to) < 25:
                continue

            row = None
            prior_row = None
            prior_d = None
            if exp_idx is not None and t in exp_idx.index:
                row = exp_idx.loc[t]
                if isinstance(row, pd.DataFrame):
                    row = row.iloc[0]
                prior_row, prior_d = _prior_export_row(exports, exp_date or asof, t)
                n_with_export += 1
                sec_n = str(row.get("Sector")) if "Sector" in row.index else sector_name
                ind_n = str(row.get("Industry")) if "Industry" in row.index else industry_name
            else:
                n_ohlc_only += 1
                sec_n, ind_n = sector_name, industry_name

            a = ab._part_a(ohlc_to)
            if row is not None:
                b = ab._part_b1(row, prior_row, prior_d)
                pb = ab._pass_b1(b)
                mode = "export"
            else:
                b = _empty_b1()
                pb = {k: 0 for k in ab.FEATURE_ORDER if k.startswith("B")}
                mode = "ohlc_only_no_export"

            pa = ab._pass_a(a)
            setup_flag, setup_val = _setup_flag(a, b)
            flags = {**pa, **pb, "A14_profitable_oversold_setup": setup_flag}
            f4 = _form4_asof(form4, t, asof)
            flags["B15_form4_insider"] = f4["flag"]
            score = int(sum(int(v) for v in flags.values()))

            peer = ctx.peer_context_asof(t, asof, groups, corr_map, n=5)
            indc = ctx.industry_context_asof(ind_n, asof, groups, ind_members, n=5)
            secc = ctx.sector_context_asof(sec_n, asof, boards)

            p01, p02, p03, p04 = peer["P01"], peer["P02"], indc["P03"], secc["P04"]
            score_ctx = int(p01 + p02 + p03 + p04)
            label = ctx.context_label(p01, p02, p03, p04)

            pair = (a.get("pair") or {}) if a.get("ok") else {}
            entry = a.get("price") if a.get("ok") else np.nan
            fwd = _forward_multi(g, asof, float(entry) if np.isfinite(entry) else np.nan)

            rows.append({
                "Ticker": t,
                "asof_date": asof,
                "score": score,
                "score_context": score_ctx,
                "score_enriched": score + score_ctx,
                "score_mode": mode,
                "context_label": label,
                "rs_5d": peer["rs_5d"],
                "own_5d": peer["own_5d"],
                "peer_med_5d": peer["peer_med_5d"],
                "beat_pct_5d": peer["beat_pct_5d"],
                "n_peers_used": peer["n_peers_used"],
                "P01": p01,
                "P02": p02,
                "ind_med_5d": indc["ind_med_5d"],
                "P03": p03,
                "sector": sec_n,
                "industry": ind_n,
                "sector_dir": secc["sector_dir"],
                "sector_score": secc["sector_score"],
                "sector_board_date": secc["sector_board_date"],
                "P04": p04,
                "rsi": a.get("rsi") if a.get("ok") else np.nan,
                "price": entry,
                "setup_flag": setup_flag,
                "pair_day_a": pair.get("d_a"),
                "pair_day_b": pair.get("d_b"),
                "body_rg_2day": pair.get("body_rg"),
                "export_used": exp_date or "none",
                **fwd,
            })

        if i % 20 == 0 or i == len(days):
            print(f"[backfill] {asof} ({i}/{len(days)}) rows={len(rows):,}")

    out = pd.DataFrame(rows)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    tag = ticker.upper() if ticker else "universe"
    stem = f"{start}_{end}_{tag}"
    parquet = OUT_DIR / f"{stem}.parquet"
    out.to_parquet(parquet, index=False)
    if ticker or len(out) < 500_000:
        out.to_csv(OUT_DIR / f"{stem}.csv", index=False)

    lines = [
        f"# AB PIT backfill — {start} → {end} — {tag}",
        "",
        f"- Rows: **{len(out):,}** · actual `{out['asof_date'].min() if len(out) else '—'}` → `{out['asof_date'].max() if len(out) else '—'}`",
        f"- Finviz exports: `{first_exp}` → `{last_exp}` (B1 only when export ≤ asof)",
        f"- Sector boards on disk: **{len(boards)}** "
        f"({board_dates[0] if board_dates else '—'} → {board_dates[-1] if board_dates else '—'}; P04=⚪ before first board)",
        f"- Peers: Correlations map × price_store 5d returns (PIT)",
        f"- Industry: 5d median of industry roster (from latest export membership)",
        "",
        "## Legend — scores",
        "",
        "| col | meaning |",
        "|-----|---------|",
        "| score | Sum of AB Part A + B1 feature flags **as of that day** |",
        "| ctx | Sum of context flags P01+P02+P03+P04 that day |",
        "| enr | score + ctx |",
        "| color on score/enr | green if >0, red if <0, white if 0 |",
        "| 1d 3d 1w 2m | forward max-upside vs |max-downside| from that day's close |",
        "",
        "## Legend — P01 to P04 (context flags)",
        "",
        "| Flag | Name | +1 (green) | -1 (red) | Data source |",
        "|------|------|------------|----------|-------------|",
        "| **P01** | Peer lead / lag | stock 5d - peer-median 5d > 0 and beats >=50% peers | lags peers | Correlations peers + price_store OHLC <= asof |",
        "| **P02** | Peers advancing | peer-basket median 5d > 0 | median 5d < 0 | same peer set |",
        "| **P03** | Industry advancing | industry median 5d > 0 | median 5d < 0 | Finviz Industry roster + price_store |",
        "| **P04** | Sector supportive | sector board Dir=up | Dir=down | nearest `01_daily/sectors/<board_date>/_BOARD.md` with board_date <= asof |",
        "",
        "Label chips: green LEAD / red LAG · peers up/down · ind up/down · sec up/down (white = neutral/no data).",
        "",
    ]

    if len(out) and ticker:
        lines += [
            f"## {ticker} — daily trail (every session)",
            "",
            "| date | score | ctx | enr | context | rs5d | sec | board_date | 1d | 3d | 1w | 2m |",
            "|------|-------|----:|-----|---------|------|-----|------------|:--:|:--:|:--:|:--:|",
        ]
        for _, r in out.sort_values("asof_date").iterrows():
            rs = r.get("rs_5d")
            lines.append(
                f"| {r['asof_date']} | {_score_cell(r['score'])} | {int(r['score_context']):+d} | "
                f"{_score_cell(r['score_enriched'])} | {_label_colored(r)} | "
                f"{(f'{rs:+.1%}' if np.isfinite(rs) else '—')} | "
                f"{_sec_dot(r.get('sector_dir'))} | {r.get('sector_board_date') or '—'} | "
                f"{_dot(r.get('fav_1d'))} | {_dot(r.get('fav_3d'))} | "
                f"{_dot(r.get('fav_1w'))} | {_dot(r.get('fav_2m'))} |"
            )

        lines += [
            "",
            "### Sector source footnote (P04)",
            "",
            f"- Ticker **{ticker}** sector **`{sector_name or '—'}`**, industry **`{industry_name or '—'}`** "
            f"(from latest Finviz export roster).",
            "- Rule: each asof day uses the **latest** `board_date <= asof`. If none, sec is white and P04=0.",
            "",
            "| board_date | file | sector row | Dir | Score |",
            "|------------|------|------------|-----|------:|",
        ]
        used = out.dropna(subset=["sector_board_date"]) if "sector_board_date" in out.columns else out.iloc[0:0]
        if len(used):
            seen = set()
            for _, r in used.sort_values("sector_board_date").iterrows():
                bd = r.get("sector_board_date")
                if not bd or bd in seen:
                    continue
                seen.add(bd)
                sc = r.get("sector_score")
                sc_s = sc if (sc is not None and __import__("numpy").isfinite(sc)) else "—"
                lines.append(
                    f"| {bd} | `01_daily/sectors/{bd}/_BOARD.md` | **{r.get('sector') or sector_name or '—'}** | "
                    f"{r.get('sector_dir') or '—'} | {sc_s} |"
                )
        else:
            lines.append("| — | _(no sector board <= any asof)_ | — | — | — |")

        lines += ["", "All sector board files on disk at run time:"]
        if board_dates:
            for bd in board_dates:
                lines.append(f"- `01_daily/sectors/{bd}/_BOARD.md`")
        else:
            lines.append("- _(none found)_")


    if len(out):
        lines.append("")
        lines.extend(_pattern_audit(out))

    lines += ["", f"- parquet: `{parquet.relative_to(ROOT)}`"]
    md = OUT_DIR / f"{stem}.md"
    md.write_text("\n".join(lines), encoding="utf-8")
    (OUT_DIR / f"{stem}.json").write_text(
        json.dumps(
            {
                "start": start,
                "end": end,
                "ticker": ticker,
                "n_rows": int(len(out)),
                "sector_boards": board_dates,
                "export_first": first_exp,
                "export_last": last_exp,
                "generated": datetime.now(ET).isoformat(),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"[backfill] wrote {parquet} rows={len(out):,}")
    print(f"[backfill] wrote {md}")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default=None)
    ap.add_argument("--end", default=None)
    ap.add_argument("--months", type=int, default=None)
    ap.add_argument("--ticker", default=None)
    args = ap.parse_args()
    run(start=args.start, end=args.end, months=args.months, ticker=args.ticker)


if __name__ == "__main__":
    main()
