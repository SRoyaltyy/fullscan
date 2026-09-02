"""15-seat backtest of the boring-winner filters from FEATURE_MINE.md.

Reads 03_scoreboard/feature_asof_panel.parquet (same vintage as the mine).
Does not rebuild the panel. No same-day Finviz leak — buckets are already as-of.

Live rule
---------
Hit engine A  = ab=good OR peer=good     (only prints from 2026-08-20)
Scale engine B = short=high OR sma20=below
Blue overlay   = lookback blue
Hard veto      = fade OR first_crack

If A printed that morning:
    pool = A AND NOT fade
Else if at least 15 blue names:
    pool = blue AND NOT fade
Else:
    pool = (short=high AND sma20=below) AND NOT fade

Rank (desc): score, points, fewer reds, live relvol, ticker.
    score = 3*blue + 2*ab_good + 2*peer_good + 1*short_high + 1*sma_below + 1*ab_up
Cap 4 names per sector. Take 15.
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
PANEL = ROOT / "03_scoreboard" / "feature_asof_panel.parquet"
OUT_MD = ROOT / "03_scoreboard" / "BORING_WINNERS.md"
OUT_JSON = ROOT / "03_scoreboard" / "boring_winners.json"
OUT_CSV = ROOT / "03_scoreboard" / "boring_winners_picks.csv"

HORIZONS = ("1d", "2d", "3d", "1w")
CLIP = 30.0
SEATS = 15
SECTOR_CAP = 4
RELVOL_RANK = {"hot": 2, "normal": 1, "dead": 0, "missing": -1}


def _finite(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    return s.replace([float("inf"), float("-inf")], pd.NA)


def load_panel(path: Path = PANEL) -> pd.DataFrame:
    df = pd.read_parquet(path)
    for h in HORIZONS:
        col = f"ret_{h}"
        if col in df.columns:
            df[col] = _finite(df[col])
    df["A"] = df["ab_good"].fillna(False) | df["peer_good"].fillna(False)
    df["B_or"] = (df["short_b"] == "high") | (df["sma20_b"] == "below")
    df["B_and"] = (df["short_b"] == "high") & (df["sma20_b"] == "below")
    df["fade_x"] = df["fade"].fillna(False) | df["first_crack"].fillna(False)
    df["score"] = (
        3 * df["blue"].fillna(False).astype(int)
        + 2 * df["ab_good"].fillna(False).astype(int)
        + 2 * df["peer_good"].fillna(False).astype(int)
        + (df["short_b"] == "high").astype(int)
        + (df["sma20_b"] == "below").astype(int)
        + df["ab_up"].fillna(False).astype(int)
    )
    df["relvol_rk"] = df["relvol_b"].map(RELVOL_RANK).fillna(-1).astype(int)
    return df


def a_printed(g: pd.DataFrame) -> bool:
    return bool(g["ab"].isin(["good", "bad", "neutral"]).any() or g["peer"].isin(["good", "bad", "neutral"]).any())


def pool_mask(g: pd.DataFrame):
    alive = ~g["fade_x"]
    if a_printed(g):
        return alive & g["A"], "A"
    if int((alive & g["blue"]).sum()) >= SEATS:
        return alive & g["blue"], "blue"
    return alive & g["B_and"], "Band"


def pick_seats(g: pd.DataFrame, mask: pd.Series, seats: int = SEATS, cap: int = SECTOR_CAP) -> pd.DataFrame:
    sub = g.loc[mask].copy()
    if sub.empty:
        return sub
    sub = sub.sort_values(
        ["score", "points", "n_red", "relvol_rk", "Ticker"],
        ascending=[False, False, True, False, True],
    )
    kept = []
    sec_n = {}
    for _, row in sub.iterrows():
        sec = str(row.get("sector_name") or "UNK") or "UNK"
        if sec_n.get(sec, 0) >= cap:
            continue
        kept.append(row)
        sec_n[sec] = sec_n.get(sec, 0) + 1
        if len(kept) >= seats:
            break
    return pd.DataFrame(kept)


def _pack(series: pd.Series) -> dict:
    r = series.dropna()
    if r.empty:
        return {"n": 0}
    clip = r.clip(-CLIP, CLIP)
    return {
        "n": int(len(r)),
        "hit": round(float((r > 0).mean()), 4),
        "mean": round(float(r.mean()), 3),
        "mean_clip30": round(float(clip.mean()), 3),
        "median": round(float(r.median()), 3),
    }


def session_row(date: str, g: pd.DataFrame) -> dict:
    mask, rule = pool_mask(g)
    pool = g.loc[mask]
    seats = pick_seats(g, mask)
    uni = {h: _pack(g[f"ret_{h}"]) for h in HORIZONS}
    out = {
        "date": date,
        "rule": rule,
        "a_printed": a_printed(g),
        "n_book": int(len(g)),
        "n_pool": int(len(pool)),
        "n_seats": int(len(seats)),
        "n_blue": int(g["blue"].sum()),
        "n_ab_good": int(g["ab_good"].sum()),
        "n_peer_good": int(g["peer_good"].sum()),
        "n_band": int(g["B_and"].sum()),
        "uni": uni,
        "pool": {h: _pack(pool[f"ret_{h}"]) for h in HORIZONS},
        "seats": {h: _pack(seats[f"ret_{h}"]) if len(seats) else {"n": 0} for h in HORIZONS},
        "picks": [
            {
                "Ticker": r["Ticker"],
                "score": int(r["score"]),
                "blue": bool(r["blue"]),
                "ab": r["ab"],
                "peer": r["peer"],
                "short": r["short_b"],
                "sma20": r["sma20_b"],
                "ab_up": bool(r["ab_up"]),
                "points": int(r["points"]),
                "n_red": int(r["n_red"]),
                "relvol": r["relvol_b"],
                "sector": r["sector_name"],
                **{f"ret_{h}": (None if pd.isna(r[f"ret_{h}"]) else float(r[f"ret_{h}"])) for h in HORIZONS},
            }
            for _, r in seats.iterrows()
        ],
    }
    return out


def run(path: Path = PANEL) -> dict:
    df = load_panel(path)
    days = []
    for date, g in df.groupby("date", sort=True):
        days.append(session_row(str(date), g))
    return {
        "generated_from": str(path),
        "rows": int(len(df)),
        "sessions": [d["date"] for d in days],
        "days": days,
    }


def _pct(x) -> str:
    if x is None:
        return "—"
    return f"{100 * x:.1f}%"


def _num(d: dict, key: str) -> str:
    if not d or d.get("n", 0) == 0 or key not in d:
        return "—"
    v = d[key]
    if key == "hit":
        return _pct(v)
    return f"{v:+.2f}"


def render(report: dict) -> str:
    lines = []
    a = lines.append
    a("# Boring winners backtest")
    a("")
    a("Filter-and-seat the FEATURE_MINE high-n edges. Not a flashy squeeze hunt.")
    a("")
    a("**Hit engine A** = `ab=good` OR `peer=good`. **Scale B** = `short=high` OR `sma20=below`.")
    a("**Blue** overlay. **Fade / first_crack** vetoed. 15 seats, 4 per sector.")
    a("")
    a("Score = `3·blue + 2·ab + 2·peer + 1·short_high + 1·sma_below + 1·ab_up`.")
    a("Tie-break: lookback points, fewer reds, relvol hot>normal>dead, ticker.")
    a("")
    a("## Read this first")
    a("")
    a("- Settled `1d` only through **2026-08-20**. 8/21 → 9/1 have names, no close-to-close yet.")
    a("- A cameras only print from **2026-08-20**. Before that the live rule falls through to blue, then `short AND sma20=below`.")
    a("- Board `ab=good` 64.6% / `peer=good` 65.8% is almost entirely **one day** (20 Aug). Universe that morning was already **65.1%** up. A matched the tape; it did not beat it.")
    a("- Board `blue` +4.46 mean is squeeze-contaminated. Clip at ±30 and the same sleeve is about **+0.55**. That is why both raw and clip-30 print here.")
    a("- Pool EW = every name the filter kept. Seats EW = the 15 the ranker kept. Do not treat a 500-name Band pile as a 15-name strategy.")
    a("")
    a("## Session tape")
    a("")
    a("| date | rule | book | pool | seats | uni 1d hit | uni 1d med | pool 1d hit | pool 1d clip | seats 1d hit | seats 1d clip |")
    a("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for d in report["days"]:
        u, p, s = d["uni"]["1d"], d["pool"]["1d"], d["seats"]["1d"]
        a(
            f"| {d['date']} | `{d['rule']}` | {d['n_book']} | {d['n_pool']} | {d['n_seats']} "
            f"| {_num(u,'hit')} | {_num(u,'median')} | {_num(p,'hit')} | {_num(p,'mean_clip30')} "
            f"| {_num(s,'hit')} | {_num(s,'mean_clip30')} |"
        )
    a("")
    a("## 15-seat books")
    a("")
    for d in report["days"]:
        a(f"### {d['date']} · rule `{d['rule']}` · pool {d['n_pool']} · A printed={str(d['a_printed']).lower()}")
        a("")
        if not d["picks"]:
            a("_empty pool_")
            a("")
            continue
        a("| # | Ticker | score | blue | ab | peer | short | sma20 | ab_up | pts | reds | relvol | sector | 1d |")
        a("|---:|---|---:|:---:|---|---|---|---|:---:|---:|---:|---|---|---:|")
        for i, r in enumerate(d["picks"], 1):
            ret = r["ret_1d"]
            ret_s = "—" if ret is None else f"{ret:+.2f}"
            a(
                f"| {i} | {r['Ticker']} | {r['score']} | {'Y' if r['blue'] else ''} "
                f"| {r['ab']} | {r['peer']} | {r['short']} | {r['sma20']} "
                f"| {'Y' if r['ab_up'] else ''} | {r['points']} | {r['n_red']} "
                f"| {r['relvol']} | {r['sector']} | {ret_s} |"
            )
        s1 = d["seats"]["1d"]
        if s1.get("n"):
            a("")
            a(
                f"Seats 1d n={s1['n']} hit={_num(s1,'hit')} raw={_num(s1,'mean')} "
                f"clip30={_num(s1,'mean_clip30')} med={_num(s1,'median')} · "
                f"universe hit={_num(d['uni']['1d'],'hit')} med={_num(d['uni']['1d'],'median')}."
            )
        else:
            a("")
            a("1d not settled — names only.")
        a("")
    a("## What this actually says")
    a("")
    a("1. **A is not a multi-week edge in this panel.** One priced session, and that session was a broad up day.")
    a("2. **Blue is not constantly winning.** 14 Aug hit-lift vs a red tape, 17 Aug 10% hit vs a 36% universe, 19 Aug modest hit-lift on a down day, 20 Aug = the tape.")
    a("3. **short AND sma20=below tracks the tape**, not a separate engine. It won 13/18/20 and lost 14/17/19 with the market.")
    a("4. Use A as a **same-morning seat-filler when the cameras printed**, not as a published expectancy. Re-score after 21 Aug / 27 Aug / 30 Aug 1d settles.")
    a("5. If you want one mechanical book tomorrow: run this file against the latest panel and take the 15. Do not hand-merge white / cond=good / join=good — those printed below the base hit on the mine board.")
    a("")
    return "\n".join(lines) + "\n"


def main() -> None:
    report = run()
    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    OUT_MD.write_text(render(report), encoding="utf-8")
    OUT_JSON.write_text(json.dumps(report, indent=2), encoding="utf-8")
    rows = []
    for d in report["days"]:
        for i, r in enumerate(d["picks"], 1):
            rows.append({"date": d["date"], "rule": d["rule"], "seat": i, **r})
    pd.DataFrame(rows).to_csv(OUT_CSV, index=False)
    print(f"[bw] days={len(report['days'])} md={OUT_MD} csv={OUT_CSV}", flush=True)


if __name__ == "__main__":
    main()
