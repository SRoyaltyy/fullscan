"""25-seat backtest of the boring-winner filters from FEATURE_MINE.md.

Reads 03_scoreboard/feature_asof_panel.parquet (same vintage as the mine).
Does not rebuild the panel. No same-day Finviz leak — buckets are already as-of.

Live long rule
--------------
Hit engine A  = ab=good OR peer=good     (only prints from 2026-08-20)
Scale engine B = short=high OR sma20=below
Blue overlay   = lookback blue
Hard veto      = fade OR first_crack

If A printed that morning:
    pool = A AND NOT fade
Else if at least 25 blue names:
    pool = blue AND NOT fade
Else:
    pool = (short=high AND sma20=below) AND NOT fade

Rank (desc): score, points, fewer reds, live relvol, ticker.
    score = 3*blue + 2*ab_good + 2*peer_good + 1*short_high + 1*sma_below + 1*ab_up
Cap 6 names per sector. Take 25.

Short / sold sleeve
-------------------
Inverse of the live rule: fade|first_crack first, else ab=bad|peer=bad when A
printed, else not-blue AND short=low AND sma20=above. Rank by inverse score.
Disjoint from the long book. Same 25 / 6 cap.
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from src import bt_report

ROOT = Path(__file__).resolve().parent.parent
PANEL = ROOT / "03_scoreboard" / "feature_asof_panel.parquet"
OUT_MD = ROOT / "03_scoreboard" / "BORING_WINNERS.md"
OUT_JSON = ROOT / "03_scoreboard" / "boring_winners.json"
OUT_CSV = ROOT / "03_scoreboard" / "boring_winners_picks.csv"
OUT_DAILY = ROOT / "03_scoreboard" / "boring_winners_daily.csv"

HORIZONS = ("1d", "2d", "3d", "1w")
CLIP = 30.0
SEATS = 25
SECTOR_CAP = 6
RELVOL_RANK = {"hot": 2, "normal": 1, "dead": 0, "missing": -1}
HOW = {
    "A": "A cameras printed; pool=ab|peer good",
    "blue": "A missing; pool=blue",
    "Band": "A and blue thin; pool=short=high AND sma20=below",
    "fade": "fade or first_crack",
    "A_bad": "A cameras printed; pool=ab|peer bad",
    "weak": "no fade/A-bad; pool=not-blue AND short=low AND sma20=above",
    "none": "no fade or bad-A print — short book empty",
}


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
    df["A_bad"] = (df["ab"] == "bad") | (df["peer"] == "bad")
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
    df["inv_score"] = (
        3 * df["fade"].fillna(False).astype(int)
        + 2 * df["first_crack"].fillna(False).astype(int)
        + 2 * (df["ab"] == "bad").astype(int)
        + 2 * (df["peer"] == "bad").astype(int)
        + (df["short_b"] == "low").astype(int)
        + (df["sma20_b"] == "above").astype(int)
    )
    df["relvol_rk"] = df["relvol_b"].map(RELVOL_RANK).fillna(-1).astype(int)
    return df


def a_printed(g: pd.DataFrame) -> bool:
    return bool(g["ab"].isin(["good", "bad", "neutral"]).any() or g["peer"].isin(["good", "bad", "neutral"]).any())


def pool_mask(g: pd.DataFrame, seats: int = SEATS):
    alive = ~g["fade_x"]
    if a_printed(g):
        return alive & g["A"], "A"
    if int((alive & g["blue"]).sum()) >= seats:
        return alive & g["blue"], "blue"
    return alive & g["B_and"], "Band"


def short_pool_mask(g: pd.DataFrame):
    """Only seat a short book when fade or bad-A actually printed. No alphabet-soup fallback."""
    if bool(g["fade_x"].any()):
        return g["fade_x"], "fade"
    if a_printed(g) and bool(g["A_bad"].any()):
        return g["A_bad"], "A_bad"
    return pd.Series(False, index=g.index), "none"


def pick_seats(
    g: pd.DataFrame,
    mask: pd.Series,
    seats: int = SEATS,
    cap: int = SECTOR_CAP,
    score_col: str = "score",
) -> pd.DataFrame:
    sub = g.loc[mask].copy()
    if sub.empty:
        return sub
    if score_col not in sub.columns:
        score_col = "score"
    sub = sub.sort_values(
        [score_col, "points", "n_red", "relvol_rk", "Ticker"],
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


def annotate_actions(prev: set[str] | None, tickers: list[str]):
    """Label today's seats vs yesterday's book. Returns actions, bought, sold, held."""
    prev = set(prev or [])
    cur = set(tickers)
    actions = ["hold" if t in prev else "buy" for t in tickers]
    bought = [t for t in tickers if t not in prev]
    sold = sorted(prev - cur)
    held = [t for t in tickers if t in prev]
    return actions, bought, sold, held


def _rets(row) -> dict:
    out = {}
    for h in HORIZONS:
        v = row.get(f"ret_{h}")
        out[f"ret_{h}"] = None if v is None or pd.isna(v) else float(v)
    return out


def _pick_dict(r, extra=None) -> dict:
    d = {
        "Ticker": r["Ticker"],
        "score": int(r.get("score") or 0),
        "inv_score": int(r.get("inv_score") or 0),
        "blue": bool(r.get("blue")),
        "ab": r.get("ab"),
        "peer": r.get("peer"),
        "short": r.get("short_b"),
        "sma20": r.get("sma20_b"),
        "ab_up": bool(r.get("ab_up")),
        "points": int(r.get("points") or 0),
        "n_red": int(r.get("n_red") or 0),
        "relvol": r.get("relvol_b"),
        "sector": r.get("sector_name"),
        **_rets(r),
    }
    if extra:
        d.update(extra)
    return d


def _mean(rows: list[dict], key: str, clip: float | None = CLIP):
    xs = [r[key] for r in rows if r.get(key) is not None]
    if not xs:
        return None
    if clip is not None:
        xs = [max(-clip, min(clip, x)) for x in xs]
    return round(float(sum(xs) / len(xs)), 3)


def _wl(rows: list[dict], key: str = "ret_1d") -> tuple[int, int]:
    xs = [r[key] for r in rows if r.get(key) is not None]
    return sum(1 for x in xs if x > 0), sum(1 for x in xs if x < 0)


def session_row(date: str, g: pd.DataFrame, prev_long: set[str] | None = None, prev_short: set[str] | None = None) -> dict:
    mask, rule = pool_mask(g)
    pool = g.loc[mask]
    seats = pick_seats(g, mask)
    smask, srule = short_pool_mask(g)
    if len(seats):
        smask = smask & ~g["Ticker"].isin(set(seats["Ticker"]))
    shorts = pick_seats(g, smask, score_col="inv_score")

    long_tickers = list(seats["Ticker"]) if len(seats) else []
    short_tickers = list(shorts["Ticker"]) if len(shorts) else []
    actions, bought, sold, held = annotate_actions(prev_long, long_tickers)
    sactions, sbought, ssold, sheld = annotate_actions(prev_short, short_tickers)

    picks = [_pick_dict(r, {"side": "buy", "action": actions[i], "rule": rule}) for i, (_, r) in enumerate(seats.iterrows())]
    short_picks = [
        _pick_dict(r, {"side": "sell", "action": "short" if sactions[i] == "buy" else "hold", "rule": srule})
        for i, (_, r) in enumerate(shorts.iterrows())
    ]

    uni = {h: bt_report.name_stats(g[f"ret_{h}"]) for h in HORIZONS}
    out = {
        "date": date,
        "rule": rule,
        "short_rule": srule,
        "how": HOW.get(rule, rule),
        "short_how": HOW.get(srule, srule),
        "a_printed": a_printed(g),
        "n_book": int(len(g)),
        "n_pool": int(len(pool)),
        "n_seats": int(len(seats)),
        "n_short": int(len(shorts)),
        "n_blue": int(g["blue"].sum()),
        "n_ab_good": int(g["ab_good"].sum()),
        "n_peer_good": int(g["peer_good"].sum()),
        "n_band": int(g["B_and"].sum()),
        "bought": bought,
        "sold": sold,
        "held": held,
        "short_bought": sbought,
        "short_sold": ssold,
        "short_held": sheld,
        "uni": uni,
        "pool": {h: bt_report.name_stats(pool[f"ret_{h}"]) for h in HORIZONS},
        "seats": {h: bt_report.name_stats(seats[f"ret_{h}"]) if len(seats) else {"n": 0} for h in HORIZONS},
        "shorts": {h: bt_report.name_stats(shorts[f"ret_{h}"]) if len(shorts) else {"n": 0} for h in HORIZONS},
        "book": {h: _mean(picks, f"ret_{h}") for h in HORIZONS},
        "short_book": {h: (None if _mean(short_picks, f"ret_{h}") is None else round(-_mean(short_picks, f"ret_{h}"), 3)) for h in HORIZONS},
        "picks": picks,
        "short_picks": short_picks,
    }
    return out


def run(path: Path = PANEL) -> dict:
    df = load_panel(path)
    days = []
    prev_long: set[str] = set()
    prev_short: set[str] = set()
    prev_picks: dict[str, dict] = {}
    ledger = []
    for date, g in df.groupby("date", sort=True):
        d = session_row(str(date), g, prev_long, prev_short)
        sell_rows = []
        for t in d["sold"]:
            prior = dict(prev_picks.get(t) or {"Ticker": t})
            prior["side"] = "buy"
            prior["action"] = "sell"
            prior["date"] = str(date)
            prior["rule"] = d["rule"]
            sell_rows.append(prior)
        d["sells"] = sell_rows
        days.append(d)
        for p in d["picks"]:
            ledger.append({"date": d["date"], **p})
        for p in sell_rows:
            ledger.append({"date": d["date"], **p})
        for p in d["short_picks"]:
            ledger.append({"date": d["date"], **p})
        prev_long = set(p["Ticker"] for p in d["picks"])
        prev_short = set(p["Ticker"] for p in d["short_picks"])
        prev_picks = {p["Ticker"]: p for p in d["picks"]}

    long_1d = [d["book"]["1d"] for d in days if d["book"]["1d"] is not None]
    long_2d = [d["book"]["2d"] for d in days if d["book"]["2d"] is not None]
    short_1d = [d["short_book"]["1d"] for d in days if d["short_book"]["1d"] is not None]
    name_1d = [r["ret_1d"] for r in ledger if r.get("side") == "buy" and r.get("action") != "sell" and r.get("ret_1d") is not None]
    name_2d = [r["ret_2d"] for r in ledger if r.get("side") == "buy" and r.get("action") != "sell" and r.get("ret_2d") is not None]
    return {
        "generated_from": str(path),
        "seats": SEATS,
        "sector_cap": SECTOR_CAP,
        "rows": int(len(df)),
        "sessions": [d["date"] for d in days],
        "days": days,
        "ledger": ledger,
        "totals": {
            "long_1d_days": bt_report.day_book_stats(long_1d),
            "long_2d_days": bt_report.day_book_stats(long_2d),
            "short_1d_days": bt_report.day_book_stats(short_1d),
            "long_1d_names": bt_report.name_stats(name_1d),
            "long_2d_names": bt_report.name_stats(name_2d),
        },
    }


def _pct(x) -> str:
    return bt_report.fmt_pct(x)


def _num(d: dict, key: str) -> str:
    if not d or not d.get("n"):
        return "—"
    if key not in d or d[key] is None:
        return "—"
    if key in ("hit", "p_win", "p_loss", "p_flat"):
        return _pct(d[key])
    return bt_report.fmt_num(d[key])


def _ret(v) -> str:
    if v is None:
        return "—"
    return f"{v:+.2f}"


def render(report: dict) -> str:
    lines = []
    a = lines.append
    tot = report.get("totals") or {}
    a("# Boring winners — 25-seat book")
    a("")
    a("Daily-rebalanced long book from the FEATURE_MINE high-n edges. Equal-weight, close-to-close.")
    a("The short sleeve is the inverse filter (fade / bad A), disjoint from the longs. Empty when those cameras did not print.")
    a("")
    a("**Long:** Hit A = `ab=good` OR `peer=good`. Scale B = `short=high` OR `sma20=below`. Blue overlay. Fade / first_crack vetoed.")
    a(f"**Seats:** {SEATS} long + {SEATS} short. **Sector cap:** {SECTOR_CAP}. Score = `3·blue + 2·ab + 2·peer + 1·short_high + 1·sma_below + 1·ab_up`.")
    a("")
    a("A cameras only print from **2026-08-20**. Before that the live rule falls through to blue, then `short AND sma20=below`.")
    a("Settled `1d` only through **2026-08-20**. Later sessions have names, no close-to-close yet.")
    a("")
    a("## Daily book returns")
    a("")
    a("Equal-weight, clip ±30. Tickers and per-name 1d/2d/3d/1w are in the ledger below and in `boring_winners_picks.csv`.")
    a("")
    a("| date | rule | n | 1d | 2d | 3d | 1w | W | L | bought | sold | held |")
    a("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for d in report["days"]:
        w, l = _wl(d["picks"])
        a(
            f"| {d['date']} | `{d['rule']}` | {d['n_seats']} "
            f"| {_ret(d['book']['1d'])} | {_ret(d['book']['2d'])} "
            f"| {_ret(d['book']['3d'])} | {_ret(d['book']['1w'])} "
            f"| {w} | {l} "
            f"| {len(d['bought'])} | {len(d['sold'])} | {len(d['held'])} |"
        )
    a("")
    t1 = tot.get("long_1d_days") or {}
    t2 = tot.get("long_2d_days") or {}
    if t1.get("n_days"):
        a(
            f"Long book 1d: {t1['n_days']} priced days · "
            f"p(loss day)={_pct(t1.get('p_loss_day'))} · "
            f"mean={bt_report.fmt_num(t1.get('mean_day'))} · "
            f"cum={bt_report.fmt_num(t1.get('cum_sum'))} · "
            f"avg win day={bt_report.fmt_num(t1.get('avg_win_day'))} · "
            f"avg loss day={bt_report.fmt_num(t1.get('avg_loss_day'))}."
        )
    if t2.get("n_days"):
        a(
            f"Long book 2d: {t2['n_days']} priced days · "
            f"p(loss day)={_pct(t2.get('p_loss_day'))} · "
            f"mean={bt_report.fmt_num(t2.get('mean_day'))} · "
            f"cum={bt_report.fmt_num(t2.get('cum_sum'))}."
        )
    n1 = tot.get("long_1d_names") or {}
    if n1.get("n"):
        a(f"Long names 1d: {bt_report.fmt_stats_row(n1)}.")
    n2 = tot.get("long_2d_names") or {}
    if n2.get("n"):
        a(f"Long names 2d: {bt_report.fmt_stats_row(n2)}.")
    a("")
    a("## Daily short book (inverse, −1 × clipped name return)")
    a("")
    a("Shorts only when fade / first_crack printed, or when A cameras printed bad. Empty otherwise.")
    a("")
    a("| date | rule | n | 1d | 2d | new | covered |")
    a("|---|---|---:|---:|---:|---:|---:|")
    for d in report["days"]:
        a(
            f"| {d['date']} | `{d['short_rule']}` | {d['n_short']} "
            f"| {_ret(d['short_book']['1d'])} | {_ret(d['short_book']['2d'])} "
            f"| {len(d['short_bought'])} | {len(d['short_sold'])} |"
        )
    s1 = tot.get("short_1d_days") or {}
    if s1.get("n_days"):
        a("")
        a(
            f"Short book 1d: {s1['n_days']} priced days · "
            f"p(loss day)={_pct(s1.get('p_loss_day'))} · "
            f"mean={bt_report.fmt_num(s1.get('mean_day'))} · "
            f"cum={bt_report.fmt_num(s1.get('cum_sum'))}."
        )
    a("")
    a("## Each stock bought / held / sold")
    a("")
    a("Long seats only. `buy` = new that morning, `hold` = still in the book, `sell` = dropped at the next rebalance (returns are the last seated close-to-close). Shorts are in the daily short table and the CSV (`side=sell`).")
    a("")
    a("| date | action | Ticker | sector | score | 1d | 2d | 3d | 1w |")
    a("|---|---|---|---|---:|---:|---:|---:|---:|")
    for r in report.get("ledger") or []:
        if r.get("side") == "sell" and r.get("action") != "sell":
            continue
        a(
            f"| {r['date']} | {r.get('action','buy')} | {r['Ticker']} "
            f"| {r.get('sector') or '—'} | {r.get('score') if r.get('score') is not None else '—'} "
            f"| {_ret(r.get('ret_1d'))} | {_ret(r.get('ret_2d'))} "
            f"| {_ret(r.get('ret_3d'))} | {_ret(r.get('ret_1w'))} |"
        )
    a("")
    a("## Daily long books (compact)")
    a("")
    for d in report["days"]:
        a(f"### {d['date']} · `{d['rule']}` · pool {d['n_pool']} · {d['how']}")
        a("")
        if not d["picks"]:
            a("_empty pool_")
            a("")
            continue
        bits = []
        for r in d["picks"]:
            bits.append(f"{r['Ticker']} {_ret(r.get('ret_1d'))}/{_ret(r.get('ret_2d'))}")
        a(" ".join(bits))
        a("")
        s1d = d["seats"]["1d"]
        if s1d.get("n"):
            a(
                f"Seats 1d {bt_report.fmt_stats_row(s1d)} · "
                f"universe hit={_num(d['uni']['1d'],'p_win')} med={_num(d['uni']['1d'],'median')}."
            )
        else:
            a("1d not settled — names only.")
        a("")
    a("## Notes")
    a("")
    a("1. **A is not a multi-week edge in this panel.** One priced A-session, and that session was a broad up day.")
    a("2. **Blue is not constantly winning.** It can lift vs a red tape and still lose hard the next session.")
    a("3. **short AND sma20=below tracks the tape**, not a separate engine.")
    a("4. Use A as a same-morning seat-filler when the cameras printed. Re-score after later 1d prints settle.")
    a("")
    return "\n".join(lines) + "\n"


def daily_rows(report: dict) -> list[dict]:
    rows = []
    for d in report["days"]:
        w1, l1 = _wl(d["picks"], "ret_1d")
        w2, l2 = _wl(d["picks"], "ret_2d")
        rows.append({
            "date": d["date"],
            "strat": "boring_winners_live25",
            "side": "buy",
            "rule": d["rule"],
            "n_bought": len(d["bought"]),
            "bought": " ".join(d["bought"]),
            "n_sold": len(d["sold"]),
            "sold": " ".join(d["sold"]),
            "n_held": len(d["held"]),
            "held": " ".join(d["held"]),
            "n_seats": d["n_seats"],
            "hold_1d_pnl": d["book"]["1d"],
            "hold_2d_pnl": d["book"]["2d"],
            "hold_3d_pnl": d["book"]["3d"],
            "hold_1w_pnl": d["book"]["1w"],
            "hold_1d_w": w1,
            "hold_1d_l": l1,
            "hold_2d_w": w2,
            "hold_2d_l": l2,
            "how": d["how"],
        })
        sw1, sl1 = _wl(d["short_picks"], "ret_1d")
        sw2, sl2 = _wl(d["short_picks"], "ret_2d")
        rows.append({
            "date": d["date"],
            "strat": "boring_winners_live25",
            "side": "sell",
            "rule": d["short_rule"],
            "n_bought": len(d["short_bought"]),
            "bought": " ".join(d["short_bought"]),
            "n_sold": len(d["short_sold"]),
            "sold": " ".join(d["short_sold"]),
            "n_held": len(d["short_held"]),
            "held": " ".join(d["short_held"]),
            "n_seats": d["n_short"],
            "hold_1d_pnl": d["short_book"]["1d"],
            "hold_2d_pnl": d["short_book"]["2d"],
            "hold_3d_pnl": d["short_book"]["3d"],
            "hold_1w_pnl": d["short_book"]["1w"],
            "hold_1d_w": sw1,
            "hold_1d_l": sl1,
            "hold_2d_w": sw2,
            "hold_2d_l": sl2,
            "how": d["short_how"],
        })
    return rows


def main() -> None:
    report = run()
    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    OUT_MD.write_text(render(report), encoding="utf-8")
    slim = {k: v for k, v in report.items() if k != "ledger"}
    OUT_JSON.write_text(json.dumps(slim, indent=2), encoding="utf-8")
    pd.DataFrame(report["ledger"]).to_csv(OUT_CSV, index=False)
    pd.DataFrame(daily_rows(report)).to_csv(OUT_DAILY, index=False)
    print(f"[bw] days={len(report['days'])} seats={SEATS} md={OUT_MD} csv={OUT_CSV} daily={OUT_DAILY}", flush=True)


if __name__ == "__main__":
    main()
