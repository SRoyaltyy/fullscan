"""25-seat backtest from FEATURE_MINE AND-stacks.

Reads 03_scoreboard/feature_asof_panel.parquet (same vintage as the mine).
Does not rebuild the panel. No same-day Finviz leak — buckets are already as-of.

The mine board is promising. Seating `ab=good OR peer=good` (1,800 names) or
`short AND sma20=below` (500 names) is not. This seater merges the printed
cameras into the mined AND-stacks, in hit-rate order, and stops at 25.

Fill order (fade / first_crack vetoed; each stack adds new names only)
--------------------------------------------------------------------
1. hot+ab+peer          70.6% 1d hit on the mine board
2. ab AND peer          tighter than A-or
3. blue AND A           blue overlay on a printed hit camera
4. steady+blue          not hot (squeeze)
5. blue+white           not hot
6. A AND B              ab|peer good AND (short=high OR sma20=below)
7. blue                 not hot
8. join AND Band        last resort when no hit camera printed

Rank inside a stack: mine_score, lookback points, fewer reds, boring relvol
(normal > dead > hot), ticker. Sector cap 6.

Short sleeve: fade / first_crack, else ab|peer bad when A printed.
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from src import bt_report

ROOT = Path(__file__).resolve().parent.parent
PANEL = ROOT / "03_scoreboard" / "feature_asof_panel.parquet"
BOOK_DIR = ROOT / "data" / "stock_book"
OUT_MD = ROOT / "03_scoreboard" / "BORING_WINNERS.md"
OUT_JSON = ROOT / "03_scoreboard" / "boring_winners.json"
OUT_CSV = ROOT / "03_scoreboard" / "boring_winners_picks.csv"
OUT_DAILY = ROOT / "03_scoreboard" / "boring_winners_daily.csv"

HORIZONS = ("1d", "2d", "3d", "1w")
CLIP = 30.0
SEATS = 25
SECTOR_CAP = 6
# Boring rank: do not seat the hottest junk first. hot+ab+peer still wins on mine_score.
RELVOL_RANK = {"normal": 2, "dead": 1, "hot": 0, "missing": -1}
HOW = {
    "hot_ab_peer": "mined `hot+ab+peer` (70.6% 1d hit)",
    "ab_and_peer": "mined `ab=good AND peer=good`",
    "blue_A": "blue AND (ab|peer good)",
    "steady_blue": "mined `steady+blue`, not hot",
    "blue_white": "mined `blue+white`, not hot",
    "A_and_B": "(ab|peer good) AND (short=high OR sma20=below)",
    "blue": "blue, not hot",
    "join_band": "no hit camera; `join=good AND short=high AND sma20=below`, not hot",
    "empty": "no mined long stack printed",
    "fade": "fade or first_crack",
    "A_bad": "A cameras printed; pool=ab|peer bad",
    "none": "no fade or bad-A print — short book empty",
}
STACK_ORDER = (
    "hot_ab_peer",
    "ab_and_peer",
    "blue_A",
    "steady_blue",
    "blue_white",
    "A_and_B",
    "blue",
    "join_band",
)


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
    df["Aand"] = df["ab_good"].fillna(False) & df["peer_good"].fillna(False)
    df["A_bad"] = (df["ab"] == "bad") | (df["peer"] == "bad")
    df["B_or"] = (df["short_b"] == "high") | (df["sma20_b"] == "below")
    df["B_and"] = (df["short_b"] == "high") & (df["sma20_b"] == "below")
    df["fade_x"] = df["fade"].fillna(False) | df["first_crack"].fillna(False)
    df["hot"] = df["relvol_b"] == "hot"
    df["steady_f"] = df["steady"].fillna(False) if "steady" in df.columns else False
    df["white_f"] = df["white"].fillna(False)
    df["join_f"] = df["join_good"].fillna(False) if "join_good" in df.columns else False
    df["cond_f"] = (df["cond"] == "good") if "cond" in df.columns else False
    df["alarm_f"] = df["alarm"].fillna(False) if "alarm" in df.columns else False
    hot_ab = df["hot"] & df["Aand"]
    df["mine_score"] = (
        5 * hot_ab.astype(int)
        + 4 * df["Aand"].astype(int)
        + 3 * df["blue"].fillna(False).astype(int)
        + 2 * df["white_f"].astype(int)
        + 2 * df["steady_f"].astype(int)
        + 2 * df["ab_good"].fillna(False).astype(int)
        + 2 * df["peer_good"].fillna(False).astype(int)
        + df["ab_up"].fillna(False).astype(int)
        + (df["short_b"] == "high").astype(int)
        + (df["sma20_b"] == "below").astype(int)
        + df["join_f"].astype(int)
        + df["cond_f"].astype(int)
        - 2 * df["alarm_f"].astype(int)
        - 2 * (df["hot"] & ~df["Aand"]).astype(int)
        - (df["relvol_b"] == "dead").astype(int)
    )
    df["score"] = df["mine_score"]
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


def stack_masks(g: pd.DataFrame) -> list[tuple[str, pd.Series]]:
    alive = ~g["fade_x"]
    not_hot = ~g["hot"]
    return [
        ("hot_ab_peer", alive & g["hot"] & g["Aand"]),
        ("ab_and_peer", alive & g["Aand"]),
        ("blue_A", alive & g["blue"].fillna(False) & g["A"]),
        ("steady_blue", alive & g["steady_f"] & g["blue"].fillna(False) & not_hot),
        ("blue_white", alive & g["blue"].fillna(False) & g["white_f"] & not_hot),
        ("A_and_B", alive & g["A"] & g["B_or"]),
        ("blue", alive & g["blue"].fillna(False) & not_hot),
        ("join_band", alive & g["join_f"] & g["B_and"] & not_hot),
    ]


def pool_mask(g: pd.DataFrame, seats: int = SEATS):
    """First mined stack that actually printed. `seats` kept for call-site compat."""
    del seats
    for name, mask in stack_masks(g):
        if bool(mask.any()):
            return mask, name
    return pd.Series(False, index=g.index), "empty"


def short_pool_mask(g: pd.DataFrame):
    """Only seat a short book when fade or bad-A actually printed."""
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
    score_col: str = "mine_score",
) -> pd.DataFrame:
    sub = g.loc[mask].copy()
    if sub.empty:
        return sub
    if score_col not in sub.columns:
        score_col = "score" if "score" in sub.columns else list(sub.columns)[0]
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


def fill_seats(g: pd.DataFrame, seats: int = SEATS, cap: int = SECTOR_CAP) -> tuple[pd.DataFrame, str]:
    """Walk mined stacks in hit-rate order and add new names until `seats`."""
    taken: set[str] = set()
    kept = []
    sec_n: dict[str, int] = {}
    used = []
    for name, mask in stack_masks(g):
        if len(kept) >= seats:
            break
        remain = mask & ~g["Ticker"].isin(taken)
        if not bool(remain.any()):
            continue
        got = 0
        chunk = pick_seats(g, remain, seats=seats - len(kept), cap=cap)
        if chunk.empty:
            continue
        for _, row in chunk.iterrows():
            sec = str(row.get("sector_name") or "UNK") or "UNK"
            if sec_n.get(sec, 0) >= cap:
                continue
            row = row.copy()
            row["stack"] = name
            kept.append(row)
            taken.add(str(row["Ticker"]))
            sec_n[sec] = sec_n.get(sec, 0) + 1
            got += 1
            if len(kept) >= seats:
                break
        if got:
            used.append(name)
    rule = "+".join(used) if used else "empty"
    return pd.DataFrame(kept), rule


def annotate_actions(prev: set[str] | None, tickers: list[str]):
    """Label today's seats vs yesterday's book. Returns actions, bought, sold, held."""
    prev = set(prev or [])
    actions = ["hold" if t in prev else "buy" for t in tickers]
    bought = [t for t in tickers if t not in prev]
    sold = sorted(prev - set(tickers))
    held = [t for t in tickers if t in prev]
    return actions, bought, sold, held


def load_book_buys(date: str, top: int = 25) -> list[str]:
    path = BOOK_DIR / f"{date}_stock_book.json"
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    buys = ((data.get("books") or {}).get("1d") or {}).get("buy") or []
    out = []
    for row in buys:
        t = str(row.get("ticker") or "").upper()
        if t:
            out.append(t)
        if len(out) >= top:
            break
    return out


def _rets(row) -> dict:
    out = {}
    for h in HORIZONS:
        v = row.get(f"ret_{h}")
        out[f"ret_{h}"] = None if v is None or pd.isna(v) else float(v)
    return out


def _pick_dict(r, extra=None) -> dict:
    d = {
        "Ticker": r["Ticker"],
        "stack": r.get("stack") or extra.get("stack") if extra else r.get("stack"),
        "score": int(r.get("mine_score") if pd.notna(r.get("mine_score")) else r.get("score") or 0),
        "inv_score": int(r.get("inv_score") or 0),
        "blue": bool(r.get("blue")),
        "ab": r.get("ab"),
        "peer": r.get("peer"),
        "short": r.get("short_b"),
        "sma20": r.get("sma20_b"),
        "ab_up": bool(r.get("ab_up")),
        "white": bool(r.get("white_f") if "white_f" in r.index else r.get("white")),
        "steady": bool(r.get("steady_f") if "steady_f" in r.index else r.get("steady")),
        "join": r.get("join"),
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


def _book_compare(g: pd.DataFrame, date: str) -> dict:
    tickers = load_book_buys(date)
    if not tickers:
        return {"n": 0, "tickers": []}
    sub = g[g["Ticker"].isin(tickers)]
    rows = [_pick_dict(r) for _, r in sub.iterrows()]
    return {
        "n": int(len(tickers)),
        "n_priced": int(sub["ret_1d"].notna().sum()) if len(sub) else 0,
        "tickers": tickers,
        "book": {h: _mean(rows, f"ret_{h}") for h in HORIZONS},
        "stats": {h: bt_report.name_stats(sub[f"ret_{h}"]) if len(sub) else {"n": 0} for h in HORIZONS},
    }


def session_row(date: str, g: pd.DataFrame, prev_long: set[str] | None = None, prev_short: set[str] | None = None) -> dict:
    seats, rule = fill_seats(g)
    first_mask, first_rule = pool_mask(g)
    pool = g.loc[first_mask]
    smask, srule = short_pool_mask(g)
    if len(seats):
        smask = smask & ~g["Ticker"].isin(set(seats["Ticker"]))
    shorts = pick_seats(g, smask, score_col="inv_score")

    long_tickers = list(seats["Ticker"]) if len(seats) else []
    short_tickers = list(shorts["Ticker"]) if len(shorts) else []
    actions, bought, sold, held = annotate_actions(prev_long, long_tickers)
    sactions, sbought, ssold, sheld = annotate_actions(prev_short, short_tickers)

    picks = [
        _pick_dict(r, {"side": "buy", "action": actions[i], "rule": r.get("stack") or rule, "stack": r.get("stack") or first_rule})
        for i, (_, r) in enumerate(seats.iterrows())
    ]
    short_picks = [
        _pick_dict(r, {"side": "sell", "action": "short" if sactions[i] == "buy" else "hold", "rule": srule, "stack": srule})
        for i, (_, r) in enumerate(shorts.iterrows())
    ]
    vs_book = _book_compare(g, date)

    uni = {h: bt_report.name_stats(g[f"ret_{h}"]) for h in HORIZONS}
    how_parts = [HOW.get(s, s) for s in rule.split("+") if s]
    out = {
        "date": date,
        "rule": rule,
        "short_rule": srule,
        "how": " → ".join(how_parts) if how_parts else HOW["empty"],
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
        "vs_book": vs_book,
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
    book_1d = [d["vs_book"]["book"]["1d"] for d in days if (d.get("vs_book") or {}).get("book", {}).get("1d") is not None]
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
            "stock_book_1d_days": bt_report.day_book_stats(book_1d),
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
    a("# Boring winners — 25-seat mined stacks")
    a("")
    a("Daily-rebalanced long book from **FEATURE_MINE AND-stacks**, not the wide OR dump.")
    a("Equal-weight, close-to-close, clip ±30 on the book line. Per-name returns below are raw.")
    a("")
    a("Fill order: `hot+ab+peer` → `ab AND peer` → `blue AND A` → `steady+blue` → `blue+white` → `A AND B` → `blue` → `join AND Band`.")
    a("Fade / first_crack vetoed. Hot names stay out except on `hot+ab+peer`. Sector cap 6. Max 25 — thin books are allowed.")
    a("")
    a("A cameras only print from **2026-08-20**. Settled `1d` only through **2026-08-20**.")
    a("Current method = stock-book 1d BUY, graded on the same as-of panel (not the yfinance book backtest).")
    a("")
    a("## Daily book returns")
    a("")
    a("| date | stacks | n | mine 1d | book BUY 1d | uni 1d | 2d | 3d | 1w | W | L |")
    a("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for d in report["days"]:
        w, l = _wl(d["picks"])
        vs = (d.get("vs_book") or {}).get("book") or {}
        a(
            f"| {d['date']} | `{d['rule']}` | {d['n_seats']} "
            f"| {_ret(d['book']['1d'])} | {_ret(vs.get('1d'))} | {_num(d['uni']['1d'],'median')} "
            f"| {_ret(d['book']['2d'])} | {_ret(d['book']['3d'])} | {_ret(d['book']['1w'])} "
            f"| {w} | {l} |"
        )
    a("")
    t1 = tot.get("long_1d_days") or {}
    tb = tot.get("stock_book_1d_days") or {}
    if t1.get("n_days"):
        a(
            f"Mine book 1d: {t1['n_days']} priced days · "
            f"p(loss day)={_pct(t1.get('p_loss_day'))} · "
            f"mean={bt_report.fmt_num(t1.get('mean_day'))} · "
            f"cum={bt_report.fmt_num(t1.get('cum_sum'))}."
        )
    if tb.get("n_days"):
        a(
            f"Stock-book BUY 1d (same panel): {tb['n_days']} priced days · "
            f"p(loss day)={_pct(tb.get('p_loss_day'))} · "
            f"mean={bt_report.fmt_num(tb.get('mean_day'))} · "
            f"cum={bt_report.fmt_num(tb.get('cum_sum'))}."
        )
    n1 = tot.get("long_1d_names") or {}
    if n1.get("n"):
        a(f"Mine names 1d: {bt_report.fmt_stats_row(n1)}.")
    n2 = tot.get("long_2d_names") or {}
    if n2.get("n"):
        a(f"Mine names 2d: {bt_report.fmt_stats_row(n2)}.")
    a("")
    a("## Daily short book (inverse, −1 × clipped name return)")
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
    a("## Each day's stocks")
    a("")
    a("One table per session. `buy` = new that morning, `hold` = still seated. `sell` rows are names dropped overnight (last seated returns).")
    a("")
    for d in report["days"]:
        vs = (d.get("vs_book") or {}).get("book") or {}
        a(f"### {d['date']} · `{d['rule']}` · n={d['n_seats']}")
        a("")
        a(d["how"])
        a("")
        a(
            f"Book 1d {_ret(d['book']['1d'])} · 2d {_ret(d['book']['2d'])} · "
            f"3d {_ret(d['book']['3d'])} · 1w {_ret(d['book']['1w'])} · "
            f"stock-book BUY 1d {_ret(vs.get('1d'))} · universe med {_num(d['uni']['1d'],'median')}."
        )
        a("")
        if not d["picks"] and not d.get("sells"):
            a("_empty pool_")
            a("")
            continue
        a("| # | action | Ticker | stack | sector | relvol | score | 1d | 2d | 3d | 1w |")
        a("|---:|---|---|---|---|---|---:|---:|---:|---:|---:|")
        for i, r in enumerate(d["picks"], 1):
            a(
                f"| {i} | {r.get('action','buy')} | {r['Ticker']} | `{r.get('stack') or d['rule']}` "
                f"| {r.get('sector') or '—'} | {r.get('relvol') or '—'} | {r.get('score') if r.get('score') is not None else '—'} "
                f"| {_ret(r.get('ret_1d'))} | {_ret(r.get('ret_2d'))} "
                f"| {_ret(r.get('ret_3d'))} | {_ret(r.get('ret_1w'))} |"
            )
        for r in d.get("sells") or []:
            a(
                f"| — | sell | {r['Ticker']} | `{r.get('stack') or 'prior'}` "
                f"| {r.get('sector') or '—'} | {r.get('relvol') or '—'} | {r.get('score') if r.get('score') is not None else '—'} "
                f"| {_ret(r.get('ret_1d'))} | {_ret(r.get('ret_2d'))} "
                f"| {_ret(r.get('ret_3d'))} | {_ret(r.get('ret_1w'))} |"
            )
        s1d = d["seats"]["1d"]
        if s1d.get("n"):
            a("")
            a(f"Seats 1d {bt_report.fmt_stats_row(s1d)}.")
        else:
            a("")
            a("1d not settled — names only.")
        a("")
    a("## Notes")
    a("")
    a("1. Board `ab=good` / `peer=good` hit-rates are almost entirely the A-camera window (from 20 Aug) and one broad up day. AND them; do not OR-dump 1,800 names.")
    a("2. `blue+relvol=hot` and `rsi=oversold` print huge means because of squeezes. They are not the boring book.")
    a("3. `join_band` is a last resort on mornings with no hit camera. It will track the tape.")
    a("4. Re-score after 21 Aug / 27 Aug / 30 Aug 1d settles.")
    a("")
    return "\n".join(lines) + "\n"


def daily_rows(report: dict) -> list[dict]:
    rows = []
    for d in report["days"]:
        w1, l1 = _wl(d["picks"], "ret_1d")
        w2, l2 = _wl(d["picks"], "ret_2d")
        vs = (d.get("vs_book") or {}).get("book") or {}
        rows.append({
            "date": d["date"],
            "strat": "boring_winners_mine25",
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
            "book_buy_1d": vs.get("1d"),
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
            "strat": "boring_winners_mine25",
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
            "book_buy_1d": None,
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
