"""25-seat book from the fixed FEATURE_MINE edges (DeepSeek hierarchy).

Reads 03_scoreboard/feature_asof_panel.parquet. Does not rebuild it.

Core fill (fade vetoed)
-----------------------
1. hot+ab+peer     up to 8 seats — 70.6% hit scalp (small n)
2. steady+blue     best risk/reward on the mine board
3. blue+white      combinatorial white (white alone is a trap)
4. blue            baseline
5. ab AND peer     up to 5 seats — high-hit modest mean
6. alarm+not_white contrarian rebound when no blue printed
7. rsi=oversold    lottery last resort
8. gap=down        lottery last resort

Never: white alone, fade, join-AND-Band dump, OR-dump of 1,800 A names.

Short sleeve: fade / first_crack only.
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
OUT_LATEST = ROOT / "03_scoreboard" / "latest_boring_winners.md"
OUT_DAYS = ROOT / "03_scoreboard" / "boring_winners"
OUT_JSON = ROOT / "03_scoreboard" / "boring_winners.json"
OUT_CSV = ROOT / "03_scoreboard" / "boring_winners_picks.csv"
OUT_DAILY = ROOT / "03_scoreboard" / "boring_winners_daily.csv"
DAILY_DIR = ROOT / "01_daily"

HORIZONS = ("1d", "2d", "3d", "1w")
CLIP = 30.0
SEATS = 25
SECTOR_CAP = 6
# Don't let 800 steady+blue names crowd out the 70% hit scalp.
STACK_CAP = {"hot_ab_peer": 8, "ab_and_peer": 5}
RELVOL_RANK = {"normal": 2, "dead": 1, "hot": 0, "missing": -1}
HOW = {
    "steady_blue": "core `steady+blue` (52% hit / +9.54 mean on the mine board)",
    "blue_white": "swing `blue+white` (white only with blue)",
    "blue": "baseline `blue`",
    "hot_ab_peer": "scalp `hot+ab+peer` (70.6% hit, small n)",
    "ab_and_peer": "scalp `ab AND peer` (high hit, modest mean)",
    "alarm_rebound": "rebound `alarm AND NOT white`",
    "rsi_oversold": "lottery `rsi=oversold` (low hit, huge mean)",
    "gap_down": "lottery `gap=down`",
    "empty": "no mined long stack printed",
    "fade": "fade / first_crack — short sleeve",
    "none": "no fade print — short book empty",
}
STACK_ORDER = (
    "hot_ab_peer",
    "steady_blue",
    "blue_white",
    "blue",
    "ab_and_peer",
    "alarm_rebound",
    "rsi_oversold",
    "gap_down",
)


def _finite(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    return s.replace([float("inf"), float("-inf")], pd.NA)


def _flag(g: pd.DataFrame, col: str, default=False) -> pd.Series:
    if col not in g.columns:
        return pd.Series(default, index=g.index)
    return g[col].fillna(False)


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
    df["steady_f"] = _flag(df, "steady")
    df["white_f"] = _flag(df, "white")
    df["join_f"] = _flag(df, "join_good")
    df["cond_f"] = df["cond"].eq("good") if "cond" in df.columns else False
    df["alarm_f"] = _flag(df, "alarm")
    df["rsi_os"] = df["rsi_b"].eq("oversold") if "rsi_b" in df.columns else False
    df["gap_dn"] = df["gap_b"].eq("down") if "gap_b" in df.columns else False
    blue = df["blue"].fillna(False)
    df["mine_score"] = (
        5 * (df["steady_f"] & blue).astype(int)
        + 4 * (blue & df["white_f"]).astype(int)
        + 3 * blue.astype(int)
        + 3 * (df["hot"] & df["Aand"]).astype(int)
        + 2 * (df["alarm_f"] & ~df["white_f"]).astype(int)
        + 2 * df["Aand"].astype(int)
        + df["ab_good"].fillna(False).astype(int)
        + df["peer_good"].fillna(False).astype(int)
        + df["ab_up"].fillna(False).astype(int)
        + df["rsi_os"].astype(int)
        + df["gap_dn"].astype(int)
        - 5 * df["fade_x"].astype(int)
        - 2 * (df["hot"] & ~blue & ~df["Aand"]).astype(int)
    )
    df["score"] = df["mine_score"]
    df["inv_score"] = (
        3 * df["fade"].fillna(False).astype(int)
        + 2 * df["first_crack"].fillna(False).astype(int)
    )
    df["relvol_rk"] = df["relvol_b"].map(RELVOL_RANK).fillna(-1).astype(int)
    return df


def a_printed(g: pd.DataFrame) -> bool:
    return bool(g["ab"].isin(["good", "bad", "neutral"]).any() or g["peer"].isin(["good", "bad", "neutral"]).any())


def stack_masks(g: pd.DataFrame) -> list[tuple[str, pd.Series]]:
    alive = ~g["fade_x"]
    blue = g["blue"].fillna(False)
    return [
        ("hot_ab_peer", alive & _flag(g, "hot") & _flag(g, "Aand")),
        ("steady_blue", alive & _flag(g, "steady_f") & blue),
        ("blue_white", alive & blue & _flag(g, "white_f")),
        ("blue", alive & blue),
        ("ab_and_peer", alive & _flag(g, "Aand")),
        ("alarm_rebound", alive & _flag(g, "alarm_f") & ~_flag(g, "white_f")),
        ("rsi_oversold", alive & _flag(g, "rsi_os")),
        ("gap_down", alive & _flag(g, "gap_dn")),
    ]


def pool_mask(g: pd.DataFrame, seats: int = SEATS):
    del seats
    for name, mask in stack_masks(g):
        if bool(mask.any()):
            return mask, name
    return pd.Series(False, index=g.index), "empty"


def short_pool_mask(g: pd.DataFrame):
    if bool(g["fade_x"].any()):
        return g["fade_x"], "fade"
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
        stack_cap = STACK_CAP.get(name)
        want = seats - len(kept)
        if stack_cap is not None:
            want = min(want, stack_cap)
        if want <= 0:
            continue
        got = 0
        chunk = pick_seats(g, remain, seats=want, cap=cap)
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
            if got >= want or len(kept) >= seats:
                break
        if got:
            used.append(name)
    return pd.DataFrame(kept), ("+".join(used) if used else "empty")


def annotate_actions(prev: set[str] | None, tickers: list[str]):
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
    ms = r.get("mine_score")
    if ms is None or (isinstance(ms, float) and pd.isna(ms)):
        ms = r.get("score") or 0
    d = {
        "Ticker": r["Ticker"],
        "stack": r.get("stack"),
        "score": int(ms),
        "inv_score": int(r.get("inv_score") or 0),
        "blue": bool(r.get("blue")),
        "ab": r.get("ab"),
        "peer": r.get("peer"),
        "short": r.get("short_b"),
        "sma20": r.get("sma20_b"),
        "ab_up": bool(r.get("ab_up")),
        "white": bool(r.get("white_f") if "white_f" in getattr(r, "index", []) else r.get("white")),
        "steady": bool(r.get("steady_f") if "steady_f" in getattr(r, "index", []) else r.get("steady")),
        "alarm": bool(r.get("alarm_f") if "alarm_f" in getattr(r, "index", []) else r.get("alarm")),
        "join": r.get("join"),
        "points": int(r.get("points") or 0),
        "n_red": int(r.get("n_red") or 0),
        "relvol": r.get("relvol_b"),
        "rsi": r.get("rsi_b"),
        "gap": r.get("gap_b"),
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
        return {"n": 0, "tickers": [], "book": {}}
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
    return {
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


def _name_table(rows: list[dict], rule: str) -> list[str]:
    lines = [
        "| # | action | Ticker | stack | sector | relvol | score | 1d | 2d | 3d | 1w |",
        "|---:|---|---|---|---|---|---:|---:|---:|---:|---:|",
    ]
    for i, r in enumerate(rows, 1):
        n = i if r.get("action") != "sell" else "—"
        lines.append(
            f"| {n} | {r.get('action','buy')} | {r['Ticker']} | `{r.get('stack') or rule}` "
            f"| {r.get('sector') or '—'} | {r.get('relvol') or '—'} | {r.get('score') if r.get('score') is not None else '—'} "
            f"| {_ret(r.get('ret_1d'))} | {_ret(r.get('ret_2d'))} "
            f"| {_ret(r.get('ret_3d'))} | {_ret(r.get('ret_1w'))} |"
        )
    return lines


def render_one_day(d: dict) -> str:
    lines = []
    a = lines.append
    vs = (d.get("vs_book") or {}).get("book") or {}
    a(f"# Boring winners — {d['date']}")
    a("")
    a(f"**Stacks:** `{d['rule']}`")
    a("")
    a(d["how"])
    a("")
    a(
        f"Book 1d {_ret(d['book']['1d'])} · 2d {_ret(d['book']['2d'])} · "
        f"3d {_ret(d['book']['3d'])} · 1w {_ret(d['book']['1w'])} · "
        f"W/L {_wl(d['picks'])[0]}/{_wl(d['picks'])[1]} · "
        f"stock-book BUY 1d {_ret(vs.get('1d'))} · universe med {_num(d['uni']['1d'],'median')}."
    )
    a("")
    a("## Longs (this morning)")
    a("")
    if d["picks"]:
        lines.extend(_name_table(d["picks"], d["rule"]))
        s1d = d["seats"]["1d"]
        a("")
        a(f"Seats 1d {bt_report.fmt_stats_row(s1d)}." if s1d.get("n") else "1d not settled — names only.")
    else:
        a("_empty core_")
    if d.get("sells"):
        a("")
        a("## Sold overnight (last seated returns)")
        a("")
        lines.extend(_name_table(d["sells"], "prior"))
    if d.get("short_picks"):
        a("")
        a("## Shorts (fade)")
        a("")
        lines.extend(_name_table(d["short_picks"], d["short_rule"]))
    a("")
    return "\n".join(lines) + "\n"


def render(report: dict) -> str:
    lines = []
    a = lines.append
    tot = report.get("totals") or {}
    a("# Boring winners — mined 25-seat book")
    a("")
    a("Daily-rebalanced from the **fixed FEATURE_MINE** edges. Equal-weight, close-to-close, clip ±30 on the book line. Per-name 1d/2d/3d/1w are raw.")
    a("")
    a("## Edges this seater uses")
    a("")
    a("| priority | stack | mine-board 1d | role |")
    a("|---:|---|---|---|")
    a("| 1 | `hot+ab+peer` | 70.6% hit · +3.14 mean · n=51 | up to 8 scalp seats |")
    a("| 2 | `steady+blue` | 52.0% hit · +9.54 mean · n=1394 | core swing |")
    a("| 3 | `blue+white` | 49.4% hit · +10.48 mean · n=1246 | white only with blue |")
    a("| 4 | `blue` | 57.7% hit · +4.46 mean · n=3387 | baseline |")
    a("| 5 | `ab AND peer` | ~65% hit · ~+1 mean | up to 5 modest fill |")
    a("| 6 | `alarm AND NOT white` | 47.7% hit · +2.27 mean | rebound when no blue |")
    a("| 7 | `rsi=oversold` / `gap=down` | low hit · huge mean | lottery last resort |")
    a("| short | `fade` / `first_crack` | 38.2% hit · −0.72 mean | short only |")
    a("")
    a("Never seated: white alone, fade as a long, `ab OR peer` 1,800-name dump, `join AND Band` alphabet dump.")
    a("Max 25, sector cap 6. Thin books are allowed. A cameras print from **2026-08-20**. Settled 1d through **2026-08-20**.")
    a("")
    a("Per-day files: `03_scoreboard/boring_winners/<date>.md` · today also at `01_daily/<date>_boring_winners.md` and `latest_boring_winners.md`.")
    a("")
    a("## Daily book returns")
    a("")
    a("| date | stacks | n | mine 1d | book BUY 1d | uni med | 2d | 3d | 1w | W | L |")
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
    a("## Daily short book (fade only, −1 × clipped name return)")
    a("")
    a("| date | n | 1d | 2d | new | covered |")
    a("|---|---:|---:|---:|---:|---:|")
    for d in report["days"]:
        a(
            f"| {d['date']} | {d['n_short']} "
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
    a("`buy` / `hold` this morning. `sell` = dropped overnight (last seated 1d/2d/3d/1w).")
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
        if d["picks"]:
            lines.extend(_name_table(d["picks"], d["rule"]))
        else:
            a("_empty pool_")
        if d.get("sells"):
            a("")
            a("Sold overnight:")
            a("")
            lines.extend(_name_table(d["sells"], "prior"))
        s1d = d["seats"]["1d"]
        a("")
        a(f"Seats 1d {bt_report.fmt_stats_row(s1d)}." if s1d.get("n") else "1d not settled — names only.")
        a("")
    a("## Notes")
    a("")
    a("1. Sub-50% hit is not the issue. Expectancy needs mean. The mine board now has both.")
    a("2. `blue` board mean +4.46 is squeeze-contaminated. Book lines clip ±30. Raw name 2d can still look insane.")
    a("3. `ab=good` / `peer=good` alone are high-hit / tiny-mean. They fill after blue, they do not lead.")
    a("4. No blue morning (8/13) falls through to lottery `rsi=oversold` / `gap=down`. That is labeled.")
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


def write_outputs(report: dict) -> None:
    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    OUT_DAYS.mkdir(parents=True, exist_ok=True)
    DAILY_DIR.mkdir(parents=True, exist_ok=True)
    OUT_MD.write_text(render(report), encoding="utf-8")
    slim = {k: v for k, v in report.items() if k != "ledger"}
    OUT_JSON.write_text(json.dumps(slim, indent=2), encoding="utf-8")
    pd.DataFrame(report["ledger"]).to_csv(OUT_CSV, index=False)
    pd.DataFrame(daily_rows(report)).to_csv(OUT_DAILY, index=False)
    for d in report["days"]:
        text = render_one_day(d)
        (OUT_DAYS / f"{d['date']}.md").write_text(text, encoding="utf-8")
    if report["days"]:
        latest = report["days"][-1]
        latest_text = render_one_day(latest)
        OUT_LATEST.write_text(latest_text, encoding="utf-8")
        (DAILY_DIR / f"{latest['date']}_boring_winners.md").write_text(latest_text, encoding="utf-8")
        (DAILY_DIR / "latest_boring_winners.md").write_text(latest_text, encoding="utf-8")


def main() -> None:
    report = run()
    write_outputs(report)
    print(
        f"[bw] days={len(report['days'])} seats={SEATS} "
        f"md={OUT_MD} days_dir={OUT_DAYS} daily={OUT_DAILY}",
        flush=True,
    )


if __name__ == "__main__":
    main()
