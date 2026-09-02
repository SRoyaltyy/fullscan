"""Book-first overlay: 1d BUY walk + FEATURE_MINE stacks + as-of cameras.

Reads 03_scoreboard/feature_asof_panel.parquet. Does not rebuild it.

The mine-only 25-seat dump lost to stock-book BUY on the priced window
(+0.11 vs +0.55 mean day). This module starts from the same 1d BUY list
the Top Gainer As-Of walk uses, then overlays the mined stacks:

  keep   book BUY that is not fade (prefer a named stack)
  drop   book BUY that printed fade / first_crack
  swap   stack-less book names lose their seat to a better extra
  add    extras the book missed — only if they pass the BUY floor
         (mcap >= $400M, ADV >= 500k) and print a named stack

Each seat is painted with the same 12 cameras + coaches + marks +
hall-pass the Top Gainer As-Of board uses.

Short sleeve: book SELL ∩ fade.

Mine-only fill stays as a comparison column, not the live book.
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from src import bt_report
from src.gainer_asof import (
    _cond_cell,
    _era_skip,
    _labeled,
    _labeled_domains,
    color_name,
    lane_label,
    load_day_context,
    same_day_buy_rows,
    same_day_sell_rows,
    session_at_lag,
)
from src.ticker_lookback import build_index

ROOT = Path(__file__).resolve().parent.parent
PANEL = ROOT / "03_scoreboard" / "feature_asof_panel.parquet"
BOOK_DIR = ROOT / "data" / "stock_book"
EXPORT_DIR = ROOT / "data" / "exports"
OUT_MD = ROOT / "03_scoreboard" / "BORING_WINNERS.md"
OUT_LATEST = ROOT / "03_scoreboard" / "latest_boring_winners.md"
OUT_DAYS = ROOT / "03_scoreboard" / "boring_winners"
OUT_JSON = ROOT / "03_scoreboard" / "boring_winners.json"
OUT_CSV = ROOT / "03_scoreboard" / "boring_winners_picks.csv"
OUT_DAILY = ROOT / "03_scoreboard" / "boring_winners_daily.csv"
DAILY_DIR = ROOT / "01_daily"

HORIZONS = ("1d", "2d", "3d", "1w")
HORIZON_LAG = {"1d": 1, "2d": 2, "3d": 3, "1w": 5}
CLIP = 30.0
SEATS = 25
SECTOR_CAP = 6
# Book-quality gates for extras the 1d BUY list missed.
BUY_MCAP_M = 400.0
BUY_ADV_K = 500.0
MAX_EXTRAS = 5
MAX_SHORT = 8
EXTRA_SECTOR_CAP = 4
EXTRA_INDUSTRY_CAP = 3
EXTRA_LARGE_CAP = 4
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
    "book": "1d BUY kept — no mined stack printed on the seat",
    "book_only": "on the 1d BUY list, not in the mine panel",
    "overlay": "1d BUY walk × mined stacks: keep / drop fade / swap stack-less / add gated extras",
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
STACK_RANK = {name: i for i, name in enumerate(STACK_ORDER)}
WEAK_STACKS = {"none", "book_only", "book"}


def _finite(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    return s.replace([float("inf"), float("-inf")], pd.NA)


def _flag(g: pd.DataFrame, col: str, default=False) -> pd.Series:
    if col not in g.columns:
        return pd.Series(default, index=g.index)
    return g[col].fillna(False)


def stock_book_dates(book_dir: Path | None = None) -> list[str]:
    """Mornings that have a 1d BUY list — includes today once Stock Book ALL lands."""
    book_dir = Path(book_dir or BOOK_DIR)
    return sorted({p.name[:10] for p in book_dir.glob("????-??-??_stock_book.json")})


def _boolish(x) -> bool:
    if isinstance(x, bool):
        return x
    return str(x).strip().lower() in {"true", "1", "yes"}


def _relvol_bucket(rec: dict) -> str:
    tone = str(rec.get("relvol_b") or "").lower()
    if tone in RELVOL_RANK:
        return tone
    try:
        v = float(rec.get("relvol") or 0)
    except (TypeError, ValueError):
        v = 0.0
    if v >= 1.5:
        return "hot"
    if 0 < v < 0.7:
        return "dead"
    if v > 0:
        return "normal"
    return "missing"


def stub_panel_day(date: str, book_dir: Path | None = None) -> pd.DataFrame:
    """Build a mine-shaped day from that morning's stock-book CSV.

    Used when FEATURE_MINE parquet has not caught up (today). 1d stays
    empty until the next Finviz tape. Overlay marks come from lb_*.
    """
    path = Path(book_dir or BOOK_DIR) / f"{date}_stock_book.csv"
    if not path.exists():
        return pd.DataFrame()
    try:
        frame = pd.read_csv(path)
    except (OSError, ValueError, pd.errors.ParserError):
        return pd.DataFrame()
    if "Ticker" not in frame.columns:
        return pd.DataFrame()
    tones = (
        "src_join_tone", "src_sector_tone", "src_gen_tone", "src_ab_tone",
        "src_peer_tone", "src_heat_tone", "src_vol_tone",
    )
    rows = []
    for rec in frame.to_dict(orient="records"):
        t = str(rec.get("Ticker") or "").upper()
        if not t:
            continue
        def tone(key, fallback="missing"):
            v = str(rec.get(key) or fallback).lower()
            return v if v in {"good", "bad", "neutral", "missing"} else "missing"
        ab, peer, join = tone("src_ab_tone"), tone("src_peer_tone"), tone("src_join_tone")
        setups = str(rec.get("lb_setups") or "")
        try:
            points = int(float(rec.get("lb_points") or 0))
        except (TypeError, ValueError):
            points = 0
        try:
            ab_score = float(rec.get("s_ab")) if rec.get("s_ab") == rec.get("s_ab") else 0.0
        except (TypeError, ValueError):
            ab_score = 0.0
        rows.append({
            "Ticker": t,
            "date": date,
            "ab": ab,
            "peer": peer,
            "join": join,
            "ab_good": ab == "good",
            "peer_good": peer == "good",
            "join_good": join == "good",
            "blue": _boolish(rec.get("lb_blue")),
            "white": _boolish(rec.get("lb_zero_red")),
            "alarm": _boolish(rec.get("lb_alarm")),
            "fade": _boolish(rec.get("lb_fade")),
            "first_crack": "first_crack" in setups,
            "steady": False,
            "cond": rec.get("lb_cond") or "missing",
            "region": rec.get("lb_region") or "missing",
            "ab_up": ab_score > 0,
            "short_b": "missing",
            "sma20_b": "missing",
            "rsi_b": "missing",
            "gap_b": "missing",
            "relvol_b": _relvol_bucket(rec),
            "points": points,
            "n_red": sum(1 for k in tones if str(rec.get(k) or "").lower() == "bad"),
            "sector_name": rec.get("sector") or "UNK",
            **{f"ret_{h}": None for h in HORIZONS},
        })
    return pd.DataFrame(rows)


def _annotate_panel(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.copy()
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


def extend_panel_with_live_books(df: pd.DataFrame, book_dir: Path | None = None) -> pd.DataFrame:
    """Append stock-book mornings the mine parquet has not ingested yet (today)."""
    have = set(df["date"].astype(str)) if df is not None and len(df) else set()
    extra = []
    for date in stock_book_dates(book_dir):
        if date in have:
            continue
        stub = stub_panel_day(date, book_dir)
        if stub is None or stub.empty:
            continue
        extra.append(_annotate_panel(stub))
    if not extra:
        return df
    return pd.concat([df, *extra], ignore_index=True, sort=False)


def load_panel(path: Path = PANEL, book_dir: Path | None = None) -> pd.DataFrame:
    df = _annotate_panel(pd.read_parquet(path))
    return extend_panel_with_live_books(df, book_dir)


def _parse_finviz_pct(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    try:
        return float(str(x).strip().replace("%", "").replace(",", ""))
    except (TypeError, ValueError):
        return None


def finviz_session_dates(export_dir: Path | None = None) -> list[str]:
    """Trading dates that have a Finviz tape, plus lookback session dates."""
    export_dir = Path(export_dir or EXPORT_DIR)
    dates = {
        p.stem.replace("finviz_", "")
        for p in export_dir.glob("finviz_????-??-??.csv")
    }
    try:
        from src.ticker_lookback import session_dates
        dates.update(session_dates())
    except Exception:
        pass
    # Keep weekend tapes too — 8/30 is in the panel and has a Finviz file.
    return sorted(dates)


def load_finviz_px(date: str, export_dir: Path | None = None) -> dict[str, dict]:
    """Ticker → {price, change} from that morning's Finviz tape."""
    path = Path(export_dir or EXPORT_DIR) / f"finviz_{date}.csv"
    if not path.exists():
        return {}
    try:
        frame = pd.read_csv(path)
    except (OSError, ValueError, pd.errors.ParserError):
        return {}
    if "Ticker" not in frame.columns:
        return {}
    out = {}
    chg_col = "Change" if "Change" in frame.columns else ("Change %" if "Change %" in frame.columns else None)
    for rec in frame.to_dict(orient="records"):
        t = str(rec.get("Ticker") or "").upper()
        if not t:
            continue
        px = pd.to_numeric(rec.get("Price"), errors="coerce")
        chg = _parse_finviz_pct(rec.get(chg_col)) if chg_col else None
        out[t] = {
            "price": None if pd.isna(px) else float(px),
            "change": chg,
        }
    return out


def fill_returns_from_finviz(
    df: pd.DataFrame,
    export_dir: Path | None = None,
    session_cal: list[str] | None = None,
) -> pd.DataFrame:
    """Fill missing ret_1d/2d/3d/1w from successive Finviz Prices.

    Does not rebuild or write the mine parquet. Existing panel returns stay.
    1d also falls back to the next tape's Change% when a Price is missing.
    """
    export_dir = Path(export_dir or EXPORT_DIR)
    cal = list(session_cal) if session_cal is not None else finviz_session_dates(export_dir)
    if not cal:
        return df
    pos = {d: i for i, d in enumerate(cal)}
    needed = set()
    for d in df["date"].astype(str).unique():
        d = str(d)[:10]
        j = pos.get(d)
        if j is None:
            continue
        needed.add(d)
        for lag in HORIZON_LAG.values():
            if j + lag < len(cal):
                needed.add(cal[j + lag])
    maps = {d: load_finviz_px(d, export_dir) for d in sorted(needed)}
    out = df.copy()
    for h, lag in HORIZON_LAG.items():
        col = f"ret_{h}"
        if col not in out.columns:
            out[col] = pd.NA
        filled = []
        for _, r in out.iterrows():
            cur = r.get(col)
            if cur is not None and not pd.isna(cur):
                filled.append(cur)
                continue
            d = str(r["date"])[:10]
            t = str(r["Ticker"]).upper()
            j = pos.get(d)
            if j is None or j + lag >= len(cal):
                filled.append(cur)
                continue
            nxt = cal[j + lag]
            entry = (maps.get(d) or {}).get(t) or {}
            exitr = (maps.get(nxt) or {}).get(t) or {}
            ep, xp = entry.get("price"), exitr.get("price")
            if ep and xp:
                filled.append(round(100.0 * (xp / ep - 1.0), 3))
            elif lag == 1 and exitr.get("change") is not None:
                filled.append(round(float(exitr["change"]), 3))
            else:
                filled.append(cur)
        out[col] = _finite(pd.Series(filled, index=out.index))
    return out


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


def stack_label(r) -> str:
    """Best named FEATURE_MINE stack on a panel row (or book_only / none)."""
    if r is None:
        return "book_only"
    fade = bool(r.get("fade_x") if "fade_x" in getattr(r, "index", []) or "fade_x" in r else False)
    if not fade:
        fade = bool(r.get("fade") or r.get("first_crack"))
    if fade:
        return "fade"
    hot = bool(r.get("hot"))
    aand = bool(r.get("Aand"))
    blue = bool(r.get("blue"))
    steady = bool(r.get("steady_f") if r.get("steady_f") is not None else r.get("steady"))
    white = bool(r.get("white_f") if r.get("white_f") is not None else r.get("white"))
    alarm = bool(r.get("alarm_f") if r.get("alarm_f") is not None else r.get("alarm"))
    if hot and aand:
        return "hot_ab_peer"
    if steady and blue:
        return "steady_blue"
    if blue and white:
        return "blue_white"
    if blue:
        return "blue"
    if aand:
        return "ab_and_peer"
    if alarm and not white:
        return "alarm_rebound"
    if r.get("rsi_os"):
        return "rsi_oversold"
    if r.get("gap_dn"):
        return "gap_down"
    return "none"


def book_gate_ok(mcap_m, adv_k, size=None) -> bool:
    try:
        m = float(mcap_m) if mcap_m is not None and mcap_m == mcap_m else 0.0
        a = float(adv_k) if adv_k is not None and adv_k == adv_k else 0.0
    except (TypeError, ValueError):
        return False
    if str(size or "").lower() == "micro":
        return False
    return m >= BUY_MCAP_M and a >= BUY_ADV_K


def _is_large(urec: dict) -> bool:
    size = str((urec or {}).get("size") or "").lower()
    try:
        m = float((urec or {}).get("market_cap_m") or 0)
    except (TypeError, ValueError):
        m = 0.0
    return size in ("large", "mega") or m > 20000.0


def load_book_universe(date: str) -> dict[str, dict]:
    """That morning's scored stock-book CSV — the gated universe extras come from."""
    path = BOOK_DIR / f"{date}_stock_book.csv"
    if not path.exists():
        return {}
    try:
        frame = pd.read_csv(path)
    except (OSError, ValueError, pd.errors.ParserError):
        return {}
    out = {}
    for rec in frame.to_dict(orient="records"):
        t = str(rec.get("Ticker") or "").upper()
        if t:
            out[t] = rec
    return out


def _pack_seat(ticker: str, source: str, panel_row, urec: dict | None = None) -> dict:
    urec = urec or {}
    fade = bool(panel_row.get("fade_x")) if panel_row is not None else False
    stack = stack_label(panel_row)
    sector = None
    if panel_row is not None:
        sector = panel_row.get("sector_name")
    sector = sector or urec.get("sector") or "UNK"
    try:
        ms = int(panel_row.get("mine_score") or 0) if panel_row is not None else 0
    except (TypeError, ValueError):
        ms = 0
    try:
        bs = float(urec.get("score_1d")) if urec.get("score_1d") == urec.get("score_1d") else 0.0
    except (TypeError, ValueError):
        bs = 0.0
    return {
        "ticker": ticker,
        "source": source,
        "stack": stack,
        "fade": fade,
        "in_panel": panel_row is not None,
        "sector": str(sector or "UNK"),
        "industry": str(urec.get("industry") or ""),
        "mine_score": ms,
        "book_score": bs,
        "large": _is_large(urec),
        "row": panel_row,
    }


def _overlay_rank(p: dict) -> tuple:
    return (
        STACK_RANK.get(p["stack"], 99),
        -int(p.get("mine_score") or 0),
        -float(p.get("book_score") or 0),
        p["ticker"],
    )


def fill_overlay(
    g: pd.DataFrame,
    book_buys: list[str],
    universe: dict[str, dict] | None = None,
    seats: int = SEATS,
    cap: int = SECTOR_CAP,
    max_extras: int = MAX_EXTRAS,
) -> tuple[list[dict], str, list[dict]]:
    """Book-first seat: keep BUY, drop fade, swap stack-less, add gated extras."""
    universe = universe or {}
    by_t = {str(r["Ticker"]).upper(): r for _, r in g.iterrows()} if len(g) else {}

    kept_book: list[dict] = []
    dropped: list[dict] = []
    for t in book_buys:
        p = _pack_seat(t, "book", by_t.get(t), universe.get(t))
        if p["fade"]:
            dropped.append({**p, "why": "fade"})
            continue
        kept_book.append(p)

    extras: list[dict] = []
    book_set = set(book_buys)
    for t, urec in universe.items():
        if t in book_set:
            continue
        if urec.get("liquid") is False:
            continue
        if not book_gate_ok(urec.get("market_cap_m"), urec.get("avg_vol_k"), urec.get("size")):
            continue
        r = by_t.get(t)
        if r is None or r.get("fade_x"):
            continue
        stack = stack_label(r)
        if stack in WEAK_STACKS or stack == "fade":
            continue
        if stack in {"rsi_oversold", "gap_down"} and int(r.get("mine_score") or 0) < 3:
            continue
        extras.append(_pack_seat(t, "extra", r, urec))

    extras.sort(key=_overlay_rank)
    extras = extras[:max_extras]
    stacked_book = [p for p in kept_book if p["stack"] not in WEAK_STACKS]
    weak_book = [p for p in kept_book if p["stack"] in WEAK_STACKS]
    stacked_book.sort(key=_overlay_rank)
    weak_book.sort(key=_overlay_rank)

    # Stacked book first, then extras (they steal seats from stack-less book names).
    queue = stacked_book + extras + weak_book
    seats_out: list[dict] = []
    sec_n: dict[str, int] = {}
    ind_n: dict[str, int] = {}
    large_n = 0
    seen: set[str] = set()
    for p in queue:
        if p["ticker"] in seen:
            continue
        extra = p["source"] == "extra"
        sec = p["sector"]
        ind = p.get("industry") or ""
        # Book names already passed the 1d BUY gates. Never drop them for
        # our sector cap — extras are the only names that re-check 4/sector.
        if extra:
            if sec and sec_n.get(sec, 0) >= EXTRA_SECTOR_CAP:
                continue
            if ind and ind not in ("", "nan", "None") and ind_n.get(ind, 0) >= EXTRA_INDUSTRY_CAP:
                continue
            if p.get("large") and large_n >= EXTRA_LARGE_CAP:
                continue
        if len(seats_out) >= seats:
            if not extra:
                dropped.append({**p, "why": "swapped_for_mine_extra"})
            continue
        seats_out.append(p)
        seen.add(p["ticker"])
        if sec:
            sec_n[sec] = sec_n.get(sec, 0) + 1
        if extra and ind and ind not in ("", "nan", "None"):
            ind_n[ind] = ind_n.get(ind, 0) + 1
        if extra and p.get("large"):
            large_n += 1
        elif not extra and p.get("large"):
            large_n += 1

    seated = {p["ticker"] for p in seats_out}
    for p in kept_book:
        if p["ticker"] not in seated and not any(d["ticker"] == p["ticker"] for d in dropped):
            dropped.append({**p, "why": "swapped_for_mine_extra"})

    used = []
    for p in seats_out:
        if p["stack"] not in used and p["stack"] not in WEAK_STACKS:
            used.append(p["stack"])
    if not used:
        rule = "book" if seats_out else "empty"
    else:
        rule = "+".join(used)
    return seats_out, rule, dropped


def fill_short_overlay(
    g: pd.DataFrame,
    book_sells: list[str],
    seats: int = MAX_SHORT,
) -> list[dict]:
    """Book SELL ∩ fade — the short sleeve that actually combines both systems."""
    by_t = {str(r["Ticker"]).upper(): r for _, r in g.iterrows()} if len(g) else {}
    out = []
    for t in book_sells:
        r = by_t.get(t)
        if r is None or not bool(r.get("fade_x")):
            continue
        out.append(_pack_seat(t, "book", r))
        if len(out) >= seats:
            break
    return out


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


def _empty_row(ticker: str) -> dict:
    return {
        "Ticker": ticker,
        "stack": "book_only",
        "score": 0,
        "inv_score": 0,
        "blue": False,
        "ab": None,
        "peer": None,
        "short": None,
        "sma20": None,
        "ab_up": False,
        "white": False,
        "steady": False,
        "alarm": False,
        "join": None,
        "points": 0,
        "n_red": 0,
        "relvol": None,
        "rsi": None,
        "gap": None,
        "sector": None,
        **{f"ret_{h}": None for h in HORIZONS},
    }


def _pick_from_pack(p: dict, extra=None) -> dict:
    r = p.get("row")
    if r is not None:
        d = _pick_dict(r, extra)
    else:
        d = _empty_row(p["ticker"])
        if extra:
            d.update(extra)
    d["Ticker"] = p["ticker"]
    d["src"] = p.get("source") or "book"
    d["stack"] = p.get("stack") or d.get("stack")
    d["book_score"] = p.get("book_score")
    if p.get("why"):
        d["why"] = p["why"]
    return d


def _paint_row(
    date: str,
    ticker: str,
    book_row: dict | None,
    idx,
    ctx: dict,
    buy_today: set[str],
    sell_today: set[str],
) -> dict:
    sess = session_at_lag(idx, date, 0) if idx else None
    prior = session_at_lag(idx, date, 1) if idx else None
    row = dict(book_row or {})
    row["ticker"] = ticker
    painted = color_name(
        sess, row, buy_today,
        market_tone=ctx.get("market_tone"),
        book_domains=ctx.get("domains"),
        sell_today=sell_today,
        market_state=ctx.get("market_state"),
        book_lanes=ctx.get("lanes"),
        book_marks=ctx.get("marks"),
        book_opp=ctx.get("opp"),
        era_skip=_era_skip(date),
        lattice_live=ctx.get("lattice_live"),
        prior_sess=prior,
    )
    return painted


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


def session_row(
    date: str,
    g: pd.DataFrame,
    prev_long: set[str] | None = None,
    prev_short: set[str] | None = None,
    *,
    idx=None,
    ctx: dict | None = None,
    paint: bool = True,
) -> dict:
    buy_rows = same_day_buy_rows(date)
    sell_rows = same_day_sell_rows(date)
    book_buys = [r["ticker"] for r in buy_rows]
    book_sells = [r["ticker"] for r in sell_rows]
    buy_today = set(book_buys)
    sell_today = set(book_sells)
    universe = load_book_universe(date)
    packs, rule, dropped_packs = fill_overlay(g, book_buys, universe)
    short_packs = fill_short_overlay(g, book_sells)

    mine_seats, mine_rule = fill_seats(g)
    first_mask, _first_rule = pool_mask(g)
    pool = g.loc[first_mask]
    mine_picks = [_pick_dict(r, {"stack": r.get("stack") or mine_rule}) for _, r in mine_seats.iterrows()] if len(mine_seats) else []

    long_tickers = [p["ticker"] for p in packs]
    short_tickers = [p["ticker"] for p in short_packs]
    actions, bought, sold, held = annotate_actions(prev_long, long_tickers)
    sactions, sbought, ssold, sheld = annotate_actions(prev_short, short_tickers)

    ctx = ctx if ctx is not None else (load_day_context(date) if paint else {})
    book_by_t = {r["ticker"]: r for r in buy_rows + sell_rows}

    picks = []
    for i, pack in enumerate(packs):
        extra = {
            "side": "buy",
            "action": actions[i],
            "rule": pack.get("stack") or rule,
            "stack": pack.get("stack") or rule,
            "src": pack.get("source"),
        }
        pick = _pick_from_pack(pack, extra)
        if paint and idx is not None:
            painted = _paint_row(date, pack["ticker"], book_by_t.get(pack["ticker"]), idx, ctx, buy_today, sell_today)
            pick["marks"] = painted.get("marks")
            pick["marks_cell"] = painted.get("marks_cell")
            pick["condition"] = painted.get("condition")
            pick["region"] = painted.get("region")
            pick["lane"] = painted.get("lane")
            pick["lane_label"] = painted.get("lane_label")
            pick["labeled"] = painted.get("labeled")
            pick["labeled_domains"] = painted.get("labeled_domains")
            pick["boxes"] = painted.get("boxes")
            pick["domains"] = painted.get("domains")
            pick["mid_opp"] = painted.get("mid_opp")
            pick["overnight_buy"] = painted.get("overnight_buy")
            pick["on_1d_buy"] = painted.get("on_1d_buy")
            pick["on_1d_sell"] = painted.get("on_1d_sell")
        picks.append(pick)

    short_picks = []
    for i, pack in enumerate(short_packs):
        extra = {
            "side": "sell",
            "action": "short" if sactions[i] == "buy" else "hold",
            "rule": "fade",
            "stack": "fade",
            "src": "book",
        }
        pick = _pick_from_pack(pack, extra)
        if paint and idx is not None:
            painted = _paint_row(date, pack["ticker"], book_by_t.get(pack["ticker"]), idx, ctx, buy_today, sell_today)
            pick["marks"] = painted.get("marks")
            pick["marks_cell"] = painted.get("marks_cell")
            pick["condition"] = painted.get("condition")
            pick["lane"] = painted.get("lane")
            pick["lane_label"] = painted.get("lane_label")
            pick["labeled"] = painted.get("labeled")
            pick["labeled_domains"] = painted.get("labeled_domains")
            pick["mid_opp"] = painted.get("mid_opp")
            pick["on_1d_buy"] = painted.get("on_1d_buy")
            pick["on_1d_sell"] = painted.get("on_1d_sell")
        short_picks.append(pick)

    dropped = [_pick_from_pack(p, {"src": p.get("source"), "why": p.get("why"), "stack": p.get("stack")}) for p in dropped_packs]

    vs_book = _book_compare(g, date)
    uni = {h: bt_report.name_stats(g[f"ret_{h}"]) for h in HORIZONS}
    how_parts = [HOW.get(s, s) for s in rule.split("+") if s]
    n_keep = sum(1 for p in packs if p.get("source") == "book")
    n_add = sum(1 for p in packs if p.get("source") == "extra")
    overlay_rows = [p.get("row") for p in packs if p.get("row") is not None]
    overlay_df = pd.DataFrame(overlay_rows) if overlay_rows else g.head(0)
    short_rows = [p.get("row") for p in short_packs if p.get("row") is not None]
    short_df = pd.DataFrame(short_rows) if short_rows else g.head(0)
    return {
        "date": date,
        "rule": rule,
        "mine_rule": mine_rule,
        "short_rule": "fade" if short_picks else "none",
        "how": HOW["overlay"] + " → " + (" → ".join(how_parts) if how_parts else HOW.get(rule, rule)),
        "short_how": "book SELL ∩ fade",
        "a_printed": a_printed(g),
        "n_book": int(len(g)),
        "n_book_buy": int(len(book_buys)),
        "n_keep": n_keep,
        "n_add": n_add,
        "n_drop": int(len(dropped)),
        "n_pool": int(len(pool)),
        "n_seats": int(len(packs)),
        "n_short": int(len(short_packs)),
        "n_mine": int(len(mine_seats)),
        "n_blue": int(g["blue"].sum()),
        "n_ab_good": int(g["ab_good"].sum()),
        "n_peer_good": int(g["peer_good"].sum()),
        "market_state": (ctx or {}).get("market_state"),
        "market_tone": (ctx or {}).get("market_tone"),
        "bought": bought,
        "sold": sold,
        "held": held,
        "short_bought": sbought,
        "short_sold": ssold,
        "short_held": sheld,
        "uni": uni,
        "pool": {h: bt_report.name_stats(pool[f"ret_{h}"]) for h in HORIZONS},
        "seats": {h: bt_report.name_stats(overlay_df[f"ret_{h}"]) if len(overlay_df) and f"ret_{h}" in overlay_df.columns else {"n": 0} for h in HORIZONS},
        "shorts": {h: bt_report.name_stats(short_df[f"ret_{h}"]) if len(short_df) and f"ret_{h}" in short_df.columns else {"n": 0} for h in HORIZONS},
        "book": {h: _mean(picks, f"ret_{h}") for h in HORIZONS},
        "short_book": {h: (None if _mean(short_picks, f"ret_{h}") is None else round(-_mean(short_picks, f"ret_{h}"), 3)) for h in HORIZONS},
        "vs_book": vs_book,
        "vs_mine": {h: _mean(mine_picks, f"ret_{h}") for h in HORIZONS},
        "mine_rule_used": mine_rule,
        "picks": picks,
        "short_picks": short_picks,
        "dropped": dropped,
    }


def run(path: Path = PANEL) -> dict:
    df = load_panel(path)
    df = fill_returns_from_finviz(df)
    idx = build_index()
    days = []
    prev_long: set[str] = set()
    prev_short: set[str] = set()
    prev_picks: dict[str, dict] = {}
    ledger = []
    for date, g in df.groupby("date", sort=True):
        d = session_row(str(date), g, prev_long, prev_short, idx=idx, paint=True)
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
    mine_1d = [d["vs_mine"]["1d"] for d in days if (d.get("vs_mine") or {}).get("1d") is not None]
    live = None
    try:
        from zoneinfo import ZoneInfo
        from datetime import datetime as _dt
        live = _dt.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
    except Exception:
        live = None
    missing_live = bool(live and live not in [d["date"] for d in days] and Path(BOOK_DIR / f"{live}_stock_book.json").exists() is False)
    return {
        "generated_from": str(path),
        "mode": "book_x_mine_overlay",
        "live_date": live,
        "missing_live_book": missing_live,
        "seats": SEATS,
        "sector_cap": SECTOR_CAP,
        "max_extras": MAX_EXTRAS,
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
            "mine_only_1d_days": bt_report.day_book_stats(mine_1d),
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
    painted = any(r.get("labeled") or r.get("marks_cell") for r in rows)
    if painted:
        lines = [
            "| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |",
            "|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|",
        ]
        for i, r in enumerate(rows, 1):
            n = i if r.get("action") != "sell" else "—"
            hall = r.get("lane_label") or lane_label(r.get("lane"))
            lines.append(
                f"| {n} | {r.get('action','buy')} | `{r['Ticker']}` | {r.get('src') or 'book'} "
                f"| `{r.get('stack') or rule}` "
                f"| {r.get('marks_cell') or '—'} | {_cond_cell(r)} | {hall or '—'} "
                f"| {r.get('labeled') or _labeled(r.get('boxes'))} "
                f"| {r.get('labeled_domains') or _labeled_domains(r.get('domains'))} "
                f"| {_ret(r.get('ret_1d'))} | {_ret(r.get('ret_2d'))} "
                f"| {_ret(r.get('ret_3d'))} | {_ret(r.get('ret_1w'))} |"
            )
        return lines
    lines = [
        "| # | action | Ticker | src | stack | sector | relvol | score | 1d | 2d | 3d | 1w |",
        "|---:|---|---|---|---|---|---|---:|---:|---:|---:|---:|",
    ]
    for i, r in enumerate(rows, 1):
        n = i if r.get("action") != "sell" else "—"
        lines.append(
            f"| {n} | {r.get('action','buy')} | `{r['Ticker']}` | {r.get('src') or 'book'} "
            f"| `{r.get('stack') or rule}` "
            f"| {r.get('sector') or '—'} | {r.get('relvol') or '—'} | {r.get('score') if r.get('score') is not None else '—'} "
            f"| {_ret(r.get('ret_1d'))} | {_ret(r.get('ret_2d'))} "
            f"| {_ret(r.get('ret_3d'))} | {_ret(r.get('ret_1w'))} |"
        )
    return lines


def _day_score_line(d: dict) -> str:
    vs = (d.get("vs_book") or {}).get("book") or {}
    vm = d.get("vs_mine") or {}
    return (
        f"Overlay 1d {_ret(d['book']['1d'])} · 2d {_ret(d['book']['2d'])} · "
        f"3d {_ret(d['book']['3d'])} · 1w {_ret(d['book']['1w'])} · "
        f"W/L {_wl(d['picks'])[0]}/{_wl(d['picks'])[1]} · "
        f"stock-book BUY 1d {_ret(vs.get('1d'))} · mine-only 1d {_ret(vm.get('1d'))} · "
        f"universe med {_num(d['uni']['1d'],'median')}."
    )


def render_one_day(d: dict) -> str:
    lines = []
    a = lines.append
    tone = d.get("market_tone") or "—"
    state = d.get("market_state") or "—"
    a(f"# Boring winners overlay — {d['date']}")
    a("")
    a(f"**Market:** {state} · tone `{tone}`")
    a("")
    a(
        f"1d BUY n={d.get('n_book_buy', 0)} · overlay n={d['n_seats']} "
        f"(keep {d.get('n_keep', 0)} · add {d.get('n_add', 0)} · drop {d.get('n_drop', 0)}) · "
        f"stacks `{d['rule']}`"
    )
    a("")
    a(d["how"])
    a("")
    a(_day_score_line(d))
    a("")
    a("## Long overlay")
    a("")
    if d["picks"]:
        lines.extend(_name_table(d["picks"], d["rule"]))
        s1d = d["seats"]["1d"]
        a("")
        a(f"Seats 1d {bt_report.fmt_stats_row(s1d)}." if s1d.get("n") else "1d not settled — names only.")
    else:
        a("_empty overlay — no 1d BUY and no gated extra_")
    if d.get("dropped"):
        a("")
        a("## Dropped from 1d BUY")
        a("")
        a("| Ticker | why | stack | 1d | 2d |")
        a("|---|---|---|---:|---:|")
        for r in d["dropped"]:
            a(
                f"| `{r['Ticker']}` | {r.get('why') or '—'} | `{r.get('stack') or '—'}` "
                f"| {_ret(r.get('ret_1d'))} | {_ret(r.get('ret_2d'))} |"
            )
    if d.get("sells"):
        a("")
        a("## Sold overnight (last seated returns)")
        a("")
        lines.extend(_name_table(d["sells"], "prior"))
    if d.get("short_picks"):
        a("")
        a("## Short overlay (SELL ∩ fade)")
        a("")
        lines.extend(_name_table(d["short_picks"], d["short_rule"]))
    a("")
    return "\n".join(lines) + "\n"


def render(report: dict) -> str:
    lines = []
    a = lines.append
    tot = report.get("totals") or {}
    a("# Boring winners — book × mine overlay")
    a("")
    a("Starts from the **same 1d BUY list** the Top Gainer As-Of walk uses, then overlays the fixed FEATURE_MINE stacks. Equal-weight, close-to-close, clip ±30 on the book line. Per-name 1d/2d/3d/1w are raw.")
    if report.get("missing_live_book") and report.get("live_date"):
        a("")
        a(f"**{report['live_date']} is not on this board** — that morning's `data/stock_book/{report['live_date']}_stock_book.json` is not in the repo yet. Run **Stock Book ALL**, then re-run **Boring Winners Backtest**. Today's 1d will stay `—` until tomorrow's Finviz tape.")
    elif report.get("days") and report["days"][-1]["date"] == report.get("live_date"):
        a("")
        a(f"**{report['live_date']} is this morning's live book.** 1d/2d are blank until later tapes. Buys/sells vs yesterday are the eval.")
    a("")
    a("## How a seat is won")
    a("")
    a("1. **Keep** a 1d BUY name that is not fade. Prefer a named mine stack.")
    a("2. **Drop** a 1d BUY name that printed fade / first_crack.")
    a("3. **Swap** a stack-less book name for a gated extra with a named stack.")
    a("4. **Add** at most 5 extras the book missed — only if they pass the BUY floor (`mcap ≥ $400M`, `ADV ≥ 500k`, not micro) and print a named stack. Extras also honor the book's 4/sector, 3/industry, 4 large/mega caps.")
    a("5. **Paint** every seat with the 12 as-of cameras, 6 coaches, 🔵🚨⚪ marks, Cond, and hall-pass — same board as Top Gainer As-Of.")
    a("6. **Short** is book SELL ∩ fade, not the raw fade dump.")
    a("")
    a("Mine-only 25-seat fill is kept as a comparison column. It is not the live book.")
    a("")
    a("## Edges the overlay prefers")
    a("")
    a("| priority | stack | mine-board 1d | role |")
    a("|---:|---|---|---|")
    a("| 1 | `hot+ab+peer` | 70.6% hit · +3.14 mean · n=51 | scalp, if the book or a gated extra has it |")
    a("| 2 | `steady+blue` | 52.0% hit · +9.54 mean · n=1394 | core swing |")
    a("| 3 | `blue+white` | 49.4% hit · +10.48 mean · n=1246 | white only with blue |")
    a("| 4 | `blue` | 57.7% hit · +4.46 mean · n=3387 | baseline |")
    a("| 5 | `ab AND peer` | ~65% hit · ~+1 mean | modest fill |")
    a("| 6 | `alarm AND NOT white` | 47.7% hit · +2.27 mean | rebound |")
    a("| 7 | `rsi=oversold` / `gap=down` | low hit · huge mean | extras only if mine_score ≥ 3 |")
    a("| short | book SELL ∩ `fade` | 38.2% hit · −0.72 mean | short only |")
    a("")
    a("Never seated as a long: white alone, fade, `ab OR peer` dump, `join AND Band` alphabet dump, micro / sub-$400M extras.")
    a("Thin 1d BUY mornings stay thin — we do not force 25 junk seats. A cameras print from **2026-08-20**. Panel yfinance 1d is settled through **2026-08-20**; later days use the next Finviz tape (`Price`/`Price`, else `Change%`). The parquet is not rebuilt.")
    a("")
    a("Per-day files: `03_scoreboard/boring_winners/<date>.md` · today also at `01_daily/<date>_boring_winners.md` and `latest_boring_winners.md`.")
    a("")
    a("## Daily book returns")
    a("")
    a("| date | stacks | n | keep | add | drop | overlay 1d | book BUY 1d | mine-only 1d | uni med | 2d | W | L |")
    a("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for d in report["days"]:
        w, l = _wl(d["picks"])
        vs = (d.get("vs_book") or {}).get("book") or {}
        vm = d.get("vs_mine") or {}
        a(
            f"| {d['date']} | `{d['rule']}` | {d['n_seats']} "
            f"| {d.get('n_keep', 0)} | {d.get('n_add', 0)} | {d.get('n_drop', 0)} "
            f"| {_ret(d['book']['1d'])} | {_ret(vs.get('1d'))} | {_ret(vm.get('1d'))} "
            f"| {_num(d['uni']['1d'],'median')} | {_ret(d['book']['2d'])} "
            f"| {w} | {l} |"
        )
    a("")
    t1 = tot.get("long_1d_days") or {}
    tb = tot.get("stock_book_1d_days") or {}
    tm = tot.get("mine_only_1d_days") or {}
    if t1.get("n_days"):
        a(
            f"Overlay 1d: {t1['n_days']} priced days · "
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
    if tm.get("n_days"):
        a(
            f"Mine-only 25 1d (comparison): {tm['n_days']} priced days · "
            f"p(loss day)={_pct(tm.get('p_loss_day'))} · "
            f"mean={bt_report.fmt_num(tm.get('mean_day'))} · "
            f"cum={bt_report.fmt_num(tm.get('cum_sum'))}."
        )
    n1 = tot.get("long_1d_names") or {}
    if n1.get("n"):
        a(f"Overlay names 1d: {bt_report.fmt_stats_row(n1)}.")
    n2 = tot.get("long_2d_names") or {}
    if n2.get("n"):
        a(f"Overlay names 2d: {bt_report.fmt_stats_row(n2)}.")
    a("")
    a("## Daily short overlay (book SELL ∩ fade, −1 × clipped name return)")
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
            f"Short overlay 1d: {s1['n_days']} priced days · "
            f"p(loss day)={_pct(s1.get('p_loss_day'))} · "
            f"mean={bt_report.fmt_num(s1.get('mean_day'))} · "
            f"cum={bt_report.fmt_num(s1.get('cum_sum'))}."
        )
    a("")
    a("## Each day's stocks")
    a("")
    a("`keep` = on 1d BUY. `add` = gated extra. `buy` / `hold` this morning. `sell` = dropped overnight. Cameras and coaches are the Top Gainer As-Of 09:30 ET paint.")
    a("")
    for d in report["days"]:
        a(
            f"### {d['date']} · `{d['rule']}` · n={d['n_seats']} "
            f"(keep {d.get('n_keep', 0)} / add {d.get('n_add', 0)} / drop {d.get('n_drop', 0)})"
        )
        a("")
        if d.get("market_state") or d.get("market_tone"):
            a(f"Market: {d.get('market_state') or '—'} · tone `{d.get('market_tone') or '—'}`")
            a("")
        a(d["how"])
        a("")
        a(_day_score_line(d))
        a("")
        if d["picks"]:
            lines.extend(_name_table(d["picks"], d["rule"]))
        else:
            a("_empty overlay_")
        if d.get("dropped"):
            a("")
            a("Dropped from 1d BUY:")
            a("")
            a("| Ticker | why | stack | 1d |")
            a("|---|---|---|---:|")
            for r in d["dropped"]:
                a(f"| `{r['Ticker']}` | {r.get('why') or '—'} | `{r.get('stack') or '—'}` | {_ret(r.get('ret_1d'))} |")
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
    a("1. The point is the combination: book quality gates + mined stacks + as-of cameras. Mine-only lost to stock-book BUY on the priced window. Overlay should track the book and beat it when a stack adds or a fade drops.")
    a("2. `blue` board mean +4.46 is squeeze-contaminated. Book lines clip ±30.")
    a("3. HARD_RED / thin BUY mornings stay thin. We do not backfill 25 lottery names.")
    a("4. A cameras (`ab` / `peer`) only print from **2026-08-20**. Before that, extras cannot fire `hot+ab+peer` / `ab+peer`.")
    a("5. 1d after 8/20 comes from the next Finviz tape we already parse every morning. Same close-to-close idea as the panel; not a parquet rebuild.")
    a("")
    return "\n".join(lines) + "\n"


def daily_rows(report: dict) -> list[dict]:
    rows = []
    for d in report["days"]:
        w1, l1 = _wl(d["picks"], "ret_1d")
        w2, l2 = _wl(d["picks"], "ret_2d")
        vs = (d.get("vs_book") or {}).get("book") or {}
        vm = d.get("vs_mine") or {}
        rows.append({
            "date": d["date"],
            "strat": "boring_winners_overlay",
            "side": "buy",
            "rule": d["rule"],
            "n_keep": d.get("n_keep"),
            "n_add": d.get("n_add"),
            "n_drop": d.get("n_drop"),
            "n_book_buy": d.get("n_book_buy"),
            "mine_only_1d": vm.get("1d"),
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
            "strat": "boring_winners_overlay",
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
