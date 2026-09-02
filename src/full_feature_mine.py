"""Full-feature as-of panel + mine.

One row per printed name per session (same idea as Top Gainer As-Of),
colored from the 09:30 packet only. Same-day Finviz / same-day book
never color a cell — Finviz numerics are the *prior* session export.

Families on every row:
  cameras          join/gen/sector/ab/peer/vol/heat + n_red
  lookback marks   blue / white (zero_red) / alarm / fade / cond / region
  featured setups  first_crack, vol+AB, blue+heat, …
  Finviz buckets   RSI, RelVol, SMA20, short float, gap, Perf Week, earn window
  quote colors     Δ / analyst (prior or same file if dated earlier)
  insider          5d open-market buy / sell cluster
  peer RS          quintile vs industry
  join rank        decile + climbed-vs-yesterday
  AB checklist     good/bad + score delta when present
  catalyst/events  name has a catalyst json or events flag that session
  stacks           steady_daily / fat_tail AND-gates

Then mine hit-rate and mean return on 1d/2d/3d/1w/2w.

  python -m src.full_feature_mine --from-date 2026-08-13
"""
from __future__ import annotations

import argparse
import json
import statistics
from datetime import datetime
from pathlib import Path

import pandas as pd

from . import book_marks as bm
from . import ticker_lookback as tl
from .ticker_lookback_setups import featured_book, match_day

ROOT = Path(__file__).resolve().parent.parent
SCORE = ROOT / "03_scoreboard"
DAILY = ROOT / "01_daily"
BOOK = ROOT / "data" / "stock_book"
EXPORTS = ROOT / "data" / "exports"
PEERS = ROOT / "data" / "peers"
QUOTE = ROOT / "data" / "quote_colors"
INSIDER = ROOT / "data" / "insider"
JOIN = ROOT / "data" / "join"
AB = ROOT / "data" / "ab_checklist"
CATAL = ROOT / "data" / "catalyst"
ASOF_DIR = ROOT / "data" / "feature_asof"
UNIV = ROOT / "data" / "universe"

OUT_ASOF_MD = SCORE / "FEATURE_ASOF.md"
OUT_MINE_MD = SCORE / "FEATURE_MINE.md"
OUT_JSON = SCORE / "full_feature_mine.json"
OUT_PARQUET = SCORE / "feature_asof_panel.parquet"

HORIZONS = ("1d", "2d", "3d", "1w", "2w")
HOPS = {"1d": 1, "2d": 2, "3d": 3, "1w": 5, "2w": 10}
MIN_N = 40
TOP_N = 20
MCAP_MIN = 100_000_000
ADV_MIN = 500_000

FINVIZ_ALIASES = {
    "Ticker": ("Ticker", "ticker"),
    "Market Cap": ("Market Cap", "MarketCap", "mcap"),
    "Average Volume": ("Average Volume", "Avg Volume", "AvgVolume"),
    "Relative Volume": ("Relative Volume", "Rel Volume", "RelVol"),
    "Relative Strength Index (14)": ("Relative Strength Index (14)", "RSI", "RSI (14)"),
    "20-Day Simple Moving Average": ("20-Day Simple Moving Average", "SMA20"),
    "Short Float": ("Short Float", "Short Float %"),
    "Gap": ("Gap",),
    "Performance (Week)": ("Performance (Week)", "Perf Week"),
    "Earnings Date": ("Earnings Date",),
    "Change": ("Change",),
    "Sector": ("Sector",),
    "Industry": ("Industry",),
}


def _tick(x) -> str:
    return str(x or "").strip().upper()


def _num(x):
    if x is None:
        return None
    if isinstance(x, (int, float)) and x == x:
        return float(x)
    s = str(x).strip().replace(",", "").replace("%", "")
    if s.endswith("B"):
        try:
            return float(s[:-1]) * 1e9
        except ValueError:
            return None
    if s.endswith("M"):
        try:
            return float(s[:-1]) * 1e6
        except ValueError:
            return None
    if s.endswith("K"):
        try:
            return float(s[:-1]) * 1e3
        except ValueError:
            return None
    try:
        return float(s)
    except ValueError:
        return None


def _rsi_bucket(v) -> str:
    x = _num(v)
    if x is None:
        return "missing"
    if x < 30:
        return "oversold"
    if x > 70:
        return "overbought"
    return "mid"


def _relvol_bucket(v) -> str:
    x = _num(v)
    if x is None:
        return "missing"
    if x >= 2.0:
        return "hot"
    if x < 0.6:
        return "dead"
    return "normal"


def _sma_bucket(v) -> str:
    x = _num(v)
    if x is None:
        return "missing"
    # Finviz SMA20 is % above/below the average
    if x >= 0:
        return "above"
    return "below"


def _short_bucket(v) -> str:
    x = _num(v)
    if x is None:
        return "missing"
    if x >= 8:
        return "high"
    if x >= 3:
        return "mid"
    return "low"


def _gap_bucket(v) -> str:
    x = _num(v)
    if x is None:
        return "missing"
    if x >= 3:
        return "up"
    if x <= -3:
        return "down"
    return "flat"


def _perf_bucket(v) -> str:
    x = _num(v)
    if x is None:
        return "missing"
    if x >= 8:
        return "extended"
    if x <= -8:
        return "washed"
    return "mid"


def _earn_soon(v, session: str) -> str:
    if v is None or (isinstance(v, float) and v != v):
        return "missing"
    s = str(v).strip()
    if not s or s.lower() in {"nan", "none", "-"}:
        return "missing"
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%b %d", "%b %d, %Y"):
        try:
            dt = datetime.strptime(s.replace("/", "-").replace(",", ""), fmt.replace("/", "-"))
            if fmt in ("%b %d",):
                dt = dt.replace(year=int(session[:4]))
            days = (dt.date() - datetime.strptime(session, "%Y-%m-%d").date()).days
            if 0 <= days <= 7:
                return "soon"
            if -3 <= days < 0:
                return "just"
            return "far"
        except ValueError:
            continue
    return "missing"


def _book_dates():
    return sorted(p.name[:10] for p in BOOK.glob("????-??-??_stock_book.csv"))


def _load_csv(path: Path) -> pd.DataFrame | None:
    if not path.is_file():
        return None
    try:
        df = pd.read_csv(path, low_memory=False)
    except OSError:
        return None
    if df is None or df.empty:
        return None
    col = "Ticker" if "Ticker" in df.columns else ("ticker" if "ticker" in df.columns else None)
    if not col:
        return None
    df = df.copy()
    df["Ticker"] = df[col].map(_tick)
    df = df[df["Ticker"] != ""]
    return df.drop_duplicates("Ticker", keep="first")


def _pick(df: pd.DataFrame, names) -> pd.Series | None:
    for n in names:
        if n in df.columns:
            return df[n]
    return None


def _finviz_for(date: str, prior: str | None) -> pd.DataFrame | None:
    # prior session export — no same-day leak
    use = prior or date
    p = EXPORTS / f"finviz_{use}.csv"
    if not p.is_file():
        files = sorted(EXPORTS.glob("finviz_????-??-??.csv"))
        p = None
        for f in files:
            if f.name[7:17] < date:
                p = f
        if p is None:
            return None
    return _load_csv(p)


def _peer_for(date: str) -> pd.DataFrame | None:
    p = PEERS / f"{date}_peer_rs.csv"
    if p.is_file():
        return _load_csv(p)
    files = sorted(PEERS.glob("????-??-??_peer_rs.csv"))
    prev = None
    for f in files:
        if f.name[:10] <= date:
            prev = f
    return _load_csv(prev) if prev else None


def _quote_for(date: str) -> pd.DataFrame | None:
    p = QUOTE / f"{date}_quote_colors.csv"
    if p.is_file():
        return _load_csv(p)
    files = sorted(QUOTE.glob("????-??-??_quote_colors.csv"))
    prev = None
    for f in files:
        if f.name[:10] < date:  # prior file only
            prev = f
    return _load_csv(prev) if prev else None


def _join_for(date: str) -> pd.DataFrame | None:
    p = JOIN / f"{date}_ranked.csv"
    return _load_csv(p) if p.is_file() else None


def _ab_for(date: str) -> pd.DataFrame | None:
    for name in (f"{date}_ab_checklist_enriched.csv", f"{date}_ab_checklist.csv"):
        p = AB / name
        if p.is_file():
            return _load_csv(p)
    return None


def _insider_maps():
    """ticker → list of (date, side) from daily trades files."""
    out: dict[str, list[tuple[str, str]]] = {}
    for p in sorted(INSIDER.glob("????-??-??_insider_trades.csv")):
        d = p.name[:10]
        try:
            df = pd.read_csv(p, low_memory=False, nrows=200000)
        except OSError:
            continue
        tcol = next((c for c in df.columns if c.lower() in {"ticker", "symbol"}), None)
        scol = next((c for c in df.columns if c.lower() in {"transaction", "type", "side", "tradetype"}), None)
        if not tcol:
            continue
        for _, r in df.iterrows():
            t = _tick(r.get(tcol))
            if not t:
                continue
            raw = str(r.get(scol) if scol else "").lower()
            side = "buy" if ("buy" in raw or "purchase" in raw or raw in {"p", "a"}) else (
                "sell" if ("sell" in raw or "sale" in raw or raw in {"s"}) else ""
            )
            if side:
                out.setdefault(t, []).append((d, side))
    return out


def _cluster(events: list[tuple[str, str]], session: str, side: str, window=5) -> bool:
    if not events:
        return False
    n = 0
    for d, s in events:
        if s != side:
            continue
        if d <= session and d >= _shift_cal(session, -window):
            n += 1
    return n >= 3


def _shift_cal(session: str, days: int) -> str:
    dt = datetime.strptime(session, "%Y-%m-%d")
    return (dt + pd.Timedelta(days=days)).date().isoformat()


def _catal_set(date: str) -> set[str]:
    names = set()
    for p in CATAL.glob(f"*_{date}.json"):
        names.add(_tick(p.name.split("_")[0]))
    return names


def _fwd(ticker: str, date: str) -> dict:
    panel = tl._price_panel()
    t = tl._tick(ticker)
    out = {h: None for h in HORIZONS}
    if panel is None or panel.empty or t not in panel.columns:
        return out
    idx = panel.index.searchsorted(pd.Timestamp(date))
    if idx >= len(panel.index) or panel.index[idx].date().isoformat() != date:
        return out
    entry = tl._num(panel[t].iloc[idx])
    if not entry:
        return out
    for h, n in HOPS.items():
        if idx + n >= len(panel.index):
            continue
        exitp = tl._num(panel[t].iloc[idx + n])
        if exitp:
            out[h] = round(100.0 * (exitp / entry - 1.0), 3)
    return out


def _cap_ok(row, liquid: bool) -> bool:
    if not liquid:
        return True
    mcap = _num(row.get("Market Cap") or row.get("mcap") or row.get("market_cap"))
    adv = _num(row.get("Average Volume") or row.get("adv") or row.get("avg_volume"))
    # If the book has no size cols, keep the row — Finviz filter applied later
    if mcap is None and adv is None:
        return True
    if mcap is not None and mcap < MCAP_MIN:
        return False
    if adv is not None and adv < ADV_MIN:
        return False
    return True


def build_panel(from_date=None, to_date=None, liquid=True) -> pd.DataFrame:
    dates = [d for d in _book_dates()
             if (not from_date or d >= from_date) and (not to_date or d <= to_date)]
    if not dates:
        return pd.DataFrame()
    insider = _insider_maps()
    rows = []
    prev_join_rank: dict[str, float] = {}
    prev_ab: dict[str, float] = {}
    print(f"[ff] sessions {dates[0]} → {dates[-1]} ({len(dates)})", flush=True)
    for i, date in enumerate(dates):
        book = _load_csv(BOOK / f"{date}_stock_book.csv")
        if book is None:
            continue
        prior = dates[i - 1] if i else None
        book = bm.attach(book, date)
        fv = _finviz_for(date, prior)
        peer = _peer_for(date)
        qc = _quote_for(date)
        jn = _join_for(date)
        ab = _ab_for(date)
        catal = _catal_set(date)
        fv_map = {r["Ticker"]: r for _, r in fv.iterrows()} if fv is not None else {}
        peer_map = {r["Ticker"]: r for _, r in peer.iterrows()} if peer is not None else {}
        qc_map = {r["Ticker"]: r for _, r in qc.iterrows()} if qc is not None else {}
        jn_map = {r["Ticker"]: r for _, r in jn.iterrows()} if jn is not None else {}
        ab_map = {r["Ticker"]: r for _, r in ab.iterrows()} if ab is not None else {}

        # peer quintile from whatever numeric rs col exists
        peer_q: dict[str, str] = {}
        if peer is not None:
            rscol = next((c for c in peer.columns if c.lower() in {
                "rs", "peer_rs", "rel_str", "rs_pct", "percentile"}), None)
            if rscol:
                try:
                    peer["_rs"] = pd.to_numeric(peer[rscol], errors="coerce")
                    peer["_q"] = pd.qcut(peer["_rs"].rank(method="first"), 5,
                                         labels=["q1", "q2", "q3", "q4", "q5"])
                    peer_q = dict(zip(peer["Ticker"], peer["_q"].astype(str)))
                except (ValueError, TypeError):
                    peer_q = {}

        for _, raw in book.iterrows():
            t = raw["Ticker"]
            if not t:
                continue
            fv_r = fv_map.get(t, {})
            if liquid:
                mcap = _num(fv_r.get("Market Cap") if isinstance(fv_r, dict) else None)
                if mcap is None:
                    mcap = _num(raw.get("Market Cap") or raw.get("mcap"))
                adv = _num(fv_r.get("Average Volume") if isinstance(fv_r, dict) else None)
                if mcap is not None and mcap < MCAP_MIN:
                    continue
                if adv is not None and adv < ADV_MIN:
                    continue
            boxes = bm.boxes_from_row(raw)
            marks = {
                "blue": bool(raw.get("lb_blue")),
                "white": bool(raw.get("lb_zero_red")),
                "alarm": bool(raw.get("lb_alarm")),
                "fade": bool(raw.get("lb_fade")),
                "cond": str(raw.get("lb_cond") or "missing"),
                "region": str(raw.get("lb_region") or "missing"),
                "points": int(raw.get("lb_points") or 0),
                "setups": str(raw.get("lb_setups") or ""),
                "tags": str(raw.get("lb_tags") or ""),
            }
            day = {
                "boxes": boxes,
                "region": {"tone": marks["region"]},
                "condition": {"tone": marks["cond"]},
                "zero_red": marks["white"],
                "signal_improved": marks["blue"],
                "signal_alarm": marks["alarm"],
                "stretch": {"tone": "missing"},
                "tag_context": [x for x in marks["tags"].split(",") if x],
            }
            setups = match_day(day)
            setup_ids = ",".join(s.get("id") or "" for s in setups if s.get("id"))
            n_red = sum(1 for v in boxes.values() if v == "bad")
            n_print = sum(1 for v in boxes.values() if v in {"good", "bad", "neutral"})
            if n_print < 3:
                continue
            fv_get = fv_r.get if isinstance(fv_r, pd.Series) else (fv_r.get if fv_r else (lambda *_: None))
            rsi = fv_get("Relative Strength Index (14)") if fv_r is not None else raw.get("rsi")
            relvol = fv_get("Relative Volume") if fv_r is not None else raw.get("relvol")
            sma20 = fv_get("20-Day Simple Moving Average") if fv_r is not None else None
            sh = fv_get("Short Float") if fv_r is not None else None
            gap = fv_get("Gap") if fv_r is not None else None
            perfw = fv_get("Performance (Week)") if fv_r is not None else None
            earn = fv_get("Earnings Date") if fv_r is not None else None
            jn_r = jn_map.get(t)
            rank = None
            if jn_r is not None:
                for c in ("rank", "Rank", "join_rank", "score", "join_score"):
                    if c in getattr(jn_r, "index", []):
                        rank = _num(jn_r.get(c))
                        break
            rank_up = False
            if rank is not None and t in prev_join_rank and prev_join_rank[t] is not None:
                rank_up = rank < prev_join_rank[t]  # lower rank number = climbed
            ab_r = ab_map.get(t)
            ab_score = None
            if ab_r is not None:
                for c in ("score", "ab_score", "total", "AB"):
                    if c in getattr(ab_r, "index", []):
                        ab_score = _num(ab_r.get(c))
                        break
            ab_up = False
            if ab_score is not None and t in prev_ab and prev_ab[t] is not None:
                ab_up = ab_score > prev_ab[t]
            qc_r = qc_map.get(t)
            qc_d = None
            qc_an = "missing"
            if qc_r is not None:
                qc_d = _num(qc_r.get("Δ") or qc_r.get("delta") or qc_r.get("d"))
                qc_an = str(qc_r.get("analyst") or qc_r.get("B19") or "missing").lower()
            ev = insider.get(t) or []
            fwd = _fwd(t, date)
            rec = {
                "Ticker": t,
                "date": date,
                "join": boxes.get("join", "missing"),
                "gen": boxes.get("gen", "missing"),
                "sector": boxes.get("sector", "missing"),
                "ab": boxes.get("ab", "missing"),
                "peer": boxes.get("peer", "missing"),
                "vol": boxes.get("vol", "missing"),
                "heat": boxes.get("heat", "missing"),
                "n_red": n_red,
                "n_print": n_print,
                "blue": marks["blue"],
                "white": marks["white"],
                "alarm": marks["alarm"],
                "fade": marks["fade"],
                "cond": marks["cond"],
                "region": marks["region"],
                "points": marks["points"],
                "setups": setup_ids or marks["setups"],
                "first_crack": "first_crack" in (setup_ids or marks["tags"]),
                "rsi_b": _rsi_bucket(rsi),
                "relvol_b": _relvol_bucket(relvol),
                "sma20_b": _sma_bucket(sma20),
                "short_b": _short_bucket(sh),
                "gap_b": _gap_bucket(gap),
                "perf_w_b": _perf_bucket(perfw),
                "earn_b": _earn_soon(earn, date),
                "qc_delta": qc_d,
                "qc_analyst": qc_an,
                "qc_hot": (qc_d is not None and qc_d >= 10),
                "ins_buy": _cluster(ev, date, "buy"),
                "ins_sell": _cluster(ev, date, "sell"),
                "peer_q": peer_q.get(t, "missing"),
                "join_rank": rank,
                "join_up": rank_up,
                "ab_score": ab_score,
                "ab_up": ab_up,
                "ab_good": boxes.get("ab") == "good",
                "peer_good": boxes.get("peer") == "good",
                "vol_good": boxes.get("vol") == "good",
                "join_good": boxes.get("join") == "good",
                "catal": t in catal,
                "sector_name": (fv_get("Sector") if fv_r is not None else raw.get("Sector")) or "",
                **{f"ret_{h}": fwd[h] for h in HORIZONS},
            }
            rec["steady"] = _steady(rec)
            rec["fat"] = _fat(rec)
            rows.append(rec)
            if rank is not None:
                prev_join_rank[t] = rank
            if ab_score is not None:
                prev_ab[t] = ab_score
        print(f"[ff] {date} rows={len(rows)}", flush=True)
    return pd.DataFrame(rows)


def _steady(row) -> bool:
    if row.get("alarm") or row.get("fade"):
        return False
    if (row.get("n_red") or 0) > 1:
        return False
    if row.get("rsi_b") == "overbought":
        return False
    if row.get("perf_w_b") == "extended":
        return False
    if row.get("ins_sell"):
        return False
    if row.get("peer_q") == "q1":
        return False
    return bool(row.get("white") or (row.get("n_red") == 0) or row.get("join_good"))


def _fat(row) -> bool:
    if row.get("relvol_b") != "hot":
        return False
    if not (row.get("ab_good") and row.get("peer_good") and row.get("vol_good")):
        return False
    return bool(row.get("ins_buy") or row.get("catal") or row.get("short_b") == "high")


def _where(rows, pred: dict) -> list[dict]:
    keep = []
    for r in rows:
        ok = True
        for k, v in pred.items():
            if r.get(k) != v:
                ok = False
                break
        if ok:
            keep.append(r)
    return keep


def day_medians(rows) -> dict:
    by: dict[str, dict[str, list]] = {}
    for row in rows:
        d = row.get("date")
        if not d:
            continue
        bucket = by.setdefault(d, {h: [] for h in HORIZONS})
        for h in HORIZONS:
            v = row.get(f"ret_{h}")
            if v is not None:
                bucket[h].append(float(v))
    out = {}
    for d, hs in by.items():
        out[d] = {h: (statistics.median(vs) if vs else None) for h, vs in hs.items()}
    return out


def attach_excess(rows, medians=None):
    medians = medians if medians is not None else day_medians(rows)
    for row in rows:
        med = medians.get(row.get("date") or "", {})
        for h in HORIZONS:
            raw = row.get(f"ret_{h}")
            mid = med.get(h)
            row[f"xs_{h}"] = None if raw is None or mid is None else float(raw) - float(mid)
    return rows


def summarize(rows, horizon="1d") -> dict:
    raw, xs = [], []
    hits = 0
    for row in rows:
        r = row.get(f"ret_{horizon}")
        if r is not None:
            raw.append(float(r))
            if r > 0:
                hits += 1
        x = row.get(f"xs_{horizon}")
        if x is not None:
            xs.append(float(x))
    if not raw:
        return {"n": 0, "horizon": horizon}
    return {
        "horizon": horizon,
        "n": len(raw),
        "hit": round(hits / len(raw), 4),
        "mean": round(statistics.fmean(raw), 3),
        "median": round(statistics.median(raw), 3),
        "mean_xs": None if not xs else round(statistics.fmean(xs), 3),
    }


def summarize_all(rows) -> dict:
    return {h: summarize(rows, h) for h in HORIZONS}


def pack_group(key, family, rows, base, min_n=MIN_N):
    stats = summarize_all(rows)
    d1 = stats.get("1d") or {}
    if d1.get("n", 0) < min_n:
        return None
    item = {"key": key, "family": family, "n": d1.get("n", 0), "stats": stats}
    for h in HORIZONS:
        s = stats.get(h) or {}
        b = (base.get(h) or {}) if base else {}
        item[f"{h}_n"] = s.get("n", 0)
        item[f"{h}_hit"] = s.get("hit")
        item[f"{h}_mean"] = s.get("mean")
        item[f"{h}_median"] = s.get("median")
        item[f"{h}_mean_xs"] = s.get("mean_xs")
        item[f"{h}_hit_lift"] = None if s.get("hit") is None or b.get("hit") is None else round(s["hit"] - b["hit"], 4)
        item[f"{h}_mean_lift"] = None if s.get("mean") is None or b.get("mean") is None else round(s["mean"] - b["mean"], 3)
    return item


PATTERNS = [
    ("mark", "blue", {"blue": True}),
    ("mark", "white", {"white": True}),
    ("mark", "alarm", {"alarm": True}),
    ("mark", "fade", {"fade": True}),
    ("mark", "blue+white", {"blue": True, "white": True}),
    ("mark", "blue+not_alarm", {"blue": True, "alarm": False}),
    ("mark", "alarm+not_white", {"alarm": True, "white": False}),
    ("mark", "first_crack", {"first_crack": True}),
    ("mark", "cond=good", {"cond": "good"}),
    ("mark", "cond=bad", {"cond": "bad"}),
    ("mark", "region=good", {"region": "good"}),
    ("mark", "region=bad", {"region": "bad"}),
    ("camera", "join=good", {"join": "good"}),
    ("camera", "vol=good", {"vol": "good"}),
    ("camera", "ab=good", {"ab": "good"}),
    ("camera", "peer=good", {"peer": "good"}),
    ("camera", "heat=good", {"heat": "good"}),
    ("camera", "heat=bad", {"heat": "bad"}),
    ("camera", "gen=bad", {"gen": "bad"}),
    ("combo", "vol+ab", {"vol": "good", "ab": "good"}),
    ("combo", "vol+ab+peer", {"vol": "good", "ab": "good", "peer": "good"}),
    ("combo", "blue+heat=bad", {"blue": True, "heat": "bad"}),
    ("combo", "blue+heat=good", {"blue": True, "heat": "good"}),
    ("combo", "alarm+heat=bad", {"alarm": True, "heat": "bad"}),
    ("combo", "white+join=good", {"white": True, "join": "good"}),
    ("combo", "vol+gen=bad", {"vol": "good", "gen": "bad"}),
    ("finviz", "rsi=oversold", {"rsi_b": "oversold"}),
    ("finviz", "rsi=overbought", {"rsi_b": "overbought"}),
    ("finviz", "relvol=hot", {"relvol_b": "hot"}),
    ("finviz", "relvol=dead", {"relvol_b": "dead"}),
    ("finviz", "sma20=above", {"sma20_b": "above"}),
    ("finviz", "sma20=below", {"sma20_b": "below"}),
    ("finviz", "short=high", {"short_b": "high"}),
    ("finviz", "gap=up", {"gap_b": "up"}),
    ("finviz", "gap=down", {"gap_b": "down"}),
    ("finviz", "perf_w=extended", {"perf_w_b": "extended"}),
    ("finviz", "perf_w=washed", {"perf_w_b": "washed"}),
    ("finviz", "earn=soon", {"earn_b": "soon"}),
    ("cross", "white+rsi=mid", {"white": True, "rsi_b": "mid"}),
    ("cross", "hot+short=high", {"relvol_b": "hot", "short_b": "high"}),
    ("cross", "hot+ab+peer", {"relvol_b": "hot", "ab": "good", "peer": "good"}),
    ("cross", "blue+relvol=hot", {"blue": True, "relvol_b": "hot"}),
    ("cross", "white+not_extended", {"white": True, "perf_w_b": "mid"}),
    ("quote", "qc_hot", {"qc_hot": True}),
    ("quote", "analyst=good", {"qc_analyst": "good"}),
    ("quote", "analyst=bad", {"qc_analyst": "bad"}),
    ("insider", "ins_buy", {"ins_buy": True}),
    ("insider", "ins_sell", {"ins_sell": True}),
    ("insider", "white+ins_buy", {"white": True, "ins_buy": True}),
    ("peer", "peer_q=q5", {"peer_q": "q5"}),
    ("peer", "peer_q=q1", {"peer_q": "q1"}),
    ("join", "join_up", {"join_up": True}),
    ("ab", "ab_up", {"ab_up": True}),
    ("event", "catal", {"catal": True}),
    ("event", "catal+hot", {"catal": True, "relvol_b": "hot"}),
    ("stack", "steady_daily", {"steady": True}),
    ("stack", "fat_tail", {"fat": True}),
    ("stack", "steady+blue", {"steady": True, "blue": True}),
    ("stack", "fat+blue", {"fat": True, "blue": True}),
    ("stack", "fat+not_alarm", {"fat": True, "alarm": False}),
]


def mine(rows):
    base_stats = summarize_all(rows)
    base = pack_group("ALL", "base", rows, base_stats, min_n=1) or {
        "key": "ALL", "family": "base", "n": 0, "stats": base_stats,
    }
    items = [base]
    for fam, key, pred in PATTERNS:
        got = _where(rows, pred)
        item = pack_group(key, fam, got, base_stats, min_n=MIN_N if fam != "event" else 15)
        if item:
            items.append(item)
    buckets: dict[str, list] = {}
    for it in items:
        buckets.setdefault(it["family"], []).append(it)
    return buckets


def _pct(x):
    return "—" if x is None else f"{100 * float(x):.1f}%"


def _nfmt(x, nd=2):
    return "—" if x is None else f"{float(x):+.{nd}f}"


def _table(rows, horizon, limit=TOP_N) -> str:
    lines = [
        f"| pattern | n | {horizon} hit | hit lift | {horizon} mean | mean lift | {horizon} xs | family |",
        "|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in rows[:limit]:
        lines.append(
            f"| `{row['key']}` | {row.get(f'{horizon}_n', row.get('n', 0))} | "
            f"{_pct(row.get(f'{horizon}_hit'))} | {_pct(row.get(f'{horizon}_hit_lift'))} | "
            f"{_nfmt(row.get(f'{horizon}_mean'))} | {_nfmt(row.get(f'{horizon}_mean_lift'))} | "
            f"{_nfmt(row.get(f'{horizon}_mean_xs'))} | {row.get('family')} |"
        )
    return "\n".join(lines) if len(lines) > 2 else "_nothing cleared min_n._"


def _rank(items, horizon, field):
    rows = [r for r in items if r.get(f"{horizon}_{field}") is not None and r.get("family") != "base"]
    rows.sort(key=lambda r: (-(r.get(f"{horizon}_{field}") or -999), -r.get("n", 0)))
    return rows


def render_mine(payload) -> str:
    meta = payload.get("meta") or {}
    buckets = payload.get("buckets") or {}
    base = (buckets.get("base") or [{}])[0]
    pool = []
    for fam, rows in buckets.items():
        if fam != "base":
            pool.extend(rows)
    L = [
        "# Full feature mine",
        "",
        f"_Generated {payload.get('generated_at')} · as-of 09:30 ET_",
        "",
        f"**{meta.get('n_rows', 0)}** printed name-days · sessions "
        f"{meta.get('from_date')} → {meta.get('to_date')}. "
        f"Liquid filter = {meta.get('liquid')}.",
        "",
        "Win = close-to-close > 0. Lift vs all-name base on the same horizon. "
        "Finviz numerics are the **prior session** export. Same-day book never colors a cell.",
        "",
        "🔵 blue = objectively better vs prior session (or +≥3 box points). "
        "⚪ white = zero_red. 🚨 alarm = purely worse. fade = featured fade setups.",
        "",
        "## Base rate",
        "",
        "| horizon | n | hit | mean | median | mean xs |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for h in HORIZONS:
        L.append(
            f"| {h} | {base.get(f'{h}_n', 0)} | {_pct(base.get(f'{h}_hit'))} | "
            f"{_nfmt(base.get(f'{h}_mean'))} | {_nfmt(base.get(f'{h}_median'))} | "
            f"{_nfmt(base.get(f'{h}_mean_xs'))} |"
        )
    L += ["", "## Lookback marks (🔵 / ⚪ / 🚨)", "", _table(buckets.get("mark") or [], "1d", 20), ""]
    L += ["## Strategy stacks", "", _table(buckets.get("stack") or [], "1d", 10), ""]
    for h in HORIZONS:
        L += [
            f"## {h} — highest win rate", "", _table(_rank(pool, h, "hit"), h), "",
            f"## {h} — highest average return", "", _table(_rank(pool, h, "mean"), h), "",
        ]
    for fam, title in (
        ("camera", "Cameras"),
        ("combo", "Light combos"),
        ("finviz", "Finviz buckets"),
        ("cross", "Cross (lights × Finviz)"),
        ("quote", "Quote-color card"),
        ("insider", "Insider clusters"),
        ("peer", "Peer RS"),
        ("join", "Join rank"),
        ("ab", "AB delta"),
        ("event", "Catalyst"),
    ):
        L += [f"## {title}", "", _table(buckets.get(fam) or [], "1d", 20), ""]
    return "\n".join(L)


def render_asof(df: pd.DataFrame, meta: dict) -> str:
    dates = sorted(df["date"].unique()) if len(df) else []
    L = [
        "# Feature as-of",
        "",
        "One row per printed name per session. Same vintage as Top Gainer As-Of: "
        "09:30 cameras + prior-session Finviz. Download `data/feature_asof/` CSVs "
        "or `03_scoreboard/feature_asof_panel.parquet` for the full grid.",
        "",
        f"Sessions **{meta.get('from_date')} → {meta.get('to_date')}** · "
        f"{meta.get('n_rows', 0)} name-days · {meta.get('n_dates', 0)} days.",
        "",
        "## Counts by session",
        "",
        "| date | n | blue | white | alarm | fade | steady | fat | catal |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for d in dates:
        sl = df[df["date"] == d]
        L.append(
            f"| {d} | {len(sl)} | {int(sl['blue'].sum())} | {int(sl['white'].sum())} | "
            f"{int(sl['alarm'].sum())} | {int(sl['fade'].sum())} | "
            f"{int(sl['steady'].sum())} | {int(sl['fat'].sum())} | {int(sl['catal'].sum())} |"
        )
    L += ["", "## Latest session sample (first 40)", ""]
    if dates:
        sl = df[df["date"] == dates[-1]].head(40)
        L += [
            "| Ticker | 🔵 | ⚪ | 🚨 | n_red | vol | ab | peer | relvol | rsi | short | setups | 1d |",
            "|---|:-:|:-:|:-:|---:|---|---|---|---|---|---|---|---:|",
        ]
        for _, r in sl.iterrows():
            L.append(
                f"| {r['Ticker']} | {'Y' if r['blue'] else ''} | {'Y' if r['white'] else ''} | "
                f"{'Y' if r['alarm'] else ''} | {r['n_red']} | {r['vol']} | {r['ab']} | {r['peer']} | "
                f"{r['relvol_b']} | {r['rsi_b']} | {r['short_b']} | {r.get('setups') or ''} | "
                f"{_nfmt(r.get('ret_1d'))} |"
            )
    return "\n".join(L)


def run(from_date=None, to_date=None, liquid=True, write=True):
    df = build_panel(from_date=from_date, to_date=to_date, liquid=liquid)
    rows = df.to_dict("records") if len(df) else []
    attach_excess(rows)
    if rows:
        df = pd.DataFrame(rows)
    buckets = mine(rows)
    dates = sorted({r["date"] for r in rows if r.get("date")})
    meta = {
        "n_rows": len(rows),
        "n_dates": len(dates),
        "from_date": from_date or (dates[0] if dates else None),
        "to_date": to_date or (dates[-1] if dates else None),
        "liquid": liquid,
        "horizons": list(HORIZONS),
        "min_n": MIN_N,
        "families": sorted({fam for fam, _, _ in PATTERNS}),
    }
    payload = {
        "generated_at": datetime.now(tl.ET).isoformat(),
        "asof": "09:30_et",
        "meta": meta,
        "buckets": buckets,
    }
    if write:
        SCORE.mkdir(parents=True, exist_ok=True)
        DAILY.mkdir(parents=True, exist_ok=True)
        ASOF_DIR.mkdir(parents=True, exist_ok=True)
        mine_md = render_mine(payload)
        asof_md = render_asof(df, meta)
        OUT_MINE_MD.write_text(mine_md, encoding="utf-8")
        OUT_ASOF_MD.write_text(asof_md, encoding="utf-8")
        day = datetime.now(tl.ET).date().isoformat()
        (DAILY / f"{day}_feature_mine.md").write_text(mine_md, encoding="utf-8")
        slim = dict(payload)
        OUT_JSON.write_text(json.dumps(slim, indent=2, default=str), encoding="utf-8")
        if len(df):
            try:
                df.to_parquet(OUT_PARQUET, index=False)
            except Exception as exc:
                print(f"[ff] parquet skip: {exc}")
            for d, sl in df.groupby("date"):
                sl.to_csv(ASOF_DIR / f"{d}_feature_asof.csv", index=False)
        print(mine_md[:8000])
        print(f"[ff] wrote {OUT_MINE_MD} and {OUT_ASOF_MD}")
    return payload


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-date", default="")
    ap.add_argument("--to-date", default="")
    ap.add_argument("--no-liquid", action="store_true")
    args = ap.parse_args()
    run(from_date=args.from_date or None,
        to_date=args.to_date or None,
        liquid=not args.no_liquid)


if __name__ == "__main__":
    main()
