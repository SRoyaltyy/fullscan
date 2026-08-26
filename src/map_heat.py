"""Finviz map-heat: industry vs parent, SPX/RUT captains, tape, calendar.

Tables not pictures. Reads the newest Elite export plus live Finviz groups /
futures / calendars. Sentiment on the top-2 names is rule-based from the
export News Title + Daily Digest (Grok batch can replace later).

Outputs:
  01_daily/map_heat/<date>_map_heat.md
  01_daily/map_heat/<date>_map_heat.json

CLI:
  python -m src.map_heat [--date YYYY-MM-DD] [--force]
"""
from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from bs4 import BeautifulSoup

from . import config, preopen

ROOT = Path(__file__).resolve().parent.parent
EXPORT_DIR = ROOT / "data" / "exports"
OUT_DIR = ROOT / "01_daily" / "map_heat"
JOIN_PATH = ROOT / "00_grounding" / "map_theme_join.json"
ET = ZoneInfo(config.TZ)

UA = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}

RUT_MIN_DOLLAR_VOL = 5_000_000  # $5m 20-day $ volume floor
HOT_N = 8
COLD_N = 8
RESIDUAL_PP = 3.0  # industry vs parent, percentage points

POS_RE = re.compile(
    r"(?i)\b(beat|beats|upgrade|upgraded|record high|approv|surge|rally|"
    r"raises? guidance|buyback|contract win|fda|initiated (at )?buy|"
    r"outperform|overweight)\b"
)
NEG_RE = re.compile(
    r"(?i)\b(miss|misses|downgrade|downgraded|plunge|lawsuit|recall|"
    r"cuts? guidance|investigation|bankruptcy|warning|underperform|"
    r"sell rating|profit warning|layoff)\b"
)

FUTURES_KEEP = [
    ("ES", "S&P 500"), ("NQ", "Nasdaq 100"), ("ER2", "Russell 2000"),
    ("YM", "DJIA"), ("VX", "VIX futures"), ("CL", "Crude WTI"), ("BZ", "Brent"),
    ("NG", "Nat gas"), ("GC", "Gold"), ("SI", "Silver"), ("HG", "Copper"),
    ("DX", "USD"), ("6E", "EUR"), ("6J", "JPY"), ("ZN", "10Y note"),
    ("ZB", "30Y bond"), ("NKD", "Nikkei"), ("DY", "DAX"), ("EX", "Euro Stoxx"),
    ("BT", "Bitcoin"),
]


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(UA)
    token = os.environ.get("FINVIZ_AUTH") or os.environ.get("AUTH_TOKEN_FINVIZ") or ""
    if token:
        s.cookies.set("auth", token, domain=".finviz.com")
    return s


def _pct(val: Any) -> float | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip().replace(",", "").replace("%", "")
    if s in ("", "-", "nan", "None"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _fmt(p: float | None, digits: int = 1) -> str:
    if p is None:
        return "—"
    sign = "+" if p > 0 else ""
    return f"{sign}{p:.{digits}f}%"


def _latest_export(asof: str | None) -> Path | None:
    files = sorted(EXPORT_DIR.glob("finviz_????-??-??.csv"))
    if not files:
        fallback = ROOT / "data" / "finviz" / "latest.csv"
        return fallback if fallback.exists() else None
    if asof:
        exact = EXPORT_DIR / f"finviz_{asof}.csv"
        if exact.exists():
            return exact
        prior = [f for f in files if f.stem.replace("finviz_", "") <= asof]
        return prior[-1] if prior else files[-1]
    return files[-1]


def _in_index(val: Any, token: str) -> bool:
    parts = [p.strip() for p in str(val or "").replace(";", ",").split(",")]
    return any(p == token or token in p for p in parts)


def load_export(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, low_memory=False)
    df["Ticker"] = df["Ticker"].astype(str).str.strip().str.upper()
    df["Industry"] = df.get("Industry", "").astype(str).str.strip()
    df["Sector"] = df.get("Sector", "").astype(str).str.strip()
    df["Index"] = df.get("Index", "").astype(str)
    df["News Title"] = df.get("News Title", "").fillna("").astype(str)
    digest = df.get("Daily Digest", "")
    df["Daily Digest"] = digest.fillna("").astype(str) if not isinstance(digest, str) else digest
    df["mcap"] = pd.to_numeric(df.get("Market Cap"), errors="coerce")
    df["price"] = pd.to_numeric(df.get("Price"), errors="coerce")
    df["avg_vol_k"] = pd.to_numeric(df.get("Average Volume"), errors="coerce")
    df["chg"] = df["Change"].map(_pct)
    df["w1"] = df["Performance (Week)"].map(_pct)
    df["m1"] = df.get("Performance (Month)", pd.Series(dtype=str)).map(_pct)
    df["dollar_vol"] = df["price"] * df["avg_vol_k"] * 1000.0
    df["asset_type"] = df.get("Asset Type", "").fillna("").astype(str)
    df["theme"] = df.get("Sector/Theme", "").fillna("").astype(str).str.strip()
    df["etf_category"] = df.get("Single Category", "").fillna("").astype(str)
    df["aum"] = pd.to_numeric(df.get("Assets Under Management"), errors="coerce")
    df["spx"] = df["Index"].map(lambda v: _in_index(v, "S&P 500"))
    df["rut"] = df["Index"].map(lambda v: _in_index(v, "RUT"))
    df = df[df["Ticker"].str.len().gt(0)]
    df = df[~df["Industry"].isin(["", "nan", "-"])]
    return df.drop_duplicates("Ticker", keep="first")


def fetch_groups(sess: requests.Session, g: str) -> dict[str, dict]:
    """Live Finviz groups v=140 → {name: {w1, m1, d1, rvol}}."""
    url = f"https://finviz.com/groups?g={g}&v=140&o=name"
    try:
        r = sess.get(url, timeout=30)
        r.raise_for_status()
    except Exception as e:  # noqa: BLE001
        print(f"[map_heat] groups {g} failed: {e}")
        return {}
    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.select_one("table.groups_table") or soup.select_one(
        "table.styled-table-new"
    )
    if table is None:
        return {}
    rows = table.find_all("tr")
    if not rows:
        return {}
    headers = [c.get_text(" ", strip=True) for c in rows[0].find_all(["th", "td"])]
    idx = {h: i for i, h in enumerate(headers)}
    out: dict[str, dict] = {}
    for tr in rows[1:]:
        cells = [c.get_text(" ", strip=True) for c in tr.find_all("td")]
        if len(cells) < 4:
            continue
        name = cells[idx.get("Name", 1)]
        if not name or name == "Name":
            continue
        out[name] = {
            "w1": _pct(cells[idx["Perf Week"]]) if "Perf Week" in idx else None,
            "m1": _pct(cells[idx["Perf Month"]]) if "Perf Month" in idx else None,
            "d1": _pct(cells[idx["Change %"]]) if "Change %" in idx else None,
            "rvol": _pct(cells[idx["Rel Volume"]]) if "Rel Volume" in idx else None,
        }
    print(f"[map_heat] groups {g}: {len(out)} rows")
    return out


def fetch_futures(sess: requests.Session) -> dict[str, dict]:
    try:
        r = sess.get("https://finviz.com/futures", timeout=30)
        r.raise_for_status()
    except Exception as e:  # noqa: BLE001
        print(f"[map_heat] futures failed: {e}")
        return {}
    m = re.search(r"var tiles = (\{.*?\});\s*\n", r.text, re.S)
    if not m:
        m = re.search(r"var tiles = (\{.*\})", r.text, re.S)
    if not m:
        print("[map_heat] futures: no tiles blob")
        return {}
    blob = m.group(1)
    # JS object is JSON-enough (quoted keys).
    try:
        tiles = json.loads(blob)
    except json.JSONDecodeError:
        blob2 = re.sub(r",\s*}", "}", blob)
        try:
            tiles = json.loads(blob2)
        except json.JSONDecodeError as e:
            print(f"[map_heat] futures json: {e}")
            return {}
    out = {}
    for key, row in tiles.items():
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("ticker") or key)
        out[ticker] = {
            "label": row.get("label") or ticker,
            "last": row.get("last"),
            "change": row.get("change"),
            "prev_close": row.get("prevClose"),
        }
    print(f"[map_heat] futures tiles: {len(out)}")
    return out


def fetch_econ(sess: requests.Session, date: str) -> list[dict]:
    try:
        r = sess.get(
            f"https://finviz.com/calendar/economic?dateFrom={date}", timeout=30
        )
        r.raise_for_status()
    except Exception as e:  # noqa: BLE001
        print(f"[map_heat] econ calendar failed: {e}")
        return []
    rows = []
    for m in re.finditer(
        r'\{"calendarId":(\d+),.*?"event":"(.*?)".*?"date":"(.*?)".*?'
        r'"actual":(.*?),.*?"previous":(.*?),.*?"forecast":(.*?),.*?'
        r'"importance":(\d+)',
        r.text,
    ):
        day = m.group(3)[:10]
        if day != date:
            continue
        actual = m.group(4).strip('"')
        prev = m.group(5).strip('"')
        fc = m.group(6).strip('"')
        if actual == "null":
            actual = None
        if prev == "null":
            prev = None
        if fc == "null":
            fc = None
        rows.append({
            "event": m.group(2).encode("utf-8").decode("unicode_escape"),
            "datetime": m.group(3),
            "actual": actual,
            "previous": prev,
            "forecast": fc,
            "surprise": _numeric_surprise(actual, fc),
            "importance": int(m.group(7)),
        })
    # de-dupe
    seen = set()
    uniq = []
    for row in rows:
        k = (row["event"], row["datetime"])
        if k in seen:
            continue
        seen.add(k)
        uniq.append(row)
    print(f"[map_heat] econ {date}: {len(uniq)}")
    return uniq


def fetch_earnings(sess: requests.Session, date: str) -> list[dict]:
    try:
        r = sess.get(
            f"https://finviz.com/calendar/earnings?dateFrom={date}", timeout=30
        )
        r.raise_for_status()
    except Exception as e:  # noqa: BLE001
        print(f"[map_heat] earnings calendar failed: {e}")
        return []
    rows = []
    for m in re.finditer(
        r'\{"earningsDate":"(.*?)".*?"ticker":"(.*?)".*?"company":"(.*?)".*?'
        r'"marketCap":([0-9.]+).*?"epsEstimate":(.*?),',
        r.text,
    ):
        day = m.group(1)[:10]
        if day != date:
            continue
        est = m.group(5)
        if est == "null":
            est = None
        else:
            try:
                est = float(est)
            except ValueError:
                pass
        when = m.group(1)
        session = "AMC" if "T16" in when or "T20" in when or "T21" in when else "BMO"
        if "T00" in when or "T04" in when:
            session = "BMO"
        rows.append({
            "ticker": m.group(2),
            "company": m.group(3),
            "datetime": when,
            "session": session,
            "mcap": float(m.group(4)),
            "eps_est": est,
        })
    rows.sort(key=lambda x: -x["mcap"])
    print(f"[map_heat] earnings {date}: {len(rows)}")
    return rows


def _numeric_surprise(actual: Any, forecast: Any) -> float | None:
    """Best-effort actual minus consensus. Units stay as displayed by Finviz."""
    a, f = _pct(actual), _pct(forecast)
    if a is None or f is None:
        return None
    return round(a - f, 4)


def fetch_stock_news(sess: requests.Session, limit: int = 250) -> list[dict]:
    """Finviz Stocks News (v=3): ticker-tagged stories, not the wire dump."""
    try:
        r = sess.get("https://finviz.com/news.ashx?v=3", timeout=30)
        r.raise_for_status()
    except Exception as e:  # noqa: BLE001
        print(f"[map_heat] stocks news failed: {e}")
        return []
    soup = BeautifulSoup(r.text, "html.parser")
    out: list[dict] = []
    for tr in soup.select("tr.news_table-row"):
        story = tr.select_one("a.nn-tab-link")
        if story is None:
            continue
        tickers = []
        for a in tr.select("a.stock-news-label[data-boxover-ticker]"):
            ticker = str(a.get("data-boxover-ticker") or "").strip().upper()
            if ticker and ticker not in tickers:
                tickers.append(ticker)
        if not tickers:
            continue
        tm = tr.select_one("td.news_date-cell")
        source_nodes = tr.select("span.news_date-cell")
        out.append({
            "time": tm.get_text(" ", strip=True) if tm else "",
            "title": story.get_text(" ", strip=True),
            "url": story.get("href") or "",
            "source": source_nodes[-1].get_text(" ", strip=True)
            if source_nodes else "",
            "tickers": tickers,
        })
        if len(out) >= limit:
            break
    print(f"[map_heat] ticker news: {len(out)}")
    return out


def fetch_major_news_tickers(sess: requests.Session) -> list[str]:
    """Finviz Elite `n_majornews` screener. Empty is an honest login failure."""
    urls = [
        "https://elite.finviz.com/screener.ashx?v=150&s=n_majornews",
        "https://finviz.com/screener.ashx?v=150&s=n_majornews",
    ]
    for url in urls:
        try:
            r = sess.get(url, timeout=30)
            r.raise_for_status()
        except Exception:
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        found = []
        for a in soup.select("[data-boxover-ticker], a[href*='quote.ashx?t=']"):
            ticker = str(a.get("data-boxover-ticker") or "").strip().upper()
            if not ticker:
                m = re.search(r"[?&]t=([A-Za-z0-9.-]+)", a.get("href") or "")
                ticker = m.group(1).upper() if m else ""
            if ticker and ticker not in found:
                found.append(ticker)
        if found:
            print(f"[map_heat] major-news tickers: {len(found)} via {url}")
            return found
    print("[map_heat] major-news screener unavailable/empty")
    return []


def fetch_event_options(earnings: list[dict], limit: int = 8) -> list[dict]:
    """Targeted event-vol flags for today's largest earnings names.

    Uses the same chain fields Finviz exposes (IV, volume, OI), with yfinance
    as a resilient machine-readable source. This is volatility, never direction.
    """
    try:
        import yfinance as yf
    except Exception:
        return []
    now = datetime.now(ET)
    out = []
    for event in earnings[:limit]:
        ticker = str(event.get("ticker") or "").upper()
        try:
            inst = yf.Ticker(ticker)
            expiries = list(inst.options or [])
            if not expiries:
                continue
            expiry = next(
                (x for x in expiries if x >= now.date().isoformat()),
                expiries[0],
            )
            chain = inst.option_chain(expiry)
            hist = inst.history(period="5d", auto_adjust=True)
            if hist.empty:
                continue
            spot = float(hist["Close"].dropna().iloc[-1])
            calls, puts = chain.calls.copy(), chain.puts.copy()
            if calls.empty or puts.empty:
                continue
            calls["dist"] = (calls["strike"] - spot).abs()
            puts["dist"] = (puts["strike"] - spot).abs()
            call = calls.sort_values("dist").iloc[0]
            put = puts.sort_values("dist").iloc[0]
            ivs = [
                float(x) for x in (call.get("impliedVolatility"),
                                   put.get("impliedVolatility"))
                if x == x and float(x) > 0
            ]
            iv = sum(ivs) / len(ivs) if ivs else None
            days = max(1, (datetime.fromisoformat(expiry).date() - now.date()).days)
            implied_move = (spot * iv * (days / 365) ** 0.5) if iv else None
            call_oi = float(call.get("openInterest") or 0)
            put_oi = float(put.get("openInterest") or 0)
            out.append({
                "ticker": ticker,
                "expiry": expiry,
                "spot": round(spot, 3),
                "atm_iv": round(iv, 4) if iv else None,
                "implied_move_pct": round(100 * implied_move / spot, 2)
                if implied_move else None,
                "put_call_oi": round(put_oi / call_oi, 3) if call_oi else None,
                "meaning": "event volatility only; not direction",
            })
        except Exception as e:  # noqa: BLE001
            print(f"[map_heat] options {ticker} skipped: {str(e)[:120]}")
    return out


def _sentiment(title: str, digest: str) -> tuple[str, str]:
    text = f"{title} {digest}".strip()
    if not text or text in ("-", "nan"):
        return "none", ""
    pos = bool(POS_RE.search(text))
    neg = bool(NEG_RE.search(text))
    if pos and neg:
        label = "mixed"
    elif pos:
        label = "pos"
    elif neg:
        label = "neg"
    else:
        label = "none"
    why = (title or digest).replace("\n", " ").strip()
    if len(why) > 90:
        why = why[:87] + "…"
    return label, why


def _captains(sub: pd.DataFrame, kind: str) -> list[dict]:
    if kind == "SPX":
        pool = sub[sub["spx"]].copy()
    else:
        pool = sub[
            sub["rut"] & (sub["dollar_vol"].fillna(0) >= RUT_MIN_DOLLAR_VOL)
        ].copy()
    pool = pool.sort_values("mcap", ascending=False, na_position="last")
    out: list[dict] = []
    for rank, (_, row) in enumerate(pool.head(2).iterrows(), start=1):
        sent, why = _sentiment(row.get("News Title", ""), row.get("Daily Digest", ""))
        mcap = row["mcap"]
        out.append({
            "ticker": row["Ticker"],
            "rank": rank,
            "mcap": None if pd.isna(mcap) else round(float(mcap), 1),
            "d1": None if pd.isna(row["chg"]) else row["chg"],
            "w1": None if pd.isna(row["w1"]) else row["w1"],
            "sent": sent,
            "why": why,
        })
    return out


def _agg_from_members(sub: pd.DataFrame) -> dict:
    d1 = sub["chg"].dropna()
    w1 = sub["w1"].dropna()
    n = len(sub)
    n_up = int((sub["chg"] > 0).sum()) if n else 0
    return {
        "n": n,
        "d1": None if d1.empty else round(float(d1.median()), 2),
        "w1": None if w1.empty else round(float(w1.median()), 2),
        "breadth": None if n == 0 else round(n_up / n, 3),
    }


def _tape_from_futures(futures: dict) -> list[dict]:
    tape = []
    for ticker, label in FUTURES_KEEP:
        row = futures.get(ticker)
        if not row and ticker == "VX":
            row = futures.get("VI") or futures.get("VIX")
        if not row:
            continue
        tape.append({
            "ticker": ticker,
            "label": row.get("label") or label,
            "last": row.get("last"),
            "change": row.get("change"),
        })
    return tape


def _calendar_fields(econ: list[dict], earns: list[dict]) -> dict:
    """Split macro vs mega-earnings. Only MACRO halves the whole book."""
    mega_earn = [e for e in earns if (e.get("mcap") or 0) >= 50_000][:12]
    high_econ = [e for e in econ if e.get("importance", 0) >= 2][:12]
    macro_gate = bool(high_econ)
    tickers = [
        str(e.get("ticker") or "").upper()
        for e in mega_earn if e.get("ticker")
    ]
    return {
        "econ": high_econ,
        "earnings": mega_earn,
        "macro_gate": macro_gate,
        "earnings_gate": bool(mega_earn),
        "size_gate": macro_gate,
        "calendar_entry_scale": 0.5 if macro_gate else 1.0,
        "earnings_entry_tickers": tickers,
    }


def overlay_live(date: str, payload: dict) -> dict:
    """Morning overlay: futures + today's calendar + ticker news.

    Does NOT re-scrape industry groups / captains / residuals — premarket
    prints distort yesterday's close. Those tables are written at 22:00 ET.
    """
    sess = _session()
    futures = fetch_futures(sess)
    econ = fetch_econ(sess, date)
    earns = fetch_earnings(sess, date)
    ticker_news = fetch_stock_news(sess)
    major_news_tickers = fetch_major_news_tickers(sess)
    out = dict(payload)
    out.update(_calendar_fields(econ, earns))
    out["tape"] = _tape_from_futures(futures)
    out["event_options"] = fetch_event_options(out.get("earnings") or [])
    out["ticker_news"] = ticker_news
    out["major_news_tickers"] = major_news_tickers
    out["overlay_at"] = datetime.now(ET).isoformat()
    out["phase"] = "morning_overlay"
    return out


def build(date: str) -> dict:
    export_path = _latest_export(date)
    if export_path is None:
        raise SystemExit("no Finviz export — cannot build map_heat")
    df = load_export(export_path)
    sess = _session()
    live_ind = fetch_groups(sess, "industry")
    live_sec = fetch_groups(sess, "sector")
    futures = fetch_futures(sess)
    econ = fetch_econ(sess, date)
    earns = fetch_earnings(sess, date)
    ticker_news = fetch_stock_news(sess)
    major_news_tickers = fetch_major_news_tickers(sess)
    join = json.loads(JOIN_PATH.read_text()) if JOIN_PATH.exists() else {"themes": []}

    sectors: list[dict] = []
    for sec, sub in df.groupby("Sector"):
        if sec in ("", "nan", "-"):
            continue
        agg = _agg_from_members(sub)
        live = live_sec.get(sec, {})
        if live.get("d1") is not None:
            agg["d1"] = live["d1"]
        if live.get("w1") is not None:
            agg["w1"] = live["w1"]
        agg["rvol"] = live.get("rvol")
        agg["source"] = "groups" if sec in live_sec else "export"
        sectors.append({"sector": sec, **agg})
    sectors.sort(key=lambda r: r["sector"])
    sec_w1 = {r["sector"]: r["w1"] for r in sectors}
    sec_d1 = {r["sector"]: r["d1"] for r in sectors}

    industries: list[dict] = []
    for (sec, ind), sub in df.groupby(["Sector", "Industry"]):
        if ind in ("", "nan", "-") or sec in ("", "nan", "-"):
            continue
        agg = _agg_from_members(sub)
        live = live_ind.get(ind, {})
        if live.get("d1") is not None:
            agg["d1"] = live["d1"]
        if live.get("w1") is not None:
            agg["w1"] = live["w1"]
        agg["rvol"] = live.get("rvol")
        parent_w = sec_w1.get(sec)
        parent_d = sec_d1.get(sec)
        res_w = None
        res_d = None
        if agg["w1"] is not None and parent_w is not None:
            res_w = round(agg["w1"] - parent_w, 2)
        if agg["d1"] is not None and parent_d is not None:
            res_d = round(agg["d1"] - parent_d, 2)
        spx_c = _captains(sub, "SPX")
        rut_c = _captains(sub, "RUT")
        industries.append({
            "sector": sec,
            "industry": ind,
            **agg,
            "vs_parent_w1": res_w,
            "vs_parent_d1": res_d,
            "spx_leaders": spx_c,
            "rut_leaders": rut_c,
            "spx_sent": _leader_sent(spx_c),
            "rut_sent": _leader_sent(rut_c),
        })

    def _score(row: dict) -> float:
        return abs(row.get("vs_parent_w1") or 0) + 0.5 * abs(row.get("vs_parent_d1") or 0)

    ranked = sorted(
        [r for r in industries if r.get("w1") is not None],
        key=lambda r: r["w1"],
        reverse=True,
    )
    hot = ranked[:HOT_N]
    cold = list(reversed(ranked[-COLD_N:])) if ranked else []
    overrides = []
    for row in industries:
        rw = row.get("vs_parent_w1")
        if rw is None:
            continue
        if abs(rw) < RESIDUAL_PP:
            continue
        if abs(row.get("w1") or 0) < 2:
            continue
        parent_dir = "up" if (sec_w1.get(row["sector"]) or 0) > 0 else "down"
        child_dir = "up" if (row["w1"] or 0) > 0 else "down"
        action = "OVERRIDE" if parent_dir != child_dir else "SPLIT"
        overrides.append({
            "industry": row["industry"],
            "sector": row["sector"],
            "w1": row["w1"],
            "parent_w1": sec_w1.get(row["sector"]),
            "vs_parent_w1": rw,
            "action": action,
            "spx_leaders": [x["ticker"] for x in row["spx_leaders"]],
            "rut_leaders": [x["ticker"] for x in row["rut_leaders"]],
        })
    overrides.sort(key=lambda r: abs(r["vs_parent_w1"]), reverse=True)

    themes = []
    by_ind = {r["industry"]: r for r in industries}
    for theme in join.get("themes") or []:
        subs = []
        for st in theme.get("subthemes") or []:
            members = [by_ind[i] for i in st.get("industries") or [] if i in by_ind]
            if not members:
                continue
            w = [m["w1"] for m in members if m["w1"] is not None]
            d = [m["d1"] for m in members if m["d1"] is not None]
            parents = theme.get("gics_parents") or []
            pw = [sec_w1.get(p) for p in parents if sec_w1.get(p) is not None]
            w1 = round(sum(w) / len(w), 2) if w else None
            parent_w = round(sum(pw) / len(pw), 2) if pw else None
            res = round(w1 - parent_w, 2) if w1 is not None and parent_w is not None else None
            agree = None
            if w1 is not None and parent_w is not None:
                agree = (w1 >= 0 and parent_w >= 0) or (w1 < 0 and parent_w < 0)
            subs.append({
                "id": st["id"],
                "label": st.get("label") or st["id"],
                "industries": [m["industry"] for m in members],
                "w1": w1,
                "d1": round(sum(d) / len(d), 2) if d else None,
                "parent_w1": parent_w,
                "vs_parent_w1": res,
                "agree": agree,
            })
        if subs:
            themes.append({"theme": theme["theme"], "gics_parents": theme.get("gics_parents"),
                           "subthemes": subs})

    # Actual Finviz ETF theme taxonomy from the Elite export. This is
    # orthogonal to the hand-maintained GICS crosswalk above and expands
    # coverage beyond its small seed list without scraping the canvas map.
    theme_tape = []
    etfs = df[df["theme"].notna() & df["theme"].ne("")].copy()
    for theme, sub in etfs.groupby("theme"):
        d1 = sub["chg"].dropna()
        w1 = sub["w1"].dropna()
        leaders = sub.sort_values("aum", ascending=False, na_position="last").head(3)
        theme_tape.append({
            "theme": theme,
            "n_etfs": int(len(sub)),
            "d1": None if d1.empty else round(float(d1.median()), 2),
            "w1": None if w1.empty else round(float(w1.median()), 2),
            "leaders": [
                {
                    "ticker": str(r["Ticker"]),
                    "aum": None if pd.isna(r["aum"]) else round(float(r["aum"]), 1),
                    "d1": None if pd.isna(r["chg"]) else float(r["chg"]),
                    "w1": None if pd.isna(r["w1"]) else float(r["w1"]),
                    "tags": str(r.get("Tags") or "")[:240],
                }
                for _, r in leaders.iterrows()
            ],
        })
    theme_tape.sort(key=lambda r: abs(r.get("w1") or 0), reverse=True)

    tape = _tape_from_futures(futures)
    gates = _calendar_fields(econ, earns)
    event_options = fetch_event_options(gates["earnings"])

    payload = {
        "date": date,
        "generated_at": datetime.now(ET).isoformat(),
        "phase": "postclose",
        "export": export_path.name,
        "n_tickers": int(len(df)),
        "sectors": sectors,
        "industries": industries,
        "hot": [{"industry": r["industry"], "sector": r["sector"],
                 "w1": r["w1"], "d1": r["d1"], "vs_parent_w1": r["vs_parent_w1"],
                 "spx_leaders": r["spx_leaders"], "rut_leaders": r["rut_leaders"]}
                for r in hot],
        "cold": [{"industry": r["industry"], "sector": r["sector"],
                  "w1": r["w1"], "d1": r["d1"], "vs_parent_w1": r["vs_parent_w1"],
                 "spx_leaders": r["spx_leaders"], "rut_leaders": r["rut_leaders"]}
                 for r in cold],
        "overrides": overrides[:15],
        "themes": themes,
        "theme_tape": theme_tape,
        "tape": tape,
        "ticker_news": ticker_news,
        "major_news_tickers": major_news_tickers,
        "event_options": event_options,
        **gates,
    }
    return payload


def _leader_sent(captains: list[dict]) -> str:
    labels = [c["sent"] for c in captains if c["sent"] != "none"]
    if not labels:
        return "none"
    if all(x == "pos" for x in labels):
        return "pos"
    if all(x == "neg" for x in labels):
        return "neg"
    return "mixed"


def _leaders_line(row: dict, key: str) -> str:
    caps = row.get(key) or []
    if not caps:
        return "—"
    bits = []
    for c in caps:
        bits.append(
            f"{c['ticker']} {_fmt(c.get('d1'))} {c.get('sent')}"
        )
    return ", ".join(bits)


def render(p: dict) -> str:
    lines = [
        f"# MAP HEAT — {p['date']}",
        "",
        f"Export `{p['export']}` · {p['n_tickers']} names · "
        f"generated {p['generated_at']}",
        "",
        "## TAPE (live futures)",
    ]
    if not p.get("tape"):
        lines.append("_futures scrape empty_")
    else:
        lines.append("| Contract | Last | Change |")
        lines.append("|---|---:|---:|")
        for t in p["tape"]:
            ch = t.get("change")
            chs = "—" if ch is None else f"{ch:+.2f}%"
            last = "—" if t.get("last") is None else t["last"]
            lines.append(f"| {t['label']} ({t['ticker']}) | {last} | {chs} |")

    lines += ["", "## CALENDAR"]
    if p.get("size_gate"):
        lines.append("**SIZE GATE on** — high-impact print and/or mega-cap earnings today.")
    else:
        lines.append("No high-impact print / mega-cap earnings flagged.")
    if p.get("econ"):
        lines.append("")
        lines.append("Econ (importance ≥ 2):")
        for e in p["econ"]:
            lines.append(
                f"- {e['datetime'][11:16]} ET  {e['event']}  "
                f"actual {e.get('actual') or '—'}  cons {e.get('forecast') or '—'}  "
                f"surprise {e.get('surprise') if e.get('surprise') is not None else '—'}  "
                f"prev {e.get('previous') or '—'}"
            )
    if p.get("earnings"):
        lines.append("")
        lines.append("Mega-cap earnings:")
        for e in p["earnings"]:
            est = e.get("eps_est")
            ests = "—" if est is None else str(est)
            lines.append(
                f"- {e['session']} **{e['ticker']}**  EPS est {ests}  "
                f"({e['company']})"
            )
    if p.get("event_options"):
        lines += ["", "Options event-vol flags (NOT direction):"]
        for o in p["event_options"]:
            lines.append(
                f"- **{o['ticker']}** exp {o['expiry']} ATM IV "
                f"{o.get('atm_iv') or '—'} implied move "
                f"{_fmt(o.get('implied_move_pct'))} put/call OI "
                f"{o.get('put_call_oi') if o.get('put_call_oi') is not None else '—'}"
            )

    lines += ["", "## SECTOR RS (live groups, else export median)"]
    lines.append("| Sector | 1d | 1w | rvol |")
    lines.append("|---|---:|---:|---:|")
    for s in p["sectors"]:
        lines.append(
            f"| {s['sector']} | {_fmt(s.get('d1'))} | {_fmt(s.get('w1'))} | "
            f"{s.get('rvol') if s.get('rvol') is not None else '—'} |"
        )

    lines += ["", "## INDUSTRY_HEAT"]
    lines.append("HOT (1w):")
    for r in p["hot"]:
        lines.append(
            f"- **{r['industry']}** ({r['sector']})  {_fmt(r.get('d1'))} 1d  "
            f"{_fmt(r.get('w1'))} 1w  vs parent {_fmt(r.get('vs_parent_w1'))}"
        )
        lines.append(
            f"  SPX: {_leaders_line(r, 'spx_leaders')} · "
            f"RUT: {_leaders_line(r, 'rut_leaders')}"
        )
    lines.append("")
    lines.append("COLD (1w):")
    for r in p["cold"]:
        lines.append(
            f"- **{r['industry']}** ({r['sector']})  {_fmt(r.get('d1'))} 1d  "
            f"{_fmt(r.get('w1'))} 1w  vs parent {_fmt(r.get('vs_parent_w1'))}"
        )
        lines.append(
            f"  SPX: {_leaders_line(r, 'spx_leaders')} · "
            f"RUT: {_leaders_line(r, 'rut_leaders')}"
        )

    lines += ["", "## OVERRIDES (industry 1w residual vs parent ≥ 3pp)"]
    if not p.get("overrides"):
        lines.append("_none_")
    else:
        for o in p["overrides"]:
            spx = ",".join(o.get("spx_leaders") or []) or "—"
            rut = ",".join(o.get("rut_leaders") or []) or "—"
            lines.append(
                f"- **{o['action']}** {o['industry']} {_fmt(o['w1'])} vs "
                f"{o['sector']} {_fmt(o['parent_w1'])}  "
                f"(gap {_fmt(o['vs_parent_w1'])})  SPX {spx} · RUT {rut}"
            )

    lines += ["", "## THEME JOIN"]
    for th in p.get("themes") or []:
        lines.append(f"**{th['theme']}** (GICS {', '.join(th.get('gics_parents') or [])})")
        for st in th.get("subthemes") or []:
            flag = "AGREE" if st.get("agree") else "DIVERGE"
            if st.get("agree") is None:
                flag = "?"
            lines.append(
                f"- {st['label']}: {_fmt(st.get('w1'))} 1w vs parent "
                f"{_fmt(st.get('parent_w1'))} → **{flag}**"
            )

    lines += ["", "## FINVIZ THEME ETF TAPE"]
    for th in (p.get("theme_tape") or [])[:20]:
        leaders = ", ".join(x.get("ticker") or "" for x in th.get("leaders") or [])
        lines.append(
            f"- **{th.get('theme')}** {_fmt(th.get('d1'))} 1d "
            f"{_fmt(th.get('w1'))} 1w · {leaders or '—'}"
        )

    lines += ["", "## TICKER-TAGGED NEWS (Finviz v=3)"]
    for n in (p.get("ticker_news") or [])[:25]:
        lines.append(
            f"- {n.get('time') or '—'} **{','.join(n.get('tickers') or [])}** "
            f"{n.get('title')} ({n.get('source') or 'source?'})"
        )

    lines += [
        "",
        "## NOTES",
        "- Captains = top 2 by market cap. RUT needs ≥ $5m 20-day dollar volume.",
        "- Sentiment is News Title / Daily Digest keywords (pos/neg/mixed/none), not a Grok essay.",
        "- Maps were not scraped. Groups v=140 + Elite export are the map.",
        "",
        "MAP_HEAT_OK",
    ]
    return "\n".join(lines) + "\n"


def write(date: str, payload: dict) -> tuple[Path, Path]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    md_path = OUT_DIR / f"{date}_map_heat.md"
    js_path = OUT_DIR / f"{date}_map_heat.json"
    md_path.write_text(render(payload), encoding="utf-8")
    # industries list is large; keep full json for the bot, compact md for humans
    js_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    latest = OUT_DIR / "latest_map_heat.md"
    latest.write_text(md_path.read_text(encoding="utf-8"), encoding="utf-8")
    print(f"[map_heat] wrote {md_path}")
    print(f"[map_heat] wrote {js_path}")
    return md_path, js_path


def already_good(date: str) -> bool:
    p = OUT_DIR / f"{date}_map_heat.md"
    js = OUT_DIR / f"{date}_map_heat.json"
    if not p.exists() or not js.exists():
        return False
    text = p.read_text(encoding="utf-8")
    try:
        payload = json.loads(js.read_text(encoding="utf-8"))
        generated_date = str(payload.get("generated_at") or "")[:10]
    except (OSError, json.JSONDecodeError):
        return False
    # A post-close job intentionally builds tomorrow's baseline from today's
    # close. It must NOT suppress the next morning's live futures/news refresh.
    return (generated_date == date and "MAP_HEAT_OK" in text
            and "INDUSTRY_HEAT" in text and len(text) > 400)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--force", action="store_true")
    ap.add_argument(
        "--overlay", action="store_true",
        help="Morning only: refresh futures/calendar/news. Do not scrape groups.",
    )
    args = ap.parse_args()
    date = args.date or datetime.now(ET).date().isoformat()
    preopen.refuse_if_late("map_heat", force=args.force)
    js = OUT_DIR / f"{date}_map_heat.json"
    if args.overlay:
        if not js.exists():
            raise SystemExit(
                f"post-close map heat missing: {js} — industry groups must be "
                "scraped at 22:00 ET, not in the premarket"
            )
        try:
            payload = json.loads(js.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as e:
            raise SystemExit(f"post-close map heat unreadable: {js}: {e}")
        if len(payload.get("industries") or []) < 50:
            raise SystemExit(
                f"post-close map heat too thin ({len(payload.get('industries') or [])} "
                f"industries) at {js}"
            )
        if (not args.force
                and str(payload.get("overlay_at") or "")[:10] == date):
            print(f"[map_heat] overlay already applied {date}")
            return
        payload = overlay_live(date, payload)
        write(date, payload)
        print(render(payload))
        return
    if already_good(date) and not args.force:
        print(f"[map_heat] skip-if-good {date}")
        return
    payload = build(date)
    write(date, payload)
    print(render(payload))


if __name__ == "__main__":
    main()
