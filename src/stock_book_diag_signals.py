"""Buy/sell decisions + .io publish path for Stock Book readiness.

The ranker writes `data/stock_book/{date}_stock_book.json`. Each name
carries the six family scores the inputs produced. This module:

  * turns those scores into the same red/yellow/green boxes as lookback
  * traces each box back to the file / workflow that printed it
  * overlays featured lookback setups (vol+AB, first crack, …)
  * checks that paper_trade injected the book into dashboard/index.html
    and that GitHub Pages is serving that file

No heal. No new ranker. Read the book that already exists.
"""
from __future__ import annotations

import csv
import json
import os
import re
import urllib.error
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

from . import config
from .ticker_lookback_setups import match_day

ROOT = Path(__file__).resolve().parent.parent
ET = config.TZ
PAGES_URL = "https://sroyaltyy.github.io/fullscan/dashboard/"
EPS = 0.05
RELVOL_SPIKE = 1.5
RELVOL_DEAD = 0.7
SLEEVE_N = 10
PRIMARY_H = "1d"
SECONDARY_H = "1m"

BOX_ICON = {
    "good": "🟢",
    "bad": "🔴",
    "neutral": "🟡",
    "missing": "⬛",
}
TONE_RANK = {"bad": 0, "neutral": 1, "good": 2}
BOX_KEYS = (
    "join", "sector", "gen", "news", "digest", "judge",
    "ab", "peer", "heat", "vol", "catal", "buy",
)
DOMAIN_KEYS = ("market", "parent", "child", "company", "setup", "flow")

# Each lookback box → the input that colored it.
FACTOR_TRACE = (
    {
        "key": "join", "label": "join",
        "file": "data/join/{date}_ranked.csv",
        "workflow": "Label + weather / Stock Book ALL",
        "score": "s_join",
        "means": "labels × regime (weather). Ranker family s_join.",
    },
    {
        "key": "sector", "label": "sect",
        "file": "01_daily/sectors/{date}/<sector>_predict.md",
        "workflow": "Pre-Open ALL",
        "score": "s_sector",
        "means": "same-day sector essay bias, gated by hit-rate.",
    },
    {
        "key": "gen", "label": "gen",
        "file": "01_daily/general/{date}_predict.md",
        "workflow": "Pre-Open ALL",
        "score": "s_general",
        "means": "same-day general-market essay bias.",
    },
    {
        "key": "news", "label": "news",
        "file": "01_daily/news/{date}_actions.json",
        "workflow": "Pre-Open ALL",
        "score": "s_news",
        "means": "ticker news-action net (plus digest overlay).",
    },
    {
        "key": "digest", "label": "dig",
        "file": "01_daily/news/{date}_finviz_digest.json",
        "workflow": "Finviz scrape",
        "score": None,
        "means": "Elite daily digest polarity for this ticker.",
    },
    {
        "key": "judge", "label": "jdg",
        "file": "01_daily/news/{date}_judge.md",
        "workflow": "Pre-Open ALL",
        "score": None,
        "means": "pre-open judge ticker tilt (else sector tilt).",
    },
    {
        "key": "ab", "label": "AB",
        "file": "data/ab_checklist/{date}_ab_checklist_enriched.csv",
        "workflow": "AB checklist",
        "score": "s_ab_intrinsic",
        "means": "intrinsic A+B1 checklist; P01-P04 group/peer context is "
        "shown separately to avoid double-counting.",
    },
    {
        "key": "peer", "label": "peer",
        "file": "data/peers/{date}_peer_rs.csv",
        "workflow": "Stock Book ALL",
        "score": "s_peer",
        "means": "peer relative strength vs industry cohort.",
    },
    {
        "key": "heat", "label": "heat",
        "file": "01_daily/map_heat/{date}_map_heat.json",
        "workflow": "Post-close research / Pre-Open ALL",
        "score": "s_heat",
        "means": "child industry/theme absolute + parent-relative tape; "
        "captain research enriches when healthy.",
    },
    {
        "key": "vol", "label": "vol",
        "file": "data/exports/finviz_{date}.csv",
        "workflow": "Finviz scrape / Label + weather",
        "score": None,
        "means": "Relative Volume. ≥1.5 green, <0.7 red.",
    },
    {
        "key": "catal", "label": "cat",
        "file": "01_daily/catalyst/{date}_dossiers.json",
        "workflow": "Catalyst dossiers",
        "score": None,
        "means": "usable dossier net_signal for this ticker.",
    },
    {
        "key": "buy", "label": "buy",
        "file": "data/stock_book/{date}_stock_book.json",
        "workflow": "Stock Book ALL (this file)",
        "score": None,
        "means": "on today's 1d BUY list (what paper_trade can fill).",
    },
)


def _p(*parts: str) -> Path:
    return ROOT.joinpath(*parts)


def _read_json(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return None


def _tick(s) -> str:
    return str(s or "").strip().upper()


def _num(x):
    if x is None:
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    if v != v:  # NaN
        return None
    return v


def polarity(x, eps: float = EPS) -> str:
    v = _num(x)
    if v is None:
        return "missing"
    if v >= eps:
        return "good"
    if v <= -eps:
        return "bad"
    return "neutral"


def _prev_weekday(date: str) -> str:
    d = datetime.fromisoformat(date).date() - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.isoformat()


def _tone_rank(tone):
    return TONE_RANK.get(str(tone or "").lower())


def objectively_better(prev, nxt) -> bool:
    improved = False
    for key in BOX_KEYS:
        a, b = _tone_rank((prev or {}).get(key)), _tone_rank((nxt or {}).get(key))
        if a is None or b is None:
            continue
        if b < a:
            return False
        if b > a:
            improved = True
    return improved


def purely_worse(prev, nxt) -> bool:
    worsened = False
    for key in BOX_KEYS:
        a, b = _tone_rank((prev or {}).get(key)), _tone_rank((nxt or {}).get(key))
        if a is None or b is None:
            continue
        if b > a:
            return False
        if b < a:
            worsened = True
    return worsened


def color_region(boxes) -> dict:
    g = sum(1 for k in BOX_KEYS if (boxes or {}).get(k) == "good")
    r = sum(1 for k in BOX_KEYS if (boxes or {}).get(k) == "bad")
    n = g + r
    if n < 3:
        return {"tone": "thin", "good": g, "bad": r}
    if g - r >= 2:
        return {"tone": "good", "good": g, "bad": r}
    if r - g >= 2:
        return {"tone": "bad", "good": g, "bad": r}
    return {"tone": "neutral", "good": g, "bad": r}


def zero_red(boxes) -> bool:
    printed = [str((boxes or {}).get(k) or "") for k in BOX_KEYS]
    tones = [t for t in printed if t in ("good", "bad", "neutral")]
    return bool(tones) and all(t != "bad" for t in tones)


# ---------------------------------------------------------------------------
# Sidecar lookups (one pass per date)
# ---------------------------------------------------------------------------

def _digest_tones(date: str) -> dict[str, str]:
    data = _read_json(_p("01_daily", "news", f"{date}_finviz_digest.json"))
    if not isinstance(data, dict):
        return {}
    try:
        from .stock_book import _digest_polarity
    except Exception:
        return {}
    out: dict[str, str] = {}
    rows = list(data.get("top_signal") or []) + list(
        data.get("all_ticker_digests_sample") or [])
    for sec_rows in (data.get("by_sector") or {}).values():
        rows.extend(sec_rows or [])
    for row in rows:
        if not isinstance(row, dict):
            continue
        t = _tick(row.get("ticker"))
        if not t or t in out or t in {"SPY", "QQQ", "DIA", "IWM"}:
            continue
        text = str(row.get("digest") or row.get("news_title") or "")
        if not text.strip():
            continue
        out[t] = polarity(_digest_polarity(text))
    return out


def _judge_lookup(date: str) -> tuple[dict[str, str], dict[str, str]]:
    data = _read_json(_p("01_daily", "news", f"{date}_judge.json")) or {}
    tickers = {}
    for t, v in (data.get("tickers") or {}).items():
        tickers[_tick(t)] = polarity(v)
    tilts = {}
    for sec, val in (data.get("sector_tilts") or {}).items():
        s = str(val or "").lower()
        if "bull" in s or s in {"up", "pos", "positive"}:
            tilts[str(sec)] = "good"
        elif "bear" in s or s in {"down", "neg", "negative"}:
            tilts[str(sec)] = "bad"
        else:
            tilts[str(sec)] = "neutral"
    return tickers, tilts


def _heat_tones(date: str) -> dict[str, str]:
    research = _read_json(_p("01_daily", "map_heat", f"{date}_research.json"))
    tb: dict = {}
    if isinstance(research, dict) and research.get("phase") == "morning_refresh":
        try:
            from .map_heat_research import ticker_boosts
            tb, _ib = ticker_boosts(date)
        except Exception:
            tb = {}
    if not tb:
        data = research if isinstance(research, dict) else None
        if not isinstance(data, dict) or data.get("phase") != "morning_refresh":
            data = _read_json(
                _p("01_daily", "map_heat", f"{date}_research_baseline.json"))
        tb = {}
        for card in (data or {}).get("cards") or []:
            direction = str(card.get("subsector_dir") or "").lower()
            sign = 1.0 if direction == "up" else -1.0 if direction == "down" else 0.0
            for cap in card.get("captains") or []:
                if not isinstance(cap, dict):
                    continue
                t = _tick(cap.get("ticker"))
                sent = str(cap.get("sent") or "")
                if not t:
                    continue
                if sent == "pos":
                    tb[t] = 0.2 * (sign or 1.0)
                elif sent == "neg":
                    tb[t] = -0.2 * (abs(sign) or 1.0)
    return {t: polarity(v) for t, v in tb.items()}


def _catal_tones(date: str) -> dict[str, str]:
    data = _read_json(_p("01_daily", "catalyst", f"{date}_dossiers.json"))
    if not isinstance(data, dict):
        return {}
    try:
        from .catalyst_daily import usable_dossier, SIGNAL_WEIGHT
    except Exception:
        SIGNAL_WEIGHT = {}
        usable_dossier = lambda r: bool(r.get("net_signal") and not r.get("error"))
    out = {}
    for row in data.get("dossiers") or []:
        if not isinstance(row, dict) or not usable_dossier(row):
            continue
        t = _tick(row.get("ticker"))
        w = SIGNAL_WEIGHT.get(str(row.get("net_signal") or "").lower())
        if w is None:
            sig = str(row.get("net_signal") or "").lower()
            w = 1.0 if "bull" in sig else -1.0 if "bear" in sig else 0.0
        out[t] = polarity(w)
    return out


def _finviz_relvol(date: str, wanted: set[str]) -> dict[str, float]:
    path = _p("data", "exports", f"finviz_{date}.csv")
    if not path.exists() or not wanted:
        return {}
    out: dict[str, float] = {}
    try:
        with path.open(encoding="utf-8", errors="ignore", newline="") as fh:
            reader = csv.DictReader(fh)
            vol_key = next(
                (c for c in (reader.fieldnames or [])
                 if c in ("Relative Volume", "Rel Volume", "Rel Vol",
                          "RelVol", "relvol")),
                None,
            )
            tick_key = next(
                (c for c in (reader.fieldnames or [])
                 if c in ("Ticker", "ticker", "Symbol")),
                None,
            )
            if not vol_key or not tick_key:
                return {}
            for row in reader:
                t = _tick(row.get(tick_key))
                if t not in wanted or t in out:
                    continue
                v = _num(str(row.get(vol_key) or "").replace(",", ""))
                if v is not None:
                    out[t] = v
                if len(out) >= len(wanted):
                    break
    except OSError:
        return {}
    return out


def _vol_tone(rel) -> str:
    if rel is None:
        return "missing"
    if rel >= RELVOL_SPIKE:
        return "good"
    if rel < RELVOL_DEAD:
        return "bad"
    return "neutral"


# ---------------------------------------------------------------------------
# Boxes + decisions
# ---------------------------------------------------------------------------

def boxes_for_row(row: dict, extras: dict, in_buy: bool) -> dict[str, str]:
    t = _tick(row.get("ticker"))
    sector = str(row.get("sector") or "")
    judge_t, judge_sec = extras.get("judge_t") or {}, extras.get("judge_sec") or {}
    computed = {
        "join": polarity(row.get("s_join")),
        "sector": polarity(row.get("s_sector")),
        "gen": polarity(row.get("s_general")),
        "news": polarity(row.get("s_news")),
        "digest": (extras.get("digest") or {}).get(t) or "missing",
        "judge": judge_t.get(t) or judge_sec.get(sector) or "missing",
        "ab": polarity(row.get("s_ab")),
        "peer": polarity(row.get("s_peer")),
        "heat": (extras.get("heat") or {}).get(t) or polarity(row.get("s_heat")),
        "vol": _vol_tone((extras.get("vol") or {}).get(t)),
        "catal": (extras.get("catal") or {}).get(t) or "missing",
        "buy": "good" if in_buy else "neutral",
    }
    # The ranker now persists each source verdict before lookback/selection.
    # Prefer it to re-parsing sidecars so the Action shows exactly what made
    # the decision.  `buy` remains a display-only circular cell.
    stored = row.get("source_boxes")
    if isinstance(stored, dict):
        for key in BOX_KEYS:
            tone = str(stored.get(key) or "")
            if key != "buy" and tone in BOX_ICON:
                computed[key] = tone
    return computed


def _annotate(boxes: dict, prev: dict | None) -> dict:
    region = color_region(boxes)
    blue = objectively_better(prev, boxes) if prev else False
    alarm = purely_worse(prev, boxes) if prev else False
    zr = zero_red(boxes)
    day = {
        "boxes": boxes,
        "region": region,
        "signal_improved": blue,
        "signal_alarm": alarm,
        "zero_red": zr,
    }
    tags = []
    if blue and region.get("tone") == "bad":
        tags.append("turn")
    elif blue and region.get("tone") == "good":
        tags.append("late")
    if alarm and region.get("tone") == "good":
        tags.append("first_crack")
    elif alarm and region.get("tone") == "bad":
        tags.append("continuation")
    if zr and region.get("tone") == "good":
        tags.append("crowded")
    day["tag_context"] = tags
    setups = []
    try:
        setups = [
            {"id": s.get("id"), "label": s.get("label")}
            for s in match_day(day)
        ]
    except Exception:
        setups = []
    day["setups"] = setups
    return day


def _sidecar(date: str, tickers: set[str]) -> dict:
    jt, js = _judge_lookup(date)
    return {
        "digest": _digest_tones(date),
        "judge_t": jt,
        "judge_sec": js,
        "heat": _heat_tones(date),
        "catal": _catal_tones(date),
        "vol": _finviz_relvol(date, tickers),
    }


def _load_book(date: str) -> dict:
    data = _read_json(_p("data", "stock_book", f"{date}_stock_book.json"))
    return data if isinstance(data, dict) else {}


def _horizon_rows(book: dict, horizon: str) -> tuple[list[dict], list[dict]]:
    entry = (book.get("books") or {}).get(horizon) or {}
    return list(entry.get("buy") or []), list(entry.get("sell") or [])


def _decision(row: dict, *, side: str, horizon: str, rank: int,
              extras: dict, prev_boxes: dict | None, in_buy: bool) -> dict:
    boxes = boxes_for_row(row, extras, in_buy=in_buy)
    marks = _annotate(boxes, prev_boxes)
    sleeve = rank <= SLEEVE_N
    return {
        "ticker": _tick(row.get("ticker")),
        "side": side,
        "horizon": horizon,
        "rank": rank,
        "score": _num(row.get("score")),
        "sector": row.get("sector"),
        "size": row.get("size"),
        "green": bool(row.get("green")),
        "reasons": str(row.get("reasons") or ""),
        "decision_lane": row.get("decision_lane") or "",
        "bull_eligible": bool(row.get("bull_eligible")),
        "bear_eligible": bool(row.get("bear_eligible")),
        "bull_decision": str(row.get("bull_decision") or ""),
        "bear_decision": str(row.get("bear_decision") or ""),
        "decision_blockers": str(row.get("decision_blockers") or ""),
        "domains": (
            row.get("domain_boxes")
            if isinstance(row.get("domain_boxes"), dict)
            else {}
        ),
        "domain_cond": row.get("domain_cond"),
        "domain_region": row.get("domain_region"),
        "domain_white": bool(row.get("domain_white")),
        "domain_name_white": bool(row.get("domain_name_white")),
        "company_summary": row.get("company_summary"),
        "company_strength": _num(row.get("company_strength")),
        "company_direct": bool(row.get("company_direct")),
        "company_price_confirmed": bool(
            row.get("company_price_confirmed")
        ),
        "group_label": row.get("group_label") or row.get("industry"),
        "child_d1": _num(row.get("child_d1")),
        "child_w1": _num(row.get("child_w1")),
        "child_residual": _num(row.get("child_residual")),
        "scores": {
            k: _num(row.get(k))
            for k in ("s_join", "s_general", "s_ab", "s_ab_intrinsic", "s_peer",
                      "s_sector", "s_news", "s_heat")
            if row.get(k) is not None
        },
        "boxes": boxes,
        "region": marks["region"],
        "blue": bool(marks["signal_improved"]),
        "alarm": bool(marks["signal_alarm"]),
        "zero_red": bool(marks["zero_red"]),
        "tags": marks["tag_context"],
        "setups": marks["setups"],
        "sleeve": sleeve,
        "where": (
            f"paper sleeve {horizon}_top (fills the .io dashboard)"
            if sleeve and side == "buy"
            else ("book only — ranked past the 10-name sleeve"
                  if side == "buy" else "SELL rank (paper does not short)")
        ),
    }


def extract_decisions(date: str) -> dict:
    book = _load_book(date)
    meta = book.get("meta") if isinstance(book.get("meta"), dict) else {}
    buys_1d, sells_1d = _horizon_rows(book, PRIMARY_H)
    tickers = {_tick(r.get("ticker")) for r in buys_1d + sells_1d}
    buys_1m, sells_1m = _horizon_rows(book, SECONDARY_H)
    tickers |= {_tick(r.get("ticker")) for r in buys_1m + sells_1m}
    extras = _sidecar(date, tickers)

    prev = _prev_weekday(date)
    prev_book = _load_book(prev)
    prev_buys, prev_sells = _horizon_rows(prev_book, PRIMARY_H)
    prev_extras = _sidecar(prev, {_tick(r.get("ticker"))
                                 for r in prev_buys + prev_sells} | tickers)
    prev_map = {}
    prev_buy_set = {_tick(r.get("ticker")) for r in prev_buys}
    for r in prev_buys + prev_sells:
        t = _tick(r.get("ticker"))
        prev_map[t] = boxes_for_row(r, prev_extras, in_buy=t in prev_buy_set)

    buy_set = {_tick(r.get("ticker")) for r in buys_1d}

    def pack(horizon, buys, sells):
        out_b = [
            _decision(r, side="buy", horizon=horizon, rank=i,
                      extras=extras, prev_boxes=prev_map.get(_tick(r.get("ticker"))),
                      in_buy=True)
            for i, r in enumerate(buys, 1)
        ]
        out_s = [
            _decision(r, side="sell", horizon=horizon, rank=i,
                      extras=extras, prev_boxes=prev_map.get(_tick(r.get("ticker"))),
                      in_buy=_tick(r.get("ticker")) in buy_set)
            for i, r in enumerate(sells, 1)
        ]
        return {"buy": out_b, "sell": out_s}

    horizons = {
        PRIMARY_H: pack(PRIMARY_H, buys_1d, sells_1d),
        SECONDARY_H: pack(SECONDARY_H, buys_1m[:SLEEVE_N], sells_1m[:SLEEVE_N]),
    }
    n_buy = len(buys_1d)
    n_sell = len(sells_1d)
    lattice = (
        meta.get("decision_lattice")
        if isinstance(meta.get("decision_lattice"), dict)
        else {}
    )
    return {
        "date": date,
        "prior_date": prev,
        "present": bool(book.get("books")),
        "ranker": meta.get("ranker") or ("green_pile" if meta.get("pile_used")
                                         else "weighted"),
        "n_pile": meta.get("n_pile"),
        "pile_used": meta.get("pile_used"),
        "n_1d_buy": n_buy,
        "n_1d_sell": n_sell,
        "market": lattice.get("market") or meta.get("market_decision") or {},
        "lattice": lattice,
        "bull_watch": list(lattice.get("bull_watch") or []),
        "bear_watch": list(lattice.get("bear_watch") or []),
        "intentional_stand_down": bool(
            (lattice.get("stand_down") or {}).get("stand_down")
        ),
        "horizons": horizons,
        "factor_trace": [
            {**row, "file": row["file"].format(date=date)}
            for row in FACTOR_TRACE
        ],
        "how": [
            "Stock Book ALL calls `python -m src.stock_book` after the "
            "upstream files land.",
            "1d uses gate → route → rank: market permission; parent/child "
            "group; direct company evidence; setup/flow.",
            "The weighted score ranks only inside an eligible standard, "
            "group-leader, or catalyst lane. It cannot offset a hard gate.",
            "The source color row remains; a deduplicated six-domain row "
            "(MKT,parent,child,company,setup,flow) owns permission.",
            "SELL/AVOID uses the bear lattice. Paper does not short.",
            f"paper_trade --top {SLEEVE_N} takes the first {SLEEVE_N} 1d "
            "BUY names into the 1d_top sleeve (fill = that day's close).",
            "paper_trade writes dashboard/index.html; stock_book_all.yml "
            f"force-pushes gh-pages → {PAGES_URL}",
        ],
    }


# ---------------------------------------------------------------------------
# Dashboard / Pages
# ---------------------------------------------------------------------------

_GEN_RE = re.compile(r'"generated":\s*"([^"]+)"')
_DATES_RE = re.compile(r'"dates":\s*\[(.*?)\]', re.S)


def dashboard_meta(text: str) -> dict:
    gen = ""
    m = _GEN_RE.search(text or "")
    if m:
        gen = m.group(1)
    dates = []
    m = _DATES_RE.search(text or "")
    if m:
        dates = re.findall(r"20\d{2}-\d{2}-\d{2}", m.group(1))
    return {"generated": gen, "dates": dates}


def inspect_dashboard_html(path: Path, date: str) -> tuple[str, str, int]:
    if not path.exists():
        return "MISSING", "missing", 0
    text = path.read_text(encoding="utf-8", errors="ignore")
    size = len(text)
    if size < 80 or "<html" not in text.lower():
        return "FAIL", f"not html or too_small({size})", size
    if "const D =" not in text and "__DATA__" in text:
        return "FAIL", "template not injected (__DATA__ still present)", size
    meta = dashboard_meta(text)
    if date not in (meta.get("dates") or []):
        last = (meta.get("dates") or ["?"])[-1]
        return "FAIL", f"session {date} not in dashboard dates (last={last})", size
    return "OK", f"generated={meta.get('generated') or '?'} last={meta.get('dates')[-1]}", size


def inspect_pages_live(date: str) -> tuple[str, str, int]:
    req = urllib.request.Request(PAGES_URL, headers={
        "User-Agent": "fullscan-stock-book-diag",
        "Cache-Control": "no-cache",
    })
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            code = resp.getcode()
            body = resp.read(400000).decode("utf-8", "ignore")
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        return "FAIL", f"unreachable: {e}"[:160], 0
    if code != 200:
        return "FAIL", f"HTTP {code}", 0
    if "const D =" not in body:
        return "FAIL", "live page has no injected book data", len(body)
    meta = dashboard_meta(body)
    if date not in (meta.get("dates") or []):
        last = (meta.get("dates") or ["?"])[-1]
        return "FAIL", (
            f"live .io is stale — session {date} not in dates "
            f"(last={last}, generated={meta.get('generated') or '?'})"
        ), len(body)
    return "OK", (
        f"live {PAGES_URL} generated={meta.get('generated') or '?'} "
        f"includes {date}"
    ), len(body)


# ---------------------------------------------------------------------------
# Render
# ---------------------------------------------------------------------------

def _box_cell(boxes: dict) -> str:
    return "".join(BOX_ICON.get((boxes or {}).get(k), "⬛") for k in BOX_KEYS)


def _domain_cell(domains: dict) -> str:
    return "".join(
        BOX_ICON.get((domains or {}).get(k), "⬛") for k in DOMAIN_KEYS
    )


def _market_line(dec: dict) -> str:
    market = dec.get("market") or {}
    if not market:
        return "MARKET unknown — no lattice record"
    return (
        f"MARKET {str(market.get('state') or '?').upper()} "
        f"score={_num(market.get('score')) or 0:+.2f} "
        f"good={_num(market.get('good_points')) or 0:+.1f} "
        f"bad={_num(market.get('bad_points')) or 0:+.1f} "
        f"risk={market.get('risk') or '?'} — "
        f"{market.get('rationale') or ''}"
    )


def _box_legend() -> str:
    bits = []
    for spec in FACTOR_TRACE:
        bits.append(spec["label"])
    return "order: " + " ".join(bits)


def _marks(d: dict) -> str:
    out = []
    if d.get("blue"):
        out.append("🔵")
    if d.get("alarm"):
        out.append("🚨")
    if d.get("zero_red"):
        out.append("⚪")
    return "".join(out)


def _setups(d: dict) -> str:
    labs = [s.get("label") or s.get("id") for s in (d.get("setups") or [])]
    return ", ".join(x for x in labs if x)


def _score_bits(d: dict) -> str:
    parts = []
    for spec in FACTOR_TRACE:
        key = spec.get("score")
        if not key:
            continue
        v = (d.get("scores") or {}).get(key)
        if v is None:
            continue
        tone = (d.get("boxes") or {}).get(
            {"s_join": "join", "s_general": "gen", "s_ab": "ab",
             "s_ab_intrinsic": "ab",
             "s_peer": "peer", "s_sector": "sector", "s_news": "news",
             "s_heat": "heat"}.get(key, ""),
            polarity(v),
        )
        parts.append(f"{BOX_ICON.get(tone, '⬛')} {spec['label']}={v:+.2f}")
    return " · ".join(parts)


def render_actions_plain(dec: dict) -> str:
    """The thing you see first: today's BUY / SELL list."""
    date = dec.get("date") or "?"
    if not dec.get("present"):
        return (
            f"======== {date} ACTIONS ========\n"
            "NO BOOK — ranker has not written any buy/sell names.\n"
        )
    h1 = (dec.get("horizons") or {}).get(PRIMARY_H) or {}
    lines = [
        f"======== {date} ACTIONS ========",
        f"ranker={dec.get('ranker')}  "
        f"1d {dec.get('n_1d_buy') or 0} BUY / {dec.get('n_1d_sell') or 0} SELL  "
        f"sleeve=first {SLEEVE_N} BUY → {PAGES_URL}",
        _market_line(dec),
    ]
    market = dec.get("market") or {}
    if market.get("bull_reasons"):
        lines.append(
            "MARKET BULL: " + "; ".join(market.get("bull_reasons") or [])
        )
    if market.get("bear_reasons"):
        lines.append(
            "MARKET BEAR: " + "; ".join(market.get("bear_reasons") or [])
        )
    lines += ["", f"--- ACTION BUY  (1d_top sleeve, fills .io) ---"]
    sleeve = [d for d in (h1.get("buy") or []) if d.get("sleeve")]
    rest = [d for d in (h1.get("buy") or []) if not d.get("sleeve")]
    if not sleeve:
        lines.append("(none)")
    for d in sleeve:
        lines.append(_action_line(d, "BUY"))
    lines += ["", "--- ACTION BUY  (book only, not in the 10-name sleeve) ---"]
    if not rest:
        lines.append("(none)")
    for d in rest:
        lines.append(_action_line(d, "BUY"))
    lines += ["", "--- BULL DECISIONS  (eligible + closest blocked) ---"]
    watches = dec.get("bull_watch") or []
    if not watches:
        lines.append("(none)")
    for i, row in enumerate(watches[:15], 1):
        lines.append(_watch_line(row, i, "BULL"))
    lines += ["", "--- ACTION SELL  (bear decisions — paper does not short) ---"]
    sells = h1.get("sell") or []
    if not sells:
        lines.append("(none)")
    for d in sells:
        lines.append(_action_line(d, "SELL"))
    lines.append("")
    return "\n".join(lines)


def _watch_line(row: dict, rank: int, side: str) -> str:
    domains = _domain_cell(row.get("domains") or {})
    decision = (
        row.get("bull_decision") if side == "BULL"
        else row.get("bear_decision")
    )
    evidence = ""
    if side == "BULL":
        company = str(row.get("company") or "")
        group = str(row.get("group") or "")
        if company or group:
            evidence = f" evidence={company or 'no direct event'} / {group}"
    return (
        f"{side} #{rank:>2} {_tick(row.get('ticker')):<6} "
        f"{domains} lane={row.get('lane') or 'blocked'} "
        f"{decision or ''}{evidence}"
    )


def _action_line(d: dict, verb: str) -> str:
    boxes = "".join(BOX_ICON.get((d.get("boxes") or {}).get(k), "⬛")
                    for k in BOX_KEYS)
    marks = ""
    if d.get("blue"):
        marks += "🔵"
    if d.get("alarm"):
        marks += "🚨"
    setups = ", ".join(
        s.get("label") or s.get("id") or ""
        for s in (d.get("setups") or [])
    )
    extra = f"  setups={setups}" if setups else ""
    decision = (
        d.get("bull_decision") if verb == "BUY"
        else d.get("bear_decision")
    )
    decision_text = f"  DECISION={decision}" if decision else ""
    return (
        f"ACTION {verb:<4} #{d.get('rank'):>2} {d.get('ticker'):<6} "
        f"{d.get('size') or '?':<5} {d.get('sector') or '?':<22} "
        f"{boxes} domains={_domain_cell(d.get('domains') or {})}  "
        f"{d.get('reasons') or ''}{marks}{extra}{decision_text}"
    )


def render_actions_markdown(dec: dict) -> list[str]:
    date = dec.get("date") or "?"
    if not dec.get("present"):
        return [
            f"## Today's actions — {date}",
            "",
            "**NO BOOK** — the ranker has not written any buy/sell names.",
            "",
        ]
    h1 = (dec.get("horizons") or {}).get(PRIMARY_H) or {}
    market = dec.get("market") or {}
    lines = [
        f"## Today's actions — {date}",
        "",
        f"### Market gate: "
        f"{BOX_ICON.get(market.get('tone'), '⬛')} "
        f"{str(market.get('state') or 'unknown').upper()}",
        "",
        f"- {_market_line(dec)}",
        f"- Allowed lanes: "
        f"`{', '.join(market.get('allowed_lanes') or []) or 'none'}` · "
        f"max long slots {market.get('max_long_slots', 0)} · "
        f"size ×{_num(market.get('position_scale')) or 0:.2f}",
        "",
        f"These are the names `src.stock_book` wrote. "
        f"The first {SLEEVE_N} BUY names are the paper sleeve that "
        f"[the .io dashboard]({PAGES_URL}) can fill.",
        "",
        "### ACTION BUY — sleeve (fills .io)",
        "",
        "| Action | # | Ticker | Source boxes | Domains | Lane | Score | Decision |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for d in (h1.get("buy") or []):
        if not d.get("sleeve"):
            continue
        lines.append(_action_md_row(d, "BUY"))
    if not [d for d in (h1.get("buy") or []) if d.get("sleeve")]:
        lines.append("| — | | | | | | | none — market/lattice stood down |")
    lines += ["", "### ACTION BUY — book only", "",
              "| Action | # | Ticker | Source boxes | Domains | Lane | Score | Decision |",
              "|---|---|---|---|---|---|---|---|"]
    book_only = [d for d in (h1.get("buy") or []) if not d.get("sleeve")]
    if not book_only:
        lines.append("| — | | | | | | | none |")
    for d in book_only:
        lines.append(_action_md_row(d, "BUY"))
    lines += [
        "",
        "### Bull decisions — eligible and closest blocked cases",
        "",
        "Domain order: **market · parent · child · company · setup · flow**.",
        "",
        "| # | Ticker | Domains | Lane | Direct company | Group | Decision |",
        "|---:|---|---|---|---|---|---|",
    ]
    for i, row in enumerate((dec.get("bull_watch") or [])[:15], 1):
        company = str(row.get("company") or "—").replace("|", "/")
        decision = str(row.get("bull_decision") or "").replace("|", "/")
        lines.append(
            f"| {i} | `{_tick(row.get('ticker'))}` | "
            f"{_domain_cell(row.get('domains') or {})} | "
            f"{row.get('lane') or 'blocked'} | {company} | "
            f"{row.get('group') or '—'} "
            f"({_num(row.get('child_d1')) or 0:+.1f}% d1 / "
            f"{_num(row.get('child_w1')) or 0:+.1f}% 1w / "
            f"{_num(row.get('child_residual')) or 0:+.1f}% rel) | "
            f"{decision} |"
        )
    lines += [
        "",
        "### ACTION SELL — bear decisions (not paper-traded)",
        "",
        "| Action | # | Ticker | Source boxes | Domains | Lane | Score | Decision |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for d in (h1.get("sell") or []):
        lines.append(_action_md_row(d, "SELL"))
    lines.append("")
    return lines


def _action_md_row(d: dict, verb: str) -> str:
    boxes = "".join(BOX_ICON.get((d.get("boxes") or {}).get(k), "⬛")
                    for k in BOX_KEYS)
    why = (
        d.get("bull_decision") if verb == "BUY"
        else d.get("bear_decision")
    ) or d.get("reasons") or ""
    why = str(why).replace("|", "/")
    return (
        f"| **{verb}** | {d.get('rank')} | `{d.get('ticker')}` | "
        f"{boxes} | {_domain_cell(d.get('domains') or {})} | "
        f"{d.get('decision_lane') or '—'} | "
        f"{d.get('score') or 0:.3f} | {why} |"
    )


def emit_action_notices(dec: dict) -> None:
    """GitHub Actions annotations so BUY names show at the top of the run."""
    if os.environ.get("GITHUB_ACTIONS") != "true":
        return
    h1 = ((dec.get("horizons") or {}).get(PRIMARY_H) or {})
    for d in (h1.get("buy") or []):
        if not d.get("sleeve"):
            continue
        reason = (d.get("reasons") or "").replace("\n", " ")[:200]
        print(
            f"::notice title=ACTION BUY {d.get('ticker')}::"
            f"#{d.get('rank')} {reason}",
            flush=True,
        )


def render_decisions_markdown(dec: dict) -> list[str]:
    if not dec.get("present"):
        return [
            "## Buy / sell decisions",
            "",
            "_No stock book JSON — the ranker has not written a name._",
            "",
        ]
    h1 = (dec.get("horizons") or {}).get(PRIMARY_H) or {}
    lines = [
        "## Buy / sell decisions",
        "",
        f"Ranker **{dec.get('ranker')}**"
        + (f" · green pile n={dec.get('n_pile')}" if dec.get("n_pile") is not None else "")
        + f" · 1d book {dec.get('n_1d_buy') or 0} buy / {dec.get('n_1d_sell') or 0} sell"
        + f" · vs prior {dec.get('prior_date')}",
        "",
        "Each name is a row the ranker actually wrote. Source boxes retain "
        "the lookback red/yellow/green language; the six-domain row owns "
        "permission and keeps duplicate evidence from voting twice.",
        "",
        f"_Boxes {_box_legend()}_",
        "",
        "### How a name gets onto the .io dashboard",
        "",
    ]
    for step in dec.get("how") or []:
        lines.append(f"- {step}")
    lines += ["", "### 1d BUY — what paper can fill", ""]
    lines += [
        "| # | Name | Sleeve | Source boxes | Domains | Lane | Score | Sector | Decision | Setups |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for d in h1.get("buy") or []:
        sleeve = "1d_top" if d.get("sleeve") else "book only"
        setups = _setups(d)
        marks = _marks(d)
        name = f"{marks} `{d['ticker']}`".strip()
        why = (d.get("bull_decision") or d.get("reasons") or "") + (
            (" · " + _score_bits(d)) if d.get("scores") else ""
        )
        lines.append(
            f"| {d['rank']} | {name} | {sleeve} | {_box_cell(d.get('boxes'))} | "
            f"{_domain_cell(d.get('domains'))} | "
            f"{d.get('decision_lane') or '—'} | "
            f"{d.get('score') or 0:.3f} | {d.get('sector') or ''} | "
            f"{why.replace('|', '/')} | {setups} |"
        )
    lines += ["", "### 1d SELL — fade list (not paper-traded)", ""]
    lines += [
        "| # | Name | Source boxes | Domains | Score | Sector | Bear decision | Setups |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for d in h1.get("sell") or []:
        marks = _marks(d)
        name = f"{marks} `{d['ticker']}`".strip()
        why = (d.get("bear_decision") or d.get("reasons") or "") + (
            (" · " + _score_bits(d)) if d.get("scores") else ""
        )
        lines.append(
            f"| {d['rank']} | {name} | {_box_cell(d.get('boxes'))} | "
            f"{_domain_cell(d.get('domains'))} | "
            f"{d.get('score') or 0:.3f} | {d.get('sector') or ''} | "
            f"{why.replace('|', '/')} | {_setups(d)} |"
        )

    h2 = (dec.get("horizons") or {}).get(SECONDARY_H) or {}
    if h2.get("buy"):
        lines += ["", "### 1m BUY (sleeve top 10)", ""]
        lines += [
            "| # | Name | Boxes | Score | Rationale |",
            "|---|---|---|---|---|",
        ]
        for d in h2.get("buy") or []:
            lines.append(
                f"| {d['rank']} | `{d['ticker']}` | {_box_cell(d.get('boxes'))} | "
                f"{d.get('score') or 0:.3f} | {(d.get('reasons') or '').replace('|', '/')} |"
            )

    lines += ["", "### Lineage — which input colored which box", ""]
    lines += [
        "| Box | File | Workflow | What it is |",
        "|---|---|---|---|",
    ]
    for spec in dec.get("factor_trace") or []:
        lines.append(
            f"| {BOX_ICON['good']}/{BOX_ICON['neutral']}/{BOX_ICON['bad']} "
            f"{spec['label']} | `{spec['file']}` | {spec['workflow']} | "
            f"{spec['means']} |"
        )
    lines.append("")
    return lines


def render_pages_markdown(pages: dict) -> list[str]:
    lines = [
        "## Dashboard / .io publish",
        "",
        f"Fixed Chrome link: {PAGES_URL}",
        "",
        "Stock Book ALL writes `dashboard/index.html` via `src.paper_trade`, "
        "then force-pushes the `gh-pages` branch. `deploy-dashboard.yml` is "
        "the backup publish.",
        "",
    ]
    local = pages.get("local") or {}
    live = pages.get("live") or {}
    lines.append(
        f"- repo `dashboard/index.html`: **{local.get('status') or '?'}** "
        f"— {local.get('reason') or ''}"
    )
    lines.append(
        f"- live Pages: **{live.get('status') or '?'}** "
        f"— {live.get('reason') or ''}"
    )
    lines.append("")
    return lines
