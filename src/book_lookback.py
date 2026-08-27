"""Look back at what the stock-book ranker knew about a name on a given day.

For a trading day D (the snapshot the ranker consumed — predictive pieces
are the pre-09:30 packet; join/AB/weather/peer may land later the same day):

  1. TICKER CARD  — every input that touched T, plus the book's rank/score.
  2. WINNER SCAN  — names that actually gained over 1d / 2d / 3d / 1w, and
     whether ANY ranker input fired (news, digest, volume, AB, peers, heat,
     join, catalyst). Blind = the move happened on information the book
     did not carry.
  3. SIGNAL BOARD — one colored box per input: good / neutral / bad / missing.

No LLM. Reads committed artifacts + the local price store.

CLI:
  python -m src.book_lookback --date 2026-08-20
  python -m src.book_lookback --date 20/08 --tickers NVDA,AAPL --min-gain 5 --top 20
"""
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from . import config
from .book_learn import _fwd_returns, _load_panel, load_frame
from .stock_book import MIN_OPP_MCAP_M, _inputs_status

ROOT = Path(__file__).resolve().parent.parent
BOOK_DIR = ROOT / "data" / "stock_book"
DAILY = ROOT / "01_daily"
SCORE = ROOT / "03_scoreboard"
ET = ZoneInfo(config.TZ)

LOOK_H = {"1d": 1, "2d": 2, "3d": 3, "1w": 5}
CATCH_EPS = 0.05
RELVOL_SPIKE = 1.5
SPECIFIC = ("s_news", "s_ab", "s_peer", "s_heat")

BOX_ICON = {"good": "🟢", "bad": "🔴", "neutral": "🟡", "missing": "⬛"}
BOX_COLS = (
    ("join", "join"),
    ("sector", "sect"),
    ("gen", "gen"),
    ("news", "news"),
    ("digest", "dig"),
    ("judge", "jdg"),
    ("ab", "AB"),
    ("peer", "peer"),
    ("heat", "heat"),
    ("vol", "vol"),
    ("catal", "cat"),
    ("buy", "buy"),
)


def _parse_date(raw: str | None) -> str:
    if not raw:
        files = sorted(BOOK_DIR.glob("????-??-??_stock_book.csv"))
        if not files:
            raise SystemExit("no stock_book CSV in data/stock_book — pass --date")
        return files[-1].name[:10]
    raw = raw.strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
        return raw
    m = re.fullmatch(r"(\d{1,2})[/-](\d{1,2})(?:[/-](\d{2,4}))?", raw)
    if m:
        d, mo, y = int(m.group(1)), int(m.group(2)), m.group(3)
        year = int(y) if y else datetime.now(ET).year
        if year < 100:
            year += 2000
        if mo > 12 and d <= 12:
            d, mo = mo, d
        return f"{year:04d}-{mo:02d}-{d:02d}"
    raise SystemExit(f"cannot parse date: {raw}")


def _jload(*parts) -> dict | list | None:
    p = ROOT.joinpath(*parts)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, low_memory=False)
    except Exception:
        return pd.DataFrame()


def _tick(s) -> str:
    return str(s or "").strip().upper()


def _polarity(x, eps: float = CATCH_EPS) -> str:
    if x is None:
        return "missing"
    try:
        v = float(x)
    except (TypeError, ValueError):
        return "missing"
    if v >= eps:
        return "good"
    if v <= -eps:
        return "bad"
    return "neutral"


def _missing_layers(inventory: list[dict] | None) -> dict[str, bool]:
    found = {}
    for row in inventory or []:
        found[str(row.get("name") or "")] = bool(row.get("found"))
    heat_ok = found.get("Map heat research") or found.get("Captain research") or found.get("Map heat tables")
    return {
        "news": not found.get("News parse + actions", True),
        "judge": not found.get("News judge", True),
        "digest": not found.get("Finviz daily digest", True),
        "heat": not heat_ok,
        "catalyst": not found.get("Catalyst dossiers", False),
        "finviz": not found.get("Finviz Elite export", True),
    }


def _boxes(signals: dict, ev: dict | None, in_buy: bool,
          missing: dict | None) -> dict[str, str]:
    s = signals or {}
    ev = ev or {}
    miss = missing or {}
    heat = ev.get("heat") or {}
    cat = ev.get("catalyst")
    fv = ev.get("finviz") or {}
    out = {
        "join": _polarity(s.get("s_join")),
        "sector": _polarity(s.get("s_sector")),
        "gen": _polarity(s.get("s_general")),
        "news": "missing" if miss.get("news") else _polarity(s.get("s_news")),
        "ab": _polarity(s.get("s_ab")),
        "peer": _polarity(s.get("s_peer")),
    }
    if miss.get("heat"):
        out["heat"] = "missing"
    elif heat.get("boost") == "veto":
        out["heat"] = "bad"
    elif heat.get("captain") or heat.get("boost") == "opportunity":
        out["heat"] = "good"
    else:
        out["heat"] = _polarity(s.get("s_heat"))
    if miss.get("digest"):
        out["digest"] = "missing"
    elif ev.get("digest"):
        out["digest"] = "good"
    else:
        out["digest"] = "neutral"
    if miss.get("judge"):
        out["judge"] = "missing"
    else:
        out["judge"] = _polarity(ev.get("judge_tilt")) if ev.get("judge_tilt") is not None else "neutral"
    if miss.get("catalyst"):
        out["catal"] = "missing"
    elif cat:
        out["catal"] = _polarity(cat.get("net_signal"))
    else:
        out["catal"] = "neutral"
    rel = fv.get("relvol")
    if miss.get("finviz") and rel is None:
        out["vol"] = "missing"
    elif rel is None:
        out["vol"] = "neutral"
    elif rel >= RELVOL_SPIKE:
        out["vol"] = "good"
    elif rel < 0.7:
        out["vol"] = "bad"
    else:
        out["vol"] = "neutral"
    out["buy"] = "good" if in_buy else "neutral"
    return out


def _icon(kind: str) -> str:
    return BOX_ICON.get(kind, BOX_ICON["missing"])


def _finviz_row(date: str, ticker: str) -> dict:
    p = ROOT / "data" / "exports" / f"finviz_{date}.csv"
    if not p.exists():
        files = sorted((ROOT / "data" / "exports").glob("finviz_????-??-??.csv"))
        p = files[-1] if files else None
    if p is None:
        return {}
    df = _csv(p)
    if df.empty:
        return {}
    tcol = "Ticker" if "Ticker" in df.columns else df.columns[0]
    df[tcol] = df[tcol].astype(str).str.strip().str.upper()
    hit = df[df[tcol] == ticker]
    if hit.empty:
        return {}
    r = hit.iloc[0].to_dict()

    def num(*names):
        for n in names:
            if n in r and r[n] == r[n]:
                try:
                    return float(r[n])
                except (TypeError, ValueError):
                    continue
        return None

    vol = num("Volume")
    adv = num("Average Volume", "Avg Volume", "Avg Vol")
    rel = num("Relative Volume", "Rel Volume", "Rel Vol", "RelVol")
    if rel is None and vol and adv and adv > 0:
        adv_shares = adv * 1000 if adv < vol else adv
        rel = vol / adv_shares if adv_shares else None
    return {
        "source": p.name,
        "price": num("Price"),
        "change_pct": num("Change", "Change %"),
        "volume": vol,
        "avg_vol_k": adv,
        "relvol": rel,
        "market_cap_m": num("Market Cap"),
        "spike": bool(rel is not None and rel >= RELVOL_SPIKE),
    }


def _news_actions(date: str, ticker: str) -> dict | None:
    data = _jload("01_daily", "news", f"{date}_actions.json") or {}
    ta = data.get("ticker_actions") or data.get("edge_actions") or data.get("actions") or {}
    items = []
    if isinstance(ta, dict):
        rec = ta.get(ticker) or ta.get(ticker.lower())
        if rec is not None:
            items.append(rec if isinstance(rec, dict) else {"raw": rec, "ticker": ticker})
    elif isinstance(ta, list):
        items = [x for x in ta if isinstance(x, dict)
                 and _tick(x.get("ticker") or x.get("Ticker")) == ticker]
    if not items:
        return None
    row = items[0]
    return {
        "side": row.get("side") or row.get("action"),
        "net": row.get("net") or row.get("weight") or row.get("score"),
        "reason": str(row.get("reason") or row.get("note") or "")[:240],
        "events": row.get("events") or [],
    }


def _digest_hits(date: str, ticker: str) -> list[str]:
    data = _jload("01_daily", "news", f"{date}_finviz_digest.json") or {}
    out = []
    rows = list(data.get("top_signal") or []) + list(data.get("all_ticker_digests_sample") or [])
    for sec_rows in (data.get("by_sector") or {}).values():
        rows.extend(sec_rows or [])
    for row in rows:
        if not isinstance(row, dict):
            continue
        if _tick(row.get("ticker")) != ticker:
            continue
        text = str(row.get("digest") or row.get("news_title") or "").strip()
        if text:
            out.append(text[:220])
    return out[:6]


def _judge_tilt(date: str, ticker: str) -> float | None:
    try:
        from .judge_apply import load_or_parse
        rec = load_or_parse(date) or {}
        tmap = rec.get("tickers") or {}
        if ticker in tmap:
            return float(tmap[ticker])
    except Exception:
        return None
    return None


def _heat_hits(date: str, ticker: str) -> dict:
    out = {"captain": False, "industry": None, "card_sent": None,
           "card_dir": None, "news": [], "boost": None}
    heat = _jload("01_daily", "map_heat", f"{date}_map_heat.json") or {}
    for row in heat.get("industries") or []:
        leads = list(row.get("spx_leaders") or []) + list(row.get("rut_leaders") or [])
        for c in leads:
            if _tick(c.get("ticker") if isinstance(c, dict) else c) == ticker:
                out["captain"] = True
                out["industry"] = row.get("industry")
    for n in heat.get("ticker_news") or []:
        ticks = {_tick(t) for t in (n.get("tickers") or [])}
        if ticker in ticks:
            out["news"].append(str(n.get("title") or n.get("headline") or "")[:180])
    for name in (f"{date}_research.json", f"{date}_research_baseline.json"):
        res = _jload("01_daily", "map_heat", name) or {}
        for card in res.get("cards") or []:
            captains = []
            for key in ("spx", "rut", "captains", "names"):
                captains.extend(card.get(key) or [])
            for c in captains:
                if isinstance(c, dict) and _tick(c.get("ticker")) == ticker:
                    out["captain"] = True
                    out["industry"] = out["industry"] or card.get("industry")
                    out["card_sent"] = c.get("sent") or card.get("sent")
                    out["card_dir"] = card.get("subsector_dir")
                elif _tick(c) == ticker:
                    out["captain"] = True
                    out["industry"] = out["industry"] or card.get("industry")
        for opp in res.get("opportunities") or []:
            ticks = set()
            if isinstance(opp, dict):
                if opp.get("ticker"):
                    ticks.add(_tick(opp.get("ticker")))
                ticks.update(_tick(x) for x in (opp.get("tickers") or []))
            if ticker in ticks:
                out["boost"] = "opportunity"
        for v in res.get("vetoes") or []:
            vt = v.get("ticker") if isinstance(v, dict) else v
            if ticker == _tick(vt):
                out["boost"] = "veto"
    try:
        from .map_heat_research import ticker_boosts
        tboost, _ = ticker_boosts(date)
        if ticker in tboost:
            out["boost"] = tboost[ticker]
    except Exception:
        pass
    out["news"] = out["news"][:5]
    return out


def _catalyst(date: str, ticker: str) -> dict | None:
    data = _jload("01_daily", "catalyst", f"{date}_dossiers.json") or {}
    for row in data.get("dossiers") or []:
        if not isinstance(row, dict):
            continue
        ticks = {_tick(row.get("ticker"))}
        ticks.update(_tick(t) for t in (row.get("tickers") or []))
        if ticker in ticks:
            return {
                "net_signal": row.get("net_signal"),
                "error": row.get("error"),
                "search_backend": row.get("search_backend"),
                "one_liner": str(row.get("one_liner") or row.get("summary") or "")[:240],
            }
    return None


def _book_placement(book: dict | None, ticker: str) -> dict:
    out = {"in_buy": [], "in_sell": [], "ranks": {}}
    if not book:
        return out
    for h, entry in (book.get("books") or {}).items():
        for i, r in enumerate(entry.get("buy") or [], 1):
            if _tick(r.get("ticker")) == ticker:
                out["in_buy"].append(h)
                out["ranks"][f"buy_{h}"] = {
                    "rank": i, "score": r.get("score"),
                    "reasons": r.get("reasons"),
                }
        for i, r in enumerate(entry.get("sell") or [], 1):
            if _tick(r.get("ticker")) == ticker:
                out["in_sell"].append(h)
                out["ranks"][f"sell_{h}"] = {"rank": i, "score": r.get("score")}
    return out


def _signals(row: pd.Series | None) -> dict:
    keys = ("s_join", "s_sector", "s_general", "s_news", "s_ab", "s_peer",
            "s_heat", "s_opp", "score_1d", "score_3d", "score_1w")
    out = {}
    for k in keys:
        if row is None or k not in row.index:
            out[k] = 0.0
            continue
        try:
            out[k] = round(float(row.get(k) or 0.0), 3)
        except (TypeError, ValueError):
            out[k] = 0.0
    return out


def _classify(row: pd.Series | None, place: dict, evidence: dict) -> str:
    if row is not None:
        try:
            mcap = float(row.get("market_cap_m") or 0)
        except (TypeError, ValueError):
            mcap = 0.0
        size = str(row.get("size") or "").lower()
        if size == "micro" or (mcap and mcap < MIN_OPP_MCAP_M):
            return "gated_out"
    if place.get("in_buy"):
        return "in_buy_book"
    fired = False
    if row is not None:
        fired = any(abs(float(row.get(c) or 0)) >= CATCH_EPS for c in SPECIFIC)
    ev = evidence or {}
    if ev.get("news_actions") or ev.get("digest") or ev.get("judge_tilt"):
        fired = True
    if (ev.get("finviz") or {}).get("spike"):
        fired = True
    if (ev.get("heat") or {}).get("captain") or (ev.get("heat") or {}).get("boost"):
        fired = True
    if ev.get("catalyst"):
        fired = True
    return "outweighed" if fired else "blind"


def _frame_row(frame: pd.DataFrame | None, ticker: str) -> pd.Series | None:
    if frame is None or frame.empty:
        return None
    hit = frame[frame["Ticker"] == ticker]
    if hit.empty:
        return None
    return hit.iloc[0]


def gather_card(date: str, ticker: str, frame: pd.DataFrame | None,
                book: dict | None, fwd: dict[str, float | None],
                missing: dict | None = None) -> dict:
    ticker = _tick(ticker)
    row = _frame_row(frame, ticker)
    evidence = {
        "news_actions": _news_actions(date, ticker),
        "digest": _digest_hits(date, ticker),
        "judge_tilt": _judge_tilt(date, ticker),
        "finviz": _finviz_row(date, ticker),
        "heat": _heat_hits(date, ticker),
        "catalyst": _catalyst(date, ticker),
    }
    place = _book_placement(book, ticker)
    cls = _classify(row, place, evidence)
    sig = _signals(row)
    return {
        "ticker": ticker,
        "date": date,
        "class": cls,
        "in_universe": row is not None,
        "sector": None if row is None else row.get("sector"),
        "industry": None if row is None else row.get("industry"),
        "size": None if row is None else row.get("size"),
        "signals": sig,
        "boxes": _boxes(sig, evidence, bool(place.get("in_buy")), missing),
        "placement": place,
        "fwd_pct": {h: (None if v is None else round(v * 100, 2)) for h, v in fwd.items()},
        "evidence": evidence,
        "reasons": None if row is None else str(row.get("reasons") or ""),
    }


def _fwd_map(panel, date: str, ticker: str) -> dict[str, float | None]:
    if panel is None:
        return {h: None for h in LOOK_H}
    out: dict[str, float | None] = {}
    for h, n in LOOK_H.items():
        rets = _fwd_returns(panel, date, n)
        if rets is None or ticker not in rets.index:
            out[h] = None
        else:
            try:
                out[h] = float(rets.loc[ticker])
            except (TypeError, ValueError):
                out[h] = None
    return out


def winner_scan(date: str, frame: pd.DataFrame | None, book: dict | None,
                panel, min_gain: float, top: int,
                missing: dict | None = None) -> dict[str, list[dict]]:
    if frame is None or panel is None:
        return {}
    buys = set()
    if book:
        for entry in (book.get("books") or {}).values():
            for r in entry.get("buy") or []:
                buys.add(_tick(r.get("ticker")))
    out: dict[str, list[dict]] = {}
    thresh = min_gain / 100.0
    for h, n in LOOK_H.items():
        # 5% per session: 1d=5, 2d=10, 3d=15, 1w=25 when min_gain=5
        need = thresh * n
        rets = _fwd_returns(panel, date, n)
        if rets is None:
            out[h] = []
            continue
        f = frame.set_index("Ticker", drop=False)
        f["fwd"] = rets.reindex(f.index)
        f = f.dropna(subset=["fwd"])
        movers = f[f["fwd"] >= need].sort_values("fwd", ascending=False).head(top)
        rows = []
        for _, r in movers.iterrows():
            t = _tick(r["Ticker"])
            ev = {
                "news_actions": _news_actions(date, t),
                "digest": _digest_hits(date, t),
                "judge_tilt": _judge_tilt(date, t),
                "finviz": _finviz_row(date, t),
                "heat": _heat_hits(date, t),
                "catalyst": _catalyst(date, t),
            }
            place = {"in_buy": [h] if t in buys else [], "in_sell": []}
            cls = _classify(r, place, ev)
            sig = _signals(r)
            fired = []
            for c in SPECIFIC:
                if abs(float(r.get(c) or 0)) >= CATCH_EPS:
                    fired.append(f"{c}={float(r.get(c) or 0):+.2f}")
            if ev.get("news_actions"):
                fired.append("news_actions")
            if ev.get("digest"):
                fired.append("digest")
            if ev.get("judge_tilt"):
                fired.append(f"judge={ev['judge_tilt']:+.1f}")
            if (ev.get("finviz") or {}).get("spike"):
                fired.append(f"relvol={ev['finviz'].get('relvol')}")
            if (ev.get("heat") or {}).get("captain"):
                fired.append("heat_captain")
            if ev.get("catalyst"):
                fired.append("catalyst")
            rows.append({
                "ticker": t,
                "fwd_pct": round(float(r["fwd"]) * 100, 2),
                "class": cls,
                "sector": r.get("sector"),
                "size": r.get("size"),
                "in_buy_book": t in buys,
                "fired": fired,
                "signals": sig,
                "boxes": _boxes(sig, ev, t in buys, missing),
                "digest": (ev.get("digest") or [])[:2],
            })
        out[h] = rows
    return out


def _inventory(date: str) -> list[dict]:
    rows = _inputs_status(date)
    extra = [
        ("Catalyst dossiers",
         (ROOT / "01_daily" / "catalyst" / f"{date}_dossiers.json").exists(),
         "pre-open layer 3; merged into news actions"),
        ("Map heat tables",
         (ROOT / "01_daily" / "map_heat" / f"{date}_map_heat.json").exists(),
         "pre-open overlay / post-close tables"),
        ("Captain research",
         (ROOT / "01_daily" / "map_heat" / f"{date}_research.json").exists()
         or (ROOT / "01_daily" / "map_heat" / f"{date}_research_baseline.json").exists(),
         "s_heat captains — missing → bootstrap stub"),
        ("Stock book CSV",
         (BOOK_DIR / f"{date}_stock_book.csv").exists(),
         "ranker snapshot (usually written after the open)"),
    ]
    for n, f, u in extra:
        rows.append({"name": n, "found": bool(f), "used_as": u})
    return rows


def _md_board(rows: list[dict]) -> list[str]:
    if not rows:
        return []
    heads = " | ".join(["Ticker", "fwd"] + [lab for _, lab in BOX_COLS])
    bars = "|".join(["---"] * (2 + len(BOX_COLS)))
    L = [
        "| " + heads + " |",
        "|" + bars + "|",
    ]
    for r in rows:
        boxes = r.get("boxes") or {}
        cells = [_icon(boxes.get(key, "missing")) for key, _ in BOX_COLS]
        L.append(
            f"| {r['ticker']} | {r['fwd_pct']:+.1f}% | " + " | ".join(cells) + " |"
        )
    L.append("")
    return L


def _md_card(c: dict) -> list[str]:
    s = c["signals"]
    ev = c["evidence"]
    fv = ev.get("finviz") or {}
    heat = ev.get("heat") or {}
    na = ev.get("news_actions")
    boxes = c.get("boxes") or {}
    L = [
        f"### {c['ticker']} · {c.get('size') or '?'} · {c.get('sector') or '?'}",
        "",
        f"**class: `{c['class']}`** · in universe: {c['in_universe']} · "
        f"buy books: {', '.join(c['placement'].get('in_buy') or []) or '—'}",
        "",
        " ".join(f"{_icon(boxes.get(k, 'missing'))}{lab}" for k, lab in BOX_COLS),
        "",
        "| 1d | 2d | 3d | 1w |",
        "|----|----|----|----|",
        "| " + " | ".join(
            ("n/a" if c["fwd_pct"].get(h) is None else f"{c['fwd_pct'][h]:+.1f}%")
            for h in LOOK_H
        ) + " |",
        "",
        "| Layer | Signal | When it lands |",
        "|-------|-------:|---------------|",
        f"| join × weather | {s.get('s_join', 0):+.2f} | after open (join) |",
        f"| sector predict | {s.get('s_sector', 0):+.2f} | pre-09:30 |",
        f"| general predict | {s.get('s_general', 0):+.2f} | pre-09:30 |",
        f"| news / judge | {s.get('s_news', 0):+.2f} | pre-09:30 |",
        f"| AB checklist | {s.get('s_ab', 0):+.2f} | afternoon |",
        f"| peer RS | {s.get('s_peer', 0):+.2f} | afternoon |",
        f"| map heat | {s.get('s_heat', 0):+.2f} | post-close + morning delta |",
        f"| mid-opp | {s.get('s_opp', 0):+.2f} | labels |",
        "",
    ]
    if c.get("reasons"):
        L += [f"Ranker reasons: `{c['reasons']}`", ""]
    bits = []
    if na:
        bits.append(
            f"news action **{na.get('side')}** net={na.get('net')} — {na.get('reason')}"
        )
    if ev.get("digest"):
        bits.append("Finviz digest: " + " / ".join(ev["digest"][:2]))
    if ev.get("judge_tilt") is not None:
        bits.append(f"news-judge tilt {ev['judge_tilt']:+.2f}")
    if fv:
        bits.append(
            f"Finviz tape change={fv.get('change_pct')} relvol={fv.get('relvol')} "
            f"spike={fv.get('spike')} ({fv.get('source')})"
        )
    if heat.get("captain") or heat.get("boost") is not None:
        bits.append(
            f"heat captain={heat.get('captain')} industry={heat.get('industry')} "
            f"sent={heat.get('card_sent')} dir={heat.get('card_dir')} boost={heat.get('boost')}"
        )
    if heat.get("news"):
        bits.append("heat ticker news: " + " / ".join(heat["news"][:2]))
    if ev.get("catalyst"):
        cat = ev["catalyst"]
        bits.append(f"catalyst net={cat.get('net_signal')} — {cat.get('one_liner')}")
    if bits:
        L.append("**What fired before the ranker:**")
        L.append("")
        for b in bits:
            L.append(f"- {b}")
        L.append("")
    else:
        L.append("_No ticker-specific news / digest / heat / catalyst / volume spike on this date._")
        L.append("")
    return L


def render(date: str, cards: list[dict], winners: dict, inventory: list[dict],
           notes: list[str], min_gain: float = 5.0) -> str:
    L = [
        f"# Book lookback — {date}",
        "",
        f"_Generated {datetime.now(ET).isoformat()}_",
        "",
        f"Winner bar: **{min_gain:g}% per session** "
        f"(1d ≥ {min_gain:g}% · 2d ≥ {min_gain*2:g}% · "
        f"3d ≥ {min_gain*3:g}% · 1w ≥ {min_gain*5:g}%).",
        "",
        "Question: **on this trading day, before 09:30 ET, what did the pipeline",
        "that feeds the Stock Book Ranker know about a name — and for names that",
        "then went up 1d/2d/3d/1w, did ANY input fire?**",
        "",
        "Honest timing: the ranker file itself is usually written **after the open**.",
        "News / judge / digest / events / map-heat / predicts / catalyst are the",
        "pre-09:30 packet. Join, AB, peers, weather may land later the same day",
        "and still moved that day's book.",
        "",
        "Classes: **in_buy_book** = ranker picked it · **outweighed** = something",
        "fired but the name was not in a buy book · **gated_out** = micro / <$400M",
        "· **blind** = no ticker-specific signal (news, AB, peers, heat, digest,",
        "catalyst, volume spike).",
        "",
        "Boxes: 🟢 good (helped / fired bullish) · 🟡 neutral (present, flat) · "
        "🔴 bad (fired against the name) · ⬛ missing (that day's file was not there).",
        "",
    ]
    for n in notes:
        L.append(f"> {n}")
        L.append("")
    L += [
        "## Inputs present for this date",
        "",
        "| Resource | Found | Lands in |",
        "|----------|-------|----------|",
    ]
    for r in inventory:
        L.append(
            f"| {r['name']} | {'yes' if r.get('found') else 'NO'} | {r.get('used_as')} |"
        )
    if cards:
        L += ["", "## Requested tickers", ""]
        for c in cards:
            L += _md_card(c)
    L += ["", "## Winners — did the ranker see *something*?", ""]
    for h in LOOK_H:
        rows = winners.get(h) or []
        need = min_gain * LOOK_H[h]
        L.append(f"### {h} (next {LOOK_H[h]} session(s), bar {need:g}%)")
        L.append("")
        if not rows:
            L.append("_No realized winners at this gain threshold (or prices not in yet)._")
            L.append("")
            continue
        n_blind = sum(1 for r in rows if r["class"] == "blind")
        n_buy = sum(1 for r in rows if r["in_buy_book"])
        L.append(
            f"{len(rows)} names ≥ {need:g}% · {n_buy} already in a buy book · "
            f"**{n_blind} blind**."
        )
        L.append("")
        L += _md_board(rows)
        L.append("| Ticker | fwd% | class | size | sector | what fired |")
        L.append("|--------|------|-------|------|--------|------------|")
        for r in rows:
            fired = ", ".join(r.get("fired") or []) or "—"
            L.append(
                f"| {r['ticker']} | {r['fwd_pct']:+.1f} | {r['class']} | "
                f"{r.get('size')} | {r.get('sector')} | {fired} |"
            )
        L.append("")
    L += [
        "## Files",
        "",
        f"- `data/stock_book/{date}_lookback.json`",
        f"- `01_daily/{date}_lookback.md`",
        f"- `03_scoreboard/BOOK_LOOKBACK.md` (this report, latest run)",
        "",
    ]
    return "\n".join(L) + "\n"


def run(date: str | None = None, tickers: list[str] | None = None,
        min_gain: float = 5.0, top: int = 20) -> dict:
    date = _parse_date(date)
    tickers = [_tick(t) for t in (tickers or []) if _tick(t)]
    notes = []
    frame = load_frame(date)
    if frame is None:
        notes.append(
            f"No `{date}_stock_book.csv` — ranker snapshot missing. "
            "Cards use raw news/heat/digest only; winner scan skipped."
        )
    book = _jload("data", "stock_book", f"{date}_stock_book.json")
    panel = _load_panel()
    if panel is None:
        notes.append("Price store missing — forward returns will be n/a.")
    inventory = _inventory(date)
    missing = _missing_layers(inventory)
    cards = []
    for t in tickers:
        cards.append(gather_card(date, t, frame, book, _fwd_map(panel, date, t), missing))
    winners = winner_scan(date, frame, book, panel, min_gain, top, missing) if frame is not None else {}
    payload = {
        "date": date,
        "generated_at": datetime.now(ET).isoformat(),
        "min_gain_pct": min_gain,
        "per_session": True,
        "notes": notes,
        "inventory": inventory,
        "cards": cards,
        "winners": winners,
        "blind_counts": {
            h: sum(1 for r in (winners.get(h) or []) if r["class"] == "blind")
            for h in LOOK_H
        },
    }
    BOOK_DIR.mkdir(parents=True, exist_ok=True)
    DAILY.mkdir(parents=True, exist_ok=True)
    SCORE.mkdir(parents=True, exist_ok=True)
    js = BOOK_DIR / f"{date}_lookback.json"
    js.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    md = render(date, cards, winners, inventory, notes, min_gain=min_gain)
    (DAILY / f"{date}_lookback.md").write_text(md, encoding="utf-8")
    (SCORE / "BOOK_LOOKBACK.md").write_text(md, encoding="utf-8")
    print(md[:8000])
    print(f"[book-lookback] wrote {js}")
    print(f"[book-lookback] wrote {DAILY / (date + '_lookback.md')}")
    return payload


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None, help="YYYY-MM-DD or DD/MM")
    ap.add_argument("--tickers", default="", help="comma-separated, optional")
    ap.add_argument("--min-gain", type=float, default=5.0,
                    help="winner threshold percent PER SESSION (default 5)")
    ap.add_argument("--top", type=int, default=20)
    args = ap.parse_args()
    tickers = [t.strip() for t in args.tickers.split(",") if t.strip()]
    run(date=args.date, tickers=tickers, min_gain=args.min_gain, top=args.top)


if __name__ == "__main__":
    main()
