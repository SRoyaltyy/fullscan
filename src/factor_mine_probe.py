"""Stock investigator cards — what a 09:30 sleeve actually saw.

Quotes cameras / coaches / news / tape from repo files only
(panel, flatten lookback, prior Finviz export, morning digest).
Never uses same-day Change% to color a cell.
"""
from __future__ import annotations

import json
import math
from pathlib import Path

from . import factor_mine as fm
from . import gainer_asof as ga
from . import ticker_lookback as tl

ROOT = Path(__file__).resolve().parent.parent
FLATTEN_JSON = ROOT / "03_scoreboard" / "flatten_lookback_action.json"
NEWS_DIR = ROOT / "01_daily" / "news"
EXPORT_DIR = ROOT / "data" / "exports"

CAM_LABS = list(tl.BOX_COLS) + [("yday", "yΔ")]
COACH_LABS = (
    ("market", "mkt"),
    ("parent", "par"),
    ("child", "chd"),
    ("company", "co"),
    ("setup", "set"),
    ("flow", "flw"),
)
CAM_FILE = {
    "join": "data/join (morning ranked file)",
    "sector": "01_daily/weather sector predict",
    "gen": "01_daily/general morning predict",
    "news": "01_daily/news actions / prior Finviz News Title",
    "digest": "01_daily/news/*_finviz_digest.json",
    "judge": "01_daily/news/*_judge.json",
    "ab": "data/ab_checklist",
    "peer": "data/peers",
    "heat": "01_daily/map_heat",
    "vol": "prior-session relative volume camera",
    "catal": "01_daily/catalyst",
    "buy": "overnight buy camera / prior book",
    "yday": "prior-session Change% (yΔ) — never today's tape",
}
COACH_FILE = {
    "market": "flatten lookback / ticker lookback market coach",
    "parent": "parent-sector coach",
    "child": "child-industry coach",
    "company": "company coach",
    "setup": "setup coach",
    "flow": "flow coach",
}
TONE_WORD = {
    "good": "green", "bad": "red", "neutral": "yellow", "missing": "blank",
}


def _round(v, n=2):
    x = fm._finite(v)
    return None if x is None else round(float(x), n)


def _parse_num(v):
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    s = str(v).strip().replace(",", "").replace("%", "")
    if not s or s.lower() in ("nan", "none", "—", "-"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def surprise_polarity(v) -> tuple[str, str]:
    """EPS surprise → tone. Date-only green is not a beat."""
    x = _parse_num(v)
    if x is None:
        return "missing", "no EPS surprise on the prior export"
    if x > 0.5:
        return "good", f"beat · EPS surprise {x:+.1f}% (prior export)"
    if x < -0.5:
        return "bad", f"miss · EPS surprise {x:+.1f}% (prior export)"
    return "neutral", f"inline · EPS surprise {x:+.1f}% (prior export)"


def recom_polarity(v) -> tuple[str, str]:
    """Finviz Analyst Recom (1=strong buy … 5=sell). Level, not a change."""
    x = _parse_num(v)
    if x is None:
        return "missing", "no analyst recom on the prior export"
    if x <= 2.0:
        return "good", f"buy-side recom {x:.1f} (level, not a change · prior export)"
    if x >= 3.5:
        return "bad", f"sell-side recom {x:.1f} (level, not a change · prior export)"
    return "neutral", f"hold-ish recom {x:.1f} (level, not a change · prior export)"


def erd_polarity(row: dict, fv: dict | None = None) -> dict:
    """Honest E / R polarity. Never paint green just because a date exists."""
    fv = fv or {}
    surp_pol, surp_lab = surprise_polarity(fv.get("EPS Surprise"))
    rec_pol, rec_lab = recom_polarity(fv.get("Analyst Recom"))
    days_e = row.get("erd_days_since_E")
    flag_e = int(row.get("erd_flag_E") or 0)
    flag_r = int(row.get("erd_flag_R") or 0)
    label_e = str(row.get("erd_E_label") or "")
    label_r = str(row.get("erd_R_label") or "")
    if label_e == "E_BEAT" or (surp_pol == "good"):
        e_pol, e_label = "good", surp_lab if surp_pol == "good" else "earnings beat"
    elif label_e == "E_MISS" or (surp_pol == "bad"):
        e_pol, e_label = "bad", surp_lab if surp_pol == "bad" else "earnings miss"
    elif surp_pol == "neutral":
        e_pol, e_label = "neutral", surp_lab
    elif days_e is not None or flag_e or row.get("erd_earn_react"):
        e_pol, e_label = "neutral", (
            f"E {int(days_e)} sess ago · polarity unknown "
            "(export stamped the date, not beat/miss)"
            if days_e is not None else
            "E on file · polarity unknown (date-only green is not a beat)"
        )
    else:
        e_pol, e_label = "missing", "no earnings date on the prior export"
    if label_r == "R_UP" or flag_r == 1:
        r_pol, r_label = "good", "analyst upgrade (R)"
    elif label_r == "R_DOWN" or flag_r == -1:
        r_pol, r_label = "bad", "analyst downgrade (R)"
    elif rec_pol != "missing":
        r_pol, r_label = rec_pol, rec_lab
    else:
        r_pol, r_label = "missing", "no analyst revision on file"
    return {
        "e_pol": e_pol, "e_label": e_label,
        "r_pol": r_pol, "r_label": r_label,
    }


_FV_CACHE: dict[str, dict] = {}


def _prior_finviz_map(date: str | None) -> dict[str, dict]:
    if not date:
        return {}
    if date in _FV_CACHE:
        return _FV_CACHE[date]
    df = ga.load_finviz(date)
    out: dict[str, dict] = {}
    if df is not None and not getattr(df, "empty", True) and "Ticker" in df.columns:
        keep = [c for c in ("Ticker", "EPS Surprise", "Analyst Recom") if c in df.columns]
        for rec in df[keep].to_dict("records"):
            t = fm._tick(rec.get("Ticker"))
            if t:
                out[t] = rec
    _FV_CACHE[date] = out
    return out


def attach_erd_polarity(panel: dict) -> dict:
    """Stamp E/R polarity onto panel rows from the prior Finviz export."""
    for row in panel.get("rows") or []:
        prior = row.get("news_export_date") or row.get("prior_date")
        fv = _prior_finviz_map(prior).get(fm._tick(row.get("ticker"))) or {}
        row.update(erd_polarity(row, fv))
    return panel


def _finviz_news(date: str | None) -> dict[str, dict]:
    if not date:
        return {}
    path = EXPORT_DIR / f"finviz_{date}.csv"
    if not path.is_file():
        return {}
    df = ga.load_finviz(date)
    if df is None or getattr(df, "empty", True) or "Ticker" not in df.columns:
        return {}
    out = {}
    for _, row in df.iterrows():
        t = fm._tick(row.get("Ticker"))
        if not t:
            continue
        title = str(row.get("News Title") or "").strip()
        digest = str(row.get("Daily Digest") or "").strip()
        when = str(row.get("News Time") or "").strip()
        url = str(row.get("News URL") or "").strip()
        if not (title or digest):
            continue
        out[t] = {
            "title": title[:160],
            "digest": digest[:160],
            "when": when,
            "url": url[:180],
            "file": f"data/exports/finviz_{date}.csv",
        }
    return out


def _digest_map(date: str) -> dict[str, dict]:
    path = NEWS_DIR / f"{date}_finviz_digest.json"
    if not path.is_file():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    out = {}
    for row in (raw.get("top_signal") or []) + (raw.get("all_ticker_digests_sample") or []):
        t = fm._tick(row.get("ticker"))
        if not t:
            continue
        out[t] = {
            "title": str(row.get("news_title") or "").strip()[:160],
            "digest": str(row.get("digest") or "").strip()[:160],
            "file": f"01_daily/news/{date}_finviz_digest.json",
        }
    return out


def _judge_tilt(date: str) -> dict[str, float]:
    path = NEWS_DIR / f"{date}_judge.json"
    if not path.is_file():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    tickers = raw.get("tickers") or {}
    out = {}
    if isinstance(tickers, dict):
        for k, v in tickers.items():
            try:
                out[fm._tick(k)] = float(v)
            except (TypeError, ValueError):
                continue
    return out


def _load_flatten() -> tuple[dict, dict]:
    if not FLATTEN_JSON.is_file():
        return {}, {}
    try:
        raw = json.loads(FLATTEN_JSON.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}, {}
    rows = {}
    for r in raw.get("rows") or []:
        key = (r.get("date"), fm._tick(r.get("ticker")))
        if key[0] and key[1]:
            rows[key] = r
    mornings = {}
    for d in raw.get("daily") or []:
        date = d.get("date")
        if not date:
            continue
        mornings[date] = {
            "s": d.get("score"),
            "hard_red": bool(d.get("hard_red")),
            "flatten_ok": bool(d.get("flatten_ok")),
            "route": d.get("route"),
            "why": d.get("why"),
            "file": "03_scoreboard/flatten_lookback_action.json",
        }
    return rows, mornings


def _news_blob(row: dict, finviz: dict, digest: dict, judge: dict) -> dict:
    t = fm._tick(row.get("ticker"))
    prior = row.get("news_export_date") or row.get("prior_date")
    fv = finviz.get(t) or {}
    dg = digest.get(t) or {}
    title = fv.get("title") or dg.get("title") or ""
    digest_txt = fv.get("digest") or dg.get("digest") or ""
    file = fv.get("file") or dg.get("file") or "data/factor_mine/panel.json"
    tilt = judge.get(t)
    tone = fm._tone(row.get("boxes"), "news")
    return {
        "tone": tone,
        "box": row.get("news_box") or "missing",
        "prior": row.get("news_prior") or "missing",
        "title": title,
        "digest": digest_txt,
        "when": fv.get("when") or prior,
        "file": file,
        "retrieved": prior,
        "judge": tilt,
        "url": fv.get("url") or "",
    }


def _n_neg(boxes: dict, domains: dict, alarm: bool) -> int:
    n = sum(1 for v in boxes.values() if v == "bad")
    n += sum(1 for v in domains.values() if v == "bad")
    if alarm:
        n += 1
    return n


def _card(row: dict, flat: dict | None, news: dict) -> dict:
    boxes = {k: fm._tone(row.get("boxes"), k) for k, _ in CAM_LABS}
    if flat:
        for k, _ in CAM_LABS:
            got = (flat.get("boxes") or {}).get(k)
            if got and (not boxes.get(k) or boxes[k] == "missing"):
                boxes[k] = str(got).lower()
        for k, v in (flat.get("boxes") or {}).items():
            if k not in boxes:
                boxes[k] = str(v or "missing").lower()
    domains = {}
    if flat:
        domains = {k: str((flat.get("domains") or {}).get(k) or "missing").lower()
                   for k, _ in COACH_LABS}
    marks = (flat or {}).get("marks") or {}
    alarm = bool(row.get("alarm") or marks.get("alarm"))
    blue = bool(row.get("blue") or marks.get("blue"))
    white = bool(row.get("zero_red") if row.get("zero_red") is not None
                 else marks.get("white"))
    srcs = ["data/factor_mine/panel.json"]
    if flat:
        srcs.append("03_scoreboard/flatten_lookback_action.json")
    if news.get("file") and news["file"] not in srcs:
        srcs.append(news["file"])
    setups = flat.get("setups") if flat else None
    if isinstance(setups, list):
        setups = "; ".join(str(x) for x in setups[:4]) if setups else ""
    return {
        "on_list": True,
        "sources": row.get("sources") or [],
        "src_rank": row.get("src_rank"),
        "boxes": boxes,
        "domains": domains or None,
        "blue": blue,
        "alarm": alarm,
        "white": white,
        "last_green": bool(row.get("last_green") or (flat or {}).get("candle_last_green")),
        "last_red": bool(row.get("last_red")),
        "candle": (flat or {}).get("candle_pattern") or (
            f"score {row.get('candle_score'):.2f}" if fm._finite(row.get("candle_score")) is not None else ""),
        "candle_capture": bool(row.get("candle_capture") or (flat or {}).get("candle_capture")),
        "ret_5": _round(row.get("ohlc_ret_5"), 2),
        "rvol": _round(row.get("ohlc_rvol"), 2),
        "hot": _round(row.get("ohlc_hot_score"), 2),
        "nr7": bool(row.get("ohlc_nr7") or (flat or {}).get("ohlc_nr7")),
        "break_10": bool(row.get("ohlc_break_10") or (flat or {}).get("ohlc_break_10")),
        "earn_react": bool(row.get("erd_earn_react") or (flat or {}).get("erd_earn_react")),
        "erd": (flat or {}).get("erd_cell") or "",
        "days_E": row.get("erd_days_since_E"),
        "days_R": row.get("erd_days_since_R"),
        "flag_E": row.get("erd_flag_E"),
        "flag_R": row.get("erd_flag_R"),
        "e_pol": row.get("e_pol") or "missing",
        "e_label": row.get("e_label") or "",
        "r_pol": row.get("r_pol") or "missing",
        "r_label": row.get("r_label") or "",
        "news": news,
        "cond_good": int(row.get("cond_good") or 0),
        "cond_bad": int(row.get("cond_bad") or 0),
        "n_neg": _n_neg(boxes, domains, alarm),
        "action": (flat or {}).get("action_call") or "",
        "action_why": (flat or {}).get("action_reason") or "",
        "setups": setups or "",
        "flatten_ok": bool((flat or {}).get("flatten_ok")) if flat else None,
        "open": _round(row.get("open"), 2),
        "files": srcs,
    }


def _flat_only_card(flat: dict, news: dict) -> dict:
    boxes = {k: str((flat.get("boxes") or {}).get(k) or "missing").lower()
             for k, _ in CAM_LABS}
    domains = {k: str((flat.get("domains") or {}).get(k) or "missing").lower()
               for k, _ in COACH_LABS}
    marks = flat.get("marks") or {}
    alarm = bool(marks.get("alarm"))
    return {
        "on_list": False,
        "sources": flat.get("sources") or [],
        "boxes": boxes,
        "domains": domains,
        "blue": bool(marks.get("blue")),
        "alarm": alarm,
        "white": bool(marks.get("white")),
        "last_green": bool(flat.get("candle_last_green")),
        "last_red": False,
        "candle": flat.get("candle_pattern") or "",
        "candle_capture": bool(flat.get("candle_capture")),
        "ret_5": _round(flat.get("ohlc_ret_5"), 2),
        "rvol": _round(flat.get("ohlc_rvol"), 2),
        "hot": _round(flat.get("ohlc_hot_score"), 2),
        "nr7": bool(flat.get("ohlc_nr7")),
        "break_10": bool(flat.get("ohlc_break_10")),
        "earn_react": bool(flat.get("erd_earn_react")),
        "erd": flat.get("erd_cell") or "",
        "flag_E": flat.get("erd_flag_E"),
        "flag_R": flat.get("erd_flag_R"),
        "e_pol": flat.get("e_pol") or "missing",
        "e_label": flat.get("e_label") or "",
        "r_pol": flat.get("r_pol") or "missing",
        "r_label": flat.get("r_label") or "",
        "news": news,
        "cond_good": int((flat.get("condition") or {}).get("good") or 0),
        "cond_bad": int((flat.get("condition") or {}).get("bad") or 0),
        "n_neg": _n_neg(boxes, domains, alarm),
        "action": flat.get("action_call") or "",
        "action_why": flat.get("action_reason") or "",
        "setups": "",
        "flatten_ok": bool(flat.get("flatten_ok")),
        "files": ["03_scoreboard/flatten_lookback_action.json"],
    }


def build_mornings() -> dict:
    _, mornings = _load_flatten()
    return mornings


def build_probe(panel: dict) -> dict:
    """date → ticker → investigator card."""
    flat_rows, _mornings = _load_flatten()
    finviz_cache: dict[str, dict] = {}
    digest_cache: dict[str, dict] = {}
    judge_cache: dict[str, dict] = {}
    probe: dict[str, dict] = {}

    def news_for(date: str, row: dict) -> dict:
        prior = row.get("news_export_date") or row.get("prior_date") or ""
        if prior not in finviz_cache:
            finviz_cache[prior] = _finviz_news(prior)
        if date not in digest_cache:
            digest_cache[date] = _digest_map(date)
            if prior:
                # Morning digest on D often reprints last night's export.
                extra = _digest_map(prior)
                for k, v in extra.items():
                    digest_cache[date].setdefault(k, v)
        if date not in judge_cache:
            judge_cache[date] = _judge_tilt(date)
        return _news_blob(row, finviz_cache.get(prior) or {},
                          digest_cache.get(date) or {},
                          judge_cache.get(date) or {})

    attach_erd_polarity(panel)
    for row in panel.get("rows") or []:
        date = row.get("date")
        ticker = fm._tick(row.get("ticker"))
        if not date or not ticker:
            continue
        news = news_for(date, row)
        card = _card(row, flat_rows.get((date, ticker)), news)
        probe.setdefault(date, {})[ticker] = card

    for (date, ticker), flat in flat_rows.items():
        if ticker in (probe.get(date) or {}):
            continue
        news = news_for(date, {
            "ticker": ticker, "date": date,
            "boxes": flat.get("boxes") or {},
            "news_export_date": None, "prior_date": None,
            "news_box": fm._tone(flat.get("boxes"), "news"),
            "news_prior": "missing",
        })
        probe.setdefault(date, {})[ticker] = _flat_only_card(flat, news)
    return probe


def slim_probe(probe: dict, extra_tickers: set[str] | None = None) -> dict:
    """Keep shopping-list cards plus flatten rows for names a sleeve bought."""
    extra = {fm._tick(t) for t in (extra_tickers or set()) if fm._tick(t)}
    out: dict[str, dict] = {}
    for date, m in (probe or {}).items():
        kept = {t: c for t, c in (m or {}).items()
                if (c or {}).get("on_list") or t in extra}
        if kept:
            out[date] = kept
    return out


def probe_meta() -> dict:
    return {
        "cam_labs": [list(x) for x in CAM_LABS],
        "coach_labs": [list(x) for x in COACH_LABS],
        "cam_file": dict(CAM_FILE),
        "coach_file": dict(COACH_FILE),
        "tone_word": dict(TONE_WORD),
    }
