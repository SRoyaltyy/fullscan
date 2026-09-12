"""Stock investigator cards — what a 09:30 sleeve actually saw.

Quotes cameras / coaches / news / tape from repo files only
(panel, flatten lookback, session-morning Finviz export, morning digest).
Never uses same-day Change% / Gap / RelVol to color a cell.

E/R polarity reads EPS Surprise from ``finviz_{session}.csv`` — the
overnight packet named for that 09:30 — so a yday-AMC beat is visible.
A date-only stamp with no surprise number stays unknown, not green.
Today's AMC (print after 09:30) is not used.
"""
from __future__ import annotations

import json
import math
from pathlib import Path

from . import factor_mine as fm
from . import factor_mine_book as fmb
from . import gainer_asof as ga
from . import sleeve_merge as sm
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
    "news": "01_daily/news actions / morning Finviz News Title (News Time < 09:30)",
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


def surprise_polarity(v, src: str = "morning export") -> tuple[str, str]:
    """EPS surprise → tone. Date-only green is not a beat."""
    x = _parse_num(v)
    if x is None:
        return "missing", f"no EPS surprise on the {src}"
    if x > 0.5:
        return "good", f"beat · EPS surprise {x:+.1f}% ({src})"
    if x < -0.5:
        return "bad", f"miss · EPS surprise {x:+.1f}% ({src})"
    return "neutral", f"inline · EPS surprise {x:+.1f}% ({src})"


def recom_polarity(v, src: str = "morning export") -> tuple[str, str]:
    """Finviz Analyst Recom (1=strong buy … 5=sell). Level, not a change."""
    x = _parse_num(v)
    if x is None:
        return "missing", f"no analyst recom on the {src}"
    if x <= 2.0:
        return "good", f"buy-side recom {x:.1f} (level, not a change · {src})"
    if x >= 3.5:
        return "bad", f"sell-side recom {x:.1f} (level, not a change · {src})"
    return "neutral", f"hold-ish recom {x:.1f} (level, not a change · {src})"


_MEM_SURP = {
    "big_beat": ("good", "big beat vs consensus (morning membership)"),
    "beat": ("good", "beat vs consensus (morning membership)"),
    "inline": ("neutral", "inline vs consensus (morning membership)"),
    "miss": ("bad", "miss vs consensus (morning membership)"),
    "big_miss": ("bad", "big miss vs consensus (morning membership)"),
}


def polarity_export_date(row: dict) -> str | None:
    """Finviz file that can hold a yday-AMC / today-BMO surprise.

    ``finviz_{session}.csv`` is the overnight packet named for that 09:30.
    The prior-session export is too early for last night's AMC print — that
    was why INO's +67.7% surprise never reached the investigator after #135.
    """
    session = str(row.get("date") or "")[:10]
    prior = row.get("news_export_date") or row.get("prior_date")
    if session and (EXPORT_DIR / f"finviz_{session}.csv").is_file():
        return session
    if prior:
        return str(prior)[:10]
    return None


def surprise_is_knowable(fv: dict | None, session: str | None) -> bool:
    """True when the export's last EPS surprise is already public at 09:30.

    Yday AMC and today BMO are in. Today's AMC (hour > 09:30) is out — a
    later overwrite of the same file must not leak the afternoon print.
    """
    from . import finviz_events as fe
    session = str(session or "")[:10]
    if not session:
        return False
    ed, hm = fe.parse_finviz_datetime((fv or {}).get("Earnings Date"))
    if not ed:
        return True
    if ed < session:
        return True
    if ed == session and (hm is None or int(hm) <= 930):
        return True
    return False


def news_is_pre_open(when, session: str | None) -> bool:
    """News Time is on or before the session and strictly before 09:30 ET."""
    from . import finviz_events as fe
    session = str(session or "")[:10]
    ed, hm = fe.parse_finviz_datetime(when)
    if not ed or not session:
        return False
    if ed < session:
        return True
    if ed == session and (hm is None or int(hm) < 930):
        return True
    return False


def erd_polarity(row: dict, fv: dict | None = None, *,
                 src: str = "morning export",
                 mem_surp: str | None = None) -> dict:
    """Honest E / R polarity. Never paint green just because a date exists."""
    fv = fv or {}
    surp_pol, surp_lab = surprise_polarity(fv.get("EPS Surprise"), src)
    if surp_pol == "missing" and mem_surp in _MEM_SURP:
        surp_pol, surp_lab = _MEM_SURP[mem_surp]
    rec_pol, rec_lab = recom_polarity(fv.get("Analyst Recom"), src)
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
        e_pol, e_label = "missing", f"no earnings date on the {src}"
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
_MEM_CACHE: dict[str, dict] = {}


def _prior_finviz_map(date: str | None) -> dict[str, dict]:
    if not date:
        return {}
    if date in _FV_CACHE:
        return _FV_CACHE[date]
    df = ga.load_finviz(date)
    out: dict[str, dict] = {}
    if df is not None and not getattr(df, "empty", True) and "Ticker" in df.columns:
        keep = [c for c in (
            "Ticker", "EPS Surprise", "Analyst Recom", "News Title",
            "News Time", "Earnings Date",
        ) if c in df.columns]
        for rec in df[keep].to_dict("records"):
            t = fm._tick(rec.get("Ticker"))
            if t:
                out[t] = rec
    _FV_CACHE[date] = out
    return out


def _membership_surp(date: str | None, ticker: str) -> str | None:
    if not date or not ticker:
        return None
    if date not in _MEM_CACHE:
        path = ROOT / "data" / "universe" / f"{date}_membership.csv"
        got: dict[str, str] = {}
        if path.is_file():
            try:
                import csv
                with path.open(encoding="utf-8") as fh:
                    for rec in csv.DictReader(fh):
                        t = fm._tick(rec.get("Ticker"))
                        if t:
                            got[t] = str(rec.get("earnsurp") or "").strip()
            except OSError:
                got = {}
        _MEM_CACHE[date] = got
    return _MEM_CACHE[date].get(fm._tick(ticker)) or None


def attach_erd_polarity(panel: dict) -> dict:
    """Stamp E/R polarity from the session-morning Finviz export."""
    for row in panel.get("rows") or []:
        session = str(row.get("date") or "")[:10]
        src = polarity_export_date(row)
        src_lab = (
            "morning export" if src and src == session else "prior export"
        )
        fv = dict(_prior_finviz_map(src).get(fm._tick(row.get("ticker"))) or {})
        if fv and not surprise_is_knowable(fv, session):
            fv.pop("EPS Surprise", None)
        mem = _membership_surp(src or session, fm._tick(row.get("ticker")))
        if mem and not surprise_is_knowable(fv, session) and src == session:
            mem = None
        row.update(erd_polarity(row, fv, src=src_lab, mem_surp=mem))
        title = ""
        if news_is_pre_open(fv.get("News Time"), session):
            title = str(fv.get("News Title") or "")
        row["headline"] = title[:160]
        row["headline_tone"] = fm.prior_news_tone(title)
        row["burst"] = fm.is_burst(row)
        if "n_neg" not in row:
            row["n_neg"] = fm.n_neg(row)
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
        "headline": row.get("headline") or "",
        "headline_tone": row.get("headline_tone") or "missing",
        "burst": bool(row.get("burst")),
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


def _morning_from_live_s(date: str) -> dict | None:
    """S / hard-red from weather or predict when flatten lookback lagged."""
    score = sm.weather_score(date)
    src = f"01_daily/weather/{date}_weather.json" if score is not None else ""
    if score is None:
        _direction, score = sm.predict_snapshot(date)
        if score is not None:
            src = f"01_daily/general/{date}_predict.md"
    if score is None:
        return None
    s = float(score)
    return {
        "s": s,
        "hard_red": bool(s <= fmb.HARD_RED),
        "flatten_ok": None,
        "route": None,
        "why": "weather / predict S after flatten lookback",
        "file": src,
    }


def _session_morning_dates(existing: dict) -> list[str]:
    dates = set(existing)
    weather_dir = ROOT / "01_daily" / "weather"
    if weather_dir.is_dir():
        for p in weather_dir.glob("*_weather.json"):
            day = p.name[:10]
            if len(day) == 10 and day[4] == "-":
                dates.add(day)
    pred_dir = ROOT / "01_daily" / "general"
    if pred_dir.is_dir():
        for p in pred_dir.glob("*_predict.md"):
            day = p.name[:10]
            if len(day) == 10 and day[4] == "-":
                dates.add(day)
    try:
        dates.update(sm.session_calendar(sm.load_payload(), sm.list_books()))
    except Exception:
        pass
    return sorted(d for d in dates if d)


def build_mornings() -> dict:
    """Per-session S for cash-start / investigator, including days after
    flatten lookback stopped (that JSON is workflow_dispatch only)."""
    _, mornings = _load_flatten()
    out = {}
    for date in _session_morning_dates(mornings):
        row = dict(mornings.get(date) or {})
        if row.get("s") is None:
            extra = _morning_from_live_s(date)
            if extra:
                row = {**extra, **{k: v for k, v in row.items() if v is not None}}
        elif row.get("hard_red") is None:
            try:
                row["hard_red"] = bool(float(row["s"]) <= fmb.HARD_RED)
            except (TypeError, ValueError):
                pass
        if row.get("s") is not None or row.get("file"):
            out[date] = row
    return out


def build_probe(panel: dict) -> dict:
    """date → ticker → investigator card."""
    flat_rows, _mornings = _load_flatten()
    finviz_cache: dict[str, dict] = {}
    digest_cache: dict[str, dict] = {}
    judge_cache: dict[str, dict] = {}
    probe: dict[str, dict] = {}

    def news_for(date: str, row: dict) -> dict:
        prior = row.get("news_export_date") or row.get("prior_date") or ""
        morning = date or ""
        if morning and morning not in finviz_cache:
            finviz_cache[morning] = _finviz_news(morning)
        if prior and prior not in finviz_cache:
            finviz_cache[prior] = _finviz_news(prior)
        if date not in digest_cache:
            digest_cache[date] = _digest_map(date)
            if prior:
                # Morning digest on D often reprints last night's export.
                extra = _digest_map(prior)
                for k, v in extra.items():
                    digest_cache[date].setdefault(k, v)
            if morning:
                extra_m = _digest_map(morning)
                for k, v in extra_m.items():
                    digest_cache[date].setdefault(k, v)
        if date not in judge_cache:
            judge_cache[date] = _judge_tilt(date)
        t = fm._tick(row.get("ticker"))
        morn_hit = (finviz_cache.get(morning) or {}).get(t) or {}
        prior_hit = (finviz_cache.get(prior) or {}).get(t) or {}
        use_morning = bool(morn_hit) and news_is_pre_open(
            morn_hit.get("when"), date)
        fv_one = {t: morn_hit if use_morning else prior_hit} if t else {}
        blob = _news_blob(row, fv_one,
                          digest_cache.get(date) or {},
                          judge_cache.get(date) or {})
        blob["retrieved"] = morning if use_morning else prior
        return blob

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
