"""Hierarchical, traceable 1-day stock-book decisions.

The legacy ranker blends unrelated evidence into one score.  This module
keeps the useful scores, but changes *permission* to trade into a lattice:

    market -> parent sector -> child industry/theme -> company -> setup/flow

Each evaluator owns one question and emits a red/yellow/green verdict with
reasons.  The verdicts route a ticker into a standard, group-leader, direct
catalyst, or blocked lane.  Numeric scores only rank names *inside* a lane.

This is deliberately a 1-day decision layer first.  Longer-horizon books
continue to use the existing ranker until their own market/group thresholds
have enough realized observations.
"""
from __future__ import annotations

import json
import math
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from . import compute_scores, scoreboard

ROOT = Path(__file__).resolve().parent.parent
NEWS_DIR = ROOT / "01_daily" / "news"
HEAT_DIR = ROOT / "01_daily" / "map_heat"
CATALYST_DIR = ROOT / "01_daily" / "catalyst"
WEATHER_DIR = ROOT / "01_daily" / "weather"
BOOK_DIR = ROOT / "data" / "stock_book"

SCHEMA_VERSION = 2
DOMAIN_KEYS = ("market", "parent", "child", "company", "setup", "flow")
SOURCE_KEYS = (
    "join", "sector", "gen", "news", "digest", "judge",
    "ab", "peer", "heat", "vol", "catal",
)
TONE_POINTS = {"bad": 0, "neutral": 1, "good": 2}

# Initial guardrails.  These are intentionally visible and deterministic;
# walk-forward evaluation may move them, but an LLM cannot do so at runtime.
HARD_MARKET_SCORE = 3.0
HARD_MARKET_BAD_POINTS = 4.0
HARD_MARKET_PILLARS = 3
GROUP_ABS_WEEK = 2.0
GROUP_ABS_DAY = 0.50
GROUP_RESIDUAL = 3.0
DIRECT_EVENT_MIN = 0.65
DIRECT_EVENT_HARD_RED_MIN = 0.70
PRICE_CONFIRM_PCT = 0.50

ETF_TICKERS = {
    "SPY", "QQQ", "DIA", "IWM", "XLE", "XLY", "XLK", "XLF", "XLV",
    "XLI", "XLB", "XLU", "XLRE", "XLC", "GDX", "SMH", "SOXX",
}

POSITIVE_RE = re.compile(
    r"\b(beat|beats|beating|raises?|raised|upgrade[sd]?|approval|approved|"
    r"record|surges?|wins?|won|contract|buyback|reaffirm[sd]?|"
    r"reduces? risk|cut(?:s)? (?:all-cause )?mortality|positive phase)\b",
    re.I,
)
NEGATIVE_RE = re.compile(
    r"\b(miss(?:es|ed)?|weak|downgrade[sd]?|lowers?|lowered|"
    r"cuts? guidance|bankrupt(?:cy)?|offering|dilution|fraud|"
    r"investigat(?:e|es|ion)|warning|plunges?|selloff|recall|"
    r"profit.taking|insider sale|insider sold|ceo .* sold|sold .* stock|"
    r"sells? \$?[\d,.]+ (?:million|billion).*(?:stock|shares))\b",
    re.I,
)
HIGH_MATERIALITY_RE = re.compile(
    r"\b(earnings|revenue|eps|guidance|phase [123]|mortality|fda|"
    r"approval|clinical|merger|acqui(?:re|res|sition)|contract|"
    r"buyback|bankrupt(?:cy)?|offering|fraud|trial|primary endpoint|"
    r"raises? (?:fy|full.year|outlook))\b",
    re.I,
)
SYMPATHY_RE = re.compile(
    r"\b(sympathy|rally (?:lifting|lifts)|peer results? (?:lift|pressure)|"
    r"sector rally (?:lifting|lifts))\b",
    re.I,
)
EVENT_RISK_RE = re.compile(
    r"\b(reports? (?:after|before)|earnings (?:due|expected|on)|"
    r"scheduled to report|will report)\b",
    re.I,
)


def _read_json(path: Path) -> dict:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError):
        return {}
    return data if isinstance(data, dict) else {}


def _num(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return default if math.isnan(out) else out


def _tick(value: Any) -> str:
    return str(value or "").strip().upper()


def _tone(value: Any, eps: float = 0.05) -> str:
    if value is None:
        return "missing"
    try:
        v = float(value)
    except (TypeError, ValueError):
        return "missing"
    if math.isnan(v):
        return "missing"
    if v >= eps:
        return "good"
    if v <= -eps:
        return "bad"
    return "neutral"


def _signed_tone(direction: str) -> str:
    d = str(direction or "").lower()
    if d in ("up", "bullish", "positive", "good"):
        return "good"
    if d in ("down", "bearish", "hawkish", "negative", "bad"):
        return "bad"
    return "neutral"


def _tone_label(tone: str) -> str:
    return {"good": "GREEN", "bad": "RED", "neutral": "YELLOW",
            "missing": "MISSING"}.get(str(tone), str(tone).upper())


def _runs_for_date(date: str) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for row in scoreboard.load().get("runs") or []:
        if row.get("date") != date or not row.get("predicted_direction"):
            continue
        out[str(row.get("topic") or "")] = row
    return out


def _load_weather(date: str) -> dict:
    return _read_json(WEATHER_DIR / f"{date}_weather.json")


def evaluate_market(
    date: str,
    general_run: dict | None = None,
    weather: dict | None = None,
) -> dict:
    """Evaluate broad-market permission from the *raw* factor scoreboard.

    Accuracy affects confidence later; it no longer dilutes an extreme,
    corroborated market warning into an 8% ticker-level contribution.
    """
    if general_run is None:
        general_run = _runs_for_date(date).get("general") or {}
    weather = weather if isinstance(weather, dict) else _load_weather(date)
    sig = weather.get("signals") if isinstance(weather.get("signals"), dict) else {}

    components = general_run.get("components") or {}
    clean = {
        key: _num(components.get(key))
        for key in compute_scores.WEIGHTS
    }
    contributions = {
        key: clean[key] * compute_scores.WEIGHTS[key]
        for key in compute_scores.WEIGHTS
    }
    good_points = sum(v for v in contributions.values() if v > 0)
    bad_points = sum(v for v in contributions.values() if v < 0)
    pillars = {
        "global sessions": (
            contributions.get("B0_ASIA", 0.0)
            + contributions.get("B0_EUROPE", 0.0)
        ),
        "overnight catalysts": contributions.get("B1_CATALYSTS", 0.0),
        "rates / Fed": (
            contributions.get("B2_BONDS", 0.0)
            + contributions.get("B3_FEDPATH", 0.0)
        ),
        "volatility": contributions.get("B4_VIX", 0.0),
        "sentiment": contributions.get("B5_SENTIMENT", 0.0),
        "futures": contributions.get("B6_FUTURES", 0.0),
        "oil / dollar": contributions.get("B7_OIL_DOLLAR", 0.0),
    }
    red_pillars = [k for k, v in pillars.items() if v <= -0.20]
    green_pillars = [k for k, v in pillars.items() if v >= 0.20]

    direction = str(
        general_run.get("predicted_direction")
        or sig.get("general_direction")
        or "flat"
    ).lower()
    total = _num(
        general_run.get("total_score"),
        _num(sig.get("general_score")),
    )
    confidence = _num(
        general_run.get("confidence_score"),
        _num(sig.get("general_confidence"), 0.5),
    )
    risk = str(sig.get("risk") or weather.get("risk") or "unknown").lower()

    hard_red = (
        direction == "down"
        and total <= -HARD_MARKET_SCORE
        and bad_points <= -HARD_MARKET_BAD_POINTS
        and risk == "off"
        and len(red_pillars) >= HARD_MARKET_PILLARS
    )
    hard_green = (
        direction == "up"
        and total >= HARD_MARKET_SCORE
        and good_points >= HARD_MARKET_BAD_POINTS
        and risk == "on"
        and len(green_pillars) >= HARD_MARKET_PILLARS
    )
    if hard_red:
        state, tone = "hard_red", "bad"
        # Still publish the best longs the lattice + lookback can clock.
        # Size stays quartered; paper should not treat these as full-risk.
        max_longs, scale = 10, 0.25
        allowed = ["catalyst_exception", "probable"]
    elif hard_green:
        state, tone = "green", "good"
        max_longs, scale = 15, 1.0
        allowed = ["standard", "group_leader", "catalyst"]
    elif direction == "down" and (risk == "off" or total <= -1.0):
        state, tone = "red", "bad"
        max_longs, scale = 8, 0.35
        allowed = ["group_leader", "catalyst", "probable"]
    elif direction == "up" and risk != "off":
        state, tone = "green", "good"
        max_longs, scale = 15, 1.0
        allowed = ["standard", "group_leader", "catalyst"]
    else:
        state, tone = "yellow", "neutral"
        max_longs, scale = 8, 0.60
        allowed = ["standard", "group_leader", "catalyst"]

    bull_reasons = [
        f"{name} {value:+.2f} points"
        for name, value in sorted(pillars.items(), key=lambda x: -x[1])
        if value >= 0.20
    ]
    bear_reasons = [
        f"{name} {value:+.2f} points"
        for name, value in sorted(pillars.items(), key=lambda x: x[1])
        if value <= -0.20
    ]
    rationale = (
        f"{state.upper()}: general {direction} score={total:+.2f}; "
        f"good={good_points:+.1f} vs bad={bad_points:+.1f}; "
        f"risk={risk}; red pillars={len(red_pillars)}"
    )
    return {
        "date": date,
        "state": state,
        "tone": tone,
        "direction": direction,
        "score": round(total, 3),
        "confidence": round(confidence, 3),
        "risk": risk,
        "good_points": round(good_points, 3),
        "bad_points": round(bad_points, 3),
        "pillars": {k: round(v, 3) for k, v in pillars.items()},
        "red_pillars": red_pillars,
        "green_pillars": green_pillars,
        "bull_reasons": bull_reasons,
        "bear_reasons": bear_reasons,
        "allowed_lanes": allowed,
        "max_long_slots": max_longs,
        "position_scale": scale,
        "rationale": rationale,
    }


def _heat_context(date: str) -> dict:
    data = _read_json(HEAT_DIR / f"{date}_map_heat.json")
    sectors = {
        str(r.get("sector") or ""): r
        for r in data.get("sectors") or []
        if isinstance(r, dict) and r.get("sector")
    }
    industries = {
        str(r.get("industry") or ""): r
        for r in data.get("industries") or []
        if isinstance(r, dict) and r.get("industry")
    }
    themes_by_industry: dict[str, list[dict]] = {}
    for theme in data.get("themes") or []:
        if not isinstance(theme, dict):
            continue
        for sub in theme.get("subthemes") or []:
            if not isinstance(sub, dict):
                continue
            record = {
                "theme": theme.get("theme"),
                "label": sub.get("label"),
                "d1": sub.get("d1"),
                "w1": sub.get("w1"),
                "parent_w1": sub.get("parent_w1"),
                "vs_parent_w1": sub.get("vs_parent_w1"),
                "agree": sub.get("agree"),
            }
            for industry in sub.get("industries") or []:
                themes_by_industry.setdefault(str(industry), []).append(record)
    generated = str(data.get("overlay_at") or data.get("generated_at") or "")
    premarket = False
    try:
        premarket = datetime.fromisoformat(generated).hour < 9
    except ValueError:
        pass
    return {
        "raw": data,
        "sectors": sectors,
        "industries": industries,
        "themes_by_industry": themes_by_industry,
        "premarket": premarket,
    }


def _digest_tone(text: str) -> str:
    value = str(text or "")
    if not value.strip() or EVENT_RISK_RE.search(value):
        return "neutral"
    if re.search(
        r"\b(insider|ceo|cfo|director)\b.*\b(sold|sells|sale)\b",
        value, re.I,
    ):
        return "bad"
    try:
        from .finviz_news import headline_tone
        tone = headline_tone(value)
        return tone if tone != "missing" else "neutral"
    except Exception:
        pos = len(POSITIVE_RE.findall(value))
        neg = len(NEGATIVE_RE.findall(value))
        if pos > neg:
            return "good"
        if neg > pos:
            return "bad"
        return "neutral"


def _digest_context(date: str) -> dict[str, dict]:
    """Company Finviz news from the full Elite export, not the ranked sample.

    The digest JSON only kept ~80 names, so the RYG news/digest boxes were
    missing for almost every liquid ticker that actually had a headline.
    """
    out: dict[str, dict] = {}
    try:
        from .finviz_news import load_company_news
        export_rows = load_company_news(date, today=date)
    except Exception:
        export_rows = {}
    for ticker, rec in export_rows.items():
        if rec.get("is_dividend"):
            continue
        text = str(rec.get("text") or "").strip()
        if not text:
            continue
        event_risk = bool(EVENT_RISK_RE.search(text))
        out[ticker] = {
            "tone": rec.get("digest_tone") or rec.get("tone") or _digest_tone(text),
            "news_tone": rec.get("news_tone") or rec.get("tone") or "neutral",
            "text": text[:240],
            "materiality": rec.get("materiality") or "normal",
            "direct": (not bool(SYMPATHY_RE.search(text))) and not event_risk,
            "event_risk": event_risk,
            "event_date": rec.get("event_date"),
            "sector": rec.get("sector"),
            "industry": rec.get("industry"),
        }
    if out:
        return out

    data = _read_json(NEWS_DIR / f"{date}_finviz_digest.json")
    rows: list[dict] = []
    for key in ("top_signal", "all_ticker_digests",
                "all_ticker_digests_sample"):
        rows.extend(r for r in data.get(key) or [] if isinstance(r, dict))
    for sec_rows in (data.get("by_sector") or {}).values():
        rows.extend(r for r in sec_rows or [] if isinstance(r, dict))
    for row in rows:
        ticker = _tick(row.get("ticker"))
        if not ticker or ticker in out or ticker in ETF_TICKERS:
            continue
        text = str(row.get("digest") or row.get("news_title") or "").strip()
        if not text or row.get("is_dividend"):
            continue
        tone = _digest_tone(text)
        high = bool(HIGH_MATERIALITY_RE.search(text))
        direct = not bool(SYMPATHY_RE.search(text))
        event_risk = bool(EVENT_RISK_RE.search(text))
        out[ticker] = {
            "tone": tone,
            "news_tone": tone,
            "text": text[:240],
            "materiality": "high" if high else "normal",
            "direct": direct and not event_risk,
            "event_risk": event_risk,
            "sector": row.get("sector"),
            "industry": row.get("industry"),
        }
    return out


def _action_context(date: str) -> tuple[dict[str, dict], dict]:
    data = _read_json(NEWS_DIR / f"{date}_actions.json")
    tickers = {}
    for row in data.get("ticker_actions") or []:
        if not isinstance(row, dict):
            continue
        ticker = _tick(row.get("ticker"))
        if ticker:
            tickers[ticker] = row
    return tickers, data


def _judge_context(date: str) -> tuple[dict[str, str], dict[str, str], dict]:
    data = _read_json(NEWS_DIR / f"{date}_judge.json")
    if not data:
        try:
            from .judge_apply import load_or_parse
            data = load_or_parse(date)
        except Exception:
            data = {}
    # JSON may have been generated by an older parser with a smaller company
    # dictionary.  Parse the markdown in-memory as well so explicit Judge
    # mentions (Salesforce/Amgen/etc.) reach the company domain today.  This
    # is adjudication metadata only; s_news still consumes the action once.
    parsed_md: dict = {}
    md_path = NEWS_DIR / f"{date}_judge.md"
    if md_path.exists():
        try:
            from .judge_apply import parse_judge_md
            parsed_md = parse_judge_md(
                md_path.read_text(encoding="utf-8", errors="ignore")
            )
        except Exception:
            parsed_md = {}
    merged_tickers = dict(data.get("tickers") or {})
    for ticker, value in (parsed_md.get("tickers") or {}).items():
        merged_tickers.setdefault(ticker, value)
    merged_sectors = dict(data.get("sector_tilts") or {})
    for sector, value in (parsed_md.get("sector_tilts") or {}).items():
        merged_sectors.setdefault(sector, value)
    tickers = {
        _tick(t): _tone(v)
        for t, v in merged_tickers.items()
        if _tick(t)
    }
    sectors = {
        str(sec): _signed_tone(value)
        for sec, value in merged_sectors.items()
    }
    return tickers, sectors, data


def _catalyst_context(date: str) -> dict[str, dict]:
    data = _read_json(CATALYST_DIR / f"{date}_dossiers.json")
    out: dict[str, dict] = {}
    try:
        from .catalyst_daily import usable_dossier
    except Exception:
        usable_dossier = lambda row: bool(  # noqa: E731
            row.get("net_signal") and not row.get("error")
        )
    for row in data.get("dossiers") or []:
        if not isinstance(row, dict):
            continue
        ticker = _tick(row.get("ticker"))
        if not ticker:
            continue
        usable = bool(usable_dossier(row))
        signal = str(row.get("net_signal") or "")
        out[ticker] = {
            "usable": usable,
            "tone": _signed_tone(signal) if usable else "missing",
            "signal": signal,
            "conviction": _num(row.get("conviction"), 0.0),
            "summary": str(row.get("catalyst_stack") or row.get("why") or "")[:240],
            "error": str(row.get("error") or ""),
            "role": row.get("role"),
        }
    return out


def build_context(
    date: str,
    general_run: dict | None = None,
    weather: dict | None = None,
) -> dict:
    actions, action_raw = _action_context(date)
    judge_tickers, judge_sectors, judge_raw = _judge_context(date)
    return {
        "date": date,
        "schema_version": SCHEMA_VERSION,
        "market": evaluate_market(date, general_run, weather),
        "heat": _heat_context(date),
        "actions": actions,
        "action_raw": action_raw,
        "digest": _digest_context(date),
        "judge_tickers": judge_tickers,
        "judge_sectors": judge_sectors,
        "judge_raw": judge_raw,
        "catalysts": _catalyst_context(date),
    }


def _tape_tone(d1: Any, w1: Any) -> tuple[str, str]:
    day, week = _num(d1), _num(w1)
    if (
        (week >= GROUP_ABS_WEEK and day >= 0)
        or (day >= GROUP_ABS_DAY and week >= 0)
    ):
        return "good", f"tape d1={day:+.1f}% w1={week:+.1f}%"
    if (
        (week <= -GROUP_ABS_WEEK and day <= 0)
        or (day <= -GROUP_ABS_DAY and week <= 0)
    ):
        return "bad", f"tape d1={day:+.1f}% w1={week:+.1f}%"
    if day * week < 0 and max(abs(day), abs(week)) >= 1.0:
        return "neutral", f"tape conflict d1={day:+.1f}% w1={week:+.1f}%"
    return "neutral", f"tape mixed d1={day:+.1f}% w1={week:+.1f}%"


def _parent_eval(row: pd.Series, context: dict) -> dict:
    sector = str(row.get("sector") or "")
    tape = (context.get("heat") or {}).get("sectors", {}).get(sector) or {}
    tape_tone, tape_reason = _tape_tone(tape.get("d1"), tape.get("w1"))
    essay_value = _num(
        row.get("s_sector_essay"), _num(row.get("s_sector"))
    )
    essay_tone = _tone(essay_value, 0.15)
    conflict = (
        tape_tone in ("good", "bad")
        and essay_tone in ("good", "bad")
        and tape_tone != essay_tone
    )
    if conflict:
        tone = "neutral"
    elif tape_tone in ("good", "bad"):
        tone = tape_tone
    elif essay_tone in ("good", "bad"):
        tone = essay_tone
    else:
        tone = "neutral"
    judge = (context.get("judge_sectors") or {}).get(sector)
    reasons = [tape_reason, f"essay={essay_value:+.2f} ({essay_tone})"]
    if conflict:
        reasons.append("measured tape and essay conflict — kept YELLOW")
    if judge:
        reasons.append(f"judge={judge} (context, not a second vote)")
    return {
        "tone": tone,
        "tape_tone": tape_tone,
        "essay_tone": essay_tone,
        "conflict": conflict,
        "d1": _num(tape.get("d1")),
        "w1": _num(tape.get("w1")),
        "breadth": _num(tape.get("breadth")),
        "rvol": _num(tape.get("rvol")),
        "reasons": reasons,
    }


def _child_eval(row: pd.Series, context: dict) -> dict:
    industry = str(row.get("industry") or "")
    heat = context.get("heat") or {}
    data = heat.get("industries", {}).get(industry) or {}
    if not data:
        return {
            "tone": "neutral",
            "absolute_tone": "neutral",
            "relative_tone": "neutral",
            "industry": industry,
            "themes": [],
            "d1": 0.0,
            "w1": 0.0,
            "residual": 0.0,
            "reasons": ["no same-day Finviz industry row — YELLOW"],
        }
    absolute, abs_reason = _tape_tone(data.get("d1"), data.get("w1"))
    residual = _num(data.get("vs_parent_w1"))
    relative = (
        "good" if residual >= GROUP_RESIDUAL
        else "bad" if residual <= -GROUP_RESIDUAL
        else "neutral"
    )
    themes = heat.get("themes_by_industry", {}).get(industry) or []
    reasons = [
        abs_reason,
        f"vs parent={residual:+.1f}% ({relative})",
    ]
    for theme in themes[:2]:
        reasons.append(
            f"{theme.get('theme')}/{theme.get('label')} "
            f"w1={_num(theme.get('w1')):+.1f}% "
            f"vs parent={_num(theme.get('vs_parent_w1')):+.1f}%"
        )
    return {
        "tone": absolute,
        "absolute_tone": absolute,
        "relative_tone": relative,
        "industry": industry,
        "themes": [
            f"{t.get('theme')}/{t.get('label')}" for t in themes if t.get("label")
        ],
        "d1": _num(data.get("d1")),
        "w1": _num(data.get("w1")),
        "residual": residual,
        "breadth": _num(data.get("breadth")),
        "rvol": _num(data.get("rvol")),
        "reasons": reasons,
    }


def _company_eval(row: pd.Series, context: dict) -> dict:
    ticker = _tick(row.get("Ticker"))
    action = (context.get("actions") or {}).get(ticker) or {}
    digest = (context.get("digest") or {}).get(ticker) or {}
    judge_tone = (context.get("judge_tickers") or {}).get(ticker, "missing")
    catalyst = (context.get("catalysts") or {}).get(ticker) or {}

    action_net = _num(action.get("net"))
    action_tone = _tone(action_net, 0.50) if action else "missing"
    digest_tone = str(digest.get("tone") or "missing")
    if action_tone == "missing" and digest:
        # Company Finviz headline is a real print. Neutral = yellow, not blank.
        action_tone = str(digest.get("news_tone") or digest.get("tone") or "neutral")
        if action_tone == "missing":
            action_tone = "neutral"
    catalyst_tone = str(catalyst.get("tone") or "missing")

    tone = "neutral"
    strength = 0.0
    direct = False
    materiality = "none"
    fresh = False
    reasons: list[str] = []
    sources: list[str] = []

    if catalyst.get("usable"):
        tone = catalyst_tone
        direct = True
        materiality = "high"
        strength = max(0.80, min(1.0, _num(catalyst.get("conviction")) / 100.0))
        sources.append("catalyst_dossier")
        fresh = True
        reasons.append(
            f"usable dossier {catalyst.get('signal') or catalyst_tone} "
            f"conv={_num(catalyst.get('conviction')):.0f}"
        )
    elif (
        digest
        and digest_tone in ("good", "bad")
        and digest.get("direct")
    ):
        tone = digest_tone
        direct = True
        materiality = str(digest.get("materiality") or "normal")
        raw_time = str(row.get("news_time") or digest.get("event_date") or "")
        try:
            from .finviz_news import parse_finviz_news_date
            dated = parse_finviz_news_date(
                raw_time, today=str(context.get("date") or ""),
            )
            fresh = bool(dated) and dated == str(context.get("date") or "")
        except Exception:
            try:
                fresh = datetime.fromisoformat(
                    raw_time.replace("Z", "+00:00")
                ).date().isoformat() == str(context.get("date") or "")
            except ValueError:
                fresh = False
        strength = (
            0.72 if materiality == "high" and fresh
            else 0.48 if materiality == "high"
            else 0.42 if fresh
            else 0.30
        )
        sources.append("finviz_digest")
        reasons.append(
            f"direct {materiality} digest "
            f"({'same-day' if fresh else 'stale/undated'}): "
            f"{digest.get('text')}"
        )
        if judge_tone == tone:
            strength = min(1.0, strength + 0.10)
            sources.append("news_judge")
            reasons.append("News Judge confirms the same ticker direction")
        if action_tone == tone:
            strength = min(1.0, strength + 0.06)
            sources.append("news_actions")
            reasons.append("news actions confirm (not double-counted)")
    elif judge_tone in ("good", "bad"):
        tone = "neutral"
        strength = 0.40
        sources.append("news_judge")
        reasons.append(
            f"Judge names ticker {judge_tone}, but no direct material event "
            "is verified — YELLOW"
        )
    elif action_tone in ("good", "bad"):
        tone = "neutral"
        strength = min(0.40, abs(action_net) / 15.0)
        sources.append("news_actions")
        reasons.append(
            f"basket/action net={action_net:+.2f}; context only, not a "
            "company catalyst"
        )
    elif digest:
        reasons.append(f"digest is non-directional: {digest.get('text')}")
    else:
        reasons.append("no direct company event")

    if catalyst and not catalyst.get("usable") and catalyst.get("error"):
        reasons.append(
            f"dossier failed ({catalyst.get('error')}); other evidence retained"
        )

    change = _num(row.get("change_pct"))
    gap = _num(row.get("gap_pct"))
    relvol = _num(row.get("relvol"))
    if tone == "good":
        price_confirm = (
            max(change, gap) >= PRICE_CONFIRM_PCT
            or (relvol >= 1.5 and max(change, gap) >= 0.20)
        )
    elif tone == "bad":
        price_confirm = (
            min(change, gap) <= -PRICE_CONFIRM_PCT
            or (relvol >= 1.5 and min(change, gap) <= -0.20)
        )
    else:
        price_confirm = False
    reasons.append(
        f"price confirmation={'yes' if price_confirm else 'no'} "
        f"(change={change:+.2f}% gap={gap:+.2f}% rvol={relvol:.2f})"
    )
    return {
        "tone": tone if strength >= DIRECT_EVENT_MIN else "neutral",
        "raw_tone": tone,
        "strength": round(strength, 3),
        "direct": bool(direct),
        "materiality": materiality,
        "fresh": bool(fresh),
        "price_confirmed": bool(price_confirm),
        "change_pct": change,
        "gap_pct": gap,
        "relvol": relvol,
        "sources": sorted(set(sources)),
        "reasons": reasons,
        "summary": reasons[0] if reasons else "no company evidence",
        "source_tones": {
            "news": action_tone,
            "digest": digest_tone,
            "judge": judge_tone,
            "catal": catalyst_tone,
        },
    }


def _setup_eval(row: pd.Series) -> dict:
    ab = _num(row.get("s_ab_intrinsic"), _num(row.get("s_ab")))
    mom = str(row.get("mom") or "").lower()
    ext = str(row.get("ext") or "").lower()
    profit = str(row.get("profit") or "").lower()
    surprise = str(row.get("earnsurp") or "").lower()
    structure = 0.0
    structure += {"uptrend": 0.5, "mixed": 0.0, "downtrend": -0.5}.get(
        mom, 0.0
    )
    structure += {
        "washed": 0.25, "neutral": 0.0, "extended": -0.25,
        "extreme": -0.50,
    }.get(ext, 0.0)
    structure += {"yes": 0.25, "no": -0.25, "thin": -0.25}.get(
        profit, 0.0
    )
    structure += {
        "beat": 0.25, "big_beat": 0.50,
        "miss": -0.25, "big_miss": -0.50,
    }.get(surprise, 0.0)
    if ab <= -0.05 or structure <= -0.75:
        tone = "bad"
    elif ab >= 0.10 and structure >= 0.0:
        tone = "good"
    else:
        tone = "neutral"
    strength = float(np.clip(0.5 + ab / 2.0 + structure / 4.0, 0.0, 1.0))
    return {
        "tone": tone,
        "strength": round(strength, 3),
        "reasons": [
            f"intrinsic AB={ab:+.2f}",
            f"intrinsic labels={structure:+.2f} "
            f"(mom={mom or '?'}, ext={ext or '?'}, "
            f"profit={profit or '?'}, earnings={surprise or '?'})",
            "join/weather remains visible but is not counted again here",
        ],
    }


def _flow_eval(row: pd.Series, context: dict) -> dict:
    peer = _num(row.get("s_peer"))
    relvol = _num(row.get("relvol"))
    change = _num(row.get("change_pct"))
    gap = _num(row.get("gap_pct"))
    premarket = bool((context.get("heat") or {}).get("premarket"))

    price_up = max(change, gap) >= PRICE_CONFIRM_PCT
    price_down = min(change, gap) <= -PRICE_CONFIRM_PCT
    dead_after_open = (not premarket) and relvol > 0 and relvol < 0.7
    if dead_after_open or (peer <= -0.05 and price_down):
        tone = "bad"
    elif (
        (peer >= 0.05 and not price_down)
        or (price_up and peer > -0.05)
        or relvol >= 1.5
    ):
        tone = "good"
    else:
        tone = "neutral"
    if relvol <= 0:
        vol_tone = "missing"
    elif premarket:
        vol_tone = "good" if relvol >= 1.5 else "neutral"
    else:
        vol_tone = "good" if relvol >= 1.5 else (
            "bad" if relvol < 0.7 else "neutral"
        )
    strength = float(np.clip(0.5 + peer / 2.0 + (
        0.2 if price_up else -0.2 if price_down else 0.0
    ), 0.0, 1.0))
    return {
        "tone": tone,
        "strength": round(strength, 3),
        "vol_tone": vol_tone,
        "premarket_volume": premarket,
        "reasons": [
            f"peer={peer:+.2f}",
            f"change={change:+.2f}% gap={gap:+.2f}%",
            (
                f"rvol={relvol:.2f} (premarket: low rvol is not a veto)"
                if premarket else f"rvol={relvol:.2f}"
            ),
        ],
    }


def _domain_condition(domains: dict[str, str]) -> tuple[str, str, bool, bool]:
    tones = [domains.get(k) for k in DOMAIN_KEYS]
    good = sum(t == "good" for t in tones)
    bad = sum(t == "bad" for t in tones)
    neutral = sum(t == "neutral" for t in tones)
    if good > bad and good > neutral:
        cond = "good"
    elif bad > good and bad > neutral:
        cond = "bad"
    else:
        cond = "neutral"
    if good - bad >= 2:
        region = "good"
    elif bad - good >= 2:
        region = "bad"
    else:
        region = "neutral"
    printed = [t for t in tones if t in TONE_POINTS]
    full_white = bool(printed) and "bad" not in printed
    name_tones = [domains.get(k) for k in ("company", "setup", "flow")]
    name_white = "bad" not in [t for t in name_tones if t in TONE_POINTS]
    return cond, region, full_white, name_white


def attach_domains(df: pd.DataFrame, context: dict) -> pd.DataFrame:
    """Attach source colors and independent domain verdicts before lookback."""
    if df is None or df.empty:
        return df
    out = df.copy()
    records: list[dict] = []
    market = context.get("market") or {}
    for _, row in out.iterrows():
        parent = _parent_eval(row, context)
        child = _child_eval(row, context)
        company = _company_eval(row, context)
        setup = _setup_eval(row)
        flow = _flow_eval(row, context)
        domains = {
            "market": market.get("tone") or "missing",
            "parent": parent["tone"],
            "child": child["tone"],
            "company": company["tone"],
            "setup": setup["tone"],
            "flow": flow["tone"],
        }
        cond, region, full_white, name_white = _domain_condition(domains)
        source = {
            "join": _tone(row.get("s_join")),
            "sector": parent["tone"],
            "gen": market.get("tone") or "missing",
            "news": company["source_tones"]["news"],
            "digest": company["source_tones"]["digest"],
            "judge": company["source_tones"]["judge"],
            "ab": _tone(row.get("s_ab_intrinsic"), 0.05),
            "peer": _tone(row.get("s_peer")),
            "heat": child["tone"],
            "vol": flow["vol_tone"],
            "catal": company["source_tones"]["catal"],
        }
        group_strength = (
            (1.0 if child["absolute_tone"] == "good" else
             -1.0 if child["absolute_tone"] == "bad" else 0.0)
            + (1.0 if child["relative_tone"] == "good" else
               -1.0 if child["relative_tone"] == "bad" else 0.0)
        ) / 2.0
        records.append({
            **{f"d_{k}": domains[k] for k in DOMAIN_KEYS},
            **{f"src_{k}_tone": source[k] for k in SOURCE_KEYS},
            "domain_cond": cond,
            "domain_region": region,
            "domain_white": full_white,
            "domain_name_white": name_white,
            "parent_conflict": parent["conflict"],
            "parent_d1": parent["d1"],
            "parent_w1": parent["w1"],
            "child_abs_tone": child["absolute_tone"],
            "child_rel_tone": child["relative_tone"],
            "child_d1": child["d1"],
            "child_w1": child["w1"],
            "child_residual": child["residual"],
            "group_label": child["industry"],
            "group_themes": ", ".join(child["themes"]),
            "group_strength": group_strength,
            "company_strength": company["strength"],
            "company_direct": company["direct"],
            "company_materiality": company["materiality"],
            "company_fresh": company["fresh"],
            "company_price_confirmed": company["price_confirmed"],
            "company_summary": company["summary"],
            "company_sources": ",".join(company["sources"]),
            "setup_strength": setup["strength"],
            "flow_strength": flow["strength"],
            "parent_trace": " | ".join(parent["reasons"]),
            "child_trace": " | ".join(child["reasons"]),
            "company_trace": " | ".join(company["reasons"]),
            "setup_trace": " | ".join(setup["reasons"]),
            "flow_trace": " | ".join(flow["reasons"]),
        })
    attached = pd.DataFrame(records, index=out.index)
    for col in attached.columns:
        out[col] = attached[col]
    return out


def _previous_domains(date: str) -> dict[str, dict[str, str]]:
    files = sorted(BOOK_DIR.glob("????-??-??_stock_book.csv"))
    prior = None
    for path in files:
        if path.name[:10] < date:
            prior = path
    if prior is None:
        return {}
    try:
        data = pd.read_csv(prior, low_memory=False)
    except OSError:
        return {}
    if "Ticker" not in data.columns or not all(
        f"d_{key}" in data.columns for key in DOMAIN_KEYS
    ):
        return {}
    result = {}
    for _, row in data.drop_duplicates("Ticker").iterrows():
        result[_tick(row.get("Ticker"))] = {
            key: str(row.get(f"d_{key}") or "missing")
            for key in DOMAIN_KEYS
        }
    return result


def _change_marks(
    domains: dict[str, str], previous: dict[str, str] | None
) -> tuple[bool, bool]:
    if not previous:
        return False, False
    improved = worsened = False
    for key in DOMAIN_KEYS:
        before = TONE_POINTS.get(previous.get(key))
        after = TONE_POINTS.get(domains.get(key))
        if before is None or after is None:
            continue
        if after > before:
            improved = True
        elif after < before:
            worsened = True
    return improved and not worsened, worsened and not improved


def _legacy_vetoes(row: pd.Series) -> list[str]:
    reasons: list[str] = []
    if bool(row.get("lb_alarm", False)):
        reasons.append("🚨 alarm")
    if bool(row.get("lb_fade", False)):
        reasons.append("featured fade")
    if str(row.get("lb_cond") or "") == "bad":
        reasons.append("legacy Cond red")
    if str(row.get("lb_region") or "") == "bad":
        reasons.append("legacy region red")
    return reasons


def finalize_decisions(
    df: pd.DataFrame,
    date: str,
    context: dict,
) -> pd.DataFrame:
    """Route each name into a lane and write explicit bull/bear decisions."""
    if df is None or df.empty:
        return df
    out = df.copy()
    previous = _previous_domains(date)
    market = context.get("market") or {}
    state = str(market.get("state") or "yellow")
    rows: list[dict] = []
    for _, row in out.iterrows():
        ticker = _tick(row.get("Ticker"))
        domains = {key: str(row.get(f"d_{key}") or "missing")
                   for key in DOMAIN_KEYS}
        blue, alarm = _change_marks(domains, previous.get(ticker))
        lookback_vetoes = _legacy_vetoes(row)
        if alarm:
            lookback_vetoes.append("v2 domain alarm")
        # Domain-region red is almost automatic on HARD_RED (market is red
        # for everyone). Keep it as a veto for ordinary lanes, not for a
        # company-news / lookback-clocked probable long.
        region_veto = []
        if str(row.get("domain_region") or "") == "bad":
            region_veto.append("v2 domain region red")
        legacy_vetoes = lookback_vetoes + region_veto
        quality_ok = not legacy_vetoes

        direct = bool(row.get("company_direct"))
        company_strength = _num(row.get("company_strength"))
        confirmed = bool(row.get("company_price_confirmed"))
        catalyst = (
            direct
            and domains["company"] == "good"
            and company_strength >= DIRECT_EVENT_MIN
            and domains["setup"] != "bad"
            and domains["flow"] != "bad"
            and quality_ok
        )
        hard_red_catalyst = (
            catalyst
            and company_strength >= DIRECT_EVENT_HARD_RED_MIN
            and confirmed
            and not (
                domains["parent"] == "bad"
                and domains["child"] == "bad"
            )
        )
        group_leader = (
            str(row.get("child_abs_tone")) == "good"
            and str(row.get("child_rel_tone")) == "good"
            and domains["setup"] == "good"
            and domains["flow"] != "bad"
            and domains["company"] != "bad"
            and quality_ok
        )
        standard = (
            domains["parent"] != "bad"
            and domains["child"] != "bad"
            and domains["company"] != "bad"
            and domains["setup"] == "good"
            and domains["flow"] != "bad"
            and quality_ok
        )
        # Most-probable long: the new method still clocks a name when the
        # tape is hostile if company news, child/theme outperformance, or
        # lookback blue/white fire — and the original alarm/fade/Cond/region
        # evaluator has not vetoed it.
        # Original lookback evaluator only — domain_name_white is too easy
        # on a HARD_RED day (market is red for everyone).
        white = bool(row.get("lb_zero_red"))
        blue_mark = bool(row.get("lb_blue") or blue)
        child_outperform = (
            str(row.get("child_abs_tone")) == "good"
            or str(row.get("child_rel_tone")) == "good"
        )
        child_strong = (
            _num(row.get("child_w1")) >= GROUP_ABS_WEEK
            or _num(row.get("child_residual")) >= GROUP_RESIDUAL
        )
        company_clock = (
            domains["company"] == "good"
            and (
                (direct and bool(row.get("company_fresh"))
                 and company_strength >= 0.48)
                or company_strength >= DIRECT_EVENT_MIN
            )
        )
        group_clock = (
            str(row.get("child_abs_tone")) == "good"
            and str(row.get("child_rel_tone")) == "good"
            and child_strong
            and domains["setup"] == "good"
            and domains["flow"] != "bad"
            and domains["company"] != "bad"
            # Industry tape alone is not a long. Need a name-level clock
            # (company news, peer RS, or lookback blue) so HARD_RED does
            # not mint a long for every name in a hot child.
            and (
                domains["company"] == "good"
                or _num(row.get("s_peer")) >= 0.05
                or bool(row.get("lb_blue") or blue)
            )
        )
        marks_clock = (
            (white or blue_mark)
            and domains["setup"] == "good"
            and domains["child"] != "bad"
            and domains["company"] != "bad"
        )
        probable = bool(
            not lookback_vetoes
            and domains["setup"] != "bad"
            and (company_clock or group_clock or marks_clock)
        )
        clocks: list[str] = []
        if company_clock:
            clocks.append(
                "company news "
                + ("fresh " if row.get("company_fresh") else "")
                + f"({company_strength:.2f})"
            )
        if group_clock:
            clocks.append(
                f"child/theme outperform "
                f"{_num(row.get('child_w1')):+.1f}% 1w / "
                f"{_num(row.get('child_residual')):+.1f}% rel"
            )
        if marks_clock:
            bits = []
            if blue_mark:
                bits.append("🔵 blue")
            if white:
                bits.append("⚪ white")
            clocks.append("lookback " + (",".join(bits) or "marks"))

        lane = "blocked"
        eligible = False
        if state == "hard_red":
            if hard_red_catalyst:
                lane, eligible = "catalyst_exception", True
            elif probable:
                lane, eligible = "probable", True
        elif state == "red":
            if catalyst and confirmed:
                lane, eligible = "catalyst", True
            elif group_leader and (
                _num(row.get("change_pct")) >= PRICE_CONFIRM_PCT
                or _num(row.get("gap_pct")) >= PRICE_CONFIRM_PCT
            ):
                lane, eligible = "group_leader", True
            elif probable:
                lane, eligible = "probable", True
        else:
            if catalyst:
                lane, eligible = "catalyst", True
            elif group_leader:
                lane, eligible = "group_leader", True
            elif standard:
                lane, eligible = "standard", True
            elif probable:
                lane, eligible = "probable", True

        blockers: list[str] = []
        if state == "hard_red" and not (hard_red_catalyst or probable):
            blockers.append(
                "HARD_RED: no company / child-outperform / lookback clock"
            )
        elif state == "red" and not (catalyst or group_leader or probable):
            blockers.append(
                "RED market: no confirmed catalyst, group leader, or probable clock"
            )
        if domains["parent"] == "bad":
            blockers.append("parent sector RED")
        if domains["child"] == "bad":
            blockers.append("child industry/theme RED")
        if domains["company"] == "bad":
            blockers.append("company evidence RED")
        if domains["setup"] != "good":
            blockers.append(f"setup {_tone_label(domains['setup'])}")
        if domains["flow"] == "bad":
            blockers.append("flow RED")
        if direct and not confirmed:
            blockers.append("direct catalyst lacks price confirmation")
        blockers.extend(legacy_vetoes)

        bull_parts = [
            f"market={state.upper()}",
            f"parent={_tone_label(domains['parent'])}",
            (
                f"child={_tone_label(domains['child'])}"
                f"/rel={_tone_label(str(row.get('child_rel_tone') or 'neutral'))}"
            ),
            (
                f"company={_tone_label(domains['company'])}"
                f"({company_strength:.2f})"
            ),
            f"setup={_tone_label(domains['setup'])}",
            f"flow={_tone_label(domains['flow'])}",
        ]
        if eligible:
            mark_bits = []
            if blue_mark:
                mark_bits.append("🔵")
            if white:
                mark_bits.append("⚪")
            if str(row.get("lb_cond") or "") == "good":
                mark_bits.append("Cond green")
            prefix = f"BUY {lane.upper()}"
            if lane == "probable":
                prefix += (
                    " — most-probable long on "
                    f"{state.upper()} (size ×{float(market.get('position_scale') or 0.25):.2f})"
                )
                if clocks:
                    prefix += "; clocks: " + "; ".join(clocks)
            bull_decision = prefix + " — " + "; ".join(
                bull_parts + (["lookback=" + ",".join(mark_bits)] if mark_bits else [])
            )
        else:
            bull_decision = (
                "BLOCK BUY — " + "; ".join(blockers or ["no lane qualified"])
                + " | " + "; ".join(bull_parts)
            )

        strong_positive_company = (
            direct and domains["company"] == "good" and company_strength >= 0.70
        )
        red_domains = [
            key for key in ("parent", "child", "company", "setup", "flow")
            if domains[key] == "bad"
        ]
        bear_eligible = (
            not strong_positive_company
            and (
                len(red_domains) >= 2
                or (
                    domains["child"] == "bad"
                    and domains["flow"] != "good"
                )
                or (
                    state in ("red", "hard_red")
                    and domains["setup"] == "bad"
                )
            )
        )
        bear_reasons = [f"market={state.upper()}"]
        if red_domains:
            bear_reasons.append("red domains=" + ",".join(red_domains))
        if str(row.get("child_rel_tone")) == "bad":
            bear_reasons.append(
                f"child lags parent {_num(row.get('child_residual')):+.1f}%"
            )
        if strong_positive_company:
            bear_reasons.append("strong direct company catalyst vetoes short")
        bear_decision = (
            ("SELL/AVOID — " if bear_eligible else "NO BEAR — ")
            + "; ".join(bear_reasons)
        )

        domain_good = sum(v == "good" for v in domains.values())
        domain_bad = sum(v == "bad" for v in domains.values())
        lane_bonus = {
            "catalyst_exception": 4.0,
            "catalyst": 3.0,
            "group_leader": 2.0,
            "standard": 1.0,
            "probable": 0.8,
            "blocked": 0.0,
        }.get(lane, 0.0)
        bull_rank = (
            (10.0 if eligible else 0.0)
            + lane_bonus
            + company_strength * 2.0
            + _num(row.get("group_strength"))
            + _num(row.get("setup_strength"))
            + _num(row.get("flow_strength"))
            + domain_good * 0.20
            - domain_bad * 0.35
            + (0.80 if blue_mark else 0.0)
            + (0.50 if white else 0.0)
            + (0.30 if str(row.get("lb_cond") or "") == "good" else 0.0)
            - (1.50 if bool(row.get("lb_alarm")) else 0.0)
            + (2.50 if company_clock and bool(row.get("company_fresh")) else 0.0)
        )
        bear_rank = (
            (10.0 if bear_eligible else 0.0)
            + domain_bad
            + (1.0 if str(row.get("child_rel_tone")) == "bad" else 0.0)
            + max(0.0, -_num(row.get("s_join")))
            + max(0.0, -_num(row.get("s_peer")))
            + (1.0 if state in ("red", "hard_red") else 0.0)
        )
        rows.append({
            "decision_lane": lane,
            "bull_eligible": bool(eligible),
            "bear_eligible": bool(bear_eligible),
            "bull_rank": round(bull_rank, 4),
            "bear_rank": round(bear_rank, 4),
            "bull_decision": bull_decision,
            "bear_decision": bear_decision,
            "decision_blockers": "; ".join(blockers),
            "domain_blue": bool(blue),
            "domain_alarm": bool(alarm),
        })
    decisions = pd.DataFrame(rows, index=out.index)
    for col in decisions.columns:
        out[col] = decisions[col]
    return out


def _watch_row(row: pd.Series) -> dict:
    return {
        "ticker": _tick(row.get("Ticker")),
        "sector": row.get("sector"),
        "industry": row.get("industry"),
        "size": row.get("size"),
        "lane": row.get("decision_lane"),
        "eligible": bool(row.get("bull_eligible")),
        "bull_rank": _num(row.get("bull_rank")),
        "bear_rank": _num(row.get("bear_rank")),
        "domains": {
            key: row.get(f"d_{key}") for key in DOMAIN_KEYS
        },
        "source_boxes": {
            key: row.get(f"src_{key}_tone") for key in SOURCE_KEYS
        },
        "company": row.get("company_summary"),
        "company_strength": _num(row.get("company_strength")),
        "company_direct": bool(row.get("company_direct")),
        "company_fresh": bool(row.get("company_fresh")),
        "company_price_confirmed": bool(row.get("company_price_confirmed")),
        "group": row.get("group_label"),
        "child_d1": _num(row.get("child_d1")),
        "child_w1": _num(row.get("child_w1")),
        "child_residual": _num(row.get("child_residual")),
        "bull_decision": row.get("bull_decision"),
        "bear_decision": row.get("bear_decision"),
        "blue": bool(row.get("lb_blue") or row.get("domain_blue")),
        "white": bool(row.get("lb_zero_red") or row.get("domain_name_white")),
        "alarm": bool(row.get("lb_alarm") or row.get("domain_alarm")),
        "cond": row.get("lb_cond") or row.get("domain_cond"),
        "region": row.get("lb_region") or row.get("domain_region"),
    }


def summarize(df: pd.DataFrame, context: dict, top_n: int = 15) -> dict:
    market = context.get("market") or {}
    if df is None or df.empty:
        return {
            "schema_version": SCHEMA_VERSION,
            "scope": "1d",
            "market": market,
            "bull_watch": [],
            "bear_watch": [],
        }
    # Same-day company clocks first so AMGN-style events cannot be buried
    # under a sea of blue / group-tape names, then fill by bull_rank.
    ordered_idx: list[Any] = []
    company_first = df.loc[
        df["company_fresh"].astype(bool)
        & df["company_direct"].astype(bool)
        & (pd.to_numeric(df["company_strength"], errors="coerce").fillna(0.0)
           >= DIRECT_EVENT_MIN)
    ].sort_values(
        ["bull_eligible", "company_strength", "bull_rank"],
        ascending=False,
    )
    for idx in company_first.head(5).index:
        ordered_idx.append(idx)
    for idx in df.sort_values("bull_rank", ascending=False).index:
        if idx not in ordered_idx:
            ordered_idx.append(idx)
        if len(ordered_idx) >= top_n:
            break
    bull = df.loc[ordered_idx]
    bear_pool = df.loc[df["bear_eligible"].astype(bool)]
    if bear_pool.empty:
        bear_pool = df
    bear = bear_pool.sort_values("bear_rank", ascending=False).head(top_n)
    n_eligible = int(df["bull_eligible"].astype(bool).sum())
    stand_down = (
        str(market.get("state")) == "hard_red" and n_eligible == 0
    )
    n_probable = int(
        (df["decision_lane"].astype(str) == "probable").sum()
    ) if "decision_lane" in df.columns else 0
    reason = (
        f"{market.get('rationale')}; no company / child / lookback clock"
        if stand_down
        else (
            f"{n_eligible} names qualified through "
            f"{','.join(market.get('allowed_lanes') or [])}"
            + (f" ({n_probable} probable)" if n_probable else "")
        )
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "scope": "1d",
        "method": "gate -> route -> rank",
        "market": market,
        "n_bull_eligible": n_eligible,
        "n_bear_eligible": int(df["bear_eligible"].astype(bool).sum()),
        "stand_down": {
            "stand_down": stand_down,
            "restrict_to_catalysts": str(market.get("state")) in ("red", "hard_red"),
            "reason": reason,
            "n_usable_catalysts": int(
                sum(
                    1 for r in (context.get("catalysts") or {}).values()
                    if r.get("usable")
                )
            ),
            "market_state": market.get("state"),
        },
        "bull_watch": [_watch_row(row) for _, row in bull.iterrows()],
        "bear_watch": [_watch_row(row) for _, row in bear.iterrows()],
    }
