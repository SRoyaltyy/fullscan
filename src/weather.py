"""Weather engine — the REGIME machine of the label → regime → join design.

Labels (src/segments.py) answer "what is this stock?". This module answers
"is today good for that kind of thing?" — one stance per label family value:

    favorable | neutral | hostile | unknown

Deterministic rules only. Thresholds and rationale text live in
00_grounding/weather_rules.json so lessons can retune the weather without
touching code. No LLM scores 6,000 stocks; the LLM's only role (elsewhere)
is proposing new rules.

Inputs (all optional — missing inputs degrade to stance "unknown" and are
listed in data_gaps, never silently invented):
  03_scoreboard/scoreboard.json            general + sector predict runs
  01_daily/general/<date>_predict.md       factor scoreboard (yields, Fed,
                                           oil & dollar, futures, ...)
  01_daily/_channel1/<date>_predict.json   VIX ratio, Fear & Greed,
                                           yield/SPX regime correlation
  01_daily/events/latest.json              event scanner (used only when
                                           scan_date matches)

Outputs (THE BACKTESTABLE RECORD — appended daily, never overwritten):
  01_daily/weather/<date>_weather.json     machine stances + signals + why
  01_daily/weather/<date>_weather.md       human weather report
  01_daily/weather/latest.json / .md       convenience copies

CLI:
  python -m src.weather                    # latest predict date
  python -m src.weather --date 2026-08-12
"""
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
DAILY = ROOT / "01_daily"
SCOREBOARD = ROOT / "03_scoreboard" / "scoreboard.json"
RULES_PATH = ROOT / "00_grounding" / "weather_rules.json"
OUT_DIR = DAILY / "weather"
ET = ZoneInfo("America/New_York")

SECTORS = ["Basic Materials", "Communication Services", "Consumer Cyclical",
           "Consumer Defensive", "Energy", "Financial", "Healthcare",
           "Industrials", "Real Estate", "Technology", "Utilities"]


# ---------------------------------------------------------------- inputs

def _load_json(p: Path):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


_DIR_RE = re.compile(r"^-\s*predicted_direction:\s*\*\*(.+?)\*\*", re.I)
_SCORE_RE = re.compile(r"^-\s*total_score:\s*\*\*([+-]?[\d.]+)")
_CONF_RE = re.compile(r"^-\s*confidence_score:\s*\*?\*?([+-]?[\d.]+)")


def _run_from_predict_md(path: Path) -> dict | None:
    """Read the deterministic decision footer when scoreboard ingest lagged."""
    if not path.exists():
        return None
    direction = None
    score = None
    conf = None
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return None
    for line in lines:
        line = line.strip()
        m = _DIR_RE.match(line)
        if m:
            direction = m.group(1).strip()
            continue
        m = _SCORE_RE.match(line)
        if m:
            try:
                score = float(m.group(1))
            except ValueError:
                pass
            continue
        m = _CONF_RE.match(line)
        if m:
            try:
                conf = float(m.group(1))
            except ValueError:
                pass
    if score is None and not direction:
        return None
    return {
        "predicted_direction": direction,
        "total_score": score,
        "confidence_score": conf,
        "source": "predict_md",
    }


def load_runs(date_str: str) -> tuple[dict | None, dict[str, dict]]:
    d = _load_json(SCOREBOARD) or {}
    general, sectors = None, {}
    for r in d.get("runs", []):
        if r.get("date") != date_str:
            continue
        topic = r.get("topic", "")
        if topic == "general":
            general = r
        elif topic.startswith("sector:"):
            sectors[topic.split(":", 1)[1]] = r
    if general is None:
        general = _run_from_predict_md(DAILY / "general" / f"{date_str}_predict.md")
    for name in SECTORS:
        if name in sectors:
            continue
        slug = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
        run = _run_from_predict_md(
            DAILY / "sectors" / date_str / f"{slug}_predict.md")
        if run:
            sectors[name] = run
    return general, sectors


_FACTOR_RE = re.compile(r"^-\s*\S+\s+(.+?):\s*([+-]?[\d.]+)\s*[×xX]")


def load_factors(date_str: str) -> dict[str, float]:
    """Factor scores from the general predict md snapshot, e.g.
    '- 🔴 Bond yields: -0.5 × 2.0 = -1.0' -> {'Bond yields': -0.5}"""
    p = DAILY / "general" / f"{date_str}_predict.md"
    if not p.exists():
        return {}
    out = {}
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip().lstrip(">").strip()
        m = _FACTOR_RE.match(line)
        if m:
            try:
                out[m.group(1).strip()] = float(m.group(2))
            except ValueError:
                pass
    return out


def load_channel1(date_str: str, live: bool = True) -> dict:
    """Live fetch when yfinance is installed; otherwise dated JSON. Merge
    so a half-failed live call cannot wipe a good VIX/DXY cache."""
    disk = _load_json(DAILY / "_channel1" / f"{date_str}_predict.json") or {}
    try:
        import yfinance  # noqa: F401
    except ImportError:
        live = False
    if not live:
        return disk
    try:
        from . import fetch_channel1
        data = fetch_channel1.build("predict")
        fetch_channel1.save(data, date_str, "predict")
        # fill holes from disk so a Yahoo outage doesn't blank VIX
        if disk:
            if not (data.get("vix") or {}).get("vix"):
                data["vix"] = disk.get("vix") or data.get("vix")
            fx = data.setdefault("commodities_fx", {})
            disk_fx = disk.get("commodities_fx") or {}
            for k, v in disk_fx.items():
                if not (fx.get(k) or {}).get("available"):
                    fx[k] = v
            if not (data.get("fred") or {}):
                data["fred"] = disk.get("fred") or {}
        print(f"[weather] live Channel 1 fetched for {date_str}")
        return data
    except Exception as e:
        print(f"[weather] live Channel 1 failed ({e}); using on-disk cache")
        return disk


def _finviz_sector_tape(date_str: str) -> dict[str, float]:
    """Median Performance (Week) by Finviz sector — always available, no LLM."""
    export = ROOT / "data" / "exports" / f"finviz_{date_str}.csv"
    if not export.exists():
        files = sorted((ROOT / "data" / "exports").glob("finviz_????-??-??.csv"))
        export = files[-1] if files else None
    if not export or not export.exists():
        return {}
    try:
        import pandas as pd
        df = pd.read_csv(export, usecols=["Sector", "Performance (Week)"], low_memory=False)
        def _pct(x):
            try:
                return float(str(x).replace("%", "").replace(",", ""))
            except Exception:
                return None
        df["pw"] = df["Performance (Week)"].map(_pct)
        out = {}
        for sec, g in df.dropna(subset=["pw"]).groupby("Sector"):
            out[str(sec)] = float(g["pw"].median())
        return out
    except Exception as e:
        print(f"[weather] sector tape failed: {e}")
        return {}


def load_events(date_str: str) -> dict:
    """Event scanner output. Accept the latest scan when its scan_date is
    within 3 days of the weather date (the scanner covers ±2 weeks, so a
    near-date scan still describes the same event landscape)."""
    d = _load_json(DAILY / "events" / "latest.json") or {}
    sd = d.get("scan_date")
    if not sd:
        return {}
    try:
        delta = abs((datetime.fromisoformat(sd)
                     - datetime.fromisoformat(date_str)).days)
    except ValueError:
        return {}
    return d if delta <= 3 else {}


# ---------------------------------------------------------------- signals

def derive_signals(date_str: str, th: dict) -> tuple[dict, list[str]]:
    gaps: list[str] = []
    general, sectors = load_runs(date_str)
    factors = load_factors(date_str)
    ch1 = load_channel1(date_str, live=True)
    events = load_events(date_str)

    sig: dict = {"date": date_str}

    # -- risk state from the general predict --
    if general and general.get("total_score") is not None:
        g = float(general["total_score"])
        sig["general_score"] = g
        sig["general_direction"] = general.get("predicted_direction")
        sig["general_confidence"] = general.get("confidence_score")
        sig["risk"] = ("on" if g >= th["risk_on_score"]
                       else "off" if g <= th["risk_off_score"] else "mixed")
    else:
        sig["risk"] = "unknown"
        gaps.append("general predict run")

    # -- factor-derived signals --
    sig["yields_score"] = factors.get("Bond yields")
    sig["fed_score"] = factors.get("Fed policy path")
    sig["dollar_oil_score"] = factors.get("Oil & dollar")
    sig["futures_score"] = factors.get("US index futures")
    if not factors:
        gaps.append("general predict factor scoreboard")

    ys = sig["yields_score"]
    # Prefer FRED DGS10 1d/1w over the LLM "Bond yields" factor (which is
    # already an equity-impact score, not a yield delta).
    fred = ch1.get("fred") or {}
    dgs10 = fred.get("DGS10") or {}
    d10 = dgs10.get("delta_1d")
    if d10 is None:
        d10 = dgs10.get("delta_1w")
    if d10 is not None:
        sig["yields"] = ("rising" if d10 > 0.02 else "falling" if d10 < -0.02 else "flat")
        sig["yields_source"] = "fred_dgs10"
        sig["dgs10_delta_1d"] = dgs10.get("delta_1d")
        sig["dgs10_current"] = dgs10.get("current")
    else:
        sig["yields"] = ("rising" if ys is not None and ys < 0
                         else "falling" if ys is not None and ys > 0
                         else "flat" if ys is not None else "unknown")
        sig["yields_source"] = "llm_factor_fallback"
    dxy = ((ch1.get("commodities_fx") or {}).get("DXY") or {})
    dxy_1d = dxy.get("pct_1d")
    if dxy_1d is not None:
        sig["dollar"] = ("strong" if dxy_1d >= 0.15 else "soft" if dxy_1d <= -0.15 else "flat")
        sig["dollar_source"] = "dxy"
        sig["dxy_pct_1d"] = dxy_1d
    else:
        # Do NOT infer dollar from the mashed "Oil & dollar" LLM factor.
        sig["dollar"] = "unknown"
        sig["dollar_source"] = "missing_dxy"

    # -- channel 1 vol: ratio, else VIX 1d delta, else VIX level --
    vix_blk = ch1.get("vix") or {}
    vix_ratio = vix_blk.get("ratio")
    vix_spot = (vix_blk.get("vix") or {}).get("current")
    vix_d1 = (vix_blk.get("vix") or {}).get("delta_1d")
    sig["vix_ratio"] = vix_ratio
    sig["vix_spot"] = vix_spot
    sig["vix_delta_1d"] = vix_d1
    sig["vix_ratio_source"] = vix_blk.get("ratio_source")
    if vix_ratio is not None and vix_ratio >= th["vix_spike_ratio"]:
        sig["vix"] = "spiking"
    elif vix_ratio is not None and vix_ratio <= th["vix_falling_ratio"]:
        sig["vix"] = "falling"
    elif vix_d1 is not None and vix_d1 >= 1.5:
        sig["vix"] = "spiking"
    elif vix_d1 is not None and vix_d1 <= -1.5:
        sig["vix"] = "falling"
    elif vix_spot is not None and vix_spot >= 25:
        sig["vix"] = "spiking"
    elif vix_spot is not None:
        sig["vix"] = "calm"
    else:
        sig["vix"] = "unknown"

    cl = (ch1.get("commodities_fx") or {}).get("CL=F") or {}
    oil_1d = cl.get("pct_1d")
    sig["oil_pct_1d"] = oil_1d
    if oil_1d is not None:
        sig["oil"] = ("rising" if oil_1d >= 1.0 else "falling" if oil_1d <= -1.0 else "flat")
    else:
        sig["oil"] = "unknown"
    fg = (ch1.get("fear_greed") or {})
    sig["fear_greed"] = fg.get("value") if fg.get("available") else None
    sig["fear_greed_label"] = fg.get("label") if fg.get("available") else None
    corr = (ch1.get("yield_spx_corr") or {})
    sig["regime_corr"] = corr.get("corr_5d") if corr.get("available") else None
    if not ch1:
        gaps.append("channel 1 predict json")

    # -- sector board (LLM) + Finviz tape (always) --
    sig["sectors"] = {
        name: {"score": r.get("total_score"), "dir": r.get("predicted_direction"),
               "conf": r.get("confidence_score"), "source": "llm"}
        for name, r in sectors.items()
    }
    tape = _finviz_sector_tape(date_str)
    sig["sector_tape"] = tape
    for name, med in tape.items():
        if name not in sig["sectors"]:
            stance_hint = ("up" if med >= 1.5 else "down" if med <= -1.5 else "flat")
            sig["sectors"][name] = {
                "score": med, "dir": stance_hint, "conf": 0.4, "source": "finviz_week",
            }
    if not sig["sectors"]:
        gaps.append("sector predict runs")
    if not tape:
        gaps.append("finviz sector tape")

    # -- events --
    ev = events.get("events", [])
    sig["events_bull"] = sum(1 for e in ev
                             if str(e.get("expected_direction", "")).lower().startswith(("bull", "pos"))
                             and (e.get("impact") or 0) >= th["event_min_impact"])
    sig["events_bear"] = sum(1 for e in ev
                             if str(e.get("expected_direction", "")).lower().startswith(("bear", "neg"))
                             and (e.get("impact") or 0) >= th["event_min_impact"])
    china = [e for e in ev
             if any("china" in str(r).lower() for r in e.get("regions", []))
             and (e.get("impact") or 0) >= th["china_event_min_impact"]]
    sig["china_event_dir"] = ("bear" if any(str(e.get("expected_direction", "")).lower().startswith(("bear", "neg")) for e in china)
                              else "bull" if china else None)
    if not events:
        gaps.append("event scanner output")

    return sig, gaps


# ---------------------------------------------------------------- stances

def _s(stance: str, conf: str, why: str) -> dict:
    return {"stance": stance, "confidence": conf, "why": why}


def build_stances(sig: dict, th: dict) -> dict[str, dict[str, dict]]:
    risk, yields, dollar, vix = sig["risk"], sig["yields"], sig["dollar"], sig["vix"]
    fg = sig.get("fear_greed")
    greed = fg is not None and fg >= th["extreme_greed"]
    fear = fg is not None and fg <= th["extreme_fear"]
    risk_known = risk != "unknown"
    out: dict[str, dict[str, dict]] = {f: {} for f in
        ["sector", "size", "index", "geo", "beta", "short", "vol", "profit",
         "lev", "style", "mom", "ext", "range"]}

    # -- sectors: LLM board if present, else Finviz week tape --
    for name in SECTORS:
        s = sig["sectors"].get(name)
        if not s or s.get("score") is None:
            out["sector"][name] = _s("unknown", "low", "no sector predict or tape")
            continue
        sc = float(s["score"])
        src = s.get("source") or "llm"
        fav_th = 1.5 if src == "finviz_week" else th.get("sector_favorable_score", 3.0)
        host_th = -1.5 if src == "finviz_week" else th.get("sector_hostile_score", -3.0)
        stance = ("favorable" if sc >= fav_th
                  else "hostile" if sc <= host_th else "neutral")
        why = (
            f"finviz sector median week {sc:+.2f}% [tape]"
            if src == "finviz_week"
            else f"sector predict score {sc:+.1f} dir {s.get('dir')} conf {s.get('conf')} [sector board]"
        )
        out["sector"][name] = _s(stance, "medium" if src == "finviz_week" else "high", why)

    # -- size --
    if risk_known:
        small_why = f"risk-{risk}, dollar {dollar} [general predict + factors]"
        if risk == "on" and dollar == "soft":
            out["size"].update({v: _s("favorable", "medium",
                f"risk-on with soft dollar — small-cap tape works; {small_why}")
                for v in ("micro", "small")})
        elif risk == "off" or dollar == "strong":
            out["size"].update({v: _s("hostile", "medium",
                f"{'risk-off' if risk == 'off' else 'strong dollar'} — small caps de-rate first; {small_why}")
                for v in ("micro", "small")})
        else:
            out["size"].update({v: _s("neutral", "low", small_why)
                                for v in ("micro", "small")})
        if risk == "off":
            out["size"].update({v: _s("favorable", "medium",
                "risk-off — defensive/quality bid concentrates in large & mega [general predict]")
                for v in ("large", "mega")})
        elif risk == "on":
            out["size"].update({v: _s("neutral", "low",
                "risk-on — mega leads less in broad rallies [general predict]")
                for v in ("large", "mega")})
        else:
            out["size"].update({v: _s("neutral", "low", f"risk-mixed [general predict]")
                                for v in ("large", "mega")})
        out["size"]["mid"] = _s("neutral", "low", "no dedicated mid-cap signal in v1")
    else:
        out["size"].update({v: _s("unknown", "low", "no general predict") for v in
                            ("micro", "small", "mid", "large", "mega")})

    # -- index: independent of size (do not clone — that double-counted in join)
    if risk_known:
        out["index"]["sp500"] = out["size"].get("mega", _s("neutral", "low", "mirrors mega"))
        out["index"]["ndx"] = out["size"].get("mega", _s("neutral", "low", "mirrors mega"))
        out["index"]["djia"] = out["size"].get("large", _s("neutral", "low", "mirrors large"))
        out["index"]["rut"] = out["size"].get("small", _s("neutral", "low", "mirrors small"))
        out["index"]["none"] = _s("neutral", "low", "non-index: not a small-cap proxy")
    else:
        out["index"].update({v: _s("unknown", "low", "no general predict")
                             for v in ("sp500", "ndx", "djia", "rut", "none")})

    # -- beta --
    if risk_known or vix != "unknown":
        if risk == "on" and vix != "spiking":
            out["beta"]["high"] = _s("favorable", "medium",
                f"risk-on, VIX {vix} — high beta outperforms [general + channel1]")
            out["beta"]["low"] = _s("neutral", "low", "risk-on — low beta lags rallies")
        elif risk == "off" or vix == "spiking":
            out["beta"]["high"] = _s("hostile", "high" if vix == "spiking" else "medium",
                f"{'VIX spiking' if vix == 'spiking' else 'risk-off'} — high beta is the exit door [channel1 + general]")
            out["beta"]["low"] = _s("favorable", "medium",
                "defensive ballast bid in stress [general + channel1]")
        else:
            out["beta"]["high"] = _s("neutral", "low", f"risk-{risk}, VIX {vix}")
            out["beta"]["low"] = _s("neutral", "low", f"risk-{risk}, VIX {vix}")
        out["beta"]["mid"] = _s("neutral", "low", "beta-neutral zone")
    else:
        out["beta"].update({v: _s("unknown", "low", "no risk/vol signals")
                            for v in ("low", "mid", "high")})

    # -- short: multiplier semantics --
    if risk_known:
        if risk == "on":
            out["short"].update({v: _s("favorable", "low",
                "risk-on — crowded shorts are squeeze FUEL if the tape rises (multiplier, not a direction) [general]")
                for v in ("high", "extreme")})
        elif risk == "off":
            out["short"].update({v: _s("hostile", "medium",
                "risk-off — heavy short interest marks balance-sheet/dilution stress; it amplifies falls [general]")
                for v in ("high", "extreme")})
        else:
            out["short"].update({v: _s("neutral", "low", "mixed tape — squeeze/stress unresolved")
                                 for v in ("high", "extreme")})
        out["short"]["low"] = _s("neutral", "low", "low short is not a tailwind by itself")
        out["short"]["mid"] = _s("neutral", "low", "no strong crowding signal")
    else:
        out["short"].update({v: _s("unknown", "low", "no general predict") for v in
                             ("low", "mid", "high", "extreme")})

    # -- vol regime --
    if risk_known or vix != "unknown":
        hostile = risk == "off" or vix == "spiking"
        out["vol"]["high"] = _s("hostile" if hostile else "neutral",
                                "medium" if hostile else "low",
                                f"{'stress market — high-vol names de-rate fast' if hostile else 'calm tape — vol regime not decisive'} [general + channel1]")
        out["vol"]["low"] = _s("favorable" if hostile else "neutral", "low",
                               "low-vol preferred in stress" if hostile else "calm tape")
        out["vol"]["mid"] = _s("neutral", "low", "—")
    else:
        out["vol"].update({v: _s("unknown", "low", "no signals") for v in ("low", "mid", "high")})

    # -- profitability / style: the yields + speculation axis --
    if yields != "unknown" or risk_known:
        spec = risk == "on" and greed
        if risk == "off":
            out["profit"]["no"] = _s("hostile", "high",
                "risk-off — unprofitable names are sold first [general]")
            out["profit"]["yes"] = _s("favorable", "medium",
                "risk-off — quality/profitability bid [general]")
        elif spec:
            out["profit"]["no"] = _s("favorable", "medium",
                f"speculative froth (F&G {fg:.0f} = extreme greed) — junk rallies [channel1]")
            out["profit"]["yes"] = _s("neutral", "low", "quality lags in pure speculation")
        else:
            out["profit"]["no"] = _s("neutral", "low", f"risk-{risk}, F&G {sig.get('fear_greed_label', 'n/a')}")
            out["profit"]["yes"] = _s("neutral", "low", "—")
        out["profit"]["thin"] = _s("neutral", "low", "—")

        if yields == "falling":
            out["style"]["growth"] = _s("favorable", "medium",
                "yields falling — duration/growth re-rates [factor: Bond yields]")
            out["style"]["value"] = _s("neutral", "low", "value lags duration rallies")
        elif yields == "rising":
            out["style"]["growth"] = _s("hostile", "medium",
                "yields rising — long-duration growth de-rates [factor: Bond yields]")
            out["style"]["value"] = _s("favorable", "medium",
                "rising yields/reflation favors value & cyclicals [factor: Bond yields]")
        else:
            out["style"]["growth"] = _s("neutral", "low", "yields flat/unknown")
            out["style"]["value"] = _s("neutral", "low", "yields flat/unknown")
        out["style"]["blend"] = _s("neutral", "low", "—")
    else:
        for f, vs in (("profit", ("no", "thin", "yes")), ("style", ("growth", "blend", "value"))):
            out[f].update({v: _s("unknown", "low", "no yields/risk signals") for v in vs})

    # -- leverage --
    if yields != "unknown" or risk_known:
        if yields == "rising" or risk == "off":
            out["lev"]["high"] = _s("hostile", "medium",
                f"{'yields rising' if yields == 'rising' else 'risk-off'} — leverage amplifies the downside [factors + general]")
            out["lev"]["low"] = _s("favorable", "low", "balance-sheet strength preferred in stress")
        elif yields == "falling" and risk == "on":
            out["lev"]["high"] = _s("favorable", "low",
                "easing + risk-on — leverage amplifies the upside [factors + general]")
            out["lev"]["low"] = _s("neutral", "low", "low leverage lags melt-ups")
        else:
            out["lev"]["high"] = _s("neutral", "low", "—")
            out["lev"]["low"] = _s("neutral", "low", "—")
        out["lev"]["mid"] = _s("neutral", "low", "—")
        out["lev"]["neg_equity"] = _s("hostile", "low",
            "negative equity is distressed in any regime")
    else:
        out["lev"].update({v: _s("unknown", "low", "no signals") for v in
                           ("low", "mid", "high", "neg_equity")})

    # -- momentum / extension / 52w zone: trend vs mean-revert day --
    if risk_known:
        trend_day = risk == "on" and (sig.get("futures_score") or 0) > 0
        if trend_day:
            out["mom"]["uptrend"] = _s("favorable", "medium",
                "risk-on + positive futures — trend continuation day [general + factors]")
            out["ext"]["washed"] = _s("favorable", "low", "washouts bounce in risk-on turns")
            out["range"]["deep_low"] = _s("neutral", "low", "bottom-fishing only with confirmation")
        elif risk == "off":
            out["mom"]["uptrend"] = _s("neutral", "low", "uptrends under test in risk-off")
            out["ext"]["washed"] = _s("hostile", "medium", "falling knives stay sharp in risk-off")
            out["range"]["deep_low"] = _s("hostile", "medium", "falling knife zone in risk-off")
        else:
            out["mom"]["uptrend"] = _s("neutral", "low", "mixed tape")
            out["ext"]["washed"] = _s("neutral", "low", "mixed tape")
            out["range"]["deep_low"] = _s("neutral", "low", "mixed tape")
        out["mom"]["downtrend"] = _s("hostile" if risk == "off" else "neutral", "low",
                                     "downtrends get no bid in risk-off" if risk == "off" else "—")
        out["mom"]["mixed"] = _s("neutral", "low", "—")
        for v in ("extended", "extreme"):
            out["ext"][v] = _s("hostile" if risk == "off" else "neutral",
                               "medium" if risk == "off" else "low",
                               "parabolic + risk-off = nasty unwind risk" if risk == "off"
                               else "extension tolerated while tape is firm")
        out["ext"]["neutral"] = _s("neutral", "low", "—")
        for v in ("top", "breakout"):
            out["range"][v] = _s("favorable" if trend_day else "hostile" if risk == "off" else "neutral",
                                 "low", "breakouts follow through on trend days" if trend_day
                                 else "high-zone names unwind in risk-off" if risk == "off" else "—")
        out["range"]["low"] = _s("neutral", "low", "—")
        out["range"]["mid"] = _s("neutral", "low", "—")
        out["range"]["high"] = out["range"].get("mid", _s("neutral", "low", "upper-range, not breakout"))
    else:
        for f, vs in (("mom", ("uptrend", "downtrend", "mixed")),
                      ("ext", ("extreme", "extended", "washed", "neutral")),
                      ("range", ("deep_low", "low", "mid", "high", "top", "breakout"))):
            out[f].update({v: _s("unknown", "low", "no general predict") for v in vs})

    # -- geo: only scored when the event scanner flags a region --
    out["geo"]["US"] = _s("favorable" if risk == "on" else "hostile" if risk == "off" else "neutral" if risk_known else "unknown",
                          "low", f"mirrors general risk-{risk} [general predict]")
    if sig.get("china_event_dir"):
        d = sig["china_event_dir"]
        out["geo"]["ADR-China"] = _s("hostile" if d == "bear" else "favorable", "medium",
                                     f"high-impact China event(s) lean {d} [event scanner]")
    else:
        out["geo"]["ADR-China"] = _s("neutral", "low", "no high-impact China event flagged")

    return out


def build_gates(sig: dict | None = None) -> dict:
    """Keys join.py actually reads, plus human notes."""
    sig = sig or {}
    risk = sig.get("risk")
    return {
        "earn:today": "reports today — event risk, not a segment bet; size down or skip",
        "earn:this_week": "reports within a week — flag, expect gap moves",
        "liq:low": "thin dollar volume — gaps on news, hard to exit; down-rank",
        "rvol:hot": "abnormal participation — moves are 'real' but confirm direction first",
        "ext:extreme + risk-off": "parabolic names into a hostile tape = veto longs",
        "elevated_short_caution": risk == "off",
        "earnings_proximity": True,
        "veto_earn_today": True,
        "veto_extreme_risk_off": risk == "off",
    }


# ---------------------------------------------------------------- output

_EMOJI = {"favorable": "🌤️", "neutral": "⛅", "hostile": "🌧️", "unknown": "❔"}


def write_outputs(date_str: str, sig: dict, stances: dict,
                  gates: dict, gaps: list[str]) -> tuple[Path, Path]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "date": date_str,
        "generated_at": datetime.now(ET).isoformat(),
        "stance_vocabulary": ["favorable", "neutral", "hostile", "unknown"],
        "signals": sig,
        "stances": stances,
        "gates": gates,
        "data_gaps": gaps,
    }
    js = OUT_DIR / f"{date_str}_weather.json"
    js.write_text(json.dumps(payload, indent=1, ensure_ascii=False), encoding="utf-8")

    L = [f"# Weather report — {date_str}", "",
         "Is today good for each *kind* of stock? Labels come from "
         "`data/universe/`; this file is the daily regime record the "
         "backtest will grade.", ""]
    L += ["## Snapshot", ""]
    fg = sig.get("fear_greed")
    L.append(f"- **Risk state:** {sig['risk'].upper()}"
             + (f" (general predict {sig.get('general_direction')} "
                f"score {sig.get('general_score'):+.1f}, "
                f"conf {sig.get('general_confidence')})"
                if sig.get("general_score") is not None else ""))
    L.append(f"- **Yields:** {sig['yields']} ({sig.get('yields_source')}) | "
             f"**Dollar:** {sig['dollar']} ({sig.get('dollar_source')}) | "
             f"**Oil:** {sig.get('oil')} | **VIX:** {sig['vix']}"
             + (f" (ratio {sig['vix_ratio']:.2f} via {sig.get('vix_ratio_source')})"
                if sig.get("vix_ratio") else "")
             + (f" spot {sig.get('vix_spot')}" if sig.get("vix_spot") else ""))
    L.append(f"- **Fear & Greed:** " + (f"{fg:.0f} ({sig.get('fear_greed_label')})"
                                        if fg is not None else "n/a")
             + f" | **Yield/SPX 5d corr:** "
             + (f"{sig['regime_corr']:+.2f}" if sig.get("regime_corr") is not None else "n/a"))
    L.append(f"- **High-impact events:** {sig.get('events_bull', 0)} bullish vs "
             f"{sig.get('events_bear', 0)} bearish"
             + (f" | China: {sig['china_event_dir']}" if sig.get("china_event_dir") else ""))
    if gaps:
        L.append(f"- ⚠️ **Data gaps:** {', '.join(gaps)}")
    L.append("")

    def _table(title: str, fam: str):
        L.extend([f"## {title}", "", "| Label | Weather | Conf | Why |",
                  "|---|---|---|---|"])
        for value, s in stances[fam].items():
            L.append(f"| {fam}:{value} | {_EMOJI[s['stance']]} {s['stance']} "
                     f"| {s['confidence']} | {s['why']} |")
        L.append("")

    _table("Sectors", "sector")
    _table("Size", "size")
    _table("Beta & volatility", "beta")
    _table("Short interest (multiplier, not direction)", "short")
    _table("Profitability & style", "profit")
    _table("Style (growth/value)", "style")
    _table("Leverage", "lev")
    _table("Momentum state", "mom")
    _table("Extension state", "ext")
    _table("52-week zone", "range")
    _table("Geography", "geo")

    L += ["## Gates (always-on cautions)", ""]
    for g, note in gates.items():
        L.append(f"- **{g}** — {note}")
    L.append("")

    md = OUT_DIR / f"{date_str}_weather.md"
    md.write_text("\n".join(L) + "\n", encoding="utf-8")

    for src, name in ((js, "latest.json"), (md, "latest.md")):
        (OUT_DIR / name).write_text(src.read_text(encoding="utf-8"),
                                    encoding="utf-8")
    return js, md


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    args = ap.parse_args()
    date_str = args.date
    if not date_str:
        d = _load_json(SCOREBOARD) or {}
        dates = sorted(r.get("date", "") for r in d.get("runs", [])
                       if r.get("topic") == "general")
        date_str = dates[-1] if dates else datetime.now(ET).date().isoformat()

    rules = _load_json(RULES_PATH) or {}
    th = rules.get("thresholds", {})
    sig, gaps = derive_signals(date_str, th)
    stances = build_stances(sig, th)
    try:
        from .judge_apply import load_or_parse
        j = load_or_parse(date_str)
        for sec, pol in (j.get("sector_tilts") or {}).items():
            if sec not in stances.get("sector", {}):
                continue
            if pol in ("bearish", "hawkish"):
                stances["sector"][sec] = _s("hostile", "medium", f"news_judge SECTOR {sec} [{pol}]")
            elif pol == "bullish":
                stances["sector"][sec] = _s("favorable", "medium", f"news_judge SECTOR {sec} [{pol}]")
        if j.get("risk_tilt") == "off" and sig.get("risk") == "mixed":
            sig["risk"] = "off"
            sig["risk_source"] = "news_judge"
            gaps.append("risk tilted off by news_judge")
        elif j.get("risk_tilt") == "on" and sig.get("risk") == "mixed":
            sig["risk"] = "on"
            sig["risk_source"] = "news_judge"
    except Exception as e:
        print(f"[weather] judge overlay skipped: {e}")
    gates = build_gates(sig)
    js, md = write_outputs(date_str, sig, stances, gates, gaps)
    n_fav = sum(1 for f in stances.values() for s in f.values()
                if s["stance"] == "favorable")
    n_host = sum(1 for f in stances.values() for s in f.values()
                 if s["stance"] == "hostile")
    n_unk = sum(1 for f in stances.values() for s in f.values()
                if s["stance"] == "unknown")
    print(f"[weather] {date_str}: risk={sig['risk']} yields={sig['yields']} "
          f"vix={sig['vix']} | {n_fav} favorable / {n_host} hostile / "
          f"{n_unk} unknown -> {js.name}, {md.name}")


if __name__ == "__main__":
    main()
