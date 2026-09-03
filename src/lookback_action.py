"""Authoritative 09:30 BUY / SELL / NO BUY / HOLD from lookback + mine.

Only uses what the screenshot already shows before the open: cameras,
setups, hall-pass lane, marks, Cond. Never same-day Change%, forward
returns, or the same-day 1d BUY list.

Featured long/fade setups come from ticker_lookback_mine (the rules
that paid market-wide). Lattice lanes apply from 2026-08-31.

Tune via PRESETS or 00_grounding/lookback_action_params.json.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from . import ticker_lookback as tl
from . import ticker_lookback_setups as setups

ROOT = Path(__file__).resolve().parent.parent
PARAMS_PATH = ROOT / "00_grounding" / "lookback_action_params.json"

ACTIONS = ("BUY", "SELL", "NO BUY", "HOLD")
OPEN_CLOCK = "09:30 ET"
CLOSE_CLOCK = "16:00 ET"
BUY_LANES = (
    "standard",
    "group_leader",
    "catalyst",
    "catalyst_exception",
)
HORIZONS = ("1d", "3d", "1w")

DEFAULTS: dict[str, Any] = {
    "min_long_edge_1d": 1.50,
    "probable_is_buy": True,
    "fade_is_sell": True,
    "blocked_is_no_buy": True,
    "long_setup_without_lane": True,
    "min_printed_boxes": 3,
    # Regime gates (need params["_regime"][date] context, see below).
    # None = gate off. Validated on the 16-session mover window:
    # the SELL exhaustion veto doubles per-day SELL pnl; a BUY gate on
    # the premarket predict did NOT validate (blocks good days too).
    "sell_max_spy_down_streak": None,
    "buy_min_predict_score": None,
}

PRESETS: dict[str, dict[str, Any]] = {
    "featured": {
        **DEFAULTS,
        "label": "featured longs + fade SELL + lattice BUY",
    },
    "gated": {
        **DEFAULTS,
        "sell_max_spy_down_streak": 3,
        "label": "featured + SELL veto when SPY fell 3+ straight sessions "
                 "into the open (exhaustion bounce risk)",
    },
    "strict": {
        **DEFAULTS,
        "min_long_edge_1d": 2.00,
        "probable_is_buy": False,
        "label": "only ≥+2.0 1d setups; probable is not BUY",
    },
    "setups": {
        **DEFAULTS,
        "probable_is_buy": False,
        "blocked_is_no_buy": False,
        "long_setup_without_lane": True,
        "lane_can_buy": False,
        "label": "mined setups only (ignore hall pass)",
    },
    "lane": {
        **DEFAULTS,
        "min_long_edge_1d": 99.0,
        "long_setup_without_lane": False,
        "fade_is_sell": True,
        "label": "lattice lane only; fade still SELL",
    },
    "loose": {
        **DEFAULTS,
        "min_long_edge_1d": 0.80,
        "label": "include 🔵 stretch / weaker longs",
    },
}

_PARAM_FILE: dict[str, Any] | None = None


def load_param_file() -> dict[str, Any]:
    global _PARAM_FILE
    if _PARAM_FILE is not None:
        return _PARAM_FILE
    if not PARAMS_PATH.is_file():
        _PARAM_FILE = {}
        return _PARAM_FILE
    try:
        _PARAM_FILE = json.loads(PARAMS_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        _PARAM_FILE = {}
    return _PARAM_FILE


def default_preset_name() -> str:
    return str(load_param_file().get("default") or "featured")


def preset_params(name: str | None = None) -> dict[str, Any]:
    key = name or default_preset_name()
    file_presets = (load_param_file().get("presets") or {})
    if key in file_presets and isinstance(file_presets[key], dict):
        base = dict(PRESETS.get(key) or DEFAULTS)
        base.update(file_presets[key])
        return base
    if key in PRESETS:
        return dict(PRESETS[key])
    return dict(PRESETS["featured"])


def _printed_n(day: dict) -> int:
    boxes = day.get("boxes") or {}
    return sum(
        1 for key, _ in tl.BOX_COLS
        if boxes.get(key) in ("good", "bad", "neutral")
    )


def _matched_setups(day: dict) -> list[dict[str, Any]]:
    raw = day.get("setups")
    if raw is None:
        return setups.match_day(day)
    return list(raw or [])


def _edge(setup: dict, key: str = "edge_1d") -> float:
    try:
        return float(setup.get(key) or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _buy_lanes(params: dict) -> tuple[str, ...]:
    lanes = list(BUY_LANES)
    if params.get("probable_is_buy", True):
        lanes.append("probable")
    return tuple(lanes)


def _regime_for(day: dict, p: dict) -> dict:
    """Per-date regime context injected by the caller as params['_regime'].

    {date: {"spy_down_streak": int, "predict_dir": str|None,
            "predict_score": float|None}} — all knowable at 09:30 ET.
    """
    reg = p.get("_regime") or {}
    return reg.get(str(day.get("date") or "")[:10]) or {}


def action_call(day: dict, params: dict | None = None) -> dict[str, Any]:
    """Return {action, reason, horizon, setups} from 09:30-only fields."""
    p = {**DEFAULTS, **(params or {})}
    regime = _regime_for(day, p)
    if (day.get("class") == "no_data" and _printed_n(day) == 0) or (
        _printed_n(day) < int(p.get("min_printed_boxes") or 0)
        and not day.get("lane")
        and not day.get("setups")
    ):
        if _printed_n(day) == 0 and day.get("class") in (None, "no_data", "no_print"):
            return {
                "action": "HOLD",
                "reason": "no 09:30 print",
                "horizon": "1d",
                "setups": [],
            }

    matched = _matched_setups(day)
    longs = [
        s for s in matched
        if s.get("verdict") == "long"
        and _edge(s) >= float(p.get("min_long_edge_1d") or 0)
    ]
    fades = [s for s in matched if s.get("verdict") == "fade"]
    longs.sort(key=_edge, reverse=True)
    lane = day.get("lane")
    lattice = bool(day.get("lattice_live"))

    if fades and p.get("fade_is_sell", True):
        names = ", ".join(s.get("short") or s.get("label") or s.get("id") for s in fades)
        max_streak = p.get("sell_max_spy_down_streak")
        streak = int(regime.get("spy_down_streak") or 0)
        if max_streak and streak >= int(max_streak):
            return {
                "action": "HOLD",
                "reason": (f"fade vetoed: SPY down {streak} straight "
                           "sessions into the open — exhaustion bounce risk"),
                "horizon": "1d",
                "setups": [s.get("id") for s in fades],
            }
        return {
            "action": "SELL",
            "reason": f"fade: {names}",
            "horizon": "1d",
            "setups": [s.get("id") for s in fades],
        }

    if lane == "blocked" and p.get("blocked_is_no_buy", True):
        return {
            "action": "NO BUY",
            "reason": "hall pass blocked",
            "horizon": "1d",
            "setups": [s.get("id") for s in matched],
        }

    buy_cut = p.get("buy_min_predict_score")
    if buy_cut is not None:
        ps = regime.get("predict_score")
        if ps is not None and float(ps) < float(buy_cut):
            return {
                "action": "HOLD",
                "reason": (f"regime gate: premarket predict score "
                           f"{float(ps):+.2f} < {float(buy_cut):+.2f}"),
                "horizon": "1d",
                "setups": [s.get("id") for s in matched],
            }

    if p.get("lane_can_buy", True) and lane in _buy_lanes(p):
        extra = ""
        if longs:
            extra = "; " + ", ".join(
                s.get("short") or s.get("label") or "" for s in longs[:2]
            )
        return {
            "action": "BUY",
            "reason": f"lane={lane}{extra}",
            "horizon": "1d",
            "setups": [s.get("id") for s in longs],
        }

    if longs and (
        (not lattice and p.get("long_setup_without_lane", True))
        or (lattice and p.get("long_setup_without_lane", True) and lane != "blocked")
    ):
        top = longs[0]
        return {
            "action": "BUY",
            "reason": (
                f"setup {top.get('short') or top.get('label')} "
                f"(1d edge { _edge(top):+.2f})"
            ),
            "horizon": "1d",
            "setups": [s.get("id") for s in longs],
        }

    if lattice and lane is None:
        return {
            "action": "HOLD",
            "reason": "lattice live, no hall pass",
            "horizon": "1d",
            "setups": [s.get("id") for s in matched],
        }
    return {
        "action": "HOLD",
        "reason": "no featured long / fade / lane",
        "horizon": "1d",
        "setups": [s.get("id") for s in matched],
    }


def attach_actions(payload: dict, params: dict | None = None) -> dict:
    """Stamp every lookback day with action_call / action_reason."""
    p = params if params is not None else preset_params()
    for rec in payload.get("names") or []:
        for day in rec.get("days") or []:
            packed = action_call(day, params=p)
            day["action_call"] = packed["action"]
            day["action_reason"] = packed["reason"]
            day["action_horizon"] = packed.get("horizon") or "1d"
            day["action_stamp"] = session_stamp(day.get("date"), OPEN_CLOCK)
            day["action_label"] = format_action(packed["action"], day.get("date"))
    payload["action_params"] = {k: v for k, v in p.items()
                                if k != "label" and not k.startswith("_")}
    payload["action_preset"] = p.get("label") or default_preset_name()
    return payload


def ensure_actions(payload: dict, params: dict | None = None) -> dict:
    names = payload.get("names") or []
    days = [d for rec in names for d in (rec.get("days") or [])]
    if days and any(d.get("action_call") not in ACTIONS for d in days):
        setups.ensure_setups(payload)
        attach_actions(payload, params=params)
    return payload


def grade_call(action: str, fwd: dict | None, *, eps: float = 0.0) -> dict[str, bool | None]:
    """True when BUY and return > eps, or SELL and return < -eps."""
    out: dict[str, bool | None] = {}
    fwd = fwd or {}
    for h in HORIZONS:
        raw = fwd.get(h)
        try:
            ret = None if raw is None else float(raw)
        except (TypeError, ValueError):
            ret = None
        if ret is None or action not in ("BUY", "SELL"):
            out[h] = None
        elif action == "BUY":
            out[h] = ret > eps
        else:
            out[h] = ret < -eps
    return out


def session_stamp(date, clock: str = OPEN_CLOCK) -> str:
    d = str(date or "")[:10]
    return f"{d} {clock}" if d else "—"


def format_action(action: str | None, date) -> str:
    """BUY/SELL/HOLD · 2026-08-17 09:30 ET — the call is known at the open."""
    verb = str(action or "").strip()
    if not verb or verb == "—":
        return "—"
    return f"{verb} · {session_stamp(date, OPEN_CLOCK)}"


def format_price(px, date, clock: str = CLOSE_CLOCK) -> str:
    if px is None:
        return "—"
    try:
        return f"${float(px):,.2f} · {session_stamp(date, clock)}"
    except (TypeError, ValueError):
        return "—"


def ret_tone(pct) -> str:
    return tl.price_tone(pct)


def ret_icon(pct) -> str:
    return tl.BOX_ICON.get(ret_tone(pct), tl.BOX_ICON["missing"])


def format_ret(pct, date, clock: str = CLOSE_CLOCK, *, marked: bool = True) -> str:
    if pct is None:
        return "—"
    try:
        text = f"{float(pct):+.2f}% · {session_stamp(date, clock)}"
    except (TypeError, ValueError):
        return "—"
    return f"{ret_icon(pct)} {text}" if marked else text


def format_open_close(pct, date, *, marked: bool = True) -> str:
    if pct is None:
        return "—"
    try:
        d = str(date or "")[:10]
        text = f"{float(pct):+.2f}% · {d} 09:30→16:00 ET"
    except (TypeError, ValueError):
        return "—"
    return f"{ret_icon(pct)} {text}" if marked else text


def cond_tally(day: dict | None) -> str:
    cond = (day or {}).get("condition")
    if not cond:
        cond = tl.general_condition((day or {}).get("boxes") or {})
    if not cond or cond.get("tone") == "missing" or not cond.get("n"):
        return "—"
    return f"{cond.get('good', 0)}/{cond.get('neutral', 0)}/{cond.get('bad', 0)}"


def action_tone(action: str) -> str:
    return {
        "BUY": "good",
        "SELL": "bad",
        "NO BUY": "bad",
        "HOLD": "neutral",
    }.get(str(action or ""), "missing")


# ---------------------------------------------------------- conviction ----

_LANE_BONUS = {
    "group_leader": 1.0,
    "catalyst": 0.8,
    "catalyst_exception": 0.8,
    "standard": 0.5,
    "probable": 0.25,
}


def conviction(day: dict, packed: dict | None = None) -> float:
    """0..~4 conviction score for ranking same-day calls. 09:30-only inputs:
    strongest matched setup edge, hall-pass lane quality, condition mix.
    Fresh rips (big edge, leader/catalyst lane, clean condition) rank first.
    """
    action = (packed or {}).get("action") or day.get("action_call")
    matched = _matched_setups(day)
    cond = day.get("condition") or {}
    good = float(cond.get("good") or 0)
    bad = float(cond.get("bad") or 0)
    lane = day.get("lane")
    if action == "BUY":
        longs = [s for s in matched if s.get("verdict") == "long"]
        edge = max((_edge(s) for s in longs), default=0.0)
        return round(edge + _LANE_BONUS.get(lane, 0.0) + 0.2 * (good - bad), 3)
    if action == "SELL":
        fades = [s for s in matched if s.get("verdict") == "fade"]
        return round(1.0 + 0.5 * len(fades) + 0.2 * (bad - good), 3)
    return 0.0
