"""Deterministic ticket-level lesson FILTER.

A lesson has imprinted when a ticket that would have printed yesterday
does not print today (or size→0), and the blotter tags which lesson +
which predicate. Pure functions. Never raise. Missing features = pass,
not veto. No LLM at ticket time.
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
REGISTRY = ROOT / "00_grounding" / "ticket_filters.json"

_REG_CACHE: dict | None = None
_FEAT_CACHE: dict[tuple[str, str], dict] = {}
_RECIPE_INDEX: dict[str, dict] | None = None


def _tick(v) -> str:
    return str(v or "").strip().upper()


def _num(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _side(v, default: str = "long") -> str:
    s = str(v or default or "long").strip().lower()
    if s in ("sell", "short", "down", "cover"):
        return "short"
    return "long"


def load_registry(path: Path | None = None) -> dict:
    """JSON registry. Empty filters on any read error."""
    global _REG_CACHE
    p = Path(path) if path is not None else REGISTRY
    if path is None and _REG_CACHE is not None:
        return _REG_CACHE
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raw = {"filters": []}
    except Exception:
        raw = {"filters": []}
    raw.setdefault("filters", [])
    if path is None:
        _REG_CACHE = raw
    return raw


def reset_caches() -> None:
    global _REG_CACHE, _FEAT_CACHE, _RECIPE_INDEX
    _REG_CACHE = None
    _FEAT_CACHE = {}
    _RECIPE_INDEX = None


def recipe_index() -> dict[str, dict]:
    """factor-mine + combo recipes. Empty on import failure."""
    global _RECIPE_INDEX
    if _RECIPE_INDEX is not None:
        return _RECIPE_INDEX
    out: dict[str, dict] = {}
    try:
        from . import factor_mine as fm
        for rec in fm.build_recipes():
            if rec.get("name"):
                out[rec["name"]] = rec
        try:
            from . import factor_mine_combo as fmc
            for spec in fmc.combo_specs():
                if spec.get("name") and spec["name"] not in out:
                    out[spec["name"]] = fmc.combo_recipe(spec)
        except Exception:
            pass
    except Exception:
        out = {}
    _RECIPE_INDEX = out
    return out


def advertised_ob_short(require, name) -> str | bool | None:
    """Does this recipe advertise the RSI≥70 / overbought / MACD-down short gate?

    None = no recipe info (missing → pass). False = recipe known, no such
    gate. str = which gate.
    """
    try:
        req = require if isinstance(require, dict) else {}
        n = str(name or "").lower()
        if not req and not n:
            return None
        rsi_min = _num(req.get("rsi_min"))
        if req.get("rsi_ob") or (rsi_min is not None and rsi_min >= 70):
            return "rsi_ob"
        if req.get("macd_down"):
            return "macd_down"
        if any(x in n for x in ("rsi_ob", "overbought", "short_rsi_ob")):
            return "name_rsi_ob"
        if any(x in n for x in ("macd_dn", "macd_down", "short_macd_dn")):
            return "name_macd_dn"
        return False
    except Exception:
        return None


def recipe_extra(strategy: str | None = None, src: str | None = None,
                 require: dict | None = None) -> dict:
    """Attach recipe require for C1. Never raise."""
    extra = {"strategy": strategy, "src": src, "recipe_require": require or {}}
    try:
        names = []
        if src:
            names.append(str(src).split(",")[0].strip())
        if strategy:
            names.append(str(strategy).strip())
        idx = recipe_index()
        for name in names:
            kid = idx.get(name) or {}
            if kid.get("require") is not None:
                extra["recipe_require"] = kid.get("require") or {}
                extra["strategy"] = kid.get("name") or name
                break
            if kid:
                extra["strategy"] = kid.get("name") or name
    except Exception:
        pass
    return extra


def _pass(reason: str = "pass", **extra) -> dict:
    out = {
        "action": "pass",
        "reason": reason,
        "features_used": {},
        "lesson_id": None,
        "filter_id": None,
        "audit": None,
    }
    out.update(extra)
    return out


def _air_pocket_from_bars(bars: list[dict]) -> dict:
    """Crash-day range / gap on the last *prior* bar. Never same-day."""
    out = {"range_pct": None, "gap_pct": None, "air_pocket": False}
    if not bars:
        return out
    last = bars[-1]
    c = _num(last.get("close"))
    h = _num(last.get("high"))
    lo = _num(last.get("low"))
    o = _num(last.get("open"))
    if c and c > 0 and h is not None and lo is not None:
        out["range_pct"] = 100.0 * (h - lo) / c
    if len(bars) >= 2 and o is not None:
        prev_c = _num(bars[-2].get("close"))
        if prev_c and prev_c > 0:
            out["gap_pct"] = 100.0 * (o / prev_c - 1.0)
    rng = out["range_pct"]
    gap = out["gap_pct"]
    out["air_pocket"] = bool(
        (rng is not None and rng >= 8.0)
        or (gap is not None and gap <= -8.0)
    )
    return out


def prior_features(ticker: str, date: str, row: dict | None = None) -> dict:
    """Leak-free morning features. Prior RSI / 1d / rvol / air-pocket.

    Overlay a panel / ticket row when it already landed (news, cameras,
    sector). Same-day close is never an input.
    """
    t = _tick(ticker)
    d = str(date or "")[:10]
    empty = {
        "ticker": t, "date": d,
        "rsi": None, "ret_1": None, "rvol": None,
        "range_pct": None, "gap_pct": None,
        "air_pocket": False, "crash": False,
        "news": None, "sector": None, "camera_net": None,
        "camera_support": None, "hard_red": None,
        "src": [],
    }
    if not t or not d:
        return empty
    cached = _FEAT_CACHE.get((t, d))
    if cached is None:
        feat = dict(empty)
        try:
            from . import ohlc_ripper as ohlc
            oh = ohlc.features(t, d)
            if oh.get("ok"):
                feat["rsi"] = _num(oh.get("rsi"))
                feat["ret_1"] = _num(oh.get("ret_1"))
                feat["rvol"] = _num(oh.get("rvol"))
                feat["src"].append("ohlc_prior")
            bars = ohlc.prior_bars(t, d, n=5)
            air = _air_pocket_from_bars(bars)
            feat["range_pct"] = air["range_pct"]
            feat["gap_pct"] = air["gap_pct"]
            feat["air_pocket"] = air["air_pocket"]
        except Exception:
            pass
        _FEAT_CACHE[(t, d)] = feat
        cached = feat
    out = dict(cached)
    out["src"] = list(cached.get("src") or [])
    if isinstance(row, dict):
        rsi = _num(row.get("rsi") if row.get("rsi") is not None else row.get("fv_rsi"))
        if rsi is not None:
            out["rsi"] = rsi
            out["src"].append("row_rsi")
        ret1 = _num(row.get("ohlc_ret_1") if row.get("ohlc_ret_1") is not None
                    else row.get("ret_1"))
        if ret1 is not None:
            out["ret_1"] = ret1
            out["src"].append("row_ret1")
        rvol = _num(row.get("ohlc_rvol") if row.get("ohlc_rvol") is not None
                    else row.get("rvol"))
        if rvol is not None:
            out["rvol"] = rvol
        boxes = row.get("boxes") if isinstance(row.get("boxes"), dict) else {}
        news = row.get("news_prior") or row.get("news") or boxes.get("news")
        if news not in (None, ""):
            out["news"] = str(news).lower()
        if boxes.get("sector"):
            out["sector"] = str(boxes.get("sector")).lower()
        cg = row.get("cond_good")
        cb = row.get("cond_bad")
        if cg is not None or cb is not None:
            try:
                out["camera_net"] = int(cg or 0) - int(cb or 0)
            except (TypeError, ValueError):
                pass
        support = None
        if row.get("blue") is True or boxes.get("news") == "good" or out.get("news") == "good":
            support = True
        elif cg is not None:
            try:
                support = int(cg) >= 3
            except (TypeError, ValueError):
                support = False
        elif boxes.get("news") in ("bad", "neutral", "missing"):
            support = False
        if support is not None:
            out["camera_support"] = support
        if row.get("hard_red") is not None:
            out["hard_red"] = bool(row.get("hard_red"))
    ret1 = _num(out.get("ret_1"))
    gap = _num(out.get("gap_pct"))
    crash = bool(ret1 is not None and ret1 <= -8.0) or bool(gap is not None and gap <= -8.0)
    out["crash"] = crash
    if out.get("air_pocket") and not crash:
        out["air_pocket"] = False
    return out


def _crash_hit(feat: dict, spec: dict) -> bool:
    """Down crash only. A wide *up* bar is not an air-pocket.

    Hits when prior 1d ≤ ret_1_max, or a gap ≤ gap_pct_max. Range air-pocket
    counts only on a down prior bar that also clears ret_1_max. Missing
    legs do not veto.
    """
    ret_max = _num((spec or {}).get("ret_1_max"))
    rng_min = _num((spec or {}).get("range_pct_min"))
    gap_max = _num((spec or {}).get("gap_pct_max"))
    ret1 = _num(feat.get("ret_1"))
    rng = _num(feat.get("range_pct"))
    gap = _num(feat.get("gap_pct"))
    if ret_max is not None and ret1 is not None and ret1 <= ret_max:
        return True
    if gap_max is not None and gap is not None and gap <= gap_max:
        return True
    if (rng_min is not None and rng is not None and rng >= rng_min
            and ret_max is not None and ret1 is not None and ret1 <= ret_max):
        return True
    return False


def _camera_support(feat: dict) -> bool | None:
    if feat.get("camera_support") is True:
        return True
    if feat.get("camera_support") is False:
        return False
    news = feat.get("news")
    if news == "good":
        return True
    if news in ("bad", "neutral"):
        return False
    net = feat.get("camera_net")
    if net is not None:
        return bool(net > 0)
    return None


def evaluate(side: str, features: dict | None,
             registry: dict | None = None,
             extra: dict | None = None) -> dict:
    """First matching enabled filter wins. Never raise."""
    try:
        feat = dict(features or {})
        want = _side(side, feat.get("side") or "long")
        extra = extra if isinstance(extra, dict) else {}
        if extra.get("new_entry") is False:
            return _pass("not_new_entry")
        reg = registry if isinstance(registry, dict) else load_registry()
        for spec in reg.get("filters") or []:
            if not isinstance(spec, dict) or spec.get("enabled") is False:
                continue
            if _side(spec.get("side") or want) != want:
                continue
            req = spec.get("require") or {}
            used = {}
            rsi = _num(feat.get("rsi"))
            ret1 = _num(feat.get("ret_1"))
            rsi_max = _num(req.get("rsi_max"))
            rsi_min = _num(req.get("rsi_min"))
            ret_min = _num(req.get("ret_1_min"))
            if rsi_max is not None:
                if rsi is None:
                    continue
                used["rsi"] = rsi
                if rsi > rsi_max:
                    continue
            if rsi_min is not None:
                if rsi is None:
                    continue
                used["rsi"] = rsi
                if rsi < rsi_min:
                    continue
            if req.get("crash"):
                if not _crash_hit(feat, spec.get("crash") or {}):
                    continue
                used["ret_1"] = ret1
                used["range_pct"] = feat.get("range_pct")
                used["gap_pct"] = feat.get("gap_pct")
                used["crash"] = True
            if ret_min is not None:
                if ret1 is None:
                    continue
                used["ret_1"] = ret1
                if ret1 < ret_min:
                    continue
            if req.get("no_camera_support"):
                support = _camera_support(feat)
                if support is True:
                    continue
                if support is not None:
                    used["camera_support"] = support
            if req.get("no_news_support"):
                news = feat.get("news")
                if news in (None, "", "missing"):
                    used["news"] = "missing"
                elif str(news).lower() == "good":
                    continue
                else:
                    used["news"] = str(news).lower()
            if req.get("hard_red"):
                hr = feat.get("hard_red")
                if extra.get("hard_red") is not None:
                    hr = bool(extra.get("hard_red"))
                if hr is not True:
                    continue
                used["hard_red"] = True
            if req.get("sector_against") or req.get("sector_good"):
                sector = str(feat.get("sector") or "").lower()
                if not sector:
                    continue
                if req.get("sector_good") and sector != "good":
                    continue
                if req.get("sector_against"):
                    against = (want == "short" and sector == "good") or (
                        want == "long" and sector == "bad")
                    if not against:
                        continue
                used["sector"] = sector
            if req.get("recipe_ob_or_macd_dn"):
                gate = advertised_ob_short(
                    extra.get("recipe_require") or feat.get("recipe_require"),
                    extra.get("strategy") or extra.get("src") or feat.get("strategy"),
                )
                if not gate:
                    continue
                used["recipe_gate"] = gate
            if req.get("camera_net_min") is not None:
                net = feat.get("camera_net")
                if net is None:
                    continue
                try:
                    need = float(req["camera_net_min"])
                except (TypeError, ValueError):
                    continue
                if float(net) < need:
                    continue
                if req.get("camera_support_and_net") and _camera_support(feat) is not True:
                    continue
                if req.get("unless_news_bad") and str(feat.get("news") or "") == "bad":
                    continue
                used["camera_net"] = net
            if req.get("news_bad"):
                news = feat.get("news")
                if news in (None, "", "missing"):
                    continue
                if str(news).lower() != "bad":
                    continue
                used["news"] = str(news).lower()
            if req.get("tape_against"):
                tape = feat.get("tape_anchor")
                if tape in (None, ""):
                    continue
                tl = str(tape).lower()
                if want == "short" and tl not in ("good", "up", "constructive"):
                    continue
                if want == "long" and tl not in ("bad", "down", "hostile"):
                    continue
                used["tape_anchor"] = tl
            action = str(spec.get("action") or "block").lower()
            if action not in ("block", "pause"):
                action = "block"
            lesson = spec.get("lesson_id") or spec.get("id")
            audit_rule = spec.get("audit") or spec.get("id")
            reason = (
                f"{_tick(feat.get('ticker'))} {want.upper()} {feat.get('date')} "
                f"BLOCKED by {lesson} "
                f"(rsi={used.get('rsi')}, 1d={used.get('ret_1')}, "
                f"rule={audit_rule})"
            )
            return {
                "action": action,
                "reason": reason,
                "features_used": used,
                "lesson_id": lesson,
                "filter_id": spec.get("id"),
                "audit": audit_rule,
            }
        return _pass()
    except Exception as e:  # noqa: BLE001 — filter must never raise
        return _pass(reason=f"filter_error:{type(e).__name__}")


def decide_row(row: dict, date: str, default_side: str = "long",
               registry: dict | None = None,
               panel_row: dict | None = None,
               extra: dict | None = None) -> dict:
    """Evaluate one ticket row. Never raise."""
    try:
        if not isinstance(row, dict):
            return _pass("not_a_row")
        t = _tick(row.get("ticker") or row.get("symbol"))
        if not t:
            return _pass("no_ticker")
        side = _side(row.get("kid_side") or row.get("side") or default_side)
        feat = prior_features(t, date, panel_row if panel_row is not None else row)
        feat["ticker"] = t
        feat["date"] = str(date or "")[:10]
        feat["side"] = side
        merged = dict(extra or {})
        merged.update(recipe_extra(
            merged.get("strategy"),
            row.get("src") or merged.get("src"),
            merged.get("recipe_require"),
        ))
        return evaluate(side, feat, registry=registry, extra=merged)
    except Exception as e:  # noqa: BLE001
        return _pass(reason=f"filter_error:{type(e).__name__}")


def filter_rows(rows, date: str, default_side: str = "long",
                registry: dict | None = None,
                panel_by_ticker: dict | None = None,
                extra: dict | None = None) -> tuple[list[dict], list[dict]]:
    """Split rows into (kept, blocked). Never raise."""
    kept: list[dict] = []
    blocked: list[dict] = []
    try:
        for row in rows or []:
            if not isinstance(row, dict):
                kept.append(row)
                continue
            t = _tick(row.get("ticker") or row.get("symbol"))
            panel = None
            if panel_by_ticker and t:
                panel = panel_by_ticker.get(t)
            dec = decide_row(row, date, default_side, registry, panel, extra)
            if dec.get("action") in ("block", "pause"):
                item = dict(row)
                item["lesson_filter"] = dec
                blocked.append(item)
            else:
                kept.append(row)
    except Exception:
        return list(rows or []), []
    return kept, blocked


def stamp_strategy(rec: dict, date: str | None = None,
                   registry: dict | None = None,
                   panel_by_ticker: dict | None = None) -> dict:
    """Filter buy/sell on one strategy ticket. Flatten sells are exits."""
    try:
        rec = dict(rec or {})
        d = str(date or rec.get("date") or rec.get("session_open") or "")[:10]
        family = str(rec.get("family") or "")
        default = _side(rec.get("side") or "long")
        extra = {
            "hard_red": bool(rec.get("hard_red") or rec.get("sit")),
            "new_entry": True,
        }
        extra.update(recipe_extra(rec.get("name"), None, rec.get("require")))
        buys, b_block = filter_rows(
            rec.get("buy") or [], d, default, registry, panel_by_ticker, extra)
        if family == "flatten":
            sells = list(rec.get("sell") or [])
            s_block: list[dict] = []
        else:
            sells, s_block = filter_rows(
                rec.get("sell") or [], d, "short", registry, panel_by_ticker, extra)
        blocked = b_block + s_block
        rec["buy"] = buys
        rec["sell"] = sells
        rec["buy_n"] = len(buys)
        rec["sell_n"] = len(sells)
        if blocked:
            rec["blocked"] = [
                {
                    "ticker": _tick(x.get("ticker")),
                    "side": _side(x.get("kid_side") or x.get("side") or default),
                    "lesson_id": (x.get("lesson_filter") or {}).get("lesson_id"),
                    "filter_id": (x.get("lesson_filter") or {}).get("filter_id"),
                    "action": (x.get("lesson_filter") or {}).get("action"),
                    "reason": (x.get("lesson_filter") or {}).get("reason"),
                    "features_used": (x.get("lesson_filter") or {}).get("features_used"),
                    "src": x.get("src"),
                }
                for x in blocked
            ]
            rec["blocked_n"] = len(blocked)
            rec["blocked_audit"] = [
                (x.get("lesson_filter") or {}).get("reason")
                for x in blocked if (x.get("lesson_filter") or {}).get("reason")
            ]
        return rec
    except Exception:
        return rec if isinstance(rec, dict) else {}


def apply_to_payload(payload: dict, registry: dict | None = None) -> dict:
    """Single dashboard hook: filter every strategy in a tickets payload."""
    try:
        payload = dict(payload or {})
        date = str(payload.get("date") or payload.get("session_open") or "")[:10]
        by = payload.get("strategies") or {}
        if not isinstance(by, dict):
            return payload
        n_block = 0
        for name, rec in list(by.items()):
            if not isinstance(rec, dict):
                continue
            stamped = stamp_strategy(rec, date, registry)
            by[name] = stamped
            n_block += int(stamped.get("blocked_n") or 0)
        payload["strategies"] = by
        payload["lesson_filter"] = {
            "n_blocked": n_block,
            "registry": "00_grounding/ticket_filters.json",
        }
        return payload
    except Exception:
        return payload if isinstance(payload, dict) else {}
