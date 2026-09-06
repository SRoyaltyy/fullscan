"""Leak-free regime-survival mine — what still wins across macro buckets.

Research only. Does not change live flatten_robust or any production recipe.

Every *selection* field is knowable at 09:30 ET on session D:

  * feature_asof cameras / marks / prior-session Finviz buckets
  * morning general S from the flatten 09:30 packet (factor_mine mornings)
  * weather risk derived from that same morning S + weather_rules thresholds
  * SPY 1d prior = prior-session Finviz Change% for SPY (never D's close)

Forward 1d / 3d / xs_* are outcomes only. Same-day Change%, Gap, RelVol,
printed book, and weather.json files stamped after 09:30 are never gates.

CLI:
  python -m src.regime_survive_mine
  python -m src.regime_survive_mine --write
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
ASOF_DIR = ROOT / "data" / "feature_asof"
EXPORT_DIR = ROOT / "data" / "exports"
WEATHER_RULES = ROOT / "00_grounding" / "weather_rules.json"
FACTOR_MINE_JSON = ROOT / "03_scoreboard" / "factor_mine.json"
FLATTEN_JSON = ROOT / "03_scoreboard" / "flatten_lookback_action.json"
HIT_MD_FALLBACK = ROOT / "03_scoreboard" / "HIT_BOARD.md"
OUT_MD = ROOT / "03_scoreboard" / "REGIME_SURVIVE.md"
OUT_JSON = ROOT / "03_scoreboard" / "regime_survive.json"
ET = ZoneInfo("America/New_York")

# Flatten long_gate is +1; hard-red sit is −3. Flat is the middle.
S_UP = 1.0
S_DOWN = -1.0
SPY_FLAT = 0.25  # |prior SPY Change%| below this is flat
MIN_N = 40
MIN_N_RARE = 25
MIN_BUCKET_DATES = 2
HORIZONS = ("1d", "3d")
CLIP = 30.0  # winsorize one-name blow-ups for mean (hit rate unclipped)
LEAK_COLS = frozenset({
    "ret_1d", "ret_2d", "ret_3d", "ret_1w", "ret_2w",
    "xs_1d", "xs_2d", "xs_3d", "xs_1w", "xs_2w",
    "day_change", "gainer_change", "outcome", "Change", "Change%",
    "Gap", "RelVol", "close", "Close",
})
CAMERA_KEYS = ("join", "gen", "sector", "ab", "peer", "vol", "heat")
TONE_COLS = CAMERA_KEYS + ("cond", "region")
HORIZON_LAG = {"1d": 1, "3d": 3}
SPY_ABS_MAX = 20.0  # reject sector Mag HIT% (36.4 / 40.0) as SPY
MARK_BOOLS = (
    "blue", "white", "alarm", "fade", "first_crack", "steady", "fat",
    "ab_good", "peer_good", "vol_good", "join_good", "catal",
    "ins_buy", "ins_sell", "qc_hot",
)
FINVIZ_BUCKETS = (
    "rsi_b", "relvol_b", "sma20_b", "short_b", "gap_b", "perf_w_b", "earn_b",
)

# io mornings where flatten_h5 actually bought and flatten_live sat.
# Hard-red HOLD sits both books; 08-26 / 08-28 are io but leftover could
# not add a share. These seven match the war-room "~7 ungated" cut.
UNGATED_IO_BUYS = (
    "2026-08-13", "2026-08-14", "2026-08-17",
    "2026-08-25", "2026-08-27",
    "2026-09-03", "2026-09-04",
)


def _finite(x):
    if x is None or x == "":
        return None
    if isinstance(x, bool):
        return None
    try:
        if isinstance(x, str):
            raw = x.strip().replace(",", "")
            if raw.endswith("%"):
                raw = raw[:-1]
            v = float(raw)
        else:
            v = float(x)
    except (TypeError, ValueError):
        return None
    if math.isnan(v) or math.isinf(v):
        return None
    return v


def _truthy(x) -> bool:
    if x is True:
        return True
    if x is False or x is None:
        return False
    return str(x).strip().lower() in {"true", "1", "yes", "y"}


def _tone(x) -> str:
    s = str(x or "").strip().lower()
    if s in {"good", "neutral", "bad", "missing"}:
        return s
    return "missing"


def _clip(v, lo=-CLIP, hi=CLIP):
    if v is None:
        return None
    return max(lo, min(hi, float(v)))


def load_weather_thresholds(path: Path | None = None) -> dict:
    raw = json.loads((path or WEATHER_RULES).read_text(encoding="utf-8"))
    th = raw.get("thresholds") or {}
    return {
        "risk_on_score": float(th.get("risk_on_score", 4.0)),
        "risk_off_score": float(th.get("risk_off_score", -4.0)),
    }


def gen_s_bucket(s) -> str:
    """Morning general S → up / flat / down. Blank S is flat (unknown)."""
    v = _finite(s)
    if v is None:
        return "flat"
    if v >= S_UP:
        return "up"
    if v <= S_DOWN:
        return "down"
    return "flat"


def weather_risk_from_s(s, th: dict | None = None) -> str:
    """PIT weather risk from morning S. Does not read post-09:30 weather.json."""
    th = th or load_weather_thresholds()
    v = _finite(s)
    if v is None:
        return "unknown"
    if v >= th["risk_on_score"]:
        return "on"
    if v <= th["risk_off_score"]:
        return "off"
    return "mixed"


def spy_prior_bucket(chg) -> str:
    v = _finite(chg)
    if v is None:
        return "unknown"
    if v > SPY_FLAT:
        return "up"
    if v < -SPY_FLAT:
        return "down"
    return "flat"


def is_weekday(day: str) -> bool:
    try:
        return date.fromisoformat(str(day)[:10]).weekday() < 5
    except ValueError:
        return False


def prior_session(cal: list[str], day: str) -> str | None:
    """Same rule as gainer_capture / factor_mine: last session strictly before D."""
    if day in cal:
        i = cal.index(day)
        return cal[i - 1] if i else None
    earlier = [d for d in cal if d < day]
    return earlier[-1] if earlier else None


def parse_pct(raw) -> float | None:
    return _finite(raw)


def load_spy_change(day: str, export_dir: Path | None = None) -> float | None:
    """Same-day Finviz Change% for SPY — outcome of ``day``, never a D feature."""
    path = (export_dir or EXPORT_DIR) / f"finviz_{day}.csv"
    if not path.exists():
        return None
    with path.open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            if str(row.get("Ticker") or "").strip().upper() == "SPY":
                chg = parse_pct(row.get("Change") or row.get("Change %"))
                if chg is not None and abs(chg) >= SPY_ABS_MAX:
                    return None
                return chg
    return None


def load_hit_actuals() -> dict[str, float]:
    """General-market Actual % only. Ignores the sector HIT% table (those are not SPY)."""
    out: dict[str, float] = {}
    if not HIT_MD_FALLBACK.exists():
        return out
    in_general = False
    for line in HIT_MD_FALLBACK.read_text(encoding="utf-8").splitlines():
        if line.startswith("## General market"):
            in_general = True
            continue
        if in_general and line.startswith("## "):
            break
        if not in_general or not line.startswith("| 2026-"):
            continue
        cells = [c.strip() for c in line.strip("|").split("|")]
        # Date | Pred dir | Mag | Score | Actual % | Actual dir | Dir | Mag
        if len(cells) < 5:
            continue
        date, pred, actual = cells[0], cells[1], cells[4]
        if pred in {"up", "down", "flat", "—", "-"} or cells[2] in {
            "mild", "flat", "notable", "—", "-",
        }:
            v = _finite(actual)
            if v is not None and abs(v) < 20:  # reject 36.4 sector HIT%
                out[date] = v
    return out


def session_calendar(mornings: dict) -> list[str]:
    return sorted(mornings)


def full_calendar(mornings: dict, hit_actuals: dict[str, float] | None = None) -> list[str]:
    """Trading sessions only. Weekend Finviz dumps and HIT blanks stay out.

    2026-08-12 is added when HIT has a general actual so 08-13 has a prior.
    """
    dates = set(mornings)
    if hit_actuals and "2026-08-12" in hit_actuals:
        dates.add("2026-08-12")
    return sorted(dates)


def spy_prior_for(day: str, cal: list[str] | None = None,
                  hit_actuals: dict[str, float] | None = None,
                  export_dir: Path | None = None) -> dict:
    """Walk prior weekdays until a Finviz SPY or HIT general actual exists.

    Never uses D's own close. Weekend dumps (Sat/Sun 0.00 tapes) are skipped.
    ``|Change%| >= 20`` is rejected so sector Mag HIT% cannot become SPY.
    ``cal`` is accepted for older callers; resolution does not require it.
    """
    if isinstance(cal, dict) and hit_actuals is None:
        hit_actuals = cal
        cal = None
    d = date.fromisoformat(str(day)[:10])
    for _ in range(14):
        d -= timedelta(days=1)
        if d.weekday() >= 5:
            continue
        prior = d.isoformat()
        if prior == day:
            raise ValueError("leak: spy prior resolved to the selection date")
        chg = load_spy_change(prior, export_dir)
        src = "finviz_prior" if chg is not None else None
        if chg is None and hit_actuals and prior in hit_actuals:
            v = _finite(hit_actuals[prior])
            if v is not None and abs(v) < SPY_ABS_MAX:
                chg = v
                src = "hit_board_prior"
        if chg is not None:
            return {
                "date": day,
                "prior_session": prior,
                "spy_prior_pct": chg,
                "spy_prior": spy_prior_bucket(chg),
                "spy_src": src or "missing",
            }
    return {
        "date": day,
        "prior_session": None,
        "spy_prior_pct": None,
        "spy_prior": "unknown",
        "spy_src": "missing",
    }


def load_mornings(path: Path | None = None) -> dict[str, dict]:
    doc = json.loads((path or FACTOR_MINE_JSON).read_text(encoding="utf-8"))
    raw = doc.get("mornings") or {}
    th = load_weather_thresholds()
    out = {}
    for date, rec in raw.items():
        s = rec.get("s")
        out[date] = {
            "date": date,
            "s": _finite(s),
            "hard_red": bool(rec.get("hard_red")),
            "flatten_ok": bool(rec.get("flatten_ok")),
            "route": rec.get("route") or "",
            "why": rec.get("why") or "",
            "gen_s": gen_s_bucket(s),
            "weather_risk": weather_risk_from_s(s, th),
        }
    return out


def attach_macro(mornings: dict[str, dict],
                 hit_actuals: dict[str, float] | None = None) -> dict[str, dict]:
    hit = hit_actuals if hit_actuals is not None else load_hit_actuals()
    cal = full_calendar(mornings, hit)
    out = {}
    for date, rec in mornings.items():
        row = dict(rec)
        row.update(spy_prior_for(date, cal, hit))
        if row.get("prior_session") == date:
            raise ValueError(f"leak: spy prior resolved to selection date {date}")
        pct = row.get("spy_prior_pct")
        if pct is not None and abs(float(pct)) >= SPY_ABS_MAX:
            raise ValueError(f"leak/garbage SPY prior on {date}: {pct}")
        out[date] = row
    return out


def load_feature_asof(path: Path | None = None) -> list[dict]:
    """09:30 feature_asof CSVs. Outcome columns stay labeled, never gated on."""
    folder = path or ASOF_DIR
    rows: list[dict] = []
    for csv_path in sorted(folder.glob("*_feature_asof.csv")):
        with csv_path.open(newline="", encoding="utf-8") as fh:
            for rec in csv.DictReader(fh):
                date = str(rec.get("date") or "").strip()
                ticker = str(rec.get("Ticker") or rec.get("ticker") or "").strip().upper()
                if not date or not ticker:
                    continue
                row = {
                    "ticker": ticker,
                    "date": date,
                    "n_print": int(_finite(rec.get("n_print")) or 0),
                    "n_red": int(_finite(rec.get("n_red")) or 0),
                    "sector_name": rec.get("sector_name") or "",
                    "setups": rec.get("setups") or "",
                }
                for cam in TONE_COLS:
                    row[cam] = _tone(rec.get(cam))
                for key in MARK_BOOLS:
                    row[key] = _truthy(rec.get(key))
                for key in FINVIZ_BUCKETS:
                    row[key] = str(rec.get(key) or "missing").strip().lower() or "missing"
                for h in ("1d", "2d", "3d", "1w", "2w"):
                    row[f"ret_{h}"] = _finite(rec.get(f"ret_{h}"))
                    row[f"xs_{h}"] = _finite(rec.get(f"xs_{h}"))
                rows.append(row)
    return rows


def liquid_subset(rows: list[dict]) -> list[dict]:
    """Drop the 08-13/14 mega dump; keep later ~2.7k liquid sessions + overlap."""
    by_date: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_date[r["date"]].append(r)
    core_dates = [d for d, rs in by_date.items() if 500 <= len(rs) <= 4000]
    if not core_dates:
        return [r for r in rows if r.get("n_print", 0) >= 3]
    keep = {r["ticker"] for d in core_dates for r in by_date[d]}
    out = []
    for r in rows:
        if r["date"] in core_dates:
            out.append(r)
        elif r["ticker"] in keep and r.get("n_print", 0) >= 3:
            out.append(r)
    return out


def join_macro(rows: list[dict], macro: dict[str, dict]) -> list[dict]:
    out = []
    for r in rows:
        m = macro.get(r["date"])
        if not m:
            continue
        rec = dict(r)
        rec["s"] = m.get("s")
        rec["gen_s"] = m["gen_s"]
        rec["weather_risk"] = m["weather_risk"]
        rec["spy_prior"] = m["spy_prior"]
        rec["spy_prior_pct"] = m.get("spy_prior_pct")
        rec["flatten_ok"] = m.get("flatten_ok")
        rec["hard_red"] = m.get("hard_red")
        rec["route"] = m.get("route")
        out.append(rec)
    return out


def factor_match(row: dict, spec: dict) -> bool:
    """AND of PIT predicates. Outcome keys are rejected."""
    for key, want in spec.items():
        if key in LEAK_COLS:
            raise ValueError(f"leak: factor spec uses outcome column {key}")
        if key in MARK_BOOLS:
            if bool(row.get(key)) != bool(want):
                return False
        elif key in TONE_COLS or key in FINVIZ_BUCKETS:
            if _tone(row.get(key)) != str(want).lower() and str(row.get(key) or "").lower() != str(want).lower():
                return False
        elif key == "n_red_max":
            if int(row.get("n_red") or 0) > int(want):
                return False
        elif key == "n_print_min":
            if int(row.get("n_print") or 0) < int(want):
                return False
        else:
            raise ValueError(f"unknown factor key {key}")
    return True


def candidate_factors() -> list[dict]:
    """Existing mine tags / combos that are 09:30-knowable."""
    recs = []

    def add(name, spec, family, rare=False):
        recs.append({"name": name, "spec": spec, "family": family, "rare": rare})

    add("blue", {"blue": True}, "mark")
    add("white", {"white": True}, "mark")
    add("alarm", {"alarm": True}, "mark")
    add("fade", {"fade": True}, "mark")
    add("first_crack", {"first_crack": True}, "mark")
    add("steady", {"steady": True}, "stack")
    add("blue+white", {"blue": True, "white": True}, "mark")
    add("blue+not_alarm", {"blue": True}, "mark")  # alarm=False added below
    recs[-1]["spec"] = {"blue": True, "alarm": False}
    add("steady+blue", {"steady": True, "blue": True}, "stack")
    add("n_red=0", {"n_red_max": 0, "n_print_min": 3}, "mark")
    for cam in TONE_COLS:
        for tone in ("good", "bad", "neutral"):
            add(f"{cam}={tone}", {cam: tone}, "camera")
    add("ab_good", {"ab_good": True}, "ab")
    add("peer_good", {"peer_good": True}, "camera")
    add("vol_good", {"vol_good": True}, "camera")
    add("join_good", {"join_good": True}, "camera")
    add("vol=good|ab=good", {"vol": "good", "ab": "good"}, "combo")
    add("gen=bad|vol=good", {"gen": "bad", "vol": "good"}, "combo")
    add("join=bad|vol=good", {"join": "bad", "vol": "good"}, "combo")
    add("ab=good|peer=good", {"ab": "good", "peer": "good"}, "combo")
    add("ab=good|vol=good", {"ab": "good", "vol": "good"}, "combo")
    add("blue|heat=good", {"blue": True, "heat": "good"}, "combo")
    add("blue|heat=bad", {"blue": True, "heat": "bad"}, "combo", rare=True)
    add("hot+ab+peer", {"relvol_b": "hot", "ab": "good", "peer": "good"}, "cross", rare=True)
    add("blue+relvol=hot", {"blue": True, "relvol_b": "hot"}, "cross", rare=True)
    add("relvol=hot", {"relvol_b": "hot"}, "finviz")
    add("rsi=oversold", {"rsi_b": "oversold"}, "finviz")
    add("sma20=below", {"sma20_b": "below"}, "finviz")
    add("short=high", {"short_b": "high"}, "finviz")
    add("gap=down", {"gap_b": "down"}, "finviz")
    add("gap=up", {"gap_b": "up"}, "finviz")
    add("perf_w=washed", {"perf_w_b": "washed"}, "finviz")
    add("perf_w=extended", {"perf_w_b": "extended"}, "finviz")
    add("ins_buy", {"ins_buy": True}, "insider", rare=True)
    add("catal", {"catal": True}, "camera", rare=True)
    return recs


def _stats(vals: list[float], hits: list[bool]) -> dict:
    if not vals:
        return {"n": 0}
    return {
        "n": len(vals),
        "hit": round(sum(1 for h in hits if h) / len(hits), 4) if hits else None,
        "mean": round(statistics.fmean(vals), 4),
        "median": round(statistics.median(vals), 4),
    }


def summarize_group(rows: list[dict], horizon: str, clip: bool = True) -> dict:
    raw, xs, hits, xs_hits = [], [], [], []
    for r in rows:
        ret = _finite(r.get(f"ret_{horizon}"))
        if ret is None:
            continue
        use = _clip(ret) if clip else ret
        raw.append(use)
        hits.append(ret > 0)
        x = _finite(r.get(f"xs_{horizon}"))
        if x is not None:
            xs.append(_clip(x) if clip else x)
            xs_hits.append(x > 0)
    out = _stats(raw, hits)
    if xs:
        out["n_xs"] = len(xs)
        out["hit_xs"] = round(sum(1 for h in xs_hits if h) / len(xs_hits), 4)
        out["mean_xs"] = round(statistics.fmean(xs), 4)
        out["median_xs"] = round(statistics.median(xs), 4)
    else:
        out["n_xs"] = 0
        out["hit_xs"] = None
        out["mean_xs"] = None
        out["median_xs"] = None
    return out


def bucket_axes() -> tuple[str, ...]:
    return ("gen_s", "weather_risk", "spy_prior")


def measure_factor(rows: list[dict], spec: dict, horizon: str,
                   min_n: int = MIN_N) -> dict:
    """Within-bucket edge vs peers that share the same morning regime."""
    picked = [r for r in rows if factor_match(r, spec)]
    overall = summarize_group(picked, horizon)
    base = summarize_group(rows, horizon)
    by_axis: dict[str, dict] = {}
    fail: list[str] = []
    pass_b: list[str] = []
    thin: list[str] = []
    for axis in bucket_axes():
        groups: dict[str, list[dict]] = defaultdict(list)
        for r in rows:
            groups[str(r.get(axis) or "unknown")].append(r)
        axis_out = {}
        for label, peers in sorted(groups.items()):
            hit_rows = [r for r in peers if factor_match(r, spec)]
            st = summarize_group(hit_rows, horizon)
            pb = summarize_group(peers, horizon)
            key = f"{axis}={label}"
            n = st.get("n", 0)
            if n < min_n or pb.get("n", 0) < min_n:
                thin.append(key)
                axis_out[label] = {
                    "n": n, "peer_n": pb.get("n", 0), "thin": True,
                    "hit": st.get("hit"), "peer_hit": pb.get("hit"),
                    "mean": st.get("mean"), "peer_mean": pb.get("mean"),
                    "mean_xs": st.get("mean_xs"), "peer_xs": pb.get("mean_xs"),
                    "edge": None, "hit_edge": None, "verdict": "thin",
                }
                continue
            edge = None
            if st.get("mean_xs") is not None and pb.get("mean_xs") is not None:
                edge = round(st["mean_xs"] - pb["mean_xs"], 4)
            elif st.get("mean") is not None and pb.get("mean") is not None:
                edge = round(st["mean"] - pb["mean"], 4)
            hit_edge = None
            if st.get("hit") is not None and pb.get("hit") is not None:
                hit_edge = round(st["hit"] - pb["hit"], 4)
            # WIN = beat peers on excess AND not lose on hit rate.
            beats = (
                edge is not None and edge > 0
                and (hit_edge is None or hit_edge >= 0)
            )
            rec = {
                "n": n, "peer_n": pb.get("n", 0), "thin": False,
                "hit": st.get("hit"), "peer_hit": pb.get("hit"),
                "mean": st.get("mean"), "peer_mean": pb.get("mean"),
                "mean_xs": st.get("mean_xs"), "peer_xs": pb.get("mean_xs"),
                "edge": edge, "hit_edge": hit_edge,
                "verdict": "pass" if beats else "fail",
            }
            axis_out[label] = rec
            (pass_b if beats else fail).append(key)
        by_axis[axis] = axis_out
    graded = len(pass_b) + len(fail)
    survive = round(len(pass_b) / graded, 4) if graded else 0.0
    ov_edge = None
    if overall.get("mean_xs") is not None and base.get("mean_xs") is not None:
        ov_edge = round(overall["mean_xs"] - base["mean_xs"], 4)
    return {
        "n": overall.get("n", 0),
        "hit": overall.get("hit"),
        "mean": overall.get("mean"),
        "mean_xs": overall.get("mean_xs"),
        "peer_n": base.get("n", 0),
        "peer_hit": base.get("hit"),
        "peer_mean": base.get("mean"),
        "peer_xs": base.get("mean_xs"),
        "edge": ov_edge,
        "survive": survive,
        "n_pass": len(pass_b),
        "n_fail": len(fail),
        "n_thin": len(thin),
        "n_graded": graded,
        "pass": pass_b,
        "fail": fail,
        "thin": thin,
        "by_axis": by_axis,
        "dates": sorted({r["date"] for r in picked}),
    }


def rank_factors(rows: list[dict], horizon: str = "1d") -> list[dict]:
    out = []
    for fac in candidate_factors():
        min_n = MIN_N_RARE if fac.get("rare") else MIN_N
        rec = measure_factor(rows, fac["spec"], horizon, min_n=min_n)
        rec.update({
            "name": fac["name"], "family": fac["family"],
            "spec": fac["spec"], "horizon": horizon, "min_n": min_n,
        })
        out.append(rec)
    out.sort(key=lambda r: (
        -(r.get("n_pass") or 0),
        -(r.get("survive") or 0),
        -(r.get("edge") or -99),
        -(r.get("n") or 0),
    ))
    return out


def inventory_features(rows: list[dict], macro: dict[str, dict]) -> dict:
    dates = sorted({r["date"] for r in rows})
    by_date = []
    for d in dates:
        day = [r for r in rows if r["date"] == d]
        m = macro.get(d) or {}
        tones = {}
        for cam in TONE_COLS:
            c = defaultdict(int)
            for r in day:
                c[r.get(cam) or "missing"] += 1
            tones[cam] = dict(c)
        marks = {k: sum(1 for r in day if r.get(k)) for k in (
            "blue", "white", "alarm", "fade", "first_crack", "steady")}
        by_date.append({
            "date": d,
            "n": len(day),
            "n_1d": sum(1 for r in day if r.get("ret_1d") is not None),
            "n_3d": sum(1 for r in day if r.get("ret_3d") is not None),
            "s": m.get("s"),
            "gen_s": m.get("gen_s"),
            "weather_risk": m.get("weather_risk"),
            "spy_prior": m.get("spy_prior"),
            "spy_prior_pct": m.get("spy_prior_pct"),
            "route": m.get("route"),
            "flatten_ok": m.get("flatten_ok"),
            "hard_red": m.get("hard_red"),
            "marks": marks,
            "tones": {k: v for k, v in tones.items()},
        })
    return {
        "n_rows": len(rows),
        "n_dates": len(dates),
        "dates": dates,
        "pit_columns": (
            list(TONE_COLS) + list(MARK_BOOLS) + list(FINVIZ_BUCKETS)
            + ["n_print", "n_red", "sector_name"]
        ),
        "outcome_columns": sorted(LEAK_COLS),
        "by_date": by_date,
    }


def load_flatten_rows(path: Path | None = None) -> list[dict]:
    doc = json.loads((path or FLATTEN_JSON).read_text(encoding="utf-8"))
    out = []
    for r in doc.get("rows") or []:
        srcs = r.get("sources") or []
        if isinstance(srcs, str):
            srcs = [srcs]
        if "flatten" not in srcs:
            continue
        pc = r.get("price_changes") or {}
        boxes = r.get("boxes") or {}
        rec = {
            "ticker": str(r.get("ticker") or "").upper(),
            "date": r.get("date"),
            "flatten_rank": r.get("flatten_rank"),
            "flatten_ok": r.get("flatten_ok"),
            "route": r.get("flatten_route"),
            "s": _finite(r.get("flatten_score")),
            "ret_1d": _finite(pc.get("1d")),
            "ret_3d": _finite(pc.get("3d")),
            "ret_1w": _finite(pc.get("1w")),
            "n_print": sum(1 for k, _ in (
                ("join", 1), ("sector", 1), ("gen", 1), ("ab", 1),
                ("peer", 1), ("vol", 1), ("heat", 1),
            ) if _tone(boxes.get(k)) in {"good", "neutral", "bad"}),
            "n_red": sum(1 for k in CAMERA_KEYS if _tone(boxes.get(k)) == "bad"),
            "blue": "🔵" in str(r.get("marks_cell") or r.get("marks") or ""),
            "alarm": "🚨" in str(r.get("marks_cell") or r.get("marks") or ""),
            "white": "⚪" in str(r.get("marks_cell") or r.get("marks") or ""),
            "setups": r.get("setups") or "",
            "erd_earn_react": bool(r.get("erd_earn_react")),
            "erd_E_color": r.get("erd_E_color"),
            "candle_last_green": bool(r.get("candle_last_green")),
            "ohlc_ret_5": _finite(r.get("ohlc_ret_5")),
            "ohlc_rvol": _finite(r.get("ohlc_rvol")),
        }
        for cam in CAMERA_KEYS:
            rec[cam] = _tone(boxes.get(cam))
        for key in MARK_BOOLS:
            rec.setdefault(key, False)
        for key in FINVIZ_BUCKETS:
            rec.setdefault(key, "missing")
        out.append(rec)
    return out


def attach_excess_local(rows: list[dict]) -> list[dict]:
    """Session median excess for panels that do not already carry xs_*."""
    by_date: dict[str, dict[str, list[float]]] = defaultdict(lambda: {h: [] for h in HORIZONS})
    for r in rows:
        for h in HORIZONS:
            v = _finite(r.get(f"ret_{h}"))
            if v is not None:
                by_date[r["date"]][h].append(v)
    med = {
        d: {h: (statistics.median(vs) if vs else None) for h, vs in hs.items()}
        for d, hs in by_date.items()
    }
    for r in rows:
        m = med.get(r["date"] or "", {})
        for h in HORIZONS:
            raw, mid = _finite(r.get(f"ret_{h}")), m.get(h)
            r[f"xs_{h}"] = None if raw is None or mid is None else raw - mid
    return rows


def load_finviz_tape(day: str, export_dir: Path | None = None) -> dict[str, dict]:
    """Ticker → {price, change} from that day's Finviz export. Outcome tape."""
    path = (export_dir or EXPORT_DIR) / f"finviz_{day}.csv"
    if not path.exists():
        return {}
    out: dict[str, dict] = {}
    with path.open(newline="", encoding="utf-8") as fh:
        for rec in csv.DictReader(fh):
            t = str(rec.get("Ticker") or "").strip().upper()
            if not t:
                continue
            out[t] = {
                "price": _finite(rec.get("Price")),
                "change": _finite(rec.get("Change") or rec.get("Change %")),
            }
    return out


def forward_calendar(mornings: dict, export_dir: Path | None = None) -> list[str]:
    """Flatten trading sessions + later weekday dumps. No Sat/Sun tapes."""
    days = {d for d in mornings if is_weekday(d)}
    last = max(days) if days else ""
    folder = export_dir or EXPORT_DIR
    for p in folder.glob("finviz_????-??-??.csv"):
        d = p.stem.replace("finviz_", "")
        if is_weekday(d) and d > last:
            days.add(d)
    return sorted(days)


def fill_returns_from_finviz(
    rows: list[dict],
    export_dir: Path | None = None,
    session_cal: list[str] | None = None,
) -> list[dict]:
    """Fill missing ret_1d / ret_3d from later-session Finviz Prices.

    Same rule as ``boring_winners_backtest.fill_returns_from_finviz``:
    ``100 * (Price_{D+k} / Price_D - 1)``, else 1d falls back to the next
    tape's Change%. Existing finite returns stay. Outcomes only.
    """
    folder = export_dir or EXPORT_DIR
    cal = [d for d in (session_cal or []) if is_weekday(d)]
    if not cal:
        return rows
    pos = {d: i for i, d in enumerate(cal)}
    needed: set[str] = set()
    for r in rows:
        d = r.get("date") or ""
        j = pos.get(d)
        if j is None:
            continue
        needed.add(d)
        for lag in HORIZON_LAG.values():
            if j + lag < len(cal):
                needed.add(cal[j + lag])
    tapes = {d: load_finviz_tape(d, folder) for d in sorted(needed)}
    for r in rows:
        t = r.get("ticker") or ""
        d = r.get("date") or ""
        j = pos.get(d)
        if j is None:
            continue
        entry = (tapes.get(d) or {}).get(t) or {}
        for h, lag in HORIZON_LAG.items():
            if _finite(r.get(f"ret_{h}")) is not None:
                continue
            if j + lag >= len(cal):
                continue
            nxt = cal[j + lag]
            if nxt <= d:
                raise ValueError(f"leak: forward {h} for {d} resolved to {nxt}")
            exitr = (tapes.get(nxt) or {}).get(t) or {}
            ep, xp = entry.get("price"), exitr.get("price")
            if ep and xp:
                r[f"ret_{h}"] = round(100.0 * (xp / ep - 1.0), 3)
            elif lag == 1 and exitr.get("change") is not None:
                r[f"ret_{h}"] = round(float(exitr["change"]), 3)
    return rows


def flatten_side_cut(macro: dict[str, dict],
                     factor_doc: dict | None = None) -> dict:
    """Extra flatten_h5 lots on io mornings vs cash (flatten_live sits)."""
    doc = factor_doc or json.loads(FACTOR_MINE_JSON.read_text(encoding="utf-8"))
    h5 = {d["date"]: d for d in (doc.get("daily") or {}).get("flatten_h5") or []}
    live = {d["date"]: d for d in (doc.get("daily") or {}).get("flatten_live_h5") or []}
    look = { (r["date"], r["ticker"]): r for r in load_flatten_rows() }
    mornings = []
    extra_lots = []
    for date in UNGATED_IO_BUYS:
        m = macro.get(date) or {}
        day_h5 = h5.get(date) or {}
        day_live = live.get(date) or {}
        bought = list(day_h5.get("bought") or [])
        live_bought = list(day_live.get("bought") or [])
        if live_bought:
            raise ValueError(f"flatten_live bought on supposed sit {date}: {live_bought}")
        lot_rows = []
        for t in bought:
            look_row = look.get((date, t)) or {}
            mark = None
            for mk in day_h5.get("marks") or []:
                if str(mk.get("ticker") or "").upper() == t:
                    mark = mk
                    break
            sess = _finite((mark or {}).get("session"))
            lot_rows.append({
                "date": date,
                "ticker": t,
                "session_pnl": sess,
                "ret_1d": look_row.get("ret_1d"),
                "ret_3d": look_row.get("ret_3d"),
                "join": look_row.get("join"),
                "gen": look_row.get("gen"),
                "vol": look_row.get("vol"),
                "ab": look_row.get("ab"),
                "blue": look_row.get("blue"),
                "alarm": look_row.get("alarm"),
                "setups": look_row.get("setups"),
                "erd_earn_react": look_row.get("erd_earn_react"),
                "erd_E_color": look_row.get("erd_E_color"),
                "ohlc_ret_5": look_row.get("ohlc_ret_5"),
            })
            extra_lots.append(lot_rows[-1])
        eq_h5 = _finite(day_h5.get("equity"))
        eq_live = _finite(day_live.get("equity"))
        mornings.append({
            "date": date,
            "s": m.get("s"),
            "gen_s": m.get("gen_s"),
            "weather_risk": m.get("weather_risk"),
            "spy_prior": m.get("spy_prior"),
            "route": m.get("route") or day_h5.get("s") and "io",
            "bought": bought,
            "n_bought": len(bought),
            "h5_equity": eq_h5,
            "live_equity": eq_live,
            "book_gap": None if eq_h5 is None or eq_live is None else round(eq_h5 - eq_live, 2),
            "h5_session_delta": _finite(day_h5.get("session_delta")),
            "live_session_delta": _finite(day_live.get("session_delta")),
            "lots": lot_rows,
        })
    rets_1d = [x["ret_1d"] for x in extra_lots if x.get("ret_1d") is not None]
    rets_3d = [x["ret_3d"] for x in extra_lots if x.get("ret_3d") is not None]
    sess_pnl = [x["session_pnl"] for x in extra_lots if x.get("session_pnl") is not None]
    last_h5 = h5.get(sorted(h5)[-1]) if h5 else {}
    last_live = live.get(sorted(live)[-1]) if live else {}
    fingerprint = _fingerprint(extra_lots)
    return {
        "n_mornings": len(UNGATED_IO_BUYS),
        "dates": list(UNGATED_IO_BUYS),
        "n_extra_lots": len(extra_lots),
        "equal_weight_1d": _stats(rets_1d, [v > 0 for v in rets_1d]),
        "equal_weight_3d": _stats(rets_3d, [v > 0 for v in rets_3d]),
        "same_day_session_pnl": {
            "n": len(sess_pnl),
            "sum": round(sum(sess_pnl), 2) if sess_pnl else None,
            "mean": round(statistics.fmean(sess_pnl), 2) if sess_pnl else None,
        },
        "final_h5_equity": _finite(last_h5.get("equity")),
        "final_live_equity": _finite(last_live.get("equity")),
        "final_book_gap": (
            None if last_h5.get("equity") is None or last_live.get("equity") is None
            else round(float(last_h5["equity"]) - float(last_live["equity"]), 2)
        ),
        "mornings": mornings,
        "fingerprint": fingerprint,
        "sit_rule": "Do not turn the live sit / hard-red gate off. Report only.",
    }


def _fingerprint(lots: list[dict]) -> dict:
    if not lots:
        return {}
    n = len(lots)
    def share(pred):
        return round(sum(1 for x in lots if pred(x)) / n, 3)
    return {
        "n": n,
        "join_good": share(lambda x: x.get("join") == "good"),
        "vol_good": share(lambda x: x.get("vol") == "good"),
        "vol_missing": share(lambda x: x.get("vol") in {None, "missing"}),
        "ab_good": share(lambda x: x.get("ab") == "good"),
        "ab_missing": share(lambda x: x.get("ab") in {None, "missing"}),
        "blue": share(lambda x: x.get("blue")),
        "alarm": share(lambda x: x.get("alarm")),
        "e_green": share(lambda x: str(x.get("erd_E_color") or "").lower() in {"green", "g", "good"}),
        "earn_react": share(lambda x: x.get("erd_earn_react")),
        "has_setup": share(lambda x: bool(x.get("setups"))),
    }


def _ew_mean(lots: list[dict], key: str):
    vals = [_finite(x.get(key)) for x in lots]
    vals = [v for v in vals if v is not None]
    return statistics.fmean(vals) if vals else None


def _pct(x, nd=1):
    v = _finite(x)
    return "—" if v is None else f"{v * 100:.{nd}f}%"


def _num(x, nd=2, signed=True):
    v = _finite(x)
    if v is None:
        return "—"
    return f"{v:+.{nd}f}" if signed else f"{v:.{nd}f}"


def _md_table(headers: list[str], rows: list[list[str]]) -> list[str]:
    lines = [
        "| " + " | ".join(headers) + " |",
        "|" + "|".join("---" if i == 0 or not h.endswith(":") else "---:"
                        for i, h in enumerate(headers)) + "|",
    ]
    # simpler: numeric-ish headers already marked
    lines[1] = "|" + "|".join("---" for _ in headers) + "|"
    for row in rows:
        lines.append("| " + " | ".join(str(c) for c in row) + " |")
    return lines


def write_markdown(payload: dict) -> str:
    inv = payload["inventory"]
    ranked = payload["ranked_1d"]
    ranked_3d = payload["ranked_3d"]
    side = payload["flatten_side"]
    macro = payload["macro_sessions"]
    claim = payload["claim"]
    lines = [
        "# Regime-survival mine — leak-free 09:30",
        "",
        f"_Generated {payload['generated_at']} · research only · "
        f"live `flatten_robust` untouched._",
        "",
        "## Claim",
        "",
        claim["headline"],
        "",
        claim["body"],
        "",
        "## How validated",
        "",
        f"- Script: `python -m src.regime_survive_mine --write`",
        f"- Seed panel: `data/feature_asof/*_feature_asof.csv` "
        f"({inv['n_rows']} name-days, {inv['n_dates']} sessions).",
        f"- Dates: {', '.join(inv['dates'])}.",
        "- Morning S: `03_scoreboard/factor_mine.json` → `mornings` "
        "(same 09:30 packet flatten lookback uses).",
        "- Weather risk: derived from that S with "
        "`00_grounding/weather_rules.json` thresholds "
        f"(on ≥ {payload['thresholds']['risk_on_score']}, "
        f"off ≤ {payload['thresholds']['risk_off_score']}). "
        "Dated `01_daily/weather/*.json` files are often stamped "
        "**after** 09:30 and are not used as gates.",
        "- SPY 1d prior: prior-session Finviz `Change%` for SPY "
        "(HIT_BOARD prior actual only if that export is missing). "
        "Same-day SPY close is never an input.",
        "- Forwards: feature_asof `ret_1d` / `ret_3d` when present; "
        f"missing labels filled from later-session Finviz Prices "
        f"(native 1d n={inv.get('n_1d_native')} → {inv.get('n_1d')}; "
        f"3d n={inv.get('n_3d_native')} → {inv.get('n_3d')}). "
        "Same-day Change% is never a selection input. "
        "Means are winsorized at ±30% so one name does not dominate; "
        "hit rate is unclipped. Excess is recomputed vs the liquid "
        "session median after the fill.",
        "- Peer baseline: every other printed name in the **same** "
        "morning bucket (not the pooled sample).",
        "- Fill / side cut: existing flatten_h5 vs flatten_live_h5 "
        "cash books (09:30 open, whole shares, Futubull fees).",
        "",
        "## Leak-free contract",
        "",
        "Selection features used: " + ", ".join(f"`{c}`" for c in inv["pit_columns"]) + ".",
        "",
        "Explicitly **not** used as inputs: " + ", ".join(f"`{c}`" for c in inv["outcome_columns"]) + ".",
        "",
        "## Macro buckets (PIT morning)",
        "",
    ]
    lines += _md_table(
        ["Date", "S", "gen S", "weather risk", "SPY prior", "prior %",
         "Route", "Flatten?", "n asof", "n 1d"],
        [[
            d["date"],
            _num(d.get("s"), 2),
            d.get("gen_s") or "—",
            d.get("weather_risk") or "—",
            d.get("spy_prior") or "—",
            _num(d.get("spy_prior_pct"), 2),
            d.get("route") or "—",
            "yes" if d.get("flatten_ok") else "no",
            d.get("n") or 0,
            d.get("n_1d") or 0,
        ] for d in inv["by_date"]],
    )
    lines += [
        "",
        "Missing feature_asof sessions in the flatten window "
        "(still used for the flatten side cut, not the market-wide mine): "
        + (", ".join(payload["missing_asof"]) or "none") + ".",
        "",
        "08-13 / 08-14 dumps are ~11k names; later sessions are the "
        "~2.7k liquid universe. Ranked tables use the **liquid subset** "
        "(later sessions + overlap on the early dumps).",
        "",
        "## Ranked survival — 1d excess vs same-bucket peers",
        "",
        "A bucket **passes** when the factor's mean excess beats the "
        "peer mean excess **and** hit rate is not below the peer "
        "(WIN = both) with n ≥ min_n. Thin buckets are reported but "
        "do not count as pass or fail.",
        "",
    ]
    top = [r for r in ranked if r.get("n_graded", 0) >= 4][:18]
    lines += _md_table(
        ["Factor", "n", "1d hit", "hit vs peer", "1d xs", "edge",
         "survive", "pass", "FAIL buckets"],
        [[
            f"`{r['name']}`",
            r.get("n") or 0,
            _pct(r.get("hit")),
            _num(r.get("hit") - r["peer_hit"], 3) if r.get("hit") is not None and r.get("peer_hit") is not None else "—",
            _num(r.get("mean_xs")),
            _num(r.get("edge")),
            f"{r.get('n_pass', 0)}/{r.get('n_graded', 0)}",
            ", ".join(r.get("pass") or []) or "—",
            ", ".join(r.get("fail") or []) or "—",
        ] for r in top],
    )
    lines += [
        "",
        "### 3d survival (same factors, liquid subset)",
        "",
    ]
    top3 = [r for r in ranked_3d if r.get("n_graded", 0) >= 3][:12]
    lines += _md_table(
        ["Factor", "n", "3d hit", "3d xs", "edge", "survive", "FAIL buckets"],
        [[
            f"`{r['name']}`",
            r.get("n") or 0,
            _pct(r.get("hit")),
            _num(r.get("mean_xs")),
            _num(r.get("edge")),
            f"{r.get('n_pass', 0)}/{r.get('n_graded', 0)}",
            ", ".join(r.get("fail") or []) or "—",
        ] for r in top3],
    )
    lines += [
        "",
        "## Top claims — failing buckets spelled out",
        "",
    ]
    for r in [x for x in ranked if x.get("n_graded", 0) >= 4][:8]:
        lines.append(f"### `{r['name']}`")
        lines.append("")
        lines.append(
            f"n={r.get('n')} · 1d hit {_pct(r.get('hit'))} vs peer "
            f"{_pct(r.get('peer_hit'))} · edge {_num(r.get('edge'))} · "
            f"survival {r.get('n_pass')}/{r.get('n_graded')}."
        )
        if r.get("fail"):
            lines.append("")
            lines.append("Fails: " + ", ".join(f"`{b}`" for b in r["fail"]) + ".")
        else:
            lines.append("")
            lines.append("No graded bucket failed.")
        if r.get("thin"):
            lines.append(f"Thin (n < {r.get('min_n')}): " + ", ".join(f"`{b}`" for b in r["thin"]) + ".")
        lines.append("")
        for axis, labels in (r.get("by_axis") or {}).items():
            for label, st in labels.items():
                flag = st.get("verdict")
                lines.append(
                    f"- `{axis}={label}` · n={st.get('n')} · "
                    f"hit {_pct(st.get('hit'))} vs {_pct(st.get('peer_hit'))} · "
                    f"edge {_num(st.get('edge'))} · {flag}"
                )
        lines.append("")
    lines += [
        "## Flatten side cut — extra lots vs cash",
        "",
        "Live `flatten_robust` sits on io/HOLD. `flatten_h5` still buys the "
        "wish-list (except hard-red S ≤ −3). This cut does **not** recommend "
        "turning the sit rule off.",
        "",
        f"Ungated io mornings where flatten_h5 bought and flatten_live sat: "
        f"**{side['n_mornings']}** ({', '.join(side['dates'])}). "
        f"Extra lots **{side['n_extra_lots']}**.",
        "",
        f"- Equal-weight 1d of those extra names: "
        f"n={side['equal_weight_1d'].get('n', 0)} · "
        f"hit {_pct(side['equal_weight_1d'].get('hit'))} · "
        f"mean {_num(side['equal_weight_1d'].get('mean'))}%.",
        f"- Equal-weight 3d: "
        f"n={side['equal_weight_3d'].get('n', 0)} · "
        f"hit {_pct(side['equal_weight_3d'].get('hit'))} · "
        f"mean {_num(side['equal_weight_3d'].get('mean'))}%.",
        f"- Same-day session $ (blotter, after fees on the book): "
        f"sum {_num(side['same_day_session_pnl'].get('sum'), 2)} · "
        f"mean {_num(side['same_day_session_pnl'].get('mean'), 2)}.",
        f"- End-of-window book gap flatten_h5 − flatten_live_h5: "
        f"{_num(side.get('final_book_gap'), 2)} "
        f"(${_num(side.get('final_h5_equity'), 2, signed=False)} vs "
        f"${_num(side.get('final_live_equity'), 2, signed=False)}). "
        "That gap also includes later mark-to-market of lots opened on "
        "those io mornings — not a same-day cash ticket.",
        "",
        "### Per morning",
        "",
    ]
    lines += _md_table(
        ["Date", "S", "gen", "risk", "SPY prior", "Extra names",
         "book gap $", "EW 1d"],
        [[
            m["date"],
            _num(m.get("s"), 2),
            m.get("gen_s") or "—",
            m.get("weather_risk") or "—",
            m.get("spy_prior") or "—",
            ", ".join(f"`{t}`" for t in m.get("bought") or []) or "—",
            _num(m.get("book_gap"), 2),
            _num(_ew_mean(m.get("lots") or [], "ret_1d")),
        ] for m in side["mornings"]],
    )
    fp = side.get("fingerprint") or {}
    lines += [
        "",
        "### Fingerprint of extra lots",
        "",
        f"n={fp.get('n', 0)} · join🟢 { _pct(fp.get('join_good')) } · "
        f"vol🟢 { _pct(fp.get('vol_good')) } · vol missing "
        f"{ _pct(fp.get('vol_missing')) } · AB🟢 { _pct(fp.get('ab_good')) } · "
        f"AB missing { _pct(fp.get('ab_missing')) } · 🔵 { _pct(fp.get('blue')) } · "
        f"🚨 { _pct(fp.get('alarm')) } · E-react { _pct(fp.get('earn_react')) } · "
        f"named setup { _pct(fp.get('has_setup')) }.",
        "",
        payload["fingerprint_read"],
        "",
        "## Honest limits",
        "",
        "- Window is ~10 liquid sessions on the asof panel (Aug 17 → Sep 1 "
        "plus overlap). That is too short to call a structural invariant.",
        "- 08-13/14 asof files are a wider dump; the liquid subset is the "
        "fair comparison.",
        "- Several flatten dates have no feature_asof file, so market-wide "
        "survival cannot see 08-24 / 08-25 / 08-26 / 08-28 / 09-02 / 09-03 / 09-04. "
        "08-21 / 08-27 / 08-31 / 09-01 asof rows had empty `ret_*`; those "
        "forwards are filled from later weekday Finviz Prices (outcomes only).",
        "- Weather.json `generated_at` is often 11:40–22:00 ET. Using those "
        "files as 09:30 risk would leak. We do not.",
        "- No recipe was changed. No live promote.",
        "",
    ]
    return "\n".join(lines) + "\n"


def _headline(ranked: list[dict], side: dict) -> dict:
    usable = [r for r in ranked if r.get("n_graded", 0) >= 5 and r.get("n", 0) >= 80]

    def _hits_peer(r):
        h, p = r.get("hit"), r.get("peer_hit")
        return h is not None and p is not None and h >= p

    full = [r for r in usable
            if r.get("n_fail", 0) == 0 and (r.get("edge") or 0) > 0 and _hits_peer(r)]
    almost = [r for r in usable
              if r.get("n_pass", 0) >= 4 and (r.get("edge") or 0) > 0]
    if full:
        top = full[0]
        headline = (
            f"Names with `{top['name']}` at 09:30 beat same-bucket peers "
            f"in {top['n_pass']}/{top['n_graded']} graded regime buckets "
            f"(1d excess edge {top.get('edge'):+.2f}, n={top.get('n')})."
        )
        body = (
            "This is the strongest leak-free survival on the liquid asof "
            "panel. Failing buckets: none among those graded. Thin buckets "
            f"({', '.join(top.get('thin') or []) or 'none'}) are below min n "
            "and do not count. The window is short — treat as a research "
            "lead, not a live promote."
        )
        kind = "claim"
    elif almost:
        top = almost[0]
        headline = (
            f"No factor beat peers in every graded bucket. Closest: "
            f"`{top['name']}` survives {top['n_pass']}/{top['n_graded']} "
            f"(1d edge {top.get('edge'):+.2f}, n={top.get('n')}). "
            f"Fails: {', '.join(top.get('fail') or []) or '—'}."
        )
        body = (
            "Across morning S / weather-risk / prior-SPY, nothing is a "
            "clean invariant. Several tags that look strong pooled "
            "(blue, vol=good|ab=good) lose at least one regime. "
            "That is a clean-enough null for a live promote, with a "
            "ranked short-list for the next window."
        )
        kind = "partial"
    else:
        headline = (
            "No robust leak-free invariant: no 09:30 factor beat same-bucket "
            "peers in most graded macro regimes with n large enough to trust."
        )
        body = (
            "Pooled mines (FEATURE_MINE / TICKER_LOOKBACK_MINE) still show "
            "edges; those edges do not survive the PIT morning-S / weather / "
            "prior-SPY split on this window. Research only."
        )
        kind = "null"
        top = usable[0] if usable else (ranked[0] if ranked else {})
    fp = side.get("fingerprint") or {}
    if fp.get("n") and fp.get("vol_good", 0) < 0.3 and fp.get("join_good", 0) > 0.7:
        fp_read = (
            "Extra io lots are mostly flatten wish-list / join🟢 names, "
            "not the market-wide `vol=good|ab=good` tag. No stable "
            "camera fingerprint that would justify ungating sit."
        )
    else:
        fp_read = (
            "Extra io lots do not share one camera combo at a rate that "
            "looks like a durable fingerprint. Sit stays on."
        )
    return {
        "kind": kind,
        "headline": headline,
        "body": body,
        "top": {k: top.get(k) for k in (
            "name", "n", "hit", "edge", "survive", "n_pass", "n_fail",
            "n_graded", "fail", "pass", "thin") if top},
        "fingerprint_read": fp_read,
    }


def run(write: bool = False) -> dict:
    th = load_weather_thresholds()
    mornings = load_mornings()
    macro = attach_macro(mornings)
    raw = load_feature_asof()
    liquid = liquid_subset(raw)
    panel = join_macro(liquid, macro)
    # Drop asof dates that are not flatten-window sessions (e.g. 08-30 Sunday dump).
    panel = [r for r in panel if r["date"] in macro]
    n_1d_native = sum(1 for r in panel if r.get("ret_1d") is not None)
    n_3d_native = sum(1 for r in panel if r.get("ret_3d") is not None)
    panel = fill_returns_from_finviz(panel, session_cal=forward_calendar(macro))
    panel = attach_excess_local(panel)
    flatten_panel = attach_excess_local(join_macro(load_flatten_rows(), macro))
    inv = inventory_features(panel, macro)
    inv["n_1d_native"] = n_1d_native
    inv["n_3d_native"] = n_3d_native
    inv["n_1d"] = sum(1 for r in panel if r.get("ret_1d") is not None)
    inv["n_3d"] = sum(1 for r in panel if r.get("ret_3d") is not None)
    asof_dates = set(inv["dates"])
    missing = [d for d in session_calendar(macro) if d not in asof_dates]
    ranked_1d = rank_factors(panel, "1d")
    ranked_3d = rank_factors(panel, "3d")
    side = flatten_side_cut(macro)
    claim = _headline(ranked_1d, side)
    payload = {
        "generated_at": datetime.now(ET).isoformat(),
        "asof": "09:30_et",
        "research_only": True,
        "live_untouched": "flatten_robust",
        "fill": "feature_asof forwards; flatten side cut uses existing 09:30 open / Futubull cash books",
        "from_date": inv["dates"][0] if inv["dates"] else None,
        "to_date": inv["dates"][-1] if inv["dates"] else None,
        "thresholds": th,
        "s_up": S_UP,
        "s_down": S_DOWN,
        "spy_flat": SPY_FLAT,
        "min_n": MIN_N,
        "clip": CLIP,
        "inventory": inv,
        "missing_asof": missing,
        "macro_sessions": {d: {k: v for k, v in m.items() if k != "why"}
                           for d, m in macro.items()},
        "n_liquid_rows": len(panel),
        "n_flatten_rows": len(flatten_panel),
        "n_1d_native": n_1d_native,
        "n_3d_native": n_3d_native,
        "ranked_1d": ranked_1d,
        "ranked_3d": ranked_3d,
        "flatten_ranked_1d": rank_factors(flatten_panel, "1d") if flatten_panel else [],
        "flatten_side": side,
        "claim": claim,
        "fingerprint_read": claim["fingerprint_read"],
    }
    if write:
        OUT_JSON.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        OUT_MD.write_text(write_markdown(payload), encoding="utf-8")
        print(f"[regime-survive] wrote {OUT_MD} and {OUT_JSON}", flush=True)
    print(claim["headline"], flush=True)
    print(f"liquid rows={len(panel)} dates={inv['dates']}", flush=True)
    return payload


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Leak-free regime-survival mine")
    p.add_argument("--write", action="store_true", help="Write MD + JSON")
    args = p.parse_args(argv)
    run(write=args.write)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
