"""Replay harness: re-score every graded general and sector run under
alternative rules and report direction/magnitude hit rates against naive
baselines, walk-forward.

Leak discipline
---------------
* Only Channel 1 snapshots fetched BEFORE 09:30 New York time are used as
  "what the engine could see". Several stored snapshots were re-fetched
  after the open (e.g. 2026-09-02 15:40, 2026-09-03 15:43); those days are
  scored with *no* tape, exactly as the live engine would have to.
* Skill multipliers (engine_policy) are computed from runs strictly before
  the date being scored (expanding window).
* Fixed constants in tape_anchor / compute_scores (score-per-pct, bands,
  betas) were chosen by hand from the repo's data, not fitted; the harness
  also reports the first-half / second-half split so drift is visible, and a
  290-session principle check on ETF open gaps from data/prices/ohlc.parquet
  that does not depend on any snapshot.

CLI: python -m src.replay_harness   -> 03_scoreboard/REPLAY_HARNESS.{md,json}
"""
from __future__ import annotations

import glob
import json
import os
from collections import defaultdict
from datetime import datetime

from . import compute_scores, compute_sector_scores, config, engine_policy
from .sector_taxonomy import SECTOR_ETFS
from .tape_anchor import general_anchor, sector_anchor

OUT_MD = os.path.join(config.SCOREBOARD_DIR, "REPLAY_HARNESS.md") if hasattr(config, "SCOREBOARD_DIR") \
    else os.path.join(os.path.dirname(config.SCOREBOARD_JSON), "REPLAY_HARNESS.md")
OUT_JSON = OUT_MD.replace(".md", ".json")
PRICES = os.path.join("data", "prices", "ohlc.parquet")
PREOPEN_CUTOFF = "09:30"


# ---------------------------------------------------------------- loading
def _fetched_hhmm(snap: dict) -> str | None:
    fa = snap.get("fetched_at") or ""
    return fa[11:16] if len(fa) >= 16 else None


def preopen_snapshots() -> dict[str, dict]:
    """{date: earliest pre-open Channel 1 snapshot}, any predict stage."""
    best: dict[str, tuple[str, dict]] = {}
    for p in sorted(glob.glob(os.path.join(config.CHANNEL1_DIR, "*predict.json"))):
        d = os.path.basename(p)[:10]
        try:
            with open(p, encoding="utf-8") as fh:
                j = json.load(fh)
        except (OSError, json.JSONDecodeError):
            continue
        hhmm = _fetched_hhmm(j)
        if not hhmm or hhmm >= PREOPEN_CUTOFF:
            continue
        fa = j.get("fetched_at", "")
        if d not in best or fa < best[d][0]:
            best[d] = (fa, j)
    return {d: j for d, (fa, j) in best.items()}


def _prev_close_pcts() -> dict[str, dict[str, float]]:
    """{ticker: {date: close-to-close %}} for SPY and the sector ETFs."""
    out: dict[str, dict[str, float]] = {}
    try:
        import pandas as pd
        df = pd.read_parquet(PRICES)
    except Exception:  # noqa: BLE001
        return out
    want = set(SECTOR_ETFS.values()) | {"SPY"}
    df = df[df["ticker"].isin(want)].sort_values("date")
    for t, g in df.groupby("ticker"):
        c = g["close"].astype(float).values
        d = [str(x)[:10] for x in g["date"].values]
        out[t] = {d[i]: (c[i] / c[i - 1] - 1) * 100 for i in range(1, len(c))}
    return out


def _yesterday_pct(series: dict[str, float], date: str) -> float | None:
    prior = [d for d in series if d < date]
    return series[max(prior)] if prior else None


def load_general_runs() -> list[dict]:
    board = json.load(open(config.SCOREBOARD_JSON, encoding="utf-8"))
    snaps = preopen_snapshots()
    pcts = _prev_close_pcts().get("SPY", {})
    runs = []
    for r in engine_policy.general_runs(board):
        if r.get("actual_pct_change") is None:
            continue
        runs.append({
            "date": r["date"], "topic": "general",
            "components": r.get("components") or {},
            "multiplier": r.get("multiplier"),
            "llm_confidence": r.get("confidence_score"),
            "legacy_direction": r.get("predicted_direction"),
            "legacy_band": r.get("predicted_magnitude_band"),
            "actual_pct": float(r["actual_pct_change"]),
            "ch1": snaps.get(r["date"]),
            "yesterday_pct": _yesterday_pct(pcts, r["date"]),
        })
    return sorted(runs, key=lambda x: x["date"])


def load_sector_runs() -> list[dict]:
    board = json.load(open(config.SCOREBOARD_JSON, encoding="utf-8"))
    snaps = preopen_snapshots()
    pcts = _prev_close_pcts()
    runs = []
    for r in engine_policy.sector_runs(board):
        if r.get("actual_pct_change") is None:
            continue
        etf = r.get("etf") or SECTOR_ETFS.get(r["sector"])
        runs.append({
            "date": r["date"], "topic": r.get("topic"), "sector": r["sector"], "etf": etf,
            "components": r.get("components") or {},
            "multiplier": r.get("multiplier"),
            "llm_confidence": r.get("confidence_score"),
            "legacy_direction": r.get("predicted_direction"),
            "legacy_band": r.get("predicted_magnitude_band"),
            "actual_pct": float(r["actual_pct_change"]),
            "ch1": snaps.get(r["date"]),
            "yesterday_pct": _yesterday_pct(pcts.get(etf, {}), r["date"]),
        })
    return sorted(runs, key=lambda x: (x["date"], x["sector"]))


# --------------------------------------------------------------- policies
def _scores_from_run(run: dict) -> dict:
    s = dict(run["components"])
    if run.get("multiplier") is not None:
        s["MULTIPLIER"] = run["multiplier"]
    if run.get("llm_confidence") is not None:
        s["CONFIDENCE"] = run["llm_confidence"]
    return s


def _has_llm(run: dict) -> bool:
    return any(v not in (None, 0, 0.0) for v in (run.get("components") or {}).values())


def policy_legacy(run, _pol):
    if not _has_llm(run):
        return None
    if run.get("legacy_direction"):
        return run["legacy_direction"], run["legacy_band"]
    d = (compute_sector_scores if run.get("sector") else compute_scores).compute_legacy(_scores_from_run(run))
    return d["predicted_direction"], d["predicted_magnitude_band"]


_GENERAL_BY_DATE: dict[str, dict] = {}


def _general_total_v2(date: str, pol: dict, with_tape: bool = True) -> float | None:
    """The general v2 total the sector engine would have seen that morning
    (the general predict runs before the sector predicts)."""
    g = _GENERAL_BY_DATE.get(date)
    if not g:
        return None
    ch1 = g.get("ch1") if with_tape else None
    if not _has_llm(g) and not ch1:
        return None
    return compute_scores.compute(_scores_from_run(g), ch1=ch1, policy=pol)["total_score"]


def policy_v2(run, pol):
    if not _has_llm(run) and not run.get("ch1"):
        return None
    if run.get("sector"):
        d = compute_sector_scores.compute(
            _scores_from_run(run), sector=run["sector"], etf=run["etf"],
            ch1=run.get("ch1"), policy=pol,
            general_total=_general_total_v2(run["date"], pol))
    else:
        d = compute_scores.compute(_scores_from_run(run), ch1=run.get("ch1"), policy=pol)
    return d["predicted_direction"], d["predicted_magnitude_band"]


def policy_v2_llm_only(run, pol):
    """v2 rules but no tape: shows what skill weights + flat zone do alone."""
    if not _has_llm(run):
        return None
    if run.get("sector"):
        d = compute_sector_scores.compute(
            _scores_from_run(run), sector=run["sector"], etf=run["etf"], ch1=None,
            policy=pol, general_total=_general_total_v2(run["date"], pol, with_tape=False))
    else:
        d = compute_scores.compute(_scores_from_run(run), ch1=None, policy=pol)
    return d["predicted_direction"], d["predicted_magnitude_band"]


def policy_tape_only(run, _pol):
    a = (sector_anchor(run["sector"], run["etf"], run.get("ch1")) if run.get("sector")
         else general_anchor(run.get("ch1")))
    if not a["available"]:
        return None
    if abs(a["score"]) < compute_scores.FLAT_EPS:
        return "flat", "flat"
    return ("up" if a["score"] > 0 else "down"), compute_scores._band_v2(a["score"])


def baseline_always(direction):
    return lambda run, _pol: (direction, "mild")


def baseline_yesterday(run, _pol):
    y = run.get("yesterday_pct")
    if y is None:
        return None
    d, b = compute_scores.actual_band(y)
    return d, b


POLICIES = [
    ("legacy engine (as shipped)", policy_legacy),
    ("v2 engine (anchor + skill-weighted LLM)", policy_v2),
    ("v2 rules, LLM only (no tape)", policy_v2_llm_only),
    ("tape anchor only (no LLM)", policy_tape_only),
    ("baseline: always up", baseline_always("up")),
    ("baseline: always down", baseline_always("down")),
    ("baseline: same as yesterday", baseline_yesterday),
]


# -------------------------------------------------------------- evaluate
def evaluate(runs: list[dict], keys, sector_mode: bool) -> dict:
    """Walk-forward: the policy for date d is built from runs before d."""
    board = json.load(open(config.SCOREBOARD_JSON, encoding="utf-8"))
    dates = sorted({r["date"] for r in runs})
    mid = dates[len(dates) // 2] if dates else None
    pol_cache: dict[str, dict] = {}
    results = {name: {"n": 0, "dir": 0, "mag": 0, "n_tape": 0, "dir_tape": 0,
                      "h1_n": 0, "h1_dir": 0, "h2_n": 0, "h2_dir": 0,
                      "flat_calls": 0, "flat_hits": 0, "rows": []}
               for name, _ in POLICIES}
    for r in runs:
        d = r["date"]
        if d not in pol_cache:
            pol_cache[d] = engine_policy.build_policy(board, before_date=d)
        pol = pol_cache[d]
        ad, ab = compute_scores.actual_band(r["actual_pct"])
        tape_ok = (sector_anchor(r["sector"], r["etf"], r.get("ch1")) if sector_mode
                   else general_anchor(r.get("ch1")))["available"]
        for name, fn in POLICIES:
            out = fn(r, pol)
            if out is None:
                continue
            pd_, pb = out
            res = results[name]
            g = compute_scores.grade(pd_, pb, r["actual_pct"])
            res["n"] += 1
            res["dir"] += int(g["direction_hit"])
            res["mag"] += int(g["magnitude_hit"])
            if tape_ok:
                res["n_tape"] += 1
                res["dir_tape"] += int(g["direction_hit"])
            half = "h1" if d < mid else "h2"
            res[f"{half}_n"] += 1
            res[f"{half}_dir"] += int(g["direction_hit"])
            if pd_ == "flat":
                res["flat_calls"] += 1
                res["flat_hits"] += int(g["direction_hit"])
            res["rows"].append({"date": d, "sector": r.get("sector"), "pred": f"{pd_}/{pb}",
                                "actual": f"{ad}/{ab}", "pct": round(r["actual_pct"], 2),
                                "dir_hit": g["direction_hit"], "mag_hit": g["magnitude_hit"]})
    return {"n_runs": len(runs), "n_tape_days": sum(1 for r in runs if (
        (sector_anchor(r["sector"], r["etf"], r.get("ch1")) if sector_mode else general_anchor(r.get("ch1")))["available"])),
        "mid_date": mid, "policies": results}


def principle_check() -> dict:
    """Large-sample check that does not use any snapshot: does the sign of
    the 09:30 open gap predict close-vs-prior-close direction? The 05:55
    futures read is a noisier version of the same quantity."""
    try:
        import numpy as np
        import pandas as pd
        df = pd.read_parquet(PRICES)
    except Exception as e:  # noqa: BLE001
        return {"available": False, "note": str(e)}
    want = ["SPY"] + list(SECTOR_ETFS.values())
    px = df[df["ticker"].isin(want)].pivot(index="date", columns="ticker", values=["open", "close"]).sort_index()

    def dirn(x):
        return np.where(x > 0.1, "up", np.where(x < -0.1, "down", "flat"))

    out = {}
    for t in want:
        if t not in px["open"].columns:
            continue
        gap = (px["open"][t] / px["close"][t].shift(1) - 1) * 100
        cc = (px["close"][t] / px["close"][t].shift(1) - 1) * 100
        m = gap.notna() & cc.notna()
        g, c = gap[m].values, cc[m].values
        gap_dir = np.where(g >= 0, "up", "down")
        act = dirn(c)
        yest = dirn(cc.shift(1)[m].values)
        out[t] = {
            "n": int(m.sum()),
            "gap_sign_hit": round(float(np.mean(gap_dir == act)), 3),
            "always_up": round(float(np.mean(act == "up")), 3),
            "always_down": round(float(np.mean(act == "down")), 3),
            "same_as_yesterday": round(float(np.mean(yest == act)), 3),
            "flat_share": round(float(np.mean(act == "flat")), 3),
            "always_mild_band": round(float(np.mean((np.abs(c) >= 0.3) & (np.abs(c) < 1.0))), 3),
        }
    return {"available": True, "by_ticker": out}


# ----------------------------------------------------------------- report
def _pct(a, b):
    return f"{100 * a / b:.1f}%" if b else "—"


def _table(res: dict) -> list[str]:
    lines = ["| Policy | n | direction hit | magnitude hit | dir hit (tape days) | 1st half | 2nd half | flat calls (hits) |",
             "|---|---:|---:|---:|---:|---:|---:|---:|"]
    for name, _ in POLICIES:
        p = res["policies"][name]
        lines.append(
            f"| {name} | {p['n']} | **{_pct(p['dir'], p['n'])}** | {_pct(p['mag'], p['n'])} | "
            f"{_pct(p['dir_tape'], p['n_tape'])} ({p['n_tape']}) | {_pct(p['h1_dir'], p['h1_n'])} | "
            f"{_pct(p['h2_dir'], p['h2_n'])} | {p['flat_calls']} ({p['flat_hits']}) |")
    return lines


def _by_sector(res_rows: dict, runs: list[dict]) -> list[str]:
    agg = defaultdict(lambda: defaultdict(lambda: [0, 0]))
    for name in ("legacy engine (as shipped)", "v2 engine (anchor + skill-weighted LLM)", "tape anchor only (no LLM)"):
        for row in res_rows["policies"][name]["rows"]:
            a = agg[row["sector"]][name]
            a[0] += int(row["dir_hit"])
            a[1] += 1
    lines = ["| Sector | legacy | v2 | tape only |", "|---|---:|---:|---:|"]
    for s in sorted(agg):
        cells = []
        for name in ("legacy engine (as shipped)", "v2 engine (anchor + skill-weighted LLM)", "tape anchor only (no LLM)"):
            h, n = agg[s][name]
            cells.append(f"{_pct(h, n)} ({n})")
        lines.append(f"| {s} | " + " | ".join(cells) + " |")
    return lines


def run(write: bool = True) -> dict:
    gen_runs = load_general_runs()
    sec_runs = load_sector_runs()
    _GENERAL_BY_DATE.clear()
    _GENERAL_BY_DATE.update({g["date"]: g for g in gen_runs})
    gen = evaluate(gen_runs, engine_policy.GENERAL_KEYS, sector_mode=False)
    sec = evaluate(sec_runs, engine_policy.SECTOR_KEYS, sector_mode=True)
    principle = principle_check()
    board = json.load(open(config.SCOREBOARD_JSON, encoding="utf-8"))
    skill = engine_policy.build_policy(board)

    now = datetime.now().isoformat(timespec="seconds")
    md = [f"# Replay harness — {now}", "",
          "Re-scores every graded run from its stored LLM components plus the earliest **pre-09:30** "
          "Channel 1 snapshot of that day. Skill multipliers are walk-forward (built only from earlier "
          "runs). Days whose only snapshot was fetched after the open are scored with no tape, as the "
          "live engine would have to. `dir hit (tape days)` restricts to days where an anchor existed.",
          "",
          f"## General market — {gen['n_runs']} graded runs, tape available on {gen['n_tape_days']}",
          "", *_table(gen), "",
          f"## Sectors — {sec['n_runs']} graded runs, tape available on {sec['n_tape_days']}",
          "", *_table(sec), "", "### Direction hit by sector", "", *_by_sector(sec, sec_runs), "",
          "## Factor skill (sign of component vs actual direction, all graded runs)", "",
          "| Factor | n | sign hit | multiplier now |", "|---|---:|---:|---:|"]
    for k in engine_policy.GENERAL_KEYS:
        s = skill["general"][k]
        md.append(f"| {k} | {s['n']} | {s['hit'] if s['hit'] is not None else '—'} | {s['mult']} |")
    for k in engine_policy.SECTOR_KEYS:
        s = skill["sector_pooled"][k]
        md.append(f"| {k} (all sectors) | {s['n']} | {s['hit'] if s['hit'] is not None else '—'} | {s['mult']} |")
    md += ["", "## Principle check — 09:30 open gap sign vs close-to-close direction (no snapshots involved)", ""]
    if principle.get("available"):
        md += ["| Ticker | n | gap-sign hit | always up | always down | same as yesterday | flat share |",
               "|---|---:|---:|---:|---:|---:|---:|"]
        for t, v in principle["by_ticker"].items():
            md.append(f"| {t} | {v['n']} | **{v['gap_sign_hit']}** | {v['always_up']} | {v['always_down']} | "
                      f"{v['same_as_yesterday']} | {v['flat_share']} |")
        md += ["", "The 05:55 futures/Europe read is a noisier version of the 09:30 gap, so the live anchor "
               "should land a little under these numbers; the leak-free snapshot rows above are the "
               "honest live estimate."]
    else:
        md.append(f"_unavailable: {principle.get('note')}_")
    md += ["", "## Per-run rows (v2 engine)", "", "| Date | Topic | pred | actual | % | dir | mag |", "|---|---|---|---|---:|---|---|"]
    for row in gen["policies"]["v2 engine (anchor + skill-weighted LLM)"]["rows"]:
        md.append(f"| {row['date']} | general | {row['pred']} | {row['actual']} | {row['pct']:+.2f} | "
                  f"{'✅' if row['dir_hit'] else '❌'} | {'✅' if row['mag_hit'] else '❌'} |")
    out = {"generated": now, "general": {k: v for k, v in gen.items()},
           "sectors": {k: v for k, v in sec.items()}, "principle_check": principle, "skill": skill}
    if write:
        os.makedirs(os.path.dirname(OUT_MD), exist_ok=True)
        with open(OUT_MD, "w", encoding="utf-8") as fh:
            fh.write("\n".join(md) + "\n")
        with open(OUT_JSON, "w", encoding="utf-8") as fh:
            json.dump(out, fh, indent=1, ensure_ascii=False, default=str)
        print(f"[replay] wrote {OUT_MD}")
    print("\n".join(md[:8 + len(_table(gen))]))
    print("\n".join(_table(sec)))
    return out


if __name__ == "__main__":
    run()
