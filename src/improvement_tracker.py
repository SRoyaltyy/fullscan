"""Session-by-session improvement tracker: is the engine getting better,
and is it beating the naive baselines?

Every graded run is scored against three baselines computed from the same
graded tape, so the comparison is apples to apples:
  * always up
  * always down
  * same as yesterday (previous graded session's actual direction, same topic)

Reported per session (general, and the 11 sectors pooled):
  * rolling 10 / 20 session direction + magnitude hit for the engine
  * the same rolling window for each baseline and the engine's edge over
    the best baseline
  * cumulative curve since the v2 engine went live (scoreboard rows carry
    `engine: "v2"` from that day on), next to the legacy-era numbers, so
    "visible improvement as sessions go by" is a number, not a feeling
  * the walk-forward replay estimate from REPLAY_HARNESS.json as the
    reference the live v2 engine is expected to converge to

Outputs
  03_scoreboard/IMPROVEMENT_TRACKER.md
  03_scoreboard/improvement_tracker.json

CLI: python -m src.improvement_tracker
"""
from __future__ import annotations

import json
import os
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo

from . import compute_scores, config, scoreboard

OUT_MD = os.path.join(os.path.dirname(config.SCOREBOARD_JSON), "IMPROVEMENT_TRACKER.md")
OUT_JSON = OUT_MD.replace("IMPROVEMENT_TRACKER.md", "improvement_tracker.json")
REPLAY_JSON = os.path.join(os.path.dirname(config.SCOREBOARD_JSON), "REPLAY_HARNESS.json")

WINDOWS = (10, 20)
BASELINES = ("always_up", "always_down", "same_as_yesterday")


def _graded(board: dict) -> list[dict]:
    out = []
    for r in board.get("runs", []):
        if r.get("direction_hit") is None or r.get("ops_fail"):
            continue
        if not r.get("predicted_direction") or r.get("actual_pct_change") is None:
            continue
        out.append(r)
    return sorted(out, key=lambda r: (r.get("date", ""), r.get("topic", "")))


def _rows(runs: list[dict]) -> list[dict]:
    """One scored row per graded run, with baseline hits filled in."""
    last_actual: dict[str, str] = {}
    rows = []
    for r in runs:
        topic = r.get("topic") or "general"
        ad, ab = compute_scores.actual_band(float(r["actual_pct_change"]))
        yest = last_actual.get(topic)
        rows.append({
            "date": r["date"], "topic": topic, "sector": r.get("sector"),
            "engine": r.get("engine") or "legacy",
            "pred": r.get("predicted_direction"), "band": r.get("predicted_magnitude_band"),
            "actual": ad, "actual_band": ab, "pct": round(float(r["actual_pct_change"]), 2),
            "dir_hit": bool(r.get("direction_hit")),
            "mag_hit": bool(r.get("magnitude_hit")),
            "always_up": ad == "up",
            "always_down": ad == "down",
            "same_as_yesterday": (yest == ad) if yest is not None else None,
        })
        last_actual[topic] = ad
    return rows


def _rate(vals) -> float | None:
    vals = [v for v in vals if v is not None]
    return round(sum(1 for v in vals if v) / len(vals), 3) if vals else None


def _sessions(rows: list[dict]) -> list[dict]:
    """Collapse runs to one entry per session date (sectors pooled)."""
    by_date: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_date[r["date"]].append(r)
    out = []
    for d in sorted(by_date):
        rs = by_date[d]
        out.append({
            "date": d, "n": len(rs),
            "dir_hits": sum(r["dir_hit"] for r in rs),
            "mag_hits": sum(r["mag_hit"] for r in rs),
            "engine": "v2" if any(r["engine"] == "v2" for r in rs) else "legacy",
            **{b: [r[b] for r in rs] for b in BASELINES},
            "dir": [r["dir_hit"] for r in rs], "mag": [r["mag_hit"] for r in rs],
        })
    return out


def _rolling(sessions: list[dict], i: int, key: str, w: int) -> float | None:
    vals = []
    for s in sessions[max(0, i - w + 1): i + 1]:
        vals.extend(s[key])
    return _rate(vals)


def _curve(sessions: list[dict]) -> list[dict]:
    curve = []
    for i, s in enumerate(sessions):
        row = {"date": s["date"], "n": s["n"], "engine": s["engine"],
               "session_dir": _rate(s["dir"]), "session_mag": _rate(s["mag"])}
        for w in WINDOWS:
            row[f"dir_{w}"] = _rolling(sessions, i, "dir", w)
            row[f"mag_{w}"] = _rolling(sessions, i, "mag", w)
            base = {b: _rolling(sessions, i, b, w) for b in BASELINES}
            row[f"base_{w}"] = base
            best = max((v for v in base.values() if v is not None), default=None)
            row[f"edge_{w}"] = (round(row[f"dir_{w}"] - best, 3)
                                if best is not None and row[f"dir_{w}"] is not None else None)
        curve.append(row)
    return curve


def _era(rows: list[dict], engine: str | None) -> dict:
    rs = [r for r in rows if engine is None or r["engine"] == engine]
    out = {"n": len(rs), "dir": _rate(r["dir_hit"] for r in rs),
           "mag": _rate(r["mag_hit"] for r in rs)}
    for b in BASELINES:
        out[b] = _rate(r[b] for r in rs)
    best = max((out[b] for b in BASELINES if out[b] is not None), default=None)
    out["edge_vs_best_baseline"] = (round(out["dir"] - best, 3)
                                    if best is not None and out["dir"] is not None else None)
    return out


def _since_v2(rows: list[dict]) -> dict:
    v2 = [r for r in rows if r["engine"] == "v2"]
    if not v2:
        return {"live": False, "n": 0}
    first = min(r["date"] for r in v2)
    cum, hits, mags = [], 0, 0
    for i, r in enumerate(sorted(v2, key=lambda x: (x["date"], x["topic"])), 1):
        hits += r["dir_hit"]
        mags += r["mag_hit"]
        cum.append({"date": r["date"], "topic": r["topic"], "n": i,
                    "cum_dir": round(hits / i, 3), "cum_mag": round(mags / i, 3)})
    return {"live": True, "since": first, "n": len(v2), **_era(v2, "v2"), "cumulative": cum}


def _replay_reference() -> dict:
    try:
        with open(REPLAY_JSON, encoding="utf-8") as fh:
            rep = json.load(fh)
    except (OSError, json.JSONDecodeError):
        return {}
    key = "v2 engine (anchor + skill-weighted LLM)"
    out = {}
    for scope in ("general", "sectors"):
        p = (rep.get(scope) or {}).get("policies", {}).get(key)
        if p and p.get("n"):
            out[scope] = {"n": p["n"], "dir": round(p["dir"] / p["n"], 3),
                          "mag": round(p["mag"] / p["n"], 3), "generated": rep.get("generated")}
    return out


def _verdict(curve: list[dict], era_all: dict) -> str:
    if len(curve) < 2:
        return "too few sessions"
    last = curve[-1]
    d10, d20, e10 = last.get("dir_10"), last.get("dir_20"), last.get("edge_10")
    parts = []
    if d10 is not None and d20 is not None:
        parts.append("improving" if d10 > d20 + 0.02 else
                     "slipping" if d10 < d20 - 0.02 else "steady")
    if e10 is not None:
        parts.append(f"{'beating' if e10 > 0.05 else 'not beating'} best baseline by {e10:+.0%} (last 10 sessions)")
    return "; ".join(parts) or "n/a"


def build() -> dict:
    board = scoreboard.load()
    rows = _rows(_graded(board))
    gen_rows = [r for r in rows if r["topic"] == "general"]
    sec_rows = [r for r in rows if r["topic"] != "general"]
    result = {"generated": datetime.now(ZoneInfo(config.TZ)).isoformat(timespec="seconds"),
              "replay_reference": _replay_reference(), "scopes": {}}
    for name, rs in (("general", gen_rows), ("sectors", sec_rows)):
        sessions = _sessions(rs)
        curve = _curve(sessions)
        by_sector = {}
        if name == "sectors":
            for s in sorted({r["sector"] for r in rs if r.get("sector")}):
                by_sector[s] = _era([r for r in rs if r["sector"] == s], None)
        result["scopes"][name] = {
            "n_runs": len(rs), "n_sessions": len(sessions),
            "all": _era(rs, None), "legacy_era": _era(rs, "legacy"),
            "since_v2": _since_v2(rs), "curve": curve, "by_sector": by_sector,
            "verdict": _verdict(curve, _era(rs, None)),
        }
    return result


# ------------------------------------------------------------------ report
def _p(x) -> str:
    return f"{x:.0%}" if isinstance(x, (int, float)) else "—"


def _e(x) -> str:
    return f"{x:+.0%}" if isinstance(x, (int, float)) else "—"


def _era_table(title: str, eras: list[tuple[str, dict]]) -> list[str]:
    L = [f"### {title}", "",
         "| Era | n | engine dir | engine mag | always up | always down | same as yesterday | edge vs best baseline |",
         "|---|---:|---:|---:|---:|---:|---:|---:|"]
    for label, e in eras:
        if not e.get("n"):
            L.append(f"| {label} | 0 | — | — | — | — | — | — |")
            continue
        L.append(f"| {label} | {e['n']} | **{_p(e['dir'])}** | {_p(e['mag'])} | {_p(e['always_up'])} | "
                 f"{_p(e['always_down'])} | {_p(e['same_as_yesterday'])} | "
                 f"**{_e(e.get('edge_vs_best_baseline'))}** |")
    return L + [""]


def _curve_table(curve: list[dict], last: int = 30) -> list[str]:
    L = ["| Session | engine | n | session dir | dir (10) | mag (10) | up (10) | down (10) | yest (10) | edge (10) | dir (20) | edge (20) |",
         "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for c in curve[-last:]:
        b = c["base_10"]
        L.append(f"| {c['date']} | {c['engine']} | {c['n']} | {_p(c['session_dir'])} | **{_p(c['dir_10'])}** | "
                 f"{_p(c['mag_10'])} | {_p(b['always_up'])} | {_p(b['always_down'])} | {_p(b['same_as_yesterday'])} | "
                 f"{_e(c['edge_10'])} | {_p(c['dir_20'])} | {_e(c['edge_20'])} |")
    return L


def write(result: dict) -> None:
    ref = result.get("replay_reference") or {}
    L = [f"# Improvement tracker — {result['generated']}", "",
         "Rolling direction/magnitude hit of the *shipped* prediction vs three naive baselines "
         "computed on the same graded runs (always up, always down, same direction as the previous "
         "graded session of that topic). `edge` = engine direction hit minus the best baseline over "
         "the same window. Sessions are dated by the predicted session; sectors are pooled (11 per day).",
         ""]
    for scope in ("general", "sectors"):
        s = result["scopes"][scope]
        title = "General market (SPX)" if scope == "general" else "Sectors (11 ETFs pooled)"
        L += [f"## {title} — {s['n_runs']} graded runs over {s['n_sessions']} sessions", "",
              f"**Read:** {s['verdict']}", ""]
        v2 = s["since_v2"]
        eras = [("all graded", s["all"]), ("legacy engine era", s["legacy_era"])]
        if v2.get("live"):
            eras.append((f"v2 engine live (since {v2['since']})", v2))
        else:
            eras.append(("v2 engine live", {"n": 0}))
        L += _era_table("Eras", eras)
        if ref.get(scope):
            r = ref[scope]
            L += [f"Walk-forward replay estimate for v2 on the same history: **{_p(r['dir'])}** direction / "
                  f"{_p(r['mag'])} magnitude (n={r['n']}, `REPLAY_HARNESS.md` {str(r.get('generated'))[:10]}). "
                  "The live v2 curve above should converge toward this as sessions accumulate; if it "
                  "sits well below it for 20+ sessions, the anchor inputs or the LLM components changed.", ""]
        if v2.get("live") and v2.get("cumulative"):
            L += ["### Cumulative since v2 went live", "", "| # | Session | topic | cum dir | cum mag |", "|---:|---|---|---:|---:|"]
            for c in v2["cumulative"][-40:]:
                L.append(f"| {c['n']} | {c['date']} | {c['topic']} | {_p(c['cum_dir'])} | {_p(c['cum_mag'])} |")
            L.append("")
        L += ["### Session curve (last 30 sessions)", "", *_curve_table(s["curve"]), ""]
        if s.get("by_sector"):
            L += ["### Per sector (all graded)", "", "| Sector | n | dir | mag | always up | always down | yest | edge |",
                  "|---|---:|---:|---:|---:|---:|---:|---:|"]
            for name, e in s["by_sector"].items():
                L.append(f"| {name} | {e['n']} | {_p(e['dir'])} | {_p(e['mag'])} | {_p(e['always_up'])} | "
                         f"{_p(e['always_down'])} | {_p(e['same_as_yesterday'])} | "
                         f"{_e(e.get('edge_vs_best_baseline'))} |")
            L.append("")
    os.makedirs(os.path.dirname(OUT_MD), exist_ok=True)
    with open(OUT_MD, "w", encoding="utf-8") as fh:
        fh.write("\n".join(L) + "\n")
    slim = json.loads(json.dumps(result))
    for s in slim["scopes"].values():
        s["curve"] = s["curve"][-60:]
        if s["since_v2"].get("cumulative"):
            s["since_v2"]["cumulative"] = s["since_v2"]["cumulative"][-200:]
    with open(OUT_JSON, "w", encoding="utf-8") as fh:
        json.dump(slim, fh, indent=1, ensure_ascii=False)
    print(f"[tracker] wrote {OUT_MD}")


def run() -> dict:
    result = build()
    write(result)
    for scope in ("general", "sectors"):
        s = result["scopes"][scope]
        print(f"[tracker] {scope}: {s['verdict']} (n={s['n_runs']})")
    return result


if __name__ == "__main__":
    run()
