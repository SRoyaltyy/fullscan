"""Join engine — the MATCH of the label → regime → join design.

Labels (src/segments.py) say what a stock is. Weather (src/weather.py) says
what today favors. This module intersects them: every stock gets one total
score, plus a full label-by-label breakdown so the ranking is explainable.

Design guards (from 00_grounding/join_rules.json — retune there, not here):
  * ONE VOTE PER FAMILY — correlated labels in the same family (e.g.
    profit:no + size:micro + style:growth on one junk biotech) never stack.
  * unknown = 0 — missing data is never a vote in either direction.
  * multi-tag families (index, themes) vote as the mean of member tags.
  * gates are vetoes/flags, not silent score edits.
  * intrinsic labels (earnsurp, analyst) carry smaller fixed weights so the
    environment dominates the score.

Inputs:
  data/universe/<date>_membership.csv   (fallback: newest membership <= date)
  01_daily/weather/<date>_weather.json  (fallback: newest weather <= date)

Outputs:
  data/join/<date>_ranked.csv           every stock: total score, votes,
                                        flags, veto — the full ranked universe
  01_daily/<date>_match.md              human report: top/bottom tables with
                                        per-stock "label: bull/bear" breakdown

CLI:
  python -m src.join                    # latest weather date
  python -m src.join --date 2026-08-12
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
UNIVERSE_DIR = ROOT / "data" / "universe"
JOIN_DIR = ROOT / "data" / "join"
WEATHER_DIR = ROOT / "01_daily" / "weather"
DAILY_DIR = ROOT / "01_daily"
RULES_PATH = ROOT / "00_grounding" / "join_rules.json"
ET = ZoneInfo("America/New_York")

WEATHER_FAMILIES = ["sector", "size", "index", "geo", "beta", "short", "vol",
                    "profit", "lev", "style", "mom", "ext", "range"]


def _load_json(p: Path):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def resolve_inputs(date_str: str | None) -> tuple[str, Path, Path]:
    weathers = sorted(WEATHER_DIR.glob("????-??-??_weather.json"))
    if not weathers:
        raise SystemExit("[join] no weather files found — run src.weather first")
    if date_str is None:
        date_str = weathers[-1].name[:10]
    wpath = WEATHER_DIR / f"{date_str}_weather.json"
    if not wpath.exists():
        older = [w for w in weathers if w.name[:10] <= date_str]
        if not older:
            raise SystemExit(f"[join] no weather file for {date_str}")
        wpath = older[-1]
    mems = sorted(UNIVERSE_DIR.glob("????-??-??_membership.csv"))
    mem_candidates = [m for m in mems if m.name[:10] <= wpath.name[:10]]
    if not mem_candidates:
        raise SystemExit("[join] no membership file found — run src.segments first")
    return wpath.name[:10], mem_candidates[-1], wpath


# ---------------------------------------------------------------- scoring

def score_universe(mem: pd.DataFrame, weather: dict, rules: dict) -> pd.DataFrame:
    stances = weather.get("stances", {})
    risk = weather.get("signals", {}).get("risk", "unknown")
    vote_map = rules["vote_map"]
    weights = rules["weather_family_weights"]
    intrinsic = rules["intrinsic_votes"]

    rows = []
    for _, r in mem.iterrows():
        detail: dict[str, dict] = {}
        total = 0.0
        n_bull = n_bear = 0

        for fam in WEATHER_FAMILIES:
            fst = stances.get(fam, {})
            w = weights.get(fam, 1.0)
            raw = str(r.get(fam, "") or "")
            if fam in ("index", "themes") and "|" in raw:
                tags = [t for t in raw.split("|") if t]
                votes = [vote_map.get(fst.get(t, {}).get("stance", "unknown"), 0)
                         for t in tags]
                if not votes or not fst:
                    detail[fam] = {"labels": raw, "stance": "unknown", "vote": 0}
                    continue
                vote = sum(votes) / len(votes)
                stance = ("favorable" if vote > 0.25 else "hostile" if vote < -0.25
                          else "neutral")
                detail[fam] = {"labels": raw, "stance": stance, "vote": round(vote, 2)}
            else:
                if not fst or raw in ("", "unknown", "nan"):
                    detail[fam] = {"labels": raw or "unknown", "stance": "unknown",
                                   "vote": 0}
                    continue
                st = fst.get(raw, {}).get("stance", "unknown")
                vote = float(vote_map.get(st, 0))
                detail[fam] = {"labels": raw, "stance": st, "vote": vote}
            contrib = w * detail[fam]["vote"]
            detail[fam]["contribution"] = round(contrib, 2)
            total += contrib
            if detail[fam]["vote"] > 0:
                n_bull += 1
            elif detail[fam]["vote"] < 0:
                n_bear += 1

        for fam, cfg in intrinsic.items():
            if fam.startswith("_"):
                continue
            raw = str(r.get(fam, "") or "")
            vote = float(cfg["map"].get(raw, 0))
            contrib = cfg["weight"] * vote
            detail[fam] = {"labels": raw or "unknown",
                           "stance": "intrinsic", "vote": vote,
                           "contribution": round(contrib, 2)}
            total += contrib
            if vote > 0:
                n_bull += 1
            elif vote < 0:
                n_bear += 1

        flags, veto = [], False
        if r.get("earn") == "today":
            veto = True
            flags.append("earn:today")
        elif r.get("earn") == "this_week":
            flags.append("earn:this_week")
        if r.get("liq") == "low":
            flags.append("liq:low")
        if r.get("ext") == "extreme":
            if risk == "off":
                veto = True
                flags.append("ext:extreme+risk_off")
            else:
                flags.append("ext:extreme")
        if (r.get("short") == "extreme"
                and stances.get("short", {}).get("extreme", {}).get("stance") == "favorable"):
            flags.append("squeeze_candidate")

        rows.append({
            "Ticker": r["Ticker"],
            "sector": r.get("sector", ""),
            "industry": r.get("industry", ""),
            "size": r.get("size", ""),
            "total_score": round(total, 2),
            "bulls": n_bull,
            "bears": n_bear,
            "flags": "|".join(flags),
            "veto": veto,
            "detail": json.dumps(detail, ensure_ascii=False),
        })

    out = pd.DataFrame(rows)
    return out.sort_values("total_score", ascending=False).reset_index(drop=True)


# ---------------------------------------------------------------- report

def _fmt_breakdown(detail_json: str, disp: dict, limit: int | None = None) -> str:
    d = json.loads(detail_json)
    parts = []
    for fam, v in d.items():
        if v["stance"] == "intrinsic":
            tag = ("bull" if v["vote"] > 0 else "bear" if v["vote"] < 0 else "neutral")
        else:
            tag = disp.get(v["stance"], "—")
        parts.append(f"{fam}:{v['labels']} {tag}")
    if limit:
        parts = parts[:limit]
    return " | ".join(parts)


def write_report(date_str: str, ranked: pd.DataFrame, weather: dict,
                 rules: dict) -> Path:
    disp = rules["display_map"]
    sig = weather.get("signals", {})
    n = len(ranked)
    longs = ranked[~ranked["veto"]]
    vetoed = ranked[ranked["veto"]]

    L = [f"# Daily match — {date_str}", "",
         "Stocks wearing today's lucky badges, minus those wearing cursed "
         "ones. One vote per label family; unknown data never votes; gates "
         "veto or flag. Full machine file: `data/join/"
         f"{date_str}_ranked.csv`.", "",
         "## Snapshot", "",
         f"- **Weather:** risk **{sig.get('risk', '?').upper()}**, yields "
         f"{sig.get('yields', '?')}, VIX {sig.get('vix', '?')}, dollar "
         f"{sig.get('dollar', '?')}",
         f"- **Universe:** {n:,} stocks scored | {len(vetoed):,} vetoed by "
         f"gates (listed at the bottom)",
         ""]

    L += ["## Top 30 — best fit for this environment", "",
          "| # | Ticker | Score | Sector | Industry | Size | Bulls | Bears | Flags |",
          "|---|---|---|---|---|---|---|---|---|"]
    for i, (_, r) in enumerate(longs.head(30).iterrows(), 1):
        L.append(f"| {i} | {r['Ticker']} | {r['total_score']:+.1f} | {r['sector']} "
                 f"| {r['industry']} | {r['size']} | {r['bulls']} | {r['bears']} "
                 f"| {r['flags'] or '—'} |")
    L.append("")

    L += ["## Bottom 15 — worst fit for this environment", "",
          "| # | Ticker | Score | Sector | Industry | Size | Bulls | Bears | Flags |",
          "|---|---|---|---|---|---|---|---|---|"]
    for i, (_, r) in enumerate(ranked.tail(15).iloc[::-1].iterrows(), 1):
        L.append(f"| {i} | {r['Ticker']} | {r['total_score']:+.1f} | {r['sector']} "
                 f"| {r['industry']} | {r['size']} | {r['bulls']} | {r['bears']} "
                 f"| {r['flags'] or '—'} |")
    L.append("")

    L += ["## Label-by-label breakdown — top 10", ""]
    for _, r in longs.head(10).iterrows():
        L.append(f"### {r['Ticker']} — total {r['total_score']:+.1f}"
                 + (f"  ⚑ {r['flags']}" if r['flags'] else ""))
        L.append("")
        L.append(_fmt_breakdown(r["detail"], disp))
        L.append("")

    if len(vetoed):
        L += ["## Vetoed by gates", ""]
        for _, r in vetoed.iterrows():
            L.append(f"- **{r['Ticker']}** (score would be {r['total_score']:+.1f}) — {r['flags']}")
        L.append("")

    DAILY_DIR.mkdir(parents=True, exist_ok=True)
    path = DAILY_DIR / f"{date_str}_match.md"
    path.write_text("\n".join(L) + "\n", encoding="utf-8")
    return path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    args = ap.parse_args()

    rules = _load_json(RULES_PATH)
    if not rules:
        raise SystemExit("[join] 00_grounding/join_rules.json missing")

    date_str, mem_path, weather_path = resolve_inputs(args.date)
    mem = pd.read_csv(mem_path, low_memory=False)
    weather = _load_json(weather_path)

    ranked = score_universe(mem, weather, rules)

    JOIN_DIR.mkdir(parents=True, exist_ok=True)
    ranked_path = JOIN_DIR / f"{date_str}_ranked.csv"
    ranked.to_csv(ranked_path, index=False)
    report = write_report(date_str, ranked, weather, rules)

    longs = ranked[~ranked["veto"]]
    print(f"[join] {date_str}: {len(ranked):,} scored "
          f"({int(ranked['veto'].sum()):,} vetoed) <- {mem_path.name} × "
          f"{weather_path.name} -> {ranked_path.name}, {report.name}")
    print(f"[join] top 5: "
          + ", ".join(f"{r['Ticker']} {r['total_score']:+.1f}"
                      for _, r in longs.head(5).iterrows()))


if __name__ == "__main__":
    main()
