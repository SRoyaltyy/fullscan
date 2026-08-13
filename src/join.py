"""Join engine — labels × weather → ranked stock universe for one trading day.

When --date is set, membership and weather must both exist for THAT date.
No silent fallback to yesterday (prevents writing the wrong day's ranked file).

CLI:
  python -m src.join
  python -m src.join --date 2026-08-13
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
WEATHER_DIR = ROOT / "01_daily" / "weather"
JOIN_DIR = ROOT / "data" / "join"
DAILY_DIR = ROOT / "01_daily"
RULES_PATH = ROOT / "00_grounding" / "join_rules.json"
ET = ZoneInfo("America/New_York")


def _load_json(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def resolve_inputs(date_str: str | None) -> tuple[str, Path, Path]:
    """Return (output_date, membership_path, weather_path).

    Explicit --date: both files must be for that calendar day.
    No --date: use newest weather date and matching membership.
    """
    weathers = sorted(WEATHER_DIR.glob("????-??-??_weather.json"))
    if not weathers:
        raise SystemExit("[join] no weather files — run: python -m src.weather --date YYYY-MM-DD")

    if date_str is None:
        date_str = weathers[-1].name[:10]
        wpath = WEATHER_DIR / f"{date_str}_weather.json"
        mems = sorted(UNIVERSE_DIR.glob("????-??-??_membership.csv"))
        mem_candidates = [m for m in mems if m.name[:10] <= date_str]
        if not mem_candidates:
            raise SystemExit("[join] no membership file — run src.segments first")
        return date_str, mem_candidates[-1], wpath

    # --- strict path for explicit trading day ---
    wpath = WEATHER_DIR / f"{date_str}_weather.json"
    if not wpath.exists():
        raise SystemExit(
            f"[join] no weather for {date_str} ({wpath.name}). "
            f"Run: python -m src.weather --date {date_str}"
        )
    mpath = UNIVERSE_DIR / f"{date_str}_membership.csv"
    if not mpath.exists():
        raise SystemExit(
            f"[join] no membership for {date_str} ({mpath.name}). "
            f"Run: python -m src.segments --date {date_str}"
        )
    return date_str, mpath, wpath


def score_universe(mem: pd.DataFrame, weather: dict, rules: dict) -> pd.DataFrame:
    stances = weather.get("stances", {})
    risk = weather.get("signals", {}).get("risk", "unknown")
    vote_map = rules["vote_map"]
    weights = rules["weather_family_weights"]
    families = rules.get("families") or list(weights.keys())

    rows = []
    for _, r in mem.iterrows():
        ticker = str(r.get("Ticker", "")).strip()
        if not ticker:
            continue
        total = 0.0
        known = 0
        bulls = 0
        bears = 0
        detail_parts = []
        flags = []
        veto = False

        for fam in families:
            w = float(weights.get(fam, 1.0))
            raw = r.get(fam)
            if raw is None or (isinstance(raw, float) and pd.isna(raw)):
                continue
            values = str(raw).split("|") if fam in ("themes", "index") else [str(raw)]
            votes = []
            for v in values:
                v = v.strip()
                if not v or v in ("nan", "None", "unknown"):
                    continue
                st = (stances.get(fam) or {}).get(v) or (stances.get(fam) or {}).get(v.lower())
                if not st:
                    continue
                stance = st.get("stance", "unknown")
                if stance == "unknown":
                    continue
                sign = vote_map.get(stance, 0)
                votes.append(sign * w)
                if sign > 0:
                    bulls += 1
                    detail_parts.append(f"{fam}:{v}=bull")
                elif sign < 0:
                    bears += 1
                    detail_parts.append(f"{fam}:{v}=bear")
            if votes:
                total += sum(votes) / len(votes)
                known += 1

        # gates
        gates = weather.get("gates") or {}
        short_v = str(r.get("short", "")).lower()
        if gates.get("elevated_short_caution") and short_v in ("high", "extreme", "very_high"):
            flags.append("short_caution")
        if gates.get("earnings_proximity") and str(r.get("earn", "")).lower() in ("this_week", "soon", "yes"):
            flags.append("earn_gate")

        # squeeze flag
        if short_v in ("high", "extreme", "very_high") and risk == "risk_on":
            flags.append("squeeze_candidate")

        score_norm = total / known if known else 0.0
        rows.append({
            "Ticker": ticker,
            "sector": r.get("sector", ""),
            "industry": r.get("industry", ""),
            "size": r.get("size", ""),
            "vol": r.get("vol", ""),
            "beta": r.get("beta", ""),
            "total_score": round(total, 4),
            "families_known": known,
            "score_norm": round(score_norm, 4),
            "bulls": bulls,
            "bears": bears,
            "flags": "|".join(flags) if flags else "",
            "veto": veto,
            "detail": "; ".join(detail_parts[:12]),
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    return df.sort_values("total_score", ascending=False).reset_index(drop=True)


def write_report(date_str: str, ranked: pd.DataFrame, weather: dict, rules: dict) -> Path:
    sig = weather.get("signals", {}) or {}
    risk = sig.get("risk", "?")
    n = len(ranked)
    veto_n = int(ranked["veto"].sum()) if n and "veto" in ranked.columns else 0
    longs = ranked[~ranked["veto"]] if n else ranked

    L = [
        f"# Daily match — {date_str}",
        "",
        "Stocks wearing today's lucky badges, minus those wearing cursed ones. "
        f"Full machine file: `data/join/{date_str}_ranked.csv`.",
        "",
        "## Snapshot",
        "",
        f"- **Weather:** risk **{str(risk).upper()}**, yields {sig.get('yields')}, "
        f"VIX {sig.get('vix')}, dollar {sig.get('dollar')}",
        f"- **Universe:** {n:,} stocks scored | {veto_n} vetoed",
        "",
        "## Top 30 — best fit for this environment",
        "",
        "| # | Ticker | Score | Norm | Sector | Industry | Size | Bulls | Bears | Flags |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for i, (_, r) in enumerate(longs.head(30).iterrows(), 1):
        L.append(
            f"| {i} | {r['Ticker']} | {r['total_score']:+.1f} | {r['score_norm']:+.2f} | "
            f"{r.get('sector','')} | {str(r.get('industry',''))[:28]} | {r.get('size','')} | "
            f"{r['bulls']} | {r['bears']} | {r.get('flags') or '—'} |"
        )

    L += [
        "",
        "## Bottom 20 — worst fit",
        "",
        "| Ticker | Score | Sector | Bears | Flags |",
        "|---|---|---|---|---|",
    ]
    for _, r in ranked.tail(20).iloc[::-1].iterrows():
        L.append(
            f"| {r['Ticker']} | {r['total_score']:+.1f} | {r.get('sector','')} | "
            f"{r['bears']} | {r.get('flags') or '—'} |"
        )

    DAILY_DIR.mkdir(parents=True, exist_ok=True)
    path = DAILY_DIR / f"{date_str}_match.md"
    path.write_text("\n".join(L) + "\n", encoding="utf-8")
    return path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None, help="Trading day YYYY-MM-DD (strict when set)")
    args = ap.parse_args()

    rules = _load_json(RULES_PATH)
    if not rules:
        raise SystemExit("[join] 00_grounding/join_rules.json missing")

    date_str, mem_path, weather_path = resolve_inputs(args.date)
    mem = pd.read_csv(mem_path, low_memory=False)
    weather = _load_json(weather_path) or {}

    ranked = score_universe(mem, weather, rules)

    JOIN_DIR.mkdir(parents=True, exist_ok=True)
    ranked_path = JOIN_DIR / f"{date_str}_ranked.csv"
    ranked.to_csv(ranked_path, index=False)
    report = write_report(date_str, ranked, weather, rules)

    longs = ranked[~ranked["veto"]] if len(ranked) else ranked
    print(
        f"[join] {date_str}: {len(ranked):,} scored "
        f"({int(ranked['veto'].sum()) if len(ranked) else 0:,} vetoed) <- {mem_path.name} × "
        f"{weather_path.name} -> {ranked_path.name}, {report.name}"
    )
    if len(longs):
        print(
            "[join] top 5: "
            + ", ".join(
                f"{r['Ticker']} {r['total_score']:+.1f}"
                for _, r in longs.head(5).iterrows()
            )
        )


if __name__ == "__main__":
    main()
