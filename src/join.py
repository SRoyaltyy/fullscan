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
    """Vectorized labels × weather + priors + intrinsincs.

    Unknown weather does not zero the universe — label_priors still rank.
    """
    import numpy as np

    df = mem.copy()
    df["Ticker"] = df["Ticker"].astype(str).str.strip()
    df = df[df["Ticker"].ne("") & df["Ticker"].ne("nan")].copy()
    n = len(df)
    if n == 0:
        return pd.DataFrame()

    stances = weather.get("stances") or {}
    risk = (weather.get("signals") or {}).get("risk", "unknown")
    vote_map = rules["vote_map"]
    weights = rules.get("weather_family_weights") or {}
    priors = {k: v for k, v in (rules.get("label_priors") or {}).items()
              if isinstance(v, dict) and not k.startswith("_")}
    intrins = {k: v for k, v in (rules.get("intrinsic_votes") or {}).items()
               if isinstance(v, dict) and not k.startswith("_")}
    gates = weather.get("gates") or {}

    total = np.zeros(n, dtype=float)
    known = np.zeros(n, dtype=float)
    bulls = np.zeros(n, dtype=int)
    bears = np.zeros(n, dtype=int)
    details: list[list[str]] = [[] for _ in range(n)]

    def _add(votes: np.ndarray, labels: list[str] | None = None) -> None:
        nonlocal total, known, bulls, bears
        total += votes
        nz = votes != 0
        known += nz.astype(float)
        bulls += (votes > 0).astype(int)
        bears += (votes < 0).astype(int)
        if labels:
            for i, lab in enumerate(labels):
                if lab and votes[i] != 0:
                    details[i].append(lab)

    # weather-family votes
    for fam, w in weights.items():
        w = float(w or 0)
        if w == 0 or fam not in df.columns:
            continue
        fam_st = stances.get(fam) or {}
        mapping = {}
        for k, st in fam_st.items():
            mapping[str(k)] = vote_map.get((st or {}).get("stance", "unknown"), 0) * w
        raw = df[fam].astype(str)
        if fam in ("themes", "index"):
            votes = np.zeros(n, dtype=float)
            labs = [""] * n
            for i, cell in enumerate(raw.tolist()):
                parts = [p.strip() for p in str(cell).split("|") if p.strip() and p.strip() not in ("nan", "None", "unknown", "")]
                if not parts:
                    continue
                vs = []
                tags = []
                for p in parts:
                    key = p.split(":", 1)[-1] if fam == "themes" and ":" in p else p
                    # themes stored as "theme:ai_capex" or "ai_capex"
                    v = mapping.get(p, mapping.get(key, mapping.get(f"theme:{key}", 0)))
                    if fam == "themes":
                        # theme weather may be missing; small prior if tagged at all
                        if v == 0:
                            v = 0.25 * w
                    vs.append(v)
                    if v:
                        tags.append(f"{fam}:{key}={'bull' if v>0 else 'bear'}")
                if vs:
                    votes[i] = sum(vs) / len(vs)
                    labs[i] = ",".join(tags[:3])
            _add(votes, labs)
        else:
            votes = raw.map(mapping).fillna(0.0).to_numpy(dtype=float)
            labs = [f"{fam}:{v}=bull" if votes[i] > 0 else (f"{fam}:{v}=bear" if votes[i] < 0 else "")
                    for i, v in enumerate(raw.tolist())]
            _add(votes, labs)

    # label priors (always — this is the join engine when weather is mixed)
    for fam, pmap in priors.items():
        if fam not in df.columns:
            continue
        raw = df[fam].astype(str)
        votes = raw.map(pmap).fillna(0.0).to_numpy(dtype=float)
        labs = [f"prior:{fam}:{v}={votes[i]:+.2f}" if votes[i] else ""
                for i, v in enumerate(raw.tolist())]
        _add(votes, labs)

    # intrinsic votes
    for fam, spec in intrins.items():
        if fam not in df.columns:
            continue
        raw = df[fam].astype(str).str.lower()
        w = float(spec.get("weight") or 0)
        mmap = spec.get("map") or {}
        mapped = raw.map(mmap).fillna(0.0).to_numpy(dtype=float) * w
        labs = [f"{fam}:{v}={'bull' if mapped[i]>0 else 'bear'}" if mapped[i] else ""
                for i, v in enumerate(raw.tolist())]
        _add(mapped, labs)

    # gates
    earn = df["earn"].astype(str).str.lower() if "earn" in df.columns else pd.Series([""] * n, index=df.index)
    ext = df["ext"].astype(str).str.lower() if "ext" in df.columns else pd.Series([""] * n, index=df.index)
    liq = df["liq"].astype(str).str.lower() if "liq" in df.columns else pd.Series([""] * n, index=df.index)
    short = df["short"].astype(str).str.lower() if "short" in df.columns else pd.Series([""] * n, index=df.index)
    flags = []
    veto = np.zeros(n, dtype=bool)
    earn_l = earn.tolist()
    ext_l = ext.tolist()
    liq_l = liq.tolist()
    short_l = short.tolist()
    for i in range(n):
        f = []
        if gates.get("elevated_short_caution") and short_l[i] in ("high", "extreme", "very_high"):
            f.append("short_caution")
        if gates.get("earnings_proximity") and earn_l[i] in ("this_week", "today"):
            f.append("earn_gate")
        if earn_l[i] == "today" and gates.get("veto_earn_today"):
            f.append("earn_today")
            veto[i] = True
        if liq_l[i] == "low":
            f.append("liq_low")
            total[i] *= 0.5
        if ext_l[i] == "extreme" and gates.get("veto_extreme_risk_off"):
            f.append("ext_riskoff")
            veto[i] = True
        if short_l[i] in ("high", "extreme", "very_high") and risk in ("on", "risk_on"):
            f.append("squeeze_candidate")
        flags.append("|".join(f))

    # z-score so stock_book tanh(s_join) has real spread
    mu, sd = float(np.mean(total)), float(np.std(total))
    if sd and sd == sd and sd > 1e-9:
        z = (total - mu) / sd
    else:
        z = total.copy()
    z = np.clip(z, -2.5, 2.5)

    extra_cols = [c for c in (
        "mom", "ext", "range", "profit", "themes", "earn", "earnsurp", "analyst",
        "rsi", "sma20", "instown", "peg", "sales_g", "q_mom", "liq", "rvol", "roe",
    ) if c in df.columns]

    out = pd.DataFrame({
        "Ticker": df["Ticker"].values,
        "sector": df["sector"].values if "sector" in df.columns else "",
        "industry": df["industry"].values if "industry" in df.columns else "",
        "size": df["size"].values if "size" in df.columns else "",
        "vol": df["vol"].values if "vol" in df.columns else "",
        "beta": df["beta"].values if "beta" in df.columns else "",
        "total_score": np.round(total, 4),
        "families_known": known.astype(int),
        "score_norm": np.round(z, 4),
        "bulls": bulls,
        "bears": bears,
        "flags": flags,
        "veto": veto,
        "detail": ["; ".join(d[:10]) for d in details],
    })
    for c in extra_cols:
        out[c] = df[c].values
    return out.sort_values("total_score", ascending=False).reset_index(drop=True)


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

    # Drop funds / unlabeled size so join is a stock universe, not ETFs
    if "size" in mem.columns:
        before = len(mem)
        mem = mem[mem["size"].astype(str).str.lower().isin(
            ["micro", "small", "mid", "large", "mega"]
        )].copy()
        print(f"[join] drop unlabeled size: {before:,} → {len(mem):,}")

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
