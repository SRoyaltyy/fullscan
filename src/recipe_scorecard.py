#!/usr/bin/env python3
"""Build data/factor_mine/recipe_scorecard.json from the daily ledgers.

The per-recipe equity curves live in data/factor_mine/ledgers/<date>.json.gz
(each recipe's `primary.daily.equity`, starting cash 10,000 on its first
day). This script folds those daily files into one compact scorecard the
dashboards can fetch without parsing megabytes:

  {
    "generated_at": "...Z",
    "baseline_cash": 10000.0,
    "start_date": "2026-08-13",
    "last_date": "2026-10-06",
    "n_days": 38,
    "recipes": {
      "<recipe>": {
        "equity": 12134.5,          # last close equity
        "total_ret_pct": 21.3,      # vs baseline_cash
        "ret_recent_pct": 3.2,      # last RECENT_SESSIONS sessions
        "n_trades": 123,            # cumulative filled trades
        "first_date": "2026-08-13",
        "last_date": "2026-10-06"
      }, ...
    }
  }

Stdlib only. Reads only data/factor_mine/ledgers/ and writes only
data/factor_mine/recipe_scorecard.json.

Run: python3 src/recipe_scorecard.py [--root .] [--out data/factor_mine/recipe_scorecard.json]
"""
from __future__ import annotations

import argparse
import datetime as dt
import gzip
import json
import sys
from pathlib import Path

BASELINE_CASH = 10_000.0
RECENT_SESSIONS = 20
LEDGERS = Path("data/factor_mine/ledgers")
OUT = Path("data/factor_mine/recipe_scorecard.json")


def fold_ledgers(root: Path) -> dict:
    """Per-recipe daily equity series from every ledger file, oldest first."""
    series: dict[str, list[tuple[str, float, int]]] = {}
    ledger_dir = root / LEDGERS
    if not ledger_dir.is_dir():
        raise SystemExit(f"[recipe-scorecard] no ledger dir: {ledger_dir}")
    files = sorted(ledger_dir.glob("*.json.gz"))
    if not files:
        raise SystemExit(f"[recipe-scorecard] no ledger files in {ledger_dir}")
    for f in files:
        day = f.name[:10]
        try:
            doc = json.loads(gzip.decompress(f.read_bytes()))
        except (OSError, json.JSONDecodeError) as e:
            print(f"[recipe-scorecard] SKIP unreadable ledger {f.name}: {e}")
            continue
        recipes = doc.get("recipes") or {}
        for name, body in recipes.items():
            daily = (body.get("primary") or {}).get("daily") or {}
            equity = daily.get("equity")
            if not isinstance(equity, (int, float)):
                continue
            trades = len((body.get("primary") or {}).get("trades") or [])
            series.setdefault(name, []).append((day, float(equity), trades))
    return series


def build_scorecard(series: dict[str, list[tuple[str, float, int]]]) -> dict:
    recipes: dict[str, dict] = {}
    for name, days in series.items():
        days.sort()  # ISO dates sort chronologically
        first_date, first_eq, _ = days[0]
        last_date, last_eq, _ = days[-1]
        n_trades = sum(t for _, _, t in days)
        base = first_eq if first_date != last_date else BASELINE_CASH
        # day-one equity already reflects day-one P&L; when only one day
        # exists, compare against the notional starting cash.
        total_ret = (last_eq / base - 1.0) * 100.0 if base else 0.0
        if len(days) > RECENT_SESSIONS:
            ref_eq = days[-1 - RECENT_SESSIONS][1]
            recent = (last_eq / ref_eq - 1.0) * 100.0 if ref_eq else 0.0
        else:
            recent = total_ret
        recipes[name] = {
            "equity": round(last_eq, 2),
            "total_ret_pct": round(total_ret, 3),
            "ret_recent_pct": round(recent, 3),
            "n_trades": n_trades,
            "first_date": first_date,
            "last_date": last_date,
        }
    all_days = sorted({d for days in series.values() for d, _, _ in days})
    return {
        "generated_at": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "baseline_cash": BASELINE_CASH,
        "recent_sessions": RECENT_SESSIONS,
        "start_date": all_days[0] if all_days else None,
        "last_date": all_days[-1] if all_days else None,
        "n_days": len(all_days),
        "n_recipes": len(recipes),
        "recipes": recipes,
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--root", default=".", help="repo root (default: cwd)")
    ap.add_argument("--out", default=str(OUT), help="output JSON path")
    ap.add_argument("--check", action="store_true",
                    help="exit 1 if the existing scorecard differs from a rebuild")
    args = ap.parse_args(argv)
    root = Path(args.root)

    scorecard = build_scorecard(fold_ledgers(root))
    out = root / args.out
    payload = json.dumps(scorecard, indent=1, sort_keys=False) + "\n"

    if args.check:
        existing = out.read_text(encoding="utf-8") if out.is_file() else None
        if existing == payload:
            print("[recipe-scorecard] up to date")
            return 0
        print("[recipe-scorecard] STALE — regenerate with: python3 src/recipe_scorecard.py")
        return 1

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(payload, encoding="utf-8")
    top = sorted(scorecard["recipes"].items(),
                 key=lambda kv: kv[1]["total_ret_pct"], reverse=True)[:5]
    print(f"[recipe-scorecard] wrote {out} — {scorecard['n_recipes']} recipes, "
          f"{scorecard['n_days']} days ({scorecard['start_date']}..{scorecard['last_date']})")
    for name, r in top:
        print(f"  {name:<40} {r['total_ret_pct']:+.2f}% total  {r['ret_recent_pct']:+.2f}% 20sess")
    return 0


if __name__ == "__main__":
    sys.exit(main())
