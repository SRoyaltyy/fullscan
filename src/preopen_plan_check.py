"""Pre-open plan check: fail each strategy BY NAME when it has no plan.

Wrapper module (reads files only; no broker calls, no writes to sealed
rows, no edits to ENGINE_SHA256-pinned files).

For session D (ET) it checks:
  * every Factor Mine recipe (roster = factor_mine names in the latest
    earlier ``data/day_board/<date>_strategy_tickets.json``, plus any in
    today's file) has a row in ``data/day_board/D_strategy_tickets.json``
    that is not a no-input sit (``no same-day panel`` / ``no_same_day_panel``
    / status error). This covers union_hot_n4_holdup.
  * h1: one ``kind == "plan"`` row for D in the forward_h1 log.
  * ``--preopen``: every ``*_preopen`` sleeve has an entry in
    ``data/factor_mine/preopen/seals/D.json`` (the seal lands 09:02-09:25
    ET, so this pass runs after it).

A sit on a real negative day (inputs present, rule says sit) is a plan
and passes. Exit 1 with one ``MISSING <name>: <why>`` line per strategy.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
ET = ZoneInfo("America/New_York")
BOARD = Path("data/day_board")
H1_LOG = Path("research/hot_n4_clean_v4/forward_h1/h1_log.jsonl")
SEALS = Path("data/factor_mine/preopen/seals")
NO_INPUT = ("no same-day panel", "no_same_day_panel", "no pre-open picks")
PREOPEN_SLEEVES = ("union_hot_n4_h1_preopen", "union_hot_n4_holdup_preopen")


def _load(p: Path):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def fm_roster(root: Path, day: str) -> list[str]:
    files = sorted((root / BOARD).glob("*_strategy_tickets.json"))
    prior = [f for f in files if f.name[:10] < day]
    names: set[str] = set()
    for f in prior[-1:] + [root / BOARD / f"{day}_strategy_tickets.json"]:
        doc = _load(f) or {}
        for k, v in (doc.get("strategies") or {}).items():
            if isinstance(v, dict) and v.get("family") == "factor_mine":
                names.add(k)
    return sorted(names)


def check_fm(root: Path, day: str) -> list[tuple[str, str]]:
    path = root / BOARD / f"{day}_strategy_tickets.json"
    doc = _load(path)
    roster = fm_roster(root, day)
    if doc is None:
        return [(n, f"no {path.relative_to(root)}") for n in roster] or [
            ("factor_mine", f"no {path.relative_to(root)}")]
    strat = doc.get("strategies") or {}
    out = []
    for n in roster:
        row = strat.get(n)
        if not isinstance(row, dict):
            out.append((n, "absent from tickets"))
            continue
        text = " ".join(str(row.get(k, "")) for k in ("note", "status", "reason")).lower()
        if row.get("status") == "error" or any(t in text for t in NO_INPUT):
            out.append((n, (row.get("note") or row.get("status") or "no plan")[:120]))
    return out


def check_h1(root: Path, day: str) -> list[tuple[str, str]]:
    n = 0
    try:
        for ln in (root / H1_LOG).read_text(encoding="utf-8").splitlines():
            if ln.strip():
                o = json.loads(ln)
                if o.get("kind") == "plan" and o.get("date") == day:
                    n += 1
    except (OSError, ValueError) as e:
        return [("h1", f"h1 log unreadable: {e}")]
    if n == 1:
        return []
    return [("h1", "no sealed plan row" if n == 0 else f"{n} plan rows (duplicate)")]


def check_preopen(root: Path, day: str) -> list[tuple[str, str]]:
    doc = _load(root / SEALS / f"{day}.json")
    if doc is None:
        return [(n, "no pre-open seal") for n in PREOPEN_SLEEVES]
    sl = doc.get("sleeves") or {}
    return [(n, "absent from seal") for n in PREOPEN_SLEEVES if n not in sl]


def run(root: Path, day: str, preopen: bool) -> list[tuple[str, str]]:
    miss = check_fm(root, day) + check_h1(root, day)
    if preopen:
        miss += check_preopen(root, day)
    return miss


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--date", default="")
    p.add_argument("--root", default=str(ROOT))
    p.add_argument("--preopen", action="store_true")
    a = p.parse_args(argv)
    day = a.date or datetime.now(ET).date().isoformat()
    miss = run(Path(a.root), day, a.preopen)
    for name, why in miss:
        print(f"MISSING {name}: {why}")
    print(f"[plan-check] {day}: {len(miss)} strategies without a pre-09:30 plan")
    return 1 if miss else 0


if __name__ == "__main__":
    sys.exit(main())
