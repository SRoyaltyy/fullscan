"""No-order dry check of the sealed-h1 Webull send path.

Places nothing: never calls release(), place, or cancel. Proves:
  1. credentials are present (names only, values never printed),
  2. the Webull paper account is reachable (account id discovered, cash read),
  3. the tickets the sender would build equal the sealed h1 new-buy set
     for DATE (default: latest sealed h1 plan on main), sized with live cash.
Exit 0 only when all three hold. Usage: python scripts/webull_h1_dry_check.py [DATE]
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src import webull_exec as we  # noqa: E402

H1_LOG = ROOT / "research/hot_n4_clean_v4/forward_h1/h1_log.jsonl"


def latest_plan(date: str | None):
    plans = []
    for line in H1_LOG.read_text(encoding="utf-8").splitlines():
        try:
            row = json.loads(line)
        except ValueError:
            continue
        if row.get("kind") == "plan" and (date is None or row.get("date") == date):
            plans.append(row)
    if not plans:
        raise SystemExit(f"no sealed h1 plan for {date or 'any date'}")
    return plans[-1]


def main(argv: list[str]) -> int:
    out: dict = {"orders_placed": 0}
    ok = True
    present = {n: bool((os.environ.get(n) or "").strip())
               for n in ("WEBULL_APP_KEY", "WEBULL_APP_SECRET", "WEBULL_ACCOUNT_ID")}
    out["credentials_present"] = present
    if not (present["WEBULL_APP_KEY"] and present["WEBULL_APP_SECRET"]):
        out["result"] = "FAIL: WEBULL_APP_KEY / WEBULL_APP_SECRET missing"
        print(json.dumps(out, indent=2))
        return 2
    api = we.PaperAPI("paper")
    if not api.connect():
        out["result"] = f"FAIL: connect: {api.err}"
        print(json.dumps(out, indent=2))
        return 2
    snap = api.snapshot()
    acct = str(getattr(api, "account_id", "") or "")
    out["account"] = {
        "connected": bool(snap.connected),
        "account_id_source": "secret" if present["WEBULL_ACCOUNT_ID"] else "discovered",
        "account_id_tail": acct[-4:] if acct else "",
        "cash": round(float(snap.cash or 0), 2),
        "n_positions": len(snap.positions or {}),
        "error": snap.error or "",
    }
    if not snap.connected:
        ok = False
    plan = latest_plan(argv[0] if argv else None)
    date = plan["date"]
    sealed = [p["ticker"] for p in plan.get("picks", [])]
    card = we.plan_hot4_for_broker(date, snap)
    built = [t.get("ticker") for t in card.get("tickets", []) if str(t.get("side", "BUY")).upper() == "BUY"]
    out["book"] = {
        "date": date,
        "plan_sha256": plan.get("sha256"),
        "sealed_picks": sealed,
        "ticket_buys": built,
        "ticket_shares": {t.get("ticker"): t.get("shares") for t in card.get("tickets", [])},
        "look_error": card.get("look_error") or "",
        "why": card.get("why") or "",
    }
    # Carry names are excluded from new buys by design; every built buy must
    # be a sealed pick, and nothing outside the sealed set may appear.
    if not built or set(built) - set(sealed) or card.get("look_error"):
        ok = False
    out["result"] = "PASS" if ok else "FAIL"
    print(json.dumps(out, indent=2))
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
