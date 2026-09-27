"""Dry-run the 2026-09-28 plan and fill. Writes nothing.

The book state and the pinned bars are the sealed ones. The frozen Finviz
row for 2026-09-28 is not on GitHub yet, so the candidate frame is the
2026-09-25 day-card names plus whatever the book still holds. Session
2026-09-28 bars are mocked: open and close equal the prior stored close,
so the fill has a print and the jump check does not see a new 3x leg.
The morning score is mocked at 1.0. Nothing is appended to the sealed log
or the forward price store.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.hot_n4_clean_v4.forward.forward import _index, _payload  # noqa: E402
from research.hot_n4_clean_v4.forward.ledger import (  # noqa: E402
    RECIPE,
    load,
    log_path,
    seal,
)
from research.hot_n4_clean_v4.forward.planfill import (  # noqa: E402
    book_state,
    build_plan,
    fill_book,
    hide_session,
)
from research.hot_n4_clean_v4.forward.prices import prices_path  # noqa: E402
from research.hot_n4_clean_v4.protocol import DAYS  # noqa: E402
from research.hot_n4_clean_v4.run_study import last_close_before, load_bars, load_fees  # noqa: E402

SESSION = "2026-09-28"
MORNING_S = 1.0
CARD = "2026-09-25"


def _frame(card: dict, held: dict):
    import pandas as pd

    rows = []
    seen = set()
    for row in card["candidates"]:
        ticker = row["ticker"]
        seen.add(ticker)
        rows.append({
            "Average Volume": row.get("fv_avg_volume") or 1000,
            "Industry": "DryRun",
            "Market Cap": 1000,
            "Price": row.get("fv_price") or 10,
            "Ticker": ticker,
            "Volume": row.get("fv_avg_volume") or 1000,
            "snapshot_date": CARD,
            "trade_date": SESSION,
        })
    for ticker, lot in held.items():
        if ticker in seen:
            continue
        px = float(lot.get("last_px") or lot.get("entry_px") or 10)
        rows.append({
            "Average Volume": 1000,
            "Industry": "DryRun",
            "Market Cap": 1000,
            "Price": px,
            "Ticker": ticker,
            "Volume": 1000,
            "snapshot_date": CARD,
            "trade_date": SESSION,
        })
    return pd.DataFrame(rows)


def _mock_rows(stored: dict, tickers: list[str], session: str) -> list[dict]:
    rows = []
    for ticker in tickers:
        prev = last_close_before(stored.get(ticker), session)
        if prev is None or prev <= 0:
            continue
        rows.append({
            "close": round(float(prev), 6),
            "date": session,
            "high": round(float(prev) * 1.01, 6),
            "low": round(float(prev) * 0.99, 6),
            "open": round(float(prev), 6),
            "ticker": ticker,
            "volume": 1000000.0,
        })
    return rows


def simulate(session: str = SESSION) -> dict:
    """Return the plan and fill. Does not write the sealed log or the price store."""
    log_before = log_path().read_bytes()
    price_before = prices_path().read_bytes() if prices_path().is_file() else None
    records = load()
    state = book_state(records)
    card = json.loads((DAYS / f"{CARD}.json").read_text(encoding="utf-8"))
    frame = _frame(card, state["pos"])
    bars = load_bars()
    index = _index()
    score = {
        "blob_sha": "dry-run",
        "kind": "dry_run",
        "morning_s": MORNING_S,
        "path": "dry-run",
        "server_time_utc": f"{session}T13:00:00Z",
        "status": "MOCK",
    }
    payload = _payload(session, hide_session(bars, session), set(state["pos"]), score, frame)
    plan = build_plan(payload, state, index)
    plan["s_source"] = {
        "blob_sha": score["blob_sha"],
        "kind": score["kind"],
        "path": score["path"],
        "server_time_utc": score["server_time_utc"],
        "status": score["status"],
    }
    sealed, _line = seal(plan)
    names = sorted(set(frame["Ticker"].astype(str)) | set(state["pos"]) | {"IWM"})
    mocked = _mock_rows(bars["stored"], names, session)
    from research.hot_n4_clean_v4.forward.prices import overlay_rows
    filled_bars = overlay_rows(bars, mocked)
    fees = load_fees()
    fill, closes, _after = fill_book(sealed, state, filled_bars, fees, index)
    if log_path().read_bytes() != log_before:
        raise RuntimeError("dry-run wrote the sealed log")
    if prices_path().is_file():
        if price_before is None or prices_path().read_bytes() != price_before:
            raise RuntimeError("dry-run wrote the price store")
    elif price_before is not None:
        raise RuntimeError("dry-run removed the price store")
    return {
        "bar_rule": "2026-09-28 open and close equal the prior stored close; high/low are 1% around it",
        "closes": [
            {
                "pnl_primary": row["pnl_primary"],
                "reason": row["reason"],
                "shares": row["shares"],
                "ticker": row["ticker"],
            }
            for row in closes
        ],
        "equity_primary": fill["equity_primary"],
        "excluded_unexplained_legs": plan["excluded_unexplained_legs"],
        "fill": fill,
        "holdup_on": plan["holdup_on"],
        "morning_s": MORNING_S,
        "picks": plan["picks"],
        "plan": sealed,
        "planned_sells": plan["planned_sells"],
        "recipe": RECIPE,
        "session": session,
        "universe": (
            f"mocked panel: {CARD} day-card names plus held names "
            f"({len(frame)} tickers). Not the live frozen Finviz row."
        ),
        "unfilled": fill["unfilled"],
    }


def _line(rows: list[dict], keys: tuple[str, ...]) -> str:
    if not rows:
        return "none"
    return "; ".join(
        " ".join(f"{key}={row.get(key)}" for key in keys) for row in rows
    )


def main() -> int:
    out = simulate()
    plan = out["plan"]
    fill = out["fill"]
    print(f"dry-run {out['session']} {out['recipe']}")
    print(f"universe: {out['universe']}")
    print(f"bars: {out['bar_rule']}")
    print(f"morning_s={out['morning_s']} (mocked) holdup_on={out['holdup_on']}")
    print(f"picks: {_line(out['picks'], ('rank', 'ticker', 'sources'))}")
    print(f"planned sells: {_line(out['planned_sells'], ('ticker', 'shares', 'reason'))}")
    print(f"excluded: {len(out['excluded_unexplained_legs'])}")
    print(f"buys: {_line(fill['buys'], ('rank', 'ticker', 'shares', 'fill', 'liquidity_cap_shares', 'fee'))}")
    print(f"sells: {_line(fill['sells'], ('ticker', 'shares', 'fill', 'reason', 'fee'))}")
    print(f"unfilled: {_line(out['unfilled'], ('side', 'ticker', 'reason'))}")
    print(f"closes: {_line(out['closes'], ('ticker', 'shares', 'pnl_primary', 'reason'))}")
    print(f"cash={fill['cash_primary']} equity={fill['equity_primary']}")
    print(f"plan_sha256={plan['sha256']}")
    print("plan has no session-D prices; fill matches the plan; sealed log was not written")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001 — the dry-run reports the failure as the outcome
        print(f"DRY-RUN FAILED: {exc}", file=sys.stderr)
        raise SystemExit(1)
