"""Regenerating a sealed h1 day fails and leaves the buy/sell line untouched.

Friday 2026-10-02 stays the sealed close book: cash plus closes is
$19,051.66. The page shows that close. It does not show last_px, which
on that mark is the open.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.hot_n4_clean_v4.forward.book import H1  # noqa: E402
from research.hot_n4_clean_v4.forward.h1_closes import build_closes  # noqa: E402
from research.hot_n4_clean_v4.forward.ledger import load  # noqa: E402
from research.hot_n4_clean_v4.forward.sealed_regen import (  # noqa: E402
    SealedDayRegen,
    buy_sell_record,
    regenerate_day,
    replay_buy_sell,
)

PAGE = ROOT / "dashboard" / "h1" / "index.html"
LOG_JSON = H1.page / "log.json"
DAY = "2026-10-02"
FRIDAY_EQUITY = 19051.655489643774
SEALED_PATHS = (
    H1.folder / "h1_log.jsonl",
    H1.folder / "LEDGER.jsonl",
    LOG_JSON,
)


def _bytes() -> dict[Path, bytes]:
    return {path: path.read_bytes() for path in SEALED_PATHS}


def _unchanged(before: dict[Path, bytes]) -> None:
    for path, raw in before.items():
        if path.read_bytes() != raw:
            raise SystemExit(f"sealed file changed {path.relative_to(ROOT)}")


def _friday(records: list[dict]) -> dict:
    found = [row for row in records if row.get("kind") == "mark" and row.get("date") == DAY]
    if len(found) != 1:
        raise SystemExit(f"friday marks {len(found)}")
    mark = found[0]
    if float(mark["equity_primary"]) != FRIDAY_EQUITY:
        raise SystemExit(f"friday equity {mark['equity_primary']}")
    return mark


def _regen_fails(records: list[dict]) -> None:
    sealed = buy_sell_record(records, DAY)
    if sealed is None or sealed.get("kind") != "open_fill":
        raise SystemExit(f"friday buy/sell line {None if sealed is None else sealed.get('kind')}")
    replay = replay_buy_sell(records, DAY)
    if [row["ticker"] for row in replay["buys"]] != [row["ticker"] for row in sealed["buys"]]:
        raise SystemExit("replay buy list differs")
    if [row["ticker"] for row in replay["sells"]] != [row["ticker"] for row in sealed["sells"]]:
        raise SystemExit("replay sell list differs")
    try:
        regenerate_day(H1.folder, DAY, sealed["buys"], sealed["sells"])
    except SealedDayRegen as exc:
        if "not rewritten" not in str(exc):
            raise SystemExit(f"same list was not a sealed refusal {exc}") from exc
    else:
        raise SystemExit("regenerating the same sealed list did not fail")
    conflict = [{"fill": 1.0, "shares": 1, "ticker": "NOPE"}]
    try:
        regenerate_day(H1.folder, DAY, conflict, [])
    except SealedDayRegen as exc:
        if "conflicting" not in str(exc):
            raise SystemExit(f"conflict was not refused {exc}") from exc
    else:
        raise SystemExit("regenerating a conflicting sealed list did not fail")
    for day in ("2026-09-29", "2026-09-30", "2026-10-01", DAY):
        try:
            regenerate_day(H1.folder, day, conflict, conflict)
        except SealedDayRegen:
            pass
        else:
            raise SystemExit(f"regenerating {day} did not fail")


def _close_total(records: list[dict]) -> None:
    mark = _friday(records)
    closes = build_closes(records, H1.folder)
    day = closes[DAY]
    stock = 0.0
    for lot in mark["holdings"]:
        stock += int(lot["shares"]) * float(day[lot["ticker"]]["close"])
    glnd = next(lot for lot in mark["holdings"] if lot["ticker"] == "GLND")
    if float(day["GLND"]["close"]) == float(glnd["last_px"]):
        raise SystemExit("GLND close is last_px, the open")
    gap = float(mark["equity_primary"]) - (float(mark["cash_primary"]) + stock)
    if abs(gap) > 1e-6:
        raise SystemExit(f"friday cash + closes gap {gap}")
    if abs(day["GLND"]["close"] - 3.73) > 1e-9:
        raise SystemExit(f"GLND close {day['GLND']['close']}")
    page = PAGE.read_text(encoding="utf-8")
    if "last_px" in page:
        raise SystemExit("h1 page displays last_px")
    for phrase in ("cash + closes", "CLOSE", "16:00", "09:30", "union_hot_n4_h1__w0", "append-only"):
        if phrase not in page:
            raise SystemExit(f"h1 page missing {phrase}")


def main() -> None:
    before = _bytes()
    from research.hot_n4_clean_v4.forward.book import reset_book, use_book

    token = use_book(H1)
    try:
        records = load()
        _friday(records)
        _regen_fails(records)
        _close_total(records)
    finally:
        reset_book(token)
        _unchanged(before)
    print("acceptance: regenerating a sealed day failed closed")


if __name__ == "__main__":
    main()
