"""Today's plan block. A sealed plan is visible before the open fill."""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.hot_n4_clean_v4.forward.render import day_block_html, today_plan_html  # noqa: E402

SESSION = "2026-09-28"
SHA = "b129d5505394941231f1e2c61c37949b9bad920fff9c9b09df3b51e6dd7b6951"
WHEN = datetime(2026, 9, 28, 13, 0, tzinfo=timezone.utc)
SEALED = (
    ROOT / "research/hot_n4_clean_v4/forward/LEDGER.jsonl",
    ROOT / "research/hot_n4_clean_v4/forward/PRICE_LEDGER.jsonl",
    ROOT / "research/hot_n4_clean_v4/forward/holdup_log.jsonl",
    ROOT / "research/hot_n4_clean_v4/forward/skips.jsonl",
    ROOT / "research/hot_n4_clean_v4/forward_h1/LEDGER.jsonl",
    ROOT / "research/hot_n4_clean_v4/forward_h1/PRICE_LEDGER.jsonl",
    ROOT / "research/hot_n4_clean_v4/forward_h1/h1_log.jsonl",
    ROOT / "research/hot_n4_clean_v4/forward_h1/skips.jsonl",
    ROOT / "dashboard/holdup/log.json",
    ROOT / "dashboard/h1/log.json",
)


def _plan(**extra) -> dict:
    body = {
        "committed_at": "2026-09-28T12:36:56Z",
        "date": SESSION,
        "holdup_on": False,
        "kind": "plan",
        "morning_s": -6.14,
        "picks": [
            {"rank": 1, "sources": ["yday_gainer", "yday_mover"], "ticker": "USDE"},
            {"rank": 2, "sources": ["yday_gainer", "yday_mover"], "ticker": "SRFM"},
            {"rank": 3, "sources": ["yday_gainer", "yday_mover"], "ticker": "SHMD"},
            {"rank": 4, "sources": ["ohlc_hot"], "ticker": "FEAM"},
        ],
        "planned_sells": [
            {"reason": "hold-expired", "shares": 377, "ticker": "GLND"},
            {"reason": "hold-expired", "shares": 58, "ticker": "SECZ"},
            {"reason": "hold-expired", "shares": 35, "ticker": "TJGC"},
        ],
        "recipe": "union_hot_n4_holdup__w0",
        "s_source": {"status": "BEFORE_1330"},
        "sha256": SHA,
    }
    body.update(extra)
    return body


def _need(html: str, phrase: str) -> None:
    if phrase not in html:
        raise SystemExit(f"missing {phrase}")


def _unfilled() -> None:
    html = today_plan_html([_plan()], now=WHEN, seal_commits={SHA: "5e6a3d04"})
    for phrase in (
        "Today's plan",
        "2026-09-28",
        "seal commit 5e6a3d04",
        "08:36 ET",
        "20:36 HKT",
        "morning score -6.14 · status BEFORE_1330",
        "holdup 2-day hold off",
        "GLND 377 hold-expired",
        "SECZ 58 hold-expired",
        "TJGC 35 hold-expired",
        "USDE yday_gainer/yday_mover · fills at 09:35 ET open",
        "SRFM yday_gainer/yday_mover · fills at 09:35 ET open",
        "SHMD yday_gainer/yday_mover · fills at 09:35 ET open",
        "FEAM ohlc_hot · fills at 09:35 ET open",
        "sealed",
    ):
        _need(html, phrase)
    if "ABSENT" in html or "filled" in html:
        raise SystemExit("unfilled plan showed ABSENT or a fill")
    if "morning_status" in _plan():
        raise SystemExit("fixture invented morning_status")


def _filled() -> None:
    opened = {
        "buys": [
            {"fill": 17.25, "shares": 10, "sources": ["yday_gainer", "yday_mover"], "ticker": "USDE"},
            {"fill": 1.11, "shares": 20, "sources": ["yday_gainer", "yday_mover"], "ticker": "SRFM"},
            {"fill": 4.37, "shares": 30, "sources": ["yday_gainer", "yday_mover"], "ticker": "SHMD"},
            {"fill": 2.8, "shares": 40, "sources": ["ohlc_hot"], "ticker": "FEAM"},
        ],
        "date": SESSION,
        "kind": "open_fill",
        "sells": [
            {"fill": 6.5, "reason": "hold-expired", "shares": 377, "ticker": "GLND"},
            {"fill": 16.2, "reason": "hold-expired", "shares": 58, "ticker": "SECZ"},
            {"fill": 29.76, "reason": "hold-expired", "shares": 35, "ticker": "TJGC"},
        ],
    }
    html = today_plan_html([_plan(), opened], now=WHEN, seal_commits={SHA: "5e6a3d04"})
    for phrase in (
        "GLND 377 hold-expired @ 6.50 filled",
        "SECZ 58 hold-expired @ 16.20 filled",
        "TJGC 35 hold-expired @ 29.76 filled",
        "USDE 10 yday_gainer/yday_mover @ 17.25 filled",
        "FEAM 40 ohlc_hot @ 2.80 filled",
        "filled",
    ):
        _need(html, phrase)
    if "fills at 09:35 ET open" in html:
        raise SystemExit("filled plan still says it fills at the open")


def _partial() -> None:
    opened = {
        "buys": [
            {"fill": 17.25, "shares": 10, "ticker": "USDE"},
        ],
        "date": SESSION,
        "kind": "open_fill",
        "sells": [
            {"fill": 6.5, "reason": "hold-expired", "shares": 377, "ticker": "GLND"},
        ],
        "unfilled": [
            {"reason": "no open", "side": "buy", "ticker": "FEAM"},
        ],
    }
    html = today_plan_html([_plan(), opened], now=WHEN)
    _need(html, "USDE 10 yday_gainer/yday_mover @ 17.25 filled")
    _need(html, "GLND 377 hold-expired @ 6.50 filled")
    _need(html, "FEAM ohlc_hot · fills at 09:35 ET open")
    _need(html, "SECZ 58 hold-expired")
    if "SECZ 58 hold-expired @" in html:
        raise SystemExit("unpriced sell was marked filled")


def _empty() -> None:
    later = datetime(2026, 9, 29, 12, 0, tzinfo=timezone.utc)
    html = today_plan_html([_plan()], now=later)
    exact = "no plan sealed yet for 2026-09-29; plan runs 09:05 ET"
    _need(html, exact)
    if "USDE" in html or "GLND" in html:
        raise SystemExit("empty board still showed the previous plan")
    weekend = datetime(2026, 9, 26, 15, 0, tzinfo=timezone.utc)
    before = today_plan_html([], now=weekend)
    _need(before, "no plan sealed yet for 2026-09-28; plan runs 09:05 ET")


def _absent() -> None:
    reason = "no predict or weather file for 2026-09-28 was on HEAD before 13:30 UTC"
    plan = _plan(
        holdup_on=True,
        morning_reason=reason,
        morning_s=None,
        morning_status="ABSENT",
        s_source={"reason": reason, "status": "ABSENT"},
    )
    html = today_plan_html([plan], now=WHEN, seal_commits={})
    _need(html, f"morning ABSENT · score null · {reason}")
    _need(html, "holdup 2-day hold on")
    _need(html, SHA[:12])
    bare = _plan()
    bare.pop("s_source", None)
    quiet = today_plan_html([bare], now=WHEN, seal_commits={})
    if "ABSENT" in quiet:
        raise SystemExit("missing morning_status was shown as ABSENT")
    _need(quiet, "morning score -6.14")


NOTE = (
    "close fill held: SRFM and SECZ open prices were read from the previous "
    "session's bar; awaiting correction decision"
)


def _opened_pending() -> None:
    """2026-09-28 with a sealed plan and an open fill is not a missing day."""
    opened = {
        "buys": [
            {"fill": 0.9439, "rank": 2, "shares": 1313, "ticker": "SRFM"},
            {"fill": 4.68, "rank": 3, "shares": 267, "ticker": "SHMD"},
            {"fill": 2.75, "rank": 4, "shares": 454, "ticker": "FEAM"},
        ],
        "date": SESSION,
        "holdings": [
            {"entry_date": "2026-09-28", "min_hold": 1, "shares": 454, "ticker": "FEAM"},
            {"entry_date": "2026-09-28", "min_hold": 1, "shares": 267, "ticker": "SHMD"},
            {"entry_date": "2026-09-28", "min_hold": 1, "shares": 1313, "ticker": "SRFM"},
            {"entry_date": "2026-09-25", "min_hold": 2, "shares": 520, "ticker": "USDE"},
        ],
        "kind": "open_fill",
        "sells": [
            {"fill": 5.138, "reason": "hold-expired", "shares": 377, "ticker": "GLND"},
            {"fill": 16.209999, "reason": "hold-expired", "shares": 58, "ticker": "SECZ"},
            {"fill": 26.5, "reason": "hold-expired", "shares": 35, "ticker": "TJGC"},
        ],
    }
    html = day_block_html([_plan(), opened], SESSION)
    _need(html, "opened; close fill pending")
    _need(html, "SRFM 1313 @ 0.9439")
    _need(html, "SHMD 267 @ 4.68")
    _need(html, "FEAM 454 @ 2.75")
    _need(html, "GLND 377 @ 5.138 hold-expired")
    _need(html, "SECZ 58 @ 16.21 hold-expired")
    _need(html, "TJGC 35 @ 26.50 hold-expired")
    _need(html, "FEAM 454 from 2026-09-28 hold 1 · P&L pending")
    _need(html, "USDE 520 from 2026-09-25 hold 2 · P&L pending")
    _need(html, ">pending<")
    _need(html, NOTE)
    if "plan not sealed before open" in html:
        raise SystemExit("open day rendered as missing")
    sealed = day_block_html([_plan()], SESSION)
    _need(sealed, "plan sealed")
    if "opened; close fill pending" in sealed:
        raise SystemExit("sealed plan rendered as opened")
    missing = day_block_html([{
        "committed_at": "2026-09-29T13:30:01Z",
        "date": "2026-09-29",
        "kind": "missing",
        "reason": "missing: plan not sealed before open",
    }], "2026-09-29")
    _need(missing, ">missing<")
    _need(missing, "missing: plan not sealed before open")
    closed = {
        "buys": opened["buys"],
        "date": SESSION,
        "kind": "fill",
        "sells": opened["sells"],
    }
    trade = {
        "date": SESSION,
        "entry_date": "2026-09-22",
        "entry_px": 2.94,
        "fill": 5.138,
        "kind": "close",
        "pnl_primary": 12.5,
        "reason": "hold-expired",
        "shares": 377,
        "ticker": "GLND",
    }
    done = day_block_html([_plan(), opened, closed, trade], SESSION)
    if "opened; close fill pending" in done:
        raise SystemExit("close fill still pending")
    _need(done, "GLND")
    _need(done, "$12.50")
    canonical = (ROOT / "research/hot_n4_clean_v4/forward/day_notes.json").read_text(encoding="utf-8")
    if NOTE not in canonical:
        raise SystemExit("notes file missing the 2026-09-28 line")
    for path in (
        ROOT / "dashboard/day_notes.json",
        ROOT / "dashboard/h1/notes.json",
        ROOT / "dashboard/holdup/notes.json",
    ):
        if path.read_text(encoding="utf-8") != canonical:
            raise SystemExit(f"notes drifted {path.relative_to(ROOT)}")
    for name in ("holdup", "h1"):
        text = (ROOT / "dashboard" / name / "index.html").read_text(encoding="utf-8")
        if "opened; close fill pending" not in text or "plan sealed" not in text:
            raise SystemExit(f"{name} page does not derive day status from the log")
        if "is missing. missing: plan not sealed before open" in text:
            raise SystemExit(f"{name} page still hardcodes the 09-28 missing sentence")
        if NOTE in text:
            raise SystemExit(f"{name} page hardcodes the note in the template")
        if "notes.json" not in text:
            raise SystemExit(f"{name} page does not load day notes")
    home = (ROOT / "dashboard/index.html").read_text(encoding="utf-8")
    if "plan not sealed before open" in home:
        raise SystemExit("home page still hardcodes the 09-28 missing sentence")
    if "opened; close fill pending" not in home or "day_notes.json" not in home:
        raise SystemExit("home page does not derive the open status from notes")


def _pages() -> None:
    for name in ("holdup", "h1"):
        text = (ROOT / "dashboard" / name / "index.html").read_text(encoding="utf-8")
        for phrase in (
            "Today's plan",
            "no plan sealed yet for ",
            "plan runs 09:05 ET",
            "fills at 09:35 ET open",
            'morning_status === "ABSENT"',
            "morning ABSENT",
            "holdup 2-day hold",
            "id=\"todayPlan\"",
        ):
            if phrase not in text:
                raise SystemExit(f"{name} page missing {phrase}")


def _sealed_unchanged(before: dict[Path, bytes]) -> None:
    for path, raw in before.items():
        if path.read_bytes() != raw:
            raise SystemExit(f"sealed file changed {path.relative_to(ROOT)}")


def main() -> None:
    before = {path: path.read_bytes() for path in SEALED}
    try:
        _unfilled()
        _filled()
        _partial()
        _empty()
        _absent()
        _opened_pending()
        _pages()
    finally:
        _sealed_unchanged(before)
    print("today plan render ok")


if __name__ == "__main__":
    main()
