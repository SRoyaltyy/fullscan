"""Today's plan block. A sealed plan is visible before the open fill."""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.hot_n4_clean_v4.forward.render import today_plan_html  # noqa: E402

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
        _pages()
    finally:
        _sealed_unchanged(before)
    print("today plan render ok")


if __name__ == "__main__":
    main()
