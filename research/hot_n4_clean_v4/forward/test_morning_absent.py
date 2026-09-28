"""A missing morning file still plans both books.

PREREG section 5: when the morning file is absent, holdup does not apply
and min_hold stays 1. h1 does not use S. The session is not skipped.
"""
from __future__ import annotations

import inspect
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.hot_n4_clean_v4.forward import forward  # noqa: E402
from research.hot_n4_clean_v4.forward.book import H1, HOLDUP, reset_book, use_book  # noqa: E402
from research.hot_n4_clean_v4.forward.planfill import build_plan, fill_book  # noqa: E402
from research.hot_n4_clean_v4.run_study import load_fees, nyse_sessions  # noqa: E402

SESSION = "2026-09-29"
PRIOR = "2026-09-25"
ENTRY = "2026-08-13"
ABSENT_REASON = "no predict or weather file for 2026-09-29 was on HEAD before 13:30 UTC"
UNREADABLE = "01_daily/weather/2026-09-29_weather.json was on GitHub before 13:30 UTC but is not readable"
DECISION = (
    "bar_cutoff", "cash_before", "date", "excluded_unexplained_legs",
    "holdup_on", "kind", "picks", "planned_sells", "recipe",
)
SEALED = (
    ROOT / "research/hot_n4_clean_v4/forward/LEDGER.jsonl",
    ROOT / "research/hot_n4_clean_v4/forward/PRICE_LEDGER.jsonl",
    ROOT / "research/hot_n4_clean_v4/forward/holdup_log.jsonl",
    ROOT / "research/hot_n4_clean_v4/forward/skips.jsonl",
    ROOT / "research/hot_n4_clean_v4/forward_h1/LEDGER.jsonl",
    ROOT / "research/hot_n4_clean_v4/forward_h1/PRICE_LEDGER.jsonl",
    ROOT / "research/hot_n4_clean_v4/forward_h1/h1_log.jsonl",
    ROOT / "research/hot_n4_clean_v4/forward_h1/skips.jsonl",
)


def _index() -> dict[str, int]:
    calendar = nyse_sessions("2026-01-01", "2026-12-31")
    return {day: i for i, day in enumerate(calendar)}


def _candidates() -> list[dict]:
    return [
        {
            "fv_avg_volume": 1_000_000.0,
            "fv_price": 20.0,
            "ohlc_hot_score": 5.0,
            "sources": ["yday_gainer"],
            "ticker": "AAA",
        },
        {
            "fv_avg_volume": 500_000.0,
            "fv_price": 10.0,
            "ohlc_hot_score": 4.0,
            "sources": ["ohlc_hot"],
            "ticker": "CCC",
        },
    ]


def _state() -> dict:
    return {
        "cash": 10000.0,
        "pos": {
            "BBB": {
                "cost_primary": 100.0,
                "entry_date": ENTRY,
                "entry_px": 10.0,
                "last_px": 10.0,
                "min_hold": 1,
                "peak_px": 10.0,
                "shares": 10,
                "ticker": "BBB",
            },
        },
    }


def _bars() -> dict:
    def blob(op: float) -> dict:
        return {
            "adjusted": True,
            "close": [10.0, 10.0, op],
            "date": [ENTRY, PRIOR, SESSION],
            "open": [10.0, 10.0, op],
        }
    return {
        "stored": {
            "AAA": blob(11.0),
            "BBB": blob(12.0),
            "CCC": blob(9.0),
            "IWM": blob(10.0),
        },
    }


class _Blobs:
    def __init__(self, blob):
        self.blob = blob
        self.saved = None

    def __enter__(self):
        self.saved = forward.blob_before_open
        forward.blob_before_open = self.blob
        return self

    def __exit__(self, *exc):
        forward.blob_before_open = self.saved


def _plan(book, morning: float | None) -> dict:
    token = use_book(book)
    try:
        payload = {
            "bar_cutoff": PRIOR,
            "candidates": _candidates(),
            "excluded_unexplained_legs": [],
            "morning_s": morning,
            "session": SESSION,
        }
        plan = build_plan(payload, _state(), _index())
        if plan["recipe"] != book.recipe:
            raise SystemExit(f"recipe {plan['recipe']} {book.recipe}")
        if not plan["picks"]:
            raise SystemExit(f"{book.recipe} produced no plan")
        return plan
    finally:
        reset_book(token)


def _decision(plan: dict) -> dict:
    return {key: plan[key] for key in DECISION}


def _fill(book, plan: dict) -> dict:
    token = use_book(book)
    try:
        body = dict(plan)
        body["sha256"] = "test"
        fill, _closes, _after = fill_book(body, _state(), _bars(), load_fees(), _index())
        return fill
    finally:
        reset_book(token)


def _holds(fill: dict) -> dict[str, int]:
    return {row["ticker"]: int(row["min_hold"]) for row in fill["holdings"] if row["ticker"] != "BBB"}


def _shares(fill: dict) -> dict[str, int]:
    return {row["ticker"]: int(row["shares"]) for row in fill["buys"]}


def _gate() -> None:
    def absent(repo, rel, session, ref=None):
        return None

    with _Blobs(absent):
        score = forward.morning_gate(SESSION)
    if score["morning_s"] is not None or score["status"] != "ABSENT":
        raise SystemExit(f"absent gate {score}")
    if score["reason"] != ABSENT_REASON:
        raise SystemExit(f"absent reason {score['reason']}")

    def unreadable(repo, rel, session, ref=None):
        if rel.endswith("_predict.md"):
            return None
        if rel.endswith("_weather.json"):
            return ("abc", "2026-09-29T12:00:00+00:00", b"{")
        return None

    with _Blobs(unreadable):
        bad = forward.morning_gate(SESSION)
    if bad["morning_s"] is not None or bad["status"] != "ABSENT":
        raise SystemExit(f"unreadable gate {bad}")
    if bad["reason"] != UNREADABLE:
        raise SystemExit(f"unreadable reason {bad['reason']}")

    def blank(repo, rel, session, ref=None):
        if rel.endswith("_predict.md"):
            return ("abc", "2026-09-29T12:00:00+00:00", b"no numeric score\n")
        return None

    with _Blobs(blank):
        empty = forward.morning_gate(SESSION)
    if empty.get("status") != "BEFORE_1330" or empty.get("morning_s") is not None:
        raise SystemExit(f"readable empty score {empty}")
    if empty.get("morning_status") == "ABSENT" or empty.get("status") == "ABSENT":
        raise SystemExit("readable file with no score was marked absent")

    def present(repo, rel, session, ref=None):
        if rel.endswith("_predict.md"):
            return ("abc", "2026-09-29T12:00:00+00:00", b"Prediction: UP session total score 1.5\n")
        return None

    with _Blobs(present):
        scored = forward.morning_gate(SESSION)
    if scored.get("morning_s") != 1.5 or scored.get("status") != "BEFORE_1330":
        raise SystemExit(f"present score {scored}")


def _plans() -> None:
    src = inspect.getsource(forward.plan_main)
    if "morning_gate(" not in src or "morning_score(" in src:
        raise SystemExit("plan_main does not plan through morning_gate")
    if "outside the v4 session calendar" not in src:
        raise SystemExit("calendar skip changed")
    for name in ("h1_forward.yml", "holdup_forward.yml"):
        text = (ROOT / ".github" / "workflows" / name).read_text(encoding="utf-8")
        needle = "python3 research/hot_n4_clean_v4/forward/forward.py"
        if text.count(needle) != 1:
            raise SystemExit(f"{name} does not use the shared forward.py path")
    for page in (ROOT / "dashboard/holdup/index.html", ROOT / "dashboard/h1/index.html"):
        html = page.read_text(encoding="utf-8")
        if 'morning_status === "ABSENT"' not in html or "morning ABSENT" not in html:
            raise SystemExit(f"{page.name} does not show an absent morning file")

    absent_h1 = _plan(H1, None)
    present_h1 = _plan(H1, 1.5)
    forward.stamp_morning(absent_h1, {
        "morning_s": None,
        "reason": ABSENT_REASON,
        "status": "ABSENT",
    })
    forward.stamp_morning(present_h1, {
        "blob_sha": "present",
        "kind": "predict",
        "morning_s": 1.5,
        "path": "01_daily/general/2026-09-29_predict.md",
        "server_time_utc": "2026-09-29T12:00:00Z",
        "status": "BEFORE_1330",
    })
    if absent_h1.get("morning_status") != "ABSENT" or absent_h1.get("morning_reason") != ABSENT_REASON:
        raise SystemExit(f"h1 absence not on the plan {absent_h1.get('morning_status')}")
    if absent_h1["s_source"]["status"] != "ABSENT" or absent_h1["s_source"]["reason"] != ABSENT_REASON:
        raise SystemExit(f"h1 s_source {absent_h1['s_source']}")
    if present_h1.get("morning_status") or present_h1["morning_s"] != 1.5:
        raise SystemExit("present h1 plan was marked absent")
    if _decision(absent_h1) != _decision(present_h1):
        raise SystemExit(
            f"h1 plan changed when S was absent {_decision(absent_h1)} {_decision(present_h1)}"
        )
    if absent_h1["holdup_on"] or present_h1["holdup_on"]:
        raise SystemExit("h1 turned holdup on")

    absent_hold = _plan(HOLDUP, None)
    present_hold = _plan(HOLDUP, 1.5)
    forward.stamp_morning(absent_hold, {
        "morning_s": None,
        "reason": ABSENT_REASON,
        "status": "ABSENT",
    })
    if absent_hold.get("morning_status") != "ABSENT":
        raise SystemExit("holdup absence not on the plan")
    if absent_hold["holdup_on"]:
        raise SystemExit("absent morning turned holdup on")
    if not present_hold["holdup_on"]:
        raise SystemExit("present S did not turn holdup on")
    for key in ("picks", "planned_sells", "cash_before", "excluded_unexplained_legs"):
        if absent_hold[key] != present_hold[key] or absent_h1[key] != present_h1[key]:
            raise SystemExit(f"S changed {key}")
    if [row["ticker"] for row in absent_hold["picks"]] != ["AAA", "CCC"]:
        raise SystemExit(f"picks {absent_hold['picks']}")
    if [row["ticker"] for row in absent_hold["planned_sells"]] != ["BBB"]:
        raise SystemExit(f"planned sells {absent_hold['planned_sells']}")

    for book, absent_plan, present_plan, want_absent, want_present in (
        (H1, absent_h1, present_h1, 1, 1),
        (HOLDUP, absent_hold, present_hold, 1, 2),
    ):
        absent_fill = _fill(book, absent_plan)
        present_fill = _fill(book, present_plan)
        if _shares(absent_fill) != _shares(present_fill):
            raise SystemExit(
                f"S changed share size on {book.recipe}: "
                f"{_shares(absent_fill)} {_shares(present_fill)}"
            )
        if not _shares(absent_fill):
            raise SystemExit(f"{book.recipe} filled no buy")
        holds = _holds(absent_fill)
        if set(holds) != {"AAA", "CCC"} or any(value != want_absent for value in holds.values()):
            raise SystemExit(f"{book.recipe} absent min_hold {holds} wanted {want_absent}")
        present_holds = _holds(present_fill)
        if any(value != want_present for value in present_holds.values()):
            raise SystemExit(f"{book.recipe} present min_hold {present_holds} wanted {want_present}")


def _sealed_unchanged(before: dict[Path, bytes]) -> None:
    for path, raw in before.items():
        if path.read_bytes() != raw:
            raise SystemExit(f"sealed file changed {path.relative_to(ROOT)}")


def main() -> None:
    before = {path: path.read_bytes() for path in SEALED}
    try:
        _gate()
        _plans()
    finally:
        _sealed_unchanged(before)
    print("morning absent still plans both books; holdup min_hold 1; h1 plan unchanged by S")


if __name__ == "__main__":
    main()
