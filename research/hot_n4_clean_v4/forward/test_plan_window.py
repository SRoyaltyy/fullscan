"""A plan seals only on the target session, from 08:00 ET until 09:30 ET.

After D's close fill, the next target is D+1. A run on D evening, or at
D+1 07:59 ET, writes nothing. D+1 08:00 through 09:29 ET seals. At or after
09:30 ET the missing record is unchanged. h1 uses this same plan_main.
"""
from __future__ import annotations

import inspect
import io
import os
import sys
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.hot_n4_clean_v4.forward import forward  # noqa: E402
from research.hot_n4_clean_v4.forward.book import H1, HOLDUP, reset_book, use_book  # noqa: E402
from research.hot_n4_clean_v4.forward.forward import MISSING_REASON  # noqa: E402
from research.hot_n4_clean_v4.protocol import SESSIONS  # noqa: E402

ET = ZoneInfo("America/New_York")
D = "2026-09-28"
TARGET = "2026-09-29"
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
    ROOT / "dashboard/holdup/status.json",
    ROOT / "dashboard/h1/log.json",
    ROOT / "dashboard/h1/status.json",
)


def _at(day: str, hour: int, minute: int) -> datetime:
    year, month, dom = (int(part) for part in day.split("-"))
    return datetime(year, month, dom, hour, minute, tzinfo=ET)


def _line(day: str, hour: int, minute: int) -> str:
    stamp = _at(day, hour, minute).strftime("%Y-%m-%d %H:%M ET")
    return (
        f"too early to plan {TARGET}: now {stamp}; "
        f"plan window is {TARGET} 08:00-09:30 ET"
    )


def _records() -> list[dict]:
    """Seeded sessions plus D's plan and close fill, so the next target is D+1."""
    rows = []
    for day in SESSIONS:
        rows.append({
            "buys": [],
            "cash_primary": 10000.0,
            "date": day,
            "holdings": [],
            "kind": "session",
            "sells": [],
        })
    rows.append({
        "date": D,
        "kind": "plan",
        "picks": [],
        "planned_sells": [],
        "recipe": HOLDUP.recipe,
    })
    rows.append({
        "buys": [],
        "cash_primary": 10000.0,
        "date": D,
        "holdings": [],
        "kind": "fill",
        "sells": [],
    })
    return rows


class _Clock:
    def __init__(self, moment: datetime):
        self.moment = moment
        self.n = 0

    def __call__(self) -> datetime:
        self.n += 1
        return self.moment


class _Steps:
    def __init__(self, moments: list[datetime]):
        self.moments = moments
        self.n = 0

    def __call__(self) -> datetime:
        if self.n >= len(self.moments):
            raise SystemExit(f"clock read {self.n + 1} with only {len(self.moments)} instants")
        moment = self.moments[self.n]
        self.n += 1
        return moment


def _run(book, clock, allow_early: bool = False) -> tuple[int, str, dict]:
    records = _records()
    calls = {"append": [], "bars": 0, "morning": 0, "page": 0, "skip": 0, "snapshot": 0}

    def load(folder=None):
        return records

    def append(bodies, folder=None):
        calls["append"].append(list(bodies))
        return bodies

    def bars(rows, target):
        calls["bars"] += 1
        return {"feat": {}, "stored": {}}, 0

    def morning(session):
        calls["morning"] += 1
        return {
            "blob_sha": "abc",
            "kind": "predict",
            "morning_s": None,
            "path": f"01_daily/general/{session}_predict.md",
            "server_time_utc": f"{session}T12:00:00Z",
            "status": "BEFORE_1330",
        }

    def snapshot(session):
        calls["snapshot"] += 1
        return None, {"path": "data/snapshots/2026-09-28.csv", "status": "MISSING"}

    def page(*args, **kwargs):
        calls["page"] += 1

    def skip(*args, **kwargs):
        calls["skip"] += 1

    def engine_ok():
        # A pull_request checkout is the merge with main. main can edit
        # src/factor_mine.py without moving the v4 pin. This test is the
        # clock, so it does not inherit that refusal.
        return None

    saved = {
        "append_records": forward.append_records,
        "load": forward.load,
        "utc_now": forward.utc_now,
        "write_page": forward.write_page,
        "_bars": forward._bars,
        "_engine_ok": forward._engine_ok,
        "_halt_known": forward._halt_known,
        "_log_skip": forward._log_skip,
        "morning_gate": forward.morning_gate,
        "snapshot_for": forward.snapshot_for,
    }
    old_early = os.environ.get("PLAN_ALLOW_EARLY")
    old_session = os.environ.get("PLAN_SESSION")
    token = use_book(book)
    out = io.StringIO()
    err = io.StringIO()
    try:
        os.environ.pop("PLAN_SESSION", None)
        if allow_early:
            os.environ["PLAN_ALLOW_EARLY"] = "1"
        else:
            os.environ.pop("PLAN_ALLOW_EARLY", None)
        forward.load = load
        forward.append_records = append
        forward.write_page = page
        forward._bars = bars
        forward._engine_ok = engine_ok
        forward._log_skip = skip
        forward._halt_known = lambda *args, **kwargs: None
        forward.morning_gate = morning
        forward.snapshot_for = snapshot
        forward.utc_now = clock
        with redirect_stdout(out), redirect_stderr(err):
            code = forward.plan_main()
    finally:
        reset_book(token)
        for name, value in saved.items():
            setattr(forward, name, value)
        if old_early is None:
            os.environ.pop("PLAN_ALLOW_EARLY", None)
        else:
            os.environ["PLAN_ALLOW_EARLY"] = old_early
        if old_session is None:
            os.environ.pop("PLAN_SESSION", None)
        else:
            os.environ["PLAN_SESSION"] = old_session
    text = out.getvalue()
    if err.getvalue():
        raise SystemExit(f"stderr {err.getvalue()!r}")
    return code, text, calls


def _no_write(book, moment: datetime, line: str) -> None:
    clock = _Clock(moment)
    code, text, calls = _run(book, clock)
    if code != 0:
        raise SystemExit(f"{book.recipe} early exit {code}")
    if text.strip() != line:
        raise SystemExit(f"{book.recipe} stdout {text!r}")
    if calls["append"] or calls["page"] or calls["skip"]:
        raise SystemExit(f"{book.recipe} wrote {calls}")
    if calls["bars"] or calls["morning"] or calls["snapshot"]:
        raise SystemExit(f"{book.recipe} built inputs {calls}")
    if clock.n != 1:
        raise SystemExit(f"{book.recipe} early clock reads {clock.n}")
    if "missing" in text or "sealed plan" in text:
        raise SystemExit(f"{book.recipe} sealed while early {text!r}")


def _seals(book, moment: datetime, stamp: str) -> None:
    clock = _Clock(moment)
    code, text, calls = _run(book, clock)
    if code != 0:
        raise SystemExit(f"{book.recipe} seal exit {code}")
    if len(calls["append"]) != 1 or len(calls["append"][0]) != 1:
        raise SystemExit(f"{book.recipe} append {calls['append']}")
    body = calls["append"][0][0]
    if body["kind"] != "plan" or body["date"] != TARGET or body["recipe"] != book.recipe:
        raise SystemExit(f"{book.recipe} sealed body {body}")
    if body["committed_at"] != stamp:
        raise SystemExit(f"{book.recipe} committed_at {body.get('committed_at')}")
    if body.get("holdup_on"):
        raise SystemExit(f"{book.recipe} changed holdup {body.get('holdup_on')}")
    if "sealed plan " + TARGET not in text:
        raise SystemExit(f"{book.recipe} seal text {text!r}")
    if calls["bars"] != 1 or calls["morning"] != 1 or calls["snapshot"] != 1:
        raise SystemExit(f"{book.recipe} inputs {calls}")
    if calls["page"] != 1 or calls["skip"]:
        raise SystemExit(f"{book.recipe} page {calls}")
    if clock.n != 3:
        raise SystemExit(f"{book.recipe} clock reads {clock.n}")


def _missing(book, moment: datetime, stamp: str) -> None:
    clock = _Clock(moment)
    code, text, calls = _run(book, clock)
    if code != 0:
        raise SystemExit(f"{book.recipe} missing exit {code}")
    if len(calls["append"]) != 1:
        raise SystemExit(f"{book.recipe} missing append {calls['append']}")
    body = calls["append"][0][0]
    if body["kind"] != "missing" or body["reason"] != MISSING_REASON or body["date"] != TARGET:
        raise SystemExit(f"{book.recipe} missing body {body}")
    if body.get("picks") or body.get("planned_sells"):
        raise SystemExit(f"{book.recipe} missing kept picks {body}")
    if body["committed_at"] != stamp:
        raise SystemExit(f"{book.recipe} missing stamp {body.get('committed_at')}")
    if MISSING_REASON not in text or "sealed plan" in text:
        raise SystemExit(f"{book.recipe} missing text {text!r}")
    if calls["page"] != 1 or calls["skip"]:
        raise SystemExit(f"{book.recipe} missing page {calls}")
    if clock.n != 3:
        raise SystemExit(f"{book.recipe} missing clock reads {clock.n}")


def _bounds() -> None:
    evening = _line(D, 16, 0)
    before = _line(TARGET, 7, 59)
    for book in (HOLDUP, H1):
        _no_write(book, _at(D, 16, 0), evening)
        _no_write(book, _at(TARGET, 7, 59), before)
        _seals(book, _at(TARGET, 8, 0), "2026-09-29T12:00:00Z")
        _seals(book, _at(TARGET, 9, 29), "2026-09-29T13:29:00Z")
        _missing(book, _at(TARGET, 9, 30), "2026-09-29T13:30:00Z")


def _second_check() -> None:
    """Inputs may be built, and the append still refuses when the re-read is early."""
    early = _at(TARGET, 7, 59)
    for book in (HOLDUP, H1):
        clock = _Steps([_at(TARGET, 8, 0), early, early])
        code, text, calls = _run(book, clock)
        if code != 0 or calls["append"] or calls["page"] or calls["skip"]:
            raise SystemExit(f"{book.recipe} second check wrote {code} {calls}")
        if calls["bars"] != 1 or calls["morning"] != 1 or calls["snapshot"] != 1:
            raise SystemExit(f"{book.recipe} second check skipped inputs {calls}")
        if _line(TARGET, 7, 59) not in text or "sealed plan" in text:
            raise SystemExit(f"{book.recipe} second check text {text!r}")
        if clock.n != 3:
            raise SystemExit(f"{book.recipe} second check reads {clock.n}")

        late = _Steps([_at(TARGET, 8, 0), _at(TARGET, 9, 30), _at(TARGET, 9, 30)])
        code, text, calls = _run(book, late)
        if code != 0 or len(calls["append"]) != 1:
            raise SystemExit(f"{book.recipe} crossed open {code} {calls['append']}")
        body = calls["append"][0][0]
        if body["kind"] != "missing" or body.get("picks"):
            raise SystemExit(f"{book.recipe} crossed open body {body}")
        if late.n != 3:
            raise SystemExit(f"{book.recipe} crossed open reads {late.n}")


def _override() -> None:
    """PLAN_ALLOW_EARLY=1 skips the lower bound and leaves the 09:30 guard."""
    code, text, calls = _run(HOLDUP, _Clock(_at(TARGET, 7, 59)), allow_early=True)
    if code != 0 or len(calls["append"]) != 1:
        raise SystemExit(f"override did not seal {code} {calls['append']}")
    body = calls["append"][0][0]
    if body["kind"] != "plan" or body["committed_at"] != "2026-09-29T11:59:00Z":
        raise SystemExit(f"override body {body}")
    if "too early to plan" in text:
        raise SystemExit(f"override printed a refusal {text!r}")

    code, text, calls = _run(H1, _Clock(_at(TARGET, 9, 30)), allow_early=True)
    if code != 0 or len(calls["append"]) != 1:
        raise SystemExit(f"override passed the open {code} {calls['append']}")
    body = calls["append"][0][0]
    if body["kind"] != "missing" or body["recipe"] != H1.recipe:
        raise SystemExit(f"override missing {body}")

    os.environ["PLAN_ALLOW_EARLY"] = "true"
    try:
        if forward.plan_too_early(TARGET, _at(TARGET, 7, 59)) is None:
            raise SystemExit("only PLAN_ALLOW_EARLY=1 skips the lower bound")
    finally:
        os.environ.pop("PLAN_ALLOW_EARLY", None)
    if forward.plan_too_early(TARGET, _at(TARGET, 7, 59)) is None:
        raise SystemExit("lower bound stayed off")

    root = ROOT / ".github" / "workflows"
    for path in sorted(root.glob("*.yml")):
        if "PLAN_ALLOW_EARLY" in path.read_text(encoding="utf-8"):
            raise SystemExit(f"{path.name} sets PLAN_ALLOW_EARLY")


def _direct() -> None:
    if forward.plan_too_early("2026-11-02", _at("2026-11-02", 7, 59)) is None:
        raise SystemExit("EST 07:59 should be early")
    if forward.plan_too_early("2026-11-02", _at("2026-11-02", 8, 0)):
        raise SystemExit("EST 08:00 should be inside the window")
    if forward.plan_too_early("2026-11-02", _at("2026-11-02", 9, 30)):
        raise SystemExit("09:30 is the upper bound, not the lower bound")
    early = forward.seal_decision(
        {"date": TARGET, "kind": "plan", "picks": [{"ticker": "AAA"}], "recipe": H1.recipe},
        _at(TARGET, 7, 59),
    )
    if early["kind"] != "plan" or not early.get("picks"):
        raise SystemExit("seal_decision lower bound changed")
    src = inspect.getsource(forward.plan_main)
    first = src.find("_early_stop(")
    second = src.find("_early_stop(", first + 1)
    if first < 0 or second < 0 or src.find("_early_stop(", second + 1) != -1:
        raise SystemExit("plan_main should check the window twice")
    bars = src.find("_bars(")
    morning = src.find("morning_gate(")
    seal = src.find("seal_decision(")
    append = src.find("append_records(")
    if not (first < bars < morning < seal < second < append):
        raise SystemExit("window checks are not before inputs and before append")
    folder = ROOT / "research/hot_n4_clean_v4/forward_h1"
    for path in folder.rglob("*.py"):
        if "def plan_main" in path.read_text(encoding="utf-8"):
            raise SystemExit(f"h1 has its own plan_main in {path}")


def _unchanged(before: dict[Path, bytes]) -> None:
    for path, raw in before.items():
        if not path.is_file():
            raise SystemExit(f"sealed file missing {path.relative_to(ROOT)}")
        current = path.read_bytes()
        if current != raw:
            path.write_bytes(raw)
            raise SystemExit(f"sealed file changed {path.relative_to(ROOT)}")


def _backfill_one(book) -> None:
    """A final session appends one late plan. The live path still writes missing."""
    final = datetime(2026, 9, 29, 21, 30, tzinfo=ZoneInfo("UTC"))
    code, text, calls = _run_mode(book, _Clock(final), "backfill_closed_plan")
    if code != 0 or len(calls["append"]) != 1:
        raise SystemExit(f"{book.recipe} backfill {code} {text!r} {calls['append']}")
    body = calls["append"][0][0]
    if body["kind"] != "plan" or not body.get("appended_late"):
        raise SystemExit(f"{book.recipe} backfill body {body}")
    if body.get("inputs_asof") != "before 09:30 ET":
        raise SystemExit(f"{book.recipe} backfill inputs {body}")
    if body["date"] != TARGET:
        raise SystemExit(f"{book.recipe} backfill date {body['date']}")
    early = datetime(2026, 9, 29, 14, 0, tzinfo=ZoneInfo("UTC"))
    code, text, calls = _run_mode(book, _Clock(early), "backfill_closed_plan")
    if code == 0 or calls["append"] or calls["bars"]:
        raise SystemExit(f"{book.recipe} backfill ran before the bar was final {code} {calls}")
    code, text, calls = _run(book, _Clock(final))
    if code != 0 or len(calls["append"]) != 1:
        raise SystemExit(f"{book.recipe} live late path {code} {text!r}")
    if calls["append"][0][0]["kind"] != "missing":
        raise SystemExit(f"{book.recipe} live late path kept picks")


def _run_mode(book, clock, fn_name: str) -> tuple[int, str, dict]:
    records = _records()
    calls = {"append": [], "bars": 0, "morning": 0, "page": 0, "skip": 0, "snapshot": 0}

    def load(folder=None):
        return records

    def append(bodies, folder=None):
        calls["append"].append(list(bodies))
        return bodies

    def bars(rows, target):
        calls["bars"] += 1
        return {"feat": {}, "stored": {}}, 0

    def morning(session):
        calls["morning"] += 1
        return {
            "blob_sha": "abc",
            "kind": "predict",
            "morning_s": None,
            "path": f"01_daily/general/{session}_predict.md",
            "server_time_utc": f"{session}T12:00:00Z",
            "status": "BEFORE_1330",
        }

    def snapshot(session):
        calls["snapshot"] += 1
        return None, {"path": "data/snapshots/2026-09-28.csv", "status": "MISSING"}

    def page(*args, **kwargs):
        calls["page"] += 1

    def skip(*args, **kwargs):
        calls["skip"] += 1

    saved = {
        "append_records": forward.append_records,
        "load": forward.load,
        "utc_now": forward.utc_now,
        "write_page": forward.write_page,
        "_bars": forward._bars,
        "_engine_ok": forward._engine_ok,
        "_halt_known": forward._halt_known,
        "_log_skip": forward._log_skip,
        "morning_gate": forward.morning_gate,
        "snapshot_for": forward.snapshot_for,
    }
    token = use_book(book)
    out = io.StringIO()
    err = io.StringIO()
    try:
        forward.load = load
        forward.append_records = append
        forward.write_page = page
        forward._bars = bars
        forward._engine_ok = lambda: None
        forward._log_skip = skip
        forward._halt_known = lambda *args, **kwargs: None
        forward.morning_gate = morning
        forward.snapshot_for = snapshot
        forward.utc_now = clock
        with redirect_stdout(out), redirect_stderr(err):
            code = getattr(forward, fn_name)()
    finally:
        reset_book(token)
        for name, value in saved.items():
            setattr(forward, name, value)
    return code, out.getvalue() + err.getvalue(), calls


def main() -> None:
    before = {path: path.read_bytes() for path in SEALED if path.is_file()}
    try:
        _direct()
        _bounds()
        _second_check()
        _override()
        _backfill_one(HOLDUP)
        _backfill_one(H1)
    finally:
        _unchanged(before)
    print("plan window 08:00-09:30 ET; early run writes nothing; both books")


if __name__ == "__main__":
    main()
