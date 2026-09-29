"""Snapshot Finviz input, the pre-open seal clock, and a gap-day fill.

2026-09-28 reads theme-radar data/snapshots/2026-09-25.csv. Later sessions
read data/snapshots/<previous session>.csv at the last commit before 09:30 ET.
A missing file or a late commit seals no Finviz names and fills no new buy,
the same as 2026-08-28. Immediately before the plan line is appended, a clock
at or after that session's 09:30 ET writes no picks and records
"missing: plan not sealed before open".
"""
from __future__ import annotations

import hashlib
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.hot_n4_clean_v4.forward.append_check import check_against  # noqa: E402
from research.hot_n4_clean_v4.forward.forward import (  # noqa: E402
    MISSING_REASON,
    SNAPSHOT_FROM,
    plan_clock,
    plan_target,
    seal_decision,
    session_open_utc,
    snapshot_for,
)
from research.hot_n4_clean_v4.forward.ledger import _new_state, _note  # noqa: E402
from research.hot_n4_clean_v4.forward.planfill import build_plan, fill_book  # noqa: E402
from research.hot_n4_clean_v4.run_study import load_fees, nyse_sessions  # noqa: E402

SESSION = "2026-09-29"
PRIOR = "2026-09-25"
SNAP = "2026-09-28"
CSV = (
    "Ticker,Industry,Market Cap,Price,Average Volume,Volume,Open\n"
    "AAA,Software,500,20,1000,1000,19\n"
)


def _git(repo: Path, *args: str, when: str | None = None) -> None:
    env = os.environ.copy()
    if when:
        env["GIT_AUTHOR_DATE"] = when
        env["GIT_COMMITTER_DATE"] = when
    subprocess.run(["git", "-C", str(repo), *args], check=True, env=env, capture_output=True)


def _repo(root: Path, when: str, name: str = "early") -> Path:
    repo = root / name
    (repo / "data" / "snapshots").mkdir(parents=True)
    raw = (repo / "data" / "snapshots" / f"{SNAP}.csv")
    raw.write_text(CSV, encoding="utf-8")
    _git(repo, "init", "-b", "main")
    _git(repo, "add", f"data/snapshots/{SNAP}.csv")
    _git(repo, "-c", "user.email=t@example.com", "-c", "user.name=t", "commit", "-m", "snap", when=when)
    return repo


def _index() -> dict[str, int]:
    calendar = nyse_sessions("2026-01-01", "2026-12-31")
    return {day: i for i, day in enumerate(calendar)}


def _clock() -> None:
    if SNAPSHOT_FROM != "2026-09-28":
        raise SystemExit(f"snapshot start {SNAPSHOT_FROM}")
    edt = session_open_utc("2026-09-29")
    est = session_open_utc("2026-11-02")
    if edt != datetime(2026, 9, 29, 13, 30, tzinfo=timezone.utc):
        raise SystemExit(f"EDT open {edt}")
    if est != datetime(2026, 11, 2, 14, 30, tzinfo=timezone.utc):
        raise SystemExit(f"EST open {est}")
    if plan_clock("2026-09-29", datetime(2026, 9, 29, 13, 29, tzinfo=timezone.utc)):
        raise SystemExit("13:29 UTC on 2026-09-29 is still before 09:30 ET")
    late = plan_clock("2026-09-29", datetime(2026, 9, 29, 13, 30, tzinfo=timezone.utc))
    if not late or "refusing" not in late:
        raise SystemExit(f"open instant should refuse ({late})")
    if plan_clock("2026-11-02", datetime(2026, 11, 2, 14, 29, tzinfo=timezone.utc)):
        raise SystemExit("14:29 UTC on 2026-11-02 is still before 09:30 ET")
    if not plan_clock("2026-11-02", datetime(2026, 11, 2, 14, 30, tzinfo=timezone.utc)):
        raise SystemExit("EST open instant should refuse")
    records = [{"kind": "session", "date": PRIOR}]
    if plan_target(records, "")[0] != "2026-09-28":
        raise SystemExit("next session after the seed")
    if plan_target(records, "2026-09-28")[0] != "2026-09-28":
        raise SystemExit("dispatch of the next session")
    _session, why = plan_target(records, "2026-09-30")
    if not why or "not the next session" not in why:
        raise SystemExit(f"skipped session should refuse ({why})")
    missed = [
        {"kind": "session", "date": PRIOR},
        {"kind": "missing", "date": "2026-09-28", "reason": MISSING_REASON},
    ]
    if plan_target(missed, "")[0] != "2026-09-29":
        raise SystemExit("a missing day is not planned again")
    plan = {
        "date": "2026-09-28",
        "kind": "plan",
        "picks": [{"rank": 1, "ticker": "AAA"}],
        "planned_sells": [{"reason": "list-drop", "shares": 1, "ticker": "BBB"}],
        "recipe": "union_hot_n4_h1__w0",
    }
    early = seal_decision(plan, datetime(2026, 9, 28, 13, 4, tzinfo=timezone.utc))
    if early["kind"] != "plan" or early["picks"] != plan["picks"]:
        raise SystemExit(f"on-time plan dropped picks {early}")
    if early["committed_at"] != "2026-09-28T13:04:00Z":
        raise SystemExit(f"commit timestamp {early.get('committed_at')}")
    if "committed_at" in plan:
        raise SystemExit("seal stamped the uncommitted plan")
    late = seal_decision(plan, datetime(2026, 9, 28, 13, 30, tzinfo=timezone.utc))
    if late["kind"] != "missing" or late["reason"] != MISSING_REASON:
        raise SystemExit(f"open instant should record missing ({late})")
    if late.get("picks") or late.get("planned_sells"):
        raise SystemExit(f"late seal kept picks {late}")
    if late["committed_at"] != "2026-09-28T13:30:00Z":
        raise SystemExit(f"missing timestamp {late.get('committed_at')}")
    stale = seal_decision(plan, datetime(2026, 9, 29, 13, 5, tzinfo=timezone.utc))
    if stale["kind"] != "missing" or stale.get("picks"):
        raise SystemExit("a later calendar day is still late for 2026-09-28")
    est_plan = dict(plan, date="2026-11-02")
    if seal_decision(est_plan, datetime(2026, 11, 2, 14, 29, tzinfo=timezone.utc))["kind"] != "plan":
        raise SystemExit("14:29 UTC on 2026-11-02 is still before 09:30 ET")
    est_late = seal_decision(est_plan, datetime(2026, 11, 2, 14, 30, tzinfo=timezone.utc))
    if est_late["kind"] != "missing" or est_late.get("picks"):
        raise SystemExit("EST open instant should record missing")
    state = _new_state()
    _note(state, {"kind": "session", "date": PRIOR, "buys": [], "sells": []})
    _note(state, {
        "committed_at": "2026-09-28T13:40:00Z",
        "date": "2026-09-28",
        "kind": "missing",
        "reason": MISSING_REASON,
        "recipe": "union_hot_n4_h1__w0",
    })
    bad = _new_state()
    _note(bad, {"kind": "session", "date": PRIOR, "buys": [], "sells": []})
    try:
        _note(bad, {
            "committed_at": "2026-09-28T13:40:00Z",
            "date": "2026-09-28",
            "kind": "missing",
            "picks": [{"ticker": "AAA"}],
            "reason": MISSING_REASON,
        })
    except RuntimeError as exc:
        if "picks" not in str(exc):
            raise SystemExit(f"missing picks error {exc}")
    else:
        raise SystemExit("missing record with picks was accepted")


def _snapshot_0928(tmp: Path) -> None:
    """Session 2026-09-28 reads data/snapshots/2026-09-25.csv committed before the open."""
    repo = tmp / "sep28"
    (repo / "data" / "snapshots").mkdir(parents=True)
    raw = repo / "data" / "snapshots" / "2026-09-25.csv"
    raw.write_text(CSV, encoding="utf-8")
    _git(repo, "init", "-b", "main")
    _git(repo, "add", "data/snapshots/2026-09-25.csv")
    _git(repo, "-c", "user.email=t@example.com", "-c", "user.name=t", "commit", "-m", "snap", when="2026-09-28T12:00:00Z")
    os.environ["THEME_RADAR_DIR"] = str(repo)
    frame, prov = snapshot_for("2026-09-28")
    if frame is None or prov.get("status") != "BEFORE_OPEN":
        raise SystemExit(f"09-28 snapshot {prov}")
    if prov["path"] != "data/snapshots/2026-09-25.csv":
        raise SystemExit(prov["path"])
    if str(frame["snapshot_date"].iloc[0]) != "2026-09-25" or str(frame["trade_date"].iloc[0]) != "2026-09-28":
        raise SystemExit("09-28 snapshot dates")
    if not str(prov["commit_time"]).startswith("2026-09-28T12:00:00"):
        raise SystemExit(f"09-28 commit time {prov['commit_time']}")
    late = tmp / "sep28-late"
    (late / "data" / "snapshots").mkdir(parents=True)
    (late / "data" / "snapshots" / "2026-09-25.csv").write_text(CSV, encoding="utf-8")
    _git(late, "init", "-b", "main")
    _git(late, "add", "data/snapshots/2026-09-25.csv")
    _git(late, "-c", "user.email=t@example.com", "-c", "user.name=t", "commit", "-m", "late", when="2026-09-28T13:31:00Z")
    os.environ["THEME_RADAR_DIR"] = str(late)
    frame, prov = snapshot_for("2026-09-28")
    if frame is not None or prov.get("status") != "MISSING":
        raise SystemExit(f"09-28 late snapshot was used ({prov})")


def _snapshot(tmp: Path) -> None:
    early = _repo(tmp, "2026-09-29T12:00:00Z", "early")
    os.environ["THEME_RADAR_DIR"] = str(early)
    frame, prov = snapshot_for(SESSION)
    if frame is None or prov.get("status") != "BEFORE_OPEN":
        raise SystemExit(f"early snapshot {prov}")
    if list(frame["Ticker"]) != ["AAA"]:
        raise SystemExit(f"tickers {list(frame['Ticker'])}")
    if "Earnings Date" in frame.columns:
        raise SystemExit("earnings column was invented")
    if str(frame["trade_date"].iloc[0]) != SESSION or str(frame["snapshot_date"].iloc[0]) != SNAP:
        raise SystemExit("snapshot dates")
    raw = (early / "data" / "snapshots" / f"{SNAP}.csv").read_bytes()
    if prov["file_sha256"] != hashlib.sha256(raw).hexdigest():
        raise SystemExit("file sha256")
    head = subprocess.run(["git", "-C", str(early), "rev-parse", "HEAD"], check=True, capture_output=True, text=True)
    if prov["commit"] != head.stdout.strip():
        raise SystemExit("commit sha")
    if not str(prov["commit_time"]).startswith("2026-09-29T12:00:00"):
        raise SystemExit(f"commit time {prov['commit_time']}")
    if prov["path"] != f"data/snapshots/{SNAP}.csv":
        raise SystemExit(prov["path"])

    late = _repo(tmp, "2026-09-29T14:00:00Z", "late")
    os.environ["THEME_RADAR_DIR"] = str(late)
    frame, prov = snapshot_for(SESSION)
    if frame is not None or prov.get("status") != "MISSING":
        raise SystemExit(f"late commit should not be used ({prov})")
    if prov.get("commit") or prov.get("file_sha256"):
        raise SystemExit("late commit leaked into the plan source")

    os.environ["THEME_RADAR_DIR"] = str(tmp / "missing")
    (tmp / "missing" / ".git").mkdir(parents=True)
    frame, prov = snapshot_for(SESSION)
    if frame is not None or prov.get("status") != "MISSING":
        raise SystemExit(f"missing file {prov}")


def _gap_fill() -> None:
    """No Finviz names: no new buy. A held lot still sells on list-drop."""
    index = _index()
    state = {
        "cash": 8000.0,
        "pos": {
            "BBB": {
                "cost_primary": 100.0,
                "entry_date": PRIOR,
                "entry_px": 10.0,
                "last_px": 10.0,
                "min_hold": 1,
                "peak_px": 10.0,
                "shares": 10,
                "ticker": "BBB",
            },
        },
    }
    payload = {
        "bar_cutoff": PRIOR,
        "candidates": [],
        "excluded_unexplained_legs": [],
        "morning_s": 1.0,
        "session": SESSION,
    }
    plan = build_plan(payload, state, index)
    plan["sha256"] = "gap"
    if plan["picks"]:
        raise SystemExit(f"gap plan picked {plan['picks']}")
    if [row["ticker"] for row in plan["planned_sells"]] != ["BBB"]:
        raise SystemExit(f"held name should still be planned {plan['planned_sells']}")
    bars = {
        "stored": {
            "BBB": {
                "adjusted": True,
                "close": [10.0, 12.5],
                "date": [PRIOR, SESSION],
                "open": [10.0, 12.0],
            },
            "IWM": {
                "adjusted": True,
                "close": [10.0, 10.2],
                "date": [PRIOR, SESSION],
                "open": [10.0, 10.0],
            },
        },
    }
    fill, _closes, _state = fill_book(plan, state, bars, load_fees(), index)
    if fill["buys"]:
        raise SystemExit(f"gap fill bought {fill['buys']}")
    if [row["ticker"] for row in fill["sells"]] != ["BBB"]:
        raise SystemExit(f"gap fill sells {fill['sells']}")


def main() -> None:
    from research.hot_n4_clean_v4.forward.test_plan_window import main as plan_window
    plan_window()
    _clock()
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _snapshot_0928(root)
        _snapshot(root)
    _gap_fill()
    from research.hot_n4_clean_v4.forward.test_morning_absent import main as morning_absent
    morning_absent()
    check_against("origin/main")
    print("snapshot plan ok")


if __name__ == "__main__":
    main()
