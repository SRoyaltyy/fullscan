"""Seal trigger plus the ECS 09:12 ET paper backstop.

Run: PYTHONPATH=. python3 -m src.test_paper_backstop
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from src.h1_sealed_exec import H1_LOG
from src.paper_backstop import already_attempted, decide, is_late, main

ROOT = Path(__file__).resolve().parent.parent
WF = ROOT / ".github" / "workflows"
ET_0912 = datetime.fromisoformat("2026-10-06T09:12:00-04:00")
ET_0925 = datetime.fromisoformat("2026-10-06T09:25:00-04:00")
ET_092501 = datetime.fromisoformat("2026-10-06T09:25:01-04:00")
ET_0934 = datetime.fromisoformat("2026-10-06T09:34:28-04:00")


def test_late_starts_after_0925() -> None:
    assert is_late(ET_0912) is False
    assert is_late(ET_0925) is False
    assert is_late(ET_092501) is True
    assert is_late(ET_0934) is True


def test_sealed_unsent_day_sends() -> None:
    rec = decide(ET_0912, H1_LOG)
    assert rec["date"] == "2026-10-06"
    assert rec["action"] == "send"
    assert rec["late"] is False
    assert rec["sealed"] is True
    assert rec["submitted_before"] is False
    assert rec["env"] == "paper"
    assert rec["live"] is False
    assert rec["owner"] == "actions"
    assert rec["host"] == "sandbox"


def test_submit_journal_does_not_send_again() -> None:
    import tempfile
    journal = Path(tempfile.mkdtemp()) / "2026-10-06_submit.json"
    journal.write_text(json.dumps({
        "date": "2026-10-06", "status": "acknowledged", "submit": True,
    }), encoding="utf-8")
    rec = decide(ET_0912, H1_LOG, journal_path=journal)
    assert rec["action"] == "skipped_already_submitted"
    assert rec["submitted_before"] is True
    assert already_attempted({"date": "2026-10-06"}, None) is True


def test_missed_deadline_status_is_not_an_attempt() -> None:
    """2026-10-06 wrote missed_deadline and no submit journal."""
    import tempfile
    status = Path(tempfile.mkdtemp()) / "2026-10-06_status.json"
    status.write_text(json.dumps({
        "date": "2026-10-06",
        "status": "missed_deadline",
        "observed_at": "2026-10-06T09:34:28.027503-04:00",
    }), encoding="utf-8")
    rec = decide(ET_0934, H1_LOG, status_path=status)
    assert rec["action"] == "send"
    assert rec["late"] is True
    assert rec["submitted_before"] is False
    blocked = {"date": "2026-10-06", "status": "blocked"}
    assert already_attempted(None, blocked) is False
    assert already_attempted(None, {"status": "acknowledged"}) is True
    assert already_attempted(None, {"status": "already_submitted"}) is True
    assert already_attempted(None, {"status": "query_failed"}) is False


def test_unsealed_day_does_not_send() -> None:
    clock = datetime.fromisoformat("2026-10-07T09:12:00-04:00")
    rec = decide(clock, H1_LOG)
    assert rec["date"] == "2026-10-07"
    assert rec["sealed"] is False
    assert rec["action"] == "skipped_unsealed"
    assert rec["late"] is False


def test_bad_log_refuses() -> None:
    import tempfile
    folder = Path(tempfile.mkdtemp())
    log = folder / "h1_log.jsonl"
    line = []
    for ln in H1_LOG.read_text(encoding="utf-8").splitlines():
        obj = json.loads(ln)
        if obj.get("kind") == "plan" and obj.get("date") == "2026-10-06":
            line.append(ln)
    assert len(line) == 1
    log.write_text(line[0] + "\n" + line[0] + "\n", encoding="utf-8")
    rec = decide(ET_0912, log)
    assert rec["action"] == "refused"
    assert rec["sealed"] is False
    assert "duplicate" in rec["result"] or "duplicate" in rec.get("note", "")


def test_late_record_keeps_the_flag(tmp_path=None) -> None:
    import tempfile
    dest = Path(tempfile.mkdtemp()) / "2026-10-06_backstop.json"
    assert main([
        "record",
        "--out", str(dest),
        "--started-at", "2026-10-06T09:34:28-04:00",
        "--action", "send",
        "--result", "missed_deadline",
        "--late", "true",
        "--sealed", "true",
        "--submitted", "false",
    ]) == 0
    got = json.loads(dest.read_text(encoding="utf-8"))
    assert got["late"] is True
    assert got["action"] == "send"
    assert got["result"] == "missed_deadline"
    assert got["live"] is False
    assert got["owner"] == "actions"
    assert got["env"] == "paper"
    found = [{
        "ticker": "SDEV",
        "client_order_id": "h1-2026-10-07-SDEV-buy",
        "match": "client_order_id",
    }]
    dest2 = dest.parent / "found.json"
    assert main([
        "record",
        "--out", str(dest2),
        "--started-at", "2026-10-07T09:12:00-04:00",
        "--action", "send",
        "--result", "already_submitted",
        "--late", "false",
        "--sealed", "true",
        "--submitted", "false",
        "--found", json.dumps(found),
    ]) == 0
    listed = json.loads(dest2.read_text(encoding="utf-8"))
    assert listed["result"] == "already_submitted"
    assert listed["found"] == found
    assert listed["live"] is False


def test_workflow_and_timer_contract() -> None:
    paper = (WF / "webull_paper.yml").read_text(encoding="utf-8")
    h1 = (WF / "h1_forward.yml").read_text(encoding="utf-8")
    install_yml = (WF / "install_paper_backstop.yml").read_text(encoding="utf-8")
    timer = (ROOT / "scripts" / "systemd" / "fullscan-paper-backstop.timer").read_text(
        encoding="utf-8")
    service = (ROOT / "scripts" / "systemd" / "fullscan-paper-backstop.service").read_text(
        encoding="utf-8")
    script = (ROOT / "scripts" / "ecs_paper_backstop.sh").read_text(encoding="utf-8")
    installer = (ROOT / "scripts" / "install_paper_backstop.sh").read_text(encoding="utf-8")
    assert h1.startswith("name: h1 append-only forward\n")
    assert 'workflows: ["h1 append-only forward"]' in paper
    assert "types: [completed]" in paper
    assert "branches: [main]" in paper
    assert "github.event.workflow_run.conclusion == 'success'" in paper
    assert "github.event.workflow_run.head_branch == 'main'" in paper
    assert "webull-paper-seal" in paper
    assert "-lt 930" in paper
    assert "OnCalendar=Mon..Fri *-*-* 09:12:00 America/New_York" in timer
    assert "Persistent=false" in timer
    assert "Unit=fullscan-paper-backstop.service" in timer
    assert "ecs_paper_backstop.sh" in service
    assert "ensure_openclaw" not in service
    assert "python -m src.paper_open --submit --owner actions" in script
    assert "PAPER_OPEN_SENDER=backstop" in script
    assert '--found "$FOUND_JSON"' in script
    assert "--owner ecs" not in script
    assert "--ready" not in script
    assert "--env real" not in script
    assert "WEBULL_LIVE" in script
    assert "unset WEBULL_LIVE" in script
    assert '--late "$LATE"' in script
    assert "_backstop.json" in script
    assert "09:25" in script
    assert "ensure_openclaw" not in script
    assert "systemctl restart" not in script
    assert 'systemctl start "$UNIT_TIMER"' in installer
    assert 'systemctl start "$UNIT_SERVICE"' not in installer
    assert "ensure_openclaw" not in installer
    assert "systemctl restart" not in installer
    assert "runs-on: [self-hosted, ecs]" in install_yml
    assert "systemctl list-timers" in install_yml
    assert "PAPER_BACKSTOP_STRICT" in install_yml
    # The dormant 08:15 unit is unchanged and still no-ops.
    old = (ROOT / "scripts" / "systemd" / "fullscan-paper-open.service").read_text(
        encoding="utf-8")
    assert "--owner ecs" in old
    assert "08:15:00 America/New_York" in (
        ROOT / "scripts" / "systemd" / "fullscan-paper-open.timer"
    ).read_text(encoding="utf-8")


def main_tests() -> None:
    test_late_starts_after_0925()
    test_sealed_unsent_day_sends()
    test_submit_journal_does_not_send_again()
    test_missed_deadline_status_is_not_an_attempt()
    test_unsealed_day_does_not_send()
    test_bad_log_refuses()
    test_late_record_keeps_the_flag()
    test_workflow_and_timer_contract()
    print("test_paper_backstop: 8 ok")


if __name__ == "__main__":
    main_tests()
