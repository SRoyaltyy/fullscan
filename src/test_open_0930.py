"""09:30 ET open pack: clock + dedicated workflow.

Run: PYTHONPATH=. python3 -m src.test_open_0930
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from src.open_0930_clock import ET, decision, main, now_et, wait_for_bell

ROOT = Path(__file__).resolve().parent.parent
WF = ROOT / ".github" / "workflows"


def _et(iso: str) -> datetime:
    raw = datetime.fromisoformat(iso)
    return raw.replace(tzinfo=ET) if raw.tzinfo is None else raw.astimezone(ET)


def test_clock_run_wait_skip() -> None:
    assert decision(_et("2026-09-15T09:30:00")) == "run"
    assert decision(_et("2026-09-15T09:30:01")) == "run"
    assert decision(_et("2026-09-15T15:59:00")) == "run"
    assert decision(_et("2026-09-15T09:29:59")) == "wait"
    assert decision(_et("2026-09-15T09:00:00")) == "wait"
    assert decision(_et("2026-09-15T08:30:00")) == "wait"
    assert decision(_et("2026-09-15T07:59:00")) == "skip"
    assert decision(_et("2026-09-15T01:00:00")) == "skip"
    assert decision(_et("2026-09-15T16:00:00")) == "skip"
    assert decision(_et("2026-09-12T09:30:00")) == "skip"  # Saturday
    assert decision(_et("2026-09-07T09:30:00")) == "skip"  # Labor Day


def test_wait_for_bell_sleeps_then_runs() -> None:
    slept: list[float] = []
    got = wait_for_bell(
        _et("2026-09-15T09:25:00"),
        sleep=slept.append,
        max_wait_s=20 * 60,
    )
    assert got == "run"
    assert slept and 4 * 60 < slept[0] < 6 * 60


def test_est_0830_waits_when_max_is_70_min() -> None:
    slept: list[float] = []
    got = wait_for_bell(
        _et("2026-09-15T08:30:00"),
        sleep=slept.append,
        max_wait_s=70 * 60,
    )
    assert got == "run"
    assert slept and 59 * 60 < slept[0] < 61 * 60
    skipped = wait_for_bell(
        _et("2026-09-15T08:30:00"),
        sleep=lambda _s: (_ for _ in ()).throw(AssertionError("must not sleep")),
        max_wait_s=20 * 60,
    )
    assert skipped == "skip"


def test_wait_noops_when_already_open() -> None:
    got = wait_for_bell(
        _et("2026-09-15T09:31:00"),
        sleep=lambda _s: (_ for _ in ()).throw(AssertionError("must not sleep")),
    )
    assert got == "run"


def test_cli_now_and_exit_codes() -> None:
    assert main(["--now", "2026-09-15T09:30:00"]) == 0
    assert main(["--now", "2026-09-15T01:00:00"]) == 2
    assert main(["--wait", "--now", "2026-09-15T09:31:00"]) == 0
    assert main(["--wait", "--now", "2026-09-15T08:30:00",
                 "--max-wait-s", "60"]) == 2


def test_now_et_aware() -> None:
    utc = datetime(2026, 9, 15, 13, 30, tzinfo=ZoneInfo("UTC"))
    et = now_et(utc)
    assert et.hour == 9 and et.minute == 30


def test_open_0930_yml_owns_the_bell() -> None:
    yml = (WF / "open_0930.yml").read_text(encoding="utf-8")
    assert 'cron: "30 13 * * 1-5"' in yml
    assert 'cron: "30 14 * * 1-5"' in yml
    assert "src.open_0930_clock" in yml
    assert "--max-wait-s 4200" in yml
    assert "scripts/publish_open_pack.sh" in yml
    assert "pip install pandas pyarrow openpyxl requests" in yml
    assert "combo_sh_macd_5050_shared" in yml
    assert "--submit" in yml
    assert "src.webull_exec" in yml
    assert "deploy-dashboard.yml" in yml
    assert "timeout-minutes: 90" in yml
    assert "group: webull-paper" in yml
    assert "workflow_run:" not in yml
    assert "api.webull.com" not in yml
    assert "--env real" not in yml
    assert "flatten_robust" not in yml.lower() or "does not change" in yml.lower()


def test_open_pack_stamps_session_open_and_restamps_pages() -> None:
    """Named verify: clock_legal_for=session open + Pages restamp."""
    script = (ROOT / "scripts" / "publish_open_pack.sh").read_text(encoding="utf-8")
    st = (ROOT / "src" / "strategy_tickets.py").read_text(encoding="utf-8")
    yml = (WF / "open_0930.yml").read_text(encoding="utf-8")
    dep = (WF / "deploy-dashboard.yml").read_text(encoding="utf-8")
    assert "src.strategy_tickets --date" in script
    assert "--write" in script
    assert "set -euo pipefail" in script
    assert "clock_legal_for" in script
    assert "session_open" in script
    assert "publish_dashboard.sh" in script
    assert "assert_session_look" in st
    assert 'clock_use": "session_open"' in st
    assert "clock_legal_for" in st
    assert "assert_session_look(payload, date)" in st
    assert "scripts/publish_open_pack.sh" in yml
    assert "publish_dashboard.sh" in script
    assert "deploy-dashboard.yml" in yml
    assert "Open 09:30 pack" in dep


def test_boards_and_paper_share_the_bell() -> None:
    """Named verify: Webull --submit at the same 09:30 clock as boards."""
    yml = (WF / "open_0930.yml").read_text(encoding="utf-8")
    assert yml.count("src.open_0930_clock --wait --max-wait-s 4200") == 2
    assert yml.count("name: Clock gate (09:30 ET)") == 2
    assert "--source combo --combo combo_sh_macd_5050_shared" in yml
    assert "--submit" in yml
    assert "src.webull_exec" in yml


def test_webull_backup_schedule_is_clock_gated() -> None:
    yml = (WF / "webull_paper.yml").read_text(encoding="utf-8")
    assert 'cron: "30 13 * * 1-5"' in yml
    assert 'cron: "30 14 * * 1-5"' in yml
    assert "src.open_0930_clock" in yml
    assert "workflow_dispatch" in yml
    assert "combo_sh_macd_5050_shared" in yml
    assert "github.event_name == 'schedule'" in yml
    assert "group: webull-paper" in yml


def test_tickets_install_requests() -> None:
    yml = (WF / "publish_strategy_tickets.yml").read_text(encoding="utf-8")
    assert "openpyxl requests" in yml


def test_ci_gates_the_bell_contract() -> None:
    yml = (WF / "open_0930_test.yml").read_text(encoding="utf-8")
    assert "src.test_open_0930" in yml
    assert "src.test_strategy_tickets" in yml
    assert "src.test_webull_exec" in yml
    assert "src.test_combo_broker" in yml
    assert "src.test_skip_if_good" in yml
    assert "api.webull.com" not in yml


def test_orch_heals_open_0930() -> None:
    yml = (WF / "daily_orchestrator.yml").read_text(encoding="utf-8")
    assert "past 09:30 ET" in yml
    assert "open_0930.yml" in yml
    assert "publish_strategy_tickets.yml" in yml
    assert "skip_if_good --job open_0930" in yml


def main_tests() -> None:
    test_clock_run_wait_skip()
    test_wait_for_bell_sleeps_then_runs()
    test_est_0830_waits_when_max_is_70_min()
    test_wait_noops_when_already_open()
    test_cli_now_and_exit_codes()
    test_now_et_aware()
    test_open_0930_yml_owns_the_bell()
    test_open_pack_stamps_session_open_and_restamps_pages()
    test_boards_and_paper_share_the_bell()
    test_webull_backup_schedule_is_clock_gated()
    test_tickets_install_requests()
    test_ci_gates_the_bell_contract()
    test_orch_heals_open_0930()
    print("test_open_0930: 13 ok")


if __name__ == "__main__":
    main_tests()
