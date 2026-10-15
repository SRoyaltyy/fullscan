from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from src.sleeve_card_freeze import should_skip

ET = ZoneInfo("America/New_York")
WF = Path(__file__).resolve().parent.parent / ".github/workflows/sleeve_merge_live.yml"


def _t(hm: str, day: str = "2026-10-15") -> datetime:
    return datetime.fromisoformat(f"{day}T{hm}:00").replace(tzinfo=ET)


def test_incident_times_are_skipped_for_every_trigger():
    for ev in ("schedule", "workflow_run", "workflow_dispatch"):
        assert should_skip(ev, _t("09:25"))[0]          # 2026-10-15 9bd8ea4c
        assert should_skip(ev, _t("09:20", "2026-10-14"))[0]
        assert should_skip(ev, _t("09:13"))[0]          # run 38740869857 start
        assert should_skip(ev, _t("03:00"))[0]
        assert should_skip(ev, _t("09:39"))[0]


def test_after_freeze_runs():
    for ev in ("schedule", "workflow_run", "workflow_dispatch"):
        assert not should_skip(ev, _t("09:40"))[0]
        assert not should_skip(ev, _t("16:30"))[0]


def test_utc_input_and_winter_cron():
    # 13:45Z in EST (Nov) is 08:45 ET -> gated.
    now = datetime.fromisoformat("2026-11-16T13:45:00+00:00")
    assert should_skip("schedule", now)[0]
    now = datetime.fromisoformat("2026-11-16T14:45:00+00:00")
    assert not should_skip("schedule", now)[0]


def test_early_workflow_run_skipped_dispatch_not():
    assert should_skip("workflow_run", _t("01:10"))[0]
    assert not should_skip("workflow_dispatch", _t("01:10"))[0]


def test_past_date_dispatch_not_gated():
    assert not should_skip("workflow_dispatch", _t("09:25"), "2026-10-14")[0]
    assert should_skip("workflow_dispatch", _t("09:25"), "2026-10-15")[0]


def test_workflow_gates_every_event_before_checkout():
    text = WF.read_text(encoding="utf-8")
    gate = text.index("src/sleeve_card_freeze.py")
    assert gate < text.index("actions/checkout@v4")
    assert '"25 13 * * 1-5"' not in text
    head = text[:text.index("- uses: actions/checkout@v4")]
    assert "if: github.event_name == 'workflow_run'" not in head
