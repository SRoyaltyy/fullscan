import json
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent))
from open_fill_gate import decide  # noqa: E402

NY = ZoneInfo("America/New_York")
NOW = datetime(2026, 11, 2, 9, 45, tzinfo=NY)


def _log(tmp: Path) -> Path:
    p = tmp / "h1_log.jsonl"
    p.write_text(json.dumps({"date": "2026-11-02", "kind": "plan"}) + "\n")
    return p


def _snap(tmp: Path):
    return {p: p.read_bytes() for p in tmp.rglob("*") if p.is_file()}


def test_plan_without_no_send_runs(tmp_path):
    assert decide(_log(tmp_path), NOW)[0] is True


def test_share_lock_skip_stops(tmp_path):
    s = tmp_path / "skips.jsonl"
    s.write_text(json.dumps({"date": "2026-11-02", "reason": "sent share lock missing for 2026-11-02"}) + "\n")
    log = _log(tmp_path)
    before = _snap(tmp_path)
    assert decide(log, NOW, skips=s)[0] is False
    assert _snap(tmp_path) == before


def test_fill_missing_note_stops(tmp_path):
    n = tmp_path / "notes.jsonl"
    n.write_text(json.dumps({"date": "2026-11-02", "kind": "fill_missing"}) + "\n")
    assert decide(_log(tmp_path), NOW, notes=n)[0] is False


def test_missed_deadline_stops(tmp_path):
    d = tmp_path / "paper_open"
    d.mkdir()
    (d / "2026-11-02_status.json").write_text(json.dumps({"status": "missed_deadline", "submit": False}))
    assert decide(_log(tmp_path), NOW, status_dir=d)[0] is False


def test_other_day_no_send_ignored(tmp_path):
    s = tmp_path / "skips.jsonl"
    s.write_text(json.dumps({"date": "2026-10-26", "reason": "sent share lock missing"}) + "\n")
    assert decide(_log(tmp_path), NOW, skips=s)[0] is True
