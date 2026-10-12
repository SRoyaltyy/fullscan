"""Cross-process OpenClaw breaker + read cap (2026-10-07 ECS hang)."""
import pytest

from src import deepseek_client as dc


def test_breaker_trips_across_processes(tmp_path, monkeypatch):
    f = tmp_path / "b.txt"
    monkeypatch.setenv("OPENCLAW_BREAKER_FILE", str(f))
    monkeypatch.setenv("OPENCLAW_BREAKER_N", "2")
    assert not dc._breaker_open()
    dc._breaker_note_timeout("SECTOR A")
    assert not dc._breaker_open()
    dc._breaker_note_timeout("SECTOR B")
    assert dc._breaker_open()
    with pytest.raises(RuntimeError, match="breaker open"):
        dc._post_openclaw([{"role": "user", "content": "x"}], 10, 0.0, "S")


def test_breaker_off_without_file(monkeypatch):
    monkeypatch.delenv("OPENCLAW_BREAKER_FILE", raising=False)
    dc._breaker_note_timeout("x")
    assert not dc._breaker_open()


def test_read_cap(monkeypatch):
    monkeypatch.setenv("OPENCLAW_READ_CAP_S", "240")
    assert dc._read_cap(10800) == 240
    assert dc._read_cap(100) == 100
    monkeypatch.delenv("OPENCLAW_READ_CAP_S")
    assert dc._read_cap(10800) == 10800
