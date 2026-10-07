"""Replay panel + labels. Run: python -m src.test_factor_mine_replay_preopen"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

from src import factor_mine_replay_preopen as rp

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "patch_fm_preopen_labels", ROOT / "scripts" / "patch_fm_preopen_labels.py")
pl = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pl)


def test_base_state_matches_printed_close() -> None:
    st = rp.base_state("union_hot_n4_holdup")
    assert sorted(st["pos"]) == ["SECZ", "TJGC", "USDE"], st["pos"]
    assert abs(st["cash"] - 12417.53) < 1e-6
    h1 = rp.base_state("union_hot_n4_h1")
    assert h1["pos"] == {} and abs(h1["cash"] - 12236.0) < 1e-6


def test_replay_file_is_outside_scoreboard_and_lock() -> None:
    assert "03_scoreboard" not in str(rp.OUT) and "past_day_lock" not in str(rp.OUT)
    doc = json.loads(rp.OUT.read_text(encoding="utf-8"))
    assert doc["sealed"] is False and doc["title"] == rp.TITLE
    for rep in doc["recipes"].values():
        assert [d["date"] for d in rep["days"]] == list(rp.DAYS)
        for d in rep["days"]:
            assert d["ticket_pre_open"] is True
            if not d["saved_picks"]:
                assert d["label"] == "no pre-open picks saved" and d["reason"]
                assert d["buys"] == []
            if d["red_morning"]:
                assert d["buys"] == []
    manifest = (ROOT / "data/past_day_lock/manifest.jsonl").read_text()
    assert "replay_preopen" not in manifest


def test_patch_is_add_only_and_idempotent() -> None:
    page = "<html><body class='x'><p>keep</p></body></html>"
    doc = {"recipes": {"union_hot_n4_h1": {"base_day": "2026-09-28", "days": [
        {"date": "2026-09-29", "saved_picks": [], "reason": "sat",
         "ticket_committed_at": "t", "buys": [], "sells": [], "equity": 1}]}}}
    out = pl.patch_html(page, doc)
    assert "<p>keep</p>" in out and pl.NEW_LABEL in out and pl.PARENT_LABEL in out
    assert "Replay from pre-open inputs, not sealed" in out
    assert "no pre-open picks saved" in out and "IRONCLAD h1" in out
    assert pl.patch_html(out, doc) == out


def main() -> None:
    test_base_state_matches_printed_close()
    test_replay_file_is_outside_scoreboard_and_lock()
    test_patch_is_add_only_and_idempotent()
    print("ok factor_mine_replay_preopen")


if __name__ == "__main__":
    main()
