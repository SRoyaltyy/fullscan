from __future__ import annotations

import json
import tempfile
from pathlib import Path

from src.preopen_plan_check import run


def _root(today_note: str | None, h1: bool, seal: bool) -> Path:
    r = Path(tempfile.mkdtemp())
    b = r / "data/day_board"; b.mkdir(parents=True)
    fm = lambda note: {"family": "factor_mine", "status": "sit" if note else "ok", "note": note or ""}
    (b / "2030-01-01_strategy_tickets.json").write_text(json.dumps(
        {"strategies": {"union_h1": fm(None), "union_hot_n4_holdup": fm(None)}}))
    if today_note is not None:
        (b / "2030-01-02_strategy_tickets.json").write_text(json.dumps(
            {"strategies": {"union_h1": fm(today_note)}}))
    lg = r / "research/hot_n4_clean_v4/forward_h1"; lg.mkdir(parents=True)
    (lg / "h1_log.jsonl").write_text(json.dumps({"kind": "plan", "date": "2030-01-02"}) + "\n" if h1 else "")
    if seal:
        s = r / "data/factor_mine/preopen/seals"; s.mkdir(parents=True)
        (s / "2030-01-02.json").write_text(json.dumps({"sleeves": {
            "union_hot_n4_h1_preopen": {}, "union_hot_n4_holdup_preopen": {}}}))
    return r


def test_names_each_missing_strategy() -> None:
    miss = dict(run(_root("no same-day panel rows for 2030-01-02 — sitting", False, False), "2030-01-02", True))
    assert set(miss) == {"union_h1", "union_hot_n4_holdup", "h1",
                         "union_hot_n4_h1_preopen", "union_hot_n4_holdup_preopen"}
    assert miss["union_hot_n4_holdup"] == "absent from tickets"


def test_missing_ticket_file_fails_roster() -> None:
    miss = dict(run(_root(None, True, True), "2030-01-02", True))
    assert set(miss) == {"union_h1", "union_hot_n4_holdup"}


def test_all_planned_passes() -> None:
    r = _root("", True, True)
    b = r / "data/day_board/2030-01-02_strategy_tickets.json"
    b.write_text(json.dumps({"strategies": {
        "union_h1": {"family": "factor_mine", "status": "sit", "note": "negative day"},
        "union_hot_n4_holdup": {"family": "factor_mine", "status": "ok"}}}))
    assert run(r, "2030-01-02", True) == []


if __name__ == "__main__":
    test_names_each_missing_strategy(); test_missing_ticket_file_fails_roster(); test_all_planned_passes(); print("ok")
