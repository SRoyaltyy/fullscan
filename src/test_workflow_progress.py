"""Progress file: schema, heartbeat stamps, rate-limited push, stage board."""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.workflow_progress import (  # noqa: E402
    SCHEMA, StageBoard, ProgressWriter, manifest_path, progress_path,
)


def _writer(tmp: Path, stages: list[str], pushes: list, **kw) -> ProgressWriter:
    def push(paths):
        pushes.append(list(paths))
    return ProgressWriter(
        "preopen", "2026-10-15", stages, root=tmp, push_fn=push,
        model="glm-5.3-flash", heartbeat=False, **kw)


def test_file_path_and_schema(tmp: Path) -> None:
    path = progress_path("preopen", "2026-10-15", tmp)
    assert path == tmp / "01_daily" / "_progress" / "2026-10-15_preopen.json"
    pushes: list = []
    writer = _writer(tmp, ["news", "judge", "general_predict"], pushes)
    writer.begin()
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["schema"] == SCHEMA == 1
    assert data["workflow"] == "preopen"
    assert data["date"] == "2026-10-15"
    assert data["model"] == "glm-5.3-flash"
    assert [row["name"] for row in data["stages"]] == [
        "news", "judge", "general_predict"]
    assert all(row["state"] == "pending" for row in data["stages"])
    assert data["heartbeat_at"]
    manifest = json.loads(manifest_path(tmp).read_text(encoding="utf-8"))
    assert manifest["runs"][-1]["file"] == "2026-10-15_preopen.json"
    assert pushes == []


def test_heartbeat_stamps_the_injected_clock(tmp: Path) -> None:
    stamp = {"v": "2026-10-15T06:00:00-04:00"}

    def clock():
        return stamp["v"]

    pushes: list = []
    writer = _writer(tmp, ["general_predict"], pushes, clock=clock)
    writer.begin()
    writer.running("general_predict")
    stamp["v"] = "2026-10-15T06:01:05-04:00"
    writer.heartbeat()
    data = json.loads(writer.path.read_text(encoding="utf-8"))
    assert data["heartbeat_at"] == "2026-10-15T06:01:05-04:00"
    stage = data["stages"][0]
    assert stage["state"] == "running"
    assert stage["heartbeat_at"] == "2026-10-15T06:01:05-04:00"


def test_heartbeat_pushes_are_rate_limited_stage_pushes_are_not(tmp: Path) -> None:
    pushes: list = []
    writer = _writer(tmp, ["sectors"], pushes)
    writer.begin()
    assert writer.push(reason="heartbeat") is True
    assert writer.push(reason="heartbeat") is False
    assert len(pushes) == 1
    writer.ok("sectors")
    assert len(pushes) == 2
    data = json.loads(writer.path.read_text(encoding="utf-8"))
    assert data["stages"][0]["state"] == "ok"


def test_push_failure_does_not_fail_the_stage(tmp: Path) -> None:
    def boom(_paths):
        raise RuntimeError("progress push refused")

    writer = ProgressWriter(
        "preopen", "2026-10-15", ["general_predict"], root=tmp,
        push_fn=boom, heartbeat=False)
    writer.begin()
    writer.ok("general_predict")
    data = json.loads(writer.path.read_text(encoding="utf-8"))
    assert data["stages"][0]["state"] == "ok"
    assert data["stages"][0]["ended_at"]


def test_stage_board_counts_then_ok(tmp: Path) -> None:
    pushes: list = []
    writer = _writer(tmp, ["news"], pushes)
    writer.begin()
    board = StageBoard(writer, {"news": ["news_parse", "events"]})
    board.on_key("news_parse", "running")
    board.on_key("news_parse", "ok")
    data = json.loads(writer.path.read_text(encoding="utf-8"))
    stage = data["stages"][0]
    assert stage["state"] == "running"
    assert stage["done"] == 1
    assert stage["total"] == 2
    board.on_key("events", "ok")
    data = json.loads(writer.path.read_text(encoding="utf-8"))
    stage = data["stages"][0]
    assert stage["state"] == "ok"
    assert stage["done"] == 2
    assert stage["total"] == 2


def test_resume_does_not_wipe_a_finished_stage(tmp: Path) -> None:
    pushes: list = []
    writer = _writer(tmp, ["news", "judge"], pushes)
    writer.begin()
    writer.ok("news")
    again = _writer(tmp, ["news", "judge", "sectors"], pushes)
    again.begin(reset=False)
    data = json.loads(again.path.read_text(encoding="utf-8"))
    by_name = {row["name"]: row for row in data["stages"]}
    assert by_name["news"]["state"] == "ok"
    assert by_name["judge"]["state"] == "pending"
    assert by_name["sectors"]["state"] == "pending"


def test_local_heartbeat_updates_when_the_push_is_limited(tmp: Path) -> None:
    n = {"i": 0}

    def clock():
        n["i"] += 1
        return f"2026-10-15T06:00:{n['i']:02d}-04:00"

    pushes: list = []
    writer = _writer(
        tmp, ["sectors"], pushes, clock=clock, heartbeat_s=0.05)
    writer.begin(heartbeat=True)
    after_begin = n["i"]
    time.sleep(0.35)
    writer.finish()
    data = json.loads(writer.path.read_text(encoding="utf-8"))
    assert n["i"] > after_begin
    assert data["heartbeat_at"].startswith("2026-10-15T06:00:")
    assert len(pushes) <= 2


if __name__ == "__main__":
    import tempfile
    os.environ["FULLSCAN_PROGRESS_NOPUSH"] = "1"
    for fn in [
        test_file_path_and_schema,
        test_heartbeat_stamps_the_injected_clock,
        test_heartbeat_pushes_are_rate_limited_stage_pushes_are_not,
        test_push_failure_does_not_fail_the_stage,
        test_stage_board_counts_then_ok,
        test_resume_does_not_wipe_a_finished_stage,
        test_local_heartbeat_updates_when_the_push_is_limited,
    ]:
        with tempfile.TemporaryDirectory() as raw:
            fn(Path(raw))
        print(f"ok {fn.__name__}")
