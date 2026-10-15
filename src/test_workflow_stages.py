"""Stage catalog, push gate, and solo-rerun refusal."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.workflow_stages import (  # noqa: E402
    PREOPEN_STAGES, SoloRefused, push_starts_preopen, run_solo, seal_path,
    write_seal,
)

ROOT = Path(__file__).resolve().parent.parent


def test_preopen_stage_names() -> None:
    names = [spec.name for spec in PREOPEN_STAGES]
    for required in (
        "news", "judge", "general_predict", "sectors", "map_heat",
        "catalyst", "book_sleeve", "qc", "deploy",
    ):
        assert required in names


def test_push_gate() -> None:
    assert push_starts_preopen("schedule", 555, 4, False) is True
    assert push_starts_preopen("workflow_dispatch", 300, 4, False) is True
    assert push_starts_preopen("push", 555, 4, True) is True
    assert push_starts_preopen("push", 555, 4, False) is False
    assert push_starts_preopen("push", 300, 4, False) is False
    assert push_starts_preopen("push", 939, 4, False) is False
    assert push_starts_preopen("push", 200, 4, False) is False
    assert push_starts_preopen("push", 940, 4, False) is False
    assert push_starts_preopen("push", 555, 6, False) is False
    assert push_starts_preopen("push", 555, 7, False) is False
    # A weekday push is never a packet. force is the only bypass.
    assert all(
        push_starts_preopen("push", hm, 3, False) is False
        for hm in (0, 354, 355, 924, 925, 1200)
    )


def test_workflow_keeps_the_clock_strings() -> None:
    yml = (ROOT / ".github" / "workflows" / "preopen_all.yml").read_text(
        encoding="utf-8")
    assert "before 03:55 ET" in yml
    assert "weekend push poke swallowed" in yml
    assert "go=no" in yml
    assert "push during 03:00-09:40 ET is not a Pre-Open packet" in yml
    assert "renew inside Pre-Open" in yml
    assert 'github.event_name != \'workflow_dispatch\' || github.event.inputs.stage == \'\'' in yml
    land = yml.split("land_book:", 1)[1].split("preopen:", 1)[0]
    assert "book_sleeve" not in land
    assert "stage:" in yml.split("workflow_dispatch:", 1)[1].split("permissions:", 1)[0]


def _sentinel(root: Path, rel: str, body: bytes = b"sealed-bytes") -> Path:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)
    return path


def _run(root: Path, stage: str, date: str, sentinel: Path | None = None):
    called = []

    def runner() -> int:
        called.append(stage)
        if sentinel is not None:
            sentinel.write_bytes(b"overwritten")
        return 0

    try:
        code = run_solo("preopen", stage, date, runner=runner, root=root)
    except SoloRefused as exc:
        code = int(exc.code)
    return code, called


def test_solo_refuses_a_seal_and_leaves_bytes(tmp: Path) -> None:
    date = "2026-10-15"
    path = _sentinel(tmp, f"01_daily/general/{date}_predict.md")
    before = path.read_bytes()
    write_seal("preopen", date, "general_predict",
               [f"01_daily/general/{date}_predict.md"], tmp)
    code, called = _run(tmp, "general_predict", date, path)
    assert code == 2
    assert called == []
    assert path.read_bytes() == before
    assert seal_path("preopen", date, "general_predict", tmp).is_file()


def test_solo_runs_when_unsealed(tmp: Path) -> None:
    code, called = _run(tmp, "judge", "2026-10-15")
    assert code == 0
    assert called == ["judge"]


def test_solo_refuses_a_frozen_manifest(tmp: Path) -> None:
    date = "2026-10-15"
    rel = f"01_daily/general/{date}_predict.md"
    path = _sentinel(tmp, rel)
    before = path.read_bytes()
    manifest_dir = tmp / "research" / "input_freeze" / date
    manifest_dir.mkdir(parents=True)
    (manifest_dir / "manifest.json").write_text(json.dumps({
        "status": "frozen",
        "files": [{"repo": "fullscan", "source_path": rel}],
    }), encoding="utf-8")
    code, called = _run(tmp, "general_predict", date, path)
    assert code == 2
    assert called == []
    assert path.read_bytes() == before


def test_solo_refuses_past_day_lock_and_backfill(tmp: Path) -> None:
    lock = tmp / "data" / "past_day_lock"
    lock.mkdir(parents=True)
    (lock / "manifest.jsonl").write_text(
        json.dumps({
            "date": "2026-10-14", "kind": "day",
            "record": "strategy_tickets", "sha256": "abc",
        }) + "\n",
        encoding="utf-8")
    sealed = _sentinel(tmp, "data/stock_book/2026-10-14_stock_book.json")
    before = sealed.read_bytes()
    code, called = _run(tmp, "book_sleeve", "2026-10-14", sealed)
    assert code == 2
    assert called == []
    assert sealed.read_bytes() == before

    missed = _sentinel(tmp, "data/stock_book/2026-10-13_stock_book.json")
    missed_before = missed.read_bytes()
    code, called = _run(tmp, "book_sleeve", "2026-10-13", missed)
    assert code == 2
    assert called == []
    assert missed.read_bytes() == missed_before

    code, called = _run(tmp, "book_sleeve", "2026-10-16")
    assert code == 0
    assert called == ["book_sleeve"]


def test_solo_refuses_quality_ok_predict(tmp: Path) -> None:
    date = "2026-10-15"
    body = "\n".join([
        "MEMORY_CONFIRM",
        "SCORES_BEGIN",
        "B0_TREND: 1",
        "B1_BREADTH: -1",
        "B2_RATES: 2",
        "B3_NEWS: 0",
        "SCORES_END",
        "x" * 2000,
    ]).encode()
    path = _sentinel(tmp, f"01_daily/general/{date}_predict.md", body)
    before = path.read_bytes()
    code, called = _run(tmp, "general_predict", date, path)
    assert code == 2
    assert called == []
    assert path.read_bytes() == before


def test_solo_refuses_a_sealed_sector_parent(tmp: Path) -> None:
    date = "2026-10-15"
    write_seal("preopen", date, "sectors", [f"01_daily/sectors/{date}/"], tmp)
    path = _sentinel(tmp, f"01_daily/sectors/{date}/technology_predict.md")
    before = path.read_bytes()
    code, called = _run(tmp, "sectors:Technology", date, path)
    assert code == 2
    assert called == []
    assert path.read_bytes() == before


def test_deadline_skip_does_not_seal(tmp: Path) -> None:
    date = "2026-10-15"

    def runner() -> int:
        return 3

    code = run_solo("preopen", "map_heat", date, runner=runner, root=tmp)
    assert code == 0
    assert not seal_path("preopen", date, "map_heat", tmp).is_file()
    data = json.loads(
        (tmp / "01_daily" / "_progress" / f"{date}_preopen.json").read_text(
            encoding="utf-8"))
    stage = next(row for row in data["stages"] if row["name"] == "map_heat")
    assert stage["state"] == "skipped"


def test_success_seals_only_a_quality_ok_output(tmp: Path) -> None:
    date = "2026-10-15"
    rel = f"01_daily/general/{date}_predict.md"

    def thin() -> int:
        _sentinel(tmp, rel, b"too small")
        return 0

    assert run_solo("preopen", "general_predict", date, runner=thin, root=tmp) == 0
    assert not seal_path("preopen", date, "general_predict", tmp).is_file()

    essay = "\n".join([
        "MEMORY_CONFIRM",
        "SCORES_BEGIN",
        "B0_TREND: 1",
        "B1_BREADTH: -1",
        "B2_RATES: 2",
        "B3_NEWS: 0",
        "SCORES_END",
        "y" * 2000,
    ]).encode()

    def good() -> int:
        _sentinel(tmp, rel, essay)
        return 0

    # The thin success did not seal, so a later run may still write.
    # Once the essay is quality-ok, the solo seals it.
    fresh = tmp / "second"
    fresh.mkdir()
    # Reuse the same root: the first run marked the stage ok but wrote
    # no seal, so sealed_reason is still empty until the essay exists.
    # Remove the thin file first so the refusal check sees no quality-ok.
    (tmp / rel).unlink()
    assert run_solo("preopen", "general_predict", date, runner=good, root=tmp) == 0
    seal = seal_path("preopen", date, "general_predict", tmp)
    assert seal.is_file()
    first = seal.read_text(encoding="utf-8")
    write_seal("preopen", date, "general_predict", [rel], tmp)
    assert seal.read_text(encoding="utf-8") == first


if __name__ == "__main__":
    import tempfile
    os.environ["FULLSCAN_PROGRESS_NOPUSH"] = "1"
    test_preopen_stage_names()
    print("ok test_preopen_stage_names")
    test_push_gate()
    print("ok test_push_gate")
    test_workflow_keeps_the_clock_strings()
    print("ok test_workflow_keeps_the_clock_strings")
    for fn in [
        test_solo_refuses_a_seal_and_leaves_bytes,
        test_solo_runs_when_unsealed,
        test_solo_refuses_a_frozen_manifest,
        test_solo_refuses_past_day_lock_and_backfill,
        test_solo_refuses_quality_ok_predict,
        test_solo_refuses_a_sealed_sector_parent,
        test_deadline_skip_does_not_seal,
        test_success_seals_only_a_quality_ok_output,
    ]:
        with tempfile.TemporaryDirectory() as raw:
            fn(Path(raw))
        print(f"ok {fn.__name__}")
