"""Sector predict mid-commit: one helper, commit after each fresh write.

Run: PYTHONPATH=. python3 -m src.test_sector_predict_land
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from unittest import mock

from src import land_file, run_sector_predict as rsp
from src.sector_taxonomy import FINVIZ_SECTORS


ROOT = Path(__file__).resolve().parent.parent
WF = ROOT / ".github" / "workflows"


def _ok_essay() -> str:
    body = (
        "MEMORY_CONFIRM yesterday's Energy call.\n"
        "HIT_GRID_BEGIN\n"
        "label | hit\n"
        "SECTOR_SCORES_BEGIN\n"
        "S0_TAPE: 1\n"
        "S1_NEWS: 2\n"
        "S2_FLOW: 0\n"
        "SECTOR_SCORES_END\n"
    )
    return body + ("essay body " * 300)


def test_should_mid_commit_only_fresh_writes() -> None:
    assert rsp._should_mid_commit({"predicted_direction": "up"}) is True
    assert rsp._should_mid_commit({
        "skipped": True, "quality": "ok", "path": "x"}) is False
    assert rsp._should_mid_commit({
        "skipped": True, "quality": "fail", "reason": "qc_failed"}) is False
    assert rsp._should_mid_commit({
        "skipped": True, "reason": "past_cutoff"}) is False
    assert rsp._should_mid_commit({"quality": "ok"}) is False


def test_sector_predict_paths_are_per_sector() -> None:
    paths = land_file.sector_predict_paths("2026-09-16", "Energy")
    names = [p.name for p in paths]
    assert names == [
        "energy_predict.md",
        "energy_predict_trace.md",
        "2026-09-16_sector_energy_predict.json",
    ]
    rels = [str(p.relative_to(land_file.ROOT)) for p in paths]
    assert rels[0] == "01_daily/sectors/2026-09-16/energy_predict.md"
    assert rels[2] == "01_daily/_transcripts/2026-09-16_sector_energy_predict.json"
    other = land_file.sector_predict_paths("2026-09-16", "Technology")
    assert all("energy" not in p.name for p in other)


def test_land_one_sector_skips_when_not_qc_ok() -> None:
    date = "2026-09-16"
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        sec = tmp / "01_daily" / "sectors" / date
        sec.mkdir(parents=True)
        (sec / "energy_predict.md").write_text("too small", encoding="utf-8")
        with mock.patch.object(land_file, "ROOT", tmp), \
                mock.patch.object(land_file, "land") as fake_land, \
                mock.patch.object(land_file, "refresh_sector_progress") as fake_qc:
            rec = land_file.land_one_sector(date, "Energy")
    assert rec["pushed"] is False
    assert rec["ok"] is False
    fake_land.assert_not_called()
    fake_qc.assert_not_called()


def test_land_one_sector_does_not_rewrite_essay() -> None:
    date = "2026-09-16"
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        sec = tmp / "01_daily" / "sectors" / date
        sec.mkdir(parents=True)
        essay = sec / "energy_predict.md"
        original = _ok_essay()
        essay.write_text(original, encoding="utf-8")
        (sec / "energy_predict_trace.md").write_text(
            "# trace\n" + ("x" * 80), encoding="utf-8")
        qc_sidecar = sec / "_qc.json"
        qc_sidecar.write_text(json.dumps({
            "date": date, "n_ok": 3, "n_total": 11, "sectors": []}),
            encoding="utf-8")

        def fake_refresh(_date):
            return [qc_sidecar]

        captured: list[dict] = []

        def fake_land(date_s, key, title="", extra_paths=None,
                      require_qc=True, commit_msg=""):
            captured.append({
                "date": date_s, "key": key, "title": title,
                "extra": [str(p) for p in (extra_paths or [])],
                "commit_msg": commit_msg,
            })
            return {"key": key, "ok": True, "pushed": True}

        with mock.patch.object(land_file, "ROOT", tmp), \
                mock.patch.object(land_file, "refresh_sector_progress",
                                  fake_refresh), \
                mock.patch.object(land_file, "land", fake_land):
            rec = land_file.land_one_sector(date, "Energy")
        assert rec["pushed"] is True
        assert essay.read_text(encoding="utf-8") == original
        assert captured and captured[0]["key"] == "sector_one"
        assert captured[0]["date"] == date
        extras = captured[0]["extra"]
        assert any(x.endswith("energy_predict.md") for x in extras)
        assert any(x.endswith("energy_predict_trace.md") for x in extras)
        assert any(x.endswith("_qc.json") for x in extras)
        assert "3/11" in captured[0]["commit_msg"]
        assert "Energy" in captured[0]["commit_msg"]
        # Must not twin-write a second Technology essay.
        assert not any("technology_predict.md" in x for x in extras)


def test_loop_mid_commits_each_success_not_skip_or_fail() -> None:
    landed: list[str] = []

    def fake_run_one(sector, date_str, ch1_md, retries=1, force=False):
        if sector == "Energy":
            return {"skipped": True, "quality": "ok", "path": "x"}
        if sector == "Technology":
            return {"skipped": True, "quality": "fail", "reason": "qc_failed"}
        if sector == "Utilities":
            return {"skipped": True, "reason": "past_cutoff"}
        return {"predicted_direction": "up", "total_score": 1.0}

    def fake_land(date, sector):
        landed.append(sector)
        return {"ok": True, "pushed": True}

    with mock.patch.object(rsp, "run_one", fake_run_one), \
            mock.patch.object(rsp.land_file, "land_one_sector", fake_land), \
            mock.patch.object(rsp.fetch_channel1, "build",
                              lambda *a, **k: {}), \
            mock.patch.object(rsp.fetch_channel1, "save",
                              lambda *a, **k: None), \
            mock.patch.object(rsp.fetch_channel1, "to_markdown",
                              lambda *a, **k: "ch1"), \
            mock.patch.object(rsp.output_qc, "preopen_report",
                              lambda d: {"sector_n_ok": 8}), \
            mock.patch.object(rsp.output_qc, "write_preopen_report",
                              lambda d: None), \
            mock.patch.object(rsp, "validate", lambda: []), \
            mock.patch.object(sys, "argv", ["x", "--date", "2026-09-16"]):
        rsp.main()

    skipped = {"Energy", "Technology", "Utilities"}
    expected = [s for s in FINVIZ_SECTORS if s not in skipped]
    assert landed == expected
    assert "Energy" not in landed
    assert "Technology" not in landed
    assert "Utilities" not in landed


def test_workflows_share_one_runner_and_safe_push() -> None:
    for name in ("sector_daily.yml", "sector_predict.yml", "sector_pipeline.yml"):
        text = (WF / name).read_text(encoding="utf-8")
        assert "src.run_sector_predict" in text, name
        assert "scripts/safe_git_push.sh" in text, name
        assert "git pull --rebase origin main" not in text, name
        # No second sector module / twin writer loop in YAML.
        assert "run_sector_predict_mid" not in text
        assert "theme-radar" not in text
    src = (ROOT / "src" / "run_sector_predict.py").read_text(encoding="utf-8")
    assert "land_file.land_one_sector" in src
    assert "_should_mid_commit" in src
    land = (ROOT / "src" / "land_file.py").read_text(encoding="utf-8")
    assert "def land_one_sector" in land
    assert "safe_git_push.sh" in land
    assert "sector_one" not in land_file.CORE_QC_RESTAMP


def test_skip_if_good_still_short_circuits_run_one() -> None:
    date = "2026-09-16"
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        sec = tmp / "01_daily" / "sectors" / date
        sec.mkdir(parents=True)
        path = sec / "energy_predict.md"
        path.write_text(_ok_essay(), encoding="utf-8")
        with mock.patch.object(rsp.config, "DAILY_SECTORS",
                               str(tmp / "01_daily" / "sectors")), \
                mock.patch.object(rsp.config, "require_llm", lambda: None), \
                mock.patch.object(rsp, "deepseek_client") as chat:
            rec = rsp.run_one("Energy", date, "ch1", force=False)
        assert rec.get("skipped") is True
        assert rec.get("quality") == "ok"
        assert chat.chat.call_count == 0
        assert path.read_text(encoding="utf-8") == _ok_essay()


def main() -> None:
    tests = [
        test_should_mid_commit_only_fresh_writes,
        test_sector_predict_paths_are_per_sector,
        test_land_one_sector_skips_when_not_qc_ok,
        test_land_one_sector_does_not_rewrite_essay,
        test_loop_mid_commits_each_success_not_skip_or_fail,
        test_workflows_share_one_runner_and_safe_push,
        test_skip_if_good_still_short_circuits_run_one,
    ]
    failed = 0
    for fn in tests:
        try:
            fn()
            print(f"ok  {fn.__name__}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"FAIL {fn.__name__}: {e}")
    if failed:
        raise SystemExit(f"{failed} test(s) failed")
    print(f"{len(tests)} tests passed")


if __name__ == "__main__":
    main()
