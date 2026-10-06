"""Sector predict files are optional from 2026-10-07. No LLM. No writes
to past day boards or status files.

Run: PYTHONPATH=. python3 -m src.test_sector_predict_optional
"""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from unittest import mock

from src import day_board, grok_review, land_file, output_qc, skip_if_good
from src import pipeline_health as ph
from src import run_preopen_all
from src import stock_book_diag as diag
from src.pages_publish_gate import required_files
from src.pipeline_health import Report

ROOT = Path(__file__).resolve().parent.parent
PAST = "2026-10-06"
NEXT = "2026-10-07"
PAST_PATHS = (
    ROOT / "data" / "day_board" / f"{PAST}.json",
    ROOT / "01_daily" / f"{PAST}_preopen_status.json",
    ROOT / "01_daily" / f"{PAST}_preopen_status.md",
    ROOT / "01_daily" / f"{PAST}_preopen_qc.json",
    ROOT / "01_daily" / "sectors" / PAST / "_qc.json",
)


def _bytes() -> dict[Path, bytes]:
    out = {}
    for path in PAST_PATHS:
        assert path.is_file(), path
        out[path] = path.read_bytes()
    return out


def _assert_untouched(before: dict[Path, bytes]) -> None:
    for path, raw in before.items():
        assert path.read_bytes() == raw, path


def _board(date: str, *, missing_general: bool = False) -> dict:
    def inspect(kind, path, day):
        if kind == "sector_predict":
            return "MISSING", "missing", 0
        if missing_general and kind == "general_predict":
            return "MISSING", "missing", 0
        return "OK", "", 120

    with mock.patch.object(diag, "inspect_kind", side_effect=inspect), \
            mock.patch.object(day_board, "_news_mode",
                              return_value={"news_mode": "on", "reason": ""}), \
            mock.patch.object(day_board, "_selections", return_value={}):
        return day_board.build(date)


def _preopen(board: dict) -> dict:
    return next(p for p in board["processes"] if p["key"] == "preopen")


def test_zero_sectors_is_ok_and_not_missing() -> None:
    before = _bytes()
    board = _board(NEXT)
    _assert_untouched(before)
    assert board["overall"] == "OK", board["overall"]
    pre = _preopen(board)
    assert pre["status"] == "OK"
    assert pre["n_req_ok"] == pre["n_req"]
    sectors = [f for f in pre["files"] if str(f["key"]).startswith("sector_")]
    assert len(sectors) == 11
    assert all(f["role"] == "optional" for f in sectors)
    assert all(f["status"] == "SKIP" for f in sectors)
    assert all(f["status"] != "MISSING" for f in sectors)
    req = required_files(pre)
    assert req and all(row.get("status") == "OK" for row in req)
    assert not any(str(row.get("key") or "").startswith("sector_") for row in req)


def test_missing_general_predict_is_partial() -> None:
    before = _bytes()
    board = _board(NEXT, missing_general=True)
    _assert_untouched(before)
    assert board["overall"] == "PARTIAL"
    pre = _preopen(board)
    assert pre["status"] == "PARTIAL"
    general = next(f for f in pre["files"] if f["key"] == "general")
    assert general["role"] == "required"
    assert general["status"] == "MISSING"
    assert pre["n_req_ok"] == pre["n_req"] - 1
    sectors = [f for f in pre["files"] if str(f["key"]).startswith("sector_")]
    assert sectors and all(f["role"] == "optional" for f in sectors)
    assert all(f["status"] != "MISSING" for f in sectors)


def test_past_boards_stay_partial_and_untouched() -> None:
    before = _bytes()
    printed = json.loads(before[ROOT / "data" / "day_board" / f"{PAST}.json"])
    assert printed["overall"] == "PARTIAL"
    printed_pre = _preopen(printed)
    printed_sectors = [
        f for f in printed_pre["files"] if str(f["key"]).startswith("sector_")
    ]
    assert printed_sectors
    assert all(f["role"] == "required" for f in printed_sectors)
    assert any(f["status"] == "MISSING" for f in printed_sectors)

    specs = {s["key"]: s for s in diag.workflow_specs(PAST)}
    roles = [
        f["role"] for f in specs["preopen"]["files"]
        if str(f["key"]).startswith("sector_")
    ]
    assert roles and set(roles) == {"required"}
    next_specs = {s["key"]: s for s in diag.workflow_specs(NEXT)}
    next_roles = [
        f["role"] for f in next_specs["preopen"]["files"]
        if str(f["key"]).startswith("sector_")
    ]
    assert set(next_roles) == {"optional"}

    report = diag.audit(PAST, gh_runs={})
    pre = next(w for w in report.workflows if w.key == "preopen")
    assert pre.status == "PARTIAL"
    assert any(
        f.role == "required" and f.status == "MISSING" and f.key.startswith("sector_")
        for f in pre.files
    )
    general = next(f for f in pre.files if f.key == "general")
    assert general.role == "required"
    _assert_untouched(before)


def test_preopen_all_ok_ignores_missing_sectors() -> None:
    ok = output_qc._ok("core", "p", "x")
    sector_miss = output_qc._fail("sector_predict", "p", "missing", empty=True)
    general_miss = output_qc._fail("general_predict", "p", "missing", empty=True)
    names = (
        "qc_events_date", "qc_news_judge", "qc_news_parse", "qc_news_actions",
        "qc_finviz_digest", "qc_finviz_market_digest", "qc_map_heat",
        "qc_map_heat_baseline", "qc_map_heat_research",
    )

    def report(date: str, general):
        patches = [
            mock.patch.object(output_qc, "qc_general_predict", return_value=general),
            mock.patch.object(output_qc, "qc_sector_predict", return_value=sector_miss),
        ]
        patches.extend(
            mock.patch.object(output_qc, name, return_value=ok) for name in names
        )
        for patch in patches:
            patch.start()
        try:
            return output_qc.preopen_report(date)
        finally:
            for patch in reversed(patches):
                patch.stop()

    future = report(NEXT, ok)
    assert future["sector_n_ok"] == 0
    assert future["sectors_required"] is False
    assert future["all_ok"] is True
    past = report(PAST, ok)
    assert past["sector_n_ok"] == 0
    assert past["sectors_required"] is True
    assert past["all_ok"] is False
    hole = report(NEXT, general_miss)
    assert hole["all_ok"] is False
    assert any(
        i["kind"] == "general_predict" and not i["ok"] for i in hole["items"]
    )


def test_sector_qc_sidecar_does_not_fail_when_optional() -> None:
    ok = output_qc._ok("core", "p", "x")
    sector_miss = output_qc._fail("sector_predict", "p", "missing", empty=True)
    names = (
        "qc_general_predict", "qc_events_date", "qc_news_judge", "qc_news_parse",
        "qc_news_actions", "qc_finviz_digest", "qc_finviz_market_digest",
        "qc_map_heat", "qc_map_heat_baseline", "qc_map_heat_research",
    )
    before = _bytes()
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        sec = root / "01_daily" / "sectors" / NEXT
        sec.mkdir(parents=True)
        cwd = os.getcwd()
        os.chdir(root)
        try:
            patches = [
                mock.patch.object(output_qc, "qc_sector_predict",
                                  return_value=sector_miss),
            ]
            patches.extend(
                mock.patch.object(output_qc, name, return_value=ok) for name in names
            )
            for patch in patches:
                patch.start()
            try:
                output_qc.write_preopen_report(NEXT)
            finally:
                for patch in reversed(patches):
                    patch.stop()
            sidecar = json.loads((sec / "_qc.json").read_text(encoding="utf-8"))
        finally:
            os.chdir(cwd)
    assert sidecar["n_ok"] == 0
    assert sidecar["required"] is False
    assert sidecar["ok"] is True
    _assert_untouched(before)


def test_pipeline_health_does_not_fail_on_missing_sectors() -> None:
    def run(date: str) -> Report:
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            old = ph.ROOT
            ph.ROOT = root
            try:
                report = Report(
                    job="preopen", date=date,
                    source_date=date, target_date=date,
                )
                ph.check_preopen(report, date)
                return report
            finally:
                ph.ROOT = old

    future = run(NEXT)
    sectors = [c for c in future.checks if c.step.startswith("preopen.sector.")]
    assert len(sectors) == 11
    assert all(c.status == "OK" and not c.required for c in sectors)
    assert all("optional" in c.detail for c in sectors)
    count = next(c for c in future.checks if c.step == "preopen.sector_count")
    assert count.status == "OK" and not count.required
    assert not any(
        c.required and c.status == "FAIL" and c.step.startswith("preopen.sector")
        for c in future.checks
    )
    general = next(c for c in future.checks if c.step == "preopen.general")
    assert general.required and general.status == "FAIL"

    past = run(PAST)
    past_sectors = [c for c in past.checks if c.step.startswith("preopen.sector.")]
    assert past_sectors and all(c.required and c.status == "FAIL" for c in past_sectors)
    past_count = next(c for c in past.checks if c.step == "preopen.sector_count")
    assert past_count.required and past_count.status == "FAIL"


def test_grok_ok_ignores_sector_absence_only() -> None:
    sector_only = {
        "ok": False,
        "fails": [{
            "path": f"01_daily/sectors/{NEXT}/*_predict.md",
            "reason": "All 11 sector predicts missing (0/11)",
        }],
        "notes": "fails solely because sectors are missing",
    }
    future = grok_review.without_optional_sector_misses(NEXT, sector_only)
    assert future["ok"] is True
    assert future["fails"] == []
    past = grok_review.without_optional_sector_misses(PAST, sector_only)
    assert past["ok"] is False
    assert past["fails"]
    mixed = grok_review.without_optional_sector_misses(NEXT, {
        "ok": False,
        "fails": [
            {"path": f"01_daily/sectors/{NEXT}/energy_predict.md",
             "reason": "missing"},
            {"path": f"01_daily/general/{NEXT}_predict.md",
             "reason": "missing"},
        ],
    })
    assert mixed["ok"] is False
    assert [f["path"] for f in mixed["fails"]] == [
        f"01_daily/general/{NEXT}_predict.md"
    ]
    assert "OPTIONAL" in grok_review.system_for(NEXT)
    assert "OPTIONAL" not in grok_review.system_for(PAST)
    assert "at least 8" in grok_review.system_for(PAST)


def test_skip_and_land_and_packet_flag() -> None:
    ok = output_qc._ok("core", "p", "x")
    bad = output_qc._fail("general_predict", "p", "missing", empty=True)
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        empty = root / "sectors"
        empty.mkdir()
        assert land_file._qc_one(empty, NEXT).ok
        past = land_file._qc_one(empty, PAST)
        assert not past.ok and past.reason == "missing"
        with mock.patch.object(skip_if_good, "ROOT", root), \
                mock.patch.object(output_qc, "qc_general_predict", return_value=ok), \
                mock.patch.object(output_qc, "qc_events_date", return_value=ok), \
                mock.patch.object(output_qc, "qc_news_judge", return_value=ok), \
                mock.patch.object(output_qc, "qc_news_parse", return_value=ok):
            assert skip_if_good.check_preopen_all(NEXT) is True
            assert skip_if_good.check_preopen_all(PAST) is False
        with mock.patch.object(skip_if_good, "ROOT", root), \
                mock.patch.object(output_qc, "qc_general_predict", return_value=bad):
            assert skip_if_good.check_preopen_all(NEXT) is False
    assert run_preopen_all._required_for("sector_predict", True, NEXT) is False
    assert run_preopen_all._required_for("sector_predict", True, PAST) is True
    assert run_preopen_all._required_for("general_predict", True, NEXT) is True


def main() -> None:
    tests = [v for k, v in globals().items() if k.startswith("test_")]
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
