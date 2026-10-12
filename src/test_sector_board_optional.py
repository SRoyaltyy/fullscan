"""The sector predict board never blocks. The general predict still does.

Cyrus's rule: sector predict is optional (from 2026-10-07, the same date as
the essays). 01_daily/sectors/<date>/_board.json must never count as a
missing required file on the day board or in decision_readiness. Days
before 2026-10-07 keep their old contract.
"""
from pathlib import Path
from unittest.mock import patch

from . import decision_ready as dr, stock_book_diag as diag

DAY = "2026-10-09"
OLD = "2026-10-06"
BOARD = f"01_daily/sectors/{DAY}/_board.json"
GENERAL = f"01_daily/general/{DAY}_predict.md"


def _by_existence(kind, path, date):
    if Path(path).exists():
        return "OK", "", Path(path).stat().st_size
    return "MISSING", "missing", 0


def _spec(key, date=DAY):
    stock = next(s for s in diag.workflow_specs(date, as_of=True) if s["key"] == "stock_book")
    pre = next(s for s in diag.workflow_specs(date, as_of=True) if s["key"] == "preopen")
    for item in stock["files"] + pre["files"]:
        if item["key"] == key:
            return item
    raise KeyError(key)


def _write(root: Path, rel: str, text: str = "{}") -> None:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def test_missing_sector_board_is_optional_skip(tmp_path):
    with patch.object(diag, "ROOT", tmp_path), patch.object(diag, "inspect_kind", _by_existence):
        for key in ("in_board", "board"):
            check = diag._check_file(_spec(key), DAY)
            assert check.status == "SKIP", (key, check)
            assert check.role == "optional", (key, check)
            assert check.reason == diag.SECTOR_BOARD_OPTIONAL
        general = diag._check_file(_spec("in_general"), DAY)
        assert (general.role, general.status) == ("input", "MISSING")
        general_out = diag._check_file(_spec("general"), DAY)
        assert (general_out.role, general_out.status) == ("required", "MISSING")


def test_present_sector_board_keeps_input_role(tmp_path):
    _write(tmp_path, BOARD)
    with patch.object(diag, "ROOT", tmp_path), patch.object(diag, "inspect_kind", _by_existence):
        check = diag._check_file(_spec("in_board"), DAY)
        assert (check.role, check.status) == ("input", "OK")


def test_sector_board_not_ok_never_blocks_inputs(tmp_path):
    def bad_board(kind, path, date):
        if kind == "sector_board":
            return "FAIL", "unreadable", 3
        return "OK", "", 1

    with patch.object(diag, "ROOT", tmp_path), patch.object(diag, "inspect_kind", bad_board):
        stock = next(s for s in diag.workflow_specs(DAY, as_of=True) if s["key"] == "stock_book")
        files = [diag._check_file(f, DAY) for f in stock["files"]]
        board = next(f for f in files if f.key == "in_board")
        assert (board.role, board.status) == ("optional", "FAIL")
        _status, inputs_ready, *_ = diag.aggregate_status(files)
        assert inputs_ready
        assert not [f for f in files if f.role == "input" and f.status != "OK"]


def test_old_day_contract_unchanged(tmp_path):
    with patch.object(diag, "ROOT", tmp_path), patch.object(diag, "inspect_kind", _by_existence):
        check = diag._check_file(_spec("in_board", OLD), OLD)
        assert (check.role, check.status) == ("input", "MISSING")


def test_day_board_blockers_skip_sector_board_keep_general(tmp_path):
    with patch.object(diag, "ROOT", tmp_path), patch.object(diag, "inspect_kind", _by_existence):
        stock = next(s for s in diag.workflow_specs(DAY, as_of=True) if s["key"] == "stock_book")
        files = [diag._check_file(f, DAY) for f in stock["files"]]
        blockers = [f.path for f in files if f.role == "input" and f.status != "OK"]
        assert BOARD not in blockers
        assert GENERAL in blockers


def test_decision_readiness_ignores_missing_sector_board(tmp_path):
    def evaluate():
        with patch.object(diag, "ROOT", tmp_path), patch.object(dr, "ROOT", tmp_path), \
             patch.object(diag, "inspect_kind", _by_existence):
            return dr.evaluate(DAY)

    stock = next(s for s in diag.workflow_specs(DAY, as_of=True) if s["key"] == "stock_book")
    needed = [f["rel"] for f in stock["files"]
              if f["role"] == "input" or f["key"] in ("join", "peers")]
    assert BOARD in needed and GENERAL in needed
    _write(tmp_path, "data/factor_mine/panel.json")
    for rel in needed:
        if rel != BOARD:
            _write(tmp_path, rel)

    ready = evaluate()
    assert ready["ready"], ready["blockers"]
    assert BOARD not in [b["path"] for b in ready["blockers"]]
    assert BOARD not in ready["inputs"]

    _write(tmp_path, BOARD, '{"board": 1}')
    with_board = evaluate()
    assert with_board["ready"]
    assert BOARD in with_board["inputs"]
    assert with_board["fingerprint"] != ready["fingerprint"]

    (tmp_path / GENERAL).unlink()
    blocked = evaluate()
    assert not blocked["ready"]
    assert GENERAL in [b["path"] for b in blocked["blockers"]]
