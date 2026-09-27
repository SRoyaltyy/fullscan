"""Scoring files are created once. A later run does not rewrite them."""
from __future__ import annotations

from pathlib import Path

import pytest

from research.pricefull.append_only import AppendOnlyError, assert_bytes_extend, write_once


def test_write_once_creates_then_accepts_the_same_bytes(tmp_path: Path):
    path = tmp_path / "ledger.jsonl"
    write_once(path, b"a\n")
    write_once(path, b"a\n")
    assert path.read_bytes() == b"a\n"


def test_write_once_refuses_a_rewrite(tmp_path: Path):
    path = tmp_path / "ledger.jsonl"
    write_once(path, b"a\n")
    with pytest.raises(AppendOnlyError):
        write_once(path, b"b\n")
    assert path.read_bytes() == b"a\n"


def test_extend_allows_a_suffix_only():
    assert_bytes_extend(b"a\n", b"a\nb\n")
    with pytest.raises(AppendOnlyError):
        assert_bytes_extend(b"a\n", b"b\n")
    with pytest.raises(AppendOnlyError):
        assert_bytes_extend(b"a\n", b"")
