"""CI guard: factor_mine_seq_labelled may only gain sessions after the last one.

Run: PYTHONHASHSEED=0 python3 -m src.test_lever_search_labelled_append
"""
from __future__ import annotations

from src.lever_search_labelled_append import (
    MANIFEST_NAME,
    RETURNS_DIR,
    AppendOnlyError,
    assert_append_only,
    check_against,
    file_sha256,
    manifest_line,
)
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PREREG_COMMIT = "d0a7516a683a478fea39214e228cb24c5a3cc790"


def _day(session: str, body: bytes) -> tuple[str, bytes, str]:
    name = f"{session}.json"
    line = manifest_line({
        "file": name,
        "input_blobs": [{
            "blob_sha": "3456f7f489a6fa7033e8ae5cc942d8279f0113e3",
            "commit": "ff996f535e1343dd739cc801780ae224018bd96c",
            "path": "data/prices/ohlc.parquet",
        }],
        "session": session,
        "sha256": file_sha256(body),
    })
    return line, body, name


def _expect_fail(message: str, fn) -> None:
    try:
        fn()
    except AppendOnlyError as exc:
        assert message in str(exc), exc
        return
    raise AssertionError("expected AppendOnlyError")


def test_empty_base_accepts_ordered_days() -> None:
    line_a, body_a, name_a = _day("2026-08-13", b"a")
    line_b, body_b, name_b = _day("2026-08-14", b"b")
    assert_append_only(
        "",
        line_a + "\n" + line_b + "\n",
        {},
        {name_a: body_a, name_b: body_b},
    )


def test_rewrite_and_reorder_fail() -> None:
    line_a, body_a, name_a = _day("2026-08-13", b"a")
    line_b, body_b, name_b = _day("2026-08-14", b"b")
    files = {name_a: body_a, name_b: body_b}
    base = line_a + "\n" + line_b + "\n"

    def rewrite() -> None:
        assert_append_only(base, base, files, {name_a: b"changed", name_b: body_b})

    def reorder() -> None:
        assert_append_only(base, line_b + "\n" + line_a + "\n", files, files)

    def missing_commit() -> None:
        bad = manifest_line({
            "file": "2026-08-13.json",
            "input_blobs": [{
                "blob_sha": "3456f7f489a6fa7033e8ae5cc942d8279f0113e3",
                "path": "data/prices/ohlc.parquet",
            }],
            "session": "2026-08-13",
            "sha256": file_sha256(b"a"),
        })
        assert_append_only("", bad + "\n", {}, {name_a: b"a"})

    _expect_fail("earlier return changed", rewrite)
    _expect_fail("manifest line reordered", reorder)
    _expect_fail("input snapshot commit missing", missing_commit)


def test_workflow_runs_the_check() -> None:
    text = (ROOT / ".github/workflows/lever_search_labelled_append_only.yml").read_text(encoding="utf-8")
    assert "lever_search_labelled_append" in text
    assert "--check-against" in text
    assert "pull_request" in text
    prereg = (ROOT / "research/lever_search_labelled/PREREG.md").read_text(encoding="utf-8")
    assert "Append-only forever" in prereg
    assert MANIFEST_NAME in prereg
    assert str(RETURNS_DIR) in prereg
    assert "assumed pre-open, not server-proven" in prereg


def test_returns_append_on_the_prereg_commit() -> None:
    """The fingerprinted commit has no per-day files. Later sessions only append."""
    check_against(PREREG_COMMIT)


def main() -> None:
    tests = [
        test_empty_base_accepts_ordered_days,
        test_rewrite_and_reorder_fail,
        test_workflow_runs_the_check,
        test_returns_append_on_the_prereg_commit,
    ]
    for test in tests:
        test()
        print("ok", test.__name__)


if __name__ == "__main__":
    main()
