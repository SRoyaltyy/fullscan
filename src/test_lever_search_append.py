"""CI guard: factor_mine_seq may only gain sessions after the last recorded one."""
from __future__ import annotations

from pathlib import Path

from src.lever_search_append import (
    MANIFEST_NAME,
    RETURNS_DIR,
    AppendOnlyError,
    assert_append_only,
    check_against,
    file_sha256,
    manifest_line,
)

ROOT = Path(__file__).resolve().parents[1]
BLOB = "a" * 40


def _day(session: str, body: bytes, *, blob: str = BLOB) -> tuple[str, bytes, str]:
    name = f"{session}.json"
    line = manifest_line(
        {
            "session": session,
            "file": name,
            "sha256": file_sha256(body),
            "input_blobs": [{"path": "data/prices/ohlc.parquet", "blob_sha": blob}],
        }
    )
    return line, body, name


def _expect_fail(message: str, fn) -> None:
    try:
        fn()
    except AppendOnlyError as exc:
        if message not in str(exc):
            raise AssertionError(f"{exc!s} did not include {message}") from exc
    else:
        raise AssertionError(message)


def test_empty_record_passes() -> None:
    assert_append_only("", "", {}, {})


def test_first_sessions_append_in_order() -> None:
    line_a, body_a, name_a = _day("2026-08-20", b'{"session":"2026-08-20"}')
    line_b, body_b, name_b = _day("2026-08-21", b'{"session":"2026-08-21"}')
    assert_append_only(
        "",
        line_a + "\n" + line_b + "\n",
        {},
        {name_a: body_a, name_b: body_b},
    )


def test_later_session_may_be_appended() -> None:
    line_a, body_a, name_a = _day("2026-08-20", b"day-a")
    line_b, body_b, name_b = _day("2026-08-21", b"day-b")
    assert_append_only(
        line_a + "\n",
        line_a + "\n" + line_b + "\n",
        {name_a: body_a},
        {name_a: body_a, name_b: body_b},
    )


def test_changed_per_day_file_fails() -> None:
    line_a, body_a, name_a = _day("2026-08-20", b"day-a")

    def check() -> None:
        assert_append_only(line_a + "\n", line_a + "\n", {name_a: body_a}, {name_a: b"day-a-rewritten"})

    _expect_fail("earlier return changed", check)
    _expect_fail("existing per-day file changed", check)


def test_changed_manifest_line_fails() -> None:
    line_a, body_a, name_a = _day("2026-08-20", b"day-a")
    edited, _, _ = _day("2026-08-20", b"day-a", blob="b" * 40)

    def check() -> None:
        assert_append_only(line_a + "\n", edited + "\n", {name_a: body_a}, {name_a: body_a})

    _expect_fail("manifest line changed", check)


def test_removed_manifest_line_fails() -> None:
    line_a, body_a, name_a = _day("2026-08-20", b"day-a")
    line_b, body_b, name_b = _day("2026-08-21", b"day-b")
    base = line_a + "\n" + line_b + "\n"
    files = {name_a: body_a, name_b: body_b}

    def check() -> None:
        assert_append_only(base, line_a + "\n", files, files)

    _expect_fail("manifest line removed", check)


def test_reordered_manifest_lines_fail() -> None:
    line_a, body_a, name_a = _day("2026-08-20", b"day-a")
    line_b, body_b, name_b = _day("2026-08-21", b"day-b")
    base = line_a + "\n" + line_b + "\n"
    files = {name_a: body_a, name_b: body_b}

    def check() -> None:
        assert_append_only(base, line_b + "\n" + line_a + "\n", files, files)

    _expect_fail("manifest line reordered", check)


def test_insert_before_the_end_fails() -> None:
    line_a, body_a, name_a = _day("2026-08-20", b"day-a")
    line_b, body_b, name_b = _day("2026-08-21", b"day-b")
    line_c, body_c, name_c = _day("2026-08-24", b"day-c")
    base = line_a + "\n" + line_b + "\n"
    head = line_a + "\n" + line_c + "\n" + line_b + "\n"

    def check() -> None:
        assert_append_only(
            base,
            head,
            {name_a: body_a, name_b: body_b},
            {name_a: body_a, name_b: body_b, name_c: body_c},
        )

    _expect_fail("manifest line reordered", check)


def test_backfill_on_or_before_the_last_session_fails() -> None:
    line_b, body_b, name_b = _day("2026-08-21", b"day-b")
    line_a, body_a, name_a = _day("2026-08-20", b"day-a")

    def check() -> None:
        assert_append_only(
            line_b + "\n",
            line_b + "\n" + line_a + "\n",
            {name_b: body_b},
            {name_b: body_b, name_a: body_a},
        )

    _expect_fail("session not after the last recorded one", check)


def test_removed_per_day_file_fails() -> None:
    line_a, body_a, name_a = _day("2026-08-20", b"day-a")

    def check() -> None:
        assert_append_only(line_a + "\n", line_a + "\n", {name_a: body_a}, {})

    _expect_fail("per-day file removed", check)


def test_sha_mismatch_and_missing_blob_fail() -> None:
    body = b"day-a"
    name = "2026-08-20.json"
    bad_sha = manifest_line(
        {
            "session": "2026-08-20",
            "file": name,
            "sha256": "0" * 64,
            "input_blobs": [{"path": "data/prices/ohlc.parquet", "blob_sha": BLOB}],
        }
    )
    no_blob = manifest_line(
        {
            "session": "2026-08-20",
            "file": name,
            "sha256": file_sha256(body),
            "input_blobs": [],
        }
    )

    def bad_hash() -> None:
        assert_append_only("", bad_sha + "\n", {}, {name: body})

    def missing_pin() -> None:
        assert_append_only("", no_blob + "\n", {}, {name: body})

    _expect_fail("per-day file sha256 does not match the manifest line", bad_hash)
    _expect_fail("input snapshot blob sha missing", missing_pin)


def test_same_bytes_are_not_a_change() -> None:
    line_a, body_a, name_a = _day("2026-08-20", b"day-a")
    assert_append_only(line_a + "\n", line_a + "\n", {name_a: body_a}, {name_a: body_a})


def test_workflow_runs_the_check() -> None:
    text = (ROOT / ".github/workflows/lever_search_append_only.yml").read_text(encoding="utf-8")
    assert "lever_search_append" in text
    assert "--check-against" in text
    assert "pull_request" in text
    prereg = (ROOT / "research/lever_search/PREREG.md").read_text(encoding="utf-8")
    assert "APPEND-ONLY FOREVER" in prereg
    assert MANIFEST_NAME in prereg
    assert "blob sha" in prereg
    assert "NEW study" in prereg
    assert str(RETURNS_DIR) in prereg


def test_head_matches_empty_base() -> None:
    """This commit has no per-day files. The guard passes against HEAD."""
    check_against("HEAD")


def main() -> None:
    tests = [
        test_empty_record_passes,
        test_first_sessions_append_in_order,
        test_later_session_may_be_appended,
        test_changed_per_day_file_fails,
        test_changed_manifest_line_fails,
        test_removed_manifest_line_fails,
        test_reordered_manifest_lines_fail,
        test_insert_before_the_end_fails,
        test_backfill_on_or_before_the_last_session_fails,
        test_removed_per_day_file_fails,
        test_sha_mismatch_and_missing_blob_fail,
        test_same_bytes_are_not_a_change,
        test_workflow_runs_the_check,
        test_head_matches_empty_base,
    ]
    failed = 0
    for fn in tests:
        try:
            fn()
            print(f"ok  {fn.__name__}")
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"FAIL {fn.__name__}: {exc}")
    if failed:
        raise SystemExit(f"{failed} test(s) failed")
    print(f"{len(tests)} tests passed")


if __name__ == "__main__":
    main()
