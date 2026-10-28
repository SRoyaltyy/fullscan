"""CI guard: breadth_mine_v1d may only gain sessions after the last recorded one."""
from __future__ import annotations

from pathlib import Path

from src.breadth_mine_v1d_append import (
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
    line = manifest_line({
        "session": session,
        "file": name,
        "sha256": file_sha256(body),
        "input_blobs": [{"path": "data/prices/ohlc.parquet", "blob_sha": blob}],
    })
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
    line_a, body_a, name_a = _day("2026-08-13", b'{"session":"2026-08-13"}')
    line_b, body_b, name_b = _day("2026-08-14", b'{"session":"2026-08-14"}')
    assert_append_only("", line_a + "\n" + line_b + "\n", {}, {name_a: body_a, name_b: body_b})


def test_later_session_may_be_appended() -> None:
    line_a, body_a, name_a = _day("2026-08-13", b"day-a")
    line_b, body_b, name_b = _day("2026-08-14", b"day-b")
    assert_append_only(
        line_a + "\n",
        line_a + "\n" + line_b + "\n",
        {name_a: body_a},
        {name_a: body_a, name_b: body_b},
    )


def test_changed_per_day_file_fails() -> None:
    line_a, body_a, name_a = _day("2026-08-13", b"day-a")

    def check() -> None:
        assert_append_only(line_a + "\n", line_a + "\n", {name_a: body_a}, {name_a: b"rewritten"})

    _expect_fail("earlier return changed", check)
    _expect_fail("existing per-day file changed", check)


def test_changed_manifest_line_fails() -> None:
    line_a, _body_a, name_a = _day("2026-08-13", b"day-a")
    other = manifest_line({
        "session": "2026-08-13",
        "file": name_a,
        "sha256": file_sha256(b"day-a"),
        "input_blobs": [{"path": "other", "blob_sha": "b" * 40}],
    })

    def check() -> None:
        assert_append_only(line_a + "\n", other + "\n", {name_a: b"day-a"}, {name_a: b"day-a"})

    _expect_fail("manifest line changed", check)


def test_removed_manifest_line_fails() -> None:
    line_a, body_a, name_a = _day("2026-08-13", b"day-a")

    def check() -> None:
        assert_append_only(line_a + "\n", "", {name_a: body_a}, {name_a: body_a})

    _expect_fail("manifest line removed", check)


def test_reordered_manifest_lines_fail() -> None:
    line_a, body_a, name_a = _day("2026-08-13", b"day-a")
    line_b, body_b, name_b = _day("2026-08-14", b"day-b")

    def check() -> None:
        assert_append_only(
            line_a + "\n" + line_b + "\n",
            line_b + "\n" + line_a + "\n",
            {name_a: body_a, name_b: body_b},
            {name_a: body_a, name_b: body_b},
        )

    _expect_fail("manifest line reordered", check)


def test_sha_mismatch_fails() -> None:
    line_a, body_a, name_a = _day("2026-08-13", b"day-a")
    bad = manifest_line({
        "session": "2026-08-14",
        "file": "2026-08-14.json",
        "sha256": "c" * 64,
        "input_blobs": [{"path": "data/prices/ohlc.parquet", "blob_sha": BLOB}],
    })

    def check() -> None:
        assert_append_only(
            line_a + "\n",
            line_a + "\n" + bad + "\n",
            {name_a: body_a},
            {name_a: body_a, "2026-08-14.json": b"nope"},
        )

    _expect_fail("sha256 does not match", check)


def test_workflow_runs_the_check() -> None:
    text = (ROOT / ".github" / "workflows" / "breadth_mine_v1d_append_only.yml").read_text(encoding="utf-8")
    assert "src.breadth_mine_v1d_append --check-against" in text
    assert "src.test_breadth_mine_v1d_append" in text
    assert "src.test_breadth_mine_v1d" in text
    assert str(RETURNS_DIR) == "research/breadth_mine_v1d/returns"
    assert MANIFEST_NAME == "manifest.jsonl"


def test_head_matches_tree() -> None:
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
        test_sha_mismatch_fails,
        test_workflow_runs_the_check,
        test_head_matches_tree,
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
