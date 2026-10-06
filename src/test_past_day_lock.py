"""Past-day lock: a new day appends, a sealed day edit fails, a header may change."""
from __future__ import annotations

from pathlib import Path

from src.past_day_lock import (
    MANIFEST_PATH,
    PastDayLockError,
    assert_factor_mine,
    assert_manifest_prefix,
    card_body,
    describe_seeds,
    guard_csv,
    load_manifest,
    prepare_flatten_card,
    seal_csv,
    seal_factor_mine,
    seal_flatten_card,
    sha256_text,
)

ROOT = Path(__file__).resolve().parents[1]


def _expect_fail(fragment: str, fn) -> None:
    try:
        fn()
    except PastDayLockError as exc:
        text = str(exc)
        assert fragment in text, text
        return
    raise AssertionError(f"expected failure containing {fragment!r}")


def _scoreboard(day_rows: list[str], *, total: str = "-1.00", fills: int = 2,
                realized: str = "-10.00", audit: str = "PASS",
                fills_rows: list[str] | None = None) -> str:
    lines = [
        "# Factor mine action — `toy`",
        "",
        f"Cash book **{total}%** ($9,900) · signal-only was -1.00%. "
        f"Starts YES **0/1**. Fills {fills} · skips 0 · realized ${realized}.",
        "",
        "## State audit",
        "",
        f"**{audit}** · 0 violations. Close cash $9,900.00.",
        "",
        "## Each session (cash + holdings state)",
        "",
        "| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | "
        "Intraday $ | Bought | Sold | Close cash | Close equity | Close held |",
        "|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|",
        *day_rows,
        "",
        "## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)",
        "",
        "| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | "
        "Equity change (sells only) | Why | Cameras |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---|---|",
        *(fills_rows or []),
    ]
    return "\n".join(lines) + "\n"


OLD_ROW = (
    "| 2026-10-05 | +0.10 | $10,000.00 | — | $10,000.00 | +0.00 | -10.00 | "
    "AAA | — | $9,990.00 | $9,990.00 | AAA×1 |"
)
NEW_ROW = (
    "| 2026-10-06 | +0.20 | $9,990.00 | AAA×1 | $9,990.00 | +0.00 | +5.00 | "
    "— | — | $9,995.00 | $9,995.00 | AAA×1 |"
)


def test_normal_append_passes(tmp: Path) -> None:
    manifest = tmp / "manifest.jsonl"
    buy = (
        "| 2026-10-06 09:30 ET | **BUY** | `AAA` | 1 | $10.00 | $1.00 | — | "
        "$9,989.00 | — | list | — |"
    )
    old = _scoreboard([OLD_ROW])
    new = _scoreboard(
        [OLD_ROW, NEW_ROW], total="-0.50", fills=3, realized="-5.00",
        fills_rows=[buy],
    )
    rows = load_manifest(manifest)
    assert_factor_mine("toy", old, new, watermark="2026-10-05", rows=rows)
    seal_factor_mine("toy", old, new, watermark="2026-10-05", manifest=manifest)
    sealed = load_manifest(manifest)
    assert sealed[0]["kind"] == "seed"
    assert sealed[0]["watermark"] == "2026-10-05"
    days = [row for row in sealed if row["kind"] == "day"]
    assert [row["date"] for row in days] == ["2026-10-06"]
    assert days[0]["name"] == "toy"
    from src.past_day_lock import locked_lines_by_date
    lines = locked_lines_by_date(new)["2026-10-06"]
    assert days[0]["sha256"] == sha256_text("\n".join(lines) + "\n")


def test_edit_past_day_fails(tmp: Path) -> None:
    manifest = tmp / "manifest.jsonl"
    old = _scoreboard([OLD_ROW])
    new = _scoreboard([OLD_ROW, NEW_ROW])
    seal_factor_mine("toy", old, new, watermark="2026-10-05", manifest=manifest)
    edited = new.replace(NEW_ROW, NEW_ROW.replace("AAA×1", "AAA×9"))
    rows = load_manifest(manifest)

    def check() -> None:
        assert_factor_mine("toy", new, edited, watermark="2026-10-05", rows=rows)

    _expect_fail("2026-10-06", check)
    assert load_manifest(manifest) == rows


def test_header_summary_update_passes(tmp: Path) -> None:
    manifest = tmp / "manifest.jsonl"
    old = _scoreboard([OLD_ROW])
    first = _scoreboard([OLD_ROW, NEW_ROW])
    seal_factor_mine("toy", old, first, watermark="2026-10-05", manifest=manifest)
    header = _scoreboard(
        [OLD_ROW, NEW_ROW], total="+4.20", fills=9, realized="+420.00", audit="PASS",
    )
    rows = load_manifest(manifest)
    assert_factor_mine("toy", first, header, watermark="2026-10-05", rows=rows)
    seal_factor_mine("toy", first, header, watermark="2026-10-05", manifest=manifest)
    assert load_manifest(manifest) == rows


def test_watermark_is_not_fingerprinted(tmp: Path) -> None:
    manifest = tmp / "manifest.jsonl"
    text = _scoreboard([OLD_ROW])
    seal_factor_mine("toy", "", text, watermark="2026-10-05", manifest=manifest)
    assert load_manifest(manifest) == []
    assert "14 Sep" not in describe_seeds() or "not fingerprinted" in describe_seeds()
    assert not MANIFEST_PATH.is_file() or "2026-09-14" not in MANIFEST_PATH.read_text(
        encoding="utf-8"
    )


def test_missed_day_is_not_filled(tmp: Path) -> None:
    manifest = tmp / "manifest.jsonl"
    old = _scoreboard([OLD_ROW])
    jumped = _scoreboard([OLD_ROW, NEW_ROW.replace("2026-10-06", "2026-10-08")])
    seal_factor_mine("toy", old, jumped, watermark="2026-10-05", manifest=manifest)
    gap = _scoreboard([
        OLD_ROW,
        NEW_ROW.replace("2026-10-06", "2026-10-07"),
        NEW_ROW.replace("2026-10-06", "2026-10-08"),
    ])
    rows = load_manifest(manifest)

    def check() -> None:
        assert_factor_mine("toy", jumped, gap, watermark="2026-10-05", rows=rows)

    _expect_fail("missed day", check)


def test_flatten_append_and_edit(tmp: Path) -> None:
    day_dir = tmp / "days"
    day_dir.mkdir()
    prior = day_dir / "2026-10-05_flatten_card.md"
    prior.write_text("# flatten_robust card — 2026-10-05\n\n| 16:00 ET | SELL | AAA |\n",
                     encoding="utf-8")
    manifest = tmp / "manifest.jsonl"
    path = day_dir / "2026-10-06_flatten_card.md"
    body = (
        "# flatten_robust card — 2026-10-06\n\n"
        "_Generated 2026-10-06T09:25:00 — live `flatten_robust`._\n\n"
        "| 09:30 ET | BUY | BBB | 10 |\n"
    )
    kept = prepare_flatten_card("2026-10-06", body, path, manifest=manifest)
    assert kept == body
    path.write_text(kept or "", encoding="utf-8")
    seal_flatten_card("2026-10-06", path, manifest=manifest)
    again = body.replace("09:25:00", "09:40:00")
    assert prepare_flatten_card("2026-10-06", again, path, manifest=manifest) is None
    edited = body.replace("BBB", "ZZZ")

    def check() -> None:
        prepare_flatten_card("2026-10-06", edited, path, manifest=manifest)

    _expect_fail("flatten_robust 2026-10-06", check)
    assert card_body(path.read_text(encoding="utf-8")).count("BBB") == 1
    sealed_dates = [row.get("date") for row in load_manifest(manifest) if row["kind"] == "day"]
    assert sealed_dates == ["2026-10-06"]


def test_csv_append_and_past_edit(tmp: Path) -> None:
    manifest = tmp / "manifest.jsonl"
    old = "date,sleeve,ticker\n2026-10-01,1d_top,AAA\n"
    new = old + "2026-10-02,1d_top,BBB\n"
    guard_csv("paper_trades", old, new, manifest=manifest)
    seal_csv("paper_trades", old, new, manifest=manifest)
    header = "date,sleeve,ticker\n2026-10-01,1d_top,AAA\n2026-10-02,1d_top,BBB\n"
    changed_old = "date,sleeve,ticker\n2026-10-01,1d_top,ZZZ\n2026-10-02,1d_top,BBB\n"
    guard_csv("paper_trades", header, changed_old, manifest=manifest)
    edited = header.replace("BBB", "QQQ")

    def check() -> None:
        guard_csv("paper_trades", header, edited, manifest=manifest)

    _expect_fail("paper_trades 2026-10-02", check)


def test_manifest_prefix() -> None:
    base = '{"kind":"seed","record":"toy","watermark":"2026-10-05"}\n'
    head = base + '{"date":"2026-10-06","kind":"day","name":"","record":"toy","sha256":"ab"}\n'
    assert_manifest_prefix(base, head)

    def changed() -> None:
        assert_manifest_prefix(base, base.replace("2026-10-05", "2026-10-04"))

    _expect_fail("manifest line changed", changed)


def test_workflow_runs_the_check() -> None:
    text = (ROOT / ".github" / "workflows" / "past_day_lock.yml").read_text(encoding="utf-8")
    assert "ubuntu-latest" in text
    assert "self-hosted" not in text
    assert "src.past_day_lock --check-against" in text
    assert "src.test_past_day_lock" in text
    yml = (ROOT / ".github" / "workflows" / "factor_mine.yml").read_text(encoding="utf-8")
    assert "data/past_day_lock/" in yml
    book = (ROOT / ".github" / "workflows" / "stock_book_all.yml").read_text(encoding="utf-8")
    assert 'runs-on: [self-hosted, ecs]' not in book.split("jobs:")[0]
    assert "data/past_day_lock/" in book


def test_sha_stable() -> None:
    assert sha256_text("abc") == sha256_text("abc")
    assert sha256_text("abc") != sha256_text("abd")


def main() -> None:
    import tempfile
    tests = [
        test_normal_append_passes,
        test_edit_past_day_fails,
        test_header_summary_update_passes,
        test_watermark_is_not_fingerprinted,
        test_missed_day_is_not_filled,
        test_flatten_append_and_edit,
        test_csv_append_and_past_edit,
        test_manifest_prefix,
        test_workflow_runs_the_check,
        test_sha_stable,
    ]
    failed = 0
    for fn in tests:
        try:
            if fn.__code__.co_argcount:
                with tempfile.TemporaryDirectory() as raw:
                    fn(Path(raw))
            else:
                fn()
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"FAIL {fn.__name__}: {exc}")
        else:
            print(f"ok {fn.__name__}")
    if failed:
        raise SystemExit(f"{failed} failed")
    print(f"{len(tests)} passed")


if __name__ == "__main__":
    main()
