"""Dated excel_bot summary: draft before 16:00 ET, final once after the close."""
from __future__ import annotations

import csv
import os
import sys
import tempfile
from datetime import datetime
from zoneinfo import ZoneInfo

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, HERE)
sys.path.insert(0, ROOT)

import gh_summary  # noqa: E402
from src.factor_mine_freeze import is_excel_signal_path  # noqa: E402

ET = ZoneInfo("America/New_York")
DAY = "2026-09-28"
FIELDS = [
    "run_date", "signal_date", "ticker", "side", "strategy", "exit_rule",
    "ref_close", "first_open", "current_price", "ret_vs_close", "ret_vs_open",
    "days_held", "signal_colors",
]


def _suggestions(path: str) -> None:
    rows = [
        {
            "run_date": DAY, "signal_date": "2026-09-25", "ticker": "AAA",
            "side": "LONG", "strategy": "L1", "exit_rule": "tp8",
            "ref_close": "10.0000", "first_open": "", "current_price": "10.0000",
            "ret_vs_close": "+0.00%", "ret_vs_open": "", "days_held": "0",
            "signal_colors": "green",
        },
        {
            "run_date": "2026-09-25", "signal_date": "2026-09-24", "ticker": "BBB",
            "side": "SHORT", "strategy": "S1", "exit_rule": "hold1",
            "ref_close": "8.0000", "first_open": "8.1000", "current_price": "7.9000",
            "ret_vs_close": "-1.25%", "ret_vs_open": "-2.47%", "days_held": "1",
            "signal_colors": "red",
        },
    ]
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def _run(tmp: str, when: datetime) -> str:
    sugg = os.path.join(tmp, "suggestions.csv")
    _suggestions(sugg)
    return gh_summary.run(
        now=when, out_dir=os.path.join(tmp, "daily"), sugg_path=sugg,
    )


def test_midday_writes_draft_only():
    when = datetime(2026, 9, 28, 12, 0, tzinfo=ET)
    with tempfile.TemporaryDirectory() as tmp:
        written = _run(tmp, when)
        daily = os.path.join(tmp, "daily")
        assert os.path.basename(written) == f"{DAY}_excel_bot_draft.md"
        assert os.path.isfile(written)
        assert "Excel-bot daily — 2026-09-28" in open(written, encoding="utf-8").read()
        assert not os.path.exists(os.path.join(daily, f"{DAY}_excel_bot.md"))
        assert not os.path.exists(os.path.join(daily, f"{DAY}_excel_bot.csv"))
        assert not os.path.exists(os.path.join(daily, f"{DAY}_excel_bot.json"))
        assert set(os.listdir(daily)) == {f"{DAY}_excel_bot_draft.md"}


def test_after_close_creates_final():
    when = datetime(2026, 9, 28, 16, 5, tzinfo=ET)
    with tempfile.TemporaryDirectory() as tmp:
        written = _run(tmp, when)
        daily = os.path.join(tmp, "daily")
        assert os.path.basename(written) == f"{DAY}_excel_bot.md"
        body = open(written, encoding="utf-8").read()
        assert "| AAA | LONG | L1 |" in body
        assert not os.path.exists(os.path.join(daily, f"{DAY}_excel_bot_draft.md"))
        assert set(os.listdir(daily)) == {f"{DAY}_excel_bot.md"}


def test_second_after_close_is_refused():
    when = datetime(2026, 9, 28, 16, 30, tzinfo=ET)
    with tempfile.TemporaryDirectory() as tmp:
        first = _run(tmp, when)
        raw = open(first, "rb").read()
        try:
            _run(tmp, when)
        except gh_summary.FinalSignalExists as exc:
            assert "REFUSE" in str(exc)
            assert os.path.basename(first) in str(exc)
        else:
            raise AssertionError("second after-close write was not refused")
        assert open(first, "rb").read() == raw
        assert set(os.listdir(os.path.join(tmp, "daily"))) == {f"{DAY}_excel_bot.md"}


def test_main_exits_when_the_final_exists():
    real = gh_summary.run

    def boom():
        raise gh_summary.FinalSignalExists(f"daily/{DAY}_excel_bot.md")

    gh_summary.run = boom
    try:
        try:
            gh_summary.main()
        except SystemExit as exc:
            assert exc.code == 2
        else:
            raise AssertionError("refused final did not fail the run")
    finally:
        gh_summary.run = real


def test_close_boundary_and_existing_final_stays_put():
    assert gh_summary.is_after_close(datetime(2026, 9, 28, 15, 59, tzinfo=ET)) is False
    assert gh_summary.is_after_close(datetime(2026, 9, 28, 16, 0, tzinfo=ET)) is True
    with tempfile.TemporaryDirectory() as tmp:
        early = _run(tmp, datetime(2026, 9, 28, 15, 59, tzinfo=ET))
        assert os.path.basename(early).endswith("_draft.md")
        final = _run(tmp, datetime(2026, 9, 28, 16, 0, tzinfo=ET))
        assert os.path.basename(final) == f"{DAY}_excel_bot.md"
        frozen = open(final, "rb").read()
        again = _run(tmp, datetime(2026, 9, 28, 12, 30, tzinfo=ET))
        assert os.path.basename(again) == f"{DAY}_excel_bot_draft.md"
        assert open(final, "rb").read() == frozen
        assert "Excel-bot daily" in open(again, encoding="utf-8").read()


def test_sibling_csv_and_json_follow_the_same_rule():
    when_mid = datetime(2026, 9, 28, 12, 0, tzinfo=ET)
    when_close = datetime(2026, 9, 28, 16, 10, tzinfo=ET)
    with tempfile.TemporaryDirectory() as tmp:
        out = os.path.join(tmp, "daily")
        for ext in (".csv", ".json"):
            draft = gh_summary.write_dated(
                out, DAY, ext, "draft\n", after_close=gh_summary.is_after_close(when_mid),
            )
            assert os.path.basename(draft) == f"{DAY}_excel_bot_draft{ext}"
            assert not os.path.exists(os.path.join(out, f"{DAY}_excel_bot{ext}"))
            final = gh_summary.write_dated(
                out, DAY, ext, "final\n", after_close=gh_summary.is_after_close(when_close),
            )
            assert os.path.basename(final) == f"{DAY}_excel_bot{ext}"
            frozen = open(final, "rb").read()
            try:
                gh_summary.write_dated(
                    out, DAY, ext, "rewrite\n",
                    after_close=gh_summary.is_after_close(when_close),
                )
            except gh_summary.FinalSignalExists:
                pass
            else:
                raise AssertionError(f"second {ext} final was not refused")
            assert open(final, "rb").read() == frozen


def test_readers_keep_the_final_and_skip_the_draft():
    final = f"excel_bot/daily/{DAY}_excel_bot.md"
    draft = f"excel_bot/daily/{DAY}_excel_bot_draft.md"
    assert is_excel_signal_path(final)
    assert is_excel_signal_path("excel_bot/suggestions/suggestions.csv")
    assert not is_excel_signal_path(draft)
    assert not is_excel_signal_path(f"excel_bot/daily/{DAY}_excel_bot_draft.csv")
    assert not is_excel_signal_path(f"excel_bot/daily/{DAY}_excel_bot_draft.json")
    allowed = {
        "excel_bot/engine/gh_summary.py",
        "excel_bot/engine/test_gh_summary.py",
        "excel_bot/README.md",
        "src/factor_mine_freeze.py",
        "src/test_factor_mine_freeze.py",
        ".github/workflows/excel_bot.yml",
    }
    hits = []
    skip = {".git", "node_modules", "__pycache__", ".venv"}
    for dirpath, dirnames, filenames in os.walk(ROOT):
        dirnames[:] = [name for name in dirnames if name not in skip]
        for name in filenames:
            if not name.endswith((".py", ".yml", ".yaml", ".sh", ".js", ".html", ".md")):
                continue
            path = os.path.join(dirpath, name)
            rel = os.path.relpath(path, ROOT)
            if rel in allowed:
                continue
            text = open(path, encoding="utf-8", errors="ignore").read()
            if "excel_bot/daily" in text or "_excel_bot_draft" in text:
                hits.append(rel)
    assert hits == []


if __name__ == "__main__":
    test_midday_writes_draft_only()
    test_after_close_creates_final()
    test_second_after_close_is_refused()
    test_main_exits_when_the_final_exists()
    test_close_boundary_and_existing_final_stays_put()
    test_sibling_csv_and_json_follow_the_same_rule()
    test_readers_keep_the_final_and_skip_the_draft()
    print("ok")
