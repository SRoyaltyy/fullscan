"""Printed paper days stay byte-for-byte. A later run only appends.

Run: python -m src.test_paper_append
"""
from __future__ import annotations

import csv
import io
import json
import os
import subprocess
from contextlib import redirect_stdout
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from src import paper_trade as pt

ET = ZoneInfo("America/New_York")


def _book(tickers: list[str]) -> dict:
    buy = [{"ticker": t} for t in tickers]
    hb = {"buy": buy, "buy_by_size": {}}
    return {"books": {h: hb for h in pt.HORIZONS}, "meta": {}}


def _sleeve(cash: float = 5000.0) -> dict:
    return {
        "cash": cash,
        "pos": {
            "AAA": {
                "shares": 10,
                "entry_date": "2026-10-01",
                "entry_px": 10.0,
                "cost": 100.0,
            }
        },
        "realized": 0.0,
        "fees": 1.0,
        "trades": 1,
        "wins": 0,
        "closed": 0,
    }


def _prices() -> pd.DataFrame:
    idx = pd.to_datetime(["2026-10-01", "2026-10-02", "2026-10-06"])
    return pd.DataFrame(
        {"SPY": [100.0, 101.0, 102.0], "AAA": [10.0, 11.0, 12.0]},
        index=idx,
    )


def _layout(tmp: Path):
    paper = tmp / "paper"
    books = tmp / "books"
    paper.mkdir()
    books.mkdir()
    (books / "2026-10-01_stock_book.json").write_text(
        json.dumps(_book(["AAA"])), encoding="utf-8")
    (books / "2026-10-02_stock_book.json").write_text(
        json.dumps(_book(["AAA"])), encoding="utf-8")
    (books / "2026-10-06_stock_book.json").write_text(
        json.dumps(_book(["AAA"])), encoding="utf-8")
    state = {f"{h}_{k}": _sleeve() for h in pt.HORIZONS for k in ("top", "size")}
    (paper / "state.json").write_text(json.dumps(state), encoding="utf-8")
    header = "date,sleeve,equity,cash,invested,fees_cum,realized_cum\n"
    row = "2026-10-01,1d_top,5100.0,5000.0,100.0,1.0,0.0\n"
    spy = "2026-10-01,SPY (benchmark),10000.0,,,,\n"
    blob = (header + row + spy).encode("utf-8")
    (paper / "equity_curve.csv").write_bytes(blob)
    return paper, books, blob


def _stamp(session: str, hour: int, minute: int) -> datetime:
    year, month, day = (int(part) for part in session.split("-"))
    return datetime(year, month, day, hour, minute, tzinfo=ET)


def _early_commit(rel: str) -> datetime:
    session = Path(rel).name[:10]
    return _stamp(session, 8, 0)


def _patch(monkeypatch, tmp: Path, prices: pd.DataFrame):
    monkeypatch.setattr(pt, "PAPER_DIR", tmp / "paper")
    monkeypatch.setattr(pt, "BOOK_DIR", tmp / "books")
    monkeypatch.setattr(pt, "DASH_DIR", tmp / "dash")
    monkeypatch.setattr(pt, "SCOREBOARD", tmp / "score")
    monkeypatch.setattr(pt, "get_prices", lambda *a, **k: prices)
    monkeypatch.setattr(pt, "load_day_meta", lambda date: {})
    monkeypatch.setattr(pt, "first_main_commit_at", _early_commit)
    pt._META_CACHE.clear()
    monkeypatch.setattr(
        pt, "_SESSION_CAL", ["2026-10-01", "2026-10-02", "2026-10-06"], raising=False,
    )


def test_resume_appends_later_books_and_keeps_printed_bytes(tmp_path, monkeypatch):
    paper, _books, blob = _layout(tmp_path)
    _patch(monkeypatch, tmp_path, _prices())
    pt.run(date="2026-10-06", top_n=10, capital=10000)
    out = (paper / "equity_curve.csv").read_bytes()
    assert out.startswith(blob)
    text = out.decode("utf-8")
    assert "2026-10-02,1d_top," in text
    assert "2026-10-06,1d_top," in text
    assert "2026-10-05" not in text
    again = (paper / "equity_curve.csv").read_bytes()
    pt.run(date="2026-10-06", top_n=10, capital=10000)
    assert (paper / "equity_curve.csv").read_bytes() == again


def test_missing_state_refuses_to_rewrite_the_curve(tmp_path, monkeypatch):
    paper, _books, blob = _layout(tmp_path)
    (paper / "state.json").unlink()
    _patch(monkeypatch, tmp_path, _prices())
    try:
        pt.run(date="2026-10-06", top_n=10, capital=10000)
    except SystemExit as exc:
        assert "refusing to rewrite" in str(exc)
    else:
        raise AssertionError("missing state rewrote the curve")
    assert (paper / "equity_curve.csv").read_bytes() == blob


def test_book_committed_before_0930_is_appended(tmp_path, monkeypatch):
    paper, _books, blob = _layout(tmp_path)
    _patch(monkeypatch, tmp_path, _prices())

    def landed(rel: str) -> datetime:
        return _stamp(Path(rel).name[:10], 9, 29)

    monkeypatch.setattr(pt, "first_main_commit_at", landed)
    logged = io.StringIO()
    with redirect_stdout(logged):
        pt.run(date="2026-10-06", top_n=10, capital=10000)
    text = (paper / "equity_curve.csv").read_text(encoding="utf-8")
    assert text.encode("utf-8").startswith(blob)
    assert "2026-10-02,1d_top," in text
    assert "2026-10-06,1d_top," in text
    out = logged.getvalue()
    assert "missing: book not on main before 09:30 ET 2026-10-02" not in out
    assert "missing: book not on main before 09:30 ET 2026-10-06" not in out


def test_late_book_stays_missing_and_the_next_session_appends(tmp_path, monkeypatch):
    paper, books, blob = _layout(tmp_path)
    # Drop AAA on both later books so a simulated 10-02 would sell that day.
    for day in ("2026-10-02", "2026-10-06"):
        (books / f"{day}_stock_book.json").write_text(
            json.dumps(_book(["BBB"])), encoding="utf-8")
    prices = _prices()
    prices["BBB"] = [20.0, 21.0, 22.0]
    _patch(monkeypatch, tmp_path, prices)

    def landed(rel: str) -> datetime | None:
        session = Path(rel).name[:10]
        if session == "2026-10-02":
            return _stamp(session, 9, 30)  # exactly 09:30 is not before
        if session == "2026-10-06":
            return _stamp(session, 9, 29)
        return None

    monkeypatch.setattr(pt, "first_main_commit_at", landed)
    logged = io.StringIO()
    with redirect_stdout(logged):
        pt.run(date="2026-10-06", top_n=10, capital=10000)
    out = (paper / "equity_curve.csv").read_bytes()
    assert out.startswith(blob)
    text = out.decode("utf-8")
    assert "2026-10-02" not in text
    assert "2026-10-06,1d_top," in text
    with (paper / "trades.csv").open(newline="", encoding="utf-8") as handle:
        fills = list(csv.DictReader(handle))
    assert fills
    assert all(str(row["date"])[:10] != "2026-10-02" for row in fills)
    assert any(str(row["date"])[:10] == "2026-10-06" and row["side"] == "sell" for row in fills)
    lines = logged.getvalue()
    assert "missing: book not on main before 09:30 ET 2026-10-02" in lines
    assert "missing: book not on main before 09:30 ET 2026-10-06" not in lines


def test_open_cutoff_is_strict_and_reads_the_oldest_main_commit(tmp_path, monkeypatch):
    assert pt.committed_before_session_open(_stamp("2026-10-02", 9, 29), "2026-10-02")
    assert not pt.committed_before_session_open(_stamp("2026-10-02", 9, 30), "2026-10-02")
    evening = datetime(2026, 10, 1, 20, 0, tzinfo=ET)
    assert pt.committed_before_session_open(evening, "2026-10-02")

    repo = tmp_path / "repo"
    book_dir = repo / "data" / "stock_book"
    book_dir.mkdir(parents=True)
    subprocess.run(["git", "init", "-b", "main"], cwd=repo, check=True, capture_output=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.name", "test"], cwd=repo, check=True)

    def _commit(session: str, stamp: str) -> None:
        path = book_dir / f"{session}_stock_book.json"
        path.write_text("{}\n", encoding="utf-8")
        subprocess.run(["git", "add", f"data/stock_book/{session}_stock_book.json"], cwd=repo, check=True)
        env = os.environ.copy()
        env["GIT_AUTHOR_DATE"] = stamp
        env["GIT_COMMITTER_DATE"] = stamp
        subprocess.run(
            ["git", "commit", "-m", f"land {session}"],
            cwd=repo, check=True, env=env, capture_output=True,
        )

    _commit("2026-10-02", "2026-10-02T09:29:00-04:00")
    _commit("2026-10-05", "2026-10-05T09:30:00-04:00")
    monkeypatch.setattr(pt, "ROOT", repo)
    assert pt.book_on_main_before_open("2026-10-02") is True
    assert pt.book_on_main_before_open("2026-10-05") is False
    assert pt.book_on_main_before_open("2026-10-06") is False


def test_unpriced_resume_does_not_skip_ahead(tmp_path, monkeypatch):
    paper, books, blob = _layout(tmp_path)
    # 10-02 has no bar at all, and nothing on or before it. 10-06 must stay missing.
    idx = pd.to_datetime(["2026-10-06"])
    prices = pd.DataFrame({"SPY": [102.0], "AAA": [12.0]}, index=idx)
    _patch(monkeypatch, tmp_path, prices)
    # Drop the 10-01 book so the only sessions after the curve are 10-02 then 10-06.
    (books / "2026-10-01_stock_book.json").unlink()
    pt.run(date="2026-10-06", top_n=10, capital=10000)
    assert (paper / "equity_curve.csv").read_bytes() == blob


if __name__ == "__main__":
    import tempfile

    class _MP:
        def setattr(self, obj, name, value):
            setattr(obj, name, value)

    with tempfile.TemporaryDirectory() as d:
        test_resume_appends_later_books_and_keeps_printed_bytes(Path(d), _MP())
    with tempfile.TemporaryDirectory() as d:
        test_missing_state_refuses_to_rewrite_the_curve(Path(d), _MP())
    with tempfile.TemporaryDirectory() as d:
        test_unpriced_resume_does_not_skip_ahead(Path(d), _MP())
    with tempfile.TemporaryDirectory() as d:
        test_book_committed_before_0930_is_appended(Path(d), _MP())
    with tempfile.TemporaryDirectory() as d:
        test_late_book_stays_missing_and_the_next_session_appends(Path(d), _MP())
    with tempfile.TemporaryDirectory() as d:
        test_open_cutoff_is_strict_and_reads_the_oldest_main_commit(Path(d), _MP())
    print("paper append ok")
