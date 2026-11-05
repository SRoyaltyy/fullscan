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
from src import paper_trade_append as pa

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
    monkeypatch.setattr(pa, "first_main_commit_at", _early_commit)
    pt._META_CACHE.clear()
    monkeypatch.setattr(
        pt, "_SESSION_CAL", ["2026-10-01", "2026-10-02", "2026-10-06"], raising=False,
    )


def test_resume_appends_later_books_and_keeps_printed_bytes(tmp_path, monkeypatch):
    paper, _books, blob = _layout(tmp_path)
    _patch(monkeypatch, tmp_path, _prices())
    pa.run(date="2026-10-06", top_n=10, capital=10000)
    out = (paper / "equity_curve.csv").read_bytes()
    assert out.startswith(blob)
    text = out.decode("utf-8")
    assert "2026-10-02,1d_top," in text
    assert "2026-10-06,1d_top," in text
    assert "2026-10-05" not in text
    again = (paper / "equity_curve.csv").read_bytes()
    pa.run(date="2026-10-06", top_n=10, capital=10000)
    assert (paper / "equity_curve.csv").read_bytes() == again


def test_missing_state_refuses_to_rewrite_the_curve(tmp_path, monkeypatch):
    paper, _books, blob = _layout(tmp_path)
    (paper / "state.json").unlink()
    _patch(monkeypatch, tmp_path, _prices())
    try:
        pa.run(date="2026-10-06", top_n=10, capital=10000)
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

    monkeypatch.setattr(pa, "first_main_commit_at", landed)
    logged = io.StringIO()
    with redirect_stdout(logged):
        pa.run(date="2026-10-06", top_n=10, capital=10000)
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

    monkeypatch.setattr(pa, "first_main_commit_at", landed)
    logged = io.StringIO()
    with redirect_stdout(logged):
        pa.run(date="2026-10-06", top_n=10, capital=10000)
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


def test_unproven_commit_does_not_record_a_miss(tmp_path, monkeypatch):
    paper, _books, blob = _layout(tmp_path)
    _patch(monkeypatch, tmp_path, _prices())
    monkeypatch.setattr(pa, "first_main_commit_at", lambda rel: None)
    logged = io.StringIO()
    with redirect_stdout(logged):
        pa.run(date="2026-10-06", top_n=10, capital=10000)
    assert (paper / "equity_curve.csv").read_bytes() == blob
    assert not (paper / "missed.json").exists()
    assert "unproven:" in logged.getvalue()


def test_false_miss_clears_and_a_late_miss_stays(tmp_path, monkeypatch):
    paper, _books, blob = _layout(tmp_path)
    _patch(monkeypatch, tmp_path, _prices())
    (paper / "missed.json").write_text(json.dumps({
        "sessions": {
            "2026-10-02": {
                "status": "missed",
                "reason": "stock book was not on main before 09:30 ET",
            },
            "2026-10-06": {
                "status": "missed",
                "reason": "stock book was not on main before 09:30 ET",
            },
        }
    }), encoding="utf-8")

    def landed(rel: str) -> datetime:
        session = Path(rel).name[:10]
        if session == "2026-10-02":
            return _stamp(session, 10, 2)
        return _stamp(session, 9, 23)

    monkeypatch.setattr(pa, "first_main_commit_at", landed)
    pa.run(date="2026-10-06", top_n=10, capital=10000)
    text = (paper / "equity_curve.csv").read_text(encoding="utf-8")
    assert text.encode("utf-8").startswith(blob)
    assert "2026-10-02" not in text
    assert "2026-10-06,1d_top," in text
    missed = json.loads((paper / "missed.json").read_text(encoding="utf-8"))
    assert missed["sessions"]["2026-10-02"]["status"] == "missed"
    assert "2026-10-06" not in missed["sessions"]


def test_open_session_waits_for_the_close(tmp_path, monkeypatch):
    paper, _books, blob = _layout(tmp_path)
    _patch(monkeypatch, tmp_path, _prices())
    (paper / "missed.json").write_text(json.dumps({
        "sessions": {
            "2026-10-06": {
                "status": "missed",
                "reason": "stock book was not on main before 09:30 ET",
            },
        }
    }), encoding="utf-8")
    monkeypatch.setattr(pa, "session_close_printed", lambda session, now=None: False)
    pa.run(date="2026-10-06", top_n=10, capital=10000)
    assert (paper / "equity_curve.csv").read_bytes() == blob
    missed = json.loads((paper / "missed.json").read_text(encoding="utf-8"))
    assert "2026-10-06" not in missed["sessions"]


def test_open_cutoff_is_strict_and_reads_the_oldest_main_commit(tmp_path, monkeypatch):
    assert pa.committed_before_session_open(_stamp("2026-10-02", 9, 29), "2026-10-02")
    assert not pa.committed_before_session_open(_stamp("2026-10-02", 9, 30), "2026-10-02")
    evening = datetime(2026, 10, 1, 20, 0, tzinfo=ET)
    assert pa.committed_before_session_open(evening, "2026-10-02")

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
    assert pa.book_on_main_before_open("2026-10-02") is True
    assert pa.book_on_main_before_open("2026-10-05") is False
    assert pa.book_on_main_before_open("2026-10-06") is False


def test_nothing_eligible_still_renders_the_dashboard(tmp_path, monkeypatch):
    paper, _books, blob = _layout(tmp_path)
    _patch(monkeypatch, tmp_path, _prices())
    pa.run(date="2026-10-06", top_n=10, capital=10000)
    locked = (paper / "equity_curve.csv").read_bytes()
    assert locked.startswith(blob)
    dash = tmp_path / "dash" / "index.html"
    dash.write_text("<html>stale</html>", encoding="utf-8")
    pa.run(date="2026-10-06", top_n=10, capital=10000)
    assert (paper / "equity_curve.csv").read_bytes() == locked
    html = dash.read_text(encoding="utf-8")
    assert "stale" not in html
    assert 'name="data-asof" content="2026-10-06"' in html


def _workflow_jobs(text: str) -> dict[str, str]:
    """Split a workflow file into top-level job bodies."""
    lines = text.splitlines()
    start = next(i for i, line in enumerate(lines) if line == "jobs:")
    jobs: dict[str, str] = {}
    name = ""
    buf: list[str] = []
    for line in lines[start + 1:]:
        if line and not line.startswith(" ") and not line.startswith("#"):
            break
        if (
            line.startswith("  ")
            and not line.startswith("   ")
            and line.rstrip().endswith(":")
            and not line.strip().startswith("#")
        ):
            if name:
                jobs[name] = "\n".join(buf)
            name = line.strip()[:-1]
            buf = [line]
        elif name:
            buf.append(line)
    if name:
        jobs[name] = "\n".join(buf)
    return jobs


def test_no_publish_gate_still_renders_the_page() -> None:
    """Run 41849547018 set publish=false and skipped the tickets job.

    The main page render and deploy live on a job that still runs.
    Ticket publishing stays behind the gate. The render step refuses
    to continue when sealed paper rows change.
    """
    root = Path(__file__).resolve().parent.parent
    text = (root / ".github/workflows/publish_strategy_tickets.yml").read_text(
        encoding="utf-8")
    jobs = _workflow_jobs(text)
    tickets = jobs["tickets"]
    page = jobs["page"]
    assert "needs.gate.outputs.publish == 'true'" in tickets
    assert "python3 -m src.paper_trade_append --render-only" not in tickets
    assert "needs.gate.outputs.publish" not in page
    assert "always()" in page
    assert "needs.gate.result == 'success'" in page
    assert "needs.tickets.result != 'cancelled'" in page
    assert "python3 -m src.paper_trade_append --render-only" in page
    assert "git diff --exit-code -- data/paper" in page
    assert "name: dashboard" in page
    assert "publish_dashboard.sh" in page
    assert "gh workflow run deploy-dashboard.yml" in page
    assert text.count("python3 -m src.paper_trade_append --render-only") == 1


def test_render_only_does_not_append(tmp_path, monkeypatch):
    paper, _books, blob = _layout(tmp_path)
    _patch(monkeypatch, tmp_path, _prices())
    pa.run(date="2026-10-06", top_n=10, capital=10000, render_only=True)
    assert (paper / "equity_curve.csv").read_bytes() == blob
    html = (tmp_path / "dash" / "index.html").read_text(encoding="utf-8")
    assert 'name="data-asof" content="2026-10-01"' in html


def test_sealed_roundtrips_still_refresh_the_dashboard(tmp_path, monkeypatch):
    """A roundtrip lock must not skip the page after the curve has appended."""
    paper, _books, blob = _layout(tmp_path)
    _patch(monkeypatch, tmp_path, _prices())
    monkeypatch.setattr(pt, "match_roundtrips", lambda *a, **k: [
        {"status": "closed", "sell_date": "2026-10-02", "ticker": "AAA"},
    ])
    monkeypatch.setattr(pt, "attach_trails", lambda *a, **k: None)
    real = pa._commit_csv

    def wrapped(record, filename, new_text, column):
        if record == "paper_roundtrips":
            raise SystemExit(
                "past-day lock: paper_roundtrips 2026-10-01 was already written "
                "and changed. Not rescoring a sealed day."
            )
        return real(record, filename, new_text, column)

    monkeypatch.setattr(pa, "_commit_csv", wrapped)
    pa.run(date="2026-10-06", top_n=10, capital=10000)
    out = (paper / "equity_curve.csv").read_bytes()
    assert out.startswith(blob)
    assert b"2026-10-06" in out
    assert not (paper / "roundtrips.csv").is_file()
    html = (tmp_path / "dash" / "index.html").read_text(encoding="utf-8")
    assert 'name="data-asof" content="2026-10-06"' in html


def test_roundtrip_lock_does_not_write_sealed_rows(tmp_path, monkeypatch):
    """A refused roundtrip write leaves every sealed row byte-for-byte."""
    from src import past_day_lock as pdl

    paper = tmp_path / "paper"
    paper.mkdir()
    manifest = tmp_path / "manifest.jsonl"
    manifest.write_text(
        '{"kind":"seed","record":"paper_roundtrips","watermark":"2026-10-06"}\n',
        encoding="utf-8",
    )
    header = "status,sell_date,ticker\n"
    sealed = header + "closed,2026-10-06,AAA\nclosed,2026-10-07,BBB\nclosed,2026-10-08,CCC\n"
    path = paper / "roundtrips.csv"
    path.write_bytes(sealed.encode("utf-8"))
    monkeypatch.setattr(pt, "PAPER_DIR", paper)
    monkeypatch.setattr(pdl, "MANIFEST_PATH", manifest)
    monkeypatch.setattr(pdl, "in_repo", lambda _path: True)
    assert pa._last_sealed_sell_day(sealed) == "2026-10-07"

    changed = (
        header
        + "closed,2026-10-06,AAA\n"
        + "closed,2026-10-07,ZZZ\n"
        + "closed,2026-10-07,NEW\n"
        + "closed,2026-10-08,CCC\n"
        + "closed,2026-10-09,DDD\n"
    )
    assert pa._commit_roundtrips(changed) is False
    assert path.read_bytes() == sealed.encode("utf-8")
    text = path.read_text(encoding="utf-8")
    assert text.count("2026-10-07") == 1
    assert "ZZZ" not in text
    assert "NEW" not in text
    assert "2026-10-09" not in text

    appended = (
        header
        + "closed,2026-10-06,AAA\n"
        + "closed,2026-10-07,BBB\n"
        + "closed,2026-10-08,CCC\n"
        + "closed,2026-10-09,DDD\n"
    )
    assert pa._commit_roundtrips(appended) is True
    after = path.read_text(encoding="utf-8")
    assert "closed,2026-10-06,AAA" in after
    assert "closed,2026-10-07,BBB" in after
    assert after.count("2026-10-07") == 1
    assert "closed,2026-10-09,DDD" in after
    old_lines = pa._dated_roundtrip_lines(sealed)
    new_lines = pa._dated_roundtrip_lines(after)
    assert new_lines["2026-10-06"] == old_lines["2026-10-06"]
    assert new_lines["2026-10-07"] == old_lines["2026-10-07"]


def test_unpriced_resume_does_not_skip_ahead(tmp_path, monkeypatch):
    paper, books, blob = _layout(tmp_path)
    # 10-02 has no bar at all, and nothing on or before it. 10-06 must stay missing.
    idx = pd.to_datetime(["2026-10-06"])
    prices = pd.DataFrame({"SPY": [102.0], "AAA": [12.0]}, index=idx)
    _patch(monkeypatch, tmp_path, prices)
    # Drop the 10-01 book so the only sessions after the curve are 10-02 then 10-06.
    (books / "2026-10-01_stock_book.json").unlink()
    pa.run(date="2026-10-06", top_n=10, capital=10000)
    assert (paper / "equity_curve.csv").read_bytes() == blob


if __name__ == "__main__":
    import tempfile

    class _MP:
        def __init__(self):
            self._saved = []

        def setattr(self, obj, name, value, raising=True):
            exists = hasattr(obj, name)
            if not exists and not raising:
                return
            self._saved.append((obj, name, getattr(obj, name, None), exists))
            setattr(obj, name, value)

        def undo(self):
            for obj, name, old, existed in reversed(self._saved):
                if existed:
                    setattr(obj, name, old)
                elif hasattr(obj, name):
                    delattr(obj, name)
            self._saved.clear()

    def _run(fn):
        with tempfile.TemporaryDirectory() as d:
            mp = _MP()
            try:
                fn(Path(d), mp)
            finally:
                mp.undo()

    _run(test_resume_appends_later_books_and_keeps_printed_bytes)
    _run(test_missing_state_refuses_to_rewrite_the_curve)
    _run(test_nothing_eligible_still_renders_the_dashboard)
    test_no_publish_gate_still_renders_the_page()
    _run(test_render_only_does_not_append)
    _run(test_sealed_roundtrips_still_refresh_the_dashboard)
    _run(test_roundtrip_lock_does_not_write_sealed_rows)
    _run(test_unpriced_resume_does_not_skip_ahead)
    _run(test_book_committed_before_0930_is_appended)
    _run(test_late_book_stays_missing_and_the_next_session_appends)
    _run(test_unproven_commit_does_not_record_a_miss)
    _run(test_false_miss_clears_and_a_late_miss_stays)
    _run(test_open_session_waits_for_the_close)
    _run(test_open_cutoff_is_strict_and_reads_the_oldest_main_commit)
    print("paper append ok")
