"""Close-mark notes are add-only. A Yahoo gap warns and does not rewrite the seal."""
from __future__ import annotations

import inspect
import json
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.hot_n4_clean_v4.forward.book import H1  # noqa: E402
from research.hot_n4_clean_v4.forward.forward import fill_main  # noqa: E402
from research.hot_n4_clean_v4.forward.mark_notes import (  # noqa: E402
    OFF_FRACTION,
    WARNING_KIND,
    append_note,
    assert_notes_append_only,
    equity_text,
    guard_close_mark,
    implied_equity,
    is_off,
    load_notes,
)

H1_LOG = H1.folder / "h1_log.jsonl"
NOTES = H1.folder / "mark_notes.jsonl"
PAGE_NOTES = H1.page / "mark_notes.json"
MANIFEST = ROOT / "data" / "past_day_lock" / "manifest.jsonl"
H1_PAGE = ROOT / "dashboard" / "h1" / "index.html"
HOLDUP_PAGE = ROOT / "dashboard" / "holdup" / "index.html"
LOCK_SHA = "db9cc5b02174515dcb0a373f4899bf591aec6fea5e58696c324ea41bd3835694"


def _git(path: Path) -> bytes:
    rel = path.relative_to(ROOT).as_posix()
    proc = subprocess.run(
        ["git", "show", f"origin/main:{rel}"],
        cwd=ROOT,
        capture_output=True,
        check=True,
    )
    return proc.stdout


def _mark(equity: float, holdings: list[dict], day: str = "2026-10-07") -> dict:
    return {
        "cash_primary": 3.0,
        "date": day,
        "equity_primary": equity,
        "holdings": holdings,
        "kind": "mark",
    }


def _bars(ticker: str, day: str, close: float, open_px: float | None = None) -> dict:
    return {
        "stored": {
            ticker: {
                "close": [close],
                "date": [day],
                "open": [close if open_px is None else open_px],
            },
        },
    }


def _yahoo(closes: dict[str, float], day: str = "2026-10-07"):
    def fetch(tickers, start, end):
        return {
            "bars": [
                {"close": closes[ticker], "date": day, "ticker": ticker}
                for ticker in tickers
                if ticker in closes
            ],
            "error": None,
            "missing": [],
            "splits": [],
        }

    return fetch


def _guard_off_does_not_rewrite() -> None:
    mark = _mark(1003.0, [{
        "entry_px": 10.0,
        "last_px": 10.0,
        "shares": 100,
        "ticker": "AAA",
    }])
    original = json.dumps(mark, sort_keys=True)
    with tempfile.TemporaryDirectory() as tmp:
        folder = Path(tmp) / "forward_h1"
        page = Path(tmp) / "dash"
        notes = guard_close_mark(
            mark, _bars("AAA", "2026-10-07", 10.0),
            fetch=_yahoo({"AAA": 3.0}), folder=folder, page=page, log=lambda *_a, **_k: None,
        )
        if json.dumps(mark, sort_keys=True) != original:
            raise SystemExit("guard rewrote the sealed mark")
        if len(notes) != 1 or notes[0]["kind"] != WARNING_KIND:
            raise SystemExit(f"warning {notes}")
        if notes[0]["sealed_equity"] != equity_text(1003.0):
            raise SystemExit("sealed equity was replaced")
        if "unchanged" not in notes[0]["note"] or "3" not in notes[0]["note"]:
            raise SystemExit(notes[0]["note"])
        saved = load_notes(folder / "mark_notes.jsonl")
        if saved != notes:
            raise SystemExit("note file is not the warning")
        rendered = json.loads((page / "mark_notes.json").read_text(encoding="utf-8"))
        if rendered != notes:
            raise SystemExit("dashboard copy is not the note")
        again = guard_close_mark(
            mark, _bars("AAA", "2026-10-07", 10.0),
            fetch=_yahoo({"AAA": 3.0}), folder=folder, page=page, log=lambda *_a, **_k: None,
        )
        lines = (folder / "mark_notes.jsonl").read_text(encoding="utf-8").splitlines()
        if lines[0] != json.dumps(notes[0], sort_keys=True, separators=(",", ":")):
            raise SystemExit("a second warning rewrote the first line")
        if len(again) != 1 or len(lines) != 2:
            raise SystemExit("a second warning did not append")


def _within_fifty_is_quiet() -> None:
    # 15 vs 10 is exactly 50%. The guard flags only a wider gap.
    if is_off(15.0, 10.0) or not is_off(15.01, 10.0):
        raise SystemExit(f"50% boundary {OFF_FRACTION}")
    if is_off(3.94, 3.94):
        raise SystemExit("a matching close was flagged")
    mark = _mark(13.0, [{
        "entry_px": 2.99,
        "last_px": 2.99,
        "shares": 1,
        "ticker": "PACB",
    }])
    with tempfile.TemporaryDirectory() as tmp:
        folder = Path(tmp) / "forward_h1"
        notes = guard_close_mark(
            mark, _bars("PACB", "2026-10-07", 2.7, 2.99),
            fetch=_yahoo({"PACB": 2.7}), folder=folder, page=None, log=lambda *_a, **_k: None,
        )
        if notes or (folder / "mark_notes.jsonl").exists():
            raise SystemExit("a close within 50% of Yahoo wrote a note")


def _holding_price_can_flag_when_the_close_matches() -> None:
    mark = _mark(394.0, [{
        "entry_px": 2.7,
        "last_px": 9.71,
        "shares": 100,
        "ticker": "SDEV",
    }])
    original = json.dumps(mark, sort_keys=True)
    with tempfile.TemporaryDirectory() as tmp:
        folder = Path(tmp) / "forward_h1"
        notes = guard_close_mark(
            mark, _bars("SDEV", "2026-10-07", 3.94, 9.71),
            fetch=_yahoo({"SDEV": 3.94}), folder=folder, page=None, log=lambda *_a, **_k: None,
        )
        if json.dumps(mark, sort_keys=True) != original:
            raise SystemExit("holding-price warning rewrote the mark")
        if len(notes) != 1:
            raise SystemExit("holding price 9.71 vs Yahoo 3.94 was not flagged")
        text = notes[0]["note"]
        if "9.71" not in text or "3.94" not in text:
            raise SystemExit(text)
        if notes[0]["tickers"][0]["valuation_close"] != 3.94:
            raise SystemExit("valuation close was not kept")


def _yahoo_failure_does_not_block() -> None:
    mark = _mark(10.0, [{"entry_px": 1.0, "last_px": 1.0, "shares": 1, "ticker": "AAA"}])

    def fetch(tickers, start, end):
        raise RuntimeError("yahoo down")

    with tempfile.TemporaryDirectory() as tmp:
        folder = Path(tmp) / "forward_h1"
        notes = guard_close_mark(
            mark, _bars("AAA", "2026-10-07", 1.0),
            fetch=fetch, folder=folder, page=None,
        )
        if notes or (folder / "mark_notes.jsonl").exists():
            raise SystemExit("a failed Yahoo compare wrote a note or raised")

    def boom(*_a, **_k):
        raise RuntimeError("note file refused")

    import research.hot_n4_clean_v4.forward.mark_notes as notes_mod
    saved = notes_mod.append_note
    notes_mod.append_note = boom
    try:
        out = guard_close_mark(
            mark, _bars("AAA", "2026-10-07", 10.0),
            fetch=_yahoo({"AAA": 1.0}), folder=Path(tempfile.mkdtemp()) / "forward_h1",
            page=None, log=lambda *_a, **_k: None,
        )
    finally:
        notes_mod.append_note = saved
    if out:
        raise SystemExit("a note-file error escaped the guard")


def _append_only() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "mark_notes.jsonl"
        append_note({"date": "2026-10-05", "note": "first"}, path)
        prior = path.read_text(encoding="utf-8")
        append_note({"date": "2026-10-06", "note": "second"}, path)
        assert_notes_append_only(prior, path.read_text(encoding="utf-8"))
        try:
            assert_notes_append_only(prior, prior.replace("first", "edited"))
        except ValueError as exc:
            if "add-only" not in str(exc):
                raise SystemExit(exc) from exc
        else:
            raise SystemExit("an edited note was accepted")


def _sealed_records_match_main() -> None:
    for path in (H1_LOG, MANIFEST):
        if path.read_bytes() != _git(path):
            raise SystemExit(f"sealed file changed {path.relative_to(ROOT)}")
    rows = [json.loads(line) for line in H1_LOG.read_text(encoding="utf-8").splitlines() if line.strip()]
    mark = next(row for row in rows if row["kind"] == "mark" and row["date"] == "2026-10-05")
    sdev = next(lot for lot in mark["holdings"] if lot["ticker"] == "SDEV")
    if sdev["last_px"] != 9.71 or mark["equity_primary"] != 13967.076193643774:
        raise SystemExit("2026-10-05 sealed mark moved")
    later = next(row for row in rows if row["kind"] == "mark" and row["date"] == "2026-10-06")
    held = {lot["ticker"]: lot["last_px"] for lot in later["holdings"]}
    if held != {"DNA": 15.08, "PACB": 2.99, "QSI": 1.29, "SDEV": 3.48}:
        raise SystemExit(f"2026-10-06 holding prices moved {held}")
    if later["equity_primary"] != 11869.509843643777:
        raise SystemExit("2026-10-06 sealed equity moved")
    locks = [
        json.loads(line) for line in MANIFEST.read_text(encoding="utf-8").splitlines() if line.strip()
    ]
    h1_locks = [row for row in locks if row.get("name") == "h1_webull_sim"]
    if [row.get("date") for row in h1_locks] != ["2026-10-06"]:
        raise SystemExit(f"h1_webull_sim lock dates changed {h1_locks}")
    if h1_locks[0].get("sha256") != LOCK_SHA:
        raise SystemExit("h1_webull_sim 2026-10-06 lock hash changed")


def _historical_notes() -> None:
    rows = load_notes(NOTES)
    if len(rows) != 3:
        raise SystemExit(f"historical notes {len(rows)}")
    rendered = json.loads(PAGE_NOTES.read_text(encoding="utf-8"))
    if rendered != rows:
        raise SystemExit("h1 dashboard notes are not the log")
    by_kind = {}
    for row in rows:
        by_kind.setdefault(row["kind"], []).append(row)
    sdev = by_kind["correction"][0]
    if sdev["date"] != "2026-10-05" or sdev["book"] != "h1":
        raise SystemExit(f"sdev note {sdev}")
    ticker = sdev["tickers"][0]
    if ticker != {"sealed_mark": 9.71, "ticker": "SDEV", "yahoo_close": 3.94}:
        raise SystemExit(f"sdev yahoo {ticker}")
    if "3.94" not in sdev["note"] or "13967.076193643774" not in sdev["note"]:
        raise SystemExit(sdev["note"])
    fresh = next(row for row in by_kind["correction"] if row["date"] == "2026-10-06")
    got = {row["ticker"]: row["yahoo_close"] for row in fresh["tickers"]}
    if got != {"PACB": 2.7, "DNA": 12.41, "QSI": 1.22}:
        raise SystemExit(f"10-06 yahoo {got}")
    sealed_marks = {row["ticker"]: row["sealed_mark"] for row in fresh["tickers"]}
    if sealed_marks != {"PACB": 2.99, "DNA": 15.08, "QSI": 1.29}:
        raise SystemExit(f"10-06 sealed marks {sealed_marks}")
    if fresh["implied_equity"] != "11869.509843643777" or fresh["sealed_equity"] != "11869.509843643777":
        raise SystemExit(f"10-06 equity {fresh['implied_equity']} {fresh['sealed_equity']}")
    if sdev["implied_equity"] != "13967.076193643774" or sdev["sealed_equity"] != "13967.076193643774":
        raise SystemExit(f"10-05 equity {sdev['implied_equity']} {sdev['sealed_equity']}")
    gap = by_kind["lock_gap"][0]
    if gap["date"] != "2026-10-05" or "2026-10-06" not in gap["dates"]:
        raise SystemExit(f"gap note {gap}")
    if "not backfilled" not in gap["note"] or "h1_webull_sim" not in gap["note"]:
        raise SystemExit(gap["note"])
    marks = {
        row["date"]: row
        for row in (json.loads(line) for line in H1_LOG.read_text(encoding="utf-8").splitlines() if line.strip())
        if row.get("kind") == "mark" and row.get("date") in ("2026-10-05", "2026-10-06")
    }
    for note in (sdev, fresh):
        closes = {}
        for lot in marks[note["date"]]["holdings"]:
            hit = next((row for row in note["tickers"] if row["ticker"] == lot["ticker"]), None)
            closes[lot["ticker"]] = float(hit["yahoo_close"]) if hit else None
        # Names the note does not correct stay at the close already inside the sealed equity.
        # Recompute from the note's Yahoo closes plus the sealed cash path checked above.
        if note["implied_equity"] != note["sealed_equity"]:
            raise SystemExit("implied equity disagrees with the sealed equity")
        yahoo_only = {row["ticker"]: row["yahoo_close"] for row in note["tickers"]}
        if note["date"] == "2026-10-05":
            yahoo_only.update({"FEAM": 3.92, "GLND": 3.71, "NAUT": 1.87})
        else:
            yahoo_only["SDEV"] = 3.25
        implied = implied_equity(marks[note["date"]], yahoo_only)
        if equity_text(implied) != note["implied_equity"]:
            raise SystemExit(f"recomputed implied {implied} {note['implied_equity']}")
        _ = closes


def _pages() -> None:
    for path in (H1_PAGE, HOLDUP_PAGE):
        text = path.read_text(encoding="utf-8")
        if "mark_notes.json" not in text or 'class="warn"' not in text or "dayNotesHtml" not in text:
            raise SystemExit(f"{path.name} does not show close-mark notes")
    h1 = H1_PAGE.read_text(encoding="utf-8")
    if "last_px" in h1:
        raise SystemExit("h1 page displays last_px")
    src = inspect.getsource(fill_main)
    if src.find("append_records(bodies)") > src.find("warn_sealed_close"):
        raise SystemExit("the guard runs before the seal")


def main() -> None:
    _guard_off_does_not_rewrite()
    _within_fifty_is_quiet()
    _holding_price_can_flag_when_the_close_matches()
    _yahoo_failure_does_not_block()
    _append_only()
    _sealed_records_match_main()
    _historical_notes()
    _pages()
    print("close-mark guard ok")


if __name__ == "__main__":
    main()
