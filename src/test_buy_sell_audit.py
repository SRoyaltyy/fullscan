"""Buy/sell fingerprint audit: a renamed past buy fails closed."""
from __future__ import annotations

import json
from pathlib import Path

from src.buy_sell_audit import (
    SKIP_THEME,
    collect,
    diff,
    fingerprint,
    load_baselines,
    seed,
)
from src.past_day_lock import sha256_text

DAY = "2030-01-02"


def _ticket(ticker: str) -> str:
    return json.dumps({
        "strategies": {
            "fm_a": {
                "family": "factor_mine",
                "status": "ok",
                "buy": [{"ticker": ticker, "shares": 3}],
                "sell": [],
                "note": "sit note must not enter the fingerprint",
            }
        }
    })


def _root(tmp: Path, ticker: str = "AAA") -> Path:
    board = tmp / "data" / "day_board"
    board.mkdir(parents=True)
    text = _ticket(ticker)
    (board / f"{DAY}_strategy_tickets.json").write_text(text, encoding="utf-8")
    lock = tmp / "data" / "past_day_lock"
    lock.mkdir(parents=True)
    row = {
        "date": DAY,
        "kind": "day",
        "name": "",
        "record": "strategy_tickets",
        "sha256": sha256_text(text),
    }
    (lock / "manifest.jsonl").write_text(json.dumps(row) + "\n", encoding="utf-8")
    return tmp


def test_fingerprint_ignores_notes_and_catches_a_rename() -> None:
    same = fingerprint(DAY, ["AAA 3"], [])
    assert same == fingerprint(DAY, ["AAA 3"], [])
    assert same != fingerprint(DAY, ["BBB 3"], [])
    assert "theme_radar" in SKIP_THEME


def test_renamed_buy_fails_and_baseline_is_not_rewritten(tmp: Path) -> None:
    root = _root(tmp, "AAA")
    seals, notes = collect(root)
    assert any(note.startswith("SKIP theme_radar") for note in notes)
    fm = [seal for seal in seals if seal.name == "fm_a"]
    assert len(fm) == 1 and fm[0].date == DAY
    path = tmp / "data" / "buy_sell_audit" / "baselines.jsonl"
    seeded = seed(path, seals)
    assert any("fm_a" in line for line in seeded)
    first = path.read_text(encoding="utf-8")
    assert seed(path, seals) == []
    assert path.read_text(encoding="utf-8") == first

    moved = _ticket("BBB")
    (root / "data" / "day_board" / f"{DAY}_strategy_tickets.json").write_text(
        moved, encoding="utf-8")
    # The whole-file lock moved, and so did the buy name.
    again, notes = collect(root)
    assert any("strategy_tickets" in note and "moved" in note for note in notes)
    lock = root / "data" / "past_day_lock" / "manifest.jsonl"
    row = json.loads(lock.read_text(encoding="utf-8"))
    row["sha256"] = sha256_text(moved)
    lock.write_text(json.dumps(row) + "\n", encoding="utf-8")
    again, _notes = collect(root)
    drifts, news = diff(again, load_baselines(path))
    assert any("fm_a" in line and DAY in line and "moved" in line for line in drifts)
    assert news == []
    try:
        seed(path, again)
    except SystemExit as exc:
        assert "refusing to rewrite" in str(exc)
    else:
        raise AssertionError("seed rewrote a first seal")
    assert path.read_text(encoding="utf-8") == first


def test_gap_is_not_backfilled(tmp: Path) -> None:
    root = _root(tmp)
    seals, _notes = collect(root)
    assert all(seal.date == DAY for seal in seals)
    assert not (root / "data" / "day_board" / "2030-01-03_strategy_tickets.json").exists()


def main() -> None:
    import tempfile
    tmp = Path(tempfile.mkdtemp())
    test_fingerprint_ignores_notes_and_catches_a_rename()
    test_renamed_buy_fails_and_baseline_is_not_rewritten(tmp / "rename")
    test_gap_is_not_backfilled(tmp / "gap")
    print("ok buy-sell audit")


if __name__ == "__main__":
    main()
