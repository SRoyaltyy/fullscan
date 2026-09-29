"""Hop-2 book: Finviz lookup only, no invented tickers."""
from __future__ import annotations

import csv
import json
import tempfile
from pathlib import Path

from src.jev_book import (
    BOOK_LIMIT,
    FORBIDDEN_HOP2_BITS,
    QUESTIONS,
    SIDES,
    apply_code_book,
    apply_jev_book,
    book_reason,
    classify_book,
    decide_book,
    default_sides,
    lookup_candidates,
    questions_are_hop2,
)
from src.jev_classify import QUESTIONS as HOP1_QUESTIONS
from src.jev_gate import QUESTIONS as HOP0_QUESTIONS

FIXTURE_ROWS = [
    {
        "Ticker": "AAL", "Company": "American Airlines Group Inc",
        "Sector": "Industrials", "Industry": "Airlines",
        "Market Cap": "10000", "Daily Digest": "", "Description": "",
        "Country": "USA", "News Title": "",
    },
    {
        "Ticker": "DAL", "Company": "Delta Air Lines Inc",
        "Sector": "Industrials", "Industry": "Airlines",
        "Market Cap": "30000", "Daily Digest": "", "Description": "",
        "Country": "USA", "News Title": "",
    },
    {
        "Ticker": "CAR", "Company": "Avis Budget Group Inc",
        "Sector": "Industrials", "Industry": "Rental & Leasing Services",
        "Market Cap": "5000", "Daily Digest": "", "Description": "",
        "Country": "USA", "News Title": "",
    },
    {
        "Ticker": "HTZ", "Company": "Hertz Global Holdings Inc",
        "Sector": "Industrials", "Industry": "Rental & Leasing Services",
        "Market Cap": "2000", "Daily Digest": "", "Description": "",
        "Country": "USA", "News Title": "",
    },
    {
        "Ticker": "AMRX", "Company": "Amneal Pharmaceuticals Inc",
        "Sector": "Healthcare", "Industry": "Drug Manufacturers - Specialty & Generic",
        "Market Cap": "4000", "Daily Digest": "", "Description": "",
        "Country": "USA", "News Title": "",
    },
    {
        "Ticker": "XOM", "Company": "Exxon Mobil Corporation",
        "Sector": "Energy", "Industry": "Oil & Gas Integrated",
        "Market Cap": "400000", "Daily Digest": "", "Description": "",
        "Country": "USA", "News Title": "",
    },
]


def _fixture_root() -> Path:
    tmp = Path(tempfile.mkdtemp())
    exports = tmp / "data" / "exports"
    exports.mkdir(parents=True)
    path = exports / "finviz_2026-09-29.csv"
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(FIXTURE_ROWS[0]))
        writer.writeheader()
        writer.writerows(FIXTURE_ROWS)
    return tmp


def _reset_index() -> None:
    import src.news_impact.finviz_linker as linker
    linker._INDEX = None
    linker._INDEX_ROOT = None


def test_hop_packs_stay_separated():
    questions_are_hop2()
    blob = json.dumps(QUESTIONS).lower()
    for bit in FORBIDDEN_HOP2_BITS:
        assert bit not in blob
    assert "named_side" not in HOP0_QUESTIONS
    assert "named_side" not in HOP1_QUESTIONS
    assert "q5" not in QUESTIONS
    assert set(QUESTIONS["named_side"]["criteria"]) == set(SIDES)


def test_weather_and_discard_have_empty_book():
    assert default_sides("time", "discard", None, "regime") is None
    assert default_sides("time", "regime_state", None, "regime") is None
    assert decide_book(
        "Strait of Hormuz remains closed",
        family="time", event_class="regime_state", q5="regime",
    ) == []


def test_lookup_uses_title_words_and_industry(tmp_ok=True):
    root = _fixture_root()
    _reset_index()
    try:
        cands = lookup_candidates(
            "TSA unpaid officers leave airport checkpoints in chaos",
            family="blast", event_class="blast_ops", root=root,
        )
        ticks = {row["ticker"] for row in cands}
        assert ticks & {"AAL", "DAL"}
        assert "CAR" in ticks
        assert "XOM" not in ticks
        assert len(cands) <= BOOK_LIMIT
    finally:
        _reset_index()


def test_blast_sides_named_down_peers_up():
    root = _fixture_root()
    _reset_index()
    try:
        book = decide_book(
            "TSA unpaid officers leave airport checkpoints in chaos",
            family="blast", event_class="blast_ops", q5="impulse",
            root=root,
        )
        by = {row["ticker"]: row for row in book}
        assert by["AAL"]["side"] == "down"
        assert by["AAL"]["role"] == "named"
        assert by["CAR"]["side"] == "up"
        assert by["CAR"]["role"] == "substitute"
        assert "XOM" not in by
        assert "↑" in book_reason(book) and "↓" in book_reason(book)
    finally:
        _reset_index()


def test_jev_can_drop_peers_and_cannot_add_a_name():
    root = _fixture_root()
    _reset_index()
    try:
        book = decide_book(
            "FDA approves Amneal lanreotide injection",
            family="permission", event_class="gate", sign="open", q5="impulse",
            answers={
                "named_side": "up",
                "peer_side": "down",
                "attach_peers": 0.1,
            },
            root=root,
        )
        ticks = {row["ticker"] for row in book}
        assert ticks == {"AMRX"}
        assert book[0]["side"] == "up"
        invented = decide_book(
            "FDA approves Amneal lanreotide injection",
            family="permission", event_class="gate", sign="open", q5="impulse",
            answers={"named_side": "up", "peer_side": "up", "attach_peers": 0.9},
            root=root,
        )
        assert "NVDA" not in {row["ticker"] for row in invented}
        assert "AMRX" in {row["ticker"] for row in invented}
    finally:
        _reset_index()


def test_apply_code_book_stamps_keeps_only():
    root = _fixture_root()
    _reset_index()
    try:
        rows = [
            {
                "title": "FDA approves Amneal lanreotide injection",
                "decision": "keep",
                "event_class": "gate",
                "q5": "impulse",
                "sign": "open",
                "family": "permission",
            },
            {
                "title": "A recap of yesterday",
                "decision": "drop",
                "reason": "opinion",
            },
        ]
        apply_code_book(rows, root=root)
        assert any(row["ticker"] == "AMRX" for row in rows[0]["book"])
        assert rows[0]["book_reason"]
        assert rows[1]["book"] == []
    finally:
        _reset_index()


def test_apply_jev_book_only_asks_on_classified_keeps():
    root = _fixture_root()
    _reset_index()
    calls = []

    def poster(state, questions, key):
        calls.append((state, questions))
        return {
            "model": "jev-test",
            "answers": {
                "named_side": {"type": "choice", "choice": "up"},
                "peer_side": {"type": "choice", "choice": "out"},
                "attach_peers": {"type": "noul", "noul": 0.1},
            },
        }

    try:
        rows = [
            {
                "title": "FDA approves Amneal lanreotide injection",
                "decision": "keep",
                "event_class": "gate",
                "q5": "impulse",
                "sign": "open",
                "family": "permission",
            },
            {"title": "trash", "decision": "drop", "reason": "opinion"},
        ]
        apply_jev_book(rows, key="x", workers=1, poster=poster, root=root)
        assert len(calls) == 1
        state, questions = calls[0]
        assert "AMRX" in state
        assert "named_side" in questions
        assert "q5" not in questions
        assert "is_opinion" not in questions
        assert rows[0]["book_source"] == "jev"
        assert {row["ticker"] for row in rows[0]["book"]} == {"AMRX"}
        assert not rows[1].get("book")
    finally:
        _reset_index()


def test_classify_book_ignores_unclassified_rows():
    assert classify_book({"title": "x", "decision": "keep"})["book"] == []


def test_no_keep_json():
    root = Path(__file__).resolve().parent.parent
    assert not (root / "keep.json").exists()


def main() -> None:
    tests = [
        test_hop_packs_stay_separated,
        test_weather_and_discard_have_empty_book,
        test_lookup_uses_title_words_and_industry,
        test_blast_sides_named_down_peers_up,
        test_jev_can_drop_peers_and_cannot_add_a_name,
        test_apply_code_book_stamps_keeps_only,
        test_apply_jev_book_only_asks_on_classified_keeps,
        test_classify_book_ignores_unclassified_rows,
        test_no_keep_json,
    ]
    failed = 0
    for fn in tests:
        try:
            fn()
            print("ok", fn.__name__)
        except Exception as exc:
            failed += 1
            print("FAIL", fn.__name__, type(exc).__name__, exc)
    if failed:
        raise SystemExit(failed)


if __name__ == "__main__":
    main()
