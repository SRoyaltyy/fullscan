"""Jev unique-title backtest: Finviz book only, publication clock."""
from __future__ import annotations

import csv
import tempfile
from pathlib import Path

from src.jev_backtest import (
    book_to_entities,
    evaluate_articles,
    vote_hit_rates,
)
from src.news_impact.ledger import build_ledger


def _fixture_root() -> Path:
    tmp = Path(tempfile.mkdtemp())
    exports = tmp / "data" / "exports"
    exports.mkdir(parents=True)
    rows = [
        {
            "Ticker": "AMRX", "Company": "Amneal Pharmaceuticals Inc",
            "Sector": "Healthcare",
            "Industry": "Drug Manufacturers - Specialty & Generic",
            "Market Cap": "4000", "Daily Digest": "lanreotide",
            "Description": "", "Country": "USA", "News Title": "",
        },
        {
            "Ticker": "AAL", "Company": "American Airlines Group Inc",
            "Sector": "Industrials", "Industry": "Airlines",
            "Market Cap": "10000", "Daily Digest": "", "Description": "",
            "Country": "USA", "News Title": "",
        },
        {
            "Ticker": "CAR", "Company": "Avis Budget Group Inc",
            "Sector": "Industrials", "Industry": "Rental & Leasing Services",
            "Market Cap": "5000", "Daily Digest": "", "Description": "",
            "Country": "USA", "News Title": "",
        },
    ]
    path = exports / "finviz_2026-09-29.csv"
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    return tmp


def _reset_index() -> None:
    import src.news_impact.finviz_linker as linker
    linker._INDEX = None
    linker._INDEX_ROOT = None


def test_book_to_entities_drops_mixed_and_invented():
    ents = book_to_entities([
        {"ticker": "AMRX", "side": "up", "role": "named", "company": "Amneal"},
        {"ticker": "NVDA", "side": "mixed", "role": "named"},
        {"ticker": "", "side": "up", "role": "named"},
        {"ticker": "CAR", "side": "up", "role": "substitute"},
    ])
    assert {e["ticker"] for e in ents} == {"AMRX", "CAR"}
    by = {e["ticker"]: e for e in ents}
    assert by["AMRX"]["direction"] == "up"
    assert by["AMRX"]["tradeable_expression"] == "direct"
    assert by["CAR"]["tradeable_expression"] == "proxy"


def test_evaluate_assigns_finviz_sides_only():
    root = _fixture_root()
    _reset_index()
    try:
        rows = evaluate_articles(
            [
                {
                    "title": "FDA approves Amneal lanreotide injection",
                    "published_at": "2026-09-15T12:00:00-04:00",
                    "source": "reuters",
                },
                {
                    "title": "TSA unpaid officers leave airport checkpoints in chaos",
                    "published_at": "2026-09-16T08:00:00-04:00",
                    "source": "reuters",
                },
                {
                    "title": "A recap of yesterday's vibe",
                    "published_at": "2026-09-16T09:00:00-04:00",
                    "source": "fool.com",
                },
            ],
            root=root,
        )
        amrx = next(r for r in rows if "Amneal" in r["title"])
        assert amrx["decision"] == "keep"
        assert amrx["usable"]
        assert any(e["ticker"] == "AMRX" and e["direction"] == "up"
                   for e in amrx["entities"])
        assert "NVDA" not in {e["ticker"] for e in amrx["entities"]}
        tsa = next(r for r in rows if "TSA" in r["title"])
        ticks = {e["ticker"]: e["direction"] for e in tsa["entities"]}
        assert ticks.get("AAL") == "down"
        assert ticks.get("CAR") == "up"
        recap = next(r for r in rows if "vibe" in r["title"])
        assert recap["decision"] == "drop"
        assert recap["entities"] == []
    finally:
        _reset_index()


def test_xy_ledger_and_publication_clock():
    rows = [
        {
            "title": "FDA approves Amneal lanreotide injection",
            "published_at": "2026-09-15T12:00:00-04:00",
            "classification": {
                "event_class": "gate", "q5": "impulse", "sign": "open",
            },
            "entities": [{
                "ticker": "AMRX", "direction": "up", "role": "named",
                "tradeable_expression": "direct",
            }],
            "usable": True,
            "performance": [{
                "ticker": "AMRX", "entry_date": "2026-09-16",
                "ret_1d": 2.0, "ret_2d": 3.0, "ret_5d": 4.0,
            }],
        },
        {
            "title": "Amneal lanreotide ships to clinics",
            "published_at": "2026-09-15T15:00:00-04:00",
            "classification": {
                "event_class": "gate", "q5": "impulse", "sign": "open",
            },
            "entities": [{
                "ticker": "AMRX", "direction": "up", "role": "named",
                "tradeable_expression": "direct",
            }],
            "usable": True,
            "performance": [{
                "ticker": "AMRX", "entry_date": "2026-09-16",
                "ret_1d": 2.0, "ret_2d": 3.0, "ret_5d": 4.0,
            }],
        },
    ]
    votes = vote_hit_rates(rows)
    assert votes["named"]["0-1d"]["hits"] == 2
    assert votes["named"]["0-1d"]["n"] == 2
    ledger = build_ledger(rows)
    assert ledger["counts"]["converge"] == 1
    hit = ledger["top_converge"][0]
    assert hit["ticker"] == "AMRX"
    assert hit["n_bull"] == 2
    assert hit["n_bear"] == 0
    assert hit["date"] == "2026-09-16"


def test_no_keep_json_and_no_lane():
    root = Path(__file__).resolve().parent.parent
    assert not (root / "keep.json").exists()
    text = (root / "src" / "jev_backtest.py").read_text(encoding="utf-8")
    assert "from .news_impact.families" not in text
    assert "from .news_impact.pipeline" not in text
    assert "keep.json" in text


def main() -> None:
    tests = [
        test_book_to_entities_drops_mixed_and_invented,
        test_evaluate_assigns_finviz_sides_only,
        test_xy_ledger_and_publication_clock,
        test_no_keep_json_and_no_lane,
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
