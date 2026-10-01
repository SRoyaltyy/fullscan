"""Finviz description pack: retrieve, do not dump the universe."""
from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from src.news_impact.finviz_lookup import (
    attach_candidates,
    lookup_candidates,
    tokens_for,
)
from src.news_impact.schema import Entity


def _write_csv(path: Path) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(
            fh,
            fieldnames=["Ticker", "Company", "Sector", "Industry", "Description"],
        )
        w.writeheader()
        w.writerow({
            "Ticker": "CAR", "Company": "Avis Budget Group",
            "Sector": "Industrials", "Industry": "Rental & Leasing Services",
            "Description": "Rents cars and trucks to travelers at airports.",
        })
        w.writerow({
            "Ticker": "AAL", "Company": "American Airlines",
            "Sector": "Industrials", "Industry": "Airlines",
            "Description": "US passenger airline.",
        })
        w.writerow({
            "Ticker": "META", "Company": "Meta Platforms",
            "Sector": "Communication Services",
            "Industry": "Internet Content & Information",
            "Description": "Social network and ads. Not a car rental firm.",
        })
        w.writerow({
            "Ticker": "COIN", "Company": "Coinbase",
            "Sector": "Financial", "Industry": "Capital Markets",
            "Description": "Crypto exchange; tokenization venue candidate.",
        })


class FinvizLookupTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.csv = Path(self.tmp.name) / "finviz_fake.csv"
        _write_csv(self.csv)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_tsa_pack_includes_rental_and_airline(self) -> None:
        pack = lookup_candidates(
            "Government shutdown leads to chaos at US airports as TSA officers go unpaid",
            event_class="blast_ops",
            universe_path=str(self.csv),
        )
        ticks = {c["ticker"] for c in pack["candidates"]}
        self.assertIn("CAR", ticks)
        self.assertIn("AAL", ticks)
        self.assertNotIn("META", ticks)

    def test_tsv_pack_hits_capital_markets(self) -> None:
        pack = lookup_candidates(
            "SEC grants exemptive relief to tokenized securities venues",
            event_class="market_structure",
            universe_path=str(self.csv),
        )
        ticks = {c["ticker"] for c in pack["candidates"]}
        self.assertIn("COIN", ticks)

    def test_tokens_do_not_require_ticker_in_title(self) -> None:
        toks = tokens_for(
            "Chaos at US airports as TSA officers go unpaid",
            event_class="blast_ops",
        )
        self.assertTrue(any("rental" in t or "airline" in t for t in toks))

    def test_attach_only_when_family_named_nobody(self) -> None:
        pack = {"finviz_candidates": [
            {"ticker": "CAR", "name": "Avis Budget"},
        ]}
        empty = attach_candidates([], pack, "blast_ops")
        self.assertEqual(empty[0].ticker, "CAR")
        self.assertEqual(empty[0].direction, "not_determined")
        already = attach_candidates([
            Entity(name="American", ticker="AAL", role="named", direction="down"),
        ], pack, "blast_ops")
        self.assertEqual([e.ticker for e in already], ["AAL"])

    def test_missing_export_is_empty_not_crash(self) -> None:
        pack = lookup_candidates("anything", universe_path="/no/such.csv")
        self.assertEqual(pack["candidates"], [])


if __name__ == "__main__":
    unittest.main()
