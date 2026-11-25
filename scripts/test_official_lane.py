"""Usefulness gate and vertical ticker link. No network, no model call."""
import unittest
from pathlib import Path

from src.news_impact.material_gate import gate
from src.news_impact.ticker_verticals import link_title, load_book

BOOK = Path("data/exports/finviz_2026-10-15.csv")


class GateTests(unittest.TestCase):
    def test_referral_and_meeting_are_not_useful(self):
        self.assertFalse(gate({"title": "Read twice and referred to the Committee on Finance."})["keep"])
        self.assertFalse(gate({"title": "Agency Information Collection Activities; Comment Request"})["keep"])
        self.assertFalse(gate({"title": "Remarks on Signing the GENIUS Act"})["keep"])
        self.assertFalse(gate({"title": "H.R. 10821 (IH) - Deposit Insurance Act"})["keep"])
        self.assertFalse(gate({"title": "Supporting the designation of National Chemistry Week"})["keep"])
        self.assertFalse(gate({"title": "Revision of Departmental Component Designations"})["keep"])

    def test_law_sanction_and_presentment_are_useful(self):
        self.assertTrue(gate({"title": "Became Public Law No: 119-123.", "document_stage": "law_published"})["keep"])
        self.assertTrue(gate({"title": "Iran-related Designations; Issuance of General Licenses"})["keep"])
        self.assertTrue(gate({"title": "H.R. 7831 presented to president. License to Drill Act"})["keep"])
        self.assertFalse(gate({
            "title": "Safety Zone; Atlantic Ocean, Culebra, Puerto Rico",
            "document_stage": "final_rule_published",
        })["keep"])


class VerticalTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.book = load_book(BOOK)

    def test_book_loads(self):
        self.assertGreater(len(self.book), 1000)

    def test_oil_producers_include_exxon_and_exclude_refiners(self):
        linked = link_title("OPEC cut leaves oil producers bullish", self.book)
        tickers = {row["ticker"] for row in linked["tickers"]}
        roles = {row["role"] for row in linked["tickers"]}
        self.assertIn("XOM", tickers)
        self.assertIn("COP", tickers)
        self.assertIn("producer", roles)
        self.assertIn("integrated", roles)
        self.assertNotIn("VLO", tickers)
        self.assertNotIn("EPD", tickers)

    def test_drill_license_is_drillers_and_producers_not_refiners(self):
        linked = link_title("License to Drill Act presented to president", self.book)
        tickers = {row["ticker"] for row in linked["tickers"]}
        self.assertIn("NE", tickers)
        self.assertIn("XOM", tickers)
        self.assertNotIn("VLO", tickers)

    def test_refiners_are_not_exxon(self):
        linked = link_title("Refinery margins jump for US refiners", self.book)
        tickers = [row["ticker"] for row in linked["tickers"]]
        self.assertIn("VLO", tickers)
        self.assertNotIn("XOM", tickers)

    def test_named_bank_beats_a_sector_guess(self):
        linked = link_title(
            "OCC Imposes $750 Million Assessment Against Bank of America",
            self.book,
        )
        self.assertEqual(linked["tickers"][0]["ticker"], "BAC")
        self.assertEqual(linked["tickers"][0]["basis"], "company_name_in_title")

    def test_sanction_without_a_vertical_does_not_become_exxon(self):
        linked = link_title(
            "Implementing the Lindsey O. Graham Sanctioning Russia and Iran Act of 2026",
            self.book,
        )
        self.assertEqual(linked["tickers"], [])

    def test_a_country_name_is_not_a_company(self):
        linked = link_title(
            "Wood Mouldings From the People's Republic of China",
            self.book,
        )
        self.assertEqual(linked["tickers"], [])

    def test_pulp_duty_links_paper_not_energy(self):
        linked = link_title(
            "High Purity Dissolving Pulp From Brazil: Final Affirmative Countervailing Duty Determination",
            self.book,
        )
        self.assertTrue(linked["tickers"])
        self.assertTrue(all(row["industry"] == "Paper & Paper Products" for row in linked["tickers"]))
        self.assertNotIn("XOM", {row["ticker"] for row in linked["tickers"]})


if __name__ == "__main__":
    unittest.main()
