"""The memo's small circles must pass Rocket Lab and fail the airline and the office REIT."""
import unittest

from collections import Counter

from src.correlation import _apply_keyness, build_board, link_articles, prepare, stock_terms
from src.filing_business import item1_business
from src.filing_parts import divide_profile

MEMO = """
THE NATIONAL SPACE TRANSPORTATION POLICY

American space transportation capabilities underpin the global economy, and commercial space innovation flourishes with affordable, reliable, and safe space transportation.

By 2030, our space transportation ranges must grow to support more than 1,000 launches and reentries every year.

Sec. 2. Space Launch and Reentry Infrastructure
The Secretary of War and the Administrator of the National Aeronautics and Space Administration shall operate Federal launch and reentry ranges.
The Secretary of Transportation shall designate priority airspace for critical space launch corridors.
Payloads, orbit, and spacecraft remain the subject of this space transportation policy.
The Secretary of Transportation shall report on spectrum for space launch.

Sec. 9. General Provisions
Nothing in this memorandum shall be construed to impair any authority. The Secretary of War is authorized to publish this memorandum.
"""

STOCKS = [
    {"ticker": "RKLB", "name": "Rocket Lab Corp.", "industry": "Aerospace & Defense", "sector": "Industrials",
     "description": "Rocket Lab develops rocket launch and control systems for the space and defense industries. Launch services fly dedicated missions. Space systems include spacecraft engineering, spacecraft components, spacecraft manufacturing, and on-orbit mission operations."},
    {"ticker": "LMT", "name": "Lockheed Martin Corp.", "industry": "Aerospace & Defense", "sector": "Industrials",
     "description": "Lockheed Martin builds military aircraft, missiles, and defense systems. The space segment supports satellites and mission systems for government customers."},
    {"ticker": "AAL", "name": "American Airlines Group Inc.", "industry": "Airlines", "sector": "Industrials",
     "description": "American Airlines offers air transportation for passengers and cargo across a network of domestic and international routes."},
    {"ticker": "UNP", "name": "Union Pacific Corp.", "industry": "Railroads", "sector": "Industrials",
     "description": "Union Pacific operates a railroad freight transportation network hauling industrial goods across the western United States."},
    {"ticker": "ARE", "name": "Alexandria Real Estate Equities Inc.", "industry": "REIT - Office", "sector": "Real Estate",
     "description": "Alexandria provides office space for lease to life science tenants, including laboratory buildings measured in square footage."},
    {"ticker": "JPM", "name": "JPMorgan Chase & Co.", "industry": "Banks - Diversified", "sector": "Financial",
     "description": "JPMorgan Chase takes deposits, makes loans, and operates consumer banking and investment banking businesses."},
]


class CorrelationTests(unittest.TestCase):
    def test_bundle_absorbs_transportation_and_drops_the_title(self):
        doc = prepare(MEMO)
        self.assertIn("space transportation", doc.bundles)
        self.assertIn("transportation", doc.absorbed)
        self.assertNotIn("transportation", doc.terms)
        self.assertNotIn("secretary", doc.terms)
        self.assertIn("launch", doc.terms)
        self.assertTrue("reentry" in doc.terms or "reentr" in doc.terms)

    def test_title_alone_does_not_leave_a_transportation_circle(self):
        doc = prepare("The Secretary of Transportation shall publish a report to Congress.")
        self.assertNotIn("transportation", doc.terms)
        self.assertNotIn("secretary", doc.terms)

    def test_memo_passes_rocket_lab_and_fails_the_lookalikes(self):
        board = build_board(STOCKS)
        [hit] = link_articles(board, [{"id": "memo", "title": "space policy", "text": MEMO}], z_min=1.5)
        self.assertEqual(hit["status"], "scored")
        passed = {row["ticker"] for row in hit["stocks"] if row["pass"]}
        self.assertIn("RKLB", passed)
        self.assertNotIn("AAL", passed)
        self.assertNotIn("ARE", passed)
        self.assertNotIn("JPM", passed)
        ranked = [row["ticker"] for row in hit["stocks"]]
        self.assertEqual(ranked[0], "RKLB")
        rklb = next(row for row in hit["stocks"] if row["ticker"] == "RKLB")
        self.assertGreater(rklb["bm25"], 0)
        self.assertFalse(rklb["name_hit"])

    def test_a_name_drop_is_not_a_correlation(self):
        board = build_board(STOCKS)
        [hit] = link_articles(board, [{
            "id": "name",
            "text": "Rocket Lab announced that Peter Beck will host a webcast for investors on Tuesday.",
        }])
        passed = {row["ticker"] for row in hit["stocks"] if row.get("pass")}
        self.assertNotIn("RKLB", passed)

    def test_a_thin_headline_stays_unknown(self):
        board = build_board(STOCKS)
        [hit] = link_articles(board, [{"id": "thin", "text": "Markets moved."}])
        self.assertEqual(hit["status"], "unknown")

    def test_the_company_name_matches_even_when_it_appears_once(self):
        board = build_board(STOCKS)
        [hit] = link_articles(board, [{
            "id": "lmt",
            "text": "Lockheed Martin wins a defense order for military aircraft. Lockheed Martin will build more missiles for defense customers next year.",
        }], z_min=1.5)
        passed = {row["ticker"] for row in hit["stocks"] if row["pass"]}
        self.assertIn("LMT", passed)
        self.assertNotIn("AAL", passed)

    def test_plan_to_launch_a_venue_is_not_a_rocket_launch(self):
        doc = prepare(
            "OKX and ICE plan to launch a trading venue for tokenized stocks. "
            "The crypto exchange will trade those stocks around the clock."
        )
        self.assertNotIn("launch", doc.terms)
        noun = prepare("Commercial launches and reentries doubled. Further launches and reentries are scheduled from the spaceport.")
        self.assertIn("launch", noun.terms)

    def test_names_and_series_are_not_stemmed(self):
        doc = prepare("Lockheed Martin launches spacecraft. The world series is not a launch.")
        self.assertIn("lockheed", doc.terms)
        self.assertNotIn("lockhe", doc.terms)
        self.assertIn("launch", doc.terms)
        self.assertNotIn("sery", doc.terms)

    def test_publisher_suffix_is_not_a_term(self):
        doc = prepare("Gold prices fall on the week\n- Fana News extra words here about gold prices")
        # The trailing publisher on the first line is removed.
        doc = prepare("Gold prices fall for the week - Fana News")
        self.assertNotIn("fana", doc.terms)
        self.assertNotIn("news", doc.terms)

    def test_inflection_matches_launch(self):
        board = build_board(STOCKS)
        [hit] = link_articles(board, [{
            "id": "plural",
            "text": "Commercial launches and reentries from the spaceport doubled. Further launches and reentries are scheduled. Spacecraft launches and orbital launches continue.",
        }], z_min=1.5)
        passed = {row["ticker"] for row in hit["stocks"] if row.get("pass")}
        self.assertIn("RKLB", passed)

    def test_a_phrase_common_in_other_articles_is_dropped(self):
        text = "The coach steps down from the club. The coach steps down today."
        docs = [prepare(text) for _ in range(8)]
        background = Counter()
        tokens = 0
        for doc in docs:
            background.update(doc.terms)
            tokens += doc.tokens
        query = _apply_keyness(docs[0], background - Counter(docs[0].terms), tokens - docs[0].tokens)
        self.assertNotIn("step down", query)

    def test_partner_names_leave_the_core_and_keep_a_role(self):
        text = """
Overview
Playboy products are apparel, beauty, and grooming.

Playboy Products & Services
collaborations with strategic partners in the nightlife and online gaming industries, including DraftKings, Fire Brands, Ainsworth and Sandbox.

Our Competition
DraftKings is discussed here as a rival and must not be reread as the business.
"""
        core, partners = divide_profile(text)
        self.assertNotIn("DraftKings", core)
        self.assertIn("apparel", core.lower())
        roles = {(row["role"], row["name"]) for row in partners}
        self.assertIn(("collaborator", "DraftKings"), roles)
        terms = stock_terms(core, "Playboy Inc")
        self.assertNotIn("draftking", terms)

    def test_a_technology_customer_is_not_a_collaborator(self):
        text = """
Wagering Services
We also provide technology services to third parties, including FanDuel and DraftKings, and we earn commissions from white label products.
"""
        core, partners = divide_profile(text)
        self.assertNotIn("DraftKings", core)
        self.assertIn("commissions", core)
        roles = {(row["role"], row["name"]) for row in partners}
        self.assertIn(("customer", "DraftKings"), roles)
        self.assertIn(("customer", "FanDuel"), roles)

    def test_item1_skips_the_table_of_contents(self):
        text = (
            "Item 1. Business 4 Item 1A. Risk Factors 17 "
            "Item 1. Business Who We Are We open access to space with launch vehicles and spacecraft. "
            + ("The launch vehicle flies. " * 40)
            + " Item 1A. Risk Factors Investing involves risks."
        )
        prose = item1_business(text)
        self.assertIn("open access to space", prose.lower())
        self.assertNotIn("Risk Factors 17", prose)


if __name__ == "__main__":
    unittest.main()
