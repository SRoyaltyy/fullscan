"""The memo's small circles must pass Rocket Lab and fail the airline and the office REIT."""
import unittest

from collections import Counter

from src.correlation import _apply_keyness, build_board, link_articles, prepare, stock_terms
from src.filing_business import item1_business
from src.filing_parts import divide_profile
from src.names import Name, NameIndex

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
        [hit] = link_articles(board, [{"id": "memo", "title": "space policy", "text": MEMO}], z_min=1.4)
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

    def test_source_place_verb_and_one_title_phrase_do_not_count(self):
        klook = (
            "KLK IPO News - Pan-Asian travel booking platform Klook Technology "
            "withdraws estimated $400 million US IPO - Renaissance Capital\n"
            "KLK IPO News - Pan-Asian travel booking platform Klook Technology "
            "withdraws estimated $400 million US IPO Renaissance Capital"
        )
        doc = prepare(klook)
        self.assertNotIn("renaissance", doc.terms)
        self.assertNotIn("capital", doc.terms)
        police = (
            "3 North Carolina police officers resign amid racist texts - ABC News\n"
            "Charlotte-Mecklenburg police investigation into racist texts in Charlotte"
        )
        doc = prepare(police)
        self.assertNotIn("charlotte", doc.terms)
        self.assertNotIn("carolina", doc.terms)
        inflation = "Wall Street's biggest obsessions are showing up in inflation - Business Insider"
        doc = prepare(inflation)
        self.assertNotIn("showing", doc.terms)
        peptide = (
            "DataMeds Applauds FDA Proposed Interim Access Policy for Peptides - Nasdaq\n"
            "DataMeds Applauds FDA Proposed Interim Access Policy for Peptides Nasdaq"
        )
        doc = prepare(peptide)
        self.assertNotIn("access policy", doc.bundles)
        self.assertNotIn("access policy", doc.terms)

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

    def test_a_publisher_is_not_a_ticker_and_a_dollar_ticker_is(self):
        index = NameIndex([
            Name("POST", "Post Holdings Inc", "post", "Packaged Foods", "Consumer Defensive"),
            Name("LMT", "Lockheed Martin Corp", "lockheed martin", "Aerospace & Defense", "Industrials"),
            Name("BKNG", "Booking Holdings Inc", "booking", "Travel Services", "Consumer Cyclical"),
            Name("DKNG", "DraftKings Inc", "draftkings", "Gambling", "Consumer Cyclical"),
        ])
        post = index.mentioned(
            "Washington Post reporters published an investigation. WASHINGTON POST had the story first."
        )
        self.assertNotIn("POST", {row.ticker for row in post})
        self.assertIn("LMT", {row.ticker for row in index.mentioned("Lockheed Martin declined to comment.")})
        dollar = index.mentioned("Shares of $POST jumped after the cereal maker raised prices.")
        self.assertIn("POST", {row.ticker for row in dollar})
        booking = index.mentioned("Pan-Asian travel booking platform Klook withdraws its IPO.")
        self.assertNotIn("BKNG", {row.ticker for row in booking})
        self.assertIn("DKNG", {row.ticker for row in index.mentioned("Investors in DraftKings are watching.")})
        news = NameIndex([Name("NWS", "News Corp", "news", "Entertainment", "Communication Services")])
        headline = news.mentioned("KLK IPO News - Pan-Asian travel booking platform Klook Technology withdraws its IPO")
        self.assertNotIn("NWS", {row.ticker for row in headline})
        tickers = NameIndex([
            Name("HOOD", "Robinhood Markets Inc", "robinhood markets"),
            Name("COIN", "Coinbase Global Inc", "coinbase global"),
            Name("TSLA", "Tesla Inc", "tesla"),
        ])
        named = {row.ticker for row in tickers.mentioned("Tokenized versions of HOOD, COIN and TSLA were mentioned.")}
        self.assertEqual(named, {"HOOD", "COIN", "TSLA"})
        wires = NameIndex([
            Name("AP", "Ampco Pittsburgh Corp", "ampco pittsburgh"),
            Name("SAN", "Banco Santander SA ADR", "banco santander"),
            Name("IRS", "Irsa Inversiones Y Representaciones SA ADR", "irsa"),
            Name("MD", "Pediatrix Medical Group Inc", "pediatrix"),
            Name("MRNA", "Moderna Inc", "moderna"),
            Name("BA", "Boeing Co", "boeing"),
            Name("LMT", "Lockheed Martin Corp", "lockheed martin"),
        ])
        self.assertNotIn("AP", {row.ticker for row in wires.mentioned("Brush fires spark in Southern California - AP News")})
        self.assertNotIn("SAN", {row.ticker for row in wires.mentioned("SAN JUAN, Puerto Rico – the attorney announced the task force.")})
        self.assertNotIn("IRS", {row.ticker for row in wires.mentioned("The IRS Criminal Investigation unit joined the DEA and HHS-OIG.")})
        self.assertNotIn("MD", {row.ticker for row in wires.mentioned("Strata Minerals MD Peter Woods will speak.")})
        self.assertIn("MRNA", {row.ticker for row in wires.mentioned("Moderna's next test is the supply chain.")})
        listed = {row.ticker for row in wires.mentioned("The effort names BA, LMT and RTX.")}
        self.assertIn("BA", listed)
        self.assertIn("LMT", listed)

    def test_a_word_the_whole_industry_uses_does_not_pass_every_stock(self):
        stocks = []
        for index in range(12):
            stocks.append({
                "ticker": f"B{index:02d}",
                "name": f"Biotech {index} Inc",
                "industry": "Biotechnology",
                "sector": "Healthcare",
                "description": "The pipeline is a kinase program. Kinase research continues. The kinase candidate advanced.",
            })
        stocks += [
            {"ticker": "JPM", "name": "JPMorgan Chase & Co.", "industry": "Banks - Diversified", "sector": "Financial",
             "description": "JPMorgan Chase takes deposits and makes loans through consumer banking."},
            {"ticker": "XOM", "name": "Exxon Mobil Corp", "industry": "Oil & Gas Integrated", "sector": "Energy",
             "description": "Exxon Mobil produces crude oil and natural gas."},
            {"ticker": "NKE", "name": "Nike Inc", "industry": "Footwear & Accessories", "sector": "Consumer Cyclical",
             "description": "Nike designs athletic footwear and apparel for sports."},
            {"ticker": "KO", "name": "Coca-Cola Co", "industry": "Beverages - Non-Alcoholic", "sector": "Consumer Defensive",
             "description": "Coca-Cola manufactures sparkling beverages and juice drinks."},
        ]
        board = build_board(stocks)
        text = (
            "The kinase candidate advanced. Kinase research expanded the kinase program. "
            "Another kinase study read out. The kinase program remains the focus."
        )
        [hit] = link_articles(board, [{"id": "kinase", "text": text}])
        passed = {row["ticker"] for row in hit["stocks"] if row["pass"]}
        self.assertEqual(passed, set())

    def test_a_partner_link_is_second_order_and_follows_the_partner_industry(self):
        stocks = STOCKS + [
            {"ticker": "DKNG", "name": "DraftKings Inc", "industry": "Gambling", "sector": "Consumer Cyclical",
             "description": "DraftKings operates an online sportsbook. Customers place sports betting wagers. Gambling and sportsbook markets are the business."},
            {"ticker": "FLUT", "name": "Flutter Entertainment plc", "industry": "Gambling", "sector": "Consumer Cyclical",
             "description": "Flutter operates sports betting and online sportsbook brands. Gambling customers wager through the sportsbook."},
            {"ticker": "PLBY", "name": "Playboy Inc", "industry": "Leisure", "sector": "Consumer Cyclical",
             "description": "Playboy sells apparel, beauty, and grooming products under lifestyle brands.",
             "filing": "Overview\nPlayboy products are apparel, beauty, and grooming.\n\nPlayboy Products & Services\ncollaborations with strategic partners in the nightlife and online gaming industries, including DraftKings, Fire Brands, Ainsworth and Sandbox."},
            {"ticker": "CHDN", "name": "Churchill Downs Inc", "industry": "Gambling", "sector": "Consumer Cyclical",
             "description": "Churchill Downs operates horse racing and a casino wagering business.",
             "filing": "Wagering Services\nWe also provide technology services to third parties, including FanDuel and DraftKings, and we earn commissions from white label products."},
            {"ticker": "XOM", "name": "Exxon Mobil Corp", "industry": "Oil & Gas Integrated", "sector": "Energy",
             "description": "Exxon Mobil produces crude oil, natural gas, and refined petroleum products."},
            {"ticker": "PFE", "name": "Pfizer Inc", "industry": "Drug Manufacturers", "sector": "Healthcare",
             "description": "Pfizer discovers and manufactures pharmaceutical medicines and vaccines."},
        ]
        board = build_board(stocks)
        article = (
            "Gambling regulators questioned the sportsbook. Sports betting and gambling rules may change. "
            "DraftKings and the sportsbook industry are waiting on the gambling ruling. "
            "Sports betting volume at the sportsbook fell. DraftKings shares fell 4%."
        )
        [hit] = link_articles(board, [{"id": "gamble", "text": article}])
        passed = {row["ticker"] for row in hit["stocks"] if row["pass"]}
        self.assertNotIn("PLBY", passed)
        self.assertIn("DKNG", {row["ticker"] for row in hit["stocks"] if row["name_hit"]})
        deps = {(row["ticker"], row["partner"], row["order"]) for row in hit["dependencies"]}
        self.assertIn(("PLBY", "DraftKings", 2), deps)
        self.assertIn(("CHDN", "DraftKings", 2), deps)
        self.assertTrue(all(row["second_order"] for row in hit["dependencies"]))
        [memo] = link_articles(board, [{"id": "memo", "text": MEMO}])
        self.assertFalse(any(row["partner"] == "DraftKings" for row in memo["dependencies"]))

    def test_uk_spelling_and_true_synonyms_fold_to_one_term(self):
        defence = prepare("The Pentagon asked defence firms to accelerate weapons production.")
        self.assertIn("defense", defence.terms)
        self.assertNotIn("defence", defence.terms)
        self.assertIn("color", prepare("The colour of the casing.").terms)
        self.assertNotIn("hor", prepare("Results are due within the hour.").terms)
        self.assertNotIn("promize", prepare("The company made a promise to customers.").terms)
        organised = prepare("The firm organised a production review and organised it again.")
        organized = prepare("The firm organized a production review and organized it again.")
        self.assertEqual(set(organised.terms), set(organized.terms))
        self.assertIn("fast", prepare("A quick charging network.").terms)
        self.assertIn("attorney", prepare("The lawyer filed the suit.").terms)
        self.assertIn("attorney", prepare("The attorney filed the suit.").terms)

    def test_defence_article_matches_a_defense_description(self):
        stocks = [
            {"ticker": "BA", "name": "Boeing Co", "industry": "Aerospace & Defense", "sector": "Industrials",
             "description": "Boeing designs defense aircraft and missiles. Defense customers buy military aircraft, missiles, and weapons systems."},
            {"ticker": "JPM", "name": "JPMorgan Chase & Co.", "industry": "Banks - Diversified", "sector": "Financial",
             "description": "JPMorgan Chase takes deposits, makes loans, and operates consumer banking and investment banking businesses."},
            {"ticker": "AAL", "name": "American Airlines Group Inc.", "industry": "Airlines", "sector": "Industrials",
             "description": "American Airlines offers air transportation for passengers and cargo across a network of routes."},
            {"ticker": "PFE", "name": "Pfizer Inc", "industry": "Drug Manufacturers", "sector": "Healthcare",
             "description": "Pfizer discovers and manufactures pharmaceutical medicines and vaccines."},
            {"ticker": "XOM", "name": "Exxon Mobil Corp", "industry": "Oil & Gas Integrated", "sector": "Energy",
             "description": "Exxon Mobil produces crude oil, natural gas, and refined petroleum products."},
            {"ticker": "NKE", "name": "Nike Inc", "industry": "Footwear & Accessories", "sector": "Consumer Cyclical",
             "description": "Nike designs and sells athletic footwear, apparel, and sporting equipment."},
            {"ticker": "KO", "name": "Coca-Cola Co", "industry": "Beverages - Non-Alcoholic", "sector": "Consumer Defensive",
             "description": "Coca-Cola manufactures sparkling beverages and juice drinks sold through bottlers."},
            {"ticker": "T", "name": "AT&T Inc", "industry": "Telecom Services", "sector": "Communication Services",
             "description": "AT&T provides wireless service, broadband, and telephone connections to consumers."},
            {"ticker": "CAT", "name": "Caterpillar Inc", "industry": "Farm & Heavy Construction Machinery", "sector": "Industrials",
             "description": "Caterpillar makes construction machinery, mining trucks, and diesel engines."},
            {"ticker": "UNP", "name": "Union Pacific Corp", "industry": "Railroads", "sector": "Industrials",
             "description": "Union Pacific operates a railroad freight network hauling industrial goods."},
        ]
        board = build_board(stocks)
        text = (
            "The Pentagon has asked defence firms to accelerate weapons production. "
            "Defence contractors are adding missile and aircraft capacity. "
            "Weapons production for defence aircraft is being pulled forward. "
            "Defence aircraft schedules and missile output are the subject of the request."
        )
        [hit] = link_articles(board, [{"id": "defence", "text": text}])
        passed = {row["ticker"] for row in hit["stocks"] if row["pass"]}
        self.assertIn("BA", passed)
        self.assertNotIn("AAL", passed)
        self.assertNotIn("JPM", passed)

    def test_quick_matches_a_fast_description(self):
        stocks = [
            {"ticker": "CHPT", "name": "ChargePoint Holdings Inc", "industry": "Specialty Retail", "sector": "Consumer Cyclical",
             "description": "ChargePoint operates a fast charging network. The fast charging stations serve electric vehicles. Fast charging is the product."},
            {"ticker": "JPM", "name": "JPMorgan Chase & Co.", "industry": "Banks - Diversified", "sector": "Financial",
             "description": "JPMorgan Chase takes deposits, makes loans, and operates consumer banking and investment banking businesses."},
            {"ticker": "AAL", "name": "American Airlines Group Inc.", "industry": "Airlines", "sector": "Industrials",
             "description": "American Airlines offers air transportation for passengers and cargo across a network of routes."},
            {"ticker": "PFE", "name": "Pfizer Inc", "industry": "Drug Manufacturers", "sector": "Healthcare",
             "description": "Pfizer discovers and manufactures pharmaceutical medicines and vaccines."},
            {"ticker": "XOM", "name": "Exxon Mobil Corp", "industry": "Oil & Gas Integrated", "sector": "Energy",
             "description": "Exxon Mobil produces crude oil, natural gas, and refined petroleum products."},
            {"ticker": "NKE", "name": "Nike Inc", "industry": "Footwear & Accessories", "sector": "Consumer Cyclical",
             "description": "Nike designs and sells athletic footwear, apparel, and sporting equipment."},
            {"ticker": "KO", "name": "Coca-Cola Co", "industry": "Beverages - Non-Alcoholic", "sector": "Consumer Defensive",
             "description": "Coca-Cola manufactures sparkling beverages and juice drinks sold through bottlers."},
            {"ticker": "T", "name": "AT&T Inc", "industry": "Telecom Services", "sector": "Communication Services",
             "description": "AT&T provides wireless service, broadband, and telephone connections to consumers."},
            {"ticker": "CAT", "name": "Caterpillar Inc", "industry": "Farm & Heavy Construction Machinery", "sector": "Industrials",
             "description": "Caterpillar makes construction machinery, mining trucks, and diesel engines."},
            {"ticker": "UNP", "name": "Union Pacific Corp", "industry": "Railroads", "sector": "Industrials",
             "description": "Union Pacific operates a railroad freight network hauling industrial goods."},
        ]
        board = build_board(stocks)
        text = (
            "Drivers gained a quick charging network along the highway. "
            "Quick charging stations opened in three states. "
            "The quick charging network is aimed at electric vehicles. "
            "Quick charging became the busiest part of the stop."
        )
        [hit] = link_articles(board, [{"id": "quick", "text": text}])
        passed = {row["ticker"] for row in hit["stocks"] if row["pass"]}
        self.assertIn("CHPT", passed)
        self.assertNotIn("JPM", passed)

    def test_an_alias_is_not_a_pass_until_something_else_corroborates(self):
        stocks = [
            {"ticker": "SPCX", "name": "Space Exploration Technologies Corp", "industry": "Aerospace & Defense", "sector": "Industrials",
             "description": "Space Exploration Technologies Corp., doing business as SpaceX, builds rockets and spacecraft and provides launch services. The company was founded by Elon Reeve Musk in 2002 and is headquartered in Starbase, TX."},
            {"ticker": "JPM", "name": "JPMorgan Chase & Co.", "industry": "Banks - Diversified", "sector": "Financial",
             "description": "JPMorgan Chase takes deposits and makes loans through consumer banking."},
            {"ticker": "NKE", "name": "Nike Inc", "industry": "Footwear & Accessories", "sector": "Consumer Cyclical",
             "description": "Nike designs athletic footwear and apparel for sports."},
            {"ticker": "KO", "name": "Coca-Cola Co", "industry": "Beverages - Non-Alcoholic", "sector": "Consumer Defensive",
             "description": "Coca-Cola manufactures sparkling beverages and juice drinks."},
            {"ticker": "PFE", "name": "Pfizer Inc", "industry": "Drug Manufacturers", "sector": "Healthcare",
             "description": "Pfizer discovers and manufactures pharmaceutical medicines and vaccines."},
            {"ticker": "XOM", "name": "Exxon Mobil Corp", "industry": "Oil & Gas Integrated", "sector": "Energy",
             "description": "Exxon Mobil produces crude oil and natural gas."},
            {"ticker": "CAT", "name": "Caterpillar Inc", "industry": "Farm & Heavy Construction Machinery", "sector": "Industrials",
             "description": "Caterpillar makes construction machinery, mining trucks, and diesel engines."},
            {"ticker": "T", "name": "AT&T Inc", "industry": "Telecom Services", "sector": "Communication Services",
             "description": "AT&T provides wireless service, broadband, and telephone connections."},
            {"ticker": "UNP", "name": "Union Pacific Corp", "industry": "Railroads", "sector": "Industrials",
             "description": "Union Pacific operates a railroad freight network hauling industrial goods."},
            {"ticker": "HD", "name": "Home Depot Inc", "industry": "Home Improvement Retail", "sector": "Consumer Cyclical",
             "description": "Home Depot sells building materials, tools, and home improvement goods."},
        ]
        board = build_board(stocks)
        [alone] = link_articles(board, [{"id": "hat", "text": "Elon Musk commented on a hat at a party and left early without discussing any business at all."}])
        spcx = next(row for row in alone["stocks"] if row["ticker"] == "SPCX")
        self.assertTrue(spcx["alias_hit"])
        self.assertFalse(spcx["pass"])
        [corroborated] = link_articles(board, [{"id": "launch", "text": "Elon Musk said the firm will build more rockets and spacecraft and expand launch services for commercial customers. Rockets and spacecraft remain the business. Launch services are booked for the year."}])
        passed = {row["ticker"] for row in corroborated["stocks"] if row["pass"]}
        self.assertIn("SPCX", passed)

    def test_a_place_scopes_a_company_and_does_not_pass_by_itself(self):
        stocks = [
            {"ticker": "DUK", "name": "Duke Energy Corp", "industry": "Utilities - Regulated Electric", "sector": "Utilities",
             "description": "Duke Energy conducts regulated electric utilities in the Carolinas, Florida and the Midwest. It is headquartered in Charlotte, NC."},
            {"ticker": "JPM", "name": "JPMorgan Chase & Co.", "industry": "Banks - Diversified", "sector": "Financial",
             "description": "JPMorgan Chase takes deposits and makes loans through consumer banking in New York."},
            {"ticker": "NKE", "name": "Nike Inc", "industry": "Footwear & Accessories", "sector": "Consumer Cyclical",
             "description": "Nike designs athletic footwear and apparel for sports."},
            {"ticker": "KO", "name": "Coca-Cola Co", "industry": "Beverages - Non-Alcoholic", "sector": "Consumer Defensive",
             "description": "Coca-Cola manufactures sparkling beverages and juice drinks."},
        ]
        board = build_board(stocks)
        [festival] = link_articles(board, [{"id": "fair", "text": "A weekend festival in North Carolina drew crowds to Charlotte. North Carolina hosts the festival every year and Charlotte closes its streets for it."}])
        duk = next(row for row in festival["stocks"] if row["ticker"] == "DUK")
        self.assertTrue(duk["jurisdiction_hit"])
        self.assertFalse(duk["pass"])
        self.assertNotIn("JPM", {row["ticker"] for row in festival["stocks"] if row["pass"]})

    def test_fuel_votes_for_refiners_and_not_for_every_refiner(self):
        refiners = []
        for index in range(12):
            refiners.append({
                "ticker": f"R{index:02d}",
                "name": f"Refiner {index} Inc",
                "industry": "Oil & Gas Refining & Marketing",
                "sector": "Energy",
                "description": "The company refines crude into fuel. Fuel production includes gasoline and diesel. The fuel business is refining.",
            })
        shippers = []
        for index in range(8):
            shippers.append({
                "ticker": f"S{index:02d}",
                "name": f"Shipper {index} Inc",
                "industry": "Marine Shipping",
                "sector": "Energy",
                "description": "The company owns tankers. Tankers carry cargo across the ocean. The tanker fleet is the business.",
            })
        others = [
            {"ticker": "JPM", "name": "JPMorgan Chase & Co.", "industry": "Banks - Diversified", "sector": "Financial",
             "description": "JPMorgan Chase takes deposits and makes loans through consumer banking."},
            {"ticker": "PFE", "name": "Pfizer Inc", "industry": "Drug Manufacturers", "sector": "Healthcare",
             "description": "Pfizer discovers and manufactures pharmaceutical medicines and vaccines."},
            {"ticker": "NKE", "name": "Nike Inc", "industry": "Footwear & Accessories", "sector": "Consumer Cyclical",
             "description": "Nike designs athletic footwear and apparel for sports."},
            {"ticker": "KO", "name": "Coca-Cola Co", "industry": "Beverages - Non-Alcoholic", "sector": "Consumer Defensive",
             "description": "Coca-Cola manufactures sparkling beverages and juice drinks."},
            {"ticker": "T", "name": "AT&T Inc", "industry": "Telecom Services", "sector": "Communication Services",
             "description": "AT&T provides wireless service, broadband, and telephone connections."},
            {"ticker": "HD", "name": "Home Depot Inc", "industry": "Home Improvement Retail", "sector": "Consumer Cyclical",
             "description": "Home Depot sells building materials, tools, and home improvement goods."},
        ]
        board = build_board(refiners + shippers + others)
        text = (
            "The fuel oil market is tight. Fuel buyers are paying more for gasoline and diesel. "
            "Fuel production is constrained. Gasoline and diesel fuel remain scarce."
        )
        [hit] = link_articles(board, [{"id": "fuel", "text": text}])
        passed_industries = {row["industry"] for row in hit["industries"] if row["pass"]}
        self.assertIn("Oil & Gas Refining & Marketing", passed_industries)
        self.assertNotIn("Marine Shipping", passed_industries)
        self.assertEqual({row["ticker"] for row in hit["stocks"] if row["pass"]}, set())


if __name__ == "__main__":
    unittest.main()
