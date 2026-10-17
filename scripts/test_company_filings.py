"""Filing selection, section extraction, and the local store. No network."""
import tempfile
import textwrap
import unittest
import urllib.parse
from pathlib import Path

from src.company_filings.edgar import blocked_reason, relay_target
from src.company_filings.extract import html_to_text, sections_for, source_points
from src.company_filings.select import fingerprint, priority, select_filings
from src.company_filings.store import Store
from src.company_filings.universe import group_by_cik, load_universe


ANNUAL = {
    "form": "10-K", "accession": "0001-26-000010", "filing_date": "2026-02-28",
    "report_date": "2025-12-31", "primary": "a.htm", "description": "", "items": "",
}
OLD_Q = {
    "form": "10-Q", "accession": "0001-25-000040", "filing_date": "2025-11-01",
    "report_date": "2025-09-30", "primary": "q.htm", "description": "", "items": "",
}
NEW_Q = {
    "form": "10-Q", "accession": "0001-26-000020", "filing_date": "2026-05-08",
    "report_date": "2026-03-31", "primary": "q.htm", "description": "", "items": "",
}
AGREEMENT = {
    "form": "8-K", "accession": "0001-26-000030", "filing_date": "2026-06-02",
    "report_date": "2026-06-01", "primary": "k.htm", "description": "8-K", "items": "1.01,9.01",
}
OFFICER = {
    "form": "8-K", "accession": "0001-26-000031", "filing_date": "2026-06-03",
    "report_date": "2026-06-03", "primary": "k.htm", "description": "8-K", "items": "5.02",
}
EARNINGS = {
    "form": "8-K", "accession": "0001-26-000032", "filing_date": "2026-05-07",
    "report_date": "2026-05-07", "primary": "k.htm", "description": "8-K", "items": "2.02,9.01",
}
BARE_FD = {
    "form": "8-K", "accession": "0001-26-000033", "filing_date": "2026-04-01",
    "report_date": "2026-04-01", "primary": "k.htm", "description": "8-K", "items": "7.01",
}
AWARD = {
    "form": "8-K", "accession": "0001-26-000034", "filing_date": "2026-04-02",
    "report_date": "2026-04-02", "primary": "k.htm",
    "description": "Entry into a launch services contract", "items": "7.01",
}


def _filing_html() -> str:
    return textwrap.dedent("""\
        <html><body>
        <p>Table of contents</p>
        <p>Item 1. Business 1</p>
        <p>Item 1A. Risk Factors 2</p>
        <p>Item 2. Properties 3</p>
        <p>Item 7. MD&A 4</p>
        <p>Item 8. Financial Statements 5</p>
        <h2>Item 1. Business</h2>
        <p>The company operates the Electron launch vehicle from Wallops, Virginia and from Mahia, New Zealand.</p>
        <p>Launch services are sold to civil and national security customers, and this paragraph is long enough that the following item header cannot be mistaken for a contents line.</p>
        <table><tr><td>1,000</td><td>2,000</td><td>$3,000</td><td>4.0%</td></tr>
        <tr><td>5,000</td><td>6,000</td><td>$7,000</td><td>8.0%</td></tr></table>
        <h2>Item 1A. Risk Factors</h2>
        <p>A range closure at Wallops would delay Electron missions and the revenue that depends on them.</p>
        <h2>Item 2. Properties</h2>
        <p>Manufacturing is in Long Beach, California. Space solar cells are built in Albuquerque, New Mexico.</p>
        <h2>Item 7. Management's Discussion and Analysis</h2>
        <p>Launch revenue and space systems revenue are discussed as separate activities for the year.</p>
        <h2>Item 8. Financial Statements</h2>
        <p>1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16</p>
        <p>Notes to Consolidated Financial Statements</p>
        <p>The company has a major customer concentration in United States government agencies for space systems.</p>
        </body></html>
    """)


class RelayTests(unittest.TestCase):
    def test_gov_urls_go_through_the_worker_and_other_hosts_do_not(self):
        relay = "https://fullscan-primary-relay.example.workers.dev"
        sec = "https://data.sec.gov/submissions/CIK0001819994.json"
        routed = relay_target(relay, sec)
        self.assertTrue(routed.startswith(relay + "/?url="))
        self.assertIn("data.sec.gov", urllib.parse.unquote(routed.split("url=", 1)[1]))
        self.assertEqual(relay_target(relay, "https://example.com/10k"), "https://example.com/10k")
        self.assertEqual(relay_target("", sec), sec)


class BlockTests(unittest.TestCase):
    def test_sec_block_pages_are_named(self):
        self.assertIn("undeclared", blocked_reason("<title>SEC.gov | Your Request Originates from an Undeclared Automated Tool</title>"))
        self.assertIn("rate threshold", blocked_reason("Request Rate Threshold Exceeded"))
        self.assertEqual(blocked_reason("Item 1. Business"), "")


class SelectTests(unittest.TestCase):
    def test_keeps_annual_later_quarters_and_business_8ks(self):
        chosen = select_filings([ANNUAL, OLD_Q, NEW_Q, AGREEMENT, OFFICER, EARNINGS, BARE_FD, AWARD])
        accessions = {row["accession"] for row in chosen}
        self.assertIn(ANNUAL["accession"], accessions)
        self.assertIn(NEW_Q["accession"], accessions)
        self.assertNotIn(OLD_Q["accession"], accessions)
        self.assertIn(AGREEMENT["accession"], accessions)
        self.assertIn(EARNINGS["accession"], accessions)
        self.assertIn(AWARD["accession"], accessions)
        self.assertNotIn(OFFICER["accession"], accessions)
        self.assertNotIn(BARE_FD["accession"], accessions)

    def test_registration_only_when_there_is_no_annual(self):
        registration = dict(ANNUAL, form="S-1", accession="0001-24-000001", filing_date="2024-08-01")
        old_amendment = dict(registration, form="S-1/A", accession="0001-24-000000", filing_date="2024-07-01")
        new_amendment = dict(registration, form="S-1/A", accession="0001-24-000002", filing_date="2024-09-01")
        chosen = select_filings([registration, old_amendment, new_amendment, OFFICER])
        self.assertEqual(
            {row["accession"] for row in chosen},
            {registration["accession"], new_amendment["accession"]},
        )

    def test_budget_order_keeps_the_newer_quarter_first(self):
        ordered = sorted([OLD_Q, NEW_Q], key=priority)
        self.assertEqual(ordered[0]["accession"], NEW_Q["accession"])

    def test_fingerprint_ignores_order(self):
        self.assertEqual(fingerprint([ANNUAL, NEW_Q]), fingerprint([NEW_Q, ANNUAL]))


class ExtractTests(unittest.TestCase):
    def test_sections_skip_the_contents_and_the_statement_table(self):
        text = html_to_text(_filing_html().encode())
        sections, status = sections_for("10-K", text)
        self.assertEqual(status, "items")
        self.assertIn("Electron", sections["1"])
        self.assertIn("Wallops", sections["1a"])
        self.assertIn("Albuquerque", sections["2"])
        self.assertIn("space systems", sections["7"])
        self.assertIn("government agencies", sections["notes"])
        self.assertNotIn("1,000", sections["1"])
        self.assertNotIn("1 2 3 4", sections.get("notes", ""))
        joined = "\n".join(sections.values())
        self.assertNotIn("Table of contents", joined)

    def test_points_are_offsets_into_the_section(self):
        text = html_to_text(_filing_html().encode())
        sections, _ = sections_for("10-K", text)
        points = source_points(sections["1"])
        self.assertGreaterEqual(len(points), 2)
        for point in points:
            quoted = sections["1"][point.start:point.end]
            self.assertIn("Electron", quoted + sections["1"])
            self.assertGreaterEqual(len(quoted), 80)

    def test_numeric_table_is_not_prose(self):
        raw = (
            b"<html><body><p>Item 1. Business</p><p>"
            + (b"Alpha beta gamma delta " * 8)
            + b"</p><table><tr>"
            + b"<td>100</td><td>200</td><td>$300</td>" * 6
            + b"</tr></table></body></html>"
        )
        text = html_to_text(raw)
        self.assertNotIn("100", text)
        self.assertIn("Alpha", text)


class StoreTests(unittest.TestCase):
    def test_round_trip_reads_the_cited_span(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = Store(Path(tmp) / "filings.sqlite")
            section = "The company operates the Electron launch vehicle from Wallops, Virginia and from Mahia, New Zealand. " * 40
            start = section.index("Electron")
            end = section.index("Wallops") + len("Wallops")
            store.replace_company(
                {
                    "ticker": "RKLB", "cik": "0000000001", "name": "Rocket Lab",
                    "sector": "Industrials", "industry": "Aerospace & Defense",
                    "market_cap_m": 1000, "fingerprint": "abc", "tickers": ["RKLB", "RKLBX"],
                },
                [{
                    "accession": "0001-26-000010", "form": "10-K", "filing_date": "2026-02-28",
                    "report_date": "2025-12-31", "items": "", "url": "https://sec.gov/a",
                    "sha256": "deadbeef", "split_status": "items",
                    "sections": {"1": section},
                    "points": {"1": [(0, start, end)]},
                    "exhibits": [],
                }],
            )
            self.assertEqual(store.point_text("0001-26-000010:1:0"), "Electron launch vehicle from Wallops")
            self.assertEqual(store.fingerprint("RKLBX"), "abc")
            summary = store.summary()
            self.assertEqual(summary["points"], 1)
            self.assertLess(summary["stored_bytes"], len(section))
            store.close()


class UniverseTests(unittest.TestCase):
    def test_liquidity_and_fund_filter(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "finviz_2026-10-16.csv"
            path.write_text(textwrap.dedent("""\
                Ticker,Company,Sector,Industry,Country,Market Cap,Price,Average Volume,Average True Range
                RKLB,Rocket Lab,Industrials,Aerospace & Defense,USA,1000,20,800,1
                TINY,Tiny Co,Tech,Software,USA,10,5,900,1
                ETFY,Some Fund,Financial,Exchange Traded Fund,USA,5000,100,5000,4
                SHELL,Blank Check,Financial,Shell Companies,USA,500,10,900,1
                SLOW,Slow Co,Tech,Software,USA,500,10,10,1
            """))
            rows = load_universe(path)
            self.assertEqual([row["ticker"] for row in rows], ["RKLB"])

    def test_one_parse_per_cik(self):
        grouped = group_by_cik([
            {"ticker": "GOOG", "cik": "0001", "market_cap_m": 100},
            {"ticker": "GOOGL", "cik": "0001", "market_cap_m": 90},
            {"ticker": "RKLB", "cik": "0002", "market_cap_m": 20},
        ])
        by_cik = {row["cik"]: row for row in grouped}
        self.assertEqual(by_cik["0001"]["ticker"], "GOOG")
        self.assertCountEqual(by_cik["0001"]["tickers"], ["GOOG", "GOOGL"])


if __name__ == "__main__":
    unittest.main()
