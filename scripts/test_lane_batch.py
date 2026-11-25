"""Lane's missing handoff: a vertical answer becomes tickers, never a guess."""
import unittest
from pathlib import Path
from unittest.mock import patch

from src.news_impact.lane_batch import apply_answer, job_for, parse_reply, render_batch
from src.news_impact.one_shot_stack import process_article
from src.news_impact.ticker_verticals import load_book

BOOK = load_book(Path("data/exports/finviz_2026-10-15.csv"))


class BatchTests(unittest.TestCase):
    def test_oil_producers_down_and_refiners_up_stay_apart(self):
        job = job_for({"title": "OPEC cut leaves oil producers bullish"}, BOOK)
        # The title locks the producer vertical. The model adds refiners.
        answer = {
            "id": job["id"],
            "event_class": "factor_impulse",
            "q5": "impulse",
            "sign": "up",
            "primary": [{"chain": "crude", "role": "producer", "direction": "up"}],
            "second_order": [{
                "chain": "crude", "role": "refiner", "direction": "down",
                "why": "crude input costs rise",
            }],
        }
        row = apply_answer(job, answer, BOOK)
        self.assertTrue(row["complete"])
        roles = {(e["ticker"], e["role"], e["direction"]) for e in row["entities"]}
        self.assertIn(("XOM", "integrated", "up"), roles)
        self.assertIn(("COP", "producer", "up"), roles)
        self.assertIn(("VLO", "refiner", "down"), roles)
        self.assertIn("BUY", row["action"])
        self.assertIn("SELL", row["action"])
        self.assertNotIn("EPD", {e["ticker"] for e in row["entities"]})

    def test_a_bank_vertical_lands_on_banks_and_not_exxon(self):
        job = job_for({"title": "Treasury restricts dollar clearing for unnamed banks"}, BOOK)
        row = apply_answer(job, {
            "id": job["id"],
            "event_class": "access_control",
            "q5": "impulse",
            "sign": "tighten",
            "primary": [{"chain": "banks", "role": "diversified", "direction": "down"}],
        }, BOOK)
        tickers = {e["ticker"] for e in row["entities"]}
        self.assertIn("BAC", tickers)
        self.assertNotIn("XOM", tickers)
        self.assertTrue(row["complete"])

    def test_sanction_without_a_vertical_does_not_become_exxon(self):
        job = job_for({
            "title": "Implementing the Lindsey O. Graham Sanctioning Russia and Iran Act of 2026",
        }, BOOK)
        answer = {
            "id": job["id"],
            "event_class": "access_control",
            "q5": "impulse",
            "sign": "tighten",
            "primary": [],
            "second_order": [],
        }
        row = apply_answer(job, answer, BOOK)
        self.assertEqual(row["entities"], [])
        self.assertIn("no_vertical", row["errors"])
        self.assertNotIn("XOM", row["action"])

    def test_reply_tickers_are_ignored(self):
        text = """Here you go:
        ```json
        [{"id":"abc","event_class":"access_control","q5":"impulse","sign":"tighten",
          "primary":[{"chain":"crude","role":"producer","direction":"down","ticker":"XOM"}]}]
        ```
        """
        parsed = parse_reply(text)
        self.assertEqual(parsed[0]["id"], "abc")
        job = job_for({"title": "New export ban on a commodity no one named"}, BOOK)
        job["id"] = "abc"
        row = apply_answer(job, parsed[0], BOOK)
        self.assertIn("COP", {e["ticker"] for e in row["entities"]})
        self.assertNotIn("XOM", {e["ticker"] for e in row["entities"]})
        self.assertTrue(all(e["basis"] == "finviz_industry_leaders" for e in row["entities"]))

    def test_named_bank_keeps_bac_and_the_model_direction(self):
        job = job_for({
            "title": "OCC Imposes $750 Million Assessment Against Bank of America",
        }, BOOK)
        row = apply_answer(job, {
            "id": job["id"],
            "event_class": "blast_legal",
            "q5": "impulse",
            "sign": "down",
            "primary": [{"chain": "crude", "role": "producer", "direction": "down"}],
        }, BOOK)
        self.assertEqual([e["ticker"] for e in row["entities"]], ["BAC"])
        self.assertEqual(row["entities"][0]["direction"], "down")

    def test_prompt_asks_for_verticals_and_lists_only_kept_rows(self):
        kept = job_for({"title": "Iran-related Designations and General Licenses"}, BOOK)
        dropped = job_for({"title": "Read twice and referred to the Committee on Finance."}, BOOK)
        text = render_batch([kept, dropped])
        self.assertIn(kept["id"], text)
        self.assertNotIn("Committee on Finance", text)
        self.assertIn("No tickers", text)
        self.assertIn("crude.producer", text)

    def test_empty_candidate_pool_still_skips_filter_and_carries_the_packet(self):
        calls = []

        def lane(stage, prompt, system, accept=None):
            calls.append(stage)
            if stage == "classify":
                blob = {
                    "event_class": "access_control", "q5": "impulse", "sign": "tighten",
                    "constraint": "Treasury transaction restrictions",
                }
            elif stage == "meta":
                blob = {
                    "m1": {"need_context": "no"},
                    "m2": [{
                        "slot": "H", "noun": "agency",
                        "question": "Which agency counterparties carry the affected payment exposure?",
                        "changes": "direction",
                    }],
                }
            else:
                self.fail(stage)
            self.assertTrue(accept(blob))
            return blob, "synthetic", "model"

        with patch("src.news_impact.one_shot_stack.candidate_rows", return_value={
            "instruments": [], "finviz_file": "synthetic.csv",
        }):
            row = process_article({
                "title": "Treasury sanctions a foreign agency",
                "body": "Treasury restricts transactions with the agency.",
                "published_at": "2026-10-09T00:00:00Z",
            }, lane, axioms=[], use_pack=False)
        self.assertEqual(calls, ["classify", "meta"])
        self.assertEqual(row["reject_reason"], "no_instrument_candidates")
        self.assertEqual(row["instruments"], [])
        packet = row["vertical_packet"]
        self.assertEqual(packet["event_class"], "access_control")
        self.assertIn("Do not emit tickers", packet["ask"])
        self.assertIn("crude", packet["chains"])


if __name__ == "__main__":
    unittest.main()
