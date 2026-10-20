"""The backtest page is the scanner gate: random article in, evidence matches out."""
import json
import random
import unittest
from pathlib import Path

from src.correlation import build_board
from src.scanner_backtest import ENGINE, PAGE, Gate, dispatch, draw_article, present, run_article


class DrawTests(unittest.TestCase):
    def test_draw_returns_a_member_of_the_pool(self):
        pool = [{"id": "one", "text": "a"}, {"id": "two", "text": "b"}]

        class Fixed:
            def randrange(self, n):
                self.n = n
                return 1

        rng = Fixed()
        drawn = draw_article(pool, rng)
        self.assertEqual(rng.n, 2)
        self.assertEqual(drawn["id"], "two")
        drawn["text"] = "changed"
        self.assertEqual(pool[1]["text"], "b")

    def test_empty_pool_has_nothing_to_draw(self):
        with self.assertRaises(ValueError):
            draw_article([], random.Random(0))


class GateTests(unittest.TestCase):
    def board(self):
        rows = [
            {
                "ticker": "AAPL",
                "name": "Apple Inc",
                "industry": "Consumer Electronics",
                "sector": "Technology",
                "description": "Apple manufactures smartphones, tablets and wearable computing devices.",
            },
            {
                "ticker": "XOM",
                "name": "Exxon Mobil Corp",
                "industry": "Oil & Gas Integrated",
                "sector": "Energy",
                "description": "Exxon Mobil produces crude oil and natural gas.",
            },
        ]
        board = build_board(rows, engine=ENGINE)
        return board

    def test_run_article_calls_the_evidence_engine(self):
        seen = {}

        def fake(board, articles, **kwargs):
            seen["engine"] = kwargs.get("engine")
            seen["count"] = len(articles)
            return [{
                "id": articles[0]["id"],
                "title": "Apple shares rise",
                "decision": "linked",
                "engine": "evidence-v2",
                "status": "scored",
                "stocks": [{
                    "pass": True,
                    "ticker": "AAPL",
                    "name": "Apple Inc",
                    "industry": "Consumer Electronics",
                    "tier": 1,
                    "reasons": ["named_subject"],
                    "terms": ["apple"],
                    "evidence": [{"channel": "named_subject", "article_passage": "Apple shares rise", "matched_terms": ["apple"]}],
                }],
                "industries": [],
            }]

        import src.scanner_backtest as gate

        original = gate.link_articles
        gate.link_articles = fake
        try:
            result = run_article(self.board(), {"id": "a1", "title": "Apple shares rise", "text": "Apple shares rise"})
        finally:
            gate.link_articles = original
        self.assertEqual(seen["engine"], "evidence")
        self.assertEqual(seen["count"], 1)
        self.assertEqual(result["pass_count"], 1)
        self.assertEqual(result["passes"][0]["ticker"], "AAPL")
        self.assertEqual(ENGINE, "evidence")

    def test_live_evidence_path_matches_apple_shares(self):
        from src.stock_evidence import compile_board

        board = self.board()
        board.evidence_board = compile_board(board.source_rows)
        result = run_article(board, {
            "id": "a1",
            "title": "Apple shares rise",
            "text": "Apple shares rise after the company reports iPhone demand.",
        })
        self.assertEqual(result["engine"], "evidence-v2")
        self.assertIn("AAPL", {row["ticker"] for row in result["passes"]})
        self.assertNotIn("XOM", {row["ticker"] for row in result["passes"]})

    def test_test_button_scores_the_drawn_article(self):
        from src.stock_evidence import compile_board

        article = {
            "id": "story-1",
            "title": "Apple shares rise",
            "url": "https://example.test/apple",
            "source": "example",
            "published_at": "2026-10-17",
            "text": "Apple shares rise after the company reports iPhone demand.",
        }
        gate = Gate([article, {**article, "id": "story-2", "title": "Other", "text": "Weather stays calm over the ocean."}])
        gate.board = self.board()
        gate.board.evidence_board = compile_board(gate.board.source_rows)
        gate.ready = True
        status, drawn, _ = dispatch(gate, "GET", "/api/article")
        self.assertEqual(status, 200)
        self.assertIn(drawn["id"], {"story-1", "story-2"})
        status, result, _ = dispatch(gate, "POST", "/api/test", json.dumps({"id": "story-1"}).encode())
        self.assertEqual(status, 200)
        self.assertIn("AAPL", {row["ticker"] for row in result["passes"]})
        status, missing, _ = dispatch(gate, "POST", "/api/test", json.dumps({"id": "missing"}).encode())
        self.assertEqual(status, 404)
        cold = Gate([article])
        status, waiting, _ = dispatch(cold, "POST", "/api/test", json.dumps({"id": "story-1"}).encode())
        self.assertEqual(status, 503)

    def test_page_exposes_the_test_button(self):
        self.assertTrue(PAGE.is_file())
        html = PAGE.read_text(encoding="utf-8")
        self.assertIn(">Test<", html)
        self.assertIn("/api/test", html)
        self.assertIn("/api/article", html)
        gate = Gate([{"id": "x", "title": "t", "text": "hello"}])
        status, page, content_type = dispatch(gate, "GET", "/")
        self.assertEqual(status, 200)
        self.assertIn("text/html", content_type)
        self.assertIn("Scanner backtest", page)
        view = present({"stocks": [{"pass": False, "ticker": "XOM"}], "decision": "no_supported_stock_link", "engine": "evidence-v2"})
        self.assertEqual(view["pass_count"], 0)


if __name__ == "__main__":
    unittest.main()
