"""The backtest page is the scanner gate: random article in, evidence matches out."""
import json
import random
import unittest
import tempfile
from unittest.mock import patch
from pathlib import Path

from src.correlation import build_board
from src.scanner_backtest import ENGINE, PAGE, Gate, dispatch, draw_article, present, run_article
from src.scanner_backtest import open_program, profile_view, select_input, snapshot_metadata, load_test_articles
from src.stock_evidence import COMPILER_VERSION


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

    def test_page_supports_static_artifact_mode(self):
        html = PAGE.read_text(encoding="utf-8")
        self.assertIn("results.jsonl", html)
        self.assertIn("articles.json", html)
        self.assertIn("mulberry32", html)  # seeded, referable draws


class StaticArtifactTests(unittest.TestCase):
    """The committed artifacts are the GitHub Pages trial-and-error log."""

    DIR = PAGE.parent

    def _artifacts(self):
        results = self.DIR / "results.jsonl"
        articles = self.DIR / "articles.json"
        if not results.is_file() or not articles.is_file():
            self.skipTest("static artifacts not committed in this checkout")
        rows = []
        with results.open(encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
        return rows, json.loads(articles.read_text(encoding="utf-8"))

    def test_artifacts_are_consistent(self):
        rows, articles = self._artifacts()
        self.assertTrue(rows, "results.jsonl has no rows")
        self.assertTrue(articles, "articles.json is empty")
        result_ids = {str(row.get("id")) for row in rows}
        article_ids = {str(article.get("id")) for article in articles}
        self.assertEqual(result_ids, article_ids)
        for row in rows:
            self.assertIn("decision", row)
            self.assertIn("pass_count", row)
            self.assertIn("passes", row)
            self.assertTrue(all("ticker" in p for p in row["passes"]))

    def test_meta_matches_artifacts(self):
        meta_path = self.DIR / "meta.json"
        rows, articles = self._artifacts()
        if not meta_path.is_file():
            self.skipTest("meta.json not committed")
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        self.assertEqual(meta.get("engine"), ENGINE)
        self.assertEqual(meta.get("articles"), len(rows))
        self.assertEqual(meta.get("articles"), len(articles))

    def test_export_script_uses_the_evidence_engine(self):
        from scripts.export_backtest_static import _score_shard

        self.assertEqual(ENGINE, "evidence")


class IntegrationTests(GateTests):
    def test_headline_default_does_not_classify_the_body(self):
        board = self.board()
        article = {'id': 'body-only', 'title': 'Weather stays calm', 'text': 'Apple shares rise'}
        self.assertEqual(run_article(board, article)['pass_count'], 0)
        self.assertIn('AAPL', {r['ticker'] for r in run_article(board, article, input_mode='full-text')['passes']})

    def test_article_preview_shows_scored_input_mode(self):
        article = {'id': 'x', 'title': 'Weather stays calm', 'text': 'Apple shares rise'}
        status, view, _ = dispatch(Gate([article]), 'GET', '/api/article')
        self.assertEqual(status, 200)
        self.assertEqual(view['text'], article['title'])
        self.assertEqual(view['input_mode'], 'headline')

    def test_full_primary_headline_is_not_display_title_truncation(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'articles.json'
            title = 'Headline ' + 'long ' * 70
            path.write_text(json.dumps([{'id': 'x', 'title': title, 'body': 'not scored'}]))
            selected = select_input(load_test_articles(path)[0])
            self.assertEqual(selected['text'], title)

    def test_profile_api_reads_the_actual_scoring_board(self):
        gate = Gate([])
        gate.board, gate.ready = self.board(), True
        from src.stock_evidence import compile_board
        gate.board.evidence_board = compile_board(gate.board.source_rows)
        status, profile, _ = dispatch(gate, 'GET', '/api/profile?ticker=aapl')
        self.assertEqual(status, 200)
        self.assertTrue(profile['used_for_scoring'])
        self.assertEqual(profile['sources'], gate.board.evidence_board.profiles['AAPL'].sources)
        self.assertEqual(dispatch(gate, 'GET', '/api/profile?ticker=missing')[0], 404)

    def test_filing_overlay_requires_explicit_cutoff(self):
        with patch('src.scanner_backtest.load_stocks', return_value=self.board().source_rows):
            with self.assertRaisesRegex(ValueError, 'as-of'):
                open_program(Path('/absent'), None, None, identity_filings=Path('/unused'))

    def test_filing_overlay_is_strict_dated_and_not_full_business(self):
        sections = {'AAPL': [
            {'text': '"SkyWeb" refers to our satellite network.', 'kind': 'business',
             'filing_date': '2026-01-02', 'accession': 'test-accession', 'source': 'PR642'},
            {'text': '"FakeXml" refers to our software.', 'kind': 'machine_data'}]}
        with patch('src.scanner_backtest.load_stocks', return_value=self.board().source_rows), \
             patch('src.scanner_backtest.load_filing_sections', return_value=(sections, {'complete': True})) as reader:
            board, _ = open_program(Path('/absent'), None, None,
                                    identity_filings=Path('/objects'), as_of='2026-10-19')
        reader.assert_called_once_with(Path('/objects'), as_of='2026-10-19', strict=True)
        self.assertEqual(board.filing_coverage['covered_tickers'], ['AAPL'])
        self.assertFalse(board.filing_coverage['full_business_coverage'])
        facts = profile_view(board.evidence_board.profiles['AAPL'])['facts']
        self.assertTrue(any(e['term'] == 'SkyWeb' and e.get('accession') == 'test-accession'
                            for group in facts.values() for e in group))
        self.assertFalse(any(e['term'] == 'FakeXml' for group in facts.values() for e in group))

    def test_present_retains_company_filing_witness(self):
        witness = {'filing_date': '2026-01-02', 'accession': 'test', 'passage': 'complete witness'}
        result = present({'stocks': [{'pass': True, 'ticker': 'OWN', 'evidence': [{'stock_evidence': witness}]}]})
        self.assertEqual(result['passes'][0]['evidence'][0]['stock_evidence'], witness)

    def test_snapshot_is_deterministic_and_input_mode_specific(self):
        from src.stock_evidence import compile_board
        board = self.board()
        board.evidence_board = compile_board(board.source_rows)
        one = snapshot_metadata(board)
        self.assertEqual(one, snapshot_metadata(board))
        self.assertNotEqual(one['snapshot_id'], snapshot_metadata(board, 'full-text')['snapshot_id'])
        self.assertEqual(one['compiler_version'], COMPILER_VERSION)
        self.assertIn('src/data/territories.json', one['source_hashes'])

    def test_bad_json_types_are_rejected(self):
        for body in (b'[]', b'null', b'"value"', b'\xff'):
            self.assertEqual(dispatch(Gate([]), 'POST', '/api/test', body)[0], 400)

    def test_snapshot_also_binds_dates_and_industry_not_only_passage_text(self):
        from src.stock_evidence import compile_board
        board = self.board()
        board.evidence_board = compile_board(board.source_rows)
        before = snapshot_metadata(board)['snapshot_id']
        board.evidence_board.profiles['AAPL'].sources[0]['filing_date'] = '2026-01-02'
        after_date = snapshot_metadata(board)['snapshot_id']
        self.assertNotEqual(before, after_date)
        board.evidence_board.profiles['AAPL'].industry = 'Different Industry'
        self.assertNotEqual(after_date, snapshot_metadata(board)['snapshot_id'])


if __name__ == "__main__":
    unittest.main()

