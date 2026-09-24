"""Theme-radar slim ingest. AMRX 09-18 16:01 is the acceptance row."""
from __future__ import annotations

import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

from src.news_impact.corpus import load_all_sources
from src.news_impact.grade import grade_one
from src.news_impact.pipeline import analyze_article
from src.news_impact.theme_radar import (
    _rows_from_text,
    amrx_acceptance,
    dedupe_elite,
    load_theme_radar,
    since_prior_snapshot,
    snapshot_clock,
)

SAMPLE = """Ticker,Company,Industry,Sector,News Title,Daily Digest,News Time
AMRX,Amneal Pharmaceuticals Inc,Drug Manufacturers - Specialty & Generic,Healthcare,Amneal Announces FDA Approval and Launch of Lanreotide Injection,Amneal wins FDA approval and begins U.S. launch of generic lanreotide injection a Somatuline Depot alternative,2026-09-18 16:01:00
AAPL,Apple Inc,Consumer Electronics,Technology,,, 
META,Meta Platforms,Internet Content,Communication Services,Meta Connect kicks off Wednesday,Glasses and Muse AI,2026-09-17 08:00:00
"""


class ThemeRadarIngestTests(unittest.TestCase):
    def test_skips_blank_news_title(self) -> None:
        rows = _rows_from_text(SAMPLE, "mem", "2026-09-18")
        ticks = {r["ticker_hint"] for r in rows}
        self.assertIn("AMRX", ticks)
        self.assertIn("META", ticks)
        self.assertNotIn("AAPL", ticks)

    def test_amrx_clock(self) -> None:
        rows = _rows_from_text(SAMPLE, "mem", "2026-09-18")
        amrx = [r for r in rows if r["ticker_hint"] == "AMRX"][0]
        self.assertEqual(amrx["published_at"], "2026-09-18 16:01:00")
        self.assertEqual(amrx["harvest_source"], "theme_radar_elite")
        self.assertIn("Lanreotide", amrx["title"])

    def test_load_from_env_root(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            snap = Path(td) / "data" / "snapshots"
            snap.mkdir(parents=True)
            (snap / "2026-09-18.csv").write_text(SAMPLE, encoding="utf-8")
            with patch.dict("os.environ", {"THEME_RADAR_ROOT": td}):
                with patch("src.news_impact.theme_radar._roots", return_value=[snap]):
                    arts = load_theme_radar("2026-09-18", allow_remote=False)
            self.assertEqual(len(arts), 2)
            proof = amrx_acceptance(arts)
            self.assertTrue(proof["ok"], proof)

    def test_amrx_friday_1601_enters_monday(self) -> None:
        et = ZoneInfo("America/New_York")
        when = datetime(2026, 9, 18, 16, 1, tzinfo=et)
        self.assertEqual(when.strftime("%A"), "Friday")
        bars = [
            {"date": "2026-09-18", "open": 9.0, "high": 9.2, "low": 8.8, "close": 9.1},
            {"date": "2026-09-21", "open": 10.0, "high": 11.2, "low": 9.9, "close": 11.0},
            {"date": "2026-09-22", "open": 11.0, "high": 11.4, "low": 10.8, "close": 11.2},
            {"date": "2026-09-23", "open": 11.2, "high": 11.5, "low": 11.0, "close": 11.3},
            {"date": "2026-09-24", "open": 11.3, "high": 11.6, "low": 11.1, "close": 11.4},
            {"date": "2026-09-25", "open": 11.4, "high": 12.0, "low": 11.2, "close": 11.8},
            {"date": "2026-09-28", "open": 11.8, "high": 12.2, "low": 11.6, "close": 12.0},
        ]
        target = {
            "ticker": "AMRX",
            "name": "AMRX",
            "direction": "up",
            "horizon": "1-6m",
            "kind": "ticker",
            "q5": "impulse",
            "event_class": "gate",
            "tradeable_expression": "direct",
        }
        g = grade_one(target, bars, when)
        self.assertEqual(g["entry_date"], "2026-09-21")
        self.assertNotEqual(g["entry_date"], "2026-09-18")
        self.assertEqual(g["ret_1d"], 10.0)
        self.assertEqual(g["ret_2d"], 13.0)  # 11.3 / 10 - 1
        self.assertIsNotNone(g["ret_5d"])
        ten = datetime(2026, 9, 18, 10, 0, tzinfo=et)
        self.assertEqual(grade_one(target, bars, ten)["entry_date"], "2026-09-21")
        early = datetime(2026, 9, 18, 9, 29, tzinfo=et)
        self.assertEqual(grade_one(target, bars, early)["entry_date"], "2026-09-18")

    def test_amrx_router_lists_up(self) -> None:
        rows = _rows_from_text(SAMPLE, "mem", "2026-09-18")
        amrx = [r for r in rows if r["ticker_hint"] == "AMRX"][0]
        self.assertIn("$AMRX", amrx["body"])
        row = analyze_article(amrx, use_lane=False, use_search=False, persist=False)
        self.assertEqual(row["classification"]["event_class"], "gate")
        self.assertEqual(row["classification"]["q5"], "impulse")
        ents = [e for e in row["entities"] if e.get("ticker") == "AMRX"]
        self.assertTrue(ents)
        self.assertEqual(ents[0]["direction"], "up")
        self.assertEqual(ents[0]["tradeable_expression"], "direct")

    def test_date_all_vendor_snapshots_increment_elite(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            snap = Path(td)
            (snap / "2026-09-18.csv").write_text(SAMPLE, encoding="utf-8")
            (snap / "2026-09-21.csv").write_text(SAMPLE, encoding="utf-8")
            empty = Path(td) / "empty"
            empty.mkdir()
            loaders = [
                "load_parsed_tagged",
                "load_finviz_exports",
                "load_finviz_digests",
                "load_events",
                "load_actions_keep",
                "load_grok_dumps",
            ]
            patches = [patch(f"src.news_impact.corpus.{name}", return_value=[]) for name in loaders]
            for p in patches:
                p.start()
            try:
                with patch("src.news_impact.theme_radar._roots", return_value=[snap]):
                    raw, meta = load_all_sources("all")
                n = meta["by_harvest_source"].get("theme_radar_elite", 0)
                self.assertGreater(n, 0)
                # 09-21 repeats the same News Times. Both are on or before the
                # 09-18 clock, so only the first snapshot's two titles stay.
                self.assertEqual(n, 2)
                with patch("src.news_impact.theme_radar._roots", return_value=[empty]):
                    raw0, meta0 = load_all_sources("all")
                self.assertEqual(meta0["by_harvest_source"].get("theme_radar_elite", 0), 0)
                self.assertGreater(n, meta0["by_harvest_source"].get("theme_radar_elite", 0))
                self.assertGreater(len(raw), len(raw0))
            finally:
                for p in patches:
                    p.stop()
        packed = load_theme_radar("2026-09-18", allow_remote=False)
        proof = amrx_acceptance(packed)
        self.assertTrue(proof["ok"], proof)
        self.assertGreater(proof["n_titles_0918"], 0)

    def test_stale_bar_is_not_the_entry(self) -> None:
        et = ZoneInfo("America/New_York")
        when = datetime(2022, 5, 2, 8, 0, tzinfo=et)
        bars = [
            {"date": "2024-03-04", "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5},
            {"date": "2024-03-05", "open": 10.5, "high": 11.0, "low": 10.0, "close": 10.8},
        ]
        g = grade_one(
            {"ticker": "AMRX", "direction": "up", "kind": "ticker",
             "q5": "impulse", "event_class": "blast_ops",
             "tradeable_expression": "direct"},
            bars, when,
        )
        self.assertIsNone(g["entry_date"])
        self.assertIsNone(g["ret_1d"])
        self.assertIn("no session", g["note"])

    def test_scrape_ts_keeps_headlines_since_prior_snapshot(self) -> None:
        prior = """Ticker,News Title,Daily Digest,News Time,scrape_ts
AAA,Older headline already in the prior export,digest,2026-08-01 09:00:00,2026-09-23T23:05:42+00:00
BBB,Headline published before the prior scrape,digest,2026-09-23 18:00:00,2026-09-23T23:05:42+00:00
"""
        latest = """Ticker,News Title,Daily Digest,News Time,scrape_ts
AAA,Older headline already in the prior export,digest,2026-08-01 09:00:00,2026-09-24T20:47:29+00:00
BBB,Headline published before the prior scrape,digest,2026-09-23 18:00:00,2026-09-24T20:47:29+00:00
CCC,Fresh headline published after the prior scrape,digest,2026-09-24 15:00:00,2026-09-24T20:47:29+00:00
DDD,Headline stamped after this scrape,digest,2026-09-24 18:00:00,2026-09-24T20:47:29+00:00
EEE,Undated headline with no news time,digest,,2026-09-24T20:47:29+00:00
"""
        self.assertEqual(
            snapshot_clock(prior).strftime("%Y-%m-%d %H:%M"),
            "2026-09-23 19:05",
        )
        anchor = snapshot_clock(latest)
        self.assertEqual(anchor.strftime("%Y-%m-%d %H:%M"), "2026-09-24 16:47")
        kept = [
            row["ticker_hint"]
            for row in _rows_from_text(latest, "mem", "2026-09-24")
            if since_prior_snapshot(row["published_at"], anchor, snapshot_clock(prior))
        ]
        self.assertEqual(kept, ["CCC"])
        with tempfile.TemporaryDirectory() as td:
            snap = Path(td) / "data" / "snapshots"
            snap.mkdir(parents=True)
            (snap / "2026-09-23.csv").write_text(prior, encoding="utf-8")
            (snap / "2026-09-24.csv").write_text(latest, encoding="utf-8")
            with patch.dict("os.environ", {"THEME_RADAR_ROOT": td}):
                arts = load_theme_radar("2026-09-24", allow_remote=False)
            self.assertEqual([a["ticker_hint"] for a in arts], ["CCC"])

    def test_missing_prior_trading_day_uses_72h_not_older_file(self) -> None:
        """2026-08-27 has no snapshot. 08-28 must not borrow 08-26's clock."""
        older = """Ticker,News Title,Daily Digest,News Time
AAA,Headline from the snapshot before the gap day,digest,2026-08-26 20:00:00
"""
        gap = """Ticker,News Title,Daily Digest,News Time
AAA,Headline from the snapshot before the gap day,digest,2026-08-26 10:00:00
BBB,Headline inside the seventy two hour fallback,digest,2026-08-28 12:00:00
CCC,Headline older than seventy two hours,digest,2026-08-24 08:00:00
"""
        with tempfile.TemporaryDirectory() as td:
            snap = Path(td) / "data" / "snapshots"
            snap.mkdir(parents=True)
            (snap / "2026-08-26.csv").write_text(older, encoding="utf-8")
            (snap / "2026-08-28.csv").write_text(gap, encoding="utf-8")
            with patch("src.news_impact.theme_radar._roots", return_value=[snap]):
                arts = load_theme_radar("2026-08-28", allow_remote=False)
            self.assertEqual([a["ticker_hint"] for a in arts], ["AAA", "BBB"])

    def test_lane_harvest_drops_old_elite_headlines(self) -> None:
        import gzip

        from src.lane_one_shot import harvest

        prior = """Ticker,News Title,Daily Digest,News Time,scrape_ts
AAA,Older headline already in the prior export day,digest,2026-08-01 09:00:00,2026-09-23T23:05:42+00:00
"""
        latest = """Ticker,News Title,Daily Digest,News Time,scrape_ts
AAA,Older headline already in the prior export day,digest,2026-08-01 09:00:00,2026-09-24T20:47:29+00:00
CCC,Fresh headline published after the prior scrape time,digest,2026-09-24 15:00:00,2026-09-24T20:47:29+00:00
"""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            elite = root / "data" / "theme_radar_snapshots"
            elite.mkdir(parents=True)
            for name, body in (("2026-09-23.csv.gz", prior), ("2026-09-24.csv.gz", latest)):
                with gzip.open(elite / name, "wt", encoding="utf-8") as fh:
                    fh.write(body)
            rows = harvest(root)
        elite_rows = [r for r in rows if r.get("harvest_source") == "elite_snapshot"]
        self.assertEqual([r["ticker_hint"] for r in elite_rows], ["CCC"])

    def test_dedupe_keeps_earliest_news_time(self) -> None:
        rows = _rows_from_text(SAMPLE, "mem", "2026-09-18")
        later = dict(rows[0])
        later["published_at"] = "2026-09-21 09:00:00"
        earlier = dict(rows[0])
        earlier["published_at"] = "2026-09-18 16:01:00"
        unique = dedupe_elite([later, earlier])
        amrx = [r for r in unique if r["ticker_hint"] == "AMRX"]
        self.assertEqual(len(amrx), 1)
        self.assertEqual(amrx[0]["published_at"], "2026-09-18 16:01:00")


if __name__ == "__main__":
    unittest.main()
