"""Theme-radar slim ingest. AMRX 09-18 16:01 is the acceptance row."""
from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.news_impact.theme_radar import _rows_from_text, amrx_acceptance, load_theme_radar

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
                arts = load_theme_radar("2026-09-18", allow_remote=False)
            self.assertEqual(len(arts), 2)
            proof = amrx_acceptance(arts)
            self.assertTrue(proof["ok"], proof)


if __name__ == "__main__":
    unittest.main()
