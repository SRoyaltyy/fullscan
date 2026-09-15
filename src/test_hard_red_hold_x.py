"""Hard-red short + scoop X × hold sweep — leak-free, pick from tests.

Run: PYTHONPATH=. python3 -m src.test_hard_red_hold_x
"""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src import hard_red_hold_x as hx
from src import hard_red_sit_research as hrs
from src.test_hard_red_sit_research import (
    DATES,
    ZERO_FEES,
    _combo_panel,
    _recs,
)


class HoldXSweep(unittest.TestCase):
    def test_hold_grid_and_dip_grid_are_the_tested_space(self):
        self.assertEqual(hx.HOLD_GRID, (1, 2, 3, 5))
        self.assertEqual(tuple(hx.DIP_GRID), (0.5, 1.0, 1.5, 2.0, 3.0))
        names = hx.default_recipes()
        have = {r["name"] for r in names}
        self.assertIn("short_news_r_macd_h3", have)
        self.assertIn("union_hot_n4_h1", have)
        self.assertTrue(any(r.get("side") == "short" for r in names))
        self.assertTrue(any((r.get("side") or "long") == "long" for r in names))

    def test_name_day_fire_short_uses_open_and_horizon_close(self):
        cal = ["2026-08-13", "2026-08-14", "2026-08-15"]
        bars = {
            ("AAA", "2026-08-13"): {
                "open": 10.0, "high": 10.2, "low": 9.7, "close": 9.9,
            },
            ("AAA", "2026-08-14"): {
                "open": 9.8, "high": 10.1, "low": 9.6, "close": 9.5,
            },
        }
        row = hx.name_day_fire(
            "AAA", "2026-08-13", cal, bars=bars, fees=ZERO_FEES,
            side="short", hold=2)
        self.assertIsNotNone(row)
        self.assertAlmostEqual(row["entry"], 10.0)
        self.assertAlmostEqual(row["exit"], 9.5)
        self.assertGreater(row["pnl"], 0)
        self.assertTrue(row["win"])
        self.assertEqual(row["kind"], "open_short")

    def test_name_day_fire_long_needs_session_low_touch(self):
        cal = ["2026-08-13"]
        bars = {
            ("AAA", "2026-08-13"): {
                "open": 10.0, "high": 10.2, "low": 9.95, "close": 10.1,
            },
        }
        miss = hx.name_day_fire(
            "AAA", "2026-08-13", cal, bars=bars, fees=ZERO_FEES,
            side="long", hold=1, dip_pct=1.0)
        hit = hx.name_day_fire(
            "AAA", "2026-08-13", cal, bars=bars, fees=ZERO_FEES,
            side="long", hold=1, dip_pct=0.5)
        self.assertIsNone(miss)
        self.assertIsNotNone(hit)
        self.assertAlmostEqual(hit["entry"], 10.0 * 0.995)
        self.assertEqual(hit["kind"], "scoop")

    def test_scoop_trigger_has_no_close_path(self):
        import inspect
        names = inspect.signature(hx.name_day_fire).parameters
        self.assertNotIn("close", names)
        self.assertNotIn("last", names)

    def test_pick_cell_prefers_keep_then_watch(self):
        keep = {
            "n_fires": 40, "win_rate": 0.60, "pnl": 0.08,
            "verdict": "KEEP", "hold": 2, "dip_pct": 1.5, "mode": "dip_scoop",
        }
        watch = {
            "n_fires": 12, "win_rate": 0.58, "pnl": 0.04,
            "verdict": "WATCH", "hold": 1, "dip_pct": 1.0, "mode": "dip_scoop",
        }
        kill = {
            "n_fires": 5, "win_rate": 0.80, "pnl": 0.20,
            "verdict": "KILL", "hold": 5, "dip_pct": 3.0, "mode": "dip_scoop",
        }
        picked = hx.pick_cell([kill, watch, keep])
        self.assertEqual(picked["dip_pct"], 1.5)
        self.assertEqual(picked["picked_by"], "KEEP bar")
        self.assertEqual(hx.pick_cell([kill, watch])["hold"], 1)

    def test_run_picks_x_and_hold_from_the_grid(self):
        panel, bars, regime = _combo_panel()
        payload = hx.run(
            from_date=DATES[0], to_date=DATES[-1], write=False,
            panel=panel, bars=bars, fees=ZERO_FEES, regime=regime,
            recipes=_recs())
        self.assertGreaterEqual(payload["hard_red_n"], 1)
        self.assertGreaterEqual(payload["n_strategies"], 3)
        names = {s["name"] for s in payload["strategies"]}
        self.assertIn(hx.LIVE_COMBO, names)
        self.assertIn("short_news_r_macd_h3", names)
        self.assertIn("union_hot_n4_h1", names)
        self.assertTrue(payload["live_sit"])
        self.assertIn("flatten_robust", payload["live_untouched"])
        combo = next(s for s in payload["strategies"] if s["name"] == hx.LIVE_COMBO)
        self.assertTrue(combo.get("kids"))
        if combo.get("picked"):
            self.assertIn(combo["picked"]["hold"], hx.HOLD_GRID)
            if combo["picked"].get("dip_pct") is not None:
                self.assertIn(combo["picked"]["dip_pct"], hx.DIP_GRID)

    def test_write_mirrors_json_and_splices_markdown(self):
        payload = {
            "from_date": "2026-08-13",
            "to_date": "2026-09-14",
            "hard_red_n": 1,
            "hold_grid": [1, 2, 3, 5],
            "dip_grid": [0.5, 1.0],
            "n_keep": 1,
            "n_watch": 1,
            "n_strategies": 2,
            "note": "Paper counterfactual.",
            "keeps": [{
                "name": "union_hot_n4_h1", "side": "long",
                "picked": {
                    "mode": "dip_scoop", "hold": 2, "dip_pct": 1.5,
                    "n_fires": 40, "win_rate": 0.56, "pnl": 1.2,
                    "verdict": "KEEP",
                },
            }],
            "watches": [{
                "name": "short_news_r_macd_h3", "side": "short",
                "picked": {
                    "mode": "short_only", "hold": 1, "dip_pct": None,
                    "n_fires": 12, "win_rate": 0.58, "pnl": 0.4,
                    "verdict": "WATCH",
                },
            }],
            "strategies": [{
                "name": "union_hot_n4_h1", "side": "long",
                "picked": {
                    "mode": "dip_scoop", "hold": 2, "dip_pct": 1.5,
                    "n_fires": 40, "win_rate": 0.56, "pnl": 1.2,
                    "verdict": "KEEP",
                },
            }, {
                "name": "short_news_r_macd_h3", "side": "short",
                "picked": {
                    "mode": "short_only", "hold": 1, "dip_pct": None,
                    "n_fires": 12, "win_rate": 0.58, "pnl": 0.4,
                    "verdict": "WATCH",
                },
            }, {
                "name": "short_alarm_h3", "side": "short",
                "picked": {
                    "mode": "short_only", "hold": 5, "dip_pct": None,
                    "n_fires": 66, "win_rate": 0.36, "pnl": 391.04,
                    "verdict": "KILL",
                },
            }],
        }
        md = Path("03_scoreboard/HARD_RED_SIT.md")
        raw = md.read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "03_scoreboard" / "hard_red_sit").mkdir(parents=True)
            (root / "03_scoreboard" / "HARD_RED_SIT.md").write_text(
                raw.split("<!-- HOLD_X_BEGIN -->")[0]
                + "<!-- HOLD_X_BEGIN -->\n<!-- HOLD_X_END -->\n"
                if "<!-- HOLD_X_BEGIN -->" in raw else raw,
                encoding="utf-8",
            )
            (root / "dashboard" / "factor-mine").mkdir(parents=True)
            (root / "dashboard" / "strategy-board").mkdir(parents=True)
            (root / "dashboard" / "hard-red-sit").mkdir(parents=True)
            with patch.object(hx, "ROOT", root), \
                 patch.object(hx, "OUT_DIR", root / "03_scoreboard" / "hard_red_sit"), \
                 patch.object(hx, "OUT_JSON", root / "03_scoreboard" / "hard_red_sit" / "hard_red_hold_x.json"), \
                 patch.object(hx, "OUT_MD", root / "03_scoreboard" / "HARD_RED_SIT.md"), \
                 patch.object(hx, "DASH_FM", root / "dashboard" / "factor-mine" / "hard_red_hold_x.json"), \
                 patch.object(hx, "DASH_SB", root / "dashboard" / "strategy-board" / "hard_red_hold_x.json"), \
                 patch.object(hx, "DASH_PAGE", root / "dashboard" / "hard-red-sit" / "index.html"):
                hx.write_payload(payload)
            out = json.loads(
                (root / "03_scoreboard" / "hard_red_sit" / "hard_red_hold_x.json")
                .read_text())
            self.assertEqual(out["keeps"][0]["picked"]["dip_pct"], 1.5)
            self.assertTrue((root / "dashboard" / "hard-red-sit" / "index.html").is_file())
            self.assertTrue((root / "dashboard" / "factor-mine" / "hard_red_hold_x.json").is_file())
            spliced = (root / "03_scoreboard" / "HARD_RED_SIT.md").read_text(
                encoding="utf-8")
            self.assertIn("1.5", spliced)
            self.assertIn("hold", spliced.lower())
            self.assertIn("KEEP", spliced)


if __name__ == "__main__":
    unittest.main()
