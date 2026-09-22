"""Convergence / clash ledger. One entity per session, reprints are one vote."""
from __future__ import annotations

import unittest

from src.news_impact.ledger import (
    amrx_week,
    build_ledger,
    channel_of,
    label_of,
    story_id,
)


def _row(title, tick, direction, published, role="named", q5="impulse",
         event_class="blast_ops", expr="direct", stays_out=False,
         ret=None, entry="2026-09-21"):
    perf = []
    if ret is not None:
        perf.append({
            "ticker": tick,
            "entry_date": entry,
            "direction": direction,
            "ret_1d": ret,
            "ret_2d": ret,
            "ret_3d": None,
            "ret_4d": None,
            "ret_5d": None,
        })
    return {
        "title": title,
        "published_at": published,
        "ticker_hint": tick,
        "harvest_source": "theme_radar_elite",
        "classification": {
            "event_class": event_class,
            "q5": q5,
            "sign": "open",
        },
        "entities": [{
            "ticker": tick,
            "name": tick,
            "role": role,
            "direction": direction,
            "tradeable_expression": expr,
            "stays_out": stays_out,
        }],
        "performance": perf,
    }


class LedgerTests(unittest.TestCase):
    def test_label_and_story_normalize(self) -> None:
        self.assertEqual(label_of(1, 0), "singleton")
        self.assertEqual(label_of(2, 0), "converge_up")
        self.assertEqual(label_of(0, 3), "converge_down")
        self.assertEqual(label_of(2, 1), "clash")
        self.assertEqual(
            story_id("Amneal Announces FDA Approval!"),
            story_id("amneal announces fda approval"),
        )

    def test_reprint_is_one_vote_and_weather_does_not_count(self) -> None:
        a = _row(
            "Acme beats and raises guidance", "ACME", "up",
            "2026-09-18 16:01:00", ret=2.0,
        )
        wrap = _row(
            "Acme beats, and raises guidance!", "ACME", "up",
            "2026-09-18 18:00:00", ret=2.0,
        )
        weather = _row(
            "Acme still sliding as the tape digests the beat", "ACME", "down",
            "2026-09-18 19:00:00", q5="regime", event_class="regime_state", ret=-1.0,
        )
        book = build_ledger([a, wrap, weather])
        self.assertEqual(book["counts"]["singleton"], 1)
        self.assertEqual(book["counts"]["converge"], 0)
        self.assertEqual(book["counts"]["clash"], 0)

    def test_converge_and_clash_grade_the_entity_once(self) -> None:
        up1 = _row("Acme wins a contract award", "ACME", "up",
                   "2026-09-18 11:00:00", ret=4.0)
        up2 = _row("Acme books a second award", "ACME", "up",
                   "2026-09-18 15:00:00", ret=4.0)
        down = _row("Other name cuts guidance", "ZZZ", "down",
                    "2026-09-18 12:00:00", ret=-3.0)
        down2 = _row("Other name misses and guides lower", "ZZZ", "down",
                     "2026-09-18 13:00:00", ret=-3.0)
        clash_a = _row("Beta soars on a beat", "BETA", "up",
                       "2026-09-18 10:00:00", ret=1.0)
        clash_b = _row("Beta guides down for the year", "BETA", "down",
                       "2026-09-18 14:00:00", ret=1.0)
        book = build_ledger([up1, up2, down, down2, clash_a, clash_b])
        self.assertEqual(book["counts"]["converge"], 2)
        self.assertEqual(book["counts"]["clash"], 1)
        self.assertEqual(book["counts"]["singleton"], 0)
        # Two up stories, one entity, one 0-1d hit — not two.
        self.assertEqual(book["hit_rates"]["converge"]["0-1d"]["n"], 2)
        self.assertEqual(book["hit_rates"]["converge"]["0-1d"]["hits"], 2)
        # Clash net is 0 (1 up, 1 down) so it is not a directional grade.
        self.assertEqual(book["hit_rates"]["clash"]["0-1d"]["n"], 0)
        tickers = {row["ticker"] for row in book["top_converge"]}
        self.assertIn("ACME", tickers)
        self.assertIn("ZZZ", tickers)

    def test_indirect_role_is_not_a_direct_name(self) -> None:
        self.assertEqual(channel_of({"role": "named", "tradeable_expression": "direct"}), "direct")
        self.assertEqual(channel_of({
            "role": "arms_dealer", "tradeable_expression": "direct", "stays_out": True,
        }), "indirect")
        self.assertEqual(channel_of({
            "role": "substitute", "tradeable_expression": "direct",
        }), "indirect")
        row = _row(
            "Airlines scramble and rent cars", "CAR", "up",
            "2026-09-18 08:00:00", role="substitute", ret=1.5,
        )
        book = build_ledger([row])
        self.assertEqual(book["counts"]["singleton"], 1)

    def test_router_theme_is_not_an_invented_trade(self) -> None:
        row = _row(
            "CPI comes in hot", "QQQ", "down",
            "2026-09-18 08:30:00", event_class="factor_impulse", ret=-0.4,
        )
        row["classification"]["factor"] = "inflation"
        row["macro_factor"] = "inflation"
        row["sectors"] = ["Technology"]
        book = build_ledger([row])
        self.assertEqual(book["n_theme_groups"], 1)
        self.assertEqual(book["counts"]["singleton"], 1)
        self.assertNotIn("Technology", {g["key"] for g in book["top_converge"]})

    def test_amrx_week_second_story(self) -> None:
        lan = _row(
            "Amneal Announces FDA Approval and Launch of Lanreotide Injection",
            "AMRX", "up", "2026-09-18 16:01:00",
            event_class="gate", ret=6.33,
        )
        alone = build_ledger([lan])
        self.assertFalse(alone["amrx_week"]["stacked_same_session"])
        other = _row(
            "Amneal prices a notes offering", "AMRX", "down",
            "2026-09-18 18:05:00", event_class="dilution", ret=6.33,
        )
        stacked = build_ledger([lan, other])
        self.assertTrue(stacked["amrx_week"]["stacked_same_session"])
        self.assertEqual(stacked["amrx_week"]["same_session"]["label"], "clash")
        self.assertGreaterEqual(len(stacked["amrx_week"]["week"]), 1)


if __name__ == "__main__":
    unittest.main()
