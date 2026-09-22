"""Lane-on-Elite tier, watermark, and env wiring. No network."""
from __future__ import annotations

import io
import os
import unittest
from contextlib import redirect_stdout
from unittest import mock

from src.lane_route import hopper_plan, is_banned_primary
from src.news_impact.lane_env import env_status, print_redacted_env, readiness
from src.news_impact.theme_radar_board import _path_text
from src.news_impact.theme_radar_lane import (
    attach_tape,
    build_report,
    is_lane_ok,
    markdown_report,
    retag_tape,
    select_tier,
    tier_reasons,
    watermark_of,
)


def _row(aid, tick, title, published, direction="up", q5="impulse",
         event_class="blast_ops", ret=1.5, lane="deterministic",
         model="news_impact_v2", source="deterministic"):
    return {
        "article_id": aid,
        "title": title,
        "published_at": published,
        "known_at": published,
        "ticker_hint": tick,
        "harvest_source": "theme_radar_elite",
        "lane": lane,
        "model": model,
        "inference_source": source,
        "classification": {
            "event_class": event_class,
            "q5": q5,
            "sign": "positive" if direction == "up" else "negative",
        },
        "entities": [{
            "ticker": tick,
            "name": tick,
            "role": "named",
            "direction": direction,
            "tradeable_expression": "direct",
            "horizon": "0-1d",
        }],
        "performance": [{
            "ticker": tick,
            "direction": direction,
            "event_class": event_class,
            "q5": q5,
            "tradeable_expression": "direct",
            "ret_1d": ret,
            "agree_1d": ret > 0 if direction == "up" else ret < 0,
            "ret_2d": ret,
            "agree_2d": ret > 0 if direction == "up" else ret < 0,
            "ret_5d": ret,
            "agree_5d": ret > 0 if direction == "up" else ret < 0,
            "skip_01d": False,
            "entry_date": "2026-09-18",
        }],
    }


class LaneTierTests(unittest.TestCase):
    def test_tier_keeps_graded_clash_amrx_and_stack(self) -> None:
        graded = _row("g1", "ACME", "Acme beats", "2026-08-06 08:00:00")
        clash = _row("c1", "BETA", "Beta offers shares", "2026-08-07 08:00:00", direction="down", ret=-2.0)
        amrx = _row(
            "a1", "AMRX",
            "Amneal Announces FDA Approval and Launch of Lanreotide Injection",
            "2026-09-18 16:01:00",
        )
        secz = _row(
            "s1", "SECZ", "SEC Sends Strong Signal",
            "2026-09-17 15:15:33",
        )
        noise = _row("n1", "ZZZZ", "Weather stays warm", "2026-08-06 08:00:00", q5="regime")
        rows = [graded, clash, amrx, secz, noise]
        picked = select_tier(rows, {"g1"}, {"c1"})
        ids = {row["article_id"] for row, _reasons in picked}
        self.assertEqual(ids, {"g1", "c1", "a1", "s1"})
        by_id = {row["article_id"]: reasons for row, reasons in picked}
        self.assertIn("graded_0_1d", by_id["g1"])
        self.assertIn("converge_or_clash", by_id["c1"])
        self.assertIn("amrx_lanreotide", by_id["a1"])
        self.assertIn("stack_2026-09-18", by_id["s1"])
        self.assertEqual(tier_reasons(noise, set(), set()), [])

    def test_stack_tickers_on_news_time(self) -> None:
        for tick in ("SECZ", "COIN", "CRCL", "SCHW"):
            row = _row("x", tick, f"{tick} wrap", "2026-09-18 18:01:00")
            self.assertIn("stack_2026-09-18", tier_reasons(row, set(), set()))

    def test_watermark_and_banned_model_is_not_lane(self) -> None:
        live = _row(
            "a", "ACME", "t", "2026-08-06 08:00:00",
            lane="zhipu", model="glm-4.7-flash", source="zhipu",
        )
        old = _row(
            "b", "ACME", "t", "2026-08-06 08:00:00",
            lane="zhipu", model="glm-4-flash-250414", source="zhipu",
        )
        det = _row("c", "ACME", "t", "2026-08-06 08:00:00")
        self.assertEqual(watermark_of(live), "zhipu::glm-4.7-flash::zhipu")
        self.assertTrue(is_lane_ok(live))
        self.assertFalse(is_lane_ok(old))
        self.assertFalse(is_lane_ok(det))
        self.assertTrue(watermark_of(det))

    def test_retag_keeps_tape_and_flips_agree(self) -> None:
        det = _row("a", "ACME", "Acme beats", "2026-08-06 08:00:00", direction="up", ret=4.0)
        lane = _row("a", "ACME", "Acme beats", "2026-08-06 08:00:00", direction="down", ret=4.0)
        retagged, missing = retag_tape(det, lane)
        self.assertEqual(missing, [])
        g = retagged["performance"][0]
        self.assertEqual(g["ret_1d"], 4.0)
        self.assertEqual(g["direction"], "down")
        self.assertIs(g["agree_1d"], False)

    def test_paired_report_same_calls_and_refuses_deterministic(self) -> None:
        det = _row("a", "ACME", "Acme beats", "2026-08-06 08:00:00", direction="up", ret=4.0)
        lane = _row(
            "a", "ACME", "Acme beats", "2026-08-06 08:00:00",
            direction="down", ret=4.0,
            lane="siliconflow", model="Qwen/Qwen3-8B", source="siliconflow",
        )
        lane = attach_tape(det, lane, fetch=False)
        report = build_report(
            [{
                "session": "2026-08-06",
                "reasons": ["graded_0_1d"],
                "det": det,
                "lane": lane,
                "attempted": True,
            }],
            {"env": {"ZHIPU_API_KEY": "PRESENT"}, "loaded_hoppers": ["siliconflow"]},
            None,
        )
        strict = report["paired"]["0-1d"]["strict"]
        self.assertEqual(strict["n_calls"], 1)
        self.assertEqual(strict["det"]["hits"], 1)
        self.assertEqual(strict["lane"]["hits"], 0)
        self.assertEqual(report["n_lane_ok"], 1)
        self.assertEqual(report["hop_histogram"], {"siliconflow::Qwen/Qwen3-8B": 1})
        self.assertIn("siliconflow::Qwen/Qwen3-8B::siliconflow", report["watermark"])
        text = markdown_report(report)
        self.assertIn("Lane-ok 1", text)
        self.assertIn("ZHIPU_API_KEY", text)
        self.assertIn("siliconflow::Qwen/Qwen3-8B", text)
        self.assertNotIn("super-secret", text)

        det_only = build_report(
            [{
                "session": "2026-08-06",
                "reasons": ["graded_0_1d"],
                "det": det,
                "lane": det,
                "attempted": True,
            }],
            {"env": {"ZHIPU_API_KEY": "PRESENT"}, "loaded_hoppers": ["zhipu"]},
            None,
        )
        self.assertEqual(det_only["n_lane_ok"], 0)
        self.assertEqual(det_only["n_deterministic_leftover"], 1)
        self.assertFalse(det_only["ship_lane"])
        self.assertIn("deterministic::news_impact_v2::deterministic", markdown_report(det_only))

    def test_path_text_does_not_call_a_skipped_run_keyless(self) -> None:
        text = _path_text(False, {"probe": {"reason": "keys_present", "ok": True}})
        self.assertNotIn("no provider keys", text)
        self.assertIn("not a Lane scoreboard", text)
        missing = _path_text(True, {"probe": {"reason": "no_keys"}, "live": 0})
        self.assertIn("not a Lane result", missing)

    def test_hop_order_never_old_flash(self) -> None:
        plan = hopper_plan("news_impact")
        names = [hop for hop, _models in plan]
        self.assertEqual(names[:4], ["zhipu", "siliconflow", "openrouter", "qwen"])
        by = dict(plan)
        self.assertEqual(by["zhipu"], ["glm-4.7-flash"])
        self.assertEqual(by["siliconflow"][0], "Qwen/Qwen3-8B")
        self.assertTrue(all(m == "openrouter/free" or str(m).endswith(":free") for m in by["openrouter"]))
        self.assertEqual(by["qwen"], ["qwen-flash"])
        blob = " ".join(m for _h, models in plan for m in models)
        self.assertNotIn("glm-4-flash-250414", blob)
        self.assertNotIn("glm-4.5-flash", blob)
        for _hop, models in plan:
            for model in models:
                self.assertFalse(is_banned_primary(model), model)

    def test_workflow_maps_secrets_and_does_not_gate_lane_off(self) -> None:
        text = (
            __import__("pathlib").Path(".github/workflows/news_impact_theme_radar_lane.yml")
            .read_text(encoding="utf-8")
        )
        for name in (
            "ZHIPU_API_KEY",
            "SILICONFLOW_API_KEY",
            "OPENROUTER_API_KEY",
            "DASHSCOPE_API_KEY",
            "TOKENHUB_API_KEY",
        ):
            self.assertIn(name, text)
        self.assertIn("env --strict", text)
        self.assertNotIn('inputs.lane', text)
        self.assertNotIn("glm-4-flash-250414", text)


class LaneEnvTests(unittest.TestCase):
    def test_print_is_redacted(self) -> None:
        with mock.patch.dict(os.environ, {"ZHIPU_API_KEY": "super-secret-value"}, clear=False):
            buf = io.StringIO()
            with redirect_stdout(buf):
                status = print_redacted_env()
            text = buf.getvalue()
        self.assertIn("ZHIPU_API_KEY: PRESENT", text)
        self.assertNotIn("super-secret-value", text)
        self.assertEqual(status["ZHIPU_API_KEY"], "PRESENT")

    def test_github_token_alone_is_not_ready(self) -> None:
        saved = {k: os.environ.get(k) for k in list(os.environ)}
        try:
            for key in list(os.environ):
                if key.endswith("_API_KEY") or key.endswith("_TOKEN") or key.endswith("_URL") or "TOKENHUB" in key or "DASHSCOPE" in key:
                    os.environ.pop(key, None)
            os.environ["GITHUB_TOKEN"] = "ghs_not_a_hopper"
            info = readiness()
            self.assertFalse(info["ready"])
            self.assertFalse(info["bug"])
            self.assertTrue(info["github_token_ignored"])
        finally:
            os.environ.clear()
            os.environ.update({k: v for k, v in saved.items() if v is not None})

    def test_present_key_missed_by_load_keys_is_a_bug(self) -> None:
        with mock.patch.dict(os.environ, {"ZHIPU_API_KEY": "present-but-unread"}, clear=False):
            with mock.patch("src.lane_route.load_keys", return_value=({}, "", "")):
                info = readiness()
        self.assertTrue(info["bug"])
        self.assertIn("zhipu", info["missed_hoppers"])
        self.assertFalse(info["ready"])
        self.assertNotIn("present-but-unread", str(info))

    def test_env_status_has_no_values(self) -> None:
        status = env_status()
        self.assertIn(status["OPENROUTER_API_KEY"], {"PRESENT", "MISSING"})
        self.assertNotIn("=", "".join(status.values()))


if __name__ == "__main__":
    unittest.main()
