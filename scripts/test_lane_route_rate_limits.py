"""Quota routing controls; no real model inference."""
import sys
import unittest
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src import lane_route as lane

class OpenRouterLimits(unittest.TestCase):
    def setUp(self):
        self.before = (lane._OR_DAY_CAPPED, set(lane._RATE_LIMITED), set(lane._SKIP), set(lane._MODEL_DENIED))
        lane._OR_DAY_CAPPED = False
        lane._RATE_LIMITED.clear(); lane._SKIP.clear(); lane._MODEL_DENIED.clear()
    def tearDown(self):
        lane._OR_DAY_CAPPED = self.before[0]
        for target, saved in zip((lane._RATE_LIMITED, lane._SKIP, lane._MODEL_DENIED), self.before[1:]):
            target.clear(); target.update(saved)
    def walk(self, error):
        calls = []
        def request(model):
            calls.append(model)
            return (None, 429, error) if len(calls) == 1 else ({'ok': True}, 200, model)
        with patch.object(lane.time, 'sleep'), patch.dict(lane.os.environ, {'LANE_SKIP_OPENROUTER': ''}):
            result = lane.hop_models('openrouter', ['google/gemma-4-31b-it:free', 'google/gemma-4-26b-a4b-it:free'], request)
        return result, calls
    def test_confirmed_upstream_429_tries_next_allowed_free_model(self):
        result, calls = self.walk({'message': 'Provider returned error', 'metadata': {'raw': 'google/gemma-4-31b-it:free is temporarily rate-limited upstream.'}})
        self.assertEqual(len(calls), 2)
        self.assertEqual(result[0], {'ok': True})
        self.assertFalse(lane._OR_DAY_CAPPED)
        self.assertNotIn('openrouter', lane._SKIP)
        self.assertIn('openrouter::' + calls[0], lane._RATE_LIMITED)
    def test_platform_daily_quota_stops_siblings(self):
        result, calls = self.walk('Rate limit exceeded: free-models-per-day')
        self.assertEqual(len(calls), 1); self.assertIsNone(result[0])
        self.assertTrue(lane._OR_DAY_CAPPED)
    def test_platform_minute_quota_stops_siblings(self):
        result, calls = self.walk('Rate limit exceeded: free-models-per-min')
        self.assertEqual(len(calls), 1); self.assertIsNone(result[0])
    def test_unknown_429_stays_conservative(self):
        result, calls = self.walk('Rate limit exceeded')
        self.assertEqual(len(calls), 1); self.assertIsNone(result[0])
    def test_structured_shared_pool_marker_is_upstream(self):
        self.assertTrue(lane._or_upstream_rate_limit({'metadata': {'limit_source': 'upstream_provider_shared_pool'}}))
    def test_current_floor_keeps_free_and_quality_guards(self):
        models = lane.primary_models_for('openrouter', 'news_classify')
        self.assertIn('qwen/qwen3.8-27b:free', models)
        self.assertTrue(all(lane._or_is_free(model) and not lane.is_classify_banned(model) for model in models))
        self.assertNotIn('z-ai/glm-5.2:free', lane.OR_MODELS)
        self.assertNotIn('minimax/minimax-m3:free', lane.OR_MODELS)

if __name__ == '__main__': unittest.main()
