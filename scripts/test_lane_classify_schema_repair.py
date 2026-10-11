"""Malformed free-model classification gets one validated re-ask."""
import sys
import unittest
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src import lane_one_shot as client
from src.news_impact.one_shot_stack import classify_repair_note

class SchemaRepair(unittest.TestCase):
    def run_client(self, replies):
        live = client.LiveLane.__new__(client.LiveLane)
        live.ctx = {'keys': {'openrouter': 'synthetic-key'}}
        live._rejected_classify = None
        calls = []
        def ask(hop, prompt, ctx, **kwargs):
            calls.append(prompt)
            blob = replies[min(len(calls)-1, len(replies)-1)]
            return (blob, 'nvidia/nemotron-3-ultra-550b-a55b:free') if kwargs['accept'](blob) else (None, None)
        with patch.object(client, 'classify_lanes', return_value=['openrouter']), \
             patch.object(client, 'classify_models_for', return_value=['nvidia/nemotron-3-ultra-550b-a55b:free']), \
             patch.object(client.lane, 'ask_lane', side_effect=ask), patch.object(client.time, 'sleep'):
            result = live._classify('Original public article', 'Synthetic system',
                lambda b: b.get('event_class') == 'discard' and b.get('q5') == 'regime')
        return result, calls

    def test_invalid_template_enum_is_reasked_and_validated(self):
        invalid = {'event_class': 'regime', 'q5': 'impulse|regime|regime_break'}
        valid = {'event_class': 'discard', 'q5': 'regime'}
        result, calls = self.run_client([invalid, valid])
        self.assertEqual(result[0], valid)
        self.assertEqual(len(calls), 2)
        self.assertIn('invalid classification enum', calls[1])
        self.assertTrue(calls[1].startswith('Original public article'))

    def test_failed_repair_is_bounded_and_never_fills_fields(self):
        result, calls = self.run_client([{'event_class': 'regime', 'q5': 'bad'}])
        self.assertIsNone(result[0])
        self.assertEqual(len(calls), 2)

    def test_valid_unrelated_class_does_not_get_a_schema_repair(self):
        self.assertEqual(classify_repair_note({'event_class': 'discard', 'q5': 'regime'}, 'Original public article'), '')

if __name__ == '__main__': unittest.main()
