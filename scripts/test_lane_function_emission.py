"""Capability-bound result emission; mocked transport is not a frontier pass."""
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src import lane_route as lane
from src.lane_one_shot import LiveLane
from src.news_impact.emission_schema import emission_schema
from src.news_impact.one_shot_stack import classify_acceptable
from src.news_impact.meta_hop import meta_acceptable

URL = 'https://openrouter.ai/api/v1/chat/completions'
MODEL = 'nvidia/nemotron-3-ultra-550b-a55b:free'

def result(blob, name='emit_lane_result'):
    return {'choices': [{'finish_reason': 'tool_calls', 'message': {'content': None, 'tool_calls': [{
        'type': 'function', 'function': {'name': name, 'arguments': json.dumps(blob)}}]}}]}

class FunctionEmission(unittest.TestCase):
    def call(self, response, **options):
        with patch.object(lane, 'http_json', return_value=(200, response, {})) as request:
            parsed, status, info = lane.openai_chat(URL, 'synthetic-key', MODEL,
                'Synthetic public article', response_schema=emission_schema('classify'), **options)
        return parsed, request

    def test_declared_function_emits_json_without_enabling_actions(self):
        blob = {'event_class': 'discard', 'q5': 'regime'}
        parsed, request = self.call(result(blob))
        self.assertEqual(parsed, blob)
        self.assertEqual(request.call_count, 1)
        payload = request.call_args.args[1]
        self.assertNotIn('response_format', payload)
        self.assertEqual(payload['tool_choice']['function']['name'], 'emit_lane_result')
        self.assertFalse(payload['parallel_tool_calls'])
        self.assertEqual(len(payload['tools']), 1)

    def test_unknown_multiple_or_nonobject_functions_are_not_results(self):
        wrong = result({'event_class': 'discard'}, 'external_action')
        multiple = result({'event_class': 'discard'})
        multiple['choices'][0]['message']['tool_calls'] *= 2
        for body in (wrong, multiple, result([]), result('not an object')):
            parsed, request = self.call(body)
            self.assertIsNone(parsed)
            self.assertEqual(request.call_count, 3)
        malformed = result({})
        malformed['choices'][0]['message']['tool_calls'][0]['function']['arguments'] = '{bad'
        self.assertIsNone(lane._emitted_result(malformed))
        truncated = result({'event_class': 'discard'})
        truncated['choices'][0]['finish_reason'] = 'length'
        self.assertIsNone(lane._emitted_result(truncated))

    def test_token_retry_keeps_schema_and_exact_free_model(self):
        parsed, request = self.call({'choices': [{'message': {'content': ''}}]})
        self.assertIsNone(parsed)
        for call in request.call_args_list:
            payload = call.args[1]
            self.assertEqual(payload['model'], MODEL)
            self.assertIn('tools', payload)
        self.assertEqual(request.call_args.args[1]['max_tokens'], 1200)

    def test_unsupported_schema_falls_back_once_to_existing_plain_route(self):
        plain = {'choices': [{'message': {'content': '{"event_class":"discard","q5":"regime"}'}}]}
        with patch.object(lane, 'http_json', side_effect=[(400, {'error': 'unsupported tool choice'}, {}), (200, plain, {})]) as request:
            parsed, _, _ = lane.openai_chat(URL, 'synthetic-key', MODEL, 'Public news',
                response_schema=emission_schema('classify'))
        self.assertEqual(parsed['event_class'], 'discard')
        self.assertEqual(request.call_count, 2)
        self.assertNotIn('tools', request.call_args.args[1])
        self.assertNotIn('response_format', request.call_args.args[1])

    def test_unconfirmed_endpoints_keep_existing_transport(self):
        plain = {'choices': [{'message': {'content': '{"ok":true}'}}]}
        for url, model in ((URL, 'google/gemma-4-31b-it:free'), ('https://example.com', MODEL)):
            with patch.object(lane, 'http_json', return_value=(200, plain, {})) as request:
                lane.openai_chat(url, 'synthetic-key', model, 'Public news', response_schema=emission_schema('meta'))
            self.assertNotIn('tools', request.call_args.args[1])
            self.assertIn('response_format', request.call_args.args[1])

    def test_schema_is_not_a_semantic_pass(self):
        invalid = {'event_class': 'regime', 'q5': 'impulse|regime|regime_break'}
        parsed, _ = self.call(result(invalid))
        self.assertFalse(classify_acceptable(parsed, 'Treasury sanctions an agency', 'Transactions restricted.'))
        meta = {'m1': {'need_context': 'no'}, 'm2': [{'slot': 'H', 'noun': 'agency',
            'question': 'Who should I buy?', 'changes': 'direction', 'blocks': ['direction']}], 'm3_dropped': []}
        self.assertFalse(meta_acceptable(meta, 'Treasury sanctions an agency', 'Transactions restricted.', 'structure'))
        live = LiveLane.__new__(LiveLane)
        self.assertEqual(live._emission_options('openclaw', 'classify'), {})
        self.assertEqual(live._emission_options('openrouter', 'pack_complete'), {})
        self.assertEqual(live._emission_options('openrouter', 'meta')['response_schema']['properties']['m2']['maxItems'], 6)

if __name__ == '__main__': unittest.main()
