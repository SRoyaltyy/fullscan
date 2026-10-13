import unittest
import io, json
from unittest.mock import patch
from src import lane_route, predict_grok_heal
import openclaw_oauth_guard


class ProviderRepairTests(unittest.TestCase):
    def test_database_expiry_and_search_denial_are_not_grok_auth(self):
        self.assertFalse(predict_grok_heal.is_auth_failure(
            '[db] timeout expired\n[search] HTTP 403 forbidden\n'
            '[openclaw] read timed out (240s)\n[llm] GROK_ONLY: OpenClaw failed'))

    def test_actual_gateway_auth_failure(self):
        for text in ('[openclaw] HTTP 401', '[openclaw] OAuth token expired',
                     'xAI refresh token rejected: invalid_grant'):
            self.assertTrue(predict_grok_heal.is_auth_failure(text))
        self.assertFalse(predict_grok_heal.is_auth_failure('[oauth-guard] token fresh'))

    def test_live_flash_is_classification_eligible(self):
        self.assertEqual(lane_route.primary_models_for('zhipu', 'news_classify')[0], 'glm-5.3-flash')

    def test_flash_payload_has_reasoning_budget_without_disabled_thinking(self):
        body={'choices':[{'message':{'content':'{"ok":true}'}}]}
        with patch.object(lane_route, 'http_json', return_value=(200, body, {})) as call:
            result=lane_route.openai_chat('https://example.invalid/chat/completions',
                                         'synthetic', 'glm-5.3-flash', 'return JSON', max_tokens=8000)
        self.assertEqual(result[0], {'ok':True})
        payload=call.call_args.args[1]
        self.assertEqual(payload['reasoning_effort'], 'low')
        self.assertEqual(payload['max_tokens'], 16000)
        self.assertNotIn('thinking', payload)

    def test_guard_requires_exact_real_answer(self):
        for content,expected in (('42',True),('142',False),('Error 42: timed out',False),('',False)):
            data=json.dumps({'choices':[{'message':{'content':content}}]}).encode()
            with patch.object(openclaw_oauth_guard,'gateway_token',return_value='synthetic'), \
                 patch.object(openclaw_oauth_guard.urllib.request,'urlopen',return_value=io.BytesIO(data)), \
                 patch.object(openclaw_oauth_guard,'log'):
                self.assertIs(openclaw_oauth_guard.real_completion({},'test'),expected)


if __name__ == '__main__':
    unittest.main()
