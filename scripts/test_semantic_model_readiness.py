import unittest
from diagnose_openclaw_lane import requested_model_ready

class RequestedModelReadinessTests(unittest.TestCase):
    def test_other_model_success_cannot_enable_requested_model(self):
        calls=[{'model_requested':'xai/grok-4.3','ok':True},
               {'model_requested':'xai/grok-4.6','ok':False}]
        self.assertTrue(requested_model_ready(calls,'xai/grok-4.3'))
        self.assertFalse(requested_model_ready(calls,'xai/grok-4.6'))

    def test_selected_success_is_required_and_busy_no_calls_is_not_ready(self):
        self.assertFalse(requested_model_ready([],'xai/grok-4.6'))
        self.assertTrue(requested_model_ready(
            [{'model_requested':'xai/grok-4.6','ok':True}],'xai/grok-4.6'))

if __name__=='__main__':unittest.main()
