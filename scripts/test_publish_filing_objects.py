import unittest
from scripts.publish_filing_objects import configuration, REQUIRED


class ObjectPublicationControls(unittest.TestCase):
    def test_missing_configuration_reports_only_names(self):
        state = configuration({'R2_ACCESS_KEY_ID':'sensitive-value'})
        self.assertFalse(state['configured'])
        self.assertNotIn('sensitive-value',repr(state))
        self.assertIn('R2_BUCKET',state['missing'])

    def test_https_and_no_embedded_credentials_required(self):
        env = {key:'configured' for key in REQUIRED}
        env.update(R2_ENDPOINT_URL='https://storage.example',R2_PUBLIC_BASE_URL='https://public.example')
        self.assertTrue(configuration(env)['configured'])
        for url in ('http://public.example','https://user:password@public.example','https://public.example?secret=x'):
            with self.assertRaises(ValueError):
                configuration({**env,'R2_PUBLIC_BASE_URL':url})
