import unittest
from .news_impact.classify import classify_text

class LegalVerbBoundaryTests(unittest.TestCase):
    def test_issued_warning_does_not_mean_sued(self):
        result=classify_text('Retailer issues product recall', 'Drivers were issued a warning.')
        self.assertEqual(result.event_class, 'product_harm')

    def test_actual_sued_verb_remains_legal(self):
        result=classify_text('Example Corp sued over alleged misconduct', 'A complaint was filed.')
        self.assertEqual(result.event_class, 'blast_legal')

if __name__ == '__main__':
    unittest.main()
