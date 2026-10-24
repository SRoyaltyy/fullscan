import unittest
from .stock_document_context import listed_names


class DocumentScopeControls(unittest.TestCase):
    def test_bare_names_in_an_authoritative_schedule_need_no_relation_verb(self):
        text='Exhibit 21.1\nSubsidiaries of the Company\nAlpha Manufacturing Inc.\nDelaware\nBeta Services LLC\nNevada\n'
        rows=listed_names({'id':'s','document':'ex21_1.htm','filing_date':'2026-01-01'},text,'Example Inc.')
        self.assertEqual([r['text'] for r in rows],['Alpha Manufacturing Inc.','Beta Services LLC'])
        self.assertTrue(all(not r['direct_ownership_established'] and r['assertion']=='reported' for r in rows))
        for row in rows:self.assertEqual(text[row['start']:row['end']],row['text'])

    def test_the_same_names_in_an_unrelated_exhibit_do_not_establish_ownership(self):
        self.assertFalse(listed_names({'id':'s','document':'ex99_1.htm'},'Alpha Manufacturing Inc.\nBeta Services LLC','Example Inc.'))

    def test_issuer_heading_cannot_become_its_own_subsidiary(self):
        rows=listed_names({'id':'s','document':'ex21.htm'},'Example Inc.\nAlpha Manufacturing Inc.','Example Inc.')
        self.assertEqual([r['text'] for r in rows],['Alpha Manufacturing Inc.'])

    def test_explicit_foreign_filer_schedule_title_is_supported_without_guessing_from_exhibit_number(self):
        rows=listed_names({'id':'s','document':'ex8_1.htm'},'Subsidiaries of the Registrant\nAlpha Manufacturing Limited','Example Inc.')
        self.assertEqual([r['text'] for r in rows],['Alpha Manufacturing Limited'])

    def test_stored_manifest_exhibit_type_overrides_filename_guessing(self):
        text='Alpha Manufacturing Inc.'
        self.assertTrue(listed_names({'id':'s','document':'unusual-name.htm','document_type':'EX-21.1'},text,'Example Inc.'))
        self.assertFalse(listed_names({'id':'s','document':'ex21.htm','document_type':'EX-10.1'},text,'Example Inc.'))


if __name__=='__main__':unittest.main()
