import binascii
import gzip
import json
import os
import pathlib
import tempfile
import unittest
from unittest.mock import patch

from src.company_filings.full_extract import extract_document, file_bytes, full_html_text, split_submission
from src.company_filings.full_archive import (LocalObjects, WholeClient, archive_filing,
    archive_company, columnar_rows, read_filing, read_original, select_whole_filings)

ACCESSION='0000000001-26-000001'
FILING={'accession':ACCESSION,'form':'10-K','native_form':'10-K','filing_date':'2026-02-20','report_date':'2025-12-31','primary':'annual.htm'}


def submission(documents):
    chunks=[b'<SEC-DOCUMENT>\n<SEC-HEADER>\n<ACCEPTANCE-DATETIME>20260220121500\n</SEC-HEADER>\n']
    for i,(name,kind,raw) in enumerate(documents):
        chunks.extend([b'<DOCUMENT>\n',f'<TYPE>{kind}\n<SEQUENCE>{i+1}\n<FILENAME>{name}\n<DESCRIPTION>Full document\n<TEXT>\n'.encode(),raw,b'\n</TEXT>\n</DOCUMENT>\n'])
    chunks.append(b'</SEC-DOCUMENT>\n')
    return b''.join(chunks)


def standard_submission():
    return submission([('annual.htm','10-K',b'<html><body><p>Business START narrative.</p><table><tr><th>Revenue</th><td>123456789</td></tr></table><p>FINAL ORIGINAL PARAGRAPH.</p></body></html>'),
                       ('ex991.htm','EX-99.1',b'<html><body>Complete earnings release INCLUDING THE END.</body></html>'),
                       ('ex101.htm','EX-10.1',b'<html><body>Every provision of the material agreement.</body></html>')])


class FakeClient:
    def __init__(self, raw=None):self.raw=raw or standard_submission();self.calls=0
    def submission(self,cik,accession):self.calls+=1;return self.raw


class FullFilingTests(unittest.TestCase):
    def test_all_attachments_and_final_paragraph_round_trip(self):
        raw=standard_submission();files=split_submission(raw)
        self.assertEqual([r['type'] for r in files],['10-K','EX-99.1','EX-10.1'])
        self.assertIn(b'FINAL ORIGINAL PARAGRAPH',file_bytes(raw,files[0]))
        with tempfile.TemporaryDirectory() as d:
            objects=LocalObjects(pathlib.Path(d));client=FakeClient(raw)
            manifest,reused=archive_filing(client,objects,'0000000001',FILING)
            self.assertFalse(reused);self.assertTrue(manifest['raw_complete']);self.assertTrue(manifest['text_complete'])
            self.assertEqual(manifest['filing_date'],'2026-02-20');self.assertEqual(manifest['report_date'],'2025-12-31')
            self.assertEqual(manifest['accepted_at_raw'],'20260220121500');self.assertFalse(manifest['truncated'])
            self.assertEqual(read_original(objects,'0000000001',ACCESSION),raw)
            self.assertEqual(read_original(objects,'0000000001',ACCESSION,'ex991.htm'),file_bytes(raw,files[1]))
            _,texts=read_filing(objects,'0000000001',ACCESSION)
            self.assertIn('123456789',texts['annual.htm']['text']);self.assertIn('FINAL ORIGINAL PARAGRAPH',texts['annual.htm']['text'])
            self.assertIn('INCLUDING THE END',texts['ex991.htm']['text'])
            _,reused=archive_filing(client,objects,'0000000001',FILING)
            self.assertTrue(reused);self.assertEqual(client.calls,1)

    def test_no_old_download_or_section_caps(self):
        raw=b'A'*(25_000_000+123)+b'ENTIRE FINAL BYTES'
        response=type('Response',(),{'content':raw,'headers':{'Content-Length':str(len(raw))}})()
        client=WholeClient(per_sec=1)
        with patch.object(client,'_get',return_value=response):self.assertEqual(client.whole_url('https://www.sec.gov/x'),raw)
        text='BEGIN '+('actual words '*40000)+' FULL END'
        result=extract_document(('<html><body><p>'+text+'</p></body></html>').encode(),'large.htm')
        self.assertGreater(len(result['text']),350000);self.assertTrue(result['text'].endswith('FULL END'))

    def test_incomplete_responses_and_submissions_fail(self):
        client=WholeClient(per_sec=1)
        response=type('Response',(),{'content':b'short','headers':{'Content-Length':'100'}})()
        with patch.object(client,'_get',return_value=response):
            with self.assertRaisesRegex(ValueError,'Incomplete HTTP'):client.whole_url('https://www.sec.gov/x')
        for raw in (standard_submission()[:-30],b'<html>Not the source</html>',standard_submission().replace(b'</DOCUMENT>',b'</BROKEN>',1)):
            with self.assertRaises(ValueError):split_submission(raw)

    def test_tables_and_inline_xbrl_visible_values_are_preserved(self):
        raw=b'<html><body><ix:header><ix:hidden>HIDDEN METADATA</ix:hidden></ix:header><p>Revenue</p><table><tr><td>Brazil</td><td>100000</td><td>42%</td></tr></table><p><ix:nonfraction>1234</ix:nonfraction> final.</p></body></html>'
        text=full_html_text(raw)
        for needle in ('Brazil','100000','42%','1234','final.'):self.assertIn(needle,text)
        self.assertNotIn('HIDDEN METADATA',text)

    def test_uuencoded_pdf_or_binary_is_preserved_without_pdf_size_skip(self):
        original=b'\x89PNG\r\n'+bytes(range(256))*3
        encoded=b'begin 644 logo.png\n'+b''.join(binascii.b2a_uu(original[i:i+45]) for i in range(0,len(original),45))+b'`\nend\n'
        raw=submission([('logo.png','GRAPHIC',encoded)])
        files=split_submission(raw)
        self.assertEqual(file_bytes(raw,files[0]),original)
        self.assertEqual(extract_document(original,'logo.png','GRAPHIC')['status'],'binary_original_preserved')

    def test_pdf_every_page_and_dates_not_inferred(self):
        import fitz
        with fitz.open() as pdf:
            for i in range(3):pdf.new_page().insert_text((72,72),f'Complete original PDF page {i+1}')
            raw=pdf.tobytes()
        extracted=extract_document(raw,'annual.pdf','EX-99.1')
        self.assertEqual(extracted['status'],'complete');self.assertEqual(extracted['pages'],3)
        for i in range(3):self.assertIn(f'PDF page {i+1}',extracted['text'])

    def test_missing_pdf_text_is_not_reported_complete(self):
        with patch('src.company_filings.full_extract.full_html_text',return_value=''):
            self.assertEqual(extract_document(b'<html><body>x</body></html>','x.htm')['status'],'extraction_failed')

    def test_missing_primary_fails_instead_of_marking_ok(self):
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaisesRegex(ValueError,'Primary document absent'):
                archive_filing(FakeClient(),LocalObjects(d),'0000000001',dict(FILING,primary='missing.htm'))

    def test_failed_reader_preserves_original_and_resume_uses_it(self):
        with tempfile.TemporaryDirectory() as d:
            objects=LocalObjects(d);client=FakeClient()
            with patch('src.company_filings.full_archive.split_submission',side_effect=ValueError('new format')):
                with self.assertRaisesRegex(ValueError,'new format'):archive_filing(client,objects,'0000000001',FILING)
            downloaded=objects.maybe_json(f'company-filings/full-v1/0000000001/{ACCESSION}/download.json')
            self.assertEqual(gzip.decompress(objects.get(downloaded['source']['key'])),client.raw)
            self.assertIsNone(objects.maybe_json(f'company-filings/full-v1/0000000001/{ACCESSION}/manifest.json'))
            manifest,_=archive_filing(client,objects,'0000000001',FILING)
            self.assertTrue(manifest['text_complete']);self.assertEqual(client.calls,1)

    def test_resume_detects_corrupt_original(self):
        with tempfile.TemporaryDirectory() as d:
            objects=LocalObjects(d);manifest,_=archive_filing(FakeClient(),objects,'0000000001',FILING)
            objects.put(manifest['source']['key'],gzip.compress(b'corrupted'))
            with self.assertRaisesRegex(ValueError,'checksum'):archive_filing(FakeClient(),objects,'0000000001',FILING)

    def test_every_later_8k_6k_and_quarter_is_selected_without_item_or_count_gate(self):
        rows=[FILING]
        for i in range(12):rows.append(dict(FILING,form='6-K',accession=f'foreign{i}',filing_date='2026-03-01'))
        rows.extend([dict(FILING,form='8-K',accession='other-item',items='8.01',filing_date='2026-04-01'),dict(FILING,form='10-Q',accession='quarter',filing_date='2026-05-01'),dict(FILING,form='10-K/A',accession='amend',filing_date='2026-05-01')])
        chosen,reason=select_whole_filings(rows)
        self.assertEqual(reason,'annual');self.assertEqual(len(chosen),16)

    def test_native_form_and_acceptance_are_retained(self):
        rows=columnar_rows({'form':['40FR12B'],'accessionNumber':[ACCESSION],'filingDate':['2026-02-20'],'reportDate':['2025-12-31'],'acceptanceDateTime':['2026-02-20T12:00:00Z'],'primaryDocument':['x.htm']})
        self.assertEqual(rows[0]['native_form'],'40FR12B');self.assertEqual(rows[0]['form'],'40-F')
        self.assertEqual(rows[0]['accepted_at_raw'],'2026-02-20T12:00:00Z')

    def test_company_does_not_hide_a_missing_selected_filing(self):
        company={'cik':'0000000001','tickers':['A','A-A']}
        inventory=([FILING,dict(FILING,accession='0000000001-26-000002')],{'expected_accessions':[ACCESSION,'0000000001-26-000002']})
        class FailingClient(FakeClient):
            def submission(self,cik,accession):
                if accession.endswith('000002'):raise IOError('missing file')
                return super().submission(cik,accession)
        with tempfile.TemporaryDirectory() as d,patch('src.company_filings.full_archive.filing_inventory',return_value=inventory):
            result=archive_company(company,FailingClient(),LocalObjects(d))
            self.assertFalse(result['raw_complete']);self.assertEqual(result['raw_complete_filings'],1)
            self.assertEqual(len(result['failures']),1);self.assertEqual(result['expected_filings'],2)

    def test_unsafe_keys_and_filenames_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises(ValueError):LocalObjects(d).put('../escape',b'x')
        with self.assertRaises(ValueError):split_submission(standard_submission().replace(b'annual.htm',b'../annual.htm'))

    def test_aggregate_rejects_green_counts_with_missing_ticker_or_filing(self):
        from scripts.verify_company_filings_full import verify
        universe={'snapshot_date':'2026-10-16','companies':[{'ticker':'A','cik':'0000000001'},{'ticker':'B','cik':'0000000002'}]}
        report={'shard':0,'shards':1,'universe_snapshot':'2026-10-16','unprocessed_ciks':[],
            'processed_issuers':1,'expected_issuers':1,'durable':False,'expected_filings':1,'raw_complete_filings':1,
            'text_complete_filings':1,'files':1,'original_bytes':100,'cached_filings':0,'llm_calls':0,
            'companies':[{'cik':'0000000001','tickers':['A'],'raw_complete':True,'text_complete':True}]}
        result=verify(universe,[report],1)
        self.assertFalse(result['complete']);self.assertEqual(result['missing_tickers'],['B'])
        report['companies'].append({'cik':'0000000002','tickers':['B'],'raw_complete':False,'text_complete':False})
        report['processed_issuers']=2;report['expected_issuers']=2
        result=verify(universe,[report],1);self.assertFalse(result['complete']);self.assertEqual(result['missing_tickers'],[])


if __name__=='__main__':unittest.main()
