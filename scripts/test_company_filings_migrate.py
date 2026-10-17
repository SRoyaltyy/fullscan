import json,pathlib,tempfile,unittest
from unittest.mock import patch
from scripts.migrate_company_filings_full import migrate
from scripts.test_company_filings_full import FILING,FakeClient,ACCESSION
from src.company_filings.full_archive import LocalObjects,archive_filing,json_bytes,read_original


class DurableObjects(LocalObjects):durable=True


class MigrateTests(unittest.TestCase):
    def test_reader_exports_entire_filing_directory_with_dates_and_all_attachments(self):
        import contextlib,io
        from scripts.read_company_filings_full import main
        with tempfile.TemporaryDirectory() as d:
            source=LocalObjects(pathlib.Path(d)/'objects');client=FakeClient()
            manifest,_=archive_filing(client,source,'0000000001',FILING)
            output=pathlib.Path(d)/'export'
            argv=['read_company_filings_full.py','--cik','1','--objects',str(source.root),'--accession',ACCESSION,'--all-originals','--output',str(output)]
            with patch('sys.argv',argv),contextlib.redirect_stdout(io.StringIO()):main()
            self.assertEqual((output/'complete-submission.txt').read_bytes(),client.raw)
            self.assertEqual(json.loads((output/'manifest.json').read_text())['filing_date'],FILING['filing_date'])
            for item in manifest['files']:
                self.assertEqual((output/'originals'/item['filename']).read_bytes(),read_original(source,'0000000001',ACCESSION,item['filename']))

    def test_whole_original_and_dates_survive_migration_and_resume(self):
        with tempfile.TemporaryDirectory() as a,tempfile.TemporaryDirectory() as b:
            source=LocalObjects(a);destination=DurableObjects(b);client=FakeClient()
            manifest,_=archive_filing(client,source,'0000000001',FILING)
            company={'cik':'0000000001','filings':[{'accession':ACCESSION}],'durable':False}
            source.put('company-filings/full-v1/0000000001/company.json',json_bytes(company))
            first=migrate(source,destination)
            self.assertEqual(first['sec_requests'],0);self.assertEqual(first['migrated_or_verified_filings'],1)
            self.assertEqual(read_original(destination,'0000000001',ACCESSION),client.raw)
            key=f'company-filings/full-v1/0000000001/{ACCESSION}/manifest.json'
            self.assertTrue(destination.maybe_json(key)['durable'])
            self.assertEqual(destination.maybe_json(key)['filing_date'],FILING['filing_date'])
            self.assertTrue(destination.maybe_json('company-filings/full-v1/0000000001/company.json')['durable'])
            self.assertEqual(migrate(source,destination)['reused_filings'],1)

    def test_failed_object_write_cannot_publish_durable_manifest(self):
        class Broken(DurableObjects):
            def put(self,key,data):
                if '/text/' in key:raise IOError('storage write failed')
                return super().put(key,data)
        with tempfile.TemporaryDirectory() as a,tempfile.TemporaryDirectory() as b:
            source=LocalObjects(a);destination=Broken(b)
            archive_filing(FakeClient(),source,'0000000001',FILING)
            with self.assertRaises(IOError):migrate(source,destination)
            self.assertIsNone(destination.maybe_json(f'company-filings/full-v1/0000000001/{ACCESSION}/manifest.json'))

    def test_original_survives_migration_even_when_reader_has_failed(self):
        with tempfile.TemporaryDirectory() as a,tempfile.TemporaryDirectory() as b:
            source=LocalObjects(a);destination=DurableObjects(b);client=FakeClient()
            with patch('src.company_filings.full_archive.split_submission',side_effect=ValueError('reader failed')):
                with self.assertRaises(ValueError):archive_filing(client,source,'0000000001',FILING)
            result=migrate(source,destination)
            self.assertEqual(result['preserved_original_checkpoints'],1)
            self.assertEqual(result['migrated_or_verified_filings'],0)
            archive_filing(client,destination,'0000000001',FILING)
            self.assertEqual(client.calls,1)

    def test_non_durable_destination_and_corrupt_source_are_rejected(self):
        with tempfile.TemporaryDirectory() as a,tempfile.TemporaryDirectory() as b:
            source=LocalObjects(a)
            with self.assertRaisesRegex(ValueError,'durable'):migrate(source,LocalObjects(b))
            manifest,_=archive_filing(FakeClient(),source,'0000000001',FILING)
            source.put(manifest['source']['key'],b'corrupted')
            with self.assertRaises(Exception):migrate(source,DurableObjects(b))


if __name__=='__main__':unittest.main()
