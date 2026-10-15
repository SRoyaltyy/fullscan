import gzip,json,pathlib,tempfile,unittest
from src.news_impact.semantic_catalog import CompanyCatalog

class IdentityCatalogTests(unittest.TestCase):
    def write_identity(self,folder,**changes):
        row={'id':'Q999999001','Company':'Harbor Sensors Pty Ltd',
             'description':'manufacturer of optical sensor systems',
             'tickers':['HSO'],'industries':['optoelectronics'],
             'article_answer':'must never enter model context'}|changes
        path=pathlib.Path(folder)/'wikidata_companies.json'
        path.write_text(json.dumps([row]))
        return path

    def test_identity_only_catalog_retains_business_facts_without_sector_answers(self):
        with tempfile.TemporaryDirectory() as f:
            path=self.write_identity(f)
            catalog=CompanyCatalog(root=f,identity_path=path)
            result=catalog.candidates('Harbor Sensors signs supply agreement')
            self.assertTrue(catalog.metadata['available'])
            self.assertEqual(result[0]['Industry'],'optoelectronics')
            self.assertEqual(result[0]['identity_source'],'wikidata:Q999999001')
            for key in ('Sector','lane_class','action','article_answer'):
                self.assertNotIn(key,result[0])

    def test_global_ticker_and_publisher_do_not_establish_identity(self):
        with tempfile.TemporaryDirectory() as f:
            path=self.write_identity(f)
            catalog=CompanyCatalog(root=f,identity_path=path)
            self.assertEqual(catalog.candidates('Unknown issuer $HSO announces results'),[])
            self.assertEqual(catalog.candidates('Markets rise - Harbor Sensors'),[])
            self.assertEqual(catalog.candidates('Harbor Sensorial reports results'),[])

    def test_same_identity_bytes_have_stable_fingerprint_and_edits_invalidate_it(self):
        with tempfile.TemporaryDirectory() as f,tempfile.TemporaryDirectory() as other:
            path=self.write_identity(f)
            second=pathlib.Path(other)/path.name;second.write_bytes(path.read_bytes())
            before=CompanyCatalog(root=f,identity_path=path).metadata['sha256']
            self.assertEqual(before,CompanyCatalog(root=other,identity_path=second).metadata['sha256'])
            self.write_identity(f,industries=['industrial machinery'])
            self.assertNotEqual(before,CompanyCatalog(root=f,identity_path=path).metadata['sha256'])

    def test_invalid_or_duplicate_identity_snapshot_fails_before_model_calls(self):
        with tempfile.TemporaryDirectory() as f:
            path=self.write_identity(f)
            rows=json.loads(path.read_text());path.write_text(json.dumps(rows+rows))
            with self.assertRaisesRegex(ValueError,'Duplicate'):
                CompanyCatalog(root=f,identity_path=path)
            self.write_identity(f,industries=[],description='')
            with self.assertRaisesRegex(ValueError,'Invalid'):
                CompanyCatalog(root=f,identity_path=path)

    def test_generic_single_word_business_names_do_not_match_ordinary_prose(self):
        with tempfile.TemporaryDirectory() as f:
            path=self.write_identity(f,Company='&above',industries=['software'])
            catalog=CompanyCatalog(root=f,identity_path=path)
            self.assertEqual(catalog.candidates('Shares trade above the prior low'),[])
            path=self.write_identity(f,Company='Insider',industries=['journalism'])
            catalog=CompanyCatalog(root=f,identity_path=path)
            self.assertEqual(catalog.candidates('A manufacturer reports insider buying'),[])

    def test_global_group_name_is_not_a_free_alias_for_a_namesake(self):
        with tempfile.TemporaryDirectory() as f:
            path=self.write_identity(f,Company='Harbor Group AG',industries=['retail'])
            catalog=CompanyCatalog(root=f,identity_path=path)
            self.assertEqual(catalog.candidates('Harbor Catalog sells television rights'),[])
            self.assertEqual(catalog.candidates('Harbor Group reports results')[0]['Company'],'Harbor Group AG')

    def test_description_only_identity_is_available_without_invented_industry(self):
        with tempfile.TemporaryDirectory() as f:
            path=self.write_identity(f,industries=[])
            facts=CompanyCatalog(root=f,identity_path=path).candidates('Harbor Sensors reports results')[0]
            self.assertEqual(facts['Industry'],'')
            self.assertIn('optical sensor',facts['Description'])
            self.assertNotIn('Sector',facts)

    def test_frozen_compressed_snapshot_supplies_identical_facts(self):
        with tempfile.TemporaryDirectory() as f:
            path=self.write_identity(f)
            compressed=path.with_suffix('.json.gz')
            compressed.write_bytes(gzip.compress(path.read_bytes(),mtime=0))
            plain=CompanyCatalog(root=f,identity_path=path)
            zipped=CompanyCatalog(root=f,identity_path=compressed)
            self.assertEqual(plain.candidates('Harbor Sensors expands'),zipped.candidates('Harbor Sensors expands'))
            self.assertNotEqual(plain.metadata['sha256'],zipped.metadata['sha256'])

if __name__=='__main__':unittest.main()
