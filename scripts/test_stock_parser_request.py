import gzip, json, tempfile, unittest
from pathlib import Path
from scripts.stock_parser_request import assemble, digest, validate_request, write, OLD, NEW
class RequestIntegrity(unittest.TestCase):
    def fixture(self, root):
        source={'ticker':'BLZE','as_of':'2026-10-20','catalog_sha256':'a'*64,
                'coverage_kind':'filing_backed','source_receipt':{'filings':2}}
        path=root/'input.json';write(path,source)
        write(str(path)+'.receipt.json',{'input_sha256':digest(path.read_bytes()),'fetch_seconds':1})
        result={'ticker':'BLZE','input_sha256':digest(path.read_bytes()),
                'source_receipt':source['source_receipt'],'parse_seconds':2}
        write(root/'old.json',{**result,'source_sha':OLD})
        write(root/'new.json',{**result,'source_sha':NEW})
        return path
    def assemble(self, root):
        return assemble(root/'input.json',root/'old.json',root/'new.json',root/'out',
                        'test-request-123456',OLD,NEW,'99')
    def test_publishes_same_input_receipt_for_both_versions(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);path=self.fixture(root);receipt=self.assemble(root)
            bundle=json.loads(gzip.decompress((root/'out/comparison.json.gz').read_bytes()))
            self.assertEqual(receipt['sha256'],digest((root/'out/comparison.json.gz').read_bytes()))
            self.assertEqual(bundle['old']['input_sha256'],bundle['new']['input_sha256'])
            self.assertEqual(bundle['input_sha256'],digest(path.read_bytes()))
    def test_other_stock_revision_hash_or_source_is_rejected(self):
        for role,field,value in [('old','ticker','OTHER'),('new','source_sha',OLD),
                                 ('new','input_sha256','f'*64),('old','source_receipt',{'filings':1})]:
            with self.subTest(role=role,field=field), tempfile.TemporaryDirectory() as temp:
                root=Path(temp);self.fixture(root);path=root/(role+'.json')
                value_json=json.loads(path.read_bytes());value_json[field]=value;write(path,value_json)
                with self.assertRaises(ValueError):self.assemble(root)
    def test_modified_input_cannot_reuse_finished_outputs(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);path=self.fixture(root);path.write_bytes(path.read_bytes()+b' ')
            with self.assertRaises(ValueError):self.assemble(root)
    def test_path_shell_payloads_or_unpinned_revisions_are_rejected(self):
        for ticker,ident,old,new in [('BLZE','../../path',OLD,NEW),
                                     ('BLZE;whoami','test-request-123456',OLD,NEW),
                                     ('BLZE','test-request-123456','main',NEW),
                                     ('BLZE','test-request-123456',OLD,OLD)]:
            with self.assertRaises(ValueError):validate_request(ticker,ident,old,new)
if __name__=='__main__':unittest.main()
