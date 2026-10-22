import unittest
from .select_parser_profiles import select,INPUTS,ROOT


def artifact(sha,created,name='full',branch='main',expired=False):
    return {'name':name,'created_at':created,'expired':expired,
            'workflow_run':{'head_sha':sha,'head_branch':branch,'id':created}}


class ProfileSelectionControls(unittest.TestCase):
    def test_development_sample_requires_explicit_opt_in(self):
        args=([artifact('old',3)],[artifact('current',2,'sample')],lambda sha:sha=='current')
        self.assertEqual(select(*args),{'ready':'false'})
        result=select(*args,allow_sample=True)
        self.assertEqual(result['artifact'],'sample')
        self.assertEqual(result['scope'],'development_sample')
        self.assertEqual(result['profile_assets'],'full')

    def test_current_market_disclosures_take_precedence_over_single_issuer_sample(self):
        from src.test_completed_profile_store import fixture
        manifest,_,_=fixture()
        result=select([artifact('old',3)],[artifact('current',2,'sample')],lambda sha:sha=='current',
            completed=manifest,manifest_url='immutable-manifest',allow_sample=True)
        self.assertEqual(result['input_kind'],'completed_review')
        self.assertEqual(result['scope'],'archive_set')
        self.assertEqual(result['manifest_url'],'immutable-manifest')

    def test_compatible_full_set_takes_precedence(self):
        result=select([artifact('current',1)],[artifact('current',2,'sample')],lambda sha:True)
        self.assertEqual(result['scope'],'archive_set')
        self.assertEqual(result['profile_assets'],'keywords')

    def test_expired_or_pr_artifacts_do_not_publish(self):
        self.assertEqual(select([artifact('current',1,expired=True)],[artifact('current',2,branch='feature')],lambda sha:True),{'ready':'false'})

    def test_no_compatible_input_keeps_existing_page(self):
        self.assertEqual(select([artifact('old',1)],[],lambda sha:False),{'ready':'false'})

    def test_selector_covers_exact_compiler_inputs(self):
        import hashlib
        from src.filing_relationships import compiler_fingerprint
        digest=hashlib.sha256()
        for path in INPUTS:digest.update(path.encode()+b'\0'+(ROOT/path).read_bytes()+b'\0')
        self.assertEqual(digest.hexdigest(),compiler_fingerprint())


if __name__=='__main__':unittest.main()
