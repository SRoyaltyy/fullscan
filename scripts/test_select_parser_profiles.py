import unittest
from .select_parser_profiles import select,INPUTS,ROOT


def artifact(sha,created,name='full',branch='main',expired=False):
    return {'name':name,'created_at':created,'expired':expired,
            'workflow_run':{'head_sha':sha,'head_branch':branch,'id':created}}


class ProfileSelectionControls(unittest.TestCase):
    def test_stale_full_set_cannot_hide_current_sample(self):
        result=select([artifact('old',3)],[artifact('current',2,'sample')],lambda sha:sha=='current')
        self.assertEqual(result['artifact'],'sample')
        self.assertEqual(result['scope'],'development_sample')
        self.assertEqual(result['profile_assets'],'full')

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
