import unittest
from types import SimpleNamespace
from .stock_keywords import compile_keywords, KeywordIndex, quality_issues, normalized_words


def graph(facts=None):
    def claim(ident, predicate, value, date='2026-01-01', status='asserted', quote=None, qualifiers=None):
        return {'id':ident,'subject':'BLZE','predicate':predicate,'object':value,'object_type':'company',
            'status':status,'qualifiers':qualifiers or {},'confidence':'explicit',
            'evidence':[{'source_id':date,'start':0,'end':len(quote or value),'quote':quote or value}]}
    return {'ticker':'BLZE','facts':facts or [claim('s','buys_from','Western Digital',qualifiers={'description':'hard drive suppliers'}),
        claim('d','depends_on','Stripe'), claim('n','depends_on','Stripe','2026-03-01','negated')],
        'sources':[{'id':date,'filing_date':date,'accession':'accession','section':'Item 1','document':'doc.htm'}
                   for date in ('2026-01-01','2026-03-01')], 'paragraphs':[], 'coverage':{}, 'claim':claim}


def profile(bundle, name='Backblaze Inc.'):
    return SimpleNamespace(name=name,keyword_records=bundle['records'],keyword_evidence=bundle['evidence'])


class KeywordTests(unittest.TestCase):
    def test_all_candidates_retained_and_directional(self):
        bundle = compile_keywords(graph(),as_of='2026-04-01')
        self.assertEqual(bundle['coverage']['retained_relationships'],3)
        rows = bundle['records']
        self.assertTrue(any(r['stock']=='BLZE' and r['keyword']=='Western Digital' and r['relationship']=='supplier' for r in rows))
        self.assertTrue(any(r['relationship']=='input' and r['keyword']=='hard drive' and r['qualifiers']['via_supplier']=='Western Digital' for r in rows))
        stripe = [r for r in rows if r['keyword']=='Stripe']
        self.assertFalse(any(r['usable_for_scoring'] for r in stripe))

    def test_historical_cutoff_and_negative_latest(self):
        bundle = compile_keywords(graph(),as_of='2026-04-01')
        index = KeywordIndex({'BLZE':profile(bundle)})
        before = index.compare('Stripe disruption',article_date='2025-12-01')
        self.assertEqual(before['matches'],[])
        january = index.compare('Stripe disruption',article_date='2026-02-01')['matches']
        self.assertTrue(any(r['usable_for_scoring'] for r in january))
        april = index.compare('Stripe disruption',article_date='2026-04-01')['matches']
        self.assertFalse(any(r['usable_for_scoring'] for r in april))
        self.assertFalse(index.compare('Stripe')['stock_passes_changed'])

    def test_known_false_positives_flagged_not_deleted(self):
        g = graph()
        g['facts'] = [g['claim']('a','uses_input','understanding',quote='As part of our audits we are required to obtain an understanding of internal control.'),
                      g['claim']('b','offers','written notice'),
                      g['claim']('c','competes_with','many other companies',quote='We compete for talented engineers and other personnel.')]
        bundle = compile_keywords(g,as_of='2026-04-01')
        self.assertEqual(len(bundle['evidence']),3)
        self.assertTrue(all(r['review_required'] and not r['usable_for_scoring'] for r in bundle['records']))
        self.assertTrue(any(r['relationship']=='talent_competition' for r in bundle['records']))

    def test_qualifiers_not_merged_between_suppliers(self):
        g = graph()
        g['facts'] = [g['claim']('a','buys_from','Western Digital',qualifiers={'description':'hard drive suppliers'}),
                      g['claim']('b','buys_from','Toshiba',qualifiers={'description':'hard drive suppliers'})]
        rows = compile_keywords(g,as_of='2026-04-01')['records']
        inputs = [r for r in rows if r['relationship']=='input']
        self.assertEqual({r['qualifiers']['via_supplier'] for r in inputs},{'Western Digital','Toshiba'})
        self.assertEqual(len(inputs),2)

    def test_hyphen_plural_offsets_aliases_and_sentence_boundary(self):
        index = KeywordIndex({'BLZE':profile(compile_keywords(graph(),as_of='2026-04-01'))})
        text = 'Western Digital and hard-drives'
        hits = index.compare(text)['matches']
        input_hit = next(r for r in hits if r['relationship']=='input')
        self.assertEqual(input_hit['article_span']['text'],'hard-drives')
        self.assertEqual(text[input_hit['article_span']['start']:input_hit['article_span']['end']],'hard-drives')
        self.assertFalse(index.compare('Western. Digital')['matches'])
        self.assertEqual(normalized_words('hard-drive'),normalized_words('hard drives'))

    def test_generic_keyword_ambiguity_not_thousands_of_stock_passes(self):
        profiles = {}
        for i in range(10):
            g = graph(); g['facts'] = [g['claim']('r','risk_mentions','tariffs',status='potential')]
            ticker = 'T'+str(i)
            profiles[ticker] = profile(compile_keywords(g,as_of='2026-04-01',ticker=ticker),'Company '+str(i))
        result = KeywordIndex(profiles).compare('Tariffs rise')
        self.assertEqual(result['matches'],[])
        self.assertEqual(result['broad_matches'][0]['candidate_stock_count'],10)

    def test_guidance_ranges_remain_separate_and_period_unknown(self):
        g = graph(); g['facts'] = []
        g['paragraphs'] = [{'id':str(i),'source_id':'2026-01-01','kind':'prose','start':0,'end':len(text),'text':text,
            'signals':[{'quote':'23%'}]} for i,text in enumerate([
                'Adjusted EBITDA margin range of 23% to 25%, raised from 19% to 21%.',
                'Adjusted EBITDA margin range of 27% to 29%, raised from 23% to 25%.'])]
        bundle = compile_keywords(g,as_of='2026-04-01')
        rows = bundle['records']; self.assertEqual(len(rows),2)
        changes = [r['qualifiers']['guidance_change'] for r in rows]
        self.assertEqual({tuple(c['new']) for c in changes},{(23,25),(27,29)})
        self.assertTrue(all(c['period'] is None for c in changes))
        self.assertTrue(all(r['review_required'] for r in rows))

    def test_keyword_adapter_preserves_source_graph_and_exposes_rows(self):
        import tempfile
        from pathlib import Path
        from .test_deep_profile_store import DeepProfileControls
        from .stock_keyword_store import attach_keyword_profiles
        from .stock_evidence import Profile,Catalog,index_profiles
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            entry = DeepProfileControls().fixture(root)
            before = (root/entry['path']).read_bytes()
            board,coverage = attach_keyword_profiles(index_profiles({'TEST':Profile('TEST','Example','','')}),
                root,as_of='2026-10-20',catalog=Catalog([]))
            self.assertEqual(coverage['keyword_schema'],'stock-keywords-1.0')
            p = board.profiles['TEST']
            self.assertEqual(p.keyword_coverage['retained_relationships'],1)
            self.assertTrue(any(r['relationship']=='supplier' for r in p.keyword_records))
            self.assertEqual(before,(root/entry['path']).read_bytes())

    def test_any_article_endpoint_uses_same_comparison_and_validates_input(self):
        import json
        from .scanner_backtest import dispatch
        bundle = compile_keywords(graph(),as_of='2026-04-01')
        gate = SimpleNamespace(ready=True,board=SimpleNamespace(profiles={'BLZE':profile(bundle)}))
        status,result,_ = dispatch(gate,'POST','/api/keywords',json.dumps({'text':'Western Digital supply disruption'}).encode())
        self.assertEqual(status,200)
        self.assertTrue(any(r['stock']=='BLZE' and r['relationship']=='supplier' for r in result['matches']))
        self.assertFalse(result['stock_passes_changed'])
        for invalid in ({},{'text':12},{'text':'Stripe','article_date':'tomorrow'}):
            self.assertEqual(dispatch(gate,'POST','/api/keywords',json.dumps(invalid).encode())[0],400)


if __name__ == '__main__':
    unittest.main()
