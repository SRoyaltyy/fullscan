import unittest
from types import SimpleNamespace
from .stock_keywords import compile_keywords, KeywordIndex, quality_issues, normalized_words, scanner_policy, scanner_visible


def graph(facts=None):
    def claim(ident, predicate, value, date='2026-01-01', status='asserted', quote=None, qualifiers=None):
        return {'id':ident,'subject':'BLZE','predicate':predicate,'object':value,'object_type':'organization' if predicate in ('buys_from','depends_on') else 'concept',
            'status':status,'qualifiers':qualifiers or {},'confidence':'explicit',
            'evidence':[{'source_id':date,'start':0,'end':len(quote or value),'quote':quote or value}]}
    return {'ticker':'BLZE','facts':facts or [claim('s','buys_from','Western Digital',quote='Western Digital is one of our hard drive suppliers.',qualifiers={'description':'hard drive suppliers'}),
        claim('d','depends_on','Stripe',quote='We rely on Stripe for payment processing.'), claim('n','depends_on','Stripe','2026-03-01','negated',quote='We do not rely on Stripe.')],
        'sources':[{'id':date,'filing_date':date,'accession':'accession','section':'Item 1','document':'doc.htm'}
                   for date in ('2026-01-01','2026-03-01')], 'paragraphs':[], 'coverage':{}, 'claim':claim}


def profile(bundle, name='Backblaze Inc.'):
    return SimpleNamespace(name=name,keyword_records=bundle['records'],keyword_evidence=bundle['evidence'])


class KeywordTests(unittest.TestCase):
    def test_semantic_types_and_local_sentence_attachment(self):
        from .stock_keywords import annotate_scanner_rows
        def admitted(value, role, quote, object_type='organization'):
            row=dict(id='r',stock='TEST',keyword=value,relationship=role,object_type=object_type,
                assertion='asserted',qualifiers={},evidence_ids=['e'])
            annotate_scanner_rows([row],{'e':{'witnesses':[dict(quote=quote,source={'filing_date':'2026-01-01'})]}})
            return scanner_visible(row)
        bad=[('Exhibit 3.1','customer','We provide notice. Exhibit 3.1'),
            ('the Foreign Corrupt Practices Act','subsidiary','Our subsidiaries must comply with the Foreign Corrupt Practices Act.'),
            ('natural disasters','input','We use suppliers and face natural disasters.'),
            ('key personnel','dependency','We depend on key personnel.'),
            ('materials','input','We purchase materials.'),
            ('SAT','customer','SAT is committed to meeting the needs of our customers.'),
            ('Carbon Fiber Sprayer Booms','customer','PRODUCT\n\nCarbon Fiber Sprayer Booms\n\nWe serve customers.'),
            ('Trust-Based Solutions','customer','We make loans to our Trust-Based Solutions customers.'),
            ('New York Stock exchange','competitor',"Our stakeholders (investors, competitors) operate in the US, coupled with shares traded on the New York Stock exchange."),
            ('Cloud Platform','dependency','We rely on Google Cloud Platform.'),
            ('Audit Committee','approval_authority','We require approval from our Audit Committee.')]
        bad += [('AAdvantage','partner','Our partners include AAdvantage partners and other partners.'),
            ('AAdvantage','dependency','We depend on commercial partners, including AAdvantage partners.'),
            ('Consolidated Net Income','subsidiary','Our subsidiaries contribute to Consolidated Net Income.'),
            ('Bank of America','subsidiary','Bank of America lends money to our subsidiaries.'),
            ('Convertible Note security','input','We purchased a Convertible Note security.'),
            ('Chief Executive Officer','product_or_service','We offer our Chief Executive Officer an employment agreement.'),
            ('FDA','product_or_service','We offer products subject to FDA approval.'),
            ('equipment','input','We purchase equipment.'),
            ('four','input','We purchase four production lines.')]
        for value,role,quote in bad:
            with self.subTest(value=value):self.assertFalse(admitted(value,role,quote))
        good=[('TikTok','customer','TikTok remains one of our largest customers.'),
            ('PACCAR','customer','Our customers include PACCAR, AB Volvo and Stellantis.'),
            ('Cyxtera Technologies','customer','The acquisition of one of our customers, Cyxtera Technologies, was completed.'),
            ('Amazon Web Services','dependency','We rely on Amazon Web Services to run our service.'),
            ('Microsoft Azure','dependency','We rely on infrastructure of third parties such as Microsoft Azure.'),
            ('DiePak Loader','product_or_service','The Company offers the DiePak Loader for DiePak Carriers.'),
            ('hard drives','input','We purchase hard drives from Western Digital.'),
            ('copper','input','We use copper in manufacturing.'),
            ('Cloudflare','competitor','We compete with providers such as Akamai and Cloudflare.')]
        for value,role,quote in good:
            with self.subTest(value=value):self.assertTrue(admitted(value,role,quote))

    def test_source_argument_whitespace_padding_is_bounded_and_keeps_word_boundaries(self):
        import subprocess,sys,textwrap
        from pathlib import Path
        command=textwrap.dedent('''
            from src.stock_keywords import annotate_scanner_rows
            def check(keyword, quote):
                row={'id':'test','stock':'TEST','keyword':keyword,'relationship':'supplier',
                     'object_type':'ORG','assertion':'asserted','qualifiers':{},'evidence_ids':['e']}
                evidence={'e':{'witnesses':[{'quote':quote,'source':{'filing_date':'2026-01-01'}}]}}
                annotate_scanner_rows([row],evidence)
                return row['scanner']
            failed=check('Named'+' '*110+'Supplier','We buy from Named'+' '*220+'SupplierX.')
            assert not failed['eligible'] and failed['reason']=='incomplete_or_nonliteral_argument'
            valid=check('Named  Supplier','We buy from Named\\tSupplier.')
            assert valid['eligible']
            padded='Operations'+' '*80+'Risk'+' '*100+'Committee'
            layout=check(padded,'We partner with '+padded+'.')
            assert not layout['eligible'] and layout['reason'] in ('table_layout_fragment_requires_named_identity','generic_class_abstract_or_fragment')
        ''')
        result=subprocess.run([sys.executable,'-c',command],cwd=Path(__file__).resolve().parents[1],
            capture_output=True,text=True,timeout=5)
        self.assertEqual(result.returncode,0,result.stderr)

    def test_concrete_framework_rejects_classes_and_fragments_for_every_issuer(self):
        for ticker in ('NORTH', 'SOUTH', 'EAST'):
            for value in ('customers', 'business partners', 'advertising agencies', 'internal systems',
                          'written notice', 'Medical Benefits', 'Equity Vesting Benefits',
                          'Executive valuable Confidential Information', 'all', 'performance marketing leads',
                          'internal forecast numbers', 'periodic examination', 'litigation matters'):
                for role in ('customer', 'partner', 'dependency', 'product_or_service', 'input'):
                    with self.subTest(ticker=ticker, value=value, role=role):
                        self.assertFalse(scanner_policy(dict(stock=ticker,keyword=value,relationship=role,assertion='asserted'))['eligible'])
        # A plural form can still name a precise entity or operating input.
        for value,role in (('Orion Precision Components', 'supplier'), ('Zephyr Medical Devices', 'customer'),
                           ('Atlas Payment Networks', 'partner'), ('warehouse lines', 'input'),
                           ('hard drives', 'input'), ('Amazon Web Services', 'dependency')):
            self.assertTrue(scanner_policy(dict(keyword=value,relationship=role,assertion='asserted'))['eligible'],value)

    def test_physical_input_classes_preserve_materials_without_a_ticker_allowlist(self):
        for value in ('copper', 'oil', 'steel', 'wheat', 'silicon'):
            self.assertTrue(scanner_policy(dict(keyword=value,relationship='input'))['eligible'],value)
        for value in ('ability', 'assurance', 'information', 'internal systems'):
            self.assertFalse(scanner_policy(dict(keyword=value,relationship='input'))['eligible'],value)

    def test_dated_officer_adapter_exports_the_same_concrete_policy(self):
        from .stock_evidence import Fact
        person=Fact('person','John McDonald','John McDonald is our Chief Executive Officer.',
                    source='SEC',filing_date='2026-01-01',accession='filing',section='Executive officers')
        p=SimpleNamespace(name='Orion Inc.',facts=[person],keyword_records=[],keyword_evidence={})
        index=KeywordIndex({'ORION':p})
        rows=[e['row'] for e in index.browser_payload()['phrases'] if e['row']['relationship']=='officer']
        self.assertTrue(rows)
        self.assertTrue(all(r['scanner']['eligible'] and r['scanner']['label']=='has officer' for r in rows))

    def test_negative_latest_connection_cannot_leak_into_any_search_result(self):
        index=KeywordIndex({'BLZE':profile(compile_keywords(graph(),as_of='2026-04-01'))})
        for date in ('', '2026-04-01'):
            result=index.compare('Backblaze uses Stripe payment processing.',article_date=date)
            self.assertFalse(any(r['keyword']=='Stripe' for key in ('matches','unresolved_matches','rejected_matches','broad_matches') for r in result[key]))
        old=index.compare('Backblaze uses Stripe payment processing.',article_date='2026-02-01')
        self.assertTrue(old['matches'])
        bundle=compile_keywords(graph(),as_of='2026-04-01')
        self.assertFalse(any(scanner_visible(r) for r in bundle['records'] if r['keyword']=='Stripe'))

    def test_legal_exclusion_closes_old_positive_alias_and_its_full_name(self):
        g=self.disclosure_graph()
        old=g['claim']('old-dma','subject_to','Digital Markets Act');old['object_type']='law';g['facts']=[old]
        rows=compile_keywords(g,as_of='2026-04-01')
        index=KeywordIndex({'BLZE':profile(rows)})
        result=index.compare('Backblaze discusses the Digital Markets Act.')
        self.assertFalse(any(result[key] for key in ('matches','unresolved_matches','rejected_matches','broad_matches')))
        self.assertFalse(any(scanner_visible(r) for r in rows['records'] if r['keyword']=='Digital Markets Act'))

    def test_relationship_direction_and_legal_scope_are_plain_labels(self):
        for role,label in (('supplier','buys from'),('customer','sells to'),('dependency','relies on'),('partner','partners with')):
            self.assertEqual(scanner_policy(dict(keyword='Orion Components',relationship=role))['label'],label)
        law=dict(keyword='Zorvan Water Act',relationship='disclosed_law',qualifiers={'direct_obligation_asserted':True})
        self.assertEqual(scanner_policy(law)['label'],'is subject to')
        law['qualifiers']['direct_obligation_asserted']=False
        self.assertEqual(scanner_policy(law)['label'],'reports legal exposure to')

    def test_explicit_unaffected_statement_overrides_a_positive_old_parse(self):
        g=graph();g['facts']=[g['claim']('wrong','depends_on','Orion Components',
            quote='We do not rely on Orion Components. We rely on Zephyr Networks.')]
        g['facts'].append(g['claim']('right','depends_on','Zephyr Networks',
            quote='We do not rely on Orion Components. We rely on Zephyr Networks.'))
        bundle=compile_keywords(g,as_of='2026-04-01')
        self.assertEqual([r['keyword'] for r in bundle['records'] if scanner_visible(r)],['Zephyr Networks'])
        g['facts']=[g['claim']('law','subject_to','Zorvan Water Act',quote='We are not affected by the Zorvan Water Act.')]
        g['facts'][0]['object_type']='law'
        self.assertFalse(scanner_visible(compile_keywords(g,as_of='2026-04-01')['records'][0]))

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
        january = index.compare('Backblaze reports Stripe disruption',article_date='2026-02-01')['matches']
        self.assertTrue(any(r['usable_for_scoring'] for r in january))
        april = index.compare('Backblaze reports Stripe disruption',article_date='2026-04-01')['matches']
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

    def disclosure_graph(self):
        import hashlib
        from .specific_filing_records import details
        g=graph();g['facts']=[];g['name']='Backblaze Inc.'
        text='We are subject to Regulation (EU) 2016/679 (also known as the General Data Protection Regulation or “GDPR”). We are not in scope of the Digital Markets Act.'
        g['paragraphs']=[{'id':'legal','source_id':'2026-01-01','start':0,'end':len(text),'text':text,
            'kind':'prose','signals':[],'labels':['regulation_legal'],'sha256':hashlib.sha256(text.encode()).hexdigest()}]
        g['specific_records']=details(g)
        return g

    def test_specific_law_alias_is_dated_evidenced_and_never_an_automatic_pass(self):
        g=self.disclosure_graph();bundle=compile_keywords(g,as_of='2026-04-01')
        self.assertEqual(bundle['coverage']['retained_relationships'],0)
        self.assertEqual(bundle['coverage']['specific_disclosure_records'],len(g['specific_records']['records']))
        self.assertEqual({r['entity'] for r in g['specific_records']['records']},{'Regulation (EU) 2016/679','Digital Markets Act'})
        index=KeywordIndex({'BLZE':profile(bundle)})
        result=index.compare('Backblaze discusses GDPR and the Digital Markets Act.',article_date='2026-02-01')
        gdpr=next(r for r in result['matches'] if r['keyword']=='Regulation (EU) 2016/679')
        self.assertEqual(gdpr['article_span']['text'],'GDPR')
        self.assertFalse(gdpr['usable_for_scoring'])
        self.assertTrue(gdpr['review_required'])
        self.assertTrue(gdpr['evidence_ids'][0].startswith('specific:'))
        self.assertFalse(any(r['keyword']=='Digital Markets Act' for key in ('matches','unresolved_matches','rejected_matches','broad_matches') for r in result[key]))
        self.assertFalse(any(e['row']['keyword']=='Digital Markets Act' for e in index.browser_payload()['phrases']))
        self.assertEqual(index.compare('Backblaze discusses GDPR.',article_date='2025-12-31')['matches'],[])

    def test_law_codes_and_years_cannot_collapse_to_the_same_identity(self):
        from .entity_resolution import surface
        self.assertNotEqual(normalized_words('Regulation (EU) 2016/679'),normalized_words('Regulation (EU) 2024/1689'))
        self.assertNotEqual(surface('Example Act of 1933'),surface('Example Act of 1934'))
        index=KeywordIndex({'BLZE':profile(compile_keywords(self.disclosure_graph(),as_of='2026-04-01'))})
        hits=index.compare('Backblaze discusses Regulation (EU) 2024/1689.')
        self.assertFalse(hits['matches'] or hits['unresolved_matches'])

    def test_explicit_alias_connects_old_short_claim_and_new_complete_name(self):
        g=self.disclosure_graph()
        old=g['claim']('old-gdpr','subject_to','GDPR');old['object_type']='law';g['facts']=[old]
        bundle=compile_keywords(g,as_of='2026-04-01')
        result=KeywordIndex({'BLZE':profile(bundle)}).compare('Backblaze discusses GDPR compliance.')
        self.assertFalse(result['unresolved_matches'])
        self.assertEqual({r['alignment']['stock_entity_id'] for r in result['matches']},{'law:BLZE:regulation eu 2016/679'})
        self.assertTrue(any(r['relationship']=='disclosed_law' for r in result['matches']))

    def test_later_alias_definition_is_not_backdated_to_old_claim(self):
        g=self.disclosure_graph();g['paragraphs'][0]['source_id']='2026-03-01'
        from .specific_filing_records import details
        g['specific_records']=details(g)
        old=g['claim']('old-gdpr','subject_to','GDPR');old['object_type']='law';g['facts']=[old]
        bundle=compile_keywords(g,as_of='2026-04-01')
        row=next(r for r in bundle['records'] if r['keyword']=='GDPR')
        self.assertNotIn('source_declared_identity',row)
        result=KeywordIndex({'BLZE':profile(bundle)}).compare('Backblaze discusses General Data Protection Regulation.',article_date='2026-02-01')
        self.assertFalse(result['matches'] or result['unresolved_matches'])

    def test_one_acronym_for_two_named_laws_remains_ambiguous(self):
        import hashlib
        from .specific_filing_records import details
        g=self.disclosure_graph();text='We are subject to the Alpha Privacy Regulation (DPA) and Beta Privacy Act (DPA).'
        g['paragraphs'][0].update(text=text,end=len(text),sha256=hashlib.sha256(text.encode()).hexdigest())
        g['specific_records']=details(g)
        old=g['claim']('old-dpa','subject_to','DPA');old['object_type']='law';g['facts']=[old]
        bundle=compile_keywords(g,as_of='2026-04-01')
        self.assertNotIn('source_declared_identity',next(r for r in bundle['records'] if r['keyword']=='DPA'))
        result=KeywordIndex({'BLZE':profile(bundle)}).compare('Backblaze discusses DPA compliance.')
        self.assertFalse(result['matches'])
        self.assertTrue(result['unresolved_matches'])

    def test_generic_background_is_retained_for_review_without_an_identity_upgrade(self):
        g=graph();g['facts']=[g['claim']('generic','subject_to','litigation matters')]
        row=compile_keywords(g,as_of='2026-04-01')['records'][0]
        self.assertIn('generic_background_head',row['review_reasons'])
        self.assertFalse(row['usable_for_scoring'])

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
        self.assertEqual(result['broad_matches'],[])
        self.assertEqual(result['unresolved_matches'],[])

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
            self.assertEqual(coverage['keyword_schema'],'stock-keywords-1.1')
            p = board.profiles['TEST']
            self.assertEqual(p.keyword_coverage['retained_relationships'],1)
            self.assertTrue(any(r['relationship']=='supplier' for r in p.keyword_records))
            self.assertEqual(before,(root/entry['path']).read_bytes())

    def test_any_article_endpoint_uses_same_comparison_and_validates_input(self):
        import json
        from .scanner_backtest import dispatch
        bundle = compile_keywords(graph(),as_of='2026-04-01')
        gate = SimpleNamespace(ready=True,board=SimpleNamespace(profiles={'BLZE':profile(bundle)}))
        status,result,_ = dispatch(gate,'POST','/api/keywords',json.dumps({'text':'Backblaze reports Western Digital supply disruption'}).encode())
        self.assertEqual(status,200)
        self.assertTrue(any(r['stock']=='BLZE' and r['relationship']=='supplier' for r in result['matches']))
        self.assertFalse(result['stock_passes_changed'])
        for invalid in ({},{'text':12},{'text':'Stripe','article_date':'tomorrow'}):
            self.assertEqual(dispatch(gate,'POST','/api/keywords',json.dumps(invalid).encode())[0],400)


if __name__ == '__main__':
    unittest.main()
