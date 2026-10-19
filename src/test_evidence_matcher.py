"""Behavioral regressions for evidence-v2, independent of the legacy engine."""
import json
import tempfile
import unittest
from pathlib import Path

from src.correlation import build_board,link_articles as _link_articles
from functools import partial
link_articles=partial(_link_articles,engine='evidence')
from src.evidence_matcher import link_evidence
from src.filing_evidence import section_views
from src.linguistics import lemma,terms,passages
from src.link_metrics import score_links
from src.eval_correlation import compare_case
from src.stock_evidence import Catalog,build_profile
from src.test_correlation import STOCKS,MEMO


EXTRA = [
 {'ticker':'ASND','name':'Ascendis Pharma','industry':'Biotechnology','sector':'Healthcare','description':'We develop TransCon CNP for the treatment of achondroplasia. Our clinical platform develops endocrine medicines.','filing':'Competition\nOur achondroplasia therapy competes with BioMarin Pharmaceutical and its VOXZOGO product.'},
 {'ticker':'BMRN','name':'BioMarin Pharmaceutical','industry':'Biotechnology','sector':'Healthcare','description':'We market VOXZOGO (vosoritide) for the treatment of achondroplasia. We manufacture enzyme therapies for rare genetic disorders.'},
 {'ticker':'DKNG','name':'DraftKings Inc','industry':'Gambling','sector':'Consumer Cyclical','description':'DraftKings operates online sports betting and casino gambling platforms.'},
 {'ticker':'PLBY','name':'Playboy Inc','industry':'Publishing','sector':'Communication','description':'Playboy publishes lifestyle magazines and licenses apparel brands.','filing':'Business\nOur collaborator is DraftKings. We publish lifestyle magazines.'},
 {'ticker':'MSFT','name':'Microsoft Corp','industry':'Software','sector':'Technology','description':'Microsoft develops enterprise operating software and cloud computing systems.'},
 {'ticker':'AAPL','name':'Apple Inc','industry':'Consumer Electronics','sector':'Technology','description':'Apple manufactures smartphones, tablets and wearable computing devices.'},
 {'ticker':'ICE','name':'Intercontinental Exchange Inc','industry':'Financial Data','sector':'Financial','description':'Intercontinental Exchange operates securities exchanges and clearing houses.'},
 {'ticker':'KOS','name':'Kosmos Energy Ltd','industry':'Oil & Gas E&P','sector':'Energy','description':'Kosmos Energy explores offshore crude petroleum wells.'},
 {'ticker':'DUK','name':'Duke Energy Corp','industry':'Utilities','sector':'Utilities','description':'We operate regulated electricity distribution utilities in North Carolina.'},
]


class EvidenceTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  cls.board=build_board(STOCKS+EXTRA)

 def score(self,text,title=''):
  return link_articles(self.board,[{'id':'x','title':title,'text':text}])[0]

 def passed(self,text,title=''):
  return {r['ticker'] for r in self.score(text,title)['stocks'] if r['pass']}

 def test_unnamed_space_policy_passes_with_source_evidence(self):
  hit=self.score(MEMO)
  self.assertIn('RKLB',{r['ticker'] for r in hit['stocks'] if r['pass']})
  self.assertNotIn('AAL',{r['ticker'] for r in hit['stocks'] if r['pass']})
  self.assertTrue(next(r for r in hit['stocks'] if r['ticker']=='RKLB')['evidence'][0]['stock_evidence']['passage'])

 def test_business_paragraph_supports_cross_sentence_operation(self):
  self.assertIn('RKLB',self.passed('Spacecraft launches and orbital launches continue.'))

 def test_business_context_does_not_join_sections_or_counterparties(self):
  for filing in ('Business\nWe manufacture spacecraft.\n\nRisk Factors\nLaunch disruptions affect suppliers.',
                 'Business\nWe manufacture spacecraft. Our competitor Rocket Lab provides launch services.'):
   b=build_board([{'ticker':'TEST','name':'Example Hardware','industry':'Aerospace','filing':filing},STOCKS[0]],engine='evidence')
   hit=link_articles(b,[{'id':'x','text':'Spacecraft launches expand.'}])[0]
   self.assertNotIn('TEST',{r['ticker'] for r in hit['stocks'] if r['pass']})

 def test_unknown_local_incident_does_not_spread_to_product_industry(self):
  b=build_board([{'ticker':'SHOP','name':'Example Apparel','industry':'Retail','description':'We operate retail stores and sell clothing.'}],engine='evidence')
  hit=link_articles(b,[{'id':'x','text':'A man faces arson charges after a retail store fire.'}])[0]
  self.assertFalse(any(r['pass'] for r in hit['stocks']))

 def test_multiword_prefix_adr_and_short_dictionary_company(self):
  b=build_board([{'ticker':'TSM','name':'Taiwan Semiconductor Manufacturing ADR','industry':'Semiconductors'},
                 {'ticker':'ARM','name':'Arm Holdings Plc ADR','industry':'Semiconductors'},
                 {'ticker':'PCG','name':'PG&E Corp','industry':'Utilities'}],engine='evidence')
  for text,ticker in [('Taiwan Semiconductor posts quarterly results.','TSM'),('Arm stock falls after a jury verdict.','ARM'),('PG&E power shutoff expands.','PCG')]:
   hit=link_articles(b,[{'id':'x','text':text}])[0]
   self.assertEqual({r['ticker'] for r in hit['stocks'] if r['pass']},{ticker})

 def test_compound_name_is_not_its_last_word_ticker(self):
  b=build_board([{'ticker':'AI','name':'C3.ai Inc','industry':'Software'}],engine='evidence')
  hit=link_articles(b,[{'id':'x','text':'Character.AI bankruptcy proceedings continue.'}])[0]
  self.assertFalse(any(r['pass'] for r in hit['stocks']))

 def test_foreign_qualifier_is_not_shorter_domestic_bank_name(self):
  b=build_board([{'ticker':'NBHC','name':'National Bank Holdings Inc','industry':'Banks'}],engine='evidence')
  hit=link_articles(b,[{'id':'x','text':'National Bank Of Hungary prepares to cut rates.'}])[0]
  self.assertFalse(any(r['pass'] for r in hit['stocks']))

 def test_blank_check_first_word_is_not_operating_company(self):
  b=build_board([{'ticker':'OTAI','name':'Starlink AI Acquisition Corp','industry':'Shell Companies','description':'Starlink AI is a blank check company.'}],engine='evidence')
  hit=link_articles(b,[{'id':'x','text':'Starlink suffers a network outage.'}])[0]
  self.assertFalse(any(r['pass'] for r in hit['stocks']))

 def test_explicit_product_list_supports_case_sensitive_plural_brand(self):
  b=build_board([{'ticker':'ORCH','name':'Orchard Devices','industry':'Computers','description':'Its products and services include eReader, Mini, and devices.'}],engine='evidence')
  for text,expected in [('Firmware launch supports Minis.',{'ORCH'}),('A mini software launch is scheduled.',set())]:
   hit=link_articles(b,[{'id':'x','text':text}])[0]
   self.assertEqual({r['ticker'] for r in hit['stocks'] if r['pass']},expected)

 def test_disease_after_development_predicate_is_not_owned_product(self):
  b=build_board([{'ticker':'BIOX','name':'Example Biotech','industry':'Biotechnology','description':'Its product ABC-123 is developed for the treatment of AML and MDS.'}],engine='evidence')
  hit=link_articles(b,[{'id':'x','text':'A bank faces fines for AML violations.'}])[0]
  self.assertFalse(any(r['pass'] for r in hit['stocks']))

 def test_place_prefix_does_not_name_company(self):
  b=build_board([{'ticker':'NJR','name':'New Jersey Resources Corp','industry':'Utilities'},
                {'ticker':'NYT','name':'New York Times Co','industry':'Publishing'}],engine='evidence')
  hit=link_articles(b,[{'id':'x','text':'New Jersey man faces arson charges after a New York City fire.'}])[0]
  self.assertFalse(any(r['pass'] for r in hit['stocks']))

 def test_compound_suffix_does_not_name_another_company(self):
  b=build_board([{'ticker':'PCG','name':'PG&E Corp','industry':'Utilities'},
                {'ticker':'EPOW','name':'E Power Inc','industry':'Utilities'}],engine='evidence')
  hit=link_articles(b,[{'id':'x','text':'PG&E power shutoff expands.'}])[0]
  self.assertEqual({r['ticker'] for r in hit['stocks'] if r['pass']},{'PCG'})

 def test_product_inside_company_name_and_undefined_acronym_do_not_pass(self):
  b=build_board([{'ticker':'QBTS','name':'D-Wave Quantum','industry':'Computers'},
                {'ticker':'CHKP','name':'Example Security','industry':'Software','description':'Its products include Quantum, CloudGuard, and tools.'},
                {'ticker':'ADV','name':'Example Advisory','industry':'Finance','description':'It offers IPO sponsorship services.'}],engine='evidence')
  hit=link_articles(b,[{'id':'x','text':'D-Wave Quantum (QBTS) reports revenue ahead of its IPO.'}])[0]
  self.assertEqual({r['ticker'] for r in hit['stocks'] if r['pass']},{'QBTS'})

 def test_brand_plural_and_raw_case_resolve_distinct_owners(self):
  b=build_board([{'ticker':'TECH','name':'Example Devices','industry':'Computers','description':'Its products include iPhone, Mac, and tools.'},
                {'ticker':'BEAUTY','name':'Example Beauty','industry':'Cosmetics','description':'Its products include MAC and skincare.'}],engine='evidence')
  for text in ('iPhones sink in a cargo accident.','A software launch supports Macs.'):
   hit=link_articles(b,[{'id':'x','text':text}])[0]
   self.assertEqual({r['ticker'] for r in hit['stocks'] if r['pass']},{'TECH'})

 def test_legal_caption_keeps_event_when_sentence_split_after_v(self):
  b=build_board([{'ticker':'FSCO','name':'FS Credit Opportunities Corp','industry':'Funds'}],engine='evidence')
  hit=link_articles(b,[{'id':'x','text':'FS Credit Opportunities Corp. v. Example Capital Ltd.'}])[0]
  self.assertEqual({r['ticker'] for r in hit['stocks'] if r['pass']},{'FSCO'})

 def test_headline_mode_excludes_related_headline_bundle(self):
  from src.link_correlation import load_articles
  with tempfile.TemporaryDirectory(dir=Path.cwd()) as tmp:
   p=Path(tmp)/'news.json'
   p.write_text(json.dumps([{'id':'x','title':'Local police investigate theft.','body':'Apple raises earnings guidance.'}]),encoding='utf-8')
   articles=load_articles(p,0,headline_only=True)
   self.assertNotIn('Apple',articles[0]['text'])
   self.assertIn('Apple',load_articles(p,0)[0]['text'])

 def test_dictionary_company_short_name_in_coordinated_financial_subject(self):
  b=build_board([{'ticker':'MU','name':'Micron Technology Inc','industry':'Semiconductors'},
                {'ticker':'AMD','name':'Advanced Micro Devices Inc','industry':'Semiconductors'}],engine='evidence')
  hit=link_articles(b,[{'id':'x','text':'Micron and AMD shares are falling.'}])[0]
  self.assertEqual({r['ticker'] for r in hit['stocks'] if r['pass']},{'MU','AMD'})

 def test_batch_permutation_and_repetition_do_not_change_decision(self):
  articles=[{'id':str(i),'text':MEMO} for i in range(9)]
  alone=self.passed(MEMO)
  for hit in link_articles(self.board,articles):
   self.assertEqual(alone,{r['ticker'] for r in hit['stocks'] if r['pass']})

 def test_company_earnings_pass_even_without_industry_jargon(self):
  self.assertEqual(self.passed('Apple raises quarterly earnings guidance.'),{'AAPL'})

 def test_negated_company_event_is_still_related(self):
  self.assertEqual(self.passed('Microsoft denies acquisition report.'),{'MSFT'})

 def test_comparison_is_not_a_direct_event_for_compared_company(self):
  self.assertNotIn('MSFT',self.passed('Unlike Microsoft, the university raised tuition prices.'))

 def test_publisher_is_not_a_company_event(self):
  self.assertNotIn('POST',self.passed('Washington Post reports that local police investigated a theft.'))

 def test_immigration_ice_is_not_exchange(self):
  self.assertNotIn('ICE',{r['ticker'] for r in self.score('ICE arrests dozens in immigration enforcement operation.')['stocks']})

 def test_foreign_ticker_collision_not_name_hit(self):
  self.assertNotIn('KOS',{r['ticker'] for r in self.score('Kosy (KOS) shares fall in Vietnam after foreclosure.')['stocks']})

 def test_owned_drug_event_resolves_biomarin_not_ascendis_alias(self):
  hit=self.score('FDA approves VOXZOGO for children with achondroplasia.')
  self.assertIn('BMRN',{r['ticker'] for r in hit['stocks'] if r['pass']})
  asnd=next((r for r in hit['stocks'] if r['ticker']=='ASND'),None)
  if asnd and asnd['pass']:
   self.assertEqual(asnd['relation'],'competitive_exposure')

 def test_person_or_alias_without_event_does_not_pass(self):
  self.assertFalse(self.passed('TransCon CNP is a named compound.'))

 def test_generic_biotech_terms_do_not_pass_others(self):
  self.assertFalse(self.passed('An unnamed company releases a drug trial readout.'))

 def test_single_valve_word_does_not_pass_any_biotech(self):
  self.assertFalse(self.passed('FDA approves a heart valve designed to grow with children.'))

 def test_place_alone_is_inspection(self):
  hit=self.score('North Carolina police investigate racist messages.')
  self.assertIn('DUK',hit['scoped']);self.assertFalse(any(r['pass'] for r in hit['stocks']))

 def test_named_subsidiary_event_reaches_parent(self):
  row=dict(EXTRA[4],filing_sections=[{'kind':'subsidiary','section':'Exhibit 21','source':'PR642','text':'LinkedIn Corporation\nGitHub Inc'}])
  b=build_board([row])
  h=link_articles(b,[{'id':'x','text':'LinkedIn cuts staff after a revenue decline.'}])[0]
  self.assertEqual({r['ticker'] for r in h['stocks'] if r['pass']},{'MSFT'})

 def test_product_components_are_not_owned_brands(self):
  self.score('A neutral passage without any event.')
  p=self.board.evidence_board.profiles['ASND']
  self.assertFalse(any(f.kind=='owned_product' and f.text=='CNP' for f in p.facts))

 def test_mixed_passages_cannot_combine_into_a_stock_link(self):
  self.assertNotIn('RKLB',self.passed('Office space rents rise. A school launches a new reading program.'))

 def test_partners_do_not_enter_core(self):
  p=self.board.evidence_board.profiles['PLBY']
  self.assertNotIn('draftking',p.core)
  self.assertTrue(any(f.kind=='partner' and f.target_ticker=='DKNG' for f in p.facts))

 def test_competition_section_is_routed_not_deleted(self):
  p=self.board.evidence_board.profiles['ASND']
  self.assertIn('voxzogo',p.buckets['competitor'])
  self.assertNotIn('voxzogo',p.core)

 def test_rare_owned_term_survives_large_industry(self):
  rows=[dict(EXTRA[0],ticker='B'+str(i),name='Biotech '+str(i),description='We develop ordinary medicines for patients.') for i in range(30)]
  b=build_board(rows+[EXTRA[1]])
  h=link_articles(b,[{'id':'x','text':'FDA approves VOXZOGO for achondroplasia.'}])[0]
  self.assertIn('BMRN',{r['ticker'] for r in h['stocks'] if r['pass']})

 def test_short_positive_is_not_forced_unknown(self):
  self.assertIn('AAPL',self.passed('$AAPL raises guidance.'))

 def test_sparse_profile_does_not_remove_company_identity(self):
  board=build_board([{'ticker':'PLUG','name':'Plug Power Inc','industry':'Electrical Equipment'}])
  hit=link_articles(board,[{'id':'x','text':'Plug Power Inc raises earnings guidance.'}])[0]
  self.assertEqual({r['ticker'] for r in hit['stocks'] if r['pass']},{'PLUG'})

 def test_compared_name_does_not_suppress_real_subject(self):
  self.assertEqual(self.passed('Unlike Microsoft, Apple raises earnings guidance.'),{'AAPL'})

 def test_financial_word_does_not_resolve_unrelated_acronym(self):
  board=build_board([{'ticker':'CIG','name':'Cemig SA','industry':'Utilities','description':'We distribute electrical power.'}])
  hit=link_articles(board,[{'id':'x','text':'The CIG pipeline carries natural gas and the operator announced earnings.'}])[0]
  self.assertFalse(any(r['pass'] for r in hit['stocks']))

 def test_sec_legal_name_variants(self):
  rows=[{'ticker':'REGN','name':'REGENERON PHARMACEUTICALS, INC.','industry':'Biotechnology'},
        {'ticker':'SERV','name':'Serve Robotics Inc. /DE/','industry':'Machinery'},
        {'ticker':'BETR','name':'Better Home & Finance Holding Co','industry':'Mortgage'},
        {'ticker':'EQT','name':'EQT Corp','industry':'Oil'}]
  b=build_board(rows,engine='evidence')
  texts=['REGENERON PHARMACEUTICALS, INC. reports earnings.',
         'Serve Robotics Inc. announces financial results.',
         'Better Home & Finance Holding Co raises revenue guidance.',
         'EQT Corp reports earnings.']
  for row,text in zip(rows,texts):
   with self.subTest(ticker=row['ticker']):
    hit=link_articles(b,[{'id':'x','text':text}])[0]
    self.assertIn(row['ticker'],{r['ticker'] for r in hit['stocks'] if r['pass']})

 def test_strategy_in_safe_harbor_is_not_strategy_inc(self):
  b=build_board([{'ticker':'MSTR','name':'Strategy Inc','industry':'Software'}],engine='evidence')
  texts=['Words such as forecast, guidance or strategy identify forward-looking statements.',
         'The chief strategy officer announced the annual results.']
  for text in texts:
   hit=link_articles(b,[{'id':'x','text':text}])[0]
   self.assertFalse(any(r['pass'] for r in hit['stocks']))

 def test_named_recall_does_not_fan_out_on_device_class(self):
  rows=[{'ticker':'BIOX','name':'Example Diagnostics','industry':'Medical Devices','filing':
         'Business\nWe manufacture diagnostic medical devices.\nCompetition\nThe FDA classifies medical devices into three classes.\nIF OUR MANUFACTURING FACILITIES FAIL, WE MAY RECALL SOME PRODUCTS.'}]
  b=build_board(rows,engine='evidence')
  h=link_articles(b,[{'id':'x','text':'Class 2 Device Recall PERMAHAND Silk Suture.'}])[0]
  self.assertFalse(any(r['pass'] for r in h['stocks']))

 def test_english_risk_word_is_not_an_owned_product(self):
  b=build_board([{'ticker':'BIOX','name':'Example Diagnostics','industry':'Medical Devices','filing':
    'Business\nWE MANUFACTURE MEDICAL DEVICES AND MAY RECALL PRODUCTS.'}],engine='evidence')
  h=link_articles(b,[{'id':'x','text':'The FDA issued a device recall.'}])[0]
  self.assertFalse(any(r['relation']=='owned_product_event' for r in h['stocks'] if r['pass']))

 def test_sports_league_is_not_owned_by_bookmaker(self):
  b=build_board([{'ticker':'BETX','name':'Example Sports','industry':'Gambling','filing':
    'Our products\nA majority of our sports betting revenue is generated from bets relating to the NFL and the NCAA.'}],engine='evidence')
  h=link_articles(b,[{'id':'x','text':'NCAA sues Ohio Governor over fifth-year eligibility order.'}])[0]
  self.assertFalse(any(r['pass'] for r in h['stocks']))

 def test_named_issuer_result_does_not_become_generic_peer_exposure(self):
  rows=[{'ticker':'FUEL','name':'Example Refinery','industry':'Refining','description':'We refine crude oil into gasoline and diesel fuel.'},
        {'ticker':'PEER','name':'Another Refinery','industry':'Refining','description':'We refine crude oil into gasoline and diesel fuel.'}]
  b=build_board(rows,engine='evidence')
  h=link_articles(b,[{'id':'x','text':'Example Refinery announces earnings from crude oil, gasoline and diesel sales.'}])[0]
  self.assertEqual({r['ticker'] for r in h['stocks'] if r['pass']},{'FUEL'})

 def test_person_role_and_common_product_word_do_not_create_identity(self):
  rows=[{'ticker':'TESTX','name':'Example Testing','industry':'Diagnostics','filing':
    'Executive Officers\nChief Financial Officer\nBusiness\nWe market our NeXT Personal test for oncology monitoring.'}]
  b=build_board(rows,engine='evidence')
  for text in ['The chief financial officer launched a federal fraud task force.',
               'Next test asks whether the supply chain can deliver.']:
   h=link_articles(b,[{'id':'x','text':text}])[0]
   self.assertFalse(any(r['pass'] for r in h['stocks']))

 def test_employer_caption_is_not_person_surname(self):
  from src.evidence_matcher import _valid_person
  from src.stock_evidence import Fact
  self.assertFalse(_valid_person(Fact('person','Brighthouse Financial','Brighthouse Financial: Executive Vice President and Chief Financial Officer')))

 def test_quoted_corporate_alias_and_foreign_legal_suffix(self):
  from src.correlation import _alias
  self.assertEqual(_alias('Ascendis Pharma A/S'),'ascendis pharma')
  b=build_board([{'ticker':'BRDX','name':'Example Technologies Corp','industry':'Software','filing':
    'Example Technologies Corp ("BrandX") develops enterprise database software.'}],engine='evidence')
  h=link_articles(b,[{'id':'x','text':'BrandX announces earnings guidance.'}])[0]
  self.assertIn('BRDX',{r['ticker'] for r in h['stocks'] if r['pass']})

 def test_common_corporate_short_name_does_not_match_plural_noun(self):
  b=build_board([{'ticker':'OILX','name':'Range Resources Corp','industry':'Oil & Gas','filing':
    'Range Resources Corp ("Range") develops natural gas wells.'}],engine='evidence')
  h=link_articles(b,[{'id':'x','text':'Space launch ranges expand capacity.'}])[0]
  self.assertFalse(any(r['pass'] or r['alias_hit'] for r in h['stocks'] if r['ticker']=='OILX'))

 def test_middle_name_preserves_surname_alias(self):
  rows=[{'ticker':'ORB','name':'Example Orbital','industry':'Aerospace','description':
    'We manufacture rockets and spacecraft. The company was founded by Elon Reeve Musk in 2002.'}]
  b=build_board(rows,engine='evidence')
  h=link_articles(b,[{'id':'x','text':'Elon Musk announces expanded rocket and spacecraft production.'}])[0]
  self.assertIn('ORB',{r['ticker'] for r in h['stocks'] if r['pass']})

 def test_cross_process_repeatability(self):
  import os,subprocess,sys
  code="from src.correlation import build_board,link_articles; from src.test_evidence_matcher import EXTRA; import json; print(json.dumps(link_articles(build_board(EXTRA,engine='evidence'),[{'id':'x','text':'FDA approves VOXZOGO for children with achondroplasia.'}],engine='evidence'),sort_keys=True))"
  outputs=[subprocess.check_output([sys.executable,'-c',code],env={**os.environ,'PYTHONHASHSEED':seed}) for seed in ('1','27')]
  self.assertEqual(outputs[0],outputs[1])

 def test_event_product_class_is_distinct_from_disease_indication(self):
  from src.evidence_matcher import _event_object
  self.assertEqual(_event_object('FDA approves first heart valve designed to grow with kids.'),'heart valve')

 def test_business_evidence_after_four_contexts_remains_usable(self):
  text=' '.join(['Satellite hardware is calibrated. Telemetry equipment is inspected.']*5)+ ' Satellite telemetry instruments monitor launch vehicles.'
  b=build_board([{'ticker':'SATX','name':'Example Orbital','industry':'Aerospace','filing':text}],engine='evidence')
  h=link_articles(b,[{'id':'x','text':'Policy expands satellite telemetry capacity.'}])[0]
  self.assertIn('SATX',{r['ticker'] for r in h['stocks'] if r['pass']})


class LanguageTests(unittest.TestCase):
 def test_dictionary_inflections(self):
  self.assertEqual(lemma('acquired'),lemma('acquire'))
  self.assertEqual(lemma('manufacturing'),lemma('manufacture'))
  self.assertEqual(lemma('launches'),lemma('launch'))

 def test_derivatives_remain_distinct(self):
  self.assertNotEqual(lemma('administrator'),lemma('administration'))
  self.assertNotEqual(lemma('administer'),lemma('administration'))

 def test_phrase_boundaries(self):
  self.assertNotIn('rocket gasoline',terms('rocket. Gasoline prices rose.'))
  self.assertIn('space transportation',terms('Space transportation expands.'))

 def test_verb_usage_does_not_delete_other_noun_occurrences(self):
  p=passages('We plan to launch a website. Rocket launch capacity expands.')
  self.assertIn('rocket launch',p[1].terms)

 def test_publisher_tail_and_duplicate_rss_body_are_not_business_evidence(self):
  title='NCAA sues Ohio Governor over eligibility order - Front Office Sports'
  ps=passages(title+'\n'+title.replace(' - ',' '),title)
  self.assertEqual(len(ps),1)
  self.assertNotIn('sports',ps[0].terms)

 def test_wordnet_pack_available(self):
  from src.semantic_lookup import load_senses
  index,meta=load_senses()
  self.assertTrue(meta['available']);self.assertGreater(meta['senses'],60000)
  self.assertGreater(len(index.get('space',[])),1)


class MeasurementTests(unittest.TestCase):
 def test_extra_unlisted_pass_fails_complete_truth(self):
  case={'id':'x','truth_complete':True,'text':'x'*50,'expected':{'topical_passes':['RKLB']}}
  hit={'status':'scored','stocks':[{'ticker':'RKLB','pass':True},{'ticker':'AAL','pass':True}]}
  self.assertEqual(compare_case(case,hit)['verdict'],'FAIL')

 def test_unknown_positive_is_false_negative(self):
  cases=[{'id':'x','truth_complete':True,'expected':{'topical_passes':['RKLB']}}]
  m=score_links(cases,{'x':{'decision':'insufficient_text','stocks':[]}})
  self.assertEqual(m['counts']['fn'],1);self.assertEqual(m['exact_set_accuracy'],0)

 def test_no_predictions_or_all_negative_cannot_reach_bar(self):
  cases=[{'id':str(i),'truth_complete':True,'expected':{'topical_passes':[]}} for i in range(100)]
  m=score_links(cases,{str(i):{'stocks':[]} for i in range(100)})
  self.assertEqual(m['exact_set_accuracy'],1);self.assertFalse(m['sample_bar_reached'])

 def test_missing_negative_output_is_failure(self):
  m=score_links([{'id':'x','truth_complete':True,'expected':{'topical_passes':[]}}],{})
  self.assertEqual(m['exact_set_accuracy'],0)

 def test_partial_assertions_are_not_accuracy_evidence(self):
  m=score_links([{'id':'x','expected':{'must_not_pass':['AAL']}}],{'x':{'stocks':[]}})
  self.assertEqual(m['counts']['excluded_partial_or_unlabeled'],1)


class ArchiveTests(unittest.TestCase):
 def test_20f_business_and_officers(self):
  text='ITEM 4. Information on the Company\n'+('We manufacture launch vehicles. '*30)+'\nITEM 6. Directors\n'+('Jane Doe is chief executive. '*10)
  v=section_views(text,'20-F')
  self.assertEqual([x['kind'] for x in v],['business','people'])

 def test_no_twenty_thousand_character_cap(self):
  text='ITEM 1. Business\n'+('We manufacture satellites. '*1500)+'\nITEM 1A. Risk Factors\n'+('Supply is constrained. '*10)
  v=section_views(text,'10-K')
  self.assertGreater(len(v[0]['text']),20000)

 def test_current_reports_are_updates(self):
  self.assertEqual(section_views('We acquired a new factory.','8-K')[0]['kind'],'update')

 def test_machine_views_not_business(self):
  self.assertEqual(section_views('<schema>data</schema>','40-F','facts.xsd')[0]['kind'],'machine_data')

 def test_future_cache_cannot_satisfy_earlier_cutoff(self):
  import gzip
  from src.profile_cache import load_profiles
  with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory:
   path=Path(directory)/'profiles.gz'
   path.write_bytes(gzip.compress((json.dumps({'format':'stock-evidence-1','as_of':'2026-10-19'})+'\n').encode()))
   with self.assertRaisesRegex(ValueError,'later than requested'):
    load_profiles(path,as_of='2026-10-01')


if __name__=='__main__':unittest.main()
