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
 def test_product_list_internal_conjunction_is_not_a_brand_boundary(self):
  row={'ticker':'N','name':'Example Software Inc','industry':'Software','description':'Its products include Remote Monitoring and Management, Backup and Data Protection, and ZedConsole.'}
  p=build_profile(row,Catalog([row]))
  self.assertFalse(any(f.kind=='owned_product' and f.text in {'Management','Protection','Monitoring'} for f in p.facts))
  self.assertTrue(any(f.kind=='owned_product' and f.text=='ZedConsole' for f in p.facts))

 def test_company_count_nouns_and_publisher_spans_are_not_issuers(self):
  b=build_board([{'ticker':'P','name':'People Inc','industry':'Media'}, {'ticker':'POST','name':'Post Holdings Inc','industry':'Food'}],engine='evidence')
  for text in ('Three People Charged in an investigation.','One local restaurant wins a Washington Post ranking.'):
   hit=link_articles(b,[{'id':'x','text':text}])[0]
   self.assertFalse(any(r['pass'] for r in hit['stocks']),(text,hit['stocks']))

 def test_catalog_symbol_inside_another_issuer_product_is_not_company(self):
  b=build_board([{'ticker':'NVDA','name':'Nvidia Corp','industry':'Chips'},{'ticker':'RTX','name':'RTX Corp','industry':'Defense'}],engine='evidence')
  hit=link_articles(b,[{'id':'x','text':'NVIDIA RTX Spark launches today.'}])[0]
  self.assertEqual({r['ticker'] for r in hit['stocks'] if r['pass']},{'NVDA'})

 def test_bound_symbol_vetoes_other_company_with_same_name(self):
  b=build_board([{'ticker':'LODE','name':'Comstock Inc','industry':'Mining'},{'ticker':'CHCI','name':'Comstock Holding Company Inc','industry':'Housing'}],engine='evidence')
  hit=link_articles(b,[{'id':'x','text':'Comstock (LODE) shares rise after results.'}])[0]
  self.assertEqual({r['ticker'] for r in hit['stocks'] if r['pass']},{'LODE'})

 def test_source_domain_root_does_not_override_longer_proper_name(self):
  b=build_board([{'ticker':'G','name':'Gold.com Inc','industry':'Gold'}],engine='evidence')
  for text in ('Soma Gold announces a new loan.','Court enters a decree against Gold Star Distribution Inc.'):
   hit=link_articles(b,[{'id':'x','text':text}])[0]
   self.assertFalse(any(r['pass'] for r in hit['stocks']),(text,hit['stocks']))

 def test_verified_filing_overlay_keeps_explicit_ownership_and_dates(self):
  b=build_board([{'ticker':'OWN','name':'Example Aerospace Inc','industry':'Aerospace','filing_sections':[{'text':'"SkyWeb" refers to our satellite network.','kind':'identity_source','source':'PR642','filing_date':'2026-01-02','complete':True}]}],engine='evidence')
  hit=link_articles(b,[{'id':'x','text':'SkyWeb faces new restrictions.','published_at':'2026-01-03'}])[0]
  self.assertEqual({r['ticker'] for r in hit['stocks'] if r['pass']},{'OWN'})
  p=b.evidence_board.profiles['OWN'];self.assertFalse(p.core)
  self.assertTrue(any('identity_overlay_only' in w for w in p.warnings))

 def test_filing_views_preserve_preamble_and_short_sections(self):
  text='Example Corp ("Zed")\nITEM 1. Business\nWe make widgets.\nITEM 1A. Risk Factors\nSupply disruption.'
  views=section_views(text,'10-K')
  self.assertEqual(''.join(v['text'] for v in views),text)
  self.assertEqual(views[0]['kind'],'identity_source')

 def test_broker_price_target_attribution_keeps_only_target(self):
  b=build_board([{'ticker':'B','name':'Zedbank Corp','industry':'Banking'}, {'ticker':'T','name':'Vexgen Biotech Inc','industry':'Biotech'}],engine='evidence')
  hit=link_articles(b,[{'id':'x','text':'Zedbank raises Vexgen Biotech price target on new data.'}])[0]
  self.assertEqual({r['ticker'] for r in hit['stocks'] if r['pass']},{'T'})

 def test_listing_notice_is_not_an_exchange_operator_event(self):
  b=build_board([{'ticker':'NDAQ','name':'Nasdaq Inc','industry':'Exchange'}, {'ticker':'TEST','name':'Vexgen Inc','industry':'Biotech'}],engine='evidence')
  for text,want in [('Vexgen receives Nasdaq Minimum Bid Price Deficiency Notice.',{'TEST'}),('Nasdaq reports record quarterly revenue.',{'NDAQ'})]:
   hit=link_articles(b,[{'id':'x','text':text}])[0]
   self.assertEqual({r['ticker'] for r in hit['stocks'] if r['pass']},want)

 def test_source_initialism_can_report_its_own_profit_without_jargon(self):
  b=build_board([{'ticker':'N','name':'Northern Semiconductor Manufacturing ADR','industry':'Chips','description':'Northern Semiconductor Manufacturing Co., Ltd. manufactures semiconductor chips.'}],engine='evidence')
  hit=link_articles(b,[{'id':'x','text':'NSMC posts record profit for a sixth quarter.'}])[0]
  self.assertEqual({r['ticker'] for r in hit['stocks'] if r['pass']},{'N'})

 def test_fund_research_method_is_not_operating_material_exposure(self):
  row={'ticker':'FUND','name':'Example Equity Fund','industry':'Fund','description':'It employs fundamental analysis focusing on overall growth prospects, labor costs, and raw material costs to create its portfolio.'}
  p=build_profile(row,Catalog([row]))
  self.assertTrue(any(f.kind=='investment_methodology' for f in p.facts))
  self.assertFalse(any(f.kind=='material_exposure' for f in p.facts))

 def test_distinctive_segment_brand_has_corporate_subject_evidence(self):
  b=build_board([{'ticker':'P','name':'Example Holdings Inc','industry':'Software','description':'Example Holdings Inc operates through two segments. The Zedgle Services segment provides search tools. The Zedgle Cloud segment provides infrastructure.'}],engine='evidence')
  hit=link_articles(b,[{'id':'x','text':'Zedgle breaks a quantum computing bottleneck.'}])[0]
  self.assertEqual({r['ticker'] for r in hit['stocks'] if r['pass']},{'P'})

 def test_product_list_preserves_compounds_and_rejects_categories(self):
  rows=[{'ticker':'SENSOR','name':'Vexcom Inc','industry':'Devices','description':'Its products include Vexcom G6, Vexcom Share, and Vexcom Hold.'},
        {'ticker':'SOFT','name':'Example Software Inc','industry':'Software','description':'Its products include Artificial Intelligence (AI) and Machine Learning (ML) software.'},
        {'ticker':'NET','name':'Example Networks Inc','industry':'Networking','description':'Its products include the following categories: Collaboration, Networking, and Security.'}]
  b=build_board(rows,engine='evidence')
  for text,want in [('Share prices rise after earnings.',set()),('Hold investor conference next week.',set()),('Artificial learning software launches.',set()),('Collaboration expands in schools.',set()),('Vexcom Share launches a new feature.',{'SENSOR'})]:
   hit=link_articles(b,[{'id':'x','text':text}])[0]
   self.assertEqual({r['ticker'] for r in hit['stocks'] if r['pass']},want,text)
  for p in b.evidence_board.profiles.values():
   self.assertFalse(any(f.kind=='owned_product' and f.text in {'Share','Hold','Artificial','Learning','Collaboration'} for f in p.facts))

 def test_retail_brands_are_not_owned_products(self):
  rows=[{'ticker':'SHOP','name':'Example Retail Inc','industry':'Retail','description':'It sells brands including ZedCola and ZedSnacks.'},
        {'ticker':'DRUG','name':'Example Pharma Inc','industry':'Biotech','description':'We market VOXZOGO (vosoritide) for genetic disorders.'}]
  b=build_board(rows,engine='evidence')
  for text,want in [('ZedCola announces a new beverage deal.',set()),('FDA approves VOXZOGO.',{'DRUG'})]:
   hit=link_articles(b,[{'id':'x','text':text}])[0]
   self.assertEqual({r['ticker'] for r in hit['stocks'] if r['pass']},want)

 def test_common_first_name_fragments_need_full_name_or_bound_ticker(self):
  rows=[{'ticker':'STOCK','name':'Stock Yards Bancorp Inc','industry':'Banking'},
        {'ticker':'OPEN','name':'Open Text Corp','industry':'Software'},
        {'ticker':'TRUMP','name':'Trump Media Corp','industry':'Media'},
        {'ticker':'FERMI','name':'Fermi Inc','industry':'Energy'}]
  b=build_board(rows,engine='evidence')
  for text,want in [('United Rentals Stock earnings rise.',set()),('Open learning expands access.',set()),('President Trump announces tariffs.',set()),('Fermi (FERMI) shares rise.',{'FERMI'}),('Stock Yards Bancorp reports earnings.',{'STOCK'})]:
   hit=link_articles(b,[{'id':'x','text':text}])[0]
   self.assertEqual({r['ticker'] for r in hit['stocks'] if r['pass']},want,text)

 def test_generic_legal_phrase_needs_corporate_usage(self):
  b=build_board([{'ticker':'QC','name':'Quantum Computing Inc','industry':'Computers'}],engine='evidence')
  for text,want in [('Scientists discuss quantum computing at a new conference.',set()),('Quantum Computing stock rises on earnings.',{'QC'})]:
   hit=link_articles(b,[{'id':'x','text':text}])[0]
   self.assertEqual({r['ticker'] for r in hit['stocks'] if r['pass']},want)

 def test_legal_domain_and_hyphen_are_exact_source_identities(self):
  b=build_board([{'ticker':'SHOP','name':'Zedshop.com Inc','industry':'Retail'}, {'ticker':'COLA','name':'Zed Cola Co','industry':'Beverages'}],engine='evidence')
  for text,want in [('Zedshop launches a new store.',{'SHOP'}),('Zed-Cola announces a new deal.',{'COLA'})]:
   hit=link_articles(b,[{'id':'x','text':text}])[0]
   self.assertEqual({r['ticker'] for r in hit['stocks'] if r['pass']},want)

 def test_lowercase_of_complement_does_not_extend_company_name(self):
  b=build_board([{'ticker':'LIVE','name':'Live Nation Entertainment Inc','industry':'Entertainment'}],engine='evidence')
  hit=link_articles(b,[{'id':'x','text':'Court clears Live Nation of negligence charges.'}])[0]
  self.assertEqual({r['ticker'] for r in hit['stocks'] if r['pass']},{'LIVE'})

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

 def test_dictionary_predicates_cover_ordinary_company_headlines(self):
  b=build_board([{'ticker':'XR','name':'Example Devices Corp','industry':'Devices'}],engine='evidence')
  for text in ('Example Devices collaborates on a new design.','Example Devices names a new executive.','Example Devices receives a stock grant.','Example Devices shares jump.'):
   hit=link_articles(b,[{'id':'x','text':text}])[0]
   self.assertEqual({r['ticker'] for r in hit['stocks'] if r['pass']},{'XR'},text)

 def test_joining_legal_tokens_is_identity_not_a_spelling_neighbor(self):
  b=build_board([{'ticker':'OIL','name':'Exxon Mobil Corp','industry':'Oil'},
                {'ticker':'NEIGHBOR','name':'Exxon Mobile Corp','industry':'Phones'}],engine='evidence')
  hit=link_articles(b,[{'id':'x','text':'ExxonMobil signals higher earnings.'}])[0]
  self.assertEqual({r['ticker'] for r in hit['stocks'] if r['pass']},{'OIL'})

 def test_public_officer_is_not_a_common_word_company(self):
  b=build_board([{'ticker':'CITY','name':'City Holding Co','industry':'Banks'}],engine='evidence')
  hit=link_articles(b,[{'id':'x','text':'The city police chief resigns.'}])[0]
  self.assertFalse(any(r['pass'] for r in hit['stocks']))

 def test_geographic_fragment_is_not_a_company_short_name(self):
  b=build_board([{'ticker':'GOOS','name':'Canada Goose Holdings Inc','industry':'Apparel'}],engine='evidence')
  hit=link_articles(b,[{'id':'x','text':'Air Canada launches flights to a new city.'}])[0]
  self.assertFalse(any(r['pass'] for r in hit['stocks']))

 def test_generic_segment_prefixes_are_not_operating_brands(self):
  b=build_board([{'ticker':'TEST','name':'Example Financial Inc','industry':'Banking','description':'Example Financial operates through two segments. The Commercial Lending segment offers loans. The Commercial Property segment offers real estate financing.'}],engine='evidence')
  hit=link_articles(b,[{'id':'x','text':'Top green hydrogen companies: commercial production capacity and investments.'}])[0]
  self.assertFalse(any(r['pass'] for r in hit['stocks']))

 def test_explicit_industry_type_vetoes_incompatible_short_name(self):
  b=build_board([{'ticker':'SHOP','name':'Citi Trends Inc','industry':'Retail Apparel'},
                {'ticker':'BANK','name':'Example Financial Inc','industry':'Banks Regional'}],engine='evidence')
  hit=link_articles(b,[{'id':'x','text':'Citi becomes latest bank to restrict account access.'}])[0]
  self.assertFalse(any(r['pass'] for r in hit['stocks']))

 def test_ampersand_quote_directory_does_not_create_event(self):
  self.assertFalse(self.passed('Apple (AAPL) stock price, news, quote & history - Yahoo Finance'))

 def test_lowercase_style_preserves_unique_owned_brand(self):
  b=build_board([{'ticker':'OWN','name':'Example Medicines Inc','industry':'Biotech','description':'We manufacture Zepbound for obesity.'}],engine='evidence')
  hit=link_articles(b,[{'id':'x','text':'fda approves zepbound for obesity.'}])[0]
  self.assertEqual({r['ticker'] for r in hit['stocks'] if r['pass']},{'OWN'})

 def test_one_named_bank_rule_does_not_become_sector_policy(self):
  b=build_board([{'ticker':'BANK','name':'Example Financial Inc','industry':'Banks','description':'We offer bank accounts and provide access to accounts through online agents.'}],engine='evidence')
  hit=link_articles(b,[{'id':'x','text':'ZedBank becomes latest bank to restrict agents from accessing accounts.'}])[0]
  self.assertFalse(any(r['pass'] for r in hit['stocks']))

 def test_exact_legal_brand_outranks_longer_short_name(self):
  b=build_board([{'ticker':'AAPL','name':'Apple Inc','industry':'Devices'},
                {'ticker':'APLE','name':'Apple Hospitality REIT Inc','industry':'Hotels'}],engine='evidence')
  hit=link_articles(b,[{'id':'x','text':'Apple moves to overturn a patent verdict.'}])[0]
  self.assertEqual({r['ticker'] for r in hit['stocks'] if r['pass']},{'AAPL'})

 def test_coordinated_issuer_subjects_share_corporate_predicate(self):
  b=build_board([{'ticker':'N','name':'Nvidia Corp','industry':'Chips'},
                {'ticker':'M','name':'Meta Platforms Inc','industry':'Software'},
                {'ticker':'A','name':'Apple Inc','industry':'Devices'},
                {'ticker':'H','name':'Apple Hospitality REIT Inc','industry':'Hotels'}],engine='evidence')
  hit=link_articles(b,[{'id':'x','text':'Nvidia, Meta and Apple unveil new models.'}])[0]
  self.assertEqual({r['ticker'] for r in hit['stocks'] if r['pass']},{'N','M','A'})

 def test_only_last_publisher_suffix_is_removed(self):
  p=passages('Strategy stock diverges from bitcoin - Strategy (NASDAQ:MSTR) - Publisher','Strategy stock diverges from bitcoin - Strategy (NASDAQ:MSTR) - Publisher')
  self.assertIn('NASDAQ:MSTR',p[0].text)

 def test_alias_definition_keeps_all_quoted_issuer_names(self):
  b=build_board([{'ticker':'AIR','name':'Southwest Airlines Co','industry':'Airlines','filing':'Southwest Airlines Co. (the "Company" or "Southwest") operates passenger aircraft.'}],engine='evidence')
  hit=link_articles(b,[{'id':'x','text':'Southwest unveils new perks.'}])[0]
  self.assertEqual({r['ticker'] for r in hit['stocks'] if r['pass']},{'AIR'})

 def test_glossary_explicitly_defines_owned_network(self):
  b=build_board([{'ticker':'OWN','name':'Example Aerospace Inc','industry':'Aerospace','filing':'"SkyWeb" refers to our global satellite constellation and broadband network.'},
                {'ticker':'PEER','name':'Other Aerospace Inc','industry':'Aerospace','filing':'"SkyWeb" refers to a competitor satellite network.'}],engine='evidence')
  hit=link_articles(b,[{'id':'x','text':'SkyWeb faces new restrictions.'}])[0]
  self.assertEqual({r['ticker'] for r in hit['stocks'] if r['pass']},{'OWN'})

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

 def test_unresolved_annotation_cannot_enter_accuracy_denominator(self):
  case={'id':'x','truth_complete':True,'adjudication_required':True,'expected':{'topical_passes':[]}}
  metric=score_links([case],{'x':{'stocks':[]}})
  self.assertIsNone(metric['exact_set_accuracy'])
  self.assertEqual(metric['counts']['excluded_partial_or_unlabeled'],1)

 def test_history_is_retained_but_is_not_operating_vocabulary(self):
  row={'ticker':'EDU','name':'Example Education','industry':'Education','description':'We provide medical education. The company was founded in March and is headquartered in Nova Lima.'}
  p=build_profile(row,Catalog([row]))
  self.assertNotIn('march',p.core);self.assertNotIn('nova',p.core)
  self.assertTrue(any(f.kind=='history' and 'Nova Lima' in f.passage for f in p.facts))

 def test_calendar_and_launch_verb_do_not_make_industry_exposure(self):
  b=build_board([{'ticker':'FUND','name':'Example Fund','industry':'Funds','description':'The fund was launched in March. We invest in corporate debt.'},
                {'ticker':'LAB','name':'Example Research','industry':'Research','description':'We manufacture instruments for universities and research centers.'}],engine='evidence')
  for text in ('A manufacturer launches a steering wheel in March.','A university launches a religious research center.'):
   hit=link_articles(b,[{'id':'x','text':text}])[0]
   self.assertFalse(any(r['pass'] for r in hit['stocks']))

 def test_exchange_qualification_is_not_exchange_operator_event(self):
  b=build_board([{'ticker':'NDAQ','name':'Nasdaq Inc','industry':'Exchanges'},
                {'ticker':'PAYS','name':'Paysign Inc','industry':'Payments'}],engine='evidence')
  hit=link_articles(b,[{'id':'x','text':'Paysign (NASDAQ:PAYS) CEO sells shares.'}])[0]
  self.assertEqual({r['ticker'] for r in hit['stocks'] if r['pass']},{'PAYS'})
  hit=link_articles(b,[{'id':'x','text':'Tech stocks slump as Nasdaq faces weekly loss.'}])[0]
  self.assertNotIn('NDAQ',{r['ticker'] for r in hit['stocks'] if r['pass']})

 def test_longer_issuer_name_owns_its_internal_name_words(self):
  b=build_board([{'ticker':'PNBK','name':'Patriot National Bancorp Inc','industry':'Banks'},
                {'ticker':'TBBK','name':'Bancorp Inc','industry':'Banks'},
                {'ticker':'BAC','name':'Bank of America Corp','industry':'Banks'},
                {'ticker':'AMX','name':'America Movil ADR','industry':'Telecom'}],engine='evidence')
  for text,ticker in [('Patriot National Bancorp reports earnings.','PNBK'),('Bank of America reports earnings.','BAC')]:
   hit=link_articles(b,[{'id':'x','text':text}])[0]
   self.assertEqual({r['ticker'] for r in hit['stocks'] if r['pass']},{ticker})

 def test_infinitive_post_is_not_cereal_company(self):
  b=build_board([{'ticker':'POST','name':'Post Holdings Inc','industry':'Food'},
                {'ticker':'HFWA','name':'Heritage Financial Corp','industry':'Banks'}],engine='evidence')
  hit=link_articles(b,[{'id':'x','text':'Heritage Financial expected to Post earnings on Thursday.'}])[0]
  self.assertEqual({r['ticker'] for r in hit['stocks'] if r['pass']},{'HFWA'})

 def test_quote_directory_is_not_a_reported_company_event(self):
  self.assertFalse(self.passed('Apple (AAPL) stock price, news, quote and history - Yahoo Finance'))

 def test_financial_disclosure_and_corporate_roles_are_events(self):
  b=build_board([{'ticker':'TSLA','name':'Tesla Inc','industry':'Auto'},
                {'ticker':'CNC','name':'Centene Corp','industry':'Health'},
                {'ticker':'CPRT','name':'Copart Inc','industry':'Auto'}],engine='evidence')
  for text,ticker in [('Tesla energy executive steps down.','TSLA'),('Form 4 CENTENE CORP Filed by: A Director','CNC'),('Copart completes an acquisition.','CPRT')]:
   hit=link_articles(b,[{'id':'x','text':text}])[0]
   self.assertEqual({r['ticker'] for r in hit['stocks'] if r['pass']},{ticker})

 def test_batch_permutation_and_repetition_do_not_change_decision(self):
  articles=[{'id':str(i),'text':MEMO} for i in range(9)]
  alone=self.passed(MEMO)
  for hit in link_articles(self.board,articles):
   self.assertEqual(alone,{r['ticker'] for r in hit['stocks'] if r['pass']})

 def test_short_name_preserves_verified_share_classes(self):
  rows=[{'ticker':t,'name':'Twin Harbor Industries Inc','industry':'Machinery','description':'Twin Harbor Industries Inc manufactures industrial pumps.'} for t in ('TWA','TWB')]
  b=build_board(rows,engine='evidence')
  hit=link_articles(b,[{'id':'x','text':'Twin invests $300 million in a factory.'}])[0]
  self.assertEqual({r['ticker'] for r in hit['stocks'] if r['pass']},{'TWA','TWB'})

 def test_ambiguous_short_name_requires_discriminating_operations(self):
  b=build_board([{'ticker':'AIR','name':'Southwest Airlines Inc','industry':'Airlines','description':'Southwest Airlines operates aircraft flights and a rewards program for flyers.'},
                {'ticker':'GAS','name':'Southwest Gas Inc','industry':'Utilities','description':'Southwest Gas distributes natural gas to residential heating customers.'}],engine='evidence')
  hit=link_articles(b,[{'id':'x','text':'Southwest unveils rewards program for flyers.'}])[0]
  self.assertEqual({r['ticker'] for r in hit['stocks'] if r['pass']},{'AIR'})

 def test_asset_code_does_not_become_short_issuer_identity(self):
  b=build_board([{'ticker':'TRON','name':'Tron Inc','industry':'Toys'},
                {'ticker':'TRX','name':'TRX Gold Inc','industry':'Gold','description':'TRX Gold mines gold deposits.'}],engine='evidence')
  hit=link_articles(b,[{'id':'x','text':'Tron files stock offering to fund TRX purchases.'}])[0]
  self.assertEqual({r['ticker'] for r in hit['stocks'] if r['pass']},{'TRON'})

 def test_named_acquisition_object_is_related(self):
  b=build_board([{'ticker':'ACVA','name':'ACV Auctions Inc','industry':'Auto'}],engine='evidence')
  hit=link_articles(b,[{'id':'x','text':'Company completes acquisition of Buffalo-based ACV.'}])[0]
  self.assertEqual({r['ticker'] for r in hit['stocks'] if r['pass']},{'ACVA'})

 def test_dba_must_belong_to_issuer_legal_introduction(self):
  b=build_board([{'ticker':'OWN','name':'Orbital Devices Corp','industry':'Aerospace','description':'Orbital Devices Corp., doing business as SkyWorks, manufactures satellite antennas.'},
                {'ticker':'PEER','name':'Example Tools Inc','industry':'Tools','description':'Our competitor Orbital Devices Corp., doing business as SkyWorks, manufactures satellite antennas.'}],engine='evidence')
  hit=link_articles(b,[{'id':'x','text':'SkyWorks announces earnings.'}])[0]
  self.assertEqual({r['ticker'] for r in hit['stocks'] if r['pass']},{'OWN'})

 def test_derived_initialism_requires_source_and_business_corroboration(self):
  b=build_board([{'ticker':'NSM','name':'Northern Semiconductor Manufacturing ADR','industry':'Chips','description':'Northern Semiconductor Manufacturing Co., Ltd. manufactures semiconductor chips.'}],engine='evidence')
  for text,want in [('Chip leader NSMC delivers quarterly earnings report.',{'NSM'}),('NSMC delivers quarterly earnings report.',set()),('The agency NSMC announces a rule.',set())]:
   hit=link_articles(b,[{'id':'x','text':text}])[0]
   self.assertEqual({r['ticker'] for r in hit['stocks'] if r['pass']},want)

 def test_owned_operating_segment_brand_needs_distinct_segments(self):
  b=build_board([{'ticker':'PARENT','name':'Example Holdings Inc','industry':'Software','description':'Example Holdings Inc operates through two segments. The Bright Services segment provides search tools. The Bright Cloud segment provides computing infrastructure.'},
                {'ticker':'PEER','name':'Other Holdings Inc','industry':'Software','description':'Other Holdings Inc operates through one segment. The Bright Services segment provides search tools.'}],engine='evidence')
  hit=link_articles(b,[{'id':'x','text':'Bright launches search tools.'}])[0]
  self.assertEqual({r['ticker'] for r in hit['stocks'] if r['pass']},{'PARENT'})

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
 def test_registration_statements_retain_business_source(self):
  for form in ('S-1','S-1/A','F-1','F-1/A'):
   self.assertEqual(section_views('Our network provides satellite broadband.',form)[0]['kind'],'business')
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

 def test_old_extractor_cache_requires_rebuild(self):
  import gzip
  from src.profile_cache import load_profiles
  with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory:
   path=Path(directory)/'profiles.gz'
   path.write_bytes(gzip.compress((json.dumps({'format':'stock-evidence-1','as_of':'2026-10-01'})+'\n').encode()))
   with self.assertRaisesRegex(ValueError,'rebuild from original sources'):
    load_profiles(path,as_of='2026-10-19')


if __name__=='__main__':unittest.main()
