"""Semantic controls independent of the frozen development replay/reference."""
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch
from src.news_impact.classify import classify_text
from src.news_impact.headline_events import identify_headline_event
from src.news_impact.headline_sectors import identify_headline_sectors
from src.news_impact.institutional_actions import identify_institutional_actions
from src.news_impact.finviz_linker import FinvizIndex
from src.news_intake import first_pass


class HeadlineAnalysis(unittest.TestCase):
    def test_embedded_event_survives_reaction(self):
        for title, expected in [
            ('Retailer shares tumble after profit warning', 'guidance'),
            ('Markets fall after payrolls miss forecasts', 'factor_impulse'),
            ('Stocks jump as diesel sanctions lifted', 'sanction_lift'),
            ('Smallcap falls after private placement closes', 'dilution'),
            ('Technology shares fall as company files for bankruptcy', 'distress_restruct'),
            ('Company stock slides after opening new factory', 'capacity'),
        ]:
            with self.subTest(title=title):
                self.assertEqual(classify_text(title).event_class, expected)
        self.assertEqual(classify_text('Company shares tumble today').event_class, 'discard')

    def test_macro_only_path_preserves_embedded_print(self):
        from src.news_impact.classify import classify_article
        with patch('src.news_impact.classify.is_macro_only',return_value=True):
            self.assertEqual(classify_article({'title':'Stocks plunge after payrolls miss estimates'}).event_class,'factor_impulse')

    def test_rejected_keyword_does_not_choose_wrong_analyst(self):
        for title in ['Football player walkout sparks debate','Automaker extends buyback scheme for car models']:
            row=first_pass({'id':'x','title':title,'first_seen':'2026-10-13T00:00:00Z'},datetime.now(timezone.utc))
            self.assertEqual(row['routing_class'],'discard')
            self.assertEqual(row['state'],'review')

    def test_distinct_word_senses(self):
        for title, expected in [
            ('Football captain comments on teammate walkout', 'discard'),
            ('Union workers announce walkout at dock', 'labor_stop'),
            ('Appliance maker extends buyback scheme for old car models', 'discard'),
            ('Appliance maker starts share buyback', 'capital_return'),
            ('County executive faces a personal tax bill', 'discard'),
            ('Furniture plant fire disrupts operations', 'blast_ops'),
            ('Actor joins television spinoff', 'discard'),
        ]:
            with self.subTest(title=title):
                self.assertEqual(classify_text(title).event_class, expected)

    def test_stage_is_not_completion(self):
        for title in ['FDA may approve device next quarter', 'US could lift sanctions next year',
                      'FDA has not approved the device']:
            with self.subTest(title=title):
                self.assertEqual(classify_text(title).event_class, 'statement_public')
                self.assertFalse(identify_institutional_actions(title)['has_concrete_reported_action'])
        row = identify_headline_event('Drug maker expects trial results next month')
        self.assertEqual(row['stage'], 'scheduled_readout')
        self.assertEqual(row['lane_class'], 'statement_public')
        row = identify_headline_event('Drug maker meets primary endpoint in controlled trial')
        self.assertEqual(row['lane_class'], 'trial_readout')
        self.assertEqual(row['stage'], 'reported')

    def test_ruling_preserves_domain_and_implicit_holding(self):
        self.assertEqual(classify_text('Drug maker wins patent ruling').event_class,'ip_ruling')
        self.assertEqual(classify_text('Court rules pipeline law unconstitutional').event_class,'gate')
        row=identify_institutional_actions('Contract Date Cannot Determine Import Date: High Court')
        self.assertTrue(row['has_concrete_reported_action'])
        self.assertEqual(row['actions'][0]['action'],'Cannot Determine')
        self.assertFalse(identify_institutional_actions('Purchase order date updated by company')['actions'])

    def test_existing_mixed_guidance_hygiene_is_preserved(self):
        mixed=classify_text('Company raises guidance but misses EPS estimates')
        self.assertEqual(mixed.event_class,'guidance')
        self.assertIsNone(mixed.sign)
        self.assertTrue(mixed.split)
        self.assertEqual(mixed.split_facts,['guidance','print_vs_priced'])
        self.assertIsNone(classify_text('Company reaffirms guidance').sign)

    def test_primary_event_beats_subordinate_keyword(self):
        self.assertEqual(classify_text('Family buys pharmacy business for $2 billion, ending IPO hopes').event_class,'corporate_action_mna')
        self.assertEqual(classify_text('Merger clearance waiting period resets').event_class,'deal_review')
        self.assertEqual(classify_text('Ministers clarify reserve release amid export ban threats').event_class,'statement_public')

    def test_authority_binding_and_passive_decision(self):
        row = identify_institutional_actions('Russia lifts export ban after talks with Trump')['actions'][0]
        self.assertEqual(row['authority'], 'Russia')
        self.assertEqual(row['jurisdiction'], 'Russia')
        row = identify_institutional_actions('Diesel sanctions lifted after diplomatic talks')['actions'][0]
        self.assertIsNone(row['authority'])
        self.assertTrue(row['concrete_reported_action'])
        row = identify_institutional_actions('9th Circuit grants retailer temporary injunction')['actions'][0]
        self.assertEqual(row['action_category'], 'judicial_ruling')
        self.assertEqual(row['stage'], 'interim_decision_reported')
        self.assertTrue(row['us_scope'])
        self.assertFalse(identify_institutional_actions("India's IPO boom increases ratings of public offers")['actions'])

    def test_negated_injunction_is_not_concrete(self):
        for title in ['US judge may grant temporary injunction', 'US judge did not grant temporary injunction']:
            self.assertFalse(identify_institutional_actions(title)['has_concrete_reported_action'])

    def test_multistory_action_sectors_are_separate(self):
        from src.news_impact.headline_analysis import analyse_headline
        row=analyse_headline('FDA approves surgical valve; Russia lifts diesel sanctions')
        self.assertEqual([a['market_sectors'] for a in row['institutional_actions']['actions']],[['Healthcare'],['Energy']])

    def test_multi_clause_modality(self):
        row=identify_institutional_actions('Ministry presents industry awards and calls for more investment')
        self.assertTrue(row['has_concrete_reported_action'])
        row=identify_institutional_actions('US Senate committee approves infrastructure bill; FDA may approve device')
        self.assertEqual([a['concrete_reported_action'] for a in row['actions']],[True,False])

    def test_enum_gap_preserves_recognized_event(self):
        row=identify_headline_event('Supplier renews supply agreement with customer')
        self.assertEqual(row['status'],'enum_gap')
        self.assertEqual(row['event_type'],'supply_agreement')
        self.assertIsNone(row['lane_class'])

    def test_catalog_aliases_do_not_turn_words_into_companies(self):
        rows=[{'ticker':'DG','company':'Dollar General Corp','aliases':['dollar general','dollar'], 'sector':'Consumer Defensive','industry':'Discount Stores','etf':False},
              {'ticker':'NOW','company':'ServiceNow Inc','aliases':['servicenow'], 'sector':'Technology','industry':'Software - Application','etf':False},
              {'ticker':'SO','company':'Southern Company','aliases':['southern'], 'sector':'Utilities','industry':'Utilities - Regulated Electric','etf':False}]
        with patch('src.news_impact.headline_sectors.get_index',return_value=FinvizIndex(rows,'fixture')):
            for title in ['Dollar strengthens now as yields rise','Earthquake hits Southern California', 'Company launches customer service now']:
                self.assertFalse(identify_headline_sectors(title)['matches'],title)
            r=identify_headline_sectors('Dollar General raises guidance')
            self.assertEqual(r['market_sectors'],['Consumer Defensive'])
            for title in ['Retailer (DG) raises guidance','Retailer $DG raises guidance','Retailer NYSE:DG raises guidance']:
                self.assertEqual(identify_headline_sectors(title)['market_sectors'],['Consumer Defensive'])
            r=identify_headline_sectors('ServiceNow wins contract')
            self.assertEqual(r['market_sectors'],['Technology'])
            self.assertFalse(identify_headline_sectors('Private company shelves IPO - NVIDIA (NASDAQ:NVDA) - Publisher')['matches'])

    def test_sectors_survive_market_abstention(self):
        with tempfile.TemporaryDirectory() as folder:
            r=identify_headline_sectors('Garment factory fire reported',root=folder)
            self.assertEqual(r['market_activities'],['Garment manufacturing'])
            self.assertFalse(r['catalog_available'])
        r=first_pass({'id':'x','title':'FDA approves surgical catheter','first_seen':'2026-10-13T00:00:00Z'},datetime.now(timezone.utc))
        self.assertEqual(r['sector_classification']['market_sectors'],['Healthcare'])
        self.assertFalse(r['sector_classification']['matches'][0]['exposure_verified'])

    def test_separator_is_not_always_a_publisher(self):
        self.assertEqual(classify_text('Stocks fall - FDA approves new catheter').event_class,'gate')

    def test_all_evidence_spans_are_literal(self):
        title='Markets tumble; US agency lifts diesel sanctions - Publisher'
        for result in [identify_headline_event(title), *identify_headline_sectors(title)['matches']]:
            evidence=result.get('evidence')
            if evidence:
                self.assertEqual(title[evidence['start']:evidence['end']],evidence['quote'])

if __name__ == '__main__': unittest.main()
