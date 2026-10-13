import ast
import unittest
from datetime import datetime, timezone
from pathlib import Path

from src.news_impact.institutional_actions import identify_institutional_actions as identify


class InstitutionalActionsTest(unittest.TestCase):
    def one(self,title):
        value=identify(title)
        self.assertEqual(len(value['actions']),1,title)
        return value['actions'][0]

    def test_us_agencies(self):
        for title,agency in [('FDA approves Abbott catheter','FDA'),
                             ('FCC approves new spectrum rules','FCC'),
                             ('FTC rejects merger proposal','FTC'),
                             ('SEC adopts final disclosure rules','SEC'),
                             ('EPA issues emissions standard','EPA'),
                             ('OFAC lifts sanctions on diesel trade','OFAC')]:
            with self.subTest(title=title):
                a=self.one(title)
                self.assertEqual(a['authority'],agency)
                self.assertEqual(a['jurisdiction'],'US')
                self.assertTrue(a['concrete_reported_action'])
                self.assertFalse(a['implementation_verified'])

    def test_passage_is_not_enactment(self):
        a=self.one('US Senate passes energy permitting bill')
        self.assertEqual(a['stage'],'chamber_approval_reported')
        b=self.one('President Trump signs energy permitting bill into law')
        self.assertEqual(b['stage'],'enacted_reported')
        self.assertEqual(self.one('US House committee approves energy bill')['stage'],'committee_approval_reported')

    def test_reported_signed_act_without_identifiable_issuer(self):
        a=self.one('Faster Services For Veterans Act Signed Into Law')
        self.assertEqual(a['stage'],'enacted_reported')
        self.assertIsNone(a['jurisdiction'])

    def test_proposals_and_scheduled_votes(self):
        for title in ['SEC proposes new disclosure rules','FDA may approve new device',
                      'Trump threatens tariffs','FCC to vote on spectrum rules']:
            with self.subTest(title=title):
                self.assertFalse(self.one(title)['concrete_reported_action'])
        self.assertEqual(self.one('FCC to vote on spectrum rules')['stage'],'scheduled_vote')

    def test_negation(self):
        self.assertFalse(self.one('FDA did not approve the device')['concrete_reported_action'])

    def test_jurisdiction_and_interim_orders(self):
        a=self.one('US Supreme Court rules against agency')
        self.assertEqual(a['jurisdiction'],'US')
        b=self.one('Supreme Court rules for company in tax case')
        self.assertIsNone(b['jurisdiction'])
        self.assertIsNone(self.one('Supreme Court rules for Google Korea in tax case')['jurisdiction'])
        c=self.one('Tokyo Court Rules Human Voices Are Protected From AI Cloning')
        self.assertEqual(c['jurisdiction'],'Japan')
        d=self.one('US judge grants temporary injunction against new law')
        self.assertEqual(d['stage'],'interim_decision_reported')

    def test_state_and_territory(self):
        a=self.one('California governor signs privacy bill into law')
        self.assertEqual(a['authority_level'],'state')
        b=self.one('Puerto Rico court blocks permit')
        self.assertEqual(b['authority_level'],'territory')
        self.assertEqual(b['jurisdiction'],'US')

    def test_foreign_agency_and_unnamed_court(self):
        self.assertEqual(self.one('Philippines FDA recalls contaminated toner')['jurisdiction'],'Philippines')
        a=self.one('United Therapeutics wins Yutrepia patent ruling')
        self.assertEqual(a['action_category'],'judicial_ruling')
        self.assertIsNone(a['authority'])
        self.assertIsNone(a['jurisdiction'])

    def test_sovereign_and_institution(self):
        self.assertEqual(self.one('Russia partially lifts diesel export ban')['jurisdiction'],'Russia')
        self.assertEqual(self.one('Nepal government approves World Bank bond mandate')['jurisdiction'],'Nepal')
        self.assertEqual(self.one('World Bank approves loan')['jurisdiction'],'international')

    def test_no_policy_from_market_colour(self):
        for title in ['Markets await FDA approval','Fed rate hike expectations rise',
                      'How to understand Supreme Court rules',
                      'Trump speaks about the diesel export ban',
                      'Abbott launches FDA-approved catheter',
                      'Musk launches new AI product']:
            with self.subTest(title=title):
                self.assertFalse(identify(title)['actions'])

    def test_exact_spans(self):
        title='Markets rise; US Senate passes energy bill'
        a=self.one(title)
        for key in ('action_evidence','authority_evidence'):
            e=a[key]
            self.assertEqual(title[e['start']:e['end']],e['quote'])

    def test_intake_does_not_require_body_or_lane_success(self):
        from src.news_intake import first_pass
        row=first_pass({'id':'x','title':'US Senate passes veterans services bill',
                       'first_seen':'2026-10-13T00:00:00+00:00'},datetime.now(timezone.utc))
        self.assertTrue(row['institutional_actions']['has_concrete_reported_action'])
        self.assertEqual(row['institutional_actions']['actions'][0]['stage'],'chamber_approval_reported')

    def test_one_shot_preserves_abstention_and_recognition(self):
        # Exercise the public wrapper without models, external packs or credentials.
        path=Path(__file__).resolve().parents[1]/'src/news_impact/one_shot_stack.py'
        node=next(n for n in ast.parse(path.read_text()).body if isinstance(n,ast.FunctionDef) and n.name=='process_article')
        module=ast.Module(body=[node],type_ignores=[])
        calls=[]
        def fake(art,lane,**kwargs):
            calls.append(kwargs);return {'keep':False,'reason':'market_unknown'}
        ns={'_process_article':fake}
        exec(compile(module,str(path),'exec'),ns)
        row=ns['process_article']({'title':'FCC approves spectrum auction'},None,axioms=[],use_pack=False)
        self.assertFalse(row['keep'])
        self.assertTrue(row['institutional_actions']['has_us_action'])
        self.assertFalse(calls[0]['use_pack'])

if __name__=='__main__':
    unittest.main()
