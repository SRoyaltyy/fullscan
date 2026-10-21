import hashlib
import unittest
from types import SimpleNamespace
from .stock_keywords import compile_keywords,KeywordIndex
from .test_stock_keywords import graph,profile
from .entity_resolution import decide,validate_annotations,person_fact_alignment,project_annotations


def fixture():
    g=graph();claim=g['claim']
    person=claim('officer','has_officer','John McDonald',quote='John McDonald is the chief executive officer of Acme Industries.')
    person['object_type']='PERSON'
    apple=claim('supplier','buys_from','Apple',quote='We purchase computer hardware from Apple.')
    cloud=claim('cloud','uses_input','cloud',quote='We use cloud computing for storage and software infrastructure.')
    chip=claim('chip','uses_input','chips',quote='We use semiconductor chips from silicon foundries and processor suppliers.')
    g['facts']=[person,apple,cloud,chip]
    a=profile(compile_keywords(g,as_of='2026-04-01',ticker='ACME'),'Acme Industries Inc.')
    empty=lambda name:SimpleNamespace(name=name,keyword_records=[],keyword_evidence={})
    return {'ACME':a,'AAPL':empty('Apple Inc.'),'MCD':empty("McDonald's Corporation")}


CASES=[
 ('Acme Industries CEO McDonald resigns','officer','resolved'),
 ('John McDonald resigns','officer','resolved'),
 ('McDonald resigns','officer','unresolved'),
 ("McDonald's reports restaurant earnings",'officer','rejected'),
 ('McDonalds reports restaurant earnings','officer','rejected'),
 ('Acme Industries CEO Ronald McDonald resigns','officer','rejected'),
 ('Apple shares rise after computer hardware earnings','supplier','resolved'),
 ('Apple harvest suffers after rain','supplier','unresolved'),
 ('cloud computing storage demand rises','input','resolved'),
 ('rain cloud forecast changes','input','unresolved'),
 ('semiconductor chips foundry production expands','input','resolved'),
 ('potato chips snack prices rise','input','unresolved'),
 ('Acme expands. CEO McDonald resigns','officer','unresolved'),
 ('Acme Industries CEO says McDonald reports restaurant earnings','officer','rejected'),
 ("Acme Industries CEO discusses McDonald's earnings",'officer','rejected'),
 ('Acme Industries reports McDonald, the CEO, resigns','officer','resolved'),
]


class EntityResolutionControls(unittest.TestCase):
    def test_person_company_and_concept_collision_controls(self):
        index=KeywordIndex(fixture())
        for text,role,state in CASES:
            with self.subTest(text=text):
                result=index.compare(text)
                all_hits=result['matches']+result['unresolved_matches']+result['rejected_matches']
                hits=[r for r in all_hits if r['relationship']==role]
                self.assertTrue(hits,text)
                self.assertEqual({r['alignment']['status'] for r in hits},{state})
                if state!='resolved':self.assertFalse(any(r['relationship']==role for r in result['matches']))

    def test_person_and_company_have_different_typed_ids(self):
        index=KeywordIndex(fixture())
        p=next(r for r in index.profiles['ACME'].keyword_records if r['relationship']=='officer')
        self.assertEqual(p['identity']['type'],'PERSON')
        self.assertNotEqual(p['identity']['id'],'org:MCD')

    def test_ner_type_conflict_beats_arbitrarily_high_context_overlap(self):
        index=KeywordIndex(fixture());text='Acme CEO John McDonald resigns'
        start=text.index('John');end=start+len('John McDonald')
        parsed={'input_sha256':hashlib.sha256(text.encode()).hexdigest(),
                'mentions':[{'start':start,'end':end,'text':'John McDonald','label':'ORG'}]}
        result=index.compare(text,entity_annotations=parsed)
        self.assertFalse(result['matches'])
        self.assertTrue(any('article_entity_type_conflict' in r['alignment']['reasons'] for r in result['rejected_matches']))
        parsed['mentions'][0]['label']='PERSON'
        self.assertTrue(index.compare(text,entity_annotations=parsed)['matches'])

    def test_wrong_input_annotations_cannot_be_reused(self):
        with self.assertRaisesRegex(ValueError,'actual scored text'):
            validate_annotations('McDonalds',{'input_sha256':'wrong','mentions':[]})
        with self.assertRaisesRegex(ValueError,'span'):
            validate_annotations('McDonalds',{'input_sha256':hashlib.sha256(b'McDonalds').hexdigest(),
                'mentions':[{'start':0,'end':4,'text':'John','label':'PERSON'}]})

    def test_two_officers_with_same_surname_require_more_than_employer(self):
        profiles=fixture();p=profiles['ACME'];other=dict(next(r for r in p.keyword_records if r['relationship']=='officer'))
        other.update(id='other-person',keyword='Jane McDonald',normalized='jane mcdonald',object_type='PERSON',relationship='officer')
        p.keyword_records.append(other)
        result=KeywordIndex(profiles).compare('Acme Industries CEO McDonald resigns')
        self.assertFalse(result['matches'])
        self.assertGreaterEqual(len(result['unresolved_matches']),2)

    def test_omitted_middle_name_preserves_first_and_last_identity(self):
        profiles=fixture();p=profiles['ACME']
        row=next(r for r in p.keyword_records if r['relationship']=='officer')
        row.update(keyword='John Alan McDonald',normalized='john alan mcdonald')
        result=KeywordIndex(profiles).compare('John McDonald resigns')
        self.assertEqual(len(result['matches']),1)
        self.assertEqual(result['matches'][0]['identity']['name'],'John Alan McDonald')

    def test_same_gate_protects_legacy_person_stock_pass_route(self):
        from .stock_evidence import Fact,Profile,index_profiles
        p=Profile('ACME','Acme Industries','','')
        fact=Fact('person','John McDonald','John McDonald is our chief executive officer.')
        p.facts=[fact];board=index_profiles({'ACME':p})
        self.assertFalse(person_fact_alignment(board,p,fact,"McDonald's restaurant earnings rise",'mcdonald'))
        self.assertTrue(person_fact_alignment(board,p,fact,'Acme Industries CEO McDonald resigns','mcdonald'))
        text='John McDonald resigns'
        self.assertFalse(person_fact_alignment(board,p,fact,text,'mcdonald',annotations=[{'start':0,'end':13,'text':'John McDonald','label':'ORG'}]))

    def test_grammar_types_survive_legacy_unicode_and_passage_normalization(self):
        text='Context. John McDonald’s business expands'
        parsed=[{'start':9,'end':22,'text':'John McDonald','label':'ORG'}]
        projected=project_annotations(text,parsed,"John McDonald's business expands")
        self.assertEqual(projected,[{'start':0,'end':13,'text':'John McDonald','label':'ORG'}])


if __name__=='__main__':unittest.main()
