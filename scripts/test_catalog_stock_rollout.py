import copy,json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from scripts.catalog_stock_rollout import assemble,payload

class CatalogRolloutControls(unittest.TestCase):
 def setUp(self):
  self.plan={'source_sha':'a'*40,'compiler_sha256':'compiler','catalog':{'X':{'batch':'batch','coverage':'filing_backed','description':'private input field'}},'batches':[{'id':'batch','branch':'branch'}]}
  self.index={'source_sha':'a'*40,'compiler_sha256':'compiler','complete':True,'profiles':{'X':{'asset':'profiles/x.json.gz','sha256':'b'*64,'coverage':'filing_backed'}}}
 def run_assembly(self,*,complete=False,index=None,plan=None):
  with tempfile.TemporaryDirectory() as d,patch('scripts.catalog_stock_rollout.auth_env',return_value={}),patch('scripts.catalog_stock_rollout.git',return_value='c'*40+'\trefs/heads/branch'),patch('scripts.catalog_stock_rollout.fetch_url',return_value=json.dumps(index or self.index).encode()):
   return assemble(copy.deepcopy(plan or self.plan),Path(d),complete)
 def test_partial_catalog_remains_incomplete_and_uses_immutable_data_urls(self):
  r=self.run_assembly();self.assertFalse(r['complete']);self.assertEqual(r['ready_stocks'],1)
  self.assertIn('/'+'c'*40+'/',r['profiles']['X']['url']);self.assertNotIn('description',r['catalog']['X'])
 def test_complete_batch_is_not_complete_catalog(self):
  with self.assertRaisesRegex(AssertionError,'catalog rebuild incomplete'):self.run_assembly(complete=True)
 def test_stale_candidate_or_compiler_is_blocked(self):
  for field in ('source_sha','compiler_sha256'):
   index=copy.deepcopy(self.index);index[field]='stale'
   with self.assertRaises(AssertionError):self.run_assembly(index=index)
 def test_stock_from_another_batch_cannot_be_admitted(self):
  plan=copy.deepcopy(self.plan);plan['catalog']['X']['batch']='another'
  with self.assertRaises(AssertionError):self.run_assembly(plan=plan)
 def test_description_only_cannot_be_reported_as_filing_backed(self):
  plan=copy.deepcopy(self.plan);plan['catalog']['X']['coverage']='description_only'
  with self.assertRaises(AssertionError):self.run_assembly(plan=plan)
 def test_only_entire_catalog_with_correct_coverage_split_can_complete(self):
  plan=copy.deepcopy(self.plan);index=copy.deepcopy(self.index)
  plan['catalog']={};index['profiles']={}
  for i in range(5996):
   t='T'+str(i);coverage='filing_backed' if i<2032 else 'description_only'
   plan['catalog'][t]={'batch':'batch','coverage':coverage,'description':''};index['profiles'][t]={'asset':'profiles/x.json.gz','sha256':'b'*64,'coverage':coverage}
  self.assertTrue(self.run_assembly(complete=True,plan=plan,index=index)['complete'])
  index['profiles'].pop('T5995')
  with self.assertRaises(AssertionError):self.run_assembly(complete=True,plan=plan,index=index)
 def test_cross_paragraph_occurrence_keeps_exact_source_excerpt(self):
  graph={'sources':[{'id':'s'}],'coverage':{},'paragraphs':[{'source_id':'s','start':0,'text':'Alpha '},{'source_id':'s','start':6,'text':'Beta'}],'stock_evidence':{'schema':'stock-evidence-graph-2','entities':[],'assertions':[],'occurrences':[{'source_id':'s','start':0,'end':10,'text':'Alpha Beta'}]}}
  p=payload(graph,{},'X','Example',None,self.plan,'description_only')
  self.assertEqual(p['evidence']['occurrences'][0]['excerpt'],'Alpha Beta');self.assertEqual(p['coverage_kind'],'description_only')
 def test_corrupt_occurrence_is_blocked(self):
  g={'sources':[{'id':'s'}],'coverage':{},'paragraphs':[{'source_id':'s','start':0,'text':'Alpha'}],'stock_evidence':{'schema':'stock-evidence-graph-2','entities':[],'assertions':[],'occurrences':[{'source_id':'s','start':0,'end':5,'text':'Wrong'}]}}
  with self.assertRaises(AssertionError):payload(g,{},'X','Example',None,self.plan,'description_only')

if __name__=='__main__':unittest.main()
