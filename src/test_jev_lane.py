import unittest,copy
from .jev_lane_candidate import QUESTIONS,decide,protocol_sha
from .jev_lane_contract import RUBRIC
from .jev_acceptance import summarize,sha
from .test_jev_acceptance import round_
def payload():
 out={'model':'test','answers':{}}
 for k,q in QUESTIONS.items():
  if q['type']=='noul':out['answers'][k]={'type':'noul','noul':0.0}
  else:
   selected='change' if k=='q5' else 'investigate' if k=='screen' else 'qualifying' if k=='eligibility' else 'event_main'
   out['answers'][k]={'type':'choice','choice':selected,'probabilities':{c:float(c==selected) for c in q['criteria']}}
 out['answers']['mechanism']['noul']=.9
 return out
class Tests(unittest.TestCase):
 def test_screen_only_no_event_class(self):
  result=decide({'title':'Company completes acquisition'},payload())
  self.assertEqual(result['decision'],'keep');self.assertNotIn('event_class',result)
 def test_live_trainer_uses_identical_classifier(self):
  from .jev_gate import gate
  calls=[]
  def poster(state,questions,key):
   calls.append(state);self.assertEqual(questions,QUESTIONS);return payload()
  result=gate([{'title':'Company completes acquisition','gold':'drop'}],live=True,policy='lane-hop0',key='mock',poster=poster)
  self.assertEqual(result[0]['protocol_sha256'],protocol_sha());self.assertEqual(result[0]['decision'],'keep')
  self.assertNotIn('GOLD',calls[0]);self.assertNotIn('drop',calls[0])
 def test_manual_trainer_retains_headline_identity(self):
  import os,datetime
  from unittest.mock import patch
  from .jev_train import annotate_gate
  row={'id':'fixture-1','title':'Company completes acquisition','source':'Fixture'}
  with patch.dict(os.environ,{'JEV_GATE_POLICY':'lane-hop0'}):
   items,model=annotate_gate([row],live=True,key='mock',workers=1,poster=lambda *args:payload(),asof=datetime.date(2026,10,1))
  self.assertEqual(items[0]['id'],row['id']);self.assertEqual(items[0]['title'],row['title'])
  self.assertEqual(items[0]['jev'],'KEEP');self.assertEqual(items[0]['protocol_sha256'],protocol_sha())
  self.assertEqual(model,'test')
 def test_invalid_missing_answer_fails(self):
  p=payload();del p['answers']['mechanism']
  with self.assertRaises(KeyError):decide({'title':'X'},p)
 def test_no_mechanism_drops(self):
  p=payload();p['answers']['mechanism']['noul']=0
  self.assertEqual(decide({'title':'X'},p)['decision'],'drop')
 def test_rumor_veto_survives_atomic_support(self):
  p=payload();p['answers']['action']['noul']=1
  p['answers']['q5']={'type':'choice','choice':'rumor','probabilities':{'rumor':1,'junk':0,'change':0,'weather':0}}
  self.assertEqual(decide({'title':'X'},p)['decision'],'drop')
 def test_nan_rejected(self):
  p=payload();p['answers']['print']['noul']=float('nan')
  with self.assertRaises(ValueError):decide({'title':'X'},p)
 def test_specific_policy_path_survives_weather_disagreement(self):
  p=payload();p['answers']['q5']={'type':'choice','choice':'weather','probabilities':{'weather':.98,'change':.02,'junk':0,'rumor':0}}
  p['answers']['path_signal']['noul']=.95
  self.assertEqual(decide({'title':'Fed voter says multiple hikes may be needed'},p)['decision'],'keep')
 def test_weather_veto_blocks_generic_atomic_false_positive(self):
  p=payload();p['answers']['q5']={'type':'choice','choice':'weather','probabilities':{'weather':.98,'change':.02,'junk':0,'rumor':0}}
  p['answers']['reported_fact']['noul']=.9
  p['answers']['eligibility']={'type':'choice','choice':'qualifying','probabilities':{'qualifying':.4,'not_new':.3,'not_a_fact':.1,'out_of_book':.1,'packaging':.1}}
  self.assertEqual(decide({'title':'Treasuries dip ahead of inflation data'},p)['decision'],'drop')
 def test_teacher_audit_invalidates_acceptance_without_relabeling(self):
  b=round_(0);b.update(rubric_sha256=sha(RUBRIC),protocol_sha256=protocol_sha(),gold_audit_invalid=True)
  result=summarize([b],rubric=RUBRIC)
  self.assertFalse(result['rounds'][0]['pass']);self.assertTrue(result['rounds'][0]['gold_audit_invalid'])
 def test_new_gold_contract_resets_old_passes(self):
  rounds=[round_(i*100) for i in range(10)]
  self.assertFalse(summarize(rounds,rubric=RUBRIC)['accepted'])
  for b in rounds:b.update(rubric_sha256=sha(RUBRIC),protocol_sha256=protocol_sha())
  self.assertTrue(summarize(rounds,rubric=RUBRIC)['accepted'])
if __name__=='__main__':unittest.main()
