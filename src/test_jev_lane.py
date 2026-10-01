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
   selected='change' if k=='q5' else 'investigate' if k=='screen' else 'event_main'
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
 def test_new_gold_contract_resets_old_passes(self):
  rounds=[round_(i*100) for i in range(10)]
  self.assertFalse(summarize(rounds,rubric=RUBRIC)['accepted'])
  for b in rounds:b.update(rubric_sha256=sha(RUBRIC),protocol_sha256=protocol_sha())
  self.assertTrue(summarize(rounds,rubric=RUBRIC)['accepted'])
if __name__=='__main__':unittest.main()
