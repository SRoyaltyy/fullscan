import unittest
from evaluate_semantic_acceptance import compare

class AcceptanceTests(unittest.TestCase):
    def fixture(self):
        inputs=[{'n':i,'title':f'Headline {i}'} for i in range(100)]
        reference={'records':[x|{'action':i<20,'acceptable_lane_classes':['gate'],'acceptable_sector_sets':[['Healthcare']]} for i,x in enumerate(inputs)]}
        outputs=[]
        for i,x in enumerate(inputs):
            model={'actions':[{'concrete_public_action':i<20,'public_authority':True}],'primary_lane_class':'gate','sectors':[{'sector':'Healthcare'}]}
            outputs.append(x|{'program_output':{'status':'analyzed','model_output':model}})
        return inputs,reference,{'outputs':outputs}
    def test_exact_correct_answers_pass(self):
        self.assertTrue(compare(*self.fixture())['pass'])
    def test_missing_outputs_count_against_all_metrics(self):
        inputs,ref,raw=self.fixture();raw['outputs']=raw['outputs'][:89]
        result=compare(inputs,ref,raw)
        self.assertFalse(result['pass']);self.assertEqual(result['metrics']['lane'],.89)
    def test_always_negative_does_not_pass(self):
        inputs,ref,raw=self.fixture()
        for o in raw['outputs']:o['program_output']['model_output']['actions'][0]['concrete_public_action']=False
        r=compare(inputs,ref,raw);self.assertFalse(r['pass']);self.assertEqual(r['metrics']['action_recall'],0)
    def test_sector_supersets_are_not_exact(self):
        inputs,ref,raw=self.fixture()
        for o in raw['outputs']:o['program_output']['model_output']['sectors'].append({'sector':'Energy'})
        r=compare(inputs,ref,raw);self.assertFalse(r['pass']);self.assertEqual(r['metrics']['sectors'],0)
    def test_partial_batch_and_duplicate_output_rejected(self):
        inputs,ref,raw=self.fixture()
        with self.assertRaises(ValueError):compare(inputs[:99],ref,raw)
        raw['outputs'].append(raw['outputs'][0])
        with self.assertRaises(ValueError):compare(inputs,ref,raw)

if __name__=='__main__':unittest.main()
