import contextlib,io,json,tempfile,unittest
from pathlib import Path
from src.correlation import build_board,link_articles
from src.test_correlation import STOCKS,MEMO
from scripts.scan_validation_batches import write_batch

class BatchTests(unittest.TestCase):
 def test_reusing_board_preserves_separate_batch_decisions(self):
  for engine in ('legacy','evidence'):
   board=build_board(STOCKS,engine=engine)
   batches=[[{'id':str(i),'text':MEMO} for i in range(9)],[{'id':'negative'+str(i),'text':'Local police investigate a theft.'} for i in range(9)]]
   expected=[link_articles(board,articles,engine=engine) for articles in batches]
   with tempfile.TemporaryDirectory(dir=Path.cwd()) as tmp:
    for i,articles in enumerate(batches):
     path=Path(tmp)/f'{i}.json';log=io.StringIO()
     with contextlib.redirect_stdout(log):write_batch(board,articles,path,engine,None,None)
     self.assertEqual(json.loads(path.read_text())['links'],expected[i])
     self.assertEqual(set(json.loads(log.getvalue())),{'file','cases'})

if __name__=='__main__':unittest.main()
