import csv,pathlib,tempfile,unittest
from src.news_impact.semantic_catalog import CompanyCatalog

class CatalogTests(unittest.TestCase):
    def catalog(self,folder):
        path=pathlib.Path(folder)/'catalog.csv'
        with path.open('w',newline='') as f:
            w=csv.DictWriter(f,fieldnames=['Ticker','Company','Sector','Industry','Daily Digest']);w.writeheader()
            w.writerows([{'Ticker':'ACME','Company':'Acme Marine Corp','Sector':'Industrials','Industry':'Marine Transport','Daily Digest':'Do not send news answers'}, {'Ticker':'WAVE','Company':'Wave Software Inc','Sector':'Technology','Industry':'Software'}, {'Ticker':'STAR','Company':'Star Holdings','Sector':'Real Estate','Industry':'Property'}])
        return CompanyCatalog(path=path)
    def test_only_four_public_business_fields_sent(self):
        with tempfile.TemporaryDirectory() as f:
            c=self.catalog(f);rows=c.candidates('Acme Marine wins contract - Publisher')
        self.assertEqual(rows[0]['Sector'],'Industrials');self.assertNotIn('Daily Digest',rows[0]);self.assertEqual(c.metadata['rows'],3)
    def test_explicit_ticker_and_publisher_exclusion(self):
        with tempfile.TemporaryDirectory() as f:
            c=self.catalog(f);self.assertEqual(c.candidates('Issuer (WAVE) announces results')[0]['Ticker'],'WAVE')
            self.assertEqual(c.candidates('Fed comments on rates - Wave Software'),[])
    def test_homonym_and_unavailable_remain_unresolved(self):
        with tempfile.TemporaryDirectory() as f:
            c=self.catalog(f);self.assertEqual(c.candidates('Star Citizen launches new update'),[])
            missing=CompanyCatalog(root=pathlib.Path(f)/'missing');self.assertFalse(missing.metadata['available'])
    def test_catalog_facts_are_candidates_not_predictions(self):
        with tempfile.TemporaryDirectory() as f:
            rows=self.catalog(f).candidates('Acme Marine expands')
        self.assertNotIn('lane_class',rows[0]);self.assertNotIn('action',rows[0]);self.assertNotIn('direction',rows[0])

if __name__=='__main__':unittest.main()
