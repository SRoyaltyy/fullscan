import json,unittest
from .news_article_retrieval import enrich_document,extract_page

TITLE='Export controls tighten as manufacturers confront rising costs'
BODY='Commerce officials slowed aircraft licensing for China. Planning capacity logistics investment orders factory demand worker safety staffing shipping regulations supply contracts need evidence and economic context. '+('Reported details about aircraft parts and permits. '*5)

class HeadlineBindingTests(unittest.TestCase):
    def page(self,rows):return '<script type="application/ld+json">'+json.dumps(rows)+'</script>'
    def row(self,body=BODY,headline=TITLE):return {'@type':'NewsArticle','headline':headline,'articleBody':body}
    def recover(self,rows):
        doc={'title':TITLE+' - Publisher Name','body':'feed echo','url':'https://publisher.example/news','extracted_url':'https://publisher.example/news'}
        return enrich_document(doc,None,lambda u:(self.page(rows).encode(),'text/html'))
    def test_exact_headline_binds_body_even_when_body_uses_different_words(self):
        d=self.recover(self.row());self.assertEqual(d['body'],BODY)
        self.assertTrue(d['review_enrichment']['exact_structured_headline_binding'])
        self.assertLess(d['review_enrichment']['title_token_overlap'],.65)
    def test_longer_unrelated_structured_story_does_not_win(self):
        d=self.recover([self.row(),self.row(body=BODY*3,headline='Unrelated report')]);self.assertEqual(d['body'],BODY)
    def test_conflicting_matching_bodies_rejected(self):
        d=self.recover([self.row(),self.row(body=BODY+' Contradictory update.')])
        self.assertEqual(d['body'],'feed echo');self.assertIn('Conflicting',d['review_enrichment']['error'])
    def test_matching_metadata_does_not_make_thin_body_substantive(self):
        d=self.recover(self.row(body=TITLE));self.assertEqual(d['body'],'feed echo')
        self.assertEqual(d['review_enrichment']['status'],'failed')
    def test_unmatched_headline_does_not_bypass_overlap(self):
        d=self.recover(self.row(headline='Different headline'));self.assertEqual(d['body'],'feed echo')
    def test_optional_title_keeps_legacy_extraction_interface(self):
        b,m=extract_page(self.page(self.row()));self.assertEqual(b,BODY);self.assertEqual(m,'structured_article_body')

if __name__=='__main__':unittest.main()
