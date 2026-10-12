import json,unittest
from .news_intake import extract_documents

class IntakeHeadlineBodyTests(unittest.TestCase):
    def document(self):
        return {'id':'fixture','title':'A source report - Publisher','url':'https://publisher.example/story',
            'body':'retained feed text','extraction_status':'feed_text'}
    def test_intake_keeps_matching_reporting_instead_of_longer_related_story(self):
        correct='The regulator describes an application under review. '*12
        unrelated='An unrelated company completed an acquisition. '*30
        rows=[{'@type':'NewsArticle','headline':'A source report','articleBody':correct},
              {'@type':'NewsArticle','headline':'Another story','articleBody':unrelated}]
        page='<script type="application/ld+json">'+json.dumps(rows)+'</script>'
        doc=self.document();extract_documents([doc],1,lambda u:(page.encode(),'text/html'))
        self.assertEqual(doc['body'],correct)
        self.assertEqual(doc['retained_feed_body'],'retained feed text')
        self.assertEqual(doc['extraction_method'],'structured_headline_bound_article_body')

    def test_intake_rejects_conflicting_bodies_and_preserves_feed(self):
        rows=[{'@type':'NewsArticle','headline':'A source report','articleBody':body*20}
              for body in ['Original source facts. ','Contradictory source facts. ']]
        page='<script type="application/ld+json">'+json.dumps(rows)+'</script>'
        doc=self.document();extract_documents([doc],1,lambda u:(page.encode(),'text/html'))
        self.assertEqual(doc['extraction_status'],'fetch_failed')
        self.assertEqual(doc['body'],'retained feed text')

if __name__=='__main__':unittest.main()
