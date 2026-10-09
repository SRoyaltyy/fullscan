import unittest
from .news_article_retrieval import source_links,retrieve_linked_primary_sources

class SourceLinkTests(unittest.TestCase):
    def test_official_citation_fetch_is_bounded_and_kept_unverified(self):
        links=source_links('<article><a href="https://agency.gov/notice">Notice</a><a href="https://other.gov/report">Report</a><a href="https://agency.gov.evil.example/">Spoof</a></article>','https://publisher.example/article')
        calls=[]
        def fetch(url):
            calls.append(url)
            return ('<article>'+('Official report content. '*40)+'</article>').encode(),'text/html'
        result=retrieve_linked_primary_sources({'publisher_source_links':links},fetch,limit=1)
        self.assertEqual(calls,['https://agency.gov/notice'])
        record=result['linked_primary_source_candidates'][0]
        self.assertFalse(record['relevance_verified']);self.assertFalse(record['claims_verified'])
        self.assertIn('+00:00',record['known_at']);self.assertEqual(len(record['fetched_page_sha256']),64)
    def test_navigation_and_failed_fetch_never_become_source_facts(self):
        links=[{'url':'https://agency.gov/nav','link_scope':'visible_page_candidate'},
               {'url':'https://agency.gov/notice','link_scope':'article_element'}]
        calls=[]
        def fetch(url):calls.append(url);raise TimeoutError('fixture timeout')
        result=retrieve_linked_primary_sources({'publisher_source_links':links},fetch)
        self.assertEqual(calls,['https://agency.gov/notice']);self.assertEqual(result['linked_primary_source_candidates'],[])
        self.assertEqual(result['linked_primary_source_requests'][0]['status'],'failed')
    def test_article_links_exclude_navigation_and_preserve_provenance(self):
        page='<nav><a href="https://agency.gov/nav">Navigation</a></nav><a href="/subscribe">Subscribe</a><article><a href="https://agency.gov/release#part">Official release</a><a href="/background">Background</a></article><footer><a href="https://agency.gov/footer">Footer</a></footer>'
        links=source_links(page,'https://publisher.example/story')
        self.assertEqual([x['anchor_text'] for x in links],['Official release','Background'])
        self.assertEqual(links[1]['url'],'https://publisher.example/background')
        self.assertTrue(links[0]['official_domain_candidate']);self.assertFalse(links[0]['source_verified'])
    def test_no_article_fallback_is_explicit_and_unsafe_schemes_are_ignored(self):
        page='<a href="file:///secret">Bad</a><a href="javascript:run()">Bad</a><a href="https://agency.gov.evil.example/">Spoof</a><a href="https://fda.gov/notice">Notice</a>'
        links=source_links(page,'https://publisher.example/story')
        self.assertEqual(len(links),2);self.assertFalse(links[0]['official_domain_candidate'])
        self.assertEqual(links[1]['link_scope'],'visible_page_candidate')

if __name__=='__main__':unittest.main()
