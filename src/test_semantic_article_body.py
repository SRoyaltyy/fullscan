import unittest
from .news_article_retrieval import extract_page

class SemanticArticleBodyTests(unittest.TestCase):
    def page(self):
        return '<meta property="og:title" content="An economic report - Publisher"><div>Menu noise</div><div itemprop="articleBody"><p>'+('Original reporting sentence. '*12)+'<div class="mainnews_add"><h3>Hot Picks Today</h3><div>Unrelated war headline</div></div><p>Closing analyst quote remains.</p><script>hidden code</script></div><div>Trailing briefing</div>'
    def test_body_and_resumed_reporting_without_widget_or_menu(self):
        text,method=extract_page(self.page(),'An economic report - Publisher')
        self.assertEqual(method,'structured_headline_bound_semantic_article_body')
        self.assertIn('Closing analyst quote',text)
        for noise in ['Menu noise','Hot Picks','Unrelated war','Trailing briefing','hidden code']:self.assertNotIn(noise,text)
    def test_wrong_headline_does_not_get_semantic_binding(self):
        self.assertNotEqual(extract_page(self.page(),'Another report - Publisher')[1],'structured_headline_bound_semantic_article_body')
    def test_conflicting_bodies_fail_closed(self):
        page=self.page()+'<div itemprop="articleBody">'+('Different article text. '*15)+'</div>'
        with self.assertRaises(ValueError):extract_page(page,'An economic report - Publisher')
    def test_article_body_excludes_other_article_elements_and_nested_divs_do_not_end_it(self):
        page='<meta property="og:title" content="An economic report"><article class="editor">Reporter covers unrelated companies</article><article itemprop="articleBody"><div><p>'+('Original source reporting. '*12)+'</p></div><p>Final project decision remains.</p></article><article class="related_news">Unrelated recall and acquisition</article>'
        text,method=extract_page(page,'An economic report')
        self.assertEqual(method,'structured_headline_bound_semantic_article_body')
        self.assertIn('Final project decision',text)
        self.assertNotIn('Reporter covers',text);self.assertNotIn('Unrelated recall',text)

if __name__=='__main__':unittest.main()
