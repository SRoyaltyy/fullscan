import hashlib
import unittest
from scripts.export_completed_parser import project, publish_assets, export


class CompletedReview(unittest.TestCase):
    def graph(self):
        text='🛰 We rely on Stripe for payments.\n'
        paragraph={'id':'p','source_id':'s','start':0,'end':len(text),'text':text,
            'sha256':hashlib.sha256(text.encode()).hexdigest(),'labels':['operating_dependencies']}
        return {'ticker':'BLZE','tickers':['BLZE'],'name':'Backblaze','as_of':'2026-10-20',
            'schema':'filing-relationships-1.1','compiler_sha256':'old','parser':{'spacy':'pinned'},
            'coverage':{'paragraphs':2},'paragraphs':[paragraph,
                {**paragraph,'id':'unused','source_id':'unused'}],
            'sources':[{'id':'s','filing_date':'2026-01-01'},{'id':'unused'}],
            'facts':[{'id':'f','object':'Stripe','predicate':'depends_on','status':'asserted',
                'evidence':[{'source_id':'s','start':2,'end':len(text)-1,'quote':text[2:-1]}]}]}

    def test_all_decisions_keep_full_exact_paragraphs_and_original_version(self):
        graph=self.graph();out=project(graph)
        self.assertEqual(out['paragraphs'][0]['text'],graph['paragraphs'][0]['text'])
        self.assertEqual(out['facts'][0]['evidence'][0]['paragraph_ids'],['p'])
        self.assertEqual(out['compiler_sha256'],'old')
        self.assertFalse(out['term_inventory_available'])
        self.assertEqual(len(out['facts']),len(graph['facts']))
        self.assertEqual(len(out['paragraphs']),1)

    def test_corrupt_quote_or_paragraph_cannot_be_published(self):
        for field in ('quote','text'):
            with self.subTest(field=field):
                graph=self.graph()
                if field=='quote':graph['facts'][0]['evidence'][0]['quote']='invented'
                else:graph['paragraphs'][0]['text']='altered'
                with self.assertRaises(ValueError):project(graph)

    def test_named_law_without_an_old_relationship_is_recovered_from_full_archive(self):
        graph=self.graph();text='We comply with the Alpha Privacy Act.'
        graph['sources'].append({'id':'legal','kind':'risk','filing_date':'2026-01-01'})
        graph['paragraphs'].append({'id':'legal-paragraph','source_id':'legal','start':0,'end':len(text),
            'text':text,'sha256':hashlib.sha256(text.encode()).hexdigest(),'labels':['regulation_legal']})
        out=project(graph)
        self.assertEqual(len(out['facts']),len(graph['facts']))
        self.assertIn('legal-paragraph',{p['id'] for p in out['paragraphs']})
        self.assertIn('legal',{s['id'] for s in out['sources']})
        self.assertEqual(out['review_counts']['relationship_witness_paragraphs'],1)
        law=next(r for r in out['specific_records']['records'] if r['category']=='law')
        self.assertEqual(law['entity'],'Alpha Privacy Act')
        self.assertEqual(law['paragraph_id'],'legal-paragraph')

    def test_witness_crossing_paragraphs_keeps_both(self):
        graph=self.graph();p=graph['paragraphs'][0];cut=14
        def part(start,end,ident):
            text=p['text'][start:end]
            return {**p,'id':ident,'start':start,'end':end,'text':text,
                'sha256':hashlib.sha256(text.encode()).hexdigest()}
        graph['paragraphs']=[part(0,cut,'first'),part(cut,p['end'],'second')]
        out=project(graph)
        self.assertEqual(out['facts'][0]['evidence'][0]['paragraph_ids'],['first','second'])

    def test_asset_publication_preserves_site_and_fast_forwards_data_branch(self):
        import json,os,subprocess,tempfile
        from pathlib import Path
        previous=Path.cwd()
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);remote=root/'remote.git';work=root/'site';work.mkdir()
            def git(*args,cwd=work):
                return subprocess.check_output(['git',*args],cwd=cwd,text=True,stderr=subprocess.DEVNULL).strip()
            git('init','--bare',str(remote));git('init');git('remote','add','origin',str(remote))
            (work/'index.html').write_text('existing testing page')
            data=root/'data';(data/'profiles').mkdir(parents=True)
            profile=data/'profiles/0001462056.json.gz';profile.write_bytes(b'first')
            (data/'manifest.json').write_text('{}')
            try:
                os.chdir(work)
                first=publish_assets(data)
                profile.write_bytes(b'second');second=publish_assets(data)
            finally:os.chdir(previous)
            self.assertEqual((work/'index.html').read_text(),'existing testing page')
            self.assertEqual(git('rev-parse',second+'^'),first)
            self.assertEqual(git('show',second+':profiles/0001462056.json.gz'),'second')
            self.assertIn(second,json.loads((data/'manifest.json').read_text())['asset_base_url'])

    def test_zip_members_are_read_without_relying_on_an_unpacked_directory(self):
        import gzip,io,json,tempfile,zipfile
        from pathlib import Path
        graph=self.graph();raw=gzip.compress(json.dumps(graph).encode())
        name='issuers/0001462056.json.gz'
        index={'complete':True,'schema':graph['schema'],'compiler_sha256':'old','as_of':graph['as_of'],
            'totals':{'companies':1,'facts':1},'profiles':{'BLZE':{'path':name,'cik':'0001462056','facts':1,'sha256':hashlib.sha256(raw).hexdigest()}}}
        data=io.BytesIO()
        with zipfile.ZipFile(data,'w') as archive:
            archive.writestr('index.json',json.dumps(index));archive.writestr(name,raw)
        with tempfile.TemporaryDirectory() as temporary,zipfile.ZipFile(data) as archive:
            result=export(Path(temporary)/'absent',Path(temporary)/'out',read_bytes=archive.read,
                expected_compiler='old',expected_companies=1,expected_tickers=1,expected_facts=1)
            self.assertEqual(result['published']['relationships'],1)


if __name__=='__main__':unittest.main()
