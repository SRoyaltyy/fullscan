"""Parse article mentions once with the pinned grammar engine, before scoring.

NER labels are evidence for the identity gate, never canonical entity IDs.
The exact selected input text/hash and mention offsets travel with the parse.
"""
import argparse
import hashlib
import json
from pathlib import Path
from src.filing_relationships import load_parser, MODEL, MODEL_VERSION, SPACY_VERSION
from src.scanner_backtest import load_test_articles, select_input
from src.term_inventory import parse_terms


def annotate(doc,text):
    if doc.text != text:
        raise ValueError('parser document differs from the selected article text')
    mentions=[]
    for entity in doc.ents:
        head=entity.root.head
        mentions.append({'start':entity.start_char,'end':entity.end_char,'text':entity.text,'label':entity.label_,
            'dependency':entity.root.dep_,'governing_lemma':head.lemma_,
            'sentence_span':{'start':entity.sent.start_char,'end':entity.sent.end_char}})
    # Keep the grammar itself for contextual resolution, including concepts
    # that NER does not recognize. These are observations, not identity claims.
    parsed=parse_terms(doc)
    noun_phrases=[{'start':chunk.start_char,'end':chunk.end_char,'text':chunk.text,
                   'head':chunk.root.i} for chunk in doc.noun_chunks]
    syntax=[{**record,'head':doc[i].head.i} for i,record in enumerate(parsed['syntax'])]
    return {'schema':'article-linguistics-2',
            'input_sha256':hashlib.sha256(text.encode()).hexdigest(),'mentions':mentions,
            'noun_phrases':noun_phrases,'syntax':syntax,
            'term_inventory_schema':parsed['schema'],'terms':parsed['terms'],
            'parser':{'spacy':SPACY_VERSION,'model':MODEL,'model_version':MODEL_VERSION}}


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument('--news',required=True,type=Path)
    p.add_argument('--out',required=True,type=Path)
    p.add_argument('--input-mode',choices=('headline','full-text'),default='headline')
    a=p.parse_args(argv)
    articles=[select_input(row,a.input_mode) for row in load_test_articles(a.news)]
    nlp=load_parser()
    payload={'input_mode':a.input_mode,'news_sha256':hashlib.sha256(a.news.read_bytes()).hexdigest(),'articles':{}}
    for row,doc in zip(articles,nlp.pipe((r['text'] for r in articles),batch_size=128)):
        payload['articles'][str(row['id'])]=annotate(doc,row['text'])
    if len(payload['articles'])!=len(articles):raise ValueError('article parse coverage is incomplete')
    a.out.write_text(json.dumps(payload,ensure_ascii=False))
    print('Parsed entity types, dependencies and exact spans for',len(articles),'article inputs')


if __name__=='__main__':main()
