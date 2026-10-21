"""Replay every source/paragraph of a selected archived filing, without fetches.

Input is an existing full filing graph, not excerpts or manually labelled terms.
Use --all for every filing in that issuer graph. This is a coverage/inspection
run, not a semantic accuracy benchmark.
"""
import argparse
from collections import defaultdict
import gzip
import hashlib
import json
from pathlib import Path
from src.filing_relationships import extract_sections,load_parser,validate_paragraph_coverage,compiler_fingerprint
from scripts.build_filing_profiles import atomic_json


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--graph',type=Path,required=True)
    selection=p.add_mutually_exclusive_group(required=True)
    selection.add_argument('--accession')
    selection.add_argument('--all',action='store_true')
    p.add_argument('--out',type=Path,required=True)
    a=p.parse_args(argv)
    data=a.graph.read_bytes()
    original=json.loads(gzip.decompress(data) if data[:2]==b'\x1f\x8b' else data)
    validate_paragraph_coverage(original)
    blocks=defaultdict(list)
    for para in original['paragraphs']:blocks[para['source_id']].append(para)
    sections=[]
    for source in original['sources']:
        if a.accession and source['accession']!=a.accession:continue
        text=''.join(p['text'] for p in sorted(blocks[source['id']],key=lambda p:p['start']))
        assert len(text)==source['characters'] and hashlib.sha256(text.encode()).hexdigest()==source['sha256']
        sections.append({**source,'text':text})
    if not sections:raise ValueError('selected accession is absent from this complete graph')
    result=extract_sections(sections,ticker=original['ticker'],name=original['name'],nlp=load_parser())
    result.update(as_of=original.get('as_of'),tickers=original.get('tickers',[original['ticker']]),
                  compiler_sha256=compiler_fingerprint())
    atomic_json(a.out/'graph.json.gz',result,compressed=True)
    rows=result['term_inventory']['terms']
    report={'input_sha256':hashlib.sha256(data).hexdigest(),'compiler_sha256':result['compiler_sha256'],
        'ticker':result['ticker'],'parser':result['parser'],'selected_accession':a.accession,
        'selection':'all issuer sources' if a.all else 'every source of one complete accession',
        'source_sections':len(sections),'source_characters':sum(len(s['text']) for s in sections),
        'coverage':result['coverage'],'inventory_schema':result['term_inventory']['schema'],
        'paragraphs_with_terms':sum(bool(p['term_ids']) for p in result['paragraphs']),
        'candidate_kinds':{kind:sum(kind in r['kinds'] for r in rows) for kind in ('named_entity','noun_phrase','noun_token')},
        'graph_sha256':hashlib.sha256((a.out/'graph.json.gz').read_bytes()).hexdigest(),
        'limitations':['All nominated parser candidates are retained; semantic extraction recall is unverified.',
                       'Contexts are occurrence-local observations, not resolved sense universes.',
                       'Only known explicit relationship rules create company edges.',
                       'Machine-readable attachment blocks are retained as inventory, not parsed as prose.']}
    atomic_json(a.out/'report.json',report)
    print(json.dumps(report,indent=2))


if __name__=='__main__':main()
