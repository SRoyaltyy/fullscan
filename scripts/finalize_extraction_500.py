"""Combine all 500 source-reviewed judgments with the immutable first run."""
import collections,hashlib,json,pathlib
ROOT=pathlib.Path(__file__).resolve().parents[1]
DATA=ROOT/'eval/raw-extraction/benchmark-500'
def read(path):return json.loads(path.read_text(encoding='utf-8'))
def digest(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def main():
    samples={s['id']:s for s in read(DATA/'precision-sample.json')}
    judgments=[];seen=set()
    for path in sorted(DATA.glob('precision-review-*.json')):
        batch=read(path)
        assert not(set(batch['invalid']) & set(batch['uncertain']))
        for ident in batch['reviewed_ids']:
            assert ident not in seen
            seen.add(ident);k=str(ident)
            status='invalid' if k in batch['invalid'] else 'uncertain' if k in batch['uncertain'] else 'valid'
            judgments.append({'id':ident,'ticker':samples[ident]['ticker'],
                'forms':samples[ident]['forms'],'normalized':samples[ident]['normalized'],
                'status':status,'reason':batch['invalid'].get(k,batch['uncertain'].get(k,batch['valid_default']))})
    assert seen==set(samples) and len(seen)==500
    judged={'annotation':'Assistant source review of all sampled outputs; not independent human certification.',
            'sample_sha256':digest(DATA/'precision-sample.json'),'judgments':judgments}
    (DATA/'precision-judgments.json').write_text(json.dumps(judged,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    counts=collections.Counter(j['status'] for j in judgments)
    by_stock={ticker:dict(collections.Counter(j['status'] for j in judgments if j['ticker']==ticker))
              for ticker in sorted({j['ticker'] for j in judgments})}
    first=read(DATA/'recall-results.json')
    result={**first,'schema':'extraction-500-final-1','first_run_results_sha256':digest(DATA/'recall-results.json'),
       'precision':{'sample_groups':500,'valid':counts['valid'],'invalid':counts['invalid'],'uncertain':counts['uncertain'],
         'definitely_valid_fraction':counts['valid']/500,
         'valid_fraction_if_all_uncertain_valid':(counts['valid']+counts['uncertain'])/500,
         'resolved_precision':counts['valid']/(counts['valid']+counts['invalid']),
         'interval_type':'Annotation-uncertainty bounds, not a statistical confidence interval.',
         'interpretation':'Standalone nominal/named/technical term validity under the frozen broad contract, not correct company relationships.',
         'judgments_sha256':digest(DATA/'precision-judgments.json'),
         'invalid_categories':dict(collections.Counter(j['reason'] for j in judgments if j['status']=='invalid')),
         'by_stock':by_stock}}
    refs=read(DATA/'reference-labels.json')
    rows=read(DATA/'term-results.json')
    source_docs=[read(DATA/'sources'/f"{c['ticker']}.json") for c in refs['cases']]
    texts=[p['text'] for d in source_docs for p in d['paragraphs']]
    result['sampling_receipt']={'issuers':len(refs['cases']),'passages':len(texts),
       'distinct_whitespace_normalized_passages':len({' '.join(t.split()) for t in texts}),
       'characters':sum(len(t) for t in texts),
       'distinct_reference_strings_across_issuers':len({' '.join(t['name'].split()).casefold() for c in refs['cases'] for t in c['terms']}),
       'single_word_references':sum(r['stratum']=='single_word' for r in rows),
       'multiword_references':sum(r['stratum']=='multiword' for r in rows)}
    result['missing_exact']=[{'ticker':r['ticker'],'term':r['name']} for r in rows if not r['new_exact']]
    result['missing_complete_span']=[{'ticker':r['ticker'],'term':r['name']} for r in rows if not r['new_complete_span']]
    (DATA/'summary.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    s=result['summary'];p=result['precision'];unit=result['by_stratum']
    lines=['# Frozen extraction benchmark: 851 references across 12 stocks','',
      'This evaluates extraction before relationship decisions. The parser remained at '
      '`6dad95df8f7c2bb3b49df4de62c9ab80813c20db` throughout the test. No parser changes or reference relabelling followed the results.','',
      '## Results','',
      '| Measure | Existing raw inventory | Extraction-first mode |','| --- | ---: | ---: |',
      f"| Exact term or predeclared source alias | {s['raw_exact']}/851 ({s['raw_exact_rate']:.1%}) | {s['new_exact']}/851 ({s['new_exact_rate']:.1%}) |",
      f"| Complete span inside any candidate, including longer phrases | {s['raw_complete_span']}/851 ({s['raw_complete_span_rate']:.1%}) | {s['new_complete_span']}/851 ({s['new_complete_span_rate']:.1%}) |",
      f"| Single-word labels, including written compounds/identifiers | {unit['single_word']['raw_exact']}/330 ({unit['single_word']['raw_exact_rate']:.1%}) | {unit['single_word']['new_exact']}/330 ({unit['single_word']['new_exact_rate']:.1%}) |",
      f"| Multiword labels | {unit['multiword']['raw_exact']}/521 ({unit['multiword']['raw_exact_rate']:.1%}) | {unit['multiword']['new_exact']}/521 ({unit['multiword']['new_exact_rate']:.1%}) |",'',
      f"Without source-alias credit, the new mode recovers {s['new_canonical_exact']}/851 canonical reference strings exactly. The exact and containment measures are intentionally separate. Relationship correctness is not scored.",'',
      '## Output noise review','',
      'A seeded, issuer-stratified sample of 500 distinct normalized output groups was reviewed against the same source passages. These are groups, not repeated occurrences. Generic business nouns and process nouns are accepted even without a known company relationship.','',
      f"- Clearly valid standalone nominal/named/technical terms: {p['valid']}/500 ({p['definitely_valid_fraction']:.1%}).",
      f"- Invalid as standalone terms: {p['invalid']}/500 ({p['invalid']/500:.1%}).",
      f"- Ambiguous scope or boundaries: {p['uncertain']}/500 ({p['uncertain']/500:.1%}).",
      f"- Validity ranges from {p['definitely_valid_fraction']:.1%} to {p['valid_fraction_if_all_uncertain_valid']:.1%} depending on the 12 unresolved judgments. This is an annotation range, not a confidence interval.",'',
      'The main unwanted categories are 71 generic verbs, 58 isolated modifiers, 29 discourse/function/quantifier items and 24 malformed or incomplete names, phrases and identifiers. Examples include "paid", "solely", "financial", "Rule 457(a" and "Wilcox A". The liberal content-word fallback improves capture while retaining these extras.','',
      '## By stock','',
      '| Stock | Reference terms | Exact term/alias | Complete span | Valid output groups / reviewed |',
      '| --- | ---: | ---: | ---: | ---: |']
    for ticker,score in result['by_stock'].items():
        review=p['by_stock'][ticker];n=sum(review.values())
        lines.append(f"| {ticker} | {score['references']} | {score['new_exact']}/{score['references']} ({score['new_exact_rate']:.1%}) | {score['new_complete_span']}/{score['references']} ({score['new_complete_span_rate']:.1%}) | {review.get('valid',0)}/{n} |")
    lines+=['','## Actual remaining failures','',
      '- NKE: the competitor names "On" and "Under Armour" were not recovered as complete terms.',
      '- LYNX/BRO/VST: statutory citations such as "Rule 457(a)", "15(d)" and "Section 401(a)" are split or clipped.',
      '- BVN: multiword legal titles and citations, including "Article 177 of the Peruvian Companies Law", remain incomplete.',
      '- Other losses involve prepositions and coordination, including "risk of liability", "results of operations", "selling, general, and administrative expenses" and "worker health and safety".',
      '- Some recovered terms have surplus possessives/modifiers, e.g. "MEM’s technical branch" instead of the minimal "technical branch". Those count in containment, not exact recovery.','',
      '## Sampling and provenance','',
      f"The sample contains {result['sampling_receipt']['passages']} retained filing passages ({result['sampling_receipt']['distinct_whitespace_normalized_passages']} distinct after whitespace normalization) from 12 issuers across 11 named sectors plus one unclassified issuer. It covers {result['sampling_receipt']['characters']:,} source characters. There are 851 distinct company–term reference pairs and {result['sampling_receipt']['distinct_reference_strings_across_issuers']} distinct reference strings across companies. Repeated mentions and the duplicated LYNX passage do not add reference units. Single and multiword units can overlap; their counts are disclosed above.",'',
      'The source profiles retain selected filing prose. They do not reconstruct every original filing. One BW passage ends mid-sentence; labels cover only the text actually present. This is passage-level extraction evidence, not whole-filing recall. The 12 issuers were excluded from the recoverable cumulative prior-issuer ledger; unrecovered historical overlap cannot be ruled out.','',
      'Source-first references and the 500-output judgments were made by the assistant, not an independent human annotator. The sources, labels, producer code hashes and all per-term evidence are retained. These clustered samples do not establish a market-wide 98% precision/recall guarantee.','',
      'Freeze order:',
      '- Draw/protocol: `3ce7585495eab4363c13d1b76d689709df077eac`.',
      '- Exact sources before annotation: `fa4e6e56b44363dc3fe2cbe7c9e13c9a5e516388`.',
      '- 851 reference pairs and scorer before predictions: `9ae093f2eff590a1cf784c57c6887ba154988117`.',
      '- First predictions and seeded review sample before output judgments: `c5944ef827b8f56e0677bd09042a736f15906982`.','',
      '## Reproduction','',
      'The independent integrity audit verified 7,427 exact source occurrence spans, recomputed all term-level results from the saved predictions, reproduced the seeded 500-group sample and checked JSON/CSV/checksum agreement. All 12 archived ticker identities matched the reserved issuers.','',
      'Install requirements-filing-parser.txt and run from the repository root:',
      '',
      '    python scripts/score_extraction_500.py --label-freeze 9ae093f2eff590a1cf784c57c6887ba154988117',
      '    python scripts/finalize_extraction_500.py',
      '    python scripts/verify_extraction_500.py',
      '',
      'The scorer checks the frozen parser files and source/label hashes, then writes the complete predictions, per-term JSON/CSV and the same seeded output sample. The finalizer reuses all 500 recorded source-review judgments; it does not automatically judge relevance or use POS tags as gold labels.','',
      'Files: `summary.json`, `term-results.csv`, `term-results.json`, `reference-labels.json`, `precision-sample.json`, `precision-judgments.json`, `predictions.json.gz`, `sources/` and the freeze manifests.','']
    (DATA/'README.md').write_text('\n'.join(lines),encoding='utf-8')
    print(json.dumps({'sampling':result['sampling_receipt'],'recall':s,'precision':p},indent=2))
if __name__=='__main__':main()
