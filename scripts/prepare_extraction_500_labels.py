"""Compile manually source-reviewed labels; never imports or runs a parser."""
import hashlib,json,pathlib,re
ROOT=pathlib.Path(__file__).resolve().parents[1]
DATA=ROOT/'eval/raw-extraction/benchmark-500'
MERGES={'CRUS':{'components':'component','manufacturers':'manufacturer','applicable laws':'applicable law'},
        'KLC':{'fiscal years':'fiscal year'},'VST':{'businesses':'business'}}
def main():
    annotations={k:v for p in sorted(DATA.glob('annotation-batch-*.json'))
                 for k,v in json.loads(p.read_text(encoding='utf-8')).items()}
    cases=[]
    for ticker,paragraphs in annotations.items():
        source=json.loads((DATA/'sources'/f'{ticker}.json').read_text(encoding='utf-8'))
        terms={}
        for items in paragraphs:
            for value in items:
                value={'text':value} if isinstance(value,str) else value
                name=MERGES.get(ticker,{}).get(value['text'],value['text'])
                key=' '.join(name.split()).casefold()
                term=terms.setdefault(key,{'name':name,'accepted_names':[],'witnesses':[]})
                for form in [value['text']]+value.get('aliases',[]):
                    if form not in term['accepted_names']:term['accepted_names'].append(form)
        for term in terms.values():
            seen=set()
            for form in term['accepted_names']:
                pattern=(r'(?<!\w)' if form[0].isalnum() else '')+re.escape(form)+(r'(?!\w)' if form[-1].isalnum() else '')
                hits=[]
                for pi,p in enumerate(source['paragraphs']):
                    for match in re.finditer(pattern,p['text'],re.I):
                        ident=(pi,match.start(),match.end())
                        if ident not in seen:
                            hits.append({'paragraph':pi,'start':match.start(),'end':match.end(),'text':match.group()})
                            seen.add(ident)
                # A case-insensitive duplicate accepted form may share witnesses.
                if not hits and not any(form.casefold()==w['text'].casefold() for w in term['witnesses']):
                    raise ValueError((ticker,form,'not present at a whole-token boundary'))
                term['witnesses'].extend(hits)
        cases.append({'ticker':ticker,'cik':source['cik'],'source_file_sha256':hashlib.sha256((DATA/'sources'/f'{ticker}.json').read_bytes()).hexdigest(),'terms':list(terms.values())})
    result={'schema':'extraction-500-reference-1',
      'annotation':'Assistant source-first review of every selected passage before parser outputs; not independent human certification. Common-number variants consolidated manually before predictions. Atomic and meaningful multiword terms both count; overlapping phrase/atomic counts are disclosed separately.',
      'reference_terms':sum(len(c['terms']) for c in cases),'cases':cases,
      'precision_sampling':{'unit':'Distinct (issuer, normalized candidate string) groups, not occurrences or resolved identities.',
        'n':500,'allocation':'42 per first 8 issuers in draw order, 41 per remaining 4.',
        'rank':'SHA256(draw.seed + :precision: + ticker + : + normalized)',
        'validity':'A complete nominal, named or technical term supported by at least one cited occurrence. Generic business nouns/classes accepted. Standalone discourse words, generic verbs/adverbs, unsupported fragments or incomplete proper-name pieces are invalid. Ambiguous judgments remain separate.',
        'annotation':'Assistant source review after output sampling; not independent human validation.'}}
    assert result['reference_terms']>=500
    (DATA/'reference-labels.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({'reference_terms':result['reference_terms'],'by_stock':{c['ticker']:len(c['terms']) for c in cases}},indent=2))
if __name__=='__main__':main()
