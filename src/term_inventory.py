"""Lossless, occurrence-level grammar observations shared by filings/articles.

Every spaCy entity, noun chunk and NOUN/PROPN token is retained, even without a
recognized company relationship. A context fingerprint is not a resolved sense
or global identity. No language model or external service runs here.
"""
import hashlib
import json
from bisect import bisect_right
from collections import defaultdict

VERSION = 'term-inventory-1'


def validate_spans(value,text):
    if isinstance(value,list):
        for item in value:validate_spans(item,text)
    elif isinstance(value,dict):
        if {'start','end','text'}<=value.keys():
            if not (0<=value['start']<value['end']<=len(text)) or text[value['start']:value['end']]!=value['text']:
                raise ValueError('term inventory source span mismatch')
        for child in value.values():validate_spans(child,text)


def validate_article_terms(text,payload):
    if not payload or 'terms' not in payload:return []
    if payload.get('term_inventory_schema')!=VERSION:
        raise ValueError('unsupported article term inventory')
    if payload.get('input_sha256')!=hashlib.sha256(text.encode()).hexdigest():
        raise ValueError('article term inventory does not match the scored text')
    validate_spans(payload['terms'],text)
    return payload['terms']


def span(value):
    return {'start':value.start_char,'end':value.end_char,'text':value.text}


def token_span(token):
    return {'start':token.idx,'end':token.idx+len(token.text),'text':token.text}


def parse_terms(doc):
    chunks=list(doc.noun_chunks)
    by_root={c.root.i:c for c in chunks}
    found={}
    sentence_contexts={}
    def add(value,kind,label=None):
        root=value.root
        key=(value.start_char,value.end_char)
        row=found.setdefault(key,{**span(value),'normalized':' '.join(
            t.lemma_.casefold() for t in value if not t.is_punct and t.pos_!='DET'),
            'kinds':[],'entity_types':[],'head':token_span(root)})
        row['kinds'].append(kind)
        if label:row['entity_types'].append(label)
    for entity in doc.ents:add(entity,'named_entity',entity.label_)
    for chunk in chunks:add(chunk,'noun_phrase')
    for token in doc:
        if token.pos_ in ('NOUN','PROPN'):add(doc[token.i:token.i+1],'noun_token')
    for row in found.values():
        root=doc.char_span(row['head']['start'],row['head']['end']).root
        phrase=next((c for c in chunks if c.start<=root.i<c.end),doc[root.i:root.i+1])
        sentence=root.sent
        governor=root
        path=[]
        while governor.head!=governor and governor.pos_ not in ('VERB','AUX'):
            path.append({'text':governor.text,'dependency':governor.dep_})
            governor=governor.head
        subjects=[by_root[c.i] if c.i in by_root else doc[c.i:c.i+1]
                  for c in governor.children if c.dep_ in ('nsubj','nsubjpass','csubj')]
        objects=[by_root[c.i] if c.i in by_root else doc[c.i:c.i+1]
                 for c in governor.children if c.dep_ in ('dobj','obj','attr','oprd')]
        if sentence.start_char not in sentence_contexts:
            sentence_contexts[sentence.start_char]=(span(sentence),sorted({t.lemma_.casefold() for t in sentence
                if t.pos_ in ('NOUN','PROPN','VERB','ADJ') and not t.is_stop}))
        sentence_span,features=sentence_contexts[sentence.start_char]
        row['kinds']=sorted(set(row['kinds']))
        row['entity_types']=sorted(set(row['entity_types']))
        row['context']={'sentence':sentence_span,'noun_phrase':span(phrase),'features':features}
        row['context']['fingerprint']=hashlib.sha256(json.dumps(
            [row['normalized'],phrase.text.casefold(),features],ensure_ascii=False).encode()).hexdigest()
        row['capacity']={'dependency':root.dep_,'path':path,
            'governor':{**token_span(governor),'lemma':governor.lemma_,'pos':governor.pos_},
            'subjects':[span(s) for s in subjects],'objects':[span(o) for o in objects],
            'negated':any(c.dep_=='neg' for c in governor.children),
            'modals':sorted({c.lemma_ for c in governor.children if c.tag_=='MD'})}
        row['sense_status']='unresolved'
    syntax=[{**token_span(t),'lemma':t.lemma_,'pos':t.pos_,'dependency':t.dep_,
             'head_start':t.head.idx,'head_end':t.head.idx+len(t.head.text)} for t in doc]
    return {'schema':VERSION,'terms':[found[k] for k in sorted(found)],'syntax':syntax}


def relocate(value,offset):
    """Relocate every nested exact span, leaving non-span fields unchanged."""
    if isinstance(value,list):return [relocate(v,offset) for v in value]
    if not isinstance(value,dict):return value
    return {k:(v+offset if k in ('start','end','head_start','head_end') else relocate(v,offset))
            for k,v in value.items()}


def bind_window(parsed, *, source_id, offset, text, paragraphs, facts):
    key=hashlib.sha256(f'{source_id}:{offset}:{offset+len(text)}'.encode()).hexdigest()
    rows=[]
    contexts={}
    starts=[p['start'] for p in paragraphs]
    arguments=[(f,ev['argument']) for f in facts for ev in f['evidence']
               if ev['source_id']==source_id and ev.get('argument')
               and offset<=ev['argument']['start']<offset+len(text)]
    for template in parsed['terms']:
        context=template['context']
        sentence=relocate(context['sentence'],offset)
        context_id=hashlib.sha256(f"{source_id}:{sentence['start']}:{sentence['end']}".encode()).hexdigest()
        contexts.setdefault(context_id,{'id':context_id,**sentence,'features':context['features']})
        # Store shared sentence text/features once, rather than repeating an
        # entire paragraph for each entity/phrase/noun occurrence it contains.
        row=relocate({**template,'context':{'noun_phrase':context['noun_phrase'],
            'fingerprint':context['fingerprint'],'sentence_id':context_id}},offset)
        row.update(id=hashlib.sha256(f"{source_id}:{row['start']}:{row['end']}".encode()).hexdigest(),
                   source_id=source_id,window_id=key)
        i=max(0,bisect_right(starts,row['start'])-1)
        row['paragraph_ids']=[]
        while i<len(paragraphs) and paragraphs[i]['start']<row['end']:
            row['paragraph_ids'].append(paragraphs[i]['id']);i+=1
        links=[]
        for fact,arg in arguments:
            if arg['start']<=row['start']<row['end']<=arg['end']:
                links.append({'relationship_id':fact['id'],'predicate':fact['predicate'],
                    'subject':fact['subject'],'assertion':fact['status'],'object':fact['object'],
                    'argument':arg,'attachment':'within_extracted_argument'})
        row['relationships']=links
        row['relationship_status']='argument_linked' if links else 'unresolved'
        rows.append(row)
    window={'id':key,'source_id':source_id,'start':offset,'end':offset+len(text),
            'sha256':hashlib.sha256(text.encode()).hexdigest(),
            'term_ids':[r['id'] for r in rows],'syntax':relocate(parsed['syntax'],offset),
            'contexts':[contexts[k] for k in sorted(contexts)]}
    return window,rows


def validate_inventory(graph):
    inventory=graph.get('term_inventory')
    if inventory is None:return  # Old graphs remain explicitly identifiable.
    if inventory.get('schema')!=VERSION:raise ValueError('unsupported term inventory')
    sources={s['id']:s for s in graph['sources']}
    paragraphs={p['id']:p for p in graph['paragraphs']}
    by_source=defaultdict(list)
    for p in paragraphs.values():by_source[p['source_id']].append(p)
    for values in by_source.values():values.sort(key=lambda p:p['start'])
    starts={s:[p['start'] for p in values] for s,values in by_source.items()}
    texts={s:''.join(p['text'] for p in by_source[s]) for s in sources}
    terms={r['id']:r for r in inventory['terms']}
    windows={w['id']:w for w in inventory['windows']}
    if len(terms)!=len(inventory['terms']) or len(windows)!=len(inventory['windows']):
        raise ValueError('duplicate linguistic window or term occurrence')
    if any(w['source_id'] not in sources for w in windows.values()):
        raise ValueError('linguistic window source is missing')
    referenced=[]
    for source_id,source in sources.items():
        cursor=0
        for w in sorted((w for w in windows.values() if w['source_id']==source_id),key=lambda w:w['start']):
            if source.get('kind')=='machine_data' or w['start']!=cursor or w['end']<=cursor:
                raise ValueError('term inventory window coverage gap or overlap')
            text=texts[source_id]
            if hashlib.sha256(text[w['start']:w['end']].encode()).hexdigest()!=w['sha256']:
                raise ValueError('term inventory window checksum mismatch')
            positions={(t['start'],t['end']) for t in w['syntax']}
            token_cursor=w['start']
            for t in w['syntax']:
                if t['start']<token_cursor or t['end']>w['end'] or text[token_cursor:t['start']].strip():
                    raise ValueError('term inventory token coverage is incomplete')
                if (t['head_start'],t['head_end']) not in positions:
                    raise ValueError('term inventory dependency head is missing')
                token_cursor=t['end']
            if text[token_cursor:w['end']].strip():raise ValueError('term inventory token coverage is incomplete')
            validate_spans(w['syntax'],text)
            validate_spans(w['contexts'],text)
            for ident in w['term_ids']:
                if ident not in terms or terms[ident]['window_id']!=w['id'] or terms[ident]['source_id']!=source_id:
                    raise ValueError('term occurrence/window link mismatch')
                referenced.append(ident)
            cursor=w['end']
        if source.get('kind')!='machine_data' and cursor!=source['characters']:
            raise ValueError('term inventory did not analyze the whole source')
    if set(referenced)!=set(terms) or len(referenced)!=len(terms):
        raise ValueError('term inventory occurrence is missing or duplicated')
    for row in terms.values():
        if row['source_id'] not in sources:raise ValueError('term occurrence source is missing')
        validate_spans(row,texts[row['source_id']])
        window=windows[row['window_id']]
        if not window['start']<=row['start']<row['end']<=window['end']:
            raise ValueError('term occurrence lies outside its window')
        context=next((c for c in window['contexts'] if c['id']==row['context']['sentence_id']),None)
        if not context or not context['start']<=row['head']['start']<row['head']['end']<=context['end']:
            raise ValueError('term occurrence context is missing or mismatched')
        values=by_source[row['source_id']]
        i=max(0,bisect_right(starts[row['source_id']],row['start'])-1)
        expected=set()
        while i<len(values) and values[i]['start']<row['end']:
            expected.add(values[i]['id']);i+=1
        if set(row['paragraph_ids'])!=expected or any(row['id'] not in paragraphs[p].get('term_ids',[]) for p in expected):
            raise ValueError('term occurrence/paragraph link mismatch')
    linked=defaultdict(set)
    for row in terms.values():
        for ident in row['paragraph_ids']:linked[ident].add(row['id'])
    for p in paragraphs.values():
        if set(p.get('term_ids',[]))!=linked[p['id']]:
            raise ValueError('paragraph term inventory is incomplete')
    facts={f['id']:f for f in graph['facts']}
    for row in terms.values():
        for link in row['relationships']:
            fact=facts.get(link['relationship_id'])
            if not fact or (link['subject'],link['predicate'],link['assertion'],link['object'])!=(
                    fact['subject'],fact['predicate'],fact['status'],fact['object']):
                raise ValueError('term relationship assertion mismatch')
            arg=link['argument']
            if not arg['start']<=row['start']<row['end']<=arg['end'] or not any(
                    ev['source_id']==row['source_id'] and ev.get('argument')==arg for ev in fact['evidence']):
                raise ValueError('term relationship argument mismatch')
    return len(terms)
