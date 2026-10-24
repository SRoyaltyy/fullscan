"""Stock-only evidence assembly across complete stored source sections.

Extraction does not depend on a successful relationship rule. Explicit
definitions join names within a document; complete names join documents.
Relationships remain dated statements, not timeless properties of an entity.
No fuzzy matching, industry inference, or article input is used here.
"""
from collections import defaultdict
import hashlib
import re

VERSION = 'stock-evidence-graph-2'
NAME_TYPES = {'ORG', 'PERSON', 'PRODUCT', 'LAW', 'GOV_AGENCY'}
GENERIC = {'we','us','our','ours','company','the company','registrant','issuer',
           'customers','business partners','internal systems','subsidiaries',
           'products','services','platform','equipment','materials','exhibit',
           'plans','following assumptions','natural disasters','key personnel'}
DESCRIPTORS = {
    'platform': ('product_platform', 'mentions_product_platform'),
    'software': ('product_platform', 'mentions_product_platform'),
    'trial': ('clinical_trial', 'mentions_clinical_trial'),
    'study': ('clinical_trial', 'mentions_clinical_trial'),
    'subsidiary': ('organization', 'owns_subsidiary'),
    'supplier': ('organization', 'buys_from'),
    'customer': ('organization', 'sells_to'),
    'partner': ('organization', 'partners_with'),
}
ALIAS = re.compile(r'\(\s*(?:(?:hereinafter|collectively|together|the)\s+)*[“"‘](?P<alias>[^”"’\n]{1,90})[”"’]\s*\)',re.I)


def key(text):
    # Proper-name normalization deliberately does not singularize or stem.
    return re.sub(r'\s+',' ',text).strip().casefold()


def ident(*parts):
    return hashlib.sha256('\0'.join(str(p) for p in parts).encode()).hexdigest()


def document_key(source):
    # An abbreviation's meaning is not shared across documents by default.
    return (source.get('accession',''),source.get('document',''),
            source.get('filing_date',''))


def specific(text):
    clean=key(text)
    return (clean not in GENERIC and bool(re.search('[A-Za-z]',text))
            and not re.fullmatch(r'(?:exhibit|item|section|page)\s+[\d.\w-]+',clean)
            and len(text.split())<=20)


def build(graph, *, as_of=None, verified_terms=()):
    """Build from fresh grammar observations, including unresolved candidates.

    Sources without the new inventory remain explicitly unsupported. This is
    not a way to relabel old rows and claim that all source text was re-extracted.
    """
    inventory=graph.get('term_inventory')
    if inventory is None:
        raise ValueError('fresh complete term inventory required for stock evidence graph')
    all_sources={s['id']:s for s in graph['sources']}
    sources={i:s for i,s in all_sources.items() if not as_of or (s.get('filing_date') and s['filing_date']<=as_of)}
    pieces=defaultdict(list)
    for p in graph['paragraphs']:
        if p['source_id'] in sources:pieces[p['source_id']].append(p)
    texts={i:''.join(p['text'] for p in sorted(pieces[i],key=lambda p:p['start'])) for i in sources}
    for i,s in sources.items():
        if len(texts[i])!=s['characters'] or hashlib.sha256(texts[i].encode()).hexdigest()!=s['sha256']:
            raise ValueError('stock evidence source checksum mismatch')
    terms=[t for t in inventory['terms'] if t['source_id'] in sources]
    by_source=defaultdict(list)
    for t in terms:by_source[t['source_id']].append(t)
    selected={}
    def add(source_id,start,end,kind,basis):
        prefix=re.match(r'(?:the|our|a|an)\s+',texts[source_id][start:end],re.I)
        if prefix:start+=prefix.end()
        value=texts[source_id][start:end]
        if not specific(value):return
        from .stock_keywords import _BACKGROUND_WORDS
        if not (kind=='LAW' and re.search(r'\d',value)) and all(
                w.casefold() in _BACKGROUND_WORDS for w in re.findall('[A-Za-z]+',value)):return
        k=(source_id,start,end)
        row=selected.setdefault(k,{'source_id':source_id,'start':start,'end':end,'text':value,
                                  'types':set(),'extraction_basis':set()})
        if kind:row['types'].add(kind)
        row['extraction_basis'].add(basis)

    # First pass: names are retained even if no relation was extracted.
    from .stock_document_context import listed_names
    schedule_rows=[r for sid,text in texts.items() for r in listed_names(sources[sid],text,graph['name'])]
    for row in schedule_rows:
        add(row['source_id'],row['start'],row['end'],'ORG','subsidiary_schedule_name')
    for t in terms:
        types=set(t.get('entity_types',[])) & NAME_TYPES
        for label in types:add(t['source_id'],t['start'],t['end'],label,'grammar_named_entity')
    for t in verified_terms:
        if t['source_id'] not in sources or texts[t['source_id']][t['start']:t['end']]!=t['text']:
            raise ValueError('verified term differs from source text')
        add(t['source_id'],t['start'],t['end'],None,'verified_term_input')
    # Specific concept arguments survive independently from their classification.
    # A rejected/wrong relationship does not remove its actual source term.
    for f in graph['facts']:
        for ev in f['evidence']:
            a=ev.get('argument')
            if ev['source_id'] not in sources or not a:continue
            value=a['text']
            named=any(t['start']>=a['start'] and t['end']<=a['end'] and set(t.get('entity_types',[])) & NAME_TYPES
                      for t in by_source[ev['source_id']])
            operating=f['predicate'] in ('offers','uses_input','mentions_clinical_trial','mentions_product_platform')
            if operating and len(value.split())>=2:
                add(ev['source_id'],a['start'],a['end'],None,'source_argument_candidate')
    # Explicit legal/institutional spans are a separate extraction path.
    disclosures=[]
    for row in graph.get('specific_records',{}).get('records',[]):
        ev=row.get('evidence',{}); anchor=ev.get('anchor') if isinstance(ev,dict) else None
        sid=row.get('source_id')
        if sid in sources and anchor and texts[sid][anchor['start']:anchor['end']]==anchor['text']:
            add(sid,anchor['start'],anchor['end'],'LAW' if row.get('category')=='law' else 'GOV_AGENCY','explicit_disclosure')
            disclosures.append((row,anchor))

    definitions=[]
    for sid,text in texts.items():
        if sources[sid].get('kind')=='machine_data':continue
        spans=by_source[sid]
        for m in ALIAS.finditer(text):
            alias=m.group('alias')
            if not specific(alias):continue
            adjacent=[t for t in spans if 0<=m.start()-t['end']<=35 and len(t['text'].split())<=20
                      and re.fullmatch(r'\s*(?:of\s+\d{4})?(?:,?\s*as amended)?\s*',text[t['end']:m.start()],re.I)]
            if not adjacent:continue
            # Prefer the complete grammar noun phrase over a nested NER token.
            full=min(adjacent,key=lambda t:t['start'])
            full_text=text[full['start']:m.start()].strip()
            full_text=re.sub(r',?\s+as amended$','',full_text,flags=re.I)
            full_text=re.sub(r'^(?:the|our|a|an)\s+','',full_text,flags=re.I)
            a,b=m.span('alias')
            heads=key(full_text).split()
            descriptor=next((DESCRIPTORS[w] for w in reversed(heads) if w in DESCRIPTORS),None)
            owned=bool(re.search(r'\b(?:our|ours)\b',text[full['start']:m.start()],re.I))
            if not specific(full_text) and not descriptor:continue
            canonical=alias if descriptor else full_text
            add(sid,a,b,descriptor[0] if descriptor else None,'explicit_defined_name')
            if not descriptor:add(sid,full['start'],full['end'],None,'explicit_full_name')
            name_start=text.index(full_text,full['start'],m.start())
            definitions.append({'id':ident(sid,m.start(),m.end()),'document':document_key(sources[sid]),
                'alias':alias,'canonical':canonical,'source_id':sid,
                'start':full['start'],'end':m.end(),'text':text[full['start']:m.end()],
                'alias_span':{'start':a,'end':b,'text':alias},
                'definition_span':{'start':name_start,'end':name_start+len(full_text),'text':full_text},
                'descriptor':descriptor,'issuer_owned':owned})

    aliases=defaultdict(list)
    for d in definitions:aliases[(d['document'],key(d['alias']))].append(d)
    # Second pass: revisit ALL source text for declared aliases. Definitions may
    # appear after the first use or in a different section of the same document.
    by_document=defaultdict(dict)
    for (scope,alias),defs in aliases.items():by_document[scope][alias]=defs
    alias_patterns={scope:re.compile(r'(?<![\w])(?:'+ '|'.join(re.escape(ds[0]['alias']) for ds in sorted(values.values(),key=lambda ds:-len(ds[0]['alias'])))+r')(?![\w])')
                    for scope,values in by_document.items()}
    for sid,text in texts.items():
        if sources[sid].get('kind')=='machine_data':continue
        doc=document_key(sources[sid])
        if doc in alias_patterns:
            for m in alias_patterns[doc].finditer(text):
                add(sid,m.start(),m.end(),None,'document_defined_alias')

    entities={};occurrences=[]
    for (sid,start,end),row in sorted(selected.items()):
        if any(s['source_id']==sid and s['start']<=start<end<=s['end'] and
               (s['start'],s['end'])!=(start,end) for s in schedule_rows):continue
        if any(d['source_id']==sid and d['definition_span']['start']<=start<end<=d['definition_span']['end']
               and (start,end)!=(d['definition_span']['start'],d['definition_span']['end'])
               for d in definitions):continue
        defs=aliases.get((document_key(sources[sid]),key(row['text'])),[])
        canonical_names={key(d['canonical']) for d in defs}
        ambiguous=len(canonical_names)>1
        canonical=defs[0]['canonical'] if len(canonical_names)==1 else row['text']
        # Short, undeclared names are scoped to their document to avoid silently
        # merging different organizations or concepts across all company sources.
        global_name=len(canonical.split())>=2 or bool(defs and not ambiguous)
        scope='stock' if global_name else repr(document_key(sources[sid]))
        entity_id=ident(graph['ticker'],scope,key(canonical)) if not ambiguous else None
        oid=ident(sid,start,end)
        occurrence={**row,'id':oid,'entity_id':entity_id,'types':sorted(row['types']),
                    'extraction_basis':sorted(row['extraction_basis']),
                    'identity_status':'ambiguous_definition' if ambiguous else 'explicit_definition' if defs else 'exact_name_only',
                    'definition_ids':sorted(d['id'] for d in defs),
                    'filing_date':sources[sid].get('filing_date','')}
        occurrences.append(occurrence)
        if entity_id:
            ent=entities.setdefault(entity_id,{'id':entity_id,'name':canonical,'types':set(),
                'occurrence_ids':[],'definition_ids':set(),'assertion_ids':[]})
            ent['types'].update(row['types']);ent['occurrence_ids'].append(oid)
            ent['definition_ids'].update(d['id'] for d in defs)

    assertions=[]
    for row in schedule_rows:
        for o in occurrences:
            if (o['source_id'],o['start'],o['end'])!=(row['source_id'],row['start'],row['end']) or not o['entity_id']:continue
            aid=ident('subsidiary_schedule',o['entity_id'],row['source_id'],row['start'])
            assertions.append({'id':aid,'entity_id':o['entity_id'],'subject':graph['ticker'],
                **{k:row[k] for k in ('predicate','assertion','filing_date','source_id','basis','direct_ownership_established')},
                'effective_date':None,'evidence':{k:row[k] for k in ('source_id','start','end','text')},
                'capacity':'group_ownership_disclosure'})
            entities[o['entity_id']]['assertion_ids'].append(aid)
    for detail,anchor in disclosures:
        for o in occurrences:
            if (o['source_id'],o['start'],o['end'])!=(detail['source_id'],anchor['start'],anchor['end']) or not o['entity_id']:continue
            aid=ident('disclosure',detail['id'],o['entity_id'])
            direct=detail.get('direct_obligation_asserted',False)
            negative=detail.get('applicability')=='explicitly_out_of_scope'
            assertions.append({'id':aid,'entity_id':o['entity_id'],'subject':graph['ticker'],
                'predicate':'subject_to' if direct else 'mentions_legal_reference' if detail['category']=='law' else 'mentions_institution',
                'assertion':'negated' if negative else 'asserted' if direct else 'reported',
                'filing_date':sources[detail['source_id']].get('filing_date',''),'effective_date':None,
                'source_id':detail['source_id'],'evidence':detail['evidence'],'basis':'explicit_disclosure',
                'capacity':'legal_reference' if detail['category']=='law' else 'institution_reference',
                'applicability':detail.get('applicability')})
            entities[o['entity_id']]['assertion_ids'].append(aid)
    for f in graph['facts']:
        for ev in f['evidence']:
            sid=ev['source_id'];arg=ev.get('argument')
            if sid not in sources or not arg:continue
            matches=[o for o in occurrences if o['source_id']==sid and
                     arg['start']<=o['start']<o['end']<=arg['end'] and o['entity_id']]
            for o in matches:
                aid=ident(f['id'],sid,o['id'])
                row={'id':aid,'entity_id':o['entity_id'],'subject':f['subject'],
                    'predicate':f['predicate'],'assertion':f['status'],
                    'filing_date':sources[sid].get('filing_date',''),
                    'effective_date':None,'source_id':sid,'evidence':ev,
                    'basis':'source_clause','capacity':f.get('qualifiers',{}).get('frame',{}).get('capacity','unknown')}
                assertions.append(row);entities[o['entity_id']]['assertion_ids'].append(aid)
    # A definition supplies a separate typed relationship statement. It never
    # transfers another passage's negation, transaction stage or operating role.
    for d in definitions:
        if not d['descriptor'] or not d['issuer_owned']:continue
        for o in occurrences:
            if d['id'] not in o['definition_ids'] or not o['entity_id']:continue
            aid=ident('definition',d['id'],o['entity_id'])
            if aid in entities[o['entity_id']]['assertion_ids']:continue
            assertions.append({'id':aid,'entity_id':o['entity_id'],'subject':graph['ticker'],
                'predicate':d['descriptor'][1],'assertion':'asserted',
                'filing_date':sources[d['source_id']].get('filing_date',''),'effective_date':None,
                'source_id':d['source_id'],'evidence':{k:d[k] for k in ('source_id','start','end','text')},
                'basis':'explicit_owned_definition','capacity':d['descriptor'][0]})
            entities[o['entity_id']]['assertion_ids'].append(aid)
    for ent in entities.values():
        ent['types']=sorted(ent['types']);ent['definition_ids']=sorted(ent['definition_ids'])
        ent['assertion_ids']=sorted(set(ent['assertion_ids']))
        ent['classification_status']='supported_statements' if ent['assertion_ids'] else 'unclassified'
    return {'schema':VERSION,'stock':graph['ticker'],'entities':list(entities.values()),
            'occurrences':occurrences,'definitions':definitions,'assertions':assertions,
            'coverage':{'source_sections':len(sources),'characters_indexed':sum(len(t) for t in texts.values()),
                        'all_grammar_candidates_retained':True,'key_term_recall_verified':False,
                        'classification_complete':False},
            'limitations':['Literal evidence assembly; implicit/coreferential or corporate-language reasoning can remain unresolved.',
                          'Matching full names is identity evidence, not independently proven business relevance.',
                          'Statements retain their original dates and qualifications; latest mention does not establish current ownership.']}
