"""Dated stock -> relationship -> keyword records shared by profile and article views.

Keyword overlap is retrieval evidence. It never independently creates a stock
pass or a bullish/bearish conclusion. All candidate claims remain inspectable.
"""
from collections import defaultdict
import hashlib
import json
import math
import re

from .linguistics import lemma
from .entity_resolution import VERSION as ALIGNMENT_VERSION, POLICY, STOP, aliases, identity, decide, validate_annotations

# Hyphens separate words, but offsets always refer to the unmodified article.
TOKEN = re.compile(r"\d+(?:[./-]\d+)*|[A-Za-z][A-Za-z0-9]*(?:['’][A-Za-z0-9]+)*")

SCHEMA = 'stock-keywords-1.1'
ROLES = {'buys_from':'supplier', 'sells_to':'customer', 'uses_input':'input',
    'depends_on':'dependency', 'competes_with':'competitor', 'partners_with':'partner',
    'licenses':'license', 'offers':'product_or_service', 'subject_to':'exposure',
    'regulated_by':'regulator', 'approval_from':'approval_authority',
    'risk_mentions':'risk', 'exposed_to':'exposure', 'operates_in':'operating_geography',
    'owns_subsidiary':'subsidiary', 'acquires':'acquisition_target',
    'divests':'divestiture_target', 'differentiation_claim':'differentiation_claim'}
ROLES['has_officer']='officer'
ENTITY_ROLES = {'issuer','supplier','customer','competitor','partner','subsidiary',
                'regulator','approval_authority','acquisition_target','divestiture_target'}
LEGAL_SUFFIX = re.compile(r'(?:[, ]+(?:incorporated|corporation|company|limited|holdings|corp|inc|ltd|plc|llc)\.?)+$',re.I)
AUDITOR = re.compile(r'\b(?:as part of our audits?|we (?:have )?audited|our responsibility is to express|we are required to obtain an understanding)\b',re.I)
ADMIN = re.compile(r'^(?:(?:written|prior|reasonable|timely|additional|certain)\s+)*(?:notices?|updates?|information|documents?|disclosures?|consents?|reports?|statements?|assurances?|opinions?|understanding)$',re.I)
UNRESOLVED = re.compile(r'^(?:which|who|it|them|these|those|one|combination|performance|such .+|(?:many |other |third.party |certain |various )*(?:companies|parties|entities))$',re.I)
METRIC = re.compile(r'\b(?:adjusted EBITDA margin|EBITDA margin|gross margin|operating margin|free cash flow|capital expenditures?|net income|net loss|revenue|revenues)\b',re.I)
GUIDANCE = re.compile(r'(?P<new_low>\d+(?:\.\d+)?)%\s+to\s+(?P<new_high>\d+(?:\.\d+)?)%\s*,?\s*(?P<change>raised|lowered)\s+from\s+(?P<old_low>\d+(?:\.\d+)?)%\s+to\s+(?P<old_high>\d+(?:\.\d+)?)%',re.I)
CLAIM_TERMS = re.compile(r'\b(?:intellectual property|trade secrets?|network effects?|switching costs?|cost advantages?|patents?|proprietary technology|trademarks?|copyrights?)\b',re.I)


def normalized_words(value):
    return tuple(lemma(m.group()) for m in TOKEN.finditer(value or ''))


def quality_issues(predicate, value, quote):
    issues = []
    from .specific_filing_records import GENERIC
    if GENERIC.fullmatch(value.strip()):
        issues.append('generic_background_head')
    if AUDITOR.search(quote):
        issues.append('auditor_voice_is_not_issuer')
    if predicate in ('offers','uses_input') and ADMIN.fullmatch(value.strip()):
        issues.append('administrative_or_abstract_object')
    if UNRESOLVED.fullmatch(value.strip()):
        issues.append('unresolved_or_generic_object')
    if predicate == 'competes_with' and re.search(r'\b(?:personnel|talent|developers|engineers|employees|compensation)\b',quote,re.I):
        issues.append('labor_competition_is_not_product_competition')
    return issues


def compile_keywords(graph, *, as_of, ticker=None):
    """Retain every claim; aggregate short keywords without losing dated witnesses."""
    ticker = ticker or graph['ticker']
    sources = {s['id']:s for s in graph['sources']}
    evidence, records = {}, {}
    inventory=graph.get('term_inventory')
    allowed_sources={s['id'] for s in sources.values() if s.get('filing_date') and s['filing_date']<=as_of}
    terms=[t for t in (inventory or {}).get('terms',[]) if t['source_id'] in allowed_sources]
    term_dates={t['id']:sources[t['source_id']]['filing_date'] for t in terms}
    term_links=defaultdict(list)
    for term in terms:
        for link in term['relationships']:term_links[link['relationship_id']].append(term['id'])
    newest = {}
    for fact in graph['facts']:
        key = (fact['predicate'], fact['object'].casefold(), fact['object_type'])
        for ev in fact['evidence']:
            date = sources[ev['source_id']].get('filing_date','')
            if date and date <= as_of:
                order = (date, fact['status'] != 'asserted',fact['id'])
                if key not in newest or order > newest[key][0]:
                    newest[key] = (order, fact['id'])

    def add(keyword, role, predicate, state, reference, *, qualifiers=None, issues=(), current=False, object_type='concept'):
        words = normalized_words(keyword)
        context = json.dumps(qualifiers or {}, sort_keys=True, separators=(',',':'))
        record_key = (ticker, words, role, predicate, state, tuple(issues), current, context,object_type)
        if record_key not in records:
            records[record_key] = {'stock':ticker,'keyword':keyword,'normalized':' '.join(words),'object_type':object_type,
                'relationship':role,'predicate':predicate,'assertion':state,
                'current_as_of_cutoff':current,'review_required':bool(issues),
                'review_reasons':list(issues),'usable_for_scoring':current and state == 'asserted' and not issues,
                'evidence_ids':[], 'qualifiers':qualifiers or {}, 'context_key':context}
        if reference not in records[record_key]['evidence_ids']:
            records[record_key]['evidence_ids'].append(reference)

    for fact in graph['facts']:
        witnesses = [{**ev,'source':sources[ev['source_id']]} for ev in fact['evidence']
                     if sources[ev['source_id']].get('filing_date','') and sources[ev['source_id']]['filing_date'] <= as_of]
        if not witnesses:
            continue
        evidence[fact['id']] = {'subject':ticker,'predicate':fact['predicate'],'object':fact['object'],
            'object_type':fact['object_type'],'assertion':fact['status'], 'witnesses':witnesses,
            'term_occurrence_ids':sorted(set(term_links[fact['id']]))}
        key = (fact['predicate'],fact['object'].casefold(),fact['object_type'])
        current = newest[key][1] == fact['id']
        role = ROLES.get(fact['predicate'],'unclassified_relationship')
        if fact['predicate'] == 'subject_to' and fact['object_type'] in ('agency','law'):
            role = 'regulatory_exposure'
        issues = sorted(set(issue for ev in witnesses for issue in quality_issues(fact['predicate'],fact['object'],ev['quote'])))
        if 'labor_competition_is_not_product_competition' in issues:
            role = 'talent_competition'
        values = [fact['object']]
        if len(normalized_words(fact['object'])) > 12:
            values = list(dict.fromkeys(m.group() for m in CLAIM_TERMS.finditer(fact['object']))) if role == 'differentiation_claim' else []
            if not values:
                values = ['unresolved '+role.replace('_',' ')]
                issues = sorted(set(issues+['object_requires_phrase_resolution']))
        for value in values:
            add(value,role,fact['predicate'],fact['status'],fact['id'],qualifiers=fact.get('qualifiers'),issues=issues,current=current,object_type=fact['object_type'])
        if fact['predicate'] == 'buys_from':
            inputs = list((fact.get('qualifiers') or {}).get('inputs') or [])
            description = (fact.get('qualifiers') or {}).get('description','')
            match = re.fullmatch(r'(?:our\s+)?(.+?)\s+suppliers?',description,re.I)
            if match and match.group(1).lower() not in ('key','major','main','primary','other','largest'):
                inputs.append(match.group(1))
            for value in dict.fromkeys(inputs):
                add(value,'input','uses_input',fact['status'],fact['id'],qualifiers={'via_supplier':fact['object']},issues=issues,current=current)

    # These are separately versioned, evidence-bound disclosures, not new
    # assertions manufactured from the old generic relationship heads.
    specific=graph.get('specific_records')
    if specific:
        from .specific_filing_records import validate_details
        validate_details(graph,specific)
        for detail in specific['records']:
            if not detail['entity_match_eligible']:
                continue
            source=sources[detail['source_id']]
            if source['id'] not in allowed_sources:
                continue
            reference='specific:'+detail['id']
            evidence[reference]={'subject':ticker,'predicate':detail['relation'],
                'object':detail['entity'],'object_type':'law' if detail['category']=='law' else 'agency' if detail['category']=='authority' else 'concept',
                'assertion':'reported','witnesses':[{**detail['evidence'],'source_id':source['id'],'source':source}],
                'specific_disclosure_id':detail['id'],'paragraph_id':detail['paragraph_id']}
            add(detail['entity'],'disclosed_'+detail['category'],detail['relation'],'reported',reference,
                qualifiers={'disclosure_extractor':specific['version'],'disclosed_aliases':detail['aliases'],
                    'applicability':detail['applicability'],'direct_obligation_asserted':detail['direct_obligation_asserted'],
                    'details':detail['fields'],'missing_fields':detail['missing_fields']},
                issues=('disclosure_requires_applicability_and_article_relevance_review',),
                current=False,object_type=evidence[reference]['object_type'])

    # Quantities keep their exact paragraph. Only a fully matched range-change
    # construction is normalized; unrelated table values are never combined.
    for p in graph.get('paragraphs',[]):
        source = sources[p['source_id']]
        if not source.get('filing_date') or source['filing_date'] > as_of or p['kind'] != 'prose' or not p.get('signals'):
            continue
        metrics = list(dict.fromkeys(m.group() for m in METRIC.finditer(p['text'])))
        if not metrics:
            continue
        reference = 'paragraph:'+p['id']
        evidence[reference] = {'subject':ticker,'predicate':'reports_metric','object':metrics,
            'assertion':'reported','witnesses':[{'source_id':p['source_id'],'start':p['start'],'end':p['end'],'quote':p['text'],'source':source}]}
        change = GUIDANCE.search(p['text'])
        qualifiers = {'reported_quantities':[s['quote'] for s in p['signals']]}
        if change and len(metrics) == 1:
            qualifiers['guidance_change'] = {'unit':'percent','new':[float(change['new_low']),float(change['new_high'])],
                'previous':[float(change['old_low']),float(change['old_high'])], 'direction':change['change'].lower(), 'period':None}
        for metric in metrics:
            add(metric,'financial_metric','reports_metric','reported',reference,qualifiers=qualifiers,
                issues=('metric_period_and_context_require_review',),current=False)
    rows = []
    for key, row in sorted(records.items(),key=lambda item:repr(item[0])):
        row['evidence_ids'].sort()
        row['id'] = hashlib.sha256(json.dumps([key,row['evidence_ids']],sort_keys=True).encode()).hexdigest()
        dates = [w['source']['filing_date'] for ref in row['evidence_ids'] for w in evidence[ref]['witnesses']]
        row['first_filing_date'], row['latest_filing_date'] = min(dates), max(dates)
        row['term_occurrence_ids']=sorted({ident for ref in row['evidence_ids']
            for ident in evidence[ref].get('term_occurrence_ids',[])})
        row['term_occurrence_dates']={ident:term_dates[ident] for ident in row['term_occurrence_ids']}
        rows.append(row)
    return {'schema':SCHEMA,'as_of':as_of,'records':rows,'evidence':evidence,
        'term_inventory':None if inventory is None else {**inventory,'terms':terms,
            'windows':[w for w in inventory['windows'] if w['source_id'] in allowed_sources]},
        'coverage':{'raw_relationships':len(graph['facts']),
            'retained_relationships':sum(not k.startswith(('paragraph:','specific:')) for k in evidence),
            'specific_disclosure_records':sum(k.startswith('specific:') for k in evidence),
            'keywords':len(rows),'review_required':sum(r['review_required'] for r in rows),
            'term_inventory_available':inventory is not None,'term_occurrences':len(terms),
            'terms_without_resolved_relationship':sum(not t['relationships'] for t in terms),
            'paragraph_coverage':graph.get('coverage',{})}}


class KeywordIndex:
    def __init__(self, profiles):
        self.first = defaultdict(list)
        self.ticker_names = defaultdict(set)
        self.df = defaultdict(set)
        self.count = len(profiles)
        self.profiles = profiles
        self.related = defaultdict(list)
        self.by_phrase = defaultdict(list)
        self.organizations = defaultdict(set)
        self.entity_peers = defaultdict(list)
        for ticker,profile in profiles.items():
            for form in aliases(profile.name,'ORG'):
                self.organizations[form].add(ticker)
        for ticker, profile in sorted(profiles.items()):
            # Dated, explicitly evidenced officers use person identities rather
            # than inheriting the company's name/plural normalization rules.
            if not hasattr(profile,'keyword_records'):profile.keyword_records=[]
            if not hasattr(profile,'keyword_evidence'):profile.keyword_evidence={}
            from .evidence_matcher import _valid_person
            for fact in getattr(profile,'facts',[]):
                if fact.kind!='person' or not fact.filing_date or not _valid_person(fact):continue
                reference='paragraph:person:'+hashlib.sha256((ticker+fact.text+fact.filing_date+fact.passage).encode()).hexdigest()
                if reference in profile.keyword_evidence:continue
                profile.keyword_evidence[reference]={'subject':ticker,'predicate':'has_officer','object':fact.text,'object_type':'PERSON','assertion':'asserted',
                    'witnesses':[{'source_id':fact.accession,'start':None,'end':None,'offset_scope':'quote_only','quote':fact.passage,
                                  'source':{'filing_date':fact.filing_date,'accession':fact.accession,'section':fact.section,'source':fact.source}}]}
                profile.keyword_records.append({'id':reference,'stock':ticker,'keyword':fact.text,'normalized':' '.join(normalized_words(fact.text)),
                    'relationship':'officer','predicate':'has_officer','object_type':'PERSON','assertion':'asserted',
                    'current_as_of_cutoff':False,'review_required':True,'review_reasons':['officer_tenure_requires_review'],
                    'usable_for_scoring':False,'qualifiers':{},'context_key':'{}','evidence_ids':[reference],
                    'first_filing_date':fact.filing_date,'latest_filing_date':fact.filing_date})
            if getattr(profile,'keyword_coverage',None):
                profile.keyword_coverage['keywords']=len(profile.keyword_records)
                profile.keyword_coverage['review_required']=sum(r['review_required'] for r in profile.keyword_records)
            names = [profile.name, LEGAL_SUFFIX.sub('',profile.name).strip()]
            for name in set(names):
                words = normalized_words(name)
                if words:
                    issuer={'stock':ticker,'relationship':'issuer','object_type':'ORG','keyword':name,'id':'issuer:'+ticker,'evidence_ids':[]}
                    issuer['identity']=identity(issuer,profile,self.organizations)
                    self.first[words[0]].append((words,issuer))
            for row in getattr(profile,'keyword_records',[]):
                row['identity'] = identity(row,profile,self.organizations)
                row['identity']['known_from']=row['first_filing_date']
                words = tuple(row['normalized'].split())
                if words:
                    self.first[words[0]].append((words,row))
                    for alias in row['identity']['aliases']:
                        alternate=normalized_words(alias)
                        if alternate and alternate!=words:self.first[alternate[0]].append((alternate,row))
                        if alternate:self.entity_peers[alternate].append(row['identity'])
                    short = normalized_words(LEGAL_SUFFIX.sub('',row['keyword']))
                    if short and short != words and row['relationship'] in ENTITY_ROLES:
                        self.first[short[0]].append((short,row))
                    self.df[words].add(ticker)
                    self.by_phrase[words].append(row)
                    self.related[(ticker,row['relationship'],row['normalized'],row['predicate'],row.get('context_key',''))].append(row)
                    self.entity_peers[words].append(row['identity'])

    def browser_payload(self):
        """Compact retrieval index; quotations stay in hash-bound stock assets.

        Export the pinned Python dictionary's relevant inflections, not a second
        JavaScript lemmatizer that could silently disagree with offline matches.
        """
        from .linguistics import _lookup, IRREGULAR, english_words
        phrases = []
        vocabulary = set()
        raw_words = set()
        for entries in self.first.values():
            for phrase, row in entries:
                phrases.append({'phrase':list(phrase), 'row':row})
                vocabulary.update(phrase)
                if row.get('identity'):
                    vocabulary.update(row['identity']['source_context_terms'])
                    for sense in row['identity']['senses']:vocabulary.update(sense.get('terms',[]))
                    for alias in row['identity']['aliases']:
                        raw_words.update(t.group().lower() for t in TOKEN.finditer(alias))
                raw_words.update(t.group().lower() for t in TOKEN.finditer(row['keyword']))
        forms = set(_lookup()) | set(IRREGULAR) | set(english_words()) | raw_words | vocabulary
        normalization = {}
        for form in sorted(forms):
            base = lemma(form)
            if base in vocabulary and base != form:
                normalization[form] = base
        return {'schema':SCHEMA, 'catalog_tickers':self.count, 'phrases':phrases,
                'normalization':normalization,'alignment_version':ALIGNMENT_VERSION,'identity_policy':POLICY,
                'stopwords':sorted(STOP), 'comparison':'typed identity and sense alignment; unresolved candidates abstain'}

    def compare(self, text, *, article_date='', entity_annotations=None):
        annotations = validate_annotations(text,entity_annotations)
        from .term_inventory import validate_article_terms
        article_terms=validate_article_terms(text,entity_annotations)
        tokens = list(TOKEN.finditer(text or ''))
        words = [lemma(t.group()) for t in tokens]
        hits, named = [], set()
        for i, word in enumerate(words):
            for phrase, row in self.first.get(word,()):
                end = i+len(phrase)
                if tuple(words[i:end]) != phrase or re.search(r'[.!?;:\n]',text[tokens[i].end():tokens[end-1].start()]):
                    continue
                if row['relationship'] == 'issuer':
                    if decide(row['identity'],text,tokens[i].start(),tokens[end-1].end(),annotations=annotations)['status']=='resolved':named.add(row['stock'])
                else:
                    hits.append((phrase,row,tokens[i].start(),tokens[end-1].end()))
        matches, broad, seen, unresolved, rejected = [], {}, set(), [], []
        date_counts = {}
        # A full name and its contained surname are one mention. Preserve
        # distinct claims/contexts, while preferring the longest same-row span.
        grouped=defaultdict(list)
        for hit in hits:grouped[hit[1]['id']].append(hit)
        hits=[]
        for group in grouped.values():
            previous_end=-1
            for hit in sorted(group,key=lambda h:(h[2],-h[3])):
                if hit[3]<=previous_end:continue
                hits.append(hit);previous_end=hit[3]
        for phrase,row,start,end in hits:
            if article_date and row['first_filing_date'] > article_date:
                continue
            key = (row['id'],start,end)
            if key in seen:
                continue
            seen.add(key)
            canonical = tuple(row['normalized'].split())
            source_identity=identity(row,self.profiles[row['stock']],self.organizations,as_of=article_date) if article_date else row['identity']
            peers=[p for p in self.entity_peers[phrase] if not article_date or p.get('known_from','')<=article_date]
            alignment = decide(source_identity,text,start,end,peers=peers,annotations=annotations)
            candidate = {**row,'identity':source_identity,'alignment':alignment,'article_span':{'start':start,'end':end,'text':text[start:end]}}
            mentions=[t for t in article_terms if t['start']<=start and t['end']>=end]
            if mentions:
                mention=min(mentions,key=lambda t:t['end']-t['start'])
                candidate['article_capacity']={**mention['capacity'],'context':mention['context']}
            if row['relationship'] in ENTITY_ROLES-{'issuer'}:
                candidate['exposure_path']={'stock':row['stock'],'relationship':row['relationship'],
                    'entity':row['keyword'],'entity_id':source_identity['id'],'order':2,
                    'core_industry_inferred':False,'context_alignment':'not_assessed','impact':'not_inferred'}
            if article_date:
                pool=self.profiles[row['stock']].keyword_evidence
                refs=[ref for ref in row['evidence_ids'] if any(w['source']['filing_date']<=article_date for w in pool[ref]['witnesses'])]
                dates=[w['source']['filing_date'] for ref in refs for w in pool[ref]['witnesses'] if w['source']['filing_date']<=article_date]
                candidate.update(evidence_ids=refs,first_filing_date=min(dates),latest_filing_date=max(dates))
                candidate['term_occurrence_dates']={ident:date for ident,date in row.get('term_occurrence_dates',{}).items() if date<=article_date}
                candidate['term_occurrence_ids']=sorted(candidate['term_occurrence_dates'])
            if alignment['status'] != 'resolved':
                (rejected if alignment['status']=='rejected' else unresolved).append(candidate)
                continue
            if canonical not in date_counts:
                date_counts[canonical] = len({r['stock'] for r in self.by_phrase[canonical]
                    if not article_date or r['first_filing_date'] <= article_date})
            common = date_counts[canonical] >= max(8,math.ceil(self.count*.05))
            if common and row['relationship'] not in ENTITY_ROLES and row['stock'] not in named:
                key = (' '.join(phrase),row['relationship'])
                broad[key] = {'keyword':row['keyword'],'relationship':row['relationship'],
                    'candidate_stock_count':date_counts[canonical],
                    'reason':'broad keyword; additional stock context is required'}
                continue
            current = row['current_as_of_cutoff']
            if article_date:
                pool = self.profiles[row['stock']].keyword_evidence
                choices = [(w['source']['filing_date'],other['assertion'] != 'asserted',ref)
                    for other in self.related[(row['stock'],row['relationship'],row['normalized'],row['predicate'],row.get('context_key',''))]
                    for ref in other['evidence_ids'] for w in pool[ref]['witnesses'] if w['source']['filing_date'] <= article_date]
                current = bool(choices) and max(choices)[2] in row['evidence_ids']
            matches.append({**candidate,'current_as_of_cutoff':current,
                            'usable_for_scoring':current and row['assertion']=='asserted' and not row['review_required'],
                            'article_span':{'start':start,'end':end,'text':text[start:end]},
                            'decision':'identity_aligned_candidate','article_date':article_date})
        return {'schema':SCHEMA,'matches':matches,'broad_matches':list(broad.values()),
                'unresolved_matches':unresolved,'rejected_matches':rejected,'alignment_version':ALIGNMENT_VERSION,
                'stock_passes_changed':False}


def compare_article(board, text, *, article_date='',entity_annotations=None):
    evidence_board = getattr(board,'evidence_board',board)
    if evidence_board is None:  # empty/synthetic engine controls
        return {'schema':SCHEMA,'matches':[],'broad_matches':[],'stock_passes_changed':False}
    index = getattr(evidence_board,'keyword_index',None)
    if index is None:
        index = KeywordIndex(evidence_board.profiles)
        evidence_board.keyword_index = index
    result = index.compare(text,article_date=article_date,entity_annotations=entity_annotations)
    # Materialized witnesses also make look-ahead checks possible. A later filing
    # may be inspectable but cannot support a historical article's comparison.
    if article_date:
        kept = []
        for hit in result['matches']:
            profile = evidence_board.profiles[hit['stock']]
            pool = getattr(profile,'keyword_evidence',{})
            references = [ref for ref in hit['evidence_ids'] if any(w['source'].get('filing_date','9999') <= article_date
                for w in pool[ref]['witnesses'])]
            if references:
                kept.append({**hit,'evidence_ids':references})
        result['matches'] = kept
    return result
