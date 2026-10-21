"""Evidence-bound disclosure details, separate from generic relationship heads.

This layer recovers literal names, obligations and explicit scope from retained
paragraphs. It never supplies a missing litigant, regulator or legal instrument
from the company's industry. Named mentions without an issuer attachment remain
mentions; they are not promoted into established legal obligations.
"""
import hashlib
import re
from collections import Counter

VERSION = 'filing-specificity-1.1'
WORD = r"[A-Z][A-Za-z0-9]*(?:[-’'][A-Za-z0-9]+)*"
# An instrument ending cannot be swallowed as a title word and joined to the
# next law in a list. Lowercase connectors and commas are part of real names.
TITLE_WORD = r'(?!(?:Acts?|Directives?|Regulations?|Laws?)\b)'+WORD
LAW = re.compile(r'\bRegulation\s*\([A-Z]{2,5}\)\s*\d{4}/\d+\b|\bDirective\s*\([A-Z]{2,5}\)\s*\d{4}/\d+\b|\b(?:'+TITLE_WORD+r'(?:\s+|,\s*)(?:(?:and|of|the|for|on|in|to)\s+)?){1,16}(?:Act|Directive|Regulations?|Code|Law)(?:\s+of\s+\d{4})?\b')
AGENCY = re.compile(r'\b(?:(?:'+WORD+r'\s+){0,3})(?:Department|Ministry|Bureau|Office)\s+of\s+(?:the\s+)?'+WORD+r'(?:\s+(?:'+WORD+r'|and)){0,7}\b|\b(?:'+WORD+r'\s+(?:(?:and|of|the)\s+)?){1,8}(?:Commission|Authority|Department|Ministry|Agency|Administration|Bureau|Board|Service)\b')
PERMIT = re.compile(r'\b(?P<title>(?:'+WORD+r'\s+){0,6}(?:Permit|Licen[sc]e|Authorization|Registration))\s+(?:No\.?|Number|#)\s*(?P<number>[A-Z0-9]+(?:[-/][A-Z0-9]+)*)\b')
GENERIC = re.compile(r'^(?:(?:the|a|an|our|their|certain|various|periodic|applicable|other|such|governmental|government)\s+)*(?:laws?|rules?|regulations?|litigation(?: matters)?|claims?|examinations?|audits?|risks?|requirements?|approvals?|permits?|obligations?)$',re.I)
LEGAL = re.compile(r'\b(?:acts?|codes?|laws?|regulat\w*|directives?|litigat\w*|lawsuits?|claims?|examin\w*|audits?|permits?|licen[sc]\w*|authoriz\w*|quotas?|levies|obligations?)\b',re.I)
ISSUER = re.compile(r"\b(?:we|us|our|the company|the registrant)\b",re.I)
EXCLUSION = re.compile(r'\b(?:we|the company|the registrant)\s+(?:are|is)\s+(?:explicitly\s+)?(?:not\s+(?:in\s+scope|subject|covered)|outside\s+(?:the\s+)?scope)|\b(?:does|do)\s+not\s+apply\s+to\s+(?:us|the company)',re.I)
POTENTIAL = re.compile(r'\b(?:may|might|could|if|proposed|proposal|considering)\b',re.I)
DIRECT = re.compile(r'\b(?:we|the company|the registrant)\s+(?:(?:are|is)\s+(?:subject\s+to|required\s+to|regulated\s+by|bound\s+by)|must|shall|compl(?:y|ies)\s+with)|\bour\s+(?:business|operations?|service)\s+(?:is|are)\s+subject\s+to\b',re.I)
REQUIREMENTS = re.compile(r'\b(?:content\s+(?:catalog\s+)?quotas?|local\s+content\s+quotas?|investment\s+obligations?|levies|data\s+localization|data\s+transfer|restrictions\s+on\s+data\s+export|local\s+ownership\s+requirements?|ownership\s+rights|net\s+neutrality|(?:broadcasting|operating|environmental|export)\s+(?:permits?|licen[sc]es?)|distribution\s+rights|public\s+performance\s+of\s+music)\b',re.I)
PLACES = re.compile(r'\b(?:European Union|EU|United States|U\.S\.|UK|United Kingdom|California|Brazil|Japan|Delaware|Canada|France|Germany|India|Australia)\b')


def _sentence_spans(text):
    # Do not split abbreviations, decimal quantities or legal section numbers.
    start = 0
    for match in re.finditer(r'(?<=[.!?])\s+(?=[A-Z•])',text):
        before=text[max(start,match.start()-12):match.start()]
        if re.search(r'\b(?:U\.S\.|Inc\.|Corp\.|Ltd\.|No\.|Co\.)$',before):continue
        yield start,match.start()
        start=match.end()
    if start<len(text):yield start,len(text)


def _trim(match):
    text=match.group()
    prefix=re.match(r'(?:(?:The|Under|Our|We|For|Such|In|Any)\s+)+',text)
    trim=prefix.end() if prefix else 0
    return match.start()+trim,match.end(),text[trim:]


def _names(text,pattern):
    return [dict(start=a,end=b,text=value) for m in pattern.finditer(text)
            for a,b,value in [_trim(m)] if len(value.split())>=2 and not GENERIC.fullmatch(value)]


def _law_groups(text):
    groups=[]
    for mention in _names(text,LAW):
        # A code and its explicitly stated alternative title are one identity.
        prior=groups[-1] if groups else None
        gap=text[prior['end']:mention['start']] if prior else ''
        if prior and re.fullmatch(r'\s*\(also known as (?:the )?',gap,re.I):
            prior['aliases'].append(mention);prior['end']=mention['end']
            group=prior
        else:
            group={**mention,'aliases':[mention]};groups.append(group)
        tail=text[mention['end']:mention['end']+60]
        # A slash between two law names is NOT an alias declaration.
        short=re.match(r'\s*(?:\(\s*[“"\']?|or\s+[“"\'])\s*([A-Z][A-Z0-9-]{1,12})[”"\']?\s*\)',tail)
        if short:
            begin=mention['end']+short.start(1);end=mention['end']+short.end(1)
            group['aliases'].append({'text':short.group(1),'start':begin,'end':end})
    return groups


def _field(text,start,end,paragraph):
    return {'text':text[start:end],'start':paragraph['start']+start,'end':paragraph['start']+end}


def _scope(sentence, *, kind, owner):
    if EXCLUSION.search(sentence):return 'explicitly_out_of_scope'
    if not ISSUER.search(sentence) and not (owner and owner.casefold() in sentence.casefold()):
        return 'issuer_risk_context' if kind=='risk' else 'mention_only'
    # Another actor's obligations must not attach to the issuer merely because
    # the same sentence contrasts them with the issuer's business.
    if re.search(r'\b(?:our customers|our suppliers|our partners|other companies|competitors)\b.{0,60}\b(?:subject to|must comply|required to)',sentence,re.I):
        return 'third_party_context'
    if POTENTIAL.search(sentence):return 'potential_or_conditional'
    if DIRECT.search(sentence):return 'issuer_disclosure'
    return 'issuer_context'


def _instrument_scope(sentence, law, laws, *, kind, owner):
    if len(laws)==1:return _scope(sentence,kind=kind,owner=owner)
    clauses=list(re.finditer(r';|\b(?:but(?!\s+not\s+limited\s+to)|whereas|while|although)\b',sentence,re.I))
    left=max([m.end() for m in clauses if m.end()<=law['start']]+[0])
    right=min([m.start() for m in clauses if m.start()>=law['end']]+[len(sentence)])
    clause=sentence[left:right]
    if re.search(r'\bnot\s+(?:(?:subject|applicable)\s+to\s+)?$',sentence[left:law['start']],re.I):
        return 'explicitly_out_of_scope'
    return _scope(clause,kind=kind,owner=owner)


def details(graph, *, paragraph_scope='complete_source_paragraphs'):
    sources={s['id']:s for s in graph['sources']}
    records=[];seen=set()
    def emit(p, category, entity, local_start, local_end, sa, sb, *, relation,
             aliases=(), fields=None, gaps=(), applicability=None, identity_type=None):
        text=p['text'];sentence=text[sa:sb];source=sources[p['source_id']]
        scope=applicability or _scope(sentence,kind=source.get('kind'),owner=graph.get('name',''))
        key=(p['id'],category,entity,local_start,scope)
        if key in seen:return
        seen.add(key)
        ident=hashlib.sha256(repr(key).encode()).hexdigest()
        anchor=_field(text,local_start,local_end,p)
        record={'id':ident,'stock':graph['ticker'],'category':category,'entity':entity,
                'entity_type':identity_type or category,'relation':relation,'applicability':scope,
                'identity_key':(identity_type or category)+':'+re.sub(r'[^a-z0-9]+','',entity.casefold()),
                'aliases':list(dict.fromkeys([entity,*[a['text'] for a in aliases]])),
                'alias_evidence':[_field(text,a['start'],a['end'],p) for a in aliases],
                'fields':fields or {},'missing_fields':list(gaps),
                'specificity':'named_entity' if category in ('law','authority','named_case','named_permit') else 'defined_requirement' if category=='operating_requirement' else 'unspecified',
                'entity_match_eligible':category in ('law','authority','named_case','named_permit') and scope not in ('mention_only','third_party_context'),
                'direct_obligation_asserted':(category=='law' or relation=='requires_permit') and scope=='issuer_disclosure',
                'rule':VERSION+':'+category,'paragraph_id':p['id'],'source_id':p['source_id'],
                'evidence':{'quote':sentence,'start':p['start']+sa,'end':p['start']+sb,'anchor':anchor}}
        records.append(record)
    for p in graph['paragraphs']:
        text=p['text']
        if p.get('kind')=='machine_data' or not LEGAL.search(text):continue
        for sa,sb in _sentence_spans(text):
            sentence=text[sa:sb]
            if not LEGAL.search(sentence):continue
            laws=_law_groups(sentence)
            for law in laws:
                start,end=sa+law['start'],sa+law['end']
                aliases=[{**a,'start':sa+a['start'],'end':sa+a['end']} for a in law['aliases']]
                # Jurisdiction cues belong to this instrument, not every place
                # named elsewhere in a mixed-jurisdiction paragraph.
                location_text=law['text']
                suffix=text[end:sb]
                attached=re.match(r'\s+(?:in|of|across|within)\s+(?:the\s+)?('+PLACES.pattern[2:-2]+r')',suffix)
                if attached:location_text+=' '+attached.group(1)
                legislative_actor=re.search(r'\b(EU|European Union|UK|United Kingdom)\s+(?:revised|adopted|enacted|introduced)\s+(?:its|the)\s*$',text[sa:start])
                if legislative_actor:location_text+=' '+legislative_actor.group(1)
                places=[m.group() for m in PLACES.finditer(location_text)]
                fields={'jurisdictions':list(dict.fromkeys(places)),
                        'provisions':[m.group() for m in re.finditer(r'\b(?:Section|Article|Rule)\s+[0-9][A-Za-z0-9().-]*(?:\([a-z0-9]+\))*',sentence)],
                        'reported_requirements':list(dict.fromkeys([m.group() for m in REQUIREMENTS.finditer(sentence)]+[m.group(1).strip() for m in re.finditer(r'\brequire(?:s)?\s+that\s+(.+?)(?=,\s+and\s+to\s+require|[.;]|$)',sentence,re.I)]+[m.group().strip() for m in re.finditer(r'\brequire\s+prominence\s+.+?(?=,\s+and|[.;]|$)',sentence,re.I)])),
                        'reported_quantities':[m.group() for m in re.finditer(r'\b\d+(?:\.\d+)?\s*(?:%|percent\b)',sentence)]}
                if len(laws)>1:
                    # Do not distribute one law's quota/Article to every law in
                    # a list. The unassigned sentence remains visible evidence.
                    fields.update(provisions=[],reported_requirements=[],reported_quantities=[])
                # Bind only an authority explicitly attached after this
                # instrument. A nearby regulator is not assigned to every law.
                authority_link=re.match(r'\s*(?:\([^)]{1,80}\)\s*)?(?:administered|enforced|implemented|overseen)\s+by\s+(?:the\s+)?',suffix,re.I)
                attached_authorities=[]
                if authority_link:
                    tail=suffix[authority_link.end():]
                    names=_names(tail,AGENCY)
                    if names and names[0]['start']==0:
                        attached_authorities=[names[0]['text']]
                fields['administering_authorities']=attached_authorities
                gaps=[] if attached_authorities else ['enforcing_authority_not_named_in_this_statement']
                if not places:gaps.append('jurisdiction_not_explicit_in_this_statement')
                emit(p,'law',law['text'],start,end,sa,sb,relation='disclosed_legal_instrument',
                     aliases=aliases,fields=fields,gaps=gaps,
                     applicability=_instrument_scope(sentence,law,laws,kind=sources[p['source_id']].get('kind'),owner=graph.get('name','')))
            for authority in _names(sentence,AGENCY):
                before=sentence[:authority['start']]
                if not re.search(r'\b(?:regulated|licensed|authorized|approved|overseen|administered|regulation|oversight)\s+by\s+(?:the\s+)?$|\bapproval\s+from\s+(?:the\s+)?$',before,re.I):continue
                emit(p,'authority',authority['text'],sa+authority['start'],sa+authority['end'],sa,sb,
                     relation='disclosed_regulatory_authority',
                     fields={'jurisdictions':[]},
                     gaps=[] if laws else ['governing_instrument_not_named_in_this_statement'])
            if re.search(r'\b(?:examin\w*|audit\w*)\b',sentence,re.I):
                authorities=_names(sentence,AGENCY)
                subject=re.search(r'\b(?:examination|audit)s?\s+of\s+(.+?)\s+by\s+',sentence,re.I)
                matter=subject.group(1).strip() if subject else None
                for authority in authorities:
                    before=sentence[:authority['start']]
                    if not re.search(r'\b(?:by|from)\s+(?:the\s+)?$',before,re.I):continue
                    fields={'subject_matter':matter,'jurisdictions':[]}
                    if matter:
                        fields['subject_matter_evidence']=_field(text,sa+subject.start(1),sa+subject.end(1),p)
                    emit(p,'authority',authority['text'],sa+authority['start'],sa+authority['end'],sa,sb,
                         relation='examined_by',fields=fields,
                         gaps=['named_authorities_only_others_unspecified'] if re.search(r'\b(?:state|local|foreign)\b.{0,35}\bauthorities\b',sentence,re.I) else [])
                if not authorities:
                    m=re.search(r'\b(?:examin\w*|audit\w*)\b',sentence,re.I)
                    emit(p,'coverage_gap',m.group(),sa+m.start(),sa+m.end(),sa,sb,
                         relation='examination_details_unresolved',gaps=['examining_authority_not_named','subject_matter_requires_review'])
            for permit in PERMIT.finditer(sentence):
                authorities=[]
                for authority in _names(sentence,AGENCY):
                    if re.search(r'\b(?:by|from)\s+(?:the\s+)?$',sentence[:authority['start']],re.I):
                        authorities.append(authority['text'])
                requires=bool(re.search(r'\b(?:must|shall|required to|need to)\s+(?:obtain|hold|maintain|renew)',sentence,re.I))
                emit(p,'named_permit',permit.group(),sa+permit.start(),sa+permit.end(),sa,sb,
                     relation='requires_permit' if requires else 'disclosed_permit',
                     fields={'permit_identifier':permit.group('number'),'authorities':authorities,
                             'jurisdictions':list(dict.fromkeys(m.group() for m in PLACES.finditer(sentence)))},
                     gaps=[] if authorities else ['issuing_authority_not_named'])
            if re.search(r'\b(?:litigation|lawsuits?|legal proceedings|claims?)\b',sentence,re.I) and ISSUER.search(sentence):
                party=re.search(r'\b(?:filed|brought)\s+(?:against\s+us\s+)?by\s+(?P<party>'+WORD+r'(?:\s+'+WORD+r'){0,8})(?=\s+(?:alleging|asserting|claiming|over)|[,;.])',sentence)
                if party:
                    emit(p,'named_case',party.group('party'),sa+party.start('party'),sa+party.end('party'),sa,sb,
                         relation='claim_brought_by',fields={'subject_matter':sentence[party.end():].strip(' ,;.')},gaps=['case_number_not_named'])
                elif re.search(r'\b(?:subject to litigation|litigation matters|litigation or claims|subject to claims)\b',sentence,re.I):
                    m=re.search(r'\b(?:litigation(?: matters)?|claims?)\b',sentence,re.I)
                    emit(p,'coverage_gap',m.group(),sa+m.start(),sa+m.end(),sa,sb,
                         relation='unnamed_litigation',gaps=['claimant_not_named','case_identifier_not_named'],
                         fields={'disclosed_subjects':[m.group() for m in re.finditer(r'\b(?:employee relations|business practices|patent infringement|copyright|employment|tax litigation|securities class actions)\b',sentence,re.I)]})
            for m in REQUIREMENTS.finditer(sentence):
                if not (ISSUER.search(sentence) or sources[p['source_id']].get('kind')=='risk'):continue
                emit(p,'operating_requirement',m.group(),sa+m.start(),sa+m.end(),sa,sb,
                     relation='disclosed_operating_requirement',
                     fields={'jurisdictions':list(dict.fromkeys(x.group() for x in PLACES.finditer(sentence)))},
                     gaps=[] if laws else ['governing_instrument_not_named_in_this_statement','enforcing_authority_not_named_in_this_statement'])
    return {'version':VERSION,'paragraph_scope':paragraph_scope,'paragraphs_examined':len(graph['paragraphs']),
            'records':records,'counts':dict(Counter(r['category'] for r in records)),
            'legal_coverage_complete':False,
            'limitations':['Literal disclosed details, not a complete legal applicability register.',
                          'Unspecified parties, instruments and authorities require additional primary sources.',
                          'A named law mention is not automatically a direct company obligation.']}


def specificity(fact):
    """Classify the old claim head without hiding or changing its evidence."""
    value=fact['object'].strip()
    if GENERIC.fullmatch(value):return 'background'
    if fact.get('object_type') in ('organization','agency','law','person') and len(value)>2:
        return 'named_candidate'
    return 'descriptive_candidate'


def validate_details(graph, extracted):
    paragraphs={p['id']:p for p in graph['paragraphs']}
    for record in extracted['records']:
        p=paragraphs[record['paragraph_id']]
        if p['source_id']!=record['source_id']:raise ValueError('detail source mismatch')
        ev=record['evidence']
        for field in [ev,ev['anchor'],*record['alias_evidence']]:
            value=field.get('text',field.get('quote'))
            if not p['start']<=field['start']<field['end']<=p['end'] or p['text'][field['start']-p['start']:field['end']-p['start']]!=value:
                raise ValueError('detail evidence does not match paragraph')
        matter=record['fields'].get('subject_matter_evidence')
        if matter and p['text'][matter['start']-p['start']:matter['end']-p['start']]!=matter['text']:
            raise ValueError('examination subject does not match paragraph')
        if record['category']=='coverage_gap' and record['entity_match_eligible']:
            raise ValueError('unnamed details cannot become entity matches')
        if record['applicability'] in ('explicitly_out_of_scope','mention_only','third_party_context') and record['direct_obligation_asserted']:
            raise ValueError('unestablished or excluded obligation asserted')
    return len(extracted['records'])
