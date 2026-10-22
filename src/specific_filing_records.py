"""Evidence-bound disclosure details, separate from generic relationship heads.

This layer recovers literal names, obligations and explicit scope from retained
paragraphs. It never supplies a missing litigant, regulator or legal instrument
from the company's industry. Named mentions without an issuer attachment remain
mentions; they are not promoted into established legal obligations.
"""
import hashlib
import json
import re
from collections import Counter
from pathlib import Path

VERSION = 'filing-specificity-1.3'
WORD = r"[A-Z][A-Za-z0-9]*(?:[-’'][A-Za-z0-9]+)*"
# An instrument ending cannot be swallowed as a title word and joined to the
# next law in a list. Lowercase connectors and commas are part of real names.
TITLE_WORD = r'(?!(?:Acts?|Directives?|Regulations?|Laws?|Statutes?|Ordinances?)\b)'+WORD
NUMBER = r'\d+[A-Za-z]*(?:[.,/-]\d+[A-Za-z]*)*(?:\([A-Za-z0-9]+\))*'
LAW = re.compile(r'\b\d+\s+(?:U\.?S\.?C\.?|C\.?F\.?R\.?)\s*(?:§{1,2}\s*|(?:sec|ss)\.\s*|Sections?\s+)?'+NUMBER+r'|\b(?:Act|Law|Ordinance|Decree|Order|Directive|Regulation|Resolution)\s+(?:No\.?|Number|#)\s*'+NUMBER+r'|\bTitle\s+\d+\s+of\s+the\s+United States Code\b|\b(?:Regulation|Directive)\s*\([A-Z]{2,5}\)\s*\d{4}/\d+\b|\b(?:Rules?\s+'+NUMBER+r'|Regulation\s+[A-Z]{1,3}(?:-[A-Z])?|(?:Local Law|Executive Order)\s+'+NUMBER+r')|\b(?:'+TITLE_WORD+r'(?:\s+|,\s*)(?:(?:and|of|the|for|on|in|to)\s+)?){1,24}(?:Act|Directive|Regulations?|Code|Law|Statute|Ordinance)(?:\s+of\s+\d{4}|\s+\d+)?\b')
AGENCY = re.compile(r'\b(?:(?:'+WORD+r'\s+){0,3})(?:Department|Ministry|Bureau|Office)\s+of\s+(?:the\s+)?'+WORD+r'(?:\s+(?:'+WORD+r'|and)){0,7}\b|\b(?:'+WORD+r'\s+(?:(?:and|of|the)\s+)?){1,8}(?:Commission|Authority|Department|Ministry|Agency|Administration|Bureau|Board|Service)\b')
PERMIT = re.compile(r'\b(?P<title>(?:'+WORD+r'\s+){0,6}(?i:Permit|Licen[sc]e|Authorization|Registration))\s+(?i:No\.?|Number|#)\s*:?\s*(?P<number>(?:[A-Z]{1,4}\s+)?[A-Z0-9]+(?:\s*[-/]\s*[A-Z0-9]+)*)\b')
GENERIC = re.compile(r'^(?:(?:the|a|an|our|their|certain|various|periodic|applicable|other|such|governmental|government)\s+)*(?:acts?|codes?|laws?|rules?|regulations?|boards?|authorit(?:y|ies)|commissions?|departments?|ministries|agenc(?:y|ies)|services?|administrations?|litigation(?: matters)?|claims?|examinations?|audits?|risks?|requirements?|approvals?|permits?|obligations?)$',re.I)
LEGAL = re.compile(r'\b(?:rules?|acts?|codes?|laws?|regulat\w*|directives?|statutes?|ordinances?|commission|authority|department|ministry|bureau|litigat\w*|lawsuits?|claims?|examin\w*|audits?|permits?|licen[sc]\w*|authoriz\w*|quotas?|levies|obligations?)\b',re.I)
ISSUER = re.compile(r"\b(?:we|us|our|the company|the registrant)\b",re.I)
EXCLUSION = re.compile(r'\b(?:we|the company|the registrant)\s+(?:are|is)\s+(?:explicitly\s+)?(?:not\s+(?:in\s+scope|subject|covered)|outside\s+(?:the\s+)?scope)|\b(?:does|do)\s+not\s+apply\s+to\s+(?:us|the company)',re.I)
POTENTIAL = re.compile(r'\b(?:may|might|could|if|proposed|proposal|considering)\b',re.I)
DIRECT = re.compile(r'\b(?:we|the company|the registrant)\s+(?:(?:are|is)\s+(?:subject\s+to|required\s+to|regulated\s+by|bound\s+by)|must|shall|compl(?:y|ies)\s+with)|\bour\s+(?:business|operations?|service)\s+(?:is|are)\s+subject\s+to\b',re.I)
REQUIREMENTS = re.compile(r'\b(?:content\s+(?:catalog\s+)?quotas?|local\s+content\s+quotas?|investment\s+obligations?|levies|data\s+localization|data\s+transfer|restrictions\s+on\s+data\s+export|local\s+ownership\s+requirements?|ownership\s+rights|net\s+neutrality|(?:broadcasting|operating|environmental|export)\s+(?:permits?|licen[sc]es?)|distribution\s+rights|public\s+performance\s+of\s+music)\b',re.I)
_ROOT = Path(__file__).resolve().parent
_PLACE_INPUTS = ('specific_filing_records.py','data/territories.json','places.py')


def source_fingerprint():
    digest=hashlib.sha256()
    for name in _PLACE_INPUTS:digest.update(name.encode()+b'\0'+(_ROOT/name).read_bytes()+b'\0')
    return digest.hexdigest()


def _jurisdiction_pattern():
    # The repository's pinned CLDR country/territory resource applies to every
    # issuer. Country codes such as IN are not guessed from ordinary words.
    from .places import STATES
    names=json.loads((_ROOT/'data/territories.json').read_text())['main']['en']['localeDisplayNames']['territories']
    values={v for k,v in names.items() if len(k.split('-')[0])==2 and len(v)>2}
    values.update(name.title() for name in STATES)
    values.update(('EU','UK','U.S.','U.K.','United States','United Kingdom'))
    return re.compile(r'(?<!\w)(?:'+'|'.join(re.escape(v) for v in sorted(values,key=lambda s:(-len(s),s)))+r')(?!\w)')


PLACES = _jurisdiction_pattern()
LEGAL_ATTACHMENT = re.compile(r'\b(?:under|pursuant\s+to|subject\s+to|comply\s+with|compliance\s+with|governed\s+by|defined\s+(?:in|by)|within\s+the\s+meaning\s+of|amendments?\s+to|(?:Sections?|Articles?)\s+[\d()\s,and.-]+\s+of)\s+(?:the\s+)?$',re.I)


def _instrument_candidate(mention, sentence, prior=None):
    """Use title structure and local legal syntax, never an issuer/name list."""
    value=mention['text']
    if re.match(r'\d|Title\s+\d|(?:Rules?|Law|Act|Ordinance|Decree|Order|Directive|Regulation|Resolution)\s+(?:No\.?\s*)?\d',value):return True
    words=re.findall(r'[A-Za-z]+',value.casefold())
    if all(w in {'general','governmental','applicable','other','such','laws','law','rules','rule','regulations','regulation','and','of','the'} for w in words):return False
    if re.search(r'\bto\s+Act$',value):return False  # infinitive, not an Act title
    if re.search(r'\b(?:Code|Law)$',value):
        # These two heads also occur in addresses, headings and legal classes.
        # Require both a distinctive multiword title (or explicit place) and
        # a local statutory construction. Merely capitalizing a field fails.
        named=[w for w in words[:-1] if w not in {'of','the','and','on','for','in','to'}]
        if len(named)<2 and not PLACES.search(value):return False
        attached=LEGAL_ATTACHMENT.search(sentence[max(0,mention['start']-160):mention['start']])
        coordinated=prior and re.fullmatch(r'\s*,?\s*(?:and|or)\s+(?:the\s+)?',sentence[prior['end']:mention['start']],re.I)
        legislative_action=re.match(r'\s+(?:establishes|requires|governs|regulates|provides|prohibits|amends)\b',sentence[mention['end']:])
        if not (attached or coordinated or legislative_action):return False
    return True


def _sentence_spans(text):
    # Do not split abbreviations, decimal quantities or legal section numbers.
    start = 0
    for match in re.finditer(r'(?<=[.!?])\s+(?=[A-Z•])',text):
        before=text[max(start,match.start()-12):match.start()]
        if re.search(r'\b(?:U\.S\.|U\.K\.|U\.S\.C\.|Inc\.|Corp\.|Ltd\.|No\.|Co\.)$',before):continue
        yield start,match.start()
        start=match.end()
    if start<len(text):yield start,len(text)


def _trim(match):
    text=match.group()
    prefix=re.match(r'(?:(?:The|Under|Our|We|For|Such|In|Any)\s+)+',text)
    trim=prefix.end() if prefix else 0
    # A determiner after a comma starts a new enumerated name. For example,
    # "GDPR, the California Consumer Privacy Act" contains two instruments,
    # not one long title. Commas inside titles such as CERCLA remain intact.
    boundaries=list(re.finditer(r',\s+(?:the|a|an)\s+',text[trim:]))
    if boundaries:trim+=boundaries[-1].end()
    value=text[trim:];end=match.end()
    while value.endswith(')') and value.count(')')>value.count('('):value=value[:-1];end-=1
    while value.endswith(('.',',')):value=value[:-1];end-=1
    return match.start()+trim,end,value


def _names(text,pattern):
    result=[]
    for m in pattern.finditer(text):
        a,b,value=_trim(m)
        if len(value.split())<2 or GENERIC.fullmatch(value):continue
        if pattern is AGENCY:
            split=next((s for s in re.finditer(r'\s+and\s+',value)
                if AGENCY.fullmatch(value[:s.start()]) and AGENCY.fullmatch(value[s.end():])),None)
            if split:
                result.extend((dict(start=a,end=a+split.start(),text=value[:split.start()]),
                    dict(start=a+split.end(),end=b,text=value[split.end():])))
                continue
        result.append(dict(start=a,end=b,text=value))
    return result


def _law_groups(text):
    groups=[]
    for mention in _names(text,LAW):
        if not _instrument_candidate(mention,text,groups[-1] if groups else None):continue
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
        if not short:short=re.match(r'\s*,?\s+or\s+([A-Z][A-Z0-9-]{1,12})(?=\s*[,;.)]|\s+and\b|$)',tail)
        if short:
            begin=mention['end']+short.start(1);end=mention['end']+short.end(1)
            group['aliases'].append({'text':short.group(1),'start':begin,'end':end})
    # A shared numbered head applies to coordinated provisions, preserving the
    # complete subparagraph identifier and each literal reference span.
    extra=[]
    for group in groups:
        head=re.match(r'(Rules?)\s+',group['text'])
        if not head:continue
        offset=group['end']
        while True:
            next_number=re.match(r'\s*(?:,\s*(?:(?:and|or)\s+)?|(?:and|or)\s+)('+NUMBER+r')',text[offset:])
            if not next_number:break
            a=offset+next_number.start(1);b=offset+next_number.end(1)
            extra.append({'text':'Rule '+next_number.group(1),'start':a,'end':b,
                'aliases':[{'text':next_number.group(1),'start':a,'end':b}]})
            offset=b
    groups.extend(extra)
    return groups


def _declared_names(graph):
    """Collect only explicit, same-document definitions before resolving uses.

    No global acronym dictionary or cross-filing alias guess is permitted.
    Conflicting declarations within one source remain unresolved.
    """
    declarations={};sources={s['id']:s for s in graph['sources']}
    for p in graph['paragraphs']:
        if p.get('kind')=='machine_data' or not LEGAL.search(p['text']):continue
        for kind,names in (('law',_law_groups(p['text'])),('authority',_names(p['text'],AGENCY))):
            for name in names:
                aliases=list(name.get('aliases',[]))
                tail=p['text'][name['end']:]
                offset=name['end']
                geography=re.match(r'\s*\(([^)]+)\)',tail)
                if geography and PLACES.fullmatch(geography.group(1)):
                    offset+=geography.end();tail=tail[geography.end():]
                short=re.match(r'\s*\(\s*(?:the\s+)?[“"\']?([A-Z0-9][A-Za-z0-9 ./-]{1,70}?)[”"\']?\s*\)',tail)
                if short and (re.search(r'[“"\']',short.group()) or re.fullmatch(r'[A-Z][A-Z0-9.-]{1,12}|[12]\d{3}\s+Act',short.group(1))):
                    aliases.append({'text':short.group(1),'start':offset+short.start(1),'end':offset+short.end(1)})
                prefix=p['text'][:name['start']]
                reverse=re.search(r'[“"\']([A-Z0-9][A-Za-z0-9 ./-]{1,70})[”"\']\s+(?:means|refers\s+to)\s+(?:the\s+)?$',prefix)
                if reverse:aliases.append({'text':reverse.group(1),'start':reverse.start(1),'end':reverse.end(1)})
                for alias in aliases:
                    if alias['text']==name['text'] or GENERIC.fullmatch(alias['text']):continue
                    key=(_document_key(sources[p['source_id']]),alias['text'])
                    definition={'kind':kind,'entity':name['text'],'paragraph_id':p['id'],'source_id':p['source_id'],
                        'name':_field(p['text'],name['start'],name['end'],p),
                        'alias':_field(p['text'],alias['start'],alias['end'],p)}
                    declarations.setdefault(key,{})[(kind,name['text'])]=definition
    return declarations


def _document_key(source):
    # A 10-K's Item 1 and Item 8 are different archived sections of the same
    # dated document. An alias may cross those sections, never another filing.
    if all(source.get(k) for k in ('accession','document','filing_date')):
        return (source['accession'],source['document'],source['filing_date'])
    return ('section',source['id'])


def _owner_named(sentence,owner):
    words=re.findall(r'[a-z0-9]+',owner.casefold())
    suffixes={'inc','incorporated','corp','corporation','co','company','ltd','limited','plc','llc'}
    while words and words[-1] in suffixes:words.pop()
    if not words:return False
    observed=re.findall(r'[a-z0-9]+',sentence.casefold())
    return any(observed[i:i+len(words)]==words for i in range(len(observed)-len(words)+1))


def _references(sentence,source_id,declarations,occupied):
    for (source,alias),definitions in declarations.items():
        if source!=source_id or len(definitions)!=1:continue
        definition=next(iter(definitions.values()))
        for m in re.finditer(r'(?<!\w)'+re.escape(alias)+r'(?!\w)',sentence):
            if any(a<m.end() and b>m.start() for a,b in occupied):continue
            yield {**definition,'start':m.start(),'end':m.end(),'text':alias}


def _field(text,start,end,paragraph):
    return {'text':text[start:end],'start':paragraph['start']+start,'end':paragraph['start']+end}


def _scope(sentence, *, kind, owner):
    if re.search(r'\bindicate\s+by\s+check\s*mark\b',sentence,re.I):return 'mention_only'
    if re.search(r'\b(?:we (?:are|have been) (?:a |an )?(?:public|independent|registered) accounting firm|our responsibility is to express|we (?:have )?audited)\b',sentence,re.I):return 'third_party_context'
    if EXCLUSION.search(sentence):return 'explicitly_out_of_scope'
    if not ISSUER.search(sentence) and not _owner_named(sentence,owner):
        return 'issuer_risk_context' if kind=='risk' else 'mention_only'
    # Another actor's obligations must not attach to the issuer merely because
    # the same sentence contrasts them with the issuer's business.
    if re.search(r'\b(?:our customers|our suppliers|our partners|other companies|competitors)\b.{0,60}\b(?:subject to|must comply|required to)',sentence,re.I):
        return 'third_party_context'
    direct=DIRECT.search(sentence)
    if direct:
        return 'potential_or_conditional' if POTENTIAL.search(sentence[:direct.start()]) else 'issuer_disclosure'
    if re.search(r'\brequired\s+to\s+comply\s+with\b',sentence,re.I):
        return 'potential_or_conditional' if POTENTIAL.search(sentence) else 'issuer_disclosure'
    if POTENTIAL.search(sentence):return 'potential_or_conditional'
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
    declarations=_declared_names(graph)
    reference_patterns={}
    for source_id in sources:
        values=[alias for (source,alias),definitions in declarations.items() if source==_document_key(sources[source_id]) and len(definitions)==1]
        if values:reference_patterns[source_id]=re.compile(r'(?<!\w)(?:'+'|'.join(re.escape(a) for a in sorted(values,key=len,reverse=True))+r')(?!\w)')
    records=[];seen=set()
    def emit(p, category, entity, local_start, local_end, sa, sb, *, relation,
             aliases=(), fields=None, gaps=(), applicability=None, identity_type=None,definition=None):
        text=p['text'];sentence=text[sa:sb];source=sources[p['source_id']]
        scope=applicability or _scope(sentence,kind=source.get('kind'),owner=graph.get('name',''))
        key=(p['id'],category,entity,local_start,scope)
        if key in seen:return
        seen.add(key)
        ident=hashlib.sha256(repr(key).encode()).hexdigest()
        anchor=_field(text,local_start,local_end,p)
        if definition:aliases=(*aliases,{'text':anchor['text'],'start':local_start,'end':local_end})
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
        if definition:record['definition_evidence']={k:definition[k] for k in ('source_id','paragraph_id','name','alias')}
        records.append(record)
    for p in graph['paragraphs']:
        text=p['text']
        reference_pattern=reference_patterns.get(p['source_id'])
        if p.get('kind')=='machine_data' or not (LEGAL.search(text) or (reference_pattern and reference_pattern.search(text))):continue
        for sa,sb in _sentence_spans(text):
            sentence=text[sa:sb]
            if not (LEGAL.search(sentence) or (reference_pattern and reference_pattern.search(sentence))):continue
            laws=_law_groups(sentence)
            for law in laws:
                declared=declarations.get((_document_key(sources[p['source_id']]),law['text']),{})
                if len(declared)==1:
                    definition=next(iter(declared.values()))
                    if definition['kind']=='law' and definition['entity']!=law['text']:
                        law.update(text=definition['entity'],definition=definition)
            occupied=[(r['aliases'][0]['start'],r['aliases'][0]['end']) for r in laws]
            references=list(_references(sentence,_document_key(sources[p['source_id']]),declarations,occupied))
            laws.extend({**r,'text':r['entity'],'aliases':[{'text':sentence[r['start']:r['end']],'start':r['start'],'end':r['end']}],'definition':r} for r in references if r['kind']=='law')
            for law in laws:
                start,end=sa+law['start'],sa+law['end']
                aliases=[{**a,'start':sa+a['start'],'end':sa+a['end']} for a in law['aliases']]
                # Jurisdiction cues belong to this instrument, not every place
                # named elsewhere in a mixed-jurisdiction paragraph.
                location_text=law['text']
                suffix=text[end:sb]
                attached=re.match(r'\s+(?:in|of|across|within)\s+(?:the\s+)?',suffix)
                if attached:
                    place=PLACES.match(suffix,attached.end())
                    if place:location_text+=' '+place.group()
                legislative_actor=re.search(r'('+PLACES.pattern+r')\s+(?:revised|adopted|enacted|introduced)\s+(?:its|the)\s*$',text[sa:start])
                if legislative_actor:location_text+=' '+legislative_actor.group(1)
                places=[m.group() for m in PLACES.finditer(location_text)]
                fields={'jurisdictions':list(dict.fromkeys(places)),
                        'provisions':[m.group() for m in re.finditer(r'\b(?:Section|Article|Rule)\s+[0-9][A-Za-z0-9().-]*(?:\([a-z0-9]+\))*',sentence)],
                        'reported_requirements':list(dict.fromkeys([m.group() for m in REQUIREMENTS.finditer(sentence)]+[m.group(1).strip() for m in re.finditer(r'\brequire(?:s)?\s+that\s+(.+?)(?=,\s+and\s+to\s+require|[.;]|$)',sentence,re.I)]+[m.group().strip() for m in re.finditer(r'\b(?:require\s+prominence|must|shall|(?:is|are)\s+required\s+to)\s+.+?(?=;|[.!?](?:\s|$)|$)',sentence,re.I)])),
                        'reported_quantities':[m.group() for m in re.finditer(r'\b\d+(?:\.\d+)?\s*(?:%|percent\b)',sentence)]}
                if len({r['text'] for r in laws})>1:
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
                anchor=law['aliases'][0]
                emit(p,'law',law['text'],sa+anchor['start'],sa+anchor['end'],sa,sb,relation='disclosed_legal_instrument',
                     aliases=aliases,fields=fields,gaps=gaps,
                     applicability=_instrument_scope(sentence,law,laws,kind=sources[p['source_id']].get('kind'),owner=graph.get('name','')),definition=law.get('definition'))
            authorities=_names(sentence,AGENCY)+[{**r,'text':r['entity'],'definition':r} for r in references if r['kind']=='authority']
            prior_authority=None
            for authority in authorities:
                before=sentence[:authority['start']]
                follows=sentence[authority['end']:]
                linked=re.search(r'\b(?:regulated|licensed|authorized|approved|overseen|administered|adopted|issued|promulgated|enforced|regulation|oversight|determinations?)\s+by\s+(?:the\s+)?$|\b(?:registered|filed)\s+with\s+(?:the\s+)?$|\b(?:approval|requirements?|rules|regulations)\s+(?:from|of)\s+(?:the\s+)?$',before,re.I)
                coordinated=prior_authority and re.fullmatch(r'\s*,?\s*(?:and|or)\s+(?:the\s+)?',sentence[prior_authority['end']:authority['start']],re.I)
                if not linked and not coordinated and not re.match(r'\s+(?:(?:may|can|will|shall)\s+)?(?:require|regulate|license|approve|enforce|examine)\b',follows,re.I):continue
                prior_authority=authority
                emit(p,'authority',authority['text'],sa+authority['start'],sa+authority['end'],sa,sb,
                     relation='disclosed_regulatory_authority',
                     fields={'jurisdictions':[]},
                     gaps=[] if laws else ['governing_instrument_not_named_in_this_statement'],definition=authority.get('definition'))
            if re.search(r'\b(?:examin\w*|audit\w*)\b',sentence,re.I):
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
                         gaps=['named_authorities_only_others_unspecified'] if re.search(r'\b(?:state|local|foreign)\b.{0,35}\bauthorities\b',sentence,re.I) else [],definition=authority.get('definition'))
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
                holder_link=bool(re.search(r'\b(?:we|the company|the registrant)\s+(?:(?:must|shall|are required to)\s+)?(?:obtain|hold|maintain|renew|have obtained|have secured)\b',sentence,re.I))
                third_party=bool(re.search(r'\b(?:commissioned by|prepared by|our customers|our suppliers|our partners)\b',p['text'],re.I))
                permit_scope='third_party_context' if third_party else None if holder_link else 'mention_only'
                emit(p,'named_permit',permit.group(),sa+permit.start(),sa+permit.end(),sa,sb,
                     relation='requires_permit' if requires else 'disclosed_permit',
                     fields={'permit_identifier':permit.group('number'),'authorities':authorities,
                             'jurisdictions':list(dict.fromkeys(m.group() for m in PLACES.finditer(sentence)))},
                     gaps=([] if authorities else ['issuing_authority_not_named'])+([] if holder_link else ['issuer_permit_holder_not_established']),
                     applicability=permit_scope)
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
    return {'version':VERSION,'source_sha256':source_fingerprint(),'paragraph_scope':paragraph_scope,'paragraphs_examined':len(graph['paragraphs']),
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
        definition=record.get('definition_evidence')
        if definition:
            declared=paragraphs[definition['paragraph_id']]
            sources={s['id']:s for s in graph['sources']}
            if (declared['source_id']!=definition['source_id']
                    or _document_key(sources[declared['source_id']])!=_document_key(sources[p['source_id']])):
                raise ValueError('alias definition crosses source documents')
            for field in (definition['name'],definition['alias']):
                if declared['text'][field['start']-declared['start']:field['end']-declared['start']]!=field['text']:
                    raise ValueError('alias definition differs from source')
        if record['category']=='coverage_gap' and record['entity_match_eligible']:
            raise ValueError('unnamed details cannot become entity matches')
        if record['applicability'] in ('explicitly_out_of_scope','mention_only','third_party_context') and record['direct_obligation_asserted']:
            raise ValueError('unestablished or excluded obligation asserted')
    return len(extracted['records'])

