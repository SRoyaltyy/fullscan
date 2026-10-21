"""Shared typed identity gates. Similar words retrieve candidates, never identities.

Scores explain context support, not probabilities. Missing/contradictory evidence
abstains or rejects before any relevance or financial-impact decision.
"""
import hashlib
import json
import re
from .linguistics import lemma, STOP

VERSION = 'entity-alignment-1.0'
WORD = re.compile(r"[A-Za-z][A-Za-z0-9]*(?:['’][A-Za-z0-9]+)*")
SUFFIX = re.compile(r'(?:[, ]+(?:incorporated|corporation|company|limited|holdings|corp|inc|ltd|plc|llc)\.?)+$',re.I)
TYPES = {'person':'PERSON','PERSON':'PERSON','organization':'ORG','company':'ORG','ORG':'ORG',
         'PRODUCT':'PRODUCT','product':'PRODUCT','GPE':'GPE','geography':'GPE','location':'GPE',
         'agency':'GOV_AGENCY','GOV_AGENCY':'GOV_AGENCY','law':'LAW'}


def tokens(value, *, stop=True):
    words=[lemma(m.group()) for m in WORD.finditer(value or '')]
    return [w for w in words if len(w)>=3 and (not stop or w not in STOP)]
# The same declarative context rules are exported to the browser. They are
# sense evidence, not replacements for identity identifiers or source quotes.
POLICY = {
 'person':r'\b(?:CEO|CFO|COO|CTO|CISO|chief|officer|director|president|founder|chairman|chairwoman|Mr|Mrs|Ms|Dr)\b',
 'person_before':r'\b(?:CEO|CFO|COO|CTO|CISO|officer|director|president|founder|chairman|chairwoman|Mr|Mrs|Ms|Dr)\.?\s+$',
 'person_after':r'^\s*[,—–-]?\s*(?:(?:is|was|serves as)\s+)?(?:(?:our|the|its)\s+)?(?:CEO|CFO|COO|CTO|CISO|chief|officer|director|president|founder|chairman|chairwoman)\b',
 'company':r'\b(?:company|corporation|corp|inc|plc|shares|stock|earnings|revenue|restaurants?|franchis\w*|retailer|supplier|processor|payments?|acquisition|business)\b',
 'place':r'\b(?:river|valley|province|country|city|county|mountain|lake)\b',
 'concepts':[
  {'id':'cloud:computing','aliases':['cloud','cloud computing','cloud storage'],'positive':r'\b(?:computing|storage|servers?|datacenters?|data centers?|software|infrastructure|SaaS|AWS)\b','negative':r'\b(?:rain|weather|storm|sky|cloudy|forecast|thunder)\b'},
  {'id':'chip:semiconductor','aliases':['chip','semiconductor','semiconductor chip'],'positive':r'\b(?:semiconductor|silicon|wafer|foundry|processor|GPU|CPU|fabrication)\b','negative':r'\b(?:potato|snack|casino|poker|tortilla|crisps?)\b'},
  {'id':'margin:financial','aliases':['margin','gross margin','operating margin','EBITDA margin','Adjusted EBITDA margin'],'positive':r'\b(?:profit|gross|EBITDA|operating|earnings|revenue|percent)\b|%','negative':r'\b(?:page|paper|coast|continental|river|shore)\b'},
  {'id':'drive:storage','aliases':['hard drive','hard disk'],'positive':r'\b(?:hard[- ]drives?|hard[- ]disks?|storage|disk|server|data|suppl\w*)\b','negative':r'\b(?:golf|road|motor|journey|success)\b'},
 ]}


def surface(value):
    """No lemmatization/plural stripping for proper names."""
    return ' '.join(m.group().replace('’',"'").lower() for m in WORD.finditer(value or ''))


def aliases(value, kind):
    forms = {surface(value)}
    if kind in ('ORG','GOV_AGENCY'):
        forms.add(surface(SUFFIX.sub('',value).strip()))
        forms.update(f.replace("'",'') for f in list(forms))
    if kind == 'PERSON' and len(surface(value).split()) > 1:
        forms.add(surface(value).split()[-1])
    return sorted(f for f in forms if f)


def identity(row, profile, organizations, *, as_of=''):
    kind = TYPES.get(row.get('object_type'),'CONCEPT')
    role = row['relationship']
    if role in ('officer','person'):kind='PERSON'
    elif role in ('supplier','customer','competitor','partner','subsidiary','acquisition_target','divestiture_target','issuer'):kind='ORG'
    elif role in ('regulator','approval_authority'):kind='GOV_AGENCY'
    name = row['keyword']
    forms = aliases(name,kind)
    owner_aliases = aliases(profile.name,'ORG')
    pool = getattr(profile,'keyword_evidence',{})
    context = ' '.join(w['quote'] for ref in row.get('evidence_ids',[]) for w in pool.get(ref,{}).get('witnesses',[])
                       if not as_of or w['source'].get('filing_date','9999')<=as_of)
    if not context:
        context = str(row.get('source_context',''))
    normalized = row.get('normalized',' '.join(tokens(name,stop=False)))
    org_ids = {ticker for form in forms for ticker in organizations.get(form,())}
    canonical = 'org:'+next(iter(org_ids)) if kind=='ORG' and len(org_ids)==1 else kind.lower()+':'+row['stock']+':'+surface(name)
    sense = None
    options = []
    if kind == 'CONCEPT':
        for rule in POLICY['concepts']:
            if normalized in {' '.join(tokens(a,stop=False)) for a in rule['aliases']}:
                options.append({'id':rule['id'],'positive':rule['positive'],'negative':rule['negative']})
        if options:
            selected = [s for s in options if re.search(s['positive'],context,re.I) and not re.search(s['negative'],context,re.I)]
            if len(selected)==1:sense=selected[0]['id']
        else:
            from .semantic_lookup import load_senses
            dictionary,_ = load_senses()
            unique = {s[0]:s for s in dictionary.get(normalized,()) if s[3]=='n'}
            words = set(tokens(context))-set(normalized.split())
            ranked = sorted([(len(words & set(s[2])),s[0]) for s in unique.values()],reverse=True)
            options = [{'id':s[0],'terms':sorted(set(s[2])-set(normalized.split()))} for s in unique.values()]
            if len(options)==1:sense=options[0]['id']
            elif ranked and ranked[0][0]>=2 and (len(ranked)==1 or ranked[0][0]>ranked[1][0]):sense=ranked[0][1]
        canonical = 'concept:'+sense if sense else 'unresolved-concept:'+normalized
    return {'id':canonical,'type':kind,'name':name,'aliases':forms,'owner':row['stock'],
        'owner_aliases':owner_aliases,'registered_organization':kind=='ORG' and len(org_ids)==1,
        'sense':sense,'senses':options,'source_context_terms':sorted(set(tokens(context))-set(normalized.split())),
        'scope':'catalog' if kind=='ORG' and len(org_ids)==1 else 'filing profile'}


def local_context(text,start,end):
    # Sentence/clause boundaries keep an unrelated mention elsewhere in a
    # pasted article from supplying another mention's employer or sense.
    left = max([m.end() for m in re.finditer(r'[!?;\n]|\.(?=\s|$)',text[:start])] or [0])
    right = re.search(r'[!?;\n]|\.(?=\s|$)',text[end:])
    return text[max(left,start-180):min(end+(right.start() if right else len(text)),end+180)]


def decide(entity, text, start, end, *, peers=(), annotations=()):
    context = local_context(text,start,end)
    actual = text[start:end]
    raw = surface(actual)
    if raw.endswith("'s"):raw=raw[:-2]
    forms = entity['aliases']
    exact = raw in forms or surface(actual) in forms
    labels = {TYPES.get(a['label'],a['label']) for a in annotations if a['start']<=start and a['end']>=end}
    kind = entity['type']
    reasons=[]
    status='unresolved'
    context_score=len(set(tokens(context)) & set(entity['source_context_terms']))
    allowed = {'ORG':{'ORG'},'PERSON':{'PERSON'},'PRODUCT':{'PRODUCT'},'GPE':{'GPE','LOC'},'GOV_AGENCY':{'GOV_AGENCY','ORG'},'LAW':{'LAW'}}.get(kind,set())
    if labels and allowed and not labels & allowed:
        return {'status':'rejected','reasons':['article_entity_type_conflict'],
                'stock_entity_id':entity['id'],'article_entity_id':None,
                'source_type':kind,'article_types':sorted(labels),'context_score':context_score}
    owner = any(re.search(r'(?<![A-Za-z0-9])'+re.escape(a)+r'(?![A-Za-z0-9])',surface(context)) for a in entity['owner_aliases'])
    competitors = {p['id'] for p in peers if p['type']==kind and (not owner or p['owner']==entity['owner'])}
    if kind=='PERSON':
        prefix=text[max(0,start-70):start]
        previous=re.search(r'\b([A-Z][a-z]+)\s*$',prefix)
        attached_role=bool(re.search(POLICY['person_before'],prefix,re.I) or re.search(POLICY['person_after'],text[end:end+70],re.I))
        different_given_name=bool(len(raw.split())==1 and previous and previous.group(1).lower() not in set(entity['name'].lower().split()) | {'ceo','cfo','coo','cto','ciso','officer','director','president','founder','chairman','chairwoman','mr','mrs','ms','dr'})
        if not exact:
            reasons.append('proper_name_surface_differs')
            status='rejected'
        elif different_given_name:
            status='rejected';reasons.append('different_given_name')
        elif (not attached_role or surface(actual).endswith("'s")) and re.search(POLICY['company'],context,re.I) and len(raw.split())==1:
            status='rejected';reasons.append('company_usage_is_not_officer_identity')
        elif len(raw.split())==1 and not (owner and attached_role):
            reasons.append('surname_requires_employer_and_person_role')
        elif len(competitors)>1:
            reasons.append('multiple_person_identities_require_employer')
        else:
            status='resolved';reasons.append('person_name_and_affiliation_supported' if owner else 'unique_full_person_name')
    elif kind in ('ORG','PRODUCT','GPE','GOV_AGENCY','LAW'):
        if not exact:status='rejected';reasons.append('proper_name_surface_differs')
        elif kind=='ORG' and (re.search(POLICY['place'],context,re.I) or (re.search(POLICY['person'],context,re.I) and not re.search(POLICY['company'],context,re.I))) and 'ORG' not in labels:
            reasons.append('organization_usage_not_established')
        elif len(competitors)>1:
            reasons.append('alias_has_multiple_entity_identities')
        elif kind=='ORG' and not entity['registered_organization'] and not owner and not (labels & {'ORG'} and context_score>=1):
            reasons.append('unregistered_name_requires_stock_or_typed_context')
        elif kind in ('PRODUCT','GPE') and not owner and kind not in labels:
            reasons.append('type_or_owner_context_required')
        elif kind=='ORG' and raw in {'apple','amazon','orange','target','shell','gap','meta'} and not (labels & {'ORG'} or re.search(POLICY['company'],context,re.I)):
            reasons.append('common_name_requires_organization_context')
        else:status='resolved';reasons.append('typed_name_and_scope_supported')
    else:
        candidates=[]
        for s in entity['senses']:
            if 'positive' in s:
                if re.search(s['negative'],context,re.I):continue
                score=2 if re.search(s['positive'],context,re.I) else 0
            else:score=len((set(tokens(context))-set(tokens(actual))) & set(s['terms']))
            candidates.append((score,s['id']))
        candidates.sort(reverse=True)
        selected=None
        if len(candidates)==1 and ('positive' not in entity['senses'][0] or candidates[0][0]>=2):selected=candidates[0][1]
        elif candidates and candidates[0][0]>=2 and (len(candidates)==1 or candidates[0][0]>candidates[1][0]):selected=candidates[0][1]
        if entity['sense'] and selected==entity['sense']:
            status='resolved';reasons.append('same_contextual_concept_sense')
        elif selected and entity['sense']:
            status='rejected';reasons.append('different_concept_sense')
        else:reasons.append('concept_sense_not_established_on_both_sides')
    return {'status':status,'reasons':reasons,'stock_entity_id':entity['id'],
        'article_entity_id':entity['id'] if status=='resolved' else None,
        'source_type':kind,'article_types':sorted(labels),'context_score':context_score}


def validate_annotations(text, payload):
    if not payload:return []
    if payload.get('input_sha256')!=hashlib.sha256(text.encode()).hexdigest():
        raise ValueError('article entity annotations do not match the actual scored text')
    for span in payload.get('mentions',[]):
        if not (0<=span['start']<span['end']<=len(text)) or text[span['start']:span['end']]!=span['text']:
            raise ValueError('invalid article entity annotation span')
    return payload.get('mentions',[])


def person_fact_alignment(board, profile, fact, text, key):
    """Use the same person gate in the older stock-pass path as keyword search."""
    def descriptor(p,f):
        return identity({'stock':p.ticker,'keyword':f.text,'relationship':'officer','object_type':'PERSON',
            'source_context':f.passage},p,{})
    entity=descriptor(profile,fact)
    peers=[descriptor(board.profiles[ticker],board.profiles[ticker].facts[ix])
           for ticker,kind,ix in board.identities.get(key,()) if kind=='person']
    sequence=list(WORD.finditer(text))
    for alias in entity['aliases']:
        expected=[lemma(w) for w in alias.split()]
        for i in range(len(sequence)-len(expected)+1):
            found=sequence[i:i+len(expected)]
            if [lemma(m.group()) for m in found]==expected:
                if decide(entity,text,found[0].start(),found[-1].end(),peers=peers)['status']=='resolved':return True
    return False
