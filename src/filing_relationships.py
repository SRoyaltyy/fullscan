"""Offline, syntax-based SEC relationship extraction.

The trained grammar model is local and pinned. Roles come from clauses, never
from embedding similarity or a company-specific supplier/competitor list.
All prose is scanned; bounded windows control memory, not source coverage.
"""
from __future__ import annotations

import hashlib
import re
from collections import Counter
from pathlib import Path
from .term_inventory import parse_terms, bind_window, relocate, validate_inventory
from .relationship_frames import assertion as frame_assertion, refine as refine_frame

VERSION = 'filing-relationships-1.2'
SPACY_VERSION = '3.8.11'
MODEL = 'en_core_web_sm'
MODEL_VERSION = '3.8.0'


def compiler_fingerprint():
    root = Path(__file__).resolve().parents[1]
    paths = ('src/filing_relationships.py', 'src/filing_evidence.py', 'src/term_inventory.py',
             'src/specific_filing_records.py', 'src/relationship_frames.py', 'src/stock_evidence_graph.py',
             'src/stock_document_context.py',
             'scripts/build_filing_profiles.py', 'requirements-filing-parser.txt')
    digest = hashlib.sha256()
    for name in paths:
        digest.update(name.encode()+b'\0'+(root/name).read_bytes()+b'\0')
    return digest.hexdigest()

TRIGGERS = re.compile(r'\b(?:suppl\w*|purchas\w*|procur\w*|sourc\w*|import\w*|export\w*|rely|relies|relied|depend\w*|manufactur\w*|produc\w*|provid\w*|offer\w*|develop\w*|market\w*|distribut\w*|sell\w*|sold|buy|buys|bought|compet\w*|partner\w*|collaborat\w*|licens\w*|regulat\w*|subject\w*|compl\w*|approv\w*|authoriz\w*|patent\w*|proprietary|differentiat\w*|tariff\w*|sanction\w*|inflation|interest\s+rates?|foreign\s+exchange|currenc\w*|export\s+controls?|privacy|litigat\w*|lawsuit\w*|shortage\w*|disrupt\w*|restrict\w*|facilit\w*|operat\w*|subsidiar\w*|acquir\w*|divest\w*|customer\w*|client\w*|raw\s+materials?|cost\w*|risk\w*|revenue\w*)\b', re.I)
MACRO = re.compile(r'\b(?:tariffs?|sanctions?|inflation|interest\s+rates?|foreign\s+exchange|exchange\s+rates?|export\s+controls?|trade\s+restrictions?|supply\s+chain\s+disruptions?|shortages?)\b', re.I)
LAW = re.compile(r'\b(?:[A-Z][\w-]*[ \t]+){0,10}(?:Act|Regulation|Code|Directive|Law)(?:[ \t]+of[ \t]+\d{4})?\b|\b(?:GDPR|CCPA|CPRA|HIPAA|SOX|ITAR|EAR|FCPA|REACH)\b')
AGENCIES = {
    'FDA': 'Food and Drug Administration', 'FTC': 'Federal Trade Commission',
    'SEC': 'Securities and Exchange Commission', 'FCC': 'Federal Communications Commission',
    'FAA': 'Federal Aviation Administration', 'EPA': 'Environmental Protection Agency',
    'IRS': 'Internal Revenue Service', 'CBP': 'U.S. Customs and Border Protection',
    'CFTC': 'Commodity Futures Trading Commission', 'FDIC': 'Federal Deposit Insurance Corporation',
    'FERC': 'Federal Energy Regulatory Commission', 'OSHA': 'Occupational Safety and Health Administration',
    'FINRA': 'Financial Industry Regulatory Authority',
}
ISSUER = {'we', 'us', 'our', 'ours', 'company', 'registrant', 'issuer'}
GENERIC = {'it', 'they', 'them', 'this', 'that', 'these', 'those', 'one', 'other', 'others'}


def load_parser():
    import spacy
    from spacy.language import Language
    spacy.util.fix_random_seed(0)
    if spacy.__version__ != SPACY_VERSION:
        raise ValueError('expected spaCy '+SPACY_VERSION)
    nlp = spacy.load(MODEL)
    if nlp.meta.get('version') != MODEL_VERSION:
        raise ValueError('expected '+MODEL+' '+MODEL_VERSION)
    nlp.max_length = 100_000
    # Filings contain headings, bullets and tables without sentence punctuation.
    # Otherwise the dependency parser can attach an entire press release to
    # "EXHIBIT", borrowing roles across unrelated archive paragraphs.
    if not Language.has_factory('filing_paragraph_boundaries'):
        @Language.component('filing_paragraph_boundaries')
        def filing_paragraph_boundaries(doc):
            last_end = 0
            for token in doc:
                if token.is_space:
                    continue
                if re.search(r'\n[^\S\n]*\n', doc.text[last_end:token.idx]):
                    token.is_sent_start = True
                last_end = token.idx+len(token.text)
            return doc
    nlp.add_pipe('filing_paragraph_boundaries', before='parser')
    ruler = nlp.add_pipe('entity_ruler', before='ner')
    patterns = []
    for short, full in AGENCIES.items():
        patterns.extend([{'label': 'GOV_AGENCY', 'pattern': short},
                         {'label': 'GOV_AGENCY', 'pattern': full}])
    ruler.add_patterns(patterns)
    return nlp


def windows(text, size=16000):
    """Yield a partition of the ENTIRE text; no excerpt or word-count filter."""
    start = 0
    while start < len(text):
        end = min(len(text), start + size)
        if end < len(text):
            # Prefer paragraph boundaries; a pathological long sentence is
            # still scanned and its split is reported in the source coverage.
            cut = text.rfind('\n\n', start + size // 2, end)
            if cut < 0:
                cut = max(text.rfind('. ', start + size // 2, end),
                          text.rfind('; ', start + size // 2, end))
            if cut >= 0:
                end = cut + 2
        yield start, text[start:end]
        start = end


def _phrase_indices(token):
    """A noun phrase, excluding relative clauses and unrelated predicates."""
    allowed = {'compound', 'amod', 'nummod', 'poss', 'det', 'flat', 'name', 'case', 'quantmod'}
    selected = {token.i}
    todo = [token]
    while todo:
        current = todo.pop()
        for child in current.children:
            if child.dep_ in allowed:
                selected.add(child.i)
                todo.append(child)
    return sorted(selected)


def _phrase(token):
    selected=_phrase_indices(token)
    # Preserve contractions/possessives and punctuation exactly as written.
    value=''.join(token.doc[i].text_with_ws for i in selected).strip()
    return re.sub(r'^(?:the|a|an|our|their)\s+', '',value,flags=re.I).strip(' ,;:')


def _argument_span(obj,offset):
    if hasattr(obj,'start_char'):
        return {'start':offset+obj.start_char,'end':offset+obj.end_char,'text':obj.text}
    if hasattr(obj,'dep_'):
        selected=_phrase_indices(obj)
        raw=obj.doc[selected[0]:selected[-1]+1]
        value=_phrase(obj)
        if value not in raw.text:return None
        start=raw.start_char+raw.text.index(value)
        return {'start':offset+start,'end':offset+start+len(value),'text':value}
    return None


def _objects(token):
    return [c for c in token.children if c.dep_ in ('dobj', 'obj', 'attr', 'oprd')]


def _preps(token, names):
    return [o for p in token.children if p.dep_ in ('prep', 'agent', 'dative') and p.lower_ in names
            for o in p.children if o.dep_ in ('pobj', 'obj')]


def _coordinated(token):
    yield token
    for other in token.conjuncts:
        if other.i != token.i:
            yield other


def _subjects(token):
    subjects = [c for c in token.children if c.dep_ in ('nsubj', 'nsubjpass', 'csubj')]
    if not subjects and token.dep_ == 'acl' and token.head.lemma_ in ('agreement','contract','commitment','plan','decision'):
        anchor=token.head
        while anchor.head!=anchor and anchor.pos_ not in ('VERB','AUX'):
            anchor=anchor.head
        if anchor.lemma_ in ('enter','sign','approve','announce','make'):
            return _subjects(anchor)
    if not subjects and token.dep_ in ('conj', 'xcomp', 'acomp', 'attr', 'oprd') and token.head != token:
        return _subjects(token.head)
    return subjects


def _issuer(token, name):
    value = _phrase(token).lower()
    definite_company = token.lower_ in ('company', 'registrant', 'issuer') and any(c.lower_ == 'the' or (c.dep_ == 'poss' and c.lower_ in ISSUER) for c in token.children)
    if token.lower_ in ('we', 'us', 'our', 'ours') or definite_company or (token.lemma_ in ('operation', 'business', 'organization') and _owned(token)):
        return True
    clean = re.sub(r'\b(?:incorporated|corporation|inc|corp|ltd|limited|plc|the|company)\b|[^a-z0-9 ]', ' ', name.lower())
    clean = ' '.join(clean.split())
    return bool(clean and (value == clean or value.startswith(clean+' ')))


def _owned(token):
    return any(c.dep_ == 'poss' and c.lower_ in ISSUER for c in token.children)


def _named_in(token):
    """Named entities strictly within the matched argument, not the sentence."""
    ids = {t.i for t in token.subtree}
    return [e for e in token.doc.ents if e.label_ in ('ORG', 'PRODUCT', 'PERSON', 'GPE', 'GOV_AGENCY')
            and all(t.i in ids for t in e)]


def _direct_named_arguments(anchor):
    """A list/apposition's own names, not names inside a different nested role."""
    for entity in _named_in(anchor):
        current = entity.root
        allowed = True
        while current != anchor:
            if current.head == current:
                allowed = False
                break
            current = current.head
            if current != anchor and (current.pos_ in ('VERB', 'AUX') or
                    current.lemma_ in ('trial', 'study', 'platform', 'customer', 'client',
                                      'supplier', 'partner', 'subsidiary', 'law', 'authority')):
                allowed = False
                break
        if allowed and ';' not in anchor.doc[min(anchor.i,entity.start):max(anchor.i+1,entity.end)].text:
            yield entity


def _status(token, sentence):
    return frame_assertion(token)[0]


def extract_doc(doc, *, ticker, name, source, offset=0):
    """Return typed clause claims, including uncertainty and exact source spans."""
    facts = []
    seen = set()
    def emit(predicate, obj, trigger, *, subject=None, value_type='concept', qualifiers=None, confidence='syntax'):
        value = obj.text if hasattr(obj, 'label_') else _phrase(obj) if hasattr(obj, 'dep_') else str(obj)
        value = value.strip()
        if value_type == 'concept':
            if value in AGENCIES or value in AGENCIES.values():
                value_type = 'agency'
            elif LAW.fullmatch(value):
                value_type = 'law'
        if not value or value.lower() in ISSUER | GENERIC or not re.search(r'[A-Za-z]', value):
            return
        sentence = trigger.sent
        status, assertion_scope = frame_assertion(trigger)
        predicate, value_type, frame = refine_frame(predicate, obj, trigger,
            objects=_objects(trigger), recipients=_preps(trigger, {'to', 'from'}),
            value_type=value_type, offset=offset)
        if frame['argument_negated']:
            status = 'negated'
        qualifiers = {**(qualifiers or {}), 'frame': frame, 'assertion_scope': assertion_scope}
        actor = subject or ticker
        signature = (predicate, actor, value, value_type, status, sentence.start_char)
        if signature in seen:
            return
        seen.add(signature)
        facts.append({'subject': actor, 'predicate': predicate, 'object': value,
                      'object_type': value_type, 'status': status, 'confidence': confidence,
                      'rule': VERSION+':'+predicate, 'qualifiers': qualifiers or {},
                      'evidence': {'quote': sentence.text, 'start': offset+sentence.start_char,
                                   'end': offset+sentence.end_char, 'source_id': source['id'],
                                   'argument':_argument_span(obj,offset),
                                   'trigger':{'start':offset+trigger.idx,'end':offset+trigger.idx+len(trigger.text),'text':trigger.text}}})

    for sent in doc.sents:
        for token in sent:
            word = token.lemma_.lower()
            subjects = _subjects(token)
            own = any(_issuer(s, name) for s in subjects)
            if word in ('regulate','subject','comply','approve','authorize','supply','provide','manufacture'):
                own = own or any(_owned(s) and s.lemma_ not in ('customer','client','supplier','partner','competitor') for s in subjects)
            objects = _objects(token)
            if own and word in ('purchase', 'buy', 'procure', 'source', 'import', 'obtain'):
                inputs = [o for obj in objects for o in _coordinated(obj)]
                providers = [o for obj in _preps(token, {'from'}) for o in _coordinated(obj)]
                for obj in inputs:
                    emit('uses_input', obj, token)
                for supplier in providers:
                    emit('buys_from', supplier, token, value_type='organization',
                         qualifiers={'inputs': [_phrase(o) for o in inputs]})
            if own and word in ('rely', 'depend'):
                for obj in _preps(token, {'on', 'upon'}):
                    argument = _phrase(obj)
                    emit('depends_on', obj, token,
                         qualifiers={'purpose': [_phrase(o) for o in _preps(token, {'for'})]})
                    for entity in _named_in(obj):
                        if entity.text != argument:
                            emit('depends_on', entity, token, value_type='organization',
                                 qualifiers={'dependency': argument})
            if own and word in ('manufacture', 'produce', 'offer', 'provide', 'develop', 'design', 'market', 'sell', 'commercialize', 'distribute', 'supply'):
                if any(s.dep_ == 'nsubjpass' for s in subjects):
                    for supplier in _preps(token, {'by'}):
                        emit('buys_from', supplier, token, value_type='organization',
                             qualifiers={'inputs': [_phrase(s) for s in subjects if s.lower_ not in ISSUER]})
                for obj in objects:
                    for item in _coordinated(obj):
                        emit('offers', item, token, qualifiers={'activity': word})
                for obj in _preps(token, {'to'}):
                    for customer in _coordinated(obj):
                        emit('sells_to', customer, token, value_type='customer',
                             qualifiers={'outputs': [_phrase(o) for o in objects]})
            if own and word in ('compete', 'competition'):
                for obj in _preps(token, {'with', 'against', 'from'}):
                    emit('competes_with', obj, token, value_type='organization')
                    for entity in _named_in(obj):
                        emit('competes_with', entity, token, value_type='organization')
            if own and word == 'face' and any(o.lemma_ == 'competition' for o in objects):
                for obj in objects:
                    for rival in _preps(obj, {'from', 'with'}):
                        emit('competes_with', rival, token, value_type='organization')
                        for entity in _named_in(rival):
                            emit('competes_with', entity, token, value_type='organization')
            if own and word in ('partner', 'collaborate', 'license'):
                for obj in _preps(token, {'with', 'from', 'to'}):
                    emit('partners_with' if word != 'license' else 'licenses', obj, token,
                         value_type='organization', qualifiers={'direction': obj.head.lower_})
            if own and word in ('subject', 'comply', 'regulate', 'approve', 'authorize'):
                for obj in _preps(token, {'to', 'with', 'by', 'under'}):
                    predicate = 'subject_to' if word in ('subject', 'comply') else 'regulated_by' if word == 'regulate' else 'approval_from'
                    emit(predicate, obj, token)
                    for entity in _named_in(obj):
                        emit(predicate, entity, token, value_type='agency' if entity.label_ == 'GOV_AGENCY' else 'organization')
                    for law in LAW.finditer(' '.join(t.text for t in obj.subtree)):
                        emit('subject_to', law.group(), token, value_type='law')
            # Named trials/platforms need not occur in an offer/partner clause.
            # Keep descriptive mentions distinct from sponsorship/commercial sale.
            if word in ('trial', 'study', 'platform') and _owned(token):
                for entity in _direct_named_arguments(token):
                    emit('mentions_product_platform' if word == 'platform' else 'mentions_clinical_trial',
                         entity, token, value_type='product' if word == 'platform' else 'clinical_trial')
            # Supplier/customer/competitor appositions: "Toshiba Corp., one of
            # our hard drive suppliers". The owner is part of the same noun tree.
            if word in ('supplier', 'customer', 'client', 'competitor', 'partner', 'subsidiary'):
                relation = {'supplier': 'buys_from', 'customer': 'sells_to', 'client': 'sells_to',
                            'competitor': 'competes_with', 'partner': 'partners_with',
                            'subsidiary': 'owns_subsidiary'}[word]
                if _owned(token) and token.dep_ in ('nsubj', 'nsubjpass') and token.head.lemma_ in ('include', 'be', 'comprise'):
                    for obj in _objects(token.head):
                        for entity in _coordinated(obj):
                            emit(relation, entity, token.head, value_type='organization',
                                 qualifiers={'description': _phrase(token)})
                if _owned(token):
                    # X, one of our suppliers, announced... is an apposition
                    # even though "supplier" is below the "one of" phrase.
                    for ancestor in token.ancestors:
                        if ancestor.dep_ == 'appos' and ancestor.head.pos_ in ('NOUN', 'PROPN'):
                            for entity in _named_in(ancestor.head):
                                if entity.label_ == 'ORG':
                                    emit(relation, entity, token, value_type='organization', qualifiers={'description': _phrase(token)})
                            break
                        if ancestor.dep_ in ('attr', 'acomp') and ancestor.head.lemma_ == 'be':
                            for subject in _subjects(ancestor.head):
                                for entity in _named_in(subject):
                                    if entity.label_ == 'ORG':
                                        emit(relation, entity, token, value_type='organization', qualifiers={'description': _phrase(token)})
                            break
                holder = token
                for _ in range(3):
                    if any(t.dep_ == 'poss' and t.lower_ in ISSUER for t in holder.subtree):
                        break
                    if holder.dep_ not in ('pobj', 'appos'):
                        break
                    holder = holder.head
                if any(t.dep_ == 'poss' and t.lower_ in ISSUER for t in holder.subtree):
                    anchor = holder.head if holder.dep_ == 'appos' else holder
                    # A whole sentence subtree would mix unrelated roles.
                    if anchor.pos_ not in ('VERB', 'AUX'):
                        candidates = [e for e in _direct_named_arguments(anchor) if not any(_issuer(t, name) for t in e)]
                        for entity in candidates:
                            emit(relation, entity, token, value_type='organization',
                                 qualifiers={'description': _phrase(token)})
            if own and word in ('operate', 'manufacture', 'locate', 'maintain'):
                for obj in _preps(token, {'in', 'at', 'throughout'}):
                    for entity in _named_in(obj):
                        if entity.label_ == 'GPE':
                            emit('operates_in', entity, token, value_type='geography')
            if own and word in ('acquire', 'divest', 'dispose'):
                for obj in objects:
                    emit('acquires' if word == 'acquire' else 'divests', obj, token, value_type='organization')
                    for entity in _direct_named_arguments(obj):
                        emit('acquires' if word == 'acquire' else 'divests', entity, token, value_type='organization')
        # Risk statements are kept as risk mentions, not invented causal links.
        # Section context permits issuer risks but never asserts an affected
        # product/country unless the grammatical extraction above supplies it.
        issuer_sentence = any(t.lower_ in ('we', 'our', 'us') for t in sent)
        if issuer_sentence or source.get('kind') == 'risk':
            for match in MACRO.finditer(sent.text):
                trigger = next((t for t in sent if t.idx <= sent.start_char+match.start() < t.idx+len(t)), sent.root)
                emit('risk_mentions', match.group(), trigger, confidence='explicit_risk_mention')
                if trigger.dep_ in ('nsubj', 'nsubjpass') and trigger.head.lemma_ in ('affect','increase','raise','reduce','disrupt','harm','impact'):
                    affected = [o for o in _objects(trigger.head) if _issuer(o, name) or _owned(o)]
                    if affected:
                        targets = _preps(trigger, {'on','against','for'})
                        emit('exposed_to', match.group(), trigger.head,
                             qualifiers={'targets': [_phrase(o) for o in targets],
                                         'geographies': sorted({e.text for o in targets for e in _named_in(o) if e.label_ == 'GPE'}),
                                         'impact': trigger.head.lemma_, 'affected': [_phrase(o) for o in affected]})
        if issuer_sentence and re.search(r'\b(?:patent|proprietary|differentiat|advantage|unlike)\w*\b', sent.text, re.I):
            # Preserve the actual claim; do not turn marketing language into
            # an independently established economic moat.
            emit('differentiation_claim', sent.text, sent.root, confidence='issuer_claim')
    return facts


# Topic matches describe what a paragraph discusses, never assert a relationship.
# Keep the exact matching spans so a reviewer can see why a label was assigned.
TOPICS = {key: re.compile(r'\b(?:'+pattern+r')\b', re.I) for key, pattern in {
    'products_services': r'products?|services?|software|platforms?|solutions?|offerings?|manufactur\w*|produc\w*',
    'supply_chain_inputs': r'suppli\w*|supply\s+chain|purchas\w*|procur\w*|raw\s+materials?|components?|inventory|inventories|outsourc\w*',
    'operating_dependencies': r'depend\w*|rely|relies|relied|reliance|critical|essential',
    'customers_demand': r'customers?|clients?|consumers?|demand|backlog|subscribers?|orders?',
    'competition': r'compet\w*|market\s+share|pricing\s+pressure|rivals?',
    'partners_licenses': r'partners?|partnerships?|collaborat\w*|licens\w*|joint\s+ventures?',
    'regulation_legal': r'regulat\w*|laws?|legal|compliance|comply|litigat\w*|lawsuits?|approvals?|permits?|GDPR|FDA|FTC|SEC|sanctions?|export\s+controls?',
    'macro_policy': r'tariffs?|sanctions?|inflation|interest\s+rates?|foreign\s+exchange|exchange\s+rates?|export\s+controls?|trade\s+restrictions?|recession|geopolit\w*',
    'financial_performance': r'revenue\w*|sales|costs?|expenses?|profits?|loss(?:es)?|earnings|margins?|income|impairment|EBITDA|cash\s+flows?',
    'liquidity_capital': r'liquidity|debt|borrow\w*|credit\s+facilit\w*|covenants?|dividends?|repurchas\w*|capital\s+expenditures?|cash\s+equivalents?|financing|securities|shares?|dilution',
    'ownership_transactions': r'subsidiar\w*|acqui\w*|merger\w*|divest\w*|dispos\w*|beneficial\s+owner\w*|equity\s+interest\w*',
    'people_governance': r'employees?|workforce|personnel|executives?|directors?|board\s+of|compensation|governance|audit\s+committee|internal\s+controls?',
    'geography_facilities': r'geograph\w*|countries|country|regions?|facilit\w*|factories|factory|warehouses?|properties|headquarters|locations?',
    'technology_ip': r'patents?|proprietary|intellectual\s+property|research|development|technolog\w*|trademarks?|copyright\w*',
    'risks_uncertainty': r'risks?|uncertain\w*|disrupt\w*|shortages?|cyber\w*|breach\w*|adverse\w*|materially|going\s+concern|default\w*',
    'accounting_tax': r'accounting|GAAP|IFRS|tax\w*|depreciation|amortization|fair\s+value|recognition|consolidated\s+statements?',
    'filing_administration': r'commission\s+file|registrant|form\s+(?:10-K|10-Q|8-K|20-F|6-K)|table\s+of\s+contents|signatures?|exhibit\s+index|check\s+mark',
    'forward_looking': r'forward[- ]looking|safe\s+harbor|actual\s+results\s+may|undue\s+reliance',
}.items()}
MATERIAL = re.compile(r'\b(?:material(?:ly)?|significant(?:ly)?|substantial(?:ly)?|sole[- ]source|single[- ]source|concentration|going\s+concern|default|bankruptcy|restatement|impairment|termination|shortage|breach)\b', re.I)
QUANTITY = re.compile(r'(?<!\w)(?:[$€£¥]\s*-?\d[\d,]*(?:\.\d+)?(?:\s*(?:billion|million|thousand|trillion))?|\d[\d,]*(?:\.\d+)?\s*(?:%|percent\b|billion\b|million\b|thousand\b))', re.I)


def paragraph_spans(raw):
    """Archive text blocks separated by line breaks; exact, lossless partition.

    The archive does not retain HTML DOM paragraph IDs. A nonempty text line is
    therefore a review paragraph; blank separators belong to its predecessor.
    Leading whitespace belongs to the first block. No length or count limit.
    """
    starts = [m.start() for m in re.finditer(r'(?m)^[^\S\r\n]*[^\s]', raw)]
    if not starts:
        return [(0, len(raw))] if raw else []
    starts[0] = 0
    return list(zip(starts, starts[1:]+[len(raw)]))


def classify_paragraph(text, *, source_id, start, end, machine=False):
    paragraph = {'id': hashlib.sha256(f'{source_id}:{start}:{end}'.encode()).hexdigest(),
                 'source_id': source_id, 'start': start, 'end': end, 'text': text,
                 'sha256': hashlib.sha256(text.encode()).hexdigest(), 'relationship_ids': []}
    clean = text.strip()
    if machine:
        kind = 'machine_data'
    elif not clean:
        kind = 'whitespace'
    elif len(clean) < 180 and (re.match(r'^(?:PART\s+[IVX]+|ITEM\s+\d+[A-Z]?)\b', clean, re.I)
                              or (len(clean.split()) <= 14 and clean.isupper())):
        kind = 'heading'
    elif clean and sum(c.isdigit() for c in clean)/len(clean) > .35:
        kind = 'numeric_or_table'
    else:
        kind = 'prose'
    matches = []
    if not machine:
        for label, rule in TOPICS.items():
            matches.extend({'label': label, 'quote': m.group(), 'start': start+m.start(),
                            'end': start+m.end(), 'rule': 'topic_keyword_v1'} for m in rule.finditer(text))
    labels = sorted({m['label'] for m in matches}) or [kind if kind != 'prose' else 'unresolved_topic']
    signals = [] if machine else [
        {'kind': 'reported_quantity', 'quote': m.group(), 'start': start+m.start(), 'end': start+m.end()}
        for m in QUANTITY.finditer(text)]
    material = [] if machine else [m.group() for m in MATERIAL.finditer(text)]
    reasons = []
    if material:
        reasons.append('Materiality or disruption language: '+', '.join(sorted(set(material))))
    if signals:
        reasons.append('Contains reported quantities; units and economic meaning require context')
    priority = 'high' if material else 'review' if 'unresolved_topic' in labels else 'normal'
    if kind in ('machine_data', 'numeric_or_table', 'whitespace', 'heading'):
        priority = 'review' if kind in ('machine_data', 'numeric_or_table') else 'administrative'
        reasons.append('Structural classification: '+kind)
    if not reasons:
        reasons.append('Topic rules matched' if matches else 'No topic rule matched; human review required')
    paragraph.update(kind=kind, labels=labels, classification_evidence=matches, signals=signals,
                     importance={'priority': priority, 'reasons': reasons, 'method': 'review_priority_rules_v1'},
                     analysis='machine_inventory' if machine else 'full_prose_dependency_parse',
                     extraction_status='non_narrative' if kind in ('machine_data', 'whitespace', 'heading')
                     else 'no_explicit_relationship' if matches else 'unresolved',
                     review_required=kind in ('machine_data', 'numeric_or_table') or not matches)
    return paragraph


def validate_paragraph_coverage(graph, texts=None):
    """Fail closed on missing text, labels, source spans or relationship links."""
    from collections import defaultdict
    by_source = defaultdict(list)
    facts = {f['id']: f for f in graph['facts']}
    ids = set()
    for paragraph in graph.get('paragraphs', []):
        if paragraph['id'] in ids:
            raise ValueError('duplicate paragraph id')
        ids.add(paragraph['id'])
        by_source[paragraph['source_id']].append(paragraph)
    sources = {s['id']: s for s in graph['sources']}
    if set(by_source)-set(sources):
        raise ValueError('paragraph has unknown source')
    reconstructed = {}
    for source_id, source in sources.items():
        cursor, pieces = 0, []
        for p in sorted(by_source[source_id], key=lambda p: p['start']):
            if p['start'] != cursor or p['end'] <= cursor or p['end']-p['start'] != len(p['text']):
                raise ValueError('paragraph coverage gap, overlap or invalid span')
            if hashlib.sha256(p['text'].encode()).hexdigest() != p['sha256']:
                raise ValueError('paragraph checksum mismatch')
            if not p['labels'] or not p['importance']['reasons'] or not p['extraction_status']:
                raise ValueError('paragraph classification is missing')
            for item in p['classification_evidence']+p['signals']:
                if not (p['start'] <= item['start'] < item['end'] <= p['end']) or p['text'][item['start']-p['start']:item['end']-p['start']] != item['quote']:
                    raise ValueError('paragraph classification witness mismatch')
            for fact_id in p['relationship_ids']:
                if fact_id not in facts or not any(e['source_id'] == source_id and e['start'] < p['end'] and e['end'] > p['start'] for e in facts[fact_id]['evidence']):
                    raise ValueError('paragraph relationship link mismatch')
            pieces.append(p['text'])
            cursor = p['end']
        text = ''.join(pieces)
        reconstructed[source_id] = text
        if cursor != source['characters'] or hashlib.sha256(text.encode()).hexdigest() != source['sha256']:
            raise ValueError('paragraphs do not reconstruct the full source')
        if texts is not None and text != texts[source_id]:
            raise ValueError('paragraphs do not match verified archive text')
    # Every fact span must be reachable from each overlapping paragraph.
    for fact_id, fact in facts.items():
        for ev in fact['evidence']:
            if ev['source_id'] not in reconstructed or not (0 <= ev['start'] < ev['end'] <= len(reconstructed[ev['source_id']])) or reconstructed[ev['source_id']][ev['start']:ev['end']] != ev['quote']:
                raise ValueError('relationship quote does not match retained paragraphs')
            covered = [p for p in by_source[ev['source_id']] if ev['start'] < p['end'] and ev['end'] > p['start']]
            if not covered or any(fact_id not in p['relationship_ids'] for p in covered):
                raise ValueError('relationship is missing from paragraph review')
    coverage = graph['coverage']
    if coverage.get('characters_read') != sum(s['characters'] for s in sources.values()) or coverage.get('source_sections') != len(sources):
        raise ValueError('paragraph source inventory is incomplete')
    if not coverage.get('all_paragraphs_classified') or coverage.get('paragraphs') != len(ids):
        raise ValueError('paragraph coverage is incomplete')
    prose = sum(s['characters'] for s in sources.values() if s.get('kind') != 'machine_data')
    if not coverage.get('all_prose_dependency_analyzed') or coverage.get('prose_characters_analyzed', 0) != prose:
        raise ValueError('paragraph prose analysis is incomplete')
    if coverage.get('all_term_candidates_retained') and 'term_inventory' not in graph:
        raise ValueError('term inventory is missing')
    validate_inventory(graph)
    return len(ids)


def extract_sections(sections, *, ticker, name, nlp):
    from bisect import bisect_right
    sources, paragraphs, facts, stats = [], [], {}, Counter()
    linguistic_windows,term_rows=[],[]
    parse_cache = {}
    for section in sections:
        raw = str(section.get('text') or '')
        metadata = {k: section.get(k, '') for k in ('source', 'form', 'filing_date', 'accepted_at', 'report_date', 'accession', 'section', 'document', 'document_type', 'document_description', 'kind', 'url')}
        metadata['sha256'] = hashlib.sha256(raw.encode()).hexdigest()
        metadata['characters'] = len(raw)
        metadata['section_ordinal'] = len(sources)
        metadata['id'] = hashlib.sha256(repr(sorted(metadata.items())).encode()).hexdigest()
        sources.append(metadata)
        stats['source_sections'] += 1
        stats['characters_read'] += len(raw)
        # XML/JSON source lines are not narrative paragraphs. Keep each whole
        # machine section as one classified inventory block, with its full text.
        spans = [(0, len(raw))] if raw and section.get('kind') == 'machine_data' else paragraph_spans(raw)
        source_paragraphs = [classify_paragraph(raw[start:end], source_id=metadata['id'],
            start=start, end=end, machine=section.get('kind') == 'machine_data')
            for start, end in spans]
        paragraphs.extend(source_paragraphs)
        for p in source_paragraphs:p['term_ids']=[]
        starts = [p['start'] for p in source_paragraphs]
        if section.get('kind') == 'machine_data':
            stats['machine_characters'] += len(raw)
            continue
        work = []
        for offset, text in windows(raw):
            stats['prose_characters_scanned'] += len(text)
            stats['windows_scanned'] += 1
            if not TRIGGERS.search(text):
                stats['windows_without_relation_cues'] += 1
            # Every prose character reaches the grammar model, including unknown
            # topics. Identical windows reuse their exact previous analysis.
            work.append((text, offset))
        pending = []
        extracted = []
        analyzed=[]
        for value, offset in work:
            key = (hashlib.sha256(value.encode()).hexdigest(), section.get('kind'))
            if key in parse_cache:
                stats['reused_dependency_windows'] += 1
                stats['prose_characters_analyzed'] += len(value)
                records,parsed=parse_cache[key]
                extracted.extend((f, offset) for f in records)
                analyzed.append((parsed,offset,value))
            else:
                pending.append((value, (offset, key)))
        for doc, (offset, key) in nlp.pipe(pending, as_tuples=True, batch_size=8):
            stats['prose_characters_dependency_parsed'] += len(doc.text)
            stats['prose_characters_analyzed'] += len(doc.text)
            records = extract_doc(doc, ticker=ticker, name=name, source=metadata, offset=0)
            parsed=parse_terms(doc)
            parse_cache[key] = (records,parsed)
            analyzed.append((parsed,offset,doc.text))
            extracted.extend((f, offset) for f in records)
        source_fact_ids=set()
        for template, offset in extracted:
            fact = dict(template)
            original = fact.pop('evidence')
            evidence = {**relocate(original,offset), 'source_id': metadata['id']}
            if raw[evidence['start']:evidence['end']] != evidence['quote']:
                raise ValueError('extracted evidence is not an exact source span')
            # Exact same claims/passages across periods share storage;
            # every source/date and evidence span is still retained.
            key = hashlib.sha256(repr(sorted(fact.items())).encode()+evidence['quote'].encode()).hexdigest()
            if key not in facts:
                facts[key] = {**fact, 'id': key, 'evidence': []}
            source_fact_ids.add(key)
            if evidence not in facts[key]['evidence']:
                facts[key]['evidence'].append(evidence)
            i = max(0, bisect_right(starts, evidence['start'])-1)
            while i < len(source_paragraphs) and source_paragraphs[i]['start'] < evidence['end']:
                paragraph = source_paragraphs[i]
                if key not in paragraph['relationship_ids']:
                    paragraph['relationship_ids'].append(key)
                paragraph['extraction_status'] = 'relationships_extracted'
                i += 1
        by_id={p['id']:p for p in source_paragraphs}
        for parsed,offset,text in sorted(analyzed,key=lambda row:row[1]):
            window,rows=bind_window(parsed,source_id=metadata['id'],offset=offset,text=text,
                paragraphs=source_paragraphs,facts=[facts[k] for k in sorted(source_fact_ids)])
            linguistic_windows.append(window)
            term_rows.extend(rows)
            for row in rows:
                for ident in row['paragraph_ids']:by_id[ident]['term_ids'].append(row['id'])
    ordered = [facts[key] for key in sorted(facts)]
    if stats['prose_characters_analyzed'] != stats['characters_read']-stats['machine_characters']:
        raise ValueError('grammar parser did not analyze all prose')
    for p in paragraphs:
        if p['relationship_ids']:
            p['importance']['reasons'].append('Contains explicit extracted relationships')
            if any(facts[k]['predicate'] in ('buys_from', 'depends_on', 'competes_with', 'exposed_to', 'acquires', 'divests') for k in p['relationship_ids']):
                p['importance']['priority'] = 'high'
        elif p['kind'] == 'prose' and TRIGGERS.search(p['text']):
            p['extraction_status'] = 'unresolved'
            p['review_required'] = True
            p['importance']['reasons'].append('Relationship or business language found, but no explicit relationship resolved')
    graph = {'schema': VERSION, 'ticker': ticker, 'name': name,
            'parser': {'spacy': SPACY_VERSION, 'model': MODEL, 'model_version': MODEL_VERSION},
            'facts': ordered, 'sources': sources, 'paragraphs': paragraphs,
            'term_inventory':{'schema':'term-inventory-1','terms':term_rows,'windows':linguistic_windows,
                'candidate_policy':'every spaCy named entity, noun chunk and NOUN/PROPN token; semantic recall is unverified',
                'sense_resolution_complete':False},
            'coverage': {**dict(stats), 'all_source_sections_read': True,
                         'all_prose_scanned': True, 'all_prose_dependency_analyzed': True,
                         'all_paragraphs_classified': True, 'paragraphs': len(paragraphs),
                         'all_term_candidates_retained':True,'term_occurrences':len(term_rows),
                         'terms_with_argument_links':sum(bool(r['relationships']) for r in term_rows),
                         'machine_inventory_blocks': sum(p['kind'] == 'machine_data' for p in paragraphs),
                         'prose_review_blocks': sum(p['kind'] != 'machine_data' for p in paragraphs),
                         'paragraph_labels': dict(Counter(label for p in paragraphs for label in p['labels'])),
                         'paragraph_outcomes': dict(Counter(p['extraction_status'] for p in paragraphs)),
                         'paragraph_priorities': dict(Counter(p['importance']['priority'] for p in paragraphs)),
                         'paragraphs_requiring_review': sum(p['review_required'] for p in paragraphs),
                         'paragraph_boundary_policy': 'archive nonempty prose text lines; blank separators retained; whole machine attachments are inventory blocks; original DOM boundaries unavailable',
                         'extraction_complete': False,
                         'strategy': 'every prose window dependency-parsed; every source block classified and retained; unknown relations may remain',
                         'facts': len(ordered), 'predicates': dict(Counter(f['predicate'] for f in ordered)),
                         'statuses': dict(Counter(f['status'] for f in ordered))}}
    from .specific_filing_records import details,validate_details
    graph['specific_records']=details(graph)
    validate_details(graph,graph['specific_records'])
    validate_paragraph_coverage(graph)
    from .stock_evidence_graph import build
    graph['stock_evidence']=build(graph)
    return graph
