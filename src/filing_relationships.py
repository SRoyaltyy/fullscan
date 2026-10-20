"""Offline, syntax-based SEC relationship extraction.

The trained grammar model is local and pinned. Roles come from clauses, never
from embedding similarity or a company-specific supplier/competitor list.
All prose is scanned; bounded windows control memory, not source coverage.
"""
from __future__ import annotations

import hashlib
import re
from collections import Counter

VERSION = 'filing-relationships-1.0'
SPACY_VERSION = '3.8.11'
MODEL = 'en_core_web_sm'
MODEL_VERSION = '3.8.0'
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
    if spacy.__version__ != SPACY_VERSION:
        raise ValueError('expected spaCy '+SPACY_VERSION)
    nlp = spacy.load(MODEL)
    if nlp.meta.get('version') != MODEL_VERSION:
        raise ValueError('expected '+MODEL+' '+MODEL_VERSION)
    nlp.max_length = 100_000
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


def _phrase(token):
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
    words = [token.doc[i].text for i in sorted(selected)]
    return re.sub(r'^(?:the|a|an|our|their)\s+', '', ' '.join(words), flags=re.I).strip(' ,;:')


def _objects(token):
    return [c for c in token.children if c.dep_ in ('dobj', 'obj', 'attr', 'oprd')]


def _preps(token, names):
    return [o for p in token.children if p.dep_ in ('prep', 'agent') and p.lower_ in names
            for o in p.children if o.dep_ in ('pobj', 'obj')]


def _coordinated(token):
    yield token
    for other in token.conjuncts:
        if other.i != token.i:
            yield other


def _subjects(token):
    subjects = [c for c in token.children if c.dep_ in ('nsubj', 'nsubjpass', 'csubj')]
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


def _status(token, sentence):
    # Clause-local negation: never turn "do not rely" into a current supplier.
    chain = [token]
    while chain[-1].dep_ in ('acomp', 'attr', 'oprd', 'xcomp') and chain[-1].head != chain[-1]:
        chain.append(chain[-1].head)
    if any(c.dep_ == 'neg' for part in chain for c in part.children):
        return 'negated'
    if re.search(r'\b(?:no longer|previously|formerly|ceased|terminated|discontinued)\b', sentence.text, re.I):
        return 'historical'
    if any(c.lemma_ in ('may', 'might', 'could', 'would', 'will') for part in chain for c in part.children) or re.search(r'\bif\b', sentence.text, re.I):
        return 'potential'
    return 'asserted'


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
        status = _status(trigger, sentence)
        actor = subject or ticker
        signature = (predicate, actor, value, value_type, status, sentence.start_char)
        if signature in seen:
            return
        seen.add(signature)
        facts.append({'subject': actor, 'predicate': predicate, 'object': value,
                      'object_type': value_type, 'status': status, 'confidence': confidence,
                      'rule': VERSION+':'+predicate, 'qualifiers': qualifiers or {},
                      'evidence': {'quote': sentence.text, 'start': offset+sentence.start_char,
                                   'end': offset+sentence.end_char, 'source_id': source['id']}})

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
                        candidates = [e for e in _named_in(anchor) if not any(_issuer(t, name) for t in e)]
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


def extract_sections(sections, *, ticker, name, nlp):
    sources, facts, stats = [], {}, Counter()
    from spacy.pipeline import Sentencizer
    segment = Sentencizer()
    parse_cache = {}
    for section in sections:
        raw = str(section.get('text') or '')
        metadata = {k: section.get(k, '') for k in ('source', 'form', 'filing_date', 'accepted_at', 'report_date', 'accession', 'section', 'document', 'kind', 'url')}
        metadata['sha256'] = hashlib.sha256(raw.encode()).hexdigest()
        metadata['characters'] = len(raw)
        metadata['id'] = hashlib.sha256(repr(sorted(metadata.items())).encode()).hexdigest()
        sources.append(metadata)
        stats['source_sections'] += 1
        stats['characters_read'] += len(raw)
        if section.get('kind') == 'machine_data':
            stats['machine_characters'] += len(raw)
            continue
        work = []
        for offset, text in windows(raw):
            stats['prose_characters_scanned'] += len(text)
            stats['windows_scanned'] += 1
            if not TRIGGERS.search(text):
                stats['windows_without_relation_cues'] += 1
                continue
            # Fast tokenization and sentence boundaries scan the whole window.
            # The expensive grammar model sees cue-bearing sentences with one
            # preceding sentence for context; no source text is discarded.
            sentences = list(segment(nlp.make_doc(text)).sents)
            selected = set()
            for i, sentence in enumerate(sentences):
                if TRIGGERS.search(sentence.text):
                    selected.add(i)
                    if i:
                        selected.add(i-1)
            runs = []
            for i in sorted(selected):
                if runs and i == runs[-1][-1] + 1:
                    runs[-1].append(i)
                else:
                    runs.append([i])
            for run in runs:
                start, end = sentences[run[0]].start_char, sentences[run[-1]].end_char
                value = text[start:end]
                work.append((value, offset+start))
        pending = []
        extracted = []
        for value, offset in work:
            key = (hashlib.sha256(value.encode()).hexdigest(), section.get('kind'))
            if key in parse_cache:
                stats['reused_dependency_windows'] += 1
                extracted.extend((f, offset) for f in parse_cache[key])
            else:
                pending.append((value, (offset, key)))
        for doc, (offset, key) in nlp.pipe(pending, as_tuples=True, batch_size=8):
            stats['prose_characters_dependency_parsed'] += len(doc.text)
            records = extract_doc(doc, ticker=ticker, name=name, source=metadata, offset=0)
            parse_cache[key] = records
            extracted.extend((f, offset) for f in records)
        for template, offset in extracted:
            fact = dict(template)
            original = fact.pop('evidence')
            evidence = {**original, 'start': original['start']+offset,
                        'end': original['end']+offset, 'source_id': metadata['id']}
            if raw[evidence['start']:evidence['end']] != evidence['quote']:
                raise ValueError('extracted evidence is not an exact source span')
            # Exact same claims/passages across periods share storage;
            # every source/date and evidence span is still retained.
            key = hashlib.sha256(repr(sorted(fact.items())).encode()+evidence['quote'].encode()).hexdigest()
            if key not in facts:
                facts[key] = {**fact, 'id': key, 'evidence': []}
            if evidence not in facts[key]['evidence']:
                facts[key]['evidence'].append(evidence)
    ordered = [facts[key] for key in sorted(facts)]
    return {'schema': VERSION, 'ticker': ticker, 'name': name,
            'parser': {'spacy': SPACY_VERSION, 'model': MODEL, 'model_version': MODEL_VERSION},
            'facts': ordered, 'sources': sources,
            'coverage': {**dict(stats), 'all_source_sections_read': True,
                         'all_prose_scanned': True, 'extraction_complete': False,
                         'strategy': 'all-prose cue scan, bounded dependency windows; explicit rules, unknown relations may remain',
                         'facts': len(ordered), 'predicates': dict(Counter(f['predicate'] for f in ordered)),
                         'statuses': dict(Counter(f['status'] for f in ordered))}}
