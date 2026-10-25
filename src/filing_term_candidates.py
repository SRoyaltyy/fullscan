"""Broad lexical candidates before relevance, identity or relationship decisions.

Source spans are authoritative. Normalized forms are search keys, not entity IDs.
"""
import hashlib
import re

VERSION = 'filing-term-candidates-1'
NAME_LABELS = {'ORG', 'PERSON', 'GPE', 'LOC', 'FAC', 'PRODUCT', 'LAW',
               'GOV_AGENCY', 'EVENT', 'WORK_OF_ART', 'NORP', 'LANGUAGE'}


def extract_candidates(doc):
    """Keep broad lexical candidates without consulting any relationship rule."""
    protected = {t.i for e in doc.ents if e.label_ in NAME_LABELS for t in e}
    found = {}
    chunks = list(doc.noun_chunks)

    def add(value, kind, label=None):
        start, end = value.start, value.end
        # Trim grammatical edges, never interior words of a phrase or name.
        while start < end and (
                (doc[start].pos_ == 'DET' and doc[start].text.islower()) or
                (doc[start].i not in protected and (doc[start].is_space or doc[start].is_punct or
                 doc[start].pos_ in {'DET', 'PRON', 'CCONJ', 'SCONJ'}))):
            start += 1
        while start < end and doc[end-1].i not in protected and (
                doc[end-1].is_space or doc[end-1].is_punct or
                doc[end-1].pos_ in {'CCONJ', 'SCONJ'}):
            end -= 1
        value = doc[start:end]
        if not value or not re.search(r'[A-Za-z]', value.text):
            return
        if not any(t.pos_ in {'NOUN', 'PROPN'} for t in value) and label not in NAME_LABELS and kind not in {
                'content_token', 'hyphenated_term', 'hyphenated_noun_phrase'}:
            return
        normalized, changes = [], []
        for t in value:
            form = t.text
            if t.pos_ == 'NOUN' and t.tag_ == 'NNS' and t.i not in protected and t.lemma_:
                form = t.lemma_
                if form.casefold() != t.text.casefold():
                    changes.append({'start': t.idx, 'end': t.idx+len(t.text),
                                    'text': t.text, 'lemma': form})
            normalized.append(form + t.whitespace_)
        row = found.setdefault((value.start_char, value.end_char), {
            'start': value.start_char, 'end': value.end_char, 'text': value.text,
            'normalized': re.sub(r'\s+', ' ', ''.join(normalized)).strip().casefold(),
            'kinds': set(), 'entity_types': set(), 'plural_normalizations': changes,
            'sentence': {'start': value.sent.start_char, 'end': value.sent.end_char,
                         'text': value.sent.text},
            'relationship_status': 'unresolved', 'identity_status': 'unresolved'})
        row['kinds'].add(kind)
        if label:
            row['entity_types'].add(label)

    for entity in doc.ents:
        add(entity, 'named_entity', entity.label_)
    for phrase in chunks:
        add(phrase, 'noun_phrase')
    for token in doc:
        if token.pos_ in {'NOUN', 'PROPN'}:
            add(doc[token.i:token.i+1], 'noun_token')
    # Preserve content words when a statistical POS guess misses a noun/brand.
    # Extra verbs/adjectives remain explicitly unresolved for later review.
    for token in doc:
        if (not token.is_stop and re.search(r'[A-Za-z]', token.text)
                and token.pos_ not in {'ADP', 'AUX', 'DET', 'CCONJ', 'SCONJ',
                                      'PRON', 'PART', 'PUNCT', 'SPACE'}):
            add(doc[token.i:token.i+1], 'content_token')
    # Contiguous nouns can survive even when the base noun-chunk boundary is wrong.
    run = None
    for index in range(len(doc)+1):
        nominal = index < len(doc) and doc[index].pos_ in {'NOUN', 'PROPN'}
        if nominal and run is None:
            run = index
        if run is not None and (not nominal or (index > run and
                doc[index].sent.start != doc[run].sent.start)):
            if index-run > 1:
                add(doc[run:index], 'nominal_sequence')
            run = index if nominal else None
    # Reconstruct literal hyphen compounds from source offsets, retaining their
    # internal function words; append adjacent noun/gerund heads as candidates.
    for match in re.finditer(r'(?<!\w)[A-Za-z0-9]+(?:[-‐‑‒–—][A-Za-z0-9]+)+(?!\w)', doc.text):
        value = doc.char_span(match.start(), match.end(), alignment_mode='expand')
        if value is None:
            continue
        add(value, 'hyphenated_term')
        end = value.end
        while (end < len(doc) and doc[end].sent.start == value.sent.start
               and (doc[end].pos_ in {'NOUN', 'PROPN'} or doc[end].tag_ == 'VBG')
               and '\n' not in doc.text[doc[end-1].idx+len(doc[end-1].text):doc[end].idx]):
            end += 1
            add(doc[value.start:end], 'hyphenated_noun_phrase')
    rows = []
    for position in sorted(found):
        row = found[position]
        row['kinds'] = sorted(row['kinds'])
        row['entity_types'] = sorted(row['entity_types'])
        rows.append(row)
    return {'schema': VERSION,
            'input_sha256': hashlib.sha256(doc.text.encode()).hexdigest(),
            'terms': rows,
            'limitations': ['Lexical candidates only; relevance, identity and relationships are unresolved.',
                            'Statistical grammar can miss or split phrases. Candidate counts are not recall scores.']}
