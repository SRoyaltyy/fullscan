"""Context-selected WordNet senses; synonyms are discounted retrieval evidence.

No learned model at scan time. Never traverse untyped graph hops or collapse
antonyms, derivatives, or all senses of a word into a common identity.
"""
import hashlib
import json
import gzip
from collections import defaultdict
from functools import lru_cache
from pathlib import Path

from .linguistics import GENERIC, STOP, tokens


@lru_cache(maxsize=1)
def load_senses():
    root = Path(__file__).with_name('data')/'senses'
    manifest_path = root/'manifest.json'
    if not manifest_path.is_file():
        return {},{'available':False,'reason':'missing_sense_pack'}
    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    index = defaultdict(list)
    for entry in manifest['files']:
        path = root/entry['file']
        if not path.is_file():
            raise ValueError('missing dictionary data; run python -m scripts.bootstrap_semantics once before scanning')
        raw = path.read_bytes()
        if entry.get('stored_sha256') and hashlib.sha256(raw).hexdigest()!=entry['stored_sha256']:
            raise ValueError('semantic compressed resource checksum mismatch: '+path.name)
        if raw.startswith(b'\x1f\x8b'):
            raw=gzip.decompress(raw)
        if hashlib.sha256(raw).hexdigest() != entry['sha256']:
            raise ValueError('semantic resource checksum mismatch: '+path.name)
        for sense,words,gloss,pos in json.loads(raw):
            members = sorted({' '.join(tokens(w,stop=False)) for w in words})
            context = frozenset(tokens(gloss)) - GENERIC
            item = (sense,tuple(members),context,pos)
            for word in members:
                if len(word) >= 3:
                    index[word].append(item)
    return dict(index),{'available':True,'senses':manifest['senses'],'files':len(manifest['files']),
        'manifest_sha256':hashlib.sha256(manifest_path.read_bytes()).hexdigest()}


def expand(query: set[str]) -> tuple[set[str],dict[str,str],list[dict]]:
    index,_ = load_senses()
    expanded = set(query)
    origin = {term:term for term in query}
    evidence = []
    context = {word for term in query for word in term.split()} - STOP - GENERIC
    for word in sorted(query):
        candidates = []
        for sense,members,gloss,pos in index.get(word,()):
            overlap = (context - set(word.split())) & gloss
            if len(overlap) >= 2:
                candidates.append((len(overlap),sense,members,overlap))
        if not candidates:
            continue
        candidates.sort(key=lambda c:(-c[0],c[1]))
        if len(candidates)>1 and candidates[0][0] == candidates[1][0]:
            continue
        score,sense,members,overlap = candidates[0]
        additions = []
        for other in members:
            if other == word or other in query or other in GENERIC or other in STOP:
                continue
            # A fixed bound prevents broad senses from expanding a document
            # without limit. Different source words stay different evidence units.
            if len(additions) == 8:
                break
            expanded.add(other);origin[other]=word;additions.append(other)
        if additions:
            evidence.append({'source_term':word,'sense':sense,'context_matches':sorted(overlap),'neighbors':additions,'credit':0.45})
    return expanded,origin,evidence
