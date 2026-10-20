"""Deterministic token/phrase analysis. No model, API, or network at scan time.

Inflections use a pinned dictionary. Derivations remain distinct. Optional
WordNet-derived neighbors are explicitly typed and never treated as identity.
"""
from __future__ import annotations

import re
import unicodedata
import json
import gzip
from pathlib import Path
from dataclasses import dataclass
from functools import lru_cache

from .names import english_words
from .spelling import spelling
from .synonyms import canonical

TOKEN = re.compile(r"[A-Za-z][A-Za-z0-9]*(?:[-'][A-Za-z0-9]+)*")
STOP = frozenset('''a an the and or but of to in on at by for from with as is are was were be been being it its we our us their they this that these those which who when where while into over under through than such including include includes company companies corporation incorporated corp inc ltd limited plc group holding holdings business businesses product products service services segment segments engage engages operate operates provide provides offer offers million billion percent year years quarter quarters said says according also new more most other all any some shall must would could will may has have had do does did not no hereby there thereafter pursuant memorandum sec section item part'''.split())
GENERIC = frozenset('''market financial growth develop development system systems technology technologies solution solutions industry industries management operations investment investor investors report reports announce announced announcement update agreement event trial drug readout study studies research commercial customer customers supplier suppliers platform global international portfolio information leading result results support national american canadian british federal state north south east west united states administration secretary administrator'''.split())
IRREGULAR = {'children':'child','men':'man','women':'woman','people':'person','indices':'index','analyses':'analysis','crises':'crisis','bought':'buy','sold':'sell','built':'build','won':'win','rose':'rise','fell':'fall','grew':'grow','sought':'seek','made':'make','took':'take','ran':'run','held':'hold','paid':'pay','laid':'lay','lost':'lose','feet':'foot','teeth':'tooth'}
PROTECTED = frozenset('analysis crisis basis diagnosis diabetes news series species gas goods earnings logistics operations communications economics physics semiconductor aerospace'.split())
IRREGULAR.update({'said':'say','found':'find','brought':'bring','gave':'give','sent':'send','set':'set','left':'leave','led':'lead'})
IRREGULAR.update({'is':'be','are':'be','was':'be','were':'be'})


@lru_cache(maxsize=1)
def _lookup():
    path = Path(__file__).with_name('data') / 'lemma_lookup.json'
    compressed=path.with_name(path.name+'.gz')
    if compressed.is_file():
        return json.loads(gzip.decompress(compressed.read_bytes()))
    return json.loads(path.read_text(encoding='utf-8')) if path.is_file() else {}


@lru_cache(maxsize=100000)
def lemma(raw: str) -> str:
    word = spelling(unicodedata.normalize('NFKC', raw).lower().replace('’', "'"))
    if word.endswith("'s"):
        word = word[:-2]
    if word in IRREGULAR:
        return canonical(IRREGULAR[word])
    if word in PROTECTED or len(word) < 5:
        return canonical(word)
    # Upstream spaCy lookup forms replace the small suffix heuristic for
    # dictionary-covered vocabulary. Derivational nouns remain separate.
    lookup = _lookup()
    if word in lookup:
        return canonical(lookup[word])
    words = english_words()
    candidates = []
    if word.endswith('ies'):
        candidates.append(word[:-3] + 'y')
    if word.endswith('ves'):
        candidates.extend((word[:-3] + 'f', word[:-3] + 'fe'))
    if word.endswith(('ches', 'shes', 'sses', 'xes', 'zes')):
        candidates.append(word[:-2])
    if word.endswith('s') and not word.endswith(('ss','us','is')):
        candidates.append(word[:-1])
    if word.endswith(('ing','ed')):
        stem = word[:-3] if word.endswith('ing') else word[:-2]
        candidates.extend((stem + 'e', stem))
        if len(stem) > 3 and stem[-1] == stem[-2]:
            candidates.insert(0, stem[:-1])
        if stem.endswith('i'):
            candidates.insert(0, stem[:-1] + 'y')
    for base in candidates:
        if base in words and len(base) >= 3:
            return canonical(base)
    return canonical(word)


def tokens(text: str, *, stop: bool = True) -> list[str]:
    values = [lemma(m.group()) for m in TOKEN.finditer(text or '')]
    return [w for w in values if len(w) >= 3 and (not stop or w not in STOP)]


def terms(text: str) -> set[str]:
    """Keep all words and contiguous 2/3-grams; never cross punctuation or stops."""
    out = set(tokens(text))
    run = []
    last_end = 0
    for m in TOKEN.finditer(text or ''):
        word = lemma(m.group())
        if re.search(r'[.!?;:\n]', text[last_end:m.start()]):
            run = []
        last_end = m.end()
        if word in STOP or len(word) < 3:
            run = []
            continue
        run.append(word)
        for n in (2,3):
            if len(run) >= n:
                out.add(' '.join(run[-n:]))
    return out


EVENT = re.compile(r'\b(?:advanc\w*|progress\w*|earnings|revenue|profit|loss|guidance|sales|contract|order|acquir\w*|merg\w*|buy\w*|bought|sell\w*|sold|approv\w*|adopt\w*|amend\w*|directives?|certif\w*|reject\w*|den\w*|recall\w*|lawsuit|sue[sd]?|sues|sued|investigat\w*|charg\w*|sanction\w*|tariff\w*|ban\w*|restrict\w*|permit\w*|licens\w*|regulat\w*|policy|legislation|legislative|rule\w*|production|capacity|demand|supply|price\w*|cost\w*|shortage\w*|disrupt\w*|outage\w*|halt\w*|shutdown\w*|launch\w*|reentr\w*|results?|trial|layoff\w*|cut\w*|rais\w*|hike\w*|expand\w*|expansion|invest\w*|fund\w*|win\w*|won|beat\w*|miss\w*|forecast\w*|appoint\w*|resign\w*|step\w*\s+down|bankrupt\w*|default\w*|dividend\w*|split\w*|repurchas\w*|breach\w*|attack\w*|fire\w*|strike\w*|export\w*|import\w*)\b', re.I)
EVENT = re.compile(r'(?:' + EVENT.pattern + r'|\b(?:orders|crash\w*|shutoffs?|warn\w*|clearance|fine[sd]?|violations?|buyout|verdict|withholding|replac\w*|dismiss\w*|boot\w*|expir\w*|prais\w*|sink\w*|sank)\b|\bv\.(?=\s|$)|\bshares?\b.{0,35}\b(?:fall\w*|ris\w*|rose|slip\w*|plung\w*|soar\w*|gain\w*|los\w*|down|up)\b)', re.I)
FINANCIAL_EVENT = re.compile(r'\b(?:earnings|revenue|profit|guidance|shares|stock|dividend|repurchase|buyback|merger|acquisition|bankruptcy|layoffs|appoint\w*|resign\w*)\b', re.I)
EVENT = re.compile(r'(?:'+EVENT.pattern+r'|\b(?:unveil\w*|complet\w*|acquisition|tender offer|disclos\w*|files?|filed|form\s+4|waves?\s+goodbye|doubl\w*|diverg\w*|plung\w*)\b)',re.I)
EVENT = re.compile(r'(?:'+EVENT.pattern+r'|\b(?:unveil\w*|complet\w*|acquisition|tender\s+offer|deal|disclos\w*|files?|form\s+4|waves?\s+goodbye)\b|\bstocks?\b.{0,45}\b(?:double\w*|diverg\w*|plung\w*|fall\w*|ris\w*|gain\w*|los\w*|down|up)\b)',re.I)
COMPARISON = re.compile(r'\b(?:unlike|compared (?:with|to)|similar to|such as|for example|like)\b', re.I)


@lru_cache(maxsize=1)
def verb_lemmas() -> frozenset[str]:
    """Local dictionary POS inventory, not a learned model or an API call."""
    from .semantic_lookup import load_senses
    index,_=load_senses()
    return frozenset(word for word,senses in index.items() if ' ' not in word and any(s[3]=='v' for s in senses))


@lru_cache(maxsize=1)
def verb_only_lemmas() -> frozenset[str]:
    from .semantic_lookup import load_senses
    index,_=load_senses()
    return frozenset(word for word,senses in index.items() if ' ' not in word and any(s[3]=='v' for s in senses) and not any(s[3]=='n' for s in senses))


@lru_cache(maxsize=10000)
def has_event(text: str) -> bool:
    # Headlines express events through ordinary verbs, not just a small
    # financial trigger list. Identity and materiality are separate gates.
    if EVENT.search(text) or re.search(r'\b(?:insider|stock|share)\s+sale\b',text,re.I):return True
    if re.search(r'\b(?:is|are|was|were)\s+(?:in|under|at)\s+',text,re.I):return True
    words=[m.group() for m in TOKEN.finditer(text)]
    for i,raw in enumerate(words):
        base=lemma(raw)
        if base not in verb_lemmas():continue
        previous=words[i-1].lower() if i else ''
        if previous in {'a','an','the'}:continue
        if base in verb_only_lemmas():return True
        if raw.lower()!=base and (raw.lower().endswith(('s','ed','ing')) or raw.lower() in IRREGULAR):return True
        if previous in {'to','will','would','could','should','can','may','must'}:return True
        if i and words[i-1].lower().endswith('s') and lemma(words[i-1])!=words[i-1].lower():return True
    return False
DISRUPTION = re.compile(r'\b(?:sole|single[- ]source|exclusive|critical|material)\b.*\b(?:halt\w*|shutdown|shortage|disrupt\w*|outage|bankrupt\w*|ban\w*|recall\w*)\b|\b(?:halt\w*|shutdown|shortage|disrupt\w*|outage|bankrupt\w*)\b', re.I)


@dataclass(frozen=True)
class Passage:
    text: str
    terms: frozenset[str]
    ordinal: int
    heading: bool
    event: bool


def sentence_parts(text: str) -> list[str]:
    """Preserve legal abbreviations and initials inside a sentence."""
    start=0
    out=[]
    for boundary in re.finditer(r'(?<=[.!?])\s+(?=[A-Z0-9])|;\s*|\n\s*\n',text):
        prefix=text[:boundary.start()]
        if re.search(r'\b(?:Inc|Corp|Co|Ltd|Dr|Mr|Mrs|Ms|Jr|Sr)\.$|\b(?:[A-Z]\.)+$',prefix):
            continue
        out.append(text[start:boundary.start()].strip())
        start=boundary.end()
    out.append(text[start:].strip())
    return [piece for piece in out if piece]


def passages(text: str, title: str = '') -> list[Passage]:
    """Keep clauses separate, including a switch of topic after a semicolon."""
    raw = unicodedata.normalize('NFKC', text or '').replace('’', "'")
    lines = [line.strip() for line in raw.splitlines() if line.strip()]
    result = []
    seen = set()
    source_tail=re.search(r'\s+-\s+([^\n]{2,80})$',' - '+title.rsplit(' - ',1)[1]) if ' - ' in title else None
    publisher=source_tail.group(1).strip() if source_tail and not EVENT.search(source_tail.group(1)) else ''
    for line_index, line in enumerate(lines):
        line=re.split(r'#{2,}\s*(?:Updated|Topics|Related|Share)\b',line,flags=re.I)[0].strip()
        # Publisher related-ticker widgets follow the primary headline. Their
        # exchange-qualified symbols are metadata, not headline participants.
        widget=re.search(r'\s+[-–—|]\s+([^\n|–—]{1,100})\((?:NASDAQ|NYSE|AMEX)\s*:',line,flags=re.I)
        if widget:
            label=widget.group(1).strip().casefold()
            # Repeating the primary subject is a legitimate headline tag;
            # introducing a different related issuer is publisher furniture.
            if not re.search(r'\b'+re.escape(label)+r'\b',line[:widget.start()],re.I):
                line=line[:widget.start()].strip()
        if publisher:
            line=re.sub(r'(?:\s+-)?\s+'+re.escape(publisher)+r'\s*$','',line,flags=re.I)
        # A publisher tail is recognized by the existing catalog, not by every dash.
        from .names import PUBLISHERS
        for source in PUBLISHERS:
            line = re.sub(r'\s+[-–—|]\s*' + re.escape(source) + r'(?:\.com)?\s*$', '', line, flags=re.I)
        for piece in sentence_parts(line):
            key = re.sub(r'\W+', ' ', piece).lower().strip()
            if not key or key in seen:
                continue
            seen.add(key)
            result.append(Passage(piece, frozenset(terms(piece)), len(result), line_index == 0, has_event(piece)))
    return result


def input_quality(text: str, title: str = '') -> dict:
    ps = passages(text, title)
    unique_words = sum(len(tokens(p.text)) for p in ps)
    headline_only = len(ps) <= 1 and len(text) < 600
    return {'status': 'headline_only' if headline_only else 'body', 'unique_content_tokens': unique_words,
            'passages': len(ps), 'has_event': any(p.event for p in ps),
            'body_available': not headline_only, 'empty': not ps}
