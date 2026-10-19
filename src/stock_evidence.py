"""Role-aware, dated stock evidence, separate from article and industry scores.

Every retained exposure points at its source passage. A competitor product
never becomes an owned product merely because it appeared in a filing.
"""
from __future__ import annotations

import hashlib
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field

from .aliases import extract_jurisdictions
from .linguistics import GENERIC, STOP, TOKEN, lemma, terms, tokens
from .places import CITIES, STATES
from .aliases import _COUNTRIES

COMPILER_VERSION = 'stock-evidence-2.2'

_LOCATIONS = {p.lower() for p in list(CITIES)+list(STATES)+list(_COUNTRIES)}
_LOCATION_RX = re.compile(r'\b(?:'+'|'.join(re.escape(p) for p in sorted(_LOCATIONS,key=len,reverse=True))+r')\b',re.I)


def locations_in(text: str) -> set[str]:
    found = {m.group().lower() for m in _LOCATION_RX.finditer(text)}
    if re.search(r'\bcarolinas\b',text,re.I):
        found.update(('north carolina','south carolina','carolinas'))
    return found

ROLES = {
    'competitor': re.compile(r'competit|compete|competitive landscape', re.I),
    'supplier': re.compile(r'supplier|subcontractor|raw materials?|material inputs?|supply chain|manufacturing partners?', re.I),
    'customer': re.compile(r'customers?|clients?|distribution|distributors?', re.I),
    'geography': re.compile(r'properties|facilities|geograph|territor|service areas?|locations?', re.I),
    'people': re.compile(r'executive officers?|directors?|management team|signatures?', re.I),
    'subsidiary': re.compile(r'subsidiar|exhibit\s*21', re.I),
    'risk': re.compile(r'risk factors?|legal proceedings?|litigation', re.I),
    'financial': re.compile(r'financial statements?|balance sheets?|cash flows?|accounting policies|auditor', re.I),
}
RELATION_CUES = {
    'supplier': re.compile(r'\b(?:suppliers?|subcontractors?|sourc\w*\s+from|purchas\w*\s+from|procure\w*\s+from|supplied by)\b', re.I),
    'customer': re.compile(r'\b(?:customers?|clients?|sell\w*\s+to|sold to|supply\w*\s+to)\b', re.I),
    'competitor': re.compile(r'\b(?:competitors?|compete\w*\s+with|competition from|unlike)\b', re.I),
    'partner': re.compile(r'\b(?:partners?|collaborat\w*|licens\w*\s+from)\b', re.I),
}
OWN = re.compile(r'\b(?:our|we|its|company|it)\b.{0,90}\b(?:develop\w*|manufactur\w*|market\w*|sell\w*|offer\w*|design\w*|commercializ\w*|produce\w*|own\w*|launch\w*)\b', re.I)
PROPER = re.compile(r'\b(?:[A-Z][A-Za-z0-9-]*)(?:\s+(?:[A-Z][A-Za-z0-9-]*)){0,4}\b')
BRAND = re.compile(r'\b(?:[A-Z][A-Za-z0-9-]+(?:\s+[A-Z][A-Z0-9-]{1,})+|[A-Z][a-z0-9]+[A-Z][A-Za-z0-9-]*|[A-Z][A-Z0-9-]{2,})\b')
BRAND_DENY = frozenset('FDA SEC CEO CFO COO CTO GAAP US USA UK EU AI API GPU CPU NASA NYSE NASDAQ ITEM PART NOT THE AND FOR OUR INC LLC LTD CORP'.lower().split())
HISTORY = re.compile(r'\b(?:was|were|is)\s+(?:founded|formed|incorporated|established|headquartered|domiciled)\b|\bformerly known as\b|\bchanged its name\b',re.I)


@dataclass
class Fact:
    kind: str
    text: str
    passage: str
    source: str = 'description'
    filing_date: str = ''
    accession: str = ''
    section: str = ''
    confidence: str = 'explicit'
    target_ticker: str = ''

    def record(self) -> dict:
        return {k: v for k, v in vars(self).items() if v != ''}


@dataclass
class Profile:
    ticker: str
    name: str
    industry: str
    sector: str
    core: set[str] = field(default_factory=set)
    buckets: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))
    facts: list[Fact] = field(default_factory=list)
    locations: set[str] = field(default_factory=set)
    # A term is a retrieval key, not an untyped alias.
    evidence: dict[str, list[int]] = field(default_factory=lambda: defaultdict(list))
    sources: list[dict] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


class Catalog:
    def __init__(self, rows: list[dict]):
        from .correlation import _alias
        self.first = defaultdict(list)
        for row in rows:
            alias = _alias(row.get('name', ''))
            parts = tuple(alias.split())
            if parts:
                self.first[parts[0]].append((parts, row['ticker']))

    def find(self, text: str) -> list[tuple[str, str]]:
        matches = list(TOKEN.finditer(text))
        words = [m.group().lower().rstrip('.') for m in matches]
        found = []
        for i, word in enumerate(words):
            for parts, ticker in self.first.get(word, ()):
                if tuple(words[i:i+len(parts)]) == parts:
                    found.append((ticker, text[matches[i].start():matches[i+len(parts)-1].end()]))
        return list(dict.fromkeys(found))


def split_sections(text: str, initial: str = 'business'):
    """Route headings rather than deleting them. Line wraps are not headings."""
    role, heading, buf = initial, initial, []
    for line in (text or '').splitlines():
        clean = line.strip()
        is_heading = bool(clean) and len(clean) < 110 and len(clean.split()) < 13 and not clean.endswith(('.', ';'))
        if is_heading:
            selected = next((kind for kind, rx in ROLES.items() if rx.search(clean)), None)
            # Reset out of competition/risk sections at a normal business heading.
            if selected is None and re.search(r'overview|our business|products?|services?|research and development|business segments?|technology|intellectual property|backlog', clean, re.I):
                selected = 'business'
            if selected:
                if buf:
                    yield role, heading, '\n'.join(buf)
                role, heading, buf = selected, clean, []
                continue
        buf.append(line)
    if buf:
        yield role, heading, '\n'.join(buf)


def _sentences(text: str):
    # Avoid splitting Inc./Corp./U.S. and numeric decimals.
    return [s.strip() for s in re.split(r'(?<=[.!?])\s+(?=[A-Z][a-z]|We\b|Our\b|The\b)|\n\s*\n', text) if s.strip()]


def profile_terms(text: str) -> set[str]:
    # Preserve every unigram. Retaining every arbitrary 3-gram from thousands
    # of long reports adds millions of boilerplate strings without meaning.
    # Dictionary phrases and explicit named products remain first-class terms;
    # sentence evidence still retains the original complete wording.
    from .semantic_lookup import load_senses
    inventory,_ = load_senses()
    return {term for term in terms(text) if ' ' not in term or term in inventory}


def build_profile(row: dict, catalog: Catalog) -> Profile:
    ticker = row['ticker'].upper()
    out = Profile(ticker, row.get('name',''), row.get('industry',''), row.get('sector',''))
    chunks = list(row.get('filing_sections') or [])
    description = str(row.get('description') or '')
    if description:
        chunks.insert(0, {'text': description, 'kind':'business', 'source':'finviz', 'section':'description'})
    if row.get('filing') and not row.get('filing_sections'):
        chunks.append({'text':str(row['filing']), 'kind':'business', 'source':'legacy_business_cache', 'section':'business'})
        out.warnings.append('legacy_business_cache: full filing completeness unknown')
    if not chunks:
        out.warnings.append('no_business_source')
    seen = set()
    for chunk in chunks:
        text = str(chunk.get('text') or '')
        if not text:
            continue
        source = {k:chunk.get(k,'') for k in ('source','form','filing_date','accession','section','complete')}
        source['sha256'] = hashlib.sha256(text.encode()).hexdigest()
        out.sources.append(source)
        initial = str(chunk.get('kind') or 'business')
        if initial == 'machine_data':
            continue
        # Only the issuer's own introductory legal name can define these
        # candidates. Arbitrary proper names elsewhere remain non-identities.
        from .correlation import _alias
        legal=_alias(out.name).split()
        intro=re.match(r'^\s*'+r'[\W_]+'.join(map(re.escape,legal))+r'\b',text,re.I) if legal else None
        if intro:
            tail=text[intro.end():intro.end()+180]
            dba=re.match(r'\s*(?:(?:Corp(?:oration)?|Inc(?:orporated)?|Co|Ltd|Limited)\.?[,\s]*)*[,\s]*(?:doing business as|trading as|d/b/a)\s+([A-Z][A-Za-z0-9-]*(?:\s+[A-Z][A-Za-z0-9-]*){0,3})',tail)
            if dba:
                _fact(out,'company_alias',dba.group(1),text[:intro.end()+dba.end()],source,'legal introduction')
            initials=''.join(w[0] for w in legal if w not in {'and','the','of'}).upper()
            if re.match(r'\s+Co\.',tail):initials+='C'
            if 3<=len(initials)<=6 and initials.lower() not in BRAND_DENY:
                ix=_fact(out,'derived_initialism',initials,text[:intro.end()+min(len(tail),120)],source,'legal introduction')
                out.facts[ix].confidence='source_derived_candidate'
        if initial in ('business','update') and (intro or re.search(r'\b(?:we|company)\s+operate\w* through\b',text,re.I)) and re.search(r'\boperates? through\b.{0,160}\bsegments?\b',text,re.I):
            segments=defaultdict(dict)
            for sentence in _sentences(text):
                for m in re.finditer(r'\b(?:The|the|Its|its)\s+([A-Z][A-Za-z0-9-]*(?:\s+[A-Z][A-Za-z0-9-]*){1,3})\s+segment\b',sentence):
                    segments[m.group(1).split()[0]][m.group(1)]=sentence
            for brand,owned in segments.items():
                if len(owned)>=2 and brand.lower() not in BRAND_DENY:
                    ix=_fact(out,'operating_brand',brand,' '.join(owned.values()),source,'operating segments')
                    out.facts[ix].confidence='shared_owned_segment_name'
        if chunk.get('complete') is False:
            out.warnings.append('incomplete_source:' + str(chunk.get('accession','')))
        for kind, heading, body in split_sections(text, initial):
            # A short business paragraph can explain one operation across
            # several sentences. Keep that contiguous source as a witness;
            # never join sections, dates, or counterparty relationships.
            if kind in ('business','update'):
                for paragraph in re.split(r'\n\s*\n', body):
                    run=[]
                    for sentence in _sentences(paragraph)+['']:
                        counterparty=any(t!=ticker for t,n in catalog.find(sentence))
                        excluded=not sentence or HISTORY.search(sentence) or (counterparty and any(rx.search(sentence) for rx in RELATION_CUES.values()))
                        if not excluded:
                            run.append(sentence)
                            continue
                        context=' '.join(run)
                        if len(run)>1 and len(tokens(context,stop=False))<=100:
                            content=profile_terms(context)
                            ix=_fact(out,'business','',context,source,heading)
                            for term in content:
                                if len(out.evidence[term])<4:out.evidence[term].append(ix)
                        run=[]
            if kind == 'subsidiary':
                for line in body.splitlines():
                    match = re.search(r'\b([A-Z][A-Za-z0-9& .-]{1,90}?\s+(?:Corporation|Corp\.?|Inc\.?|Incorporated|LLC|Limited|Ltd\.?|GmbH|B\.V\.))\b',line)
                    if match:
                        _fact(out,'subsidiary',match.group(1).strip(),line,source,heading)
            for sentence in _sentences(body):
                if len(sentence) < 10:
                    continue
                local_kind = kind
                if kind in ('business','update') and HISTORY.search(sentence):
                    local_kind='history'
                    _fact(out,'history','',sentence,source,heading)
                relation = next((role for role, rx in RELATION_CUES.items() if rx.search(sentence)), None)
                counterparties = [(t, name) for t, name in catalog.find(sentence) if t != ticker]
                # A named counterparty is not the company's own operation.
                if relation and counterparties:
                    local_kind = relation
                if local_kind in ('competitor','supplier','customer','partner'):
                    _fact(out,local_kind,'',sentence,source,heading)
                if kind == 'financial' and not re.search(r'\b(?:interest|currency|debt|credit|liquidity|hedg\w*|covenant|maturity|maturities)\b',sentence,re.I):
                    continue
                locations = locations_in(sentence)
                out.locations.update(locations)
                for location in locations:
                    _fact(out, 'geography', location, sentence, source, heading)
                for target, name in counterparties:
                    if relation or kind in ('competitor','supplier','customer','partner','subsidiary'):
                        _fact(out, relation or kind, name, sentence, source, heading, target=target)
                # Keep the relationship phrase as retrieval evidence but outside core.
                content = profile_terms(sentence)
                out.buckets[local_kind].update(content)
                if local_kind in ('business','update'):
                    out.core.update(content)
                    ix = _fact(out, 'business', '', sentence, source, heading)
                    for term in content:
                        if len(out.evidence[term]) < 4:
                            out.evidence[term].append(ix)
                    # An issuer's glossary can explicitly define a named
                    # service/network as its own without a manufacturing verb.
                    for m in re.finditer(r'["\u201c]([^"\u201d\n]{3,60})["\u201d]\s+refers? to our\b',sentence):
                        value=m.group(1).strip()
                        if any(w not in STOP|GENERIC for w in tokens(value)):
                            _fact(out,'owned_product',value,sentence,source,heading)
                if kind in ('risk','financial','supplier') or re.search(r'\b(?:depend\w* on|rely\w* on|require|consume|raw materials?|input costs?|sensitive to|exposed to)\b',sentence,re.I):
                    _fact(out, 'financial_exposure' if kind == 'financial' else 'material_exposure', '', sentence, source, heading)
                if local_kind in ('business','update') and not re.search(r'\b(?:no longer|ceased|divested|discontinued|formerly|sold our)\b',sentence,re.I) and (OWN.search(sentence) or re.search(r'\b(?:our products?|product portfolio|pipeline|marketed products?)\b', heading, re.I) or re.search(r'\b(?:our|its) products?(?: and services?)? include\b',sentence,re.I)):
                    product_list=re.search(r'\b(?:our|its) products?(?: and services?)? include\s+(.+?)(?:\.|$)',sentence,re.I)
                    if product_list:
                        for item in re.split(r',|\band\b',product_list.group(1)):
                            value=re.sub(r'\s+products?$','',item.strip())
                            if re.fullmatch(r'(?:[a-z]?[A-Z][A-Za-z0-9-]*)(?:\s+[A-Z][A-Za-z0-9-]*){0,2}',value):
                                _fact(out,'owned_product',value,sentence,source,heading)
                    for m in BRAND.finditer(sentence):
                        value = m.group().strip()
                        if len(value) < 3 or value.lower() in BRAND_DENY or value.lower() in (out.name.lower(), ticker.lower()):
                            continue
                        if not any(tok not in STOP for tok in tokens(value)):
                            continue
                        # Ownership requires a first-person development/sales statement.
                        _fact(out, 'owned_product', value, sentence, source, heading)
                    # Pharmaceuticals often use a single capitalized brand plus generic name.
                    for m in re.finditer(r'\b([A-Z][A-Za-z0-9-]{3,})\s*\(([a-z][a-z0-9-]{3,})\)', sentence):
                        for value in m.groups():
                            _fact(out, 'owned_product', value, sentence, source, heading)
                # A disease is an exposure, never a product alias.
                for m in re.finditer(r'\b(?:treatment of|treat(?:ing|s)?|indicated for)\s+([a-z][a-z -]{3,65})(?=[,.();]|$)', sentence, re.I):
                    _fact(out, 'indication', m.group(1).strip(), sentence, source, heading)
                if kind == 'people' or re.search(r'\bfounded by\b', sentence, re.I):
                    for m in re.finditer(r'\b([A-Z][a-z]+(?:\s+(?:[A-Z]\.|[A-Z][a-z]+)){1,3})\b', sentence):
                        if any(word in m.group().lower() for word in ('united states','chief executive','board director')):
                            continue
                        _fact(out, 'person', m.group(), sentence, source, heading)
                for m in re.finditer(r'\b(?:our|wholly[- ]owned)\s+subsidiary\s+([A-Z][A-Za-z0-9& .-]{1,80}?\s+(?:Corporation|Corp|Inc|LLC|Limited|Ltd))\b',sentence):
                    _fact(out,'subsidiary',m.group(1),sentence,source,heading)
                # Only locally defined acronyms get a candidate key.
                for m in re.finditer(r'\b([A-Za-z][A-Za-z -]{4,70})\s+\(["“]?([A-Z][A-Z0-9]{1,7})["”]?\)', sentence):
                    if m.group(2).lower() not in BRAND_DENY:
                        _fact(out, 'acronym', m.group(2), sentence, source, heading)
    return out


def _fact(out: Profile, kind: str, value: str, sentence: str, source: dict, section: str, target: str = '') -> int:
    fact = Fact(kind, value, sentence, source.get('source') or 'filing', source.get('filing_date',''), source.get('accession',''), section, target_ticker=target)
    out.facts.append(fact)
    return len(out.facts)-1


@dataclass
class EvidenceBoard:
    profiles: dict[str, Profile]
    postings: dict[str, set[str]]
    df: Counter
    industry_terms: dict[str, set[str]]
    industry_members: dict[str, set[str]]
    identities: dict[str, list[tuple[str,str,int]]]
    scope: dict[str, set[str]]
    source_fingerprint: str
    # Relationship lookup is built once, rather than walking the entire
    # filing archive for every sentence of every incoming article.
    exposures: dict = field(default_factory=dict)
    dependencies: dict = field(default_factory=dict)


def compile_board(rows: list[dict]) -> EvidenceBoard:
    rows = sorted(rows, key=lambda r:r['ticker'])
    catalog = Catalog(rows)
    profiles = {row['ticker']:build_profile(row, catalog) for row in rows}
    return index_profiles(profiles)


def index_profiles(profiles: dict[str, Profile]) -> EvidenceBoard:
    postings = defaultdict(set)
    df = Counter()
    industries = defaultdict(set)
    counts = defaultdict(Counter)
    ids = defaultdict(list)
    scope = defaultdict(set)
    exposures = defaultdict(lambda: defaultdict(list))
    dependencies = defaultdict(list)
    relation_terms = {}
    for ticker, p in sorted(profiles.items()):
        from .correlation import _alias
        legal_words=_alias(p.name).split()
        corporate_alias=re.compile(r'\b'+r'[\W_]+'.join(map(re.escape,legal_words))+r'\s*,?\s*(?:(?:Inc|Corp|Corporation|Ltd|Limited|Co)\.?\s*)?\(([^)\n]{3,200})\)',re.I) if legal_words else None
        # Rarity must account for incidental risk/relationship vocabulary too;
        # a term common in everyone's risks is not a company-specific anchor.
        all_terms=set(p.core)
        for vocab in p.buckets.values():all_terms.update(vocab)
        df.update(all_terms)
        industries[p.industry].add(ticker)
        counts[p.industry].update(p.core)
        for term in p.core:
            postings[term].add(ticker)
        for i, fact in enumerate(p.facts):
            if corporate_alias and '(' in fact.passage:
                for match in corporate_alias.finditer(fact.passage):
                    for raw in re.findall(r'["\u201c]([^"\u201d]{2,60})["\u201d]',match.group(1)):
                        value=' '.join(tokens(raw,stop=False))
                        if value and any(w not in STOP|GENERIC for w in value.split()) and value not in {'parent','registrant','issuer','company'}:
                            ids[value].append((ticker,'company_alias',i))
            if fact.kind in ('supplier','customer','partner','competitor') and fact.target_ticker:
                dependencies[fact.target_ticker].append((ticker,i))
            if fact.kind in ('competitor','supplier','material_exposure','financial_exposure'):
                # Identical filing sentences often recur over many periods.
                # Share tokenization work while retaining every dated fact.
                words = relation_terms.get(fact.passage)
                if words is None:
                    words = frozenset(tokens(fact.passage)) - GENERIC - STOP
                    relation_terms[fact.passage] = words
                for word in words:
                    exposures[ticker][word].append(i)
                    postings[word].add(ticker)
            if fact.kind in ('owned_product','person','acronym','subsidiary','company_alias','derived_initialism','operating_brand') and fact.text:
                ids[' '.join(tokens(fact.text,stop=False))].append((ticker,fact.kind,i))
                if fact.kind == 'person' and len(tokens(fact.text,stop=False)) >= 2:
                    ids[tokens(fact.text,stop=False)[-1]].append((ticker,'person',i))
                if fact.kind == 'subsidiary':
                    from .correlation import _alias
                    short=' '.join(tokens(_alias(fact.text),stop=False))
                    if short:
                        ids[short].append((ticker,'subsidiary',i))
            # Relationship/indication/input terms are available for inspection.
            if fact.kind not in ('business','geography','financial'):
                for term in terms(fact.text):
                    postings[term].add(ticker)
        for location in p.locations:
            scope[location].add(ticker)
    characteristic = {}
    n = max(len(profiles),1)
    for industry, members in industries.items():
        home = counts[industry]
        characteristic[industry] = {
            term for term, count in home.items()
            if term not in GENERIC and term not in STOP
            and (count / len(members)) > ((df[term]-count) / max(n-len(members),1)) + 0.05
            and (count >= 2 or len(members) <= 3)
        }
    fingerprint = hashlib.sha256('\n'.join(t+':'+','.join(s['sha256'] for s in p.sources) for t,p in sorted(profiles.items())).encode()).hexdigest()
    return EvidenceBoard(profiles,dict(postings),df,characteristic,dict(industries),dict(ids),dict(scope),fingerprint,
                         dict(exposures),dict(dependencies))
