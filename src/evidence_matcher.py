"""Evidence-based article-to-stock decisions, independent of batch composition.

Scores are diagnostics, not probabilities. A pass names a concrete evidence
route: direct company event, owned-product event, or evidenced exposure.
"""
from __future__ import annotations

import math
import re
from collections import defaultdict, Counter
from dataclasses import replace
from functools import lru_cache

from .aliases import extract_jurisdictions
from .linguistics import COMPARISON, DISRUPTION, EVENT, FINANCIAL_EVENT, GENERIC, STOP, TOKEN, IRREGULAR, has_event, lemma, verb_lemmas, verb_only_lemmas, input_quality, passages, terms, tokens
from .stock_evidence import EvidenceBoard, compile_board, locations_in, RELATION_CUES
from .semantic_lookup import expand,load_senses
from .names import english_words

ROUNDUP = re.compile(r'\b(?:market wrap|stocks? to watch|top \d+ stocks?|top\s+[^:;.]{0,80}\bcompanies|stock picks?|stocks? roundup|analyst picks?|cruise winners)\b', re.I)
AGENCY_ICE = re.compile(r'\bICE\b.{0,100}\b(?:immigration|deport\w*|arrest\w*|detain\w*|enforcement)\b|\b(?:immigration|customs enforcement)\b.{0,100}\bICE\b', re.I)
FOREIGN_EXCHANGE = re.compile(r'\b(?:HOSE|HNX|UPCOM|BSE|NSE|ASX|LSE|TSE|HKEX)\s*[:(]|\b(?:Vietnam|Vietnamese|Kosy)\b', re.I)
SCOPE_EVENT = re.compile(r'\b(?:tariff|sanction|ban|restrict\w*|regulat\w*|policy|permit|price differential|constitutional)\b', re.I)
EVENT_ONLY = frozenset('recall approve approval reject rejection earnings revenue guidance report result class governor secretary minister senator president spokeswoman spokesman key overall respective range broad focus factor'.split())
CALENDAR = frozenset('january february march april may june july august september october november december monday tuesday wednesday thursday friday saturday sunday daily weekly monthly annually yesterday today tomorrow q1 q2 q3 q4'.split())
QUOTE_PAGE = re.compile(r'\bstock price\s*,?\s*news\s*,?\s*quote\s+(?:and|&)\s+history\b|\b8-K\s*&\s*SEC filings\b',re.I)
QUOTE_PAGE = re.compile(QUOTE_PAGE.pattern+r'|\banalyst insights\s*,\s*price targets\s+and\s+recommendations\b',re.I)


@lru_cache(maxsize=10000)
def _identity_text(text: str) -> str:
    # An exchange qualification and a market-index move are not an event at
    # the listed exchange operator. Keep raw text for explicit ticker syntax.
    pattern=r'\b(?:NASDAQ|NYSE|AMEX|OTC)(?=\s*:|[- ]listed\b)|\bNasdaq(?:\s+(?:Composite|100))?(?=\s+(?:faces|falls|rises|gains|loses|heads|slides|slumps|sinks)\b)'
    from .names import PUBLISHERS,AGENCIES,_masked_token_starts,_TOKEN
    masked=_masked_token_starts(text)
    text=_TOKEN.sub(lambda m:' '*len(m.group()) if m.start() in masked else m.group(),text)
    # Mask the complete government code before hyphen normalization, so
    # IRS-CI cannot leave a stand-alone CI behind.
    text=re.sub(r'\b(?:'+ '|'.join(re.escape(x.upper()) for x in AGENCIES)+r')(?:-[A-Z]{2,8})+\b',lambda m:' '*len(m.group()),text)
    # A local crime headline's resident qualifier is a place, not an issuer.
    text=re.sub(r'^(?:[A-Z][A-Za-z]+\s+){1,6}(?=(?:Man|Woman|Resident)\s+(?:Pleads|Sentenced|Charged|Indicted|Arrested)\b)',lambda m:' '*len(m.group()),text)
    text=re.sub(r'(?<=[A-Za-z])-(?=[A-Za-z])',' ',text)
    for phrase in PUBLISHERS:
        text=re.sub(r'\b'+re.escape(phrase)+r'\b',lambda m:' '*len(m.group()),text,flags=re.I)
    text=re.sub(r'\b(?:'+ '|'.join(re.escape(x.upper()) for x in AGENCIES)+r')\b',lambda m:' '*len(m.group()),text)
    text=re.sub(r'\b(?:Nasdaq|NYSE|AMEX)(?=\s+(?:minimum\s+bid|listing|continued\s+listing|delisting|deficiency|compliance)\b)',lambda m:' '*len(m.group()),text,flags=re.I)
    text=re.sub(r'\b(?:Nasdaq|NYSE|AMEX)(?=\s+(?:approv\w*|grant\w*|notif\w*|allow\w*|accept\w*)\b.{0,100}\b(?:split|listing|compliance|bid|deficiency)\b)',lambda m:' '*len(m.group()),text,flags=re.I)
    return re.sub(pattern,lambda m:' '*len(m.group()),text,flags=re.I)


@lru_cache(maxsize=10000)
def _corporate_usage(alias: str, text: str, allow_object: bool = True) -> bool:
    text=_identity_text(text)
    name=r'[\W_]+'.join(map(re.escape,alias.split()))
    for m in re.finditer(r'\b'+name+r'\b',text,re.I):
        before=text[max(0,m.start()-25):m.start()]
        after=text[m.end():]
        if re.search(r'\b(?:to|will|would|could|should|can|may|must)\s*$',before,re.I):continue
        if re.match(r'\s+(?:(?:city|local|county|school)\s+)?(?:police|sheriff|council|senator|governor)\b',after,re.I):return False
        if re.match(r'\s+(?:flight attendants?|pilots?|workers?|staffers?)\b',after,re.I):return True
        tail=re.sub(r"^['\u2019]s\s+",'',after)
        tail=re.sub(r'^\s+(?:also|just|recently|reportedly|now)\s+',' ',tail,flags=re.I)
        words=list(TOKEN.finditer(tail))
        if words:
            first=words[0].group();base=lemma(first)
            if first[0].isupper() and first.lower().endswith('ing') and re.match(r'\s*:',tail[words[0].end():]):continue
            if base in verb_lemmas() and re.search(r'(?:,|\band)\s*$',before,re.I):return True
            if after.startswith(("'s",'\u2019s')) and first.lower().endswith('s') and len(words)>1 and lemma(words[1].group()) in verb_lemmas():return True
            if first.lower()=='to' and len(words)>1 and lemma(words[1].group()) in verb_lemmas():return True
            if first.lower() in {'will','would','could','should','can','may','must'} and len(words)>1 and lemma(words[1].group()) in verb_lemmas():return True
            if base in verb_lemmas() and (base in verb_only_lemmas() or first.lower() in IRREGULAR or (len(words)>1 and first.lower().endswith(('s','ed','ing')))):return True
            if len(words)>1 and first[0].isupper():
                second=words[1].group()
                if lemma(second) in verb_lemmas() and second.lower().endswith(('s','ed','ing')):return True
        if allow_object and alias.lower() not in english_words() and re.search(r'\b(?:acquir\w*|buy\w*|purchas\w*)\s+(?:[A-Za-z-]+\s+){0,3}$',before,re.I):return True
        if allow_object and len(alias.split())>1 and re.search(r'\b(?:replac\w*|switch\w*)\b.{0,60}\b(?:with|from|to)\s*$',text[:m.start()],re.I):return True
        if alias.lower() not in english_words() and after.startswith(("'s",'\u2019s')) and re.search(r'\b(?:approv\w*|recall\w*)\s*$',before,re.I):return True
    if re.search(r'\b'+re.escape(alias)+r"['\u2019]s\s+\$[\d.,]+\s+(?:tender|cash)\s+offer\b",text,re.I):return True
    pattern=r'\b'+name+r"(?:['\u2019]s)?\s+(?:(?:quarterly|annual|fiscal|preliminary)\s+)?(?:reports?|reported|announces?|announced|raises?|raised|cuts?|cut|denies|denied|acquires?|acquired|launch\w*|unveil\w*|invest\w*|files?|earnings|revenue|shares|stock|guidance|inc|corp|corporation|ltd|plc|(?:tender|cash)\s+offer|moves?\s+to\s+(?:overturn|appeal|block)|waves?\s+goodbye|(?:[A-Za-z-]+\s+){0,2}(?:executives?|employees|president|CEO|CFO|chief|board)|driver|flying)\b"
    for m in re.finditer(pattern,text,re.I):
        if not re.search(r'\b(?:to|will|would|could|should|can|may|must)\s*$',text[max(0,m.start()-20):m.start()],re.I):
            return True
    object_roles=r'against|accuses?|accused|clears?|cleared|sues?|sued|fines?|fined|judgment for|verdict for|ruling for'
    if allow_object:object_roles+='|acquisition of|tender offer for'
    return bool(re.search(r'\b(?:'+object_roles+r')\s+(?:[A-Za-z-]+\s+){0,2}'+name+r'\b',text,re.I) or re.search(r'\b'+name+r'\s+(?:completes?|completed)\s+(?:an?\s+|\$[\d.,]+\s+(?:billion|million)\s+)?acquisition\b',text,re.I))


def _short_surface(word: str, text: str, industry: str = '') -> bool:
    """A legal-name fragment cannot be buried in a different proper name."""
    for m in re.finditer(r'\b'+re.escape(word)+r'\b',text,re.I):
        before=text[:m.start()];after=text[m.end():]
        # A possessive proper-name prefix binds the following noun phrase:
        # Zedcloud's Data Centers is not a short name for Data Holdings.
        owned_phrase=re.match(r'\s+([A-Z][A-Za-z]+)\s+([A-Za-z]+)',after)
        if re.search(r"\b[A-Z][A-Za-z0-9-]*['\u2019]s\s*$",before) and owned_phrase and lemma(owned_phrase.group(1)) not in verb_only_lemmas() and lemma(owned_phrase.group(2)) in verb_lemmas():continue
        previous=re.search(r'([A-Za-z]+)\s*$',before)
        if previous and previous.group(1)[0].isupper():
            p=previous.group(1).lower()
            if p in {'partners','resources','services','technologies','industries','metals','energy','holdings','bancorp'}:continue
            if p not in {'of','in','at','by','for','with','against','and','to'} and not (lemma(p) in verb_lemmas() and (lemma(p)!=p or p in verb_only_lemmas())):continue
        following=re.match(r'\s+([A-Z][A-Za-z]+)',after)
        if following:
            raw=following.group(1);w=raw.lower()
            typed_business=lemma(w) in set(tokens(industry)) and _corporate_usage(word,text)
            if not typed_business and w not in {'stock','shares','earnings','revenue','profit','guidance','inc','corp','corporation','ltd','plc','to','will','would','can','could','may','must','also','just','now'} and not (lemma(w) in verb_lemmas() and (lemma(w)!=w or w in verb_only_lemmas())):continue
        return True
    return False


def _issuer_family(board, tickers) -> bool:
    """Share classes may resolve together only with the same sourced issuer."""
    signatures=set()
    for ticker in tickers:
        p=board.profiles[ticker]
        signatures.add((p.name.casefold(),p.industry,tuple(sorted(s['sha256'] for s in p.sources))))
    return len(signatures)==1


def _broad_business_event(text: str) -> bool:
    # An unnamed local incident does not affect every company that happens
    # to sell the object involved. Sector context stays at tier 3 unless the
    # article states a policy, constraint, or change in a business activity.
    if re.search(r'\b(?:FDA|recall\w*|approv\w*|clearance|lawsuit|charg\w*|arson|indict\w*)\b',text,re.I):
        return False
    activity_noun=bool(re.search(r'\b(?:launches|reentries)\s+(?:and|of|from|at|are|were|have|continue|doubled?|expand\w*|increas\w*|decreas\w*|rise|fall)\b',text,re.I))
    # Locate a subject/predicate boundary using the pinned verb inventory,
    # including headline infinitives and modals. A fixed list of verbs
    # misses ordinary forms such as "Zedchem to Expand ...".
    actor=None
    words=list(TOKEN.finditer(text))
    for i in range(1,min(7,len(words))):
        raw=words[i].group();base=lemma(raw)
        previous=words[i-1].group().lower()
        finite=base in verb_lemmas() and (base in verb_only_lemmas() or raw.lower() in IRREGULAR or raw.lower()!=base)
        infinitive=previous in {'to','will','would','could','should','can','may','must'} and base in verb_lemmas()
        if not (finite or infinitive):continue
        end=words[i-1].start() if infinitive else words[i].start()
        subject=text[:end].strip()
        if re.fullmatch(r"(?:The\s+)?[A-Z][A-Za-z0-9&'-]*(?:\s+(?:[A-Z][A-Za-z0-9&'-]*|of|the)){0,4}",subject):
            actor=subject
            break
    if actor and not activity_noun and not re.search(r'\b(?:government|congress|senate|house|department|ministry|commission|regulators?|legislature|council|court|president|administration|FDA|EPA|FCC|FTC|SEC|policy|legislation|industry|production|capacity|demand|supply|shipments|costs?|prices?|shortages?|launch(?:es)?|reentr(?:y|ies))\b',actor,re.I):
        # One named actor changing its own rules is not evidence of a rule
        # affecting every company with overlapping business vocabulary.
        return False
    broad=bool(re.search(r'\b(?:policy|legislation|legislative|tariffs?|sanctions?|bans?|restrict\w*|export controls?|regulations?|directives?|shortages?|supply disruptions?|shipments|production|capacity|demand|input costs?|commodity prices?)\b',text,re.I))
    return broad or activity_noun


def _independent_words(matched: set[str]) -> set[str]:
    return {word for term in matched for word in term.split() if word not in STOP and word not in GENERIC and word not in EVENT_ONLY and word not in CALENDAR}


def _specific_identifier(value: str, fact) -> bool:
    words=value.split()
    return any(w not in english_words() for w in words) or bool(re.search(r'[\u00ae\u2122]|["\u201c]'+re.escape(fact.text)+r'["\u201d]',fact.passage,re.I))


def _product_surface(fact,text):
    # Case still separates colliding owners (Mac/MAC). Entirely lowercase
    # publisher style can resolve only if the normalized owner is unique.
    flags=re.I if text.islower() else 0
    for match in re.finditer(r'\b'+re.escape(fact.text)+r's?\b',text,flags):
        if len(fact.text.split())==1 and fact.text.lower() in english_words():
            if fact.section=='explicit issuer glossary' and not _corporate_usage(fact.text,text):continue
            if re.match(r'\s+(?:of|for|in|at|by|on|with)\b',text[match.end():],re.I) and not _corporate_usage(fact.text,text):continue
            following=re.match(r'\s+([A-Z][A-Za-z]+)',text[match.end():])
            # Family Practice and Quantum Computer are compound nouns, not
            # independent references to a Family or Quantum product brand.
            if following and (lemma(following.group(1)) not in verb_lemmas() or lemma(following.group(1))==following.group(1).lower()):continue
        return True
    return False


def _ownership_evidenced(fact, profile) -> bool:
    # A heading such as "Our products" does not make every named sports
    # league, supplier or regulator beneath it an owned product.
    from .stock_evidence import OWN,RESALE
    if RESALE.search(fact.passage):return False
    if fact.confidence in {'explicit_owned_segment_list','explicit_issuer_brand_list'}:return True
    match=re.search(re.escape(fact.text),fact.passage,re.I)
    if not match:return False
    if re.search(r'["\u201c]'+re.escape(fact.text)+r'["\u201d]\s+refers? to our\b',fact.passage):return True
    prefix=fact.passage[max(0,match.start()-180):match.start()]
    if re.search(r'\boperated by\b',prefix,re.I):return False
    if re.search(r'\b(?:(?:our|its) products?(?: and services?)? (?:include|are)|our product portfolio (?:includes|comprises))\b',prefix,re.I):return True
    ownership=list(OWN.finditer(prefix))
    if not ownership:return False
    tail=prefix[ownership[-1].end():]
    return not re.search(r'\b(?:from|using|through|relating to|based on|compatible with|supplied by|provided by|bets? (?:on|placed)|treatment of|treating|indicated for|targeting)\b',tail,re.I)


def _valid_person(fact) -> bool:
    if re.search(r'\b(?:financial|securities|department|university|company|corporation|partners|holdings|services|technologies|capital|group)\b',fact.text,re.I):
        return False
    name=re.escape(fact.text)
    return bool(re.search(name+r'(?:\s*,?\s*\d{1,2},?)?\s+(?:is|was|serves?|served|has|joined|founded)\b',fact.passage) or
                re.search(r'\b(?:founded by|founder|officer|president|director)\s+'+name+r'\b',fact.passage,re.I) or
                (any(w not in english_words() for w in tokens(fact.text)) and re.search(name+r'.{0,25}\b(?:Chief|President|Director|Founder)\b',fact.passage)))


def _event_object(text: str) -> str:
    match=re.search(r'\b(?:approv\w*|recall\w*)\s+(?:the\s+|a\s+|an\s+|first\s+|new\s+)*(.+)',text,re.I)
    if not match:return ''
    words=tokens(match.group(1),stop=False)[:6]
    index,_=load_senses()
    for i in range(min(3,len(words))):
        for size in (3,2):
            phrase=' '.join(words[i:i+size])
            if len(phrase.split())==size and any(s[3]=='n' for s in index.get(phrase,())):
                return phrase
    return ''


def _identity_matches(board: EvidenceBoard, text: str):
    # Match on canonical token sequences; separate words are never substring aliases.
    words = tokens(text,stop=False)
    for width in (1,2,3,4,5,6):
        for i in range(len(words)-width+1):
            key = ' '.join(words[i:i+width])
            for ticker,kind,ix in board.identities.get(key,()):
                yield ticker,kind,ix,key
            if key.endswith('s'):
                for ticker,kind,ix in board.identities.get(key[:-1],()):
                    if kind=='owned_product':yield ticker,kind,ix,key[:-1]


def _comparison_name(text: str, alias: str) -> bool:
    match = re.search(re.escape(alias),text,re.I) if alias else None
    if not match:
        return False
    if re.match(r'\s+(?:raises?|cuts?|lifts?|lowers?)\b.{0,100}\bprice target\b',text[match.end():],re.I):return True
    if re.match(r'\s+(?:maintain\w*|reiterat\w*|initiat\w*|upgrad\w*|downgrad\w*)\b.{0,100}\b(?:rating|buy|sell|hold|overweight|underweight|outperform|neutral)\b',text[match.end():],re.I):return True
    if re.match(r'\s*:',text[match.end():]) and re.search(r'\b(?:profitability|price target|rating|overweight|outperform|underweight)\b',text[match.end():],re.I):return True
    prefix = text[max(0,match.start()-50):match.start()]
    local_prefix = re.split(r'[,;.!?]', prefix)[-1]
    if re.search(r'\b(?:engages?|engaged|hires?|hired|retains?|retained|appoints?|appointed)\s*$',local_prefix,re.I) and re.search(r'\b(?:advis\w*|refinanc\w*|restructur\w*)\b',text,re.I):return True
    if re.match(r'\s+(?:says?|said|reports?|reported|analysts?\s+(?:say|said))\b',text[match.end():],re.I) and re.search(r'[,;]\s*$',prefix):return True
    if re.match(r'.{0,45}\b(?:built|made|developed|launched)\b.{0,35}\b(?:years?|decades?|centuries) ago\b',text[match.end():],re.I):return True
    return bool(COMPARISON.search(local_prefix) or re.search(r'\b(?:according to|reported by|reports? from|former|formerly|previously|historically)\s*$',local_prefix,re.I))


def _is_subject(record, text: str, *, partial: bool = False) -> bool:
    raw_text=text
    text=_identity_text(text)
    if not has_event(text) or _comparison_name(text,record.alias):
        return False
    if AGENCY_ICE.search(text) and record.ticker == 'ICE':
        return False
    if FOREIGN_EXCHANGE.search(text) and record.alias.lower() not in text.lower() and record.name.lower() not in text.lower():
        return False
    # A bare all-caps English ticker is not identity unless the passage has
    # a financial context. Explicit exchange/$ syntax remains strong.
    explicit = bool(re.search(r'(?:\$|(?:NYSE|NASDAQ|AMEX)\s*:\s*)'+re.escape(record.ticker)+r'\b',raw_text,re.I))
    name_pattern = r'[\W_]+' .join(re.escape(word) for word in record.alias.split())
    # A symbol bound to another issuer is explicit negative identity proof.
    if name_pattern and re.search(r'\b'+name_pattern+r'\s*\(\s*(?:(?:NYSE|NASDAQ)\s*:\s*)?(?!'+re.escape(record.ticker)+r'\b)[A-Z]{1,6}\s*\)',text):return False
    full_name = bool(name_pattern and any(not m.start() or text[m.start()-1] not in '.&-' for m in re.finditer(r'\b'+name_pattern+r'\b',text,re.I)))
    if full_name and all(w in english_words() for w in record.alias.split()):
        # A dictionary-word company needs an actual corporate predicate.
        # Merely listing "guidance or strategy" is not Strategy Inc.
        raw_company=any(m.group()[0].isupper() for m in re.finditer(r'\b'+name_pattern+r'\b',text,re.I))
        lower_financial=text.islower() and bool(re.search(r'\b(?:stock|shares|earnings|revenue|dividend)\b',text))
        full_name = (raw_company or lower_financial) and _corporate_usage(record.alias,text)
        if len(record.alias.split())>1:
            senses,_=load_senses()
            proper_phrase=any(all(w.group()[0].isupper() or w.group().lower() in {'of','the','and'} for w in TOKEN.finditer(m.group())) for m in re.finditer(r'\b'+name_pattern+r'\b',text,re.I))
            if record.alias not in senses and proper_phrase and not partial:full_name=True
        if len(record.alias.split())==1:
            for m in re.finditer(r'\b'+name_pattern+r'\b',text,re.I):
                before=text[:m.start()];after=text[m.end():]
                if re.search(r'\b(?:\d+|one|two|three|four|five|six|seven|eight|nine|ten|many|several|more|fewer)\s*$',before,re.I):full_name=False
                if re.search(r'\b[A-Z][A-Za-z]+\s*$',before) and not re.search(r'\b(?:at|by|of|for|with|against|sues|acquires|buy|buys)\s*$',before,re.I):full_name=False
                following=re.match(r'\s+([A-Z][A-Za-z]+)',after)
                if following and (lemma(following.group(1)) not in verb_lemmas() or lemma(following.group(1))==following.group(1).lower()) and following.group(1).lower() not in {'stock','shares','earnings','revenue','profit','guidance','inc','corp','corporation','ltd','plc','to','will','would','can','could','may','must','also','just','now'}:full_name=False
    if record.alias and re.search(r'(?i:\b'+name_pattern+r'\s+of\s+)[A-Z]',text) and ' of ' not in record.alias:
        full_name=False
    # A parenthesized symbol is identity only when bound to this name.
    first=record.alias.split()[0] if record.alias else ''
    bound=bool(first and re.search(r'\b(?:'+name_pattern+'|'+re.escape(first)+r')\s*\(\s*'+re.escape(record.ticker)+r'\s*\)',text,re.I))
    explicit=explicit or bound
    if record.alias and re.search(r'\b'+name_pattern+r'\s+(?:raises?|cuts?|lifts?|lowers?)\b.{0,100}\bprice target\b',text,re.I):return False
    bare_financial = False
    for match in re.finditer(r'\b'+re.escape(record.ticker)+r'\b',text):
        if match.start() and text[match.start()-1] in '.&-':
            continue
        after = text[match.end():match.end()+65]
        before = text[max(0,match.start()-45):match.start()]
        if re.match(r"(?:'s)?\s+(?:stock|shares|earnings|revenue|profit|guidance|dividend|buyback|merger|acquisition|bankruptcy|layoffs|corp|corporation|inc|ltd|limited|plc)\b",after,re.I) or re.search(r'\b(?:shares of|stock in|earnings from|buyback by)\s*$',before,re.I):
            bare_financial = True
    if record.ticker.lower() not in english_words() and FINANCIAL_EVENT.search(text) and re.search(r'\b'+re.escape(record.ticker)+r'\b',text) and _short_surface(record.ticker,text) and _corporate_usage(record.ticker,text,False):
        bare_financial=True
    if not (explicit or full_name or bare_financial):
        return False
    return True


def link_evidence(legacy_board, articles: list[dict], *, names=None, profiles=None, **unused) -> list[dict]:
    from .correlation import _name_index
    board = getattr(legacy_board,'evidence_board',None)
    if board is None:
        rows = getattr(legacy_board,'source_rows',None)
        if rows is None:
            raise ValueError('evidence engine requires build_board source rows')
        board = compile_board(rows)
        legacy_board.evidence_board = board
    if names is None:
        # The legacy board may omit stocks with thin descriptions. Identity
        # resolution must still see every stock in the evidence catalog.
        from .correlation import _alias
        from .names import Name, NameIndex
        names = NameIndex([Name(p.ticker,p.name,_alias(p.name),p.industry,p.sector)
                           for p in board.profiles.values()])
    name_index = _name_index(legacy_board,names)
    short_names = defaultdict(list)
    prefix_names = defaultdict(list)
    industry_labels={w for industry in board.industry_members for w in tokens(industry) if w not in STOP|GENERIC}
    for record in name_index.by_ticker.values():
        parts = record.alias.split()
        p=board.profiles.get(record.ticker)
        shell=bool(p and (p.industry=='Shell Companies' or any('blank check company' in f.passage.lower() for f in p.facts[:3])))
        if shell:
            continue
        for size in range(2,min(4,len(parts))+1):
            if parts[size-1] not in STOP:
                prefix_names[tuple(parts[:size])].append(record)
        if parts and len(parts[0]) >= 3 and parts[0] not in GENERIC and parts[0] not in STOP:
            short_names[parts[0]].append(record)
    # A longer catalog stem is ambiguity evidence, never a positive alias
    # derivation: Citi cannot silently stand for Citi Trends or Citigroup.
    competing_stems={word for word in short_names if any(other!=word and other.startswith(word) for other in short_names)}
    output = []
    for article in articles:
        text = str(article.get('text') or article.get('body') or '')
        title = str(article.get('title') or '')
        if title and not text.lstrip().startswith(title):
            text = title + '\n' + text
        ps = passages(text,title)
        quality = input_quality(text,title)
        places = locations_in(text)
        scoped = sorted({t for place in places for t in board.scope.get(place,())})
        named = {}
        rows = {}
        industry_evidence = defaultdict(set)
        roundup = bool(ROUNDUP.search(title or (ps[0].text if ps else '')) or QUOTE_PAGE.search(title or text))
        passage_semantics = {}
        article_date = str(article.get('published_at') or article.get('date') or '')[:10]

        def eligible(fact):
            return not article_date or not fact.filing_date or fact.filing_date <= article_date

        def row_for(ticker):
            if ticker not in rows:
                p = board.profiles.get(ticker)
                record = name_index.by_ticker.get(ticker)
                rows[ticker] = {'ticker':ticker,'name':p.name if p else record.name if record else ticker,
                    'industry':p.industry if p else record.industry if record else '',
                    'sector':p.sector if p else record.sector if record else '',
                    'pass':False,'tier':4,'relation':'inspection','name_hit':False,'alias_hit':False,
                    'off_board':p is None,'jurisdiction_hit':ticker in scoped,
                    'desc_score':0.0,'desc_z':0.0,'desc_pass':False,'ind_pass':False,
                    'terms':[],'bm25':0.0,'evidence':[],'reasons':[]}
            return rows[ticker]

        def pass_row(ticker,tier,route,passage,matched=(),fact=None):
            if fact is not None and not eligible(fact):
                return
            row = row_for(ticker)
            if tier<row['tier']:
                row['relation']=route
            row.update({'pass':True,'tier':min(row['tier'],tier),'desc_pass':True})
            row['terms'] = sorted(set(row['terms']) | set(matched))
            row['desc_score'] = max(row['desc_score'],round(sum((math.log((len(board.profiles)+1)/(board.df[w]+1))+1) *
                (1.0 if w in passage.terms else 0.45) for w in sorted(_independent_words(set(matched)))),4))
            ev = {'channel':route,'article_passage':passage.text,'article_ordinal':passage.ordinal,
                  'matched_terms':sorted(matched)}
            ev['semantic_expansion'] = passage_semantics.get(passage.ordinal,[])
            if fact is not None:
                ev['stock_evidence'] = fact.record()
            if ev not in row['evidence']:
                row['evidence'].append(ev)
            if route not in row['reasons']:
                row['reasons'].append(route)

        for passage in ps:
            literal_query = set(passage.terms)
            event_object=_event_object(passage.text)
            query,semantic_origin,semantic_evidence = expand(literal_query)
            passage_semantics[passage.ordinal] = semantic_evidence
            identity_text=_identity_text(passage.text)
            mentions = name_index.mentioned(identity_text)
            for m in re.finditer(r'(?:\$|(?:NYSE|NASDAQ|AMEX)\s*:\s*)([A-Za-z]{1,5})\b',passage.text,re.I):
                explicit_record=name_index.by_ticker.get(m.group(1).upper())
                if explicit_record and explicit_record not in mentions:mentions.append(explicit_record)
            coordinated=set()
            coordination=None
            for candidate in TOKEN.finditer(identity_text):
                if lemma(candidate.group()) in verb_lemmas() and candidate.start()>0:
                    prefix=re.sub(r'\s+to\s*$','',identity_text[:candidate.start()].strip())
                    if ',' in prefix or ' and ' in prefix.lower():
                        coordination=prefix
                        break
            if coordination:
                subjects=[s.strip() for s in re.split(r',\s*|\s+and\s+',coordination) if s.strip()]
                resolved=[]
                for subject in subjects:
                    group=short_names.get(subject.lower(),())
                    exact=[r for r in group if r.alias==subject.lower()]
                    if exact:group=exact
                    if not group or not _issuer_family(board,[r.ticker for r in group]):break
                    resolved.extend(group)
                else:
                    if len(subjects)>=2:
                        coordinated={r.ticker for r in resolved}
                        for record in resolved:
                            if record not in mentions:mentions.append(record)
            matched_prefixes=defaultdict(list)
            raw_words=re.findall(r'[A-Za-z0-9]+',identity_text)
            raw_starts=[m.start() for m in re.finditer(r'[A-Za-z0-9]+',identity_text)]
            lower_words=[w.lower() for w in raw_words]
            if passage.event:
                for i in range(len(raw_words)):
                    if raw_starts[i] and passage.text[raw_starts[i]-1] in '.&-':continue
                    for size in (2,3,4):
                        prefix=tuple(lower_words[i:i+size])
                        records=prefix_names.get(prefix,())
                        if not records or not _issuer_family(board,[r.ticker for r in records]) or not raw_words[i][0].isupper():continue
                        if ' '.join(prefix) in places and size<len(records[0].alias.split()):continue
                        if i+size+1<len(lower_words) and lower_words[i+size]=='of' and raw_words[i+size+1][0].isupper() and ' of ' not in records[0].alias:continue
                        for record in records:
                            matched_prefixes[record.ticker].append(' '.join(prefix))
                            if record not in mentions:mentions.append(record)
            # Common-word brands (Apple, Booking) and unique short legal
            # names need financial context, not a global capitalization rule.
            qualified_short=set()
            if passage.event:
                for word in sorted(set(w.lower() for w in re.findall(r'[A-Za-z0-9]+',identity_text))):
                    records = short_names.get(word,())
                    if not records:continue
                    family=_issuer_family(board,[r.ticker for r in records])
                    for record in records:
                        if not _short_surface(word,identity_text,record.industry):continue
                        p = board.profiles.get(record.ticker)
                        definition=re.search(r'\b'+re.escape(word)+r'\s+(?:is|becomes|remains)\s+(.{0,55}?)(?=\b(?:to|that|which|with)\b|[,.;:]|$)',passage.text,re.I)
                        if p and definition:
                            declared=set(tokens(definition.group(1)))&industry_labels
                            if declared and not (declared&set(tokens(p.industry))):continue
                        unrelated_name_words=set(tokens(record.alias,stop=False))|{w for place in places for w in tokens(place,stop=False)}
                        short_evidence=_independent_words(literal_query & p.core)-unrelated_name_words if p else set()
                        industry_support=_independent_words(literal_query & board.industry_terms.get(p.industry,set()))-unrelated_name_words if p else set()
                        business = bool(p and len(short_evidence)>=2 and len(industry_support)>=2)
                        legal = bool(re.search(r'\b'+re.escape(word)+r'\s+(?:company|corp|inc|plc|ltd)\b',passage.text,re.I))
                        discriminating=short_evidence-set().union(*(board.profiles[r.ticker].core for r in records if r.name!=record.name))
                        exact_alias=record.alias==word and _is_subject(record,passage.text)
                        if not family and not (discriminating or exact_alias):continue
                        single_legal_word = len(record.alias.split()) == 1
                        common = word in english_words()
                        dictionary,_=load_senses()
                        # Dictionary membership alone includes adjectives and
                        # agency abbreviations. Require an overt modal or a
                        # corporate personnel role before accepting a common
                        # legal prefix without ordinary noun senses.
                        overt_role=bool(re.search(r'\b'+re.escape(word)+r'\s+(?:may|must|will|would|could|should|can|employees|executives|CEO|CFO|staff|workers)\b',identity_text,re.I))
                        mixed_case=bool(re.search(r'\b'+re.escape(word[:1].upper()+word[1:].lower())+r'\b',identity_text))
                        distinctive_predicate=overt_role and mixed_case and not any(s[3]=='n' for s in dictionary.get(word,())) and _corporate_usage(word,passage.text)
                        bound_short=_is_subject(replace(record,alias=''),passage.text) or bool(re.search(r'\b'+re.escape(word)+r'\s*\(\s*'+re.escape(record.ticker)+r'\s*\)',passage.text,re.I))
                        sourced_alias=any(t==record.ticker and k=='company_alias' for t,k,ix in board.identities.get(' '.join(tokens(word,stop=False)),()))
                        if common and sourced_alias and not _is_subject(replace(record,alias=word),passage.text):continue
                        if common and not single_legal_word and ' of ' not in record.alias and re.search(r'(?i:\b'+re.escape(word)+r'\s+of\s+)[A-Z]',passage.text):continue
                        coordinated_financial=bool(re.search(r'\b'+re.escape(word)+r'\s*(?:,|and\b).{0,50}\bshares?\s+(?:are\s+)?(?:fall\w*|ris\w*|down|up)\b',passage.text,re.I))
                        shared_classes=family and len(records)>1 and bool(re.search(r'\b'+re.escape(word)+r'\s+invests?\s+\$',passage.text,re.I))
                        financial_short=False
                        for occurrence in re.finditer(r'\b'+re.escape(word)+r'\b',passage.text,re.I):
                            before=passage.text[:occurrence.start()];after=passage.text[occurrence.end():]
                            previous=re.search(r'([A-Za-z]+)\s*$',before)
                            embedded=previous and previous.group(1)[0].isupper() and not (lemma(previous.group(1)) in verb_lemmas() and previous.group(1).lower()!=lemma(previous.group(1))) and previous.group(1).lower() not in {'of','in','at','by','for','with','against'}
                            if not embedded and occurrence.group()[0].isupper() and re.match(r'\s+(?:stock|shares|earnings|revenue|profit|dividend|(?:will|could|may)\s+\w+\s+\$)\b',after,re.I):financial_short=True
                        acquisition_code=family and bool(re.search(r'\b'+re.escape(word.upper())+r'\b',passage.text)) and _corporate_usage(word,passage.text) and bool(re.search(r'\bacquisition\s+of\b',passage.text,re.I))
                        if common and not single_legal_word and not (legal or bound_short or sourced_alias or business or (not family and discriminating) or coordinated_financial or shared_classes or acquisition_code or financial_short or distinctive_predicate):continue
                        if not common and word in competing_stems and not (legal or bound_short or sourced_alias or business or _corporate_usage(word,passage.text)):continue
                        predicate = _is_subject(record,passage.text)
                        if common and single_legal_word and not (predicate or record.ticker in coordinated):continue
                        short_predicate = _corporate_usage(word,passage.text)
                        proper = any(m.group()[0].isupper() for m in re.finditer(r'\b'+re.escape(word)+r'\b',passage.text,re.I))
                        if common and not proper and not (passage.text.islower() and re.search(r'\b(?:stock|shares|earnings|revenue|dividend)\b',passage.text)):continue
                        financial_list_name=proper and bool(re.search(r'\bshares?\s+(?:are\s+)?(?:fall\w*|ris\w*|down|up)\b',passage.text,re.I))
                        if not family and not (business or discriminating or exact_alias):continue
                        if word in places and not business and record.alias!=word:continue
                        abbreviation=not single_legal_word and len(word)<=5 and bool(re.search(r'\b'+re.escape(word.upper())+r'\b',passage.text))
                        if abbreviation and not (business or legal or acquisition_code or _corporate_usage(word,passage.text,False) or _is_subject(replace(record,alias=''),passage.text)):continue
                        possessive=bool(re.search(r'\b'+re.escape(word)+r"['\u2019]s\b",identity_text,re.I))
                        if not (business or legal or (proper and not common and family and (single_legal_word or FINANCIAL_EVENT.search(passage.text) or possessive)) or short_predicate or financial_list_name or (single_legal_word and FINANCIAL_EVENT.search(passage.text) and (not common or predicate))):
                            continue
                        qualified_short.add(record.ticker)
                        if record not in mentions:
                            mentions.append(record)
            for record in mentions:
                first=record.alias.split()[0] if record.alias else ''
                if first and re.search(r'(?i:\b'+re.escape(first)+r'\s*\(\s*(?:(?:NYSE|NASDAQ)\s*:\s*)?)(?!'+re.escape(record.ticker)+r'\b)[A-Z]{1,6}\s*\)',passage.text):continue
                if record.alias==record.ticker.lower() and any(other.ticker!=record.ticker and other.alias and re.search(r'\b'+re.escape(other.alias)+r'\s+'+re.escape(record.ticker)+r'\b',identity_text,re.I) for other in mentions):continue
                aliases=[record.alias]+matched_prefixes[record.ticker]
                first=record.alias.split()[0] if record.alias else ''
                if first:aliases.append(first)
                spans=[m.span() for alias in aliases if alias for m in re.finditer(r'\b'+r'[\W_]+'.join(map(re.escape,alias.split()))+r'\b',identity_text,re.I)]
                longer=[m.span() for other in mentions if other.ticker!=record.ticker for alias in [other.alias]+matched_prefixes[other.ticker] if alias for m in re.finditer(r'\b'+r'[\W_]+'.join(map(re.escape,alias.split()))+r'\b',identity_text,re.I)]
                if spans and all(any(a<=start and end<=b and b-a>end-start for a,b in longer) for start,end in spans):
                    continue
                # Do not retain a proven cross-exchange or agency collision as
                # a factual name hit, even if it would fail the pass gate.
                if record.ticker == 'ICE' and AGENCY_ICE.search(passage.text):
                    continue
                if FOREIGN_EXCHANGE.search(passage.text) and not (record.alias and record.alias.lower() in passage.text.lower()):
                    continue
                named[record.ticker] = record
                row_for(record.ticker)['name_hit'] = True
                first = record.alias.split()[0] if record.alias else ''
                shortened = bool(record.ticker in qualified_short and not _comparison_name(passage.text,first))
                prefix_subject=any(_is_subject(replace(record,alias=prefix),passage.text,partial=True) for prefix in matched_prefixes[record.ticker])
                financial_list=bool(re.search(r'\bshares?\s+(?:are\s+)?(?:fall\w*|ris\w*|down|up)\b',passage.text,re.I) and (re.search(r'\b'+re.escape(record.ticker)+r'\b',passage.text) or (first and len(short_names.get(first,()))==1 and re.search(r'\b'+re.escape(first)+r'\b',passage.text,re.I))))
                if not roundup and (_is_subject(record,passage.text) or (passage.event and (shortened or financial_list or prefix_subject or record.ticker in coordinated))):
                    pass_row(record.ticker,1,'direct_event',passage)

            hits = defaultdict(set)
            for term in sorted(query):
                for ticker in sorted(board.postings.get(term,())):
                    hits[ticker].add(term)
            for industry,vocab in board.industry_terms.items():
                shared = query & vocab
                if len(_independent_words(shared)) >= 2:
                    industry_evidence[industry].update(shared)
            identity_hits = list(_identity_matches(board,identity_text))
            # A named product recall/approval cannot be spread to every firm
            # sharing its broad product class. Unknown brand names remain
            # unknown until the stock evidence actually supports that name.
            focus=set()
            if re.search(r'\b(?:recall|FDA\s+approv\w*|licens\w*)\b',passage.text,re.I):
                focus={w.lower() for w in re.findall(r'\b[A-Z][A-Z0-9-]{4,}\b',passage.text)
                       if w.lower() not in english_words() and w.lower() not in {'nasdaq','nyse','usfda'}}
            for ticker,kind,ix,key in identity_hits:
                p = board.profiles[ticker]
                fact=p.facts[ix]
                if kind in ('company_alias','derived_initialism','operating_brand') and _comparison_name(passage.text,fact.text or key):continue
                if kind=='acronym':
                    continue
                if kind=='person' and (not _valid_person(fact) or not re.search(r'\b'+re.escape(fact.text.split()[-1])+r'\b',passage.text)):
                    continue
                if kind=='person':
                    from .entity_resolution import person_fact_alignment
                    if not person_fact_alignment(board,p,fact,passage.text,key):continue
                if kind=='person' and key in english_words() and not re.search(r'\b'+re.escape(fact.text.split()[0])+r'\s+(?:[A-Za-z]+\.?\s+){0,2}'+re.escape(fact.text.split()[-1])+r'\b',identity_text):continue
                if kind=='owned_product' and not _product_surface(fact,passage.text):
                    continue
                if kind=='owned_product' and re.fullmatch(r'[A-Z]{2,5}',fact.text) and not re.search(r'[\u00ae\u2122]|["\u201c]'+re.escape(fact.text)+r'["\u201d]|\('+re.escape(fact.text)+r'\)',fact.passage):
                    continue
                direct_subjects={t for t,r in rows.items() if {'direct_event','company_alias_event','source_identity_event','owned_product_event'}&set(r['reasons'])}
                if kind=='owned_product' and ticker not in direct_subjects and any(set(key.split()) <= (set(tokens(name_index.by_ticker[t].name,stop=False))|{t.lower()}) for t in direct_subjects if t in name_index.by_ticker):
                    continue
                if kind=='company_alias' and fact.text and any(w in english_words() for w in key.split()):
                    if not re.search(r'\b'+re.escape(fact.text)+r'\b',passage.text):continue
                    if len(key.split())==1 and not _is_subject(replace(name_index.by_ticker[ticker],alias=fact.text.lower()),passage.text):continue
                if kind=='company_alias' and not fact.text and any(w in english_words() for w in key.split()):
                    raw=next((v for v in re.findall(r'["\u201c]([^"\u201d]+)["\u201d]',fact.passage) if ' '.join(tokens(v,stop=False))==key),'')
                    if not raw or not re.search(r'\b'+re.escape(raw)+r'\b',passage.text):
                        continue
                    if len(key.split())==1 and not FINANCIAL_EVENT.search(passage.text):
                        continue
                row = row_for(ticker)
                row['alias_hit'] = True
                matched = hits.get(ticker,set())
                extra = _independent_words(matched) - set(key.split())
                unique_owner = {t for t,k,i in board.identities.get(key,()) if k == kind} == {ticker}
                owners={t for t,k,i in board.identities.get(key,()) if k==kind}
                if owners and _issuer_family(board,owners):unique_owner=True
                if kind=='owned_product':
                    owners={t for t,k,i in board.identities.get(key,()) if k==kind and eligible(board.profiles[t].facts[i]) and _product_surface(board.profiles[t].facts[i],identity_text) and _ownership_evidenced(board.profiles[t].facts[i],board.profiles[t])}
                    unique_owner=bool(owners) and _issuer_family(board,owners)
                if not eligible(p.facts[ix]):
                    continue
                if kind in ('derived_initialism','operating_brand'):
                    raw=bool(re.search(r'\b'+re.escape(fact.text)+r'\b',passage.text))
                    corroboration=_independent_words(literal_query & p.core)-set(tokens(p.name,stop=False))-set(key.split())
                    corroboration={w for w in corroboration if board.df[w]<=max(3,0.08*len(board.profiles))}
                    if kind=='derived_initialism':
                        corroboration={w for w in corroboration if board.df[w]<=max(3,0.08*len(board.profiles))}
                    financial_subject=kind=='derived_initialism' and len(fact.text)>=4 and bool(re.search(r'\b'+re.escape(fact.text)+r"(?:\s+['\u2018][^'\u2019]+['\u2019])?\s+(?:posts?|posted|reports?|reported|announces?|announced|beats?|beat)\b.{0,70}\b(?:profit|earnings|revenue)\b",passage.text))
                    brand_subject=_is_subject(replace(name_index.by_ticker[ticker],alias=fact.text.lower()),passage.text)
                    brand_possessive=bool(re.search(r'\b'+re.escape(fact.text)+r"['\u2019]s\b",identity_text))
                    dictionary,_=load_senses()
                    ordinary_noun=any(s[3]=='n' for s in dictionary.get(key,()))
                    strong_brand=(fact.confidence in {'legal_segment_brand','segment_product_family'} and not ordinary_noun) or key not in english_words()
                    if kind=='operating_brand' and key in english_words() and not (_corporate_usage(fact.text,passage.text) or (strong_brand and brand_possessive)):continue
                    if kind=='operating_brand' and not strong_brand and len(corroboration)<2:continue
                    distinctive_brand=kind=='operating_brand' and (_corporate_usage(fact.text,passage.text) or (strong_brand and brand_possessive)) and _short_surface(fact.text,identity_text)
                    if unique_owner and raw and (corroboration or financial_subject or distinctive_brand) and passage.event and not roundup and (kind=='operating_brand' or FINANCIAL_EVENT.search(passage.text) or re.search(r'\b(?:report|beat.and.raise)\b',passage.text,re.I)):
                        pass_row(ticker,1,'source_identity_event',passage,{key}|corroboration,fact)
                elif kind=='company_alias' and unique_owner and passage.event and not roundup:
                    pass_row(ticker,1,'company_alias_event',passage,{key},p.facts[ix])
                elif kind == 'owned_product' and unique_owner and (_specific_identifier(key,p.facts[ix]) or fact.confidence in {'explicit_owned_segment_list','explicit_issuer_brand_list'} or re.search(r'\b(?:our|its) products?(?: and services?)? include\b',fact.passage,re.I)) and _ownership_evidenced(p.facts[ix],p) and passage.event and not roundup:
                    # Product ownership is a sourced relation; the event is
                    # independently present in the article's same passage.
                    pass_row(ticker,1,'owned_product_event',passage,{key},p.facts[ix])
                elif kind == 'subsidiary' and unique_owner and passage.event and not roundup:
                    pass_row(ticker,2,'subsidiary_event',passage,{key},p.facts[ix])
                elif kind in ('person','acronym','subsidiary') and passage.event and not roundup and not focus:
                    rare_extra={w for w in extra if board.df[w]<=max(3,0.08*len(board.profiles))}
                    full_person=kind=='person' and bool(re.search(r'\b'+re.escape(fact.text.split()[0])+r'\s+(?:[A-Za-z]+\.?\s+){0,2}'+re.escape(fact.text.split()[-1])+r"['\u2019]s\s+(?:[A-Za-z]+\s+){0,2}(?:company|business|firm|empire)\b",identity_text,re.I))
                    if len(rare_extra)<2 and not (full_person and rare_extra):continue
                    pass_row(ticker,2,'alias_with_business_evidence',passage,matched,p.facts[ix])

            for ticker,matched in sorted(hits.items()):
                p = board.profiles[ticker]
                direct_subjects={t for t,r in rows.items() if {'direct_event','company_alias_event','source_identity_event','owned_product_event'}&set(r['reasons'])}
                if passage.event and not roundup:
                    candidate_facts = Counter()
                    for word in sorted(_independent_words(query)):
                        # Every exposure route below requires two rare anchors.
                        # Common words cannot qualify, so do not materialize
                        # their millions of risk-factor postings per sentence.
                        if board.df[word]>max(3,0.08*len(board.profiles)):
                            continue
                        candidate_facts.update(board.exposures.get(ticker,{}).get(word,()))
                    for ix, count in sorted(candidate_facts.items()):
                        if count < 2:
                            continue
                        fact = p.facts[ix]
                        if not eligible(fact):
                            continue
                        if direct_subjects and ticker not in direct_subjects and fact.target_ticker not in direct_subjects:
                            continue
                        shared = query & terms(fact.passage)
                        units = _independent_words({semantic_origin.get(t,t) for t in shared})
                        if len(units) < 2 or not (units & _independent_words(p.core)):
                            continue
                        if focus and not focus <= set(tokens(fact.passage,stop=False)):
                            continue
                        if event_object and event_object not in terms(fact.passage):
                            continue
                        if len({w for w in units if board.df[w]<=max(3,0.08*len(board.profiles))})<2:
                            continue
                        if places and SCOPE_EVENT.search(passage.text) and not (places & p.locations):
                            continue
                        if fact.kind == 'competitor' and RELATION_CUES['competitor'].search(fact.passage) and re.search(r'\b(?:approv\w*|reject\w*|licens\w*|recall\w*|launch\w*|phase\s*[3III]+|trial)\b',passage.text,re.I):
                            pass_row(ticker,2,'competitive_exposure',passage,shared,fact)
                        elif fact.kind in ('material_exposure','financial_exposure','supplier') and re.search(r'\b(?:price\w*|cost\w*|rate\w*|shortage|halt\w*|ban\w*|tariff\w*|disrupt\w*)\b',passage.text,re.I):
                            pass_row(ticker,2,'documented_exposure',passage,shared,fact)
                core_hits = matched & p.core
                direct_subjects={t for t,r in rows.items() if {'direct_event','company_alias_event','source_identity_event','owned_product_event'}&set(r['reasons'])}
                if direct_subjects and ticker not in direct_subjects:
                    # A named issuer's financial result is not an unnamed
                    # industry-wide event. Other firms require an evidenced
                    # competitive/dependency route, evaluated above.
                    continue
                distinct = _independent_words({semantic_origin.get(term,term) for term in core_hits})
                # Two words in a phrase are two words, but a phrase plus its
                # component unigrams never becomes three independent channels.
                if len(distinct) < 2 or not passage.event or roundup:
                    continue
                if not _broad_business_event(passage.text):
                    continue
                # Ignore company-name words when proving an unnamed exposure.
                from .correlation import _alias
                business_words = distinct - set(tokens(_alias(p.name)))
                if len(business_words) < 2:
                    continue
                # A place is a candidate scope. A jurisdiction-limited rule
                # requires evidenced operations in that jurisdiction.
                if places and SCOPE_EVENT.search(passage.text) and not (places & p.locations):
                    continue
                # Rare company words are NOT multiplied by industry coverage.
                # Shared generic words cannot satisfy the two-anchor condition.
                source_words = _independent_words({semantic_origin.get(t,t) for t in core_hits}) - set(tokens(_alias(p.name)))
                place_words = {w for place in places for w in tokens(place,stop=False)}
                anchors = {w for w in source_words-place_words if board.df[w] <= max(3,0.08*len(board.profiles))}
                if len(anchors) < 2:
                    continue
                if not (anchors & _independent_words(literal_query)):
                    continue
                # At least one stock-side passage must support both anchors;
                # unrelated words scattered through a 10-K cannot combine.
                fact = None
                for word in sorted(core_hits):
                    for i in p.evidence.get(word,()):
                        candidate = p.facts[i]
                        supported=terms(candidate.passage)&core_hits
                        independent=_independent_words({semantic_origin.get(t,t) for t in supported})
                        if candidate.kind=='business' and eligible(candidate) and len(independent & anchors) >= 2 and (not event_object or event_object in terms(candidate.passage)):
                            fact = candidate
                            break
                    if fact:
                        break
                if fact is None:
                    # The portable cache has a compact first-four-context
                    # index. On a miss, inspect every retained business fact
                    # and cache the complete index for this company. No source
                    # passage is silently excluded by that fast-path limit.
                    from .stock_evidence import profile_terms
                    full_indexes=getattr(board,'full_business_indexes',None)
                    if full_indexes is None:
                        full_indexes={};board.full_business_indexes=full_indexes
                    if ticker not in full_indexes:
                        postings=defaultdict(list)
                        for i,candidate in enumerate(p.facts):
                            if candidate.kind=='business':
                                for term in profile_terms(candidate.passage):postings[term].append(i)
                        full_indexes[ticker]=postings
                    indices={i for word in core_hits for i in full_indexes[ticker].get(word,())}
                    for i in sorted(indices):
                        candidate=p.facts[i]
                        supported=terms(candidate.passage)&core_hits
                        independent=_independent_words({semantic_origin.get(t,t) for t in supported})
                        if eligible(candidate) and len(independent & anchors)>=2 and (not event_object or event_object in terms(candidate.passage)):
                            fact=candidate;break
                if fact is None:
                    continue
                if focus and not focus <= set(tokens(fact.passage,stop=False)):
                    continue
                row = row_for(ticker)
                row['ind_pass'] = True
                pass_row(ticker,2,'business_exposure',passage,core_hits,fact)

        # A dependency is material only with an actual disruptive event and
        # an explicit sole/critical dependency in the filing, not just a name.
        dependencies = []
        for target_ticker in named:
            for ticker,ix in board.dependencies.get(target_ticker,()):
                p = board.profiles[ticker]
                fact = p.facts[ix]
                for passage in ps:
                    target = named[fact.target_ticker]
                    if not (target.alias and re.search(r'\b'+re.escape(target.alias)+r'\b',passage.text,re.I)):
                        continue
                    if target.industry not in industry_evidence and not _is_subject(target,passage.text):
                        continue
                    dep = {'ticker':ticker,'partner':fact.text,'role':fact.kind,'order':2,'second_order':True,
                        'section':fact.section,'stock_evidence':fact.record()}
                    if dep not in dependencies:
                        dependencies.append(dep)
                    if fact.kind in ('supplier','customer') and re.search(r'\b(?:sole|single[- ]source|exclusive|critical|material)\b',fact.passage,re.I) and DISRUPTION.search(passage.text):
                        pass_row(ticker,2,'material_dependency_event',passage,{fact.text},fact)

        industries = []
        for industry,matched in sorted(industry_evidence.items()):
            industries.append({'industry':industry,'score':len(_independent_words(matched)),'z':0.0,
                'pass':True,'tier':3,'terms':sorted(matched),'relation':'sector_context'})
        for row in rows.values():
            row['ind_pass'] = row['ind_pass'] or row['industry'] in industry_evidence
            row['evidence'].sort(key=lambda e:(e['article_ordinal'],e['channel'],str(e['matched_terms']),e.get('stock_evidence',{}).get('accession',''),e.get('stock_evidence',{}).get('passage','')))
        actual = sorted(rows.values(),key=lambda r:(not r['pass'],r['tier'],not r['name_hit'],-r['desc_score'],r['ticker']))
        linked = any(r['pass'] for r in actual)
        reason = 'linked' if linked else 'mention_only' if named else 'insufficient_text' if quality['unique_content_tokens'] < 4 else 'no_supported_stock_link'
        output.append({'id':article.get('id'),'title':title,'url':article.get('url',''),'source':article.get('source',''),
            'status':'scored' if actual or industries else 'unknown','decision':reason,
            'input_quality':quality,'stocks':actual,'industries':industries,'dependencies':dependencies,
            'scoped':scoped,'bundles':[],'absorbed':[],
            'engine':'evidence-v2','profile_fingerprint':board.source_fingerprint,'semantic_resources':load_senses()[1],
            'profile_coverage':{'companies':len(board.profiles),'with_warnings':sum(bool(p.warnings) for p in board.profiles.values())}})
    return output

