"""Evidence-based article-to-stock decisions, independent of batch composition.

Scores are diagnostics, not probabilities. A pass names a concrete evidence
route: direct company event, owned-product event, or evidenced exposure.
"""
from __future__ import annotations

import math
import re
from collections import defaultdict, Counter

from .aliases import extract_jurisdictions
from .linguistics import COMPARISON, DISRUPTION, EVENT, FINANCIAL_EVENT, GENERIC, STOP, input_quality, passages, terms, tokens
from .stock_evidence import EvidenceBoard, compile_board, locations_in, RELATION_CUES
from .semantic_lookup import expand,load_senses
from .names import english_words

ROUNDUP = re.compile(r'\b(?:market wrap|stocks? to watch|top \d+ stocks?|stock picks?|stocks? roundup|analyst picks?|cruise winners)\b', re.I)
AGENCY_ICE = re.compile(r'\bICE\b.{0,100}\b(?:immigration|deport\w*|arrest\w*|detain\w*|enforcement)\b|\b(?:immigration|customs enforcement)\b.{0,100}\bICE\b', re.I)
FOREIGN_EXCHANGE = re.compile(r'\b(?:HOSE|HNX|UPCOM|BSE|NSE|ASX|LSE|TSE|HKEX)\s*[:(]|\b(?:Vietnam|Vietnamese|Kosy)\b', re.I)
SCOPE_EVENT = re.compile(r'\b(?:tariff|sanction|ban|restrict\w*|regulat\w*|policy|permit|price differential|constitutional)\b', re.I)
EVENT_ONLY = frozenset('recall approve approval reject rejection earnings revenue guidance report result class governor secretary minister senator president spokeswoman spokesman'.split())


def _independent_words(matched: set[str]) -> set[str]:
    return {word for term in matched for word in term.split() if word not in STOP and word not in GENERIC and word not in EVENT_ONLY}


def _specific_identifier(value: str, fact) -> bool:
    words=value.split()
    return any(w not in english_words() for w in words) or bool(re.search(r'[\u00ae\u2122]|["\u201c]'+re.escape(fact.text)+r'["\u201d]',fact.passage,re.I))


def _ownership_evidenced(fact, profile) -> bool:
    # A heading such as "Our products" does not make every named sports
    # league, supplier or regulator beneath it an owned product.
    from .stock_evidence import OWN
    match=re.search(re.escape(fact.text),fact.passage,re.I)
    if not match:return False
    prefix=fact.passage[max(0,match.start()-180):match.start()]
    if re.search(r'\b(?:our products? (?:include|are)|our product portfolio (?:includes|comprises))\s*$',prefix,re.I):return True
    ownership=list(OWN.finditer(prefix))
    if not ownership:return False
    tail=prefix[ownership[-1].end():]
    return not re.search(r'\b(?:from|using|through|relating to|based on|compatible with|supplied by|provided by|bets? (?:on|placed))\b',tail,re.I)


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


def _comparison_name(text: str, alias: str) -> bool:
    match = re.search(re.escape(alias),text,re.I) if alias else None
    if not match:
        return False
    prefix = text[max(0,match.start()-50):match.start()]
    local_prefix = re.split(r'[,;.!?]', prefix)[-1]
    return bool(COMPARISON.search(local_prefix) or re.search(r'\b(?:according to|reported by|reports? from|formerly|previously|historically)\s*$',local_prefix,re.I))


def _is_subject(record, text: str) -> bool:
    if not EVENT.search(text) or _comparison_name(text,record.alias):
        return False
    if AGENCY_ICE.search(text) and record.ticker == 'ICE':
        return False
    if FOREIGN_EXCHANGE.search(text) and record.alias.lower() not in text.lower() and record.name.lower() not in text.lower():
        return False
    # A bare all-caps English ticker is not identity unless the passage has
    # a financial context. Explicit exchange/$ syntax remains strong.
    explicit = bool(re.search(r'(?:\$|(?:NYSE|NASDAQ|AMEX)\s*:\s*)'+re.escape(record.ticker)+r'\b',text,re.I))
    name_pattern = r'[\W_]+' .join(re.escape(word) for word in record.alias.split())
    full_name = bool(name_pattern and re.search(r'\b'+name_pattern+r'\b',text,re.I))
    if full_name and len(record.alias.split()) == 1 and record.alias.lower() in english_words():
        # A dictionary-word company needs an actual corporate predicate.
        # Merely listing "guidance or strategy" is not Strategy Inc.
        full_name = bool(re.search(r'\b'+re.escape(record.alias)+r"(?:'s)?\s+(?:(?:quarterly|annual|fiscal|preliminary)\s+)?(?:reports?|reported|announces?|announced|raises?|raised|cuts?|cut|denies|denied|acquires?|acquired|earnings|revenue|shares|stock|guidance|inc|corp|corporation|ltd|plc)\b",text,re.I))
    bare_financial = False
    for match in re.finditer(r'\b'+re.escape(record.ticker)+r'\b',text):
        after = text[match.end():match.end()+65]
        before = text[max(0,match.start()-45):match.start()]
        if re.match(r"(?:'s)?\s+(?:stock|shares|earnings|revenue|profit|guidance|dividend|buyback|merger|acquisition|bankruptcy|layoffs|corp|corporation|inc|ltd|limited|plc)\b",after,re.I) or re.search(r'\b(?:shares of|stock in|earnings from|buyback by)\s*$',before,re.I):
            bare_financial = True
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
    for record in name_index.by_ticker.values():
        parts = record.alias.split()
        if parts and len(parts[0]) >= 4 and parts[0] not in GENERIC and parts[0] not in STOP:
            short_names[parts[0]].append(record)
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
        roundup = bool(ROUNDUP.search(title or (ps[0].text if ps else '')))
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
            row.update({'pass':True,'tier':min(row['tier'],tier),'relation':route,'desc_pass':True})
            row['terms'] = sorted(set(row['terms']) | set(matched))
            row['desc_score'] = round(sum((math.log((len(board.profiles)+1)/(board.df[w]+1))+1) *
                (1.0 if w in passage.terms else 0.45) for w in _independent_words(set(matched))),4)
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
            mentions = name_index.mentioned(passage.text)
            # Common-word brands (Apple, Booking) and unique short legal
            # names need financial context, not a global capitalization rule.
            if passage.event:
                for word in set(tokens(passage.text,stop=False)):
                    records = short_names.get(word,())
                    if len(records) == 1 and re.search(r'\b'+re.escape(word)+r"(?:'s)?\b",passage.text,re.I):
                        p = board.profiles.get(records[0].ticker)
                        unrelated_name_words=set(tokens(records[0].alias,stop=False))|{w for place in places for w in tokens(place,stop=False)}
                        short_evidence=_independent_words(literal_query & p.core)-unrelated_name_words if p else set()
                        industry_support=_independent_words(literal_query & board.industry_terms.get(p.industry,set()))-unrelated_name_words if p else set()
                        business = bool(p and len(short_evidence)>=2 and len(industry_support)>=2)
                        legal = bool(re.search(r'\b'+re.escape(word)+r'\s+(?:company|corp|inc|plc|ltd)\b',passage.text,re.I))
                        single_legal_word = len(records[0].alias.split()) == 1
                        common = word in english_words()
                        predicate = _is_subject(records[0],passage.text)
                        if not (business or legal or (single_legal_word and FINANCIAL_EVENT.search(passage.text) and (not common or predicate))):
                            continue
                        if records[0] not in mentions:
                            mentions.append(records[0])
            for record in mentions:
                # Do not retain a proven cross-exchange or agency collision as
                # a factual name hit, even if it would fail the pass gate.
                if record.ticker == 'ICE' and AGENCY_ICE.search(passage.text):
                    continue
                if FOREIGN_EXCHANGE.search(passage.text) and not (record.alias and record.alias.lower() in passage.text.lower()):
                    continue
                named[record.ticker] = record
                row_for(record.ticker)['name_hit'] = True
                if not roundup and _is_subject(record,passage.text):
                    pass_row(record.ticker,1,'direct_event',passage)

            hits = defaultdict(set)
            for term in query:
                for ticker in board.postings.get(term,()):
                    hits[ticker].add(term)
            for industry,vocab in board.industry_terms.items():
                shared = query & vocab
                if len(_independent_words(shared)) >= 2:
                    industry_evidence[industry].update(shared)
            identity_hits = list(_identity_matches(board,passage.text))
            # A named product recall/approval cannot be spread to every firm
            # sharing its broad product class. Unknown brand names remain
            # unknown until the stock evidence actually supports that name.
            focus=set()
            if re.search(r'\b(?:recall|FDA\s+approv\w*|licens\w*)\b',passage.text,re.I):
                focus={w.lower() for w in re.findall(r'\b[A-Z][A-Z0-9-]{4,}\b',passage.text)
                       if w.lower() not in english_words() and w.lower() not in {'nasdaq','nyse','usfda'}}
            for ticker,kind,ix,key in identity_hits:
                p = board.profiles[ticker]
                row = row_for(ticker)
                row['alias_hit'] = True
                matched = hits.get(ticker,set())
                extra = _independent_words(matched) - set(key.split())
                unique_owner = {t for t,k,i in board.identities.get(key,()) if k == kind} == {ticker}
                if not eligible(p.facts[ix]):
                    continue
                if kind=='person' and (not _valid_person(p.facts[ix]) or not re.search(r'\b'+re.escape(p.facts[ix].text.split()[-1])+r'\b',passage.text)):
                    continue
                if kind=='acronym':
                    # A filing-defined law, regulator or disease abbreviation
                    # is not a corporate identity. Keep it for inspection;
                    # business exposure must establish its own source route.
                    continue
                if kind=='owned_product' and any(w in english_words() for w in key.split()) and not re.search(r'\b'+re.escape(p.facts[ix].text)+r'\b',passage.text):
                    continue
                if kind=='company_alias' and unique_owner and passage.event and not roundup:
                    pass_row(ticker,1,'company_alias_event',passage,{key},p.facts[ix])
                elif kind == 'owned_product' and unique_owner and _specific_identifier(key,p.facts[ix]) and _ownership_evidenced(p.facts[ix],p) and passage.event and not roundup:
                    # Product ownership is a sourced relation; the event is
                    # independently present in the article's same passage.
                    pass_row(ticker,1,'owned_product_event',passage,{key},p.facts[ix])
                elif kind == 'subsidiary' and unique_owner and passage.event and not roundup:
                    pass_row(ticker,2,'subsidiary_event',passage,{key},p.facts[ix])
                elif kind in ('person','acronym','subsidiary') and passage.event and len({w for w in extra if board.df[w]<=max(3,0.08*len(board.profiles))}) >= 2 and not roundup and not focus:
                    pass_row(ticker,2,'alias_with_business_evidence',passage,matched,p.facts[ix])

            for ticker,matched in hits.items():
                p = board.profiles[ticker]
                direct_subjects={t for t,r in rows.items() if 'direct_event' in r['reasons']}
                if passage.event and not roundup:
                    candidate_facts = Counter()
                    for word in _independent_words(query):
                        # Every exposure route below requires two rare anchors.
                        # Common words cannot qualify, so do not materialize
                        # their millions of risk-factor postings per sentence.
                        if board.df[word]>max(3,0.08*len(board.profiles)):
                            continue
                        candidate_facts.update(board.exposures.get(ticker,{}).get(word,()))
                    for ix, count in candidate_facts.items():
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
                direct_subjects={t for t,r in rows.items() if 'direct_event' in r['reasons']}
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
                industry_words = _independent_words({semantic_origin.get(t,t) for t in query & board.industry_terms.get(p.industry,set())})
                if len(industry_words) < 2:
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
