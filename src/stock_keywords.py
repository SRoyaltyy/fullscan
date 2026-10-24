"""Dated stock -> relationship -> keyword records shared by profile and article views.

Keyword overlap is retrieval evidence. It never independently creates a stock
pass or a bullish/bearish conclusion. All candidate claims remain inspectable.
"""
from collections import defaultdict
from functools import lru_cache
import hashlib
import json
import math
import re

from .linguistics import lemma
from .entity_resolution import VERSION as ALIGNMENT_VERSION, POLICY, STOP, aliases, identity, decide, validate_annotations, surface

# Hyphens separate words, but offsets always refer to the unmodified article.
TOKEN = re.compile(r"\d+(?:[./-]\d+)*|[A-Za-z][A-Za-z0-9]*(?:['’][A-Za-z0-9]+)*")

SCHEMA = 'stock-keywords-1.1'
ROLES = {'buys_from':'supplier', 'sells_to':'customer', 'uses_input':'input',
    'depends_on':'dependency', 'competes_with':'competitor', 'partners_with':'partner',
    'licenses':'license', 'offers':'product_or_service', 'subject_to':'exposure',
    'regulated_by':'regulator', 'approval_from':'approval_authority',
    'risk_mentions':'risk', 'exposed_to':'exposure', 'operates_in':'operating_geography',
    'owns_subsidiary':'subsidiary', 'acquires':'acquisition_target',
    'divests':'divestiture_target', 'differentiation_claim':'differentiation_claim'}
ROLES['has_officer']='officer'
ROLES.update({'provides_employee_benefit':'employee_benefit',
    'holds_financial_instrument':'financial_instrument',
    'issues_financial_instrument':'financial_instrument',
    'financing_counterparty':'financing_counterparty',
    'transaction_counterparty':'transaction_counterparty',
    'benefit_recipient':'benefit_recipient',
    'mentions_clinical_trial':'clinical_trial',
    'mentions_product_platform':'product_platform',
    'mentions_transaction_asset':'transaction_asset',
    'mentions_legal_reference':'legal_reference',
    'mentions_governance_body':'governance_body',
    'unresolved_relationship':'unresolved_relationship'})
ENTITY_ROLES = {'issuer','supplier','customer','competitor','partner','subsidiary',
                'regulator','approval_authority','acquisition_target','divestiture_target'}
LEGAL_SUFFIX = re.compile(r'(?:[, ]+(?:incorporated|corporation|company|limited|holdings|corp|inc|ltd|plc|llc)\.?)+$',re.I)
AUDITOR = re.compile(r'\b(?:as part of our audits?|we (?:have )?audited|our responsibility is to express|we are required to obtain an understanding)\b',re.I)
ADMIN = re.compile(r'^(?:(?:written|prior|reasonable|timely|additional|certain)\s+)*(?:notices?|updates?|information|documents?|disclosures?|consents?|reports?|statements?|assurances?|opinions?|understanding)$',re.I)
UNRESOLVED = re.compile(r'^(?:which|who|it|them|these|those|one|combination|performance|such .+|(?:many |other |third.party |certain |various )*(?:companies|parties|entities))$',re.I)
METRIC = re.compile(r'\b(?:adjusted EBITDA margin|EBITDA margin|gross margin|operating margin|free cash flow|capital expenditures?|net income|net loss|revenue|revenues)\b',re.I)
GUIDANCE = re.compile(r'(?P<new_low>\d+(?:\.\d+)?)%\s+to\s+(?P<new_high>\d+(?:\.\d+)?)%\s*,?\s*(?P<change>raised|lowered)\s+from\s+(?P<old_low>\d+(?:\.\d+)?)%\s+to\s+(?P<old_high>\d+(?:\.\d+)?)%',re.I)
CLAIM_TERMS = re.compile(r'\b(?:intellectual property|trade secrets?|network effects?|switching costs?|cost advantages?|patents?|proprietary technology|trademarks?|copyrights?)\b',re.I)


def normalized_words(value):
    return tuple(lemma(m.group()) for m in TOKEN.finditer(value or ''))


# A retrieval/display contract, not a ticker-specific list or an accuracy score.
# Complete source decisions remain in the archive, including excluded claims.
SCANNER_POLICY = 'concrete-connections-2'
RELATION_LABELS = {
    'employee_benefit': ('Employee benefits', 'provides employee benefit'),
    'financial_instrument': ('Financial instruments', 'reports financial instrument'),
    'financing_counterparty': ('Financing counterparties', 'financing transaction with'),
    'transaction_counterparty': ('Asset transaction counterparties', 'asset transaction with'),
    'benefit_recipient': ('Employee benefits', 'provides benefits to'),
    'clinical_trial': ('Clinical trials', 'mentions clinical trial'),
    'product_platform': ('Product platforms', 'mentions platform'),
    'transaction_asset': ('Transaction assets', 'mentions transaction asset'),
    'legal_reference': ('Legal references', 'mentions legal reference'),
    'governance_body': ('Corporate governance', 'mentions internal body'),
    'unresolved_relationship': ('Unresolved relationships', 'relationship needs review'),
    'supplier': ('Upstream suppliers', 'buys from'),
    'customer': ('Downstream customers', 'sells to'),
    'partner': ('Business partners', 'partners with'),
    'dependency': ('Operating dependencies', 'relies on'),
    'input': ('Upstream inputs', 'uses as an input'),
    'product_or_service': ('Products and downstream outputs', 'offers'),
    'competitor': ('Competitors', 'competes with'),
    'subsidiary': ('Subsidiaries', 'owns'),
    'acquisition_target': ('Acquisitions', 'acquires'),
    'divestiture_target': ('Divestitures', 'divests'),
    'officer': ('Company officers', 'has officer'),
    'regulator': ('Government institutions', 'is regulated by'),
    'approval_authority': ('Government institutions', 'requires approval from'),
    'regulatory_exposure': ('Laws and government institutions', 'reports exposure to'),
    'disclosed_authority': ('Government institutions', 'reports dealings with'),
    'disclosed_law': ('Laws and policies', 'reports legal exposure to'),
    'disclosed_permit': ('Permits and licences', 'reports permit or licence'),
    'disclosed_named_permit': ('Permits and licences', 'reports permit or licence'),
    'disclosed_named_case': ('Named legal proceedings', 'reports legal proceedings involving'),
    'operating_geography': ('Operating jurisdictions', 'operates in'),
}
_BACKGROUND_WORDS = set('''a an the our their its all any such other certain various
    new additional internal external own third party parties business businesses
    partner partners customer customers consumer consumers member members client clients
    supplier suppliers vendor vendors agency agencies advertising marketing lead leads
    system systems service services product products offering offerings technology technologies
    information data process processes procedure procedures operation operations
    company companies entity entities institution institutions government governmental
    law laws regulation regulations rule rules risk risks litigation matter matters
    examination examinations audit audits requirement requirements obligation obligations
    approval approvals permit permits licence licences license licenses authority authorities
    ability assurance reasonable understanding performance value variety range success
    experience experiences quality superior continued inaccurate representation representations
    number numbers forecast notice notices written statement statements oral securities
    shareholder shareholders holder holders underwriter underwriters executive participant
    benefit benefits agreement agreements time conversion exchange issue issues exercise
    content games security payment payments right rights tools model models proprietary
    innovation technological confidential necessary required respective applicable
    full valuable assistance financing opinion opinions amount equal several hedging
    national federal state local global international major minor relevant regulator regulators
    counterparty counterparties loan purchaser purchasers person persons'''.split())
_BACKGROUND_WORDS.update('effect effects amendment termination trustee trustees agent agents firm shares common stock'.split())
_BACKGROUND_WORDS.update('''key personnel staff employees workforce talent core primary
    material materials raw manufactured component components part parts replacement
    software application applications computer computers channel channels indirect
    strategic distribution complex worldwide supply chain production original originals
    document documents corporate network networks market markets condition conditions
    economic numerous extensive portfolio note notes interest interests purchased
    associated consolidated financial operating partnership general sole'''.split())
_BACKGROUND_WORDS.update('''largest developer developers programmer programmers
    multiple outsourced provider providers adequate alternative replacements existing
    statutes brands strong driven strategies group compute asset assets intangible
    insurance inventory pound pounds dollar dollars euro euros currency currencies
    percent percentage up three shares venture ventures manufacturing cdn cdns waf
    ic audit committee board directors'''.split())
_BACKGROUND_WORDS.update('automation trade secrets interconnection'.split())
_BACKGROUND_WORDS.update('''property properties qualified vessel vessels significant
    restricted entity entities income net liability umbrella excess needed supplies
    protection level portion four buyer buyers investor investors each combination
    business contributed surplus premium chief legal officer executive dealer
    notifies feedstock loan payable receivable capital hardware equipment
    results adjusted non gaap eps quantity quantities specified target transaction
    transactions negotiated teaming arrangements organizations collaborators
    independent regional carrier carriers single source manufacturers contract
    manufacturing patents patent intellectual research foreign cmo cmos cro cros'''.split())
_BACKGROUND_WORDS.update('convertible comprehensive individual training programs courses knowledge resources change management saas'.split())
_FILING_FRAGMENT = re.compile(r'\b(?:exhibits?\s+[\dA-Z]|financial statements?|table of contents|corporate documents|signature pages?|percentage|certificates? of|opinion of counsel)\b|\n\s*\n', re.I)
_CLASS_ONLY = re.compile(r'^(?:LLC|Inc\.?|Ltd\.?|PLC|OEMs?|ODMs?|REITs?|TRSs?|USD|ISO|SOC|[kMG]W(?:h)?|kW/MW)$', re.I)
_NON_INPUT = re.compile(r'\b(?:disasters?|earthquakes?|hurricanes?|pandemics?|recessions?|inflation|economic conditions|disruptions?|uncertainty)\b', re.I)
_BAD_OBJECT = re.compile(r"^(?:[a-z]\)|unresolved\b|(?:we|it|they|this|that|these|those|such|our|their|its|any|all|most)\b)|\b(?:of|with|to|and|or|subject)$", re.I)
_ABSTRACT_HEAD = re.compile(r'\b(?:benefits?|assurances?|understanding|assistance|representations?|numbers?|notices?|agreements?|information|limitations?|matters?|risks?|requirements?)$', re.I)
_ENTITY_CLASS = re.compile(r'\b(?:manufacturers?|operators?|agencies|providers?|institutions?|counterparties)$', re.I)


@lru_cache(maxsize=1)
def _physical_input_terms():
    """Pinned WordNet classes admit materials without a commodity allowlist."""
    import gzip
    from pathlib import Path
    root = Path(__file__).with_name('data')/'senses'
    manifest = json.loads((root/'manifest.json').read_text())
    terms = set()
    for entry in manifest['files']:
        if entry['file'] not in {name+'.json.gz' for name in ('noun.substance','noun.artifact','noun.food','noun.plant','noun.animal')}:
            continue
        raw = (root/entry['file']).read_bytes()
        if entry.get('stored_sha256') and hashlib.sha256(raw).hexdigest() != entry['stored_sha256']:
            raise ValueError('physical input resource checksum mismatch')
        raw = gzip.decompress(raw)
        if hashlib.sha256(raw).hexdigest() != entry['sha256']:
            raise ValueError('physical input resource checksum mismatch')
        for _, names, _, _ in json.loads(raw):
            terms.update(normalized_words(name) for name in names)
    return terms


def scanner_policy(row):
    """Admit identifiable names/citations or qualified operating inputs only.

    Plurals are allowed: distinctiveness and relationship capacity matter.
    Generic participant classes, abstract assertions and malformed arguments
    cannot be repaired merely by capitalizing them or assigning an entity type.
    """
    role, value = row['relationship'], row['keyword'].strip()
    q = row.get('qualifiers') or {}
    group, label = RELATION_LABELS.get(role, ('', ''))
    reason = ''
    words = re.findall(r'[A-Za-z0-9]+', value)
    distinctive = [w for w in words if w.lower() not in _BACKGROUND_WORDS]
    issues = set(row.get('review_reasons', ()))
    if row.get('assertion') == 'negated' or q.get('applicability') == 'explicitly_out_of_scope':
        reason = 'explicitly_no_connection'
    elif not group:
        reason = 'no_specific_operating_relationship'
    elif _FILING_FRAGMENT.search(value) or _CLASS_ONLY.fullmatch(value):
        reason = 'filing_fragment_or_unnamed_class'
    elif role in ('input', 'dependency') and _NON_INPUT.search(value):
        reason = 'risk_event_is_not_an_operating_input'
    elif role == 'input' and re.search(r'\b(?:percent|shares|assets?|ventures?|calls?)\b', value, re.I):
        reason = 'financial_or_organizational_object_is_not_an_upstream_input'
    elif role in ('disclosed_law', 'regulatory_exposure') and row.get('object_type') == 'law' and (
            re.search(r'\b(?:and|or|,)\s+Regulation$', value) or
            (re.search(r'\bRegulations?$', value) and len(distinctive) < 2)):
        reason = 'incomplete_or_unspecified_legal_instrument'
    elif not distinctive or _BAD_OBJECT.search(value) or _ABSTRACT_HEAD.search(value) or len(words) > 14:
        reason = 'generic_class_abstract_or_fragment'
    elif issues & {'generic_background_head', 'administrative_or_abstract_object',
                   'unresolved_or_generic_object', 'auditor_voice_is_not_issuer',
                   'object_requires_phrase_resolution'}:
        reason = 'unsupported_argument_or_voice'
    elif role in ENTITY_ROLES and (
            (_ENTITY_CLASS.search(value) and sum(w[0].isupper() for w in distinctive) < 2)
            or re.fullmatch(r'[A-Z]{2,}s', value)):
        reason = 'participant_class_without_named_entity'
    elif role in ENTITY_ROLES and row.get('object_type') in ('geography', 'location', 'GPE'):
        reason = 'place_is_not_a_business_counterparty'
    else:
        named = any(w[0].isupper() and len(w) > 1 for w in distinctive)
        citation = bool(re.search(r'\d', value)) and row.get('object_type') == 'law'
        declared = bool(row.get('source_declared_identity'))
        qualified_input = role in ('input', 'dependency') and len(distinctive) >= 2
        # Existing typed concept definitions (e.g. computing versus weather)
        # retain their separate context/identity gate at article comparison.
        typed_input = role in ('input', 'dependency') and any(
            normalized_words(value) in {normalized_words(a) for a in rule['aliases']} for rule in POLICY['concepts'])
        physical_input = role in ('input', 'dependency') and normalized_words(value) in _physical_input_terms()
        if not (named or citation or declared or qualified_input or typed_input or physical_input):
            reason = 'unnamed_or_unqualified_object'
    if not reason and role in ENTITY_ROLES:
        from .specific_filing_records import PLACES, LAW
        stripped = re.sub(r'^the\s+', '', value, flags=re.I)
        if PLACES.fullmatch(stripped):
            reason = 'place_is_not_a_business_counterparty'
        elif row.get('object_type') == 'law' or (row.get('identity') or {}).get('type') == 'LAW' or LAW.fullmatch(stripped):
            reason = 'law_is_not_a_business_counterparty'
        elif surface(value) in (row.get('identity') or {}).get('owner_aliases', ()):
            reason = 'issuer_is_not_its_own_counterparty'
    if not reason and role in ('input', 'dependency') and re.fullmatch(r'[A-Z]{1,3}', value):
        reason = 'unqualified_input_abbreviation'
    if not reason and role == 'dependency':
        from .specific_filing_records import PLACES
        if PLACES.fullmatch(re.sub(r'^the\s+', '', value, flags=re.I)):
            reason = 'jurisdiction_is_not_an_operating_dependency'
    if not reason and role in ('regulator', 'approval_authority'):
        from .specific_filing_records import AGENCY
        from .filing_relationships import AGENCIES
        if row.get('object_type') not in ('agency', 'GOV_AGENCY') and value not in AGENCIES and value not in AGENCIES.values() and not AGENCY.fullmatch(re.sub(r'^the\s+', '', value, flags=re.I)):
            reason = 'authority_role_requires_a_named_external_institution'
    if not reason and role == 'product_or_service':
        from .filing_relationships import AGENCIES
        if value in AGENCIES or value in AGENCIES.values() or re.search(r'\b(?:Debentures?|Securities|Note\s+(?:Receivable|Payable)|notifies)\b', value, re.I):
            reason = 'authority_or_filing_instrument_is_not_a_commercial_product'
    if role == 'disclosed_law' and q.get('direct_obligation_asserted'):
        label = 'is subject to'
    elif role == 'disclosed_law' and q.get('applicability') == 'third_party_context':
        label = 'reports indirect policy exposure to'
    if role == 'regulatory_exposure':
        group = 'Laws and policies' if row.get('object_type') == 'law' else 'Government and regulatory institutions'
    if role in ('disclosed_authority', 'regulator', 'approval_authority'):
        group = 'Government and regulatory institutions'
        if re.search(r"\b(?:(?:company[’']s|our|directors?)\s+board|audit committee|board of directors)\b", value, re.I):
            reason = 'internal_governance_is_not_a_government_institution'
    return {'version': SCANNER_POLICY, 'eligible': not reason, 'reason': reason,
            'group': group, 'label': label}


_CAPACITY = {
    'supplier': r'\b(?:suppliers?|vendors?|purchas\w*|buy\w*|sourc\w*)\b',
    'customer': r'\b(?:customers?|clients?|sell\w*|sold|sales|suppl\w*)\b',
    'partner': r'\b(?:partners?|partnership|collaborat\w*|alliances?|joint venture)\b',
    'competitor': r'\b(?:compet\w*|rivals?)\b',
    'subsidiary': r'\b(?:subsidiar\w*|wholly.owned|majority.owned|controlling interest)\b',
    'acquisition_target': r'\b(?:acquir\w*|acquisitions?|purchas\w*|merg\w*)\b',
    'divestiture_target': r'\b(?:divest\w*|sold|sell\w*|sale of)\b',
    'dependency': r'\b(?:rel(?:y|ies|ied)|relian\w*|depend\w*)\b',
    'input': r'\b(?:us(?:e|es|ed|ing)|purchas\w*|buy\w*|sourc\w*|inputs?|raw materials?|components?|suppliers?)\b',
    'product_or_service': r'\b(?:offers?|offering|launch\w*|sell\w*|sold|manufactur\w*|produc\w*|market\w*|products?|services?|platform|technology)\b',
}


@lru_cache(maxsize=256)
def _local_capacity(role, keyword, quote):
    """Require the capacity in the argument's own prose block and sentence.

    Old extraction windows can span risk lists, tables and several paragraphs.
    A customer cue in the next block cannot turn a product table into buyers.
    This validates preserved assertions; it never invents a replacement role.
    """
    pattern = _CAPACITY.get(role)
    if not pattern:
        return True
    from .specific_filing_records import _sentence_spans
    name = re.compile(r'(?<![\w-])'+r'\s+'.join(re.escape(p) for p in keyword.split())+r'(?![\w-])', re.I)
    for block in re.split(r'\n\s*\n|[•●]', quote):
        if not name.search(block):
            continue
        for start, end in _sentence_spans(block):
            sentence = block[start:end]
            mention = name.search(sentence)
            if mention and re.search(pattern, sentence, re.I):
                if re.search(r'\b(?:opinions? expressed|independent investigation|legal opinion|as counsel|we have assumed)\b', sentence, re.I):
                    continue
                before, after = sentence[:mention.start()], sentence[mention.end():]
                if role in ('supplier','customer','partner','dependency') and re.match(r'^\s+(?:partners?|customers?|suppliers?|vendors?|providers?)\b', after, re.I):
                    # A programme/brand modifying a participant class is not
                    # the identity of one of those unnamed participants.
                    continue
                # A model's ORG label is not enough: an object has to be in
                # the named participant list or apposition, not elsewhere in
                # the sentence (e.g. SAT meeting the needs of its customers).
                if role in ('supplier', 'customer', 'partner', 'subsidiary'):
                    head = {'supplier':r'suppliers?|vendors?', 'customer':r'customers?|clients?', 'partner':r'partners?', 'subsidiary':r'subsidiar(?:y|ies)'}[role]
                    list_intro = re.search(r'\b(?:'+head+r')\b[^.!?;]{0,220}?(?:\b(?:include|including|includes|such as|namely|are|were|is|was)\b|:)\s*[^.!?;]*$', before, re.I)
                    apposition = re.match(r'^\s*(?:(?:is|are|was|were|remains?)\s+|,\s*(?=one of|our|a |an ))(?:one of\s+)?(?:the\s+)?(?:(?:our|a|an)\s+)?(?:(?!(?:of|to|for|with|is|are)\b)[A-Za-z-]+\s+){0,4}(?:'+head+r')\b', after, re.I)
                    direct = re.search({'supplier':r'\b(?:purchas\w*|buy\w*|sourc\w*)\b[^.!?;]{0,100}\bfrom\s+(?:the\s+)?$', 'customer':r'\b(?:sell\w*|sold|sales|suppl\w*)\b[^.!?;]{0,100}\bto\s+(?:the\s+)?$', 'partner':r'\b(?:partner\w*|collaborat\w*)\s+with\s+(?:the\s+)?$', 'subsidiary':r'\b(?:wholly.owned|majority.owned)\s+(?:subsidiar(?:y|ies)\s+)?$'}[role], before, re.I)
                    # "one of our customers, Cyxtera Technologies".
                    following_head = re.search(r'\b(?:our|a|an)\s+(?:'+head+r')\s*[,:(]\s*$', before, re.I)
                    if not (list_intro or apposition or direct or following_head):
                        continue
                if role == 'competitor' and re.search(r'\bcompet\w*\s+(?:for|in)\s+(?:capital|investors?|talent|employees?|personnel)\b', sentence, re.I):
                    continue
                if role == 'competitor' and not (re.search(r'\b(?:compet(?:e|es|ed|ing|ition)\b[^.!?;]{0,100}\b(?:with|against|from)|competitors?\b[^.!?;]{0,80}\b(?:include|including|such as|like|are))\b[^.!?;]*$', before, re.I) or re.match(r'^\s*(?:is|are|was|were)\s+(?:one of\s+)?(?:our\s+)?(?:\w+\s+){0,3}competitor', after, re.I)):
                    continue
                if role == 'dependency' and re.search(r'\bfor\s*$', before) and not re.search(r'\b(?:on|upon)\s*$', before):
                    continue
                if role == 'product_or_service' and re.search(r'\b(?:our competitors|foreign exchange|currency|hedg\w*|stock options?|preferred stock|common stock)\b', sentence, re.I):
                    continue
                # Do not cut "Google" off the front of "Google Cloud Platform".
                if re.search(r'\b[A-Z][a-z]+\s+$', before) and keyword[0].isupper() and not re.search(r'\b(?:Our|The|Company|including|as)\s+$', before):
                    continue
                return True
    return False


def scanner_visible(row, article_date=''):
    policy = row.get('scanner') or scanner_policy(row)
    if not policy['eligible']:
        return False
    events = [e for e in policy.get('connection_events', ()) if not article_date or e['date'] <= article_date]
    return bool(events[-1]['connected']) if events else not policy.get('connection_events')


def _negative_connection_witness(row, quote):
    """An explicit issuer negation attached to this named argument only."""
    q = row.get('qualifiers') or {}
    names = {row['keyword'], *q.get('disclosed_aliases', ())}
    names.update((row.get('source_declared_identity') or {}).get('aliases', ()))
    before = re.compile(r'\b(?:we|the company|the registrant|our (?:business|operations?))\s+'
        r'(?:(?:are|is|were|was)\s+(?:unaffected by|not affected by|not subject to|not in scope of|not dependent on)'
        r'|(?:do|does|did)\s+not\s+(?:rely on|depend on|use|purchase from|buy from|sell to|partner with))\s+(?:the\s+)?$',re.I)
    after = re.compile(r'^\s+(?:does|do)\s+not\s+apply\s+to\s+(?:us|the company|the registrant)\b',re.I)
    for name in names:
        for m in re.finditer(r'(?<![\w-])'+re.escape(name)+r'(?![\w-])',quote,re.I):
            if before.search(quote[max(0,m.start()-160):m.start()]) or after.search(quote[m.end():m.end()+100]):
                return True
    return False


def _partial_law_title(row, witnesses):
    """A declaration cannot validate a clipped canonical law name itself."""
    if row.get('object_type') != 'law':
        return False
    canonical=(row.get('source_declared_identity') or {}).get('name', row['keyword'])
    if canonical != row['keyword']:
        return False  # A separately evidenced full-name/short-alias binding.
    argument=r'\s+'.join(re.escape(part) for part in row['keyword'].split())
    raw=re.compile(r'(?<![\w-])'+argument, re.I)
    clipped=[]
    for witness in witnesses:
        for match in raw.finditer(witness['quote']):
            before=witness['quote'][:match.start()]
            after=witness['quote'][match.end():]
            continuation=bool(re.match(r'[\w]', after) or (re.search(r'\d', row['keyword']) and re.match(r'\s*[-‐‑‒–—−]\s*[A-Za-z0-9]', after)))
            prefix=re.search(r'\b([A-Z][A-Za-z]+)\s+(?:(?:with|and|of|the|for|on|in|to)\s+)?$', before)
            title_prefix=bool(re.search(r'\b(?:Act|Law|Directive|Legislation)\b', row['keyword']) and prefix and prefix.group(1) not in {'The','Our','We','Under','For','In','Such','This','That','Section','Article','Act','Acts','Law','Laws','Directive','Regulation','Regulations'})
            clipped.append(continuation or title_prefix)
    return bool(clipped) and all(clipped)


def annotate_scanner_rows(rows, evidence):
    # Explicit negatives close the same named connection, including older
    # positive grammar heads. Same-date contradictions abstain. Date gates
    # retain a supported connection for an article preceding its termination.
    events = defaultdict(dict)
    def key(row):
        name = (row.get('source_declared_identity') or {}).get('name', row['keyword'])
        # A regulator/customer can have several independent roles. Legal
        # exclusions apply across the old and new legal record representations.
        family = 'legal' if row.get('object_type') == 'law' else row['relationship']
        return (row['stock'], surface(name), family)
    for row in rows:
        row['scanner'] = scanner_policy(row)
        # Verify a whole literal argument, rather than a suffix of a hyphenated
        # law, a page heading, or separated words accidentally joined by a rule.
        witnesses = [w for ref in row['evidence_ids'] for w in evidence[ref]['witnesses']]
        if witnesses and all(_negative_connection_witness(row,w['quote']) for w in witnesses):
            row['scanner'].update(eligible=False,reason='explicitly_no_connection')
        declared = bool(row.get('source_declared_identity'))
        validated_detail = any(ref.startswith('specific:') for ref in row['evidence_ids'])
        # A whitespace run is one separator. Turning each padding character
        # into a separate \s+ creates catastrophic backtracking on table text.
        argument = r'\s+'.join(re.escape(part) for part in re.split(r'\s+', row['keyword'].strip()))
        literal = re.compile(r'(?<![\w-])'+argument+r'(?![\w-])')
        reason = ''
        if not witnesses:
            reason = 'missing_source_evidence'
        elif not (declared or validated_detail) and len(re.findall(r'[ \t]{8,}', row['keyword'])) >= 2:
            # Several wide column gaps are layout, not a resolved entity name.
            reason = 'table_layout_fragment_requires_named_identity'
        elif not (declared or validated_detail or any(literal.search(w['quote']) for w in witnesses)):
            reason = 'incomplete_or_nonliteral_argument'
        elif re.match(r'^\d', row['keyword']) and row.get('object_type') != 'law' and not validated_detail:
            reason = 'quantity_or_table_fragment'
        elif row['relationship'] == 'product_or_service' and not any(
                re.search(r'\b(?:offers?|offering|launch\w*|sell\w*|manufactur\w*|produc\w*|market\w*|platform|products?|services?|technology)\b',w['quote'],re.I)
                and not re.search(r'\b(?:divest\w*|sale transaction|planned (?:sale|divestiture))\b',w['quote'],re.I)
                for w in witnesses):
            reason = 'no_commercial_offering_evidence'
        elif row.get('object_type') == 'law' and not declared and any(
                (match := literal.search(w['quote'])) and re.match(r'\s*(?:-[A-Za-z0-9]|\([a-z0-9]|\s+Authority\b)', w['quote'][match.end():])
                for w in witnesses):
            reason = 'partial_legal_instrument_or_institution'
        if reason and row['scanner']['eligible']:
            row['scanner'].update(eligible=False, reason=reason)
        if row['scanner']['eligible'] and _partial_law_title(row, witnesses):
            row['scanner'].update(eligible=False, reason='clipped_canonical_legal_name')
        if row['scanner']['eligible'] and not any(_local_capacity(row['relationship'], row['keyword'], w['quote']) for w in witnesses):
            row['scanner'].update(eligible=False, reason='relationship_not_supported_in_argument_sentence')
        connected = row['scanner']['reason'] != 'explicitly_no_connection'
        if not connected or row['scanner']['eligible']:
            for ref in row['evidence_ids']:
                for w in evidence[ref]['witnesses']:
                    date = w['source']['filing_date']
                    existing = events[key(row)].get(date, True)
                    events[key(row)][date] = existing and connected and not _negative_connection_witness(row,w['quote'])
    for row in rows:
        row['scanner']['connection_events'] = [dict(date=d, connected=v) for d,v in sorted(events[key(row)].items())]


def quality_issues(predicate, value, quote):
    issues = []
    from .specific_filing_records import GENERIC
    if GENERIC.fullmatch(value.strip()):
        issues.append('generic_background_head')
    if AUDITOR.search(quote):
        issues.append('auditor_voice_is_not_issuer')
    if predicate in ('offers','uses_input') and ADMIN.fullmatch(value.strip()):
        issues.append('administrative_or_abstract_object')
    if UNRESOLVED.fullmatch(value.strip()):
        issues.append('unresolved_or_generic_object')
    if predicate == 'competes_with' and re.search(r'\b(?:personnel|talent|developers|engineers|employees|compensation)\b',quote,re.I):
        issues.append('labor_competition_is_not_product_competition')
    return issues


def compile_keywords(graph, *, as_of, ticker=None):
    """Retain every claim; aggregate short keywords without losing dated witnesses."""
    ticker = ticker or graph['ticker']
    sources = {s['id']:s for s in graph['sources']}
    evidence, records = {}, {}
    inventory=graph.get('term_inventory')
    allowed_sources={s['id'] for s in sources.values() if s.get('filing_date') and s['filing_date']<=as_of}
    terms=[t for t in (inventory or {}).get('terms',[]) if t['source_id'] in allowed_sources]
    term_dates={t['id']:sources[t['source_id']]['filing_date'] for t in terms}
    term_links=defaultdict(list)
    for term in terms:
        for link in term['relationships']:term_links[link['relationship_id']].append(term['id'])
    newest = {}
    for fact in graph['facts']:
        key = (fact['predicate'], fact['object'].casefold(), fact['object_type'])
        for ev in fact['evidence']:
            date = sources[ev['source_id']].get('filing_date','')
            if date and date <= as_of:
                order = (date, fact['status'] != 'asserted',fact['id'])
                if key not in newest or order > newest[key][0]:
                    newest[key] = (order, fact['id'])

    def add(keyword, role, predicate, state, reference, *, qualifiers=None, issues=(), current=False, object_type='concept'):
        words = normalized_words(keyword)
        context = json.dumps(qualifiers or {}, sort_keys=True, separators=(',',':'))
        record_key = (ticker, words, role, predicate, state, tuple(issues), current, context,object_type)
        if record_key not in records:
            records[record_key] = {'stock':ticker,'keyword':keyword,'normalized':' '.join(words),'object_type':object_type,
                'relationship':role,'predicate':predicate,'assertion':state,
                'current_as_of_cutoff':current,'review_required':bool(issues),
                'review_reasons':list(issues),'usable_for_scoring':current and state == 'asserted' and not issues,
                'evidence_ids':[], 'qualifiers':qualifiers or {}, 'context_key':context}
        if reference not in records[record_key]['evidence_ids']:
            records[record_key]['evidence_ids'].append(reference)

    for fact in graph['facts']:
        witnesses = [{**ev,'source':sources[ev['source_id']]} for ev in fact['evidence']
                     if sources[ev['source_id']].get('filing_date','') and sources[ev['source_id']]['filing_date'] <= as_of]
        if not witnesses:
            continue
        evidence[fact['id']] = {'subject':ticker,'predicate':fact['predicate'],'object':fact['object'],
            'object_type':fact['object_type'],'assertion':fact['status'], 'witnesses':witnesses,
            'term_occurrence_ids':sorted(set(term_links[fact['id']]))}
        key = (fact['predicate'],fact['object'].casefold(),fact['object_type'])
        current = newest[key][1] == fact['id']
        role = ROLES.get(fact['predicate'],'unclassified_relationship')
        if fact['predicate'] == 'subject_to' and fact['object_type'] in ('agency','law'):
            role = 'regulatory_exposure'
        issues = sorted(set(issue for ev in witnesses for issue in quality_issues(fact['predicate'],fact['object'],ev['quote'])))
        if (fact.get('qualifiers') or {}).get('frame', {}).get('resolution') == 'needs_review':
            issues=sorted(set(issues+['relationship_capacity_unresolved']))
        if graph.get('schema')=='completed-parser-review-1':
            issues=sorted(set(issues+['preserved_relationship_requires_current_method_validation']))
        if 'labor_competition_is_not_product_competition' in issues:
            role = 'talent_competition'
        values = [fact['object']]
        if len(normalized_words(fact['object'])) > 12:
            values = list(dict.fromkeys(m.group() for m in CLAIM_TERMS.finditer(fact['object']))) if role == 'differentiation_claim' else []
            if not values:
                values = ['unresolved '+role.replace('_',' ')]
                issues = sorted(set(issues+['object_requires_phrase_resolution']))
        for value in values:
            add(value,role,fact['predicate'],fact['status'],fact['id'],qualifiers=fact.get('qualifiers'),issues=issues,current=current,object_type=fact['object_type'])
        if fact['predicate'] == 'buys_from':
            inputs = list((fact.get('qualifiers') or {}).get('inputs') or [])
            description = (fact.get('qualifiers') or {}).get('description','')
            match = re.fullmatch(r'(?:our\s+)?(.+?)\s+suppliers?',description,re.I)
            if match and match.group(1).lower() not in ('key','major','main','primary','other','largest'):
                inputs.append(match.group(1))
            for value in dict.fromkeys(inputs):
                add(value,'input','uses_input',fact['status'],fact['id'],qualifiers={'via_supplier':fact['object']},issues=issues,current=current)

    # These are separately versioned, evidence-bound disclosures, not new
    # assertions manufactured from the old generic relationship heads.
    specific=graph.get('specific_records')
    if specific:
        from .specific_filing_records import validate_details
        validate_details(graph,specific)
        for detail in specific['records']:
            if not detail['entity_match_eligible']:
                continue
            source=sources[detail['source_id']]
            if source['id'] not in allowed_sources:
                continue
            reference='specific:'+detail['id']
            evidence[reference]={'subject':ticker,'predicate':detail['relation'],
                'object':detail['entity'],'object_type':'law' if detail['category']=='law' else 'agency' if detail['category']=='authority' else 'concept',
                'assertion':'reported','witnesses':[{**detail['evidence'],'source_id':source['id'],'source':source}],
                'specific_disclosure_id':detail['id'],'paragraph_id':detail['paragraph_id']}
            add(detail['entity'],'disclosed_'+detail['category'],detail['relation'],'reported',reference,
                qualifiers={'disclosure_extractor':specific['version'],'disclosed_aliases':detail['aliases'],
                    'applicability':detail['applicability'],'direct_obligation_asserted':detail['direct_obligation_asserted'],
                    'details':detail['fields'],'missing_fields':detail['missing_fields'],
                    **({'definition_evidence':detail['definition_evidence']} if detail.get('definition_evidence') else {})},
                issues=('disclosure_requires_applicability_and_article_relevance_review',),
                current=False,object_type=evidence[reference]['object_type'])

    # Quantities keep their exact paragraph. Only a fully matched range-change
    # construction is normalized; unrelated table values are never combined.
    for p in graph.get('paragraphs',[]):
        source = sources[p['source_id']]
        if not source.get('filing_date') or source['filing_date'] > as_of or p.get('kind') != 'prose' or not p.get('signals'):
            continue
        metrics = list(dict.fromkeys(m.group() for m in METRIC.finditer(p['text'])))
        if not metrics:
            continue
        reference = 'paragraph:'+p['id']
        evidence[reference] = {'subject':ticker,'predicate':'reports_metric','object':metrics,
            'assertion':'reported','witnesses':[{'source_id':p['source_id'],'start':p['start'],'end':p['end'],'quote':p['text'],'source':source}]}
        change = GUIDANCE.search(p['text'])
        qualifiers = {'reported_quantities':[s['quote'] for s in p['signals']]}
        if change and len(metrics) == 1:
            qualifiers['guidance_change'] = {'unit':'percent','new':[float(change['new_low']),float(change['new_high'])],
                'previous':[float(change['old_low']),float(change['old_high'])], 'direction':change['change'].lower(), 'period':None}
        for metric in metrics:
            add(metric,'financial_metric','reports_metric','reported',reference,qualifiers=qualifiers,
                issues=('metric_period_and_context_require_review',),current=False)
    rows = []
    for key, row in sorted(records.items(),key=lambda item:repr(item[0])):
        row['evidence_ids'].sort()
        row['id'] = hashlib.sha256(json.dumps([key,row['evidence_ids']],sort_keys=True).encode()).hexdigest()
        dates = [w['source']['filing_date'] for ref in row['evidence_ids'] for w in evidence[ref]['witnesses']]
        row['first_filing_date'], row['latest_filing_date'] = min(dates), max(dates)
        row['term_occurrence_ids']=sorted({ident for ref in row['evidence_ids']
            for ident in evidence[ref].get('term_occurrence_ids',[])})
        row['term_occurrence_dates']={ident:term_dates[ident] for ident in row['term_occurrence_ids']}
        rows.append(row)
    declarations=defaultdict(list)
    for row in rows:
        if row['object_type'] not in ('law','agency') or not row['qualifiers'].get('disclosure_extractor'):
            continue
        for alias in row['qualifiers']['disclosed_aliases']:
            declarations[(row['object_type'],surface(alias))].append(row)
    for row in rows:
        candidates=[r for r in declarations.get((row['object_type'],surface(row['keyword'])),())
                    if r['first_filing_date']<=row['first_filing_date']]
        names={surface(r['keyword']) for r in candidates}
        # An explicitly declared alias can connect an old short claim to its
        # complete name. Conflicting names stay unresolved, and a later alias
        # declaration cannot supply historical evidence to an earlier claim.
        if len(names)==1:
            row['source_declared_identity']={'name':candidates[0]['keyword'],
                'aliases':sorted({a for r in candidates for a in r['qualifiers']['disclosed_aliases']}),
                'known_from':min(r['first_filing_date'] for r in candidates),
                'evidence_ids':sorted({ref for r in candidates for ref in r['evidence_ids']})}
    annotate_scanner_rows(rows, evidence)
    stock_evidence=None
    if graph.get('stock_evidence'):
        if set(sources)<=allowed_sources:
            stock_evidence=graph['stock_evidence']
        else:
            from .stock_evidence_graph import build
            stock_evidence=build(graph,as_of=as_of)
    return {'schema':SCHEMA,'as_of':as_of,'records':rows,'evidence':evidence,
        'stock_evidence':stock_evidence,'stock_evidence_review_only':True,
        'term_inventory':None if inventory is None else {**inventory,'terms':terms,
            'windows':[w for w in inventory['windows'] if w['source_id'] in allowed_sources]},
        'coverage':{'raw_relationships':len(graph['facts']),
            'retained_relationships':sum(not k.startswith(('paragraph:','specific:')) for k in evidence),
            'specific_disclosure_records':sum(k.startswith('specific:') for k in evidence),
            'keywords':len(rows),'review_required':sum(r['review_required'] for r in rows),
            'term_inventory_available':inventory is not None,'term_occurrences':len(terms),
            'terms_without_resolved_relationship':sum(not t['relationships'] for t in terms),
            'paragraph_coverage':graph.get('coverage',{})}}


class KeywordIndex:
    def __init__(self, profiles):
        self.first = defaultdict(list)
        self.ticker_names = defaultdict(set)
        self.df = defaultdict(set)
        self.count = len(profiles)
        self.profiles = profiles
        self.related = defaultdict(list)
        self.by_phrase = defaultdict(list)
        self.organizations = defaultdict(set)
        self.entity_peers = defaultdict(list)
        for ticker,profile in profiles.items():
            for form in aliases(profile.name,'ORG'):
                self.organizations[form].add(ticker)
        for ticker, profile in sorted(profiles.items()):
            # Dated, explicitly evidenced officers use person identities rather
            # than inheriting the company's name/plural normalization rules.
            if not hasattr(profile,'keyword_records'):profile.keyword_records=[]
            if not hasattr(profile,'keyword_evidence'):profile.keyword_evidence={}
            from .evidence_matcher import _valid_person
            for fact in getattr(profile,'facts',[]):
                if fact.kind!='person' or not fact.filing_date or not _valid_person(fact):continue
                reference='paragraph:person:'+hashlib.sha256((ticker+fact.text+fact.filing_date+fact.passage).encode()).hexdigest()
                if reference in profile.keyword_evidence:continue
                profile.keyword_evidence[reference]={'subject':ticker,'predicate':'has_officer','object':fact.text,'object_type':'PERSON','assertion':'asserted',
                    'witnesses':[{'source_id':fact.accession,'start':None,'end':None,'offset_scope':'quote_only','quote':fact.passage,
                                  'source':{'filing_date':fact.filing_date,'accession':fact.accession,'section':fact.section,'source':fact.source}}]}
                profile.keyword_records.append({'id':reference,'stock':ticker,'keyword':fact.text,'normalized':' '.join(normalized_words(fact.text)),
                    'relationship':'officer','predicate':'has_officer','object_type':'PERSON','assertion':'asserted',
                    'current_as_of_cutoff':False,'review_required':True,'review_reasons':['officer_tenure_requires_review'],
                    'usable_for_scoring':False,'qualifiers':{},'context_key':'{}','evidence_ids':[reference],
                    'first_filing_date':fact.filing_date,'latest_filing_date':fact.filing_date})
            if getattr(profile,'keyword_coverage',None):
                profile.keyword_coverage['keywords']=len(profile.keyword_records)
                profile.keyword_coverage['review_required']=sum(r['review_required'] for r in profile.keyword_records)
            names = [profile.name, LEGAL_SUFFIX.sub('',profile.name).strip()]
            for name in set(names):
                words = normalized_words(name)
                if words:
                    issuer={'stock':ticker,'relationship':'issuer','object_type':'ORG','keyword':name,'id':'issuer:'+ticker,'evidence_ids':[]}
                    issuer['identity']=identity(issuer,profile,self.organizations)
                    self.first[words[0]].append((words,issuer))
            for row in getattr(profile,'keyword_records',[]):
                # The dated officer adapter above also feeds browser exports.
                if 'scanner' not in row:
                    row['scanner'] = scanner_policy(row)
                if not row['scanner']['eligible']:
                    continue
                row['identity'] = identity(row,profile,self.organizations)
                row['identity']['known_from']=row['first_filing_date']
                words = tuple(row['normalized'].split())
                if words:
                    self.first[words[0]].append((words,row))
                    for alias in row['identity']['aliases']:
                        alternate=normalized_words(alias)
                        if alternate and alternate!=words:self.first[alternate[0]].append((alternate,row))
                        if alternate:self.entity_peers[alternate].append(row['identity'])
                    short = normalized_words(LEGAL_SUFFIX.sub('',row['keyword']))
                    if short and short != words and row['relationship'] in ENTITY_ROLES:
                        self.first[short[0]].append((short,row))
                    self.df[words].add(ticker)
                    self.by_phrase[words].append(row)
                    self.related[(ticker,row['relationship'],row['normalized'],row['predicate'],row.get('context_key',''))].append(row)
                    self.entity_peers[words].append(row['identity'])

    def browser_payload(self):
        """Compact retrieval index; quotations stay in hash-bound stock assets.

        Export the pinned Python dictionary's relevant inflections, not a second
        JavaScript lemmatizer that could silently disagree with offline matches.
        """
        from .linguistics import _lookup, IRREGULAR, english_words
        phrases = []
        vocabulary = set()
        raw_words = set()
        for entries in self.first.values():
            for phrase, row in entries:
                phrases.append({'phrase':list(phrase), 'row':row})
                vocabulary.update(phrase)
                if row.get('identity'):
                    vocabulary.update(row['identity']['source_context_terms'])
                    for sense in row['identity']['senses']:vocabulary.update(sense.get('terms',[]))
                    for alias in row['identity']['aliases']:
                        raw_words.update(t.group().lower() for t in TOKEN.finditer(alias))
                raw_words.update(t.group().lower() for t in TOKEN.finditer(row['keyword']))
        forms = set(_lookup()) | set(IRREGULAR) | set(english_words()) | raw_words | vocabulary
        normalization = {}
        for form in sorted(forms):
            base = lemma(form)
            if base in vocabulary and base != form:
                normalization[form] = base
        return {'schema':SCHEMA, 'catalog_tickers':self.count, 'phrases':phrases,
                'scanner_policy':SCANNER_POLICY,
                'normalization':normalization,'alignment_version':ALIGNMENT_VERSION,'identity_policy':POLICY,
                'stopwords':sorted(STOP), 'comparison':'typed identity and sense alignment; unresolved candidates abstain'}

    def compare(self, text, *, article_date='', entity_annotations=None):
        annotations = validate_annotations(text,entity_annotations)
        from .term_inventory import validate_article_terms
        article_terms=validate_article_terms(text,entity_annotations)
        tokens = list(TOKEN.finditer(text or ''))
        words = [lemma(t.group()) for t in tokens]
        hits, named = [], set()
        for i, word in enumerate(words):
            for phrase, row in self.first.get(word,()):
                end = i+len(phrase)
                if tuple(words[i:end]) != phrase or re.search(r'[.!?;:\n]',text[tokens[i].end():tokens[end-1].start()]):
                    continue
                if row['relationship'] == 'issuer':
                    if decide(row['identity'],text,tokens[i].start(),tokens[end-1].end(),annotations=annotations)['status']=='resolved':named.add(row['stock'])
                else:
                    hits.append((phrase,row,tokens[i].start(),tokens[end-1].end()))
        matches, broad, seen, unresolved, rejected = [], {}, set(), [], []
        date_counts = {}
        # A full name and its contained surname are one mention. Preserve
        # distinct claims/contexts, while preferring the longest same-row span.
        grouped=defaultdict(list)
        for hit in hits:grouped[hit[1]['id']].append(hit)
        hits=[]
        for group in grouped.values():
            previous_end=-1
            for hit in sorted(group,key=lambda h:(h[2],-h[3])):
                if hit[3]<=previous_end:continue
                hits.append(hit);previous_end=hit[3]
        for phrase,row,start,end in hits:
            if not scanner_visible(row, article_date):
                continue
            if article_date and row['first_filing_date'] > article_date:
                continue
            key = (row['id'],start,end)
            if key in seen:
                continue
            seen.add(key)
            canonical = tuple(row['normalized'].split())
            source_identity=identity(row,self.profiles[row['stock']],self.organizations,as_of=article_date) if article_date else row['identity']
            peers=[p for p in self.entity_peers[phrase] if not article_date or p.get('known_from','')<=article_date]
            alignment = decide(source_identity,text,start,end,peers=peers,annotations=annotations)
            candidate = {**row,'identity':source_identity,'alignment':alignment,'article_span':{'start':start,'end':end,'text':text[start:end]}}
            mentions=[t for t in article_terms if t['start']<=start and t['end']>=end]
            if mentions:
                mention=min(mentions,key=lambda t:t['end']-t['start'])
                if mention['capacity'].get('negated'):
                    continue
                candidate['article_capacity']={**mention['capacity'],'context':mention['context']}
            if row['relationship'] in ENTITY_ROLES-{'issuer'}:
                candidate['exposure_path']={'stock':row['stock'],'relationship':row['relationship'],
                    'entity':row['keyword'],'entity_id':source_identity['id'],'order':2,
                    'core_industry_inferred':False,'context_alignment':'not_assessed','impact':'not_inferred'}
            if article_date:
                pool=self.profiles[row['stock']].keyword_evidence
                refs=[ref for ref in row['evidence_ids'] if any(w['source']['filing_date']<=article_date for w in pool[ref]['witnesses'])]
                dates=[w['source']['filing_date'] for ref in refs for w in pool[ref]['witnesses'] if w['source']['filing_date']<=article_date]
                candidate.update(evidence_ids=refs,first_filing_date=min(dates),latest_filing_date=max(dates))
                candidate['term_occurrence_dates']={ident:date for ident,date in row.get('term_occurrence_dates',{}).items() if date<=article_date}
                candidate['term_occurrence_ids']=sorted(candidate['term_occurrence_dates'])
            if alignment['status'] != 'resolved':
                (rejected if alignment['status']=='rejected' else unresolved).append(candidate)
                continue
            if canonical not in date_counts:
                date_counts[canonical] = len({r['stock'] for r in self.by_phrase[canonical]
                    if not article_date or r['first_filing_date'] <= article_date})
            common = date_counts[canonical] >= max(8,math.ceil(self.count*.05))
            if common and row['relationship'] not in ENTITY_ROLES and row['stock'] not in named:
                key = (' '.join(phrase),row['relationship'])
                broad[key] = {'keyword':row['keyword'],'relationship':row['relationship'],
                    'candidate_stock_count':date_counts[canonical],
                    'reason':'broad keyword; additional stock context is required'}
                continue
            current = row['current_as_of_cutoff']
            if article_date:
                pool = self.profiles[row['stock']].keyword_evidence
                choices = [(w['source']['filing_date'],other['assertion'] != 'asserted',ref)
                    for other in self.related[(row['stock'],row['relationship'],row['normalized'],row['predicate'],row.get('context_key',''))]
                    for ref in other['evidence_ids'] for w in pool[ref]['witnesses'] if w['source']['filing_date'] <= article_date]
                current = bool(choices) and max(choices)[2] in row['evidence_ids']
            matches.append({**candidate,'current_as_of_cutoff':current,
                            'usable_for_scoring':current and row['assertion']=='asserted' and not row['review_required'],
                            'article_span':{'start':start,'end':end,'text':text[start:end]},
                            'decision':'identity_aligned_candidate','article_date':article_date})
        return {'schema':SCHEMA,'matches':matches,'broad_matches':list(broad.values()),
                'unresolved_matches':unresolved,'rejected_matches':rejected,'alignment_version':ALIGNMENT_VERSION,
                'stock_passes_changed':False}


def compare_article(board, text, *, article_date='',entity_annotations=None):
    evidence_board = getattr(board,'evidence_board',board)
    if evidence_board is None:  # empty/synthetic engine controls
        return {'schema':SCHEMA,'matches':[],'broad_matches':[],'stock_passes_changed':False}
    index = getattr(evidence_board,'keyword_index',None)
    if index is None:
        index = KeywordIndex(evidence_board.profiles)
        evidence_board.keyword_index = index
    result = index.compare(text,article_date=article_date,entity_annotations=entity_annotations)
    # Materialized witnesses also make look-ahead checks possible. A later filing
    # may be inspectable but cannot support a historical article's comparison.
    if article_date:
        kept = []
        for hit in result['matches']:
            profile = evidence_board.profiles[hit['stock']]
            pool = getattr(profile,'keyword_evidence',{})
            references = [ref for ref in hit['evidence_ids'] if any(w['source'].get('filing_date','9999') <= article_date
                for w in pool[ref]['witnesses'])]
            if references:
                kept.append({**hit,'evidence_ids':references})
        result['matches'] = kept
    return result
