"""Headline-only institutional action candidates, independent of impact analysis.

An explicit reported decision is not verified law or proof of implementation.
Jurisdiction follows the named authority; an unnamed Supreme Court is ambiguous.
"""
import re

VERSION = 'institutional-actions-v1'
STATES = ('Alabama|Alaska|Arizona|Arkansas|California|Colorado|Connecticut|Delaware|'
          'Florida|Georgia|Hawaii|Idaho|Illinois|Indiana|Iowa|Kansas|Kentucky|Louisiana|'
          'Maine|Maryland|Massachusetts|Michigan|Minnesota|Mississippi|Missouri|Montana|'
          'Nebraska|Nevada|New Hampshire|New Jersey|New Mexico|New York|North Carolina|'
          'North Dakota|Ohio|Oklahoma|Oregon|Pennsylvania|Rhode Island|South Carolina|'
          'South Dakota|Tennessee|Texas|Utah|Vermont|Virginia|Washington|West Virginia|'
          'Wisconsin|Wyoming|District of Columbia')

# name pattern, canonical authority, type, jurisdiction, authority level
AUTHORITIES = [
    (r'\b(?:Philippine|Philippines)\s+(?:FDA|Food and Drug Administration)\b', 'Philippines FDA', 'regulator', 'Philippines', None),
    (r'\b(?:FDA|(?:U\.S\.\s+)?Food and Drug Administration)\b', 'FDA', 'regulator', 'US', 'federal'),
    (r'\b(?:FCC|Federal Communications Commission)\b', 'FCC', 'regulator', 'US', 'federal'),
    (r'\b(?:FTC|Federal Trade Commission)\b', 'FTC', 'regulator', 'US', 'federal'),
    (r'\b(?:SEC|Securities and Exchange Commission)\b', 'SEC', 'regulator', 'US', 'federal'),
    (r'\b(?:EPA|Environmental Protection Agency)\b', 'EPA', 'regulator', 'US', 'federal'),
    (r'\b(?:OFAC|Office of Foreign Assets Control)\b', 'OFAC', 'regulator', 'US', 'federal'),
    (r'\bFederal Reserve\b', 'Federal Reserve', 'central_bank', 'US', 'federal'),
    (r'\b(?:U\.S\.|US|United States)\s+(?:Treasury|Department of (?:Justice|Commerce|Energy|Labor))\b', None, 'executive_agency', 'US', 'federal'),
    (r'\b(?:White House|President Trump|President Biden|Trump|Biden)\b', None, 'executive', 'US', 'federal'),
    (r'\b(?:U\.S\.|US|United States)\s+(?:Supreme Court|Court of Appeals|District Court|court)\b', None, 'court', 'US', 'federal'),
    (r'\b(?:\d+(?:st|nd|rd|th)|First|Second|Third|Fourth|Fifth|Sixth|Seventh|Eighth|Ninth|Tenth|Eleventh|D\.C\.)\s+Circuit\b', None, 'court', 'US', 'federal'),
    (r'\b(?:U\.S\.|US)\s+(?:Senate|House(?: of Representatives)?|Congress)\b', None, 'legislature', 'US', 'federal'),
    (r'\bCongress\b', 'Congress', 'legislature', None, None),
    (r'\b(?:Senate|House(?: of Representatives)?|Parliament|Assembly)\b', None, 'legislature', None, None),
    (rf'\b(?:{STATES})\s+(?:Supreme Court|appeals court|court|judge|governor|legislature|Senate|House)\b', None, 'state_authority', 'US', 'state'),
    (r'\b(?:Guam|Puerto Rico|U\.S\. Virgin Islands|US Virgin Islands|American Samoa|Northern Mariana Islands)\s+(?:court|judge|governor|legislature)\b', None, 'territorial_authority', 'US', 'territory'),
    (r'\b(?:World Bank|IMF|International Monetary Fund|WTO|World Trade Organization)\b', None, 'international_institution', 'international', None),
    (r'\b(?:European Commission|European Parliament|European Central Bank|ECB|EU regulator)\b', None, 'institution', 'EU', None),
    (r'\b(?:Bank of England|Bank of Japan|BoJ|PBOC|PBoC|People\W+s Bank of China)\b', None, 'central_bank', None, None),
    (r'\b(?:Supreme Court|High Court|appeals court|court|judge)\b', None, 'court', None, None),
    (r'\b(?:government|regulator|ministry|governor|assemblywoman|assemblyman)\b', None, 'public_authority', None, None),
]
FOREIGN = [(r'\b(?:Russia|Russian)\b','Russia'), (r'\b(?:Tokyo|Japan|Japanese)\b','Japan'),
           (r'\b(?:Korea|Korean)\b','Korea_unspecified'), (r'\b(?:Nepal|Nepali)\b','Nepal'),
           (r'\b(?:UK|U\.K\.|British|United Kingdom)\b','UK'),
           (r'\b(?:India|Indian|Haryana|Madras)\b','India'),
           (r'\b(?:Canada|Canadian|Alberta|Ottawa)\b','Canada'),
           (r'\b(?:Australia|Australian)\b','Australia'), (r'\b(?:China|Chinese)\b','China')]

ACTION_PATTERNS = [
    ('judicial_ruling', r'\b(?:rules?\s+(?:for|against|that|on|in favor|in favour|human|federal|landlords)|ruled|upholds?|overturns?|strikes? down|dismisses?|grants? (?:an? )?(?:temporary )?injunction|denies? (?:an? )?(?:temporary )?injunction|orders?|blocks?|allows?)\b', 'decision_reported'),
    ('legislation', r'\b(?:signed into law|signs? .{0,100}?into law|enacts?|enacted|becomes? law)\b', 'enacted_reported'),
    ('legislation', r'\b(?:passes|passed|approves|approved)\b.{0,100}?\b(?:bill|act|legislation)\b', 'chamber_approval_reported'),
    ('regulatory_action', r'\b(?:approves?|approved|approval|grants?|granted|rejects?|rejected|denies?|denied|revokes?|revoked|recalls?|recalled|fines?|fined)\b', 'decision_reported'),
    ('policy_action', r'\b(?:rules? out|raises?|raised|cuts?|cut|lifts?|lifted|bans|banned|imposes?|imposed|suspends?|suspended|tightens?|tightened|adopts?|adopted|issues?|issued|signs?|signed)\b', 'action_reported'),
    ('policy_proposal', r'\b(?:proposes?|proposed|introduces?|introduced|seeks?|considers?|considering|calls? for|threatens?|threatened|plans? to|to vote|will vote|scheduled to vote)\b', 'proposed_or_scheduled'),
]

def _span(text, match):
    return {'quote': match[0], 'start': match.start(), 'end': match.end()}

def identify_institutional_actions(title):
    """Return explicit headline candidates; never use body, tickers or trade gates."""
    if not isinstance(title, str):
        raise TypeError('title must be a string')
    records=[]
    # Separate explicitly delimited stories instead of binding one agency to another.
    segments=list(re.finditer(r'[^;\n]+', title))
    for segment in segments:
        text=segment[0]
        authorities=[]
        for pattern, name, kind, jurisdiction, level in AUTHORITIES:
            m=re.search(pattern,text,re.I)
            if m:
                authorities.append((m, name or m[0], kind, jurisdiction, level))
        # Match the most specific authority at a position, retaining separate actors.
        authorities.sort(key=lambda x:(x[0].start(), -(x[0].end()-x[0].start())))
        authorities=[a for i,a in enumerate(authorities) if not any(b[0].start()<=a[0].start() and b[0].end()>=a[0].end() for b in authorities[:i])]
        action=None
        for kind,pattern,stage in ACTION_PATTERNS:
            m=re.search(pattern,text,re.I)
            if m and (kind!='judicial_ruling' or any(a[2]=='court' or re.search(r'court|judge|Circuit',a[1],re.I) for a in authorities)):
                action=(kind,m,stage);break
        if not action:
            ruling=re.search(r'\b(?:wins?|loses?)\b.{0,100}?\b(?:patent|tax) ruling\b',text,re.I)
            if ruling:
                action=('judicial_ruling',ruling,'decision_reported')
        if not action:
            continue
        kind,m,stage=action
        if re.search(r'\bFDA-approved\b',text,re.I) and m[0].lower()=='approved':
            continue  # An adjectival prior approval is not a newly reported action.
        if not authorities and kind!='legislation':
            # Explicit sovereign actions (e.g. Russia lifts an export ban).
            sovereign=re.match(r'\s*(US|U\.S\.|United States|Russia|UK|Nepal|Canada|India)\b',text,re.I)
            if sovereign and kind in ('policy_action','policy_proposal'):
                country='US' if sovereign[1].lower() in ('us','u.s.','united states') else sovereign[1]
                authorities=[(sovereign,sovereign[1],'sovereign_authority',country,None)]
            elif kind=='judicial_ruling':
                pass  # A reported ruling can omit the court's identity.
            else:
                continue
        # An action must have a nearby authority. Avoid interpreting an issuer's
        # FDA-approved product launch as a new regulatory approval.
        if authorities:
            preceding=[a for a in authorities if a[0].start()<=m.start()]
            authority=min(preceding or authorities,key=lambda a:abs(m.start()-a[0].end()))
        else:
            authority=None
        if kind=='regulatory_action' and m[0].lower()=='approval' and not re.search(r'\b(?:receives?|wins?|gets?|grants?|granted|approves?|approved)\b',text,re.I):
            continue
        if re.search(r'\b(?:how to|guide to|what if|would|could|might|may|expected to|expects? to|not yet|awaiting|seeks?|considers?|plans? to|threatens?|calls? for)\b',text[:m.end()],re.I):
            stage='proposed_or_uncertain'
        if re.search(r'\b(?:to vote|will vote|scheduled to vote)\b',text,re.I):
            stage='scheduled_vote'
        if re.search(r'\b(?:did not|does not|has not|not yet)\b',text[:m.end()],re.I):
            stage='negated_or_uncertain'
        if kind=='judicial_ruling' and re.search(r'\binjunction|temporarily|temporary|stay\b',text,re.I):
            stage='interim_decision_reported'
        if kind=='legislation' and stage=='chamber_approval_reported':
            # A chamber's passage never implies a signed law.
            stage='committee_approval_reported' if re.search(r'\bcommittee\b',text[:m.start()],re.I) else 'chamber_approval_reported'
        jurisdiction=authority[3] if authority else None
        if jurisdiction is None:
            authority_context=text[:authority[0].end()] if authority else ''
            countries=[country for pattern,country in FOREIGN if re.search(pattern,authority_context,re.I)]
            if len(set(countries))==1:jurisdiction=countries[0]
            if re.search(r'\b(?:US|U\.S\.|United States)\b',text,re.I) and not countries:jurisdiction='US'
        if authority and authority[2]=='central_bank' and jurisdiction is None:
            jurisdiction={'Bank of England':'UK','Bank of Japan':'Japan','BoJ':'Japan','PBOC':'China','PBoC':'China'}.get(authority[1])
        action_start=segment.start()+m.start();action_end=segment.start()+m.end()
        issuer_span=({'quote':authority[0][0],'start':segment.start()+authority[0].start(),'end':segment.start()+authority[0].end()} if authority else None)
        object_text=text[m.end():].strip(' ,:.-')
        if ' - ' in object_text:object_text=object_text.rsplit(' - ',1)[0]
        if kind=='legislation' and not object_text:object_text=text[:m.start()].strip()
        concrete=stage not in ('proposed_or_uncertain','proposed_or_scheduled','scheduled_vote','negated_or_uncertain')
        records.append({'authority':authority[1] if authority else None,
            'authority_type':authority[2] if authority else ('court_unspecified' if kind=='judicial_ruling' else 'legislature_unspecified'),
            'jurisdiction':jurisdiction,'us_scope':jurisdiction=='US',
            'authority_level':authority[4] if authority else None,
            'action_category':kind,'action':m[0],'subject':object_text or None,
            'stage':stage,'concrete_reported_action':concrete,
            'authority_evidence':issuer_span,
            'action_evidence':{'quote':m[0],'start':action_start,'end':action_end},
            'verification':'headline_reported_not_primary_verified',
            'effective_date':None,'implementation_verified':False,
            'lane_classification':None,'market_sectors':[],
            'unknowns':['Legal scope, effective date and implementation require source terms',
                        'Institution recognition alone does not establish companies or market direction']})
    return {'version':VERSION,'input_scope':'title_only','actions':records,
            'status':'identified' if records else 'no_supported_action_identified',
            'has_concrete_reported_action':any(r['concrete_reported_action'] for r in records),
            'has_us_action':any(r['us_scope'] for r in records)}
