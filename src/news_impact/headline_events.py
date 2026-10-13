"""Bounded headline event grammar, before tape filtering.

These are reported candidates, not verified facts or directional trades. Patterns
bind verbs to their objects, preserve prospective stages, and retain enum gaps.
No benchmark titles, expected answers, sample IDs or model output are consulted.
"""
import re
from .hygiene import guidance_hygiene

VERSION = 'headline-events-v1'


def headline_text(title):
    # Publisher and related-ticker footer are metadata, not event participants.
    text = re.split(r'\s+-\s+(?:[A-Z][\w .&-]+\s+\((?:NASDAQ|NYSE|ARCA):)', title)[0]
    if ' - ' in text:
        head, publisher = text.rsplit(' - ', 1)
        # A separator may delimit another factual clause. Keep it if it has
        # an event predicate; only ordinary publisher footers are removed.
        if not re.search(r'\b(?:approv\w*|announc\w*|signs?|lifts?|raises?|cuts?|misses?|beats?|files?|buys?|acquires?|expected|results)\b',publisher,re.I):
            text = head
            while text.endswith(' - ' + publisher):
                text = text[:-(len(publisher)+3)]
    return text

# Specific relations precede broad action families. Stage describes the claim;
# an announced factory does not become an already operating factory.
RULES = [
 ('deal_review', 'merger_review', r'\bmerger (?:clearance|review)|\b(?:conditional|regulatory|EU) merger (?:approval|clearance)|\bmerger.{0,25}waiting period', None),
 ('corporate_action_mna', 'acquisition', r'\b(?:buys?|acquires?|acquired)\b.{0,90}\b(?:for|stake|business|company|assets)\b|\b(?:proposed |announces? |agrees? |completed? )+acquisition\b|\b(?:to buy|to acquire|merger)\b', None),
 ('sanction_lift', 'restriction_relief', r'\b(?:lifts?|lifted|eases?|eased|removes?|removed)\b.{0,55}\b(?:sanctions?|export ban|import ban)\b|\b(?:sanctions?|export ban|import ban)\b.{0,25}\b(?:lifted|eased|removed)\b', 'lift'),
 ('access_control', 'restriction_change', r'\b(?:signs?|issues?)\b.{0,65}\b(?:order|directive)\b.{0,60}\b(?:paus\w+|suspend\w+)\b.{0,35}\b(?:inspection|enforcement)', 'lift'),
 ('price_cap', 'regulated_price', r'\b(?:price (?:ceiling|cap)|water rate hike|fuel surcharge recovery|regulated (?:price|rate))\b', None),
 ('ip_ruling', 'patent_ruling', r'\b(?:patent|intellectual property) ruling\b', None),
 ('gate', 'judicial_permission', r'\bcourt\b.{0,90}\blaw unconstitutional\b', None),
 ('tax_fiscal', 'tax_change', r'\b(?:property|sales|income|corporate) tax (?:increase|cut|hike)|\btax case|\bimport duty benefit', None),
 ('subsidy', 'subsidy', r'\b(?:subsid(?:y|ies)|interest aid)\b', None),
 ('key_person', 'personnel_departure', r'\b(?:auditor|councilmember|councillor|mayor|minister|CEO|CFO|chair(?:man|woman)?)\b.{0,45}\b(?:resigns?|resigned|steps? down)\b', None),
 ('distress_restruct', 'bankruptcy', r'\b(?:files?|filed|filing)\b.{0,25}\b(?:bankruptcy|chapter (?:7|11|15))\b', None),
 ('credit_funding', 'credit_change', r'\b(?:repays?|repaid|renews?|renewed|refinances?|refinanced|secures?|extends?)\b.{0,65}\b(?:credit facility|revolving credit|loan facility|debt)\b', None),
 ('dilution', 'equity_issue', r'\b(?:private placement|public offering|secondary offering|equity placement|ATM offering)\b', None),
 ('flow_index', 'index_membership', r'\b(?:added|joins?|included|removed)\b.{0,35}\b(?:S&P|Russell|MSCI|index)\b', None),
 ('strategic_review', 'strategic_review', r'\bstrategic review\b', None),
 ('reserve_revision', 'resource_revision', r'\b(?:increase|decrease|revision|revises?|updates?)\b.{0,50}\b(?:mineral resource|reserves? estimate)|\bmineral resource\b.{0,35}\b(?:increases?|decreases?|revised)\b', None),
 ('channel_stock', 'dealer_inventory', r'\b(?:dealer|dealership|channel) inventory\b.{0,30}\b(?:up|down|rises?|falls?|build|draw|\d+\s*percent)\b', None),
 ('blast_ops', 'operational_disruption', r'\b(?:factory|plant|facility|warehouse|refinery)\b.{0,30}\b(?:fire|explosion|outage)\b|\b(?:service|network) outage\b|\b(?:Hormuz|strait)\b.{0,25}\battacks?\b', None),
 ('cat_weather', 'natural_hazard', r'\b(?:earthquake|hurricane|tornado|flood)\b.{0,35}\b(?:strikes?|hits?|damages?|landfall)\b', None),
 ('guidance', 'guidance_change', r'\bprofit warning\b|\b(?:raises?|cuts?|lowers?|updates?|reaffirms?|maintains?)\b.{0,30}\b(?:guidance|outlook)\b', None),
 ('trial_readout', 'clinical_result', r'\b(?:meets?|met|misses?|missed)\b.{0,35}\b(?:primary|secondary) endpoint\b|\b(?:trial|study)\b.{0,35}\b(?:results|data|readout)\b', None),
 ('capacity', 'capacity_change', r'\b(?:opens?|builds?|expands?|expanding|doubles?|to double)\b.{0,60}\b(?:factory|plant|manufacturing|production capacity|assembly|network)\b|\b(?:new|expanded) (?:factory|plant|fab)\b', 'add'),
 ('gate', 'regulatory_approval', r'\bFDA\b.{0,25}\b(?:approv(?:e[sd]?|al))\b', 'open'),
 ('gate', 'judicial_injunction', r'\b(?:grants?|secures?|wins?|denies?|obtains?)\b.{0,40}\binjunction\b', None),
 ('factor_impulse', 'macro_print', r'\b(?:payrolls?|jobs report)\b.{0,30}\b(?:miss\w*|beat\w*|blowout|surprise\w*)\b|\bblowout jobs report\b|\b(?:jobs|inflation) data\b.{0,25}\bease\b|\b(?:CPI|PCE|PPI)\b.{0,35}\b(?:held|rose|fell|\d+(?:\.\d+)?%)\b', None),
 ('input_cost', 'stock_release', r'\b(?:G7|government|agency)\b.{0,25}\b(?:stock|reserve) release\b', 'down'),
 ('input_cost', 'fuel_cost', r'\bfuel surcharge\b', None),
 ('capital_return', 'share_return', r'\b(?:share|equity|stock) (?:buyback|repurchase)\b|\bbuyback\b.{0,35}\b(?:shares|share capital)\b|\b(?:cash dividend|dividend declared)\b', 'up'),
 ('listing_flow', 'listing', r'\bIPO\b', None),
 # Explicit typed events without a defensible existing Lane enum.
 (None, 'customer_buyback', r'\bbuyback (?:scheme|program)\b.{0,35}\b(?:ICE|EV|vehicle|car|models)\b', None),
 (None, 'supply_agreement', r'\b(?:signs?|extends?|renews?)\b.{0,35}\bsupply agreement\b', None),
 (None, 'product_launch', r'\b(?:launches?|announces?)\b.{0,35}\b(?:new feature|refinancing service|new product)\b', None),
]


def identify_headline_event(title):
    text = headline_text(title)
    comment = re.search(r'\b(?:could|may|might)\b.{0,80}\bdeal\b|\bIPO boom\b', text, re.I)
    if comment:
        return _event('statement_public', 'proposal_or_comment', text, comment, 'proposal_or_comment', None)
    for lane, kind, pattern, sign in RULES:
        m = re.search(pattern, text, re.I)
        if not m:
            continue
        # Discussion of a concept isn't the underlying action.
        if kind == 'listing' and re.search(r'\b(?:boom|case for|mandatory ratings|hopes)\b', text, re.I):
            continue
        if kind == 'subsidy' and re.search(r'\b(?:call for|calls for|demand\w*|urges?|knocks)\b', text, re.I):
            return _event('statement_public', 'policy_comment', text, m, 'proposal_or_comment', None)
        if kind == 'restriction_relief' and re.search(r"\b(?:don.t|do not|should|urges?)\b", text, re.I):
            return _event('statement_public', 'policy_comment', text, m, 'proposal_or_comment', None)
        stage = 'reported'
        prefix = text[max(0, m.start()-65):m.end()]
        if re.search(r'\b(?:expected to|expects?|may|might|could|set to|plans?|to double|proposed|to vote|will vote)\b', prefix, re.I):
            stage = 'prospective_or_proposed'
        if kind in ('regulatory_approval','restriction_relief') and stage=='prospective_or_proposed':
            lane='statement_public'
        if re.search(r'\b(?:did not|has not|does not|not yet)\b',prefix,re.I):
            stage='negated_or_uncertain';lane='statement_public';sign=None
        if kind == 'clinical_result' and (stage != 'reported' or re.search(r'\b(?:due|expected|in H[12]|next (?:year|month))\b', text, re.I)):
            lane = 'statement_public'
            stage = 'scheduled_readout'
        if kind == 'regulatory_approval' and re.search(r'\b(?:undervalued|after|following|prior)\b', text[:m.start()], re.I):
            stage = 'retrospective'
        if kind == 'guidance_change':
            hy = guidance_hygiene(text)
            sign = 'raise' if re.search(r'\braises?\b', m[0], re.I) else ('cut' if re.search(r'warning|cuts?|lowers?', m[0], re.I) else None)
        result=_event(lane, kind, text, m, stage, sign)
        if kind=='guidance_change' and hy['why']!='guidance unsigned':
            result['sign']=hy['sign'];result['split']=hy['split'];result['split_facts']=hy['split_facts']
        return result
    # Explicit communications take precedence over threats in a subordinate clause.
    m = re.search(r'\b(?:clarif(?:y|ies|ied)|presents? .{0,30}awards|confirms? review|calls? for)\b', text, re.I)
    if m:
        return _event('statement_public', 'public_statement', text, m, 'reported_communication', None)
    # Expected rate fixings and retrospective export-control commentary are states.
    m = re.search(r'\bLPR fixing\b|\bexport controls?\b.{0,35}\b(?:what|reveals|explained)\b', text, re.I)
    if m:
        return _event('regime_state', 'background', text, m, 'background', None)
    return {'version': VERSION, 'status': 'no_supported_event', 'lane_class': None,
            'event_type': None, 'stage': 'unknown', 'evidence': None, 'sign': None}


def _event(lane, kind, text, match, stage, sign):
    return {'version': VERSION, 'status': 'identified' if lane else 'enum_gap',
            'lane_class': lane, 'event_type': kind, 'stage': stage, 'sign': sign,
            'evidence': {'quote': match[0], 'start': match.start(), 'end': match.end()},
            'verification': 'headline_reported_not_primary_verified'}
