"""Clause-bound business capacity, independent of issuer/ticker word lists.

These are explicit linguistic rules, not a general semantic model. Unknown
capacity remains unresolved. All candidates stay in the separate term inventory.
"""
from __future__ import annotations

VERSION = 'relationship-frames-1'
INSTRUMENTS = {'share', 'stock', 'note', 'debenture', 'bond', 'warrant', 'option',
               'swap', 'derivative', 'security'}
ASSETS = {'interest', 'stake', 'subsidiary', 'venture', 'mine', 'business',
          'division', 'asset', 'property', 'operation'}
REPORTING = {'believe', 'think', 'expect', 'anticipate', 'consider', 'understand'}
INTENTIONS = {'agree', 'plan', 'intend', 'propose', 'seek', 'attempt', 'aim'}


def token_span(token, offset):
    return {'start': offset+token.idx, 'end': offset+token.idx+len(token.text), 'text': token.text}


def clause_tokens(trigger):
    """Exclude independent/relative/embedded predicates from this proposition."""
    result, pending = [], [trigger]
    while pending:
        current = pending.pop()
        result.append(current)
        pending.extend(c for c in current.children
                       if c.dep_ not in ('advcl', 'relcl', 'ccomp', 'parataxis')
                       and not (c.dep_ == 'conj' and c.pos_ in ('VERB', 'AUX')))
    return sorted(result, key=lambda t: t.i)


def assertion(trigger):
    """Scope qualifiers to their governing clause, including reported belief."""
    chain = [trigger]
    while chain[-1].dep_ in ('acomp', 'attr', 'oprd', 'xcomp', 'ccomp') and chain[-1].head != chain[-1]:
        chain.append(chain[-1].head)
    local = clause_tokens(trigger)
    negated = any(c.dep_ == 'neg' for c in trigger.children)
    reporting = [p for p in chain[1:] if p.lemma_ in REPORTING]
    belief_negated = any(c.dep_ == 'neg' for p in reporting for c in p.children)
    modals = sorted({c.lemma_ for p in chain for c in p.children if c.tag_ == 'MD'})
    temporal = sorted({c.lemma_ for c in local if c.lemma_ in
                       ('previously', 'formerly', 'cease', 'terminate', 'discontinue')})
    conditional = any(c.dep_ == 'mark' and c.lower_ in ('if', 'unless') for p in chain for c in p.children)
    if belief_negated:
        status = 'uncertain'  # "does not believe subject" is not "is not subject".
    elif negated or any(c.dep_ == 'neg' for p in chain[1:] if p not in reporting for c in p.children):
        status = 'negated'
    elif temporal:
        status = 'historical'
    elif modals or conditional or reporting or any(p.lemma_ in INTENTIONS for p in chain) or (
            trigger.dep_=='acl' and trigger.head.lemma_ in ('agreement','contract','commitment','plan','proposal','offer')):
        status = 'potential'
    else:
        status = 'asserted'
    return status, {'modals': modals, 'conditional': conditional,
                    'reported_belief': [p.lemma_ for p in reporting],
                    'belief_negated': belief_negated, 'temporal_cues': temporal,
                    'transaction_stage': 'proposed_or_agreed' if any(p.lemma_ in INTENTIONS for p in chain) else 'not_established',
                    'event_tense': trigger.tag_}


def _root(obj):
    return obj.root if hasattr(obj, 'root') else obj if hasattr(obj, 'dep_') else None


def _noun_terms(root):
    if root is None:
        return set()
    # Modifiers of THIS argument only. A neighboring clause's "trial" or
    # "customer" cannot type the object.
    return {t.lemma_.casefold() for t in root.subtree
            if not any(a.dep_ in ('relcl', 'advcl', 'ccomp') for a in t.ancestors
                       if a != root and root in list(a.ancestors))}


def refine(predicate, obj, trigger, *, objects, recipients, value_type, offset=0):
    root = _root(obj)
    terms = _noun_terms(root)
    head = root.lemma_.casefold() if root is not None else ''
    object_heads = {o.lemma_.casefold() for o in objects}
    recipient_heads = {o.lemma_.casefold() for o in recipients}
    capacity = 'operating'
    resolution = 'rule_supported'
    argument_negated = False
    nominal = root
    while nominal is not None:
        if any(c.dep_ == 'neg' or (c.dep_ in ('det', 'preconj') and c.lower_ in ('no', 'not'))
               for c in nominal.children):
            argument_negated = True
        nominal = nominal.head if nominal.dep_ == 'conj' and nominal.head != nominal else None
    # A named argument's nearest descriptive head outranks a distant role word.
    # Example: "our partners' activities ... our clinical trials, including X".
    descriptive_head = None
    if root is not None:
        for ancestor in root.ancestors:
            if ancestor == trigger or ancestor.pos_ in ('VERB', 'AUX'):
                break
            if ancestor.lemma_ in ('trial', 'study', 'platform'):
                descriptive_head = ancestor.lemma_
                break
    if predicate in ('partners_with', 'sells_to', 'buys_from') and descriptive_head:
        predicate = 'mentions_clinical_trial' if descriptive_head in ('trial', 'study') else 'mentions_product_platform'
        capacity = 'clinical_research' if descriptive_head in ('trial', 'study') else 'product_platform'
        value_type = 'clinical_trial' if descriptive_head in ('trial', 'study') else 'product'
    if predicate in ('offers', 'uses_input'):
        if head in ('plan', 'benefit', 'compensation') and (
                terms & {'pension', 'retirement', 'employee', '401', 'compensation',
                         'contribution', 'nonpension', 'postretirement', 'retiree'}
                or recipient_heads & {'employee', 'worker', 'executive', 'director'}):
            predicate, capacity = 'provides_employee_benefit', 'employee_benefit'
        elif head in INSTRUMENTS:
            predicate, capacity = 'holds_financial_instrument' if predicate == 'uses_input' else 'issues_financial_instrument', 'financing'
        elif head == '%' or object_heads & ASSETS:
            predicate, capacity, resolution = 'mentions_transaction_asset', 'asset_transaction', 'needs_review'
    elif predicate in ('sells_to', 'buys_from'):
        if object_heads & INSTRUMENTS:
            predicate, capacity = 'financing_counterparty', 'financing'
        elif object_heads & ASSETS:
            predicate, capacity = 'transaction_counterparty', 'asset_transaction'
        elif '%' in object_heads:
            predicate, capacity, resolution = 'unresolved_relationship', 'transaction_capacity_unknown', 'needs_review'
        elif recipient_heads & {'employee', 'worker', 'executive', 'director'}:
            predicate, capacity = 'benefit_recipient', 'employee_benefit'
        elif not objects and trigger.pos_ == 'VERB' and trigger.lemma_ in ('sell', 'buy', 'purchase'):
            # "sell to X" may be commercial, but this clause alone has not
            # established the business capacity. Keep the name, abstain on role.
            predicate, capacity, resolution = 'unresolved_relationship', 'unknown', 'needs_review'
    if predicate in ('partners_with', 'sells_to', 'buys_from', 'owns_subsidiary'):
        if head in ('trial', 'study') and terms & {'clinical', 'trial', 'phase'}:
            predicate, capacity, value_type = 'mentions_clinical_trial', 'clinical_research', 'clinical_trial'
        elif value_type == 'law' or head in ('act', 'regulation', 'directive', 'statute'):
            predicate, capacity, value_type = 'mentions_legal_reference', 'legal_reference', 'law'
        elif head in ('board', 'committee') and terms & {'director', 'audit', 'compensation', 'our'}:
            predicate, capacity, value_type = 'mentions_governance_body', 'corporate_governance', 'governance_body'
    subjects = [c for c in trigger.children if c.dep_ in ('nsubj', 'nsubjpass', 'csubj')]
    frame = {'schema': VERSION, 'trigger': token_span(trigger, offset),
             'subjects': [token_span(s, offset) for s in subjects],
             'object_heads': [token_span(o, offset) for o in objects],
             'recipient_heads': [token_span(o, offset) for o in recipients],
             'capacity': capacity, 'resolution': resolution,
             'argument_negated': argument_negated,
             'descriptive_head': descriptive_head,
             'argument_type_basis': 'argument-local grammar, not inherited ticker or relationship type'}
    return predicate, value_type, frame
