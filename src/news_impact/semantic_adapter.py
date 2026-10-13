"""Adapt raw semantic output to the existing headline annotation contract.

No judgment correction. Unknown/unavailable fields remain unknown. This adapter
allows callers to opt into live semantics without changing the free collector.
"""

def annotations_from_semantic(title,result):
    raw=result.get('model_output')
    base={'semantic_analysis':result}
    if raw is None:
        return base|{'headline_event':{'status':'unavailable','lane_class':None,'stage':'unknown','event_type':None,'evidence':None},
            'institutional_actions':{'status':'unavailable','actions':[],'has_concrete_reported_action':None,'has_us_action':None,'input_scope':'title_only'},
            'sector_classification':{'status':'unavailable','matches':[],'market_sectors':[],'market_activities':[],'input_scope':'title_only'}}
    def span(quote):
        if quote is None:return None
        start=title.index(quote)
        return {'quote':quote,'start':start,'end':start+len(quote)}
    primary=raw['primary_lane_class'];event=next((e for e in raw['events'] if e['lane_class']==primary),None)
    headline={'status':'identified' if primary is not None else 'unresolved','lane_class':primary,'stage':event['stage'] if event else 'unknown',
        'event_type':event['event_type'] if event else None,'evidence':span(event['evidence'][0]) if event else None,'why':raw['primary_why'],'events':raw['events']}
    records=[]
    for action in raw['actions']:
        if action['public_authority'] is False:continue
        records.append({'authority':action['actor'],'public_authority':action['public_authority'],'jurisdiction':action['jurisdiction'],'us_scope':action['jurisdiction']=='US' if action['jurisdiction'] else None,
            'action':action['action'],'subject':action['object'],'stage':action['stage'],'concrete_reported_action':action['concrete_public_action'],
            'authority_evidence':span(action['actor_evidence']),'action_evidence':span(action['action_evidence']),
            'verification':'model_interpretation_of_reported_headline','implementation_verified':False})
    concrete=any(a['concrete_reported_action'] is True for a in records)
    if not concrete and any(a['concrete_reported_action'] is None for a in records):concrete=None
    inst={'status':'identified' if records else 'no_supported_public_action','actions':records,'has_concrete_reported_action':concrete,
          'has_us_action':any(a['us_scope'] is True for a in records),'input_scope':'title_only'}
    matches=[dict(s,evidence=span(s['evidence']),exposure_verified=False,business_identity_verified=False) for s in raw['sectors']]
    sectors={'status':'identified' if matches else 'unknown','matches':matches,'market_sectors':sorted({s['sector'] for s in matches}),
             'market_activities':sorted({s['activity'] for s in matches}),'input_scope':'title_only','unknowns':raw['unknowns']}
    return base|{'headline_event':headline,'institutional_actions':inst,'sector_classification':sectors}
