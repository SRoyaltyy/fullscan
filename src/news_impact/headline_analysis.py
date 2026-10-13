"""Shared headline annotations for intake and one-shot output."""
import re
from .headline_events import identify_headline_event, headline_text
from .headline_sectors import identify_headline_sectors
from .institutional_actions import identify_institutional_actions


def analyse_headline(title, *, root=None, semantic_client=None):
    if semantic_client is not None:
        from .semantic_adapter import annotations_from_semantic
        return annotations_from_semantic(title, semantic_client.analyze(title))
    event = identify_headline_event(title)
    sectors = identify_headline_sectors(title, root=root)
    institutions = identify_institutional_actions(title)
    segments=list(re.finditer(r'[^;\n]+',headline_text(title)))
    for action in institutions['actions']:
        pos=action['action_evidence']['start']
        segment=next(s for s in segments if s.start()<=pos<s.end())
        # Only annotate the action's own story; another semicolon-delimited
        # company's sector must not be attached to this authority's decision.
        own_event=event if len(segments)==1 else identify_headline_event(segment[0])
        own_sectors=sectors if len(segments)==1 else identify_headline_sectors(segment[0],root=root)
        action['lane_classification']=own_event['lane_class']
        action['market_sectors']=own_sectors['market_sectors']
        action['market_activities']=own_sectors['market_activities']
        action['sector_scope']='same_headline_segment_relevance_not_verified_exposure'
    return {'headline_event':event,'sector_classification':sectors,'institutional_actions':institutions}
