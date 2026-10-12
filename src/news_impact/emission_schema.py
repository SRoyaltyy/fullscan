"""Output shapes, not judgments: existing semantic acceptance remains mandatory."""
from src.news_impact.schema import EVENT_CLASSES
from src.news_impact.meta_hop import CHANGES, SLOTS

def object_schema(properties, required=None):
    return {'type': 'object', 'properties': properties,
            'required': list(properties) if required is None else required,
            'additionalProperties': False}

def emission_schema(stage):
    text = {'type': 'string'}
    if stage == 'classify':
        return object_schema({
            'event_class': {'type': 'string', 'enum': list(EVENT_CLASSES)},
            'q5': {'type': 'string', 'enum': ['impulse', 'regime', 'regime_break']},
            'sign': {'type': ['string', 'null']}, 'constraint': text,
            'split': {'type': 'boolean'},
            'split_facts': {'type': 'array', 'items': text}, 'why': text,
        })
    if stage == 'meta':
        changes = {'type': 'string', 'enum': sorted(CHANGES)}
        return object_schema({
            'm1': object_schema({'need_context': {'type': 'string', 'enum': ['yes', 'no']}}),
            'm2': {'type': 'array', 'minItems': 1, 'maxItems': 6,
                   'items': object_schema({
                       'slot': {'type': 'string', 'enum': sorted(SLOTS)},
                       'noun': text, 'question': text, 'changes': changes,
                       'blocks': {'type': 'array', 'items': changes},
                   })},
            'm3_dropped': {'type': 'array', 'items': object_schema({'question': text, 'why': text})},
        })
    return None
