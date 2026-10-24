"""Evidence from a document's explicit purpose, not adjacent relation words.

A subsidiary schedule can establish a group relationship even when every row
is a bare legal name. It does not establish direct ownership or current status.
"""
import hashlib
import re
from .entity_resolution import aliases

LEGAL_NAME = re.compile(r"^\s*(?:\d+[.)]\s*)?(?P<name>[A-Z][A-Za-z0-9&’'.,() /-]{1,170}\b(?:Corporation|Company|Limited|Holdings|GmbH|Corp\.?|Inc\.?|Ltd\.?|LLC|L\.L\.C\.|LP))(?=\s*(?:$|[,;]|[A-Z]))")
HEADER = re.compile(r'^\s*(?:exhibit\b|subsidiaries(?:\s+of|\s*$)|list\s+of\s+subsidiaries|names?\s+of|jurisdiction\s+of|as\s+of)',re.I)


def subsidiary_schedule(source,text):
    doc=source.get('document','')
    declared=source.get('document_type','').upper()
    if declared:
        if re.fullmatch(r'EX-?21(?:\.\d+)?',declared):return True
        if not re.fullmatch(r'EX-?8(?:\.\d+)?',declared):return False
        return bool(re.search(r'(?im)^\s*(?:list\s+of\s+)?subsidiaries\s+of\s+(?:the\s+)?(?:registrant|company)\s*$',text[:1000]))
    if re.search(r'(?:^|[-_])(?:ex|exhibit)[-_]?21(?:\D|$|1\.)',doc,re.I):return True
    return bool(re.search(r'(?im)^\s*(?:list\s+of\s+)?subsidiaries\s+of\s+(?:the\s+)?(?:registrant|company)\s*$',text[:1000]))


def listed_names(source,text,issuer_name):
    if not subsidiary_schedule(source,text):return []
    owner=set(aliases(issuer_name,'ORG'));rows=[];offset=0
    for line in text.splitlines(keepends=True):
        if not HEADER.search(line):
            m=LEGAL_NAME.search(line)
            if m:
                name=m.group('name').strip()
                if not set(aliases(name,'ORG')) & owner:
                    start=offset+m.start('name')
                    rows.append({'source_id':source['id'],'start':start,'end':start+len(name),'text':name,
                                 'predicate':'listed_subsidiary','assertion':'reported',
                                 'direct_ownership_established':False,
                                 'filing_date':source.get('filing_date',''),
                                 'basis':'explicit_subsidiary_document_scope'})
        offset+=len(line)
    return rows
