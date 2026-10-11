"""Keep publisher promotion out of reporting, with exact source spans."""
import hashlib
import re

_PREFERRED = re.compile(
    r"\bIf you rely on our team for trusted reporting\b"
    r"[\s\S]{0,300}?\bselect us as your Preferred Source on Google\b"
    r"[\s\S]{0,300}?\bEdited by:\s*[^\n]{1,100}?\s+Advertisement\s*$",
    re.I,
)

def reporting_scope(body):
    match = _PREFERRED.search(body)
    end = len(body[:match.start()].rstrip()) if match else len(body)
    clean = body[:end]
    return {'body': clean, 'source_body_start': 0, 'source_body_end': end,
            'rules': ['terminal_publisher_preferred_source_promotion'] if match else [],
            'original_body_sha256': hashlib.sha256(body.encode()).hexdigest(),
            'analysis_body_sha256': hashlib.sha256(clean.encode()).hexdigest(),
            'excluded_characters': len(body)-end}
