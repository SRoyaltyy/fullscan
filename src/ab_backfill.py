"""AB PIT backfill — loads body from split base64 parts."""
from __future__ import annotations

import base64
from pathlib import Path

_dir = Path(__file__).resolve().parent
_parts = sorted(_dir.glob("_ab_backfill.b64.p*"))
if not _parts:
    raise RuntimeError("missing src/_ab_backfill.b64.p* payload files")
_b64 = "".join(p.read_text().strip() for p in _parts)
_src = base64.b64decode(_b64)
exec(compile(_src, __file__, "exec"), globals())
