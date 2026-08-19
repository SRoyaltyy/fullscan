"""AB PIT backfill — loads body from _ab_backfill.b64 (keeps repo pushes small)."""
from __future__ import annotations

import base64
from pathlib import Path

_b64 = (Path(__file__).resolve().parent / "_ab_backfill.b64").read_text().strip()
_src = base64.b64decode(_b64)
exec(compile(_src, __file__, "exec"), globals())
