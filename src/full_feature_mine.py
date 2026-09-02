"""Full-feature as-of panel + mine.

Large body is stored as base64 parts next to this file so GitHub pushes
stay small. Parts are decoded at import time.
"""
from __future__ import annotations

from pathlib import Path
import base64
import sys

_DIR = Path(__file__).resolve().parent
_PARTS = sorted(_DIR.glob("_ff_mine.b64.p*"))
if not _PARTS:
    raise ImportError("missing src/_ff_mine.b64.p* payload for full_feature_mine")
_blob = base64.b64decode("".join(p.read_text().strip() for p in _PARTS))
_mod = sys.modules[__name__]
exec(compile(_blob, str(_DIR / "_ff_mine_decoded.py"), "exec"), _mod.__dict__)
