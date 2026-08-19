"""AB PIT backfill — loads from src/_ab_er.p* base64 parts."""
from __future__ import annotations
import base64
from pathlib import Path
_dir = Path(__file__).resolve().parent
_parts = sorted(_dir.glob("_ab_er.p*"))
if not _parts:
    raise RuntimeError("missing src/_ab_er.p* — deploy payload incomplete")
_src = base64.b64decode("".join(p.read_text().strip() for p in _parts))
exec(compile(_src, str(Path(__file__).resolve()), "exec"), globals())
