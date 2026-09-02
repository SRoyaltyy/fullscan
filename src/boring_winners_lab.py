"""Loader — concatenates _bw_lab.p* into this module."""
from pathlib import Path
_parts = sorted(Path(__file__).resolve().parent.glob("_bw_lab.p*"))
if not _parts:
    raise RuntimeError("missing src/_bw_lab.p* parts")
exec(compile("".join(p.read_text(encoding="utf-8") for p in _parts), str(Path(__file__).resolve()), "exec"), globals())
