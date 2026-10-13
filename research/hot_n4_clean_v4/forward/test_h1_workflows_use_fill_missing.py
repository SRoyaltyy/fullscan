"""Every h1 workflow step that runs a forward script must go through h1_fill_missing.py.

10-13: 'Refresh the h1 close display' ran h1_closes.py bare, so ledger._note
refused the 10-13 plan because 10-09's noted fill_missing plan looked open.
"""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
WF = ROOT / ".github" / "workflows"
SCRIPTS = ("forward.py", "prices.py", "h1_closes.py", "h1_seal_gate.py")
_RUN = re.compile(r"python3\s+((?:\S+/)?h1_fill_missing\.py\s+)?research/hot_n4_clean_v4/forward/(\w+\.py)")


def test_h1_steps_wrap_with_fill_missing():
    bad = []
    for name in ("h1_fill.yml", "h1_forward.yml", "h1_open_fill.yml"):
        for n, line in enumerate((WF / name).read_text(encoding="utf-8").splitlines(), 1):
            for m in _RUN.finditer(line):
                if m.group(2) in SCRIPTS and not m.group(1):
                    bad.append(f"{name}:{n} {m.group(2)}")
    assert not bad, f"h1 steps not wrapped by h1_fill_missing.py: {bad}"
