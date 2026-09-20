#!/usr/bin/env python3
"""Inject new-buy vs held calendar into factor-mine index.html."""
from pathlib import Path
import sys
here = Path(__file__).resolve().parent
p = Path(sys.argv[1] if len(sys.argv)>1 else "dashboard/factor-mine/index.html")
html = p.read_text(encoding="utf-8", errors="replace")
CSS = here.joinpath("fm_ovcal.css").read_text(encoding="utf-8")
DIV = here.joinpath("fm_ovcal.div.html").read_text(encoding="utf-8")
JS = here.joinpath("fm_ovcal.js").read_text(encoding="utf-8")
if "function ovSplitDays" not in html:
    if "</style>" in html:
        html = html.replace("</style>", CSS + "</style>", 1)
    if '<div class="cards" id="cards"></div>' in html and 'id="ovCal"' not in html:
        html = html.replace(
            '<div class="cards" id="cards"></div>',
            '<div class="cards" id="cards"></div>\n' + DIV,
            1,
        )
    old = ' · <a href="../flatten-lookback/" style="color:#93c5fd">flatten lookback</a>'
    new = ' · <a href="#ovCalBox" style="color:#fbbf24">new vs held</a>' + old
    if "new vs held" not in html and old in html:
        html = html.replace(old, new, 1)
    if "function renderAll(){" in html:
        html = html.replace("function renderAll(){", JS + "function renderAll(){", 1)
    if "renderOvCal();" not in html:
        html = html.replace("  renderCards();\n", "  renderCards();\n  renderOvCal();\n", 1)
    p.write_text(html, encoding="utf-8")
    print("patched", p, "bytes", p.stat().st_size)
else:
    print("already patched", p)
