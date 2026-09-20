#!/usr/bin/env python3
"""Inject / refresh new-buy vs held calendar on factor-mine index.html."""
from pathlib import Path
import re
import sys

here = Path(__file__).resolve().parent
p = Path(sys.argv[1] if len(sys.argv) > 1 else "dashboard/factor-mine/index.html")
html = p.read_text(encoding="utf-8", errors="replace")
CSS = here.joinpath("fm_ovcal.css").read_text(encoding="utf-8")
DIV = here.joinpath("fm_ovcal.div.html").read_text(encoding="utf-8")
JS = here.joinpath("fm_ovcal.js").read_text(encoding="utf-8")

html = re.sub(r"\n \.ovcal-wrap\{.*?(?=\n @media|\n </style>|\n\.)", "", html, count=1, flags=re.S)
if ".ovcal-wrap{" not in html and "</style>" in html:
    html = html.replace("</style>", CSS + "\n</style>", 1)
elif ".ovcal-wrap{" not in html:
    pass
else:
    html = html.replace("</style>", CSS + "\n</style>", 1)

html = re.sub(
    r'<details class="ovcal-wrap" id="ovCalBox"[^>]*>.*?</details>\s*',
    DIV + "\n",
    html,
    count=1,
    flags=re.S,
)
if 'id="ovCal"' not in html and '<div class="cards" id="cards"></div>' in html:
    html = html.replace(
        '<div class="cards" id="cards"></div>',
        '<div class="cards" id="cards"></div>\n' + DIV,
        1,
    )

if 'id="ovCalSleeve"' not in html:
    html = html.replace(
        '<canvas id="chart" height="420"></canvas>\n<div class="legend" id="legend"></div>',
        '<div class="curve-row"><div><canvas id="chart" height="420"></canvas>\n<div class="legend" id="legend"></div></div><div id="ovCalSleeve"></div></div>',
        1,
    )

if "function ovParseHeld" in html:
    html = re.sub(
        r"\nfunction ovParseHeld[\s\S]*?\nfunction renderAll\(\)\{",
        "\n" + JS + "function renderAll(){",
        html,
        count=1,
    )
elif "function renderAll(){" in html:
    html = html.replace("function renderAll(){", JS + "function renderAll(){", 1)

if "renderOvCal();" not in html:
    html = html.replace("  renderCards();\n", "  renderCards();\n  renderOvCal();\n", 1)

old = ' · <a href="../flatten-lookback/" style="color:#93c5fd">flatten lookback</a>'
new = ' · <a href="#ovCalBox" style="color:#fbbf24">new vs held</a>' + old
if "new vs held" not in html and old in html:
    html = html.replace(old, new, 1)

p.write_text(html, encoding="utf-8")
print("patched", p, "bytes", p.stat().st_size,
      "ovCal", html.count('id="ovCal"'),
      "sleeve", html.count('id="ovCalSleeve"'),
      "ovSimBook", html.count("function ovSimBook"),
      "renderOvCal", html.count("function renderOvCal"))
