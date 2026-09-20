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
        '<div class="curve-row"><div><canvas id="chart" height="560"></canvas>\n<div class="legend" id="legend"></div></div><div id="ovCalSleeve"></div></div>',
        1,
    )
else:
    html = html.replace('height="420"', 'height="560"')

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

old_so = """function seriesOf(name){
  if(name===pickedName()){
    const sp=startPath();
    if(sp && sp.equity && sp.equity.some(v=>v!=null)) return sp.equity;
  }"""
new_so = """function seriesOf(name){
  if(name===pickedName()){
    const sp=startPath();
    if(sp && sp.equity && sp.equity.some(v=>v!=null)) return ovAlignSeries(sp.equity, sp.start);
    const b=typeof simBook==='function' ? simBook() : null;
    if(b && (b.daily||[]).length) return ovSeriesFromDaily(b.daily, sp && sp.start);
  }"""
if old_so in html:
    html = html.replace(old_so, new_so, 1)

old_draw = "  const dates=D.dates||[];\n  const X=i=>40+i/(Math.max(1,dates.length-1))*(W-70);"
new_draw = "  const allDates=D.dates||[];\n  const dates=typeof ovChartDates==='function'?ovChartDates():allDates;\n  const off=Math.max(0, allDates.length-dates.length);\n  const X=i=>40+i/(Math.max(1,dates.length-1))*(W-70);"
if old_draw in html:
    html = html.replace(old_draw, new_draw, 1)

old_line = "    seriesOf(k).forEach((v,j)=>{if(v==null)return; started?ctx.lineTo(X(j),Y(v)):ctx.moveTo(X(j),Y(v)); started=true;});"
new_line = "    seriesOf(k).forEach((v,j)=>{if(v==null)return; const x=X(Math.max(0,j-off)); started?ctx.lineTo(x,Y(v)):ctx.moveTo(x,Y(v)); started=true;});"
if old_line in html:
    html = html.replace(old_line, new_line, 1)

old = ' · <a href="../flatten-lookback/" style="color:#93c5fd">flatten lookback</a>'
new = ' · <a href="#ovCalBox" style="color:#fbbf24">new vs held</a>' + old
if "new vs held" not in html and old in html:
    html = html.replace(old, new, 1)

p.write_text(html, encoding="utf-8")
print("patched", p, "bytes", p.stat().st_size,
      "sleeve", html.count('id="ovCalSleeve"'),
      "ovJump", "function ovJumpDate" in html,
      "seriesOfFix", "ovSeriesFromDaily" in html and "ovAlignSeries(sp.equity" in html,
      "drawFix", "ovChartDates" in html)
