#!/usr/bin/env python3
"""Inject / refresh new-buy vs held calendar on factor-mine index.html.

Also slices the equity curve to the selected cash-start date. Idempotent:
a remine / restamp / Pages overlay can run this after every bake.
"""
from __future__ import annotations

from pathlib import Path
import re
import sys

HERE = Path(__file__).resolve().parent
CSS_MARK_BEGIN = "/* fm-ovcal-begin */"
CSS_MARK_END = "/* fm-ovcal-end */"


def _assets() -> tuple[str, str, str]:
    css = HERE.joinpath("fm_ovcal.css").read_text(encoding="utf-8")
    div = HERE.joinpath("fm_ovcal.div.html").read_text(encoding="utf-8")
    js = HERE.joinpath("fm_ovcal.js").read_text(encoding="utf-8")
    js = js.lstrip("\n")
    if not js.endswith("\n"):
        js += "\n"
    return css, div, js


def patch_html(html: str) -> str:
    """Return HTML with the calendar + start-date chart fix applied."""
    css, div, js = _assets()

    html = re.sub(
        r"\n?/\* fm-ovcal-begin \*/.*?/\* fm-ovcal-end \*/\n?",
        "",
        html,
        count=1,
        flags=re.S,
    )
    html = re.sub(
        r"\n \.ovcal-wrap\{.*?\n #ovCalSleeve\{min-width:0\}\n canvas\{width:100%;max-width:100%;height:560px;[^}]*\}\n",
        "",
        html,
        count=1,
        flags=re.S,
    )
    if ".ovcal-wrap{" not in html and "</style>" in html:
        html = html.replace(
            "</style>",
            f"\n{CSS_MARK_BEGIN}{css}{CSS_MARK_END}\n</style>",
            1,
        )

    html = re.sub(
        r'<details class="ovcal-wrap" id="ovCalBox"[^>]*>.*?</details>\s*',
        div + "\n",
        html,
        count=1,
        flags=re.S,
    )
    if 'id="ovCal"' not in html and '<div class="cards" id="cards"></div>' in html:
        html = html.replace(
            '<div class="cards" id="cards"></div>',
            '<div class="cards" id="cards"></div>\n' + div,
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
            "\n" + js + "function renderAll(){",
            html,
            count=1,
        )
    elif "function renderAll(){" in html:
        html = html.replace("function renderAll(){", js + "function renderAll(){", 1)

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
    return html


def patch_file(path: Path | str) -> Path:
    p = Path(path)
    html = p.read_text(encoding="utf-8", errors="replace")
    new = patch_html(html)
    if new != html:
        p.write_text(new, encoding="utf-8")
    print(
        "patched", p, "bytes", p.stat().st_size,
        "sleeve", new.count('id="ovCalSleeve"'),
        "ovJump", "function ovJumpDate" in new,
        "seriesOfFix", "ovSeriesFromDaily" in new and "ovAlignSeries(sp.equity" in new,
        "drawFix", "ovChartDates" in new,
    )
    return p


def main(argv: list[str] | None = None) -> None:
    args = list(sys.argv[1:] if argv is None else argv)
    target = Path(args[0] if args else "dashboard/factor-mine/index.html")
    patch_file(target)


if __name__ == "__main__":
    main()
