"""New-vs-held calendar + cash-start chart must survive a remine/restamp."""
from __future__ import annotations

from pathlib import Path

from src import factor_mine as fm

ROOT = Path(__file__).resolve().parent.parent
MINI = """<!DOCTYPE html>
<html><head><style>
 .curve-box{max-width:820px}
</style></head><body>
<div class="sub">
 · <a href="../flatten-lookback/" style="color:#93c5fd">flatten lookback</a>
</div>
<div class="cards" id="cards"></div>
<canvas id="chart" height="420"></canvas>
<div class="legend" id="legend"></div>
<script>
function seriesOf(name){
  if(name===pickedName()){
    const sp=startPath();
    if(sp && sp.equity && sp.equity.some(v=>v!=null)) return sp.equity;
  }
  return (D.series||{})[name]||[];
}
function draw(){
  const dates=D.dates||[];
  const X=i=>40+i/(Math.max(1,dates.length-1))*(W-70);
    seriesOf(k).forEach((v,j)=>{if(v==null)return; started?ctx.lineTo(X(j),Y(v)):ctx.moveTo(X(j),Y(v)); started=true;});
}
function renderAll(){
  renderCards();
  renderChips();
}
</script>
</body></html>
"""


def _ovcal():
    import importlib.util
    path = ROOT / "scripts" / "patch_fm_ovcal.py"
    spec = importlib.util.spec_from_file_location("patch_fm_ovcal", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_ovcal_patcher_injects_calendar_and_start_date_chart() -> None:
    mod = _ovcal()
    once = mod.patch_html(MINI)
    twice = mod.patch_html(once)
    assert once.count('id="ovCalBox"') == 1
    assert twice.count('id="ovCalBox"') == 1
    assert twice.count("/* fm-ovcal-begin */") == 1
    assert "function ovJumpDate" in twice
    assert "renderOvCal();" in twice
    assert "ovAlignSeries(sp.equity" in twice
    assert "ovSeriesFromDaily" in twice
    assert "function ovChartDates" in twice
    assert "const x=X(Math.max(0,j-off))" in twice
    assert 'href="#ovCalBox"' in twice
    assert "new vs held" in twice
    assert twice.count("function ovParseHeld") == 1


def test_write_dash_html_keeps_ovcal_after_restamp(tmp_path, monkeypatch) -> None:
    (tmp_path / "factor_mine_dash.html").write_text(
        MINI.replace("</script>", "__SIM_JS__\nconst B64 = \"__DATA__\";\n</script>"),
        encoding="utf-8",
    )
    (tmp_path / "factor_mine_sim.js").write_text("var FMSim={};", encoding="utf-8")
    dest = tmp_path / "out"
    dest.mkdir()
    monkeypatch.setattr(fm, "TEMPLATE", tmp_path / "factor_mine_dash.html")
    monkeypatch.setattr(fm, "SIM_JS", tmp_path / "factor_mine_sim.js")
    monkeypatch.setattr(fm, "DASH_DIR", dest)
    out = fm.write_dash_html({"to_date": "2026-09-21", "n_recipes": 1, "dates": ["2026-09-21"]})
    text = out.read_text(encoding="utf-8")
    assert 'id="ovCalBox"' in text
    assert "ovAlignSeries(sp.equity" in text
    assert "function ovChartDates" in text
    assert "var FMSim={}" in text


def test_baked_and_template_have_ovcal_hooks() -> None:
    tpl = (ROOT / "src" / "factor_mine_dash.html").read_text(encoding="utf-8")
    baked = (ROOT / "dashboard" / "factor-mine" / "index.html").read_text(
        encoding="utf-8")
    for html in (tpl, baked):
        assert 'id="ovCalBox"' in html
        assert "ovAlignSeries(sp.equity" in html
        assert "function ovChartDates" in html
        assert "renderOvCal();" in html


if __name__ == "__main__":
    test_ovcal_patcher_injects_calendar_and_start_date_chart()
    print("ok  test_ovcal_patcher_injects_calendar_and_start_date_chart")
