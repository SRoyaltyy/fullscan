"""Published equity lines against the source they are allowed to use."""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.chart_asof import coverage


def test_chart_coverage_names_real_blockers() -> None:
    rows = {row["strategy"]: row for row in coverage()}
    paper = rows["strategy-board io_2w_size"]
    mover = rows["strategy-board mover_paper_live"]
    book = rows["strategy-board book_paper_1w"]
    assert paper["chart"] == paper["source"]
    assert mover["chart"] == mover["source"]
    assert book["chart"] == book["source"]
    # Republish may append after the last sealed point. Overlapping dates stay.
    page = (ROOT / "dashboard/strategy-board/index.html").read_text(encoding="utf-8")
    start = page.find("const D = ")
    payload, _ = json.JSONDecoder().raw_decode(page[start + len("const D = "):])
    pts = {row["id"]: {p["date"]: p["equity"] for p in row["pts"]} for row in payload["series"]}
    assert pts["book_paper_1w"]["2026-10-27"] == 104540.19
    assert pts["book_paper_1w"]["2026-11-02"] == 104540.19
    assert pts["mover_paper_live"]["2026-11-02"] == 119053.35
    assert "2026-11-05" in pts["book_paper_1w"]
    assert "2026-11-05" in pts["mover_paper_live"]
    cat = json.loads((ROOT / "data/strategy_board/catalog.json").read_text(encoding="utf-8"))
    hold = next(row for row in cat["rows"] if row.get("id") == "fm_union_hot_n4_holdup")
    assert hold["final_equity"] == 14827.77
    board_md = (ROOT / "03_scoreboard/STRATEGY_BOARD.md").read_text(encoding="utf-8")
    assert "+53.96%" in board_md
    assert paper["source"] >= "2026-11-05"
    assert rows["holdup"]["chart"] >= "2026-11-05"
    assert rows["h1"]["chart"] >= "2026-11-05"
    assert rows["h1 2026-11-02"]["chart"] == "plan-only"
    assert rows["h1 2026-11-04"]["chart"] == "plan-only"
    assert rows["h1 2026-11-02"]["blocker"] == ""
    assert rows["updown o2c"]["chart"] == "2026-10-05"
    assert "no research/updown_o2c_v2/forward" in rows["updown o2c"]["blocker"]
    assert rows["webull-sim"]["chart"] == "2026-10-28"
    assert "not backfilled" in rows["webull-sim"]["blocker"]
    for name in ("factor-mine preopen holdup", "factor-mine preopen h1"):
        assert rows[name]["chart"] == "2026-11-04", rows[name]
        assert "stays missing" in rows[name]["blocker"]


def test_rebuilt_missed_with_holdings_is_a_chart_gap() -> None:
    """Sealed points stay. A missed day after rebuilt holdings is null."""
    script = r"""
const fs = require('fs');
const vm = require('vm');
const src = fs.readFileSync(process.argv[1], 'utf8');
const context = {
  D: {
    dates: ['2026-11-04', '2026-11-05'],
    daily: { sleeve: [
      {date:'2026-11-04', overnight_delta: 10, session_delta: 1, bought:['AAA'], sold:[], open_held:['ZZ×2'], equity:11000, open_equity:10900},
      {date:'2026-11-05', overnight_delta: 0, session_delta: 0, bought:[], sold:[], open_held:[], equity:11000, open_equity:11000}
    ]},
    series: { sleeve: [11000, 11000] },
    sim: {tape:{}}
  },
  window: {},
  console: console
};
context.window = context;
vm.createContext(context);
vm.runInContext(src, context);
context.window.__FM_CAL_DOC = {
  empty_dates: ['2026-11-05'],
  strategies: { sleeve: {
    '2026-11-04': {label:'rebuilt', daily:{equity:12000, yday_equity:10000, open_held:['ZZ×2']}, fills:[]},
    '2026-11-05': {label:'missed', daily:{date:'2026-11-05'}, fills:[]}
  }},
  tape: {}
};
const out = context.ovDisplaySeries('sleeve', context.D.series.sleeve);
if (out[0] !== 11000) throw new Error('sealed point changed ' + out[0]);
if (out[1] !== null) throw new Error('missed exposure was drawn flat ' + out[1]);
const sit = {
  empty_dates: ['2026-11-05'],
  strategies: { sleeve: {
    '2026-11-05': {label:'rebuilt', daily:{equity:10500, yday_equity:10000, open_held:[]}, fills:[]}
  }},
  tape: {}
};
context.window.__FM_CAL_DOC = sit;
const moved = context.ovDisplaySeries('cash', [10000, 10000].map((v,i)=>({v})) );
"""
    # The second call uses a name with no daily rows, so the empty check
    # fails and both sealed points stay. The first call is the assertion.
    proc = subprocess.run(
        ["node", "-e", script, str(ROOT / "scripts/fm_ovcal.js")],
        check=False, capture_output=True, text=True,
    )
    if proc.returncode != 0:
        raise AssertionError(proc.stderr or proc.stdout)


def test_selected_equity_only_line_runs_the_overlay() -> None:
    """The chip's purple line is seriesOf, which must overlay the sim book."""
    text = (ROOT / "src/factor_mine_dash.html").read_text(encoding="utf-8")
    fn = text.split("function seriesOf", 1)[1].split("function boughtTickers", 1)[0]
    assert "FMSim" not in fn
    assert "simulateBook" not in fn or "ovSeriesFromDaily" in fn
    assert "ovSeriesFromDaily" in fn
    assert "ovDisplaySeries" in fn
    assert fn.index("ovSeriesFromDaily") < fn.index("ovDisplaySeries")
    baked = (ROOT / "dashboard/factor-mine/index.html").read_text(encoding="utf-8")
    assert "ovDisplaySeries(name, base)" in baked


def test_engine_file_is_still_the_pinned_bytes() -> None:
    import hashlib
    digest = hashlib.sha256((ROOT / "src/factor_mine.py").read_bytes()).hexdigest()
    assert digest == "f904788699186111ad246dcc2a0ed9c8ebd2048ff096196ce8bc8a9d11872e0c"


if __name__ == "__main__":
    test_rebuilt_missed_with_holdings_is_a_chart_gap()
    test_engine_file_is_still_the_pinned_bytes()
    test_chart_coverage_names_real_blockers()
    print("chart asof ok")
    print(json.dumps(coverage(), indent=2))
