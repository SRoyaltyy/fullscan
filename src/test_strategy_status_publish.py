from __future__ import annotations

import json
import tempfile
from pathlib import Path

from datetime import datetime
from zoneinfo import ZoneInfo

from src.strategy_status_publish import FetchError, build

D = "2030-01-02"


def _root() -> Path:
    r = Path(tempfile.mkdtemp())
    b = r / "data/day_board"; b.mkdir(parents=True)
    (b / "2030-01-01_strategy_tickets.json").write_text(json.dumps({"strategies": {
        "gone": {"family": "factor_mine", "status": "ok"}}}))
    (b / f"{D}_strategy_tickets.json").write_text(json.dumps({"strategies": {
        "fm_a": {"family": "factor_mine", "status": "ok", "buy": [{"ticker": "AAA"}], "sell": []},
        "fm_b": {"family": "factor_mine", "status": "sit", "note": "no same-day panel rows"},
        "flatten_robust": {"family": "flatten", "status": "ok", "buy": [], "sell": [{"ticker": "BBB"}]},
        "excel_all": {"family": "excel", "status": "no_session_signals"}}}))
    h = r / "research/hot_n4_clean_v4/forward_h1"; h.mkdir(parents=True)
    (h / "h1_log.jsonl").write_text(json.dumps({"kind": "plan", "date": D,
        "picks": [{"ticker": "KOD"}], "planned_sells": [{"ticker": "DNA", "shares": 5}]}) + "\n")
    s = r / "data/factor_mine/preopen/seals"; s.mkdir(parents=True)
    (s / f"{D}.json").write_text(json.dumps({"sleeves": {"union_hot_n4_h1_preopen": {
        "picks": [{"ticker": "CIEN"}], "reasons": []}}}))
    w = r / "data/webull_sim"; w.mkdir(parents=True)
    (w / "days.jsonl").write_text(json.dumps({"name": "x_webull_sim", "date": D, "fills": [
        {"date": D, "side": "buy", "ticker": "ZZZ", "shares": 1}]}) + "\n")
    return r


def test_every_family_ok_or_missing_by_name() -> None:
    plan = "cell,signal_date,ticker,entry_date,hold_days,rules_sha256,source\nc1,x,TTT,x,3,x,x\n# status=fires\n"
    st = {r["name"]: r for r in build(_root(), D, fetch=lambda u: plan)["strategies"]}
    assert st["fm_a"]["status"] == "OK" and st["fm_a"]["buys"] == ["AAA"]
    assert st["fm_b"]["status"] == "MISSING"
    assert st["gone"]["status"] == "MISSING"
    assert st["flatten_robust"]["sells"] == ["BBB"]
    assert st["excel_all"]["status"] == "SIT"
    assert st["h1"]["buys"] == ["KOD"] and st["h1"]["sells"] == ["DNA 5"]
    assert st["union_hot_n4_h1_preopen"]["buys"] == ["CIEN"]
    assert st["union_hot_n4_holdup_preopen"]["status"] == "MISSING"
    assert st["x_webull_sim"]["buys"] == ["ZZZ 1"]
    assert st["theme_radar:c1"]["shorts"] == ["TTT"] and st["theme_radar:c1"]["buys"] == []
    assert st["theme_radar:c1"]["research_only"] is True


ET = ZoneInfo("America/New_York")
CELLS = ("fpe_delta_t3_earn_today_3d", "fresh_dcp_t1_ep_ge03_2d", "fresh_dcp_t1_avoid_ah_3d")
HDR = "cell,signal_date,ticker,entry_date,hold_days,rules_sha256,source\n"


def _tr(fetch, hh=8, mm=0):
    now = datetime(2030, 1, 2, hh, mm, tzinfo=ET)
    doc = build(_root(), D, fetch=fetch, now=now)
    return {r["name"]: r for r in doc["strategies"] if r["family"] == "theme_radar"}, doc


def test_theme_radar_one_row_per_cell_ok_and_sit() -> None:
    plan = (HDR + f"{CELLS[0]},2030-01-01,bbb,{D},3,x,x\n{CELLS[0]},2030-01-01,AAA,{D},3,x,x\n"
            f"# status=fires rows=2 {CELLS[0]}=2 {CELLS[1]}=0 {CELLS[2]}=0\n"
            f"# built_at_utc=2030-01-02T10:20:00Z signal_date=2030-01-01\n")
    st, doc = _tr(lambda u: plan)
    assert set(st) == {f"theme_radar:{c}" for c in CELLS}
    ok = st[f"theme_radar:{CELLS[0]}"]
    assert ok["status"] == "OK" and ok["shorts"] == ["AAA", "BBB"] and ok["buys"] == []
    assert "RESEARCH ONLY" in ok["note"] and "05:20 ET" in ok["note"]
    assert st[f"theme_radar:{CELLS[1]}"]["status"] == "SIT"
    assert doc["counts"]["theme_radar"] == {"OK": 1, "SIT": 2, "MISSING": 0}


def test_theme_radar_no_fires_is_sit() -> None:
    st, _ = _tr(lambda u: HDR + "# status=no_fires rows=0\n")
    assert [r["status"] for r in st.values()] == ["SIT"] * 3
    assert all(r["shorts"] == [] and r["research_only"] for r in st.values())


def test_theme_radar_missing_file_waits_until_0900_then_missing() -> None:
    st, doc = _tr(lambda u: None, 8, 59)
    assert [r["status"] for r in st.values()] == ["WAIT"] * 3
    assert doc["counts"]["theme_radar"]["WAIT"] == 3
    st, _ = _tr(lambda u: None, 9, 0)
    assert [r["status"] for r in st.values()] == ["MISSING"] * 3


def test_theme_radar_fetch_error_is_not_ok() -> None:
    def boom(u):
        raise FetchError("HTTP 500")
    st, _ = _tr(boom, 9, 5)
    assert all(r["status"] == "MISSING" and "HTTP 500" in r["note"] for r in st.values())


def test_theme_radar_plan_built_after_open_is_missing() -> None:
    plan = HDR + f"{CELLS[0]},x,AAA,{D},3,x,x\n# status=fires rows=1\n# built_at_utc=2030-01-02T14:31:00Z\n"
    st, _ = _tr(lambda u: plan, 10, 0)
    assert st[f"theme_radar:{CELLS[0]}"]["status"] == "MISSING"


def test_never_writes_outside_status_dir() -> None:
    r = _root()
    before = {p: p.read_bytes() for p in r.rglob("*") if p.is_file()}
    build(r, D, fetch=lambda u: None)
    assert before == {p: p.read_bytes() for p in r.rglob("*") if p.is_file()}
