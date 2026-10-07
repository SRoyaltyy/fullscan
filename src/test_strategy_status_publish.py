from __future__ import annotations

import json
import tempfile
from pathlib import Path

from src.strategy_status_publish import build

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
    assert st["theme_radar:c1"]["buys"] == ["TTT"]


def test_theme_radar_missing_file_is_missing() -> None:
    st = {r["name"]: r for r in build(_root(), D, fetch=lambda u: None)["strategies"]}
    assert st["theme_radar"]["status"] == "MISSING"


def test_never_writes_outside_status_dir() -> None:
    r = _root()
    before = {p: p.read_bytes() for p in r.rglob("*") if p.is_file()}
    build(r, D, fetch=lambda u: None)
    assert before == {p: p.read_bytes() for p in r.rglob("*") if p.is_file()}
