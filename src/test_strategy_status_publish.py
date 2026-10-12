from __future__ import annotations

import json
import os
import subprocess
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
    predict = f"01_daily/general/{D}_predict.md"
    assert st["fm_a"]["status"] == "MISSING" and predict in st["fm_a"]["note"]
    assert st["fm_a"]["buys"] == ["AAA"]
    assert st["fm_b"]["status"] == "MISSING"
    assert st["gone"]["status"] == "MISSING"
    assert st["flatten_robust"]["status"] == "OK" and st["flatten_robust"]["sells"] == ["BBB"]
    assert st["excel_all"]["status"] == "MISSING"
    assert "excel_bot/freeze_manifest.json" in st["excel_all"]["note"]
    assert "predict" not in st["excel_all"]["note"].lower()
    assert st["h1"]["status"] == "MISSING" and predict in st["h1"]["note"]
    assert st["h1"]["buys"] == ["KOD"] and st["h1"]["sells"] == ["DNA 5"]
    assert st["union_hot_n4_h1_preopen"]["buys"] == ["CIEN"]
    assert st["union_hot_n4_holdup_preopen"]["status"] == "MISSING"
    assert st["x_webull_sim"]["buys"] == ["ZZZ 1"]
    assert st["theme_radar:c1"]["shorts"] == ["TTT"] and st["theme_radar:c1"]["buys"] == []
    assert st["theme_radar:c1"]["status"] == "OK"
    assert st["theme_radar:c1"]["research_only"] is True
    assert "predict" not in st["theme_radar:c1"]["note"].lower()


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


def _commit(root: Path, when: str) -> None:
    env = os.environ.copy()
    env["GIT_AUTHOR_DATE"] = when
    env["GIT_COMMITTER_DATE"] = when
    subprocess.run(["git", "init", "-q"], cwd=root, check=True)
    subprocess.run(["git", "add", "-A"], cwd=root, check=True)
    subprocess.run(
        ["git", "-c", "user.name=Test", "-c", "user.email=test@example.com",
         "commit", "-q", "-m", "seal"],
        cwd=root, check=True, env=env,
    )


def _write_lock(root: Path, picks, n: int) -> None:
    man = root / "excel_bot"
    man.mkdir(exist_ok=True)
    (man / "freeze_manifest.json").write_text(json.dumps({
        "schema": 1,
        "entries": [
            {"kind": "lock", "signal_date": "2029-12-31", "n_picks": n,
             "sha256": "cf5804cf653cb4f9aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
             "pick_ids": picks},
            {"kind": "first_open", "signal_date": "2029-12-31", "n_picks": n,
             "sha256": "cf5804cf653cb4f9aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
             "pick_ids": [["ZZZ", "ignored"]]},
        ],
    }), encoding="utf-8")


def test_excel_row_uses_prior_session_lock_not_same_day_signal() -> None:
    """10-08 orders are the prior lock committed before the open, not a same-day signal."""
    r = _root()
    _write_lock(r, [["BBB", "L1_long"], ["AAA", "L3_long"]], 2)
    before = (r / "excel_bot/freeze_manifest.json").read_bytes()
    _commit(r, "2029-12-31T20:00:00-05:00")
    st = {row["name"]: row for row in build(r, D, fetch=lambda u: None)["strategies"]}
    row = st["excel_all"]
    assert row["family"] == "excel_bot"
    assert row["status"] == "OK"
    assert row["buys"] == ["BBB L1_long", "AAA L3_long"]
    assert row["sells"] == []
    assert "excel_bot/freeze_manifest.json" in row["note"]
    assert "n_picks=2" in row["note"]
    assert "sha=cf5804cf653c" in row["note"]
    assert "signal_date" not in row["note"]
    assert "predict" not in row["note"].lower()
    assert "ZZZ" not in row["buys"]
    assert (r / "excel_bot/freeze_manifest.json").read_bytes() == before
    ticket = r / "data/day_board" / f"{D}_strategy_tickets.json"
    doc = json.loads(ticket.read_text())
    doc["strategies"]["excel_all"] = {
        "family": "excel", "status": "ok", "buy": [{"ticker": "QQQ"}], "sell": [],
    }
    ticket.write_text(json.dumps(doc))
    st = {row["name"]: row for row in build(r, D, fetch=lambda u: None)["strategies"]}
    assert st["excel_all"]["status"] == "OK"
    assert st["excel_all"]["buys"] == ["BBB L1_long", "AAA L3_long"]
    assert "QQQ" not in st["excel_all"]["buys"]
    assert "signal_date" not in st["excel_all"]["note"]
    assert "predict" not in st["excel_all"]["note"].lower()


def test_excel_lock_after_open_is_not_ok() -> None:
    r = _root()
    _write_lock(r, [["BBB", "L1_long"]], 1)
    _commit(r, "2030-01-02T09:30:00-05:00")
    row = {x["name"]: x for x in build(r, D, fetch=lambda u: None)["strategies"]}["excel_all"]
    assert row["status"] == "MISSING"
    assert "excel_bot/freeze_manifest.json" in row["note"]
    assert "09:30" in row["note"]
    assert "predict" not in row["note"].lower()


def test_factor_mine_and_h1_require_predict_presence_only() -> None:
    rel = f"01_daily/general/{D}_predict.md"
    r = _root()
    ticket = r / "data/day_board" / f"{D}_strategy_tickets.json"
    doc = json.loads(ticket.read_text())
    doc["strategies"]["fm_sit"] = {"family": "factor_mine", "status": "sit", "buy": [], "sell": []}
    ticket.write_text(json.dumps(doc))
    st = {row["name"]: row for row in build(r, D, fetch=lambda u: None)["strategies"]}
    assert st["fm_a"]["status"] == "MISSING" and rel in st["fm_a"]["note"]
    assert st["fm_sit"]["status"] == "MISSING" and rel in st["fm_sit"]["note"]
    assert st["h1"]["status"] == "MISSING" and rel in st["h1"]["note"]
    assert st["flatten_robust"]["status"] == "OK"
    assert st["union_hot_n4_h1_preopen"]["status"] == "OK"
    path = r / rel
    path.parent.mkdir(parents=True)
    path.write_text("x", encoding="utf-8")  # presence only, not a quality check
    st = {row["name"]: row for row in build(r, D, fetch=lambda u: None)["strategies"]}
    assert st["fm_a"]["status"] == "OK" and st["fm_a"]["buys"] == ["AAA"]
    assert st["fm_sit"]["status"] == "SIT"
    assert st["h1"]["status"] == "OK"
    assert "theme_radar:fpe_delta_t3_earn_today_3d" in st


def test_predict_not_on_main_refuses_ok_even_if_worktree_has_it() -> None:
    rel = f"01_daily/general/{D}_predict.md"
    r = _root()
    _commit(r, "2030-01-02T08:00:00-05:00")
    subprocess.run(["git", "update-ref", "refs/remotes/origin/main", "HEAD"], cwd=r, check=True)
    path = r / rel
    path.parent.mkdir(parents=True)
    path.write_text("worktree only\n", encoding="utf-8")
    st = {row["name"]: row for row in build(r, D, fetch=lambda u: None)["strategies"]}
    assert st["fm_a"]["status"] == "MISSING" and rel in st["fm_a"]["note"]
    assert st["h1"]["status"] == "MISSING"
    subprocess.run(["git", "add", "-A"], cwd=r, check=True)
    env = os.environ.copy()
    env["GIT_AUTHOR_DATE"] = "2030-01-02T08:05:00-05:00"
    env["GIT_COMMITTER_DATE"] = "2030-01-02T08:05:00-05:00"
    subprocess.run(
        ["git", "-c", "user.name=Test", "-c", "user.email=test@example.com",
         "commit", "-q", "-m", "predict"],
        cwd=r, check=True, env=env,
    )
    subprocess.run(["git", "update-ref", "refs/remotes/origin/main", "HEAD"], cwd=r, check=True)
    st = {row["name"]: row for row in build(r, D, fetch=lambda u: None)["strategies"]}
    assert st["fm_a"]["status"] == "OK"
    assert st["h1"]["status"] == "OK"
    assert st["theme_radar:fpe_delta_t3_earn_today_3d"]["status"] in ("WAIT", "MISSING")
