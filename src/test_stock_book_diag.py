"""Tests for the Stock Book upstream diagnostic.

Run: python -m src.test_stock_book_diag
"""
from __future__ import annotations

import json
from pathlib import Path

from src import stock_book_diag as diag
from src.stock_book_diag import FileCheck, aggregate_status, inspect_kind


ROOT = Path(__file__).resolve().parent.parent
WF = ROOT / ".github" / "workflows" / "stock_book_diag.yml"


def _fc(role: str, status: str, key: str = "x") -> FileCheck:
    return FileCheck(key=key, name=key, path=key, role=role, status=status)


def test_aggregate_all_required_ok():
    files = [_fc("required", "OK", "a"), _fc("required", "OK", "b"),
             _fc("optional", "MISSING", "c")]
    status, ready, n_ok, n_req, n_opt_ok, n_opt = aggregate_status(files)
    assert status == "OK"
    assert ready is True
    assert (n_ok, n_req) == (2, 2)
    assert (n_opt_ok, n_opt) == (0, 1)


def test_aggregate_partial_and_fail():
    partial = [_fc("required", "OK", "a"), _fc("required", "FAIL", "b")]
    status, *_ = aggregate_status(partial)
    assert status == "PARTIAL"

    missing_mix = [_fc("required", "OK", "a"), _fc("required", "MISSING", "b")]
    status, *_ = aggregate_status(missing_mix)
    assert status == "PARTIAL"

    all_bad = [_fc("required", "FAIL", "a"), _fc("required", "MISSING", "b")]
    status, *_ = aggregate_status(all_bad)
    assert status == "FAIL"

    all_missing = [_fc("required", "MISSING", "a")]
    status, *_ = aggregate_status(all_missing)
    assert status == "FAIL"


def test_aggregate_inputs_blocked():
    files = [
        _fc("input", "MISSING", "in"),
        _fc("required", "MISSING", "out"),
    ]
    status, ready, *_ = aggregate_status(files)
    assert status == "FAIL"
    assert ready is False


def test_aggregate_optional_only_catalyst():
    missing = [_fc("input", "OK", "in"), _fc("optional", "MISSING", "dossiers")]
    status, ready, *_ = aggregate_status(missing)
    assert ready is True
    assert status == "PARTIAL"

    qc_fail = [_fc("input", "OK", "in"), _fc("optional", "FAIL", "dossiers")]
    status, *_ = aggregate_status(qc_fail)
    assert status == "FAIL"

    ok = [_fc("input", "OK", "in"), _fc("optional", "OK", "dossiers")]
    status, *_ = aggregate_status(ok)
    assert status == "OK"


def test_inspect_empty_and_timeout(tmp_path: Path):
    empty = tmp_path / "empty.md"
    empty.write_text("", encoding="utf-8")
    status, reason, _ = inspect_kind("md", empty, "2026-08-31")
    assert status == "FAIL"
    assert "empty" in reason

    stub = tmp_path / "stub.md"
    stub.write_text("LLM request timed out.\nThe model did not produce a response.",
                    encoding="utf-8")
    status, reason, _ = inspect_kind("md", stub, "2026-08-31")
    assert status == "FAIL"
    assert "timeout" in reason

    missing = tmp_path / "nope.md"
    status, reason, _ = inspect_kind("md", missing, "2026-08-31")
    assert status == "MISSING"


def test_inspect_weather_and_ab(tmp_path: Path):
    wx = tmp_path / "wx.json"
    wx.write_text(json.dumps({
        "date": "2026-08-31",
        "signals": {"sectors": {s: {"dir": "flat"} for s in (
            "Technology", "Energy", "Financial", "Healthcare", "Utilities",
        )}},
    }), encoding="utf-8")
    status, reason, _ = inspect_kind("weather", wx, "2026-08-31")
    assert status == "OK", reason

    thin = tmp_path / "thin.json"
    thin.write_text(json.dumps({"date": "2026-08-31", "signals": {"sectors": {}}}
                               ), encoding="utf-8")
    status, reason, _ = inspect_kind("weather", thin, "2026-08-31")
    assert status == "FAIL"
    assert "too_few_sectors" in reason

    ab = tmp_path / "ab.csv"
    rows = ["Ticker,score_enriched\n"] + [f"AAA{i},{i + 1}\n" for i in range(60)]
    ab.write_text("".join(rows), encoding="utf-8")
    status, reason, _ = inspect_kind("ab_enriched", ab, "2026-08-31")
    assert status == "OK", reason

    zeros = tmp_path / "zeros.csv"
    zeros.write_text("Ticker,score_enriched\n" + "AAA,0\n" * 60, encoding="utf-8")
    status, reason, _ = inspect_kind("ab_enriched", zeros, "2026-08-31")
    assert status == "FAIL"
    assert "zero" in reason


def test_inspect_morning_bootstrap(tmp_path: Path):
    js = tmp_path / "2026-08-31_research.json"
    js.write_text(json.dumps({
        "date": "2026-08-31",
        "phase": "morning_bootstrap",
        "cards": [{}] * 25,
    }), encoding="utf-8")
    status, reason, _ = inspect_kind("map_heat_research_json", js, "2026-08-31")
    assert status == "FAIL"
    assert "bootstrap" in reason


def test_inspect_review_sidecar_is_not_packet_fail(tmp_path: Path):
    p = tmp_path / "review.json"
    p.write_text(json.dumps({
        "date": "2026-08-31",
        "ok": False,
        "notes": "map_heat_research missing",
    }), encoding="utf-8")
    status, reason, _ = inspect_kind("grok_review", p, "2026-08-31")
    assert status == "OK", reason
    assert "ok=False" in reason


def test_inspect_stock_book_json(tmp_path: Path):
    p = tmp_path / "book.json"
    p.write_text(json.dumps({
        "meta": {"date": "2026-08-31"},
        "books": {"1d": {"buy": [{"ticker": "AAA"}], "sell": []}},
    }), encoding="utf-8")
    status, reason, _ = inspect_kind("stock_book_json", p, "2026-08-31")
    assert status == "OK", reason

    empty = tmp_path / "empty_book.json"
    empty.write_text(json.dumps({
        "meta": {"date": "2026-08-31"},
        "books": {"1d": {"buy": [], "sell": []}},
    }), encoding="utf-8")
    status, reason, _ = inspect_kind("stock_book_json", empty, "2026-08-31")
    assert status == "FAIL"


def test_inspect_join(tmp_path: Path):
    p = tmp_path / "join.csv"
    p.write_text("Ticker,score_norm\n" + "AAA,1\n" * 1200, encoding="utf-8")
    status, reason, _ = inspect_kind("join_ranked", p, "2026-08-31")
    assert status == "OK", reason

    tiny = tmp_path / "tiny.csv"
    tiny.write_text("Ticker,score_norm\nAAA,1\n", encoding="utf-8")
    status, reason, _ = inspect_kind("join_ranked", tiny, "2026-08-31")
    assert status == "FAIL"
    assert "too_few_rows" in reason


def test_specs_match_user_contract():
    specs = {s["key"]: s for s in diag.workflow_specs("2026-08-31")}
    assert set(specs) == {
        "postclose", "finviz", "preopen", "weather", "ab",
        "catalyst", "stock_book", "publish",
    }
    post = {f["key"] for f in specs["postclose"]["files"]}
    assert post == {"baseline_json", "baseline_md", "heat_json", "heat_md"}
    assert all(f["role"] == "required" for f in specs["postclose"]["files"]
               if f["key"] in ("baseline_json", "heat_json"))

    pre_req = {f["key"] for f in specs["preopen"]["files"] if f["role"] == "required"}
    assert {"parsed", "events", "judge", "research_json", "research_md",
            "general", "qc", "status", "review"} <= pre_req
    assert len([f for f in specs["preopen"]["files"]
                if f["key"].startswith("sector_")]) == 11
    assert any(f["key"] == "actions" and f["role"] == "optional"
               for f in specs["preopen"]["files"])

    book_in = {f["key"] for f in specs["stock_book"]["files"] if f["role"] == "input"}
    assert book_in == {
        "in_general", "in_board", "in_actions", "in_digest",
        "in_judge", "in_weather", "in_ab",
    }
    book_req = {f["key"] for f in specs["stock_book"]["files"] if f["role"] == "required"}
    assert book_req == {"join", "peers", "book_json", "book_md"}


def test_audit_live_days():
    """Real packet days: 08-28 is a completed pre-open; 08-31 is scrape+baseline."""
    d28 = diag.audit("2026-08-28", gh_runs={})
    by = {w.key: w for w in d28.workflows}
    assert by["postclose"].status in ("OK", "PARTIAL")
    assert by["finviz"].status == "OK"
    assert by["preopen"].n_req >= 11
    # 08-28 ran pre-open; ranker inputs may still miss sector board / actions.
    assert d28.overall in ("OK", "PARTIAL", "FAIL")

    d31 = diag.audit("2026-08-31", gh_runs={})
    by31 = {w.key: w for w in d31.workflows}
    assert by31["finviz"].status == "OK"
    assert by31["postclose"].status == "OK"
    # Morning refresh is the hole on this packet; other pre-open files landed.
    assert by31["preopen"].status == "PARTIAL"
    research = next(f for f in by31["preopen"].files if f.key == "research_md")
    assert research.status == "MISSING"
    md = diag.render_markdown(d31)
    assert "Stock Book readiness — 2026-08-31" in md
    assert "READY" in md or "BLOCKED" in md
    assert "Post-close research" in md
    assert "Pre-Open ALL" in md
    assert "Today's actions" in md
    assert "ACTION BUY" in md or "**BUY**" in md
    assert "Dashboard / .io" in md
    assert "⬜ MISSING" in md or "MISSING" in md


def test_decisions_trace_to_inputs():
    from src.stock_book_diag_signals import (
        BOX_KEYS, FACTOR_TRACE, extract_decisions, polarity,
    )
    assert polarity(0.94) == "good"
    assert polarity(-0.07) == "bad"
    assert polarity(0.0) == "neutral"
    dec = extract_decisions("2026-08-31")
    assert dec["present"] is True
    buys = dec["horizons"]["1d"]["buy"]
    sells = dec["horizons"]["1d"]["sell"]
    assert len(sells) >= 10
    assert dec["market"]["state"] == "hard_red"
    assert len(dec["bull_watch"]) >= 10
    # HARD_RED may still list probable longs; empty BUY is only a stand-down
    # when nothing clocked.
    if not buys:
        assert dec["intentional_stand_down"] is True
    top = buys[0] if buys else sells[0]
    assert top["ticker"]
    assert top["rank"] == 1
    assert top["sleeve"] is True  # rank metadata; SELL is never paper-filled
    assert set(top["boxes"]) == set(BOX_KEYS)
    assert set(top["domains"]) == {
        "market", "parent", "child", "company", "setup", "flow",
    }
    # Scores on the book row must color the matching box.
    assert top["boxes"]["join"] == polarity(top["scores"]["s_join"])
    assert top["boxes"]["ab"] == polarity(top["scores"]["s_ab_intrinsic"])
    assert top["reasons"]
    assert top["bear_decision"]
    files = {s["key"]: s["file"] for s in dec["factor_trace"]}
    assert "2026-08-31" in files["join"]
    assert files["ab"].endswith("_ab_checklist_enriched.csv")
    assert files["gen"].endswith("_predict.md")
    assert len(FACTOR_TRACE) == 12
    from src.stock_book_diag_signals import (
        render_actions_markdown, render_actions_plain,
    )
    banner = render_actions_plain(dec)
    assert "ACTIONS" in banner
    assert "MARKET HARD_RED" in banner
    assert "BULL DECISIONS" in banner
    assert "ACTION SELL" in banner
    if buys:
        assert "ACTION BUY" in banner
        assert top["ticker"] in banner
    else:
        assert "(none)" in banner
    act_md = "\n".join(render_actions_markdown(dec))
    assert top["ticker"] in act_md
    assert "Bull decisions" in act_md
    assert "Market gate" in act_md
    md = "\n".join(__import__(
        "src.stock_book_diag_signals", fromlist=["render_decisions_markdown"]
    ).render_decisions_markdown(dec))
    assert top["ticker"] in md
    assert "1d_top" in md
    assert "sroyaltyy.github.io/fullscan/dashboard" in "\n".join(dec["how"])


def test_workflow_yaml_is_read_only():
    text = WF.read_text(encoding="utf-8")
    assert "name: Stock Book readiness" in text
    assert "src.stock_book_diag" in text
    assert "--as-of" in text
    assert "--rebuild-if-missing" in text
    assert "self-hosted" not in text
    assert "contents: read" in text
    assert "workflow_dispatch" in text
    # Must not dispatch or heal other jobs.
    assert "gh workflow run" not in text
    assert "GROK_ONLY" not in text


def test_action_ok_historical_and_today():
    d13 = diag.audit("2026-08-13", gh_runs={})
    assert d13.ranker_ready is True
    assert diag.action_ok(d13) is True
    d31 = diag.audit("2026-08-31", gh_runs={})
    assert d31.decisions.get("market", {}).get("state") == "hard_red"
    assert diag.action_ok(d31) is True


def test_render_json_roundtrip():
    report = diag.audit("2026-08-31", gh_runs={})
    payload = diag.report_to_json(report)
    assert payload["date"] == "2026-08-31"
    assert payload["overall"] in ("OK", "PARTIAL", "FAIL")
    assert len(payload["workflows"]) == 8
    assert {w["key"] for w in payload["workflows"]} == {
        "postclose", "finviz", "preopen", "weather", "ab",
        "catalyst", "stock_book", "publish",
    }
    assert "horizons" in (payload.get("decisions") or {})


if __name__ == "__main__":
    # pytest-style helpers that take tmp_path need a real temp dir.
    import tempfile
    import traceback

    passed = 0
    failed = 0

    def _run(fn, *args):
        global passed, failed
        try:
            fn(*args)
            print(f"  ok  {fn.__name__}")
            passed += 1
        except Exception:
            failed += 1
            print(f"  FAIL {fn.__name__}")
            traceback.print_exc()

    _run(test_aggregate_all_required_ok)
    _run(test_aggregate_partial_and_fail)
    _run(test_aggregate_inputs_blocked)
    _run(test_aggregate_optional_only_catalyst)
    with tempfile.TemporaryDirectory() as td:
        p = Path(td)
        _run(test_inspect_empty_and_timeout, p)
        _run(test_inspect_weather_and_ab, p)
        _run(test_inspect_morning_bootstrap, p)
        _run(test_inspect_review_sidecar_is_not_packet_fail, p)
        _run(test_inspect_stock_book_json, p)
        _run(test_inspect_join, p)
    _run(test_specs_match_user_contract)
    _run(test_decisions_trace_to_inputs)
    _run(test_audit_live_days)
    _run(test_workflow_yaml_is_read_only)
    _run(test_action_ok_historical_and_today)
    _run(test_render_json_roundtrip)
    print(f"{passed} passed, {failed} failed")
    raise SystemExit(1 if failed else 0)
