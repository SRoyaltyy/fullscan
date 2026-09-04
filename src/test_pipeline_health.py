"""Unit tests for pipeline health date math and heal routing."""
from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from src.pipeline_health import (
    HUMAN_STEPS,
    GROK_WORKFLOWS,
    UBUNTU_WORKFLOWS,
    Check,
    Report,
    _dispatch_payload,
    _healable,
    _next_weekday,
    _prev_weekday,
    _should_heal,
    _workflow_for_step,
    oauth_verdict,
    packet_dates,
    pick_job,
    reauth_payload_from_report,
)

ET = ZoneInfo("America/New_York")


def _dt(s: str) -> datetime:
    return datetime.fromisoformat(s).replace(tzinfo=ET)


def test_prev_next_weekday_skips_weekend():
    assert _prev_weekday("2026-08-28") == "2026-08-27"  # Fri → Thu
    assert _prev_weekday("2026-08-31") == "2026-08-28"  # Mon → Fri
    assert _next_weekday("2026-08-27") == "2026-08-28"  # Thu → Fri
    assert _next_weekday("2026-08-28") == "2026-08-31"  # Fri → Mon


def test_pick_job_by_clock():
    assert pick_job("auto", _dt("2026-08-28T07:00:00")) == "preopen"
    assert pick_job("auto", _dt("2026-08-28T00:30:00")) == "postclose"
    assert pick_job("auto", _dt("2026-08-27T16:31:00")) == "postclose"
    assert pick_job("preopen", _dt("2026-08-27T16:31:00")) == "preopen"


def test_packet_preopen_0700():
    session, src, tgt, book = packet_dates("preopen", _dt("2026-08-28T07:00:00"))
    assert (session, src, tgt, book) == ("2026-08-28", "2026-08-27", "2026-08-28", "2026-08-28")


def test_packet_postclose_0030():
    # Friday 00:30: research dated Friday (Thu night), book Thursday
    session, src, tgt, book = packet_dates("postclose", _dt("2026-08-28T00:30:00"))
    assert session == "2026-08-28"
    assert src == "2026-08-27"
    assert tgt == "2026-08-28"
    assert book == "2026-08-27"


def test_packet_postclose_after_bell():
    # Thursday 22:10: research for Friday, book Thursday
    session, src, tgt, book = packet_dates("postclose", _dt("2026-08-27T22:10:00"))
    assert (session, src, tgt, book) == ("2026-08-27", "2026-08-27", "2026-08-28", "2026-08-27")


def test_packet_afternoon_heals_todays_book():
    # Thursday 16:31: last-night research dated Thursday + Thursday book
    session, src, tgt, book = packet_dates("postclose", _dt("2026-08-27T16:31:00"))
    assert session == "2026-08-27"
    assert tgt == "2026-08-27"
    assert book == "2026-08-27"
    assert src == "2026-08-26"


def test_workflow_routing():
    assert _workflow_for_step("runtime.oauth") is None
    assert "runtime.oauth" in HUMAN_STEPS
    assert _workflow_for_step("scrape.tape") == "finviz_preopen_scrape.yml"
    assert _workflow_for_step("postclose.baseline_json") == "postclose_all.yml"
    assert _workflow_for_step("preopen.events") == "preopen_all.yml"
    assert _workflow_for_step("book.weather") == "stock_book_all.yml"
    assert _workflow_for_step("book.weather_sectors") == "stock_book_all.yml"
    assert _workflow_for_step("book.ab") == "stock_book_all.yml"
    assert _workflow_for_step("book.book_json") == "stock_book_all.yml"
    assert _workflow_for_step("outcome.general") == "postclose_all.yml"
    assert _workflow_for_step("outcome.sector_count") == "postclose_all.yml"
    assert _workflow_for_step("learn.learnings") == "postclose_all.yml"
    assert _workflow_for_step("pages.dashboard") == "deploy-dashboard.yml"
    # GH run history is observational — do not heal from clock.*.yml
    assert _workflow_for_step("clock.learn_cycle.yml") is None
    assert _workflow_for_step("clock.map_heat_postclose.yml") is None


def test_ubuntu_vs_ecs_split():
    assert "finviz_preopen_scrape.yml" in UBUNTU_WORKFLOWS
    assert "label_weather.yml" in UBUNTU_WORKFLOWS
    assert "ab_checklist.yml" in UBUNTU_WORKFLOWS
    assert "preopen_all.yml" not in UBUNTU_WORKFLOWS
    assert "postclose_all.yml" not in UBUNTU_WORKFLOWS
    assert "map_heat_postclose.yml" not in UBUNTU_WORKFLOWS
    assert "stock_book_all.yml" not in UBUNTU_WORKFLOWS


def test_dispatch_payloads():
    p = _dispatch_payload("finviz_preopen_scrape.yml", "2026-08-28", "2026-08-27", "2026-08-28", "2026-08-27")
    assert p["inputs"]["force"] == "true"
    assert p["inputs"]["run_date"] == "2026-08-28"
    p = _dispatch_payload("ab_checklist.yml", "2026-08-28", "2026-08-27", "2026-08-28", "2026-08-27")
    assert p["inputs"]["date"] == "2026-08-27"
    p = _dispatch_payload("label_weather.yml", "2026-08-28", "2026-08-27", "2026-08-28", "2026-08-27")
    assert p["inputs"]["run_date"] == "2026-08-27"
    p = _dispatch_payload("daily_pipeline.yml", "2026-08-28", "2026-08-27", "2026-08-28", "2026-08-27")
    assert p["inputs"]["stage"] == "outcome"
    p = _dispatch_payload("postclose_all.yml", "2026-08-28", "2026-08-27", "2026-08-28", "2026-08-27")
    assert p["inputs"]["run_date"] == "2026-08-27"
    assert p["inputs"]["force"] == "true"


def test_clock_yml_not_healable():
    c = Check(step="clock.learn_cycle.yml", name="learn", group="clock",
              status="FAIL", required=True)
    assert not _should_heal(c)
    c2 = Check(step="postclose.baseline_json", name="base", group="postclose",
               status="FAIL", required=True)
    assert _should_heal(c2)
    c3 = Check(step="runtime.oauth", name="oauth", group="runtime",
               status="FAIL", required=True)
    assert not _should_heal(c3)


def test_oauth_does_not_block_grok_healable():
    assert "postclose_all.yml" in GROK_WORKFLOWS
    assert "map_heat_postclose.yml" in GROK_WORKFLOWS
    r = Report(job="postclose", date="2026-08-27",
               source_date="2026-08-27", target_date="2026-08-28")
    r.checks = [
        Check(step="runtime.oauth", name="oauth", group="runtime",
              status="FAIL", required=True,
              detail="HUMAN: login"),
        Check(step="postclose.baseline_json", name="base", group="postclose",
              status="FAIL", required=True, detail="missing"),
        Check(step="book.weather", name="wx", group="book",
              status="FAIL", required=True, detail="missing"),
    ]
    heal = {c.step for c in _healable(r)}
    assert "postclose.baseline_json" in heal
    assert "book.weather" in heal
    assert "runtime.oauth" not in heal


def test_oauth_verdict():
    st, req, _ = oauth_verdict(0, False)
    assert (st, req) == ("OK", True)
    st, req, _ = oauth_verdict(2, False)
    assert (st, req) == ("WARN", False)  # expiring, still usable
    st, req, _ = oauth_verdict(1, True)
    assert (st, req) == ("WARN", False)  # expired text but PONG works
    st, req, _ = oauth_verdict(1, False)
    assert (st, req) == ("FAIL", True)
    st, req, _ = oauth_verdict(None, True)
    assert st == "WARN"


def test_reauth_payload_fail_is_needs_reauth():
    r = Report(job="postclose", date="2026-08-28",
               source_date="2026-08-27", target_date="2026-08-28")
    r.checks = [
        Check(step="runtime.oauth", name="oauth", group="runtime",
              status="FAIL", required=True, detail="expired"),
        Check(step="runtime.pong", name="pong", group="runtime",
              status="FAIL", required=True, detail="no"),
    ]
    p = reauth_payload_from_report(r)
    assert p["status"] == "needs_reauth"
    assert p["verification_uri"].startswith("https://accounts.x.ai")


if __name__ == "__main__":
    tests = [v for k, v in globals().items() if k.startswith("test_")]
    for t in tests:
        t()
        print("ok", t.__name__)
    print(f"{len(tests)} passed")
