"""Parser + clock tests for the Finviz homepage market-day digest.

Fixtures: Cyrus's Friday narrative (plain text) and the live homepage
`why-stock-moving-init-data` HTML (Weekend Brief about that Friday).

Run: python -m src.test_finviz_market_digest
"""
from __future__ import annotations

import gzip
import json
from datetime import datetime
from pathlib import Path
from unittest import mock
from zoneinfo import ZoneInfo

from src.finviz_market_digest import (
    ET,
    THEME_RADAR_KEYS,
    backfill_range,
    build_report,
    clock_legal_at,
    decode_html_bytes,
    existing_morning_ok,
    parse_homepage_html,
    parse_market_digest_text,
    pick_capture_for_date,
    report_has_narrative,
    save_report,
    theme_radar_fields,
    to_markdown,
    wayback_ts_to_dt,
)
from src.output_qc import qc_finviz_market_digest

HERE = Path(__file__).resolve().parent
HTML_FIXTURE = HERE / "testdata" / "finviz_homepage_market_digest.html"
TEXT_FIXTURE = HERE / "testdata" / "cyrus_friday_finviz_market_digest.txt"


def _assert_friday_structure(parsed: dict) -> None:
    moves = {m["code"]: m["change_pct"] for m in parsed.get("index_moves") or []}
    assert abs(moves.get("SPX", 0) - 0.86) < 0.001
    assert abs(moves.get("COMP", 0) - 0.96) < 0.001
    assert abs(moves.get("DJI", 0) - 0.98) < 0.001
    oil = parsed.get("oil") or {}
    assert oil.get("price") == 104.60
    assert "brent" in str(oil.get("name") or "").lower()
    fed = parsed.get("fed_odds") or {}
    assert fed.get("pct") == 90
    assert fed.get("action") == "hike"
    cpi = parsed.get("cpi") or {}
    assert "cpi" in str(cpi.get("text") or "").lower()
    tickers = parsed.get("named_tickers") or []
    for t in ("DELL", "HPE", "HPQ", "ORCL"):
        assert t in tickers, tickers
    cal = " ".join(parsed.get("calendar") or []).lower()
    assert "monday" in cal and "light" in cal
    raw = (parsed.get("raw_text") or "").lower()
    assert "s&p 500" in raw and "0.86" in raw
    assert "brent" in raw
    _assert_friday_theme_radar(parsed)


def _assert_friday_theme_radar(parsed: dict) -> None:
    radar = theme_radar_fields(parsed) if "prior_close" not in parsed else parsed
    for key in THEME_RADAR_KEYS:
        assert key in radar, key
    prior = radar["prior_close"]
    assert abs((prior.get("spx") or 0) - 0.86) < 0.001
    assert abs((prior.get("nasdaq") or 0) - 0.96) < 0.001
    assert abs((prior.get("dow") or 0) - 0.98) < 0.001
    oil = radar.get("oil") or {}
    assert oil.get("price") == 104.60
    assert oil.get("direction") == "down"
    cpi_fed = radar.get("cpi_fed") or {}
    cpi = cpi_fed.get("cpi") or radar.get("cpi") or {}
    fed = cpi_fed.get("fed_odds") or radar.get("fed_odds") or {}
    assert "hotter" in str(cpi.get("text") or "").lower()
    assert fed.get("pct") == 90
    assert fed.get("action") == "hike"
    leaders = radar.get("named_leaders") or []
    for t in ("DELL", "HPE", "HPQ", "ORCL"):
        assert t in leaders, leaders
    nxt = radar.get("next_session_calendar") or {}
    assert nxt.get("housing") is True
    assert nxt.get("retail") is True
    assert nxt.get("fed") is True
    nxt_blob = " ".join([nxt.get("text") or ""] + list(nxt.get("bullets") or [])).lower()
    assert "housing" in nxt_blob
    assert "retail" in nxt_blob
    earn = radar.get("earnings_slate") or {}
    for t in ("HAIN", "RFIL", "CODA", "PLAY"):
        assert t in (earn.get("tickers") or []), earn
    geo = radar.get("geo_grain") or {}
    assert geo.get("geo") is True
    assert geo.get("grain") is True
    geo_text = str(geo.get("text") or "").lower()
    assert "middle east" in geo_text
    assert "grain" in geo_text


def test_parse_cyrus_friday_text() -> None:
    text = TEXT_FIXTURE.read_text(encoding="utf-8")
    parsed = parse_market_digest_text(text)
    _assert_friday_structure(parsed)


def test_parse_homepage_html_fixture() -> None:
    html = HTML_FIXTURE.read_text(encoding="utf-8")
    parsed = parse_homepage_html(html)
    assert parsed is not None
    assert "Weekend Brief" in (parsed.get("headline") or "")
    _assert_friday_structure(parsed)
    assert parsed.get("finviz_source") == "market_summary"
    assert str(parsed.get("finviz_published_at") or "").startswith("2026-09-12")


def test_gzip_wayback_body_decodes() -> None:
    html = HTML_FIXTURE.read_bytes()
    blob = gzip.compress(html)
    assert blob[:2] == b"\x1f\x8b"
    parsed = parse_homepage_html(blob)
    assert parsed is not None
    assert "0.86" in (parsed.get("raw_text") or "")
    assert decode_html_bytes(blob).startswith("<!DOCTYPE")


def test_rejects_login_and_empty() -> None:
    login = "<html><title>Login</title><form action=login_submit.ashx><input name=password></form></html>"
    assert parse_homepage_html(login) is None
    assert parse_homepage_html("<html><body>screener only</body></html>") is None


def test_clock_legal_uses_archive_snapshot_not_generated() -> None:
    # Friday 2026-09-11 09:30 ET = 13:30 UTC. 06:06 UTC is 02:06 ET → legal.
    assert clock_legal_at(wayback_ts_to_dt("20260910060618"), "2026-09-10") is True
    # 02:10 UTC Sep 12 = 22:10 ET Sep 11 → after Friday open.
    assert clock_legal_at(wayback_ts_to_dt("20260912021039"), "2026-09-11") is False
    late = datetime(2026, 9, 11, 16, 5, tzinfo=ET)
    assert clock_legal_at(late, "2026-09-11") is False
    early = datetime(2026, 9, 11, 5, 40, tzinfo=ET)
    assert clock_legal_at(early, "2026-09-11") is True


def test_wayback_report_stamps_source_and_clock() -> None:
    html = HTML_FIXTURE.read_text(encoding="utf-8")
    report = build_report(
        asof="2026-09-10",
        html=html,
        source="wayback",
        source_url="https://web.archive.org/web/20260910060618id_/https://finviz.com/",
        archive_ts="20260910060618",
        archive_url="https://web.archive.org/web/20260910060618id_/https://finviz.com/",
    )
    assert report["source"] == "wayback"
    assert report["archive_snapshot_ts"] == "20260910060618"
    assert report["generated_at"].startswith("2026-09-10T02:06:18")
    assert report["clock_legal"] is True
    assert report["timezone"] == "America/New_York"
    md = to_markdown(report)
    assert "**Source:** `wayback`" in md
    assert "**Banner:**" in md and "Weekend Brief" in md
    assert "**SPX:** +0.86%" in md
    assert "**Nasdaq:** +0.96%" in md
    assert "**Dow:** +0.98%" in md
    assert "**Prior close:**" in md and "SPX +0.86%" in md
    assert "**Oil:**" in md and "104.6" in md and "down" in md
    assert "**CPI/Fed:**" in md and "90" in md
    assert "**Leaders:**" in md and "DELL" in md
    assert "**Next session:**" in md and "housing yes" in md and "retail yes" in md
    assert "**Earnings slate:**" in md and "HAIN" in md
    assert "**Geo/grain:**" in md and "geo" in md and "grain" in md
    assert "## Theme Radar" in md
    assert "before 09:30 ET" in md
    assert "finviz_digest.md" in md
    for key in THEME_RADAR_KEYS:
        assert key in report, key
    _assert_friday_theme_radar(report)


def test_late_capture_is_not_written() -> None:
    html = HTML_FIXTURE.read_text(encoding="utf-8")
    report = build_report(
        asof="2026-09-11",
        html=html,
        source="wayback",
        archive_ts="20260912021039",
    )
    assert report["clock_legal"] is False
    import src.finviz_market_digest as md
    news = Path("/tmp/fullscan-market-digest-late")
    news.mkdir(parents=True, exist_ok=True)
    for p in news.glob("*"):
        p.unlink()
    with mock.patch.object(md, "NEWS_DIR", news):
        assert save_report(report) is None
        assert list(news.glob("*finviz_market_digest*")) == []


def test_does_not_invent_from_quote_digest() -> None:
    # Quote-page digest HTML has ticker Daily Digest cells, not the homepage widget.
    quote = """
    <table><tr>
      <td class="snapshot-td2">Daily Digest</td>
      <td>Bank of America cuts Apple price target</td>
    </tr></table>
    """
    assert parse_homepage_html(quote) is None
    report = build_report(asof="2026-08-21", html=quote, source="wayback",
                          archive_ts="20260821052017")
    assert report_has_narrative(report) is False


def test_pick_capture_prefers_preopen() -> None:
    rows = [
        {"et_date": "2026-09-10", "et": "2026-09-10T02:06:18-04:00",
         "timestamp": "20260910060618", "original": "https://finviz.com/"},
        {"et_date": "2026-09-10", "et": "2026-09-10T20:03:11-04:00",
         "timestamp": "20260911000311", "original": "https://finviz.com/"},
    ]
    pick = pick_capture_for_date(rows, "2026-09-10")
    assert pick is not None
    assert pick["timestamp"] == "20260910060618"
    assert pick_capture_for_date(rows, "2026-09-12") is None
    afternoon_only = [rows[1] | {"et_date": "2026-09-11",
                                 "et": "2026-09-11T20:03:11-04:00"}]
    assert pick_capture_for_date(afternoon_only, "2026-09-11") is None


def test_save_and_morning_ok(tmp_path: Path | None = None) -> None:
    import src.finviz_market_digest as md
    news = Path("/tmp/fullscan-market-digest-test")
    news.mkdir(parents=True, exist_ok=True)
    html = HTML_FIXTURE.read_text(encoding="utf-8")
    report = build_report(asof="2026-09-10", html=html, source="wayback",
                          archive_ts="20260910060618")
    legacy_md = news / "2026-09-10_finviz_digest.md"
    legacy_json = news / "2026-09-10_finviz_digest.json"
    legacy_md.write_text("quote-page digest — do not touch\n", encoding="utf-8")
    legacy_json.write_text("{}", encoding="utf-8")
    with mock.patch.object(md, "NEWS_DIR", news):
        jp, mp = save_report(report)
        assert jp.exists() and mp.exists()
        assert jp.name.endswith("_finviz_market_digest.json")
        assert mp.name.endswith("_finviz_market_digest.md")
        assert not jp.name.endswith("_finviz_digest.json")
        assert list(news.glob("*_finviz_digest.md")) == [legacy_md]
        assert list(news.glob("*_finviz_digest.json")) == [legacy_json]
        assert legacy_md.read_text(encoding="utf-8") == "quote-page digest — do not touch\n"
        payload = json.loads(jp.read_text(encoding="utf-8"))
        assert payload["source"] == "wayback"
        assert payload["clock_legal"] is True
        assert payload["generated_at"].startswith("2026-09-10T02:06:18")
        for key in THEME_RADAR_KEYS:
            assert key in payload, key
        _assert_friday_theme_radar(payload)
        qc_j = qc_finviz_market_digest(jp)
        qc_m = qc_finviz_market_digest(mp)
        assert qc_j.ok, qc_j.reason
        assert qc_m.ok, qc_m.reason
        assert existing_morning_ok("2026-09-10") is True
        assert existing_morning_ok("2026-09-10", force=True) is False
        payload["generated_at"] = "2026-09-10T16:05:00-04:00"
        payload["clock_legal"] = False
        jp.write_text(json.dumps(payload), encoding="utf-8")
        assert existing_morning_ok("2026-09-10") is False
        late_qc = qc_finviz_market_digest(jp)
        assert not late_qc.ok
        assert "generated_after_0930" in (late_qc.reason or "")


def test_backfill_writes_wayback_and_leaves_gaps() -> None:
    import src.finviz_market_digest as md
    html = HTML_FIXTURE.read_text(encoding="utf-8")
    news = Path("/tmp/fullscan-market-digest-backfill")
    news.mkdir(parents=True, exist_ok=True)
    for p in news.glob("*finviz_market_digest*"):
        p.unlink()
    rows = [
        {"timestamp": "20260910060618", "original": "https://finviz.com/",
         "et": "2026-09-10T02:06:18-04:00", "et_date": "2026-09-10",
         "utc": "2026-09-10T06:06:18+00:00"},
    ]
    with mock.patch.object(md, "NEWS_DIR", news), \
         mock.patch.object(md, "collect_cdx", return_value=rows), \
         mock.patch.object(md, "fetch_wayback_html",
                           return_value=(html, "https://web.archive.org/x")), \
         mock.patch.object(md, "fetch_archive_ph_newest",
                           return_value=(None, None, None)):
        summary = backfill_range("2026-09-10", "2026-09-11", force=True,
                                 try_archive_ph=True)
    assert "2026-09-10" in summary["wrote"]
    assert "2026-09-11" in summary["gaps"]
    jp = news / "2026-09-10_finviz_market_digest.json"
    payload = json.loads(jp.read_text(encoding="utf-8"))
    assert payload["source"] == "wayback"
    assert payload["archive_snapshot_ts"] == "20260910060618"
    assert payload["clock_legal"] is True
    assert not (news / "2026-09-11_finviz_market_digest.json").exists()


def test_qc_rejects_afternoon_and_missing_radar() -> None:
    html = HTML_FIXTURE.read_text(encoding="utf-8")
    report = build_report(
        asof="2026-09-11", html=html, source="wayback",
        archive_ts="20260912021039",
    )
    assert report["clock_legal"] is False
    news = Path("/tmp/fullscan-market-digest-qc")
    news.mkdir(parents=True, exist_ok=True)
    jp = news / "2026-09-11_finviz_market_digest.json"
    jp.write_text(json.dumps(report), encoding="utf-8")
    qc = qc_finviz_market_digest(jp)
    assert not qc.ok
    assert "generated_after_0930" in (qc.reason or "") or "not_clock_legal" in (qc.reason or "")
    report["generated_at"] = "2026-09-11T05:40:00-04:00"
    report["clock_legal"] = True
    del report["prior_close"]
    jp.write_text(json.dumps(report), encoding="utf-8")
    qc2 = qc_finviz_market_digest(jp)
    assert not qc2.ok
    assert "missing_prior_close" in (qc2.reason or "")


def main() -> None:
    tests = [
        test_parse_cyrus_friday_text,
        test_parse_homepage_html_fixture,
        test_gzip_wayback_body_decodes,
        test_rejects_login_and_empty,
        test_clock_legal_uses_archive_snapshot_not_generated,
        test_wayback_report_stamps_source_and_clock,
        test_late_capture_is_not_written,
        test_does_not_invent_from_quote_digest,
        test_pick_capture_prefers_preopen,
        test_save_and_morning_ok,
        test_backfill_writes_wayback_and_leaves_gaps,
        test_qc_rejects_afternoon_and_missing_radar,
    ]
    failed = 0
    for fn in tests:
        try:
            fn()
            print(f"ok  {fn.__name__}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"FAIL {fn.__name__}: {e}")
            import traceback
            traceback.print_exc()
    if failed:
        raise SystemExit(f"{failed} test(s) failed")
    print(f"{len(tests)} tests passed")


if __name__ == "__main__":
    main()
