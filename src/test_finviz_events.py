"""Finviz quote-chart E / R / D markers match the public AAPL overlay."""
from __future__ import annotations

from pathlib import Path

from src import event_markers as em
from src import finviz_events as fe
from src import flatten_lookback_action as fla
from src.test_flatten_lookback_action import _sample_payload

FIXTURE = Path(__file__).resolve().parent / "testdata" / "finviz_aapl_quote_events.html"


def _html() -> str:
    return FIXTURE.read_text(encoding="utf-8")


def test_aapl_chart_events_match_finviz_chips() -> None:
    html = _html()
    raw = fe.extract_chart_events(html)
    assert len(raw) == 35
    kinds = {e["eventType"] for e in raw}
    assert "chartEvent/earnings" in kinds
    assert "chartEvent/ratings" in kinds
    assert "chartEvent/dividends" in kinds

    rows = fe.parse_quote_html(html, ticker="AAPL")
    e = [r for r in rows if r["kind"] == "E"]
    r = [r for r in rows if r["kind"] == "R"]
    d = [r for r in rows if r["kind"] == "D"]
    # Screenshot (Sep-04 daily, ~1y): E early Nov/Feb/May/Aug, D ~2w later,
    # R upgrades (green) and downgrades (red) on those weeks.
    e_dates = {x["event_date"] for x in e}
    d_dates = {x["event_date"] for x in d}
    assert "2025-10-30" in e_dates  # early Nov on the daily axis
    assert "2026-01-29" in e_dates
    assert "2026-04-30" in e_dates
    assert "2026-07-30" in e_dates  # last E before Sep-04; Elite export agrees
    assert "2025-11-10" in d_dates
    assert "2026-02-09" in d_dates
    assert "2026-05-11" in d_dates
    assert "2026-08-10" in d_dates  # mid-Aug D after the Jul-30 E
    last_e = [x for x in e if x["event_date"] == "2026-07-30"][0]
    assert last_e["hm"] == 1630  # 4:30 PM ET AMC
    assert last_e["color"] == "green"
    assert last_e["label"] == "E_BEAT"

    up = [x for x in r if x["event_date"] == "2026-08-17"]
    down = [x for x in r if x["event_date"] == "2026-08-10"]
    assert up and up[0]["color"] == "green" and up[0]["label"] == "R_UP"
    assert down and down[0]["color"] == "red" and down[0]["label"] == "R_DOWN"


def test_export_and_token_dates() -> None:
    d, hm = fe.parse_finviz_datetime("7/30/2026 4:30:00 PM")
    assert d == "2026-07-30" and hm == 1630
    d, hm = fe.parse_finviz_datetime("8/10/2026")
    assert d == "2026-08-10" and hm is None
    d, hm = fe.parse_finviz_datetime("Aug-17-26")
    assert d == "2026-08-17"
    rows = fe.events_from_export_fields(
        "AAPL", "7/30/2026 4:30:00 PM", "8/10/2026",
    )
    assert {x["kind"] for x in rows} == {"E", "D"}
    assert rows[0]["event_date"] == "2026-07-30"


def test_asof_is_leak_free_at_0930() -> None:
    rows = fe.parse_quote_html(_html(), ticker="AAPL")
    # 2026-08-18 09:30: last E is 7-30 AMC, last upgrade 8-17, last D 8-10.
    snap = fe.asof_snapshot(rows, "2026-08-18")
    assert snap["last_E_date"] == "2026-07-30"
    assert snap["last_R_date"] == "2026-08-17"
    assert snap["last_R_color"] == "green"
    assert snap["last_D_date"] == "2026-08-10"
    assert "E🟢" in snap["cell"] and "R🟢" in snap["cell"] and "D🔵" in snap["cell"]
    # Same-day upgrade must not color the open.
    mid = fe.asof_snapshot(rows, "2026-08-17")
    assert mid["last_R_date"] != "2026-08-17"
    assert mid["last_R_date"] == "2026-08-10"
    assert mid["last_R_color"] == "red"
    # Same-day AMC earnings are not knowable at 09:30.
    pre = fe.asof_snapshot(rows, "2026-07-30")
    assert pre["last_E_date"] != "2026-07-30"
    assert pre["last_E_date"] == "2026-04-30"


def test_universe_export_covers_every_name() -> None:
    idx = fe.load_export_events("2026-09-04")
    assert len(idx) > 1000
    aapl = idx["AAPL"]
    kinds = {r["kind"] for r in aapl}
    assert "E" in kinds and "D" in kinds
    assert any(r["event_date"] == "2026-07-30" for r in aapl if r["kind"] == "E")
    assert any(r["event_date"] == "2026-08-10" for r in aapl if r["kind"] == "D")


def test_event_markers_reads_finviz_not_yf() -> None:
    df = em.fetch("AAPL", html=_html())
    assert not df.empty
    assert set(df["kind"]) >= {"E", "R", "D"}
    assert "2026-07-30" in set(df.loc[df["kind"] == "E", "event_date"])
    snap = em.asof_snapshot(df, "2026-09-04")
    assert snap["last_E_date"] == "2026-07-30"
    assert snap["last_D_date"] == "2026-08-10"


def test_flatten_lookback_shows_erd_column() -> None:
    page = fla.render_html(_sample_payload())
    assert "E/R/D" in page
    assert "E🟢 7-30" in page
    md = fla.render_markdown(_sample_payload(), source="flatten", date="2026-08-14")
    assert "E/R/D" in md
    assert "7-30" in md


def test_attach_row_uses_cached_chart() -> None:
    rows = fe.parse_quote_html(_html(), ticker="AAPL")
    fe.save_ticker_events("AAPL", rows)
    rec = fe.attach_row({"ticker": "AAPL", "date": "2026-09-04"}, "2026-09-04", "AAPL")
    assert rec["erd_E_date"] == "2026-07-30"
    assert rec["erd_D_date"] == "2026-08-10"
    assert rec["erd_R_date"] == "2026-08-17"
    assert rec["erd_R_color"] == "green"


if __name__ == "__main__":
    test_aapl_chart_events_match_finviz_chips()
    test_export_and_token_dates()
    test_asof_is_leak_free_at_0930()
    test_universe_export_covers_every_name()
    test_event_markers_reads_finviz_not_yf()
    test_flatten_lookback_shows_erd_column()
    test_attach_row_uses_cached_chart()
    print("7 finviz-events tests passed")
