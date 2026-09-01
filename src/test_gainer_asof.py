"""Top-gainer as-of walk: pre-open boxes on names that then ripped."""
from __future__ import annotations

from src import gainer_asof, ticker_lookback as tl


def test_liquid_gainers_skip_penny_spikes():
    df = gainer_asof.load_finviz("2026-08-13")
    assert not df.empty
    names = gainer_asof.liquid_gainers(df, top_n=15)
    ticks = [r["ticker"] for r in names]
    assert "ARX" in ticks
    assert "XHG" not in ticks  # +271% micro junk
    assert all(r["change_pct"] > 0 for r in names)
    assert all((r["mcap_m"] or 0) >= gainer_asof.MIN_MCAP_M for r in names)


def test_0813_asof_boxes_are_prior_tape_not_same_day_close():
    day = gainer_asof.day_walk("2026-08-13")
    assert day["coverage"]["status"] == "full"
    assert day["rows"]
    row = day["rows"][0]
    assert set(row["boxes"]) == {k for k, _ in tl.BOX_COLS}
    vintage = row["factor_vintage"]
    assert vintage.get("asof") == "09:30_et"
    # Vol / AB / overnight buy come from the last completed tape before D.
    if row["boxes"]["vol"] != "missing":
        assert vintage.get("vol")
        assert vintage["vol"] < "2026-08-13"
    assert "finviz" not in (row.get("sources") or [])
    assert "book" not in (row.get("sources") or [])
    # Outcome is same-day Change%; buy box is overnight, not "they ripped".
    assert row["change_pct"] > 0
    assert row["labeled"].startswith("join")
    assert "sect" in row["labeled"]
    assert row["overnight_buy"] is False or row["boxes"]["buy"] == "good"


def test_walk_summary_covers_dashboard_start():
    payload = gainer_asof.walk(
        from_date="2026-08-13", to_date="2026-08-14", top_n=5, force=True
    )
    dates = [d["date"] for d in payload["days"]]
    assert dates == ["2026-08-13", "2026-08-14"]
    assert payload["summary"]["n_names"] >= 5
    md = gainer_asof.render_markdown(payload)
    assert "Hit rate on names that then ripped" in md
    assert "join" in md
    assert "ARX" in md or "`ARX`" in md
    day_md = "\n".join(gainer_asof.render_day_markdown("2026-08-13"))
    assert "as-of 09:30" in day_md
    assert "join" in day_md


def test_era_skip_marks_pre_digest_days():
    skip = gainer_asof._era_skip("2026-08-13")
    assert "digest" in skip
    assert "judge" in skip
    assert "ab" in skip
    assert "peer" in skip
    assert "join" not in skip
    assert "catal" in gainer_asof._era_skip("2026-08-20")
    assert "catal" not in gainer_asof._era_skip("2026-08-31")


if __name__ == "__main__":
    test_liquid_gainers_skip_penny_spikes()
    test_0813_asof_boxes_are_prior_tape_not_same_day_close()
    test_walk_summary_covers_dashboard_start()
    test_era_skip_marks_pre_digest_days()
    print("ok")
