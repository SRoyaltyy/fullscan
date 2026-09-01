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
    assert set(row["boxes"]) >= {k for k, _ in tl.BOX_COLS}
    assert "yday" in row["boxes"]
    assert set(row["domains"]) == {k for k, _ in gainer_asof.DOMAIN_COLS}
    assert row["labeled_domains"].startswith("mkt")
    vintage = row["factor_vintage"]
    assert vintage.get("asof") == "09:30_et"
    if row["boxes"]["vol"] != "missing":
        assert vintage.get("vol")
        assert vintage["vol"] < "2026-08-13"
    assert "finviz" not in (row.get("sources") or [])
    assert "book" not in (row.get("sources") or [])
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
    assert "Hit rate" in md
    assert "yΔ" in md or "yday" in md
    assert "mkt" in md and "par" in md and "chd" in md
    assert "Domains" in md
    assert "join" in md
    assert "ARX" in md or "`ARX`" in md
    day_md = "\n".join(gainer_asof.render_day_markdown("2026-08-13"))
    assert "as-of 09:30" in day_md
    assert "join" in day_md


def test_all_above_five_percent_is_uncapped():
    df = gainer_asof.load_finviz("2026-08-13")
    names = gainer_asof.liquid_gainers(df, top_n=0, min_change=5.0, liquid=True)
    assert len(names) >= 40
    assert all(r["change_pct"] >= 5.0 for r in names)
    assert names[0]["change_pct"] >= names[-1]["change_pct"]
    assert "XHG" not in [r["ticker"] for r in names]
    capped = gainer_asof.liquid_gainers(df, top_n=15, min_change=5.0)
    assert len(capped) == 15
    assert [r["ticker"] for r in capped] == [r["ticker"] for r in names[:15]]


def test_yday_uses_prior_tape_not_same_day():
    day = gainer_asof.day_walk("2026-08-14")
    row = day["rows"][0]
    assert "yday" in row["boxes"]
    vintage = row["factor_vintage"]
    if row["boxes"]["yday"] != "missing":
        assert vintage.get("yday")
        assert vintage["yday"] < "2026-08-14"
        assert vintage["yday"] == row["prior_date"]


def test_two_percent_floor_is_wider_than_five():
    df = gainer_asof.load_finviz("2026-08-13")
    n2 = len(gainer_asof.liquid_gainers(df, top_n=0, min_change=2.0))
    n5 = len(gainer_asof.liquid_gainers(df, top_n=0, min_change=5.0))
    assert n2 > n5 >= 40


def test_buy_sleeve_is_colored_alongside_gainers():
    day = gainer_asof.day_walk("2026-08-13", include_buys=True)
    assert day["buys"]
    buy = day["buys"][0]
    assert buy["on_1d_buy"] is True
    assert "yday" in buy["boxes"]
    payload = gainer_asof.walk(
        from_date="2026-08-13", to_date="2026-08-13",
        top_n=0, floors=[2.0, 5.0], include_buys=True, force=True,
    )
    md = gainer_asof.render_markdown(payload)
    assert "today's 1d BUY" in md
    assert "What the boxes actually said" in md
    assert "mkt" in md
    assert buy.get("labeled_domains", "").startswith("mkt")
    assert "Regime" in md
    assert payload["floors_detail"]["2"]["summary"]["n_names"] >= payload[
        "floors_detail"]["5"]["summary"]["n_names"]
    assert payload["buys"]["summary"]["n_names"] >= 1


def test_0831_uses_saved_lattice_domains():
    day = gainer_asof.day_walk("2026-08-31", include_buys=True, top_n=5)
    assert day["buys"]
    buy = next((r for r in day["buys"] if r["ticker"] == "CRM"), day["buys"][0])
    assert buy["factor_vintage"].get("domains") in ("book", "book+derived")
    assert buy["domains"]["market"] == "bad"  # HARD_RED
    assert "mkt🔴" in buy["labeled_domains"] or buy["domains"]["market"] == "bad"


def test_era_skip_marks_pre_digest_days():
    skip = gainer_asof._era_skip("2026-08-13")
    assert "digest" in skip
    assert "judge" in skip
    assert "ab" in skip
    assert "peer" in skip
    assert "join" not in skip
    assert "catal" in gainer_asof._era_skip("2026-08-20")
    assert "catal" not in gainer_asof._era_skip("2026-08-31")


def test_infer_lane_hall_pass_labels():
    assert gainer_asof.infer_lane({"setup": "bad"}) == "blocked"
    assert gainer_asof.infer_lane(
        {"company": "good", "setup": "good", "flow": "neutral"},
        market_state="green",
    ) == "catalyst"
    assert gainer_asof.infer_lane(
        {"company": "good", "setup": "good", "flow": "neutral"},
        market_state="hard_red",
    ) == "catalyst_exception"
    assert gainer_asof.infer_lane(
        {"child": "good", "setup": "good", "flow": "neutral", "company": "neutral"},
    ) == "group_leader"
    assert gainer_asof.infer_lane(
        {"setup": "good", "flow": "good", "company": "neutral", "parent": "neutral"},
        market_state="green",
    ) == "standard"
    assert gainer_asof.infer_lane(
        {"setup": "good", "flow": "good", "company": "neutral", "parent": "neutral"},
        market_state="hard_red",
    ) == "probable"
    assert gainer_asof.infer_lane({}, saved="group_leader") == "group_leader"
    assert gainer_asof.infer_lane({}, lattice_live=False) is None
    assert gainer_asof.infer_lane({}, lattice_live=True) is None
    assert gainer_asof.lane_label("group_leader") == "group leader"
    assert gainer_asof.lane_label("catalyst_exception") == "catalyst exception"
    assert gainer_asof.lane_label(None) == gainer_asof.GREY


def test_grey_icon_for_era_skip_missing():
    assert gainer_asof._icon("missing", era=True) == gainer_asof.GREY
    assert gainer_asof._icon("missing", era=False) == "⬛"
    labeled = gainer_asof._labeled({"digest": "missing", "join": "good"}, era_skip=["digest"])
    assert "dig⬜" in labeled
    assert "join" in labeled


def test_liquid_losers_are_down():
    df = gainer_asof.load_finviz("2026-08-13")
    if df.empty:
        return
    names = gainer_asof.liquid_losers(df, top_n=15)
    assert all(r["change_pct"] <= gainer_asof.LOSER_FLOOR for r in names)
    ticks = [r["ticker"] for r in names]
    assert "XHG" not in ticks


def test_cli_accepts_sells_and_losers():
    p = gainer_asof.build_parser()
    args = p.parse_args([
        "--from", "2026-08-13", "--floors", "2,5", "--all",
        "--buys", "--sells", "--losers", "--write", "--min-mcap", "100",
    ])
    assert args.sells is True
    assert args.losers is True
    assert args.buys is True
    off = p.parse_args(["--no-sells", "--no-losers"])
    assert off.sells is False
    assert off.losers is False


def test_day_walk_carries_hall_pass_and_sides():
    day = gainer_asof.day_walk(
        "2026-08-31", include_buys=True, include_sells=True,
        include_losers=True, top_n=5,
    )
    assert "sells" in day and "losers" in day
    assert day.get("sells")
    assert day.get("losers")
    row = (day.get("buys") or day.get("rows") or [None])[0]
    if not row:
        return
    assert row.get("lane") in gainer_asof.LANES or row.get("lane") is None
    assert "lane_label" in row
    assert "marks" in row
    assert "bucket" in row
    sell = day["sells"][0]
    loser = day["losers"][0]
    assert set(sell["boxes"]) >= {k for k, _ in tl.BOX_COLS}
    assert set(sell["domains"]) == {k for k, _ in gainer_asof.DOMAIN_COLS}
    assert sell["labeled"].startswith("join")
    assert sell.get("labeled_domains", "").startswith("mkt")
    assert set(loser["boxes"]) >= {k for k, _ in tl.BOX_COLS}
    assert set(loser["domains"]) == {k for k, _ in gainer_asof.DOMAIN_COLS}
    assert loser["change_pct"] <= gainer_asof.LOSER_FLOOR
    md = "\n".join(gainer_asof.render_day_markdown("2026-08-31", day=day))
    assert "Hall pass" in md
    assert "1d SELL" in md
    assert "Marks" in md
    assert "mid_opp" in md
    assert "Source boxes (cameras)" in md
    assert "Domains (coaches)" in md
    assert "🔵" in md or "🚨" in md or "⚪" in md or gainer_asof.GREY in (sell.get("marks_cell") or "")
    assert "group leader" in md or "standard" in md or "blocked" in md or "probable" in md or "catalyst" in md
    crm = next((r for r in day["buys"] if r["ticker"] == "CRM"), None)
    if crm:
        assert crm["lane"] == "probable"
        assert crm["marks"]["blue"] is True
        assert "🔵" in (crm.get("marks_cell") or "")


def test_pre_lattice_hall_pass_is_grey_not_blocked():
    day = gainer_asof.day_walk("2026-08-13", include_buys=True, top_n=5)
    row = (day.get("rows") or [None])[0]
    assert row
    assert row.get("lattice_live") is False
    assert row.get("lane") is None
    assert row.get("lane_label") == gainer_asof.GREY
    assert "marks" in row
    assert "marks_cell" in row
    md = "\n".join(gainer_asof.render_day_markdown("2026-08-13", day=day))
    assert "Marks" in md
    assert "Hall pass" in md


if __name__ == "__main__":
    test_liquid_gainers_skip_penny_spikes()
    test_0813_asof_boxes_are_prior_tape_not_same_day_close()
    test_walk_summary_covers_dashboard_start()
    test_all_above_five_percent_is_uncapped()
    test_yday_uses_prior_tape_not_same_day()
    test_two_percent_floor_is_wider_than_five()
    test_buy_sleeve_is_colored_alongside_gainers()
    test_0831_uses_saved_lattice_domains()
    test_era_skip_marks_pre_digest_days()
    test_infer_lane_hall_pass_labels()
    test_grey_icon_for_era_skip_missing()
    test_liquid_losers_are_down()
    test_cli_accepts_sells_and_losers()
    test_day_walk_carries_hall_pass_and_sides()
    test_pre_lattice_hall_pass_is_grey_not_blocked()
    print("ok")
