"""Point-in-time retro guards. Git history is the clock, not the working tree."""
from __future__ import annotations

from pathlib import Path

from src import factor_mine_retro as retro


def test_0922_digest_commit_is_the_preopen_one() -> None:
    info = retro.classify_day("2026-09-22")
    rel = "01_daily/news/2026-09-22_finviz_digest.json"
    sha = info["sources"][rel]
    assert sha is not None
    assert sha.startswith("4d822a3")
    assert "digest" not in info["missing_late"]
    assert info["label"] == "pit_rebuilt"


def test_0901_baseline_only_exists_after_the_open() -> None:
    info = retro.classify_day("2026-09-01")
    assert info["label"] == "incomplete_pit"
    assert "baseline" in info["missing_late"]


def test_absent_input_is_not_a_late_fill() -> None:
    info = retro.classify_day("2026-08-13")
    assert "digest" in info["absent"]
    assert "digest" not in info["missing_late"]
    assert info["label"] == "incomplete_pit"
    assert "judge" in info["missing_late"]


def test_materialize_does_not_copy_a_post_cutoff_digest() -> None:
    import tempfile
    digest = "01_daily/news/2026-08-25_finviz_digest.json"
    weather = "01_daily/weather/2026-08-25_weather.json"
    cutoff = retro.cutoff_for("2026-08-25")
    assert retro.commit_asof(digest, cutoff) is None
    wsha = retro.commit_asof(weather, cutoff)
    assert wsha
    history = {
        digest: retro._git_commits(digest),
        weather: retro._git_commits(weather),
    }
    with tempfile.TemporaryDirectory() as d:
        dest = Path(d)
        retro.materialize("2026-08-25", dest, history)
        assert not (dest / digest).exists()
        assert (dest / weather).is_file()


def test_flat_15bp_is_half_per_side() -> None:
    fee = retro.flat_15bp_order_fees(100, 10.0, "buy", {})
    assert abs(fee - 0.75) < 1e-9
    assert retro.flat_15bp_order_fees(0, 10.0, "sell", {}) == 0.0


def test_drop_ticker_removes_glnd_only() -> None:
    panel = {
        "session_dates": ["2026-09-24"],
        "rows": [
            {"date": "2026-09-24", "ticker": "GLND"},
            {"date": "2026-09-24", "ticker": "AAA"},
        ],
    }
    out = retro.drop_ticker(panel, "GLND")
    assert [r["ticker"] for r in out["rows"]] == ["AAA"]
    assert "GLND" not in (out["by_date"].get("2026-09-24") or [{}])[0].get("ticker", "AAA") or True
    tickers = [r["ticker"] for r in out["by_date"]["2026-09-24"]]
    assert tickers == ["AAA"]


def test_linear_percentile_and_median() -> None:
    vals = list(range(1, 11))
    assert retro.linear_percentile(vals, 5) == 1.45
    assert retro.linear_percentile(vals, 50) == 5.5
    assert retro.linear_percentile(vals, 95) == 9.55
    assert retro.linear_percentile([3.0], 50) == 3.0
    assert retro.median_count([1, 2, 3, 4]) == 2.5
    assert retro.median_count([1, 2, 3]) == 2


def test_random4_seed_is_stable_and_drops_glnd() -> None:
    pools = {
        "2026-09-23": ["AAA", "BBB", "CCC", "DDD", "GLND"],
        "2026-09-24": ["EEE", "GLND"],
    }
    first = retro.random4_draw(pools, 0)
    assert first == retro.random4_draw(pools, 0)
    assert first != retro.random4_draw(pools, 1)
    assert len(first["2026-09-23"]) == 4
    assert len(first["2026-09-24"]) == 2
    assert set(first["2026-09-23"]) <= set(pools["2026-09-23"])
    for i in range(20):
        got = retro.random4_draw(pools, i, exclude="GLND")
        assert "GLND" not in got["2026-09-23"]
        assert got["2026-09-24"] == ["EEE"]


def test_random4_sells_when_dropped_and_iwm_holds() -> None:
    dates = ["2026-09-23", "2026-09-24"]
    regime = {d: {"predict_score": 0.0} for d in dates}
    panel = retro.panel_from_picks(dates, {
        "2026-09-23": ["AAA", "BBB"],
        "2026-09-24": ["BBB", "CCC"],
    })
    bars = {
        ("AAA", "2026-09-23"): {"open": 10.0, "close": 11.0},
        ("AAA", "2026-09-24"): {"open": 12.0, "close": 12.0},
        ("BBB", "2026-09-23"): {"open": 20.0, "close": 20.0},
        ("BBB", "2026-09-24"): {"open": 21.0, "close": 22.0},
        ("CCC", "2026-09-24"): {"open": 5.0, "close": 5.5},
    }
    scored = retro.score_panel(
        panel, retro.random4_recipe(), bars, flat_15bp=True, regime=regime,
    )
    fills = [
        (t["ticker"], t["side"]) for t in scored["trades"]
        if t["side"] not in ("OPEN", "CLOSE")
    ]
    assert ("AAA", "BUY") in fills
    assert ("AAA", "SELL") in fills
    assert ("BBB", "BUY") in fills
    assert ("BBB", "SELL") not in fills
    assert ("CCC", "BUY") in fills
    iwm = retro.panel_from_picks(dates, {d: ["IWM"] for d in dates})
    iwm_bars = {
        ("IWM", "2026-09-23"): {"open": 100.0, "close": 101.0},
        ("IWM", "2026-09-24"): {"open": 102.0, "close": 110.0},
    }
    held = retro.score_panel(
        iwm, retro.iwm_recipe(), iwm_bars, regime=regime,
        rules={"hard_red_no_new": False},
    )
    sides = [t["side"] for t in held["trades"] if t["side"] not in ("OPEN", "CLOSE")]
    assert sides == ["BUY"]
    assert held["n_open"] == 1
    assert held["total_ret_pct"] > 0


def test_baselines_section_keeps_the_hot4_table() -> None:
    text = (
        "# title\n\n## HOT4 and holdup\n\n"
        "| `union_hot_n4_h1` | futubull | with GLND | 2026-08-13 | 23.467 | 61 |\n"
    )
    payload = {
        "random4": {"rows": []},
        "iwm": {"tape": "memory", "rows": []},
        "open_check": "snapshot Open from 2026-09-25",
    }
    section = retro._baseline_md(payload)
    once = retro._splice_baselines_md(text, section)
    twice = retro._splice_baselines_md(once, section)
    assert twice.count("## Baselines") == 1
    assert "23.467" in twice
    assert "## HOT4 and holdup" in twice
    raw = '{\n  "classed": [],\n  "scores": [{"total_ret_pct": 23.467}]\n}\n'
    out = retro._splice_baselines_json(raw, {"random4": {"draws": 1}})
    again = retro._splice_baselines_json(out, {"random4": {"draws": 2}})
    assert again.count('"baselines"') == 1
    assert '"total_ret_pct": 23.467' in again
    assert '"draws": 2' in again
    assert '"draws": 1' not in again


def test_incomplete_snapshot_carries_no_rows() -> None:
    info = {
        "label": "incomplete_pit",
        "cutoff": "2026-09-01T09:30:00-04:00",
        "sources": {},
        "missing_late": ["baseline"],
        "absent": [],
    }
    snap = retro.snapshot_for("2026-09-01", info, [{"ticker": "LEAK"}], None)
    assert snap["rows"] == []
    assert snap["label"] == "incomplete_pit"
    assert snap["carry"] is True


if __name__ == "__main__":
    test_0922_digest_commit_is_the_preopen_one()
    test_0901_baseline_only_exists_after_the_open()
    test_absent_input_is_not_a_late_fill()
    test_materialize_does_not_copy_a_post_cutoff_digest()
    test_flat_15bp_is_half_per_side()
    test_drop_ticker_removes_glnd_only()
    test_linear_percentile_and_median()
    test_random4_seed_is_stable_and_drops_glnd()
    test_random4_sells_when_dropped_and_iwm_holds()
    test_baselines_section_keeps_the_hot4_table()
    test_incomplete_snapshot_carries_no_rows()
    print("factor-mine retro tests passed")
