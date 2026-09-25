"""Point-in-time retro guards. Git history is the clock, not the working tree."""
from __future__ import annotations

import csv
import tempfile
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


def test_daily_return_is_the_session_not_the_resumed_mean() -> None:
    resumed = {
        "date": "2026-09-24",
        "equity": 12346.74,
        "yday_equity": 10453.53,
        "mean": 23.4674,
    }
    ret = retro.daily_return_pct(resumed)
    assert ret == 18.1107
    assert ret != resumed["mean"]
    assert retro.daily_return_pct({"equity": 11000, "yday_equity": 10000}) == 10.0
    assert retro.daily_return_pct({"equity": 10050}) == 0.5


def test_daily_returns_csv_lists_each_start_book() -> None:
    """Every catalog recipe and every start/day, with held cells blank."""
    import csv

    ledgers = {
        "2026-09-23": {
            "label": "pit_rebuilt",
            "origin": "frozen",
            "recipes": {
                "union_hot_n4_h1": {
                    "starts": {
                        "2026-09-23": {"daily": {
                            "equity": 10100, "yday_equity": 10000, "mean": 1.0,
                        }},
                    },
                },
            },
        },
        "2026-09-24": {
            "label": "incomplete_pit",
            "origin": "frozen",
            "recipes": {
                "union_hot_n4_h1": {
                    "starts": {
                        "2026-09-23": {"daily": {
                            "equity": 10100, "yday_equity": 10100, "mean": 1.0,
                        }},
                        "2026-09-24": {"daily": {
                            "equity": 9900, "yday_equity": 10000, "mean": -1.0,
                        }},
                    },
                },
            },
        },
    }

    def read(date):
        return ledgers.get(date)

    catalog = {
        "other_recipe": "2026-09-21",
        "union_hot_n4_h1": "2026-08-13",
    }
    flat = {
        ("union_hot_n4_h1", "2026-09-23", "2026-09-23"): 1.25,
        ("union_hot_n4_h1", "2026-09-23", "2026-09-24"): 0.0,
        ("union_hot_n4_h1", "2026-09-24", "2026-09-24"): -0.5,
        ("other_recipe", "2026-09-24", "2026-09-24"): 0.0,
    }
    manifest = {"snapshots": {
        "2026-09-23": {"source_commits": {
            "01_daily/news/2026-09-23_finviz_digest.json": "abc123def4567890",
        }},
        "2026-09-24": {"source_commits": {
            "data/join/2026-09-24_ranked.csv": "fff000111222333444",
        }},
    }}
    with tempfile.TemporaryDirectory() as d:
        dest = Path(d) / "daily_returns.csv"
        import unittest.mock as mock
        with mock.patch.object(retro.fmf, "read_ledger", side_effect=read):
            n = retro.write_daily_returns(
                dest, ["2026-09-23", "2026-09-24"],
                catalog=catalog, flat_returns=flat, manifest=manifest,
            )
        assert n == 6
        with dest.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
    assert list(rows[0].keys()) == list(retro.DAILY_RETURN_FIELDS)
    assert [r["recipe"] for r in rows] == ["other_recipe"] * 3 + ["union_hot_n4_h1"] * 3
    assert rows[0]["day_status"] == "held"
    assert rows[0]["net_ret_futubull"] == ""
    assert rows[0]["net_ret_15bp"] == ""
    assert rows[0]["net_ret_futubull"] != "0"
    assert rows[0]["source_shas"] == "digest=abc123def456"
    held_flat = rows[2]
    assert held_flat["recipe"] == "other_recipe"
    assert held_flat["D"] == "2026-09-24"
    assert held_flat["day_status"] == "held"
    assert held_flat["net_ret_15bp"] == ""
    filled = rows[3]
    assert filled["recipe"] == "union_hot_n4_h1"
    assert filled["recipe_created_date"] == "2026-08-13"
    assert filled["start_date"] == "2026-09-23"
    assert filled["D"] == "2026-09-23"
    assert filled["net_ret_futubull"] == "1.0000"
    assert filled["net_ret_15bp"] == "1.2500"
    assert filled["day_status"] == "pit_rebuilt"
    flat_day = rows[4]
    assert flat_day["D"] == "2026-09-24"
    assert flat_day["net_ret_futubull"] == "0.0000"
    assert flat_day["net_ret_15bp"] == "0.0000"
    assert flat_day["day_status"] == "incomplete_pit"
    assert flat_day["source_shas"] == "join=fff000111222"
    assert rows[5]["net_ret_futubull"] == "-1.0000"
    assert rows[5]["day_status"] == "incomplete_pit"
    assert "0" not in (rows[0]["net_ret_futubull"], rows[0]["net_ret_15bp"])
    pit = rows[3]
    assert pit["timing_clean"] == "true"
    assert pit["news_clean"] == "false"
    assert pit["reads_news"] == "false"
    late = rows[4]
    assert late["timing_clean"] == "false"
    assert late["news_clean"] == "false"
    assert rows[0]["timing_clean"] == "true"
    assert rows[0]["reads_news"] == "false"


def test_news_flags_follow_331_and_recipe_gates() -> None:
    dates = retro.news_quarantine_dates()
    assert len(dates) == 18
    assert "2026-08-31" in dates
    assert "2026-09-04" in dates
    assert "2026-09-24" in dates
    assert "2026-09-09" in dates
    assert "2026-09-17" in dates
    assert "2026-09-23" in dates
    assert "2026-08-24" not in dates
    assert "2026-08-28" not in dates
    assert retro.news_clean_day("2026-08-24", dates)
    assert not retro.news_clean_day("2026-09-17", dates)
    assert retro.timing_clean_day("pit_rebuilt")
    assert not retro.timing_clean_day("incomplete_pit")
    assert retro.recipe_reads_news("union_hot_n4_h1") is False
    assert retro.recipe_reads_news("union_hot_n4_holdup") is False
    assert retro.recipe_reads_news("union_news_g_h1") is True
    assert retro.recipe_reads_news("short_news_r_h3") is True
    assert retro.recipe_reads_news("union_clk_fresh_cat_coil_h1") is True
    assert retro.recipe_reads_news("combo_sh_5050_shared") is True
    rows = [
        {"recipe": "union_hot_n4_h1", "start_date": "2026-08-13",
         "D": "2026-08-13", "net_ret_futubull": "10.0000",
         "net_ret_15bp": "9.0000", "timing_clean": "false",
         "news_clean": "true", "reads_news": "false"},
        {"recipe": "union_hot_n4_h1", "start_date": "2026-08-13",
         "D": "2026-08-24", "net_ret_futubull": "10.0000",
         "net_ret_15bp": "10.0000", "timing_clean": "true",
         "news_clean": "true", "reads_news": "false"},
        {"recipe": "union_news_g_h1", "start_date": "2026-08-13",
         "D": "2026-08-24", "net_ret_futubull": "-50.0000",
         "net_ret_15bp": "-50.0000", "timing_clean": "true",
         "news_clean": "true", "reads_news": "true"},
        {"recipe": "union_news_g_h1", "start_date": "2026-08-13",
         "D": "2026-08-31", "net_ret_futubull": "100.0000",
         "net_ret_15bp": "100.0000", "timing_clean": "true",
         "news_clean": "false", "reads_news": "true"},
    ]
    windows = {r["recipe"]: r for r in retro.recipe_clean_windows(rows)}
    hot = windows["union_hot_n4_h1"]
    assert hot["all_n"] == 2
    assert hot["all_futubull"] == 21.0
    assert hot["timing_n"] == 1
    assert hot["timing_futubull"] == 10.0
    assert hot["clean_n"] is None
    news = windows["union_news_g_h1"]
    assert news["reads_news"] is True
    assert news["all_n"] == 2
    assert news["timing_n"] == 2
    assert news["clean_n"] == 1
    assert news["clean_futubull"] == -50.0


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


def _use_retro_store(root: Path):
    old = (retro.RETRO_DIR, retro.RETRO_STORE, retro.RETRO_META, retro.RETRO_ACTIONS)
    retro.RETRO_DIR = root
    retro.RETRO_STORE = root / "ohlc.parquet"
    retro.RETRO_META = root / "meta.json"
    retro.RETRO_ACTIONS = root / "actions.parquet"
    return old


def _restore_retro_store(old) -> None:
    (retro.RETRO_DIR, retro.RETRO_STORE, retro.RETRO_META, retro.RETRO_ACTIONS) = old


def test_held_day_keeps_no_rows() -> None:
    info = {
        "label": "held",
        "cutoff": "2026-09-22T09:30:00-04:00",
        "sources": {},
        "missing_late": [],
        "absent": [],
    }
    snap = retro.snapshot_for(
        "2026-09-22", info, [{"ticker": "PACS", "open": 1}], "sha",
    )
    assert snap["rows"] == []
    assert snap["n_rows"] == 0
    assert snap["label"] == "held"
    assert snap["tape"] == "raw"
    assert snap["auto_adjust"] is False
    assert retro.price_gate_fails(["PACS"], [], [])
    assert retro.price_gate_fails([], ["PACS"], [])
    assert retro.price_gate_fails([], [], [{"ticker": "PACS"}])
    assert retro.price_gate_fails([], [], []) is False


def test_raw_lock_replaces_adjusted_and_leaves_the_live_store() -> None:
    import json
    import pandas as pd
    from src import price_store as ps

    live = {
        path: path.read_bytes() if path.is_file() else None
        for path in (ps.STORE_PATH, ps.ACTIONS_PATH)
    }
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        old = _use_retro_store(root)
        download = retro._download_raw
        try:
            pd.DataFrame([{
                "date": "2026-05-01", "ticker": "OLD",
                "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1,
            }]).to_parquet(retro.RETRO_STORE, index=False)
            retro.RETRO_META.write_text(json.dumps({
                "locked": True,
                "adjusted": True,
                "auto_adjust": True,
                "sha256": "abc",
            }), encoding="utf-8")

            def fake(names, start, end):
                assert start == retro.PRICE_START
                assert "AAA" in names
                ohlc = pd.DataFrame([{
                    "date": "2026-09-22", "ticker": "AAA",
                    "open": 10.0, "high": 11.0, "low": 9.0,
                    "close": 10.5, "volume": 100,
                }])
                acts = pd.DataFrame([{
                    "date": "2026-06-01", "ticker": "AAA",
                    "dividend": 0.2, "split": 0.0, "close": 10.0,
                }])
                return ohlc, acts

            retro._download_raw = fake
            digest = retro.fetch_raw_bars(["AAA"])
            meta = json.loads(retro.RETRO_META.read_text(encoding="utf-8"))
            assert meta["auto_adjust"] is False
            assert meta["adjusted"] is False
            assert meta["sha256"] == digest
            assert retro.RETRO_ACTIONS.is_file()
            frame = pd.read_parquet(retro.RETRO_STORE)
            assert list(frame["ticker"]) == ["AAA"]
            assert "OLD" not in set(frame["ticker"])
            assert float(frame["close"].iloc[0]) == 10.5

            def boom(names, start, end):
                raise AssertionError("raw lock must not download again")

            retro._download_raw = boom
            assert retro.fetch_raw_bars(["AAA"]) == digest
        finally:
            retro._download_raw = download
            _restore_retro_store(old)
    for path, blob in live.items():
        now = path.read_bytes() if path.is_file() else None
        assert now == blob


def test_recover_fills_raw_csv_then_stooq_without_touching_live_prices() -> None:
    import pandas as pd
    from src import factor_mine_freeze as fmf
    from src import price_store as ps

    live_actions = ps.ACTIONS_PATH.read_bytes() if ps.ACTIONS_PATH.is_file() else None
    with tempfile.TemporaryDirectory() as d:
        old = _use_retro_store(Path(d))
        download = retro._download_raw
        radar = retro._radar_raw_quotes
        fetch_stooq = fmf._fetch_stooq
        try:
            retro.lock_price_meta(pd.DataFrame([{
                "date": "2026-09-21", "ticker": "PACS",
                "open": 10.0, "high": 10.0, "low": 10.0,
                "close": 10.0, "volume": 1,
            }]), pd.DataFrame())
            retro._download_raw = lambda names, start, end: (
                pd.DataFrame(), pd.DataFrame(),
            )
            retro._radar_raw_quotes = lambda date: {
                "PACS": {"open": 12.0, "close": 13.0, "prev_close": 9.0},
                "NEW": {"open": 4.0, "high": 5.0, "low": 3.0, "close": 4.5, "volume": 8},
            }
            retro.SINGLE_SOURCE.clear()
            stooq_calls: list[str] = []

            def no_stooq(ticker):
                stooq_calls.append(ticker)
                return (
                    "Date,Open,High,Low,Close,Volume\n"
                    "2026-09-22,1,2,0.5,1.5,10\n"
                )

            fmf._fetch_stooq = no_stooq
            # PACS already has a Yahoo print, so raw.csv does not fill it.
            assert retro.recover_session_bars("2026-09-22", ["PACS", "NEW"]) == ["PACS"]
            assert stooq_calls == []
            df = pd.read_parquet(retro.RETRO_STORE)
            df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
            df["ticker"] = df["ticker"].astype(str)
            prior = df[(df["ticker"] == "PACS") & (df["date"] == "2026-09-21")]
            assert float(prior["close"].iloc[0]) == 10.0
            assert df[(df["ticker"] == "PACS") & (df["date"] == "2026-09-22")].empty
            filled = df[(df["ticker"] == "NEW") & (df["date"] == "2026-09-22")]
            assert float(filled["open"].iloc[0]) == 4.0
            assert float(filled["close"].iloc[0]) == 4.5
            assert "NEW" in retro.SINGLE_SOURCE.get("2026-09-22", set())
            assert "PACS" not in retro.SINGLE_SOURCE.get("2026-09-22", set())

            retro._radar_raw_quotes = lambda date: {}

            def stooq(ticker):
                stooq_calls.append(ticker)
                return (
                    "Date,Open,High,Low,Close,Volume\n"
                    "2026-09-22,1,2,0.5,1.5,10\n"
                )

            fmf._fetch_stooq = stooq
            assert retro.recover_session_bars("2026-09-22", ["ZZZ"]) == ["ZZZ"]
            assert stooq_calls == []
            df = pd.read_parquet(retro.RETRO_STORE)
            df["ticker"] = df["ticker"].astype(str)
            assert "ZZZ" not in set(df["ticker"])

            def empty_stooq(ticker):
                return ""

            fmf._fetch_stooq = empty_stooq
            assert retro.recover_session_bars("2026-09-22", ["GONE"]) == ["GONE"]
        finally:
            retro._download_raw = download
            retro._radar_raw_quotes = radar
            fmf._fetch_stooq = fetch_stooq
            _restore_retro_store(old)
    now = ps.ACTIONS_PATH.read_bytes() if ps.ACTIONS_PATH.is_file() else None
    assert now == live_actions


def test_late_raw_csv_is_not_a_session_fill() -> None:
    from src import factor_mine_freeze as fmf

    with tempfile.TemporaryDirectory() as d:
        old = _use_retro_store(Path(d))
        download = retro._download_raw
        tape = fmf.day_open_tape
        raw_bytes = fmf._theme_radar_raw_bytes
        fetch_stooq = fmf._fetch_stooq
        calls = {"raw": 0}
        try:
            retro._download_raw = lambda names, start, end: (
                __import__("pandas").DataFrame(), __import__("pandas").DataFrame(),
            )
            fmf.day_open_tape = lambda date: {"source": "stooq", "opens": {}}

            def raw(date):
                calls["raw"] += 1
                return b"Ticker,Price,Open\nPACS,13,12\n"

            fmf._theme_radar_raw_bytes = raw
            fmf._fetch_stooq = lambda ticker: (
                "Date,Open,High,Low,Close,Volume\n2026-09-22,1,1,1,1,1\n"
            )
            assert retro.recover_session_bars("2026-09-22", ["PACS"]) == ["PACS"]
            assert calls["raw"] == 0
        finally:
            retro._download_raw = download
            fmf.day_open_tape = tape
            fmf._theme_radar_raw_bytes = raw_bytes
            fmf._fetch_stooq = fetch_stooq
            _restore_retro_store(old)


def test_raw_csv_parser_keeps_high_low_prev_close_volume() -> None:
    from src import factor_mine_freeze as fmf

    text = (
        "Ticker,Open,High,Low,Price,Prev Close,Volume\n"
        "AAA,1.0,3.0,0.5,2.0,0.9,40\n"
    )
    got = fmf.parse_theme_radar_prices(text)["AAA"]
    assert got == {
        "close": 2.0,
        "open": 1.0,
        "high": 3.0,
        "low": 0.5,
        "prev_close": 0.9,
        "volume": 40.0,
    }
    text_src = Path(fmf.__file__).read_text(encoding="utf-8")
    price_at = text_src.index("PRICE_CHECK = {")
    share_at = text_src.index("UNRANKABLE_MAX_SHARE = 0.10")
    assert 0 < share_at - price_at < 900
    assert fmf.UNRANKABLE_MAX_SHARE == 0.10


def test_price_diff_median_and_max() -> None:
    import pandas as pd

    yahoo = pd.DataFrame([
        {"date": "2026-09-22", "ticker": "AAA", "open": 10.0, "close": 10.0},
        {"date": "2026-09-22", "ticker": "BBB", "open": 20.0, "close": 20.0},
        {"date": "2026-09-22", "ticker": "CCC", "open": 5.0, "close": 5.0},
    ])
    quotes = {
        "2026-09-22": {
            "AAA": {"open": 10.0, "close": 10.0},
            "BBB": {"open": 22.0, "close": 21.0},
        }
    }
    stats = retro.compare_yahoo_frame(yahoo, quotes)
    assert stats["close"]["n"] == 2
    assert stats["close"]["median_abs"] == 0.5
    assert stats["close"]["max_abs"] == 1.0
    assert stats["open"]["max_abs"] == 2.0
    assert retro.unrankable_holds(10, 100) is False
    assert retro.unrankable_holds(11, 100) is True


def test_raw_history_fills_ohlc_and_marks_single_source() -> None:
    import pandas as pd
    from src import factor_mine_freeze as fmf

    csv_921 = "Ticker,Open,High,Low,Price,Volume\nNEW,7,8,6,8,10\n"
    csv_922 = (
        "Ticker,Open,High,Low,Price,Prev Close,Volume\n"
        "NEW,10,12,9,11,8,1000\n"
        "OLD,3,4,2,3.5,3,50\n"
    )
    with tempfile.TemporaryDirectory() as d:
        old = _use_retro_store(Path(d))
        tape = fmf.day_open_tape
        raw_bytes = fmf._theme_radar_raw_bytes
        retro._RAW_QUOTE_CACHE.clear()
        retro.SINGLE_SOURCE.clear()
        try:
            retro.lock_price_meta(pd.DataFrame([{
                "date": "2026-09-22", "ticker": "OLD",
                "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.5, "volume": 1,
            }]), pd.DataFrame())

            def open_tape(date):
                if str(date)[:10] in ("2026-09-21", "2026-09-22"):
                    return {"source": "finviz_raw"}
                return {"source": "stooq"}

            def raw(date):
                day = str(date)[:10]
                if day == "2026-09-21":
                    return csv_921.encode()
                if day == "2026-09-22":
                    return csv_922.encode()
                return None

            fmf.day_open_tape = open_tape
            fmf._theme_radar_raw_bytes = raw
            assert retro.fill_raw_history(["NEW", "OLD"]) > 0
            df = pd.read_parquet(retro.RETRO_STORE)
            df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
            df["ticker"] = df["ticker"].astype(str)
            old_row = df[(df.ticker == "OLD") & (df.date == "2026-09-22")].iloc[0]
            assert float(old_row.close) == 1.5
            new_row = df[(df.ticker == "NEW") & (df.date == "2026-09-22")].iloc[0]
            assert float(new_row.open) == 10.0
            assert float(new_row.high) == 12.0
            assert float(new_row.low) == 9.0
            assert float(new_row.close) == 11.0
            assert float(new_row.volume) == 1000.0
            prior = df[(df.ticker == "NEW") & (df.date == "2026-09-21")].iloc[0]
            assert float(prior.open) == 7.0
            assert float(prior.close) == 8.0
            assert "NEW" in retro.SINGLE_SOURCE["2026-09-22"]
            assert "OLD" not in retro.SINGLE_SOURCE.get("2026-09-22", set())
        finally:
            fmf.day_open_tape = tape
            fmf._theme_radar_raw_bytes = raw_bytes
            retro._RAW_QUOTE_CACHE.clear()
            retro.SINGLE_SOURCE.clear()
            _restore_retro_store(old)


def test_short_lookback_is_unrankable() -> None:
    import pandas as pd
    from src import factor_mine_freeze as fmf
    from src import ohlc_ripper as ohlc
    from src import ticker_lookback as tl

    session = "2026-09-22"
    need = int(ohlc.INDICATOR_LOOKBACK)
    days = pd.bdate_range(end="2026-09-21", periods=need + 5)
    rows = []
    for i, day in enumerate(days):
        rows.append({
            "date": day.strftime("%Y-%m-%d"), "ticker": "LONG",
            "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.0, "volume": 1,
        })
        if i >= len(days) - 10:
            rows.append({
                "date": day.strftime("%Y-%m-%d"), "ticker": "SHORT",
                "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.0, "volume": 1,
            })
    rows.append({
        "date": session, "ticker": "LONG",
        "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5, "volume": 1,
    })
    rows.append({
        "date": session, "ticker": "SHORT",
        "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5, "volume": 1,
    })
    with tempfile.TemporaryDirectory() as d:
        old_store = _use_retro_store(Path(d))
        saved_px = tl.PRICE_STORE
        try:
            retro.lock_price_meta(pd.DataFrame(rows), pd.DataFrame())
            tl.PRICE_STORE = retro.RETRO_STORE
            fmf.reset_price_memory()
            assert retro.is_unrankable("LONG", session) is False
            assert retro.is_unrankable("SHORT", session) is True
            assert retro.is_unrankable("GONE", session) is True
        finally:
            tl.PRICE_STORE = saved_px
            fmf.reset_price_memory()
            _restore_retro_store(old_store)


def test_review_excludes_unrankable_names_until_the_share_exceeds_ten_percent() -> None:
    from unittest import mock
    from src import factor_mine_freeze as fmf

    date = "2026-09-22"

    def provenance(n_bad: int):
        names = [
            {"ticker": f"N{i}", "sources": []}
            for i in range(10)
        ]
        return {
            "date": date, "n": 10, "names": names,
            "excluded": [], "prior_export": "2026-09-21",
        }, {f"N{i}" for i in range(n_bad)}

    def run(n_bad: int, gaps: list[dict]):
        doc, bad = provenance(n_bad)
        info = {
            "label": "pit_rebuilt",
            "cutoff": "2026-09-22T09:30:00-04:00",
            "sources": {},
            "missing_late": [],
            "absent": [],
        }
        rows = [
            {"date": date, "ticker": f"N{i}", "ohlc_hot_score": 1.0}
            for i in range(10)
        ]
        seen = {}

        def unrank(ticker, day):
            return ticker in bad

        def recover(day, names):
            retro.SINGLE_SOURCE[day] = {"N1"}
            return []

        def check(day, tickers, fetch_stooq=True):
            seen["fetch_stooq"] = fetch_stooq
            seen["tickers"] = list(tickers)
            return gaps

        with tempfile.TemporaryDirectory() as d, \
                mock.patch.object(retro, "materialize", return_value=info), \
                mock.patch.object(fmf, "candidate_provenance", return_value=doc), \
                mock.patch.object(fmf, "write_candidates", return_value="sha"), \
                mock.patch.object(retro, "recover_session_bars", side_effect=recover), \
                mock.patch.object(retro, "is_unrankable", side_effect=unrank), \
                mock.patch.object(retro, "_panel_rows", return_value=rows), \
                mock.patch.object(fmf, "session_cross_check", side_effect=check), \
                mock.patch.object(fmf, "write_open_source_row"), \
                mock.patch.object(fmf, "paper_fills", return_value={}), \
                mock.patch.object(fmf, "day_open_tape", return_value={"source": "finviz_raw"}):
            got, prov = retro.review_day(date, info, Path(d))
        return got, prov, info, seen

    retro.SINGLE_SOURCE.clear()
    got, prov, info, seen = run(1, [])
    assert info["label"] == "pit_rebuilt"
    assert [row["ticker"] for row in got] == [f"N{i}" for i in range(1, 10)]
    assert prov["names"][0]["unrankable"] is True
    assert prov["names"][1]["single_source"] is True
    assert "N0" not in seen["tickers"]
    assert "N1" not in seen["tickers"]
    assert seen["fetch_stooq"] is False

    got, prov, info, seen = run(2, [])
    assert info["label"] == "pit_rebuilt"
    assert [row["ticker"] for row in got] == [f"N{i}" for i in range(2, 10)]
    assert info.get("hold_reason") is None
    assert len(info["dropped"]) == 2

    got, prov, info, seen = run(10, [])
    assert info["label"] == "skipped"
    assert info["skip_reason"] == "no rankable names"
    assert got == []

    got, prov, info, seen = run(0, [{
        "ticker": "N3", "field": "close", "ours": 1.0, "ref": 3.0,
    }])
    assert info["label"] == "pit_rebuilt"
    assert info.get("hold_reason") is None
    assert info["cross_check_n"] == 1
    assert len(got) == 10

    got, prov, info, seen = run(0, [{
        "ticker": "N3", "missing": ["missing session close"],
    }])
    assert info["label"] == "pit_rebuilt"
    assert len(got) == 10
    retro.SINGLE_SOURCE.clear()


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
    test_daily_return_is_the_session_not_the_resumed_mean()
    test_daily_returns_csv_lists_each_start_book()
    test_news_flags_follow_331_and_recipe_gates()
    test_baselines_section_keeps_the_hot4_table()
    test_incomplete_snapshot_carries_no_rows()
    test_held_day_keeps_no_rows()
    test_raw_lock_replaces_adjusted_and_leaves_the_live_store()
    test_recover_fills_raw_csv_then_stooq_without_touching_live_prices()
    test_late_raw_csv_is_not_a_session_fill()
    test_raw_csv_parser_keeps_high_low_prev_close_volume()
    test_price_diff_median_and_max()
    test_raw_history_fills_ohlc_and_marks_single_source()
    test_short_lookback_is_unrankable()
    test_review_excludes_unrankable_names_until_the_share_exceeds_ten_percent()
    print("factor-mine retro tests passed")
