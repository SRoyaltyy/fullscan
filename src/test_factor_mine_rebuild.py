"""Rebuilt Factor Mine days stay out of the sealed store.

Run: python -m src.test_factor_mine_rebuild
"""
from __future__ import annotations

from pathlib import Path

from src.factor_mine_freeze import refuse_past_restate
from src.factor_mine_rebuild import (
    _write_once,
    classify_zero,
    describes_after_open,
    leak_split,
    matching_earlier_export,
    select_rows,
)


def test_leak_check_uses_the_time_the_row_describes() -> None:
    prior = {
        "date": "2026-10-14",
        "ticker": "AAA",
        "prior_date": "2026-10-13",
        "news_export_date": "2026-10-13",
        "close": 12.5,
        "asof": "",
    }
    assert describes_after_open(
        prior, "2026-10-14",
        panel_asof="09:30_et",
        panel_leak="prior tape + pre-open packet; news from prior export or morning box",
    ) == ""
    morning = dict(prior, described_at="2026-10-14T09:15:00-04:00")
    assert describes_after_open(morning, "2026-10-14") == ""
    landed_later = dict(prior, described_at="2026-10-14T08:00:00-04:00")
    assert describes_after_open(landed_later, "2026-10-14") == ""
    afternoon = dict(prior, described_at="2026-10-14T10:02:00-04:00")
    assert "after 09:30" in describes_after_open(afternoon, "2026-10-14")
    same_day_export = dict(prior, news_export_date="2026-10-14", described_at="")
    assert "after 09:30" in describes_after_open(same_day_export, "2026-10-14")
    close_asof = {"date": "2026-10-14", "asof": "16:00"}
    assert describes_after_open(close_asof, "2026-10-14").startswith("asof=")
    kept, dropped = leak_split(
        [prior, same_day_export], "2026-10-14", panel_asof="09:30_et")
    assert len(kept) == 1 and len(dropped) == 1
    assert "close" not in kept[0]
    assert kept[0]["tag"] == "rebuilt"
    assert kept[0]["ticker"] == "AAA"


def test_reconstructed_names_replace_the_stuck_list() -> None:
    rows = [
        {"ticker": "STUCK", "sources": ["ohlc_hot"]},
        {"ticker": "FLAT", "sources": ["flatten"]},
        {"ticker": "FRESH", "sources": ["yday_gainer"]},
    ]
    fresh = select_rows(rows, {
        "reconstructed": True,
        "names": ["FRESH"],
        "stuck_list_reused": False,
    })
    assert [row["ticker"] for row in fresh] == ["FLAT", "FRESH"]
    flagged = select_rows(rows, {
        "reconstructed": False,
        "names": ["STUCK", "FRESH"],
        "flag": "no prior-day export",
        "stuck_list_reused": False,
    })
    assert [row["ticker"] for row in flagged] == ["FLAT"]
    text = Path(__file__).with_name("factor_mine_rebuild.py").read_text(encoding="utf-8")
    assert "candidate_path" not in text
    assert "stuck_list_reused" in text


def test_a_copied_export_is_not_a_reconstructed_morning() -> None:
    digests = {
        "2026-10-12": "abc",
        "2026-10-13": "abc",
        "2026-10-14": "abc",
    }
    assert matching_earlier_export("2026-10-13", digests) == "2026-10-12"
    assert matching_earlier_export("2026-10-12", digests) is None
    assert matching_earlier_export("2026-10-09", digests) is None


def test_classify_zero_days() -> None:
    hard = classify_zero(
        hard_red=True, n_picks=4, skips=[], both_zero=True,
        empty_input=True, candidate_ok=True, universe_rows=10,
    )
    assert hard["class"] == "sat out (rule)"
    assert "S<=-3" in hard["reason"]
    lock = classify_zero(
        hard_red=False, n_picks=4,
        skips=[{"kind": "no_price", "reason": "no 09:30 open"}],
        both_zero=True, empty_input=False, candidate_ok=True,
        universe_rows=10, rebuilt_fills=0,
    )
    assert lock["class"] == "still broken" and lock["step"] == "lock"
    rule = classify_zero(
        hard_red=False, n_picks=0, skips=[], both_zero=True,
        empty_input=False, candidate_ok=True, universe_rows=0,
    )
    assert rule["class"] == "sat out (rule)"
    first = classify_zero(
        hard_red=False, n_picks=0, skips=[], both_zero=True,
        empty_input=False, candidate_ok=True, universe_rows=9,
    )
    assert first["step"] == "build"
    missing = classify_zero(
        hard_red=False, n_picks=0, skips=[], both_zero=True,
        empty_input=True, candidate_ok=False, universe_rows=0,
    )
    assert missing["step"] == "build"
    filled = classify_zero(
        hard_red=False, n_picks=2, skips=[], both_zero=False,
        empty_input=True, candidate_ok=True, universe_rows=20,
        rebuilt_fills=2,
    )
    assert filled["class"] == "rebuilt"
    render = classify_zero(
        hard_red=False, n_picks=0, skips=[], both_zero=True,
        empty_input=True, candidate_ok=True, universe_rows=4,
        sim_error="RuntimeError: book failed",
    )
    assert render["step"] == "render"


def test_rebuilt_file_is_write_once(tmp_path: Path) -> None:
    path = tmp_path / "2026-10-14.json"
    assert _write_once(path, {"date": "2026-10-14", "tag": "rebuilt"})
    path.write_text('{"tag":"rebuilt","sealed":true}\n', encoding="utf-8")
    assert _write_once(path, {"date": "2026-10-14", "tag": "changed"}) is False
    assert path.read_text(encoding="utf-8") == '{"tag":"rebuilt","sealed":true}\n'


def test_restate_still_refuses_a_past_day() -> None:
    try:
        refuse_past_restate(["2026-10-14"])
    except SystemExit as exc:
        text = str(exc)
        assert "refuses past" in text
        assert "2026-10-14" in text
        return
    raise AssertionError("past restate was allowed")


def test_morning_reads_prior_raw_not_its_own_file() -> None:
    from src.theme_radar_raw import (
        before_morning_open,
        load_prior_raw,
        raw_url,
    )

    assert raw_url("2026-10-12").endswith("/2026-10-12.raw.csv")
    assert not raw_url("2026-10-12").endswith("/2026-10-13.raw.csv")
    assert before_morning_open("2026-10-12T20:51:39+00:00", "2026-10-13")
    assert before_morning_open("2026-10-13T13:30:00+00:00", "2026-10-13") is False
    seen = []

    def fetch(url: str) -> bytes:
        seen.append(url)
        if url.endswith("manifest.json"):
            return (
                b'{"runs":[{"date":"2026-10-12",'
                b'"scrape_ts_utc":"2026-10-12T20:51:39+00:00"}]}'
            )
        assert url.endswith("/2026-10-12.raw.csv"), url
        assert "2026-10-13.raw.csv" not in url
        return (
            b"Ticker,Change,Volume,Average Volume,Market Cap,Industry,"
            b"Earnings Date,News Title\n"
            b"AAA,1.2%,1000,500,2000,Software,10/13/2026 8:00:00 AM,AAA rises\n"
        )

    loaded = load_prior_raw("2026-10-13", fetch=fetch)
    assert loaded["source"] == "theme_radar_snapshot"
    assert loaded["source_file"] == "data/snapshots/2026-10-12.raw.csv"
    assert loaded["source_session"] == "2026-10-12"
    assert len(loaded["source_sha256"]) == 64
    assert list(loaded["frame"]["Ticker"]) == ["AAA"]
    try:
        load_prior_raw(
            "2026-10-13",
            fetch=fetch,
            manifest={"runs": [{"date": "2026-10-12",
                                "scrape_ts_utc": "2026-10-13T14:00:00+00:00"}]},
        )
    except ValueError as exc:
        assert "09:30" in str(exc)
    else:
        raise AssertionError("a post-open snapshot was accepted")

    def cleaned(url: str) -> bytes:
        if url.endswith("manifest.json"):
            return (
                b'{"runs":[{"date":"2026-10-12",'
                b'"scrape_ts_utc":"2026-10-12T20:51:39+00:00"}]}'
            )
        return b"Ticker,Volume,Average Volume,Market Cap,Industry,News Title\nAAA,1,1,1,Software,x\n"

    try:
        load_prior_raw("2026-10-13", fetch=cleaned)
    except ValueError as exc:
        assert "Earnings Date" in str(exc) and "Change" in str(exc)
    else:
        raise AssertionError("a file without Earnings Date and Change was accepted")


def test_auth_export_is_tried_before_login() -> None:
    import os
    import sys
    import types

    sys.modules.setdefault("psycopg2", types.ModuleType("psycopg2"))
    extras = sys.modules.setdefault("psycopg2.extras", types.ModuleType("psycopg2.extras"))
    extras.execute_values = lambda *args, **kwargs: None
    sys.modules.setdefault("db", types.ModuleType("db"))
    connection = sys.modules.setdefault("db.connection", types.ModuleType("db.connection"))
    connection.get_connection = lambda: None
    from collectors import finviz_financials as ff

    calls = []

    class Response:
        status_code = 200
        content = b"Ticker,Change\nAAA,1%\n"

    def fake_get(url, params=None, timeout=None, headers=None):
        calls.append(url.split("?", 1)[0])
        return Response()

    old_export = os.environ.get("FINVIZ_EXPORT")
    old_key = os.environ.get("FINVIZ_API_KEY")
    os.environ["FINVIZ_EXPORT"] = "https://elite.finviz.com/export.ashx?v=152&auth=hidden"
    os.environ.pop("FINVIZ_API_KEY", None)
    try:
        import requests
        original = requests.get
        requests.get = fake_get
        frame = ff.try_auth_export()
    finally:
        requests.get = original
        if old_export is None:
            os.environ.pop("FINVIZ_EXPORT", None)
        else:
            os.environ["FINVIZ_EXPORT"] = old_export
        if old_key is not None:
            os.environ["FINVIZ_API_KEY"] = old_key
    assert frame is not None and list(frame["Ticker"]) == ["AAA"]
    assert calls == ["https://elite.finviz.com/export.ashx"]
    assert "hidden" not in str(calls)


def test_2026_11_04_reads_2026_11_03_raw() -> None:
    from src.theme_radar_raw import applies, before_morning_open, load_prior_raw, raw_url

    assert applies("2026-11-04")
    assert applies("2026-11-05") is False
    assert before_morning_open("2026-11-03T22:06:10+00:00", "2026-11-04")
    assert before_morning_open("2026-11-04T14:30:00+00:00", "2026-11-04") is False
    seen = []

    def fetch(url: str) -> bytes:
        seen.append(url)
        assert url == raw_url("2026-11-03"), url
        assert not url.endswith("/2026-11-04.raw.csv")
        return (
            b"Ticker,Change,Volume,Average Volume,Market Cap,Industry,"
            b"Earnings Date,News Title\n"
            b"AAA,1.2%,1000,500,2000,Software,11/4/2026 8:00:00 AM,AAA rises\n"
        )

    loaded = load_prior_raw(
        "2026-11-04",
        fetch=fetch,
        manifest={"runs": [{"date": "2026-11-03",
                            "scrape_ts_utc": "2026-11-03T22:06:10+00:00"}]},
    )
    assert loaded["source"] == "theme_radar_snapshot"
    assert loaded["source_file"] == "data/snapshots/2026-11-03.raw.csv"
    assert loaded["source_session"] == "2026-11-03"
    assert len(loaded["source_sha256"]) == 64
    assert seen == [raw_url("2026-11-03")]
    try:
        load_prior_raw(
            "2026-11-04",
            fetch=fetch,
            manifest={"runs": [{"date": "2026-11-03",
                                "scrape_ts_utc": "2026-11-04T14:30:00+00:00"}]},
        )
    except ValueError as exc:
        assert "09:30" in str(exc)
        assert "2026-11-03.raw.csv" in str(exc)
    else:
        raise AssertionError("a scrape at 09:30 ET on 2026-11-04 was accepted")


def test_ovcal_names_rebuilt_rule_and_missed() -> None:
    html = (Path(__file__).resolve().parent / "factor_mine_ovcal.html").read_text(
        encoding="utf-8")
    assert "sat out (rule)" in html
    assert "rebuilt" in html
    assert "missed" in html
    assert "cell.missed" in html
    assert "labelText" in html
    assert "rebuilt_labels.json" in html


def main() -> None:
    import tempfile
    tests = [
        test_leak_check_uses_the_time_the_row_describes,
        test_reconstructed_names_replace_the_stuck_list,
        test_a_copied_export_is_not_a_reconstructed_morning,
        test_classify_zero_days,
        test_rebuilt_file_is_write_once,
        test_restate_still_refuses_a_past_day,
        test_morning_reads_prior_raw_not_its_own_file,
        test_2026_11_04_reads_2026_11_03_raw,
        test_auth_export_is_tried_before_login,
        test_ovcal_names_rebuilt_rule_and_missed,
    ]
    for fn in tests:
        if fn.__code__.co_argcount:
            with tempfile.TemporaryDirectory() as raw:
                fn(Path(raw))
        else:
            fn()
        print("ok", fn.__name__)


if __name__ == "__main__":
    main()
