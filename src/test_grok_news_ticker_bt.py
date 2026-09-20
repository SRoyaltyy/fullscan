"""Leak-rule tests for the Grok news ticker replay. No live API."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from src import grok_automation_harvest as hv
from src import grok_news_ticker_bt as bt

ET = ZoneInfo("America/New_York")
CAL = [
    "2026-08-13", "2026-08-14", "2026-08-17", "2026-08-18",
    "2026-09-08", "2026-09-09", "2026-09-10", "2026-09-17", "2026-09-18",
]


def test_0930_exactly_is_too_late() -> None:
    stamp = datetime(2026, 9, 17, 9, 30, 0, tzinfo=ET)
    assert bt.next_open_after(stamp, CAL) == "2026-09-18"


def test_pre_open_fills_same_session() -> None:
    stamp = datetime(2026, 9, 17, 9, 29, 59, tzinfo=ET)
    assert bt.next_open_after(stamp, CAL) == "2026-09-17"


def test_rth_print_goes_next_open() -> None:
    stamp = datetime(2026, 9, 17, 10, 5, 0, tzinfo=ET)
    assert bt.next_open_after(stamp, CAL) == "2026-09-18"


def test_stale_news_time_on_later_file_cannot_buy_earlier_open() -> None:
    stamp = datetime(2026, 9, 8, 8, 0, 0, tzinfo=ET)
    known, fill = bt.usable_known_at(stamp, "2026-09-17", CAL)
    assert fill == "2026-09-17"
    assert known is not None
    assert known.strftime("%Y-%m-%d") == "2026-09-17"


def test_future_stamp_on_file_is_dropped() -> None:
    stamp = datetime(2026, 9, 20, 8, 0, 0, tzinfo=ET)
    known, fill = bt.usable_known_at(stamp, "2026-09-17", CAL)
    assert known is None and fill is None


def test_hormuz_carry_is_not_an_article() -> None:
    assert bt.is_hormuz_carry("Hormuz risk premium still in oil", status="carried")
    assert bt.is_hormuz_carry("There Will Be Investing Opportunities When the Strait of Hormuz Reopens")
    assert not bt.is_hormuz_carry(
        "IRGC strikes/detains Togo-flagged tanker in Hormuz", status="new")
    assert not bt.keep_article(
        "Hormuz risk premium still in oil", status="carried", category="geopolitical")


def test_drop_sec_filing_and_single_name_fda() -> None:
    assert not bt.keep_article("Acme files with the SEC")
    assert not bt.keep_article("Amneal Announces FDA Approval and Launch of Lanreotide Injection")
    assert bt.keep_article("SEC Greenlights Tokenized Stocks After Clarity Act Fails in Senate")


def test_close_digest_paths_are_blocked() -> None:
    assert bt.is_blocked_news_path(Path("01_daily/news/2026-09-18_finviz_market_digest_close.json"))
    assert bt.is_blocked_news_path(Path("01_daily/map_heat/2026-09-18_research_baseline.json"))
    assert not bt.is_blocked_news_path(Path("01_daily/news/2026-09-18_parsed.json"))


def test_mapper_ignores_finviz_row_ticker() -> None:
    """AMC row carrying a tokenization headline is not a map to AMC."""
    profiles = {
        "AMC": {
            "ticker": "AMC",
            "company": "AMC Entertainment Holdings Inc",
            "sector": "Communication Services",
            "industry": "Entertainment",
            "text": "amc entertainment holdings inc communication services entertainment",
            "export": "2026-09-17",
        },
        "COIN": {
            "ticker": "COIN",
            "company": "Coinbase Global Inc",
            "sector": "Financial",
            "industry": "Capital Markets",
            "text": "coinbase global inc financial capital markets crypto bitcoin exchange",
            "export": "2026-09-17",
        },
        "SECZ": {
            "ticker": "SECZ",
            "company": "Securitize Inc",
            "sector": "Financial",
            "industry": "Capital Markets",
            "text": "securitize inc financial capital markets tokenization digital asset",
            "export": "2026-09-17",
        },
    }
    art = {
        "title": "SEC Greenlights Tokenized Stocks After Clarity Act Fails in Senate",
        "digest": "Chair Atkins granted a five-year innovation exemption.",
    }
    hits = {h["ticker"] for h in bt.map_tickers(art, profiles)}
    assert "AMC" not in hits
    assert "COIN" in hits or "SECZ" in hits


def test_mapper_does_not_assign_random_software_on_epa() -> None:
    profiles = {
        "MSFT": {
            "ticker": "MSFT",
            "company": "Microsoft Corp",
            "sector": "Technology",
            "industry": "Software - Infrastructure",
            "text": "microsoft corp technology software infrastructure",
            "export": "2026-09-16",
        },
        "VST": {
            "ticker": "VST",
            "company": "Vistra Corp",
            "sector": "Utilities",
            "industry": "Utilities - Independent Power Producers",
            "text": "vistra corp utilities independent power producers coal natural gas generation",
            "export": "2026-09-16",
        },
    }
    art = {"title": "EPA moves to repeal GHG standards for existing coal plants", "digest": ""}
    hits = {h["ticker"] for h in bt.map_tickers(art, profiles)}
    assert "MSFT" not in hits
    assert "VST" in hits


def test_harvest_skips_standtest_and_13q() -> None:
    assert "webull" not in str(hv.TASKS).lower()
    assert "13 question" not in str(hv.TASKS).lower()
    assert any("STANDTEST" in s for s in hv.SKIPPED)


def test_harvest_without_connector_writes_stub(tmp_path: Path) -> None:
    report = hv.run(since="2026-08-13", dest=tmp_path)
    assert report["n_days"] == 0
    assert report["automation_get_results"] is False
    assert (tmp_path / "_coverage.json").is_file()


def test_replay_workflow_has_no_live_automation_call() -> None:
    root = Path(__file__).resolve().parent.parent
    wf = root / ".github" / "workflows" / "grok_news_ticker_bt.yml"
    text = wf.read_text(encoding="utf-8")
    assert "workflow_dispatch" in text
    assert "src.grok_news_ticker_bt" in text
    assert "src.grok_automation_harvest" not in text
    assert "GROK_AUTOMATION" not in text


def main() -> None:
    test_0930_exactly_is_too_late()
    test_pre_open_fills_same_session()
    test_rth_print_goes_next_open()
    test_stale_news_time_on_later_file_cannot_buy_earlier_open()
    test_future_stamp_on_file_is_dropped()
    test_hormuz_carry_is_not_an_article()
    test_drop_sec_filing_and_single_name_fda()
    test_close_digest_paths_are_blocked()
    test_mapper_ignores_finviz_row_ticker()
    test_mapper_does_not_assign_random_software_on_epa()
    test_harvest_skips_standtest_and_13q()
    from tempfile import TemporaryDirectory
    with TemporaryDirectory() as td:
        test_harvest_without_connector_writes_stub(Path(td))
    test_replay_workflow_has_no_live_automation_call()
    print("test_grok_news_ticker_bt: ok")


if __name__ == "__main__":
    main()
