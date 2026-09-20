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
    # unnamed names are not a map — theme packs do not expand
    assert "COIN" not in hits
    assert "SECZ" not in hits


def test_mapper_exact_company_and_ticker() -> None:
    profiles = {
        "COIN": {
            "ticker": "COIN",
            "company": "Coinbase Global Inc",
            "sector": "Financial",
            "industry": "Capital Markets",
            "text": "coinbase global inc financial capital markets",
            "export": "2026-09-17",
        },
        "HOOD": {
            "ticker": "HOOD",
            "company": "Robinhood Markets Inc",
            "sector": "Financial",
            "industry": "Capital Markets",
            "text": "robinhood markets inc financial capital markets",
            "export": "2026-09-17",
        },
        "DVN": {
            "ticker": "DVN",
            "company": "Devon Energy Corp",
            "sector": "Energy",
            "industry": "Oil & Gas E&P",
            "text": "devon energy corp energy oil gas",
            "export": "2026-09-17",
        },
        "COP": {
            "ticker": "COP",
            "company": "ConocoPhillips",
            "sector": "Energy",
            "industry": "Oil & Gas E&P",
            "text": "conocophillips energy oil gas",
            "export": "2026-09-17",
        },
    }
    coin = bt.map_tickers(
        {"title": "Coinbase Debuts Tokenized Stocks On Base Network", "digest": ""},
        profiles,
    )
    assert {h["ticker"] for h in coin} == {"COIN"}
    assert coin[0]["side"] == "bullish"
    hood = bt.map_tickers(
        {"title": "Robinhood CEO Calls On U.S. To Approve Tokenized Stocks",
         "digest": ""},
        profiles,
    )
    assert {h["ticker"] for h in hood} == {"HOOD"}
    dvn = bt.map_tickers(
        {"title": "Surprise US crude inventory build and Fed rate hike drive 5.63% DVN drop",
         "digest": ""},
        profiles,
    )
    assert {h["ticker"] for h in dvn} == {"DVN"}
    assert dvn[0]["side"] == "bearish"
    hormuz = bt.map_tickers(
        {"title": "IRGC strikes Togo-flagged tanker in Hormuz", "digest": ""},
        profiles,
    )
    assert "COP" not in {h["ticker"] for h in hormuz}


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
    assert "VST" not in hits  # Vistra is not named
    named = bt.map_tickers(
        {"title": "EPA repeal of GHG standards lifts Vistra (VST)", "digest": ""},
        profiles,
    )
    assert {h["ticker"] for h in named} == {"VST"}


def test_mapper_rejects_headline_noise_tickers() -> None:
    """S&P / U.S. / Fed / Reserve / investors are not a stock map."""
    profiles = {
        "ARCM": {
            "ticker": "ARCM",
            "company": "Arrow Reserve Capital Management ETF",
            "sector": "Financial",
            "industry": "Exchange Traded Fund",
            "text": "arrow reserve capital management etf",
            "export": "2026-09-15",
        },
        "IFED": {
            "ticker": "IFED",
            "company": "ETRACS IFED Invest with the Fed TR Index ETN",
            "sector": "Financial",
            "industry": "Exchange Traded Fund",
            "text": "etracs ifed invest with the fed",
            "export": "2026-09-15",
        },
        "S": {
            "ticker": "S",
            "company": "SentinelOne Inc",
            "sector": "Technology",
            "industry": "Software - Infrastructure",
            "text": "sentinelone inc technology software",
            "export": "2026-09-15",
        },
        "U": {
            "ticker": "U",
            "company": "Unity Software Inc",
            "sector": "Technology",
            "industry": "Software - Application",
            "text": "unity software inc technology",
            "export": "2026-09-15",
        },
        "EO": {
            "ticker": "EO",
            "company": "Corgi EOSE 2x Daily ETF",
            "sector": "Financial",
            "industry": "Exchange Traded Fund",
            "text": "corgi eose 2x daily etf",
            "export": "2026-09-15",
        },
        "KLAC": {
            "ticker": "KLAC",
            "company": "KLA Corporation",
            "sector": "Technology",
            "industry": "Semiconductor Equipment & Materials",
            "text": "kla corporation technology semiconductor",
            "export": "2026-09-15",
        },
    }
    fed = bt.map_tickers(
        {"title": "Federal Reserve: Gradual easing path – UOB", "digest": ""},
        profiles,
    )
    assert {h["ticker"] for h in fed} == set()
    spx = bt.map_tickers(
        {"title": "Stock market today: Dow, S&P 500, Nasdaq gain on soft inflation data",
         "digest": ""},
        profiles,
    )
    assert "S" not in {h["ticker"] for h in spx}
    us = bt.map_tickers(
        {"title": "U.S. inflation data dents September Fed hike bets", "digest": ""},
        profiles,
    )
    assert "U" not in {h["ticker"] for h in us}
    eo = bt.map_tickers(
        {"title": "EO 14420: national emergency on U.S. bulk-power system", "digest": ""},
        profiles,
    )
    assert "EO" not in {h["ticker"] for h in eo}
    kla = bt.map_tickers(
        {"title": "KLA Corporation (KLAC) Positioned to Benefit from Semiconductor Chip Complexity",
         "digest": ""},
        profiles,
    )
    assert {h["ticker"] for h in kla} == {"KLAC"}
    profiles["NFJ"] = {
        "ticker": "NFJ",
        "company": "Virtus AllianzGI Dividend Interest & Premium Strategy Fund",
        "sector": "Financial",
        "industry": "Closed-End Fund - Equity",
        "text": "virtus dividend interest premium strategy fund",
        "export": "2026-09-15",
    }
    profiles["AVAT"] = {
        "ticker": "AVAT",
        "company": "Avalanche Treasury Corp",
        "sector": "Financial",
        "industry": "Asset Management",
        "text": "avalanche treasury corp asset management",
        "export": "2026-09-15",
    }
    assert not bt.map_tickers(
        {"title": "Divided Fed holds interest rates steady, but three members voted to hike",
         "digest": ""},
        profiles,
    )
    assert not bt.map_tickers(
        {"title": "Treasury yields dip as Wall Street awaits wholesale inflation data",
         "digest": ""},
        profiles,
    )


def test_wrap_is_not_a_catalyst() -> None:
    assert bt.is_wrap("Stock Market Today: Tech Futures Sink As Treasury Yields Jump")
    assert bt.is_wrap("Nasdaq, Dow, S&P 500 Futures Rise After 4-Day Market Slide As CPI Looms Large: ORCL In Focus")
    assert not bt.is_wrap("Coinbase Debuts Tokenized Stocks On Base Network")
    assert not bt.is_catalyst(
        "Stock Market Today: Nvidia, Micron, Sandisk All Tumble")
    assert bt.is_catalyst("SEC Greenlights Tokenized Stocks After Clarity Act Fails in Senate")


def test_catalyst_mapper_drops_noun_collisions() -> None:
    profiles = {
        "AAT": {
            "ticker": "AAT",
            "company": "American Assets Trust Inc",
            "sector": "Real Estate",
            "industry": "REIT - Diversified",
            "text": "american assets trust inc reit",
            "export": "2026-09-12",
        },
        "BBGI": {
            "ticker": "BBGI",
            "company": "Beasley Broadcast Group Inc",
            "sector": "Communication Services",
            "industry": "Broadcasting",
            "text": "beasley broadcast group inc broadcasting",
            "export": "2026-09-12",
        },
        "DIS": {
            "ticker": "DIS",
            "company": "Walt Disney Co",
            "sector": "Communication Services",
            "industry": "Entertainment",
            "text": "walt disney co communication entertainment",
            "export": "2026-09-12",
        },
        "GHG": {
            "ticker": "GHG",
            "company": "GreenTree Hospitality Group Ltd",
            "sector": "Consumer Cyclical",
            "industry": "Lodging",
            "text": "greentree hospitality lodging",
            "export": "2026-09-12",
        },
        "COIN": {
            "ticker": "COIN",
            "company": "Coinbase Global Inc",
            "sector": "Financial",
            "industry": "Capital Markets",
            "text": "coinbase global inc financial",
            "export": "2026-09-12",
        },
        "SMX": {
            "ticker": "SMX",
            "company": "SMX (Security Matters) Plc",
            "sector": "Industrials",
            "industry": "Specialty Business Services",
            "text": "smx security matters plc",
            "export": "2026-09-12",
        },
    }
    assert "AAT" not in {h["ticker"] for h in bt.map_tickers(
        {"title": "SEC proposes Regulation Crypto Assets", "digest": ""}, profiles)}
    assert "BBGI" not in {h["ticker"] for h in bt.map_tickers(
        {"title": "Disney sues FCC over Trump’s broadcast-license threat",
         "digest": ""}, profiles)}
    assert {h["ticker"] for h in bt.map_tickers(
        {"title": "Disney sues FCC over Trump’s broadcast-license threat",
         "digest": ""}, profiles)} == {"DIS"}
    assert "GHG" not in {h["ticker"] for h in bt.map_tickers(
        {"title": "EPA finalizes repeal of 2024 power-plant GHG standards",
         "digest": ""}, profiles)}
    assert {h["ticker"] for h in bt.map_tickers(
        {"title": "Coinbase Debuts Tokenized Stocks On Base Network", "digest": ""},
        profiles)} == {"COIN"}
    assert "SMX" not in {h["ticker"] for h in bt.map_tickers(
        {"title": "The dollar’s rally matters — but it still won’t help Fed’s Warsh",
         "digest": ""}, profiles)}


def test_overlay_rewrites_existing_list() -> None:
    base = ["AAA", "BBB", "CCC", "DDD"]
    news = {"BBB": -3, "CCC": 3, "EEE": 5, "FFF": 2}
    assert bt.apply_overlay(base, news, "veto_bear", top_n=4) == [
        "AAA", "CCC", "DDD",
    ]
    assert bt.apply_overlay(base, news, "require_bull", top_n=4) == ["CCC"]
    added = bt.apply_overlay(base, news, "add_named", top_n=4)
    assert added[:4] == base
    assert "EEE" in added and "FFF" in added
    full = bt.apply_overlay(base, news, "full", top_n=4)
    assert "BBB" not in full
    assert "EEE" in full


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
    test_mapper_exact_company_and_ticker()
    test_mapper_does_not_assign_random_software_on_epa()
    test_mapper_rejects_headline_noise_tickers()
    test_wrap_is_not_a_catalyst()
    test_catalyst_mapper_drops_noun_collisions()
    test_overlay_rewrites_existing_list()
    test_harvest_skips_standtest_and_13q()
    from tempfile import TemporaryDirectory
    with TemporaryDirectory() as td:
        test_harvest_without_connector_writes_stub(Path(td))
    test_replay_workflow_has_no_live_automation_call()
    print("test_grok_news_ticker_bt: ok")


if __name__ == "__main__":
    main()
