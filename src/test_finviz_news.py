"""Company-level Finviz news for the RYG news box. No network.

Run: python -m src.test_finviz_news
"""
from __future__ import annotations

from pathlib import Path

from src.finviz_news import (
    actions_from_company_news,
    headline_tone,
    load_company_news,
    parse_finviz_news_date,
)


def test_headline_tone_grades_catalyst_language() -> None:
    assert headline_tone(
        "AbbVie announces positive Phase 3 etentamig multiple myeloma data"
    ) == "good"
    assert headline_tone(
        "Piper Sandler cuts AppLovin price target to $325, maintains Neutral"
    ) == "bad"
    assert headline_tone("My Impressions Watching Baseball in Apple Immersive") == "neutral"
    assert headline_tone(
        "Apple hit with new £2 billion UK collective lawsuit alleging "
        "App Tracking Transparency rules favor its own services"
    ) == "bad"
    assert headline_tone(
        "Cardinal Health CEO sold $29 million of company stock following "
        "the post-earnings share surge to record levels"
    ) == "bad"


def test_load_company_news_reads_title_and_digest(tmp_path: Path) -> None:
    csv = tmp_path / "finviz_2026-09-04.csv"
    csv.write_text(
        "Ticker,News Time,News Title,Daily Digest,Sector,Industry\n"
        "ABBV,2026-09-03 16:32:00,AbbVie Phase 3,"
        "AbbVie announces positive Phase 3 etentamig data,Healthcare,Drug\n"
        "AAPL,Sep-03-26 07:15AM,Apple immersive,"
        "My Impressions Watching Baseball,Technology,Consumer Electronics\n"
        "KLIC,2026-09-04 08:00:00,Kulicke Advances AI,"
        "Kulicke declares a quarterly dividend of $0.205 per share,Technology,Semi\n"
        "NAVI,2026-09-04 08:00:00,Navient declares third quarter common stock dividend,"
        "Navient announces quarterly dividend,Financial,Credit\n"
        "SPY,2026-09-04 09:00:00,Index wrap,S&P 500 best day,,"
        "\n",
        encoding="utf-8",
    )
    news = load_company_news("2026-09-04", path=csv, today="2026-09-04")
    assert "ABBV" in news
    assert news["ABBV"]["news_tone"] == "good"
    assert news["ABBV"]["event_date"] == "2026-09-03"
    assert news["AAPL"]["tone"] == "neutral"
    assert news["AAPL"]["event_date"] == "2026-09-03"
    assert news["KLIC"]["is_dividend"] is False
    assert news["NAVI"]["is_dividend"] is True
    assert "SPY" not in news
    booked = actions_from_company_news(news)
    assert booked["ABBV"]["net"] > 0
    assert "AAPL" not in booked
    assert "NAVI" not in booked


def test_digest_polarity_fills_vague_title() -> None:
    csv = Path("data/exports/finviz_2026-09-04.csv")
    if not csv.exists():
        return
    news = load_company_news("2026-09-04", today="2026-09-04")
    # Vague title, material digest — news box follows the digest.
    assert news["AAPL"]["news_title"].startswith("My Impressions")
    assert news["AAPL"]["news_tone"] == "bad"


def test_parse_still_handles_quote_page_stamps() -> None:
    assert parse_finviz_news_date("Sep-03-26 04:32PM", today="2026-09-04") == "2026-09-03"
    assert parse_finviz_news_date("07:15AM", last_date="2026-09-03",
                                 today="2026-09-04") == "2026-09-03"


def test_old_asof_does_not_use_future_export() -> None:
    news = load_company_news("2019-01-01", today="2019-01-01")
    assert news == {}


def test_lookback_news_box_uses_company_tone() -> None:
    from src.ticker_lookback_cli import _boxes

    present = {
        "join": False, "ab": False, "peer": False, "finviz": False,
        "sector": False, "gen": False, "news": True, "overnight_book": False,
    }
    sig = {k: None for k in (
        "s_join", "s_sector", "s_general", "s_news", "s_ab", "s_peer",
    )}
    assert _boxes(sig, None, False, present, news_tone="good")["news"] == "good"
    assert _boxes(sig, None, False, present, news_tone="neutral")["news"] == "neutral"
    empty = dict(present, news=False)
    assert _boxes(sig, None, False, empty)["news"] == "missing"


def test_real_export_grades_company_headlines() -> None:
    export = Path("data/exports/finviz_2026-09-04.csv")
    if not export.exists():
        return
    news = load_company_news("2026-09-04", today="2026-09-04")
    assert news["ABBV"]["news_tone"] == "good"
    assert news["AAPL"]["news_tone"] == "bad"
    booked = actions_from_company_news(news)
    assert "ABBV" in booked
    assert "AAPL" in booked
    assert len(booked) > 100


def main() -> None:
    import tempfile
    tests = [
        test_headline_tone_grades_catalyst_language,
        test_parse_still_handles_quote_page_stamps,
        test_old_asof_does_not_use_future_export,
        test_lookback_news_box_uses_company_tone,
        test_real_export_grades_company_headlines,
        test_digest_polarity_fills_vague_title,
    ]
    for fn in tests:
        fn()
        print(f"ok  {fn.__name__}")
    with tempfile.TemporaryDirectory() as tmp:
        test_load_company_news_reads_title_and_digest(Path(tmp))
        print("ok  test_load_company_news_reads_title_and_digest")
    print("7 tests passed")


if __name__ == "__main__":
    main()
