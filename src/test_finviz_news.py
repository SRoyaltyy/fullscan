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


def test_load_company_news_reads_title_and_digest(tmp_path: Path) -> None:
    csv = tmp_path / "finviz_2026-09-04.csv"
    csv.write_text(
        "Ticker,News Time,News Title,Daily Digest,Sector,Industry\n"
        "ABBV,2026-09-03 16:32:00,AbbVie Phase 3,"
        "AbbVie announces positive Phase 3 etentamig data,Healthcare,Drug\n"
        "AAPL,Sep-03-26 07:15AM,Apple immersive,"
        "My Impressions Watching Baseball,Technology,Consumer Electronics\n"
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
    assert "SPY" not in news
    booked = actions_from_company_news(news)
    assert booked["ABBV"]["net"] > 0
    assert "AAPL" not in booked


def test_parse_still_handles_quote_page_stamps() -> None:
    assert parse_finviz_news_date("Sep-03-26 04:32PM", today="2026-09-04") == "2026-09-03"
    assert parse_finviz_news_date("07:15AM", last_date="2026-09-03",
                                 today="2026-09-04") == "2026-09-03"


def main() -> None:
    import tempfile
    tests = [
        test_headline_tone_grades_catalyst_language,
        test_parse_still_handles_quote_page_stamps,
    ]
    for fn in tests:
        fn()
        print(f"ok  {fn.__name__}")
    with tempfile.TemporaryDirectory() as tmp:
        test_load_company_news_reads_title_and_digest(Path(tmp))
        print("ok  test_load_company_news_reads_title_and_digest")
    print("3 tests passed")


if __name__ == "__main__":
    main()
