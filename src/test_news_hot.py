from src.news_hot import HotBoard

COMPANIES = [
    {"ticker": "MU", "name": "Micron", "description": "manufactures DRAM NAND memory chips and high bandwidth memory"},
    {"ticker": "XOM", "name": "Exxon", "description": "explores and produces crude oil and natural gas"},
    {"ticker": "CVX", "name": "Chevron", "description": "explores and produces crude oil and natural gas"},
    {"ticker": "GOOGL", "name": "Alphabet", "description": "search advertising cloud and artificial intelligence models"},
]


def test_micron_is_the_link():
    hit = HotBoard(COMPANIES).link("Micron raises DRAM prices as HBM demand tightens")
    assert hit["kind"] == "company"
    assert hit["hits"][0]["ticker"] == "MU"


def test_oil_is_a_sector():
    hit = HotBoard(COMPANIES).link("Crude oil supply fears lift energy producers")
    assert hit["kind"] == "sector"
    tickers = {r["ticker"] for r in hit["hits"]}
    assert tickers == {"XOM", "CVX"}


def test_unrelated_is_cold():
    hit = HotBoard(COMPANIES).link("Penguins crowd the ice in Antarctica")
    assert hit["kind"] == "none"
