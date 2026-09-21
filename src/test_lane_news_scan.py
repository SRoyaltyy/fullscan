from src.lane_news_scan import FINVIZ_SECTORS, _add, _norm, _rollup


def test_norm_dedupes():
    bag = {}
    _add(bag, "SEC Clears Tokenized Stocks", "body a", "finviz")
    _add(bag, "sec clears tokenized stocks!!!", "body b", "parsed")
    assert len(bag) == 1


def test_cramer_dropped():
    bag = {}
    _add(bag, "Jim Cramer says buy banks", "x", "parsed")
    assert bag == {}


def test_rollup_watermark():
    rows = [
        {
            "ok": True,
            "bullish": ["Energy", "Financial"],
            "bearish": ["Technology"],
            "lane": "zhipu",
            "model": "glm-4.5-flash",
        },
        {
            "ok": True,
            "bullish": ["Energy"],
            "bearish": [],
            "lane": "tokenhub",
            "model": "glm-5.3-flash",
        },
    ]
    roll = _rollup(rows)
    assert roll["bullish_mentions"]["Energy"] == 2
    assert "zhipu::glm-4.5-flash" in roll["hopper_watermark"]
    assert set(FINVIZ_SECTORS) >= {"Energy", "Technology", "Financial"}
