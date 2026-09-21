from src.lane_news_scan import (
    FINVIZ_SECTORS,
    NEWS_SCAN_TEMPLATE,
    _add,
    _norm,
    _rollup,
    news_scan_hopper_plan,
    news_scan_lanes,
    news_scan_models,
)
from src.lane_route import (
    LAST_RESORT_MODELS,
    NEWS_HEAD,
    is_banned_primary,
    lanes_for,
)


def test_norm_collapses_punctuation_and_spaces():
    assert _norm("AAPL!!!  Extra   spaces") == "aapl extra spaces"


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
            "model": "glm-4.7-flash",
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
    assert "zhipu::glm-4.7-flash" in roll["hopper_watermark"]
    assert set(FINVIZ_SECTORS) >= {"Energy", "Technology", "Financial"}


def test_news_scan_current_flash_order():
    """News sector scan uses news_to_tickers current-flash hopper plan."""
    assert NEWS_SCAN_TEMPLATE == "news_to_tickers"
    assert news_scan_lanes() == lanes_for("news_to_tickers")
    assert news_scan_lanes()[:4] == NEWS_HEAD
    assert NEWS_HEAD == ["zhipu", "siliconflow", "openrouter", "qwen"]

    plan = news_scan_hopper_plan()
    assert [hop for hop, _ in plan[:4]] == NEWS_HEAD
    by_hop = dict(plan)
    assert by_hop["zhipu"] == ["glm-4.7-flash"]
    assert news_scan_models("zhipu") == ["glm-4.7-flash"]
    assert news_scan_models("siliconflow")[0] == "Qwen/Qwen3-8B"
    assert news_scan_models("qwen") == ["qwen-flash"]
    assert news_scan_models("deepseek")[0] == "deepseek-flash"
    th = news_scan_models("tokenhub")
    assert th[0] == "glm-5.3-flash"
    assert "glm-5.3-flashx" in th
    assert "deepseek-v4-flash" in th
    assert th[-1] == "hy3"
    or_ids = news_scan_models("openrouter")
    assert or_ids
    assert all(m == "openrouter/free" or str(m).endswith(":free") for m in or_ids)


def test_news_scan_never_uses_banned_ids():
    banned = (
        "glm-4-flash-250414",
        "glm-4.5-flash",
        "glm-4-flash",
        "qwen2.5-7b-instruct",
        "Qwen/Qwen2.5-7B-Instruct",
    )
    for hop, models in news_scan_hopper_plan():
        blob = " ".join(models).lower()
        for bad in banned:
            assert bad.lower() not in blob, (hop, bad, models)
        for mid in models:
            assert not is_banned_primary(mid), (hop, mid)
        for last in LAST_RESORT_MODELS:
            assert last not in models, (hop, last)


def test_infer_one_never_lands_on_banned_id():
    """If a hopper returns a banned ID, news scan keeps hopping."""
    from src import lane_news_scan
    from src import lane_route

    calls: list[str] = []

    def fake_ask(hop, prompt, ctx, max_tokens=400, system=None, tmpl="custom"):
        calls.append(hop)
        if hop == "zhipu":
            return {"bullish": ["Energy"], "bearish": [], "notes": ""}, "glm-4-flash-250414"
        if hop == "siliconflow":
            return {"bullish": ["Technology"], "bearish": [], "notes": ""}, "Qwen/Qwen3-8B"
        return None, None

    orig = lane_route.ask_lane
    lane_route.ask_lane = fake_ask
    try:
        row = lane_news_scan._infer_one(
            {"title": "Oil jump", "body": "Brent spiked", "source_file": "t"},
            {"keys": {}},
        )
    finally:
        lane_route.ask_lane = orig

    assert row["ok"] is True
    assert row["lane"] == "siliconflow"
    assert row["model"] == "Qwen/Qwen3-8B"
    assert "zhipu" in calls and "siliconflow" in calls
    assert row["model"] not in LAST_RESORT_MODELS


def main() -> None:
    tests = [
        test_norm_collapses_punctuation_and_spaces,
        test_norm_dedupes,
        test_cramer_dropped,
        test_rollup_watermark,
        test_news_scan_current_flash_order,
        test_news_scan_never_uses_banned_ids,
        test_infer_one_never_lands_on_banned_id,
    ]
    failed = 0
    for fn in tests:
        try:
            fn()
            print(f"ok  {fn.__name__}")
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"FAIL {fn.__name__}: {exc}")
    if failed:
        raise SystemExit(f"{failed} test(s) failed")
    print(f"{len(tests)} tests passed")


if __name__ == "__main__":
    main()
