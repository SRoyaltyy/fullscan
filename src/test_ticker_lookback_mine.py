"""Unit tests for the market-wide lookback pattern mine."""
from __future__ import annotations

from src import ticker_lookback_mine as mine


def _row(date, ret, xs, **kw):
    boxes = kw.pop("boxes", {"join": "good", "ab": "neutral", "vol": "bad"})
    return {
        "ticker": kw.pop("ticker", "TEST"),
        "date": date,
        "n_print": sum(1 for v in boxes.values() if v in {"good", "neutral", "bad"}),
        "boxes": boxes,
        "blue": kw.pop("blue", False),
        "alarm": kw.pop("alarm", False),
        "white": kw.pop("white", False),
        "region": kw.pop("region", "good"),
        "stretch": kw.pop("stretch", "good"),
        "cond": kw.pop("cond", "neutral"),
        "tag_context": kw.pop("tag_context", []),
        "cls": kw.pop("cls", "asof_0930"),
        "ret_1d": ret,
        "ret_3d": ret,
        "ret_1w": ret,
        "xs_1d": xs,
        "xs_3d": xs,
        "xs_1w": xs,
    }


def test_day_medians_and_excess() -> None:
    rows = [
        {"date": "2026-08-20", "ret_1d": 2.0, "ret_3d": None, "ret_1w": 1.0},
        {"date": "2026-08-20", "ret_1d": 0.0, "ret_3d": None, "ret_1w": -1.0},
        {"date": "2026-08-21", "ret_1d": 1.0, "ret_3d": 3.0, "ret_1w": None},
    ]
    med = mine.day_medians(rows)
    assert med["2026-08-20"]["1d"] == 1.0
    mine.attach_excess(rows, med)
    assert rows[0]["xs_1d"] == 1.0
    assert rows[1]["xs_1d"] == -1.0
    assert rows[2]["xs_1d"] == 0.0


def test_verdict_long_fade_noise() -> None:
    long_rows = [_row("2026-08-20", 1.0, 0.4) for _ in range(90)]
    fade_rows = [_row("2026-08-20", -1.0, -0.4) for _ in range(90)]
    noise_rows = [_row("2026-08-20", 0.1, 0.02) for _ in range(90)]
    thin_rows = [_row("2026-08-20", 2.0, 1.0) for _ in range(10)]
    assert mine.verdict(mine.summarize(long_rows)) == "long"
    assert mine.verdict(mine.summarize(fade_rows)) == "fade"
    assert mine.verdict(mine.summarize(noise_rows)) == "noise"
    assert mine.verdict(mine.summarize(thin_rows)) == "thin"


def test_mine_buckets_tag_region() -> None:
    rows = []
    # blue on red → up (turn)
    rows += [_row("2026-08-20", 1.2, 0.5, blue=True, region="bad",
                  tag_context=["turn"]) for _ in range(50)]
    # alarm on green → down (first crack)
    rows += [_row("2026-08-20", -1.0, -0.4, alarm=True, region="good",
                  tag_context=["first_crack"],
                  boxes={"join": "bad", "ab": "good", "vol": "bad"})
             for _ in range(50)]
    buckets = mine.mine_buckets(rows)
    keys = {r["key"]: r for r in buckets["tag_region"]}
    assert "blue|bad" in keys
    assert keys["blue|bad"]["verdict"] == "long"
    assert "alarm|good" in keys
    assert keys["alarm|good"]["verdict"] == "fade"
    ctx = {r["key"]: r for r in buckets["tag_context"]}
    assert ctx["turn"]["verdict"] == "long"
    assert ctx["first_crack"]["verdict"] == "fade"


def test_mine_collects_real_printed_days() -> None:
    payload = mine.run(
        names=["AAPL", "MSFT"],
        from_date="2026-08-19",
        to_date="2026-08-21",
        write=False,
    )
    assert payload["meta"]["n_names"] == 2
    assert payload["meta"]["n_rows"] >= 4
    base = payload["buckets"]["base"][0]
    assert base["n"] >= 4
    assert "hit" in base


def test_render_md_lists_usable() -> None:
    rows = [_row("2026-08-20", 1.0, 0.4, blue=True, region="bad",
                 tag_context=["turn"]) for _ in range(90)]
    mine.attach_excess(rows)
    payload = {
        "generated_at": "t",
        "meta": {"n_names": 1, "n_rows": 90,
                 "from_date": "2026-08-20", "to_date": "2026-08-20"},
        "buckets": mine.mine_buckets(rows),
    }
    md = mine.render_md(payload)
    assert "Ticker lookback mine" in md
    assert "blue|bad" in md
    assert "long" in md


if __name__ == "__main__":
    test_day_medians_and_excess()
    test_verdict_long_fade_noise()
    test_mine_buckets_tag_region()
    test_mine_collects_real_printed_days()
    test_render_md_lists_usable()
    print("5 tests passed")
