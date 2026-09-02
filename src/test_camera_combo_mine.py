"""Unit tests for the camera combo mine."""
from __future__ import annotations

from src import camera_combo_mine as mine


def _row(date="2026-08-20", ret=1.0, **kw):
    boxes = kw.pop("boxes", {"join": "good", "sector": "good", "gen": "good",
                             "ab": "neutral", "peer": "good", "vol": "bad",
                             "heat": "missing", "news": "missing",
                             "digest": "good", "judge": "good",
                             "catal": "missing", "buy": "neutral"})
    boxes_d1 = kw.pop("boxes_d1", {k: "good" if k != "vol" else "bad" for k in boxes})
    boxes_d2 = kw.pop("boxes_d2", dict(boxes_d1))
    n_red = sum(1 for v in boxes.values() if v == "bad")
    return {
        "ticker": kw.pop("ticker", "TEST"),
        "date": date,
        "boxes": boxes,
        "boxes_d1": boxes_d1,
        "boxes_d2": boxes_d2,
        "n_print": sum(1 for v in boxes.values() if v in {"good", "neutral", "bad"}),
        "n_red": n_red,
        "n_red_d1": sum(1 for v in boxes_d1.values() if v == "bad"),
        "zero_red": n_red == 0,
        "zero_red_d1": all(v != "bad" for v in boxes_d1.values()),
        "blue": False,
        "alarm": False,
        "ret_1d": ret,
        "ret_2d": ret,
        "ret_3d": ret,
        "ret_1w": ret,
        "ret_2w": ret,
        "xs_1d": ret - 0.2,
        "xs_2d": ret - 0.2,
        "xs_3d": ret - 0.2,
        "xs_1w": ret - 0.2,
        "xs_2w": ret - 0.2,
    }


def test_zero_red_beats_has_red() -> None:
    clean = [_row(ret=1.2, boxes={k: "good" for k in (
        "join", "sector", "gen", "ab", "peer", "vol", "heat", "news",
        "digest", "judge", "catal", "buy")}) for _ in range(90)]
    dirty = [_row(ret=-0.6, boxes={k: "bad" for k in (
        "join", "sector", "gen", "ab", "peer", "vol", "heat", "news",
        "digest", "judge", "catal", "buy")}) for _ in range(90)]
    buckets = mine.mine(clean + dirty)
    zr = {r["key"]: r for r in buckets["zero_red"]}
    assert zr["zero_red"]["1d_mean"] > zr["has_red"]["1d_mean"]
    assert zr["zero_red"]["1d_hit"] > zr["has_red"]["1d_hit"]


def test_lag_turn_vs_persist() -> None:
    persist = [_row(ret=0.8,
                    boxes={"join": "good", "sector": "good", "gen": "good",
                           "ab": "good", "peer": "good", "vol": "good",
                           "heat": "good", "news": "missing", "digest": "good",
                           "judge": "good", "catal": "missing", "buy": "good"},
                    boxes_d1={"join": "good", "sector": "good", "gen": "good",
                              "ab": "good", "peer": "good", "vol": "good",
                              "heat": "good", "news": "missing", "digest": "good",
                              "judge": "good", "catal": "missing", "buy": "good"})
               for _ in range(50)]
    turn = [_row(ret=-0.4,
                 boxes={"join": "good", "sector": "good", "gen": "good",
                        "ab": "good", "peer": "good", "vol": "good",
                        "heat": "good", "news": "missing", "digest": "good",
                        "judge": "good", "catal": "missing", "buy": "good"},
                 boxes_d1={"join": "bad", "sector": "bad", "gen": "bad",
                           "ab": "bad", "peer": "bad", "vol": "bad",
                           "heat": "bad", "news": "missing", "digest": "bad",
                           "judge": "bad", "catal": "missing", "buy": "bad"})
            for _ in range(50)]
    buckets = mine.mine(persist + turn)
    keys = {r["key"]: r for r in buckets["lag"]}
    persist_key = "join@D0=good & join@D-1=good"
    turn_key = "join@D0=good & join@D-1=bad"
    assert persist_key in keys
    assert turn_key in keys
    assert keys[persist_key]["1d_mean"] > keys[turn_key]["1d_mean"]


def test_leaderboards_split_hit_and_mean() -> None:
    rows = [_row(ret=0.4) for _ in range(90)]
    rows += [_row(ret=2.0, boxes={
        "join": "good", "sector": "neutral", "gen": "bad", "ab": "good",
        "peer": "neutral", "vol": "good", "heat": "missing", "news": "missing",
        "digest": "neutral", "judge": "neutral", "catal": "missing", "buy": "good",
    }) for _ in range(90)]
    buckets = mine.mine(rows)
    pool = []
    for fam in ("single", "combo2", "combo3", "zero_red", "lag"):
        pool.extend(buckets[fam])
    by_hit = mine._rank(pool, "1d", "hit")
    by_mean = mine._rank(pool, "1d", "mean")
    assert by_hit
    assert by_mean
    assert by_hit[0]["1d_hit"] >= by_hit[-1]["1d_hit"]
    assert by_mean[0]["1d_mean"] >= by_mean[-1]["1d_mean"]


def test_render_lists_horizons() -> None:
    rows = [_row() for _ in range(90)]
    payload = {
        "generated_at": "t",
        "meta": {"n_names": 1, "n_rows": 90,
                 "from_date": "2026-08-20", "to_date": "2026-08-20"},
        "buckets": mine.mine(rows),
    }
    md = mine.render_md(payload)
    assert "Camera combo mine" in md
    assert "2w" in md
    assert "Zero reds" in md
    assert "Lag interactions" in md


if __name__ == "__main__":
    test_zero_red_beats_has_red()
    test_lag_turn_vs_persist()
    test_leaderboards_split_hit_and_mean()
    test_render_lists_horizons()
    print("4 tests passed")
