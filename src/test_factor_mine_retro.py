"""Point-in-time retro guards. Git history is the clock, not the working tree."""
from __future__ import annotations

from pathlib import Path

from src import factor_mine_retro as retro


def test_0922_digest_commit_is_the_preopen_one() -> None:
    info = retro.classify_day("2026-09-22")
    rel = "01_daily/news/2026-09-22_finviz_digest.json"
    sha = info["sources"][rel]
    assert sha is not None
    assert sha.startswith("4d822a3")
    assert "digest" not in info["missing_late"]
    assert info["label"] == "pit_rebuilt"


def test_0901_baseline_only_exists_after_the_open() -> None:
    info = retro.classify_day("2026-09-01")
    assert info["label"] == "incomplete_pit"
    assert "baseline" in info["missing_late"]


def test_absent_input_is_not_a_late_fill() -> None:
    info = retro.classify_day("2026-08-13")
    assert "digest" in info["absent"]
    assert "digest" not in info["missing_late"]
    assert info["label"] == "incomplete_pit"
    assert "judge" in info["missing_late"]


def test_materialize_does_not_copy_a_post_cutoff_digest() -> None:
    import tempfile
    digest = "01_daily/news/2026-08-25_finviz_digest.json"
    weather = "01_daily/weather/2026-08-25_weather.json"
    cutoff = retro.cutoff_for("2026-08-25")
    assert retro.commit_asof(digest, cutoff) is None
    wsha = retro.commit_asof(weather, cutoff)
    assert wsha
    history = {
        digest: retro._git_commits(digest),
        weather: retro._git_commits(weather),
    }
    with tempfile.TemporaryDirectory() as d:
        dest = Path(d)
        retro.materialize("2026-08-25", dest, history)
        assert not (dest / digest).exists()
        assert (dest / weather).is_file()


def test_flat_15bp_is_half_per_side() -> None:
    fee = retro.flat_15bp_order_fees(100, 10.0, "buy", {})
    assert abs(fee - 0.75) < 1e-9
    assert retro.flat_15bp_order_fees(0, 10.0, "sell", {}) == 0.0


def test_drop_ticker_removes_glnd_only() -> None:
    panel = {
        "session_dates": ["2026-09-24"],
        "rows": [
            {"date": "2026-09-24", "ticker": "GLND"},
            {"date": "2026-09-24", "ticker": "AAA"},
        ],
    }
    out = retro.drop_ticker(panel, "GLND")
    assert [r["ticker"] for r in out["rows"]] == ["AAA"]
    assert "GLND" not in (out["by_date"].get("2026-09-24") or [{}])[0].get("ticker", "AAA") or True
    tickers = [r["ticker"] for r in out["by_date"]["2026-09-24"]]
    assert tickers == ["AAA"]


def test_incomplete_snapshot_carries_no_rows() -> None:
    info = {
        "label": "incomplete_pit",
        "cutoff": "2026-09-01T09:30:00-04:00",
        "sources": {},
        "missing_late": ["baseline"],
        "absent": [],
    }
    snap = retro.snapshot_for("2026-09-01", info, [{"ticker": "LEAK"}], None)
    assert snap["rows"] == []
    assert snap["label"] == "incomplete_pit"
    assert snap["carry"] is True


if __name__ == "__main__":
    test_0922_digest_commit_is_the_preopen_one()
    test_0901_baseline_only_exists_after_the_open()
    test_absent_input_is_not_a_late_fill()
    test_materialize_does_not_copy_a_post_cutoff_digest()
    test_flat_15bp_is_half_per_side()
    test_drop_ticker_removes_glnd_only()
    test_incomplete_snapshot_carries_no_rows()
    print("factor-mine retro tests passed")
