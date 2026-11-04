"""Rebuilt Factor Mine days stay out of the sealed store.

Run: python -m src.test_factor_mine_rebuild
"""
from __future__ import annotations

from pathlib import Path

from src.factor_mine_freeze import refuse_past_restate
from src.factor_mine_rebuild import (
    _write_once,
    classify_zero,
    describes_after_open,
    leak_split,
    matching_earlier_export,
    select_rows,
)


def test_leak_check_uses_the_time_the_row_describes() -> None:
    prior = {
        "date": "2026-10-14",
        "ticker": "AAA",
        "prior_date": "2026-10-13",
        "news_export_date": "2026-10-13",
        "close": 12.5,
        "asof": "",
    }
    assert describes_after_open(
        prior, "2026-10-14",
        panel_asof="09:30_et",
        panel_leak="prior tape + pre-open packet; news from prior export or morning box",
    ) == ""
    morning = dict(prior, described_at="2026-10-14T09:15:00-04:00")
    assert describes_after_open(morning, "2026-10-14") == ""
    landed_later = dict(prior, described_at="2026-10-14T08:00:00-04:00")
    assert describes_after_open(landed_later, "2026-10-14") == ""
    afternoon = dict(prior, described_at="2026-10-14T10:02:00-04:00")
    assert "after 09:30" in describes_after_open(afternoon, "2026-10-14")
    same_day_export = dict(prior, news_export_date="2026-10-14", described_at="")
    assert "after 09:30" in describes_after_open(same_day_export, "2026-10-14")
    close_asof = {"date": "2026-10-14", "asof": "16:00"}
    assert describes_after_open(close_asof, "2026-10-14").startswith("asof=")
    kept, dropped = leak_split(
        [prior, same_day_export], "2026-10-14", panel_asof="09:30_et")
    assert len(kept) == 1 and len(dropped) == 1
    assert "close" not in kept[0]
    assert kept[0]["tag"] == "rebuilt"
    assert kept[0]["ticker"] == "AAA"


def test_reconstructed_names_replace_the_stuck_list() -> None:
    rows = [
        {"ticker": "STUCK", "sources": ["ohlc_hot"]},
        {"ticker": "FLAT", "sources": ["flatten"]},
        {"ticker": "FRESH", "sources": ["yday_gainer"]},
    ]
    fresh = select_rows(rows, {
        "reconstructed": True,
        "names": ["FRESH"],
        "stuck_list_reused": False,
    })
    assert [row["ticker"] for row in fresh] == ["FLAT", "FRESH"]
    flagged = select_rows(rows, {
        "reconstructed": False,
        "names": ["STUCK", "FRESH"],
        "flag": "no prior-day export",
        "stuck_list_reused": False,
    })
    assert [row["ticker"] for row in flagged] == ["FLAT"]
    text = Path(__file__).with_name("factor_mine_rebuild.py").read_text(encoding="utf-8")
    assert "candidate_path" not in text
    assert "stuck_list_reused" in text


def test_a_copied_export_is_not_a_reconstructed_morning() -> None:
    digests = {
        "2026-10-12": "abc",
        "2026-10-13": "abc",
        "2026-10-14": "abc",
    }
    assert matching_earlier_export("2026-10-13", digests) == "2026-10-12"
    assert matching_earlier_export("2026-10-12", digests) is None
    assert matching_earlier_export("2026-10-09", digests) is None


def test_classify_zero_days() -> None:
    hard = classify_zero(
        hard_red=True, n_picks=4, skips=[], both_zero=True,
        empty_input=True, candidate_ok=True, universe_rows=10,
    )
    assert hard["class"] == "sat out (rule)"
    assert "S<=-3" in hard["reason"]
    lock = classify_zero(
        hard_red=False, n_picks=4,
        skips=[{"kind": "no_price", "reason": "no 09:30 open"}],
        both_zero=True, empty_input=False, candidate_ok=True,
        universe_rows=10, rebuilt_fills=0,
    )
    assert lock["class"] == "still broken" and lock["step"] == "lock"
    rule = classify_zero(
        hard_red=False, n_picks=0, skips=[], both_zero=True,
        empty_input=False, candidate_ok=True, universe_rows=0,
    )
    assert rule["class"] == "sat out (rule)"
    first = classify_zero(
        hard_red=False, n_picks=0, skips=[], both_zero=True,
        empty_input=False, candidate_ok=True, universe_rows=9,
    )
    assert first["step"] == "build"
    missing = classify_zero(
        hard_red=False, n_picks=0, skips=[], both_zero=True,
        empty_input=True, candidate_ok=False, universe_rows=0,
    )
    assert missing["step"] == "build"
    filled = classify_zero(
        hard_red=False, n_picks=2, skips=[], both_zero=False,
        empty_input=True, candidate_ok=True, universe_rows=20,
        rebuilt_fills=2,
    )
    assert filled["class"] == "rebuilt"
    render = classify_zero(
        hard_red=False, n_picks=0, skips=[], both_zero=True,
        empty_input=True, candidate_ok=True, universe_rows=4,
        sim_error="RuntimeError: book failed",
    )
    assert render["step"] == "render"


def test_rebuilt_file_is_write_once(tmp_path: Path) -> None:
    path = tmp_path / "2026-10-14.json"
    assert _write_once(path, {"date": "2026-10-14", "tag": "rebuilt"})
    path.write_text('{"tag":"rebuilt","sealed":true}\n', encoding="utf-8")
    assert _write_once(path, {"date": "2026-10-14", "tag": "changed"}) is False
    assert path.read_text(encoding="utf-8") == '{"tag":"rebuilt","sealed":true}\n'


def test_restate_still_refuses_a_past_day() -> None:
    try:
        refuse_past_restate(["2026-10-14"])
    except SystemExit as exc:
        text = str(exc)
        assert "refuses past" in text
        assert "2026-10-14" in text
        return
    raise AssertionError("past restate was allowed")


def test_ovcal_names_rebuilt_rule_and_missed() -> None:
    html = (Path(__file__).resolve().parent / "factor_mine_ovcal.html").read_text(
        encoding="utf-8")
    assert "sat out (rule)" in html
    assert "rebuilt" in html
    assert "missed" in html
    assert "cell.missed" in html
    assert "labelText" in html
    assert "rebuilt_labels.json" in html


def main() -> None:
    import tempfile
    tests = [
        test_leak_check_uses_the_time_the_row_describes,
        test_reconstructed_names_replace_the_stuck_list,
        test_a_copied_export_is_not_a_reconstructed_morning,
        test_classify_zero_days,
        test_rebuilt_file_is_write_once,
        test_restate_still_refuses_a_past_day,
        test_ovcal_names_rebuilt_rule_and_missed,
    ]
    for fn in tests:
        if fn.__code__.co_argcount:
            with tempfile.TemporaryDirectory() as raw:
                fn(Path(raw))
        else:
            fn()
        print("ok", fn.__name__)


if __name__ == "__main__":
    main()
