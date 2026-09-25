"""One frozen input set for live tickets and the Factor Mine record.

Run: PYTHONPATH=. python3 -m src.test_factor_mine_send_inputs
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from unittest import mock
from zoneinfo import ZoneInfo

from src import factor_mine as fm
from src import factor_mine_freeze as fmf
from src import factor_mine_send_inputs as fsi
from src import strategy_tickets as st

ET = ZoneInfo("America/New_York")
DATE = "2026-09-28"


def _row(ticker: str, date: str = DATE, hot: float = 1.0, rank: int = 0) -> dict:
    return {
        "date": date,
        "ticker": ticker,
        "sources": ["union"],
        "src_rank": rank,
        "ohlc_hot_score": hot,
        "alarm": False,
    }


def _use_dir(tmp: Path):
    return mock.patch.object(fsi, "DIR", tmp)


def test_record_picks_match_sent_tickets_before_skips(tmp=None) -> None:
    """pick_day on the frozen rows is the sent list, before book skips."""
    import tempfile
    tmp = tmp or Path(tempfile.mkdtemp())
    rows = [
        _row("AAA", hot=3.0, rank=1),
        _row("BBB", hot=1.0, rank=0),
        _row("FUTURE", date="2026-09-29", hot=9.0, rank=0),
    ]
    hot = fm.make_recipe("union_hot_n4_h1", top_n=4, rank="hot_score", forbid={})
    hold = fm.make_recipe(
        "union_hot_n4_holdup", top_n=4, rank="hot_score",
        s_boost="holdup", forbid={})
    same_day = fsi.rows_for_record(DATE, {"rows": rows})
    sent = [r["ticker"] for r in fm.pick_day(same_day, hot)]
    hold_sent = [r["ticker"] for r in fm.pick_day(same_day, hold)]
    assert sent == ["AAA", "BBB"]
    assert "FUTURE" not in sent
    session = {
        "date": DATE,
        "source": "panel",
        "error": "",
        "rows": rows,
        "picks": {
            "union_hot_n4_h1": sent,
            "union_hot_n4_holdup": hold_sent,
        },
    }
    payload = {
        "date": DATE,
        "decision_readiness": {"inputs": {"data/join/2026-09-28_join.csv": "abc"}},
        "strategies": {
            "union_hot_n4_h1": {
                "buy": [{"ticker": "AAA", "px": 10.5, "open_px": 10.0, "px_src": "elite"}],
                "sell": [],
            },
            "union_hot_n4_holdup": {"buy": [{"ticker": t} for t in hold_sent], "sell": []},
        },
    }
    morning = datetime(2026, 9, 28, 8, 30, tzinfo=ET)
    with _use_dir(tmp):
        fsi.store(DATE, payload, session, now=morning)
        assert fsi.picks_from_send_inputs(DATE, hot) == sent
        assert fsi.picks_from_send_inputs(DATE, hold) == hold_sent
        doc = fsi.load(DATE)
    assert doc["picks"]["union_hot_n4_h1"] == sent
    assert doc["sha256"] == fsi.content_sha(doc)
    assert doc["prices"][0]["ticker"] == "AAA"
    assert doc["prices"][0]["px"] == 10.5
    assert doc["prices"][0]["open_px"] == 10.0
    assert all(row.get("date") == DATE for row in doc["rows"])
    assert any(item["path"] == "data/join/2026-09-28_join.csv" for item in doc["files"])


def test_missing_panel_sits_without_live_lookup() -> None:
    panel = {
        "to_date": "2026-09-25",
        "by_date": {"2026-09-25": [_row("OLD", date="2026-09-25")]},
        "rows": [_row("OLD", date="2026-09-25")],
        "session_dates": ["2026-09-25"],
    }
    with mock.patch("src.combo_broker.resolve_rows", side_effect=AssertionError("live lookup")):
        looked = st._session_look(DATE, panel)
    assert looked["rows"] == []
    assert looked["source"] == "no_same_day_panel"
    assert looked["stale"] is False
    assert "sitting" in looked["error"]

    hot = fm.make_recipe("union_hot_n4_h1", top_n=4, rank="hot_score", forbid={})
    hold = fm.make_recipe(
        "union_hot_n4_holdup", top_n=4, rank="hot_score",
        s_boost="holdup", forbid={})
    with mock.patch.object(st, "_load_json", return_value=panel), \
            mock.patch("src.factor_mine.rehydrate_panel", side_effect=lambda raw: raw), \
            mock.patch("src.factor_mine.build_recipes", return_value=[hot, hold]), \
            mock.patch("src.factor_mine_book.morning_s", return_value=1.0), \
            mock.patch("src.morning_scan.aisle_rows", side_effect=lambda _d, rows: rows), \
            mock.patch("src.factor_mine_combo.combo_specs", return_value=[]), \
            mock.patch("src.combo_broker.resolve_rows", side_effect=AssertionError("live lookup")):
        out = st.recipe_strats(DATE)
    by_name = {row["name"]: row for row in out}
    assert by_name["union_hot_n4_h1"]["status"] == "sit"
    assert by_name["union_hot_n4_h1"]["buy"] == []
    assert by_name["union_hot_n4_holdup"]["buy"] == []
    assert st.recipe_strats.last_session["source"] == "no_same_day_panel"
    assert st.recipe_strats.last_session["picks"]["union_hot_n4_h1"] == []
    payload = {
        "date": DATE,
        "clock_legal_for": DATE,
        "look": {
            "source": "no_same_day_panel",
            "panel_bake_date": "2026-09-25",
            "stale": False,
        },
        "strategies": {
            "union_hot_n4_h1": {
                "family": "factor_mine",
                "date": DATE,
                "clock_legal_for": DATE,
                "buy": [],
            },
        },
    }
    st.assert_session_look(payload, DATE)


def test_dates_before_the_cutoff_still_use_the_live_look() -> None:
    def fake(date, _panel):
        return {
            "date": date,
            "rows": [{"ticker": "AMD", "date": date}],
            "stale": False,
            "source": "look",
        }

    with mock.patch("src.combo_broker.resolve_rows", side_effect=fake) as look:
        looked = st._session_look("2026-09-25", {"by_date": {}, "to_date": "2026-09-24"})
    assert look.called
    assert looked["source"] == "look"
    assert looked["rows"][0]["ticker"] == "AMD"
    assert fsi.store("2026-09-25", {}, {"rows": [_row("AAA", date="2026-09-25")]}) is None
    assert fsi.applies("2026-09-25") is False
    assert fsi.applies("2026-09-28") is True


def test_send_inputs_are_immutable_once_locked(tmp=None) -> None:
    import tempfile
    tmp = tmp or Path(tempfile.mkdtemp())
    session = {
        "date": DATE, "source": "panel", "error": "",
        "rows": [_row("AAA")],
        "picks": {"union_hot_n4_h1": ["AAA"], "union_hot_n4_holdup": ["AAA"]},
    }
    revised = {
        "date": DATE, "source": "panel", "error": "",
        "rows": [_row("BBB")],
        "picks": {"union_hot_n4_h1": ["BBB"], "union_hot_n4_holdup": ["BBB"]},
    }
    morning = datetime(2026, 9, 28, 8, 30, tzinfo=ET)
    pre_open = datetime(2026, 9, 28, 9, 0, tzinfo=ET)
    evening = datetime(2026, 9, 28, 16, 15, tzinfo=ET)
    with _use_dir(tmp):
        fsi.store(DATE, {}, session, now=morning)
        fsi.store(DATE, {}, revised, now=pre_open)
        frozen = (tmp / f"{DATE}.json").read_bytes()
        assert b"BBB" in frozen
        fsi.store(DATE, {}, session, now=evening)
        assert (tmp / f"{DATE}.json").read_bytes() == frozen
        doc = fsi.load(DATE)
    assert doc["picks"]["union_hot_n4_h1"] == ["BBB"]


def test_no_future_rows_and_no_snapshot_read(tmp=None) -> None:
    import tempfile
    tmp = tmp or Path(tempfile.mkdtemp())
    rows = [_row("SENT"), _row("LATER", date="2026-09-29", hot=20)]
    rec = fm.make_recipe("union_hot_n4_h1", top_n=4, rank="hot_score", forbid={})
    session = {
        "date": DATE, "source": "panel", "rows": rows, "error": "",
        "picks": {"union_hot_n4_h1": ["SENT"], "union_hot_n4_holdup": ["SENT"]},
    }
    with _use_dir(tmp):
        fsi.store(DATE, {}, session, now=datetime(2026, 9, 28, 8, 30, tzinfo=ET))
        with mock.patch.object(
                fmf, "snapshot_path", side_effect=AssertionError("snapshot read")):
            picks = fsi.picks_from_send_inputs(DATE, rec)
    assert picks == ["SENT"]


def test_ledger_uses_send_rows_for_live_recipes_only(tmp=None) -> None:
    import tempfile
    tmp = tmp or Path(tempfile.mkdtemp())
    snap = _row("SNAP")
    sent = _row("SENT", hot=4)
    panel = {
        "session_dates": [DATE],
        "rows": [snap],
        "by_date": {DATE: [snap]},
    }
    recipes = [
        fm.make_recipe("union_hot_n4_h1", top_n=4, rank="hot_score", forbid={}),
        fm.make_recipe("union_h1", top_n=1, rank="list", forbid={}),
    ]
    session = {
        "date": DATE, "source": "panel", "rows": [sent], "error": "",
        "picks": {"union_hot_n4_h1": ["SENT"], "union_hot_n4_holdup": ["SENT"]},
    }
    seen: dict[str, list[str]] = {}

    def fake_sim(panel_in, rec, **_kwargs):
        rows = (panel_in.get("by_date") or {}).get(DATE) or []
        seen[rec["name"]] = [row.get("ticker") for row in rows]
        return {
            "trades": [],
            "daily": [{"date": DATE, "equity": 10000}],
            "skips": [{"date": DATE, "ticker": "SENT", "kind": "hard_red"}],
            "cash": 10000,
            "pos": {},
        }

    with _use_dir(tmp), \
            mock.patch.object(fmf, "latest_ledger_before", return_value=None), \
            mock.patch.object(fmf, "bridge_published", return_value=None), \
            mock.patch.object(fmf, "_simulate_single", side_effect=fake_sim):
        fsi.store(DATE, {}, session, now=datetime(2026, 9, 28, 8, 30, tzinfo=ET))
        ledger = fmf.build_ledger(panel, {}, recipes, DATE, {})
        # The book skip is not the pick list the record shares with the ticket.
        assert fsi.picks_from_send_inputs(DATE, recipes[0]) == ["SENT"]
    assert ledger["input_source"] == "send_inputs"
    assert seen["union_hot_n4_h1"] == ["SENT"]
    assert seen["union_h1"] == ["SNAP"]
    labeled = fmf.splice_payload(
        {"dates": [], "mornings": {DATE: {}}, "capital": 10000},
        DATE, ledger,
    )
    assert labeled["input_sources"][DATE] == "send_inputs"


def test_absent_send_file_labels_the_day_snapshot() -> None:
    snap = _row("SNAP")
    panel = {
        "session_dates": [DATE],
        "rows": [snap],
        "by_date": {DATE: [snap]},
    }
    recipes = [fm.make_recipe("union_hot_n4_h1", top_n=4, rank="hot_score", forbid={})]
    seen: dict[str, list[str]] = {}

    def fake_sim(panel_in, rec, **_kwargs):
        rows = (panel_in.get("by_date") or {}).get(DATE) or []
        seen[rec["name"]] = [row.get("ticker") for row in rows]
        return {"trades": [], "daily": [], "skips": [], "cash": 1, "pos": {}}

    with mock.patch.object(fmf, "latest_ledger_before", return_value=None), \
            mock.patch.object(fmf, "bridge_published", return_value=None), \
            mock.patch.object(fmf, "_simulate_single", side_effect=fake_sim), \
            mock.patch.object(fsi, "DIR", Path("/tmp/fm-send-inputs-missing-0928")):
        ledger = fmf.build_ledger(panel, {}, recipes, DATE, {})
    assert ledger["input_source"] == "snapshot"
    assert seen["union_hot_n4_h1"] == ["SNAP"]


def test_old_ledger_days_are_not_relabelled() -> None:
    panel = {
        "session_dates": ["2026-09-25"],
        "rows": [],
        "by_date": {"2026-09-25": []},
    }
    recipes = [fm.make_recipe("union_h1", hold=1, top_n=1)]

    def fake_sim(_panel, _rec, **_kwargs):
        return {"trades": [], "daily": [], "skips": [], "cash": 1, "pos": {}}

    with mock.patch.object(fmf, "latest_ledger_before", return_value=None), \
            mock.patch.object(fmf, "bridge_published", return_value=None), \
            mock.patch.object(fmf, "_simulate_single", side_effect=fake_sim):
        ledger = fmf.build_ledger(panel, {}, recipes, "2026-09-25", {})
    assert "input_source" not in ledger


def test_write_saves_send_inputs_beside_the_dated_ticket(tmp=None) -> None:
    import tempfile
    tmp = tmp or Path(tempfile.mkdtemp())
    day = tmp / "day"
    send = tmp / "send"
    payload = {
        "date": DATE,
        "clock_legal_for": DATE,
        "look": {"source": "panel", "panel_bake_date": DATE},
        "decision_readiness": {"ready": True, "fingerprint": "abc"},
        "strategies": {
            "union_hot_n4_h1": {
                "date": DATE,
                "buy": [{"ticker": "AAA", "px": 3}],
                "sell": [],
                "status": "ok",
                "family": "factor_mine",
                "clock_legal_for": DATE,
            },
        },
    }
    st.recipe_strats.last_session = {
        "date": DATE,
        "source": "panel",
        "error": "",
        "rows": [_row("AAA")],
        "picks": {"union_hot_n4_h1": ["AAA"], "union_hot_n4_holdup": ["AAA"]},
    }
    morning = datetime(2026, 9, 28, 8, 30, tzinfo=ET)
    evening = datetime(2026, 9, 28, 17, 5, tzinfo=ET)
    with mock.patch.object(st, "DAY", day), \
            mock.patch.object(st, "FM_DIR", tmp / "fm"), \
            mock.patch.object(st, "DASH_FM", tmp / "dash"), \
            mock.patch.object(st, "ROOT", tmp), \
            mock.patch.object(st, "assert_session_look"), \
            mock.patch("src.hard_red_sit_research.write_per_sleeve"), \
            _use_dir(send):
        st.write(DATE, payload, now=morning)
        frozen = (send / f"{DATE}.json").read_bytes()
        assert b"AAA" in frozen
        assert (day / f"{DATE}_strategy_tickets.json").is_file()
        st.recipe_strats.last_session = {
            "date": DATE, "source": "panel", "error": "",
            "rows": [_row("ZZZ")],
            "picks": {"union_hot_n4_h1": ["ZZZ"], "union_hot_n4_holdup": []},
        }
        st.write(DATE, dict(payload, strategies={
            "union_hot_n4_h1": {
                "date": DATE, "buy": [{"ticker": "ZZZ"}], "sell": [],
                "status": "ok", "family": "factor_mine", "clock_legal_for": DATE,
            },
        }), now=evening)
        assert (send / f"{DATE}.json").read_bytes() == frozen
    root = Path(__file__).resolve().parent.parent
    yml = (root / ".github/workflows/publish_strategy_tickets.yml").read_text(
        encoding="utf-8")
    assert "data/factor_mine/send_inputs/${DATE}.json" in yml
    assert "python -m src.paper_open --submit --ready --owner actions" in yml


def main() -> None:
    test_record_picks_match_sent_tickets_before_skips()
    test_missing_panel_sits_without_live_lookup()
    test_dates_before_the_cutoff_still_use_the_live_look()
    test_send_inputs_are_immutable_once_locked()
    test_no_future_rows_and_no_snapshot_read()
    test_ledger_uses_send_rows_for_live_recipes_only()
    test_absent_send_file_labels_the_day_snapshot()
    test_old_ledger_days_are_not_relabelled()
    test_write_saves_send_inputs_beside_the_dated_ticket()
    print("send-input tests ok")


if __name__ == "__main__":
    main()
