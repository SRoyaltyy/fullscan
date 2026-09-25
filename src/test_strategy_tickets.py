"""Combo morning tickets are the union of member pick_day lists.

Run: PYTHONPATH=. python3 -m src.test_strategy_tickets
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest import mock

from src import strategy_tickets as st


class _FakeFM:
    def __init__(self, picks: dict):
        self.picks = picks

    def pick_day(self, rows, rec):
        return list(self.picks.get(rec["name"], []))


def test_combo_would_buy_unions_member_lists() -> None:
    rec_by = {
        "short_news_r_h3": {"name": "short_news_r_h3", "side": "short"},
        "union_e_fresh_h3": {"name": "union_e_fresh_h3", "side": "long"},
        "union_hot_n4_h1": {"name": "union_hot_n4_h1", "side": "long"},
    }
    fm = _FakeFM({
        "short_news_r_h3": [{"ticker": "SPIR"}, {"ticker": "AEHR"}],
        "union_e_fresh_h3": [{"ticker": "AVO"}],
        "union_hot_n4_h1": [{"ticker": "INTC"}, {"ticker": "AEHR"}],
    })
    buys = st._combo_would_buy(
        [], rec_by,
        ["short_news_r_h3", "union_e_fresh_h3", "union_hot_n4_h1"],
        {"net": "priority"}, fm,
    )
    names = [b["ticker"] for b in buys]
    assert names == ["SPIR", "AEHR", "AVO", "INTC"]
    assert next(b for b in buys if b["ticker"] == "AEHR")["src"] == "short_news_r_h3"


def test_combo_skip_drops_long_and_short_clash() -> None:
    rec_by = {
        "short_news_r_h3": {"name": "short_news_r_h3", "side": "short"},
        "union_e_fresh_h3": {"name": "union_e_fresh_h3", "side": "long"},
    }
    fm = _FakeFM({
        "short_news_r_h3": [{"ticker": "AEHR"}, {"ticker": "SPIR"}],
        "union_e_fresh_h3": [{"ticker": "AEHR"}, {"ticker": "AVO"}],
    })
    buys = st._combo_would_buy(
        [], rec_by, ["short_news_r_h3", "union_e_fresh_h3"],
        {"net": "skip"}, fm,
    )
    assert [b["ticker"] for b in buys] == ["SPIR", "AVO"]


def test_recipe_strats_never_emits_combo_needs_mine() -> None:
    """A combo is a union of member 09:30 lists — not a mine-only stub."""
    src = __import__("pathlib").Path(st.__file__).read_text(encoding="utf-8")
    assert "combo_needs_mine" not in src
    assert "_combo_would_buy" in src
    rows = st.recipe_strats("2026-09-10")
    combos = [r for r in rows if r["name"].startswith("combo_")]
    assert combos, "combo_specs should be attached even without a blotter md"
    bad = [r["name"] for r in combos if r.get("status") == "combo_needs_mine"]
    assert not bad, bad
    # At least one mix should inherit a member name that pick_day already lists.
    with_names = [r for r in combos if r.get("buy_n")]
    assert with_names, "expected at least one combo shopping list from the panel"


def test_source_uses_session_look_not_last_bake() -> None:
    src = __import__("pathlib").Path(st.__file__).read_text(encoding="utf-8")
    assert "older[-1]" not in src
    assert "_session_look" in src
    assert "resolve_rows" in src
    assert "assert_session_look" in src
    assert "clock_legal_for" in src
    assert "SESSION_OPEN_LOCK" in src
    assert "morning_scan" in src
    assert "pick_morning" in src
    assert "attach_paper_dual_run" not in src


def test_session_look_uses_panel_when_session_present() -> None:
    rows = [{"ticker": "INDP"}]
    panel = {"by_date": {"2026-09-14": rows}, "to_date": "2026-09-14"}
    looked = st._session_look("2026-09-14", panel)
    assert looked["source"] == "panel"
    assert looked["rows"] == rows
    assert looked["stale"] is False
    assert looked["date"] == "2026-09-14"


def test_session_look_uses_open_rows_when_panel_stops_early() -> None:
    rows = [{"ticker": "INDP"}, {"ticker": "GPRO"}]
    panel = {
        "to_date": "2026-09-11",
        "session_dates": ["2026-09-11"],
        "by_date": {"2026-09-11": [{"ticker": "QRVO"}]},
    }
    with mock.patch("src.combo_broker.resolve_rows", return_value={
        "date": "2026-09-14", "rows": rows, "stale": False,
        "source": "look", "want_date": "2026-09-14",
    }) as resolve:
        looked = st._session_look("2026-09-14", panel)
    resolve.assert_called_once()
    assert looked["rows"] == rows
    assert looked["stale"] is False
    assert looked["date"] == "2026-09-14"
    assert looked["source"] == "look"


def test_session_look_refuses_last_bake() -> None:
    panel = {
        "to_date": "2026-09-11",
        "session_dates": ["2026-09-11"],
        "by_date": {"2026-09-11": [{"ticker": "QRVO"}]},
    }
    with mock.patch("src.combo_broker.resolve_rows", return_value={
        "date": "2026-09-11", "rows": [{"ticker": "QRVO"}],
        "stale": True, "source": "panel_asof", "want_date": "2026-09-14",
    }):
        looked = st._session_look("2026-09-14", panel)
    assert looked["rows"] == []
    assert looked["stale"] is True
    assert looked["date"] == "2026-09-14"
    assert "refusing last-bake" in (looked.get("error") or "")


def test_assert_session_look_blocks_stale_ship() -> None:
    payload = {
        "clock_legal_for": "2026-09-14",
        "look": {"source": "look", "stale": False, "panel_bake_date": "2026-09-11"},
        "strategies": {
            "union_hot_n4_h1": {
                "family": "factor_mine", "date": "2026-09-11",
                "clock_legal_for": "2026-09-11",
                "buy_n": 3, "buy": [{"ticker": "QRVO"}],
            },
            "stock_book_1d": {
                "family": "stock_book", "date": "2026-09-14",
                "buy_n": 1,
            },
        },
        "errors": [],
    }
    try:
        st.assert_session_look(payload, "2026-09-14")
    except AssertionError as e:
        assert "2026-09-14" in str(e)
        assert "union_hot_n4_h1" in str(e)
    else:
        raise AssertionError("expected refuse-to-ship")


def test_assert_fails_when_bake_is_not_session_open() -> None:
    payload = {
        "clock_legal_for": "2026-09-14",
        "look": {"source": "panel_asof", "stale": True,
                 "panel_bake_date": "2026-09-11", "date": "2026-09-11"},
        "strategies": {},
        "errors": [],
    }
    try:
        st.assert_session_look(payload, "2026-09-14")
    except AssertionError as e:
        assert "panel bake date" in str(e)
        assert "2026-09-14" in str(e)
    else:
        raise AssertionError("expected bake ≠ session fail")
    try:
        st.assert_session_look(
            {"look": {"source": "look", "stale": False}, "strategies": {}},
            "2026-09-14",
        )
    except AssertionError as e:
        assert "clock_legal_for" in str(e)
    else:
        raise AssertionError("expected missing clock_legal_for")


def test_open_lock_pins_indp_and_drops_friday() -> None:
    buys = [
        {"ticker": "QRVO", "kid_side": "short", "src": "short_news_r_macd_h3"},
        {"ticker": "CMRC", "kid_side": "long", "src": "union_hot_n4_h1"},
        {"ticker": "GPRO", "kid_side": "long", "src": "union_hot_n4_h1"},
        {"ticker": "BKV", "kid_side": "short", "src": "short_news_r_macd_h3"},
    ]
    out = st.apply_open_lock("2026-09-14", st.LIVE_WEBULL_COMBO, buys)
    names = [b["ticker"] for b in out]
    assert "QRVO" not in names and "MYGN" not in names
    longs = {b["ticker"] for b in out if b.get("kid_side") == "long"}
    shorts = {b["ticker"] for b in out if b.get("kid_side") == "short"}
    assert longs == {"INDP", "GPRO", "VERI", "HUT", "CMRC"}
    assert shorts == {"BKV", "AMD"}
    assert next(b for b in out if b["ticker"] == "INDP")["src"] == "hard_red_sit"
    # Other recipes / dates are untouched.
    assert st.apply_open_lock("2026-09-14", "union_hot_n4_h1", buys) == buys
    assert st.apply_open_lock("2026-09-11", st.LIVE_WEBULL_COMBO, buys) == buys


def test_assert_open_lock_requires_webull_sit_names() -> None:
    payload = {
        "clock_legal_for": "2026-09-14",
        "look": {"source": "look", "stale": False, "panel_bake_date": "2026-09-11"},
        "strategies": {
            "combo_sh_macd_5050_shared": {
                "family": "factor_mine", "date": "2026-09-14",
                "clock_legal_for": "2026-09-14",
                "buy": [
                    {"ticker": "CMRC", "kid_side": "long"},
                    {"ticker": "BKV", "kid_side": "short"},
                ],
            },
        },
        "errors": [],
    }
    try:
        st.assert_session_look(payload, "2026-09-14")
    except AssertionError as e:
        assert "INDP" in str(e) or "missing_longs" in str(e)
    else:
        raise AssertionError("expected lock fail without INDP")
    locked = st.SESSION_OPEN_LOCK["2026-09-14"]
    payload["strategies"]["combo_sh_macd_5050_shared"]["buy"] = (
        [{"ticker": t, "kid_side": "long"} for t in locked["longs"]]
        + [{"ticker": t, "kid_side": "short"} for t in locked["shorts"]]
    )
    st.assert_session_look(payload, "2026-09-14")


def test_evening_run_does_not_rewrite_dated_tickets(tmp_path=None) -> None:
    from datetime import datetime
    from zoneinfo import ZoneInfo
    import tempfile

    if tmp_path is None:
        tmp_path = Path(tempfile.mkdtemp())
    et = ZoneInfo("America/New_York")
    date = "2026-09-23"
    morning = {
        "date": date,
        "generated_at": "2026-09-23T08:30:00-04:00",
        "decision_readiness": {"ready": True, "fingerprint": "abc"},
        "strategies": {
            "union_hot_n4_h1": {
                "date": date, "buy": [{"ticker": "INDP"}], "sell": [],
                "s": 2.2, "status": "ok",
            },
        },
    }
    revised = {
        "date": date,
        "generated_at": "2026-09-23T09:20:00-04:00",
        "decision_readiness": {"ready": True, "fingerprint": "abc"},
        "strategies": {
            "union_hot_n4_h1": {
                "date": date, "buy": [{"ticker": "GLND"}], "sell": [],
                "s": 2.2, "status": "ok",
            },
        },
    }
    evening = {
        "date": date,
        "generated_at": "2026-09-23T17:05:00-04:00",
        "decision_readiness": {"ready": True, "fingerprint": "abc"},
        "strategies": {
            "union_hot_n4_h1": {
                "date": date, "buy": [{"ticker": "FEAM"}], "sell": [],
                "s": 2.2, "status": "ok",
            },
        },
    }
    with mock.patch.object(st, "DAY", tmp_path / "day"), \
         mock.patch.object(st, "FM_DIR", tmp_path / "fm"), \
         mock.patch.object(st, "DASH_FM", tmp_path / "dash"), \
         mock.patch.object(st, "ROOT", tmp_path), \
         mock.patch.object(st, "assert_session_look"), \
         mock.patch("src.hard_red_sit_research.write_per_sleeve"):
        st.write(date, morning, now=datetime(2026, 9, 23, 8, 30, tzinfo=et))
        dated = tmp_path / "day" / f"{date}_strategy_tickets.json"
        assert "INDP" in dated.read_text(encoding="utf-8")
        st.write(date, revised, now=datetime(2026, 9, 23, 9, 20, tzinfo=et))
        assert "GLND" in dated.read_text(encoding="utf-8")
        frozen = dated.read_bytes()
        import contextlib
        import io
        for clock in (
            datetime(2026, 9, 23, 9, 30, tzinfo=et),
            datetime(2026, 9, 23, 17, 5, tzinfo=et),
        ):
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                st.write(date, evening, now=clock)
            log = buf.getvalue()
            assert "WARN" in log
            assert "Evening body is in" in log
            assert "Job failed" not in log
            assert dated.read_bytes() == frozen
            lock = st.write.last_lock or {}
            assert lock.get("status") == "draft"
            assert "09:30" in (lock.get("reason") or "")
        draft = tmp_path / "day" / f"{date}_strategy_tickets_draft.json"
        assert "FEAM" in draft.read_text(encoding="utf-8")
        live = (tmp_path / "day" / "strategy_tickets.json").read_text(encoding="utf-8")
        assert "FEAM" in live
        assert "INDP" not in dated.read_text(encoding="utf-8")
        assert "GLND" in dated.read_text(encoding="utf-8")
        slim = json.loads((tmp_path / "day" / "today_strategies.json").read_text())
        assert slim["ticket_lock"]["dated_unchanged"] is True
        assert slim["ticket_lock"]["draft"].endswith("_strategy_tickets_draft.json")
        st.write(date, revised, now=datetime(2026, 9, 23, 17, 5, tzinfo=et))
        assert st.write.last_lock is None
        assert dated.read_bytes() == frozen
        late = "2026-09-24"
        st.write(late, dict(evening, date=late), now=datetime(2026, 9, 24, 16, 0, tzinfo=et))
        assert (tmp_path / "day" / f"{late}_strategy_tickets.json").is_file()


def test_journal_locks_the_dated_file_before_the_open(tmp_path=None) -> None:
    from datetime import datetime
    from zoneinfo import ZoneInfo
    import tempfile

    if tmp_path is None:
        tmp_path = Path(tempfile.mkdtemp())
    et = ZoneInfo("America/New_York")
    date = "2026-09-22"
    sent = {
        "date": date,
        "decision_readiness": {"ready": True, "fingerprint": "abc"},
        "strategies": {"union_hot_n4_h1": {
            "date": date, "buy": [{"ticker": "AMD"}], "sell": [], "status": "ok",
        }},
    }
    rewrite = {
        "date": date,
        "decision_readiness": {"ready": True, "fingerprint": "abc"},
        "strategies": {"union_hot_n4_h1": {
            "date": date, "buy": [{"ticker": "ZS"}], "sell": [], "status": "ok",
        }},
    }
    with mock.patch.object(st, "DAY", tmp_path / "day"), \
         mock.patch.object(st, "FM_DIR", tmp_path / "fm"), \
         mock.patch.object(st, "DASH_FM", tmp_path / "dash"), \
         mock.patch.object(st, "ROOT", tmp_path), \
         mock.patch.object(st, "assert_session_look"), \
         mock.patch("src.hard_red_sit_research.write_per_sleeve"):
        morning = datetime(2026, 9, 22, 8, 30, tzinfo=et)
        st.write(date, sent, now=morning)
        dated = tmp_path / "day" / f"{date}_strategy_tickets.json"
        frozen = dated.read_bytes()
        journal = tmp_path / "data" / "paper_open" / f"{date}_submit.json"
        journal.parent.mkdir(parents=True, exist_ok=True)
        journal.write_text("{}", encoding="utf-8")
        st.write(date, rewrite, now=morning)
        assert dated.read_bytes() == frozen
        assert "AMD" in dated.read_text(encoding="utf-8")
        lock = st.write.last_lock or {}
        assert lock.get("status") == "draft"
        assert "journal" in (lock.get("reason") or "")
        draft = tmp_path / "day" / f"{date}_strategy_tickets_draft.json"
        assert "ZS" in draft.read_text(encoding="utf-8")


def test_locked_overwrite_and_failed_draft_still_fail(tmp_path=None) -> None:
    from datetime import datetime
    from zoneinfo import ZoneInfo
    import tempfile

    if tmp_path is None:
        tmp_path = Path(tempfile.mkdtemp())
    et = ZoneInfo("America/New_York")
    date = "2026-09-25"
    morning = {
        "date": date,
        "decision_readiness": {"ready": True, "fingerprint": "abc"},
        "strategies": {"union_hot_n4_h1": {
            "date": date, "buy": [{"ticker": "AMD"}], "sell": [], "status": "ok",
        }},
    }
    evening = {
        "date": date,
        "decision_readiness": {"ready": True, "fingerprint": "abc"},
        "strategies": {"union_hot_n4_h1": {
            "date": date, "buy": [{"ticker": "FEAM"}], "sell": [], "status": "ok",
        }},
    }
    clock = datetime(2026, 9, 25, 16, 15, tzinfo=et)
    with mock.patch.object(st, "DAY", tmp_path / "day"), \
         mock.patch.object(st, "FM_DIR", tmp_path / "fm"), \
         mock.patch.object(st, "DASH_FM", tmp_path / "dash"), \
         mock.patch.object(st, "ROOT", tmp_path), \
         mock.patch.object(st, "assert_session_look"), \
         mock.patch("src.hard_red_sit_research.write_per_sleeve"):
        st.write(date, morning, now=datetime(2026, 9, 25, 8, 30, tzinfo=et))
        dated = tmp_path / "day" / f"{date}_strategy_tickets.json"
        frozen = dated.read_bytes()
        real = Path.write_text

        def also_touch_dated(self, data, *args, **kwargs):
            real(self, data, *args, **kwargs)
            if self.name == f"{date}_strategy_tickets_draft.json":
                real(dated, data, *args, **kwargs)

        with mock.patch.object(Path, "write_text", also_touch_dated):
            try:
                st.write(date, evening, now=clock)
            except st.DatedTicketsLocked as exc:
                assert "modified" in str(exc)
                assert "Job failed" in str(exc)
            else:
                raise AssertionError("overwrite of a locked dated file must fail")
        dated.write_bytes(frozen)
        leftover = tmp_path / "day" / f"{date}_strategy_tickets_draft.json"
        if leftover.is_file():
            leftover.unlink()

        def skip_draft(self, data, *args, **kwargs):
            if self.name == f"{date}_strategy_tickets_draft.json":
                return 0
            return real(self, data, *args, **kwargs)

        with mock.patch.object(Path, "write_text", skip_draft):
            try:
                st.write(date, evening, now=clock)
            except st.DatedTicketsLocked as exc:
                assert "draft write failed" in str(exc)
                assert "Job failed" in str(exc)
            else:
                raise AssertionError("a failed draft write must fail the job")
        assert dated.read_bytes() == frozen


def test_postclose_does_not_wait_on_ticket_publish() -> None:
    """Post-close grades on its own clock. A ticket refusal must not gate it."""
    root = Path(__file__).resolve().parent.parent
    post = (root / ".github/workflows/postclose_all.yml").read_text(encoding="utf-8")
    assert "Publish strategy tickets" not in post
    assert "workflow_run:" not in post
    factor = (root / ".github/workflows/factor_mine.yml").read_text(encoding="utf-8")
    assert 'Post-Close ALL (grade + learn + next captains)' in factor
    assert "Publish strategy tickets" not in factor
    orch = (root / ".github/workflows/daily_orchestrator.yml").read_text(encoding="utf-8")
    assert "maybe postclose_all.yml" in orch
    assert "skip Post-Close ALL until 16:00 ET" in orch
    tickets = (root / ".github/workflows/publish_strategy_tickets.yml").read_text(
        encoding="utf-8")
    assert 'python3 -m src.decision_ready --date "$DATE" --publish' in tickets
    assert "python -m src.paper_open --submit --ready --owner actions" in tickets


def test_ticket_restate_log_appends_one_line_per_restore(tmp_path=None) -> None:
    import tempfile
    if tmp_path is None:
        tmp_path = Path(tempfile.mkdtemp())
    dest = tmp_path / "RESTATEMENTS.log"
    st.log_ticket_restate(
        "2026-09-22", commit="18039b02cfe8d52c037df7ce765b0c9edb3e8f02",
        prev_sha="aaa", send_sha="bbb", path=dest,
    )
    st.log_ticket_restate(
        "2026-09-23", commit="409c73e31a8a011c2e204045e16c89df75bd85fb",
        prev_sha="ccc", send_sha="ddd", path=dest,
    )
    lines = dest.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    assert "2026-09-22_strategy_tickets.json" in lines[0]
    assert "18039b02cfe8d52c037df7ce765b0c9edb3e8f02" in lines[0]
    assert "send_sha256=bbb" in lines[0]
    assert "2026-09-23_strategy_tickets.json" in lines[1]


def main() -> None:
    test_combo_would_buy_unions_member_lists()
    test_combo_skip_drops_long_and_short_clash()
    test_recipe_strats_never_emits_combo_needs_mine()
    test_source_uses_session_look_not_last_bake()
    test_session_look_uses_panel_when_session_present()
    test_session_look_uses_open_rows_when_panel_stops_early()
    test_session_look_refuses_last_bake()
    test_assert_session_look_blocks_stale_ship()
    test_assert_fails_when_bake_is_not_session_open()
    test_open_lock_pins_indp_and_drops_friday()
    test_assert_open_lock_requires_webull_sit_names()
    test_evening_run_does_not_rewrite_dated_tickets()
    test_journal_locks_the_dated_file_before_the_open()
    test_locked_overwrite_and_failed_draft_still_fail()
    test_postclose_does_not_wait_on_ticket_publish()
    test_ticket_restate_log_appends_one_line_per_restore()
    from src.test_factor_mine_send_inputs import main as send_inputs_main
    send_inputs_main()
    from src.test_morning_scan import main as morning_scan_main
    morning_scan_main()
    from src.test_hot4_wire import main as hot4_wire_main
    hot4_wire_main()
    print("ok")


if __name__ == "__main__":
    main()
