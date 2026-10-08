"""Live ticket copies lock at the session's 09:30 ET (2026-10-07 rewrite).

Run: python3 -m src.test_ticket_session_lock
"""
from __future__ import annotations

import contextlib
import io
import json
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from unittest import mock
from zoneinfo import ZoneInfo

from src import strategy_tickets as st
from src import ticket_session_lock as tsl

ET = ZoneInfo("America/New_York")
ROOT = Path(__file__).resolve().parents[1]


def _payload(date: str, ticker: str, generated: str) -> dict:
    return {
        "date": date,
        "clock_legal_for": date,
        "session_open": date,
        "generated_at": generated,
        "decision_readiness": {"ready": True, "fingerprint": "abc"},
        "strategies": {
            "union_h1": {
                "date": date, "family": "factor_mine", "status": "ok",
                "buy": [{"ticker": ticker}], "sell": [],
            },
        },
    }


@contextlib.contextmanager
def _sandbox(tmp: Path, manifest: Path):
    real_assert = tsl.assert_sealed
    real_seal = tsl.seal_locked
    with mock.patch.object(st, "DAY", tmp / "day"), \
         mock.patch.object(st, "FM_DIR", tmp / "fm"), \
         mock.patch.object(st, "DASH_FM", tmp / "dash"), \
         mock.patch.object(st, "ROOT", tmp), \
         mock.patch.object(st, "assert_session_look"), \
         mock.patch.object(st, "_freeze_send_inputs"), \
         mock.patch.object(tsl, "assert_sealed",
                           lambda path, **kw: real_assert(path, manifest=manifest, rel=tsl.CANONICAL)), \
         mock.patch.object(tsl, "seal_locked",
                           lambda path, date, **kw: real_seal(path, date, manifest=manifest,
                                                              rel=tsl.CANONICAL, **kw)), \
         mock.patch("src.hard_red_sit_research.write_per_sleeve"):
        yield


def _quiet(fn, *args, **kwargs):
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        out = fn(*args, **kwargs)
    return out, buf.getvalue()


def test_post_close_run_writes_only_a_marked_draft() -> None:
    tmp = Path(tempfile.mkdtemp())
    manifest = tmp / "manifest.jsonl"
    d = "2026-10-07"
    with _sandbox(tmp, manifest):
        _quiet(st.write, d, _payload(d, "AAA", "2026-10-07T06:27:00-04:00"),
               now=datetime(2026, 10, 7, 6, 27, tzinfo=ET))
        canon = tmp / "fm" / "strategy_tickets.json"
        send_time = canon.read_text(encoding="utf-8")
        assert "AAA" in send_time
        _, log = _quiet(st.write, d, _payload(d, "ZZZ", "2026-10-07T20:32:57-04:00"),
                        now=datetime(2026, 10, 7, 20, 32, 57, tzinfo=ET))
        assert "Not rewriting a started session" in log
        assert canon.read_text(encoding="utf-8") == send_time
        for rel in ("day/strategy_tickets.json", "dash/strategy_tickets.json",
                    "dash/today_strategies.json", "day/today_strategies.json",
                    "dashboard/today_strategies.json"):
            assert "ZZZ" not in (tmp / rel).read_text(encoding="utf-8"), rel
        dated = tmp / "day" / f"{d}_strategy_tickets.json"
        assert dated.read_text(encoding="utf-8") == send_time
        draft = json.loads((tmp / "day" / f"{d}_strategy_tickets_draft.json").read_text())
        assert draft["draft"] is True and draft["not_for_trading"] is True
        assert draft["strategies"]["union_h1"]["buy"] == [{"ticker": "ZZZ"}]
        rows = tsl.load_manifest(manifest)
        assert len(rows) == 1 and rows[0]["kind"] == "seal" and rows[0]["date"] == d
        assert rows[0]["sha256"] == tsl.sha256_text(send_time)
        # A second evening run adds no second seal.
        _quiet(st.write, d, _payload(d, "YYY", "2026-10-07T21:00:00-04:00"),
               now=datetime(2026, 10, 7, 21, 0, tzinfo=ET))
        assert len(tsl.load_manifest(manifest)) == 1


def test_next_morning_writes_the_next_session_before_0930() -> None:
    """#512 morning flow: 10-08 replaces a locked (even void) 10-07 copy."""
    tmp = Path(tempfile.mkdtemp())
    manifest = tmp / "manifest.jsonl"
    with _sandbox(tmp, manifest):
        _quiet(st.write, "2026-10-07", _payload("2026-10-07", "AAA", "2026-10-07T06:27:00-04:00"),
               now=datetime(2026, 10, 7, 6, 27, tzinfo=ET))
        canon = tmp / "fm" / "strategy_tickets.json"
        # The void 20:32 body sits on disk, recorded by a void row.
        void = json.dumps(_payload("2026-10-07", "VOID", "2026-10-07T20:32:57-04:00"), indent=2)
        tsl._append({"kind": "seal", "date": "2026-10-07", "path": tsl.CANONICAL,
                     "sha256": tsl.sha256_text(canon.read_text(encoding="utf-8"))}, manifest)
        canon.write_text(void, encoding="utf-8")
        tsl._append({"kind": "void", "date": "2026-10-07", "path": tsl.CANONICAL,
                     "sha256": tsl.sha256_text(void)}, manifest)
        for clock in (datetime(2026, 10, 8, 6, 30, tzinfo=ET), datetime(2026, 10, 8, 9, 7, tzinfo=ET)):
            tick = "BBB" if clock.hour == 6 else "CCC"
            _quiet(st.write, "2026-10-08",
                   _payload("2026-10-08", tick, clock.isoformat()), now=clock)
            for rel in ("fm/strategy_tickets.json", "day/strategy_tickets.json",
                        "dash/strategy_tickets.json", "day/today_strategies.json"):
                body = (tmp / rel).read_text(encoding="utf-8")
                assert tsl.session_of(body) == "2026-10-08", rel
                assert tick in body, rel
            assert st.write.last_lock is None
        assert "CCC" in (tmp / "day" / "2026-10-08_strategy_tickets.json").read_text()
        # 10-07 seal + void stay; no 10-08 seal before the open.
        assert [r["date"] for r in tsl.load_manifest(manifest)] == ["2026-10-07", "2026-10-07"]


def test_older_session_never_replaces_a_newer_copy() -> None:
    tmp = Path(tempfile.mkdtemp())
    manifest = tmp / "manifest.jsonl"
    with _sandbox(tmp, manifest):
        _quiet(st.write, "2026-10-08", _payload("2026-10-08", "BBB", "2026-10-08T07:00:00-04:00"),
               now=datetime(2026, 10, 8, 7, 0, tzinfo=ET))
        before = (tmp / "fm" / "strategy_tickets.json").read_text(encoding="utf-8")
        _quiet(st.write, "2026-10-07", _payload("2026-10-07", "OLD", "2026-10-08T07:05:00-04:00"),
               now=datetime(2026, 10, 8, 7, 5, tzinfo=ET))
        assert (tmp / "fm" / "strategy_tickets.json").read_text(encoding="utf-8") == before


def test_sealed_copy_changed_fails_closed() -> None:
    tmp = Path(tempfile.mkdtemp())
    manifest = tmp / "manifest.jsonl"
    d = "2026-10-09"
    with _sandbox(tmp, manifest):
        _quiet(st.write, d, _payload(d, "AAA", "2026-10-09T08:00:00-04:00"),
               now=datetime(2026, 10, 9, 8, 0, tzinfo=ET))
        _quiet(st.write, d, _payload(d, "AAA", "2026-10-09T08:00:00-04:00"),
               now=datetime(2026, 10, 9, 10, 0, tzinfo=ET))
        assert len(tsl.load_manifest(manifest)) == 1
        canon = tmp / "fm" / "strategy_tickets.json"
        canon.write_text(json.dumps(_payload(d, "EVIL", "2026-10-09T08:00:00-04:00")), encoding="utf-8")
        try:
            _quiet(st.write, d, _payload(d, "AAA", "2026-10-09T08:00:00-04:00"),
                   now=datetime(2026, 10, 9, 17, 0, tzinfo=ET))
        except tsl.TicketSessionLockError as exc:
            assert "changed after its seal" in str(exc)
        else:
            raise AssertionError("a changed sealed copy must fail the write")


def test_may_replace_rules() -> None:
    a7 = json.dumps(_payload("2026-10-07", "A", "x"))
    b7 = json.dumps(_payload("2026-10-07", "B", "x"))
    c8 = json.dumps(_payload("2026-10-08", "C", "x"))
    assert tsl.may_replace(None, a7, "2026-10-07", "after 09:30")[0]
    assert tsl.may_replace(a7, a7, "2026-10-07", "after 09:30")[0]
    assert tsl.may_replace(a7, b7, "2026-10-07", None)[0]
    assert not tsl.may_replace(a7, b7, "2026-10-07", "after 2026-10-07 09:30 ET")[0]
    assert not tsl.may_replace(a7, b7, "2026-10-07", "paper send journaled")[0]
    assert tsl.may_replace(a7, c8, "2026-10-08", None)[0]
    assert tsl.may_replace(a7, c8, "2026-10-08", "after 2026-10-08 09:30 ET")[0]
    assert not tsl.may_replace(c8, a7, "2026-10-07", "after 2026-10-07 09:30 ET")[0]


def test_ci_copy_change_rule() -> None:
    pre = json.dumps(_payload("2026-10-07", "A", "2026-10-07T06:27:09-04:00"))
    pre2 = json.dumps(_payload("2026-10-07", "B", "2026-10-07T09:07:00-04:00"))
    late = json.dumps(_payload("2026-10-07", "Z", "2026-10-07T20:32:57-04:00"))
    nxt = json.dumps(_payload("2026-10-08", "N", "2026-10-08T06:30:00-04:00"))
    tsl.assert_copy_change("x", pre, pre2)
    tsl.assert_copy_change("x", late, nxt)
    tsl.assert_copy_change("x", None, pre)
    for base, head in ((pre, late), (nxt, pre)):
        try:
            tsl.assert_copy_change("x", base, head)
        except tsl.TicketSessionLockError:
            pass
        else:
            raise AssertionError("must fail")


def test_manifest_is_append_only() -> None:
    a, b = '{"kind":"note"}', '{"kind":"note","n":2}'
    tsl.assert_manifest_prefix(a, a + "\n" + b)
    for base, head in ((a + "\n" + b, a), (a, b)):
        try:
            tsl.assert_manifest_prefix(base, head)
        except tsl.TicketSessionLockError:
            pass
        else:
            raise AssertionError("must fail")


def test_repo_manifest_documents_2026_10_07() -> None:
    rows = tsl.load_manifest()
    seals = [r for r in rows if r.get("kind") == "seal" and r.get("date") == "2026-10-07"]
    voids = [r for r in rows if r.get("kind") == "void" and r.get("date") == "2026-10-07"]
    assert len(seals) == 1
    seal = seals[0]
    assert seal["path"] == tsl.CANONICAL
    assert seal["commit"].startswith("907da2b3")
    # The pre-open copy is byte-identical to the sealed dated 10-07 file.
    dated = ROOT / "data" / "day_board" / "2026-10-07_strategy_tickets.json"
    assert tsl.sha256_text(dated.read_text(encoding="utf-8")) == seal["sha256"]
    assert any(str(v.get("commit") or "").startswith("ed32867f") for v in voids)
    tsl.check_manifest()


def test_publish_workflow_checks_and_pushes_the_lock() -> None:
    wf = (ROOT / ".github/workflows/publish_strategy_tickets.yml").read_text(encoding="utf-8")
    assert "python3 -m src.ticket_session_lock --check-against" in wf
    assert "data/ticket_session_lock/" in wf
    ci = (ROOT / ".github/workflows/ticket_session_lock.yml").read_text(encoding="utf-8")
    assert "python3 -m src.test_ticket_session_lock" in ci


def main() -> None:
    test_post_close_run_writes_only_a_marked_draft()
    test_next_morning_writes_the_next_session_before_0930()
    test_older_session_never_replaces_a_newer_copy()
    test_sealed_copy_changed_fails_closed()
    test_may_replace_rules()
    test_ci_copy_change_rule()
    test_manifest_is_append_only()
    test_repo_manifest_documents_2026_10_07()
    test_publish_workflow_checks_and_pushes_the_lock()
    print("test_ticket_session_lock: ok")


if __name__ == "__main__":
    main()
