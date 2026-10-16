import json
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from src import sleeve_live_card as slc

ET = ZoneInfo("America/New_York")
CARD = {
    "date": "2026-10-15", "policy": "flatten_robust",
    "tickets": [
        {"date": "2026-10-15", "clock": "16:00 ET", "side": "SELL",
         "ticker": "FANG", "shares": 25, "px": 197.51, "sleeve": "io_core",
         "reason": "io 3d done", "status": "plan"},
        {"date": "2026-10-15", "clock": "09:30 ET", "side": "BUY",
         "ticker": "ARQT", "shares": 187, "px": 26.41, "sleeve": "io_core",
         "reason": "x", "status": "plan"},
    ],
}
FROZEN = b'{"date": "2026-10-15", "frozen": true}'


def _t(hm, day="2026-10-15"):
    return datetime.fromisoformat(f"{day}T{hm}:00").replace(tzinfo=ET)


@pytest.fixture
def paths(tmp_path):
    t = tmp_path / "today.json"
    t.write_bytes(FROZEN)
    return t, tmp_path / "today_live.json"


@pytest.mark.parametrize("hm", ["03:00", "09:13", "09:20", "09:25", "09:39"])
def test_in_freeze_today_json_byte_identical_live_written(paths, hm):
    t, live = paths
    out = slc.write_today(CARD, now=_t(hm), today_path=t, live_path=live)
    assert t.read_bytes() == FROZEN
    assert "today" not in out
    assert json.loads(live.read_text()) == CARD


@pytest.mark.parametrize("hm", ["09:40", "12:00", "16:30", "02:59"])
def test_outside_freeze_both_files_same_bytes(paths, hm):
    t, live = paths
    out = slc.write_today(CARD, now=_t(hm), today_path=t, live_path=live)
    assert out["today"] == t
    assert t.read_bytes() == live.read_bytes()
    assert json.loads(t.read_text()) == CARD


def test_past_date_card_in_freeze_still_writes_today(paths):
    t, live = paths
    card = dict(CARD, date="2026-10-14")
    slc.write_today(card, now=_t("09:25"), today_path=t, live_path=live)
    assert json.loads(t.read_text())["date"] == "2026-10-14"


def test_winter_utc_clock():
    assert slc.today_json_frozen(
        "2026-11-16", datetime.fromisoformat("2026-11-16T14:25:00+00:00"))
    assert not slc.today_json_frozen(
        "2026-11-16", datetime.fromisoformat("2026-11-16T14:45:00+00:00"))


def test_reader_prefers_live_then_today(paths):
    t, live = paths
    assert slc.flatten_card_path(t, live) == t
    slc.write_today(CARD, now=_t("09:25"), today_path=t, live_path=live)
    assert slc.flatten_card_path(t, live) == live
    assert slc.load_flatten_card(t, live) == CARD


def _write_card(monkeypatch, tmp_path, hm):
    from src import sleeve_merge_live as sml
    from src import past_day_lock as pdl
    d = tmp_path / "sm"; d.mkdir(exist_ok=True)
    (d / "today.json").write_bytes(FROZEN)
    monkeypatch.setattr(sml, "TODAY_JSON", d / "today.json")
    monkeypatch.setattr(sml, "POSITIONS_JSON", d / "positions.json")
    monkeypatch.setattr(sml, "DAILY_DIR", tmp_path / "daily")
    monkeypatch.setattr(sml, "DASH_DIR", tmp_path / "dash")
    monkeypatch.setattr(sml, "OUT_DIR", d)
    monkeypatch.setattr(sml, "card_markdown", lambda c: "# card\n")
    monkeypatch.setattr(sml, "today_panel_html", lambda c: "<p>panel</p>")
    monkeypatch.setattr(sml, "positions_doc", lambda c: {"date": c["date"]})
    monkeypatch.setattr(pdl, "prepare_flatten_card", lambda dt, txt, p: txt)
    monkeypatch.setattr(pdl, "seal_flatten_card", lambda dt, p: None)
    monkeypatch.setattr(slc, "_now", lambda: _t(hm))
    return sml.write_card(dict(CARD)), d


def test_write_card_in_freeze(monkeypatch, tmp_path):
    out, d = _write_card(monkeypatch, tmp_path, "09:25")
    assert (d / "today.json").read_bytes() == FROZEN
    assert json.loads((d / "today_live.json").read_text()) == CARD
    assert "today" not in out and out["live"] == d / "today_live.json"
    assert (tmp_path / "daily" / "2026-10-15_flatten_card.md").is_file()
    assert (d / "positions.json").is_file()


def test_write_card_post_freeze_unchanged(monkeypatch, tmp_path):
    out, d = _write_card(monkeypatch, tmp_path, "09:45")
    assert json.loads((d / "today.json").read_text()) == CARD
    assert (d / "today.json").read_bytes() == (d / "today_live.json").read_bytes()
    assert out["today"] == d / "today.json"


def test_strategy_tickets_flatten_identical(monkeypatch, tmp_path):
    from src import strategy_tickets as st
    t = tmp_path / "today.json"
    # Before: card in today.json (old behaviour).
    t.write_text(json.dumps(CARD))
    monkeypatch.setattr(st, "SLEEVE", t)
    before = st.flatten_strat("2026-10-15")
    # After: in-freeze write, today.json holds stale frozen bytes.
    t.write_bytes(FROZEN)
    slc.write_today(CARD, now=_t("09:25"), today_path=t,
                    live_path=tmp_path / "today_live.json")
    after = st.flatten_strat("2026-10-15")
    assert before == after
    assert t.read_bytes() == FROZEN


def test_freeze_manifest_never_reads_live_file():
    from pathlib import Path
    root = Path(__file__).resolve().parent.parent
    for rel in ("research/input_freeze/tool/freeze_inputs.py",
                "research/input_freeze/tool/check_decision_freeze.py",
                "src/decision_ready.py"):
        p = root / rel
        if p.is_file():
            assert "today_live" not in p.read_text(encoding="utf-8"), rel
