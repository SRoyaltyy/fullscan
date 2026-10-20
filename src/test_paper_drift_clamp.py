"""h1 sealed sells use min(book, sandbox held). Run: python -m pytest -q src/test_paper_drift_clamp.py"""
import json
from src import paper_drift as pd_

D = "2026-10-20"


def _t(side, ticker, shares):
    return {"side": side, "ticker": ticker, "shares": shares, "date": D}


def test_peng_held_33_book_34_sends_33_and_notes_gap():
    send, skip, foreign, warn = pd_.partition_tickets(
        [_t("SELL", "PENG", 34)], {"PENG": {"shares": 33}}, D)
    assert skip == [] and foreign == []
    assert send[0]["shares"] == 33 and send[0]["book_shares"] == 34
    c = warn["sell_clamped"][0]
    assert (c["ticker"], c["book_shares"], c["held_shares"], c["sent_shares"], c["gap_shares"]) == ("PENG", 34, 33, 33, 1)


def test_sdev_not_held_is_skipped_not_sent():
    send, skip, _, warn = pd_.partition_tickets([_t("SELL", "SDEV", 1222)], {}, D)
    assert send == [] and skip[0]["ticker"] == "SDEV"
    assert skip[0]["status"] == pd_.DRIFT_SKIPPED
    assert "sell_clamped" not in warn


def test_held_more_than_book_sends_book_qty():
    send, skip, _, warn = pd_.partition_tickets(
        [_t("SELL", "NAUT", 1234)], {"NAUT": 1398}, D)
    assert send[0]["shares"] == 1234 and "book_shares" not in send[0]
    assert warn is None


def test_exact_match_unchanged_no_note():
    t = _t("SELL", "KOD", 27)
    send, skip, _, warn = pd_.partition_tickets([t], {"KOD": 27}, D)
    assert send == [t] and warn is None


def test_buys_are_never_resized():
    t = _t("BUY", "BYRN", 503)
    send, skip, _, warn = pd_.partition_tickets([t], {"BYRN": 10}, D)
    assert send == [t] and skip == []


def test_bad_book_qty_fails_closed():
    for bad in (0, -3, 2.5, None, "x", True):
        send, skip, _, _ = pd_.partition_tickets([_t("SELL", "PENG", bad)], {"PENG": 33}, D)
        assert send == [] and len(skip) == 1, bad


def test_other_day_ticket_still_refused():
    t = {**_t("SELL", "PENG", 34), "date": "2026-10-19"}
    send, skip, foreign, _ = pd_.partition_tickets([t], {"PENG": 33}, D)
    assert send == [] and foreign[0]["ticker"] == "PENG"


def test_unknown_positions_do_not_clamp():
    t = _t("SELL", "PENG", 34)
    send, _, _, warn = pd_.partition_tickets([t], {}, D, positions_known=False)
    assert send == [t] and warn is None


def test_note_is_append_only(tmp_path):
    p = tmp_path / "drift_log.jsonl"
    p.write_text('{"kind":"warning","date":"2026-10-06"}\n')
    _, _, _, warn = pd_.partition_tickets([_t("SELL", "PENG", 34)], {"PENG": 33}, D)
    pd_.append_drift(warn, p)
    lines = p.read_text().splitlines()
    assert lines[0] == '{"kind":"warning","date":"2026-10-06"}'
    assert json.loads(lines[1])["sell_clamped"][0]["gap_shares"] == 1
