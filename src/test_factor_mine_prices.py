"""Factor-mine OPEN/CLOSE marks must be official regular-session prints."""
from __future__ import annotations

import json
from pathlib import Path

from src import ticker_lookback as tl

ROOT = Path(__file__).resolve().parent.parent
FM_JSON = ROOT / "03_scoreboard" / "factor_mine.json"

# Yahoo regular-session prints (auto_adjust=False), 2026-09-04 / 2026-09-08.
OFFICIAL = {
    ("CABA", "2026-09-04"): (3.46, 3.47, 3.60),
    ("CABA", "2026-09-08"): (3.43, 3.27, 3.48),
    ("ATRC", "2026-09-04"): (52.03, 51.52, 52.53),
    ("ATRC", "2026-09-08"): (54.31, 53.73, 55.72),
    ("HRMY", "2026-09-04"): (41.50, 42.25, 42.34),
    ("HRMY", "2026-09-08"): (42.20, 42.07, 43.25),
}


def _close_enough(got, want, tol=0.02) -> bool:
    if got is None or want is None:
        return False
    return abs(float(got) - float(want)) <= tol


def test_session_bar_matches_official_held_names() -> None:
    tl.reset_price_caches()
    for (t, d), (op, cl, hi) in OFFICIAL.items():
        bar = tl.session_bar(t, d)
        assert _close_enough(bar["open"], op), (t, d, "open", bar, op)
        assert _close_enough(bar["close"], cl), (t, d, "close", bar, cl)
        if bar.get("high") is not None:
            assert float(bar["high"]) <= float(hi) + 0.02, (t, d, bar, hi)
    caba8 = tl.session_bar("CABA", "2026-09-08")
    assert caba8["close"] != 3.89
    assert caba8["high"] is None or caba8["high"] <= 3.50
    hol = tl.session_bar("CABA", "2026-09-07")
    assert hol["open"] is None and hol["close"] is None


def test_rebuilt_factor_mine_books_use_official_marks() -> None:
    assert FM_JSON.is_file(), "factor_mine.json missing — run python -m src.factor_mine --write"
    payload = json.loads(FM_JSON.read_text(encoding="utf-8"))
    dates = payload.get("dates") or []
    assert "2026-09-07" not in dates, dates[-6:]
    assert "2026-09-04" in dates and "2026-09-08" in dates
    daily = payload.get("daily") or {}
    books = payload.get("books") or {}
    assert daily or books, "no book payload"

    caba_closes = []

    def walk(obj):
        if isinstance(obj, dict):
            if str(obj.get("ticker") or "").upper() == "CABA":
                if obj.get("close_px") is not None:
                    caba_closes.append((obj.get("date"), obj.get("close_px"), obj.get("open_px")))
            for v in obj.values():
                walk(v)
        elif isinstance(obj, list):
            for v in obj:
                walk(v)

    walk(daily)
    walk(books)
    assert caba_closes, "no CABA marks in rebuilt books"
    for date, close_px, open_px in caba_closes:
        assert close_px != 3.89, (date, close_px, open_px)
        if date == "2026-09-08":
            assert _close_enough(close_px, 3.27), (date, close_px, open_px)
            if open_px is not None:
                assert _close_enough(open_px, 3.43), (date, open_px)
        if date == "2026-09-04":
            if open_px is not None:
                assert _close_enough(open_px, 3.46), (date, open_px)
            if close_px is not None:
                assert _close_enough(close_px, 3.47), (date, close_px)
        if date == "2026-09-07":
            raise AssertionError("Labor Day CABA mark in book")


if __name__ == "__main__":
    test_session_bar_matches_official_held_names()
    print("session_bar official prints ok")
    if FM_JSON.is_file():
        test_rebuilt_factor_mine_books_use_official_marks()
        print("rebuilt factor-mine books ok")
    else:
        print("skip book audit — factor_mine.json not rebuilt yet")
