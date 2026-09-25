"""Sequential state files: resume, no lookahead, ledger left alone."""
from __future__ import annotations

import json
import shutil
from pathlib import Path

from . import factor_mine as fm
from . import factor_mine_freeze as fmf
from . import factor_mine_sequential as seq

DATES = ["2026-08-13", "2026-08-14", "2026-08-17"]
FUTURE = "2026-12-01"


def _rec() -> dict:
    return {
        "name": "toy_h1",
        "universe": "union",
        "hold": 1,
        "side": "long",
        "top_n": 1,
        "rank": "hot_score",
        "size": "leftover",
        "sell": "list",
        "s_boost": "none",
        "forbid": {"alarm": True},
        "require": {},
    }


def _row(date: str, ticker: str, hot: float) -> dict:
    return {
        "date": date,
        "ticker": ticker,
        "sources": ["union"],
        "ohlc_hot_score": hot,
        "alarm": False,
        "rsi": 50.0,
        "flow_in": 0,
        "macd_hist": 0.1,
        "rsi_os": False,
        "ohlc_ret_1": 1.0,
        "_clock_b": True,
        "oppset": False,
    }


def _bars() -> dict:
    out = {}
    for date in DATES:
        out[("AAA", date)] = {"open": 10.0, "high": 11.0, "low": 9.0, "close": 12.0}
        out[("BBB", date)] = {"open": 8.0, "high": 9.0, "low": 7.0, "close": 9.0}
    out[("AAA", FUTURE)] = {"open": 1.0, "high": 1.0, "low": 1.0, "close": 500.0}
    return out


def _rows_for(date: str) -> list[dict]:
    ticker = "BBB" if date == DATES[-1] else "AAA"
    return [_row(date, ticker, 5.0)]


def _regime() -> dict:
    return {d: {"predict_score": 1.0} for d in DATES}


def _walk(root: Path, bars: dict | None = None, snap: Path | None = None) -> dict[str, bytes]:
    store = bars if bars is not None else _bars()

    def rows_for(date: str):
        if snap is not None:
            return seq.rows_for_day(date, snap)
        return _rows_for(date)

    def bars_for(_date: str):
        return store

    seq.walk(
        DATES, [_rec()], rows_for=rows_for, bars_for=bars_for,
        root=root, persist=True, fees=fm.pt_fees(), regime=_regime(),
    )
    name = _rec()["name"]
    return {
        date: (root / name / f"{date}.json").read_bytes()
        for date in DATES
    }


def test_rewrite_is_refused(tmp_path: Path) -> None:
    root = tmp_path / "state"
    seq.write_state("toy_h1", DATES[0], {"date": DATES[0], "cash": 1}, root)
    seq.write_state("toy_h1", DATES[0], {"date": DATES[0], "cash": 1}, root)
    try:
        seq.write_state("toy_h1", DATES[0], {"date": DATES[0], "cash": 2}, root)
        raised = False
    except fmf.FrozenHistory:
        raised = True
    assert raised
    body = json.loads(seq.state_path("toy_h1", DATES[0], root).read_text())
    assert body["cash"] == 1


def test_resume_from_saved_day_is_byte_identical(tmp_path: Path) -> None:
    full = _walk(tmp_path / "full")
    name = _rec()["name"]
    resumed_root = tmp_path / "resumed"
    src = tmp_path / "full" / name / f"{DATES[0]}.json"
    dest = resumed_root / name / f"{DATES[0]}.json"
    dest.parent.mkdir(parents=True)
    shutil.copy(src, dest)
    resumed = _walk(resumed_root)
    assert resumed == full
    assert full[DATES[0]] == full[DATES[0]]
    day2 = json.loads(full[DATES[1]])
    day3 = json.loads(full[DATES[-1]])
    assert day2["holdings"] == ["AAA"]
    assert day2["mean"] == 0.0
    assert "BBB" in day3["buys"]
    assert "AAA" in day3["sells"]


def test_future_price_and_file_do_not_change_earlier_days(tmp_path: Path) -> None:
    clean_bars = {
        key: bar for key, bar in _bars().items() if key[1] != FUTURE
    }
    without = _walk(tmp_path / "without", bars=clean_bars)
    with_future = _walk(tmp_path / "with", bars=_bars())
    assert with_future == without

    snap_a = tmp_path / "snap_a"
    snap_b = tmp_path / "snap_b"
    for folder in (snap_a, snap_b):
        folder.mkdir()
        for date in DATES:
            doc = {"date": date, "rows": _rows_for(date)}
            (folder / f"{date}.json").write_text(json.dumps(doc))
    (snap_b / f"{FUTURE}.json").write_text(json.dumps({
        "date": FUTURE,
        "rows": [_row(FUTURE, "ZZZ", 99.0)],
    }))
    from_a = _walk(tmp_path / "a", bars=clean_bars, snap=snap_a)
    from_b = _walk(tmp_path / "b", bars=clean_bars, snap=snap_b)
    assert from_a == from_b
    assert seq.rows_for_day(DATES[0], snap_b)[0]["ticker"] == "AAA"
    assert seq.bars_through(_bars(), DATES[-1]).keys().isdisjoint({("AAA", FUTURE)})


def test_walk_does_not_rewrite_ledgers(tmp_path: Path) -> None:
    before = seq.ledger_fingerprint()
    _walk(tmp_path / "books")
    after = seq.ledger_fingerprint()
    assert after == before


def _main() -> None:
    import tempfile
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        test_rewrite_is_refused(root / "rewrite")
        test_resume_from_saved_day_is_byte_identical(root / "resume")
        test_future_price_and_file_do_not_change_earlier_days(root / "future")
        test_walk_does_not_rewrite_ledgers(root / "ledgers")
    print("factor-mine sequential tests passed")


if __name__ == "__main__":
    _main()
