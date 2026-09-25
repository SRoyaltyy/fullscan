"""OOS-0914 guards: no future read, resume bytes, append refusal."""
from __future__ import annotations

import json
from pathlib import Path

from src import factor_mine as fm
from src import factor_mine_oos0914 as oos
from src import factor_mine_rules as fmr
from src import factor_mine_sequential as seq


def test_preregister_stays_small() -> None:
    doc = oos.load_preregister()
    assert doc["cutoff"] == "2026-09-14"
    assert doc["designed_after"] == "2026-09-14"
    assert doc["selection"]["random4"]["seed"] == 20260813
    assert doc["selection"]["random4"]["draws"] == 1000
    cands = oos.expand_candidates(doc)
    assert len(cands) <= 50
    assert len(cands) == 40
    assert cands[0]["id"] == "lg_hot_h1_sx"
    assert cands[-1]["id"] == "zero_candle_h2_s8"
    assert cands[1]["stop_pct"] == 0.08


def test_no_future_read(tmp_path: Path) -> None:
    folder = tmp_path / "snapshots"
    folder.mkdir(parents=True)
    (folder / "2026-09-11.json").write_text(json.dumps({
        "date": "2026-09-11",
        "rows": [{"date": "2026-09-11", "ticker": "AAA", "ohlc_hot_score": 1}],
    }), encoding="utf-8")
    future = folder / "2026-09-14.json"
    future.write_text(json.dumps({
        "date": "2026-09-14",
        "canary": "FUTURE",
        "rows": [{"date": "2026-09-14", "ticker": "ZZZ"}],
    }), encoding="utf-8")
    opened: list[str] = []

    def reader(path: Path) -> str:
        opened.append(path.name)
        if path.name.startswith("2026-09-14"):
            raise AssertionError("opened a file dated on or after the cutoff")
        return path.read_text(encoding="utf-8")

    found = oos.load_snapshot_dir(
        folder, start="2026-08-13", end="2026-09-11", read_text=reader,
    )
    assert opened == ["2026-09-11.json"]
    assert list(found) == ["2026-09-11"]
    assert "ZZZ" not in json.dumps(found)
    assert "FUTURE" not in json.dumps(found)

    store = tmp_path / "prices"
    store.mkdir(parents=True)
    (store / "meta.json").write_text(
        json.dumps({"last_date": "2026-09-14"}), encoding="utf-8")
    parquet = store / "ohlc.parquet"
    parquet.write_bytes(b"not-read")
    try:
        oos.load_session_bars(
            parquet, ["2026-09-11"], {"AAA"}, max_date="2026-09-11",
            allow_test=False,
        )
        raised = False
    except oos.FutureLeak:
        raised = True
    assert raised
    assert parquet.read_bytes() == b"not-read"


def _toy():
    dates = ["2026-08-13", "2026-08-14"]
    rec = fm.make_recipe(
        "toy_oos", hold=1, top_n=1, rank="hot_score", sell="time",
    )

    def rows_for(date: str):
        ticker = "BBB" if date == dates[-1] else "AAA"
        return [{
            "date": date, "ticker": ticker, "sources": ["union"],
            "ohlc_hot_score": 3.0, "alarm": False, "e_pol": False,
            "rsi": 40.0,
        }]

    bars = {}
    for date in dates:
        bars[("AAA", date)] = {"open": 10.0, "high": 11.0, "low": 9.5, "close": 10.5}
        bars[("BBB", date)] = {"open": 8.0, "high": 8.5, "low": 7.5, "close": 8.2}
    return dates, rec, rows_for, bars


def test_resume_byte_identical(tmp_path: Path) -> None:
    dates, rec, rows_for, bars = _toy()
    full_root = tmp_path / "full"
    history: dict[str, list] = {}
    seq.walk(
        dates, [rec], rows_for=rows_for, bars_for=lambda _d: bars,
        root=full_root, persist=True, fees=fm.pt_fees(), regime={},
        history=history,
    )
    full_bytes = {
        date: (full_root / rec["name"] / f"{date}.json").read_bytes()
        for date in dates
    }
    resumed = tmp_path / "resumed"
    src = full_root / rec["name"] / f"{dates[0]}.json"
    dest = resumed / rec["name"] / f"{dates[0]}.json"
    dest.parent.mkdir(parents=True)
    dest.write_bytes(src.read_bytes())
    seq.walk(
        dates, [rec], rows_for=rows_for, bars_for=lambda _d: bars,
        root=resumed, persist=True, fees=fm.pt_fees(), regime={},
    )
    for date in dates:
        assert (resumed / rec["name"] / f"{date}.json").read_bytes() == full_bytes[date]


def test_append_refusal(tmp_path: Path) -> None:
    dates, rec, rows_for, bars = _toy()
    root = tmp_path / "state"
    seq.walk(
        dates[:1], [rec], rows_for=rows_for, bars_for=lambda _d: bars,
        root=root, persist=True, fees=fm.pt_fees(), regime={},
    )
    state = seq.read_state(rec["name"], dates[0], root)
    doc = {
        "date": dates[0],
        "recipes": {
            rec["name"]: {
                "buys": state.get("buys") or [],
                "sells": state.get("sells") or [],
                "trades": state.get("fills") or [],
                "equity": state.get("equity"),
                "mean": state.get("mean"),
            },
        },
    }
    path = tmp_path / "ledgers" / f"{dates[0]}.json"
    oos.write_oos_ledger(dates[0], doc, path)
    first = path.read_bytes()
    oos.write_oos_ledger(dates[0], json.loads(json.dumps(doc)), path)
    assert path.read_bytes() == first
    changed = json.loads(json.dumps(doc))
    changed["recipes"][rec["name"]]["buys"] = [
        {"ticker": "ZZZ", "side": "BUY", "shares": 1, "price": 1.0},
    ]
    try:
        oos.write_oos_ledger(dates[0], changed, path)
        raised = False
    except fmr.AppendDrift:
        raised = True
    assert raised
    assert path.read_bytes() == first
    try:
        seq.write_state(rec["name"], dates[0], {"date": dates[0], "cash": 1}, root)
        state_raised = False
    except Exception:
        state_raised = True
    assert state_raised


def main() -> None:
    import tempfile
    test_preregister_stays_small()
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        test_no_future_read(root / "future")
        test_resume_byte_identical(root / "resume")
        test_append_refusal(root / "append")
    print("oos0914 tests passed")


if __name__ == "__main__":
    main()
