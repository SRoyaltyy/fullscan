"""Held/new calendar prefers the rebuilt book on sealed empty mornings.

The scoreboard daily and books stay the sealed rows. The paint reads
``rebuilt_calendar.json`` and only replaces a day the send file marked
``no_same_day_panel`` when that sealed day is empty.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.factor_mine_dash_pack import (
    calendar_overlay_document,
    sealed_calendar_empty,
    write_calendar_overlay,
)

JS = ROOT / "scripts" / "fm_ovcal.js"
NODE = "node"


def _bars(prices: dict):
    def lookup(ticker, date):
        pair = (prices.get(ticker) or {}).get(date)
        if not pair:
            return {}
        return {"open": pair[0], "close": pair[1]}
    return lookup


def _paint(payload: dict) -> dict:
    fixture = ROOT / "src" / "_calendar_paint_fixture.json"
    # The fixture is a temp file next to the test so node can read it.
    # Callers pass a path via payload["_path"] when they already wrote one.
    path = Path(payload.pop("_path"))
    script = r"""
const fs = require('fs');
const vm = require('vm');
const src = fs.readFileSync(process.argv[1], 'utf8');
const fixture = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
const context = {
  D: fixture.D,
  window: {},
  console: console,
  sleeveFilter: fixture.name,
  esc: function(s){ return String(s == null ? '' : s); },
};
context.window = context;
vm.createContext(context);
vm.runInContext(src, context);
context.window.__FM_CAL_DOC = fixture.overlay;
const name = fixture.name;
const sealed = fixture.sealed_date;
const original = (context.D.daily[name] || []).find(function(d){ return d.date === sealed; });
const before = JSON.stringify(original);
const booksBefore = JSON.stringify(context.D.books);
const dailyBefore = JSON.stringify(context.D.daily);
const pack = context.ovDaysAndTrades(name);
const kept = pack.days.find(function(d){ return d.date === sealed; });
const rows = context.ovSplitDays(name);
const html = context.ovCalHtml(name, 'big');
if (!original || kept !== original) {
  throw new Error('sealed non-empty day was replaced');
}
if (JSON.stringify(original) !== before) {
  throw new Error('sealed non-empty day bytes changed');
}
process.stdout.write(JSON.stringify({
  rows: rows,
  html: html,
  same_book: JSON.stringify(context.D.books) === booksBefore,
  same_daily: JSON.stringify(context.D.daily) === dailyBefore,
}));
"""
    proc = subprocess.run(
        [NODE, "-e", script, str(JS), str(path)],
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise AssertionError(proc.stderr or proc.stdout)
    return json.loads(proc.stdout)


def _write_fixture(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_overlay_keeps_sits_and_misses_free_of_picks(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    send = root / "data" / "factor_mine" / "send_inputs"
    rebuilt = root / "data" / "factor_mine" / "rebuilt"
    send.mkdir(parents=True)
    rebuilt.mkdir(parents=True)
    (send / "2026-10-14.json").write_text(
        json.dumps({"date": "2026-10-14", "source": "no_same_day_panel"}),
        encoding="utf-8",
    )
    (send / "2026-10-15.json").write_text(
        json.dumps({"date": "2026-10-15", "source": "panel"}),
        encoding="utf-8",
    )
    (rebuilt / "2026-10-14.json").write_text(json.dumps({
        "date": "2026-10-14",
        "empty_input": True,
        "sealed_source": "no_same_day_panel",
        "chain_from": "2026-10-13",
        "strategies": {
            "hot": {
                "class": "rebuilt",
                "daily": {
                    "date": "2026-10-14",
                    "open_held": ["ZZ×10"],
                    "bought": ["AAA"],
                    "sold": [],
                    "equity": 110.0,
                    "open_equity": 100.0,
                    "overnight_delta": 4.0,
                    "session_delta": 6.0,
                    "s": 1.0,
                    "hard_red": False,
                },
                "fills": [
                    {"date": "2026-10-14", "side": "BUY", "ticker": "AAA",
                     "shares": 2, "price": 5, "fees": 0.1, "reason": "drop me"},
                    {"date": "2026-10-14", "side": "SELL", "ticker": "ZZ",
                     "shares": 1, "price": 9},
                ],
            },
            "sitter": {
                "class": "sat out (rule)",
                "daily": {
                    "date": "2026-10-14",
                    "open_held": ["ZZ×1"],
                    "bought": ["FAKE"],
                    "overnight_delta": -2.0,
                    "session_delta": -3.0,
                    "s": -4,
                    "hard_red": True,
                },
                "fills": [
                    {"date": "2026-10-14", "side": "BUY", "ticker": "FAKE",
                     "shares": 9, "price": 1},
                ],
            },
            "broken": {
                "class": "still broken",
                "daily": {"date": "2026-10-14", "bought": ["INVENTED"], "open_held": ["ZZ×1"]},
                "fills": [
                    {"date": "2026-10-14", "side": "BUY", "ticker": "INVENTED",
                     "shares": 1, "price": 1},
                ],
                "picks": ["INVENTED"],
            },
        },
    }), encoding="utf-8")
    (rebuilt / "2026-10-15.json").write_text(json.dumps({
        "date": "2026-10-15",
        "strategies": {"hot": {"class": "rebuilt", "daily": {"bought": ["NO"]}, "fills": []}},
    }), encoding="utf-8")
    ledger = root / "data" / "factor_mine" / "ledgers" / "2026-10-14.json"
    ledger.parent.mkdir(parents=True)
    ledger.write_text('{"sealed": true}\n', encoding="utf-8")
    state = root / "data" / "factor_mine" / "state" / "hot" / "2026-10-14.json"
    state.parent.mkdir(parents=True)
    state.write_text('{"equity": 1}\n', encoding="utf-8")
    before_ledger = ledger.read_bytes()
    before_state = state.read_bytes()
    doc = calendar_overlay_document(root, bars=_bars({
        "ZZ": {"2026-10-13": [1, 1], "2026-10-14": [2, 3]},
        "AAA": {"2026-10-14": [5, 6]},
    }))
    assert doc["preference"] == "no_same_day_panel"
    assert doc["empty_dates"] == ["2026-10-14"]
    assert "2026-10-15" not in doc["strategies"].get("hot", {})
    hot = doc["strategies"]["hot"]["2026-10-14"]
    assert hot["label"] == "rebuilt"
    assert [row["ticker"] for row in hot["fills"]] == ["AAA"]
    assert "reason" not in hot["fills"][0]
    sit = doc["strategies"]["sitter"]["2026-10-14"]
    assert sit["label"] == "sat out (rule)"
    assert sit["fills"] == []
    assert sit["daily"]["bought"] == []
    assert sit["daily"]["open_held"] == ["ZZ×1"]
    missed = doc["strategies"]["broken"]["2026-10-14"]
    assert missed["label"] == "missed"
    assert missed["fills"] == []
    assert missed["daily"] == {"date": "2026-10-14"}
    assert "INVENTED" not in json.dumps(missed)
    assert doc["tape"]["AAA"]["2026-10-14"] == [5, 6]
    dest = tmp_path / "dash"
    written = write_calendar_overlay(dest, root, bars=_bars({}))
    assert written.name == "rebuilt_calendar.json"
    assert ledger.read_bytes() == before_ledger
    assert state.read_bytes() == before_state
    assert sealed_calendar_empty(
        {"overnight_delta": 0, "session_delta": 0, "bought": [], "sold": [], "open_held": []},
    )
    assert not sealed_calendar_empty(
        {"overnight_delta": 1, "session_delta": 0, "bought": [], "sold": [], "open_held": ["ZZ×1"]},
    )


def test_paint_uses_rebuilt_book_and_leaves_sealed_days() -> None:
    sleeve = [
        {
            "date": "2026-10-13",
            "overnight_delta": 5, "session_delta": 1,
            "bought": ["KEEP"], "sold": [], "open_held": ["ZZ×2"],
            "equity": 10010, "open_equity": 10005, "s": 0.2, "hard_red": False,
        },
        {
            "date": "2026-10-14",
            "overnight_delta": 0, "session_delta": 0,
            "bought": [], "sold": [], "open_held": [],
            "equity": 10010, "open_equity": 10010, "s": 0, "hard_red": False,
        },
        {
            "date": "2026-10-15",
            "overnight_delta": 0, "session_delta": 0,
            "bought": [], "sold": [], "open_held": [],
            "equity": 10010, "open_equity": 10010, "s": -4, "hard_red": True,
        },
        {
            "date": "2026-10-16",
            "overnight_delta": 8, "session_delta": 2,
            "bought": ["KEEP"], "sold": [], "open_held": ["ZZ×2"],
            "equity": 10020, "open_equity": 10012, "s": 0.4, "hard_red": False,
        },
        {
            "date": "2026-11-05",
            "overnight_delta": 0, "session_delta": 0,
            "bought": [], "sold": [], "open_held": [],
            "equity": 10020, "open_equity": 10020, "s": 0, "hard_red": False,
        },
    ]
    payload = {
        "name": "sleeve",
        "sealed_date": "2026-10-16",
        "D": {
            "daily": {"sleeve": sleeve},
            "books": {"sleeve": {"trades": [
                {"date": "2026-10-13", "side": "BUY", "ticker": "KEEP",
                 "shares": 1, "price": 3, "fees": 0},
                {"date": "2026-10-16", "side": "BUY", "ticker": "KEEP",
                 "shares": 1, "price": 4, "fees": 0},
            ]}},
            "sim": {"tape": {}},
            "dates": [row["date"] for row in sleeve],
        },
        "overlay": {
            "preference": "no_same_day_panel",
            "empty_dates": ["2026-10-14", "2026-10-15", "2026-11-05"],
            "strategies": {"sleeve": {
                "2026-09-02": {
                    "label": "rebuilt",
                    "daily": {"date": "2026-09-02", "bought": ["NOPE"], "open_held": [],
                              "overnight_delta": 9, "session_delta": 9,
                              "equity": 1, "open_equity": 1},
                    "fills": [{"date": "2026-09-02", "side": "BUY", "ticker": "NOPE",
                               "shares": 1, "price": 1}],
                },
                "2026-10-14": {
                    "label": "rebuilt",
                    "daily": {
                        "date": "2026-10-14",
                        "open_held": ["ZZ×10"],
                        "bought": ["AAA"],
                        "sold": [],
                        "equity": 10080,
                        "open_equity": 10040,
                        "overnight_delta": 40,
                        "session_delta": 40,
                        "s": 1,
                        "hard_red": False,
                    },
                    "fills": [{"date": "2026-10-14", "side": "BUY", "ticker": "AAA",
                               "shares": 10, "price": 10, "fees": 0}],
                },
                "2026-10-15": {
                    "label": "sat out (rule)",
                    "daily": {
                        "date": "2026-10-15",
                        "open_held": ["ZZ×10"],
                        "bought": ["FAKE"],
                        "sold": [],
                        "equity": 10100,
                        "open_equity": 10050,
                        "overnight_delta": 20,
                        "session_delta": 30,
                        "s": -4,
                        "hard_red": True,
                    },
                    "fills": [{"date": "2026-10-15", "side": "BUY", "ticker": "FAKE",
                               "shares": 5, "price": 1}],
                },
                "2026-10-16": {
                    "label": "rebuilt",
                    "daily": {
                        "date": "2026-10-16",
                        "bought": ["NOPE"],
                        "open_held": [],
                        "overnight_delta": 50,
                        "session_delta": 50,
                        "equity": 9,
                        "open_equity": 1,
                    },
                    "fills": [{"date": "2026-10-16", "side": "BUY", "ticker": "NOPE",
                               "shares": 4, "price": 1}],
                },
                "2026-11-05": {
                    "label": "missed",
                    "daily": {"date": "2026-11-05", "bought": ["INVENTED"]},
                    "fills": [{"date": "2026-11-05", "side": "BUY", "ticker": "INVENTED",
                               "shares": 1, "price": 1}],
                },
            }},
            "tape": {
                "ZZ": {
                    "2026-10-13": [10, 10],
                    "2026-10-14": [14, 15],
                    "2026-10-15": [16, 18],
                },
                "AAA": {"2026-10-14": [10, 12]},
            },
        },
    }
    path = ROOT / "src" / "_calendar_paint_fixture.json"
    try:
        _write_fixture(path, payload)
        painted = _paint({"_path": str(path), "D": payload["D"]})
    finally:
        path.unlink(missing_ok=True)
    by = {row["date"]: row for row in painted["rows"]}
    rebuilt = by["2026-10-14"]
    assert abs(rebuilt["held"]) >= 0.5 and abs(rebuilt["neu"]) >= 0.5
    assert rebuilt["calendar_label"] == "rebuilt"
    assert [lot["ticker"] for lot in rebuilt["buyLots"]] == ["AAA"]
    sit = by["2026-10-15"]
    assert sit["calendar_label"] == "sat out (rule)"
    assert sit["buyLots"] == []
    assert "FAKE" not in json.dumps(sit["buyLots"])
    assert abs(sit["held"]) >= 0.5
    missed = by["2026-11-05"]
    assert missed["calendar_label"] == "missed"
    assert missed["buyLots"] == []
    assert missed["heldLots"] == []
    html = painted["html"]
    assert ">sit<" in html
    assert ">missed<" in html
    assert "INVENTED" not in html
    assert "FAKE" not in html
    kept = by["2026-10-16"]
    assert kept["calendar_label"] == ""
    assert "NOPE" not in json.dumps(kept)
    assert painted["same_book"] is True
    assert painted["same_daily"] is True


def _flat(row: dict) -> bool:
    if row.get("calendar_label") in ("sat out (rule)", "missed"):
        return False
    held = row.get("held")
    neu = row.get("neu")
    return abs(float(held or 0)) < 0.5 and abs(float(neu or 0)) < 0.5


def test_holdup_calendar_leaves_both_halves_zero() -> None:
    """Real union_hot_n4_holdup book: empty no_same_day_panel days leave 0/0."""
    name = "union_hot_n4_holdup"
    with gzip.open(ROOT / "03_scoreboard/factor_mine/shards/daily.json.gz", "rt") as handle:
        daily = json.load(handle)[name]
    with gzip.open(ROOT / "03_scoreboard/factor_mine/shards/books.json.gz", "rt") as handle:
        book = json.load(handle)[name]
    paper = ROOT / "src" / "paper_trade.py"
    paper_before = hashlib.sha256(paper.read_bytes()).hexdigest()
    send = ROOT / "data/factor_mine/send_inputs/2026-10-14.json"
    send_before = send.read_bytes()
    doc = calendar_overlay_document(ROOT)
    assert send.read_bytes() == send_before
    assert hashlib.sha256(paper.read_bytes()).hexdigest() == paper_before
    payload = {
        "name": name,
        "sealed_date": "2026-09-28",
        "D": {
            "daily": {name: daily},
            "books": {name: {"trades": book.get("trades") or []}},
            "sim": {"tape": {}},
            "dates": [row["date"] for row in daily],
        },
        "overlay": {
            "preference": doc["preference"],
            "empty_dates": [],
            "strategies": {},
            "tape": {},
        },
    }
    bare_path = ROOT / "src" / "_calendar_paint_bare.json"
    full_path = ROOT / "src" / "_calendar_paint_full.json"
    try:
        _write_fixture(bare_path, payload)
        bare = _paint({"_path": str(bare_path)})
        payload["overlay"] = {
            "preference": doc["preference"],
            "empty_dates": doc["empty_dates"],
            "strategies": {name: doc["strategies"][name]},
            "tape": doc["tape"],
        }
        _write_fixture(full_path, payload)
        full = _paint({"_path": str(full_path)})
    finally:
        bare_path.unlink(missing_ok=True)
        full_path.unlink(missing_ok=True)
    assert full["same_book"] is True
    assert full["same_daily"] is True
    before = {row["date"]: row for row in bare["rows"]}
    after = {row["date"]: row for row in full["rows"]}
    changed = []
    for date, row in before.items():
        if not _flat(row):
            continue
        nxt = after[date]
        if _flat(nxt):
            continue
        changed.append((date, nxt.get("calendar_label") or "rebuilt",
                        round(float(nxt.get("held") or 0), 2),
                        round(float(nxt.get("neu") or 0), 2)))
    labels = {date: label for date, label, _held, _neu in changed}
    # 9 rebuilt with a real held/new split, 5 rule sits, 1 missed.
    assert labels == {
        "2026-09-30": "rebuilt",
        "2026-10-01": "rebuilt",
        "2026-10-02": "rebuilt",
        "2026-10-05": "rebuilt",
        "2026-10-06": "rebuilt",
        "2026-10-07": "sat out (rule)",
        "2026-10-08": "sat out (rule)",
        "2026-10-14": "rebuilt",
        "2026-10-19": "rebuilt",
        "2026-10-20": "sat out (rule)",
        "2026-10-21": "rebuilt",
        "2026-10-22": "sat out (rule)",
        "2026-10-23": "sat out (rule)",
        "2026-10-27": "rebuilt",
        "2026-11-05": "missed",
    }
    active = after["2026-10-14"]
    assert abs(active["held"]) >= 0.5 and abs(active["neu"]) >= 0.5
    assert after["2026-10-07"]["buyLots"] == []
    assert "INVENTED" not in json.dumps(after["2026-11-05"])
    # Chained days that are not no_same_day_panel stay the sealed flat cell.
    assert _flat(after["2026-09-02"])
    assert _flat(after["2026-10-26"])


if __name__ == "__main__":
    import tempfile
    with tempfile.TemporaryDirectory() as folder:
        test_overlay_keeps_sits_and_misses_free_of_picks(Path(folder))
    test_paint_uses_rebuilt_book_and_leaves_sealed_days()
    test_holdup_calendar_leaves_both_halves_zero()
    print("factor-mine calendar overlay tests passed")
