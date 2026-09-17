"""Clock math + baker for the day-movers dashboard."""
from __future__ import annotations

import json
from pathlib import Path

from src import day_movers as dm
from src.hard_red_exceptions import open_camera_setup


ROOT = Path(__file__).resolve().parent.parent


def _row(date, open_, close, **kw):
    rec = {
        "date": date, "open": open_, "close": close,
        "s": kw.pop("s", 1.0), "hard_red": kw.pop("hard_red", False),
        "n_pos": kw.pop("n_pos", 2), "n_neg": kw.pop("n_neg", 1),
        "idio": kw.pop("idio", 1), "sources": kw.pop("sources", ["panel"]),
        "h1": kw.pop("h1", None), "h3": None, "h5": None,
    }
    rec.update(kw)
    return rec


def test_oc_pct_is_open_to_close():
    assert dm.oc_pct(10, 11) == 10.0
    assert dm.oc_pct(10, 9) == -10.0
    assert dm.oc_pct(0, 11) is None
    assert dm.oc_pct(None, 11) is None


def test_gap_pct_is_prior_close_to_open():
    assert dm.gap_pct(10, 12) == 20.0
    assert dm.gap_pct(10, 8) == -20.0
    assert dm.gap_pct(0, 12) is None
    assert dm.gap_pct(10, None) is None


def test_interday_prefers_gap_never_day_pct():
    # Same-day o→c is −50%; gap is +20%. Interday must be the gap.
    assert dm.oc_pct(12, 6) == -50.0
    pct, kind = dm.interday_move(10, 12, 6)
    assert kind == "gap"
    assert pct == 20.0
    assert pct != dm.oc_pct(12, 6)


def test_interday_fallback_is_prior_close_to_close():
    pct, kind = dm.interday_move(10, None, 13)
    assert kind == "cc"
    assert pct == 30.0
    # Still not o→c (which cannot be computed without an open).
    assert dm.oc_pct(None, 13) is None


def test_interday_missing_prior_is_none():
    assert dm.interday_move(None, 12, 13) == (None, None)


def test_interday_skips_split_incompatible_prior_close():
    # BYND-style 1-for-30 reverse split leftover prior print.
    assert dm.px_comparable(0.41, 12.45) is False
    assert dm.interday_move(0.41, 12.45, 12.21) == (None, None)
    assert dm.px_comparable(13.61, 19.61) is True


def test_rank_side_splits_gainers_and_losers():
    rows = [
        {"t": "AAA", "pct": 5.0},
        {"t": "BBB", "pct": 12.0},
        {"t": "CCC", "pct": -8.0},
        {"t": "DDD", "pct": None},
    ]
    g = dm.rank_side(rows, side="gainers", n=2)
    lo = dm.rank_side(rows, side="losers", n=2)
    assert [r["t"] for r in g] == ["BBB", "AAA"]
    assert [r["t"] for r in lo] == ["CCC", "AAA"]


def test_pack_clocks_differ_on_same_session():
    """AAA gaps +50% then fades; BBB is flat overnight then rips o→c."""
    cal = ["2026-08-12", "2026-08-13"]
    histories = [
        ("AAA", [
            _row("2026-08-12", 9.0, 10.0),
            _row("2026-08-13", 15.0, 11.0),
        ]),
        ("BBB", [
            _row("2026-08-12", 10.0, 10.0),
            _row("2026-08-13", 10.5, 20.0),
        ]),
        ("CCC", [
            _row("2026-08-12", 8.0, 8.0),
            _row("2026-08-13", 7.0, 6.5),
        ]),
    ]
    doc = dm.pack_from_histories(histories, from_date="2026-08-13",
                                 to_date="2026-08-13")
    day = doc["days"][0]
    assert day["date"] == "2026-08-13"
    ig = day["interday"]["gainers"][0]
    ag = day["intraday"]["gainers"][0]
    assert ig["t"] == "AAA"
    assert ig["pct"] == 50.0
    assert ig["k"] == "gap"
    assert ag["t"] == "BBB"
    assert ag["pct"] == 90.48
    assert ag["k"] == "oc"
    assert ig["t"] != ag["t"]
    # Interday list is not sorted by same-day Change%.
    intra_of_aaa = dm.oc_pct(15.0, 11.0)
    assert intra_of_aaa == -26.67
    assert ig["pct"] != intra_of_aaa
    _ = cal


def test_pack_uses_px_fallback_prior_close_not_change_pct():
    # No prior HRE row. Fallback prior close 10 → open 13 = +30% gap.
    # Same-day Change% is −20%. Interday must still be the gap.
    histories = [
        ("ZZZ", [_row("2026-08-13", 13.0, 10.4)]),
    ]
    fb = {("ZZZ", "2026-08-13"): {"prev": 10.0, "high": 14.0, "low": 10.0}}
    doc = dm.pack_from_histories(
        histories, from_date="2026-08-13", to_date="2026-08-13",
        px_fallback=fb,
    )
    ig = doc["days"][0]["interday"]["gainers"][0]
    ag = doc["days"][0]["intraday"]["gainers"][0]
    assert ig["t"] == "ZZZ"
    assert ig["pct"] == 30.0
    assert ig["k"] == "gap"
    assert ag["pct"] == -20.0
    assert ig["h"] == 14.0
    assert ig["l"] == 10.0


def test_write_days_from_tmp_histories(tmp_path, monkeypatch):
    tick = tmp_path / "t"
    tick.mkdir()
    (tick / "AAA.json").write_text(json.dumps({
        "ticker": "AAA",
        "from_date": "2026-08-13",
        "to_date": "2026-08-14",
        "rows": [
            _row("2026-08-13", 10.0, 12.0),
            _row("2026-08-14", 13.0, 12.5),
        ],
    }), encoding="utf-8")
    out = tmp_path / "dash"
    monkeypatch.setattr(dm, "DASH_DIR", out)
    monkeypatch.setattr(dm, "HRE_DASH", tmp_path)
    doc = dm.bake(tick_dir=tick, meta={"from_date": "2026-08-13",
                                      "to_date": "2026-08-14"},
                  membership=False, px_fill=False)
    dest = dm.write_days(doc)
    assert dest.is_file()
    packed = json.loads(dest.read_text(encoding="utf-8"))
    assert packed["from_date"] == "2026-08-13"
    assert packed["to_date"] == "2026-08-14"
    assert packed["n_days"] == 2
    assert packed["days"][1]["interday"]["gainers"][0]["pct"] == 8.33


def test_open_camera_setup_badge_roundtrip():
    rows = [
        _row("2026-08-13", 12, 12, n_pos=1, n_neg=1),
        _row("2026-08-14", 11, 11, n_pos=1, n_neg=1),
        _row("2026-08-17", 13, 13, n_pos=1, n_neg=1),
        _row("2026-08-18", 14, 14, n_pos=1, n_neg=1),
        _row("2026-08-19", 10, 10.5, n_pos=4, n_neg=1),
    ]
    s = open_camera_setup(rows, 4)
    assert s["side"] == "long"
    assert s["quality"] == "clean"
    doc = dm.pack_from_histories([("AAA", rows)])
    day = doc["days"][-1]
    g = day["intraday"]["gainers"][0]
    assert g["su"] == "l"
    assert g["sq"] == "c"


def test_page_and_nav_sit_next_to_investigator():
    page = (ROOT / "dashboard" / "day-movers" / "index.html").read_text(
        encoding="utf-8")
    assert "Day movers — gainers / losers" in page
    assert "Intraday" in page and "Interday" in page
    assert "Gainers" in page and "Losers" in page
    assert "same-session close — not knowable at 09:30" in page
    assert "click a name" in page.lower()
    assert "../hard-red-exceptions/" in page
    assert "days.json" in page
    hre = (ROOT / "dashboard" / "hard-red-exceptions" / "index.html").read_text(
        encoding="utf-8")
    assert "../day-movers/" in hre
    assert "../hard-red-exceptions/" in hre or "Investigator" in hre
    db = (ROOT / "dashboard" / "day-board" / "index.html").read_text(
        encoding="utf-8")
    assert "../day-movers/" in db
    fm = (ROOT / "src" / "factor_mine_dash.html").read_text(encoding="utf-8")
    assert "../day-movers/" in fm
    baked = (ROOT / "dashboard" / "factor-mine" / "index.html").read_text(
        encoding="utf-8")
    assert "../day-movers/" in baked


def test_baked_days_clocks_differ_on_aug13():
    path = ROOT / "dashboard" / "day-movers" / "days.json"
    doc = json.loads(path.read_text(encoding="utf-8"))
    assert doc["from_date"] == "2026-08-13"
    assert doc["to_date"] >= "2026-09-17"
    day = next(d for d in doc["days"] if d["date"] == "2026-08-13")
    ig = day["interday"]["gainers"][0]
    ag = day["intraday"]["gainers"][0]
    assert ig["t"] and ag["t"]
    assert ig["k"] in ("gap", "cc")
    assert ag["k"] == "oc"
    assert ig["t"] != ag["t"] or ig["pct"] != ag["pct"]
    assert ig["pct"] != ag["pct"]


def test_hre_write_emits_day_movers():
    text = (ROOT / "src" / "hard_red_exceptions.py").read_text(encoding="utf-8")
    assert "day_movers" in text
    assert "write_from_hre" in text
    yml = (ROOT / ".github" / "workflows" / "hard_red_exceptions.yml").read_text(
        encoding="utf-8")
    assert "src/test_day_movers.py" in yml
    assert "dashboard/day-movers/days.json" in yml
