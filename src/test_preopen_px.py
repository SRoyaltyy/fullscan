"""Run: python -m pytest -q src/test_preopen_px.py"""
import json
from src import preopen_px as pp

PRE = {"start": 100, "end": 200}


def _chart(ts, closes, **meta):
    return {"chart": {"result": [{"meta": {"currentTradingPeriod": {"pre": PRE}, **meta},
                                  "timestamp": ts, "indicators": {"quote": [{"close": closes}]}}]}}


def test_premarket_last_trade_wins():
    assert pp.parse_chart(_chart([110, 150, 250], [5.0, 5.2, 9.9], chartPreviousClose=4.0)) == (5.2, "yahoo_premarket")


def test_prior_close_fallback():
    assert pp.parse_chart(_chart([250], [9.9], chartPreviousClose=4.0)) == (4.0, "yahoo_prior_close")
    assert pp.parse_chart(_chart([120], [None], chartPreviousClose=4.0)) == (4.0, "yahoo_prior_close")


def test_garbage_is_none():
    assert pp.parse_chart({}) == (None, None)
    assert pp.parse_chart(_chart([], [], chartPreviousClose=0)) == (None, None)


def test_write_labels_sources_and_omits_missing(tmp_path):
    f = {"KOD": (61.2, "yahoo_premarket"), "PENG": (20.0, "yahoo_prior_close"), "SDEV": (None, None)}
    p = pp.write_preopen("2026-10-21", ["KOD", "PENG", "SDEV"], fetch=f.get, out_dir=tmp_path)
    o = json.loads(p.read_text())
    assert o["date"] == "2026-10-21" and o["prices"] == {"KOD": 61.2, "PENG": 20.0}
    assert o["sources"] == {"KOD": "yahoo_premarket", "PENG": "yahoo_prior_close"}
    assert o["missing"] == ["SDEV"]


def test_add_only(tmp_path):
    p = tmp_path / "2026-10-21_preopen_px.json"
    p.write_text("{}")
    pp.write_preopen("2026-10-21", ["KOD"], fetch=lambda t: (1.0, "x"), out_dir=tmp_path)
    assert p.read_text() == "{}"


def test_shape_matches_h1_loader(tmp_path):
    from src import h1_sealed_exec as h
    pp.write_preopen("2026-10-21", ["KOD"], fetch=lambda t: (61.2, "yahoo_premarket"), out_dir=tmp_path)
    if hasattr(h, "load_preopen_prices"):
        assert h.load_preopen_prices("2026-10-21", root=tmp_path) == {"KOD": 61.2}
