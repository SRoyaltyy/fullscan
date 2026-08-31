"""JSON extract + target pick. No network.

Run: python -m src.test_map_heat_research
"""
from __future__ import annotations

from src.map_heat_research import extract_json, select_targets, tape_boosts, ticker_boosts


def test_extract_cards() -> None:
    text = """prose first
```json
{"date": "2026-08-26", "cards": [
  {"industry": "Uranium", "sector": "Energy", "subsector_dir": "up",
   "action": "OVERRIDE", "captains": [{"ticker": "UEC", "sent": "pos"}]}
]}
```
"""
    obj = extract_json(text)
    assert obj and len(obj["cards"]) == 1
    assert obj["cards"][0]["industry"] == "Uranium"


def test_select_skips_no_captain() -> None:
    heat = {
        "overrides": [
            {"industry": "Ghost", "action": "OVERRIDE"},
            {"industry": "Uranium", "action": "OVERRIDE"},
        ],
        "hot": [],
        "cold": [],
        "industries": [
            {"industry": "Ghost", "sector": "X", "spx_leaders": [], "rut_leaders": []},
            {"industry": "Uranium", "sector": "Energy",
             "w1": 12.3, "vs_parent_w1": 14.2,
             "spx_leaders": [],
             "rut_leaders": [{"ticker": "UEC", "sent": "pos"}]},
        ],
    }
    picked = select_targets(heat)
    assert [p["industry"] for p in picked] == ["Uranium"]


def test_ticker_boosts_empty_ok() -> None:
    t, i = ticker_boosts("1900-01-01")
    assert t == {} and i == {}


def test_tape_boosts_from_finviz_overrides() -> None:
    import json
    import tempfile
    from pathlib import Path

    from src import map_heat_research as research
    orig = research.OUT_DIR
    with tempfile.TemporaryDirectory() as d:
        research.OUT_DIR = Path(d)
        js = Path(d) / "2099-01-01_map_heat.json"
        js.write_text(json.dumps({
            "overrides": [
                {"industry": "Thermal Coal", "vs_parent_w1": 6.26,
                 "action": "OVERRIDE", "spx_leaders": [],
                 "rut_leaders": [{"ticker": "CNR"}, "BTU"]},
                {"industry": "Solar", "vs_parent_w1": -4.5,
                 "action": "OVERRIDE", "spx_leaders": ["FSLR"],
                 "rut_leaders": []},
            ],
        }))
        t, i = tape_boosts("2099-01-01")
        assert i["Thermal Coal"] > 0
        assert i["Solar"] < 0
        assert t["CNR"] > 0
        assert t["BTU"] > 0
        assert t["FSLR"] < 0
        tt, ii = ticker_boosts("2099-01-01")
        assert tt == t and ii == i
    research.OUT_DIR = orig


if __name__ == "__main__":
    test_extract_cards()
    test_select_skips_no_captain()
    test_ticker_boosts_empty_ok()
    test_tape_boosts_from_finviz_overrides()
    print("ok")
