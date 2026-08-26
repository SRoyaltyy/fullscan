"""JSON extract + target pick. No network.

Run: python -m src.test_map_heat_research
"""
from __future__ import annotations

from src.map_heat_research import extract_json, select_targets, ticker_boosts


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


if __name__ == "__main__":
    test_extract_cards()
    test_select_skips_no_captain()
    test_ticker_boosts_empty_ok()
    print("ok")
