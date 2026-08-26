"""Guard rails: salvage JSON, keep first-print, do not overwrite with stub.

Run: python -m src.test_artifact_guard
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

from src import artifact_guard, output_qc


def test_salvage_trailing_comma_and_fence() -> None:
    raw = '```json\n{"ok": true, "fails": [], "notes": "fine",}\n```'
    obj = artifact_guard.salvage_json(raw)
    assert isinstance(obj, dict) and obj["ok"] is True
    assert artifact_guard.looks_garbled_json('{"ok": true,}') is False
    assert artifact_guard.looks_garbled_json("{not json") is True


def test_first_print_uranium_is_usable_and_qc_ok() -> None:
    cards = []
    for i in range(10):
        cards.append({
            "industry": "Uranium" if i == 0 else f"Industry-{i}",
            "sector": "Energy" if i == 0 else "Basic Materials",
            "action": "HEAT",
            "subsector_dir": "up",
            "conviction": "high",
            "captains": [
                {"ticker": "CCJ" if i == 0 else f"T{i}", "index": "SPX",
                 "sent": "pos"},
            ],
            "one_line": "first print card",
        })
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        folder = root / "01_daily" / "map_heat"
        folder.mkdir(parents=True)
        js = folder / "2099-01-02_captains.json"
        md = folder / "2099-01-02_captains.md"
        js.write_text(json.dumps({
            "date": "2099-01-02", "phase": "first_print",
            "cards": cards, "n_cards": 10,
        }))
        md.write_text(
            "# MAP HEAT RESEARCH — 2099-01-02\n\n"
            "10 captain cards including Uranium / CCJ.\n\n"
            "CAPTAIN_CARDS_OK\nOPPORTUNITY_OK\n"
        )
        assert artifact_guard.research_usable(js)
        assert artifact_guard.research_quality(js) == "usable"
        found = artifact_guard.resolve_artifact(
            "map_heat_research", "2099-01-02",
            preferred=str(folder / "2099-01-02_research.md"),
            root=root,
        )
        assert found is not None
        assert "captain" in found.name
        qc = output_qc.qc_map_heat_research(md)
        assert qc.ok, qc.explain()


def test_bootstrap_does_not_outrank_first_print() -> None:
    with tempfile.TemporaryDirectory() as d:
        dest = Path(d) / "a_research.json"
        src = Path(d) / "b_research.json"
        dest.write_text(json.dumps({
            "phase": "first_print",
            "cards": [
                {"industry": "Uranium", "captains": [{"ticker": "CCJ", "sent": "pos"}]},
                {"industry": "Gold", "captains": [{"ticker": "NEM", "sent": "pos"}]},
                {"industry": "Silver", "captains": [{"ticker": "PAAS", "sent": "pos"}]},
            ],
        }))
        src.write_text(json.dumps({
            "phase": "morning_bootstrap", "cards": [],
        }))
        assert artifact_guard.should_overwrite(dest, src) is False
        assert artifact_guard.should_overwrite(src, dest) is True


def test_step_for_path() -> None:
    assert artifact_guard.step_for_path("01_daily/map_heat/2026-08-26_research.md") == "map_heat_research"
    assert artifact_guard.step_for_path("01_daily/events/2026-08-26_events.json") == "events"
    assert artifact_guard.step_for_path("(review)") == "grok_review"


def main() -> None:
    tests = [
        test_salvage_trailing_comma_and_fence,
        test_first_print_uranium_is_usable_and_qc_ok,
        test_bootstrap_does_not_outrank_first_print,
        test_step_for_path,
    ]
    failed = 0
    for fn in tests:
        try:
            fn()
            print(f"ok  {fn.__name__}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"FAIL {fn.__name__}: {e}")
    if failed:
        raise SystemExit(f"{failed} test(s) failed")
    print(f"{len(tests)} tests passed")


if __name__ == "__main__":
    main()
