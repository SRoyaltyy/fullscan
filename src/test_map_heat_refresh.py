"""Morning refresh passthrough when Grok X-records fail. No LLM.

Run: python -m src.test_map_heat_refresh
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

from src import map_heat_refresh as mr
from src import output_qc


def _cards(n: int = 22) -> list[dict]:
    return [{
        "industry": f"I{i}", "sector": "X", "action": "HEAT",
        "subsector_dir": "flat", "conviction": "low",
        "captains": [{"ticker": "AAA", "sent": "none",
                      "search_note": "n/a", "evidence": []}],
    } for i in range(n)]


def test_passthrough_writes_qc_ok_research() -> None:
    orig = mr.OUT
    with tempfile.TemporaryDirectory() as d:
        mr.OUT = Path(d)
        heat = {
            "macro_gate": False, "size_gate": False, "earnings_gate": False,
            "tape": [{"ticker": "ES"}], "econ": [], "earnings": [],
        }
        baseline = {
            "generated_at": "2026-09-01T22:00:00",
            "n_targets": 22,
            "cards": _cards(),
            "opportunities": [],
            "parent_splits": [],
        }
        payload = mr._write_passthrough(
            "2026-09-02", heat, baseline,
            ["Uranium:UEC:x_used_without_mention_delta"])
        assert payload["phase"] == "morning_refresh"
        assert payload["passthrough"] is True
        assert payload["n_refreshed"] == 0
        assert payload["n_cards"] == 22
        assert payload["evidence_errors"] == []
        md = Path(d) / "2026-09-02_research.md"
        js = Path(d) / "2026-09-02_research.json"
        assert md.exists() and js.exists()
        qc = output_qc.qc_map_heat_research(md)
        assert qc.ok, qc.reason
        data = json.loads(js.read_text(encoding="utf-8"))
        assert data["morning_refresh_errors"]
    mr.OUT = orig


if __name__ == "__main__":
    test_passthrough_writes_qc_ok_research()
    print("1 test passed")
