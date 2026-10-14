"""Factor Mine seal requires a Grok predict (pending Cyrus).

Run: python -m pytest -q src/test_fm_grok_gate.py
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

from src import fm_grok_gate as g
from src import strategy_tickets as st
from src.strategy_status_publish import build

D = "2030-01-02"


def _root(ok=True, model: str | None = "grok-4.20-0309-reasoning", files=True) -> Path:
    r = Path(tempfile.mkdtemp())
    b = r / "data/day_board"; b.mkdir(parents=True)
    (b / f"{D}_strategy_tickets.json").write_text(json.dumps({"strategies": {
        "fm_a": {"family": "factor_mine", "status": "ok", "buy": [{"ticker": "AAA"}], "sell": []}}}))
    gen = r / "01_daily/general"; gen.mkdir(parents=True)
    (gen / f"{D}_predict.md").write_text("predict\n")
    if files:
        (r / f"01_daily/{D}_preopen_status.json").write_text(json.dumps({"grok_ok": ok}))
        if model is not None:
            (gen / f"{D}_predict_trace.md").write_text(f"Model: `{model}`\n")
    return r


def _rows():
    return [st._entry("fm_a", "factor_mine", D, [{"ticker": "AAA"}], [{"ticker": "BBB"}]),
            st._entry("flatten_robust", "flatten", D, [], [{"ticker": "CCC"}])]


def _fm(r: Path) -> dict:
    return {x["name"]: x for x in build(r, D, fetch=lambda u: None)["strategies"]}["fm_a"]


def test_grok_seals() -> None:
    r = _root()
    assert g.reason(D, r) is None
    assert _fm(r)["status"] == "OK" and _fm(r)["buys"] == ["AAA"]


def test_glm_refuses() -> None:
    r = _root(model="glm-5.3-flash")
    why = g.reason(D, r)
    assert why and "glm-5.3-flash" in why and "not Grok" in why
    out = g.refuse_rows(_rows(), D, why)
    fm = out[0]
    assert fm["status"] == g.STATUS and fm["buy"] == [] and fm["sell"] == []
    assert "glm-5.3-flash" in fm["note"]
    assert out[1]["sell"][0]["ticker"] == "CCC"  # other families untouched
    row = _fm(r)
    assert row["status"] == "MISSING" and "glm-5.3-flash" in row["note"]


def test_grok_ok_false_refuses_even_with_grok_trace() -> None:
    r = _root(ok=False)
    assert "grok_ok=False" in g.reason(D, r)
    assert _fm(r)["status"] == "MISSING"


def test_missing_refuses() -> None:
    r = _root(files=False)
    why = g.reason(D, r)
    assert why and "_preopen_status.json is not on main" in why
    assert _fm(r)["status"] == "MISSING"
    r = _root(model=None)
    assert "_predict_trace.md is not on main" in g.reason(D, r)
    assert _fm(r)["status"] == "MISSING"


def test_sessions_before_from_are_not_rejudged() -> None:
    r = _root(model="glm-5.3-flash")
    assert g.reason("2026-10-13", r) is None


def test_install_wraps_recipe_strats(monkeypatch=None) -> None:
    orig = st.recipe_strats
    try:
        st.recipe_strats = lambda date, look_out=None: _rows()
        for r, refused in ((_root(), False), (_root(model="glm-5.3-flash"), True)):
            st.recipe_strats = lambda date, look_out=None: _rows()
            g.install(r)
            fm = st.recipe_strats(D)[0]
            assert (fm["status"] == g.STATUS) is refused
            assert (fm["buy"] == []) is refused
    finally:
        st.recipe_strats = orig


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
    print("fm grok gate: grok seals; glm, grok_ok=false and missing refuse")
