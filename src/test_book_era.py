"""Tests for as-of stock-book eras.

Run: python -m src.test_book_era
"""
from __future__ import annotations

from src import book_era
from src import stock_book_diag as diag
from src.stock_book_diag_signals import extract_decisions, render_actions_markdown


def test_feature_starts():
    assert book_era.live("2026-08-13", "join") is True
    assert book_era.live("2026-08-13", "finviz_digest") is False
    assert book_era.live("2026-08-13", "news_judge") is False
    assert book_era.live("2026-08-13", "ab_enriched") is False
    assert book_era.live("2026-08-13", "peer_rs") is False
    assert book_era.live("2026-08-13", "decision_lattice") is False
    assert book_era.live("2026-08-19", "news_judge") is True
    assert book_era.live("2026-08-20", "finviz_digest") is True
    assert book_era.live("2026-08-31", "decision_lattice") is True


def test_method_from_saved_books():
    m13 = book_era.method_for("2026-08-13", book_era.load_book_meta("2026-08-13"))
    assert m13 == "weighted_4"
    m31 = book_era.method_for("2026-08-31", book_era.load_book_meta("2026-08-31"))
    assert m31 == "decision_lattice"
    assert book_era.method_for("2026-08-24") == "weighted"


def test_era_demotes_0813_inputs():
    specs = {s["key"]: s for s in diag.workflow_specs("2026-08-13")}
    book = {f["key"]: f for f in specs["stock_book"]["files"]}
    assert book["in_digest"]["role"] == "era"
    assert book["in_judge"]["role"] == "era"
    assert book["in_ab"]["role"] == "era"
    assert book["peers"]["role"] == "era"
    assert book["join"]["role"] == "required"
    assert book["book_json"]["role"] == "required"
    assert book["in_weather"]["role"] == "input"
    assert book["in_general"]["role"] == "input"

    modern = {s["key"]: s for s in diag.workflow_specs("2026-08-31")}
    book31 = {f["key"]: f for f in modern["stock_book"]["files"]}
    assert book31["in_digest"]["role"] == "input"
    assert book31["peers"]["role"] == "required"


def test_audit_0813_does_not_block_on_later_era_files():
    report = diag.audit("2026-08-13", gh_runs={})
    assert report.ranker_ready is True
    assert report.era.get("method") == "weighted_4"
    book = next(w for w in report.workflows if w.key == "stock_book")
    skipped = {f.key for f in book.files if f.status == "SKIP"}
    assert {"in_digest", "in_judge", "in_ab", "peers"} <= skipped
    assert diag.action_ok(report) is True
    dec = report.decisions
    assert dec["present"] is True
    assert dec["n_1d_buy"] >= 10
    assert dec["ranker"] == "weighted_4"
    how = " ".join(dec["how"])
    assert "lattice" not in how.lower() or "not today's lattice" in how.lower()
    assert "4-family" in how or "weighted" in how.lower()
    paper = dec.get("paper") or {}
    assert paper.get("spy_return") is not None
    # First dashboard session: SPY is still flat; the downturn prints later.
    if paper.get("through") and paper.get("start") and paper["through"] > paper["start"]:
        assert paper["spy_return"] < 0
    assert paper.get("note")


def test_0813_actions_name_the_as_of_method():
    dec = extract_decisions("2026-08-13")
    md = "\n".join(render_actions_markdown(dec))
    assert "TKVA" in md or dec["horizons"]["1d"]["buy"][0]["ticker"]
    assert "As-of method" in md
    assert "weighted" in md.lower()
    assert "Paper vs SPY" in md
    assert "HARD_RED" not in md


def test_paper_context_explains_the_11pct():
    ctx = book_era.paper_context("2026-08-31")
    assert ctx["spy_return"] is not None
    assert ctx["spy_return"] < 0
    best = ctx["best"]
    assert best is not None
    assert best["ret"] > 0.08
    assert best["sleeve"].endswith("_size") or best["sleeve"].endswith("_top")
    assert "1d_top" in ctx["note"] or "sleeve" in ctx["note"]


if __name__ == "__main__":
    import traceback

    passed = 0
    failed = 0

    def _run(fn):
        global passed, failed
        try:
            fn()
            print(f"  ok  {fn.__name__}")
            passed += 1
        except Exception:
            failed += 1
            print(f"  FAIL {fn.__name__}")
            traceback.print_exc()

    _run(test_feature_starts)
    _run(test_method_from_saved_books)
    _run(test_era_demotes_0813_inputs)
    _run(test_audit_0813_does_not_block_on_later_era_files)
    _run(test_0813_actions_name_the_as_of_method)
    _run(test_paper_context_explains_the_11pct)
    print(f"{passed} passed, {failed} failed")
    raise SystemExit(1 if failed else 0)
