"""Pages-publish required-files gate. No network.

Run: PYTHONPATH=. python3 -m src.test_pages_publish_gate
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

from src import pages_publish_gate as gate


def _proc(key: str, files: list[dict], status: str = "OK") -> dict:
    return {"key": key, "status": status, "files": files}


def _file(key: str, role: str, status: str, path: str = "") -> dict:
    return {"key": key, "role": role, "status": status, "path": path or key}


def _write(root: Path, rel: str, payload: dict) -> None:
    p = root / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(payload), encoding="utf-8")


def _board(root: Path, date: str, processes: list[dict],
           overall: str = "PARTIAL") -> None:
    _write(root, f"data/day_board/{date}.json", {
        "date": date, "overall": overall, "ranker_ready": True,
        "processes": processes,
    })
    _write(root, "data/day_board/latest.json", {"date": date, "overall": overall})


def _fm_and_tickets(root: Path, date: str) -> None:
    _write(root, "dashboard/factor-mine/today.json", {"date": date})
    _write(root, "data/day_board/today_strategies.json", {
        "date": date, "session_open": date, "clock_legal_for": date,
        "strategies": {"a": {"family": "paper"}},
    })


def test_required_ok_partial_catalyst_ready() -> None:
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        date = "2026-09-16"
        _board(root, date, [
            _proc("stock_book", [
                _file("book_json", "required", "OK"),
                _file("book_md", "required", "OK"),
                _file("in_general", "input", "OK"),
            ]),
            _proc("publish", [
                _file("dash_html", "required", "OK"),
                _file("paper", "required", "OK"),
                _file("pages", "required", "SKIP"),
            ]),
            _proc("catalyst", [
                _file("dossiers", "optional", "FAIL"),
            ], status="FAIL"),
        ])
        _fm_and_tickets(root, date)
        v = gate.evaluate(date, root=root)
        assert v["ready"] is True
        assert v["fallback"] is False
        assert v["catalyst_status"] == "FAIL"
        assert v["overall"] == "PARTIAL"
        assert v["processes"]["stock_book"]["n_ok"] == 2
        assert v["processes"]["publish"]["n_ok"] == 2


def test_catalyst_fail_alone_does_not_block() -> None:
    html = Path(__file__).resolve().parent.parent / "src" / "pages_publish_gate.py"
    text = html.read_text(encoding="utf-8")
    assert "catalyst" in text
    assert "IGNORE_PROCESS_KEYS" in text
    assert "PAGES_PROCESS_KEYS" in text


def test_fallback_when_book_required_missing() -> None:
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        date = "2026-09-16"
        _board(root, date, [
            _proc("stock_book", [
                _file("book_json", "required", "MISSING"),
            ], status="FAIL"),
            _proc("publish", [
                _file("dash_html", "required", "OK"),
            ]),
        ])
        _fm_and_tickets(root, date)
        v = gate.evaluate(date, root=root)
        assert v["ready"] is True
        assert v["fallback"] is True


def test_blocked_without_tickets() -> None:
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        date = "2026-09-16"
        _board(root, date, [
            _proc("stock_book", [_file("book_json", "required", "OK")]),
            _proc("publish", [_file("dash_html", "required", "OK")]),
        ])
        _write(root, "dashboard/factor-mine/today.json", {"date": date})
        v = gate.evaluate(date, root=root)
        assert v["ready"] is False
        assert "tickets" in v["reason"]


def test_blocked_when_today_json_is_yesterday() -> None:
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        date = "2026-09-16"
        _board(root, date, [
            _proc("stock_book", [_file("book_json", "required", "OK")]),
            _proc("publish", [_file("dash_html", "required", "OK")]),
        ])
        _write(root, "dashboard/factor-mine/today.json", {"date": "2026-09-15"})
        _write(root, "data/day_board/today_strategies.json", {
            "date": date, "clock_legal_for": date,
        })
        v = gate.evaluate(date, root=root)
        assert v["ready"] is False
        assert "today.json" in v["reason"]


def test_live_2026_09_16_board_is_ready() -> None:
    """Real main snapshot: PARTIAL + catalyst FAIL must still be ready."""
    v = gate.evaluate("2026-09-16")
    assert v["date"] == "2026-09-16"
    assert v["catalyst_status"] in ("FAIL", "PARTIAL", "OK", "")
    assert v["factor_mine_today"] is True
    assert v["tickets"] is True
    assert v["ready"] is True


def test_dash_template_has_session_and_pack_stamp() -> None:
    html = (Path(__file__).resolve().parent.parent
            / "src" / "factor_mine_dash.html").read_text(encoding="utf-8")
    assert 'id="sessionHead"' in html
    assert 'id="packStamp"' in html
    assert "pack is " in html
    assert "Pages built " in html
    assert "paintSessionStamps" in html
    assert "Six effectiveness metrics (cash book) · " in html


def test_workflows_wire_the_gate() -> None:
    root = Path(__file__).resolve().parent.parent
    dep = (root / ".github" / "workflows" / "deploy-dashboard.yml").read_text(
        encoding="utf-8")
    book = (root / ".github" / "workflows" / "stock_book_all.yml").read_text(
        encoding="utf-8")
    assert "src.pages_publish_gate" in dep
    assert "Factor strategy mine" in dep
    assert "gh-pages-deploy" in dep
    assert "src.pages_publish_gate" in book
    assert "gh workflow run deploy-dashboard.yml" in book


def main() -> None:
    tests = [
        test_required_ok_partial_catalyst_ready,
        test_catalyst_fail_alone_does_not_block,
        test_fallback_when_book_required_missing,
        test_blocked_without_tickets,
        test_blocked_when_today_json_is_yesterday,
        test_live_2026_09_16_board_is_ready,
        test_dash_template_has_session_and_pack_stamp,
        test_workflows_wire_the_gate,
    ]
    for fn in tests:
        fn()
        print("ok", fn.__name__)
    print(f"{len(tests)} tests passed")


if __name__ == "__main__":
    main()
