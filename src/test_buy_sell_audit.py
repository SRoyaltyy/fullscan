"""Buy/sell fingerprint audit: a renamed past buy fails closed."""
from __future__ import annotations

import io
import json
from contextlib import redirect_stdout
from pathlib import Path

from src.buy_sell_audit import (
    THEME_AUDIT_ARGV,
    THEME_STRATEGY,
    collect,
    diff,
    fingerprint,
    load_baselines,
    main as audit_main,
    run_theme_radar_audit,
    seed,
    theme_fail_line,
    theme_passes,
)
from src.past_day_lock import sha256_text

DAY = "2030-01-02"


def _ticket(ticker: str) -> str:
    return json.dumps({
        "strategies": {
            "fm_a": {
                "family": "factor_mine",
                "status": "ok",
                "buy": [{"ticker": ticker, "shares": 3}],
                "sell": [],
                "note": "sit note must not enter the fingerprint",
            }
        }
    })


def _root(tmp: Path, ticker: str = "AAA") -> Path:
    board = tmp / "data" / "day_board"
    board.mkdir(parents=True)
    text = _ticket(ticker)
    (board / f"{DAY}_strategy_tickets.json").write_text(text, encoding="utf-8")
    lock = tmp / "data" / "past_day_lock"
    lock.mkdir(parents=True)
    row = {
        "date": DAY,
        "kind": "day",
        "name": "",
        "record": "strategy_tickets",
        "sha256": sha256_text(text),
    }
    (lock / "manifest.jsonl").write_text(json.dumps(row) + "\n", encoding="utf-8")
    return tmp


def test_fingerprint_ignores_notes_and_catches_a_rename() -> None:
    same = fingerprint(DAY, ["AAA 3"], [])
    assert same == fingerprint(DAY, ["AAA 3"], [])
    assert same != fingerprint(DAY, ["BBB 3"], [])
    assert THEME_STRATEGY == "theme_radar_research_shorts"
    assert THEME_AUDIT_ARGV == (
        "research/shadow_log/shadow_log_lock.py", "audit", "--json", "-",
    )


def test_renamed_buy_fails_and_baseline_is_not_rewritten(tmp: Path) -> None:
    root = _root(tmp, "AAA")
    seals, notes = collect(root)
    assert not any(note.startswith("SKIP theme_radar") for note in notes)
    fm = [seal for seal in seals if seal.name == "fm_a"]
    assert len(fm) == 1 and fm[0].date == DAY
    path = tmp / "data" / "buy_sell_audit" / "baselines.jsonl"
    seeded = seed(path, seals)
    assert any("fm_a" in line for line in seeded)
    first = path.read_text(encoding="utf-8")
    assert seed(path, seals) == []
    assert path.read_text(encoding="utf-8") == first

    moved = _ticket("BBB")
    (root / "data" / "day_board" / f"{DAY}_strategy_tickets.json").write_text(
        moved, encoding="utf-8")
    # The whole-file lock moved, and so did the buy name.
    again, notes = collect(root)
    assert any("strategy_tickets" in note and "moved" in note for note in notes)
    lock = root / "data" / "past_day_lock" / "manifest.jsonl"
    row = json.loads(lock.read_text(encoding="utf-8"))
    row["sha256"] = sha256_text(moved)
    lock.write_text(json.dumps(row) + "\n", encoding="utf-8")
    again, _notes = collect(root)
    drifts, news = diff(again, load_baselines(path))
    assert any("fm_a" in line and DAY in line and "moved" in line for line in drifts)
    assert news == []
    try:
        seed(path, again)
    except SystemExit as exc:
        assert "refusing to rewrite" in str(exc)
    else:
        raise AssertionError("seed rewrote a first seal")
    assert path.read_text(encoding="utf-8") == first


def test_gap_is_not_backfilled(tmp: Path) -> None:
    root = _root(tmp)
    seals, _notes = collect(root)
    assert all(seal.date == DAY for seal in seals)
    assert not (root / "data" / "day_board" / "2030-01-03_strategy_tickets.json").exists()


def _pass_report() -> dict:
    return {
        "schema": "theme_radar_research_shorts_ironclad_audit_v1",
        "strategy": THEME_STRATEGY,
        "ok": True,
        "drift_dates": [],
        "summary": {"drifts": 0, "missing_seals": 0, "gaps": 1},
        "gaps": [{"date": "2026-10-13", "reason": "no plan file"}],
    }


def test_theme_radar_pass_is_not_a_drift() -> None:
    report = _pass_report()
    assert theme_passes(0, report)
    assert not theme_passes(1, report)
    assert not theme_passes(0, {**report, "strategy": "other"})
    assert not theme_passes(0, {**report, "ok": False})
    line = theme_fail_line(1, {
        "strategy": THEME_STRATEGY,
        "ok": False,
        "drift_dates": ["2026-10-06"],
        "summary": {"drifts": 1, "missing_seals": 0},
    })
    assert line.startswith(f"DRIFT {THEME_STRATEGY}: FAIL ")
    assert "drift_dates=2026-10-06" in line
    assert "FAIL" in line


def _fake_theme(checkout: Path, body: str) -> None:
    folder = checkout / "research" / "shadow_log"
    folder.mkdir(parents=True)
    (folder / "shadow_log_lock.py").write_text(body, encoding="utf-8")


def test_theme_radar_nonzero_fails_the_audit(tmp: Path) -> None:
    checkout = tmp / "theme-radar"
    _fake_theme(checkout, """
import json, sys
open("argv.txt", "w", encoding="utf-8").write("\\n".join(sys.argv[1:]))
json.dump({
    "strategy": "theme_radar_research_shorts",
    "ok": False,
    "drift_dates": ["2026-10-06"],
    "summary": {"drifts": 1, "missing_seals": 2},
}, sys.stdout)
sys.stderr.write("IRONCLAD fingerprint audit: FAIL\\n")
raise SystemExit(1)
""")
    buf = io.StringIO()
    with redirect_stdout(buf):
        code = audit_main([
            "--check", "--root", str(tmp / "fullscan"),
            "--theme-radar", str(checkout),
        ])
    text = buf.getvalue()
    assert code == 1
    assert f"DRIFT {THEME_STRATEGY}: FAIL exit 1" in text
    assert "drift_dates=2026-10-06" in text
    assert f"::error::DRIFT {THEME_STRATEGY}:" in text
    assert "theme-radar: IRONCLAD fingerprint audit: FAIL" in text
    assert (checkout / "argv.txt").read_text(encoding="utf-8") == "audit\n--json\n-"
    missing = run_theme_radar_audit(tmp / "absent")
    assert missing[-1].startswith(f"DRIFT {THEME_STRATEGY}: FAIL exit 127")
    assert "shadow_log_lock.py" in missing[-1]


def test_theme_radar_exit_zero_without_strategy_fails(tmp: Path) -> None:
    checkout = tmp / "theme-radar"
    _fake_theme(checkout, """
import json, sys
json.dump({"strategy": "something_else", "ok": True}, sys.stdout)
raise SystemExit(0)
""")
    buf = io.StringIO()
    with redirect_stdout(buf):
        code = audit_main([
            "--check", "--root", str(tmp / "fullscan"),
            "--theme-radar", str(checkout),
        ])
    text = buf.getvalue()
    assert code == 1
    assert f"DRIFT {THEME_STRATEGY}: FAIL exit 0" in text
    assert "strategy='something_else'" in text

    ok = tmp / "ok-theme"
    _fake_theme(ok, """
import json, sys
json.dump({
    "strategy": "theme_radar_research_shorts",
    "ok": True,
    "drift_dates": [],
    "summary": {"drifts": 0, "missing_seals": 0, "gaps": 1},
}, sys.stdout)
sys.stderr.write("IRONCLAD fingerprint audit: PASS\\n")
raise SystemExit(0)
""")
    buf = io.StringIO()
    with redirect_stdout(buf):
        code = audit_main([
            "--check", "--root", str(tmp / "fullscan-ok"),
            "--theme-radar", str(ok),
        ])
    text = buf.getvalue()
    assert code == 0, text
    assert f"PASS {THEME_STRATEGY}" in text
    assert f"DRIFT {THEME_STRATEGY}" not in text


def main() -> None:
    import tempfile
    tmp = Path(tempfile.mkdtemp())
    test_fingerprint_ignores_notes_and_catches_a_rename()
    test_renamed_buy_fails_and_baseline_is_not_rewritten(tmp / "rename")
    test_gap_is_not_backfilled(tmp / "gap")
    test_theme_radar_pass_is_not_a_drift()
    test_theme_radar_nonzero_fails_the_audit(tmp / "theme-fail")
    test_theme_radar_exit_zero_without_strategy_fails(tmp / "theme-strategy")
    print("ok buy-sell audit")


if __name__ == "__main__":
    main()
