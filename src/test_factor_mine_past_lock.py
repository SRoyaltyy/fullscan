"""The Factor Mine past-day lock sits outside the pinned book writer.

Run: python -m src.test_factor_mine_past_lock
"""
from __future__ import annotations

from pathlib import Path

from src import factor_mine_book as fmb
from src import past_day_lock as pdl
from src.factor_mine_past_lock import (
    locked_write_action_mds,
    render_recipe_texts,
)
from src.past_day_lock import PastDayLockError

ROOT = Path(__file__).resolve().parents[1]


def _toy(equity: float = 9990.0, extra_day: bool = False):
    rec = {
        "name": "toy",
        "hold": 1,
        "side": "long",
        "universe": "union",
        "top_n": 1,
        "rank": "list",
        "note": "",
        "require": {},
        "size": "leftover",
        "sell": "list",
        "s_boost": "none",
        "explain": {"kid": "toy", "inputs": [], "buy": [], "sell": []},
    }
    stats = [{
        "name": "toy",
        "total_ret_pct": -1.0,
        "final_equity": 9900,
        "signal_ret_pct": -1.0,
        "start_green": 0,
        "start_n": 1,
        "hold": 1,
        "side": "long",
        "universe": "union",
        "top_n": 1,
    }]
    daily = [{
        "date": "2026-10-05",
        "s": 0.1,
        "open_cash": 10000.0,
        "open_held": [],
        "open_equity": 10000.0,
        "overnight_delta": 0.0,
        "session_delta": -10.0,
        "bought": ["AAA"],
        "sold": [],
        "cash": 9990.0,
        "equity": equity,
        "lots": [{"ticker": "AAA", "shares": 1}],
    }]
    if extra_day:
        daily.append({
            "date": "2026-10-06",
            "s": 0.2,
            "open_cash": 9990.0,
            "open_held": ["AAA×1"],
            "open_equity": 9990.0,
            "overnight_delta": 0.0,
            "session_delta": 5.0,
            "bought": [],
            "sold": [],
            "cash": 9990.0,
            "equity": 9995.0,
            "lots": [{"ticker": "AAA", "shares": 1}],
        })
    book = {
        "n_trades": 0,
        "n_skips": 0,
        "realized": 0.0,
        "cash": 9990.0,
        "audit": {"ok": True, "final_cash": 9990.0},
        "daily": daily,
        "trades": [],
        "skips": [],
        "open": [],
    }
    payload = {
        "recipes": [rec],
        "from_date": "2026-08-13",
        "to_date": "2026-10-06" if extra_day else "2026-10-05",
    }
    return payload, stats, {"toy": book}


def _write(tmp: Path, payload, stats, books, writer) -> None:
    writer(
        payload, stats, books, ["toy"],
        out_dir=tmp, out_index=tmp / "INDEX.md", daily_md=tmp / "daily.md",
    )


def test_rendered_text_matches_pinned_writer(tmp: Path) -> None:
    payload, stats, books = _toy()
    rendered = render_recipe_texts(payload, stats, books)
    _write(tmp, payload, stats, books, fmb.write_action_mds)
    assert rendered
    for name, text in rendered:
        assert (tmp / f"{name}.md").read_text(encoding="utf-8") == text


def test_changed_past_day_is_not_written(tmp: Path) -> None:
    payload, stats, books = _toy()
    _write(tmp, payload, stats, books, fmb.write_action_mds)
    saved = (tmp / "toy.md").read_bytes()
    manifest = tmp / "manifest.jsonl"
    pdl.seal_factor_mine(
        "toy", "", saved.decode("utf-8"),
        watermark="2026-10-04", manifest=manifest,
    )
    saved_in_repo = pdl.in_repo
    saved_dir = pdl.FACTOR_MINE_DIR
    saved_manifest = pdl.MANIFEST_PATH
    pdl.in_repo = lambda _path: True
    pdl.FACTOR_MINE_DIR = tmp
    pdl.MANIFEST_PATH = manifest
    try:
        changed = _toy(equity=1.0)
        wrapped = locked_write_action_mds(fmb.write_action_mds)
        try:
            _write(tmp, *changed, wrapped)
        except PastDayLockError as exc:
            assert "2026-10-05" in str(exc)
        else:
            raise AssertionError("a sealed day edit was written")
        assert (tmp / "toy.md").read_bytes() == saved
    finally:
        pdl.in_repo = saved_in_repo
        pdl.FACTOR_MINE_DIR = saved_dir
        pdl.MANIFEST_PATH = saved_manifest


def test_new_day_appends_and_seals(tmp: Path) -> None:
    payload, stats, books = _toy()
    _write(tmp, payload, stats, books, fmb.write_action_mds)
    manifest = tmp / "manifest.jsonl"
    pdl.seal_factor_mine(
        "toy", "", (tmp / "toy.md").read_text(encoding="utf-8"),
        watermark="2026-10-04", manifest=manifest,
    )
    saved_in_repo = pdl.in_repo
    saved_dir = pdl.FACTOR_MINE_DIR
    saved_manifest = pdl.MANIFEST_PATH
    pdl.in_repo = lambda _path: True
    pdl.FACTOR_MINE_DIR = tmp
    pdl.MANIFEST_PATH = manifest
    try:
        nxt = _toy(extra_day=True)
        wrapped = locked_write_action_mds(fmb.write_action_mds)
        _write(tmp, *nxt, wrapped)
        text = (tmp / "toy.md").read_text(encoding="utf-8")
        assert "2026-10-06" in text
        days = [
            row["date"] for row in pdl.load_manifest(manifest)
            if row.get("kind") == "day"
        ]
        assert "2026-10-06" in days
    finally:
        pdl.in_repo = saved_in_repo
        pdl.FACTOR_MINE_DIR = saved_dir
        pdl.MANIFEST_PATH = saved_manifest


def test_workflow_calls_the_wrapper() -> None:
    yml = (ROOT / ".github" / "workflows" / "factor_mine.yml").read_text(encoding="utf-8")
    assert "src.factor_mine_past_lock" in yml
    assert "python -m src.factor_mine " not in yml
    assert "src.factor_mine_oos0914" in yml


def main() -> None:
    import tempfile
    tests = [
        test_rendered_text_matches_pinned_writer,
        test_changed_past_day_is_not_written,
        test_new_day_appends_and_seals,
        test_workflow_calls_the_wrapper,
    ]
    failed = 0
    for fn in tests:
        try:
            if fn.__code__.co_argcount:
                with tempfile.TemporaryDirectory() as raw:
                    fn(Path(raw))
            else:
                fn()
        except (Exception, SystemExit) as exc:  # noqa: BLE001
            failed += 1
            print(f"FAIL {fn.__name__}: {exc}")
        else:
            print(f"ok {fn.__name__}")
    if failed:
        raise SystemExit(f"{failed} failed")
    print(f"{len(tests)} passed")


if __name__ == "__main__":
    main()
