"""forward_shadow_v1: keep-held fills, frozen recipes, append-only ledger."""
from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

from src.factor_mine_book import HARD_RED, lot_should_sell
from src.forward_shadow_v1 import (
    FILL_MODEL,
    FORWARD_START,
    ForwardError,
    assert_spec_matches_group3,
    build_spec_document,
    canon,
    earliest_send_inputs,
    initial_state,
    load_fees,
    load_spec,
    record_fills,
    record_picks,
    step_day,
    validate_winner,
)
from src.forward_shadow_v1_append import AppendOnlyError, assert_hook, assert_manifest
from src.forward_shadow_v1_bars import assert_split_consistent, session_jumps
from src.webull_exec import size_hot4_tickets

ROOT = Path(__file__).resolve().parents[1]


def _expect(exc_type, message: str, fn) -> None:
    try:
        fn()
    except exc_type as exc:
        if message not in str(exc):
            raise AssertionError(f"{exc!s} did not include {message}") from exc
    else:
        raise AssertionError(message)


def _row(ticker: str, score: float, alarm: bool = False) -> dict:
    return {
        "alarm": alarm,
        "date": "2026-09-28",
        "ohlc_hot_score": score,
        "sources": ["ohlc_hot"],
        "ticker": ticker,
    }


def _bars(session: str, prices: dict[str, tuple[float, float]], splits=None) -> dict:
    return {
        "adjustment": "split-adjusted, dividends not applied",
        "auto_adjust": False,
        "bars": [
            {
                "close": close, "date": session, "high": max(open_, close),
                "low": min(open_, close), "open": open_, "ticker": ticker,
                "volume": 1000.0,
            }
            for ticker, (open_, close) in sorted(prices.items())
        ],
        "missing": [],
        "session": session,
        "splits": list(splits or []),
    }


def _install(tmp: Path) -> Path:
    if tmp.exists():
        shutil.rmtree(tmp)
    tmp.mkdir(parents=True)
    spec = build_spec_document()
    (tmp / "recipes.json").write_text(json.dumps(spec, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (tmp / "hooks").mkdir()
    (tmp / "hooks" / "v4_winners.json").write_text(
        json.dumps({"schema": 1, "study": "forward_shadow_v1", "winners": []}, indent=2) + "\n",
        encoding="utf-8",
    )
    (tmp / "ledger").mkdir()
    (tmp / "ledger" / "manifest.jsonl").write_text("", encoding="utf-8")
    return tmp


def _send(session: str, rows: list[dict], *, score=None) -> dict:
    doc = {"date": session, "picks": {}, "rows": rows, "source": "panel"}
    if score is not None:
        doc["s"] = score
    raw = (canon(doc) + "\n").encode("utf-8")
    return {
        "blob_sha": "b" * 40,
        "commit": "c" * 40,
        "doc": doc,
        "path": f"data/factor_mine/send_inputs/{session}.json",
        "raw": raw,
    }


def test_spec_is_group3_keep_held() -> None:
    doc = assert_spec_matches_group3(None)
    assert doc["fill_model"] == "keep-held"
    assert doc["fill_model"] == FILL_MODEL
    assert doc["sell"] == "list"
    assert doc["forward_start"] == FORWARD_START
    assert doc["hard_red_max"] == HARD_RED
    names = [rec["name"] for rec in doc["recipes"]]
    assert names == ["fwd_union_hot_n4_h1", "fwd_union_hot_score_h3"]
    by = {rec["name"]: rec for rec in doc["recipes"]}
    assert by["fwd_union_hot_n4_h1"]["top_n"] == 4
    assert by["fwd_union_hot_n4_h1"]["hold"] == 1
    assert by["fwd_union_hot_n4_h1"]["rank"] == "hot_score"
    assert by["fwd_union_hot_score_h3"]["top_n"] == 8
    assert by["fwd_union_hot_score_h3"]["hold"] == 3
    assert by["fwd_union_hot_n4_h1"]["forbid"] == {"alarm": True}
    assert doc["spec_sha256"] == load_spec(None)["spec_sha256"]


def test_renewed_held_name_has_zero_trades_and_zero_fees() -> None:
    recipe = next(rec for rec in build_spec_document()["recipes"] if rec["name"] == "fwd_union_hot_n4_h1")
    fees = load_fees()
    rows = {"AAA": _row("AAA", 9.0)}
    state, first = step_day(
        recipe, initial_state(), "2026-09-28", ["2026-09-28"], ["AAA"],
        rows, {"AAA": 10.0}, {"AAA": 11.0}, fees, s=None,
    )
    assert first["buys"] and first["fees_futubull"] > 0
    shares = state["positions"][0]["shares"]
    cash = state["cash_f"]
    held_lot = state["positions"][0]
    do_sell, kind = lot_should_sell(
        held_lot, held=1, min_hold=1, early=False, dropped=False,
        sell_mode="list", px=12.0, side="long", take_pct=None, stop_pct=None,
    )
    assert do_sell is False and kind == "keep"
    state2, second = step_day(
        recipe, state, "2026-09-29", ["2026-09-28", "2026-09-29"], ["AAA"],
        rows, {"AAA": 12.0}, {"AAA": 13.0}, fees, s=None,
    )
    assert second["sells"] == []
    assert second["buys"] == []
    assert second["renewals"] == ["AAA"]
    assert second["fees_futubull"] == 0.0
    assert second["fees_flat_15bp"] == 0.0
    assert state2["positions"][0]["shares"] == shares
    assert state2["positions"][0]["entry"] == "2026-09-28"
    assert state2["cash_f"] == cash
    live, skips = size_hot4_tickets(
        [{"ticker": "AAA", "side": "long", "px": 12.0}],
        cash=1000.0, held={"AAA"}, date="2026-09-29", s=1.0,
    )
    assert live == []
    assert skips[0]["kind"] == "held"


def test_list_drop_sells_after_min_hold_and_keeps_before_it() -> None:
    fees = load_fees()
    hot = next(rec for rec in build_spec_document()["recipes"] if rec["name"] == "fwd_union_hot_n4_h1")
    slow = next(rec for rec in build_spec_document()["recipes"] if rec["name"] == "fwd_union_hot_score_h3")
    state, _day = step_day(
        hot, initial_state(), "2026-09-28", ["2026-09-28"], ["AAA"],
        {"AAA": _row("AAA", 1)}, {"AAA": 10.0}, {"AAA": 11.0}, fees, s=None,
    )
    _state, dropped = step_day(
        hot, state, "2026-09-29", ["2026-09-28", "2026-09-29"], [],
        {}, {"AAA": 12.0}, {"AAA": 12.5}, fees, s=None,
    )
    assert [row["ticker"] for row in dropped["sells"]] == ["AAA"]
    assert dropped["buys"] == []
    state3, _opened = step_day(
        slow, initial_state(), "2026-09-28", ["2026-09-28"], ["BBB"],
        {"BBB": _row("BBB", 1)}, {"BBB": 8.0}, {"BBB": 8.5}, fees, s=None,
    )
    _kept, still = step_day(
        slow, state3, "2026-09-29", ["2026-09-28", "2026-09-29"], [],
        {}, {"BBB": 9.0}, {"BBB": 9.2}, fees, s=None,
    )
    assert still["sells"] == []
    assert still["fees_futubull"] == 0.0
    assert still["skips"][0]["kind"] == "min_hold"


def test_hard_red_sits_new_buys_and_keeps_the_lot() -> None:
    recipe = next(rec for rec in build_spec_document()["recipes"] if rec["name"] == "fwd_union_hot_n4_h1")
    fees = load_fees()
    state, first = step_day(
        recipe, initial_state(), "2026-09-28", ["2026-09-28"], ["AAA"],
        {"AAA": _row("AAA", 1)}, {"AAA": 10.0}, {"AAA": 10.5}, fees, s=None,
    )
    assert first["buys"]
    _state, second = step_day(
        recipe, state, "2026-09-29", ["2026-09-28", "2026-09-29"], ["AAA", "BBB"],
        {"AAA": _row("AAA", 2), "BBB": _row("BBB", 1)},
        {"AAA": 11.0, "BBB": 4.0}, {"AAA": 11.2, "BBB": 4.1}, fees, s=-3.0,
    )
    assert second["hard_red"] is True
    assert second["sells"] == []
    assert second["buys"] == []
    assert second["renewals"] == ["AAA"]
    assert second["fees_futubull"] == 0.0
    assert any(skip["ticker"] == "BBB" and skip["kind"] == "hard_red" for skip in second["skips"])


def test_no_day_before_start_and_future_file_does_not_rewrite(tmp_path: Path) -> None:
    root = _install(tmp_path)
    _expect(ForwardError, "no day before 2026-09-28", lambda: record_picks(
        "2026-09-25", root=root, loaded=_send("2026-09-25", [_row("AAA", 1)]),
        allow_missing=True,
    ))
    first = _send("2026-09-28", [_row("AAA", 3, alarm=False), _row("ZZZ", 9, alarm=True)])
    assert record_picks("2026-09-28", root=root, loaded=first, allow_missing=True) == "wrote"
    raw = (root / "ledger" / "2026-09-28.picks.json").read_bytes()
    later = _send("2026-09-29", [_row("BBB", 1)])
    assert record_picks("2026-09-29", root=root, loaded=later, allow_missing=True) == "wrote"
    assert (root / "ledger" / "2026-09-28.picks.json").read_bytes() == raw
    picks = json.loads(raw)
    assert picks["recipes"]["fwd_union_hot_n4_h1"]["picks"] == ["AAA"]
    assert "ZZZ" not in picks["recipes"]["fwd_union_hot_n4_h1"]["picks"]
    mutated = _send("2026-09-28", [_row("BBB", 9)])
    _expect(ForwardError, "existing picks changed", lambda: record_picks(
        "2026-09-28", root=root, loaded=mutated, allow_missing=True,
    ))


def test_ledger_renewal_writes_zero_fees(tmp_path: Path) -> None:
    root = _install(tmp_path)
    fees = load_fees()
    d1, d2 = "2026-09-28", "2026-09-29"
    rows1 = [_row("AAA", 4.0)]
    rows1[0]["date"] = d1
    rows2 = [_row("AAA", 4.0)]
    rows2[0]["date"] = d2
    assert record_picks(d1, root=root, loaded=_send(d1, rows1), allow_missing=True) == "wrote"
    bars1 = _bars(d1, {"AAA": (10.0, 11.0), "IWM": (200.0, 201.0)})
    assert record_fills(d1, bars1, root=root, fees=fees, rows=rows1) == "wrote"
    assert record_picks(d2, root=root, loaded=_send(d2, rows2), allow_missing=True) == "wrote"
    bars2 = _bars(d2, {"AAA": (12.0, 13.0), "IWM": (202.0, 203.0)})
    assert record_fills(d2, bars2, root=root, fees=fees, rows=rows2) == "wrote"
    second = json.loads((root / "ledger" / f"{d2}.fills.json").read_text(encoding="utf-8"))
    sleeve = second["recipes"]["fwd_union_hot_n4_h1"]
    assert sleeve["sells"] == []
    assert sleeve["buys"] == []
    assert sleeve["renewals"] == ["AAA"]
    assert sleeve["fees_futubull"] == 0.0
    assert sleeve["fees_flat_15bp"] == 0.0
    assert second["fill_model"] == "keep-held"


def test_split_jump_fails_without_a_yahoo_split() -> None:
    first = _bars("2026-09-28", {"AAA": (10.0, 10.0), "IWM": (200.0, 200.0)})
    second = _bars("2026-09-29", {"AAA": (100.0, 100.0), "IWM": (201.0, 201.0)})
    _expect(ForwardError, "IRONCLAD 26", lambda: assert_split_consistent([first, second]))
    explained = _bars(
        "2026-09-29", {"AAA": (100.0, 100.0), "IWM": (201.0, 201.0)},
        splits=[{"date": "2026-09-29", "ticker": "AAA", "split": 0.1}],
    )
    assert_split_consistent([first, explained])
    assert session_jumps([first, explained])[0]["explained"] is True


def test_v4_winner_is_not_backfilled() -> None:
    recipe = {
        "exit_when": {}, "forbid": {"alarm": True}, "hold": 1, "name": "fwd_v4_example",
        "note": "", "rank": "hot_score", "require": {}, "side": "long", "top_n": 4,
        "trades_at_open": True, "universe": "union",
    }
    from src.forward_shadow_v1 import recipe_fingerprint
    winner = {
        "fingerprint_commit": "d" * 40,
        "fingerprint_sha256": recipe_fingerprint(recipe),
        "first_session": "2026-09-28",
        "name": "fwd_v4_example",
        "recipe": recipe,
    }
    _expect(ForwardError, "backfill", lambda: validate_winner(winner, last_session="2026-09-28"))
    validate_winner(winner, last_session=None, commit_day="2026-09-27")
    _expect(ForwardError, "after the fingerprint", lambda: validate_winner(
        winner, last_session=None, commit_day="2026-09-28",
    ))


def test_append_only_guard() -> None:
    def line(kind: str, session: str, body: bytes, bars: bytes | None = None) -> str:
        row = {
            "file": f"ledger/{session}.{kind}.json",
            "fill_model": "keep-held",
            "kind": kind,
            "session": session,
            "sha256": __import__("hashlib").sha256(body).hexdigest(),
            "spec_sha256": "a" * 64,
        }
        if bars is not None:
            row["bars"] = f"bars/{session}.json"
            row["bars_sha256"] = __import__("hashlib").sha256(bars).hexdigest()
        return json.dumps(row, sort_keys=True, separators=(",", ":"))

    picks = b'{"kind":"picks"}\n'
    fills = b'{"kind":"fills"}\n'
    bars = b'{"session":"2026-09-28"}\n'
    base_line = line("picks", "2026-09-28", picks)
    base_files = {"ledger/2026-09-28.picks.json": picks}
    head_files = dict(base_files)
    head_files["ledger/2026-09-28.fills.json"] = fills
    head_files["bars/2026-09-28.json"] = bars
    head = "\n".join([
        base_line,
        line("fills", "2026-09-28", fills, bars),
    ])
    assert_manifest(base_line, head, base_files, head_files)
    _expect(AppendOnlyError, "existing per-day file changed", lambda: assert_manifest(
        base_line, base_line, base_files, {"ledger/2026-09-28.picks.json": b"nope\n"},
    ))
    _expect(AppendOnlyError, "no day before 2026-09-28", lambda: assert_manifest(
        "", line("picks", "2026-09-25", picks), {}, {"ledger/2026-09-25.picks.json": picks},
    ))
    _expect(AppendOnlyError, "reordered", lambda: assert_manifest(
        "\n".join([base_line, line("picks", "2026-09-29", picks)]),
        "\n".join([line("picks", "2026-09-29", picks), base_line]),
        base_files, base_files,
    ))
    assert_hook(b'{"winners":[]}\n', b'{"winners":[]}\n', None)
    added = json.dumps({"winners": [{"first_session": "2026-09-28", "name": "fwd_v4_example"}]})
    _expect(AppendOnlyError, "backfill", lambda: assert_hook(
        b'{"winners":[]}\n', added.encode(), "2026-09-28",
    ))


def test_earliest_commit_ignores_a_later_rewrite(tmp_path: Path) -> None:
    if tmp_path.exists():
        shutil.rmtree(tmp_path)
    repo = tmp_path / "repo"
    repo.mkdir(parents=True)
    subprocess.check_call(["git", "init"], cwd=repo, stdout=subprocess.DEVNULL)
    subprocess.check_call(["git", "config", "user.email", "t@example.com"], cwd=repo)
    subprocess.check_call(["git", "config", "user.name", "t"], cwd=repo)
    rel = Path("data/factor_mine/send_inputs")
    (repo / rel).mkdir(parents=True)
    path = repo / rel / "2026-09-28.json"
    path.write_text(json.dumps({"date": "2026-09-28", "source": "panel", "rows": [{"ticker": "OLD"}]}), encoding="utf-8")
    subprocess.check_call(["git", "add", str(path.relative_to(repo))], cwd=repo)
    subprocess.check_call(["git", "commit", "-m", "first"], cwd=repo, stdout=subprocess.DEVNULL)
    path.write_text(json.dumps({"date": "2026-09-28", "source": "panel", "rows": [{"ticker": "NEW"}]}), encoding="utf-8")
    subprocess.check_call(["git", "add", str(path.relative_to(repo))], cwd=repo)
    subprocess.check_call(["git", "commit", "-m", "rewrite"], cwd=repo, stdout=subprocess.DEVNULL)
    loaded = earliest_send_inputs("2026-09-28", rev="HEAD", cwd=repo)
    assert loaded is not None
    assert loaded["doc"]["rows"][0]["ticker"] == "OLD"


def test_missing_send_waits_and_does_not_invent_picks(tmp_path: Path) -> None:
    root = _install(tmp_path)
    assert record_picks("2026-09-28", root=root, loaded=None, allow_missing=False) == "waiting"
    assert not (root / "ledger" / "2026-09-28.picks.json").exists()


def test_paper_free_surface() -> None:
    banned = ("webull_exec", "paper_open", "supabase", "flatten_robust", "paper_flatten")
    files = [
        ROOT / "src/forward_shadow_v1.py",
        ROOT / "src/forward_shadow_v1_bars.py",
        ROOT / "src/forward_shadow_v1_append.py",
        ROOT / ".github/workflows/forward_shadow_v1.yml",
        ROOT / ".github/workflows/forward_shadow_v1_append_only.yml",
    ]
    for path in files:
        text = path.read_text(encoding="utf-8")
        for line in text.splitlines():
            stripped = line.strip()
            if stripped.startswith(("import ", "from ")) or "python -m src." in stripped or "python3 -m src." in stripped:
                for name in banned:
                    if name in stripped:
                        raise AssertionError(f"{path.name} references {name}")
        if path.suffix == ".yml":
            for name in banned:
                assert name not in text


def main() -> None:
    test_spec_is_group3_keep_held()
    test_renewed_held_name_has_zero_trades_and_zero_fees()
    test_list_drop_sells_after_min_hold_and_keeps_before_it()
    test_hard_red_sits_new_buys_and_keeps_the_lot()
    test_no_day_before_start_and_future_file_does_not_rewrite(Path("/tmp/fwd-shadow-nobackfill"))
    test_ledger_renewal_writes_zero_fees(Path("/tmp/fwd-shadow-ledger"))
    test_split_jump_fails_without_a_yahoo_split()
    test_v4_winner_is_not_backfilled()
    test_append_only_guard()
    test_earliest_commit_ignores_a_later_rewrite(Path("/tmp/fwd-shadow-git"))
    test_missing_send_waits_and_does_not_invent_picks(Path("/tmp/fwd-shadow-wait"))
    test_paper_free_surface()
    print("ok")


if __name__ == "__main__":
    main()
