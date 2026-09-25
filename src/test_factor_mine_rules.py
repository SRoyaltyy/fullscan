"""Rule fingerprint, designed_after labels, and the ledger append guard."""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

from src import factor_mine as fm
from src import factor_mine_freeze as fmf
from src import factor_mine_rules as fmr


def test_fingerprint_locks_the_definition_and_does_not_rescore() -> None:
    rec = fm.make_recipe(
        "rules_probe_h1", hold=1, top_n=4, require={"vol": "good"},
        forbid={"alarm": True}, rank="hot_score",
    )
    again = fm.make_recipe(
        "rules_probe_h1", hold=1, top_n=4, require={"vol": "good"},
        forbid={"alarm": True}, rank="hot_score",
    )
    assert fmr.recipe_fingerprint(rec) == fmr.recipe_fingerprint(again)
    changed = fm.make_recipe(
        "rules_probe_h1", hold=1, top_n=4, require={"vol": "bad"},
        forbid={"alarm": True}, rank="hot_score",
    )
    assert fmr.recipe_fingerprint(changed) != fmr.recipe_fingerprint(rec)
    body = fmr.recipe_body(rec)
    assert body["same_bar"] == "stop_first"
    assert body["fee_model_sha256"]
    text = Path(fmr.__file__).read_text(encoding="utf-8")
    assert "does not rescore" in text.lower()
    with tempfile.TemporaryDirectory() as raw:
        old = fmf.MANIFEST_PATH
        fmf.MANIFEST_PATH = Path(raw) / "freeze_manifest.json"
        try:
            man = fmr.lock_recipe_rules([rec], write=True, locked_on="2026-09-25")
            digest = fmr.recipe_fingerprint(rec)
            assert man["recipe_rules"]["rules_probe_h1"]["sha256"] == digest
            assert man["recipe_rules"]["rules_probe_h1"]["same_bar"] == "stop_first"
            fmr.lock_recipe_rules([again], write=True, locked_on="2026-09-25")
            disk = json.loads(fmf.MANIFEST_PATH.read_text(encoding="utf-8"))
            assert disk["recipe_rules"]["rules_probe_h1"]["sha256"] == digest
            assert disk.get("snapshots") == {}
            try:
                fmr.lock_recipe_rules([changed], write=True)
                raised = False
            except fmr.RuleDrift as exc:
                raised = True
                assert "not rescoring" in str(exc).lower()
                assert "new name" in str(exc).lower()
            assert raised
            kept = json.loads(fmf.MANIFEST_PATH.read_text(encoding="utf-8"))
            assert kept["recipe_rules"]["rules_probe_h1"]["sha256"] == digest
        finally:
            fmf.MANIFEST_PATH = old


def test_designed_after_keeps_pre_creation_days_out_of_the_real_total() -> None:
    assert fm.recipe_created_on("union_hot_n4_holdup") == "2026-09-21"
    assert fm.recipe_created_on("union_h1") == "2026-08-13"
    assert fm.recipe_created_on("overnight_mega_h1") == "2026-09-21"
    assert fm.recipe_created_on("flatten_h5_s8") == "2026-09-14"
    assert fmr.is_designed_after("union_hot_n4_holdup", "2026-09-18")
    assert not fmr.is_designed_after("union_hot_n4_holdup", "2026-09-21")
    assert not fmr.is_designed_after("union_h1", "2026-08-13")
    combo = {"name": "combo_oh_5050_shared", "members": [
        "overnight_mega_h1", "union_hot_n4_holdup",
    ]}
    assert fm.recipe_created_on("combo_oh_5050_shared", combo) == "2026-09-21"
    dates = ["2026-08-13", "2026-09-18", "2026-09-21", "2026-09-24"]
    assert fmr.first_real_day("union_hot_n4_holdup", dates) == "2026-09-21"
    rows = [
        {"date": "2026-09-18", "mean": 10.0},
        {"date": "2026-09-21", "mean": 5.0},
        {"date": "2026-09-24", "mean": -2.0},
    ]
    split = fmr.split_means("union_hot_n4_holdup", rows)
    assert split["n_designed_after"] == 1
    assert split["n_real"] == 2
    assert split["designed_after_compound_pct"] == 10.0
    # 1.05 * 0.98 - 1 = 0.029
    assert split["real_compound_pct"] == 2.9
    mixed = fmr.compound_pct([10.0, 5.0, -2.0])
    assert mixed != split["real_compound_pct"]


def test_append_guard_allows_one_new_recipe_and_refuses_a_rewrite() -> None:
    prior = {
        "date": "2026-09-24",
        "recipes": {
            "union_hot_n4_h1": {
                "buys": [{"ticker": "GLND", "side": "BUY", "shares": 10, "price": 2.7}],
                "sells": [{"ticker": "FEAM", "side": "SELL", "shares": 4, "price": 2.61}],
                "trades": [
                    {"ticker": "FEAM", "side": "SELL", "shares": 4, "price": 2.61},
                    {"ticker": "GLND", "side": "BUY", "shares": 10, "price": 2.7},
                ],
                "equity": 12499.09,
                "mean": 18.1549,
            },
        },
    }
    fmr.assert_ledger_append(prior, json.loads(json.dumps(prior)))
    added = json.loads(json.dumps(prior))
    added["recipes"]["union_hot_n4_holdup"] = {
        "buys": [{"ticker": "INDP", "side": "BUY", "shares": 3, "price": 3.1}],
        "sells": [],
        "trades": [{"ticker": "INDP", "side": "BUY", "shares": 3, "price": 3.1}],
        "equity": 10100.0,
        "mean": 1.0,
    }
    fmr.assert_ledger_append(prior, added)
    changed = json.loads(json.dumps(prior))
    changed["recipes"]["union_hot_n4_h1"]["buys"] = [
        {"ticker": "AMD", "side": "BUY", "shares": 10, "price": 2.7},
    ]
    try:
        fmr.assert_ledger_append(prior, changed)
        raised = False
    except fmr.AppendDrift:
        raised = True
    assert raised
    pnl = json.loads(json.dumps(prior))
    pnl["recipes"]["union_hot_n4_h1"]["mean"] = 1.0
    try:
        fmr.assert_ledger_append(prior, pnl)
        raised = False
    except fmr.AppendDrift as exc:
        raised = True
        assert "pnl" in str(exc) or "rewrite" in str(exc)
    assert raised
    dropped = {"date": "2026-09-24", "recipes": {}}
    try:
        fmr.assert_ledger_append(prior, dropped)
        raised = False
    except fmr.AppendDrift:
        raised = True
    assert raised
    with tempfile.TemporaryDirectory() as raw:
        path = Path(raw) / "2026-09-24.json"
        fmr.write_day_ledger("2026-09-24", prior, path=path)
        first = path.read_bytes()
        fmr.write_day_ledger("2026-09-24", json.loads(json.dumps(prior)), path=path)
        assert path.read_bytes() == first
        try:
            fmr.write_day_ledger("2026-09-24", changed, path=path)
            raised = False
        except fmr.AppendDrift:
            raised = True
        assert raised
        assert path.read_bytes() == first


def test_scoreboard_writer_refuses_a_locked_day_and_accepts_a_new_name() -> None:
    payload = {
        "n_recipes": 1,
        "dates": ["2026-09-24"],
        "stats": [{"name": "union_h1"}],
        "recipes": [fm.make_recipe("union_h1", hold=1)],
        "daily": {"union_h1": [{
            "date": "2026-09-24", "bought": ["OLD"], "sold": [],
            "equity": 10050.0, "mean": 0.5,
        }]},
        "books": {"union_h1": {"trades": [{
            "date": "2026-09-24", "ticker": "OLD", "side": "BUY",
            "shares": 10, "price": 5.0,
        }]}},
        "starts": {"union_h1": []},
        "probe": {},
        "sim": {},
        "series": {"union_h1": [10050.0]},
    }
    with tempfile.TemporaryDirectory() as raw:
        dest = Path(raw) / "factor_mine.json"
        fm.write_scoreboard(payload, dest)
        fm.write_scoreboard(json.loads(json.dumps(payload)), dest)
        changed = json.loads(json.dumps(payload))
        changed["daily"]["union_h1"][0]["bought"] = ["NEW"]
        try:
            fm.write_scoreboard(changed, dest)
            raised = False
        except fmr.AppendDrift as exc:
            raised = True
            assert "picks" in str(exc)
        assert raised
        added = json.loads(json.dumps(payload))
        added["daily"]["fresh_h1"] = [{
            "date": "2026-09-24", "bought": ["CCC"], "sold": [],
            "equity": 10000.0, "mean": 0.0,
        }]
        added["books"]["fresh_h1"] = {"trades": []}
        added["recipes"].append(fm.make_recipe("fresh_h1", hold=1))
        fm.write_scoreboard(added, dest)
        kept = fm.load_scoreboard(dest)
        assert kept["daily"]["union_h1"][0]["bought"] == ["OLD"]
        assert kept["daily"]["fresh_h1"][0]["bought"] == ["CCC"]


def test_same_bar_count_is_zero_without_both_levels() -> None:
    rows = fmr.same_bar_counts([
        fm.make_recipe("union_h1"),
        fm.make_recipe("flatten_h5_s8", stop_pct=0.08),
        fm.make_recipe("both_h1", stop_pct=0.08, take_pct=0.12),
    ])
    by_name = {row["name"]: row["same_bar_trades"] for row in rows}
    assert by_name["union_h1"] == 0
    assert by_name["flatten_h5_s8"] == 0
    assert by_name["both_h1"] is None


def main() -> None:
    test_fingerprint_locks_the_definition_and_does_not_rescore()
    test_designed_after_keeps_pre_creation_days_out_of_the_real_total()
    test_append_guard_allows_one_new_recipe_and_refuses_a_rewrite()
    test_scoreboard_writer_refuses_a_locked_day_and_accepts_a_new_name()
    test_same_bar_count_is_zero_without_both_levels()
    print("factor-mine rules tests passed")


if __name__ == "__main__":
    main()
