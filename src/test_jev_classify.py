"""Hop-1 classify: locked enum, no tickers, no hop-0 drift."""
from __future__ import annotations

import json
from pathlib import Path
from unittest import mock

from src.jev_classify import (
    CLASS_QUESTION,
    FAMILIES,
    FORBIDDEN_HOP1_BITS,
    QUESTIONS,
    apply_code_classify,
    apply_jev_classify,
    class_reason,
    classification_json,
    classify_keep,
    classes_by_family,
    code_classify,
    decide_classify,
    empty_class_fields,
    gold_check,
    gold_rows,
    questions_are_hop1,
)
from src.jev_gate import FORBIDDEN_QUESTION_BITS, QUESTIONS as HOP0_QUESTIONS
from src.news_impact.schema import EVENT_CLASSES, Q5_STATUS, SIGNS, family_of

ROOT = Path(__file__).resolve().parent.parent


def test_hop0_pack_still_forbids_classify_bits():
    blob = json.dumps(HOP0_QUESTIONS).lower()
    for bit in FORBIDDEN_QUESTION_BITS:
        assert bit not in blob
    assert "q5" not in HOP0_QUESTIONS
    assert "event_class" not in HOP0_QUESTIONS


def test_hop1_pack_is_the_locked_taxonomy():
    questions_are_hop1()
    blob = json.dumps(QUESTIONS).lower()
    for bit in FORBIDDEN_HOP1_BITS:
        assert bit not in blob
    assert set(QUESTIONS["q5"]["criteria"]) == set(Q5_STATUS)
    assert set(QUESTIONS["family"]["criteria"]) == set(FAMILIES)
    named = set()
    for key in CLASS_QUESTION:
        named.update(QUESTIONS[key]["criteria"])
    assert named == set(EVENT_CLASSES)
    by_fam = classes_by_family()
    for key, fam in CLASS_QUESTION.items():
        assert set(QUESTIONS[key]["criteria"]) == set(by_fam[fam])
    sign_choices = set(QUESTIONS["sign"]["criteria"])
    assert sign_choices == set(SIGNS) | {"none"}


def test_decide_classify_uses_family_then_class():
    got = decide_classify(
        "FDA approves Amneal lanreotide injection",
        {
            "q5": "impulse",
            "family": "permission",
            "class_permission": "gate",
            "class_print": "guidance",
            "sign": "open",
        },
    )
    assert got.event_class == "gate"
    assert got.q5 == "impulse"
    assert got.sign == "open"
    assert got.family == "permission"
    assert "ticker" not in got.why


def test_family_class_mismatch_falls_to_family_default():
    got = decide_classify(
        "Union Pacific to acquire Norfolk Southern in a cash deal",
        {
            "q5": "impulse",
            "family": "firm",
            "class_blast": "blast_ops",
            "sign": "none",
        },
    )
    assert got.event_class == "corporate_action_mna"
    assert got.family == "firm"
    assert got.sign is None


def test_regime_time_defaults():
    weather = decide_classify(
        "Strait of Hormuz remains closed",
        {"q5": "regime", "family": "time", "sign": "none"},
    )
    assert weather.event_class == "regime_state"
    assert weather.q5 == "regime"
    brk = decide_classify(
        "Hormuz ceasefire as hulls move",
        {"q5": "regime_break", "family": "time", "sign": "lift"},
    )
    assert brk.event_class == "regime_break"
    assert brk.q5 == "regime_break"
    assert brk.sign == "lift"


def test_classify_keep_jev_overrides_code_when_valid():
    row = {"title": "Fed holds rates at 4.25 percent after FOMC meeting"}
    code = classify_keep(row, None)
    assert code["class_source"] == "code"
    assert code["event_class"] in EVENT_CLASSES
    jev = classify_keep(
        row,
        {
            "q5": "impulse",
            "family": "print",
            "class_print": "factor_impulse",
            "sign": "none",
        },
    )
    assert jev["class_source"] == "jev"
    assert jev["event_class"] == "factor_impulse"
    assert jev["q5"] == "impulse"
    assert jev["class_reason"] == "factor_impulse|impulse"
    assert classification_json(jev)["sign"] is None


def test_gold_fixture_matches_locked_enum():
    report = gold_check()
    assert report["ok"], [row for row in report["rows"] if not row["ok"]]
    assert report["n"] >= 12
    ids = {row["id"] for row in gold_rows()}
    assert {"fda", "fomc", "hormuz_weather", "stock_day", "tsa"} <= ids
    for item in gold_rows():
        expect = item["expect"]
        assert expect["event_class"] in EVENT_CLASSES
        assert expect["q5"] in Q5_STATUS
        assert family_of(expect["event_class"]) == expect["family"]


def test_code_prior_knows_the_gold_titles():
    by_id = {item["id"]: item for item in gold_rows()}
    gate = code_classify(by_id["fda"]["title"])
    assert gate.event_class == "gate"
    assert gate.q5 == "impulse"
    weather = code_classify(by_id["hormuz_weather"]["title"])
    assert weather.event_class == "regime_state"
    trash = code_classify(by_id["stock_day"]["title"])
    assert trash.event_class == "discard"


def test_apply_code_classify_stamps_keeps_only():
    rows = [
        {"title": "FDA approves Amneal lanreotide injection", "decision": "keep",
         "reason": "code_instrument"},
        {"title": "Why this is the stock of the day", "decision": "drop",
         "reason": "opinion"},
    ]
    apply_code_classify(rows)
    assert rows[0]["event_class"] == "gate"
    assert rows[0]["class_reason"] == class_reason(
        rows[0]["event_class"], rows[0]["q5"], rows[0]["sign"]
    )
    assert rows[0]["reason"] == "code_instrument"
    assert rows[1]["event_class"] == ""
    assert rows[1]["class_reason"] == ""
    assert rows[1]["reason"] == "opinion"
    empty = empty_class_fields()
    assert rows[1]["q5"] == empty["q5"]


def test_apply_jev_classify_only_asks_on_keeps():
    calls = []

    def poster(state, questions, key):
        calls.append(questions)
        return {
            "model": "jev-test",
            "answers": {
                "q5": {"type": "choice", "choice": "impulse"},
                "family": {"type": "choice", "choice": "permission"},
                "class_permission": {"type": "choice", "choice": "gate"},
                "sign": {"type": "choice", "choice": "open"},
            },
        }

    rows = [
        {"title": "FDA approves Amneal lanreotide injection", "decision": "keep",
         "reason": "code_instrument"},
        {"title": "A recap of yesterday", "decision": "drop", "reason": "opinion"},
    ]
    apply_jev_classify(rows, key="x", workers=1, poster=poster)
    assert len(calls) == 1
    assert "q5" in calls[0]
    assert "is_opinion" not in calls[0]
    assert rows[0]["event_class"] == "gate"
    assert rows[0]["class_source"] == "jev"
    assert not rows[1].get("event_class")


def test_classification_json_is_the_lane_shape():
    row = classify_keep(
        {"title": "FDA approves Amneal lanreotide injection"},
        {
            "q5": "impulse",
            "family": "permission",
            "class_permission": "gate",
            "sign": "open",
        },
    )
    blob = classification_json(row)
    assert set(blob) == {
        "event_class", "sign", "q5", "constraint", "split", "split_facts", "why",
    }
    assert blob["event_class"] == "gate"
    assert blob["q5"] == "impulse"
    assert blob["sign"] == "open"
    assert blob["split"] is False
    assert blob["split_facts"] == []
    dumped = json.dumps(blob).lower()
    assert "ticker" not in dumped
    assert "winner" not in dumped


def test_no_keep_json_in_repo_root():
    assert not (ROOT / "keep.json").exists()
    assert not list((ROOT / "00_grounding").glob("keep.json"))


def test_hop1_poster_does_not_see_hop0_questions():
    with mock.patch("src.jev_classify.jev_post") as post:
        post.return_value = {
            "model": "jev-test",
            "answers": {
                "q5": {"type": "choice", "choice": "regime"},
                "family": {"type": "choice", "choice": "time"},
                "class_time": {"type": "choice", "choice": "discard"},
                "sign": {"type": "choice", "choice": "none"},
            },
        }
        apply_jev_classify(
            [{"title": "stock of the day", "decision": "keep", "reason": "core_material"}],
            key="x", workers=1,
        )
    questions = post.call_args[0][1]
    assert "q5" in questions
    assert "is_opinion" not in questions
    assert "event_class" not in questions


def main() -> None:
    tests = [
        test_hop0_pack_still_forbids_classify_bits,
        test_hop1_pack_is_the_locked_taxonomy,
        test_decide_classify_uses_family_then_class,
        test_family_class_mismatch_falls_to_family_default,
        test_regime_time_defaults,
        test_classify_keep_jev_overrides_code_when_valid,
        test_gold_fixture_matches_locked_enum,
        test_code_prior_knows_the_gold_titles,
        test_apply_code_classify_stamps_keeps_only,
        test_apply_jev_classify_only_asks_on_keeps,
        test_classification_json_is_the_lane_shape,
        test_no_keep_json_in_repo_root,
        test_hop1_poster_does_not_see_hop0_questions,
    ]
    failed = 0
    for fn in tests:
        try:
            fn()
            print("ok", fn.__name__)
        except Exception as exc:
            failed += 1
            print("FAIL", fn.__name__, type(exc).__name__, exc)
    if failed:
        raise SystemExit(failed)


if __name__ == "__main__":
    main()
