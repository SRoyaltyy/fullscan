"""Unseen-100 stop gold: shape, teacher formula, no banned teacher keeps."""
from __future__ import annotations

import json
from pathlib import Path

from .jev_bits import BIT_QUESTIONS
from .jev_sixbit_stop import (
    BANNED_KEEP,
    KEEP_LABELS,
    STOP_PATH,
    load_stop,
    score,
    teacher_keep,
)

LOCK_PATH = Path(__file__).with_name("jev_bits_lock.json")
SHEETS = (
    "20260930_0858_draw.json",
    "20260930_0932_draw.json",
    "20260930_0943_draw.json",
    "20260930_0952_draw.json",
)


def _seen_titles() -> set[str]:
    seen = set()
    lock = json.loads(LOCK_PATH.read_text(encoding="utf-8"))
    for item in lock.get("items") or []:
        seen.add((item.get("title") or "").strip().lower())
    root = Path(__file__).resolve().parent.parent / "00_grounding" / "jev_train"
    for name in SHEETS:
        path = root / name
        if not path.is_file():
            continue
        blob = json.loads(path.read_text(encoding="utf-8"))
        for item in blob.get("items") or []:
            seen.add((item.get("title") or "").strip().lower())
    for spec in BIT_QUESTIONS.values():
        crit = spec.get("criteria") or {}
        for side in ("true", "false"):
            for chunk in str(crit.get(side) or "").split(". "):
                chunk = chunk.strip(" .")
                if len(chunk) >= 24:
                    seen.add(chunk.lower())
    return seen


def test_stop_gold_is_fresh_100() -> None:
    blob = load_stop()
    assert blob.get("schema") == "jev-sixbit-stop-1"
    items = blob["items"]
    assert len(items) == 100
    titles = [it["title"] for it in items]
    assert len(set(titles)) == 100
    seen = _seen_titles()
    overlap = [t for t in titles if t.strip().lower() in seen]
    assert not overlap, overlap
    assert all(it["label"] in {
        "tape", "preview", "finished_act",
        "official_print", "officer_voice", "junk",
    } for it in items)


def test_teacher_keeps_are_print_or_done_only() -> None:
    items = load_stop()["items"]
    keeps = [it for it in items if teacher_keep(it["label"])]
    assert keeps
    assert all(it["label"] in KEEP_LABELS for it in keeps)
    banned = [it["title"] for it in keeps if any(
        bit in it["title"].lower() for bit in BANNED_KEEP
    )]
    assert not banned, banned


def test_score_perfect_formula_answers() -> None:
    items = load_stop()["items"]
    decided = []
    for it in items:
        if it["label"] in {"official_print", "officer_voice"}:
            answers = {k: 0.05 for k in BIT_QUESTIONS}
            answers["print"] = 0.9
        elif it["label"] == "finished_act":
            answers = {k: 0.05 for k in BIT_QUESTIONS}
            answers["done"] = 0.9
        elif it["label"] == "tape":
            answers = {k: 0.05 for k in BIT_QUESTIONS}
            answers["tape"] = 0.9
        elif it["label"] == "preview":
            answers = {k: 0.05 for k in BIT_QUESTIONS}
            answers["soft"] = 0.9
        else:
            answers = {k: 0.05 for k in BIT_QUESTIONS}
        from .jev_bits import decide
        decided.append(decide({"title": it["title"]}, answers))
    report = score(items, decided)
    assert report["pass"]
    assert report["precision"] == 1.0
    assert report["recall_print_done"] == 1.0
    assert not report["banned_keeps"]


def main() -> None:
    tests = [
        test_stop_gold_is_fresh_100,
        test_teacher_keeps_are_print_or_done_only,
        test_score_perfect_formula_answers,
    ]
    failed = 0
    for fn in tests:
        try:
            fn()
            print("ok", fn.__name__)
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print("FAIL", fn.__name__, type(exc).__name__, exc)
    if failed:
        raise SystemExit(failed)


if __name__ == "__main__":
    main()
