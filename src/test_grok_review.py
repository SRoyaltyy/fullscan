"""Grok text-review tests. No live LLM.

Run: python -m src.test_grok_review
"""
from __future__ import annotations

import json
from pathlib import Path

from src import grok_review


def test_parse_ok_json() -> None:
    v = grok_review.parse_verdict(
        '{"ok": true, "fails": [], "notes": "all real"}'
    )
    assert v["ok"] and v["fails"] == []


def test_parse_fenced_and_fails() -> None:
    text = (
        "here you go\n```json\n"
        '{"ok": false, "fails": [{"path": "x.md", "reason": "timeout stub"}],'
        ' "notes": "stub"}\n```\n'
    )
    v = grok_review.parse_verdict(text)
    assert not v["ok"]
    assert v["fails"][0]["path"] == "x.md"


def test_parse_empty_and_timeout_fail_closed() -> None:
    assert not grok_review.parse_verdict("").get("ok")
    stub = (
        "LLM request timed out.\n\n"
        "The model did not produce a response before the model idle timeout."
    )
    v = grok_review.parse_verdict(stub)
    assert not v["ok"]
    assert v["fails"][0]["reason"] == "timeout_stub"


def test_parse_unparseable_fail_closed() -> None:
    v = grok_review.parse_verdict("looks fine to me, ship it")
    assert not v["ok"]
    assert v["fails"][0]["reason"] == "unparseable_verdict"


def test_ok_true_with_fails_is_fail() -> None:
    v = grok_review.parse_verdict(
        '{"ok": true, "fails": [{"path": "a", "reason": "carry"}], "notes": ""}'
    )
    assert not v["ok"]


def test_bundle_includes_missing_and_present(tmp_path: Path) -> None:
    date = "2026-08-25"
    gen = tmp_path / "01_daily" / "general"
    gen.mkdir(parents=True)
    (gen / f"{date}_predict.md").write_text(
        "MEMORY_CONFIRM\nSCORES_BEGIN\nreal essay\nSCORES_END\n",
        encoding="utf-8",
    )
    prompt = grok_review.bundle_preopen(date, root=tmp_path)
    assert f"TODAY (America/New_York) = {date}" in prompt
    assert f"01_daily/general/{date}_predict.md" in prompt
    assert "real essay" in prompt
    assert "MISSING" in prompt  # sectors / events not written


def test_review_pass_and_fail(tmp_path: Path) -> None:
    date = "2026-08-25"
    (tmp_path / "01_daily").mkdir()

    def ok_chat(messages, **kwargs):
        assert kwargs.get("tools") is False
        assert "hostile" in messages[0]["content"].lower()
        return '{"ok": true, "fails": [], "notes": "packet is real"}'

    payload = grok_review.review_preopen(
        date, root=tmp_path, chat_fn=ok_chat,
    )
    assert payload["ok"]
    assert grok_review.prior_ok(date, root=tmp_path)
    saved = json.loads(
        (tmp_path / "01_daily" / f"{date}_grok_review.json").read_text()
    )
    assert saved["ok"] is True

    def fail_chat(messages, **kwargs):
        return json.dumps({
            "ok": False,
            "fails": [{"path": "01_daily/events/x.json",
                       "reason": "CARRIED FORWARD from Sunday"}],
            "notes": "events are a carry",
        })

    payload = grok_review.review_preopen(
        date, root=tmp_path, chat_fn=fail_chat,
    )
    assert not payload["ok"]
    assert not grok_review.prior_ok(date, root=tmp_path)


def test_review_empty_reply_fail_closed(tmp_path: Path) -> None:
    date = "2026-08-25"
    (tmp_path / "01_daily").mkdir()
    payload = grok_review.review_preopen(
        date, root=tmp_path, chat_fn=lambda *a, **k: "",
    )
    assert not payload["ok"]


def test_clip_keeps_head_and_tail() -> None:
    text = "A" * 9000 + "MID" + "B" * 4000
    out = grok_review._clip(text, 13000)
    assert out.startswith("A" * 9000)
    assert out.endswith("B" * 4000)
    assert "clipped" in out
    assert "MID" not in out


def main() -> None:
    # tmp_path-style tests need a real tmp dir when run as __main__
    import tempfile
    import traceback
    passed = 0
    failed = 0
    tests = [
        test_parse_ok_json,
        test_parse_fenced_and_fails,
        test_parse_empty_and_timeout_fail_closed,
        test_parse_unparseable_fail_closed,
        test_ok_true_with_fails_is_fail,
        test_clip_keeps_head_and_tail,
    ]
    for fn in tests:
        try:
            fn()
            print(f"  ok  {fn.__name__}")
            passed += 1
        except Exception as e:  # noqa: BLE001
            print(f"  FAIL {fn.__name__}: {e}")
            traceback.print_exc()
            failed += 1
    with tempfile.TemporaryDirectory() as td:
        p = Path(td)
        for fn in (
            test_bundle_includes_missing_and_present,
            test_review_pass_and_fail,
            test_review_empty_reply_fail_closed,
        ):
            try:
                fn(p)
                print(f"  ok  {fn.__name__}")
                passed += 1
            except Exception as e:  # noqa: BLE001
                print(f"  FAIL {fn.__name__}: {e}")
                traceback.print_exc()
                failed += 1
            # reset between tests that share the tmp root
            for child in p.iterdir():
                if child.is_dir():
                    import shutil
                    shutil.rmtree(child)
                else:
                    child.unlink()
    print(f"{passed} passed, {failed} failed")
    raise SystemExit(failed)


if __name__ == "__main__":
    main()
