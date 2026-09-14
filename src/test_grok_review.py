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


def test_review_stale_on_fail_or_newer_core(tmp_path: Path) -> None:
    date = "2026-09-14"
    daily = tmp_path / "01_daily"
    daily.mkdir()
    news = daily / "news"
    news.mkdir()
    parsed = news / f"{date}_parsed.json"
    parsed.write_text('{"raw_count": 10}', encoding="utf-8")
    qc = daily / f"{date}_preopen_qc.json"
    grok = daily / f"{date}_grok_review.json"

    # Honest FAIL that is newer than core files is current, not stale.
    qc.write_text(json.dumps({"all_ok": False}), encoding="utf-8")
    grok.write_text(json.dumps({"ok": False, "fails": [{"path": "x"}]}),
                    encoding="utf-8")
    import os
    import time
    now = time.time()
    os.utime(parsed, (now, now))
    os.utime(qc, (now + 20, now + 20))
    os.utime(grok, (now + 20, now + 20))
    assert grok_review.qc_stamp_stale(date, root=tmp_path) is False
    assert grok_review.review_stale(date, root=tmp_path) is False

    qc.write_text(json.dumps({"all_ok": True}), encoding="utf-8")
    grok.write_text(json.dumps({"ok": True, "fails": []}), encoding="utf-8")
    now = time.time()
    os.utime(qc, (now + 20, now + 20))
    os.utime(grok, (now + 20, now + 20))
    os.utime(parsed, (now, now))
    assert grok_review.qc_stamp_stale(date, root=tmp_path) is False
    assert grok_review.review_stale(date, root=tmp_path) is False

    os.utime(parsed, (now + 40, now + 40))
    assert grok_review.qc_stamp_stale(date, root=tmp_path) is True
    assert grok_review.review_stale(date, root=tmp_path) is True


def test_restamp_rewrites_qc_and_grok(tmp_path: Path) -> None:
    date = "2026-09-14"
    daily = tmp_path / "01_daily"
    daily.mkdir()
    news = daily / "news"
    news.mkdir()
    (news / f"{date}_parsed.json").write_text(json.dumps({
        "raw_count": 12, "usable_count": 3,
        "usable_top": [{"title": "Fed holds"}],
        "all_items": [{"title": "x"}],
    }), encoding="utf-8")
    (daily / f"{date}_preopen_qc.json").write_text(
        json.dumps({"all_ok": False, "items": []}), encoding="utf-8")
    (daily / f"{date}_grok_review.json").write_text(
        json.dumps({"ok": False, "fails": [{"path": "parsed.json",
                                            "reason": "missing"}]}),
        encoding="utf-8")
    (daily / f"{date}_preopen_status.json").write_text(json.dumps({
        "all_ok": False, "qc_all_ok": False, "book_ok": True,
        "grok_ok": False, "missing_required": ["news_parse"],
        "qc": {"items": []},
    }), encoding="utf-8")
    (daily / f"{date}_preopen_status.md").write_text(
        f"# Pre-open ALL status — {date}\n\n"
        "all_ok=False  qc_all_ok=False  book_ok=True  grok_ok=False  "
        "missing=['news_parse']\n",
        encoding="utf-8")

    def chat(messages, **kwargs):
        return json.dumps({
            "ok": True, "fails": [],
            "notes": "parsed.json present; comms sector missing is 8-of-11",
        })

    out = grok_review.restamp(date, root=tmp_path, chat_fn=chat, force_grok=True)
    assert out["qc"]["date"] == date
    saved = json.loads((daily / f"{date}_grok_review.json").read_text())
    assert saved["ok"] is True
    assert (daily / f"{date}_grok_review.md").exists()
    status = json.loads((daily / f"{date}_preopen_status.json").read_text())
    assert status["grok_ok"] is True
    assert "news_parse" not in (status.get("missing_required") or [])


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
            test_review_stale_on_fail_or_newer_core,
            test_restamp_rewrites_qc_and_grok,
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
