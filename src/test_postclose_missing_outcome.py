"""Post-close must not stay green when the session has no outcome.

An empty LLM reply or a DeepSeek 402 makes run_outcome return without
writing ``*_outcome.md``. The other stages still run and the pack is
still pushed. The process then exits 1 and prints OUTCOME MISSING <date>.
A missing general reflect does the same. Other gaps stay DEGRADED / exit 0.

Run: PYTHONPATH=. python3 -m src.test_postclose_missing_outcome
"""
from __future__ import annotations

import io
import os
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from src import run_postclose_all as pc

ROOT = Path(__file__).resolve().parent.parent
WF = ROOT / ".github" / "workflows"
DATE = "2099-01-05"


def _line(text: str, exact: str) -> bool:
    return any(line.strip() == exact for line in text.splitlines())


def _patched_body(date: str, fake_run, **check_overrides):
    """Run one session with subprocesses replaced. Returns (code, stdout)."""
    prev = os.environ.get("FINVIZ_SKIP_LIVE")
    patches = [
        mock.patch.object(pc, "_run", side_effect=fake_run),
        mock.patch.object(pc, "morning_box_yield", return_value=False),
        mock.patch.object(pc, "seconds_until_yield", return_value=None),
    ]
    for name, value in check_overrides.items():
        patches.append(mock.patch.object(pc.skip_if_good, name, return_value=value))
    buf = io.StringIO()
    try:
        with redirect_stdout(buf):
            for patch in patches:
                patch.start()
            try:
                code = pc._run_one_body(date, force=False)
            finally:
                for patch in reversed(patches):
                    patch.stop()
    finally:
        if prev is None:
            os.environ.pop("FINVIZ_SKIP_LIVE", None)
        else:
            os.environ["FINVIZ_SKIP_LIVE"] = prev
    return code, buf.getvalue()


def _record_run(codes: dict[str, int] | None = None):
    seen: list[str] = []

    def fake_run(cmd, timeout_s=None):
        line = " ".join(cmd)
        seen.append(line)
        print("RAN " + line, flush=True)
        if codes:
            for key, value in codes.items():
                if key in line:
                    return value
        return 0

    return seen, fake_run


def test_missing_outcome_and_reflect_still_run_later_stages() -> None:
    """Empty outcome child (exit 0, no file) must not end the pack green."""
    outcome = ROOT / "01_daily" / "general" / f"{DATE}_outcome.md"
    reflect = ROOT / "01_daily" / "general" / f"{DATE}_reflect.md"
    assert not outcome.exists()
    assert not reflect.exists()
    seen, fake_run = _record_run()
    code, text = _patched_body(DATE, fake_run)
    assert code == 1, text
    assert _line(text, f"[postclose-all] OUTCOME MISSING {DATE}")
    assert _line(text, f"[postclose-all] REFLECT MISSING {DATE}")
    assert "DEGRADED" in text
    joined = "\n".join(seen)
    for needle in (
        "src.run_outcome",
        "src.run_reflect",
        "src.run_sector_outcome",
        "src.learn_cycle",
        "src.map_heat_postclose",
        "safe_git_push.sh",
    ):
        assert needle in joined, needle
    # The last persist happens, then the failure lines. Later stages
    # are not skipped because the outcome child exited 0.
    assert text.rfind("safe_git_push.sh") < text.find(f"OUTCOME MISSING {DATE}")
    assert text.find("src.map_heat_postclose") < text.find(f"OUTCOME MISSING {DATE}")
    assert text.find("src.learn_cycle") < text.find(f"OUTCOME MISSING {DATE}")
    assert text.find(f"OUTCOME MISSING {DATE}") < text.find(f"REFLECT MISSING {DATE}")


def test_reflect_missing_fails_when_outcome_landed() -> None:
    seen, fake_run = _record_run()
    code, text = _patched_body(
        DATE, fake_run,
        check_daily_pipeline_outcome=True,
        check_general_reflect=False,
    )
    assert code == 1, text
    assert _line(text, f"[postclose-all] REFLECT MISSING {DATE}")
    assert f"OUTCOME MISSING {DATE}" not in text
    joined = "\n".join(seen)
    assert "src.run_outcome" not in joined
    assert "src.run_reflect" in joined
    assert "src.learn_cycle" in joined
    assert text.rfind("safe_git_push.sh") < text.find(f"REFLECT MISSING {DATE}")


def test_outcome_missing_alone_names_that_date() -> None:
    seen, fake_run = _record_run({"src.run_outcome": 0})
    code, text = _patched_body(
        DATE, fake_run,
        check_daily_pipeline_outcome=False,
        check_general_reflect=True,
    )
    assert code == 1, text
    assert _line(text, f"[postclose-all] OUTCOME MISSING {DATE}")
    assert f"REFLECT MISSING {DATE}" not in text
    joined = "\n".join(seen)
    assert "src.run_outcome" in joined
    assert "src.run_reflect" not in joined
    assert "src.map_heat_postclose" in joined
    assert text.find("src.map_heat_postclose") < text.find(f"OUTCOME MISSING {DATE}")


def test_other_gaps_stay_degraded_and_green() -> None:
    """Missing sectors / learn / captains still persist and exit 0."""
    seen, fake_run = _record_run()
    code, text = _patched_body(
        DATE, fake_run,
        check_daily_pipeline_outcome=True,
        check_general_reflect=True,
    )
    assert code == 0, text
    assert "DEGRADED" in text
    assert "OUTCOME MISSING" not in text
    assert "REFLECT MISSING" not in text
    joined = "\n".join(seen)
    assert "src.learn_cycle" in joined
    assert "src.map_heat_postclose" in joined
    assert "safe_git_push.sh" in joined


def test_complete_pack_does_not_rerun_or_fail() -> None:
    def boom(*_a, **_k):
        raise AssertionError("complete pack must not start stages")

    code, text = _patched_body(DATE, boom, check_postclose_all=True)
    assert code == 0, text
    assert "nothing to do" in text
    assert "OUTCOME MISSING" not in text
    assert "REFLECT MISSING" not in text


def test_close_pack_pushes_before_the_missing_line() -> None:
    pushed: list[str] = []

    def push(date: str) -> None:
        pushed.append(date)
        print(f"PUSHED {date}", flush=True)

    with mock.patch.object(pc, "_push_pack", side_effect=push), \
         mock.patch.object(pc.skip_if_good, "check_daily_pipeline_outcome",
                           return_value=False), \
         mock.patch.object(pc.skip_if_good, "check_general_reflect",
                           return_value=True), \
         mock.patch.object(pc.skip_if_good, "check_postclose_all",
                           return_value=False):
        buf = io.StringIO()
        with redirect_stdout(buf):
            code = pc._close_pack(DATE)
    text = buf.getvalue()
    assert code == 1
    assert pushed == [DATE]
    assert text.index(f"PUSHED {DATE}") < text.index(f"OUTCOME MISSING {DATE}")
    assert _line(text, f"[postclose-all] OUTCOME MISSING {DATE}")
    assert "REFLECT MISSING" not in text


def test_run_finishes_later_dates_then_fails() -> None:
    seen: list[str] = []

    def one(date: str, force: bool = False) -> int:
        seen.append(date)
        return 1 if date.endswith("02") else 0

    with mock.patch.object(pc.config, "apply_llm_backend", return_value="auto"), \
         mock.patch.object(pc, "night_pack_dates",
                           return_value=["2099-01-02", "2099-01-05"]), \
         mock.patch.object(pc, "_run_one", side_effect=one):
        code = pc.run()
    assert seen == ["2099-01-02", "2099-01-05"]
    assert code == 1


def test_main_exits_with_the_pack_code() -> None:
    with mock.patch.object(pc, "run", return_value=1), \
         mock.patch("sys.argv", ["run_postclose_all", "--date", DATE]):
        try:
            pc.main()
        except SystemExit as exc:
            assert exc.code == 1
        else:
            raise AssertionError("missing outcome must SystemExit")
    with mock.patch.object(pc, "run", return_value=0), \
         mock.patch("sys.argv", ["run_postclose_all"]):
        try:
            pc.main()
        except SystemExit as exc:
            assert exc.code == 0
        else:
            raise AssertionError("a complete pack must SystemExit 0")


def test_ecs_timer_reports_python_status_after_save() -> None:
    text = (ROOT / "scripts" / "ecs_map_postclose.sh").read_text(encoding="utf-8")
    code_at = text.index("code=$?")
    push_at = text.index("safe_git_push.sh", code_at)
    exit_at = text.index('exit "$code"', push_at)
    assert code_at < push_at < exit_at
    # Early skips stay green. Do not hard-code a failing status in the shell.
    assert "exit 0" in text
    assert "exit 1" not in text


def test_github_jobs_do_not_swallow_the_python_status() -> None:
    """The commit step still runs after a red python step."""
    for name in ("postclose_all.yml", "postclose_last_closed.yml"):
        text = (WF / name).read_text(encoding="utf-8")
        run_lines = [
            line for line in text.splitlines()
            if "python -m src.run_postclose_all" in line
        ]
        assert run_lines, name
        for line in run_lines:
            assert "|| true" not in line, (name, line)
        run_at = text.index("python -m src.run_postclose_all")
        commit_at = text.index("Commit post-close", run_at)
        window = text[commit_at:commit_at + 250]
        assert "always()" in window, name


def test_log_phrase_and_no_hard_fail_banner() -> None:
    src = (ROOT / "src" / "run_postclose_all.py").read_text(encoding="utf-8")
    assert "OUTCOME MISSING {date}" in src
    assert "REFLECT MISSING {date}" in src
    assert "[postclose-all] FAIL" not in src
    assert "DEGRADED" in src
    outcome = (ROOT / "src" / "run_outcome.py").read_text(encoding="utf-8")
    assert "empty/thin/tool-dump LLM" in outcome
    assert "not writing a stub" in outcome


def main() -> None:
    tests = [
        test_missing_outcome_and_reflect_still_run_later_stages,
        test_reflect_missing_fails_when_outcome_landed,
        test_outcome_missing_alone_names_that_date,
        test_other_gaps_stay_degraded_and_green,
        test_complete_pack_does_not_rerun_or_fail,
        test_close_pack_pushes_before_the_missing_line,
        test_run_finishes_later_dates_then_fails,
        test_main_exits_with_the_pack_code,
        test_ecs_timer_reports_python_status_after_save,
        test_github_jobs_do_not_swallow_the_python_status,
        test_log_phrase_and_no_hard_fail_banner,
    ]
    failed = 0
    for fn in tests:
        try:
            fn()
            print(f"ok  {fn.__name__}")
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"FAIL {fn.__name__}: {exc}")
    if failed:
        raise SystemExit(f"{failed} test(s) failed")
    print(f"{len(tests)} tests passed")


if __name__ == "__main__":
    main()
