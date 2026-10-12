"""Decision freeze guard: frozen inputs are never re-landed (2026-10-12)."""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

from src import day_board, freeze_guard as fg

ROOT = Path(__file__).resolve().parent.parent
D = "2026-10-12"
WX = f"01_daily/weather/{D}_weather.json"


def _manifest(status: str = "frozen") -> dict:
    return {"date": D, "status": status, "files": [
        {"repo": "fullscan", "source_path": WX, "status": "copied"},
        {"repo": "fullscan", "source_path": f"01_daily/news/{D}_parsed.json"},
        {"repo": "fullscan", "source_path": "excel_bot/suggestions/suggestions.csv"},
        {"repo": "fullscan", "source_path": "01_daily/general/2026-10-08_predict.md"},
        {"repo": "theme-radar", "source_path": f"x/{D}.csv"},
    ]}


def _loader(m):
    return lambda d: json.dumps(m).encode() if d == D else None


def test_every_dated_manifest_file_is_blocked_not_rolling_or_other_days():
    paths = [WX, f"01_daily/news/{D}_parsed.json", "excel_bot/suggestions/suggestions.csv",
             "01_daily/general/2026-10-08_predict.md", f"x/{D}.csv",
             f"01_daily/weather/2026-10-13_weather.json", f"data/day_board/{D}.json"]
    got = fg.blocked_paths(paths, "HEAD", loader=_loader(_manifest()))
    assert got == {WX: D, f"01_daily/news/{D}_parsed.json": D}


def test_late_or_absent_manifest_blocks_nothing():
    assert fg.blocked_paths([WX], "HEAD", loader=_loader(_manifest("late: not frozen"))) == {}
    assert fg.blocked_paths([WX], "HEAD", loader=lambda d: None) == {}
    assert fg.blocked_paths([WX], "HEAD", loader=lambda d: b"{not json") == {}


def test_filter_logs_summary_and_adds_board_row():
    with tempfile.TemporaryDirectory() as t:
        tmp = Path(t)
        board = tmp / "day_board"
        board.mkdir()
        (board / f"{D}.json").write_text(json.dumps({"date": D, "lands": [{"key": "x"}]}))
        summ = tmp / "summary.md"
        os.environ["GITHUB_STEP_SUMMARY"] = str(summ)
        try:
            out = fg.run_filter([WX, f"01_daily/weather/2026-10-13_weather.json"], "HEAD",
                                "auto: land weather [2026-10-12]", cwd=tmp,
                                board_dir=board, loader=_loader(_manifest()))
        finally:
            os.environ.pop("GITHUB_STEP_SUMMARY", None)
        assert out == [WX]
        assert WX in summ.read_text()
        b = json.loads((board / f"{D}.json").read_text())
        assert b["lands"] == [{"key": "x"}]
        assert b["frozen_write_skips"][0]["path"] == WX


def test_day_board_merge_and_rebuild_keep_skips():
    a = {"generated_at": "1", "lands": [], "frozen_write_skips": [{"path": "a", "at": "1"}]}
    b = {"generated_at": "2", "lands": []}
    assert day_board.merge_boards(a, b)["frozen_write_skips"] == [{"path": "a", "at": "1"}]
    b["frozen_write_skips"] = [{"path": "b", "at": "2"}, {"path": "a", "at": "1"}]
    assert [r["path"] for r in day_board.merge_boards(a, b)["frozen_write_skips"]] == ["a", "b"]
    src = (ROOT / "src" / "day_board.py").read_text()
    assert 'out["frozen_write_skips"] = list(prev["frozen_write_skips"])' in src


def _git(cwd, *a):
    subprocess.run(["git", *a], cwd=cwd, check=True, capture_output=True)


def test_safe_git_push_skips_frozen_file_and_lands_the_rest():
    """End to end against a local bare origin: frozen weather stays as on main."""
    with tempfile.TemporaryDirectory() as t:
        tmp = Path(t)
        origin, work = tmp / "origin.git", tmp / "work"
        _git(tmp, "init", "-q", "--bare", "-b", "main", str(origin))
        _git(tmp, "clone", "-q", str(origin), str(work))
        for rel in ("scripts/safe_git_push.sh", "src/freeze_guard.py"):
            dst = work / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            if (ROOT / rel).exists():
                shutil.copy(ROOT / rel, dst)
            else:
                dst.write_text("")
        (work / WX).parent.mkdir(parents=True)
        (work / WX).write_text('{"v": "frozen"}')
        mdir = work / "research" / "input_freeze" / D
        mdir.mkdir(parents=True)
        (mdir / "manifest.json").write_text(json.dumps(_manifest()))
        (work / "data" / "day_board").mkdir(parents=True)
        (work / "data" / "day_board" / f"{D}.json").write_text(json.dumps({"date": D}))
        _git(work, "-c", "user.name=t", "-c", "user.email=t@t", "add", "-A")
        _git(work, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "base")
        _git(work, "push", "-q", "origin", "HEAD:main")
        (work / WX).write_text('{"v": "rewritten after freeze"}')
        other = work / "01_daily" / "weather" / "2026-10-13_weather.json"
        other.write_text('{"v": "next day"}')
        env = {k: v for k, v in os.environ.items()
               if not k.startswith("GITHUB") and k not in ("GH_TOKEN",)}
        env["HOME"] = str(tmp)
        env["GITHUB_STEP_SUMMARY"] = str(tmp / "summary.md")
        r = subprocess.run(["bash", "scripts/safe_git_push.sh", f"auto: land weather [{D}]",
                            WX, str(other.relative_to(work))],
                           cwd=work, env=env, capture_output=True, text=True, timeout=120)
        assert r.returncode == 0, r.stdout + r.stderr
        assert "FROZEN" in r.stdout + r.stderr
        show = lambda rel: subprocess.run(["git", "show", f"main:{rel}"], cwd=origin,
                                          capture_output=True, text=True).stdout
        assert show(WX) == '{"v": "frozen"}'
        assert show("01_daily/weather/2026-10-13_weather.json") == '{"v": "next day"}'
        board = json.loads(show(f"data/day_board/{D}.json"))
        assert board["frozen_write_skips"][0]["path"] == WX
        assert WX in (tmp / "summary.md").read_text()
        assert (work / WX).read_text() == '{"v": "rewritten after freeze"}'  # runner copy untouched


def test_safe_git_push_all_frozen_exits_zero():
    with tempfile.TemporaryDirectory() as t:
        tmp = Path(t)
        origin, work = tmp / "origin.git", tmp / "work"
        _git(tmp, "init", "-q", "--bare", "-b", "main", str(origin))
        _git(tmp, "clone", "-q", str(origin), str(work))
        (work / "scripts").mkdir(); (work / "src").mkdir()
        shutil.copy(ROOT / "scripts/safe_git_push.sh", work / "scripts")
        shutil.copy(ROOT / "src/freeze_guard.py", work / "src")
        (work / "src/__init__.py").write_text("")
        (work / WX).parent.mkdir(parents=True)
        (work / WX).write_text("a")
        mdir = work / "research" / "input_freeze" / D
        mdir.mkdir(parents=True)
        (mdir / "manifest.json").write_text(json.dumps(_manifest()))
        _git(work, "-c", "user.name=t", "-c", "user.email=t@t", "add", "-A")
        _git(work, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "base")
        _git(work, "push", "-q", "origin", "HEAD:main")
        before = subprocess.run(["git", "rev-parse", "main"], cwd=origin, capture_output=True, text=True).stdout
        (work / WX).write_text("b")
        env = {k: v for k, v in os.environ.items() if not k.startswith("GITHUB")}
        env["HOME"] = str(tmp)
        r = subprocess.run(["bash", "scripts/safe_git_push.sh", "m", WX], cwd=work, env=env,
                           capture_output=True, text=True, timeout=120)
        assert r.returncode == 0, r.stdout + r.stderr
        assert before
        files = subprocess.run(["git", "show", "--name-only", "--format=", "main"], cwd=origin,
                               capture_output=True, text=True).stdout.split()
        assert files == [f"data/day_board/{D}.json"]  # only the skip note lands
        assert subprocess.run(["git", "show", f"main:{WX}"], cwd=origin,
                              capture_output=True, text=True).stdout == "a"
