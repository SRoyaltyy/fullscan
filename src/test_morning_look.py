"""Morning look reads only what was committed before 09:30 ET.

Run: python -m src.test_morning_look
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

from src import morning_look as ml

HERE = Path(__file__).resolve().parent
DAY = "2026-10-08"

STUB = '''
import json
from pathlib import Path
def build_look_rows(date):
    doc = json.loads(Path("data/inputs.json").read_text())
    return [{"date": date, "ticker": t, "sources": ["ohlc_hot"],
             "open": None, "close": None, "prior_date": doc["prior"]}
            for t in doc["names"]]
'''


def _commit(repo: Path, rel: str, body: str, when: str) -> None:
    p = repo / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(body, encoding="utf-8")
    env = {**os.environ, "GIT_COMMITTER_DATE": when, "GIT_AUTHOR_DATE": when}
    subprocess.run(["git", "add", rel], cwd=repo, check=True)
    subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@t",
                    "commit", "-qm", rel], cwd=repo, check=True, env=env)


def _repo() -> Path:
    repo = Path(tempfile.mkdtemp())
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    _commit(repo, "src/__init__.py", "", "2026-10-07T20:00:00-04:00")
    _commit(repo, "src/morning_look.py", (HERE / "morning_look.py").read_text(),
            "2026-10-07T20:00:00-04:00")
    _commit(repo, "src/combo_broker.py", STUB, "2026-10-07T20:00:00-04:00")
    return repo


def _inputs(names, prior="2026-10-07") -> str:
    return json.dumps({"names": names, "prior": prior})


def test_last_commit_before_open() -> None:
    repo = _repo()
    _commit(repo, "data/inputs.json", _inputs(["AAA"]), "2026-10-08T09:00:00-04:00")
    _commit(repo, "data/inputs.json", _inputs(["LATE"]), "2026-10-08T09:45:00-04:00")
    sha, at = ml.preopen_commit(DAY, repo)
    assert at.startswith("2026-10-08T09:00:00"), at
    assert not ml.tree_is_commit(sha, repo)  # HEAD moved past the open


def test_build_reads_the_pre_open_tree_not_head() -> None:
    repo = _repo()
    _commit(repo, "data/inputs.json", _inputs(["AAA", "BBB"]), "2026-10-08T05:40:00-04:00")
    _commit(repo, "data/inputs.json", _inputs(["LATE"]), "2026-10-08T09:31:00-04:00")
    doc = ml.build_rows(DAY, repo)
    assert doc["read_from"] == "worktree"
    assert [r["ticker"] for r in doc["rows"]] == ["AAA", "BBB"], doc["rows"]
    assert doc["asof_committed_at"].startswith("2026-10-08T05:40:00")
    assert all(r["prior_date"] == "2026-10-07" for r in doc["rows"])
    # worktree cleaned up
    out = subprocess.run(["git", "worktree", "list"], cwd=repo, capture_output=True, text=True)
    assert len(out.stdout.strip().splitlines()) == 1, out.stdout


def test_clean_head_reads_in_place_and_dirty_does_not() -> None:
    repo = _repo()
    _commit(repo, "data/inputs.json", _inputs(["AAA"]), "2026-10-08T05:40:00-04:00")
    sha, _ = ml.preopen_commit(DAY, repo)
    assert ml.tree_is_commit(sha, repo)
    (repo / "data" / "inputs.json").write_text(_inputs(["DIRTY"]))
    assert not ml.tree_is_commit(sha, repo)
    doc = ml.build_rows(DAY, repo)
    assert [r["ticker"] for r in doc["rows"]] == ["AAA"]
    (repo / "data" / "inputs.json").write_text(_inputs(["AAA"]))
    (repo / "data" / "extra.json").write_text("{}")
    assert not ml.tree_is_commit(sha, repo)


def test_refuses_shallow_and_no_pre_open_commit() -> None:
    repo = _repo()
    _commit(repo, "data/inputs.json", _inputs(["AAA"]), "2026-10-08T05:40:00-04:00")
    shallow = Path(tempfile.mkdtemp()) / "s"
    subprocess.run(["git", "clone", "-q", "--depth", "1", f"file://{repo}", str(shallow)], check=True)
    try:
        ml.preopen_commit(DAY, shallow)
        raise AssertionError("shallow clone must refuse")
    except ml.MorningLookRefused as e:
        assert "shallow" in str(e)
    try:
        ml.preopen_commit("2026-10-06", repo)
        raise AssertionError("no commit before that open")
    except ml.MorningLookRefused:
        pass
    shutil.rmtree(shallow.parent, ignore_errors=True)


def test_same_day_print_is_refused() -> None:
    try:
        ml.check_rows(DAY, [{"date": DAY, "ticker": "X", "open": 1.0}])
        raise AssertionError("same-day open must refuse")
    except ml.MorningLookRefused:
        pass
    try:
        ml.check_rows(DAY, [{"date": "2026-10-07", "ticker": "X"}])
        raise AssertionError("wrong date must refuse")
    except ml.MorningLookRefused:
        pass


def test_install_turns_sit_into_look() -> None:
    from src import strategy_tickets as st
    orig = st._session_look
    real_lob = ml.load_or_build
    try:
        st._session_look = lambda d, p: {"date": d, "rows": [], "stale": False,
                                         "source": "no_same_day_panel", "sit": True,
                                         "error": "no same-day panel rows"}
        ml.load_or_build = lambda d: {"rows": [{"date": d, "ticker": "AAA"}],
                                      "asof_commit": "abc", "asof_committed_at": "t"}
        ml.install()
        got = st._session_look(DAY, {})
        assert got["source"] == "look" and got["rows"][0]["ticker"] == "AAA"
        assert got["asof_commit"] == "abc"
        def boom(d):
            raise ml.MorningLookRefused("shallow clone")
        ml.load_or_build = boom
        got = st._session_look(DAY, {})
        assert got["source"] == "no_same_day_panel" and "shallow" in got["error"]
        assert st._session_look("2026-10-06", {})["source"] == "no_same_day_panel"
    finally:
        st._session_look = orig
        ml.load_or_build = real_lob


def main() -> None:
    test_last_commit_before_open()
    test_build_reads_the_pre_open_tree_not_head()
    test_clean_head_reads_in_place_and_dirty_does_not()
    test_refuses_shallow_and_no_pre_open_commit()
    test_same_day_print_is_refused()
    test_install_turns_sit_into_look()
    print("ok morning_look")


if __name__ == "__main__":
    main()
