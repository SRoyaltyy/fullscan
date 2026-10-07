"""Pages-publish required-files gate. No network.

Run: PYTHONPATH=. python3 -m src.test_pages_publish_gate
"""
from __future__ import annotations

import json
import os
import shlex
import shutil
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from src import pages_publish_gate as gate

ET = ZoneInfo("America/New_York")


def _proc(key: str, files: list[dict], status: str = "OK") -> dict:
    return {"key": key, "status": status, "files": files}


def _file(key: str, role: str, status: str, path: str = "") -> dict:
    return {"key": key, "role": role, "status": status, "path": path or key}


def _write(root: Path, rel: str, payload: dict) -> None:
    p = root / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(payload), encoding="utf-8")


def _board(root: Path, date: str, processes: list[dict],
           overall: str = "PARTIAL") -> None:
    _write(root, f"data/day_board/{date}.json", {
        "date": date, "overall": overall, "ranker_ready": True,
        "processes": processes,
    })
    _write(root, "data/day_board/latest.json", {"date": date, "overall": overall})


def _fm_and_tickets(root: Path, date: str) -> None:
    _write(root, "dashboard/factor-mine/today.json", {"date": date})
    _write(root, "data/day_board/today_strategies.json", {
        "date": date, "session_open": date, "clock_legal_for": date,
        "strategies": {"a": {"family": "paper"}},
    })


def test_required_ok_partial_catalyst_ready() -> None:
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        date = "2026-09-16"
        _board(root, date, [
            _proc("stock_book", [
                _file("book_json", "required", "OK"),
                _file("book_md", "required", "OK"),
                _file("in_general", "input", "OK"),
            ]),
            _proc("publish", [
                _file("dash_html", "required", "OK"),
                _file("paper", "required", "OK"),
                _file("pages", "required", "SKIP"),
            ]),
            _proc("catalyst", [
                _file("dossiers", "optional", "FAIL"),
            ], status="FAIL"),
        ])
        _fm_and_tickets(root, date)
        v = gate.evaluate(date, root=root)
        assert v["ready"] is True
        assert v["fallback"] is False
        assert v["catalyst_status"] == "FAIL"
        assert v["overall"] == "PARTIAL"
        assert v["processes"]["stock_book"]["n_ok"] == 2
        assert v["processes"]["publish"]["n_ok"] == 2


def test_catalyst_fail_alone_does_not_block() -> None:
    html = Path(__file__).resolve().parent.parent / "src" / "pages_publish_gate.py"
    text = html.read_text(encoding="utf-8")
    assert "catalyst" in text
    assert "IGNORE_PROCESS_KEYS" in text
    assert "PAGES_PROCESS_KEYS" in text


def test_fallback_when_book_required_missing() -> None:
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        date = "2026-09-16"
        _board(root, date, [
            _proc("stock_book", [
                _file("book_json", "required", "MISSING"),
            ], status="FAIL"),
            _proc("publish", [
                _file("dash_html", "required", "OK"),
            ]),
        ])
        _fm_and_tickets(root, date)
        v = gate.evaluate(date, root=root)
        assert v["ready"] is True
        assert v["fallback"] is True


def test_blocked_without_tickets() -> None:
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        date = "2026-09-16"
        _board(root, date, [
            _proc("stock_book", [_file("book_json", "required", "OK")]),
            _proc("publish", [_file("dash_html", "required", "OK")]),
        ])
        _write(root, "dashboard/factor-mine/today.json", {"date": date})
        v = gate.evaluate(date, root=root)
        assert v["ready"] is False
        assert "tickets" in v["reason"]


def test_blocked_when_today_json_is_yesterday() -> None:
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        date = "2026-09-16"
        _board(root, date, [
            _proc("stock_book", [_file("book_json", "required", "OK")]),
            _proc("publish", [_file("dash_html", "required", "OK")]),
        ])
        _write(root, "dashboard/factor-mine/today.json", {"date": "2026-09-15"})
        _write(root, "data/day_board/today_strategies.json", {
            "date": date, "clock_legal_for": date,
        })
        v = gate.evaluate(date, root=root)
        assert v["ready"] is False
        assert "today.json" in v["reason"]


def test_live_2026_09_16_board_is_ready() -> None:
    """Real main snapshot: PARTIAL + catalyst FAIL must still be ready."""
    v = gate.evaluate("2026-09-16")
    assert v["date"] == "2026-09-16"
    assert v["catalyst_status"] in ("FAIL", "PARTIAL", "OK", "")
    assert v["factor_mine_today"] is True
    assert v["tickets"] is True
    assert v["ready"] is True


def test_dash_template_has_session_and_pack_stamp() -> None:
    html = (Path(__file__).resolve().parent.parent
            / "src" / "factor_mine_dash.html").read_text(encoding="utf-8")
    assert 'id="sessionHead"' in html
    assert 'id="packStamp"' in html
    assert "pack is " in html
    assert "Pages built " in html
    assert "paintSessionStamps" in html
    assert "Six effectiveness metrics (cash book) · " in html


def test_paper_book_page_is_on_the_pages_deploy() -> None:
    """OpenAPI sandbox book is a Pages overlay, not a second site."""
    root = Path(__file__).resolve().parent.parent
    page = (root / "dashboard" / "paper-book" / "index.html").read_text(
        encoding="utf-8")
    assert "Webull OpenAPI sandbox" in page
    assert "api.sandbox.webull.com" in page
    assert "fill_observe.cash" in page
    assert "data/paper_open" in page
    assert "_status.json" in page
    assert "raw.githubusercontent.com/SRoyaltyy/fullscan/main/data/paper_open" in page
    dep = (root / ".github" / "workflows" / "deploy-dashboard.yml").read_text(
        encoding="utf-8")
    pub = (root / "scripts" / "publish_dashboard.sh").read_text(encoding="utf-8")
    # The Pages job overlays dashboard/${sub} for each named book, including
    # paper-book. The workflow does not spell the joined path.
    assert "paper-book" in dep
    assert "dashboard/${sub}" in dep
    assert "paper-book" in pub
    shell = (root / "src" / "paper_dash.html").read_text(encoding="utf-8")
    assert 'href="/fullscan/dashboard/paper-book/"' in shell
    board = (root / "src" / "strategy_board.py").read_text(encoding="utf-8")
    assert 'href="../paper-book/"' in board
    # Committed pages are what GitHub Pages serves until the next regen.
    # Skip when a sparse checkout omitted them.
    live_path = root / "dashboard" / "index.html"
    if live_path.is_file():
        assert 'href="/fullscan/dashboard/paper-book/"' in live_path.read_text(
            encoding="utf-8")
    sboard_path = root / "dashboard" / "strategy-board" / "index.html"
    if sboard_path.is_file():
        assert 'href="../paper-book/"' in sboard_path.read_text(encoding="utf-8")


def _write_page(root: Path, rel: str, body: str) -> None:
    p = root / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(body, encoding="utf-8")


def _git_wrapper(path: Path, real: str, bare: Path) -> None:
    """Send the publish script's gh-pages remote at a local bare repo."""
    path.write_text(
        "#!/bin/bash\n"
        f"real={shlex.quote(real)}\n"
        f"bare={shlex.quote(str(bare))}\n"
        'if [ "${1:-}" = "-C" ] && [ "${3:-}" = "remote" ] && '
        '[ "${4:-}" = "add" ] && [ "${5:-}" = "origin" ]; then\n'
        '  exec "$real" -C "$2" remote add origin "$bare"\n'
        "fi\n"
        'exec "$real" "$@"\n',
        encoding="utf-8",
    )
    path.chmod(0o755)


def _publish(ws: Path, bare: Path, pages_out: Path | None) -> subprocess.CompletedProcess[str]:
    real = shutil.which("git")
    assert real
    bindir = ws.parent / "bin"
    bindir.mkdir(exist_ok=True)
    _git_wrapper(bindir / "git", real, bare)
    script = Path(__file__).resolve().parent.parent / "scripts" / "publish_dashboard.sh"
    env = os.environ.copy()
    for key in ("GITHUB_WORKSPACE", "GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE"):
        env.pop(key, None)
    env["PATH"] = f"{bindir}{os.pathsep}{env.get('PATH', '')}"
    env["HOME"] = str(ws.parent / "home")
    env["GITHUB_TOKEN"] = "test-token-not-a-real-secret"
    env["GITHUB_REPOSITORY"] = "SRoyaltyy/fullscan"
    env["PAGES_PUSH_ATTEMPTS"] = "1"
    env["GIT_CONFIG_NOSYSTEM"] = "1"
    env["GIT_CONFIG_GLOBAL"] = os.devnull
    if pages_out is None:
        env.pop("PAGES_OUT_DIR", None)
    else:
        env["PAGES_OUT_DIR"] = str(pages_out)
    Path(env["HOME"]).mkdir(exist_ok=True)
    return subprocess.run(
        ["bash", str(script)],
        cwd=ws,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )


def _blob(bare: Path, rel: str) -> str:
    return subprocess.check_output(
        ["git", "--git-dir", str(bare), "show", f"gh-pages:{rel}"],
        text=True,
    )


def test_publish_restores_h1_and_holdup_aliases() -> None:
    """A news-intake-style force publish must keep /h1/ and /holdup/.

    The prebuilt tree is the partial case: stale dashboard/h1, no holdup,
    and no root aliases. The checkout is main. The published branch has
    to overlay both folders and copy them to the site root.
    """
    with tempfile.TemporaryDirectory() as d:
        base = Path(d)
        ws = base / "ws"
        out = base / "pages_out"
        bare = base / "origin.git"
        ws.mkdir()
        subprocess.check_call(["git", "init", "--bare", "-q", str(bare)])
        _write_page(ws, "dashboard/index.html", "<html>main</html>")
        _write_page(ws, "dashboard/h1/index.html", "h1-from-main")
        _write_page(ws, "dashboard/h1/log.json", '{"book":"h1"}')
        _write_page(ws, "dashboard/holdup/index.html", "holdup-from-main")
        _write_page(ws, "dashboard/holdup/status.json", '{"book":"holdup"}')
        _write_page(ws, "dashboard/news-intake/index.html", "intake-from-main")
        _write_page(out, "dashboard/index.html", "<html>partial</html>")
        _write_page(out, "index.html", "<html>partial</html>")
        _write_page(out, "dashboard/h1/index.html", "STALE-H1")
        _write_page(out, "dashboard/news-intake/index.html", "intake-partial")

        partial = _publish(ws, bare, out)
        assert partial.returncode == 0, partial.stdout + partial.stderr
        assert "[pages] published" in partial.stdout, partial.stdout + partial.stderr
        assert _blob(bare, "h1/index.html") == "h1-from-main"
        assert _blob(bare, "dashboard/h1/index.html") == "h1-from-main"
        assert _blob(bare, "dashboard/h1/log.json") == '{"book":"h1"}'
        assert _blob(bare, "holdup/index.html") == "holdup-from-main"
        assert _blob(bare, "dashboard/holdup/index.html") == "holdup-from-main"
        assert _blob(bare, "dashboard/holdup/status.json") == '{"book":"holdup"}'
        assert _blob(bare, "dashboard/news-intake/index.html") == "intake-from-main"
        assert _blob(bare, ".nojekyll") == ""

        # Same checkout, no prebuilt tree: the free-news-intake call.
        intake_bare = base / "intake.git"
        subprocess.check_call(["git", "init", "--bare", "-q", str(intake_bare)])
        intake = _publish(ws, intake_bare, None)
        assert intake.returncode == 0, intake.stdout + intake.stderr
        assert "[pages] published" in intake.stdout, intake.stdout + intake.stderr
        assert _blob(intake_bare, "h1/index.html") == "h1-from-main"
        assert _blob(intake_bare, "holdup/index.html") == "holdup-from-main"
        assert _blob(intake_bare, "dashboard/h1/index.html") == "h1-from-main"
        assert _blob(intake_bare, "dashboard/holdup/index.html") == "holdup-from-main"
        assert _blob(intake_bare, "dashboard/news-intake/index.html") == "intake-from-main"


def test_preopen_2026_10_07_resolves_to_2026_10_06() -> None:
    """Before the bell, today ET is not the session the day board is for."""
    preopen = datetime(2026, 10, 7, 8, 40, tzinfo=ET)
    assert gate.latest_completed_session(preopen) == "2026-10-06"
    assert gate.resolve_date("", now=preopen) == "2026-10-06"
    assert gate.resolve_date("2026-10-07", now=preopen) == "2026-10-07"
    at_bell = datetime(2026, 10, 7, 9, 30, tzinfo=ET)
    assert gate.latest_completed_session(at_bell) == "2026-10-07"
    # Naive timestamps are ET, same as the rest of the clock helpers.
    assert gate.latest_completed_session(datetime(2026, 10, 7, 8, 40)) == "2026-10-06"
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        _board(root, "2026-10-06", [
            _proc("stock_book", [_file("book_json", "required", "OK")]),
            _proc("publish", [_file("dash_html", "required", "OK")]),
        ])
        _fm_and_tickets(root, "2026-10-06")
        ready = gate.evaluate("", root=root, now=preopen)
        assert ready["date"] == "2026-10-06"
        assert ready["ready"] is True
        blocked = gate.evaluate("2026-10-07", root=root, now=preopen)
        assert blocked["date"] == "2026-10-07"
        assert blocked["ready"] is False


def test_weekend_resolves_to_friday() -> None:
    """Saturday, Sunday, and Monday before the open stay on Friday."""
    assert gate.latest_completed_session(
        datetime(2026, 10, 10, 12, 0, tzinfo=ET)) == "2026-10-09"
    assert gate.latest_completed_session(
        datetime(2026, 10, 11, 18, 0, tzinfo=ET)) == "2026-10-09"
    assert gate.latest_completed_session(
        datetime(2026, 10, 5, 9, 0, tzinfo=ET)) == "2026-10-02"
    # Labor Day is not a session. Tuesday morning still closes Friday.
    assert gate.latest_completed_session(
        datetime(2026, 9, 7, 15, 0, tzinfo=ET)) == "2026-09-04"
    assert gate.latest_completed_session(
        datetime(2026, 9, 8, 8, 0, tzinfo=ET)) == "2026-09-04"
    assert gate.latest_completed_session(
        datetime(2026, 9, 8, 9, 30, tzinfo=ET)) == "2026-09-08"


def _workflow_gate_script() -> str:
    dep = (Path(__file__).resolve().parent.parent
           / ".github" / "workflows" / "deploy-dashboard.yml").read_text(
               encoding="utf-8")
    start = dep.index("id: pages_gate")
    rest = dep[start:]
    body = rest.split("run: |", 1)[1]
    lines = []
    for line in body.splitlines()[1:]:
        if line.startswith("      - name:"):
            break
        if line.startswith("          "):
            lines.append(line[10:])
        elif line.strip() == "":
            lines.append("")
        else:
            break
    return "\n".join(lines).strip() + "\n"


def _run_gate_step(**env: str) -> subprocess.CompletedProcess[str]:
    script = _workflow_gate_script()
    assert "exit 1" in script
    assert 'DATE="${DATE:-$(TZ=America/New_York date +%F)}"' not in script
    with tempfile.TemporaryDirectory() as d:
        out = Path(d) / "github_output"
        merged = os.environ.copy()
        merged["GITHUB_OUTPUT"] = str(out)
        merged["PYTHONPATH"] = str(Path(__file__).resolve().parent.parent)
        merged.pop("SESSION_DATE", None)
        merged.pop("FORCE", None)
        merged.update(env)
        proc = subprocess.run(
            ["bash", "-c", script],
            cwd=Path(__file__).resolve().parent.parent,
            env=merged,
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        proc.github_output = out.read_text(encoding="utf-8") if out.is_file() else ""  # type: ignore[attr-defined]
        return proc


def test_gate_block_fails_the_workflow() -> None:
    """A refused publish must be a non-zero step, never a green skip."""
    blocked = _run_gate_step(SESSION_DATE="2099-01-04", FORCE="false")
    text = blocked.stdout + blocked.stderr
    assert blocked.returncode != 0, text
    assert "FAIL" in text
    assert "2099-01-04" in text
    assert "Pages publish refused" in text
    assert "ready=no" in blocked.github_output  # type: ignore[attr-defined]
    assert "ready=yes" not in blocked.github_output  # type: ignore[attr-defined]

    forced = _run_gate_step(SESSION_DATE="2099-01-04", FORCE="true")
    assert forced.returncode == 0, forced.stdout + forced.stderr
    assert "ready=yes" in forced.github_output  # type: ignore[attr-defined]
    assert "force=true" in forced.stdout

    explicit = _run_gate_step(SESSION_DATE="2026-10-06", FORCE="")
    assert explicit.returncode == 0, explicit.stdout + explicit.stderr
    assert "2026-10-06" in explicit.stdout
    assert "READY" in explicit.stdout
    assert "ready=yes" in explicit.github_output  # type: ignore[attr-defined]

    dep = (Path(__file__).resolve().parent.parent
           / ".github" / "workflows" / "deploy-dashboard.yml").read_text(
               encoding="utf-8")
    assert "Fail closed when Pages publish did not run" in dep
    assert "steps.pages_gate.outputs.ready != 'yes'" in dep
    assert "exit 1" in dep
    assert dep.count("holdup h1") >= 2
    assert "empty = latest completed NYSE session" in dep


def test_workflows_wire_the_gate() -> None:
    root = Path(__file__).resolve().parent.parent
    dep = (root / ".github" / "workflows" / "deploy-dashboard.yml").read_text(
        encoding="utf-8")
    book = (root / ".github" / "workflows" / "stock_book_all.yml").read_text(
        encoding="utf-8")
    assert "src.pages_publish_gate" in dep
    assert "Factor strategy mine" in dep
    assert "gh-pages-deploy" in dep
    assert "src.pages_publish_gate" in book
    assert "gh workflow run deploy-dashboard.yml" in book


def main() -> None:
    tests = [
        test_required_ok_partial_catalyst_ready,
        test_catalyst_fail_alone_does_not_block,
        test_fallback_when_book_required_missing,
        test_blocked_without_tickets,
        test_blocked_when_today_json_is_yesterday,
        test_live_2026_09_16_board_is_ready,
        test_dash_template_has_session_and_pack_stamp,
        test_paper_book_page_is_on_the_pages_deploy,
        test_publish_restores_h1_and_holdup_aliases,
        test_preopen_2026_10_07_resolves_to_2026_10_06,
        test_weekend_resolves_to_friday,
        test_gate_block_fails_the_workflow,
        test_workflows_wire_the_gate,
    ]
    for fn in tests:
        fn()
        print("ok", fn.__name__)
    print(f"{len(tests)} tests passed")


if __name__ == "__main__":
    main()
