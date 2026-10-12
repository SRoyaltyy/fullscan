"""Morning ticket rows built only from what was on main before 09:30 ET.

Root cause this fixes: from 2026-09-28 ``strategy_tickets._session_look``
uses same-day rows only from ``data/factor_mine/panel.json``. That panel is
built after the close (about 17:00 ET), so at 09:30 it never has the day,
and every morning ticket since 09-29 was saved as ``no_same_day_panel``
with empty factor-mine buy lists.

This wrapper keeps that rule (no live lookup on the working tree) and adds
a provable pre-open look:

1. ``preopen_commit(D)`` is the last first-parent commit on HEAD with a
   committer time before 09:30 ET on D. A shallow clone refuses.
2. If HEAD is that commit and no non-``src/`` file differs from it, the
   tree already is the pre-open commit and is read in place. Otherwise
   that commit is checked out into a temporary ``git worktree``. The
   existing leak-free builder ``combo_broker.build_look_rows(D)`` runs in a
   child process whose working directory is that tree, so every file it
   opens (candidates, prior-day Finviz export, prior-day OHLC tape,
   session cards, flatten plan) is the bytes committed before the open.
   Its lists (yday_gainer, yday_mover, ohlc_hot, probable, ...) and
   hot_score come from the prior session's data.
3. Rows are checked: every row must be dated D, and the same-day 09:30 /
   16:00 prints (``open`` / ``close``) must be empty, otherwise the build
   is refused (a leak means the tree already held D's tape).
4. The rows plus the commit id and time are written to
   ``data/factor_mine/morning_look/<D>.json`` (write-once), and
   ``_session_look`` returns them with ``source="look"``.

Nothing pinned is edited. Entry points (the ticket writers call these):
  python -m src.morning_look tickets --date D [--write]      (strategy_tickets)
  python -m src.morning_look live_boards --date D --write ... (publish_live_boards)
  python -m src.morning_look decision_ready --date D ...      (decision_ready)
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
ET = ZoneInfo("America/New_York")
FROM = "2026-10-07"
OUT_DIR = ROOT / "data" / "factor_mine" / "morning_look"
SAME_DAY_FIELDS = ("open", "close")


class MorningLookRefused(RuntimeError):
    pass


def open_at(day: str) -> datetime:
    y, m, d = (int(x) for x in day.split("-"))
    return datetime(y, m, d, 9, 30, tzinfo=ET)


def _git(args: list[str], root: Path) -> str:
    out = subprocess.run(["git", *args], cwd=str(root), capture_output=True,
                         text=True, check=False)
    if out.returncode != 0:
        raise MorningLookRefused(f"git {' '.join(args)}: {out.stderr.strip()[-400:]}")
    return out.stdout.strip()


def preopen_commit(day: str, root: Path | None = None) -> tuple[str, str]:
    """(sha, committer time ET) of the last first-parent commit before 09:30 ET."""
    root = Path(root or ROOT)
    if _git(["rev-parse", "--is-shallow-repository"], root) == "true":
        raise MorningLookRefused("shallow clone: commit times not provable")
    cut = open_at(day)
    for line in _git(["log", "--first-parent", "--format=%H %cI", "HEAD"], root).splitlines():
        sha, stamp = line.split(" ", 1)
        when = datetime.fromisoformat(stamp.strip())
        if when < cut:
            return sha, when.astimezone(ET).isoformat()
    raise MorningLookRefused(f"no commit before 09:30 ET on {day}")


def check_rows(day: str, rows: list[dict]) -> list[dict]:
    out = []
    for r in rows:
        if str(r.get("date") or "")[:10] != day:
            raise MorningLookRefused(f"row {r.get('ticker')} dated {r.get('date')} != {day}")
        for k in SAME_DAY_FIELDS:
            if r.get(k) is not None:
                raise MorningLookRefused(
                    f"row {r.get('ticker')} has same-day {k}={r.get(k)}: the "
                    "pre-open tree already held the session tape")
        out.append(r)
    return out


def _child_rows(day: str, out: Path) -> None:
    """Runs inside the pre-open worktree."""
    from . import combo_broker as cb
    rows = cb.build_look_rows(day)
    out.write_text(json.dumps(rows, default=str), encoding="utf-8")


def tree_is_commit(sha: str, root: Path) -> bool:
    """True when HEAD is ``sha`` and no input file differs from it.

    Code under ``src/`` is not an input. Any other modified or untracked
    file means the working tree is not the pre-open commit.
    """
    if _git(["rev-parse", "HEAD"], root) != sha:
        return False
    dirty = _git(["status", "--porcelain", "--untracked-files=all", "--",
                  ".", ":(exclude)src"], root)
    return dirty == ""


def build_rows(day: str, root: Path | None = None, *, python: str | None = None) -> dict:
    root = Path(root or ROOT)
    sha, at = preopen_commit(day, root)
    tmp = Path(tempfile.mkdtemp(prefix="morning_look_"))
    in_place = tree_is_commit(sha, root)
    tree = root if in_place else tmp / "tree"
    if not in_place:
        _git(["worktree", "add", "--detach", str(tree), sha], root)
    try:
        dest = tmp / "rows.json"
        env = {k: v for k, v in os.environ.items() if k not in ("PYTHONPATH",)}
        env["PYTHONPATH"] = str(tree)
        env.setdefault("PYTHONHASHSEED", "0")
        proc = subprocess.run(
            [python or sys.executable, "-m", "src.morning_look", "_rows",
             "--date", day, "--out", str(dest)],
            cwd=str(tree), env=env, capture_output=True, text=True, check=False,
        )
        if proc.returncode != 0 or not dest.is_file():
            raise MorningLookRefused(
                f"pre-open build failed at {sha[:9]}: {(proc.stderr or proc.stdout)[-400:]}")
        rows = json.loads(dest.read_text(encoding="utf-8"))
    finally:
        if not in_place:
            subprocess.run(["git", "worktree", "remove", "--force", str(tree)],
                           cwd=str(root), capture_output=True, check=False)
        shutil.rmtree(tmp, ignore_errors=True)
    rows = check_rows(day, rows)
    return {"date": day, "asof_commit": sha, "asof_committed_at": at,
            "read_from": "head_clean" if in_place else "worktree",
            "cutoff_et": open_at(day).isoformat(), "n": len(rows), "rows": rows}


def store(doc: dict, out_dir: Path | None = None) -> Path:
    path = Path(out_dir or OUT_DIR) / f"{doc['date']}.json"
    text = json.dumps(doc, indent=2, sort_keys=True, default=str) + "\n"
    if path.is_file() and path.read_text(encoding="utf-8") != text:
        print(f"[morning-look] {path.name} already written; left as is", flush=True)
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def load_or_build(day: str) -> dict:
    path = OUT_DIR / f"{day}.json"
    if path.is_file():
        return json.loads(path.read_text(encoding="utf-8"))
    doc = build_rows(day)
    store(doc)
    return doc


def install() -> None:
    from . import strategy_tickets as st
    if getattr(st._session_look, "_morning_look", False):
        return
    original = st._session_look

    def _session_look(date: str, panel: dict) -> dict:
        looked = original(date, panel)
        if looked.get("source") != "no_same_day_panel" or str(date) < FROM:
            return looked
        try:
            doc = load_or_build(str(date))
        except MorningLookRefused as e:
            err = f"{looked.get('error') or ''}; pre-open look refused: {e}"
            print(f"[morning-look] WARN: {err}", flush=True)
            return {**looked, "error": err.strip('; ')}
        if not doc.get("rows"):
            return {**looked, "error": f"pre-open look at {doc.get('asof_commit','')[:9]} had no rows"}
        return {
            "date": date, "rows": doc["rows"], "stale": False, "source": "look",
            "want_date": date, "panel_bake_date": looked.get("panel_bake_date"),
            "asof_commit": doc.get("asof_commit"),
            "asof_committed_at": doc.get("asof_committed_at"),
        }

    _session_look._morning_look = True
    st._session_look = _session_look


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv:
        raise SystemExit("usage: morning_look {tickets|live_boards|decision_ready|build|_rows} ...")
    cmd, rest = argv[0], argv[1:]
    if cmd == "_rows":
        import argparse
        ap = argparse.ArgumentParser()
        ap.add_argument("--date", required=True)
        ap.add_argument("--out", required=True)
        a = ap.parse_args(rest)
        _child_rows(a.date, Path(a.out))
        return 0
    if cmd == "build":
        import argparse
        ap = argparse.ArgumentParser()
        ap.add_argument("--date", required=True)
        ap.add_argument("--write", action="store_true")
        a = ap.parse_args(rest)
        doc = build_rows(a.date)
        if a.write:
            print(store(doc))
        print(json.dumps({k: v for k, v in doc.items() if k != "rows"}))
        return 0
    install()
    if cmd == "tickets":
        from . import strategy_tickets as st
        return st.main(rest)
    if cmd == "live_boards":
        from . import publish_live_boards as plb
        sys.argv = ["publish_live_boards", *rest]
        return plb.main() or 0
    if cmd == "decision_ready":
        from . import decision_ready as dr
        sys.argv = ["decision_ready", *rest]
        return dr.main() or 0
    raise SystemExit(f"unknown command {cmd}")


if __name__ == "__main__":
    raise SystemExit(main())
