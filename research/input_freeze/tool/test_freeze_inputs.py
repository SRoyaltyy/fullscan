"""Unit tests for research/input_freeze/tool/freeze_inputs.py (stdlib only).

Run: python3 research/input_freeze/tool/test_freeze_inputs.py
"""
from __future__ import annotations

import datetime as dt
import gzip
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import freeze_inputs as fi  # noqa: E402

UTC = dt.timezone.utc


def t(s: str) -> dt.datetime:
    return fi.parse_now(s)


def sh(repo: Path, *args: str, date: str | None = None) -> str:
    env = dict(os.environ)
    if date:
        env.update(GIT_COMMITTER_DATE=date, GIT_AUTHOR_DATE=date)
    env.update(GIT_AUTHOR_NAME="t", GIT_AUTHOR_EMAIL="t@t", GIT_COMMITTER_NAME="t",
               GIT_COMMITTER_EMAIL="t@t")
    return subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True,
                          text=True, env=env).stdout


def commit(repo: Path, files: dict[str, str], date: str, msg: str = "c") -> str:
    for rel, body in files.items():
        p = repo / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(body, encoding="utf-8")
    sh(repo, "add", "-A")
    sh(repo, "commit", "-q", "-m", msg, date=date)
    return sh(repo, "rev-parse", "HEAD").strip()


class GateTests(unittest.TestCase):
    def test_edt_on_time_sleeps_to_0920(self):
        g = fi.gate(t("2026-10-07T13:15:00Z"), False)
        self.assertEqual((g["action"], g["sleep_s"], g["date"]), ("freeze", 300, "2026-10-07"))
        self.assertEqual(g["open_utc"], "2026-10-07T13:30:00Z")

    def test_edt_after_0928_is_late(self):
        for now in ("2026-10-07T13:28:00Z", "2026-10-07T13:30:00Z", "2026-10-07T21:03:00Z"):
            self.assertEqual(fi.gate(t(now), False)["action"], "late", now)

    def test_inside_window_freezes_now(self):
        g = fi.gate(t("2026-10-07T13:24:00Z"), False)
        self.assertEqual((g["action"], g["sleep_s"]), ("freeze", 0))

    def test_too_early_exits(self):
        g = fi.gate(t("2026-10-07T09:00:00Z"), False)  # 05:00 ET
        self.assertEqual(g["action"], "early")
        g = fi.gate(t("2026-10-07T10:00:00Z"), False)  # 06:00 ET, 200 min wait
        self.assertEqual(g["action"], "freeze")

    def test_late_cron_after_midnight_et_is_early_not_late(self):
        # Monday's cron firing 01:20Z Tuesday is 21:20 ET Monday -> Monday late.
        self.assertEqual(fi.gate(t("2026-10-13T01:20:00Z"), False)["date"], "2026-10-12")
        # 05:00Z Tuesday is 01:00 ET Tuesday -> early for Tuesday, nothing written.
        self.assertEqual(fi.gate(t("2026-10-13T05:00:00Z"), False)["action"], "early")

    def test_existing_folder_never_rewritten(self):
        for now in ("2026-10-07T13:15:00Z", "2026-10-07T15:00:00Z"):
            self.assertEqual(fi.gate(t(now), True)["action"], "exists")

    def test_weekend_and_holiday_closed(self):
        self.assertEqual(fi.gate(t("2026-10-10T13:15:00Z"), False)["action"], "closed")
        self.assertEqual(fi.gate(t("2026-11-26T14:15:00Z"), False)["action"], "closed")

    def test_before_first_date_skipped(self):
        self.assertEqual(fi.gate(t("2026-10-06T13:15:00Z"), False)["action"], "skip")
        self.assertEqual(fi.gate(t("2026-10-06T21:03:00Z"), False)["action"], "skip")

    def test_est_after_november_switch(self):
        # 2026-11-01 DST ends. 09:30 ET = 14:30Z.
        g = fi.gate(t("2026-11-09T14:15:00Z"), False)
        self.assertEqual((g["action"], g["sleep_s"], g["open_utc"]), ("freeze", 300, "2026-11-09T14:30:00Z"))
        self.assertEqual(fi.gate(t("2026-11-09T13:35:00Z"), False)["action"], "freeze")
        self.assertEqual(fi.gate(t("2026-11-09T14:28:00Z"), False)["action"], "late")


class PickTests(unittest.TestCase):
    paths = ["a/2026-10-05_x.json", "a/2026-10-06_x.json", "a/2026-10-07_x.json",
             "s/2026-10-06/tech_predict.md", "s/2026-10-06/fin_predict.md", "s/2026-10-06/_qc.json"]

    def test_newest_not_after_day(self):
        self.assertEqual(fi.pick(self.paths, "a/{date}_x.json", "2026-10-06"),
                         ("2026-10-06", ["a/2026-10-06_x.json"]))

    def test_before_day(self):
        self.assertEqual(fi.pick(self.paths, "a/{date}_x.json", "2026-10-06", before_day=True)[0],
                         "2026-10-05")

    def test_glob(self):
        self.assertEqual(fi.pick(self.paths, "s/{date}/*_predict.md", "2026-10-06")[1],
                         ["s/2026-10-06/fin_predict.md", "s/2026-10-06/tech_predict.md"])

    def test_missing(self):
        self.assertEqual(fi.pick(self.paths, "zz/{date}.csv", "2026-10-06"), (None, []))


class BuildTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = Path(self.tmp.name) / "fullscan"
        self.repo.mkdir()
        sh(self.repo, "init", "-q", "-b", "main")
        commit(self.repo, {
            "data/stock_book/2026-10-06_stock_book.json": "v1-before" + "x" * 70000,
            "data/stock_book/2026-10-05_stock_book.json": "old",
            "data/insider/2026-09-03_insider.json": "{}",
            "excel_bot/suggestions/suggestions.csv": "s1",
        }, "2026-10-06T13:00:00Z")
        commit(self.repo, {
            "data/stock_book/2026-10-06_stock_book.json": "v2-after-freeze",
            "data/day_board/2026-10-06_strategy_tickets.json": "post-freeze tickets",
            "excel_bot/suggestions/suggestions.csv": "s2",
        }, "2026-10-06T13:25:00Z")

    def tearDown(self):
        self.tmp.cleanup()

    def build(self):
        return fi.build("2026-10-06", t("2026-10-06T13:20:00Z"),
                        {"fullscan": self.repo, "theme-radar": None})

    def rows(self, m, pat):
        return [r for r in m["files"] if r["pattern"] == pat]

    def test_only_versions_committed_before_freeze(self):
        m, blobs = self.build()
        r = self.rows(m, "data/stock_book/{date}_stock_book.json")[0]
        self.assertEqual(r["status"], "copied")
        self.assertTrue(r["gzip"])
        raw = gzip.decompress(blobs[r["stored_path"]])
        self.assertTrue(raw.startswith(b"v1-before"))
        self.assertEqual(r["sha256"], hashlib.sha256(raw).hexdigest())
        self.assertLess(r["source_commit_time_utc"], m["freeze_time_utc"])
        s = self.rows(m, "excel_bot/suggestions/suggestions.csv")[0]
        self.assertEqual(blobs[s["stored_path"]], b"s1")
        # A file first committed after the freeze is missing, not copied.
        tk = self.rows(m, "data/day_board/{date}_strategy_tickets.json")[0]
        self.assertEqual(tk["status"], "missing")

    def test_stale_and_missing_recorded(self):
        m, _ = self.build()
        ins = self.rows(m, "data/insider/{date}_insider.json")[0]
        self.assertEqual((ins["status"], ins["as_of_date"]), ("stale", "2026-09-03"))
        tr = [r for r in m["files"] if r["repo"] == "theme-radar"]
        self.assertTrue(tr and all(r["status"] == "missing" for r in tr))
        self.assertEqual(m["sources"]["theme-radar"]["status"], "missing")
        ext = [r for r in m["files"] if r["repo"] == "external"]
        self.assertEqual(ext[0]["status"], "missing")

    def test_write_day_append_only(self):
        m, blobs = self.build()
        out = Path(self.tmp.name) / "out"
        fi.write_day(out, "2026-10-06", m, blobs)
        man = json.loads((out / "2026-10-06" / "manifest.json").read_text())
        self.assertEqual(man["status"], "frozen")
        with self.assertRaises(SystemExit):
            fi.write_day(out, "2026-10-06", m, blobs)

    def test_freeze_refuses_after_open(self):
        with self.assertRaises(SystemExit):
            fi.main(["freeze", "--date", "2026-10-06", "--now", "2026-10-06T13:31:00Z",
                     "--fullscan", str(self.repo), "--out-root", str(Path(self.tmp.name) / "o")])

    def test_dry_run_writes_nothing(self):
        out = Path(self.tmp.name) / "dry"
        rc = fi.main(["freeze", "--date", "2026-10-06", "--now", "2026-10-06T13:20:00Z",
                      "--fullscan", str(self.repo), "--out-root", str(out), "--dry-run"])
        self.assertEqual(rc, 0)
        self.assertFalse(out.exists())


class CheckTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = Path(self.tmp.name)
        sh(self.repo, "init", "-q", "-b", "main")
        self.base = commit(self.repo, {"research/input_freeze/2026-10-07/manifest.json": "{}",
                                       "README.md": "r"}, "2026-10-07T13:21:00Z")

    def tearDown(self):
        self.tmp.cleanup()

    def test_new_day_ok(self):
        head = commit(self.repo, {"research/input_freeze/2026-10-08/manifest.json": "{}"},
                      "2026-10-08T13:21:00Z")
        self.assertEqual(fi.changed_day_files(self.repo, self.base, head), [])
        self.assertEqual(fi.only_new_day(self.repo, self.base, head, "2026-10-08"), [])
        self.assertEqual(fi.main(["check", "--repo", str(self.repo), "--base", self.base,
                                  "--head", head, "--only-day", "2026-10-08"]), 0)

    def test_past_day_change_fails(self):
        head = commit(self.repo, {"research/input_freeze/2026-10-07/manifest.json": "{\"x\":1}"},
                      "2026-10-08T13:21:00Z")
        self.assertEqual(fi.main(["check", "--repo", str(self.repo), "--base", self.base,
                                  "--head", head]), 1)

    def test_add_into_past_day_fails(self):
        head = commit(self.repo, {"research/input_freeze/2026-10-07/files/extra.txt": "x"},
                      "2026-10-08T13:21:00Z")
        self.assertTrue(fi.changed_day_files(self.repo, self.base, head))

    def test_delete_past_day_fails(self):
        sh(self.repo, "rm", "-q", "-r", "research/input_freeze/2026-10-07")
        sh(self.repo, "commit", "-q", "-m", "rm", date="2026-10-08T13:21:00Z")
        head = sh(self.repo, "rev-parse", "HEAD").strip()
        self.assertTrue(fi.changed_day_files(self.repo, self.base, head))

    def test_freeze_commit_outside_day_flagged(self):
        head = commit(self.repo, {"research/input_freeze/2026-10-08/manifest.json": "{}",
                                  "README.md": "changed"}, "2026-10-08T13:21:00Z")
        self.assertEqual(fi.only_new_day(self.repo, self.base, head, "2026-10-08"), ["README.md"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
