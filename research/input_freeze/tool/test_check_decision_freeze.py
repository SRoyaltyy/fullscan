#!/usr/bin/env python3
"""Unit tests for research/input_freeze/tool/check_decision_freeze.py (stdlib only).

Run: python3 research/input_freeze/tool/test_check_decision_freeze.py
"""
from __future__ import annotations

import datetime as dt
import gzip
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import check_decision_freeze as cdc  # noqa: E402

UTC = dt.timezone.utc
DAY = "2026-10-07"


def sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def make_row(rel: str, data: bytes, status: str = "copied", schema: str = "input_freeze/v2",
             stored: bool = True) -> dict:
    row = {"category": "x", "repo": "fullscan", "pattern": rel, "source_path": rel,
           "status": status, "sha256": sha(data), "_data": data}
    if stored and status == "copied":
        gz = gzip.compress(data, compresslevel=9, mtime=0)
        row.update({"stored_path": f"files/fullscan/{rel}.gz", "gzip": True,
                    "stored_bytes": len(gz), "stored_sha256": sha(gz)})
    return row


def write_day(root: Path, *, inputs: dict[str, str], rows: list[dict],
              completed_at: str, freeze_time_utc: str = "2026-10-07T13:20:00Z",
              schema: str = "input_freeze/v2", ticket_name: str = None) -> Path:
    (root / "research/input_freeze" / DAY).mkdir(parents=True, exist_ok=True)
    manifest = {"schema": schema, "date": DAY, "status": "frozen",
                "freeze_time_utc": freeze_time_utc, "files": rows}
    for row in rows:
        payload = row.pop("_data", None)
        if row.get("stored_path") and payload is not None:
            p = root / "research/input_freeze" / DAY / row["stored_path"]
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(gzip.compress(payload, compresslevel=9, mtime=0))
    (root / "research/input_freeze" / DAY / "manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8")
    name = ticket_name or f"{DAY}_strategy_tickets.json"
    tdir = root / "data/day_board"
    tdir.mkdir(parents=True, exist_ok=True)
    ticket = {"date": DAY, "decision_readiness": {"date": DAY, "ready": True,
              "inputs": inputs, "completed_at": completed_at}}
    (tdir / name).write_text(json.dumps(ticket), encoding="utf-8")
    return root


class CheckDayTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_match_and_absent_pass(self):
        data = b"hello inputs"
        write_day(self.root,
                  inputs={"a/file.json": sha(data), "b/missing.csv": "absent"},
                  rows=[make_row("a/file.json", data),
                        make_row("b/missing.csv", b"", status="missing", stored=False)],
                  completed_at="2026-10-07T06:27:11-04:00")
        infos, warns, fails = cdc.check_day(self.root, DAY)
        self.assertEqual(fails, [])
        self.assertEqual(warns, [])
        self.assertTrue(any(l.startswith("match") for l in infos))
        self.assertTrue(any(l.startswith("absent-ok") for l in infos))

    def test_mismatch_before_freeze_is_drift_ok(self):
        write_day(self.root,
                  inputs={"a/file.json": sha(b"at publish time")},
                  rows=[make_row("a/file.json", b"later version")],
                  completed_at="2026-10-07T06:27:11-04:00")
        infos, warns, fails = cdc.check_day(self.root, DAY)
        self.assertEqual(fails, [])
        self.assertTrue(any(l.startswith("drift-ok") for l in infos))

    def test_mismatch_after_freeze_fails(self):
        write_day(self.root,
                  inputs={"a/file.json": sha(b"old")},
                  rows=[make_row("a/file.json", b"newer")],
                  completed_at="2026-10-07T10:00:00-04:00")  # after 13:20Z freeze
        _, _, fails = cdc.check_day(self.root, DAY)
        self.assertEqual(len(fails), 1)
        self.assertIn("MISMATCH", fails[0])

    def test_absent_vs_present_fails(self):
        data = b"it exists"
        write_day(self.root,
                  inputs={"b/file.csv": "absent"},
                  rows=[make_row("b/file.csv", data)],
                  completed_at="2026-10-07T06:27:11-04:00")
        _, _, fails = cdc.check_day(self.root, DAY)
        self.assertEqual(len(fails), 1)
        self.assertIn("ticket says absent", fails[0])

    def test_uncovered_warns_in_v1_fails_in_v2(self):
        write_day(self.root,
                  inputs={"data/factor_mine/panel.json": sha(b"panel")},
                  rows=[], schema="input_freeze/v1",
                  completed_at="2026-10-07T06:27:11-04:00")
        _, warns, fails = cdc.check_day(self.root, DAY)
        self.assertEqual(fails, [])
        self.assertEqual(len(warns), 1)

        write_day(self.root,
                  inputs={"data/factor_mine/panel.json": sha(b"panel")},
                  rows=[], schema="input_freeze/v2",
                  completed_at="2026-10-07T06:27:11-04:00")
        _, warns, fails = cdc.check_day(self.root, DAY)
        self.assertEqual(warns, [])
        self.assertEqual(len(fails), 1)
        self.assertIn("uncovered input", fails[0])

    def test_tampered_stored_copy_fails(self):
        data = b"genuine"
        write_day(self.root,
                  inputs={"a/file.json": sha(data)},
                  rows=[make_row("a/file.json", data)],
                  completed_at="2026-10-07T06:27:11-04:00")
        stored = self.root / "research/input_freeze" / DAY / "files/fullscan/a/file.json.gz"
        stored.write_bytes(gzip.compress(b"TAMPERED", compresslevel=9, mtime=0))
        _, _, fails = cdc.check_day(self.root, DAY)
        self.assertTrue(any("stored copy altered" in f for f in fails))

    def test_missing_stored_copy_fails(self):
        data = b"genuine"
        write_day(self.root,
                  inputs={"a/file.json": sha(data)},
                  rows=[make_row("a/file.json", data)],
                  completed_at="2026-10-07T06:27:11-04:00")
        stored = self.root / "research/input_freeze" / DAY / "files/fullscan/a/file.json.gz"
        stored.unlink()
        _, _, fails = cdc.check_day(self.root, DAY)
        self.assertTrue(any("stored copy missing" in f for f in fails))

    def test_no_inputs_is_failure(self):
        write_day(self.root, inputs={}, rows=[],
                  completed_at="2026-10-07T06:27:11-04:00")
        _, _, fails = cdc.check_day(self.root, DAY)
        self.assertEqual(len(fails), 1)
        self.assertIn("no decision_readiness.inputs", fails[0])

    def test_main_cli_exit_codes(self):
        data = b"ok"
        write_day(self.root,
                  inputs={"a/file.json": sha(data)},
                  rows=[make_row("a/file.json", data)],
                  completed_at="2026-10-07T06:27:11-04:00")
        self.assertEqual(cdc.main(["--root", str(self.root), "--date", DAY]), 0)

    def test_main_no_date_picks_newest_pair(self):
        data = b"ok"
        write_day(self.root,
                  inputs={"a/file.json": sha(data)},
                  rows=[make_row("a/file.json", data)],
                  completed_at="2026-10-07T06:27:11-04:00")
        self.assertEqual(cdc.newest_checkable_date(self.root), DAY)


class ParseTsTests(unittest.TestCase):
    def test_z_and_offsets(self):
        a = cdc.parse_ts("2026-10-07T13:20:00Z")
        b = cdc.parse_ts("2026-10-07T06:20:00-07:00")
        self.assertEqual(a, b)
        self.assertEqual(a.tzinfo, UTC)

    def test_garbage_is_none(self):
        self.assertIsNone(cdc.parse_ts("not a time"))
        self.assertIsNone(cdc.parse_ts(None))


if __name__ == "__main__":
    unittest.main(verbosity=1)
