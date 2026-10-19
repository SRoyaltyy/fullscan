"""The 2026-10-16 fill-missing note and the 2026-10-19 session-missing note, add-only.

Ledger seq 1-165 stay byte-identical; the 10-16 plan (seq 165) stays exactly
as sealed. With the notes installed and the ledger as of seq 165, the 10-16
plan is no longer the open plan, 10-19 is passed as missing, and the next
session the seal may take is 2026-10-20. A 10-20 plan line passes the
append gate on a temp copy of the book. No backfill, no Webull.
"""
from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.hot_n4_clean_v4.forward import forward, h1_fill_missing, ledger, prices  # noqa: E402

H1_DIR = ROOT / "research/hot_n4_clean_v4/forward_h1"
PLAN_SHA = "c38b12490f6899e398d3fcf730fbb255ae3aa1918d1ab976aab0281d55bdc8d4"
SEQ = 165


class FillMissing1016(unittest.TestCase):
    def setUp(self):
        self.saved = {k: os.environ.get(k) for k in ("FORWARD_BOOK", "PLAN_SESSION")}
        os.environ["FORWARD_BOOK"] = "h1"
        os.environ.pop("PLAN_SESSION", None)

    def tearDown(self):
        h1_fill_missing.uninstall()
        for k, v in self.saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def test_seq_165_is_the_sealed_10_16_plan(self):
        rows = [json.loads(x) for x in (H1_DIR / "LEDGER.jsonl").read_bytes().splitlines()]
        self.assertEqual((rows[SEQ - 1]["seq"], rows[SEQ - 1]["kind"], rows[SEQ - 1]["date"], rows[SEQ - 1]["sha256"]),
                         (SEQ, "plan", "2026-10-16", PLAN_SHA))

    def test_notes_are_add_only_and_match(self):
        notes = h1_fill_missing.load_notes()
        by = {(n["date"], n["kind"]): n for n in notes}
        note = by[("2026-10-16", "fill_missing")]
        self.assertEqual((note["plan_seq"], note["plan_sha256"]), (SEQ, PLAN_SHA))
        self.assertTrue(note["no_backfill"] and note["no_webull"])
        self.assertIn(("2026-10-19", "session_missing"), by)

    def test_notes_unblock_10_20(self):
        h1_fill_missing.install()
        records = ledger.load()[:SEQ]
        self.assertIsNone(forward.open_plan(records))
        self.assertEqual(forward.plan_target(records, ""), ("2026-10-20", None))
        self.assertEqual(forward.plan_target(records, "2026-10-20"), ("2026-10-20", None))
        self.assertIsNotNone(forward.plan_target(records, "2026-10-19")[1])
        self.assertEqual(prices.target_session(records, forward.next_session), "2026-10-20")

    def test_10_20_plan_passes_the_append_gate(self):
        body = {"date": "2026-10-20", "excluded_unexplained_legs": [], "kind": "plan",
                "picks": [], "planned_sells": [], "recipe": "union_hot_n4_h1__w0"}
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            log = (H1_DIR / "h1_log.jsonl").read_bytes().splitlines(keepends=True)[:SEQ]
            led = (H1_DIR / "LEDGER.jsonl").read_bytes().splitlines(keepends=True)[:SEQ]
            (tmp / "h1_log.jsonl").write_bytes(b"".join(log))
            (tmp / "LEDGER.jsonl").write_bytes(b"".join(led))
            with self.assertRaises(RuntimeError):
                ledger.append_records([dict(body)], folder=tmp)
            h1_fill_missing.install()
            sealed = ledger.append_records([dict(body)], folder=tmp)
            self.assertEqual((sealed[0]["kind"], sealed[0]["date"]), ("plan", "2026-10-20"))
            self.assertEqual((tmp / "h1_log.jsonl").read_bytes().splitlines(keepends=True)[:SEQ], log)


if __name__ == "__main__":
    unittest.main()
