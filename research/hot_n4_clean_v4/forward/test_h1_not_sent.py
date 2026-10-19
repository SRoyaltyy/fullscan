"""h1 post-close fill on a day nothing was sent: add-only fill_missing note, exit 0.

10-09, 10-13 and 10-16 each failed 'Seal the post-close fill' with
'sent share lock missing ... refusing to size buys from the open' and left
the plan open, which blocked the next seal. h1_not_sent.py turns exactly
that case (no journal, paper status not submit:true, no open fill) into a
fill_missing note. Every other refusal still refuses. No ledger line is
written or rewritten.
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.hot_n4_clean_v4.forward import h1_fill_missing, h1_not_sent, ledger, share_lock  # noqa: E402

DAY = "2026-10-16"
PLAN = {"date": DAY, "kind": "plan", "recipe": "union_hot_n4_h1__w0", "sha256": "ab" * 32}


class NotSent(unittest.TestCase):
    def setUp(self):
        self.saved = {k: os.environ.get(k) for k in ("FORWARD_BOOK", "HOLDUP_MODE", "SENT_SHARE_LOCK_ROOT")}
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        (self.root / "data" / "paper_open").mkdir(parents=True)
        os.environ.update(FORWARD_BOOK="h1", HOLDUP_MODE="fill", SENT_SHARE_LOCK_ROOT=str(self.root))
        self.orig = h1_not_sent._ORIG_DECIDE_FILL
        self.raise_with = share_lock.SentShareLockMissing(h1_not_sent.missing_reason(DAY))

        def fake(records, bars, fees, index):
            raise self.raise_with

        h1_not_sent._ORIG_DECIDE_FILL = fake
        h1_not_sent.PENDING.clear()

    def tearDown(self):
        h1_not_sent._ORIG_DECIDE_FILL = self.orig
        h1_not_sent.PENDING.clear()
        self.tmp.cleanup()
        for k, v in self.saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def _status(self, payload):
        (self.root / "data" / "paper_open" / f"{DAY}_status.json").write_text(json.dumps(payload))

    def _journal(self):
        (self.root / "data" / "paper_open" / f"{DAY}_submit.json").write_text("{}")

    def _call(self, records=None):
        return h1_not_sent.decide_fill(records or [dict(PLAN)], {}, {}, {})

    def test_missed_deadline_day_writes_one_note_and_returns_clean(self):
        self._status({"date": DAY, "status": "missed_deadline", "submit": False,
                      "events": [{"submit": False, "note": "day record kept; no order sent"}]})
        bodies, why = self._call()
        self.assertEqual(bodies, [])
        self.assertIn("not sent, fill missing", why)
        self.assertEqual(len(h1_not_sent.PENDING), 1)
        note = h1_not_sent.PENDING[0]
        self.assertEqual((note["kind"], note["date"], note["plan_seq"], note["plan_sha256"]),
                         ("fill_missing", DAY, 1, PLAN["sha256"]))
        self.assertTrue(note["no_backfill"] and note["no_webull"])
        self.assertEqual(note["recipe"], "union_hot_n4_h1__w0")

    def test_no_status_file_still_counts_as_not_sent(self):
        self.assertIsNotNone(h1_not_sent.nothing_sent(DAY))
        self.assertEqual(self._call()[0], [])

    def test_journal_present_still_refuses(self):
        self._journal()
        with self.assertRaises(share_lock.SentShareLockMissing):
            self._call()
        self.assertEqual(h1_not_sent.PENDING, [])

    def test_status_submit_true_still_refuses(self):
        self._status({"date": DAY, "submit": True})
        with self.assertRaises(share_lock.SentShareLockMissing):
            self._call()
        self._status({"date": DAY, "submit": False, "events": [{"submit": True}]})
        with self.assertRaises(share_lock.SentShareLockMissing):
            self._call()

    def test_other_lock_refusals_still_refuse(self):
        self.raise_with = share_lock.SentShareLockMissing(
            "sent share count for X is 3 but the locked count is 4; refusing to pick one")
        with self.assertRaises(share_lock.SentShareLockMissing):
            self._call()

    def test_open_fill_present_still_refuses(self):
        opened = {"date": DAY, "kind": "open_fill", "recipe": "union_hot_n4_h1__w0", "sha256": "cd" * 32}
        with self.assertRaises(share_lock.SentShareLockMissing):
            self._call([dict(PLAN), opened])

    def test_holdup_and_other_modes_untouched(self):
        os.environ["FORWARD_BOOK"] = "holdup"
        with self.assertRaises(share_lock.SentShareLockMissing):
            self._call()
        os.environ["FORWARD_BOOK"] = "h1"
        os.environ["HOLDUP_MODE"] = "open_fill"
        with self.assertRaises(share_lock.SentShareLockMissing):
            self._call()

    def test_append_notes_is_add_only_and_idempotent(self):
        path = self.root / "notes.jsonl"
        first = {"date": "2026-10-09", "kind": "session_missing", "recipe": "union_hot_n4_h1__w0"}
        before = (json.dumps(first, sort_keys=True, separators=(",", ":")) + "\n").encode()
        path.write_bytes(before)
        self._call()
        note = h1_not_sent.PENDING[0]
        self.assertEqual(h1_not_sent.append_notes([note], path), [note])
        self.assertTrue(path.read_bytes().startswith(before))
        self.assertEqual(len(path.read_bytes().splitlines()), 2)
        self.assertEqual(h1_not_sent.append_notes([note], path), [])
        self.assertEqual(len(path.read_bytes().splitlines()), 2)
        self.assertEqual(h1_fill_missing.load_notes(path)[-1]["date"], DAY)

    def test_real_10_16_plan_note_matches_the_sealed_ledger(self):
        h1_fill_missing.install()
        try:
            records = ledger.load()[:165]
            plan = ledger.plan_on(records, DAY)
            note = h1_not_sent.build_note(records, plan, "no journal")
            self.assertEqual((note["plan_seq"], note["plan_sha256"]),
                             (165, "c38b12490f6899e398d3fcf730fbb255ae3aa1918d1ab976aab0281d55bdc8d4"))
            h1_fill_missing.check_notes(records, h1_fill_missing.load_notes() + [note])
        finally:
            h1_fill_missing.uninstall()


if __name__ == "__main__":
    unittest.main()
