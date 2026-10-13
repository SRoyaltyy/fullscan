"""holdup_restart: next session jumps to the restart open, in memory only."""
import os
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from research.hot_n4_clean_v4.forward import holdup_restart as hr  # noqa: E402
from research.hot_n4_clean_v4.forward import ledger  # noqa: E402
from research.hot_n4_clean_v4.forward.book import HOLDUP, H1, use_book, reset_book  # noqa: E402


def recs():
    return [{"date": "2026-09-25", "kind": "fill"}, {"date": "2026-10-05", "kind": "missing"}]


class Restart(unittest.TestCase):
    def setUp(self):
        os.environ.pop("FORWARD_BOOK", None)
        self.notes = [{"kind": "restart", "from": "2026-10-14", "recipe": HOLDUP.recipe}]
        self._load = hr.load_notes
        hr.load_notes = lambda path=None: self.notes
        self.tok = None

    def tearDown(self):
        hr.load_notes = self._load
        if self.tok is not None:
            reset_book(self.tok)

    def test_floor_is_session_before_restart(self):
        self.assertEqual(hr.restart_floor(recs(), self.notes), "2026-10-13")

    def test_book_dates_extended_only_in_memory(self):
        r = recs()
        self.assertEqual(hr.book_dates(r), ["2026-09-25", "2026-10-13"])
        self.assertEqual(r, recs())

    def test_inert_once_ledger_reaches_restart(self):
        r = recs() + [{"date": "2026-10-14", "kind": "plan"}]
        self.assertEqual(hr.book_dates(r), hr._ORIG_BOOK_DATES(r))

    def test_h1_untouched(self):
        self.tok = use_book(H1)
        self.assertEqual(hr.book_dates(recs()), ["2026-09-25"])

    def test_real_notes_file_valid(self):
        hr.load_notes = self._load
        notes = hr.load_notes()
        self.assertTrue(all(n["from"] >= "2026-10-14" for n in notes))

    def test_ledger_has_no_lines_between(self):
        hr.load_notes = self._load
        self.tok = use_book(HOLDUP)
        real = ledger.load()
        self.assertFalse([x for x in real if "2026-10-06" <= x["date"] <= "2026-10-13"])


if __name__ == "__main__":
    unittest.main()
