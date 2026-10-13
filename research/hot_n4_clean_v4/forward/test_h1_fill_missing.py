"""The 2026-10-09 fill-missing note unblocks the h1 2026-10-13 seal, add-only.

Ledger seq 1-162 and every sealed jsonl stay byte-identical. With the notes
installed the 10-09 plan is no longer the open plan and the next session is
2026-10-13. The #543 gate still refuses 10-13 without the general predict
and lets it through with it. Holdup is untouched.
"""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.hot_n4_clean_v4.forward import forward, h1_fill_missing, h1_seal_gate, ledger  # noqa: E402

H1_DIR = ROOT / "research/hot_n4_clean_v4/forward_h1"
SEALED = sorted(H1_DIR.glob("*.jsonl")) + sorted((ROOT / "research/hot_n4_clean_v4/forward").glob("*.jsonl"))
SEALED = [p for p in SEALED if p.name != h1_fill_missing.NOTES_NAME]
PLAN_SHA = "6731f04041fff7bddb4e21fd953b0ba5af9c553468afd2279375a954376773b0"


def _digest() -> dict[str, str]:
    return {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in SEALED}


def _repo(tmp: Path, session: str, with_predict: bool) -> Path:
    repo = tmp / ("with" if with_predict else "without")
    repo.mkdir()

    def git(*args: str) -> None:
        subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)

    git("init", "-q")
    git("config", "user.email", "t@t")
    git("config", "user.name", "t")
    (repo / "README").write_text("x\n")
    if with_predict:
        rel = repo / h1_seal_gate.predict_rel(session)
        rel.parent.mkdir(parents=True)
        rel.write_text("Prediction: UP total score 1.0\n")
    git("add", "-A")
    git("commit", "-q", "-m", "seed")
    return repo


class FillMissing(unittest.TestCase):
    def setUp(self):
        self.saved = {k: os.environ.get(k) for k in ("FORWARD_BOOK", "HOLDUP_MODE", "PLAN_SESSION")}
        os.environ["FORWARD_BOOK"] = "h1"
        os.environ["HOLDUP_MODE"] = "plan"
        os.environ.pop("PLAN_SESSION", None)
        self.before = _digest()
        self.ledger_lines = (H1_DIR / "LEDGER.jsonl").read_bytes().splitlines(keepends=True)

    def tearDown(self):
        h1_fill_missing.uninstall()
        for k, v in self.saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        self.assertEqual(_digest(), self.before, "a sealed jsonl changed")
        self.assertEqual((H1_DIR / "LEDGER.jsonl").read_bytes().splitlines(keepends=True)[:162], self.ledger_lines[:162])

    def test_seq_162_is_the_sealed_plan_and_verifies(self):
        rows = [json.loads(x) for x in self.ledger_lines]
        self.assertEqual([r["seq"] for r in rows[:162]], list(range(1, 163)))
        self.assertEqual((rows[161]["kind"], rows[161]["date"], rows[161]["sha256"]), ("plan", "2026-10-09", PLAN_SHA))
        records = ledger.load()  # raises if any line disagrees with LEDGER.jsonl
        self.assertEqual(records[-1]["sha256"], PLAN_SHA)

    def test_without_notes_installed_h1_is_blocked(self):
        records = ledger.load()
        self.assertEqual(forward.open_plan(records)["date"], "2026-10-09")

    def test_notes_unblock_10_13(self):
        h1_fill_missing.install()
        records = ledger.load()
        self.assertIsNone(forward.open_plan(records))
        self.assertIsNone(h1_seal_gate.open_plan(records))
        self.assertEqual(forward.plan_target(records, ""), ("2026-10-13", None))
        self.assertEqual(forward.plan_target(records, "2026-10-13"), ("2026-10-13", None))
        self.assertIsNotNone(forward.plan_target(records, "2026-10-12")[1])

    def test_gate_refuses_without_predict_allows_with(self):
        h1_fill_missing.install()
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            without = _repo(tmp, "2026-10-13", False)
            with_ = _repo(tmp, "2026-10-13", True)
            ran = []
            run = lambda: ran.append(1) or 0  # noqa: E731
            self.assertEqual(h1_seal_gate.main(run, lambda: "2026-10-13", without, "HEAD"), 1)
            self.assertEqual(ran, [])
            self.assertEqual(h1_seal_gate.main(run, lambda: "2026-10-13", with_, "HEAD"), 0)
            self.assertEqual(ran, [1])

    def test_holdup_untouched(self):
        h1_fill_missing.install()
        os.environ.pop("FORWARD_BOOK", None)
        self.assertEqual(h1_fill_missing.noted_dates([]), set())

    def test_bad_note_refuses(self):
        records = ledger.load()
        bad = [{"date": "2026-10-09", "kind": "fill_missing", "plan_sha256": "0" * 64, "recipe": "union_hot_n4_h1__w0"}]
        with self.assertRaises(RuntimeError):
            h1_fill_missing.check_notes(records, bad)
        filled = [{"date": "2026-10-08", "kind": "session_missing", "recipe": "union_hot_n4_h1__w0"}]
        with self.assertRaises(RuntimeError):
            h1_fill_missing.check_notes(records, filled)


if __name__ == "__main__":
    unittest.main()
