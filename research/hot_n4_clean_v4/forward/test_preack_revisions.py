"""preack_revisions: acks match what the seal counts, and write nothing else."""
from __future__ import annotations

import sys
import tempfile
from types import SimpleNamespace
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.hot_n4_clean_v4.forward.ledger import canonical_bytes  # noqa: E402
from research.hot_n4_clean_v4.forward.preack_revisions import ack_lines  # noqa: E402
from research.hot_n4_clean_v4.forward.prices import (  # noqa: E402
    _ack_key, acks_path, approved_ack_keys, load_acks,
)

NOW = datetime(2026, 10, 28, 11, 30, tzinfo=timezone.utc)


def _bar(t, d, o, h, l, c):
    return {"ticker": t, "date": d, "open": o, "high": h, "low": l, "close": c, "volume": 1000.0}


def main() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        folder = Path(tmp)
        book = SimpleNamespace(folder=folder, folder_name="forward_h1")
        records = [{"kind": "open_fill", "date": "2026-10-27", "sha256": "a" * 64,
                    "buys": [{"ticker": "CHKP"}, {"ticker": "SUB"}, {"ticker": "SPL"}],
                    "sells": [], "holdings": []}]
        stored = [_bar("CHKP", "2026-10-27", 158.080002, 160, 157, 159),
                  _bar("SUB", "2026-10-27", 10.0, 11, 9, 10),
                  _bar("SPL", "2026-10-27", 10.0, 11, 9, 10),
                  _bar("FREE", "2026-10-27", 10.0, 11, 9, 10)]
        fetched = {"bars": [_bar("CHKP", "2026-10-27", 157.729996, 160.005, 157, 159),
                            _bar("SUB", "2026-10-27", 10.01, 11, 9, 10),   # 1c: seal ignores
                            _bar("SPL", "2026-10-27", 5.0, 5.5, 4.5, 5),   # split: skip
                            _bar("FREE", "2026-10-27", 12.0, 11, 9, 10)],  # not sealed
                   "splits": [{"ticker": "SPL"}]}
        rows, skipped = ack_lines(book, records, stored, fetched, NOW, "1")
        assert [(r["ticker"], r["field"]) for r in rows] == [("CHKP", "open")], rows
        assert skipped and all("SPL" in x for x in skipped), skipped
        path = acks_path(folder)
        path.write_bytes(b"".join(canonical_bytes(r) for r in rows))
        keys = approved_ack_keys(load_acks(folder))
        assert _ack_key("CHKP", "2026-10-27", "open", 158.080002, 157.729996) in keys
        again, _ = ack_lines(book, records, stored, fetched, NOW, "2")
        assert again == [], "second run must not duplicate"
        assert sorted(p.name for p in folder.iterdir()) == ["price_revision_acks.jsonl"]
    print("ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
