"""v4 engine pins must match the files on disk.

Run: python -m src.test_v4_engine_pins
"""
from __future__ import annotations

import hashlib
from pathlib import Path

from research.hot_n4_clean_v4.protocol import (
    ENGINE_SHA256,
    FEES_PATH,
    FEES_SHA256,
)

ROOT = Path(__file__).resolve().parents[1]


def _sha(rel: str) -> str:
    return hashlib.sha256((ROOT / rel).read_bytes()).hexdigest()


def test_engine_and_fee_pins() -> None:
    bad = []
    for rel, digest in ENGINE_SHA256.items():
        got = _sha(rel)
        if got != digest:
            bad.append(f"{rel} {got} != {digest}")
    fees = _sha(FEES_PATH)
    if fees != FEES_SHA256:
        bad.append(f"{FEES_PATH} {fees} != {FEES_SHA256}")
    if bad:
        raise SystemExit("engine pin mismatch:\n" + "\n".join(bad))


def main() -> None:
    test_engine_and_fee_pins()
    print(f"ok {len(ENGINE_SHA256)} engine files + fees")


if __name__ == "__main__":
    main()
