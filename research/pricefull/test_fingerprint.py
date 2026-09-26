"""The preregistration fingerprint covers the protocol body and nothing else."""
from __future__ import annotations

import hashlib
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PREREG = ROOT / "PREREG.md"
MARKER = "<!-- BEGIN COVERED -->\n"


def covered_bytes(text: str) -> bytes:
    i = text.find(MARKER)
    assert i >= 0
    assert text.find(MARKER, i + 1) < 0
    return text[i + len(MARKER):].encode("utf-8")


def header_fingerprint(text: str) -> str:
    for line in text.splitlines():
        if line.startswith("- fingerprint_sha256:"):
            return line.split(":", 1)[1].strip()
    raise AssertionError("fingerprint line missing")


def test_prereg_fingerprint_matches_covered_body() -> None:
    raw = PREREG.read_bytes()
    assert b"\r" not in raw
    text = raw.decode("utf-8")
    assert text.endswith("\n")
    digest = hashlib.sha256(covered_bytes(text)).hexdigest()
    assert header_fingerprint(text) == digest
    assert digest == "0dc51aeced8323f5466145e2311acd66a89319548ae5705db06c235b81200f3e"
