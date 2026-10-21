"""Locked calendar and pins for breadth_mine_v1e.

No scores live here. breadth_mine_v1, breadth_mine_v1b, and breadth_mine_v1c stay untouched.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PREREG = ROOT / "research" / "breadth_mine_v1e" / "PREREG.md"
MANIFEST_PATH = ROOT / "research" / "breadth_mine_v1e" / "INPUT_MANIFEST.json"
RETURNS = ROOT / "research" / "breadth_mine_v1e" / "returns"
MARKER = "<!-- BEGIN COVERED -->\n"
STUDY = "breadth_mine_v1e"
STUDY_LABEL = "assumed pre-open, not server-proven"
FINGERPRINT = "c5e58acf2547b70cee511357edd5adc1a891cdc9225956c3b8a0dbd667dae535"
MANIFEST_SHA256 = "980367c54e3edc3a44db8477b3be1e276ef2d7a7f7ea3f2a25fed112b251fe2f"
BAR_PATH = "research/breadth_mine_v1e/bars/ohlc.parquet"
BAR_SHA256 = "06a905777f571020dc7eefdfc42ea8489905ae5d8ed72fbc97f42cc513888154"
BAR_BLOB_SHA = "68eaa3500faa047f0dc198de30d093db87d5a15e"
SPLIT_PATH = "research/breadth_mine_v1e/bars/splits.json"
SPLIT_SHA256 = "40c3733b6428f885239236e358ba4ee6f5c5b6ac9f77c5f30e206aa8daa4d338"
SPLIT_BLOB_SHA = "b5380bede11ea265d084d384cb12289a0929938f"
FORMATION_END = "2026-09-13"
FEE_SHA256 = "019ebdba0fc0b20e02c91f116dc5591b81e96630f60c110dfbfd3b5d8e16c0d3"
CREATION_DATE = "2026-09-26"
OOS_START = "2026-09-14"

SESSIONS: tuple[str, ...] = (
    "2026-08-13", "2026-08-14", "2026-08-17", "2026-08-18", "2026-08-19",
    "2026-08-20", "2026-08-21", "2026-08-24", "2026-08-25", "2026-08-26",
    "2026-08-27", "2026-08-28", "2026-08-31", "2026-09-01", "2026-09-02",
    "2026-09-03", "2026-09-04", "2026-09-08", "2026-09-09", "2026-09-10",
    "2026-09-11", "2026-09-14", "2026-09-15", "2026-09-16", "2026-09-17",
    "2026-09-18", "2026-09-21", "2026-09-22", "2026-09-23", "2026-09-24",
    "2026-09-25",
)
RUN_WINDOW: tuple[str, ...] = tuple(day for day in SESSIONS if day <= "2026-09-11")
CHECK_CALENDAR: tuple[str, ...] = tuple(day for day in RUN_WINDOW if day >= "2026-08-20")
DESIGNED_AFTER_START = "2026-08-13"
DESIGNED_AFTER_END = "2026-09-25"


class ProtocolError(Exception):
    """The fingerprinted prereg or a pin does not match."""


def covered_bytes(text: str) -> bytes:
    idx = text.index(MARKER) + len(MARKER)
    return text[idx:].encode("utf-8")


def fingerprint_sha256(text: str | None = None) -> str:
    if text is None:
        text = PREREG.read_text(encoding="utf-8")
    return hashlib.sha256(covered_bytes(text)).hexdigest()


def header_fingerprint(text: str) -> str:
    for line in text.splitlines():
        if line.startswith("- fingerprint_sha256:"):
            return line.split(":", 1)[1].strip()
    raise ProtocolError("fingerprint line missing")


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def assert_prereg() -> None:
    text = PREREG.read_text(encoding="utf-8")
    digest = fingerprint_sha256(text)
    claimed = header_fingerprint(text)
    if digest != claimed or digest != FINGERPRINT:
        raise ProtocolError(f"prereg fingerprint {digest} != {claimed}")
    for label, path, pin in (
        ("input manifest", MANIFEST_PATH, MANIFEST_SHA256),
        ("bar snapshot", ROOT / BAR_PATH, BAR_SHA256),
        ("split events", ROOT / SPLIT_PATH, SPLIT_SHA256),
    ):
        got = _sha(path)
        if got != pin or pin not in text:
            raise ProtocolError(f"{label} sha256 {got} != {pin}")
    if STUDY_LABEL not in text:
        raise ProtocolError("study label missing")


def load_manifest() -> tuple[dict, dict[tuple[str, str, str], dict]]:
    assert_prereg()
    data = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    if data.get("study") != STUDY or data.get("label") != STUDY_LABEL:
        raise ProtocolError("manifest study label")
    bars = data.get("bars") or {}
    if bars.get("blob_sha") != BAR_BLOB_SHA or bars.get("sha256") != BAR_SHA256:
        raise ProtocolError("bar pin")
    if bars.get("splits_sha256") != SPLIT_SHA256:
        raise ProtocolError("split pin")
    index: dict[tuple[str, str, str], dict] = {}
    for row in data["inputs"]:
        index[(row["date"], row["input"], row["path"])] = row
    return data, index


def roles_on(index: dict[tuple[str, str, str], dict], day: str) -> set[str]:
    present = {row[1] for row in index if row[0] == day}
    if "ab_checklist" in present and "ab_enriched" in present:
        present.add("ab")
    return present
