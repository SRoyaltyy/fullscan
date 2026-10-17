"""Locked calendar and pins for breadth_mine_v1.

No scores live here. The preregistration fingerprint and the input
manifest are the protocol. A later score reads them and does not edit them.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PREREG = ROOT / "research" / "breadth_mine_v1" / "PREREG.md"
MANIFEST_PATH = ROOT / "research" / "breadth_mine_v1" / "INPUT_MANIFEST.json"
RETURNS = ROOT / "research" / "breadth_mine_v1" / "returns"
MARKER = "<!-- BEGIN COVERED -->\n"
STUDY = "breadth_mine_v1"
STUDY_LABEL = "assumed pre-open, not server-proven"
PRIOR_LUCK = 9390
# Filled when the preregistration is fingerprinted. The test refuses a mismatch.
FINGERPRINT = "2dc65df4207938f3f0325168b68f730576d92b31aab32e1f40fe43941c302531"
MANIFEST_SHA256 = "ec5f49855b03b308b475509bdda14a6d0b8e82dc2d9f2458a4f311365a9094dc"

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
DESIGNED_AFTER_START = "2026-09-14"
DESIGNED_AFTER_END = "2026-09-25"

BAR_PATH = "data/prices/ohlc.parquet"
BAR_COMMIT = "ff996f535e1343dd739cc801780ae224018bd96c"
BAR_BLOB_SHA = "3456f7f489a6fa7033e8ae5cc942d8279f0113e3"
BAR_SHA256 = "559c8cf099808930bef2b4de4280b4e902883c9a1de85c8a417074f11aaefa55"
FEE_SHA256 = "019ebdba0fc0b20e02c91f116dc5591b81e96630f60c110dfbfd3b5d8e16c0d3"


class ProtocolError(Exception):
    """The fingerprinted prereg or the input manifest does not match."""


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


def assert_prereg() -> None:
    text = PREREG.read_text(encoding="utf-8")
    digest = fingerprint_sha256(text)
    claimed = header_fingerprint(text)
    if digest != claimed or digest != FINGERPRINT:
        raise ProtocolError(f"prereg fingerprint {digest} != {claimed}")
    raw = MANIFEST_PATH.read_bytes()
    got = hashlib.sha256(raw).hexdigest()
    if got != MANIFEST_SHA256 or MANIFEST_SHA256 not in text:
        raise ProtocolError(f"input manifest sha256 {got} != pin")
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
    index: dict[tuple[str, str, str], dict] = {}
    for row in data["inputs"]:
        index[(row["date"], row["input"], row["path"])] = row
    return data, index


def roles_on(index: dict[tuple[str, str, str], dict], day: str) -> set[str]:
    present = {row[1] for row in index if row[0] == day}
    if "ab_checklist" in present and "ab_enriched" in present:
        present.add("ab")
    return present
