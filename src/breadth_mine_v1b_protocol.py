"""Locked calendar and pins for breadth_mine_v1b.

No scores live here. The preregistration fingerprint, the input
manifest, and the fresh Yahoo bar snapshot are the protocol.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PREREG = ROOT / "research" / "breadth_mine_v1b" / "PREREG.md"
MANIFEST_PATH = ROOT / "research" / "breadth_mine_v1b" / "INPUT_MANIFEST.json"
RETURNS = ROOT / "research" / "breadth_mine_v1b" / "returns"
BAR_AUDIT = ROOT / "research" / "breadth_mine_v1b" / "BAR_AUDIT.json"
MARKER = "<!-- BEGIN COVERED -->\n"
STUDY = "breadth_mine_v1b"
STUDY_LABEL = "assumed pre-open, not server-proven"
# Filled when the preregistration is fingerprinted. The test refuses a mismatch.
FINGERPRINT = "a396ff65a36c54908b28533d1721b730c0fbf9123bf436f17c97ef9c32cb1939"
MANIFEST_SHA256 = "88c68f70a217005939c8cbfb14a33e71c1362ea0333d452a98d52669340c1606"
BAR_PATH = "research/breadth_mine_v1b/bars/ohlc.parquet"
BAR_SHA256 = "53ea564340a6d7fb452dc44c798f583d50d39968af145b577a8929d5e1daa797"
BAR_BLOB_SHA = "5a7927a31aaf6eb213b31db175a167a79303af69"
SPLIT_PATH = "research/breadth_mine_v1b/bars/splits.json"
SPLIT_SHA256 = "18e586e42670c4214fffc4f9a40dde340d6ec1bf75a409ac538973f209ca052a"
SPLIT_BLOB_SHA = "c171dbbc11737d4e71d543ee4a87810691e9cf7d"
AUDIT_SHA256 = "3970bf3e48b1c50bb25c8efd5ecd8651b45b74b0b4154cd9f2ac601d6671be06"
FEE_SHA256 = "019ebdba0fc0b20e02c91f116dc5591b81e96630f60c110dfbfd3b5d8e16c0d3"

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
        ("bar audit", BAR_AUDIT, AUDIT_SHA256),
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
    if bars.get("splits_sha256") != SPLIT_SHA256 or bars.get("splits_blob_sha") != SPLIT_BLOB_SHA:
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
