"""Locked calendar and pins for breadth_rank_v1b.

No scores live here. breadth_rank_v1 and the breadth_mine studies are
not imported and are not rewritten.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PREREG = ROOT / "research" / "breadth_rank_v1b" / "PREREG.md"
MANIFEST_PATH = ROOT / "research" / "breadth_rank_v1b" / "INPUT_MANIFEST.json"
RETURNS = ROOT / "research" / "breadth_rank_v1b" / "returns"
FROZEN_PATH = ROOT / "research" / "breadth_rank_v1b" / "FROZEN_RANK.json"
BAR_PATH = "research/breadth_rank_v1b/bars/ohlc.parquet"
SPLIT_PATH = "research/breadth_rank_v1b/bars/splits.json"
DROPPED_PATH = "research/breadth_rank_v1b/bars/DROPPED.json"
FEE_PATH = "00_grounding/futubull_fees.json"
MARKER = "<!-- BEGIN COVERED -->\n"
STUDY = "breadth_rank_v1b"
STUDY_LABEL = "assumed pre-open, not server-proven"
CREATION_DATE = "2026-09-26"
OOS_START = "2026-09-14"
RANK_END = "2026-09-11"
FIT_CUTOFF = "2026-09-13"
LAST_SESSION = "2026-09-25"

FINGERPRINT = "c6aa22dc78ec9385818e768f88fb9944b7ef490b68dc9fbdacf50426822a8093"
MANIFEST_SHA256 = "014f6a9d635dfd3158fabc65081106043ecb12b72d10926b6f774e9e74ff0e01"
BAR_SHA256 = "8ac67b7110176e954c6cc938dea58f821aabef352da6205fa3b79015505a5339"
BAR_BLOB_SHA = "eaad5e25fe223314815c690dfd9376b7aa7cbcab"
SPLIT_SHA256 = "2bcbb61cc8c13cc0f590480c995f093cd893c268a7a129574c4c7c17c236c091"
SPLIT_BLOB_SHA = "7f67193aac2aab348dd4d4a27248b54e5ab15a4a"
DROPPED_SHA256 = "924e0efd448cba93b4500e5d35f67337ba488f56be25941a878e49f04bd33aad"
DROPPED_BLOB_SHA = "93726f0bbf52207b5be4edea5eadf1984b1846c4"
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
RANK_SESSIONS: tuple[str, ...] = tuple(day for day in SESSIONS if day <= RANK_END)
TEST_SESSIONS: tuple[str, ...] = tuple(day for day in SESSIONS if day >= OOS_START)


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


def file_sha256(path: Path) -> str:
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
    pins = (
        ("input manifest", MANIFEST_PATH, MANIFEST_SHA256),
        ("bar snapshot", ROOT / BAR_PATH, BAR_SHA256),
        ("split events", ROOT / SPLIT_PATH, SPLIT_SHA256),
        ("dropped tickers", ROOT / DROPPED_PATH, DROPPED_SHA256),
        ("futubull fees", ROOT / FEE_PATH, FEE_SHA256),
    )
    for label, path, pin in pins:
        got = file_sha256(path)
        if got != pin or pin not in text:
            raise ProtocolError(f"{label} sha256 {got} != {pin}")
    if STUDY_LABEL not in text:
        raise ProtocolError("study label missing")
    if "nothing fitted after 2026-09-13" not in text:
        raise ProtocolError("fit cutoff missing")


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
    if bars.get("dropped_sha256") != DROPPED_SHA256:
        raise ProtocolError("dropped pin")
    index: dict[tuple[str, str, str], dict] = {}
    for row in data["inputs"]:
        index[(row["date"], row["input"], row["path"])] = row
    return data, index
