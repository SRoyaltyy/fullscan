"""Locked calendar and input pins for factor_mine_seq_clean_tape.

No scores live here. The preregistration fingerprint and the input
manifest are the protocol. A later score reads them and does not edit them.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from src.lever_search_proof import build_group3_recipes, required_roles

ROOT = Path(__file__).resolve().parents[1]
PREREG = ROOT / "research" / "lever_search_clean" / "PREREG.md"
MANIFEST_PATH = ROOT / "research" / "lever_search_clean" / "INPUT_MANIFEST.json"
RETURNS = ROOT / "research" / "lever_search_clean" / "returns"
MARKER = "<!-- BEGIN COVERED -->\n"
STUDY = "factor_mine_seq_clean_tape"
STUDY_LABEL = "assumed pre-open, clean tape (split-artefact names absent or corrected)"
LUCK_N = 9500
MANIFEST_SHA256 = "0112bf246bbd947c04d508ca84709e799ba0f44f70686d7d4226c61d6e7f2d37"
FINGERPRINT = "5c29bd10af16edcfd94dda79254df06ac0a29b3e8077432f677ae4d53af5910b"

# The clean tape pins (PR #362 rebuild of the Yahoo bars).
CLEAN_BAR_PATH = "research/breadth_rank_v1c/bars/ohlc.parquet"
CLEAN_BAR_COMMIT = "ad10f862ad51df88f4dd03ff3dbcdf628f7e2685"
CLEAN_BAR_BLOB_SHA = "e7bbac335cfa87243f9d0ddf8c331a0739dd0df8"
CLEAN_BAR_SHA256 = "5c272584309e14496dc006a3a356c6960ed5b340f945af3fd6f299301b261ef2"

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


def load_manifest() -> tuple[dict, dict[tuple[str, str], dict]]:
    assert_prereg()
    data = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    if data.get("study") != STUDY or data.get("label") != STUDY_LABEL:
        raise ProtocolError("manifest study label")
    if data.get("panel_fallback") != "not used":
        raise ProtocolError("panel fallback was not the locked choice")
    bars = data.get("bars") or {}
    if (
        bars.get("path") != CLEAN_BAR_PATH
        or bars.get("commit") != CLEAN_BAR_COMMIT
        or bars.get("blob_sha") != CLEAN_BAR_BLOB_SHA
        or bars.get("sha256") != CLEAN_BAR_SHA256
    ):
        raise ProtocolError("clean tape pin mismatch")
    index: dict[tuple[str, str], dict] = {}
    for row in data["inputs"]:
        index[(row["date"], row["input"])] = row
    return data, index


def role_present(index: dict[tuple[str, str], dict], day: str, role: str) -> bool:
    if role == "ab":
        return (day, "ab_checklist") in index and (day, "ab_enriched") in index
    return (day, role) in index


def search_day_list(recipe: dict, index: dict[tuple[str, str], dict]) -> tuple[str, ...]:
    need = required_roles(recipe)
    return tuple(
        day for day in RUN_WINDOW
        if all(role_present(index, day, role) for role in need)
    )


def check_day_list(recipe: dict, index: dict[tuple[str, str], dict]) -> tuple[str, ...]:
    calendar = set(CHECK_CALENDAR)
    return tuple(day for day in search_day_list(recipe, index) if day in calendar)


def _median(values: list[int]) -> float:
    ordered = sorted(values)
    count = len(ordered)
    mid = count // 2
    if count % 2:
        return float(ordered[mid])
    return (ordered[mid - 1] + ordered[mid]) / 2.0


def group_rows(index: dict[tuple[str, str], dict] | None = None) -> list[dict]:
    if index is None:
        _, index = load_manifest()
    buckets: dict[tuple[str, ...], list[dict]] = {}
    for recipe in build_group3_recipes():
        buckets.setdefault(tuple(sorted(required_roles(recipe))), []).append(recipe)
    rows = []
    for roles, group in buckets.items():
        searches = [len(search_day_list(recipe, index)) for recipe in group]
        checks = [len(check_day_list(recipe, index)) for recipe in group]
        rows.append({
            "roles": roles,
            "n_recipes": len(group),
            "search_min": min(searches),
            "search_median": _median(searches),
            "search_max": max(searches),
            "check_min": min(checks),
            "check_median": _median(checks),
            "check_max": max(checks),
        })
    rows.sort(key=lambda row: (-row["n_recipes"], row["roles"]))
    return rows


def overall_counts(index: dict[tuple[str, str], dict] | None = None) -> dict:
    if index is None:
        _, index = load_manifest()
    searches = []
    checks = []
    for recipe in build_group3_recipes():
        searches.append(len(search_day_list(recipe, index)))
        checks.append(len(check_day_list(recipe, index)))
    return {
        "search_min": min(searches),
        "search_median": _median(searches),
        "search_max": max(searches),
        "check_min": min(checks),
        "check_median": _median(checks),
        "check_max": max(checks),
        "n": len(searches),
    }
