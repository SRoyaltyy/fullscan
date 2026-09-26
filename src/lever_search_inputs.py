"""Initial-run inputs for the lever search.

The scored run is not this module. It pins the server-proven dates, the
dropped Excel rows, and the manifest hashes. A mismatch raises.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = ROOT / "research" / "lever_search" / "INPUT_ASOF_MANIFEST.json"
PREREG_PATH = ROOT / "research" / "lever_search" / "PREREG.md"

# Layer A atoms (41) plus the 16 Finviz atoms. Pairs of all 57. Six price deltas.
INITIAL_ATOMS = 57
INITIAL_PAIRS = INITIAL_ATOMS * (INITIAL_ATOMS - 1) // 2
INITIAL_DELTAS = 6
INITIAL_SIGNALS = INITIAL_ATOMS + INITIAL_PAIRS + INITIAL_DELTAS
INITIAL_OTHER = 2 * 2 * 4 * 15 * 4 * 4 * 5
INITIAL_N = INITIAL_SIGNALS * INITIAL_OTHER
TALLY_PRIOR = 37 + 8264 + 868
RUNNING_TALLY = INITIAL_N + TALLY_PRIOR

FINVIZ_PROVEN_DATES: tuple[str, ...] = (
    "2026-08-13",
    "2026-08-14",
    "2026-08-17",
    "2026-08-18",
    "2026-08-19",
    "2026-08-20",
    "2026-08-21",
    "2026-08-24",
    "2026-08-25",
    "2026-08-26",
    "2026-08-27",
    "2026-08-31",
    "2026-09-01",
    "2026-09-02",
    "2026-09-03",
    "2026-09-04",
    "2026-09-08",
    "2026-09-09",
    "2026-09-10",
    "2026-09-11",
)

# Walk-forward sessions whose Excel signal columns are server-proven in
# research/lever_search/excel_preopen_proof.csv. The proof is the Excel Bot
# run whose job log pushed the pre-open commit, with run updated_at before
# 09:30 ET. The other walk-forward sessions are dropped for Excel columns.
EXCEL_SESSIONS: tuple[str, ...] = (
    "2026-08-31",
    "2026-09-02",
    "2026-09-03",
    "2026-09-04",
    "2026-09-08",
    "2026-09-10",
    "2026-09-11",
)

EXCEL_DROPPED_SESSIONS: tuple[str, ...] = (
    "2026-08-27",
    "2026-08-28",
    "2026-09-01",
    "2026-09-09",
)

# Signal columns only. `strategy` is the card letter (L1, L2, L3, L4, L5, S1, S2).
EXCEL_SIGNAL_COLUMNS: tuple[str, ...] = ("ticker", "strategy", "signal_date")

EXCEL_OUTCOME_COLUMNS: frozenset[str] = frozenset({
    "ref_close",
    "first_open",
    "current_price",
    "ret_vs_close",
    "ret_vs_open",
    "days_held",
})

# Session date, not signal_date. The row's signal_date is the prior session.
EXCEL_DROPPED: frozenset[tuple[str, str]] = frozenset({
    ("CMII", "2026-09-02"),
    ("AUBN", "2026-09-04"),
    ("SVCC", "2026-09-11"),
})

WALK_FORWARD: tuple[str, ...] = (
    "2026-08-27",
    "2026-08-28",
    "2026-08-31",
    "2026-09-01",
    "2026-09-02",
    "2026-09-03",
    "2026-09-04",
    "2026-09-08",
    "2026-09-09",
    "2026-09-10",
    "2026-09-11",
)


class InputHashError(Exception):
    """A pinned input's sha256 does not match the manifest."""


class DroppedInput(Exception):
    """A dropped date, row, or later-add-on input was requested."""


def load_manifest(path: Path | None = None) -> dict:
    return json.loads((path or MANIFEST_PATH).read_text(encoding="utf-8"))


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def assert_manifest_hashes(root: Path | None = None, manifest: dict | None = None) -> None:
    """Re-hash every pinned file. One mismatch raises InputHashError."""
    root = root or ROOT
    manifest = manifest or load_manifest()
    for item in manifest.get("pinned_files") or []:
        rel = item["path"]
        expect = item["sha256"]
        path = root / rel
        if not path.is_file():
            raise InputHashError(f"missing pinned file {rel}")
        got = sha256_file(path)
        if got != expect:
            raise InputHashError(f"{rel} sha256 {got} != manifest {expect}")


def assert_initial_finviz_column(name: str) -> None:
    """Initial Finviz reads the 15 snapshot fields. Outcome and score columns raise."""
    from src.lever_search_panel import (
        FINVIZ_LEVER_COLUMNS,
        JOIN_KEYS,
        LeverColumnError,
        OutcomeColumnError,
        column_is_outcome,
    )

    if column_is_outcome(name):
        raise OutcomeColumnError(name)
    if name not in FINVIZ_LEVER_COLUMNS and name not in JOIN_KEYS:
        raise LeverColumnError(name)


def assert_finviz_date(trade_date: str, manifest: dict | None = None) -> None:
    manifest = manifest or load_manifest()
    allowed = manifest["finviz_proven_dates"]
    if trade_date not in allowed:
        raise DroppedInput(trade_date)
    if trade_date > "2026-09-11":
        raise DroppedInput(trade_date)


def assert_excel_column(name: str) -> None:
    """Signal columns pass. Price and return columns are outcomes."""
    from src.lever_search_panel import LeverColumnError, OutcomeColumnError

    if name in EXCEL_OUTCOME_COLUMNS:
        raise OutcomeColumnError(name)
    if name not in EXCEL_SIGNAL_COLUMNS:
        raise LeverColumnError(name)


def assert_excel_row(ticker: str, session_date: str) -> None:
    """Allow one suggestions row on a server-proven session. Dropped pairs raise."""
    if session_date in EXCEL_DROPPED_SESSIONS or session_date not in EXCEL_SESSIONS:
        raise DroppedInput(session_date)
    key = (str(ticker or "").upper(), session_date)
    if key in EXCEL_DROPPED:
        raise DroppedInput(f"{key[0]} {key[1]}")


def proof_is_before_open(entry: dict, session_date: str) -> bool:
    """True when a server-side time is before 09:30 ET.

    Git author and committer dates are ignored. An Actions run start counts
    only when `head_sha` equals the file commit. An Excel Bot safe-push
    counts when the run's `updated_at` is before the open.
    """
    proof = entry.get("proof") or {}
    cutoff = f"{session_date}T13:30:00Z"
    kind = proof.get("kind")
    if kind == "actions_run_start":
        started = proof.get("run_started_at") or ""
        head = proof.get("head_sha") or ""
        commit = entry.get("commit_sha") or proof.get("commit_sha") or ""
        if not started or head != commit:
            return False
        return started < cutoff
    if kind == "push_event":
        pushed = proof.get("push_created_at") or ""
        commit = entry.get("commit_sha") or ""
        head = proof.get("head_sha") or commit
        if not pushed or (commit and head != commit):
            return False
        return pushed < cutoff
    if kind == "excel_bot_safe_push":
        # Excel Bot job-log push. The clock is the run's updated_at.
        updated = proof.get("run_updated_at") or ""
        if not updated or not proof.get("run_id"):
            return False
        return updated < cutoff
    return False


def covered_fingerprint(text: str) -> str:
    marker = "<!-- BEGIN COVERED -->\n"
    return hashlib.sha256(text.split(marker, 1)[1].encode("utf-8")).hexdigest()
