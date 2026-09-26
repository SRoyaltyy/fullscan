"""Initial-run inputs for the lever search.

The scored run is not this module. It pins the server-proven dates, the
dropped Excel rows, and the manifest hashes. A mismatch raises.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import subprocess
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

# Group 1: no Excel card. Group 2: the signal includes an Excel card.
# Pairs that mix an Excel card with another atom stay in Group 2.
EXCEL_ATOMS = 7
GROUP1_ATOMS = INITIAL_ATOMS - EXCEL_ATOMS
GROUP1_PAIRS = GROUP1_ATOMS * (GROUP1_ATOMS - 1) // 2
GROUP1_SIGNALS = GROUP1_ATOMS + GROUP1_PAIRS + INITIAL_DELTAS
GROUP2_PAIRS = INITIAL_PAIRS - GROUP1_PAIRS
GROUP2_SIGNALS = EXCEL_ATOMS + GROUP2_PAIRS
GROUP1_N = GROUP1_SIGNALS * INITIAL_OTHER
GROUP2_N = GROUP2_SIGNALS * INITIAL_OTHER
# Original Factor Mine recipe space, PR #126, commit 8e8c36a. Tonight's run.
GROUP3_N = 110
EXCEL_ML_N = 1
# 110 + 37 + 8,264 + 868 + 1 Excel ML.
LUCK_N = GROUP3_N + TALLY_PRIOR + EXCEL_ML_N
# Groups 1 and 2 stay specified and are not run tonight.
DEFERRED_N = GROUP1_N + GROUP2_N
SEARCH_N = GROUP3_N
RUNNING_TALLY = LUCK_N
MIN_CHECK_DAYS = 10
FULLSCAN_PROOF_PATH = "research/audit/FULLSCAN_FILE_PROOF.csv"

# Daily excel-bot commits rewrite this path. The manifest sha256 is the
# blob at the pinned_files commit_sha (the copy in the #351 preregistration),
# not the worktree file. That hash is never rewritten.
SUGGESTIONS_CSV = "excel_bot/suggestions/suggestions.csv"

# Cells that must match the pinned row. Daily commits may refresh only
# SUGGESTION_TRACKING_COLUMNS on those rows.
SUGGESTION_SIGNAL_COLUMNS: tuple[str, ...] = (
    "run_date",
    "signal_date",
    "ticker",
    "side",
    "strategy",
    "exit_rule",
    "ref_close",
    "first_open",
    "signal_colors",
)
SUGGESTION_TRACKING_COLUMNS: tuple[str, ...] = (
    "current_price",
    "ret_vs_close",
    "ret_vs_open",
    "days_held",
)

# First session on which that column family is present in panel_meta.json.
COLUMN_FAMILY_START: tuple[tuple[str, str], ...] = (
    ("plain_finviz", "2026-08-07"),
    ("tr1d", "2026-08-10"),
    ("tr1w", "2026-08-10"),
    ("tr1m", "2026-08-10"),
    ("trf", "2026-08-10"),
    ("seg", "2026-08-12"),
    ("trc", "2026-08-13"),
    ("excel", "2026-08-31"),
)

FINVIZ_PROVEN_DATES: tuple[str, ...] = (
    "2026-08-07",
    "2026-08-10",
    "2026-08-11",
    "2026-08-12",
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

# Excel signal columns are server-proven only on these sessions, from
# research/lever_search/excel_preopen_proof.csv. The proof is the Excel Bot
# run whose job log pushed the pre-open commit, with run updated_at before
# 09:30 ET. Every other session in the 2026-08-07..2026-09-11 run window is
# sat out for a recipe that reads an Excel card.
EXCEL_SESSIONS: tuple[str, ...] = (
    "2026-08-31",
    "2026-09-02",
    "2026-09-03",
    "2026-09-04",
    "2026-09-08",
    "2026-09-10",
    "2026-09-11",
)

# Check days with no Excel proof. Training sessions before the first check
# day are also sat out for Excel, because they are absent from EXCEL_SESSIONS.
EXCEL_DROPPED_SESSIONS: tuple[str, ...] = (
    "2026-08-20",
    "2026-08-21",
    "2026-08-24",
    "2026-08-25",
    "2026-08-26",
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

# First check day stays 2026-08-20. A recipe that starts 2026-08-07 trains on
# nine sessions. A recipe whose columns start 2026-08-13 trains on the last five.
TRAINING_LOOKBACK: tuple[str, ...] = (
    "2026-08-07",
    "2026-08-10",
    "2026-08-11",
    "2026-08-12",
    "2026-08-13",
    "2026-08-14",
    "2026-08-17",
    "2026-08-18",
    "2026-08-19",
)

TRAINING_FROM_0813: tuple[str, ...] = TRAINING_LOOKBACK[4:]

WALK_FORWARD: tuple[str, ...] = (
    "2026-08-20",
    "2026-08-21",
    "2026-08-24",
    "2026-08-25",
    "2026-08-26",
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


def git_repo(start: Path) -> Path:
    """Directory whose history holds the pinned blob. A fixture tree falls back to this repo."""
    for candidate in (start, *start.parents):
        if (candidate / ".git").exists():
            return candidate
    if (ROOT / ".git").exists():
        return ROOT
    return start


def git_blob_bytes(repo: Path, commit: str, rel: str) -> bytes:
    """Bytes of `git show commit:rel`. The blob is the committed file."""
    proc = subprocess.run(
        ["git", "-C", str(repo), "show", f"{commit}:{rel}"],
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        detail = proc.stderr.decode("utf-8", errors="replace").strip()
        raise InputHashError(f"cannot read {rel} at {commit}: {detail}")
    return proc.stdout


def sha256_git_blob(repo: Path, commit: str, rel: str) -> str:
    """sha256 of `git show commit:rel`. The bytes are the committed blob."""
    return hashlib.sha256(git_blob_bytes(repo, commit, rel)).hexdigest()


def suggestions_pin(manifest: dict | None = None) -> dict:
    """The pinned_files entry for suggestions.csv. Its sha256 is left as written."""
    manifest = manifest or load_manifest()
    for item in manifest.get("pinned_files") or []:
        if item.get("path") == SUGGESTIONS_CSV:
            return item
    raise InputHashError(f"missing pinned file {SUGGESTIONS_CSV}")


def read_pinned_suggestions(root: Path | None = None, manifest: dict | None = None) -> bytes:
    """suggestions.csv as of the pin commit, checked against the manifest hash.

    The worktree file is not opened. The manifest hash is not changed.
    """
    root = root or ROOT
    item = suggestions_pin(manifest)
    commit = str(item.get("commit_sha") or "").strip()
    expect = item["sha256"]
    if not commit:
        raise InputHashError(f"{SUGGESTIONS_CSV} pin has no commit_sha")
    blob = git_blob_bytes(git_repo(root), commit, SUGGESTIONS_CSV)
    got = hashlib.sha256(blob).hexdigest()
    if got != expect:
        raise InputHashError(f"{SUGGESTIONS_CSV} sha256 {got} != manifest {expect}")
    return blob


def load_pinned_suggestions(root: Path | None = None, manifest: dict | None = None) -> list[dict]:
    """Suggestion rows from the preregistration blob. The live file is not opened."""
    text = read_pinned_suggestions(root, manifest).decode("utf-8")
    return list(csv.DictReader(io.StringIO(text)))


def _suggestion_rows(text: str) -> list[dict]:
    reader = csv.DictReader(io.StringIO(text))
    rows = list(reader)
    fields = reader.fieldnames or []
    missing = [name for name in SUGGESTION_SIGNAL_COLUMNS if name not in fields]
    if missing:
        raise InputHashError(f"suggestions csv missing columns {missing}")
    return rows


def assert_suggestions_signal_columns(pinned_csv: str, live_csv: str) -> None:
    """Pinned rows keep their signal cells in the live file.

    ``current_price``, ``ret_vs_close``, ``ret_vs_open``, and ``days_held``
    may change. Every other listed cell on a pinned row must match the live
    row at the same index. Rows appended after the pinned copy are ignored.
    """
    pinned = _suggestion_rows(pinned_csv)
    live = _suggestion_rows(live_csv)
    if len(live) < len(pinned):
        raise InputHashError(
            f"live suggestions has {len(live)} rows; pinned copy has {len(pinned)}"
        )
    for index, prow in enumerate(pinned):
        lrow = live[index]
        for name in SUGGESTION_SIGNAL_COLUMNS:
            left = prow.get(name) or ""
            right = lrow.get(name) or ""
            if left != right:
                raise InputHashError(
                    f"suggestions row {index} {name} {left!r} != live {right!r}"
                )


def assert_manifest_hashes(root: Path | None = None, manifest: dict | None = None) -> None:
    """Re-hash every pinned file. One mismatch raises InputHashError.

    ``excel_bot/suggestions/suggestions.csv`` is rewritten by the daily
    excel-bot commit. Its manifest hash is the blob at that entry's
    ``commit_sha``, the copy present at preregistration, not the live file.
    """
    root = root or ROOT
    manifest = manifest or load_manifest()
    for item in manifest.get("pinned_files") or []:
        rel = item["path"]
        expect = item["sha256"]
        if rel == SUGGESTIONS_CSV:
            read_pinned_suggestions(root, manifest)
            continue
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


def fullscan_status_is_used(status: str) -> bool:
    """PROVEN is used. Every other status sits that input out."""
    return status == "PROVEN"


def check_days_from(start: str) -> tuple[str, ...]:
    """Check-calendar sessions on or after a recipe's start day."""
    return tuple(day for day in WALK_FORWARD if day >= start)


def covered_fingerprint(text: str) -> str:
    marker = "<!-- BEGIN COVERED -->\n"
    return hashlib.sha256(text.split(marker, 1)[1].encode("utf-8")).hexdigest()
