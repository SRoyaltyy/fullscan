"""Append-only guard for breadth_mine_v1d.

Each session is written once under research/breadth_mine_v1d/returns/.
A later rebuild may only add sessions after the last recorded one.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RETURNS_DIR = Path("research/breadth_mine_v1d/returns")
MANIFEST_NAME = "manifest.jsonl"
DAY_FILE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}\.json$")
SESSION_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
BLOB_SHA_RE = re.compile(r"^[0-9a-f]{40}$")


class AppendOnlyError(Exception):
    """An earlier session, manifest line, or return was not left as written."""


def file_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def manifest_line(obj: dict) -> str:
    return json.dumps(obj, separators=(",", ":"), sort_keys=True)


def split_manifest(text: str) -> list[str]:
    if text == "":
        return []
    lines = text.splitlines()
    if any(line == "" for line in lines):
        raise AppendOnlyError("manifest line changed")
    return lines


def _parse_line(line: str) -> dict:
    try:
        obj = json.loads(line)
    except json.JSONDecodeError as exc:
        raise AppendOnlyError("manifest line changed") from exc
    if not isinstance(obj, dict):
        raise AppendOnlyError("manifest line changed")
    return obj


def _require_pin(obj: dict) -> None:
    blobs = obj.get("input_blobs")
    if not isinstance(blobs, list) or not blobs:
        raise AppendOnlyError("input snapshot blob sha missing")
    for item in blobs:
        if not isinstance(item, dict):
            raise AppendOnlyError("input snapshot blob sha missing")
        path = item.get("path")
        blob = item.get("blob_sha")
        if not isinstance(path, str) or path == "":
            raise AppendOnlyError("input snapshot blob sha missing")
        if not isinstance(blob, str) or BLOB_SHA_RE.fullmatch(blob) is None:
            raise AppendOnlyError("input snapshot blob sha missing")


def _prefix_failure(base_lines: list[str], head_lines: list[str]) -> None:
    if all(line in head_lines for line in base_lines):
        raise AppendOnlyError("manifest line reordered")
    if len(head_lines) < len(base_lines):
        raise AppendOnlyError("manifest line removed")
    raise AppendOnlyError("manifest line changed")


def assert_append_only(
    base_manifest: str,
    head_manifest: str,
    base_files: dict[str, bytes],
    head_files: dict[str, bytes],
) -> None:
    base_lines = split_manifest(base_manifest)
    head_lines = split_manifest(head_manifest)
    if head_lines[: len(base_lines)] != base_lines:
        _prefix_failure(base_lines, head_lines)

    base_rows = [_parse_line(line) for line in base_lines]
    base_named = [row.get("file") for row in base_rows]
    if set(base_files) != {name for name in base_named if isinstance(name, str)}:
        raise AppendOnlyError("base manifest does not match base per-day files")
    if len(base_named) != len(set(base_named)):
        raise AppendOnlyError("manifest line changed")

    for name, blob in base_files.items():
        if name not in head_files:
            raise AppendOnlyError("per-day file removed")
        if head_files[name] != blob:
            raise AppendOnlyError(f"existing per-day file changed ({name}); earlier return changed")

    last_session = ""
    for row in base_rows:
        session = str(row.get("session") or "")
        if SESSION_RE.fullmatch(session) is None or session <= last_session:
            raise AppendOnlyError("manifest line changed")
        last_session = session

    new_lines = head_lines[len(base_lines) :]
    new_names: list[str] = []
    for line in new_lines:
        row = _parse_line(line)
        session = row.get("session")
        name = row.get("file")
        digest = row.get("sha256")
        if not isinstance(session, str) or SESSION_RE.fullmatch(session) is None:
            raise AppendOnlyError("session not after the last recorded one")
        if last_session and session <= last_session:
            raise AppendOnlyError("session not after the last recorded one")
        if name != f"{session}.json" or DAY_FILE_RE.fullmatch(name) is None:
            raise AppendOnlyError("manifest line changed")
        if not isinstance(digest, str) or SHA256_RE.fullmatch(digest) is None:
            raise AppendOnlyError("per-day file sha256 does not match the manifest line")
        if name in base_files:
            raise AppendOnlyError(f"existing per-day file changed ({name}); earlier return changed")
        blob = head_files.get(name)
        if blob is None:
            raise AppendOnlyError("manifest line missing per-day file")
        if file_sha256(blob) != digest:
            raise AppendOnlyError("per-day file sha256 does not match the manifest line")
        _require_pin(row)
        last_session = session
        new_names.append(name)

    extra = set(head_files) - set(base_files) - set(new_names)
    if extra:
        raise AppendOnlyError("per-day file without a manifest line")
    if len(new_names) != len(set(new_names)):
        raise AppendOnlyError("manifest line changed")


def _git_bytes(rev: str, path: str) -> bytes | None:
    proc = subprocess.run(
        ["git", "show", f"{rev}:{path}"],
        cwd=ROOT,
        check=False,
        capture_output=True,
    )
    if proc.returncode != 0:
        return None
    return proc.stdout


def _git_day_files(rev: str) -> dict[str, bytes]:
    listing = subprocess.run(
        ["git", "ls-tree", "-r", "--name-only", rev, str(RETURNS_DIR)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    files: dict[str, bytes] = {}
    if listing.returncode != 0:
        return files
    for rel in listing.stdout.splitlines():
        name = Path(rel).name
        if DAY_FILE_RE.fullmatch(name) is None:
            continue
        blob = _git_bytes(rev, rel)
        if blob is None:
            raise AppendOnlyError("per-day file removed")
        files[name] = blob
    return files


def _read_head_days() -> dict[str, bytes]:
    directory = ROOT / RETURNS_DIR
    files: dict[str, bytes] = {}
    if not directory.is_dir():
        return files
    for path in sorted(directory.iterdir()):
        if path.is_file() and DAY_FILE_RE.fullmatch(path.name):
            files[path.name] = path.read_bytes()
    return files


def check_against(rev: str) -> None:
    base_manifest = _git_bytes(rev, f"{RETURNS_DIR.as_posix()}/{MANIFEST_NAME}")
    head_path = ROOT / RETURNS_DIR / MANIFEST_NAME
    head_manifest = head_path.read_bytes() if head_path.is_file() else b""
    assert_append_only(
        (base_manifest or b"").decode("utf-8"),
        head_manifest.decode("utf-8"),
        _git_day_files(rev),
        _read_head_days(),
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check-against", dest="rev", required=True)
    args = parser.parse_args()
    check_against(args.rev)


if __name__ == "__main__":
    main()
