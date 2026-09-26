"""Append-only guard for research/forward_shadow_v1.

Picks, fills, and pinned bars may only gain sessions after the last
recorded one. The recipe spec and preregistration stay byte-identical
once committed. v4 winners may only be appended, and their first
session is after every session already in the base ledger.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STUDY = Path("research/forward_shadow_v1")
FORWARD_START = "2026-09-28"
FROZEN = (
    "research/forward_shadow_v1/recipes.json",
    "research/forward_shadow_v1/PREREG.md",
)
HOOK = "research/forward_shadow_v1/hooks/v4_winners.json"
MANIFEST = "research/forward_shadow_v1/ledger/manifest.jsonl"


class AppendOnlyError(Exception):
    """An earlier session, spec, or hook entry was not left as written."""


def file_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _git_bytes(rev: str, path: str) -> bytes | None:
    proc = subprocess.run(
        ["git", "show", f"{rev}:{path}"],
        cwd=ROOT, check=False, capture_output=True,
    )
    if proc.returncode != 0:
        return None
    return proc.stdout


def _parse(line: str) -> dict:
    try:
        obj = json.loads(line)
    except json.JSONDecodeError as exc:
        raise AppendOnlyError("manifest line changed") from exc
    if not isinstance(obj, dict):
        raise AppendOnlyError("manifest line changed")
    return obj


def _split(text: str) -> list[str]:
    if text == "":
        return []
    lines = text.splitlines()
    if any(line == "" for line in lines):
        raise AppendOnlyError("manifest line changed")
    return lines


def assert_manifest(base_text: str, head_text: str, base_files: dict[str, bytes],
                    head_files: dict[str, bytes]) -> None:
    base_lines = _split(base_text)
    head_lines = _split(head_text)
    if head_lines[: len(base_lines)] != base_lines:
        if len(head_lines) < len(base_lines):
            raise AppendOnlyError("manifest line removed")
        if all(line in head_lines for line in base_lines):
            raise AppendOnlyError("manifest line reordered")
        raise AppendOnlyError("manifest line changed")
    for rel, blob in base_files.items():
        if rel not in head_files:
            raise AppendOnlyError("per-day file removed")
        if head_files[rel] != blob:
            raise AppendOnlyError(f"existing per-day file changed ({rel})")
    last = {"fills": "", "picks": ""}
    seen = {"fills": set(), "picks": set()}
    for line in base_lines:
        row = _parse(line)
        kind = str(row.get("kind") or "")
        session = str(row.get("session") or "")
        if session < FORWARD_START or kind not in last:
            raise AppendOnlyError("manifest line changed")
        if last[kind] and session <= last[kind]:
            raise AppendOnlyError("manifest line changed")
        last[kind] = session
        seen[kind].add(session)
    new_files = []
    for line in head_lines[len(base_lines):]:
        row = _parse(line)
        kind = str(row.get("kind") or "")
        session = str(row.get("session") or "")
        rel = str(row.get("file") or "")
        digest = str(row.get("sha256") or "")
        if kind not in last or session < FORWARD_START:
            raise AppendOnlyError("no day before 2026-09-28 is filled")
        if last[kind] and session <= last[kind]:
            raise AppendOnlyError("session not after the last recorded one")
        if rel != f"ledger/{session}.{kind}.json":
            raise AppendOnlyError("manifest line changed")
        blob = head_files.get(rel)
        if blob is None or file_sha256(blob) != digest:
            raise AppendOnlyError("per-day file sha256 does not match the manifest line")
        if kind == "fills":
            if session not in seen["picks"]:
                raise AppendOnlyError("fills require the morning picks file")
            bars_rel = str(row.get("bars") or "")
            bars_sha = str(row.get("bars_sha256") or "")
            if bars_rel != f"bars/{session}.json":
                raise AppendOnlyError("bar file missing")
            bars = head_files.get(bars_rel)
            if bars is None or file_sha256(bars) != bars_sha:
                raise AppendOnlyError("bar file sha256 does not match the manifest line")
            new_files.append(bars_rel)
        if row.get("fill_model") != "keep-held":
            raise AppendOnlyError("fill model is not keep-held")
        last[kind] = session
        seen[kind].add(session)
        new_files.append(rel)
    extra = set(head_files) - set(base_files) - set(new_files)
    if extra:
        raise AppendOnlyError("per-day file without a manifest line")


def _winners(raw: bytes | None) -> list[dict]:
    if not raw:
        return []
    try:
        doc = json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise AppendOnlyError("v4 hook changed") from exc
    winners = doc.get("winners") or []
    if not isinstance(winners, list):
        raise AppendOnlyError("v4 hook changed")
    return winners


def assert_hook(base_raw: bytes | None, head_raw: bytes | None,
                last_session: str | None) -> None:
    base = _winners(base_raw)
    head = _winners(head_raw)
    if [json.dumps(row, sort_keys=True) for row in head[: len(base)]] != [
        json.dumps(row, sort_keys=True) for row in base
    ]:
        raise AppendOnlyError("v4 winner changed")
    if len(head) < len(base):
        raise AppendOnlyError("v4 winner removed")
    for winner in head[len(base):]:
        first = str(winner.get("first_session") or "")
        if first < FORWARD_START:
            raise AppendOnlyError("v4 winner cannot start before 2026-09-28")
        if last_session and first <= last_session:
            raise AppendOnlyError("v4 winner would backfill a locked session")


def _day_files_from_git(rev: str) -> dict[str, bytes]:
    listing = subprocess.run(
        ["git", "ls-tree", "-r", "--name-only", rev, str(STUDY)],
        cwd=ROOT, check=False, capture_output=True, text=True,
    )
    files: dict[str, bytes] = {}
    if listing.returncode != 0:
        return files
    for rel in listing.stdout.splitlines():
        name = Path(rel).name
        parent = Path(rel).parent.name
        if parent == "ledger" and (name.endswith(".picks.json") or name.endswith(".fills.json")):
            blob = _git_bytes(rev, rel)
            if blob is None:
                raise AppendOnlyError("per-day file removed")
            files[rel[len("research/forward_shadow_v1/"):]] = blob
        if parent == "bars" and name.endswith(".json"):
            blob = _git_bytes(rev, rel)
            if blob is None:
                raise AppendOnlyError("per-day file removed")
            files[rel[len("research/forward_shadow_v1/"):]] = blob
    return files


def _head_day_files() -> dict[str, bytes]:
    root = ROOT / STUDY
    files: dict[str, bytes] = {}
    ledger = root / "ledger"
    bars = root / "bars"
    if ledger.is_dir():
        for path in sorted(ledger.iterdir()):
            if path.name.endswith(".picks.json") or path.name.endswith(".fills.json"):
                files[f"ledger/{path.name}"] = path.read_bytes()
    if bars.is_dir():
        for path in sorted(bars.iterdir()):
            if path.suffix == ".json":
                files[f"bars/{path.name}"] = path.read_bytes()
    return files


def _last_session(files: dict[str, bytes]) -> str | None:
    days = []
    for rel in files:
        name = Path(rel).name
        if name.endswith(".fills.json"):
            days.append(name[:10])
    return max(days) if days else None


def check_against(rev: str) -> None:
    base_manifest = _git_bytes(rev, MANIFEST)
    head_manifest_path = ROOT / MANIFEST
    head_manifest = head_manifest_path.read_bytes() if head_manifest_path.is_file() else b""
    base_files = _day_files_from_git(rev)
    head_files = _head_day_files()
    assert_manifest(
        (base_manifest or b"").decode("utf-8"),
        head_manifest.decode("utf-8"),
        base_files,
        head_files,
    )
    for rel in FROZEN:
        base = _git_bytes(rev, rel)
        head_path = ROOT / rel
        head = head_path.read_bytes() if head_path.is_file() else None
        if base is None:
            if head is None:
                raise AppendOnlyError("recipe spec missing")
            continue
        if head != base:
            raise AppendOnlyError(f"frozen file changed ({rel})")
    spec_path = ROOT / "research/forward_shadow_v1/recipes.json"
    if not spec_path.is_file():
        raise AppendOnlyError("recipe spec missing")
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    if spec.get("fill_model") != "keep-held":
        raise AppendOnlyError("fill model is not keep-held")
    body = {key: value for key, value in spec.items() if key != "spec_sha256"}
    digest = hashlib.sha256(
        json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    ).hexdigest()
    if digest != spec.get("spec_sha256"):
        raise AppendOnlyError("recipe spec sha256 does not match the file")
    assert_hook(
        _git_bytes(rev, HOOK),
        (ROOT / HOOK).read_bytes() if (ROOT / HOOK).is_file() else None,
        _last_session(base_files),
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Fail if an earlier forward_shadow session changed.")
    parser.add_argument("--check-against", required=True, metavar="REV")
    args = parser.parse_args(argv)
    check_against(args.check_against)


if __name__ == "__main__":
    main()
