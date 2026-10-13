"""Pre-push land guard for Factor Mine frozen days and the past-day lock.

Incident 2026-10-12: two "Factor strategy mine" land runs overlapped. The
second one checked out b070934c (the stale workflow_run sha, before the
first run's seal 57f6a737), rebuilt 2026-10-12 from scratch and
``safe_git_push.sh`` took "ours" for every file. That swapped the four
sealed 10-12 hashes in ``data/factor_mine/freeze_manifest.json`` (only
``code_sha`` differed) and dropped two 10-12 lines from
``data/past_day_lock/manifest.jsonl`` that another job had appended.

This wrapper runs right before the push, against the freshly fetched
``origin/main``:

* any (slot, date) already in main's freeze manifest is restored
  byte-for-byte (file and manifest entry) and logged
  ``already frozen — no rewrite <date>``. If our rebuilt file differs in
  real content (anything beyond ``code_sha`` / ``written_at``), it fails.
* ``past_day_lock/manifest.jsonl`` becomes main's lines, in order, plus our
  new lines. A line whose key (record, kind, date, name) is on main with
  different content fails. No existing line can vanish.

It never edits the byte-pinned engine files. It only writes the work tree
of the land job; it never rewrites data on main.
"""
from __future__ import annotations

import argparse
import gzip
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FREEZE_MANIFEST = "data/factor_mine/freeze_manifest.json"
PAST_LOCK_MANIFEST = "data/past_day_lock/manifest.jsonl"
FILE_SLOTS = {
    "snapshots": "data/factor_mine/snapshots/{d}.json",
    "candidates": "data/factor_mine/candidates/{d}.json",
    "lineups": "data/factor_mine/lineups/{d}.json",
    "prices": "data/factor_mine/prices/{d}.json",
    "ledgers": "data/factor_mine/ledgers/{d}.json.gz",
}
VOLATILE_KEYS = ("code_sha", "written_at")


class LandGuardError(RuntimeError):
    """A land would change or drop sealed history."""


# --------------------------------------------------------------- helpers
def _decode(slot: str, raw: bytes):
    if slot == "ledgers":
        raw = gzip.decompress(raw)
    return json.loads(raw.decode("utf-8"))


def _strip(obj):
    if isinstance(obj, dict):
        return {k: v for k, v in obj.items() if k not in VOLATILE_KEYS}
    return obj


def same_content(slot: str, frozen: bytes, ours: bytes) -> bool:
    """True when the bytes differ only in code_sha / written_at."""
    if frozen == ours:
        return True
    try:
        return _strip(_decode(slot, frozen)) == _strip(_decode(slot, ours))
    except (OSError, ValueError, EOFError):
        return False


def dump_manifest(doc: dict) -> str:
    # Same layout as factor_mine_freeze.save_manifest.
    return json.dumps(doc, indent=2, sort_keys=True)


# ---------------------------------------------------- freeze manifest
def merge_freeze(upstream: dict, ours: dict, read_up, read_ours,
                 log=print) -> tuple[dict, dict[str, bytes]]:
    """Return (merged manifest, {path: bytes to restore}).

    ``read_up(path)`` / ``read_ours(path)`` return bytes or None.
    """
    merged: dict = {}
    restore: dict[str, bytes] = {}
    for key in sorted(set(upstream) | set(ours)):
        up, me = upstream.get(key), ours.get(key)
        if key not in upstream:
            merged[key] = me
        elif key not in ours:
            merged[key] = up
        elif isinstance(up, dict) and isinstance(me, dict):
            out = dict(me)
            out.update(up)              # main's entries always win
            merged[key] = out
        elif isinstance(up, list) and isinstance(me, list):
            merged[key] = list(up) + [x for x in me if x not in up]
        else:
            merged[key] = up
    for slot, pattern in FILE_SLOTS.items():
        up_slot = upstream.get(slot) or {}
        me_slot = ours.get(slot) or {}
        for date in sorted(up_slot):
            path = pattern.format(d=date)
            frozen = read_up(path)
            if frozen is None:
                continue
            mine = read_ours(path)
            entry_same = me_slot.get(date) == up_slot[date]
            if mine == frozen and entry_same:
                continue
            if mine is not None and not same_content(slot, frozen, mine):
                raise LandGuardError(
                    f"frozen day content mismatch {slot} {date}: {path} "
                    "differs from main beyond code_sha/written_at — refusing land"
                )
            if mine != frozen:
                restore[path] = frozen
            log(f"already frozen — no rewrite {date} ({slot})")
    snaps = merged.get("snapshots") or {}
    if isinstance(snaps, dict) and "first_frozen" in merged:
        merged["first_frozen"] = min(snaps) if snaps else None
    return merged, restore


# ------------------------------------------------- past-day lock jsonl
def _key(line: str) -> tuple:
    obj = json.loads(line)
    return (obj.get("record"), obj.get("kind"), obj.get("date"), obj.get("name"))


def merge_past_lock(upstream_text: str, ours_text: str) -> str:
    """Main's lines in order, then our new lines. Never drops or edits."""
    up_lines = [l for l in upstream_text.splitlines() if l.strip()]
    me_lines = [l for l in ours_text.splitlines() if l.strip()]
    have = set(up_lines)
    up_keys = {_key(l): l for l in up_lines}
    out = list(up_lines)
    for line in me_lines:
        if line in have:
            continue
        k = _key(line)
        if k in up_keys:
            raise LandGuardError(
                f"past-day lock line would change: {k} main={up_keys[k]} ours={line}"
            )
        out.append(line)
        have.add(line)
        up_keys[k] = line
    if out[: len(up_lines)] != up_lines:  # pragma: no cover - by construction
        raise LandGuardError("past-day lock: existing line would vanish")
    return ("\n".join(out) + "\n") if out else ""


def assert_lock_kept(upstream_text: str, result_text: str) -> None:
    up = [l for l in upstream_text.splitlines() if l.strip()]
    res = [l for l in result_text.splitlines() if l.strip()]
    if res[: len(up)] != up:
        raise LandGuardError("past-day lock: existing line would change or vanish")


# ----------------------------------------------------------------- CLI
def _git_show(ref: str, rel: str) -> bytes | None:
    proc = subprocess.run(["git", "show", f"{ref}:{rel}"], cwd=ROOT,
                          capture_output=True, check=False)
    return proc.stdout if proc.returncode == 0 else None


def _read(rel: str) -> bytes | None:
    p = ROOT / rel
    return p.read_bytes() if p.is_file() else None


def reconcile(ref: str = "origin/main", fetch: bool = True) -> int:
    if fetch:
        subprocess.run(["git", "fetch", "origin", "main"], cwd=ROOT, check=True)
    up_raw = _git_show(ref, FREEZE_MANIFEST)
    me_raw = _read(FREEZE_MANIFEST)
    if up_raw is not None and me_raw is not None:
        upstream = json.loads(up_raw)
        ours = json.loads(me_raw)
        merged, restore = merge_freeze(
            upstream, ours, lambda p: _git_show(ref, p), _read)
        for rel, raw in restore.items():
            (ROOT / rel).write_bytes(raw)
        text = dump_manifest(merged)
        if text.encode("utf-8") == up_raw:
            (ROOT / FREEZE_MANIFEST).write_bytes(up_raw)
        else:
            (ROOT / FREEZE_MANIFEST).write_text(text, encoding="utf-8")
        print(f"[land-guard] freeze manifest merged; restored {len(restore)} frozen file(s)")
    up_lock = _git_show(ref, PAST_LOCK_MANIFEST)
    me_lock = _read(PAST_LOCK_MANIFEST)
    if up_lock is not None:
        up_text = up_lock.decode("utf-8")
        me_text = me_lock.decode("utf-8") if me_lock is not None else ""
        result = merge_past_lock(up_text, me_text)
        assert_lock_kept(up_text, result)
        out = up_lock if result == up_text else result.encode("utf-8")
        (ROOT / PAST_LOCK_MANIFEST).write_bytes(out)
        n_new = len(result.splitlines()) - len(up_text.splitlines())
        print(f"[land-guard] past-day lock append-only: +{n_new} line(s)")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("cmd", choices=["reconcile"])
    ap.add_argument("--ref", default="origin/main")
    ap.add_argument("--no-fetch", action="store_true")
    args = ap.parse_args(argv)
    try:
        return reconcile(args.ref, fetch=not args.no_fetch)
    except LandGuardError as exc:
        print(f"::error::{exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
