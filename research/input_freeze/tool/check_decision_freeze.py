#!/usr/bin/env python3
"""Replay check: the sealed decision ticket must hash-match the frozen inputs.

For a session date, compare the per-input sha256 digests recorded in
``data/day_board/<date>_strategy_tickets.json -> decision_readiness.inputs``
against the rows of ``research/input_freeze/<date>/manifest.json`` (IRONCLAD
C.10: live tickets and the record use the same input set).

Verdicts per input:
  match    ticket digest == manifest sha256 (file pinned at freeze time).
  absent   ticket digest "absent" and the freeze row status is "missing".
  drift    hashes differ but the ticket was completed BEFORE the freeze
           cutoff — the freeze legitimately pins a later version. INFO only.
  FAIL     hashes differ and the ticket was completed AFTER the freeze —
           the sealed decision used inputs that are not the frozen ones.
  FAIL     ticket says "absent" but the freeze saw the file (or vice versa
           under a real digest with a "missing"/"after_freeze" row).
  WARN     fingerprinted input has no freeze row at all in a v1 folder
           (v1 predates full SPEC coverage); FAIL in v2+ folders.
  FAIL     a stored ("copied") freeze file on disk no longer matches its
           recorded stored_sha256 / original sha256 (folder tampered with).

Exit code is 1 when any FAIL exists, 0 otherwise. Stdlib only; reads only
files already checked out — no git, no network.

Run:
  python3 research/input_freeze/tool/check_decision_freeze.py
  python3 research/input_freeze/tool/check_decision_freeze.py --date 2026-10-07
"""
from __future__ import annotations

import argparse
import datetime as dt
import gzip
import hashlib
import json
import sys
from pathlib import Path

FREEZE_ROOT = Path("research/input_freeze")
TICKET_TMPL = "data/day_board/{}_strategy_tickets.json"
ABSENT = "absent"


# ------------------------------------------------------------------ time
def parse_ts(s: str | None) -> dt.datetime | None:
    """Parse '...Z' / offset-naive ISO timestamps into aware UTC."""
    if not s:
        return None
    try:
        t = dt.datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except ValueError:
        return None
    if t.tzinfo is None:
        t = t.replace(tzinfo=dt.timezone.utc)
    return t.astimezone(dt.timezone.utc)


# ----------------------------------------------------------------- loads
def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def newest_checkable_date(root: Path) -> str | None:
    """Newest date with a 'frozen' folder AND a sealed strategy ticket."""
    best: str | None = None
    if not (root / FREEZE_ROOT).is_dir():
        return None
    for folder in sorted((root / FREEZE_ROOT).iterdir()):
        if not folder.is_dir():
            continue
        manifest = folder / "manifest.json"
        if not manifest.is_file():
            continue
        try:
            m = load_json(manifest)
        except (OSError, json.JSONDecodeError):
            continue
        if m.get("status") != "frozen":
            continue
        if not (root / TICKET_TMPL.format(folder.name)).is_file():
            continue
        best = folder.name
    return best


# ----------------------------------------------------------------- check
def check_day(root: Path, day: str) -> tuple[list[str], list[str], list[str]]:
    """Return (infos, warnings, failures) for one session date."""
    infos: list[str] = []
    warns: list[str] = []
    fails: list[str] = []

    manifest_path = root / FREEZE_ROOT / day / "manifest.json"
    ticket_path = root / TICKET_TMPL.format(day)
    if not manifest_path.is_file():
        return infos, warns, [f"no freeze manifest for {day}"]
    if not ticket_path.is_file():
        return infos, warns, [f"no sealed strategy ticket for {day}"]

    manifest = load_json(manifest_path)
    ticket = load_json(ticket_path)
    readiness = ticket.get("decision_readiness") or {}
    inputs = readiness.get("inputs") or {}
    if not inputs:
        return infos, warns, [f"{ticket_path}: no decision_readiness.inputs to verify"]

    schema = str(manifest.get("schema") or "input_freeze/v1")
    freeze_at = parse_ts(manifest.get("freeze_time_utc"))
    done_at = parse_ts(readiness.get("completed_at"))

    rows = [f for f in manifest.get("files", []) if isinstance(f, dict)]
    by_path: dict[str, list[dict]] = {}
    for row in rows:
        sp = row.get("source_path")
        if sp:
            by_path.setdefault(str(sp), []).append(row)

    # Stored-copy integrity: a copied row whose bytes on disk no longer
    # hash to the recorded digests means the append-only folder was altered.
    for row in rows:
        if row.get("status") != "copied" or not row.get("stored_path"):
            continue
        p = root / FREEZE_ROOT / day / row["stored_path"]
        label = f"{row.get('source_path', row.get('pattern'))}"
        if not p.is_file():
            fails.append(f"stored copy missing on disk: {label} ({row['stored_path']})")
            continue
        data = p.read_bytes()
        stored_sha = hashlib.sha256(data).hexdigest()
        if stored_sha != row.get("stored_sha256"):
            fails.append(f"stored copy altered: {label} (disk {stored_sha[:12]} "
                         f"!= manifest {str(row.get('stored_sha256'))[:12]})")
            continue
        original = gzip.decompress(data) if row.get("gzip") else data
        if hashlib.sha256(original).hexdigest() != row.get("sha256"):
            fails.append(f"stored copy corrupt: {label} (decompressed bytes != source sha256)")

    for rel, digest in sorted(inputs.items()):
        rel = str(rel)
        digest = str(digest)
        matched = by_path.get(rel, [])

        if not matched:
            msg = f"uncovered input (no freeze row): {rel}"
            if schema == "input_freeze/v1":
                warns.append(msg + "  [v1 folder predates full SPEC coverage]")
            else:
                fails.append(msg + f"  [{schema} folder must cover every fingerprinted input]")
            continue

        row = matched[0]
        status = row.get("status")
        row_sha = row.get("sha256")

        if digest == ABSENT:
            if status == "missing":
                infos.append(f"absent-ok    {rel}")
            else:
                fails.append(f"ticket says absent but freeze row status={status}: {rel}")
            continue

        if status in ("missing", "after_freeze"):
            ordering = "drift (ticket predates freeze)" if (
                done_at and freeze_at and done_at <= freeze_at) else "FAIL"
            if ordering == "FAIL":
                fails.append(f"freeze never pinned it (row {status}) yet the ticket "
                             f"has a digest, ticket completed after freeze: {rel}")
            else:
                infos.append(f"drift-ok     {rel} (freeze row {status}; ticket "
                             f"completed before freeze cutoff)")
            continue

        if row_sha == digest:
            infos.append(f"match        {rel}")
            continue

        if done_at and freeze_at and done_at <= freeze_at:
            infos.append(f"drift-ok     {rel} (ticket {done_at:%H:%M:%SZ} <= freeze "
                         f"{freeze_at:%H:%M:%SZ}; freeze pins a later commit)")
        else:
            when = (f"ticket completed {done_at} vs freeze {freeze_at}"
                    if done_at and freeze_at else "no timestamps to order the mismatch")
            fails.append(f"MISMATCH     {rel}: ticket {digest[:16]} != frozen "
                         f"{str(row_sha)[:16]} ({when})")

    return infos, warns, fails


# -------------------------------------------------------------------- cli
def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--date", help="session date YYYY-MM-DD (default: newest "
                                   "date with a frozen folder and a ticket)")
    ap.add_argument("--root", default=".", help="repo root (default: cwd)")
    args = ap.parse_args(argv)

    root = Path(args.root)
    day = args.date or newest_checkable_date(root)
    if not day:
        print("[decision-freeze] nothing to check: no date has both a frozen "
              "folder and a sealed strategy ticket")
        return 0

    print(f"[decision-freeze] checking {day} "
          f"(ticket data/day_board/{day}_strategy_tickets.json vs "
          f"research/input_freeze/{day}/manifest.json)")
    infos, warns, fails = check_day(root, day)
    for line in infos:
        print("  " + line)
    for line in warns:
        print("  WARN  " + line)
    for line in fails:
        print("  FAIL  " + line)

    print(f"[decision-freeze] {day}: {len(infos)} ok, {len(warns)} warn, {len(fails)} fail")
    if fails:
        print("[decision-freeze] REPLAY CHECK FAILED — the sealed ticket is not "
              "provably built from the frozen inputs")
        return 1
    print("[decision-freeze] OK — sealed ticket hash-matches the frozen input set")
    return 0


if __name__ == "__main__":
    sys.exit(main())
