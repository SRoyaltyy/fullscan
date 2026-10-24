#!/usr/bin/env python3
"""Fingerprint and lock replay pages day by day, add-only.

Replay pages (boring-winners, mover-paper, book-paper, flatten-lookback,
sleeve-combine) are rebuilt after the fact from stored bars. They are NOT
live-sealed. This script gives each rebuilt day a sha256 fingerprint in
data/replay_locks/<page>.jsonl, labelled "rebuilt after the fact (replay)".

Add-only rules:
  * a date already locked is never rewritten;
  * if a later rebuild produces a different fingerprint for a locked date,
    a {"kind": "drift"} line is appended (the original lock stays the
    reference) and a warning is printed, so history changes are visible;
  * new dates get a {"kind": "lock"} line.

Usage: python scripts/replay_lock.py PAGE SOURCE [SOURCE ...]
SOURCE is a .csv with a `date` column, or a .json holding a list of dicts
with a `date` key (the largest such list is used).
"""
import csv, hashlib, json, os, sys, datetime

LABEL = "rebuilt after the fact (replay)"


def rows_by_date(path):
    out = {}
    if path.endswith(".csv"):
        with open(path, newline="") as fh:
            for r in csv.DictReader(fh):
                d = (r.get("date") or "")[:10]
                if d:
                    out.setdefault(d, []).append(json.dumps(r, sort_keys=True))
        return out
    obj = json.load(open(path))
    best = []

    def walk(o):
        nonlocal best
        if isinstance(o, list) and o and all(isinstance(x, dict) and "date" in x for x in o[:50]):
            if len(o) > len(best):
                best = o
        if isinstance(o, dict):
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)

    walk(obj)
    for r in best:
        d = str(r.get("date") or "")[:10]
        if d:
            out.setdefault(d, []).append(json.dumps(r, sort_keys=True, default=str))
    return out


def main():
    page, sources = sys.argv[1], sys.argv[2:]
    lock_path = os.path.join("data", "replay_locks", f"{page}.jsonl")
    os.makedirs(os.path.dirname(lock_path), exist_ok=True)
    locked = {}
    if os.path.exists(lock_path):
        for line in open(lock_path):
            line = line.strip()
            if line:
                e = json.loads(line)
                if e.get("kind") == "lock":
                    locked.setdefault((e["source"], e["date"]), e["sha256"])
    now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    run = os.environ.get("GITHUB_RUN_ID", "local")
    new = drift = 0
    with open(lock_path, "a") as fh:
        for src in sources:
            if not os.path.exists(src):
                print(f"[replay-lock] {page}: missing {src}, skipped")
                continue
            for d, rows in sorted(rows_by_date(src).items()):
                sha = hashlib.sha256("\n".join(sorted(rows)).encode()).hexdigest()
                key = (src, d)
                base = {"page": page, "source": src, "date": d, "n_rows": len(rows),
                        "sha256": sha, "label": LABEL, "at": now, "run": run}
                if key not in locked:
                    fh.write(json.dumps({"kind": "lock", **base}, sort_keys=True) + "\n")
                    locked[key] = sha
                    new += 1
                elif locked[key] != sha:
                    fh.write(json.dumps({"kind": "drift", "locked_sha256": locked[key], **base},
                                        sort_keys=True) + "\n")
                    drift += 1
    print(f"[replay-lock] {page}: {new} new day locks, {drift} drift notes -> {lock_path}")
    if drift:
        print(f"::warning::{page}: {drift} already-locked replay days rebuilt differently (kept original lock)")


if __name__ == "__main__":
    main()
