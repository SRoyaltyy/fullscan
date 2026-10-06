"""Append-only fingerprints for excel_bot suggestion days.

Cyrus's IRONCLAD rule, applied here on 2026-10-06: from that signal_date
forward, each day's picks are locked. Days before 2026-10-06 are pre-lock.
They are not fingerprinted and this module does not rewrite them.

A day's fingerprint is the sha256 of its locked pick fields, in canonical
order: run_date, signal_date, ticker, side, strategy, exit_rule, ref_close,
signal_colors. run_date is included because daily_run sets it once per pick
and never rewrites it.

first_open is write-once and is not part of that hash. A blank may become a
value once; a non-blank value must not change. That fill appends a new
manifest entry. It does not edit the entry that locked the day.

Live marks may keep moving and are not hashed: current_price, ret_vs_close,
ret_vs_open, days_held.

Manifest entries are only appended. A mismatch fails closed: callers must
not write suggestions and must not commit.

A new lock entry is refused when that signal_date's NYSE session has not
closed yet (before 16:00 America/New_York, or a later session). An entry
already in the manifest is not edited or removed.

One pre-close lock may be voided by a later entry. The void names that
lock's signal_date and sha256, and the original lock entry stays as
written. The only void the checker accepts is the 2026-10-06 draft lock
from commit 856c5f64c. After that void, the day may be locked once more.
"""
from __future__ import annotations

import csv
import hashlib
import json
import os
import sys
from datetime import date, datetime

LOCK_FROM = "2026-10-06"
LOCKED_FIELDS = (
    "run_date",
    "signal_date",
    "ticker",
    "side",
    "strategy",
    "exit_rule",
    "ref_close",
    "signal_colors",
)
LIVE_FIELDS = (
    "current_price",
    "ret_vs_close",
    "ret_vs_open",
    "days_held",
)
WRITE_ONCE_FIELDS = ("first_open",)

_ENGINE_DIR = os.path.dirname(os.path.abspath(__file__))
EXCEL_DIR = os.path.dirname(_ENGINE_DIR)
MANIFEST_PATH = os.path.join(EXCEL_DIR, "freeze_manifest.json")
SUGG_PATH = os.path.join(EXCEL_DIR, "suggestions", "suggestions.csv")

# The only lock a void entry may name. Commit 856c5f64c
# (856c5f64c033af7fecd410c2220ee73e29d873da) was authored at
# 2026-10-06T17:37:43Z, which is 13:37 ET, during GitHub run 37499263674.
# That is before the 16:00 ET close. locked_at is that commit time.
# _taken_before_close checks it with the same New York clock the bot uses.
# No other sha256 is accepted.
VOIDABLE_LOCKS = (
    {
        "signal_date": "2026-10-06",
        "sha256": "9d910cb9de3933b888159d438e95925d105bd8d485b5aaeda60272ad65e44992",
        "locked_at": "2026-10-06T17:37:43Z",
        "added_at": "2026-10-06T21:25:00Z",
        "archive": "excel_bot/void/2026-10-06_pre_close_draft.csv",
        "note": "excel_bot/void/2026-10-06_pre_close_draft.md",
        "reason": (
            "pre-close draft lock, run 37499263674, commit 856c5f64c, "
            "13:37 ET 2026-10-06; no book traded on it"
        ),
    },
)

_DATE_FORMATS = ("%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%Y/%m/%d")


class SignalFreezeError(RuntimeError):
    """A locked signal day would change. Commit nothing."""


class FreezePlan:
    """Additions to append, plus the manifest prefix they were planned against."""

    def __init__(self, additions: list, prior_entries: list):
        self.additions = additions
        self.prior_entries = prior_entries


def norm_date(value) -> str:
    """ISO date. Locale Excel rewrites are accepted; unknown text is kept."""
    text = str(value or "").strip()
    if not text:
        return ""
    head = text[:10]
    try:
        return date.fromisoformat(head).isoformat()
    except ValueError:
        pass
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return text


def _cell(row: dict, key: str) -> str:
    value = row.get(key)
    if value is None:
        return ""
    return str(value).strip()


def _fail(message: str) -> None:
    raise SignalFreezeError(
        "[freeze] FAIL CLOSED: "
        + message
        + " Manifest entries were not removed or edited."
        + " This run commits nothing."
    )


def initial_manifest() -> dict:
    return {
        "schema": 1,
        "lock_from": LOCK_FROM,
        "pre_lock": f"signal_date < {LOCK_FROM}",
        "pre_lock_note": (
            "Rows with signal_date before 2026-10-06 are pre-lock. "
            "They are not fingerprinted, not backfilled, and this guard "
            "does not rewrite them. Live mark columns may still refresh."
        ),
        "locked_fields": list(LOCKED_FIELDS),
        "write_once_fields": list(WRITE_ONCE_FIELDS),
        "live_fields": list(LIVE_FIELDS),
        "entries": [],
    }


def _header_ok(manifest: dict) -> None:
    expected = initial_manifest()
    for key, value in expected.items():
        if key == "entries":
            continue
        if manifest.get(key) != value:
            _fail(
                f"freeze manifest field {key!r} does not match the "
                f"{LOCK_FROM} lock policy."
            )
    if not isinstance(manifest.get("entries"), list):
        _fail("freeze manifest entries must be a list.")


def _as_int(value, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        _fail(f"{label} must be an integer.")
    return value


def _check_entry(entry: dict, index: int) -> None:
    if not isinstance(entry, dict):
        _fail(f"manifest entry {index} is not an object.")
    need = ("signal_date", "sha256", "n_picks", "pick_ids", "first_opens", "kind")
    missing = [key for key in need if key not in entry]
    if missing:
        _fail(f"manifest entry {index} is missing {missing}.")
    signal_date = entry["signal_date"]
    if signal_date != norm_date(signal_date) or signal_date < LOCK_FROM:
        _fail(
            f"manifest entry {index} fingerprints {signal_date!r}. "
            f"Only signal_date >= {LOCK_FROM} may be locked. "
            "Pre-lock days must not be backfilled."
        )
    digest = str(entry["sha256"])
    if len(digest) != 64 or any(ch not in "0123456789abcdef" for ch in digest):
        _fail(f"manifest entry {index} has a bad sha256.")
    n_picks = _as_int(entry["n_picks"], f"manifest entry {index} n_picks")
    if n_picks < 1:
        _fail(f"manifest entry {index} has no picks.")
    pick_ids = entry["pick_ids"]
    first_opens = entry["first_opens"]
    if not isinstance(pick_ids, list) or not isinstance(first_opens, list):
        _fail(f"manifest entry {index} pick lists are not lists.")
    if len(pick_ids) != n_picks or len(first_opens) != n_picks:
        _fail(f"manifest entry {index} pick lists do not match n_picks.")
    for pick_i, pick_id in enumerate(pick_ids):
        if (
            not isinstance(pick_id, list)
            or len(pick_id) != 2
            or not all(isinstance(part, str) for part in pick_id)
        ):
            _fail(f"manifest entry {index} pick_ids[{pick_i}] is not [ticker, strategy].")
    for open_i, first_open in enumerate(first_opens):
        if not isinstance(first_open, str):
            _fail(f"manifest entry {index} first_opens[{open_i}] is not a string.")
    if entry["kind"] not in ("lock", "first_open", "void"):
        _fail(f"manifest entry {index} kind {entry['kind']!r} is unknown.")
    if entry["kind"] == "void":
        _check_void_entry(entry, index)


def load_manifest(path: str | None = None) -> dict:
    path = path or MANIFEST_PATH
    if not os.path.exists(path):
        _fail(
            f"freeze manifest is missing at {path}. "
            "Refusing to invent fingerprints."
        )
    with open(path, encoding="utf-8") as handle:
        manifest = json.load(handle)
    if not isinstance(manifest, dict):
        _fail("freeze manifest is not an object.")
    _header_ok(manifest)
    for index, entry in enumerate(manifest["entries"]):
        _check_entry(entry, index)
    _check_history(manifest["entries"])
    return manifest


def _allowlisted_void(signal_date: str, sha256: str) -> dict | None:
    for item in VOIDABLE_LOCKS:
        if item["signal_date"] == signal_date and item["sha256"] == sha256:
            return item
    return None


def _taken_before_close(signal_date: str, locked_at: str) -> bool:
    """True when `locked_at` is still before 16:00 ET on `signal_date`."""
    import gh_summary
    text = locked_at.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        instant = datetime.fromisoformat(text)
    except ValueError:
        return False
    if instant.tzinfo is None:
        return False
    stamp = gh_summary.resolve_session(instant)
    return stamp.session.isoformat() == signal_date and not stamp.write_final


def _check_void_entry(entry: dict, index: int) -> None:
    """A void may name only the one allowlisted pre-close lock."""
    for key in ("reason", "locked_at", "added_at", "archive", "note"):
        value = entry.get(key)
        if not isinstance(value, str) or not value.strip():
            _fail(f"manifest entry {index} void is missing {key}.")
    allowed = _allowlisted_void(entry["signal_date"], str(entry["sha256"]))
    if allowed is None:
        _fail(
            f"manifest entry {index} voids sha256 {entry['sha256']} "
            f"for signal_date {entry['signal_date']}. "
            "That lock is not the allowlisted pre-close draft lock. "
            "Refusing to void a different sha256."
        )
    for key in ("reason", "locked_at", "added_at", "archive", "note"):
        if entry[key] != allowed[key]:
            _fail(
                f"manifest entry {index} void field {key} does not match "
                "the allowlisted pre-close lock."
            )
    if not _taken_before_close(allowed["signal_date"], allowed["locked_at"]):
        _fail(
            f"signal_date {allowed['signal_date']} lock {allowed['sha256']} "
            f"at {allowed['locked_at']} was not before the 16:00 ET close. "
            "Refusing to void it."
        )


def void_entry_for(lock: dict) -> dict:
    """The void entry that names `lock`. Fails if `lock` is not allowlisted."""
    allowed = _allowlisted_void(lock["signal_date"], lock["sha256"])
    if allowed is None:
        _fail(
            f"signal_date {lock.get('signal_date')} sha256 {lock.get('sha256')} "
            "is not the allowlisted pre-close draft lock. "
            "Refusing to void a different sha256."
        )
    if not _taken_before_close(allowed["signal_date"], allowed["locked_at"]):
        _fail(
            f"signal_date {allowed['signal_date']} was not locked before the close."
        )
    return {
        "signal_date": lock["signal_date"],
        "sha256": lock["sha256"],
        "n_picks": lock["n_picks"],
        "pick_ids": [list(item) for item in lock["pick_ids"]],
        "first_opens": list(lock["first_opens"]),
        "kind": "void",
        "reason": allowed["reason"],
        "locked_at": allowed["locked_at"],
        "added_at": allowed["added_at"],
        "archive": allowed["archive"],
        "note": allowed["note"],
    }


def active_lock(chain: list) -> dict | None:
    """The lock that still binds the day.

    No void: the original lock. A void with no later lock: nothing. A void
    followed by one fresh lock: that fresh lock.
    """
    void_at = None
    for index, entry in enumerate(chain):
        if entry.get("kind") == "void":
            void_at = index
            break
    if void_at is None:
        if chain and chain[0].get("kind") == "lock":
            return chain[0]
        return None
    for entry in chain[void_at + 1:]:
        if entry.get("kind") == "lock":
            return entry
    return None


def _check_history(entries: list) -> None:
    """Lock, at most one void of that lock, then one fresh lock, then first_opens.

    A void is accepted only as the next entry after the lock it names.
    The original lock is not edited. After the void, exactly one fresh
    lock may be appended. first_open fills attach to the active lock.
    """
    seen: dict[str, list] = {}
    for index, entry in enumerate(entries):
        day = entry["signal_date"]
        chain = seen.setdefault(day, [])
        kind = entry["kind"]
        if not chain:
            if kind != "lock":
                _fail(
                    f"signal_date {day} manifest history does not start with a lock entry."
                )
            chain.append(entry)
            continue
        if kind == "void":
            if any(item["kind"] == "void" for item in chain):
                _fail(
                    f"signal_date {day} already has a void at manifest index {index}. "
                    "A second void is refused."
                )
            if len(chain) != 1 or chain[0]["kind"] != "lock":
                _fail(
                    f"signal_date {day} void at manifest index {index} "
                    "is not directly after the lock it names."
                )
            lock = chain[0]
            if (
                entry["sha256"] != lock["sha256"]
                or entry["pick_ids"] != lock["pick_ids"]
                or entry["n_picks"] != lock["n_picks"]
                or entry["first_opens"] != lock["first_opens"]
            ):
                _fail(
                    f"signal_date {day} void at manifest index {index} "
                    f"names sha256 {entry['sha256']} but the lock is {lock['sha256']}. "
                    "A void must name the lock it follows. "
                    "Refusing to void a different sha256."
                )
            chain.append(entry)
            continue
        if kind == "lock":
            if len(chain) == 2 and chain[1]["kind"] == "void" and active_lock(chain) is None:
                chain.append(entry)
                continue
            _fail(
                f"signal_date {day} repeated a lock entry at manifest index {index}. "
                "Lock entries are not rewritten."
            )
        if kind != "first_open":
            _fail(f"manifest entry {index} kind {kind!r} is unknown.")
        acting = active_lock(chain)
        if acting is None:
            _fail(
                f"signal_date {day} first_open at manifest index {index} "
                "has no active lock. A voided day must be locked again first."
            )
        if entry["sha256"] != acting["sha256"] or entry["pick_ids"] != acting["pick_ids"]:
            _fail(
                f"signal_date {day} fingerprint changed inside the manifest "
                f"at index {index}."
            )
        if entry["n_picks"] != acting["n_picks"]:
            _fail(f"signal_date {day} n_picks changed inside the manifest.")
        _opens_step(
            chain[-1]["first_opens"],
            entry["first_opens"],
            day,
            f"manifest index {index}",
            entry["pick_ids"],
        )
        chain.append(entry)


def canonical_picks(rows: list) -> dict[str, list]:
    """signal_date -> picks sorted by locked fields. Dates are ISO."""
    grouped: dict[str, list] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        signal_date = norm_date(_cell(row, "signal_date"))
        if not signal_date:
            continue
        pick = {key: _cell(row, key) for key in LOCKED_FIELDS}
        pick["run_date"] = norm_date(pick["run_date"])
        pick["signal_date"] = signal_date
        pick["first_open"] = _cell(row, "first_open")
        grouped.setdefault(signal_date, []).append(pick)
    for picks in grouped.values():
        picks.sort(key=lambda pick: tuple(pick[key] for key in LOCKED_FIELDS))
    return grouped


def fingerprint(picks: list) -> str:
    locked = [{key: pick[key] for key in LOCKED_FIELDS} for pick in picks]
    blob = json.dumps(locked, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _pick_ids(picks: list) -> list:
    return [[pick["ticker"], pick["strategy"]] for pick in picks]


def _opens_step(previous, current, signal_date: str, where: str, pick_ids: list) -> bool:
    """True when at least one blank first_open was filled. Illegal edits raise."""
    if len(previous) != len(current):
        _fail(
            f"signal_date {signal_date} {where}: first_open count "
            f"{len(previous)} -> {len(current)}."
        )
    filled = False
    for index, (left, right) in enumerate(zip(previous, current)):
        if left == right:
            continue
        label = pick_ids[index] if index < len(pick_ids) else [str(index), ""]
        if left == "" and right != "":
            filled = True
            continue
        _fail(
            f"signal_date {signal_date} {where}: {label[0]} {label[1]} "
            f"first_open {left!r} -> {right!r}. "
            "A blank first_open may be filled once. "
            "A non-blank first_open must not change."
        )
    return filled


def _id_change(stored_ids: list, current_ids: list) -> tuple[list, list]:
    from collections import Counter
    old = Counter(tuple(item) for item in stored_ids)
    new = Counter(tuple(item) for item in current_ids)
    added = [list(item) for item in (new - old).elements()]
    removed = [list(item) for item in (old - new).elements()]
    return added, removed


def session_has_closed(signal_date: str, now=None) -> bool:
    """True once `signal_date`'s NYSE session has reached 16:00 ET.

    Uses gh_summary.resolve_session. A signal_date after the stamped
    session has not closed. The stamped session itself has not closed
    while that stamp is still a draft.
    """
    import gh_summary
    stamp = gh_summary.resolve_session(now)
    try:
        day = date.fromisoformat(norm_date(signal_date))
    except ValueError:
        return False
    if day > stamp.session:
        return False
    if day == stamp.session and not stamp.write_final:
        return False
    return True


def _refuse_unclosed_lock(signal_date: str, now=None) -> None:
    if session_has_closed(signal_date, now):
        return
    _fail(
        f"signal_date {signal_date} has not closed yet "
        "(before 16:00 ET America/New_York). "
        "A pre-close run cannot lock that session."
    )


def plan(rows: list, manifest: dict, now=None) -> FreezePlan:
    """Entries to append so `rows` matches the lock. Raises on a broken day."""
    _header_ok(manifest)
    entries = manifest["entries"]
    for index, entry in enumerate(entries):
        _check_entry(entry, index)
    _check_history(entries)
    by_day: dict[str, list] = {}
    for entry in entries:
        by_day.setdefault(entry["signal_date"], []).append(entry)
    grouped = canonical_picks(rows)
    additions = []
    for signal_date in sorted(by_day):
        chain = by_day[signal_date]
        locked = active_lock(chain)
        if locked is None:
            # Voided, and the fresh lock is not in the manifest yet.
            # Picks for this day are a new lock, not a change to the voided one.
            continue
        picks = grouped.get(signal_date) or []
        current_hash = fingerprint(picks) if picks else ""
        current_ids = _pick_ids(picks)
        if current_hash != locked["sha256"] or current_ids != locked["pick_ids"]:
            added, removed = _id_change(locked["pick_ids"], current_ids)
            if not picks:
                _fail(
                    f"signal_date {signal_date} is locked "
                    f"({locked['n_picks']} picks) and has no rows left. "
                    "Removing a locked day is refused."
                )
            if added or removed:
                _fail(
                    f"signal_date {signal_date} picks added {added} "
                    f"removed {removed}. sha256 {locked['sha256']} != {current_hash}. "
                    "A locked day's picks cannot be added, removed, or changed."
                )
            _fail(
                f"signal_date {signal_date} locked fields changed for {current_ids}. "
                f"sha256 {locked['sha256']} != {current_hash}. "
                "A locked day's picks cannot be added, removed, or changed."
            )
        current_opens = [pick["first_open"] for pick in picks]
        if _opens_step(
            chain[-1]["first_opens"],
            current_opens,
            signal_date,
            "suggestions",
            locked["pick_ids"],
        ):
            additions.append({
                "signal_date": signal_date,
                "sha256": locked["sha256"],
                "n_picks": locked["n_picks"],
                "pick_ids": [list(item) for item in locked["pick_ids"]],
                "first_opens": current_opens,
                "kind": "first_open",
            })
    for signal_date in sorted(grouped):
        if signal_date < LOCK_FROM:
            continue
        chain = by_day.get(signal_date)
        if chain is not None and active_lock(chain) is not None:
            continue
        try:
            date.fromisoformat(signal_date)
        except ValueError:
            continue
        picks = grouped[signal_date]
        _refuse_unclosed_lock(signal_date, now)
        additions.append({
            "signal_date": signal_date,
            "sha256": fingerprint(picks),
            "n_picks": len(picks),
            "pick_ids": _pick_ids(picks),
            "first_opens": [pick["first_open"] for pick in picks],
            "kind": "lock",
        })
    return FreezePlan(additions, [dict(entry) for entry in entries])


def plan_rows(rows: list, manifest_path: str | None = None, now=None) -> FreezePlan:
    return plan(rows, load_manifest(manifest_path), now=now)


def save_manifest(path: str, manifest: dict, prior_entries: list) -> None:
    """Write `manifest` only when its entries extend the on-disk prefix."""
    _header_ok(manifest)
    if os.path.exists(path):
        with open(path, encoding="utf-8") as handle:
            on_disk = json.load(handle)
        if not isinstance(on_disk, dict) or not isinstance(on_disk.get("entries"), list):
            _fail("on-disk freeze manifest has no entries list.")
        disk_entries = on_disk["entries"]
        if disk_entries != list(prior_entries):
            _fail(
                "on-disk manifest entries do not match the sealed prefix. "
                "Refusing to rewrite them."
            )
        if len(manifest["entries"]) < len(disk_entries):
            _fail("refusing to remove freeze manifest entries.")
        if manifest["entries"][:len(disk_entries)] != disk_entries:
            _fail("refusing to edit existing freeze manifest entries.")
    elif prior_entries:
        _fail("freeze manifest is missing. Refusing to append onto nothing.")
    text = json.dumps(manifest, indent=2, ensure_ascii=False) + "\n"
    parsed = json.loads(text)
    if parsed["entries"][:len(prior_entries)] != list(prior_entries):
        _fail("refusing to edit existing freeze manifest entries.")
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    tmp = f"{path}.{os.getpid()}.tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        handle.write(text)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)


def append_entries(planned: FreezePlan, manifest_path: str | None = None, now=None) -> list:
    """Append planned entries. A no-op plan does not rewrite the file."""
    if not planned.additions:
        return []
    for entry in planned.additions:
        if entry.get("kind") == "lock":
            _refuse_unclosed_lock(entry["signal_date"], now)
    path = manifest_path or MANIFEST_PATH
    manifest = load_manifest(path)
    if manifest["entries"] != planned.prior_entries:
        _fail("freeze manifest changed before the append. Refusing to write it.")
    updated = dict(manifest)
    updated["entries"] = list(planned.prior_entries) + list(planned.additions)
    for index, entry in enumerate(updated["entries"]):
        _check_entry(entry, index)
    _check_history(updated["entries"])
    save_manifest(path, updated, planned.prior_entries)
    return list(planned.additions)


def seal(rows: list, manifest_path: str | None = None, now=None) -> list:
    """Verify `rows` and append any new lock or first_open entries."""
    path = manifest_path or MANIFEST_PATH
    planned = plan_rows(rows, path, now=now)
    return append_entries(planned, path, now=now)


def load_csv(path: str | None = None) -> list:
    path = path or SUGG_PATH
    with open(path, newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def verify_store(csv_path: str | None = None, manifest_path: str | None = None) -> None:
    """Fail if the CSV is not already described by the manifest.

    A new lock day, or a legal first_open fill that is not yet appended,
    is a failure here. The daily run seals those before this check, and
    the commit step uses this so an unsealed file is not pushed.
    """
    planned = plan_rows(load_csv(csv_path), manifest_path)
    if planned.additions:
        dates = sorted({entry["signal_date"] for entry in planned.additions})
        kinds = sorted({entry["kind"] for entry in planned.additions})
        _fail(
            f"suggestions need unsealed manifest entries for {dates} ({kinds}). "
            "Refusing to commit until the freeze manifest is appended."
        )


def main(argv: list | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if args not in ([], ["--verify"]):
        print(
            "usage: python excel_bot/engine/signal_freeze.py --verify",
            file=sys.stderr,
        )
        return 2
    try:
        verify_store()
    except SignalFreezeError as exc:
        print(exc, file=sys.stderr)
        return 1
    print(
        "[freeze] ok: locked signal dates match the manifest. "
        "Pre-lock rows (signal_date < 2026-10-06) are not fingerprinted.",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
