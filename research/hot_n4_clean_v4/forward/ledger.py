"""Append-only hash ledger for the holdup session log.

A record is written once. The sha256 on the record is the hash of the
canonical body with that field removed. The ledger line stores the same
hash and the byte length of the log line. Verification refuses a run when
any line disagrees with the ledger. Nothing here rewrites an earlier line.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
LOG_NAME = "holdup_log.jsonl"
LEDGER_NAME = "LEDGER.jsonl"
RECIPE = "union_hot_n4_holdup__w0"


def canonical_bytes(obj: dict) -> bytes:
    raw = json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True, allow_nan=False)
    return (raw + "\n").encode("utf-8")


def sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def log_path(folder: Path | None = None) -> Path:
    return (folder or HERE) / LOG_NAME


def ledger_path(folder: Path | None = None) -> Path:
    return (folder or HERE) / LEDGER_NAME


def _split(text: bytes, what: str) -> list[bytes]:
    if b"\r" in text:
        raise RuntimeError(f"{what} cr")
    if text and not text.endswith(b"\n"):
        raise RuntimeError(f"{what} newline")
    if not text:
        return []
    lines = text.splitlines(keepends=True)
    for line in lines:
        if line == b"\n" or not line.endswith(b"\n"):
            raise RuntimeError(f"blank {what} line")
    return lines


def seal(body: dict) -> tuple[dict, bytes]:
    """Return the record and the exact log line. ``body`` must not carry sha256."""
    if "sha256" in body:
        raise RuntimeError("sha256 is sealed, not supplied")
    digest = sha256_bytes(canonical_bytes(body))
    record = dict(body)
    record["sha256"] = digest
    line = canonical_bytes(record)
    return record, line


def load(folder: Path | None = None) -> list[dict]:
    """Parse and check every line. Raise if the log and the ledger disagree."""
    folder = folder or HERE
    log_lines = _split(log_path(folder).read_bytes() if log_path(folder).is_file() else b"", "log")
    led_lines = _split(ledger_path(folder).read_bytes() if ledger_path(folder).is_file() else b"", "ledger")
    if len(log_lines) != len(led_lines):
        raise RuntimeError("ledger length")
    records = []
    session_dates = []
    close_keys = []
    for seq, (raw, led_raw) in enumerate(zip(log_lines, led_lines), start=1):
        try:
            obj = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"log line is not json seq {seq}") from exc
        if canonical_bytes(obj) != raw:
            raise RuntimeError(f"log line is not canonical seq {seq}")
        digest = obj.get("sha256")
        body = {key: value for key, value in obj.items() if key != "sha256"}
        if sha256_bytes(canonical_bytes(body)) != digest:
            raise RuntimeError(f"record hash seq {seq}")
        try:
            led = json.loads(led_raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"ledger line is not json seq {seq}") from exc
        if canonical_bytes(led) != led_raw:
            raise RuntimeError(f"ledger line is not canonical seq {seq}")
        if led.get("sha256") != digest or int(led.get("bytes")) != len(raw):
            raise RuntimeError(f"ledger mismatch seq {seq}")
        if int(led.get("seq")) != seq or led.get("kind") != obj.get("kind"):
            raise RuntimeError(f"ledger seq {seq}")
        if led.get("date") != obj.get("date") or led.get("recipe") != RECIPE:
            raise RuntimeError(f"ledger identity seq {seq}")
        if obj.get("recipe") != RECIPE:
            raise RuntimeError(f"recipe seq {seq}")
        kind = obj.get("kind")
        if kind == "session":
            if obj["date"] in session_dates:
                raise RuntimeError("duplicate session")
            session_dates.append(obj["date"])
            for buy in obj.get("buys") or []:
                if "pnl" in buy or "pnl_primary" in buy:
                    raise RuntimeError("pnl on a buy")
            for sell in obj.get("sells") or []:
                if "pnl" in sell or "pnl_primary" in sell:
                    raise RuntimeError("pnl on a sell")
        elif kind == "close":
            key = (obj["date"], obj["ticker"], obj["entry_date"])
            if key in close_keys:
                raise RuntimeError("duplicate close")
            close_keys.append(key)
            if "pnl_primary" not in obj:
                raise RuntimeError("close without pnl")
        else:
            raise RuntimeError(f"kind {kind}")
        records.append(obj)
    if session_dates != sorted(session_dates):
        raise RuntimeError("session order")
    return records


def session_dates(records: list[dict]) -> list[str]:
    return [row["date"] for row in records if row["kind"] == "session"]


def has_session(records: list[dict], day: str) -> bool:
    return day in session_dates(records)


def append_records(bodies: list[dict], folder: Path | None = None) -> list[dict]:
    """Append new lines. Refuse if the current file does not match the ledger.

    Returns the sealed records. Does not rewrite a byte already on disk.
    """
    if not bodies:
        raise RuntimeError("empty append")
    folder = folder or HERE
    folder.mkdir(parents=True, exist_ok=True)
    existing = load(folder)
    start = len(existing)
    dates = session_dates(existing)
    close_keys = {
        (row["date"], row["ticker"], row["entry_date"])
        for row in existing if row["kind"] == "close"
    }
    for body in bodies:
        kind = body.get("kind")
        if kind == "session":
            if body["date"] in dates:
                raise RuntimeError("session exists")
            if dates and body["date"] <= dates[-1]:
                raise RuntimeError("session order")
            dates.append(body["date"])
        elif kind == "close":
            key = (body["date"], body["ticker"], body["entry_date"])
            if key in close_keys:
                raise RuntimeError("close exists")
            close_keys.add(key)
        else:
            raise RuntimeError(f"kind {kind}")
    log_file = log_path(folder)
    led_file = ledger_path(folder)
    before_log = log_file.read_bytes() if log_file.is_file() else b""
    before_led = led_file.read_bytes() if led_file.is_file() else b""
    log_out = []
    led_out = []
    sealed = []
    for offset, body in enumerate(bodies):
        if body.get("recipe") != RECIPE:
            raise RuntimeError("recipe")
        record, line = seal(body)
        seq = start + offset + 1
        led = {
            "bytes": len(line),
            "date": record["date"],
            "kind": record["kind"],
            "recipe": RECIPE,
            "seq": seq,
            "sha256": record["sha256"],
        }
        log_out.append(line)
        led_out.append(canonical_bytes(led))
        sealed.append(record)
    with log_file.open("ab") as handle:
        for line in log_out:
            handle.write(line)
    with led_file.open("ab") as handle:
        for line in led_out:
            handle.write(line)
    if not log_file.read_bytes().startswith(before_log):
        raise RuntimeError("log prefix changed")
    if not led_file.read_bytes().startswith(before_led):
        raise RuntimeError("ledger prefix changed")
    load(folder)
    return sealed
