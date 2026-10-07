"""Append-only past-day lock for records that did not have one yet.

Same rule as ``research/hot_n4_clean_v4/forward/append_check.py`` and
``src/breadth_rank_v1_append.py``: a day, once sealed, is a prefix the
next run must not edit. A mismatch raises ``PastDayLockError``
(a ``SystemExit``) so the run stops with a non-zero status.

This does not fingerprint days already on disk. The watermark is the
latest day present before the first new day is appended after this
code runs. That next day is the first locked day. Older rows stay
unhashed. A missing day is not filled in.

Header lines on a Factor Mine scoreboard (totals, fills, realized,
audit) are not part of the day hash. Day rows and buy/sell lines are.

``data/paper/`` (trades, roundtrips, skipped, equity_curve) and
``data/sleeve_merge/`` (trades, skipped, equity_curve) are locked
going forward. The watermark is the latest day already in the file
before the first new day is appended after this code runs. That next
day is the first day that can be sealed, and only once a later
session is written. Older rows are not fingerprinted and are not
rewritten. The newest day stays open so a same-day rerun may still
replace it. A sealed day that would change raises
``PastDayLockError``. Sleeve-merge trades are closed round-trips
keyed by ``exit_date``. Paper round-trips key closed rows by
``sell_date``; open lots stay writable.

The flatten card for the newest day stays open: same-day reruns rewrite
the equity lines. That day is sealed when a later session is written.
After 09:30 ET, sleeve_merge_live owns post-open card updates so
Stock Book ALL does not stomp a card already on disk. Strategy
tickets stay writable until the 09:30 ET / paper-send lock; the
08:07 and 09:07 passes may both change the file.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = ROOT / "data" / "past_day_lock" / "manifest.jsonl"
FACTOR_MINE_DIR = ROOT / "03_scoreboard" / "factor_mine"
FLATTEN_DIR = ROOT / "01_daily"
TICKET_DIR = ROOT / "data" / "day_board"
PAPER_DIR = ROOT / "data" / "paper"
SLEEVE_DIR = ROOT / "data" / "sleeve_merge"
WEBULL_DIR = ROOT / "data" / "webull_sim"

# record -> (directory, filename, date column). Open round-trip rows
# have an empty sell_date and are not part of a sealed day.
_CSV_RECORDS: dict[str, tuple[Path, str, str]] = {
    "paper_trades": (PAPER_DIR, "trades.csv", "date"),
    "paper_equity": (PAPER_DIR, "equity_curve.csv", "date"),
    "paper_skipped": (PAPER_DIR, "skipped.csv", "date"),
    "paper_roundtrips": (PAPER_DIR, "roundtrips.csv", "sell_date"),
    "sleeve_trades": (SLEEVE_DIR, "trades.csv", "exit_date"),
    "sleeve_equity": (SLEEVE_DIR, "equity_curve.csv", "date"),
    "sleeve_skipped": (SLEEVE_DIR, "skipped.csv", "date"),
}

DAY_LINE = re.compile(r"^\| (\d{4}-\d{2}-\d{2})\b")
CARD_FILE = re.compile(r"^(\d{4}-\d{2}-\d{2})_flatten_card\.md$")
TICKET_FILE = re.compile(r"^(\d{4}-\d{2}-\d{2})_strategy_tickets\.json$")
GENERATED_LINE = re.compile(r"^_Generated .*_$")
ISO_DAY = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class PastDayLockError(SystemExit):
    """A sealed past day was edited, removed, or rewritten."""

    def __init__(self, message: str):
        super().__init__(message)


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def manifest_line(obj: dict) -> str:
    return json.dumps(obj, separators=(",", ":"), sort_keys=True)


def in_repo(path: Path) -> bool:
    try:
        path.resolve().relative_to(ROOT.resolve())
    except ValueError:
        return False
    return True


def load_manifest(path: Path | None = None) -> list[dict]:
    dest = Path(path or MANIFEST_PATH)
    if not dest.is_file():
        return []
    rows = []
    for raw in dest.read_text(encoding="utf-8").splitlines():
        if raw == "":
            raise PastDayLockError("past-day lock: blank manifest line")
        try:
            obj = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise PastDayLockError("past-day lock: manifest line is not json") from exc
        if not isinstance(obj, dict):
            raise PastDayLockError("past-day lock: manifest line is not an object")
        rows.append(obj)
    return rows


def _append_lines(rows: list[dict], path: Path | None = None) -> None:
    if not rows:
        return
    dest = Path(path or MANIFEST_PATH)
    dest.parent.mkdir(parents=True, exist_ok=True)
    blob = "".join(manifest_line(row) + "\n" for row in rows)
    with dest.open("a", encoding="utf-8") as handle:
        handle.write(blob)


def _seed_row(rows: list[dict], record: str) -> dict | None:
    for row in rows:
        if row.get("kind") == "seed" and row.get("record") == record:
            return row
    return None


def _day_rows(rows: list[dict], record: str, name: str = "") -> list[dict]:
    out = []
    for row in rows:
        if row.get("kind") != "day" or row.get("record") != record:
            continue
        if str(row.get("name") or "") != name:
            continue
        out.append(row)
    return out


def _watermark(rows: list[dict], record: str, on_disk: list[str], *,
               writing: str | None, existed: bool) -> str:
    """Latest day that stays unfingerprinted.

    A stored seed wins. Otherwise a file that already exists is part of
    the pre-lock record. A brand-new day is compared with the latest
    day that was already there.
    """
    seed = _seed_row(rows, record)
    if seed is not None:
        return str(seed.get("watermark") or "")
    dates = sorted(d for d in on_disk if ISO_DAY.fullmatch(d or ""))
    if existed:
        return dates[-1] if dates else ""
    prior = [d for d in dates if d != writing]
    return prior[-1] if prior else ""


def _ensure_seed(rows: list[dict], record: str, watermark: str,
                 path: Path | None) -> list[dict]:
    if _seed_row(rows, record) is not None:
        return rows
    row = {"kind": "seed", "record": record, "watermark": watermark}
    _append_lines([row], path)
    rows.append(row)
    return rows


def locked_lines_by_date(text: str) -> dict[str, list[str]]:
    """Day rows and buy/sell lines. Summary lines are left out."""
    out: dict[str, list[str]] = {}
    for line in text.splitlines():
        match = DAY_LINE.match(line)
        if not match:
            continue
        out.setdefault(match.group(1), []).append(line)
    return out


def card_body(text: str) -> str:
    lines = [line for line in text.splitlines() if not GENERATED_LINE.match(line)]
    return "\n".join(lines) + ("\n" if lines else "")


def _csv_days(text: str) -> dict[str, list[str]]:
    lines = text.splitlines()
    out: dict[str, list[str]] = {}
    for line in lines[1:]:
        if not line:
            continue
        date = line.split(",", 1)[0][:10]
        if ISO_DAY.fullmatch(date):
            out.setdefault(date, []).append(line)
    return out


def _fail_if_sealed_differs(record: str, name: str, date: str,
                            digest: str, rows: list[dict]) -> None:
    for row in _day_rows(rows, record, name):
        if row.get("date") != date:
            continue
        prev = str(row.get("sha256") or "")
        if prev != digest:
            label = f"{record} {name} {date}".replace("  ", " ")
            raise PastDayLockError(
                f"past-day lock: {label} changed "
                f"(sha256 {prev} -> {digest}). Not rescoring a sealed day."
            )
        return


def _seal(rows: list[dict], record: str, date: str, digest: str,
          path: Path | None, *, name: str = "", watermark: str) -> list[dict]:
    if not ISO_DAY.fullmatch(date or ""):
        raise PastDayLockError(f"past-day lock: bad date {date!r}")
    if watermark and date <= watermark:
        return rows
    for row in _day_rows(rows, record, name):
        if row.get("date") == date:
            if str(row.get("sha256") or "") != digest:
                label = f"{record} {name} {date}".replace("  ", " ")
                raise PastDayLockError(
                    f"past-day lock: {label} changed. Not rescoring a sealed day."
                )
            return rows
    sealed_dates = [str(row.get("date") or "") for row in _day_rows(rows, record, name)]
    if sealed_dates and date < max(sealed_dates):
        raise PastDayLockError(
            f"past-day lock: {record} {date} is before the last sealed day "
            f"{max(sealed_dates)}. A missed day stays missing."
        )
    rows = _ensure_seed(rows, record, watermark, path)
    row = {
        "date": date,
        "kind": "day",
        "name": name,
        "record": record,
        "sha256": digest,
    }
    _append_lines([row], path)
    rows.append(row)
    return rows


def _dates_in_dir(directory: Path, pattern: re.Pattern[str]) -> list[str]:
    if not directory.is_dir():
        return []
    found = []
    for path in directory.iterdir():
        match = pattern.fullmatch(path.name)
        if match and path.is_file():
            found.append(match.group(1))
    return sorted(set(found))


def prepare_flatten_card(date: str, new_text: str, path: Path, *,
                         manifest: Path | None = None) -> str | None:
    """Return the text to write, or None to leave an already-sealed file.

    A generated-at clock line is not part of the seal. Buy and sell
    lines are. Rewriting a day on or before the watermark is unchanged
    from before this lock existed.
    """
    if manifest is None and not in_repo(path):
        return new_text
    day = str(date)[:10]
    rows = load_manifest(manifest)
    existed = path.is_file()
    on_disk = _dates_in_dir(path.parent, CARD_FILE)
    if existed and day not in on_disk:
        on_disk.append(day)
    wm = _watermark(rows, "flatten_robust", on_disk, writing=day, existed=existed)
    digest = sha256_text(card_body(new_text))
    sealed = {row.get("date"): row for row in _day_rows(rows, "flatten_robust")}
    sealed_dates = [str(d) for d in sealed if d]
    if sealed_dates and day < max(sealed_dates) and day not in sealed:
        raise PastDayLockError(
            f"past-day lock: flatten_robust {day} is before the last sealed "
            f"day {max(sealed_dates)}. A missed day stays missing."
        )
    if day in sealed:
        prev = str(sealed[day].get("sha256") or "")
        if prev != digest:
            raise PastDayLockError(
                f"past-day lock: flatten_robust {day} changed "
                f"(sha256 {prev} -> {digest}). Not rescoring a sealed day."
            )
        return None
    newer = [d for d in on_disk if d > day and (not wm or d > wm)]
    if newer:
        raise PastDayLockError(
            f"past-day lock: flatten_robust {day} is before {max(newer)}. "
            f"A missed day stays missing."
        )
    if wm and day <= wm:
        return new_text
    # Newest day stays open so a same-day rerun can refresh 16:00 marks.
    # Pin the watermark now so that rerun does not swallow this day.
    if _seed_row(rows, "flatten_robust") is None:
        _ensure_seed(rows, "flatten_robust", wm, manifest)
    if existed and path.read_text(encoding="utf-8") == new_text:
        return None
    return new_text


def seal_flatten_card(date: str, path: Path, *,
                      manifest: Path | None = None) -> None:
    """Seal cards from earlier sessions. The day just written stays open.

    A same-day rerun rewrites 09:30 and 16:00 marks. Those bytes are the
    record only once a later session is written.
    """
    if manifest is None and not in_repo(path):
        return
    if not path.parent.is_dir():
        return
    day = str(date)[:10]
    rows = load_manifest(manifest)
    if _seed_row(rows, "flatten_robust") is None:
        return
    wm = str(_seed_row(rows, "flatten_robust").get("watermark") or "")
    on_disk = _dates_in_dir(path.parent, CARD_FILE)
    for prior in sorted(on_disk):
        if prior >= day or (wm and prior <= wm):
            continue
        prior_path = path.parent / f"{prior}_flatten_card.md"
        if not prior_path.is_file():
            continue
        digest = sha256_text(card_body(prior_path.read_text(encoding="utf-8")))
        rows = _seal(
            rows, "flatten_robust", prior, digest, manifest, watermark=wm,
        )


def assert_ticket(date: str, path: Path, new_text: str, *,
                  keep_existing: bool = False,
                  manifest: Path | None = None) -> None:
    """Fail before a sealed ticket file would take different bytes.

    ``keep_existing`` is the evening path: the dated file stays as
    written and only a draft is updated. That is not a past-day edit.
    """
    if manifest is None and not in_repo(path):
        return
    day = str(date)[:10]
    rows = load_manifest(manifest)
    existed = path.is_file()
    on_disk = _dates_in_dir(path.parent, TICKET_FILE)
    wm = _watermark(rows, "strategy_tickets", on_disk, writing=day, existed=existed)
    if wm and day <= wm:
        return
    sealed = {row.get("date"): row for row in _day_rows(rows, "strategy_tickets")}
    if day not in sealed:
        # Morning passes (decision_ready, workflow_run, 08:07 and 09:07)
        # rewrite this file until the 09:30 / paper-send lock seals it.
        return
    prev = str(sealed[day].get("sha256") or "")
    if keep_existing:
        if not existed or sha256_text(path.read_text(encoding="utf-8")) != prev:
            raise PastDayLockError(
                f"past-day lock: strategy_tickets {day} no longer matches "
                f"its seal. Not rescoring a sealed day."
            )
        return
    digest = sha256_text(new_text)
    if prev != digest:
        raise PastDayLockError(
            f"past-day lock: strategy_tickets {day} changed "
            f"(sha256 {prev} -> {digest}). Not rescoring a sealed day."
        )


def seal_ticket(date: str, path: Path, *, locked: bool,
                manifest: Path | None = None) -> None:
    """Seal a ticket once the 09:30 / paper-send lock has held the bytes."""
    if not locked or not path.is_file():
        return
    if manifest is None and not in_repo(path):
        return
    day = str(date)[:10]
    rows = load_manifest(manifest)
    # Later dated files must not raise the watermark over this session.
    on_disk = [d for d in _dates_in_dir(path.parent, TICKET_FILE) if d < day]
    wm = _watermark(rows, "strategy_tickets", on_disk, writing=day, existed=False)
    if wm and day <= wm:
        return
    digest = sha256_text(path.read_text(encoding="utf-8"))
    _seal(rows, "strategy_tickets", day, digest, manifest, watermark=wm)


def guard_csv(record: str, old_text: str, new_text: str, *,
              manifest: Path | None = None) -> None:
    """Fail if a sealed day, or a post-watermark day already on disk, changed.

    The header and days on or before the watermark may change. New days
    may only be appended. Nothing here inserts a missing day.
    """
    rows = load_manifest(manifest)
    old_days = _csv_days(old_text)
    new_days = _csv_days(new_text)
    existed_dates = sorted(old_days)
    wm = _watermark(
        rows, record, existed_dates,
        writing=None, existed=bool(existed_dates),
    )
    # A stored seed uses that watermark. Before the seed, every date
    # already in the file is the pre-lock era (watermark = max old day).
    for row in _day_rows(rows, record):
        date = str(row.get("date") or "")
        if wm and date <= wm:
            raise PastDayLockError(
                f"past-day lock: {record} {date} is on or before the "
                f"watermark {wm} and must not be fingerprinted."
            )
        lines = new_days.get(date)
        if lines is None:
            raise PastDayLockError(
                f"past-day lock: {record} {date} removed. "
                f"A missed day stays missing."
            )
        digest = sha256_text("\n".join(lines) + "\n")
        prev = str(row.get("sha256") or "")
        if digest != prev:
            raise PastDayLockError(
                f"past-day lock: {record} {date} changed "
                f"(sha256 {prev} -> {digest}). Not rescoring a sealed day."
            )
    fresh = [
        date for date in new_days
        if date not in old_days and (not wm or date > wm)
    ]
    if fresh and _seed_row(rows, record) is None:
        rows = _ensure_seed(rows, record, wm, manifest)
    if _seed_row(rows, record) is None:
        return
    sealed = {str(row.get("date")) for row in _day_rows(rows, record)}
    for date, lines in old_days.items():
        if wm and date <= wm:
            continue
        if date in sealed:
            continue
        new_lines = new_days.get(date)
        if new_lines != lines:
            raise PastDayLockError(
                f"past-day lock: {record} {date} was already written "
                f"and changed. Not rescoring a sealed day."
            )


def seal_csv(record: str, old_text: str, new_text: str, *,
             manifest: Path | None = None) -> None:
    rows = load_manifest(manifest)
    old_days = _csv_days(old_text)
    new_days = _csv_days(new_text)
    existed_dates = sorted(old_days)
    wm = _watermark(
        rows, record, existed_dates,
        writing=None, existed=bool(existed_dates),
    )
    fresh = []
    for date in sorted(new_days):
        if wm and date <= wm:
            continue
        if any(row.get("date") == date for row in _day_rows(rows, record)):
            continue
        if date in old_days and new_days[date] == old_days[date]:
            fresh.append(date)
        elif date not in old_days:
            fresh.append(date)
    if not fresh:
        return
    last = ""
    for row in _day_rows(rows, record):
        last = max(last, str(row.get("date") or ""))
    for date in fresh:
        if last and date < last:
            raise PastDayLockError(
                f"past-day lock: {record} {date} is before the last sealed "
                f"day {last}. A missed day stays missing."
            )
        last = max(last, date)
    rows = _ensure_seed(rows, record, wm, manifest)
    for date in fresh:
        digest = sha256_text("\n".join(new_days[date]) + "\n")
        rows = _seal(rows, record, date, digest, manifest, watermark=wm)


def _csv_newline(text: str) -> str:
    return "\r\n" if "\r\n" in text else "\n"


def _split_csv(text: str, column: str) -> tuple[str, dict[str, list[str]], list[str]]:
    """Header, dated body lines, and rows with no session date.

    Lines keep their original text without the newline. An empty
    ``sell_date`` (an open paper lot) lands in the undated list.
    """
    if text.strip() == "":
        return "", {}, []
    lines = text.splitlines()
    header = lines[0]
    cols = next(csv.reader([header]))
    if column not in cols:
        raise PastDayLockError(
            f"past-day lock: csv has no {column} column"
        )
    idx = cols.index(column)
    days: dict[str, list[str]] = {}
    undated: list[str] = []
    for line in lines[1:]:
        if line == "":
            continue
        cells = next(csv.reader([line]))
        raw = cells[idx] if idx < len(cells) else ""
        date = raw[:10]
        if ISO_DAY.fullmatch(date or ""):
            days.setdefault(date, []).append(line)
        else:
            undated.append(line)
    return header, days, undated


def _headers_match(left: str, right: str) -> bool:
    if left == right:
        return True
    return next(csv.reader([left])) == next(csv.reader([right]))


def _reserialize_line(line: str, src_header: str, dst_header: str) -> str:
    src_cols = next(csv.reader([src_header]))
    dst_cols = next(csv.reader([dst_header]))
    cells = next(csv.reader([line]))
    row = {src_cols[i]: cells[i] if i < len(cells) else "" for i in range(len(src_cols))}
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=dst_cols, lineterminator="\n", extrasaction="ignore")
    writer.writerow({col: row.get(col, "") for col in dst_cols})
    return buf.getvalue().rstrip("\n")


def _project_lines(lines: list[str] | None, *, from_new: bool,
                   new_header: str, old_header: str) -> list[str]:
    if not lines:
        return []
    if not from_new or not old_header or _headers_match(old_header, new_header):
        return list(lines)
    return [_reserialize_line(line, new_header, old_header) for line in lines]


def _join_csv(header: str, order: list[str], days: dict[str, list[str]],
              undated: list[str], newline: str) -> str:
    parts = [header]
    for date in order:
        parts.extend(days.get(date) or [])
    parts.extend(undated)
    return newline.join(parts) + newline


def merge_open_tail_csv(record: str, old_text: str, new_text: str, *,
                        column: str, manifest: Path | None = None) -> str:
    """Text to write. Watermark days stay byte-for-byte. Newest day stays open.

    A sealed day, or any post-watermark day that is no longer the newest,
    must match or this raises ``PastDayLockError``. A day that is not
    already on disk and sits before a later on-disk day is not inserted.
    The first write of an empty file is stored as-is and is not hashed.
    """
    if not old_text.strip():
        if new_text and not new_text.endswith(("\n", "\r\n")):
            return new_text + "\n"
        return new_text
    old_header, old_days, _old_undated = _split_csv(old_text, column)
    if new_text.strip() == "":
        return old_text
    new_header, new_days, new_undated = _split_csv(new_text, column)
    if not new_days and not new_undated:
        return old_text
    rows = load_manifest(manifest)
    seed = _seed_row(rows, record)
    pinned = seed is not None
    wm = str(seed.get("watermark") or "") if seed is not None else (
        max(old_days) if old_days else ""
    )
    sealed = {str(row.get("date")): row for row in _day_rows(rows, record)}
    all_dates = sorted(set(old_days) | set(new_days))
    newest = all_dates[-1] if all_dates else ""
    merged: dict[str, list[str]] = {}
    for date in all_dates:
        old_lines = old_days.get(date)
        new_lines = new_days.get(date)
        if wm and date < wm:
            if old_lines is None:
                continue
            merged[date] = list(old_lines)
            continue
        if wm and date == wm:
            if old_lines is None:
                continue
            if pinned or date != newest:
                merged[date] = list(old_lines)
            else:
                chosen = new_lines if new_lines is not None else old_lines
                merged[date] = _project_lines(
                    chosen, from_new=new_lines is not None,
                    new_header=new_header, old_header=old_header,
                )
            continue
        if date in sealed:
            if new_lines != old_lines:
                prev = str(sealed[date].get("sha256") or "")
                digest = sha256_text("\n".join(new_lines or []) + "\n")
                raise PastDayLockError(
                    f"past-day lock: {record} {date} changed "
                    f"(sha256 {prev} -> {digest}). Not rescoring a sealed day."
                )
            merged[date] = list(old_lines or [])
            continue
        if date != newest:
            later_on_disk = [item for item in old_days if item > date]
            if old_lines is None and later_on_disk:
                raise PastDayLockError(
                    f"past-day lock: {record} {date} is before {max(later_on_disk)}. "
                    f"A missed day stays missing."
                )
            if old_lines is not None and new_lines != old_lines:
                raise PastDayLockError(
                    f"past-day lock: {record} {date} was already written "
                    f"and changed. Not rescoring a sealed day."
                )
            chosen = new_lines if new_lines is not None else old_lines
            merged[date] = _project_lines(
                chosen, from_new=new_lines is not None and old_lines is None,
                new_header=new_header, old_header=old_header,
            )
            continue
        chosen = new_lines if new_lines is not None else old_lines
        merged[date] = _project_lines(
            chosen, from_new=new_lines is not None,
            new_header=new_header, old_header=old_header,
        )
    if not pinned and old_days and any(date > wm for date in merged):
        _ensure_seed(rows, record, wm, manifest)
    order: list[str] = []
    for date in old_days:
        if date in merged and date not in order:
            order.append(date)
    for date in sorted(merged):
        if date not in order:
            order.append(date)
    undated = _project_lines(
        new_undated, from_new=True, new_header=new_header, old_header=old_header,
    )
    return _join_csv(old_header, order, merged, undated, _csv_newline(old_text))


def seal_open_tail_csv(record: str, text: str, *, column: str,
                       manifest: Path | None = None) -> None:
    """Seal every post-watermark day except the newest. The open day stays unhashed."""
    if text.strip() == "":
        return
    rows = load_manifest(manifest)
    if _seed_row(rows, record) is None:
        return
    wm = str(_seed_row(rows, record).get("watermark") or "")
    _header, days, _undated = _split_csv(text, column)
    if not days:
        return
    newest = max(days)
    for date in sorted(days):
        if wm and date <= wm:
            continue
        if date == newest:
            continue
        digest = sha256_text("\n".join(days[date]) + "\n")
        rows = _seal(rows, record, date, digest, manifest, watermark=wm)


def commit_open_tail_csv(record: str, path: Path, new_text: str, *,
                         column: str, manifest: Path | None = None) -> None:
    """Merge, write, then seal. Paths outside the repo stay unlocked for tests."""
    path = Path(path)
    if manifest is None and not in_repo(path):
        path.parent.mkdir(parents=True, exist_ok=True)
        if new_text and not new_text.endswith(("\n", "\r\n")):
            new_text += "\n"
        path.write_text(new_text, encoding="utf-8")
        return
    old = path.read_text(encoding="utf-8") if path.is_file() else ""
    merged = merge_open_tail_csv(
        record, old, new_text, column=column, manifest=manifest,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(merged, encoding="utf-8")
    seal_open_tail_csv(record, merged, column=column, manifest=manifest)


def assert_factor_mine(name: str, old_text: str, new_text: str, *,
                       watermark: str, rows: list[dict]) -> None:
    """Fail when a sealed day row or buy/sell line changes.

    Totals, fill counts, realized dollars, and the audit line are not
    sealed. Days on or before ``watermark`` are not sealed.
    """
    new_days = locked_lines_by_date(new_text)
    old_days = locked_lines_by_date(old_text)
    sealed = _day_rows(rows, "factor_mine", name)
    sealed_dates = []
    for row in sealed:
        date = str(row.get("date") or "")
        sealed_dates.append(date)
        if watermark and date <= watermark:
            raise PastDayLockError(
                f"past-day lock: factor_mine {name} {date} is on or before "
                f"the watermark and must not be fingerprinted."
            )
        lines = new_days.get(date)
        if not lines:
            raise PastDayLockError(
                f"past-day lock: factor_mine {name} {date} day row removed. "
                f"Not rescoring a sealed day."
            )
        digest = sha256_text("\n".join(lines) + "\n")
        prev = str(row.get("sha256") or "")
        if digest != prev:
            raise PastDayLockError(
                f"past-day lock: factor_mine {name} {date} changed "
                f"(sha256 {prev} -> {digest}). Not rescoring a sealed day."
            )
    if _seed_row(rows, "factor_mine") is None:
        return
    last = max(sealed_dates) if sealed_dates else ""
    for date in sorted(old_days):
        if watermark and date <= watermark:
            continue
        if date in sealed_dates:
            continue
        if new_days.get(date) != old_days.get(date):
            raise PastDayLockError(
                f"past-day lock: factor_mine {name} {date} was already "
                f"written and changed. Not rescoring a sealed day."
            )
    for date in sorted(new_days):
        if watermark and date <= watermark:
            continue
        if date in old_days or date in sealed_dates:
            continue
        if last and date < last:
            raise PastDayLockError(
                f"past-day lock: factor_mine {name} {date} is before the "
                f"last sealed day {last}. A missed day stays missing."
            )


def factor_mine_watermark(old_texts: list[str], rows: list[dict]) -> str:
    seed = _seed_row(rows, "factor_mine")
    if seed is not None:
        return str(seed.get("watermark") or "")
    found = ""
    for text in old_texts:
        for date in locked_lines_by_date(text):
            if date > found:
                found = date
    return found


def seal_factor_mine(name: str, old_text: str, new_text: str, *,
                     watermark: str, manifest: Path | None = None) -> None:
    rows = load_manifest(manifest)
    old_days = locked_lines_by_date(old_text)
    new_days = locked_lines_by_date(new_text)
    fresh = []
    for date in sorted(new_days):
        if watermark and date <= watermark:
            continue
        if any(row.get("date") == date for row in _day_rows(rows, "factor_mine", name)):
            continue
        if date in old_days and new_days[date] == old_days[date]:
            fresh.append(date)
        elif date not in old_days:
            fresh.append(date)
    if not fresh:
        return
    for date in fresh:
        digest = sha256_text("\n".join(new_days[date]) + "\n")
        rows = _seal(
            rows, "factor_mine", date, digest, manifest,
            name=name, watermark=watermark,
        )


def guard_factor_mine_dir(dest: Path, rendered: list[tuple[str, str]], *,
                          manifest: Path | None = None) -> str:
    """Check every recipe before any scoreboard file is replaced."""
    if not in_repo(dest):
        return ""
    rows = load_manifest(manifest)
    old_texts = []
    paired: list[tuple[str, str, str]] = []
    for name, new_text in rendered:
        path = dest / f"{name}.md"
        old = path.read_text(encoding="utf-8") if path.is_file() else ""
        old_texts.append(old)
        paired.append((name, old, new_text))
    watermark = factor_mine_watermark(old_texts, rows)
    for name, old, new in paired:
        assert_factor_mine(name, old, new, watermark=watermark, rows=rows)
    new_dates = []
    for _name, old, new in paired:
        old_days = locked_lines_by_date(old)
        for date in locked_lines_by_date(new):
            if date not in old_days and (not watermark or date > watermark):
                new_dates.append(date)
    if new_dates and _seed_row(rows, "factor_mine") is None:
        _ensure_seed(rows, "factor_mine", watermark, manifest)
    return watermark


def seal_factor_mine_dir(dest: Path, rendered: list[tuple[str, str]], *,
                         watermark: str, manifest: Path | None = None) -> None:
    if not in_repo(dest):
        return
    # Files already hold the new text. Dates on or before the watermark
    # are not hashed. A date after it that is not yet sealed is the append.
    for name, new_text in rendered:
        seal_factor_mine(
            name, "", new_text, watermark=watermark, manifest=manifest,
        )


def _git_bytes(rev: str, rel: str) -> bytes | None:
    proc = subprocess.run(
        ["git", "show", f"{rev}:{rel}"],
        cwd=ROOT, check=False, capture_output=True,
    )
    if proc.returncode != 0:
        return None
    return proc.stdout


def _split_manifest_text(text: str) -> list[str]:
    if text == "":
        return []
    lines = text.splitlines()
    if any(line == "" for line in lines):
        raise PastDayLockError("past-day lock: blank manifest line")
    return lines


def assert_manifest_prefix(base_text: str, head_text: str) -> None:
    """The committed manifest may only grow at the end. Same rule as breadth_rank."""
    base_lines = _split_manifest_text(base_text)
    head_lines = _split_manifest_text(head_text)
    if head_lines[: len(base_lines)] != base_lines:
        if len(head_lines) < len(base_lines):
            raise PastDayLockError("past-day lock: manifest line removed")
        if all(line in head_lines for line in base_lines):
            raise PastDayLockError("past-day lock: manifest line reordered")
        raise PastDayLockError("past-day lock: manifest line changed")


def _digest_for_row(row: dict) -> str | None:
    record = str(row.get("record") or "")
    date = str(row.get("date") or "")
    name = str(row.get("name") or "")
    if record == "flatten_robust":
        path = FLATTEN_DIR / f"{date}_flatten_card.md"
        if not path.is_file():
            return None
        return sha256_text(card_body(path.read_text(encoding="utf-8")))
    if record == "strategy_tickets":
        path = TICKET_DIR / f"{date}_strategy_tickets.json"
        if not path.is_file():
            return None
        return sha256_text(path.read_text(encoding="utf-8"))
    if record == "factor_mine":
        path = FACTOR_MINE_DIR / f"{name}.md"
        if not path.is_file():
            return None
        lines = locked_lines_by_date(path.read_text(encoding="utf-8")).get(date) or []
        if not lines:
            return None
        return sha256_text("\n".join(lines) + "\n")
    spec = _CSV_RECORDS.get(record)
    if spec is not None:
        directory, filename, column = spec
        path = directory / filename
        if not path.is_file():
            return None
        _header, days, _undated = _split_csv(path.read_text(encoding="utf-8"), column)
        lines = days.get(date) or []
        if not lines:
            return None
        return sha256_text("\n".join(lines) + "\n")
    if record == "webull_sim":
        path = WEBULL_DIR / "days.jsonl"
        if not path.is_file():
            return None
        matched = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if str(row.get("name") or "") == name and str(row.get("date") or "") == date:
                matched.append(line)
        if len(matched) != 1:
            return None
        return sha256_text(matched[0] + "\n")
    raise PastDayLockError(f"past-day lock: unknown record {record}")


def check_manifest(rows: list[dict] | None = None, *,
                   path: Path | None = None) -> None:
    rows = load_manifest(path) if rows is None else rows
    watermarks: dict[str, str] = {}
    for row in rows:
        if row.get("kind") == "seed":
            watermarks[str(row.get("record") or "")] = str(row.get("watermark") or "")
    for row in rows:
        if row.get("kind") != "day":
            continue
        record = str(row.get("record") or "")
        date = str(row.get("date") or "")
        wm = watermarks.get(record, "")
        if wm and date <= wm:
            raise PastDayLockError(
                f"past-day lock: fingerprint for {record} {date} is on or "
                f"before watermark {wm}. Historical days are not backfilled."
            )
        digest = _digest_for_row(row)
        prev = str(row.get("sha256") or "")
        name = str(row.get("name") or "")
        label = f"{record} {name} {date}".replace("  ", " ")
        if digest is None:
            raise PastDayLockError(
                f"past-day lock: {label} is sealed but the day text is missing."
            )
        if digest != prev:
            raise PastDayLockError(
                f"past-day lock: {label} changed "
                f"(sha256 {prev} -> {digest}). Not rescoring a sealed day."
            )


def check_against(rev: str, *, manifest: Path | None = None) -> None:
    dest = Path(manifest or MANIFEST_PATH)
    rel = dest.relative_to(ROOT).as_posix() if in_repo(dest) else dest.as_posix()
    base = _git_bytes(rev, rel)
    head = dest.read_text(encoding="utf-8") if dest.is_file() else ""
    assert_manifest_prefix((base or b"").decode("utf-8"), head)
    check_manifest(path=dest)


def skip_flatten_card_after_open(date: str, now=None,
                                 card_path: Path | None = None) -> bool:
    """True when Stock Book ALL must not rewrite today's flatten card.

    After 09:30 ET, sleeve_merge_live owns post-open updates to
    ``01_daily/<date>_flatten_card.md``. Stock Book ALL writes that
    same file. Once the open has passed and the card is already on
    disk, the book job skips so the two jobs do not replace each
    other's tickets. Before 09:30 the book job may still write. A
    missing card after the open is still written once. Sealed buy/sell
    lines stay under prepare/seal; this does not invent fills.
    """
    from datetime import datetime
    from zoneinfo import ZoneInfo

    et = ZoneInfo("America/New_York")
    clock = now or datetime.now(et)
    if clock.tzinfo is None:
        clock = clock.replace(tzinfo=et)
    else:
        clock = clock.astimezone(et)
    day = str(date)[:10]
    try:
        year, month, day_n = (int(part) for part in day.split("-"))
    except ValueError:
        return False
    open_at = datetime(year, month, day_n, 9, 30, tzinfo=et)
    if clock < open_at:
        return False
    path = card_path or (FLATTEN_DIR / f"{day}_flatten_card.md")
    return path.is_file()


def describe_seeds() -> str:
    """Plain-English note. No historical day is hashed by this file."""
    return (
        "The first locked day is the next day each record appends after "
        "this code is running. The watermark is the latest day already "
        "on disk at that moment (flatten cards and sleeve tickets through "
        "the latest dated file, Factor Mine scoreboards through the latest "
        "day row). Days on or before that watermark are not fingerprinted. "
        "No sha256 is written for 14-24 Sep or any other day already in "
        "the record. The newest flatten card stays open until the next "
        "session. Strategy tickets stay open until 09:30 ET or the paper "
        "send journal. data/paper/trades.csv, roundtrips, skipped, "
        "equity_curve, and data/sleeve_merge/trades.csv, skipped, and "
        "equity_curve are locked the same way: the newest day stays open, "
        "days on or before the watermark are not fingerprinted and are "
        "not rewritten, and a sealed day that would change fails closed. "
        "Webull sim books fingerprint from 2026-10-06. Days before that "
        "are built after the fact and are not fingerprinted."
    )


def main(argv: list[str] | None = None) -> int:
    import argparse
    parser = argparse.ArgumentParser(description="Check past-day locks")
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--check-against", dest="rev", default="")
    args = parser.parse_args(argv)
    if args.rev:
        check_against(args.rev)
        print(f"past-day lock ok against {args.rev}")
        return 0
    check_manifest()
    print("past-day lock ok")
    print(describe_seeds())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
