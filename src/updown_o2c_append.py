"""Append-only writer for dashboard/updown updown_o2c_v2.

git log -- dashboard/updown/data shows the o2c files were written by hand,
not by a workflow:

- cffac6ee16 (2026-10-06) shipped data/index.json and data/y/*.bin.gz
- b4b0b34922 (2026-10-06) switched those files to updown_o2c_v2

No later commit changes data/index.json or data/y/. The commits that do
touch dashboard/updown/data after that are updown_shadow, and that
workflow writes only dashboard/updown/data/expmove/ (plus the shadow
page). features_v2.py, wf2.py, and preds/o2c_v2_*.parquet were never
committed. This job does not rebuild that model and does not invent
p_up or realized returns.

A session after the last sealed day is appended only when
research/updown_o2c_v2/forward/YYYY-MM-DD.json exists and contains at
least one p_up. Days with no source file stay missing. Rows dated on or
before the last sealed day are not rewritten: the year-bin p and r
bytes for those days are copied, and the index.json day arrays are the
same objects.

Last sealed day is the newest day already in index.json when that day
is already in the past. The newest day stays open only when it is still
today. The past-day lock manifest is not edited.
"""
from __future__ import annotations

import argparse
import gzip
import json
import struct
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "dashboard" / "updown" / "data"
FORWARD_DIR = ROOT / "research" / "updown_o2c_v2" / "forward"
MAGIC = b"UD01"
MISSING_P = 65535
MISSING_R = -32768


def _session_after(day: str) -> str:
    from .skip_if_good import _session_date

    cursor = date.fromisoformat(day) + timedelta(days=1)
    while not _session_date(cursor):
        cursor += timedelta(days=1)
    return cursor.isoformat()


def sessions_after(last: str, through: str) -> list[str]:
    """NYSE sessions strictly after ``last`` and on or before ``through``."""
    if not last or not through or through <= last:
        return []
    out = []
    cursor = _session_after(last)
    while cursor <= through:
        out.append(cursor)
        nxt = _session_after(cursor)
        if nxt <= cursor:
            break
        cursor = nxt
    return out


def last_closed_session(now: datetime | None = None) -> str:
    from .skip_if_good import last_closed_session as _closed

    return _closed(now)


def et_today(now: datetime | None = None) -> str:
    from .skip_if_good import ET

    now = now or datetime.now(ET)
    if now.tzinfo is None:
        now = now.replace(tzinfo=ET)
    return now.astimezone(ET).date().isoformat()


def sealed_through(day_dates: list[str], *, today: str) -> str | None:
    uniq = sorted({str(d)[:10] for d in day_dates if str(d or "")[:10]})
    if not uniq:
        return None
    newest = uniq[-1]
    if newest >= today:
        return uniq[-2] if len(uniq) > 1 else None
    return newest


def _num(value) -> float | None:
    if value is None or value == "":
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if out != out:  # NaN
        return None
    return out


def _row_tuple(item) -> tuple[str, float, float | None] | None:
    ticker = ""
    p = None
    r = None
    if isinstance(item, dict):
        ticker = str(item.get("ticker") or item.get("t") or "").upper()
        p = _num(item.get("p") if "p" in item else item.get("p_up"))
        if "r" in item or "o2c" in item:
            r = _num(item.get("r") if "r" in item else item.get("o2c"))
    elif isinstance(item, (list, tuple)) and len(item) >= 2:
        ticker = str(item[0] or "").upper()
        p = _num(item[1])
        if len(item) >= 3:
            r = _num(item[2])
    if not ticker or p is None:
        return None
    return ticker, p, r


def load_forward(folder: Path) -> dict[str, dict[str, tuple[float, float | None]]]:
    """date -> ticker -> (p_up, realized o2c or None). Empty if no source."""
    out: dict[str, dict[str, tuple[float, float | None]]] = {}
    if not folder.is_dir():
        return out
    for path in sorted(folder.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict):
            continue
        day = str(payload.get("date") or path.stem)[:10]
        if len(day) != 10 or day[4] != "-" or day[7] != "-":
            continue
        rows = payload.get("rows") or payload.get("preds") or []
        bucket = out.setdefault(day, {})
        for item in rows:
            parsed = _row_tuple(item)
            if parsed is None:
                continue
            ticker, p, r = parsed
            bucket[ticker] = (p, r)
        if not bucket:
            out.pop(day, None)
    return out


def pack_p(p: float) -> int:
    value = int(round(float(p) * 10000))
    if value < 0:
        return 0
    if value > 10000:
        return 10000
    return value


def pack_r(r: float | None) -> int:
    if r is None:
        return MISSING_R
    value = int(round(float(r) * 10000))
    if value <= MISSING_R:
        return MISSING_R + 1
    if value > 32767:
        return 32767
    return value


def _auc(pairs: list[tuple[float, int]]) -> float | None:
    pos = [score for score, label in pairs if label]
    neg = [score for score, label in pairs if not label]
    if not pos or not neg:
        return None
    wins = 0.0
    for score in pos:
        for other in neg:
            if score > other:
                wins += 1.0
            elif score == other:
                wins += 0.5
    return wins / (len(pos) * len(neg))


def day_stats(p_row: list[int], r_row: list[int]) -> list:
    scored_p = [p / 10000 for p in p_row if p != MISSING_P]
    realized = [r / 10000 for r in r_row if r != MISSING_R]
    both = [
        (p / 10000, r / 10000)
        for p, r in zip(p_row, r_row)
        if p != MISSING_P and r != MISSING_R
    ]
    n = len(scored_p)
    mean_p = round(sum(scored_p) / n, 4) if n else None
    up = None
    if realized:
        up = round(sum(1 for r in realized if r > 0) / len(realized), 4)
    hit = None
    auc = None
    spread = None
    if both:
        hits = [(1 if (p > 0.5) == (r > 0) else 0) for p, r in both]
        hit = round(sum(hits) / len(hits), 4)
        auc_v = _auc([(p, 1 if r > 0 else 0) for p, r in both])
        auc = None if auc_v is None else round(auc_v, 4)
        order = sorted(range(len(both)), key=lambda i: both[i][0])
        dec = [0] * len(both)
        for rank, i in enumerate(order):
            dec[i] = rank * 10 // len(both) + 1
        bot = [both[i][1] for i in range(len(both)) if dec[i] == 1]
        top = [both[i][1] for i in range(len(both)) if dec[i] == 10]
        if bot and top:
            spread = round((sum(top) / len(top) - sum(bot) / len(bot)) * 100, 3)
    return [n, up, hit, auc, spread, mean_p]


def append_year_blob(blob: bytes, new_global_indices: list[int],
                     p_rows: list[list[int]], r_rows: list[list[int]]) -> bytes:
    """Copy sealed p/r bytes. Append new day rows at the end."""
    if not new_global_indices:
        return blob
    if not (len(new_global_indices) == len(p_rows) == len(r_rows)):
        raise ValueError("year append row count mismatch")
    magic, n_t, n_d = struct.unpack_from("<4sHH", blob, 0)
    if magic != MAGIC:
        raise ValueError("updown year bin is not UD01")
    off = 8
    tick_end = off + 2 * n_t
    day_end = tick_end + 2 * n_d
    p_end = day_end + 2 * n_t * n_d
    r_end = p_end + 2 * n_t * n_d
    if r_end != len(blob):
        raise ValueError("updown year bin length does not match the header")
    for row in p_rows:
        if len(row) != n_t:
            raise ValueError("p row width does not match sealed nT")
    for row in r_rows:
        if len(row) != n_t:
            raise ValueError("r row width does not match sealed nT")
    n_d2 = n_d + len(new_global_indices)
    if n_d2 > 65535:
        raise ValueError("too many days for u16")
    out = bytearray()
    out += struct.pack("<4sHH", MAGIC, n_t, n_d2)
    out += blob[off:tick_end]
    out += blob[tick_end:day_end]
    out += struct.pack("<" + "H" * len(new_global_indices), *new_global_indices)
    out += blob[day_end:p_end]
    for row in p_rows:
        out += struct.pack("<" + "H" * n_t, *row)
    out += blob[p_end:r_end]
    for row in r_rows:
        out += struct.pack("<" + "h" * n_t, *row)
    return bytes(out)


def sealed_matrix_bytes(blob: bytes) -> tuple[bytes, bytes, bytes]:
    """ticker index, day index, and the p then r matrices."""
    magic, n_t, n_d = struct.unpack_from("<4sHH", blob, 0)
    if magic != MAGIC:
        raise ValueError("updown year bin is not UD01")
    off = 8
    tick_end = off + 2 * n_t
    day_end = tick_end + 2 * n_d
    p_end = day_end + 2 * n_t * n_d
    r_end = p_end + 2 * n_t * n_d
    return blob[off:tick_end], blob[tick_end:day_end], blob[day_end:r_end]


def sealed_prefix_ok(old_blob: bytes, new_blob: bytes) -> bool:
    """Sealed ticker index, day index, and p/r rows are a byte prefix."""
    old_tick, old_days, old_matrix = sealed_matrix_bytes(old_blob)
    new_tick, new_days, new_matrix = sealed_matrix_bytes(new_blob)
    if new_tick != old_tick or not new_days.startswith(old_days):
        return False
    _, n_t, n_d = struct.unpack_from("<4sHH", new_blob, 0)
    _, _, old_n_d = struct.unpack_from("<4sHH", old_blob, 0)
    old_p_len = 2 * n_t * old_n_d
    new_p_len = 2 * n_t * n_d
    if len(old_matrix) != old_p_len * 2 or len(new_matrix) != new_p_len * 2:
        return False
    old_p = old_matrix[:old_p_len]
    old_r = old_matrix[old_p_len:]
    new_p = new_matrix[:new_p_len]
    new_r = new_matrix[new_p_len:]
    return new_p.startswith(old_p) and new_r.startswith(old_r)


def read_year(path: Path) -> bytes:
    with gzip.open(path, "rb") as handle:
        return handle.read()


def write_year(path: Path, blob: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as handle:
            handle.write(blob)


def _ticker_index(blob: bytes, tickers: list[str]) -> dict[str, int]:
    magic, n_t, n_d = struct.unpack_from("<4sHH", blob, 0)
    if magic != MAGIC:
        raise ValueError("updown year bin is not UD01")
    raw = struct.unpack_from("<" + "H" * n_t, blob, 8)
    del n_d
    out = {}
    for local, global_i in enumerate(raw):
        if global_i < len(tickers):
            out[tickers[global_i]] = local
    return out


def _rows_for_year(scores: dict[str, tuple[float, float | None]],
                   local: dict[str, int], n_t: int) -> tuple[list[int], list[int]] | None:
    p_row = [MISSING_P] * n_t
    r_row = [MISSING_R] * n_t
    used = 0
    for ticker, (p, r) in scores.items():
        slot = local.get(ticker)
        if slot is None:
            continue
        p_row[slot] = pack_p(p)
        r_row[slot] = pack_r(r)
        used += 1
    if used == 0:
        return None
    return p_row, r_row


def plan(index: dict, forward: dict[str, dict], *, today: str,
         through: str) -> dict:
    days = list(index.get("days") or [])
    dates = [str(row[0])[:10] for row in days if isinstance(row, list) and row]
    sealed = sealed_through(dates, today=today)
    last = dates[-1] if dates else ""
    wanted = sessions_after(sealed or last, through) if (sealed or last) else []
    have = set(dates)
    appended = []
    missing = []
    for day in wanted:
        if day in have:
            continue
        scores = forward.get(day) or {}
        if scores:
            appended.append(day)
        else:
            missing.append(day)
    return {
        "model": index.get("model"),
        "last_on_disk": last,
        "sealed_through": sealed,
        "through": through,
        "append": appended,
        "missing": missing,
    }


def _built_stamp(now: datetime | None = None) -> str:
    try:
        from zoneinfo import ZoneInfo
        zone = ZoneInfo("Asia/Hong_Kong")
    except Exception:
        zone = None
    now = now or datetime.now(zone) if zone else datetime.now()
    if zone is not None:
        now = now.astimezone(zone)
    return now.strftime("%Y-%m-%d %H:%M HKT")


def apply(index: dict, data_dir: Path, forward: dict, report: dict,
          *, write: bool, sealed_day_blobs: list[str] | None = None) -> dict:
    """Append ``report['append']`` onto index and the year bins.

    Sealed day arrays and sealed matrix bytes are unchanged.
    """
    days = index.get("days")
    if not isinstance(days, list):
        raise ValueError("index.json days is not a list")
    tickers = list(index.get("tickers") or [])
    holdout = str(index.get("holdout_start") or "2026-07-06")
    blobs: dict[str, bytes] = {}
    per_year: dict[str, list] = {}
    skipped: list[str] = []
    no_names: list[str] = []
    for day in list(report["append"]):
        year = day[:4]
        path = data_dir / "y" / f"{year}.bin.gz"
        if year not in blobs:
            if not path.is_file():
                skipped.append(day)
                continue
            blobs[year] = read_year(path)
        blob = blobs[year]
        _magic, n_t, _n_d = struct.unpack_from("<4sHH", blob, 0)
        local = _ticker_index(blob, tickers)
        built = _rows_for_year(forward.get(day) or {}, local, n_t)
        if built is None:
            no_names.append(day)
            continue
        p_row, r_row = built
        per_year.setdefault(year, []).append((day, p_row, r_row))
    if skipped:
        report["skipped_no_year_bin"] = skipped
    drop = set(skipped) | set(no_names)
    if drop:
        report["append"] = [day for day in report["append"] if day not in drop]
        report["missing"] = list(report["missing"]) + [day for day in no_names + skipped
                                                       if day not in report["missing"]]
    report["append"] = [
        day for day in report["append"]
        if any(day == item[0] for rows in per_year.values() for item in rows)
    ]
    if not report["append"] or not write:
        report["wrote"] = False
        return report
    pending: dict[str, bytes] = {}
    for year, rows in per_year.items():
        blob = blobs[year]
        indices = []
        p_rows = []
        r_rows = []
        for day, p_row, r_row in rows:
            indices.append(len(days))
            period = "h" if day >= holdout else "d"
            stats = day_stats(p_row, r_row)
            days.append([day, period, *stats])
            p_rows.append(p_row)
            r_rows.append(r_row)
        new_blob = append_year_blob(blob, indices, p_rows, r_rows)
        if not sealed_prefix_ok(blob, new_blob):
            raise SystemExit("past-day lock: sealed updown matrix changed")
        pending[year] = new_blob
    index["built"] = _built_stamp()
    text = json.dumps(index, separators=(",", ":"))
    for blob in sealed_day_blobs or []:
        if blob not in text:
            raise SystemExit("past-day lock: sealed updown day row is not in index.json")
    for year, new_blob in pending.items():
        write_year(data_dir / "y" / f"{year}.bin.gz", new_blob)
    (data_dir / "index.json").write_text(text, encoding="utf-8")
    report["wrote"] = True
    return report


def run(*, data_dir: Path | None = None, forward_dir: Path | None = None,
        today: str | None = None, through: str | None = None,
        write: bool = False, now: datetime | None = None) -> dict:
    data_dir = Path(data_dir) if data_dir is not None else DATA_DIR
    forward_dir = Path(forward_dir) if forward_dir is not None else FORWARD_DIR
    index_path = data_dir / "index.json"
    raw = index_path.read_text(encoding="utf-8")
    index = json.loads(raw)
    today = today or et_today(now)
    through = through or last_closed_session(now)
    forward = load_forward(forward_dir)
    report = plan(index, forward, today=today, through=through)
    sealed = report.get("sealed_through")
    sealed_day_blobs = []
    for row in index.get("days") or []:
        if isinstance(row, list) and row and sealed and str(row[0])[:10] <= sealed:
            sealed_day_blobs.append(json.dumps(row, separators=(",", ":")))
    before_files = {}
    if write:
        for path in [index_path, *sorted((data_dir / "y").glob("*.bin.gz"))]:
            if path.is_file():
                before_files[str(path.relative_to(data_dir))] = path.read_bytes()
    report = apply(
        index, data_dir, forward, report,
        write=write, sealed_day_blobs=sealed_day_blobs,
    )
    report["sealed_day_blobs"] = sealed_day_blobs
    if write and not report.get("wrote"):
        for rel, blob in before_files.items():
            path = data_dir / rel
            if path.read_bytes() != blob:
                raise SystemExit(f"past-day lock: no-op rewrote {rel}")
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Append updown_o2c_v2 days")
    parser.add_argument("--write", action="store_true",
                        help="Write index.json and y/YYYY.bin.gz. Default prints the plan.")
    parser.add_argument("--through", default="",
                        help="Last session to consider (YYYY-MM-DD). Default: last closed NYSE session.")
    parser.add_argument("--today", default="")
    parser.add_argument("--data-dir", default="")
    parser.add_argument("--forward-dir", default="")
    args = parser.parse_args(argv)
    report = run(
        data_dir=Path(args.data_dir) if args.data_dir else None,
        forward_dir=Path(args.forward_dir) if args.forward_dir else None,
        today=args.today or None,
        through=args.through or None,
        write=args.write,
    )
    public = {
        key: report[key]
        for key in (
            "model", "last_on_disk", "sealed_through", "through",
            "append", "missing", "wrote", "skipped_no_year_bin",
        )
        if key in report
    }
    print(json.dumps(public, indent=2))
    if public.get("append"):
        print(
            f"[updown_o2c] append {public['append'][0]} → {public['append'][-1]} "
            f"({len(public['append'])} days)"
        )
    else:
        print(
            f"[updown_o2c] nothing to append after {public.get('sealed_through')}; "
            f"{len(public.get('missing') or [])} sessions have no source file"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
