"""Nightly blotter audit for 2026-08-13 through 2026-09-24.

Each ``chore: factor strategy mine`` commit is one night. The next night
is compared with the previous night for every recipe blotter. A past
trade day whose buy or sell tickers changed is a rewrite. A session that
appears for the first time is an append.

The forward append guard (fail the nightly job if an earlier day changes)
belongs on the rules pull request from main after #335 merges.
"""
from __future__ import annotations

import hashlib
import os
import re
import shutil
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
BLOTTER_DIR = "03_scoreboard/factor_mine"
OUT_MD = ROOT / "03_scoreboard" / "FACTOR_MINE_CHANGELOG.md"
NIGHTLY_SUBJECT = "chore: factor strategy mine"
WINDOW_START = "2026-08-13"
WINDOW_END = "2026-09-24"
ET = ZoneInfo("America/New_York")

FILL_RE = re.compile(
    r"\| (?P<date>\d{4}-\d{2}-\d{2}) [^|\n]*\| "
    r"\*\*(?P<side>BUY|SELL|SHORT|COVER)\*\* \| "
    r"`(?P<ticker>[A-Z0-9.\-]+)` \| (?P<shares>[\d,]+) \|"
)
CASH_RE = re.compile(
    r"Cash book \*\*(?P<pct>[+-]?\d+(?:\.\d+)?)%\*\*"
    r"(?: \(\$(?P<equity>[\d,]+(?:\.\d+)?)\))?"
)
BUY_SIDES = frozenset({"BUY", "COVER"})
SELL_SIDES = frozenset({"SELL", "SHORT"})


def _git(repo: Path, args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", *args],
        cwd=repo,
        capture_output=True,
        text=True,
        check=False,
    )


def nightly_commits(repo: Path | None = None,
                    start: str = WINDOW_START,
                    end: str = WINDOW_END) -> list[dict]:
    """Chronological nightly blotter commits whose ET date is in range."""
    repo = Path(repo or ROOT)
    proc = _git(repo, [
        "log", "--reverse", "--format=%H%x09%cI%x09%s", "--", BLOTTER_DIR,
    ])
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or "git log failed")
    nights = []
    for line in proc.stdout.splitlines():
        if not line.strip():
            continue
        sha, iso, subject = line.split("\t", 2)
        if not subject.startswith(NIGHTLY_SUBJECT):
            continue
        when = datetime.fromisoformat(iso).astimezone(ET)
        et_date = when.strftime("%Y-%m-%d")
        if et_date < start or et_date > end:
            continue
        nights.append({
            "sha": sha,
            "short": sha[:9],
            "committed_at": when.strftime("%Y-%m-%d %H:%M ET"),
            "et_date": et_date,
            "subject": subject,
        })
    return nights


def parse_blotter(text: str) -> dict:
    """Cash-book total and ordered buy/sell labels by trade date."""
    cash = CASH_RE.search(text)
    total = float(cash.group("pct")) if cash else None
    equity = None
    if cash and cash.group("equity"):
        equity = cash.group("equity").replace(",", "")
    days: dict[str, dict[str, list[str]]] = {}
    for match in FILL_RE.finditer(text):
        side = match.group("side")
        shares = int(match.group("shares").replace(",", ""))
        label = f"{side} {match.group('ticker')}×{shares}"
        bucket = days.setdefault(match.group("date"), {"buys": [], "sells": []})
        if side in BUY_SIDES:
            bucket["buys"].append(label)
        elif side in SELL_SIDES:
            bucket["sells"].append(label)
    return {"total_pct": total, "equity": equity, "days": days}


def _recipe_name(path: str) -> str | None:
    if not path.startswith(BLOTTER_DIR + "/"):
        return None
    rest = path[len(BLOTTER_DIR) + 1:]
    if "/" in rest or not rest.endswith(".md"):
        return None
    return rest[:-3]


def _split_grep(line: str, sha: str) -> tuple[str, str] | None:
    prefix = sha + ":"
    if not line.startswith(prefix):
        return None
    rest = line[len(prefix):]
    path, _, content = rest.partition(":")
    if not path:
        return None
    return path, content


def load_books(repo: Path, sha: str) -> dict[str, dict]:
    """Parse every recipe blotter at ``sha`` without checking the tree out."""
    books: dict[str, dict] = {}
    cash = _git(repo, [
        "grep", "-E", r"Cash book \*\*", sha, "--", BLOTTER_DIR,
    ])
    if cash.returncode not in (0, 1):
        raise RuntimeError(cash.stderr.strip() or "git grep cash failed")
    for line in cash.stdout.splitlines():
        parsed = _split_grep(line, sha)
        if parsed is None:
            continue
        path, content = parsed
        name = _recipe_name(path)
        if name is None:
            continue
        books[name] = parse_blotter(content)
    fills = _git(repo, [
        "grep", "-E", r"\*\*(BUY|SELL|SHORT|COVER)\*\*", sha, "--", BLOTTER_DIR,
    ])
    if fills.returncode not in (0, 1):
        raise RuntimeError(fills.stderr.strip() or "git grep fills failed")
    grouped: dict[str, list[str]] = {}
    for line in fills.stdout.splitlines():
        parsed = _split_grep(line, sha)
        if parsed is None:
            continue
        path, content = parsed
        name = _recipe_name(path)
        if name is None:
            continue
        grouped.setdefault(name, []).append(content)
    for name, lines in grouped.items():
        book = books.setdefault(name, {"total_pct": None, "equity": None, "days": {}})
        parsed = parse_blotter("\n".join(lines))
        book["days"] = parsed["days"]
    return books


def _labels(items: list[str]) -> str:
    return ", ".join(items) if items else "(none)"


def _arrow(before: list[str], after: list[str]) -> str:
    if before == after:
        return "same"
    return f"{_labels(before)} -> {_labels(after)}"


def diff_books(prev: dict[str, dict], curr: dict[str, dict]) -> dict:
    """Past-day buy/sell rewrites, plus every recipe's cumulative move."""
    added = sorted(set(curr) - set(prev))
    removed = sorted(set(prev) - set(curr))
    changes = []
    moves = []
    for name in sorted(set(prev) & set(curr)):
        old = prev[name]
        new = curr[name]
        past = set(old["days"])
        day_rows = []
        for date in sorted(past):
            before = old["days"].get(date) or {"buys": [], "sells": []}
            after = new["days"].get(date) or {"buys": [], "sells": []}
            if before["buys"] == after["buys"] and before["sells"] == after["sells"]:
                continue
            day_rows.append({
                "date": date,
                "buys": _arrow(before["buys"], after["buys"]),
                "sells": _arrow(before["sells"], after["sells"]),
            })
        before_total = old["total_pct"]
        after_total = new["total_pct"]
        delta = None
        if before_total is not None and after_total is not None:
            delta = round(after_total - before_total, 2)
        appended = sorted(set(new["days"]) - past)
        if day_rows:
            changes.append({
                "recipe": name,
                "total_before": before_total,
                "total_after": after_total,
                "delta": delta,
                "days": day_rows,
                "appended": appended,
            })
        if delta not in (None, 0.0):
            moves.append({
                "recipe": name,
                "total_before": before_total,
                "total_after": after_total,
                "delta": delta,
                "rewritten_days": len(day_rows),
            })
    moves.sort(key=lambda row: abs(row["delta"]), reverse=True)
    return {
        "recipes_checked": len(set(prev) & set(curr)),
        "added": added,
        "removed": removed,
        "changes": changes,
        "moves": moves,
    }


def build_diffs(repo: Path | None = None) -> tuple[list[dict], list[dict]]:
    repo = Path(repo or ROOT)
    nights = nightly_commits(repo)
    books = [load_books(repo, night["sha"]) for night in nights]
    diffs = []
    for prev, curr, old, new in zip(nights, nights[1:], books, books[1:]):
        item = diff_books(old, new)
        item["prev"] = prev
        item["curr"] = curr
        item["prev_recipes"] = len(old)
        item["curr_recipes"] = len(new)
        diffs.append(item)
        print(
            f"[changelog] {curr['et_date']} {curr['short']} "
            f"rewrites={len(item['changes'])} "
            f"recipes={item['recipes_checked']}",
            flush=True,
        )
    counts = [len(book) for book in books]
    for night, count in zip(nights, counts):
        night["recipes"] = count
    return nights, diffs


def _pct(value) -> str:
    if value is None:
        return "—"
    return f"{value:+.2f}%"


def _pp(value) -> str:
    if value is None:
        return "—"
    return f"{value:+.2f} pp"


def _cell(text: str) -> str:
    return text.replace("|", "/")


def summary_stats(nights: list[dict], diffs: list[dict]) -> dict:
    rewrite_nights = [item for item in diffs if item["changes"]]
    recipes = sorted({
        row["recipe"]
        for item in diffs
        for row in item["changes"]
    })
    day_rows = sum(len(row["days"]) for item in diffs for row in item["changes"])
    swings = []
    for item in diffs:
        for move in item["moves"]:
            swings.append({
                "night": item["curr"]["committed_at"],
                "sha": item["curr"]["short"],
                **move,
            })
    swings.sort(key=lambda row: abs(row["delta"]), reverse=True)
    restated = [row for row in swings if row["rewritten_days"]]
    return {
        "nights": len(nights),
        "comparisons": len(diffs),
        "rewrite_nights": len(rewrite_nights),
        "recipes_affected": len(recipes),
        "day_rows": day_rows,
        "swings": swings[:8],
        "restated_swings": restated[:5],
        "recipe_names": recipes,
    }


def render_markdown(nights: list[dict], diffs: list[dict],
                    determinism: dict | None = None) -> str:
    stats = summary_stats(nights, diffs)
    lines = [
        "# Factor Mine change log",
        "",
        _summary_prose(nights, stats),
        "",
        "## Determinism",
        "",
    ]
    lines.extend(_determinism_lines(determinism))
    lines += [
        "",
        "The forward append log ships on the rules pull request from main "
        "after #335 merges. That nightly step appends the new day's buys "
        "and sells here and fails the job when an earlier day changed.",
        "",
        "## Per night",
        "",
    ]
    if nights:
        first = nights[0]
        lines += [
            f"### {first['committed_at']} (`{first['short']}`)",
            "",
            f"First nightly blotter in this window. "
            f"{first.get('recipes', 0)} recipes. "
            f"There is no earlier nightly blotter to compare.",
            "",
        ]
    for item in diffs:
        lines.extend(_night_section(item))
    lines.append("")
    return "\n".join(lines)


def _summary_prose(nights: list[dict], stats: dict) -> str:
    if not nights:
        return (
            "No nightly `chore: factor strategy mine` commit touched "
            f"`{BLOTTER_DIR}` between {WINDOW_START} and {WINDOW_END}."
        )
    first, last = nights[0], nights[-1]
    prose = [
        f"{stats['nights']} nightly commits, from {first['committed_at']} "
        f"(`{first['short']}`) through {last['committed_at']} "
        f"(`{last['short']}`). Each night is a commit whose subject starts "
        f"with `{NIGHTLY_SUBJECT}`. The blotter files compared are "
        f"`{BLOTTER_DIR}/*.md`. A past trade day is a session that already "
        f"had fills the night before. A session that shows up for the first "
        f"time is an append.",
        "",
        f"Nights that rewrote a past buy or sell: "
        f"**{stats['rewrite_nights']}** of {stats['comparisons']}. "
        f"Recipes affected: **{stats['recipes_affected']}**. "
        f"Past day rows rewritten: **{stats['day_rows']}**.",
    ]
    if stats["swings"]:
        prose += ["", "Biggest cumulative swings (percentage points):", ""]
        prose.append(
            "| Night | Commit | Recipe | Total before | Total after | Change | Past days rewritten |"
        )
        prose.append("|---|---|---|---:|---:|---:|---:|")
        for row in stats["swings"]:
            prose.append(
                f"| {row['night']} | `{row['sha']}` | `{row['recipe']}` | "
                f"{_pct(row['total_before'])} | {_pct(row['total_after'])} | "
                f"{_pp(row['delta'])} | {row['rewritten_days']} |"
            )
    if stats["restated_swings"]:
        top = stats["restated_swings"][0]
        prose += [
            "",
            f"Largest swing that also rewrote a past day: `{top['recipe']}` "
            f"on {top['night']} ({top['sha']}), {_pct(top['total_before'])} "
            f"to {_pct(top['total_after'])} ({_pp(top['delta'])}), "
            f"{top['rewritten_days']} past day(s).",
        ]
    else:
        prose += [
            "",
            "No night rewrote a past day's buy or sell tickers. "
            "Cumulative totals moved when a new session was appended.",
        ]
    return "\n".join(prose)


def _determinism_lines(determinism: dict | None) -> list[str]:
    if not determinism:
        return [
            "The double rebuild from the locked snapshots has not been recorded.",
        ]
    status = determinism.get("status") or "FAIL"
    lines = [f"**{status}.** {determinism.get('detail') or ''}"]
    for extra in determinism.get("lines") or []:
        lines.append("")
        lines.append(extra)
    return lines


def _night_section(item: dict) -> list[str]:
    curr = item["curr"]
    prev = item["prev"]
    lines = [
        f"### {curr['committed_at']} (`{curr['short']}`)",
        "",
        f"Compared with {prev['committed_at']} (`{prev['short']}`). "
        f"Recipes checked: {item['recipes_checked']}. "
        f"Recipes on this night: {item['curr_recipes']}.",
        "",
    ]
    if item["added"]:
        lines.append(
            f"First published this night ({len(item['added'])}): "
            + ", ".join(f"`{name}`" for name in item["added"])
            + "."
        )
        lines.append("")
    if item["removed"]:
        lines.append(
            f"Missing this night ({len(item['removed'])}): "
            + ", ".join(f"`{name}`" for name in item["removed"])
            + "."
        )
        lines.append("")
    if not item["changes"]:
        sentence = (
            "Past buy and sell tickers stayed the same. "
            "Cumulative totals moved only where a new session was appended."
        )
        if item["moves"]:
            top = item["moves"][0]
            sentence += (
                f" Largest total move: `{top['recipe']}` "
                f"{_pct(top['total_before'])} to {_pct(top['total_after'])} "
                f"({_pp(top['delta'])})."
            )
        lines.append(sentence)
        lines.append("")
        return lines
    lines.append(
        f"Past days rewritten: {sum(len(row['days']) for row in item['changes'])} "
        f"across {len(item['changes'])} recipes."
    )
    lines += [
        "",
        "| Recipe | Past day | Buys before -> after | Sells before -> after | Total before | Total after | Change |",
        "|---|---|---|---|---:|---:|---:|",
    ]
    for row in item["changes"]:
        for index, day in enumerate(row["days"]):
            total_before = _pct(row["total_before"]) if index == 0 else ""
            total_after = _pct(row["total_after"]) if index == 0 else ""
            delta = _pp(row["delta"]) if index == 0 else ""
            lines.append(
                f"| `{row['recipe']}` | {day['date']} | {_cell(day['buys'])} | "
                f"{_cell(day['sells'])} | {total_before} | {total_after} | {delta} |"
            )
    lines.append("")
    return lines


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _dir_fingerprint(directory: Path) -> tuple:
    if not directory.is_dir():
        return ()
    rows = []
    for path in sorted(directory.iterdir()):
        if not path.is_file():
            continue
        stat = path.stat()
        rows.append((path.name, stat.st_size, stat.st_mtime_ns))
    return tuple(rows)


def _flat_from_csv(path: Path) -> dict:
    import csv

    out = {}
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            cell = (row.get("net_ret_15bp") or "").strip()
            out[(row["recipe"], row["start_date"], row["D"])] = (
                None if cell == "" else float(cell)
            )
    return out


def _write_ledger_bytes(directory: Path, date: str, ledger: dict) -> bytes:
    from . import factor_mine_freeze as fmf

    raw = fmf.encode_frozen("ledgers", ledger)
    directory.mkdir(parents=True, exist_ok=True)
    (directory / f"{date}.json.gz").write_bytes(raw)
    return raw


def rebuild_ledgers_once(dest: Path) -> dict[str, str]:
    """Build every session ledger from the locked snapshots into ``dest``.

    The frozen ledger directory, the freeze manifest, and the retro report
    stay where they are. ``write_report`` is not called.
    """
    from . import factor_mine_freeze as fmf
    from . import factor_mine_retro as retro

    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)
    saved = fmf.LEDGER_DIR
    fmf.LEDGER_DIR = dest
    hashes: dict[str, str] = {}
    try:
        recipes = retro.retro_recipes()
        sessions = list(retro.SESSIONS)
        for date in sessions:
            snap = fmf.read_json(fmf.snapshot_path(date)) or {}
            label = snap.get("label") or "pit_rebuilt"
            landed = [day for day in sessions if day <= date]
            panel = retro.assemble_panel(landed)
            bars = retro._bars_for(panel, date)
            if label in ("held", "skipped"):
                reason = snap.get("skip_reason") or snap.get("hold_reason") or label
                ledger = retro._held_ledger(date, reason)
            else:
                ledger = fmf.build_ledger(panel, {}, recipes, date, bars)
            ledger["label"] = label
            ledger["code_sha"] = ledger.get("code_sha") or fmf.code_sha()
            raw = _write_ledger_bytes(dest, date, ledger)
            hashes[date] = hashlib.sha256(raw).hexdigest()
            print(f"[changelog] rebuilt {date} sha={hashes[date][:12]}", flush=True)
    finally:
        fmf.LEDGER_DIR = saved
    return hashes


def _csv_bytes(ledger_dir: Path, dest: Path, flat: dict) -> bytes:
    from . import factor_mine_freeze as fmf
    from . import factor_mine_retro as retro

    saved = fmf.LEDGER_DIR
    fmf.LEDGER_DIR = ledger_dir
    try:
        retro.write_daily_returns(
            dest, dates=list(retro.SESSIONS), flat_returns=flat,
        )
    finally:
        fmf.LEDGER_DIR = saved
    return dest.read_bytes()


def double_rebuild(work: Path | None = None) -> dict:
    """Rebuild ledgers and the daily-returns CSV twice. Compare the bytes."""
    from . import factor_mine_freeze as fmf
    from . import factor_mine_retro as retro

    locked_csv = retro.DAILY_RETURNS_CSV
    report = retro.REPORT_MD
    before = {
        "ledgers": _dir_fingerprint(fmf.LEDGER_DIR),
        "manifest": _sha256(fmf.MANIFEST_PATH) if fmf.MANIFEST_PATH.is_file() else "",
        "csv": _sha256(locked_csv) if locked_csv.is_file() else "",
        "report": _sha256(report) if report.is_file() else "",
    }
    flat = _flat_from_csv(locked_csv) if locked_csv.is_file() else {}
    own = work is None
    work = Path(work or tempfile.mkdtemp(prefix="fm-changelog-det-"))
    try:
        first = work / "run1"
        second = work / "run2"
        hashes_a = rebuild_ledgers_once(first)
        hashes_b = rebuild_ledgers_once(second)
        mismatched = sorted(
            date for date in set(hashes_a) | set(hashes_b)
            if hashes_a.get(date) != hashes_b.get(date)
        )
        csv_a = _csv_bytes(first, work / "run1.csv", flat)
        csv_b = _csv_bytes(second, work / "run2.csv", flat)
        csv_equal = csv_a == csv_b
        ledger_equal = not mismatched and hashes_a.keys() == hashes_b.keys()
        status = "PASS" if ledger_equal and csv_equal else "FAIL"
        detail = (
            f"Two rebuilds of {len(hashes_a)} locked sessions. "
            f"Ledger bytes {('matched' if ledger_equal else 'differed')}. "
            f"daily_returns.csv bytes {('matched' if csv_equal else 'differed')} "
            f"({len(csv_a)} bytes, then {len(csv_b)} bytes)."
        )
        lines = [
            "Inputs were the locked daily snapshots and the retro price store. "
            "Flat 15bp cells were copied from the locked "
            "`data/factor_mine/daily_returns.csv` so the check is the "
            "Futubull ledger rebuild. `write_report` was not called. "
            "The frozen ledger files were left in place.",
        ]
        if mismatched:
            lines.append(
                "Ledger dates that differed: " + ", ".join(mismatched) + "."
            )
        after = {
            "ledgers": _dir_fingerprint(fmf.LEDGER_DIR),
            "manifest": _sha256(fmf.MANIFEST_PATH) if fmf.MANIFEST_PATH.is_file() else "",
            "csv": _sha256(locked_csv) if locked_csv.is_file() else "",
            "report": _sha256(report) if report.is_file() else "",
        }
        untouched = before == after
        if untouched:
            lines.append(
                "Frozen ledgers, the freeze manifest, the locked CSV, and "
                "`FACTOR_MINE_RETRO_PIT.md` were unchanged on disk."
            )
        else:
            status = "FAIL"
            detail += " A locked file on disk changed during the check."
        return {
            "status": status,
            "detail": detail,
            "lines": lines,
            "ledger_equal": ledger_equal,
            "csv_equal": csv_equal,
            "sessions": len(hashes_a),
            "mismatched": mismatched,
            "untouched": untouched,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "status": "FAIL",
            "detail": f"The double rebuild raised {type(exc).__name__}: {exc}",
            "lines": [],
            "ledger_equal": False,
            "csv_equal": False,
            "sessions": 0,
            "mismatched": [],
            "untouched": False,
        }
    finally:
        if own:
            shutil.rmtree(work, ignore_errors=True)


def write_page(path: Path | None = None, *,
               determinism: dict | None = None,
               repo: Path | None = None) -> dict:
    nights, diffs = build_diffs(repo)
    stats = summary_stats(nights, diffs)
    text = render_markdown(nights, diffs, determinism)
    dest = Path(path or OUT_MD)
    dest.write_text(text, encoding="utf-8")
    print(f"[changelog] wrote {dest} bytes={dest.stat().st_size}", flush=True)
    return stats


def main() -> None:
    skip = os.environ.get("FM_CHANGELOG_SKIP_DETERMINISM") == "1"
    determinism = None if skip else double_rebuild()
    if determinism:
        print(f"[changelog] determinism {determinism['status']}", flush=True)
    write_page(determinism=determinism)


if __name__ == "__main__":
    main()
