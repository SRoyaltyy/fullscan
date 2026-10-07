"""Visible certificate: past buy/sell records still match their first seal.

The badge is on the Factor Mine and h1 dashboards. The baseline is the
first seal in history, never whatever this run sees on disk. Stored seals
are not rewritten. A new day is an append in the seal that already owns it.

Fingerprint is buys and sells only: names, sides, sizes, and dates.
Equity marks, live prices, and sit-out notes are not part of the
fingerprint. A page turns red when any past sealed day differs.
"""
from __future__ import annotations

import csv
import hashlib
import json
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BEGIN = "<!-- RECORD_CERTIFICATE_BEGIN -->"
END = "<!-- RECORD_CERTIFICATE_END -->"
EXCEL_LOCK_FROM = "2026-10-06"
# Cyrus kept this pre-close draft lock. There is no final markdown for it.
EXCEL_LOCK_SHA = "9d910cb9de3933b888159d438e95925d105bd8d485b5aaeda60272ad65e44992"
EXCEL_LOCK_PICKS = 104

FACTOR_DIR = "03_scoreboard/factor_mine"
TICKET_DIR = "data/day_board"
EXCEL_DAILY = "excel_bot/daily"
SUGGESTIONS = "excel_bot/suggestions/suggestions.csv"
FREEZE_MANIFEST = "excel_bot/freeze_manifest.json"
H1_LOG = "research/hot_n4_clean_v4/forward_h1/h1_log.jsonl"
HOLDUP_LOG = "research/hot_n4_clean_v4/forward/holdup_log.jsonl"
WEBULL_BOOKS = "data/webull_sim/days.jsonl"
PAST_LOCK = "data/past_day_lock/manifest.jsonl"
WEBULL_FIRST = "2026-10-06"

PAPER_FAMILIES = frozenset({"paper", "stock_book", "flatten"})
FILL_RE = re.compile(
    r"^\| (\d{4}-\d{2}-\d{2})\b[^|]*\| \*\*(BUY|SELL|COVER|SHORT)\*\* "
    r"\| `([A-Za-z0-9.\-]+)` \| ([^|]*?)\s*\|",
    re.I,
)
ISO_DAY = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# Live badge. Other strategy pages stay unstamped until a later pass.
# factor_mine.py is engine-pinned, so the Factor Mine bake workflows
# stamp the page after write_dash_html. h1 stamps inside write_page.
BOARD_PAGES: dict[str, tuple[str, ...]] = {
    "factor_mine": ("dashboard/factor-mine/index.html",),
    "h1": ("dashboard/h1/index.html",),
}


@dataclass
class Certificate:
    board: str
    title: str
    status: str
    checked: int
    failed: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    baseline: str = ""

    fail_count: int = 0

    def add_fail(self, line: str) -> None:
        self.status = "fail"
        self.fail_count += 1
        if len(self.failed) < 12:
            self.failed.append(line)


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def norm_side(side: str) -> str:
    text = (side or "").replace("*", "").strip().lower()
    if text in ("buy", "long", "cover"):
        return "buy"
    if text in ("sell", "short"):
        return "sell"
    return text


def norm_size(size) -> str:
    if size is None:
        return ""
    text = str(size).strip().replace(",", "")
    if text in ("", "—", "-", "none", "null"):
        return ""
    try:
        num = float(text)
    except ValueError:
        return text
    if num.is_integer():
        return str(int(num))
    return format(num, "f").rstrip("0").rstrip(".")


def leg(date: str, side: str, name: str, size="", book: str = "") -> str:
    ticker = (name or "").strip().upper()
    return "|".join((
        str(date)[:10],
        norm_side(side),
        ticker,
        norm_size(size),
        book or "",
    ))


def _git(root: Path, args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args], cwd=root, check=False, capture_output=True, text=True,
    )


def git_show(root: Path, sha: str, rel: str) -> str | None:
    proc = _git(root, ["show", f"{sha}:{rel}"])
    if proc.returncode != 0:
        return None
    return proc.stdout


def commit_list(root: Path, rel: str, *, reverse: bool = False) -> list[str]:
    args = ["log", "--format=%H"]
    if reverse:
        args.append("--reverse")
    args.extend(["--", rel])
    proc = _git(root, args)
    if proc.returncode != 0:
        return []
    return [line for line in proc.stdout.splitlines() if line]


def first_commit_text(root: Path, rel: str) -> tuple[str, str] | None:
    """Oldest commit that contains ``rel``. Not the working tree."""
    shas = commit_list(root, rel, reverse=True)
    for sha in shas:
        text = git_show(root, sha, rel)
        if text is not None:
            return sha, text
    return None


def blob_with_sha256(root: Path, rel: str, digest: str) -> tuple[str, str] | None:
    """Historical blob whose bytes hash to ``digest``. Working tree is last."""
    for sha in commit_list(root, rel, reverse=True):
        text = git_show(root, sha, rel)
        if text is not None and sha256_text(text) == digest:
            return sha, text
    return None


def _signal_freeze():
    engine = str(ROOT / "excel_bot" / "engine")
    if engine not in sys.path:
        sys.path.insert(0, engine)
    import signal_freeze
    return signal_freeze


def scoreboard_fill_legs(text: str) -> dict[str, tuple[str, ...]]:
    """Buy/sell lines only. Prices, fees, and equity marks are left out."""
    found: dict[str, list[str]] = {}
    for line in text.splitlines():
        match = FILL_RE.match(line)
        if not match:
            continue
        date, side, ticker, shares = match.groups()
        found.setdefault(date, []).append(leg(date, side, ticker, shares))
    return {date: tuple(sorted(rows)) for date, rows in found.items()}


def _both_equities(row: dict) -> bool:
    return row.get("eq_0930") is not None and row.get("eq_close") is not None


def factor_mine_prime_legs(root: Path) -> dict[tuple[str, str], tuple[str, tuple[str, ...]]]:
    """First commit that printed both equities. Later text does not replace it."""
    from .honest_scorecard import _Git, _diff_blobs, parse_scoreboard

    proc = _git(root, ["rev-list", "--reverse", "HEAD", "--", FACTOR_DIR])
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or "factor mine history unreadable")
    commits = [line for line in proc.stdout.splitlines() if line]
    out: dict[tuple[str, str], tuple[str, tuple[str, ...]]] = {}
    git = _Git(root)
    try:
        for commit in commits:
            for sha, path in _diff_blobs(root, commit, FACTOR_DIR):
                name = Path(path).stem
                raw = git.blob(sha)
                if raw is None:
                    continue
                text = raw.decode("utf-8", "replace")
                parsed = parse_scoreboard(text)
                fills = scoreboard_fill_legs(text)
                for date, row in parsed.items():
                    key = (name, date)
                    if key in out or not _both_equities(row):
                        continue
                    out[key] = (commit, fills.get(date, ()))
    finally:
        git.close()
    return out


def certify_factor_mine(root: Path) -> Certificate:
    cert = Certificate(
        board="factor_mine",
        title="Factor Mine",
        status="pass",
        checked=0,
        baseline=(
            "Prime first-print commit: the first commit that printed that "
            "day's close equity and that morning's 09:30 equity. Buys and "
            "sells are taken from that commit. Later equity marks are ignored."
        ),
    )
    prime = factor_mine_prime_legs(root)
    folder = root / FACTOR_DIR
    current: dict[tuple[str, str], tuple[str, ...]] = {}
    if folder.is_dir():
        for path in sorted(folder.glob("*.md")):
            text = path.read_text(encoding="utf-8")
            fills = scoreboard_fill_legs(text)
            parsed = {}
            from .honest_scorecard import parse_scoreboard
            parsed = parse_scoreboard(text)
            for date, row in parsed.items():
                if _both_equities(row):
                    current[(path.stem, date)] = fills.get(date, ())
    for key, (commit, legs) in sorted(prime.items()):
        cert.checked += 1
        got = current.get(key)
        name, date = key
        if got is None:
            cert.add_fail(f"{name} {date} removed after prime {commit[:10]}")
            continue
        if got != legs:
            cert.add_fail(
                f"{name} {date} buys/sells differ from prime {commit[:10]}"
            )
    fresh = [key for key in current if key not in prime]
    if fresh:
        cert.notes.append(
            f"{len(fresh)} day(s) have both equities and are not in a "
            "committed prime print yet. They are not a sealed baseline."
        )
    if cert.checked == 0 and cert.status == "pass":
        cert.notes.append("No prime first-print day is in this tree.")
    return cert


def _excel_suggestion_legs(text: str, date: str) -> tuple[str, ...]:
    rows: list[str] = []
    started = False
    for line in text.splitlines():
        if line.startswith("| ticker |"):
            started = True
            continue
        if not started:
            continue
        if not line.startswith("|"):
            break
        if set(line.replace("|", "").strip()) <= {"-", " "}:
            continue
        cols = [part.strip() for part in line.strip().strip("|").split("|")]
        if len(cols) < 3:
            continue
        ticker, side, strategy = cols[0], cols[1], cols[2]
        # ref close, colors, and the live scoreboard below are not hashed.
        rows.append(leg(date, side, ticker, "", strategy))
    return tuple(sorted(rows))


def _load_suggestions(root: Path) -> list[dict]:
    path = root / SUGGESTIONS
    if not path.is_file():
        return []
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def certify_excel(root: Path) -> Certificate:
    cert = Certificate(
        board="excel",
        title="excel_bot",
        status="pass",
        checked=0,
        baseline=(
            f"From {EXCEL_LOCK_FROM} on, the freeze-manifest lock entry "
            "(sha256 of locked fields, written once). Before that, the "
            "first commit of excel_bot/daily/<date>_excel_bot.md. "
            f"{EXCEL_LOCK_FROM} has no final markdown; the manifest entry "
            "is the baseline."
        ),
    )
    sf = _signal_freeze()
    manifest_path = root / FREEZE_MANIFEST
    try:
        if manifest_path.is_file():
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        else:
            manifest = None
    except json.JSONDecodeError as exc:
        cert.add_fail(f"freeze manifest is not json ({exc})")
        return cert
    suggestions = _load_suggestions(root)
    grouped = sf.canonical_picks(suggestions) if suggestions else {}
    locks: dict[str, dict] = {}
    if manifest is None:
        cert.add_fail("freeze manifest is missing. Refusing to invent a lock.")
    else:
        for entry in manifest.get("entries") or []:
            day = str(entry.get("signal_date") or "")
            if entry.get("kind") == "lock" and day not in locks:
                locks[day] = entry
        for day, entry in sorted(locks.items()):
            cert.checked += 1
            picks = grouped.get(day) or []
            digest = sf.fingerprint(picks) if picks else ""
            if digest != entry.get("sha256") or len(picks) != entry.get("n_picks"):
                cert.add_fail(
                    f"{day} locked fields differ from freeze manifest "
                    f"{entry.get('sha256')} ({entry.get('n_picks')} picks)"
                )
        real = manifest_path.resolve() == (ROOT / FREEZE_MANIFEST).resolve()
        if real and EXCEL_LOCK_FROM not in locks:
            cert.add_fail(f"{EXCEL_LOCK_FROM} freeze-manifest lock is missing")
        elif real and (
            locks[EXCEL_LOCK_FROM].get("sha256") != EXCEL_LOCK_SHA
            or locks[EXCEL_LOCK_FROM].get("n_picks") != EXCEL_LOCK_PICKS
        ):
            cert.add_fail(
                f"{EXCEL_LOCK_FROM} manifest is not the kept draft lock "
                f"{EXCEL_LOCK_SHA} ({EXCEL_LOCK_PICKS} picks)"
            )
    final_md = root / EXCEL_DAILY / f"{EXCEL_LOCK_FROM}_excel_bot.md"
    if final_md.exists():
        cert.add_fail(
            f"{EXCEL_LOCK_FROM}_excel_bot.md exists. The baseline is the "
            "freeze-manifest entry, not an invented final."
        )
    folder = root / EXCEL_DAILY
    if folder.is_dir():
        for path in sorted(folder.glob("*_excel_bot.md")):
            if path.name.endswith("_draft.md"):
                continue
            day = path.name[:10]
            if not ISO_DAY.match(day) or day >= EXCEL_LOCK_FROM:
                continue
            cert.checked += 1
            first = first_commit_text(root, path.relative_to(root).as_posix())
            if first is None:
                cert.add_fail(f"{day} excel markdown has no committed baseline")
                continue
            sha, old = first
            old_legs = _excel_suggestion_legs(old, day)
            new_legs = _excel_suggestion_legs(
                path.read_text(encoding="utf-8"), day,
            )
            if old_legs != new_legs:
                cert.add_fail(
                    f"{day} suggestion names differ from first commit {sha[:10]}"
                )
    if "2026-10-07" not in locks and "2026-10-07" not in grouped:
        cert.notes.append(
            "2026-10-07 has no sealed buy/sell. A sit-out is a note, "
            "not a buy/sell change."
        )
    return cert


def _plan_legs(row: dict) -> tuple[str, ...]:
    date = str(row.get("date") or "")[:10]
    rows = []
    for pick in row.get("picks") or []:
        if isinstance(pick, dict) and pick.get("ticker"):
            rows.append(leg(
                date, "buy", pick["ticker"], pick.get("shares") or "",
            ))
    for sell in row.get("planned_sells") or []:
        if isinstance(sell, dict) and sell.get("ticker"):
            rows.append(leg(date, "sell", sell["ticker"], sell.get("shares") or ""))
    return tuple(sorted(rows))


def _plans_in_jsonl(text: str) -> dict[str, tuple[str, ...]]:
    found: dict[str, tuple[str, ...]] = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if row.get("kind") != "plan":
            continue
        date = str(row.get("date") or "")[:10]
        if ISO_DAY.match(date) and date not in found:
            found[date] = _plan_legs(row)
    return found


def _plans_in_log_json(text: str) -> dict[str, tuple[str, ...]]:
    try:
        rows = json.loads(text)
    except json.JSONDecodeError:
        return {}
    if not isinstance(rows, list):
        return {}
    found: dict[str, tuple[str, ...]] = {}
    for row in rows:
        if isinstance(row, dict) and row.get("kind") == "plan":
            date = str(row.get("date") or "")[:10]
            if ISO_DAY.match(date) and date not in found:
                found[date] = _plan_legs(row)
    return found


def _certify_sealed_log(root: Path, *, board: str, title: str,
                        rel: str, page_log: str) -> Certificate:
    cert = Certificate(
        board=board,
        title=title,
        status="pass",
        checked=0,
        baseline=(
            "First sealed plan for that day: the first commit of the "
            "append-only log that contains kind=plan. Names, sides, and "
            "sell sizes are the fingerprint. Later marks are not."
        ),
    )
    path = root / rel
    if not path.is_file():
        cert.add_fail(f"sealed log missing at {rel}")
        return cert
    current = _plans_in_jsonl(path.read_text(encoding="utf-8"))
    prime: dict[str, tuple[str, tuple[str, ...]]] = {}
    for sha in commit_list(root, rel, reverse=True):
        text = git_show(root, sha, rel)
        if text is None:
            continue
        for date, legs in _plans_in_jsonl(text).items():
            prime.setdefault(date, (sha, legs))
    for date, (sha, legs) in sorted(prime.items()):
        cert.checked += 1
        got = current.get(date)
        if got is None:
            cert.add_fail(f"{date} plan removed after {sha[:10]}")
        elif got != legs:
            cert.add_fail(f"{date} plan differs from first seal {sha[:10]}")
    page = root / page_log
    if page.is_file():
        shown = _plans_in_log_json(page.read_text(encoding="utf-8"))
        for date, (sha, legs) in sorted(prime.items()):
            if shown.get(date) != legs:
                cert.add_fail(
                    f"{date} page log.json differs from first seal {sha[:10]}"
                )
    else:
        cert.add_fail(f"dashboard log missing at {page_log}")
    return cert


def certify_h1(root: Path) -> Certificate:
    return _certify_sealed_log(
        root, board="h1", title="h1",
        rel=H1_LOG, page_log="dashboard/h1/log.json",
    )


def certify_holdup(root: Path) -> Certificate:
    return _certify_sealed_log(
        root, board="holdup", title="holdup",
        rel=HOLDUP_LOG, page_log="dashboard/holdup/log.json",
    )


def _ticket_legs(doc: dict, families: frozenset[str] | None) -> tuple[str, ...]:
    date = str(doc.get("date") or "")[:10]
    rows = []
    for name, rec in (doc.get("strategies") or {}).items():
        if not isinstance(rec, dict):
            continue
        family = str(rec.get("family") or "")
        if families is not None and family not in families:
            continue
        for key in ("buy", "sell"):
            for item in rec.get(key) or []:
                if isinstance(item, str):
                    rows.append(leg(date, key, item, "", name))
                    continue
                if not isinstance(item, dict) or not item.get("ticker"):
                    continue
                side = item.get("side") or ("buy" if key == "buy" else "sell")
                rows.append(leg(
                    date, side, item["ticker"], item.get("shares") or "", name,
                ))
    return tuple(sorted(rows))


def _ticket_sha_index(root: Path) -> dict[str, str]:
    path = root / PAST_LOCK
    found: dict[str, str] = {}
    if not path.is_file():
        return found
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line:
            continue
        row = json.loads(line)
        if row.get("kind") == "day" and row.get("record") == "strategy_tickets":
            found[str(row.get("date") or "")] = str(row.get("sha256") or "")
    return found


def _commit_instants(root: Path, rel: str) -> list[tuple[str, str]]:
    """``(sha, committer ISO time)``, newest first."""
    proc = _git(root, ["log", "--format=%H%x09%cI", "--", rel])
    if proc.returncode != 0:
        return []
    found = []
    for line in proc.stdout.splitlines():
        if "\t" not in line:
            continue
        sha, iso = line.split("\t", 1)
        found.append((sha, iso))
    return found


def _send_time_ticket_text(root: Path, rel: str, day: str) -> tuple[str, str, bool] | None:
    """Body the session open seals.

    The last commit strictly before that day's 09:30 ET is the send-time
    plan. Earlier same-morning drafts are not the seal. When every commit
    is after the open, the oldest commit is the only historical record.
    The third value is True when a pre-open commit was used.
    """
    from datetime import datetime
    from zoneinfo import ZoneInfo

    cutoff = datetime.fromisoformat(f"{day}T09:30:00").replace(
        tzinfo=ZoneInfo("America/New_York"),
    )
    newest_before: tuple[datetime, str, str] | None = None
    oldest: tuple[datetime, str, str] | None = None
    for sha, iso in _commit_instants(root, rel):
        when = datetime.fromisoformat(iso).astimezone(cutoff.tzinfo)
        text = git_show(root, sha, rel)
        if text is None:
            continue
        if oldest is None or when < oldest[0]:
            oldest = (when, sha, text)
        if when < cutoff and (newest_before is None or when > newest_before[0]):
            newest_before = (when, sha, text)
    chosen = newest_before or oldest
    if chosen is None:
        return None
    return chosen[1], chosen[2], newest_before is not None


def _sealed_ticket_text(root: Path, rel: str, day: str,
                        locked_sha: str) -> tuple[str, str, bool] | None:
    """Manifest lock when this day is sealed, else the send-time plan.

    The third value is True when the baseline is a real seal (manifest
    lock or a commit before 09:30 ET).
    """
    if locked_sha:
        found = blob_with_sha256(root, rel, locked_sha)
        if found is not None:
            return found[0], found[1], True
        current = (root / rel).read_text(encoding="utf-8")
        if sha256_text(current) == locked_sha:
            return "working-tree-matches-lock", current, True
        return None
    return _send_time_ticket_text(root, rel, day)


def _certify_tickets(root: Path, *, board: str, title: str,
                     families: frozenset[str] | None,
                     baseline: str) -> Certificate:
    cert = Certificate(
        board=board, title=title, status="pass", checked=0, baseline=baseline,
    )
    locks = _ticket_sha_index(root)
    folder = root / TICKET_DIR
    if not folder.is_dir():
        cert.add_fail("strategy ticket directory is missing")
        return cert
    for path in sorted(folder.glob("*_strategy_tickets.json")):
        day = path.name[:10]
        if not ISO_DAY.match(day):
            continue
        rel = path.relative_to(root).as_posix()
        cert.checked += 1
        sealed = _sealed_ticket_text(root, rel, day, locks.get(day, ""))
        if sealed is None:
            cert.add_fail(f"{day} sealed ticket bytes are not in git history")
            continue
        sha, old, preopen = sealed
        if not preopen:
            cert.notes.append(
                f"{day} has no commit before 09:30 ET. The first commit "
                "is the only historical record."
            )
        try:
            old_doc = json.loads(old)
            new_doc = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            cert.add_fail(f"{day} ticket file is not json")
            continue
        if _ticket_legs(old_doc, families) != _ticket_legs(new_doc, families):
            cert.add_fail(
                f"{day} buy/sell names differ from sealed plan {sha[:10]}"
            )
    return cert


def certify_paper(root: Path) -> Certificate:
    return _certify_tickets(
        root, board="paper", title="Paper sleeves / stock book",
        families=PAPER_FAMILIES,
        baseline=(
            "First sealed plan: the past-day-lock bytes when that ticket "
            "day is sealed, otherwise the last commit of the ticket file "
            "before that session's 09:30 ET. Earlier morning drafts are "
            "not the seal. Paper, stock-book, and flatten families. "
            "Prices are not hashed."
        ),
    )


def certify_tickets(root: Path) -> Certificate:
    return _certify_tickets(
        root, board="tickets", title="Strategy tickets",
        families=None,
        baseline=(
            "First sealed plan for every family on the dated ticket file: "
            "the past-day-lock bytes, or the last commit before that "
            "session's 09:30 ET. A manifest lock wins over an earlier draft."
        ),
    )


def _sim_legs(row: dict) -> tuple[str, ...]:
    """Names and sides on the sim row. Sim share counts are sizing, not the seal."""
    date = str(row.get("date") or "")[:10]
    rows = []
    for fill in row.get("fills") or []:
        ticker = str(fill.get("ticker") or "")
        if not ticker:
            continue
        rows.append(leg(date, str(fill.get("side") or ""), ticker, ""))
    return tuple(sorted(rows))


def _next_weekday(day: str) -> str:
    from datetime import date, timedelta
    cursor = date.fromisoformat(day) + timedelta(days=1)
    while cursor.weekday() >= 5:
        cursor += timedelta(days=1)
    return cursor.isoformat()


def _excel_entry_baselines(root: Path) -> dict[tuple[str, str], tuple[str, ...]]:
    """strategy, entry-date -> legs from the first commit of that final md."""
    out: dict[tuple[str, str], tuple[str, ...]] = {}
    folder = root / EXCEL_DAILY
    if not folder.is_dir():
        return out
    for path in sorted(folder.glob("*_excel_bot.md")):
        if "draft" in path.name:
            continue
        file_day = path.name[:10]
        if not ISO_DAY.match(file_day) or file_day >= EXCEL_LOCK_FROM:
            continue
        entry = _next_weekday(file_day)
        first = first_commit_text(root, path.relative_to(root).as_posix())
        if first is None:
            continue
        _sha, text = first
        for line_leg in _excel_suggestion_legs(text, entry):
            _date, _side, _ticker, _size, strategy = line_leg.split("|")
            key = (strategy, entry)
            out.setdefault(key, [])
            out[key].append(leg(entry, _side, _ticker, ""))
    return {key: tuple(sorted(rows)) for key, rows in out.items()}


def _freeze_pick_legs(root: Path) -> dict[tuple[str, str], tuple[str, ...]]:
    """signal_date strategy -> names from the lock entry's pick_ids."""
    path = root / FREEZE_MANIFEST
    if not path.is_file():
        return {}
    manifest = json.loads(path.read_text(encoding="utf-8"))
    suggestions = _load_suggestions(root)
    sf = _signal_freeze()
    grouped = sf.canonical_picks(suggestions)
    out: dict[tuple[str, str], list[str]] = {}
    seen = set()
    for entry in manifest.get("entries") or []:
        day = str(entry.get("signal_date") or "")
        if entry.get("kind") != "lock" or day in seen:
            continue
        seen.add(day)
        picks = grouped.get(day) or []
        if sf.fingerprint(picks) != entry.get("sha256"):
            continue
        for pick in picks:
            strategy = pick.get("strategy") or ""
            out.setdefault((strategy, day), []).append(
                leg(day, pick.get("side") or "buy", pick.get("ticker") or "", "")
            )
    return {key: tuple(sorted(rows)) for key, rows in out.items()}


def certify_webull(root: Path, *, theme_plans=None) -> Certificate:
    """Theme Radar shorts use the theme-radar plan's first commit.

    ``theme_plans(day) -> dict[book, tuple[leg, ...]] | None``. None means
    that day's plan history could not be read. Nothing is copied into
    fullscan.
    """
    cert = Certificate(
        board="webull",
        title="Webull sim",
        status="pass",
        checked=0,
        baseline=(
            "Theme Radar shorts: first commit of "
            "research/shadow_log/plans/plan_<date>.csv in SRoyaltyy/theme-radar. "
            "Other locked books: the strategy's first sealed plan "
            "(h1 log, excel final or freeze manifest, strategy ticket). "
            "Sim share counts and equity marks are not the fingerprint. "
            f"Rows before {WEBULL_FIRST} are built after the fact."
        ),
    )
    path = root / WEBULL_BOOKS
    if not path.is_file():
        cert.add_fail("webull sim days.jsonl is missing")
        return cert
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if str(row.get("date") or "") >= WEBULL_FIRST:
            rows.append(row)
    h1 = {}
    h1_path = root / H1_LOG
    if h1_path.is_file():
        prime: dict[str, tuple[str, ...]] = {}
        for sha in commit_list(root, H1_LOG, reverse=True):
            text = git_show(root, sha, H1_LOG)
            if text is None:
                continue
            for date, legs in _plans_in_jsonl(text).items():
                prime.setdefault(date, legs)
        h1 = prime
    tickets: dict[tuple[str, str], tuple[str, ...]] = {}
    locks = _ticket_sha_index(root)
    folder = root / TICKET_DIR
    if folder.is_dir():
        for ticket in sorted(folder.glob("*_strategy_tickets.json")):
            day = ticket.name[:10]
            if not ISO_DAY.match(day):
                continue
            rel = ticket.relative_to(root).as_posix()
            sealed = _sealed_ticket_text(root, rel, day, locks.get(day, ""))
            if sealed is None:
                continue
            _sha, text, _preopen = sealed
            doc = json.loads(text)
            for name, rec in (doc.get("strategies") or {}).items():
                if not isinstance(rec, dict) or str(name).startswith("excel_"):
                    continue
                # Entry picks only. Shorts the sim treats as later exits stay
                # on the ticket certificate, not on this entry row.
                buys = []
                for item in rec.get("buy") or []:
                    if isinstance(item, dict) and item.get("ticker"):
                        buys.append(leg(
                            day, item.get("side") or "buy", item["ticker"], "",
                        ))
                tickets[(str(name), day)] = tuple(sorted(buys))
    excel_entries = _excel_entry_baselines(root)
    freeze_picks = _freeze_pick_legs(root)
    sit_outs = 0
    unread = 0
    if theme_plans is None:
        theme_plans = _theme_plan_legs
    theme_cache: dict[str, dict[str, tuple[str, ...]] | None] = {}
    for row in rows:
        date = str(row.get("date") or "")
        name = str(row.get("name") or "")
        current = _sim_legs(row)
        reason = str(row.get("reason") or "")
        if name.startswith("theme_radar_"):
            if date not in theme_cache:
                theme_cache[date] = theme_plans(root, date)
            loaded = theme_cache[date]
            if loaded is None:
                unread += 1
                cert.status = "incomplete" if cert.status == "pass" else cert.status
                continue
            baseline = loaded.get(name, ())
        elif name == "h1_webull_sim":
            baseline = tuple(
                item for item in h1.get(date, ()) if item.split("|")[1] == "buy"
            )
            baseline = tuple(
                leg(date, "buy", item.split("|")[2], "") for item in baseline
            )
        else:
            strategy = name[:-len("_webull_sim")] if name.endswith("_webull_sim") else name
            if (strategy, date) in excel_entries:
                baseline = excel_entries[(strategy, date)]
            elif (strategy, date) in freeze_picks:
                baseline = freeze_picks[(strategy, date)]
            else:
                baseline = tickets.get((strategy, date), ())
        cert.checked += 1
        if not current and not baseline:
            sit_outs += 1
            continue
        if not current and ("sat out" in reason or reason == "no pre-09:30 plan"):
            if not baseline:
                sit_outs += 1
                continue
        if current != tuple(sorted(baseline)):
            cert.add_fail(f"{name} {date} buys/sells differ from the first seal")
    if sit_outs:
        cert.notes.append(
            f"{sit_outs} past row(s) sat out with no buy/sell on either side. "
            "That note is not a change."
        )
    if unread:
        cert.notes.append(
            f"{unread} Theme Radar row(s) were not certified: the plan's "
            "first commit in SRoyaltyy/theme-radar could not be read. "
            "No second baseline was written here."
        )
    built_after = 0
    # Count is informational. Those rows are not compared.
    if path.is_file():
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if str(row.get("date") or "") < WEBULL_FIRST:
                built_after += 1
    if built_after:
        cert.notes.append(
            f"{built_after} row(s) before {WEBULL_FIRST} stay in the "
            "built-after section and are not this seal."
        )
    return cert


def _theme_plan_legs(root: Path, day: str) -> dict[str, tuple[str, ...]] | None:
    """First-commit plan legs. None when theme-radar history cannot be read."""
    del root  # the plan lives in theme-radar, not in this working tree
    from .webull_sim import book_name, plans_from_plan_file

    path = f"research/shadow_log/plans/plan_{day}.csv"
    built = plans_from_plan_file(path, day)
    if built is None:
        return None
    out: dict[str, tuple[str, ...]] = {}
    for book, plan in built.items():
        rows = []
        for pick in plan.get("picks") or []:
            ticker = getattr(pick, "ticker", "") or ""
            side = getattr(pick, "side", "") or "short"
            if ticker:
                rows.append(leg(day, side, ticker, ""))
        out[book if book.startswith("theme_radar_") else book_name(book)] = tuple(
            sorted(rows)
        )
    return out


def certify_board(root: Path) -> Certificate:
    return _rollup([
        certify_factor_mine(root),
        certify_excel(root),
        certify_h1(root),
        certify_holdup(root),
        certify_paper(root),
        certify_webull(root),
        certify_tickets(root),
    ])


def certify(board: str, root: Path | None = None) -> Certificate:
    root = Path(root or ROOT)
    fn = {
        "factor_mine": certify_factor_mine,
        "excel": certify_excel,
        "h1": certify_h1,
        "holdup": certify_holdup,
        "paper": certify_paper,
        "webull": certify_webull,
        "tickets": certify_tickets,
        "board": certify_board,
    }.get(board)
    if fn is None:
        raise KeyError(board)
    return fn(root)


def render_block(cert: Certificate) -> str:
    label = {"pass": "PASS", "fail": "FAIL", "incomplete": "NOT CERTIFIED"}.get(
        cert.status, cert.status.upper(),
    )
    fails = "".join(f"<li>{_esc(line)}</li>" for line in cert.failed)
    notes = "".join(f"<li>{_esc(line)}</li>" for line in cert.notes[:6])
    fail_html = f"<ul>{fails}</ul>" if fails else ""
    extra = cert.fail_count - len(cert.failed)
    if extra > 0:
        fail_html += f"<p>{extra} more past day(s) also differ.</p>"
    note_html = f"<ul>{notes}</ul>" if notes else ""
    return (
        f'<section id="record-certificate" data-status="{cert.status}" '
        f'data-board="{_esc(cert.board)}" role="status">'
        f"<strong>PAST BUY/SELL CERTIFICATE — {label}</strong>"
        f"<p>{_esc(cert.title)} · {cert.checked} sealed day check(s). "
        f"{_esc(cert.baseline)}</p>"
        f"{fail_html}{note_html}"
        "<style>"
        "#record-certificate{margin:0 0 14px;padding:10px 12px;border-radius:10px;"
        "font:14px/1.4 system-ui,sans-serif}"
        "#record-certificate[data-status=pass]{background:#052e16;color:#bbf7d0;"
        "border:2px solid #4ade80}"
        "#record-certificate[data-status=fail]{background:#450a0a;color:#fecaca;"
        "border:3px solid #f87171}"
        "#record-certificate[data-status=incomplete]{background:#451a03;color:#fde68a;"
        "border:2px solid #fbbf24}"
        "#record-certificate ul{margin:6px 0 0;padding-left:18px}"
        "html[data-record-certificate=fail],html[data-record-certificate=fail] body{"
        "background:#7f1d1d !important;background-color:#7f1d1d !important}"
        "</style>"
        "<script>(function(){var s=document.documentElement.getAttribute("
        "'data-record-certificate');if(s!=='fail')return;"
        "document.documentElement.style.background='#7f1d1d';"
        "if(document.body){document.body.style.background='#7f1d1d';"
        "document.body.style.color='#fff';}})();</script>"
        "</section>"
    )


def _esc(text: str) -> str:
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _set_status(html: str, status: str) -> str:
    match = re.search(r"<html\b[^>]*>", html, flags=re.I)
    if not match:
        return f'<html data-record-certificate="{status}">' + html
    tag = re.sub(r'\sdata-record-certificate="[^"]*"', "", match.group(0))
    tag = tag[:-1] + f' data-record-certificate="{status}">'
    return html[: match.start()] + tag + html[match.end() :]


def stamp_html(html: str, cert: Certificate) -> str:
    block = BEGIN + "\n" + render_block(cert) + "\n" + END
    if BEGIN in html and END in html:
        pre, rest = html.split(BEGIN, 1)
        _old, post = rest.split(END, 1)
        html = pre + block + post
    else:
        match = re.search(r"<body[^>]*>", html, flags=re.I)
        if match:
            html = html[: match.end()] + "\n" + block + html[match.end() :]
        else:
            html = block + "\n" + html
    return _set_status(html, cert.status)


def stamp_dashboard(path: Path, board: str, *, root: Path | None = None) -> Certificate | None:
    """Rewrite one dashboard file in place. Does not write book data."""
    root = Path(root or ROOT)
    path = Path(path)
    try:
        path.resolve().relative_to(root.resolve())
    except ValueError:
        return None
    if not path.is_file():
        return None
    try:
        cert = certify(board, root)
    except Exception as exc:
        cert = Certificate(
            board=board,
            title=board,
            status="fail",
            checked=0,
            failed=[f"certificate check failed closed: {exc}"],
            baseline="The check could not be finished, so the page fails closed.",
        )
    path.write_text(stamp_html(path.read_text(encoding="utf-8"), cert), encoding="utf-8")
    return cert


def _rollup(parts: list[Certificate]) -> Certificate:
    status = "pass"
    if any(part.status == "fail" for part in parts):
        status = "fail"
    elif any(part.status == "incomplete" for part in parts):
        status = "incomplete"
    failed: list[str] = []
    notes: list[str] = []
    checked = 0
    fail_count = 0
    for part in parts:
        checked += part.checked
        fail_count += part.fail_count
        for line in part.failed:
            if len(failed) < 12:
                failed.append(f"{part.title}: {line}")
        if part.status != "pass":
            notes.append(f"{part.title}: {part.status}")
    return Certificate(
        board="board",
        title="Strategy board",
        status=status,
        checked=checked,
        failed=failed,
        fail_count=fail_count,
        notes=notes,
        baseline=(
            "Rollup of Factor Mine, excel_bot, h1, holdup, paper sleeves, "
            "Webull sim, and strategy tickets. Any past-day difference "
            "fails this page."
        ),
    )


def stamp_all(root: Path | None = None) -> list[Certificate]:
    """Stamp Factor Mine and h1 only. Does not write book data."""
    root = Path(root or ROOT)
    out = []
    for board, pages in BOARD_PAGES.items():
        cert = certify(board, root)
        out.append(cert)
        _write_pages(root, pages, cert)
    return out


def _write_pages(root: Path, pages: tuple[str, ...], cert: Certificate) -> None:
    for rel in pages:
        path = root / rel
        if path.is_file():
            path.write_text(
                stamp_html(path.read_text(encoding="utf-8"), cert),
                encoding="utf-8",
            )
    print(
        f"[certificate] {cert.board} {cert.status} checked={cert.checked} "
        f"fails={len(cert.failed)}",
        flush=True,
    )


def main(argv: list[str] | None = None) -> int:
    import argparse
    parser = argparse.ArgumentParser(description="Past buy/sell certificate")
    parser.add_argument("--stamp", action="store_true")
    parser.add_argument("--board", default="")
    args = parser.parse_args(argv)
    if args.stamp:
        if args.board:
            cert = certify(args.board)
            for rel in BOARD_PAGES.get(args.board, ()):
                path = ROOT / rel
                if path.is_file():
                    path.write_text(
                        stamp_html(path.read_text(encoding="utf-8"), cert),
                        encoding="utf-8",
                    )
            print(f"[certificate] {args.board} {cert.status}")
        else:
            stamp_all()
        return 0
    board = args.board or "board"
    cert = certify(board)
    print(f"{cert.title}: {cert.status} ({cert.checked} checks)")
    for line in cert.failed:
        print(f"  FAIL {line}")
    for line in cert.notes:
        print(f"  note {line}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
