"""Honest scorecard. Reporting only. No buy, sell, or sizing change.

Day percent is (close equity − that morning's 09:30 equity) / 09:30 equity.
The overnight gap is not a day's result. Factor Mine days before the lock
use the prime first print: the first git commit that showed both equities
for that recipe and day. Later scoreboard rewrites are ignored.

A win needs more than 55% winning closed trades and at least 30 of them.
The dollar result after fees is always written next to the label.
"""
from __future__ import annotations

import csv
import datetime as dt
import html
import json
import random
import re
import subprocess
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
ET = ZoneInfo("America/New_York")
SCOREBOARD = ROOT / "03_scoreboard" / "HONEST_SCORECARD.md"
DASHBOARD = ROOT / "dashboard" / "honest-scorecard" / "index.html"
FACTOR_DIR = ROOT / "03_scoreboard" / "factor_mine"
H1_LOG = ROOT / "research" / "hot_n4_clean_v4" / "forward_h1" / "h1_log.jsonl"
HOLDUP_LOG = ROOT / "research" / "hot_n4_clean_v4" / "forward" / "holdup_log.jsonl"
FLATTEN_DIR = ROOT / "01_daily"
TICKET_DIR = ROOT / "data" / "day_board"
CANDIDATE_DIR = ROOT / "data" / "factor_mine" / "candidates"
SLEEVE_TRADES = ROOT / "data" / "sleeve_merge" / "trades.csv"
PAPER_TRADES = ROOT / "data" / "paper" / "trades.csv"
LOCK_MANIFEST = ROOT / "data" / "past_day_lock" / "manifest.jsonl"
FEES_PATH = ROOT / "00_grounding" / "futubull_fees.json"

WIN_TRADES = 30
WIN_RATE = 0.55
RANDOM4_N = 4
RANDOM4_DRAWS = 1000
RANDOM4_SEED = 20260813

ISO_DAY = re.compile(r"^(\d{4}-\d{2}-\d{2})$")
DAY_PREFIX = re.compile(r"^(\d{4}-\d{2}-\d{2})\b")
MONEY_RE = re.compile(r"-?\$?-?[0-9][0-9,]*\.\d+")
OPEN_EQ_RE = re.compile(r"09:30 equity \$([0-9,]+\.\d+)")
CLOSE_EQ_RE = re.compile(
    r"close \$([0-9,]+\.\d+) vs 09:30 \$([0-9,]+\.\d+)", re.I
)
CARD_EQ_RE = re.compile(
    r"09:30 \*\*\$([0-9,]+\.\d+)\*\*.*?16:00 \*\*\$([0-9,]+\.\d+)\*\*"
)


def money(text: str) -> float | None:
    raw = (text or "").strip().replace(",", "").replace("$", "")
    if raw in ("", "—", "-", "n/a", "na"):
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def day_percent(eq_0930: float | None, eq_close: float | None) -> float | None:
    """Session result. Overnight gap is not included."""
    if eq_0930 is None or eq_close is None or eq_0930 == 0:
        return None
    return (eq_close - eq_0930) / eq_0930


def compound(percents: list[float]) -> float | None:
    if not percents:
        return None
    growth = 1.0
    for pct in percents:
        growth *= 1.0 + pct
    return growth - 1.0


def verdict(n_trades: int, n_wins: int, dollars: float | None) -> str:
    """Win only above 55% wins over at least 30 closed trades.

    55.0% is not a win. The dollar result after fees is always beside the label.
    """
    shown = "dollar result after fees not printed" if dollars is None else (
        f"${dollars:+,.2f} after fees"
    )
    if n_trades < WIN_TRADES:
        return f"not enough trades yet ({shown})"
    if n_trades > 0 and (n_wins / n_trades) > WIN_RATE:
        return f"win ({shown})"
    return f"not cleared ({shown})"


def _cells(line: str) -> list[str]:
    parts = line.split("|")
    if line.startswith("|"):
        parts = parts[1:]
    if parts and parts[-1].strip() == "":
        parts = parts[:-1]
    return [part.strip() for part in parts]


def _blank_day() -> dict:
    return {
        "eq_0930": None,
        "eq_close": None,
        "session_0930": None,
        "session_close": None,
        "mark_0930": None,
        "mark_close": None,
        "pnls": [],
        "lots": {},
    }


def parse_scoreboard(text: str) -> dict[str, dict]:
    """Pull both equities, closed-trade P/L, and lot Day $ from one print."""
    days: dict[str, dict] = {}
    mode = ""
    cols: dict[str, int] = {}

    def slot(date: str) -> dict:
        return days.setdefault(date, _blank_day())

    for line in text.splitlines():
        if not line.startswith("|"):
            continue
        cells = _cells(line)
        if not cells or set(cells[0]) <= set("-: "):
            continue
        head = " | ".join(cell.lower() for cell in cells)
        if cells[0].lower() == "date":
            cols = {cell.lower(): i for i, cell in enumerate(cells)}
            if "09:30 equity" in head and "close equity" in head:
                mode = "session"
            elif "day $" in head and "ticker" in head:
                mode = "lot"
            elif "p/l" in head and "side" in head:
                mode = "fill"
            else:
                mode = ""
            continue
        matched = DAY_PREFIX.match(cells[0])
        if not matched:
            continue
        date = matched.group(1)
        row = slot(date)
        if mode == "session" and ISO_DAY.match(cells[0]):
            i_open = cols.get("09:30 equity")
            i_close = cols.get("close equity")
            if i_open is not None and i_close is not None and i_close < len(cells):
                row["session_0930"] = money(cells[i_open])
                row["session_close"] = money(cells[i_close])
        if mode == "lot" and ISO_DAY.match(cells[0]):
            i_tick = cols.get("ticker")
            i_day = cols.get("day $")
            if (
                i_tick is not None and i_day is not None
                and i_day < len(cells) and i_tick < len(cells)
            ):
                tick = cells[i_tick].strip("`").upper()
                got = money(cells[i_day])
                if tick and got is not None:
                    row["lots"][tick] = row["lots"].get(tick, 0.0) + got
        if mode == "fill":
            side = cells[cols["side"]] if "side" in cols and cols["side"] < len(cells) else ""
            side = side.replace("*", "").upper()
            if side in ("SELL", "COVER") and "p/l" in cols and cols["p/l"] < len(cells):
                pnl = money(cells[cols["p/l"]])
                if pnl is not None:
                    row["pnls"].append(pnl)
            if side == "OPEN":
                found = OPEN_EQ_RE.search(line)
                if found:
                    row["mark_0930"] = money(found.group(1))
            if side == "CLOSE":
                found = CLOSE_EQ_RE.search(line)
                if found:
                    row["mark_close"] = money(found.group(1))
                    row["mark_0930"] = money(found.group(2))
    for row in days.values():
        if row["session_0930"] is not None and row["session_close"] is not None:
            row["eq_0930"] = row["session_0930"]
            row["eq_close"] = row["session_close"]
        elif row["mark_0930"] is not None and row["mark_close"] is not None:
            row["eq_0930"] = row["mark_0930"]
            row["eq_close"] = row["mark_close"]
    return days


def prime_from_versions(versions: list[tuple[str, str]]) -> dict[str, dict]:
    """First version that printed both equities. Later text does not replace it."""
    prime: dict[str, dict] = {}
    for commit, text in versions:
        for date, row in parse_scoreboard(text).items():
            if date in prime:
                if not prime[date]["lots"] and row["lots"]:
                    prime[date]["lots"] = dict(row["lots"])
                    prime[date]["lots_commit"] = commit
                continue
            if row["eq_0930"] is None or row["eq_close"] is None:
                continue
            kept = dict(row)
            kept["commit"] = commit
            if row["lots"]:
                kept["lots_commit"] = commit
            prime[date] = kept
    return prime


def parse_flatten_card(text: str) -> tuple[float, float] | None:
    found = CARD_EQ_RE.search(text)
    if not found:
        return None
    return money(found.group(1)), money(found.group(2))


def flatten_bad_print(text: str, eq_0930: float, eq_close: float) -> str | None:
    """A first card that took the planned buy out of equity and did not mark the shares.

    The close column is a copy of the 09:30 price, and the session dollar
    matches the planned buy. That is a units bug, not a day's result.
    """
    buy_match = re.search(r"Planned buy cost \*\*\$([\d,.]+)\*\*", text)
    if not buy_match:
        return None
    buy = money(buy_match.group(1))
    if buy is None or buy < 1000:
        return None
    session = float(eq_close) - float(eq_0930)
    if abs(session + buy) > max(250.0, 0.05 * buy):
        return None
    rows = 0
    copied = 0
    cols: dict[str, int] = {}
    active = False
    for line in text.splitlines():
        if not line.startswith("|"):
            if active:
                break
            continue
        cells = _cells(line)
        if not cells or set(cells[0]) <= set("-: "):
            continue
        if cells[0].lower() == "ticker" and "close" in {cell.lower() for cell in cells}:
            cols = {cell.lower(): i for i, cell in enumerate(cells)}
            active = True
            continue
        if not active or "09:30" not in cols or "close" not in cols:
            continue
        if cols["close"] >= len(cells) or cols["09:30"] >= len(cells):
            continue
        rows += 1
        if cells[cols["close"]] == cells[cols["09:30"]]:
            copied += 1
    if rows == 0 or copied != rows:
        return None
    return (
        "known bad print: the planned buy was taken out of 16:00 equity "
        "and every close price is a copy of the 09:30 price, so the new "
        "shares were not marked"
    )


def parse_flatten_lots(text: str) -> dict[str, float]:
    """Day $ by ticker from the one-day session-mark table on a flatten card."""
    lots: dict[str, float] = {}
    cols: dict[str, int] = {}
    active = False
    for line in text.splitlines():
        if not line.startswith("|"):
            if active:
                break
            continue
        cells = _cells(line)
        if not cells or set(cells[0]) <= set("-: "):
            continue
        if cells[0].lower() == "ticker" and any(cell.lower() == "day $" for cell in cells):
            cols = {cell.lower(): i for i, cell in enumerate(cells)}
            active = True
            continue
        if not active:
            continue
        tick = cells[cols["ticker"]].strip("`").upper()
        got = money(cells[cols["day $"]]) if cols["day $"] < len(cells) else None
        if tick and tick not in ("—", "-") and got is not None:
            lots[tick] = lots.get(tick, 0.0) + got
    return lots


class _Git:
    def __init__(self, root: Path):
        self.root = root
        self.proc = subprocess.Popen(
            ["git", "cat-file", "--batch"],
            cwd=root, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        )

    def close(self) -> None:
        if self.proc.stdin:
            self.proc.stdin.close()
        if self.proc.stdout:
            self.proc.stdout.close()
        self.proc.wait(timeout=10)

    def blob(self, sha: str) -> bytes | None:
        assert self.proc.stdin and self.proc.stdout
        self.proc.stdin.write(f"{sha}\n".encode())
        self.proc.stdin.flush()
        header = self.proc.stdout.readline()
        if not header or b"missing" in header:
            return None
        parts = header.split()
        if len(parts) < 3:
            return None
        size = int(parts[2])
        data = self.proc.stdout.read(size)
        self.proc.stdout.read(1)
        return data

    def versions(self, path: str) -> list[tuple[str, str]]:
        log = subprocess.run(
            ["git", "log", "--reverse", "--pretty=format:%H", "--", path],
            cwd=self.root, check=True, capture_output=True, text=True,
        )
        out = []
        for commit in log.stdout.splitlines():
            if not commit:
                continue
            raw = self.blob(f"{commit}:{path}")
            if raw is None:
                continue
            out.append((commit, raw.decode("utf-8", errors="replace")))
        return out


def _diff_blobs(root: Path, commit: str, pathspec: str) -> list[tuple[str, str]]:
    raw = subprocess.run(
        [
            "git", "diff-tree", "--no-commit-id", "-r", "--root",
            "--diff-filter=AM", "-z", commit, "--", pathspec,
        ],
        cwd=root, check=True, capture_output=True,
    ).stdout
    parts = [part for part in raw.split(b"\0") if part]
    found = []
    index = 0
    while index + 1 < len(parts):
        meta, path = parts[index], parts[index + 1]
        index += 2
        if not path.endswith(b".md"):
            continue
        bits = meta.split()
        if len(bits) < 5:
            continue
        found.append((bits[3].decode(), path.decode()))
    return found


def factor_mine_prime(root: Path | None = None) -> dict[str, dict[str, dict]]:
    """Prime first print of every recipe day. Does not read the working tree."""
    root = root or ROOT
    commits = subprocess.run(
        ["git", "rev-list", "--reverse", "HEAD", "--", "03_scoreboard/factor_mine"],
        cwd=root, check=True, capture_output=True, text=True,
    ).stdout.splitlines()
    books: dict[str, dict[str, dict]] = {}
    git = _Git(root)
    try:
        for i, commit in enumerate(commits, start=1):
            added = 0
            for sha, path in _diff_blobs(root, commit, "03_scoreboard/factor_mine"):
                name = Path(path).stem
                raw = git.blob(sha)
                if raw is None:
                    continue
                parsed = parse_scoreboard(raw.decode("utf-8", errors="replace"))
                dest = books.setdefault(name, {})
                for date, row in parsed.items():
                    if date in dest:
                        if not dest[date]["lots"] and row["lots"]:
                            dest[date]["lots"] = dict(row["lots"])
                            dest[date]["lots_commit"] = commit
                        continue
                    if row["eq_0930"] is None or row["eq_close"] is None:
                        continue
                    kept = dict(row)
                    kept["commit"] = commit
                    if row["lots"]:
                        kept["lots_commit"] = commit
                    dest[date] = kept
                    added += 1
            print(
                f"[scorecard] factor mine commit {i}/{len(commits)} "
                f"{commit[:10]} new-days {added}",
                flush=True,
            )
    finally:
        git.close()
    return books


def factor_mine_watermark(root: Path | None = None) -> str:
    """Days on or before this date stay on the prime print.

    No manifest means the lock is not in this tree, so every printed day
    is before the lock.
    """
    path = (root or ROOT) / "data" / "past_day_lock" / "manifest.jsonl"
    if not path.is_file():
        return ""
    watermark = ""
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line:
            continue
        row = json.loads(line)
        if row.get("kind") == "seed" and row.get("record") == "factor_mine":
            watermark = str(row.get("watermark") or "")
    return watermark


def _book_equity(row: dict) -> float | None:
    cash = row.get("cash_primary")
    holdings = row.get("holdings") or []
    if cash is None:
        return None
    total = float(cash)
    for held in holdings:
        try:
            total += float(held["shares"]) * float(held["last_px"])
        except (KeyError, TypeError, ValueError):
            return None
    return total


def read_jsonl_book(path: Path) -> dict[str, dict]:
    """h1 / holdup. Append-only, so this file is the first print of each line."""
    days: dict[str, dict] = {}
    if not path.is_file():
        return days
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line:
            continue
        row = json.loads(line)
        date = str(row.get("date") or "")
        if not ISO_DAY.match(date):
            continue
        slot = days.setdefault(date, {
            "eq_0930": None, "eq_close": None, "pnls": [], "lots": {},
            "committed_at": None, "commit": "log",
        })
        kind = row.get("kind")
        if kind == "plan" and row.get("committed_at") and not slot["committed_at"]:
            slot["committed_at"] = row["committed_at"]
        elif kind == "open_fill" and slot["eq_0930"] is None:
            slot["eq_0930"] = _book_equity(row)
        elif kind == "mark" and row.get("equity_primary") is not None and slot["eq_close"] is None:
            slot["eq_close"] = float(row["equity_primary"])
        elif kind == "close" and row.get("pnl_primary") is not None:
            slot["pnls"].append(float(row["pnl_primary"]))
            tick = str(row.get("ticker") or "").upper()
            if tick:
                slot["lots"][tick] = slot["lots"].get(tick, 0.0) + float(row["pnl_primary"])
    return days


def before_0930(when: str | None, day: str) -> bool:
    if not when:
        return False
    stamp = dt.datetime.fromisoformat(when.replace("Z", "+00:00"))
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=dt.timezone.utc)
    open_at = dt.datetime.fromisoformat(f"{day}T09:30:00").replace(tzinfo=ET)
    return stamp < open_at


def ticket_lock_times(root: Path | None = None) -> dict[str, str]:
    """First commit time of each dated ticket file, ISO with offset."""
    root = root or ROOT
    folder = root / "data" / "day_board"
    times = {}
    if not folder.is_dir():
        return times
    for path in sorted(folder.glob("*_strategy_tickets.json")):
        day = path.name[:10]
        if not ISO_DAY.match(day):
            continue
        log = subprocess.run(
            ["git", "log", "--reverse", "--pretty=format:%cI", "--",
             path.relative_to(root).as_posix()],
            cwd=root, check=True, capture_output=True, text=True,
        )
        first = (log.stdout.splitlines() or [""])[0]
        if first:
            times[day] = first
    return times


def live_dates(lock_flags: dict[str, bool]) -> tuple[str, set[str]]:
    """First day locked before 09:30, and every later day that was too.

    A later day that was not locked before the open stays out of the live set.
    """
    locked = sorted(day for day, flag in lock_flags.items() if flag)
    if not locked:
        return "", set()
    first = locked[0]
    return first, {day for day in locked if day >= first}


def flatten_prime(root: Path | None = None) -> dict[str, dict]:
    root = root or ROOT
    git = _Git(root)
    out = {}
    try:
        for path in sorted((root / "01_daily").glob("*_flatten_card.md")):
            day = path.name[:10]
            if not ISO_DAY.match(day):
                continue
            rel = path.relative_to(root).as_posix()
            for commit, text in git.versions(rel):
                pair = parse_flatten_card(text)
                if pair and pair[0] is not None and pair[1] is not None:
                    out[day] = {
                        "eq_0930": pair[0], "eq_close": pair[1],
                        "pnls": [], "lots": parse_flatten_lots(text),
                        "commit": commit,
                        "bad_print": flatten_bad_print(text, pair[0], pair[1]),
                    }
                    break
    finally:
        git.close()
    return out


def sleeve_trade_pnls(path: Path | None = None) -> list[tuple[str, float]]:
    path = path or SLEEVE_TRADES
    if not path.is_file():
        return []
    pnls = []
    with path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            day = (row.get("exit_date") or "").strip()[:10]
            if not ISO_DAY.match(day):
                continue
            got = money(row.get("pnl") or "")
            if got is not None:
                pnls.append((day, got))
    return pnls


def paper_sell_pnls(path: Path | None = None) -> dict[str, list[float]]:
    path = path or PAPER_TRADES
    out: dict[str, list[float]] = defaultdict(list)
    if not path.is_file():
        return {}
    with path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if (row.get("side") or "").lower() != "sell":
                continue
            got = money(row.get("realized_pnl") or "")
            if got is None:
                continue
            out[row.get("sleeve") or "paper"].append(got)
    return dict(out)


@dataclass
class Section:
    title: str
    days: list[dict] = field(default_factory=list)
    pnls: list[float] = field(default_factory=list)
    note: str = ""
    book: str = ""
    baseline: dict = field(default_factory=dict)

    def _scored_days(self) -> list[dict]:
        return [day for day in self.days if not day.get("bad_print")]

    @property
    def percents(self) -> list[float]:
        vals = []
        for day in self._scored_days():
            pct = day_percent(day.get("eq_0930"), day.get("eq_close"))
            if pct is not None:
                vals.append(pct)
        return vals

    @property
    def session_dollars(self) -> float | None:
        parts = []
        for day in self._scored_days():
            if day.get("eq_0930") is None or day.get("eq_close") is None:
                continue
            parts.append(float(day["eq_close"]) - float(day["eq_0930"]))
        if not parts:
            return None
        return sum(parts)

    @property
    def n_trades(self) -> int:
        return len(self.pnls)

    @property
    def n_wins(self) -> int:
        return sum(1 for pnl in self.pnls if pnl > 0)

    def lot_sums(self) -> dict[str, float]:
        totals: dict[str, float] = defaultdict(float)
        for day in self._scored_days():
            for tick, got in (day.get("lots") or {}).items():
                totals[tick] += float(got)
        return dict(totals)

    def without_best(self) -> tuple[str, float, float] | None:
        """Remove the single best stock's printed Day $ from the session dollars.

        This is the printed contribution taken off, not a fresh booking.
        """
        base = self.session_dollars
        totals = self.lot_sums()
        if base is None or not totals:
            return None
        tick = max(totals, key=lambda name: totals[name])
        return tick, totals[tick], base - totals[tick]


def split_sections(days: dict[str, dict], live: set[str], *,
                   trade_pnls: list[tuple[str, float]] | None = None,
                   live_note: str = "", built_note: str = "") -> list[Section]:
    live_sec = Section("LIVE-LOCKED", note=live_note)
    built_sec = Section("BUILT AFTER THE FACT", note=built_note)
    for date in sorted(days):
        row = dict(days[date])
        row["date"] = date
        bucket = live_sec if date in live else built_sec
        bucket.days.append(row)
        if trade_pnls is None:
            bucket.pnls.extend(row.get("pnls") or [])
    if trade_pnls is not None:
        for date, pnl in trade_pnls:
            if date in live:
                live_sec.pnls.append(pnl)
            else:
                built_sec.pnls.append(pnl)
    return [live_sec, built_sec]


def fmt_pct(value: float | None) -> str:
    if value is None:
        return "—"
    return f"{100.0 * value:+.2f}%"


def fmt_money(value: float | None) -> str:
    if value is None:
        return "—"
    return f"${value:+,.2f}"


def section_row(name: str, section: Section, baseline: dict | None = None) -> str:
    dollars = section.session_dollars
    label = verdict(section.n_trades, section.n_wins, dollars)
    best = section.without_best()
    if best is None:
        best_s = "not in the prime print"
    else:
        tick, got, left = best
        best_s = f"without {tick} {fmt_money(got)} → {fmt_money(left)} (printed $ removed, not a fresh booking)"
    base = baseline if baseline is not None else section.baseline
    hit = ""
    if section.n_trades:
        hit = f"{100.0 * section.n_wins / section.n_trades:.1f}% of {section.n_trades}"
    else:
        hit = "0 trades"
    return (
        f"| {name} | {section.title} | {len(section.percents)} | "
        f"{fmt_pct(compound(section.percents))} | {fmt_money(dollars)} | "
        f"{hit} | {label} | {best_s} | "
        f"{base.get('random4', '—')} | {base.get('iwm', '—')} |"
    )


def render_markdown(blocks: list[tuple[str, list[Section], str]],
                    baselines_note: str) -> str:
    lines = [
        "# Honest scorecard",
        "",
        "Reporting only. Nothing here changes a buy, a sell, a fee, or a size.",
        "",
        "A day's percent is (close equity − that morning's 09:30 equity) / "
        "the 09:30 equity. The gap from one day's close to the next morning "
        "is not counted. Percents are compounded only across the days in "
        "that section. LIVE-LOCKED and BUILT AFTER THE FACT are never added together.",
        "",
        "Factor Mine days before the lock use the prime first print: the first "
        "git commit that showed that day's close equity and that morning's "
        "09:30 equity for that recipe. The current scoreboard file is not "
        "used for those days. Nightly runs on 25 Sep and 28 Sep–2 Oct rewrote "
        "scoreboard text; those later texts are ignored.",
        "",
        "A strategy is a **win** only with more than 55% winning closed trades "
        "over at least 30 trades. 55.0% is not a win. Otherwise the label is "
        "**not cleared**, or **not enough trades yet** when there are fewer "
        "than 30. The dollar result after fees is always next to the label. "
        "That dollar is the sum of (close equity − 09:30 equity) across the "
        "days in the section, so the overnight gap is not inside it.",
        "",
        baselines_note,
        "",
    ]
    named = {"h1", "holdup", "flatten_robust"}
    for title, sections, blurb in blocks:
        lines += ["", f"## {title}", "", blurb, ""]
        if not sections:
            continue
        lines += [
            "| Book | Section | Priced days | Compounded day % | Session $ after fees | "
            "Winning trades | Label | Without best stock | RANDOM4 | IWM |",
            "|---|---|---:|---:|---:|---:|---|---|---|---|",
        ]
        any_row = False
        details: list[str] = []
        for section in sections:
            if not section.percents and not section.pnls:
                lines.append(f"_{section.title}: no days._")
                continue
            any_row = True
            lines.append(section_row(section.book or title, section))
            if title in named and section.percents:
                details += [
                    "",
                    f"### {section.book or title} — {section.title}",
                    "",
                    "| Date | Day % | Session $ | Print |",
                    "|---|---:|---:|---|",
                ]
                for day in section.days:
                    pct = day_percent(day.get("eq_0930"), day.get("eq_close"))
                    if pct is None:
                        continue
                    commit = str(day.get("commit") or "")[:10]
                    if day.get("bad_print"):
                        details.append(
                            f"| {day['date']} | known bad print | "
                            f"{fmt_money(float(day['eq_close']) - float(day['eq_0930']))} "
                            f"printed, not scored | {commit} |"
                        )
                        continue
                    details.append(
                        f"| {day['date']} | {fmt_pct(pct)} | "
                        f"{fmt_money(float(day['eq_close']) - float(day['eq_0930']))} | "
                        f"{commit} |"
                    )
            if section.note:
                details += ["", section.note, ""]
        if not any_row and not sections:
            lines.append("_No day-percent row._")
        lines.extend(details)
    lines += [
        "",
        "## Not scored, and why",
        "",
        "- Paper stock-book sleeves (`data/paper/equity_curve.csv`) print a "
        "close equity only. There is no 09:30 equity, so a close-to-close "
        "change is not used as the day percent.",
        "- breadth_rank v1 / v1b / v1c, lever_search, and OOS-0914 seals hash "
        "inputs or return files. They do not print a 09:30 equity and a close "
        "equity for this day percent.",
        "- forward_shadow_v1 prints a full-session return, not this "
        "open-equity to close-equity percent, so it is not folded into the live number.",
        "- `data/sleeve_merge/trades.csv` is a round-trip blotter. It supplies "
        "the flatten closed-trade count. It is not the day percent.",
        "",
    ]
    return "\n".join(lines)


def render_html(markdown: str) -> str:
    body = html.escape(markdown)
    return (
        "<!doctype html><html><head><meta charset='utf-8'>"
        "<title>Honest scorecard</title>"
        "<style>body{font:16px/1.45 system-ui,sans-serif;margin:2rem;max-width:1100px}"
        "pre{white-space:pre-wrap}</style></head><body><pre>"
        f"{body}</pre></body></html>\n"
    )


def load_fees() -> dict:
    return json.loads(FEES_PATH.read_text(encoding="utf-8"))


def order_fees(shares: int, price: float, side: str, fees: dict) -> float:
    """Same Futubull schedule as paper_trade.order_fees."""
    if shares <= 0 or price <= 0:
        return 0.0
    amount = shares * price
    comm = min(
        max(fees["commission_per_share"] * shares, fees["commission_min_per_order"]),
        fees["commission_max_pct_of_amount"] * amount,
    )
    plat = min(
        max(fees["platform_per_share"] * shares, fees["platform_min_per_order"]),
        fees["platform_max_pct_of_amount"] * amount,
    )
    total = comm + plat + fees["settlement_per_share"] * shares
    if side == "sell":
        total += max(
            fees["regulatory_pct_of_amount_sell_only"] * amount,
            fees["regulatory_min_per_order"],
        )
        total += min(
            max(fees["taf_per_share_sell_only"] * shares, fees["taf_min_per_order"]),
            fees["taf_max_per_order"],
        )
    return round(total, 4)


def random4_draw(pools: dict[str, list[str]], draw_i: int, *,
                 n: int = RANDOM4_N, seed: int = RANDOM4_SEED) -> dict[str, list[str]]:
    """Same draw as factor_mine_retro.random4_draw with no exclude."""
    rng = random.Random(int(seed) + int(draw_i))
    out = {}
    for date in sorted(pools):
        pool = [tick for tick in pools[date]]
        k = min(int(n), len(pool))
        out[date] = rng.sample(pool, k) if k else []
    return out


def _buy_sell_day(cash: float, names: list[str],
                  opens: dict[str, float], closes: dict[str, float],
                  fees: dict) -> tuple[float, float | None]:
    priced = [tick for tick in names if opens.get(tick) and closes.get(tick)]
    if not priced or cash <= 0:
        return cash, 0.0 if cash > 0 else None
    budget = cash / len(priced)
    lots = []
    spent = 0.0
    for tick in priced:
        op = opens[tick]
        shares = int(budget // op) if op > 0 else 0
        while shares > 0 and shares * op + order_fees(shares, op, "buy", fees) > budget + 1e-6:
            shares -= 1
        if shares <= 0:
            continue
        fee = order_fees(shares, op, "buy", fees)
        spent += shares * op + fee
        lots.append((tick, shares))
    cash_left = cash - spent
    open_eq = cash_left + sum(shares * opens[tick] for tick, shares in lots)
    close_cash = cash_left
    for tick, shares in lots:
        cl = closes[tick]
        close_cash += shares * cl - order_fees(shares, cl, "sell", fees)
    if open_eq <= 0:
        return cash, None
    return close_cash, (close_cash - open_eq) / open_eq


def random4_compounded(dates: list[str], pools: dict[str, list[str]],
                       prices: dict[tuple[str, str], tuple[float, float]],
                       capital: float, fees: dict, *,
                       draws: int = RANDOM4_DRAWS) -> list[float] | None:
    """1000 seeded books. Flat overnight, so the gap is not in the compound."""
    if not dates or capital <= 0:
        return None
    # A name with no split-adjusted open and close cannot fill at 09:30.
    # A morning with no 4-name universe is left out and named in the report.
    use_pools = {}
    for day in dates:
        names = [
            tick for tick in (pools.get(day) or [])
            if prices.get((day, tick))
        ]
        if len(names) >= RANDOM4_N:
            use_pools[day] = names
    if not use_pools:
        return None
    # Need a price for the names we might draw. Caller passes the full map.
    out = []
    for i in range(draws):
        picks = random4_draw(use_pools, i)
        cash = capital
        percents = []
        ok = True
        for day in sorted(use_pools):
            opens = {}
            closes = {}
            for tick in picks[day]:
                pair = prices.get((day, tick))
                if not pair:
                    ok = False
                    break
                opens[tick], closes[tick] = pair
            if not ok:
                break
            cash, pct = _buy_sell_day(cash, picks[day], opens, closes, fees)
            if pct is None:
                ok = False
                break
            percents.append(pct)
        if not ok:
            continue
        got = compound(percents)
        if got is not None:
            out.append(got)
    if len(out) < draws * 0.8:
        return None
    return out, sorted(use_pools)


def percentile_rank(strategy: float, draws: list[float]) -> float:
    if not draws:
        return 0.0
    below = sum(1 for value in draws if value < strategy)
    ties = sum(1 for value in draws if value == strategy)
    return 100.0 * (below + 0.5 * ties) / len(draws)


def iwm_path(dates: list[str], prices: dict[tuple[str, str], tuple[float, float]],
             capital: float, fees: dict) -> tuple[float, float] | None:
    if not dates or capital <= 0:
        return None
    first = dates[0]
    pair = prices.get((first, "IWM"))
    if not pair or pair[0] <= 0:
        return None
    op0 = pair[0]
    shares = int(capital // op0)
    while shares > 0 and shares * op0 + order_fees(shares, op0, "buy", fees) > capital + 1e-6:
        shares -= 1
    if shares <= 0:
        return None
    cash = capital - shares * op0 - order_fees(shares, op0, "buy", fees)
    percents = []
    dollars = 0.0
    for i, day in enumerate(dates):
        bar = prices.get((day, "IWM"))
        if not bar:
            return None
        op, cl = bar
        open_eq = cash + shares * op
        close_eq = cash + shares * cl
        if i == len(dates) - 1:
            close_eq -= order_fees(shares, cl, "sell", fees)
        if open_eq <= 0:
            return None
        percents.append((close_eq - open_eq) / open_eq)
        dollars += close_eq - open_eq
    got = compound(percents)
    if got is None:
        return None
    return got, dollars


def candidate_pools(root: Path | None = None) -> dict[str, list[str]]:
    folder = (root or ROOT) / "data" / "factor_mine" / "candidates"
    pools = {}
    if not folder.is_dir():
        return pools
    for path in sorted(folder.glob("*.json")):
        day = path.stem
        if not ISO_DAY.match(day):
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        names = []
        for row in payload.get("names") or []:
            tick = str(row.get("ticker") or "").upper()
            if tick:
                names.append(tick)
        pools[day] = sorted(set(names))
    return pools


def describe_baselines(strategy_pct: float | None, draws: list[float] | None,
                       iwm: tuple[float, float] | None) -> dict[str, str]:
    out = {"random4": "unavailable", "iwm": "unavailable"}
    if draws:
        med = sorted(draws)[len(draws) // 2]
        if strategy_pct is None:
            out["random4"] = f"median {fmt_pct(med)} (strategy day % missing)"
        else:
            rank = percentile_rank(strategy_pct, draws)
            out["random4"] = f"median {fmt_pct(med)}; strategy percentile {rank:.1f}"
    if iwm is not None:
        out["iwm"] = f"{fmt_pct(iwm[0])} compounded, {fmt_money(iwm[1])} session $"
    return out


def _priced_dates(section: Section) -> list[str]:
    dates = []
    for day in section._scored_days():
        if day_percent(day.get("eq_0930"), day.get("eq_close")) is not None:
            dates.append(str(day["date"]))
    return dates


def _attach_baselines(blocks, prices: dict, root: Path) -> None:
    fees = load_fees()
    pools = candidate_pools(root)
    cache: dict[tuple[str, ...], tuple] = {}
    for _title, sections, _blurb in blocks:
        for section in sections:
            dates = _priced_dates(section)
            if not dates:
                continue
            key = tuple(dates)
            if key not in cache:
                capital = 10000.0
                for day in section.days:
                    if day.get("eq_0930"):
                        capital = float(day["eq_0930"])
                        break
                if not prices:
                    cache[key] = (None, None, dates)
                else:
                    iwm_dates = [day for day in dates if prices.get((day, "IWM"))]
                    cache[key] = (
                        random4_compounded(dates, pools, prices, capital, fees),
                        iwm_path(iwm_dates, prices, capital, fees) if iwm_dates else None,
                        iwm_dates,
                    )
            packed, iwm, iwm_dates = cache[key]
            if packed:
                draws, used = packed
                used_set = set(used)
                overlap = []
                for day in section.days:
                    if day.get("date") not in used_set:
                        continue
                    pct = day_percent(day.get("eq_0930"), day.get("eq_close"))
                    if pct is not None:
                        overlap.append(pct)
                section.baseline = describe_baselines(compound(overlap), draws, iwm)
                omitted = [day for day in dates if day not in used_set]
                if omitted:
                    shown = omitted[0] if len(omitted) == 1 else f"{len(omitted)} days"
                    section.baseline["random4"] += (
                        f"; omitted {shown} (no 4-name universe). "
                        "Percentile uses the strategy on the same days as the draws."
                    )
            else:
                section.baseline = describe_baselines(None, None, iwm)
            if iwm is not None and len(iwm_dates) != len(dates):
                missing = [day for day in dates if day not in set(iwm_dates)]
                section.baseline["iwm"] += (
                    f"; omitted {', '.join(missing)} (no IWM bar). "
                    "IWM is the other scored days only, same Futubull fees."
                )


def fetch_split_adjusted(tickers: list[str], start: str, end: str) -> dict:
    """Yahoo split-adjusted open and close. Missing names are left out."""
    import yfinance as yf
    out: dict[tuple[str, str], tuple[float, float]] = {}
    names = sorted({tick.upper() for tick in tickers if tick})
    end_day = (dt.date.fromisoformat(end) + dt.timedelta(days=7)).isoformat()
    for offset in range(0, len(names), 80):
        batch = names[offset:offset + 80]
        try:
            frame = yf.download(
                batch, start=start, end=end_day, auto_adjust=True,
                group_by="ticker", threads=True, progress=False,
            )
        except Exception as exc:  # noqa: BLE001 — a bad batch must not drop the rest
            print(f"[scorecard] price batch failed: {exc}", flush=True)
            continue
        if frame is None or len(frame) == 0:
            continue
        for tick in batch:
            try:
                sub = frame[tick] if len(batch) > 1 else frame
            except (KeyError, TypeError):
                continue
            if sub is None or getattr(sub, "empty", True):
                continue
            for stamp, row in sub.iterrows():
                day = str(stamp)[:10]
                try:
                    op = float(row["Open"])
                    cl = float(row["Close"])
                except (KeyError, TypeError, ValueError):
                    continue
                if op > 0 and cl > 0:
                    out[(day, tick)] = (op, cl)
        print(f"[scorecard] prices {min(offset + 80, len(names))}/{len(names)}", flush=True)
    return out


def _not_scored_notes() -> list[tuple[str, list[Section], str]]:
    paper = paper_sell_pnls()
    blocks = []
    if paper:
        bits = []
        for sleeve, pnls in sorted(paper.items()):
            wins = sum(1 for pnl in pnls if pnl > 0)
            dollars = sum(pnls)
            bits.append(
                f"{sleeve}: {verdict(len(pnls), wins, dollars)} "
                f"on closed sells only"
            )
        blocks.append((
            "Paper sleeves (no day percent)",
            [],
            "No 09:30 equity is printed on the equity curve, so these are "
            "trade labels only and are not a live day-percent score. "
            + " ".join(bits),
        ))
    return blocks


def build_report(root: Path | None = None, *,
                 with_baselines: bool = False,
                 prices: dict | None = None) -> str:
    root = root or ROOT
    blocks: list[tuple[str, list[Section], str]] = []

    h1_days = read_jsonl_book(root / "research" / "hot_n4_clean_v4" / "forward_h1" / "h1_log.jsonl")
    h1_flags = {day: before_0930(row.get("committed_at"), day) for day, row in h1_days.items()}
    h1_first, h1_live = live_dates(h1_flags)
    blocks.append((
        "h1",
        split_sections(h1_days, h1_live),
        "First locked-before-09:30 day is "
        f"{h1_first or 'none'}. "
        "09:30 equity is cash plus shares times the open_fill last price "
        "(holdings store the open). Close equity is mark.equity_primary, "
        "which matches cash plus that day's prices.jsonl close. "
        "Days with only a session line and no close mark have no day percent. "
        "A day after the first lock that was committed after 09:30 stays in "
        "BUILT AFTER THE FACT. The win rate counts closed trades only; "
        "a name still held at the close is in the day percent and not in "
        "that count. 2026-10-05 is SDEV, 1,222 shares, open 9.71 to close 3.94.",
    ))

    hold_days = read_jsonl_book(root / "research" / "hot_n4_clean_v4" / "forward" / "holdup_log.jsonl")
    hold_flags = {day: before_0930(row.get("committed_at"), day) for day, row in hold_days.items()}
    hold_first, hold_live = live_dates(hold_flags)
    blocks.append((
        "holdup",
        split_sections(hold_days, hold_live),
        "Same print rule as h1. First locked-before-09:30 day is "
        f"{hold_first or 'none'}.",
    ))

    tickets = ticket_lock_times(root)
    flags = {day: before_0930(when, day) for day, when in tickets.items()}
    flat_first, flat_live = live_dates(flags)
    flat_days = flatten_prime(root)
    # A card with no before-09:30 ticket is not live, including days the
    # ticket file never had.
    blocks.append((
        "flatten_robust",
        split_sections(
            flat_days, flat_live,
            trade_pnls=sleeve_trade_pnls(root / "data" / "sleeve_merge" / "trades.csv"),
        ),
        "Day percent is the first git commit of that day's flatten card that "
        "printed both the 09:30 equity and the 16:00 equity. Later edits of "
        "the card are ignored. "
        f"First ticket locked before 09:30 is {flat_first or 'none'}. "
        "A later day whose ticket was first committed after 09:30 is not in "
        "the live section. Sleeves share this card; the card is the daily record. "
        "Closed-trade P/L is the round-trip blotter (after entry and exit fees). "
        "A known bad print is the first card whose session dollar matches the "
        "planned buy and whose close price is a copy of the 09:30 price. "
        "Those days stay in the table and are not in the compound.",
    ))

    watermark = factor_mine_watermark(root)
    prime = factor_mine_prime(root)
    mine_sections: list[Section] = []
    for name in sorted(prime):
        for section in split_sections(prime[name], set()):
            if section.title != "BUILT AFTER THE FACT":
                continue
            if not section.percents and not section.pnls:
                continue
            section.book = name
            mine_sections.append(section)
    blocks.append((
        "Factor Mine",
        mine_sections,
        (
            "No past-day lock manifest is in this tree, so every Factor Mine "
            "day is before the lock."
            if not watermark else
            f"Factor Mine days on or before the lock watermark {watermark} "
            "use the prime first print."
        ) + " LIVE-LOCKED has no Factor Mine days: the scoreboard is printed "
        "after the close, and these days were not sealed before 09:30. "
        "Each row is BUILT AFTER THE FACT from the prime first print and is "
        "not added into a live score.",
    ))
    blocks.extend(_not_scored_notes())
    if with_baselines:
        _attach_baselines(blocks, prices if prices is not None else {}, root)
    note = (
        "RANDOM4 is 1,000 draws, seed 20260813, four names from that morning's "
        "factor-mine candidate file, whole shares, Futubull fees, bought at the "
        "split-adjusted open and sold at the close. IWM is buy-and-hold on "
        "Yahoo split-adjusted bars with the same fee schedule. "
        "Prices are not the raw on-disk OHLC store. A candidate with no "
        "split-adjusted open and close that morning is left out of the draw. "
        "RANDOM4 and IWM use the scored days of that same section and the "
        "Futubull schedule in `00_grounding/futubull_fees.json` (the same "
        "formula as `paper_trade.order_fees`). A known bad print is not a "
        "scored day. A morning with no four priced names, or no IWM bar, is "
        "named and left out of that baseline; the percentile uses the book "
        "on the days that remain."
    )
    if not with_baselines:
        note += " Baselines are filled when --baselines can download split-adjusted Yahoo prices."
    return render_markdown(blocks, note)


def main(argv: list[str] | None = None) -> int:
    import argparse
    parser = argparse.ArgumentParser(description="Write the honest scorecard")
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--baselines", action="store_true")
    args = parser.parse_args(argv)
    prices = None
    if args.baselines:
        pools = candidate_pools()
        tickers = sorted({tick for names in pools.values() for tick in names})
        tickers.append("IWM")
        days = sorted(pools)
        if days:
            print(f"[scorecard] downloading {len(tickers)} split-adjusted names", flush=True)
            prices = fetch_split_adjusted(tickers, days[0], days[-1])
            print(f"[scorecard] price rows {len(prices)}", flush=True)
    text = build_report(with_baselines=args.baselines, prices=prices)
    if args.write:
        SCOREBOARD.parent.mkdir(parents=True, exist_ok=True)
        SCOREBOARD.write_text(text, encoding="utf-8")
        DASHBOARD.parent.mkdir(parents=True, exist_ok=True)
        DASHBOARD.write_text(render_html(text), encoding="utf-8")
        print(f"wrote {SCOREBOARD}")
        print(f"wrote {DASHBOARD}")
    else:
        print(text[:2000])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
