"""Theme Radar skip gates, and the separate short research books.

A skip gate removes long picks on the morning it fires and does not
backfill them. The short book enters at that session's close and covers
h sessions later. It is research only and is not a freeze candidate.
Train does not open a snapshot dated on or after 2026-09-14.
"""
from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd

from . import factor_mine as fm
from . import factor_mine_oos0914 as oos

SNAP_DIR = Path("/tmp/theme-radar/data/snapshots")
FIELDS = ("Forward P/E", "Market Cap", "Analyst Recom")
EARN_OK = {"today", "tomorrow", "this_week"}
MEMBER_DIR = oos.ROOT / "data" / "universe"
LETTER_PATH = oos.ROOT / "excel_bot" / "research" / "excel_clear_letter_panel.csv"


def snapshot_dates(folder: Path, *, allow_test: bool) -> list[str]:
    found = []
    if not folder.is_dir():
        return found
    for path in sorted(folder.glob("*.csv")):
        if ".raw." in path.name or path.name in ("current.csv", "previous.csv"):
            continue
        date = oos.file_date(path)
        if not date:
            continue
        if date >= oos.CUTOFF and not allow_test:
            continue
        found.append(date)
    return found


def read_snapshot(folder: Path, date: str, *, allow_test: bool) -> pd.DataFrame | None:
    if date >= oos.CUTOFF and not allow_test:
        raise oos.FutureLeak(f"theme radar snapshot {date} is on or after the cutoff")
    path = folder / f"{date}.csv"
    if not path.is_file():
        return None
    frame = pd.read_csv(path, usecols=["Ticker", *FIELDS])
    frame["ticker"] = frame["Ticker"].astype(str).str.upper()
    for col in FIELDS:
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    return frame.drop(columns=["Ticker"]).drop_duplicates("ticker").set_index("ticker")


def _lag_date(clock: list[str], day: str, lag: str, since_first: str) -> str | None:
    if lag == "level":
        return day
    if lag == "since_first":
        return since_first if since_first in clock else None
    step = int(str(lag).split("-")[1])
    if day not in clock:
        return None
    index = clock.index(day)
    if index < step:
        return None
    return clock[index - step]


def _top_quintile(values: pd.Series) -> set[str]:
    series = pd.to_numeric(values, errors="coerce").dropna()
    if len(series) < 5:
        return set()
    try:
        bins = pd.qcut(series, 5, labels=False, duplicates="drop")
    except ValueError:
        return set()
    top = int(bins.max())
    return {str(t) for t in bins[bins == top].index}


def _signal(panel: pd.DataFrame, prior: pd.DataFrame | None, rule: dict) -> set[str] | None:
    field = rule["field"]
    if rule["lag"] == "level":
        values = panel[field]
    else:
        if prior is None:
            return set()
        values = panel[field] - prior[field]
    if rule["op"] == "top_quintile":
        return _top_quintile(values)
    if rule["op"] == "positive":
        return {str(t) for t in values[values > 0].dropna().index}
    if rule["op"] == "negative":
        return {str(t) for t in values[values < 0].dropna().index}
    raise RuntimeError(f"unknown theme radar op {rule['op']}")


def _earn(date: str) -> set[str] | None:
    path = MEMBER_DIR / f"{date}_membership.csv"
    if not path.is_file():
        return None
    names = set()
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if str(row.get("earn") or "") in EARN_OK:
                ticker = str(row.get("Ticker") or "").upper()
                if ticker:
                    names.add(ticker)
    return names


def _letters(path: Path = LETTER_PATH) -> dict[str, dict[str, set[str]]]:
    out: dict[str, dict[str, set[str]]] = {}
    if not path.is_file():
        return out
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            date = str(row.get("date") or "")[:10]
            ticker = str(row.get("ticker") or "").upper()
            if not date or not ticker:
                continue
            slot = out.setdefault(date, {"er": set(), "hammer": set()})
            try:
                er = int(float(row.get("ER") or 0))
            except (TypeError, ValueError):
                er = 0
            if er == 1:
                slot["er"].add(ticker)
            text = str(row.get("DF_lag1") or "")
            if "Hammer" in text and "Inverted" not in text:
                slot["hammer"].add(ticker)
    return out


def skip_calendar(dates: list[str], rules: list[dict], *,
                  allow_test: bool, folder: Path = SNAP_DIR) -> dict[str, dict[str, set[str] | None]]:
    """Per rule, per morning: tickers to skip, or None when the gate cannot be scored."""
    clock = snapshot_dates(folder, allow_test=allow_test)
    need = set()
    for date in dates:
        prior = [d for d in clock if d < date]
        if not prior:
            continue
        day_t = prior[-1]
        need.add(day_t)
        for rule in rules:
            lag = _lag_date(clock, day_t, rule["lag"], rule.get("since_first") or "2026-08-06")
            if lag:
                need.add(lag)
    panels = {
        day: read_snapshot(folder, day, allow_test=allow_test)
        for day in sorted(need)
    }
    letters = _letters()
    out = {rule["id"]: {} for rule in rules}
    for date in dates:
        prior = [d for d in clock if d < date]
        day_t = prior[-1] if prior else None
        panel = panels.get(day_t) if day_t else None
        earn = _earn(date)
        letter = letters.get(date)
        for rule in rules:
            if panel is None:
                out[rule["id"]][date] = None
                continue
            if rule.get("earn") and earn is None:
                out[rule["id"]][date] = None
                continue
            if (rule.get("er") or rule.get("hammer")) and letter is None:
                out[rule["id"]][date] = None
                continue
            lag = _lag_date(clock, day_t, rule["lag"], rule.get("since_first") or "2026-08-06")
            prior_panel = panels.get(lag) if lag and lag != day_t else None
            if rule["lag"] != "level" and prior_panel is None:
                out[rule["id"]][date] = set()
                continue
            flagged = _signal(panel, prior_panel, rule)
            if flagged is None:
                out[rule["id"]][date] = None
                continue
            if rule.get("earn"):
                flagged &= earn or set()
            if rule.get("er"):
                flagged &= (letter or {}).get("er") or set()
            if rule.get("hammer"):
                flagged &= (letter or {}).get("hammer") or set()
            out[rule["id"]][date] = flagged
    return out


def kept_rows(rows: list[dict], rec: dict, banned: set[str] | None) -> list[dict]:
    """Top long picks with the gate applied. A missing gate sits. No backfill."""
    if banned is None:
        return []
    chosen = fm.pick_day(rows, rec)
    return [
        row for row in chosen
        if str(row.get("ticker") or "").upper() not in banned
    ]


def score_shorts(dates: list[str], rules: list[dict], closes: dict,
                 calendar: list[str], *, allow_test: bool,
                 folder: Path = SNAP_DIR) -> dict[str, dict]:
    """Enter at the close, cover h sessions later. 15bp plus 0.3% borrow."""
    flags = skip_calendar(dates, rules, allow_test=allow_test, folder=folder)
    cal = [d for d in calendar if d <= (dates[-1] if dates else "")]
    index = {d: i for i, d in enumerate(cal)}
    reports = {}
    for rule in rules:
        hold = int(rule["hold"])
        cash = float(fm.CAPITAL)
        pos: dict[str, dict] = {}
        daily = []
        fires = 0
        start = float(fm.CAPITAL)
        for date in dates:
            for ticker in list(pos):
                if pos[ticker]["exit"] != date:
                    continue
                px = (closes.get((ticker, date)) or {}).get("close")
                if px is None:
                    continue
                shares = pos[ticker]["shares"]
                fee = shares * float(px) * 0.00075
                cash -= shares * float(px) + fee
                pos.pop(ticker)
            flagged = flags.get(rule["id"], {}).get(date)
            fresh = [] if flagged is None else [
                t for t in flagged if t not in pos and (closes.get((t, date)) or {}).get("close")
            ]
            if fresh and cash > 0:
                budget = cash / float(len(fresh))
                exit_i = index.get(date)
                exit_date = None
                if exit_i is not None and exit_i + hold < len(cal):
                    exit_date = cal[exit_i + hold]
                elif exit_i is not None:
                    # Cover falls after this window. Stay open and mark at the last close.
                    exit_date = ""
                for ticker in sorted(fresh):
                    if exit_date is None:
                        break
                    px = float(closes[(ticker, date)]["close"])
                    shares = int(budget // px)
                    if shares < 1:
                        continue
                    notion = shares * px
                    fee = notion * 0.00075 + notion * 0.003
                    if notion + fee > cash:
                        continue
                    cash += notion - fee
                    pos[ticker] = {"shares": shares, "entry": px, "exit": exit_date}
                    fires += 1
            equity = _short_equity(cash, pos, closes, date)
            daily.append({
                "date": date,
                "ret_pct": round(100.0 * (equity / start - 1.0), 4) if start else 0.0,
                "equity": round(equity, 4),
            })
            start = equity
        end = daily[-1]["equity"] if daily else fm.CAPITAL
        reports[rule["id"]] = {
            "id": rule["id"],
            "fires": fires,
            "after_fees_return": round(100.0 * (end / fm.CAPITAL - 1.0), 3),
            "daily": daily,
            "costs": "15bp round trip plus 0.3 percent borrow",
            "research": True,
        }
    return reports


def _short_equity(cash: float, pos: dict, closes: dict, date: str) -> float:
    mark = cash
    for ticker, lot in pos.items():
        px = (closes.get((ticker, date)) or {}).get("close")
        if px is None:
            px = lot["entry"]
        mark -= lot["shares"] * float(px)
    return mark
