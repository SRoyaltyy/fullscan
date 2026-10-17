"""Active audit of sealed buy/sell lists for live fullscan strategies.

Fingerprint = session date, side, ticker, size. Equity marks, sit notes,
prices, and would-have-bought lines are not hashed. A baseline row is the
first seal for that strategy and day and is never rewritten. A day with no
sealed buy/sell stays missing; this module does not backfill one.

Theme Radar research shorts live in SRoyaltyy/theme-radar. This repo does
not audit them.

The strategy set is the one ``strategy_status_publish`` reads (tickets,
pre-open seals, h1 plans, holdup plans, excel lock, webull sim), limited
to days ``data/past_day_lock`` has already sealed for tickets and webull.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from src import strategy_status_publish as ssp
from src.past_day_lock import sha256_text

ROOT = Path(__file__).resolve().parents[1]
BASELINES = Path("data/buy_sell_audit/baselines.jsonl")
SKIP_THEME = (
    "SKIP theme_radar: Theme Radar owns its own audit "
    "(SRoyaltyy/theme-radar); not checked in this repo"
)


class AuditError(SystemExit):
    def __init__(self, message: str):
        super().__init__(message)


@dataclass(frozen=True)
class Seal:
    family: str
    name: str
    date: str
    sha256: str
    source: str

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.family, self.name, self.date)


def fingerprint(date: str, buys: list[str], sells: list[str]) -> str:
    """Stable hash of buy/sell names, sides, sizes, and the session date."""
    lines = [f"{date}\tbuy\t{item}" for item in buys]
    lines += [f"{date}\tsell\t{item}" for item in sells]
    lines.sort()
    body = ("\n".join(lines) + "\n") if lines else ""
    return sha256_text(body)


def _seal(family: str, name: str, date: str, buys, sells, source: str) -> Seal:
    return Seal(family, name, date, fingerprint(date, list(buys), list(sells)), source)


def _load_json(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _lock_rows(root: Path) -> list[dict]:
    path = root / "data" / "past_day_lock" / "manifest.jsonl"
    if not path.is_file():
        return []
    rows = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        if raw == "":
            raise AuditError("buy-sell audit: blank past_day_lock line")
        obj = json.loads(raw)
        if isinstance(obj, dict):
            rows.append(obj)
    return rows


def _plans(path: Path) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {}
    if not path.is_file():
        return out
    for raw in path.read_text(encoding="utf-8").splitlines():
        if not raw.strip():
            continue
        obj = json.loads(raw)
        if obj.get("kind") == "plan" and obj.get("date"):
            out.setdefault(str(obj["date"]), []).append(obj)
    return out


def _plan_seal(family: str, name: str, date: str, plans: dict[str, list[dict]],
               source: str) -> tuple[Seal | None, str | None]:
    rows = plans.get(date) or []
    if len(rows) != 1:
        why = "no sealed plan row" if not rows else f"{len(rows)} plan rows"
        return None, f"MISSING {family} {name} {date}: {why}"
    plan = rows[0]
    buys, sells = ssp._tk(plan.get("picks")), ssp._tk(plan.get("planned_sells"))
    return _seal(family, name, date, buys, sells, source), None


def _ticket_rows(root: Path, day: str) -> list[dict]:
    """Same families as ``from_tickets``, without the excel git walk.

    Excel orders are read from the lock entry below. Commit-time proof
    stays in ``strategy_status_publish.from_excel_lock``.
    """
    path = ssp.BOARD / f"{day}_strategy_tickets.json"
    doc = ssp._load(root / path)
    strat = (doc or {}).get("strategies") or {}
    rows = []
    roster = set(ssp.fm_roster(root, day))
    names = sorted(roster | {
        key for key, val in strat.items()
        if isinstance(val, dict) and val.get("family") == "factor_mine"
    })
    for name in names:
        rows.append(ssp._ticket_row(
            name, "factor_mine", strat.get(name), str(path), doc))
    for key, val in strat.items():
        if isinstance(val, dict) and val.get("family") in ssp.TICKET_FAMILY:
            rows.append(ssp._ticket_row(
                key, ssp.TICKET_FAMILY[val["family"]], val, str(path), doc))
    if doc is None:
        for fam in ("flatten_robust", "paper"):
            rows.append(ssp._row(fam, fam, "MISSING", source=str(path),
                                 note="no sealed ticket"))
    return [row for row in rows if row.get("family") != "excel_bot"]


def _excel_row(root: Path, day: str) -> dict:
    from src.skip_if_good import _prev_weekday

    prior = _prev_weekday(day)
    src = str(ssp.EXCEL_MANIFEST)
    doc = _load_json(root / ssp.EXCEL_MANIFEST)
    if not isinstance(doc, dict):
        return ssp._row("excel_all", "excel_bot", "MISSING", source=src,
                        note=f"missing {src}")
    locks = [entry for entry in (doc.get("entries") or [])
             if isinstance(entry, dict) and entry.get("kind") == "lock"
             and str(entry.get("signal_date") or "") == prior]
    if not locks:
        return ssp._row("excel_all", "excel_bot", "MISSING", source=src,
                        note=f"missing prior-session kind:lock for {prior}")
    entry = locks[-1]
    buys: list[str] = []
    for item in entry.get("pick_ids") or []:
        if isinstance(item, (list, tuple)) and item:
            ticker = str(item[0] or "").strip().upper()
            card = str(item[1]).strip() if len(item) > 1 and item[1] else ""
            if ticker:
                buys.append(f"{ticker} {card}".strip())
        elif isinstance(item, str) and item.strip():
            buys.append(item.strip().upper())
    status = "OK" if buys else "SIT"
    return ssp._row("excel_all", "excel_bot", status, buys, (), src,
                    note="prior-session kind:lock pick_ids")


def _take(row: dict, seals: list[Seal], missing: list[str]) -> None:
    if row.get("family") == "theme_radar" or str(row.get("name") or "").startswith("theme_radar:"):
        return
    if row.get("status") not in ("OK", "SIT"):
        missing.append(
            f"MISSING {row.get('family')} {row.get('name')} {row.get('date', '')}: "
            f"{row.get('note') or row.get('status')}"
        )
        return
    seals.append(_seal(
        str(row["family"]), str(row["name"]), str(row["date"]),
        row.get("buys") or [], row.get("sells") or [], str(row.get("source") or ""),
    ))


def _webull_seals(root: Path, locked: list[dict], missing: list[str]) -> list[Seal]:
    path = root / ssp.WEBULL_DAYS
    if not path.is_file():
        missing.append(f"SKIP webull_sim: missing {ssp.WEBULL_DAYS}")
        return []
    idx: dict[tuple[str, str], list[str]] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        if not raw.strip():
            continue
        obj = json.loads(raw)
        idx.setdefault((str(obj.get("name") or ""), str(obj.get("date") or "")), []).append(raw)
    seals = []
    for row in locked:
        if row.get("kind") != "day" or row.get("record") != "webull_sim":
            continue
        name, date = str(row.get("name") or ""), str(row.get("date") or "")
        if name.startswith("theme_radar"):
            continue
        lines = idx.get((name, date), [])
        if len(lines) != 1 or sha256_text(lines[0] + "\n") != row.get("sha256"):
            missing.append(f"DRIFT webull_sim {name} {date}: sealed days.jsonl line moved")
            continue
        obj = json.loads(lines[0])
        fills = [fill for fill in (obj.get("fills") or [])
                 if fill.get("date") in (None, date)]
        buys = ssp._tk([fill for fill in fills if fill.get("side") == "buy"])
        sells = ssp._tk([fill for fill in fills
                         if fill.get("side") in ("sell", "short", "cover")])
        seals.append(_seal("webull_sim", name, date, buys, sells, str(ssp.WEBULL_DAYS)))
    return seals


def collect(root: Path) -> tuple[list[Seal], list[str]]:
    """Sealed buy/sell rows, plus SKIP/MISSING/DRIFT notes that are not seals."""
    root = Path(root)
    notes = [SKIP_THEME]
    notes.append(
        "SKIP flatten card equity: 01_daily/*_flatten_card.md marks stay "
        "outside the fingerprint; flatten_robust buy/sell is the sealed ticket"
    )
    seals: list[Seal] = []
    locked = _lock_rows(root)
    ticket_days = sorted({
        str(row.get("date") or "")
        for row in locked
        if row.get("kind") == "day" and row.get("record") == "strategy_tickets" and row.get("date")
    })
    ticket_sha = {
        str(row.get("date")): str(row.get("sha256") or "")
        for row in locked
        if row.get("kind") == "day" and row.get("record") == "strategy_tickets"
    }
    for day in ticket_days:
        path = root / "data" / "day_board" / f"{day}_strategy_tickets.json"
        if not path.is_file():
            notes.append(f"DRIFT strategy_tickets {day}: sealed file missing")
            continue
        text = path.read_text(encoding="utf-8")
        if sha256_text(text) != ticket_sha.get(day):
            notes.append(
                f"DRIFT strategy_tickets {day}: sealed file hash moved vs past_day_lock"
            )
            continue
        for row in _ticket_rows(root, day):
            row = dict(row)
            row["date"] = day
            _take(row, seals, notes)
        excel = _excel_row(root, day)
        excel = dict(excel)
        excel["date"] = day
        _take(excel, seals, notes)
        for row in ssp.from_preopen(root, day):
            row = dict(row)
            row["date"] = day
            _take(row, seals, notes)

    h1_path = root / ssp.H1_LOG
    h1_plans = _plans(h1_path)
    if not h1_path.is_file():
        notes.append(f"SKIP h1: missing {ssp.H1_LOG}")
    else:
        for day in sorted(set(ticket_days) | set(h1_plans)):
            seal, miss = _plan_seal("h1", "h1", day, h1_plans, str(ssp.H1_LOG))
            if seal:
                seals.append(seal)
            elif day in ticket_days and miss:
                notes.append(miss)

    hold_path = root / ssp.HOLDUP_LOG
    hold_plans = _plans(hold_path)
    if not hold_path.is_file():
        notes.append(f"SKIP holdup: missing {ssp.HOLDUP_LOG}")
    else:
        for day in sorted(set(ticket_days) | set(hold_plans)):
            seal, miss = _plan_seal("holdup", "holdup", day, hold_plans, str(ssp.HOLDUP_LOG))
            if seal:
                seals.append(seal)
            elif day in ticket_days and miss:
                notes.append(miss)

    seals.extend(_webull_seals(root, locked, notes))
    # One row per strategy-day. A duplicate means two sources disagreed.
    seen: dict[tuple[str, str, str], Seal] = {}
    for seal in seals:
        prev = seen.get(seal.key)
        if prev is not None and prev.sha256 != seal.sha256:
            notes.append(
                f"DRIFT {seal.family} {seal.name} {seal.date}: "
                f"two sealed sources disagree"
            )
            continue
        seen[seal.key] = seal
    return list(seen.values()), notes


def baseline_line(seal: Seal) -> str:
    obj = {
        "date": seal.date,
        "family": seal.family,
        "kind": "baseline",
        "name": seal.name,
        "sha256": seal.sha256,
        "source": seal.source,
    }
    return json.dumps(obj, separators=(",", ":"), sort_keys=True)


def load_baselines(path: Path) -> dict[tuple[str, str, str], str]:
    if not path.is_file():
        return {}
    out = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        if raw == "":
            raise AuditError("buy-sell audit: blank baseline line")
        obj = json.loads(raw)
        if obj.get("kind") != "baseline":
            continue
        key = (str(obj.get("family") or ""), str(obj.get("name") or ""),
               str(obj.get("date") or ""))
        if key in out and out[key] != obj.get("sha256"):
            raise AuditError(f"buy-sell audit: two baselines for {key}")
        out[key] = str(obj.get("sha256") or "")
    return out


def diff(seals: list[Seal], baselines: dict[tuple[str, str, str], str]) -> tuple[list[str], list[str]]:
    by_key = {seal.key: seal for seal in seals}
    drifts, news = [], []
    for key, sha in baselines.items():
        seal = by_key.get(key)
        label = f"{key[0]} {key[1]} {key[2]}"
        if seal is None:
            drifts.append(f"DRIFT {label}: sealed buy/sell record removed")
        elif seal.sha256 != sha:
            drifts.append(
                f"DRIFT {label}: buy/sell fingerprint moved "
                f"(first seal {sha[:12]} current {seal.sha256[:12]})"
            )
    for seal in seals:
        if seal.key not in baselines:
            news.append(
                f"NEW {seal.family} {seal.name} {seal.date}: no first-seal baseline"
            )
    return drifts, news


def _git_text(root: Path, rev: str, rel: str) -> str:
    proc = subprocess.run(
        ["git", "show", f"{rev}:{rel}"],
        cwd=root, capture_output=True, check=False,
    )
    if proc.returncode != 0:
        return ""
    return proc.stdout.decode("utf-8")


def assert_append_only(old: str, new: str) -> None:
    old_lines = old.splitlines()
    new_lines = new.splitlines()
    if any(line == "" for line in old_lines + new_lines):
        raise AuditError("buy-sell audit: blank baseline line")
    if new_lines[: len(old_lines)] != old_lines:
        raise AuditError(
            "buy-sell audit: baselines manifest was rewritten "
            "(append-only; first seal is never edited)"
        )


def _json_plans(path: Path) -> dict[str, list[dict]]:
    """Plan rows from a dashboard log.json array (not jsonl)."""
    doc = _load_json(path)
    rows = doc if isinstance(doc, list) else []
    out: dict[str, list[dict]] = {}
    for obj in rows:
        if isinstance(obj, dict) and obj.get("kind") == "plan" and obj.get("date"):
            out.setdefault(str(obj["date"]), []).append(obj)
    return out


def page_drifts(root: Path, seals: list[Seal]) -> list[str]:
    """In-repo published artifacts the dashboards render. Not a live HTTP fetch."""
    by_key = {seal.key: seal for seal in seals}
    drifts = []
    status_dir = root / "data" / "strategy_status"
    published_dates = set()
    if status_dir.is_dir():
        for path in sorted(status_dir.glob("????-??-??.json")):
            doc = _load_json(path)
            if not isinstance(doc, dict):
                continue
            day = str(doc.get("date") or path.name[:10])
            published_dates.add(day)
            seen = set()
            for row in doc.get("strategies") or []:
                if not isinstance(row, dict):
                    continue
                if row.get("family") == "theme_radar" or str(row.get("name") or "").startswith("theme_radar"):
                    continue
                if row.get("status") not in ("OK", "SIT"):
                    continue
                key = (str(row.get("family") or ""), str(row.get("name") or ""), day)
                seen.add(key)
                seal = by_key.get(key)
                got = fingerprint(day, list(row.get("buys") or []), list(row.get("sells") or []))
                label = f"{key[0]} {key[1]} {day}"
                if seal is None:
                    drifts.append(f"DRIFT page {label}: published buy/sell has no sealed file")
                elif got != seal.sha256:
                    drifts.append(
                        f"DRIFT page {label}: published buy/sell != sealed file "
                        f"({seal.sha256[:12]} page {got[:12]})"
                    )
            for seal in seals:
                if seal.date == day and seal.key not in seen:
                    drifts.append(
                        f"DRIFT page {seal.family} {seal.name} {day}: "
                        f"published strategy-status row missing"
                    )
    h1_page = _json_plans(root / "dashboard" / "h1" / "log.json")
    for seal in seals:
        if seal.family != "h1":
            continue
        page, miss = _plan_seal("h1", "h1", seal.date, h1_page, "dashboard/h1/log.json")
        if page is None or page.sha256 != seal.sha256:
            drifts.append(
                f"DRIFT page h1 h1 {seal.date}: dashboard/h1 calendar != sealed plan"
            )
    hold_page = _json_plans(root / "dashboard" / "holdup" / "log.json")
    for seal in seals:
        if seal.family != "holdup":
            continue
        page, _miss = _plan_seal("holdup", "holdup", seal.date, hold_page,
                                 "dashboard/holdup/log.json")
        if page is None or page.sha256 != seal.sha256:
            drifts.append(
                f"DRIFT page holdup holdup {seal.date}: dashboard/holdup calendar != sealed plan"
            )
    pre = _load_json(root / "dashboard" / "factor-mine" / "preopen.json") or {}
    page_pre: dict[tuple[str, str], str] = {}
    for name, sleeve in (pre.get("sleeves") or {}).items():
        for day in sleeve.get("days") or []:
            date = str(day.get("date") or "")
            if not date:
                continue
            buys = [str(item) for item in day.get("buys") or []]
            sells = [str(item) for item in day.get("sells") or []]
            page_pre[(str(name), date)] = fingerprint(date, buys, sells)
            if (buys or sells) and ("factor_mine_preopen", str(name), date) not in by_key:
                drifts.append(
                    f"DRIFT page factor_mine_preopen {name} {date}: "
                    f"calendar lists buys/sells with no seal file"
                )
    for seal in seals:
        if seal.family != "factor_mine_preopen":
            continue
        got = page_pre.get((seal.name, seal.date))
        if got != seal.sha256:
            drifts.append(
                f"DRIFT page factor_mine_preopen {seal.name} {seal.date}: "
                f"preopen calendar != sealed file"
            )
    return drifts


def seed(path: Path, seals: list[Seal]) -> list[str]:
    """Append baselines for strategy-days that do not have one. Never rewrite."""
    existing_text = path.read_text(encoding="utf-8") if path.is_file() else ""
    if existing_text and not existing_text.endswith("\n"):
        raise AuditError("buy-sell audit: baselines manifest missing trailing newline")
    known = load_baselines(path)
    fresh = []
    for seal in sorted(seals, key=lambda item: item.key):
        prev = known.get(seal.key)
        if prev is None:
            fresh.append(seal)
        elif prev != seal.sha256:
            raise AuditError(
                f"DRIFT {seal.family} {seal.name} {seal.date}: "
                f"refusing to rewrite first seal {prev[:12]}"
            )
    if not fresh:
        return []
    path.parent.mkdir(parents=True, exist_ok=True)
    blob = "".join(baseline_line(seal) + "\n" for seal in fresh)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(blob)
    assert_append_only(existing_text, path.read_text(encoding="utf-8"))
    return [f"SEED {seal.family} {seal.name} {seal.date}" for seal in fresh]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Audit sealed buy/sell fingerprints")
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--seed", action="store_true")
    parser.add_argument("--pages", action="store_true",
                        help="Also compare in-repo dashboard artifacts (not live HTTP)")
    parser.add_argument("--check-against", default="",
                        help="Git rev whose baselines file must be a prefix")
    parser.add_argument("--date", default="", help="Printed focus day; every sealed day is still checked")
    parser.add_argument("--root", default=str(ROOT))
    parser.add_argument("--baselines", default="")
    args = parser.parse_args(argv)
    root = Path(args.root)
    path = Path(args.baselines) if args.baselines else root / BASELINES
    seals, notes = collect(root)
    drifts = [note for note in notes if note.startswith("DRIFT ")]
    for note in notes:
        if not note.startswith("DRIFT "):
            print(note)
    if args.check_against:
        rel_text = path.read_text(encoding="utf-8") if path.is_file() else ""
        assert_append_only(_git_text(root, args.check_against, BASELINES.as_posix()), rel_text)
    if args.seed:
        try:
            seeded = seed(path, seals)
        except AuditError as exc:
            print(exc)
            return 1
        for line in seeded:
            print(line)
        print(f"[buy-sell-audit] seeded {len(seeded)} new baselines")
    known = load_baselines(path)
    moved, news = diff(seals, known)
    drifts.extend(moved)
    if args.pages:
        drifts.extend(page_drifts(root, seals))
    focus = args.date.strip()
    if focus:
        hit = [seal for seal in seals if seal.date == focus]
        print(f"[buy-sell-audit] session {focus}: {len(hit)} sealed strategy rows")
    for line in drifts + news:
        print(line)
        if line.startswith(("DRIFT ", "NEW ")):
            print(f"::error::{line}")
    print(
        f"[buy-sell-audit] sealed={len(seals)} baselines={len(known)} "
        f"drift={len(drifts)} new={len(news)}"
    )
    if drifts or news:
        print("RED: a past buy/sell fingerprint moved, a sealed row vanished, "
              "or a sealed day has no first-seal baseline. "
              "See docs/buy_sell_audit.md.")
        return 1
    print("PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
