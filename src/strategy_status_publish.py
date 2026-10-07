"""Publish every strategy's sealed buy/sell list to the strategy-status page.

Wrapper module. It ONLY READS sealed records and writes one derived file,
``data/strategy_status/<D>.json`` (plus ``latest.json``). It never touches a
sealed row, a book, an ENGINE_SHA256-pinned file, or anything under
``dashboard/`` (so it cannot clobber the Pages deploy patches, see #511).
The page polls raw.githubusercontent main, so a commit of this file is the
publish; no batch Deploy is needed.

Sources, in page order:
  factor_mine  data/day_board/D_strategy_tickets.json (family factor_mine;
               roster also from the latest earlier ticket, so a recipe that
               dropped out shows MISSING by name)
  *_preopen    data/factor_mine/preopen/seals/D.json
  h1           research/hot_n4_clean_v4/forward_h1/h1_log.jsonl kind=plan
  flatten / paper / excel / stock_book   same ticket file
  webull_sim   data/webull_sim/days.jsonl rows for D
  theme_radar  SRoyaltyy/theme-radar research/shadow_log/plans/plan_D.csv

Status: OK (decision made, buys/sells listed), SIT (decision made, nothing
to trade), MISSING (no plan; the reason is given).
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import sys
import urllib.request
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from src.preopen_plan_check import (BOARD, H1_LOG, NO_INPUT, PREOPEN_SLEEVES,
                                    SEALS, fm_roster)

ROOT = Path(__file__).resolve().parent.parent
ET = ZoneInfo("America/New_York")
OUT_DIR = Path("data/strategy_status")
WEBULL_DAYS = Path("data/webull_sim/days.jsonl")
THEME_RADAR_URL = ("https://raw.githubusercontent.com/SRoyaltyy/theme-radar/main/"
                   "research/shadow_log/plans/plan_{d}.csv")
# ticket statuses that are a made decision (no_session_signals = excel had
# zero signals for the session: a sit, not a missing plan)
MADE = ("ok", "sit", "no_session_signals")
TICKET_FAMILY = {"flatten": "flatten_robust", "paper": "paper",
                 "excel": "excel_bot", "stock_book": "stock_book"}


def _load(p: Path):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _tk(items) -> list[str]:
    out = []
    for it in items or []:
        if isinstance(it, dict):
            t = it.get("ticker")
            if t:
                sh = it.get("shares")
                out.append(f"{t} {sh}" if sh not in (None, "") else str(t))
        elif it:
            out.append(str(it))
    return out


def _row(name, family, status, buys=(), sells=(), source="", note=""):
    return {"name": name, "family": family, "status": status,
            "buys": list(buys), "sells": list(sells),
            "source": source, "note": str(note or "")[:200]}


def from_tickets(root: Path, day: str) -> list[dict]:
    path = BOARD / f"{day}_strategy_tickets.json"
    doc = _load(root / path)
    strat = (doc or {}).get("strategies") or {}
    rows = []
    roster = set(fm_roster(root, day))
    names = sorted(roster | {k for k, v in strat.items()
                             if isinstance(v, dict) and v.get("family") == "factor_mine"})
    for n in names:
        rows.append(_ticket_row(n, "factor_mine", strat.get(n), str(path), doc))
    for k, v in strat.items():
        if isinstance(v, dict) and v.get("family") in TICKET_FAMILY:
            rows.append(_ticket_row(k, TICKET_FAMILY[v["family"]], v, str(path), doc))
    if doc is None:
        for fam in ("flatten_robust", "paper", "excel_bot"):
            rows.append(_row(fam, fam, "MISSING", source=str(path), note="no sealed ticket"))
    return rows


def _ticket_row(name, family, v, src, doc):
    if doc is None:
        return _row(name, family, "MISSING", source=src, note="no sealed ticket")
    if not isinstance(v, dict):
        return _row(name, family, "MISSING", source=src, note="absent from ticket")
    note = v.get("note") or v.get("reason") or ""
    text = f"{note} {v.get('status', '')}".lower()
    if v.get("status") not in MADE or any(t in text for t in NO_INPUT):
        return _row(name, family, "MISSING", source=src, note=note or v.get("status"))
    buys, sells = _tk(v.get("buy")), _tk(v.get("sell"))
    st = "OK" if (buys or sells) else "SIT"
    return _row(name, family, st, buys, sells, src, note)


def from_preopen(root: Path, day: str) -> list[dict]:
    path = SEALS / f"{day}.json"
    doc = _load(root / path)
    rows = []
    for n in PREOPEN_SLEEVES:
        sl = ((doc or {}).get("sleeves") or {}).get(n)
        if not isinstance(sl, dict):
            rows.append(_row(n, "factor_mine_preopen", "MISSING", source=str(path),
                             note="no pre-open seal" if doc is None else "absent from seal"))
            continue
        buys = _tk(sl.get("picks"))
        sells = _tk(sl.get("sells"))
        st = "OK" if (buys or sells) else "SIT"
        rows.append(_row(n, "factor_mine_preopen", st, buys, sells, str(path),
                         "; ".join(sl.get("reasons") or [])))
    return rows


def from_h1(root: Path, day: str) -> list[dict]:
    plans = []
    try:
        for ln in (root / H1_LOG).read_text(encoding="utf-8").splitlines():
            if ln.strip():
                o = json.loads(ln)
                if o.get("kind") == "plan" and o.get("date") == day:
                    plans.append(o)
    except (OSError, ValueError) as e:
        return [_row("h1", "h1", "MISSING", source=str(H1_LOG), note=f"log unreadable: {e}")]
    if len(plans) != 1:
        return [_row("h1", "h1", "MISSING", source=str(H1_LOG),
                     note="no sealed plan row" if not plans else f"{len(plans)} plan rows")]
    p = plans[0]
    buys, sells = _tk(p.get("picks")), _tk(p.get("planned_sells"))
    return [_row("h1", "h1", "OK" if (buys or sells) else "SIT", buys, sells,
                 str(H1_LOG), f"sealed {p.get('committed_at', '')}")]


def from_webull_sim(root: Path, day: str) -> list[dict]:
    names, today = set(), {}
    try:
        for ln in (root / WEBULL_DAYS).read_text(encoding="utf-8").splitlines():
            if ln.strip():
                o = json.loads(ln)
                names.add(o.get("name"))
                if o.get("date") == day:
                    today[o.get("name")] = o
    except (OSError, ValueError):
        return [_row("webull_sim", "webull_sim", "MISSING", source=str(WEBULL_DAYS),
                     note="days.jsonl unreadable")]
    rows = []
    for n in sorted(x for x in names if x):
        o = today.get(n)
        if o is None:
            rows.append(_row(n, "webull_sim", "MISSING", source=str(WEBULL_DAYS),
                             note=f"no row for {day}"))
            continue
        fills = [f for f in o.get("fills") or [] if f.get("date") in (None, day)]
        buys = _tk([f for f in fills if f.get("side") == "buy"])
        sells = _tk([f for f in fills if f.get("side") in ("sell", "short", "cover")])
        rows.append(_row(n, "webull_sim", "OK" if (buys or sells) else "SIT", buys, sells,
                         str(WEBULL_DAYS), o.get("reason") or o.get("note")))
    return rows


def _fetch(url: str) -> str | None:
    try:
        with urllib.request.urlopen(url, timeout=20) as r:
            return r.read().decode("utf-8")
    except Exception:  # noqa: BLE001 - 404 means no plan
        return None


def from_theme_radar(day: str, fetch=_fetch) -> list[dict]:
    url = THEME_RADAR_URL.format(d=day)
    text = fetch(url)
    if text is None:
        return [_row("theme_radar", "theme_radar", "MISSING", source=url, note="no plan file")]
    body = [ln for ln in text.splitlines() if ln.strip() and not ln.startswith("#")]
    status = next((ln[2:].strip() for ln in text.splitlines() if ln.startswith("# status=")), "")
    cells: dict[str, list[str]] = {}
    for r in csv.DictReader(io.StringIO("\n".join(body))):
        cells.setdefault(r.get("cell") or "theme_radar", []).append(r.get("ticker") or "")
    if not cells:
        return [_row("theme_radar", "theme_radar", "SIT", source=url, note=status or "no_fires")]
    return [_row(f"theme_radar:{c}", "theme_radar", "OK", sorted(set(t)), [], url, status)
            for c, t in sorted(cells.items())]


def build(root: Path, day: str, fetch=_fetch) -> dict:
    rows = (from_tickets(root, day) + from_preopen(root, day) + from_h1(root, day)
            + from_webull_sim(root, day) + from_theme_radar(day, fetch))
    counts: dict[str, dict[str, int]] = {}
    for r in rows:
        c = counts.setdefault(r["family"], {"OK": 0, "SIT": 0, "MISSING": 0})
        c[r["status"]] += 1
    return {"date": day, "generated_at": datetime.now(ET).isoformat(timespec="seconds"),
            "counts": counts, "strategies": rows}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--date", default="")
    p.add_argument("--root", default=str(ROOT))
    a = p.parse_args(argv)
    day = a.date or datetime.now(ET).date().isoformat()
    root = Path(a.root)
    doc = build(root, day)
    out = root / OUT_DIR
    out.mkdir(parents=True, exist_ok=True)
    text = json.dumps(doc, indent=1, sort_keys=True) + "\n"
    (out / f"{day}.json").write_text(text, encoding="utf-8")
    (out / "latest.json").write_text(text, encoding="utf-8")
    for fam, c in sorted(doc["counts"].items()):
        print(f"[strategy-status] {day} {fam}: {c}")
    for r in doc["strategies"]:
        if r["status"] == "MISSING" and r["family"] != "webull_sim":
            print(f"MISSING {r['name']}: {r['note']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
