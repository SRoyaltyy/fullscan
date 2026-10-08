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
               one row per cell (THEME_RADAR_CELLS); research-only shorts,
               never traded (``research_only: true``, tickers in ``shorts``)

Status: OK (decision made, buys/sells listed), SIT (decision made, nothing
to trade), MISSING (no plan; the reason is given). WAIT is used only for a
Theme Radar cell whose plan file has not landed yet and whose 09:00 ET
deadline on D has not passed; after 09:00 ET a missing file is MISSING.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import sys
import urllib.error
import urllib.request
from datetime import datetime, time
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
# Theme Radar's three research cells (theme-radar research/shadow_log/
# plan_build.py CELLS). One status row per cell, every day.
THEME_RADAR_CELLS = ("fpe_delta_t3_earn_today_3d", "fresh_dcp_t1_ep_ge03_2d",
                     "fresh_dcp_t1_avoid_ah_3d")
THEME_RADAR_DEADLINE = time(9, 0)    # no plan file by 09:00 ET on D -> MISSING
THEME_RADAR_PREOPEN = time(9, 30)    # a plan built after the open is not a pre-open plan
THEME_RADAR_NOTE = "RESEARCH ONLY, not traded (Theme Radar shadow short)"
STATUSES = ("OK", "SIT", "WAIT", "MISSING")
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


class FetchError(Exception):
    """The plan URL could not be read for a reason other than 404."""


def _fetch(url: str) -> str | None:
    """Plan text, or None on 404 (no plan). Other failures raise FetchError."""
    last = ""
    for _ in range(3):
        try:
            with urllib.request.urlopen(url, timeout=20) as r:
                return r.read().decode("utf-8")
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            last = f"HTTP {e.code}"
        except Exception as e:  # noqa: BLE001 - network hiccup, retry
            last = type(e).__name__
    raise FetchError(last)


def _tr_row(cell, status, shorts=(), source="", note=""):
    r = _row(f"theme_radar:{cell}", "theme_radar", status, (), (), source,
             f"{THEME_RADAR_NOTE}. {note}".strip())
    r["research_only"] = True
    r["shorts"] = list(shorts)
    return r


def _past_deadline(day: str, now: datetime, at: time) -> bool:
    cut = datetime.combine(datetime.fromisoformat(day).date(), at, tzinfo=ET)
    return now >= cut


def from_theme_radar(day: str, fetch=_fetch, now: datetime | None = None) -> list[dict]:
    """One row per Theme Radar cell from plan_<D>.csv on theme-radar main.

    OK      plan has rows for the cell (the shorts are listed)
    SIT     plan landed and the cell fired nothing (``# status=no_fires``)
    WAIT    no plan file yet, before 09:00 ET on D
    MISSING no plan file at/after 09:00 ET on D (or a plan built after 09:30 ET)
    """
    now = now or datetime.now(ET)
    url = THEME_RADAR_URL.format(d=day)
    try:
        text = fetch(url)
    except FetchError as e:
        text, err = None, str(e)
    else:
        err = ""
    if text is None:
        late = _past_deadline(day, now, THEME_RADAR_DEADLINE)
        why = f"plan fetch failed ({err})" if err else "no plan file"
        st = "MISSING" if late else "WAIT"
        why += " by 09:00 ET" if late else " yet (due ~05:20 ET, MISSING at 09:00 ET)"
        return [_tr_row(c, st, source=url, note=why) for c in THEME_RADAR_CELLS]
    meta = [ln[1:].strip() for ln in text.splitlines() if ln.startswith("#")]
    body = [ln for ln in text.splitlines() if ln.strip() and not ln.startswith("#")]
    status = next((m for m in meta if m.startswith("status=")), "")
    built = ""
    for m in meta:
        for tok in m.split():
            if tok.startswith("built_at_utc="):
                built = tok.split("=", 1)[1]
    late_build = False
    if built:
        try:
            b = datetime.fromisoformat(built.replace("Z", "+00:00")).astimezone(ET)
            late_build = b >= datetime.combine(datetime.fromisoformat(day).date(),
                                               THEME_RADAR_PREOPEN, tzinfo=ET)
            built = b.strftime("%H:%M ET")
        except ValueError:
            pass
    cells: dict[str, list[str]] = {}
    for r in csv.DictReader(io.StringIO("\n".join(body))):
        t = (r.get("ticker") or "").strip().upper()
        if t:
            cells.setdefault((r.get("cell") or "").strip() or "unknown", []).append(t)
    names = list(THEME_RADAR_CELLS) + sorted(c for c in cells if c not in THEME_RADAR_CELLS)
    rows = []
    for c in names:
        shorts = sorted(set(cells.get(c, [])))
        tag = f"plan built {built}" if built else "plan landed"
        if late_build:
            rows.append(_tr_row(c, "MISSING", shorts, url,
                                f"{tag}, after 09:30 ET: not a pre-open plan"))
        elif shorts:
            rows.append(_tr_row(c, "OK", shorts, url, f"{tag}; {len(shorts)} short(s) at the open"))
        else:
            rows.append(_tr_row(c, "SIT", (), url,
                                f"{tag}; {status or 'status=no_fires'}; no fires for this cell"))
    return rows


def build(root: Path, day: str, fetch=_fetch, now: datetime | None = None) -> dict:
    now = now or datetime.now(ET)
    rows = (from_tickets(root, day) + from_preopen(root, day) + from_h1(root, day)
            + from_webull_sim(root, day) + from_theme_radar(day, fetch, now))
    counts: dict[str, dict[str, int]] = {}
    for r in rows:
        c = counts.setdefault(r["family"], {"OK": 0, "SIT": 0, "MISSING": 0})
        c[r["status"]] = c.get(r["status"], 0) + 1
    return {"date": day, "generated_at": now.isoformat(timespec="seconds"),
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
        if r["status"] in ("MISSING", "WAIT") and r["family"] != "webull_sim":
            print(f"{r['status']} {r['name']}: {r['note']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
