"""Replay from pre-open inputs, NOT sealed. Research only.

Runs ``union_hot_n4_h1`` and ``union_hot_n4_holdup`` through the pinned
engine (``factor_mine_book.simulate_book``) for 2026-09-29 .. 2026-10-06,
one session at a time, from the 2026-09-28 end state printed on main in
``03_scoreboard/factor_mine/<recipe>.md``.

Each day's buys come ONLY from the parent's buy list in the FIRST saved
``data/day_board/<D>_strategy_tickets.json`` (``git show`` of the first
commit), and only if that commit is before 09:30 ET on D. A ticket is
never rebuilt or regenerated. An empty first-saved list is labelled
``no pre-open picks saved`` with the note from the file; held lots carry
forward and follow the recipe's exits. A morning with S <= -3 buys
nothing. Fills are the 09:30 Yahoo open (``ticker_lookback.session_bar``)
with Futubull fees (``paper_trade.load_fees``).

Output: ``data/factor_mine/replay_preopen/replay.json`` and
``dashboard/factor-mine/replay_preopen.json``. Never a scoreboard file,
never the past-day lock manifest.
"""
from __future__ import annotations

import json
import re
import subprocess
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
ET = ZoneInfo("America/New_York")
DAYS = ("2026-09-29", "2026-09-30", "2026-10-01", "2026-10-02",
        "2026-10-05", "2026-10-06")
BASE_DAY = "2026-09-28"
RECIPES = ("union_hot_n4_h1", "union_hot_n4_holdup")
OUT = ROOT / "data" / "factor_mine" / "replay_preopen" / "replay.json"
DASH = ROOT / "dashboard" / "factor-mine" / "replay_preopen.json"
TITLE = "Replay from pre-open inputs, not sealed"
HARD_RED = -3.0
_MONEY = r"\$([\d,]+\.\d+)"


def _git(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], cwd=str(ROOT),
                          capture_output=True, check=False)


def first_saved(rel: str) -> dict:
    """First commit of ``rel`` and its bytes. Never the working tree."""
    log = _git("log", "--reverse", "--format=%H %cI", "HEAD", "--", rel)
    lines = [x for x in log.stdout.decode().splitlines() if x.strip()]
    if not lines:
        return {"path": rel, "commit": None, "committed_at": None,
                "doc": None, "text": ""}
    sha, stamp = lines[0].split(" ", 1)
    raw = _git("show", f"{sha}:{rel}").stdout
    text = raw.decode("utf-8", errors="replace")
    try:
        doc = json.loads(raw.decode("utf-8"))
    except ValueError:
        doc = None
    return {"path": rel, "commit": sha,
            "committed_at": datetime.fromisoformat(stamp.strip()).astimezone(ET),
            "doc": doc, "text": text}


def open_at(day: str) -> datetime:
    y, m, d = (int(x) for x in day.split("-"))
    return datetime(y, m, d, 9, 30, tzinfo=ET)


def _f(text: str) -> float:
    return float(text.replace(",", ""))


def base_state(recipe: str, md_text: str | None = None) -> dict:
    """End state after the 09-28 close, read from the scoreboard on main.

    Open lots are BUY rows not yet sold; the 09-28 CLOSE row gives cash,
    per-name closes, and equity.
    """
    if md_text is None:
        md_text = (ROOT / "03_scoreboard" / "factor_mine" / f"{recipe}.md"
                   ).read_text(encoding="utf-8")
    lots: dict[str, dict] = {}
    close_row = None
    for line in md_text.splitlines():
        m = re.match(r"^\| (\d{4}-\d{2}-\d{2}) (09:30|16:00) ET \| \*\*(\w+)\*\*", line)
        if not m:
            continue
        day, _clock, kind = m.groups()
        if day > BASE_DAY:
            break
        cells = [c.strip() for c in line.split("|")[1:-1]]
        if kind == "BUY":
            t = cells[2].strip("`")
            shares = int(cells[3])
            px = _f(cells[4].lstrip("$"))
            fee = _f(cells[5].lstrip("$"))
            note = cells[9]
            lots[t] = {
                "ticker": t, "shares": shares, "entry_px": px,
                "entry_date": day, "fee_in": fee,
                "notional": shares * px, "cost": shares * px + fee,
                "last_px": px, "peak_px": px, "reason": note,
                "min_hold": 2 if recipe.endswith("holdup") else 1,
            }
        elif kind in ("SELL", "COVER"):
            lots.pop(cells[2].strip("`"), None)
        elif kind == "CLOSE" and day == BASE_DAY:
            close_row = cells
    if close_row is None:
        raise SystemExit(f"replay: no {BASE_DAY} close row for {recipe}")
    cash = _f(re.search(_MONEY, close_row[7]).group(1))
    detail = close_row[9]
    m_eq = re.search(r"close \$([\d,]+\.\d+)", close_row[8])
    equity = _f(m_eq.group(1)) if m_eq else cash
    held = {}
    for m in re.finditer(r"([A-Z][A-Z0-9.\-]*)×(\d+) 09:30 \$[\d,.]+ → close \$([\d,]+\.\d+)", detail):
        t, shares, close = m.group(1), int(m.group(2)), _f(m.group(3))
        lot = dict(lots.get(t) or {})
        if not lot or lot.get("shares") != shares:
            raise SystemExit(f"replay: {recipe} {t}×{shares} on the {BASE_DAY} close has no matching BUY row")
        lot["close_px"] = lot["last_px"] = close
        lot["peak_px"] = max(lot["peak_px"], close)
        held[t] = lot
    lots = held
    marked = cash + sum(l["shares"] * l["close_px"] for l in lots.values())
    if abs(marked - equity) > 0.05:
        raise SystemExit(f"replay: {recipe} {BASE_DAY} rebuilt equity {marked:.2f} != printed {equity:.2f}")
    return {"after": BASE_DAY, "cash": cash, "pos": lots, "yday_equity": equity}


def morning_s(day: str):
    """Morning S as first saved before 09:30 ET: predict md, then weather.

    Same regex as ``sleeve_merge.predict_snapshot``; read from the first
    commit of the file, never the working tree. None when neither file
    was saved before the open.
    """
    info = first_saved(f"01_daily/general/{day}_predict.md")
    at = info["committed_at"]
    if at and at < open_at(day):
        m = re.search(r"Prediction:\s*(UP|DOWN|FLAT).*?total score\s*(-?[\d.]+)",
                      info["text"])
        if m:
            return float(m.group(2)), f"predict md {at.isoformat()}"
    info = first_saved(f"01_daily/weather/{day}_weather.json")
    at = info["committed_at"]
    if at and at < open_at(day):
        v = ((info["doc"] or {}).get("signals") or {}).get("general_score")
        try:
            return float(v), f"weather {at.isoformat()}"
        except (TypeError, ValueError):
            pass
    return None, "no pre-open S saved"


def ticket_day(recipe: str, day: str, s_day) -> dict:
    rel = f"data/day_board/{day}_strategy_tickets.json"
    info = first_saved(rel)
    doc = info["doc"] or {}
    rec = (doc.get("strategies") or {}).get(recipe) or {}
    picks = [str(b.get("ticker")).upper() for b in rec.get("buy") or []
             if isinstance(b, dict) and b.get("ticker")]
    at = info["committed_at"]
    pre_open = bool(at and at < open_at(day))
    s, s_src = morning_s(day)
    label, reason = "pre-open picks saved", ""
    if not info["commit"]:
        label, reason, picks = "no pre-open picks saved", "no ticket file", []
    elif not pre_open:
        label = "no pre-open picks saved"
        reason = f"first ticket commit {at.isoformat()} is not before 09:30 ET"
        picks = []
    elif not picks:
        label = "no pre-open picks saved"
        reason = str(rec.get("note") or (doc.get("look") or {}).get("error")
                     or rec.get("status") or "empty list")
    red = s is not None and float(s) <= HARD_RED
    return {
        "date": day, "ticket_commit": info["commit"],
        "ticket_committed_at": at.isoformat() if at else None,
        "ticket_pre_open": pre_open,
        "look_source": (doc.get("look") or {}).get("source"),
        "saved_picks": picks, "label": label, "reason": reason,
        "s": s, "s_source": s_src, "red_morning": red,
        "buy_list": [] if red else picks,
    }


def synthetic_rows(day: str, picks: list[str]) -> list[dict]:
    """Saved list order as rank: the first pick gets the highest hot score."""
    n = len(picks)
    return [{"ticker": t, "date": day, "sources": ["preopen_ticket"],
             "ohlc_hot_score": float(n - i), "alarm": False, "e_pol": None}
            for i, t in enumerate(picks)]


def replay(recipe: str, *, bars=None, fees=None, tickets=None,
           base: dict | None = None) -> dict:
    from . import factor_mine as fm
    from . import factor_mine_book as fmb
    from . import factor_mine_sequential as fms
    rec = next(dict(r) for r in fm.build_recipes() if r.get("name") == recipe)
    state = base if base is not None else base_state(recipe)
    fees = fees if fees is not None else fm.pt_fees()
    from .webull_sim import nyse_sessions_through
    first = min([BASE_DAY] + [str(l.get("entry_date") or BASE_DAY)
                              for l in (state.get("pos") or {}).values()])
    cal = nyse_sessions_through(first, BASE_DAY)
    days = []
    prior = {"state": state}
    start_eq = state.get("yday_equity")
    for day in DAYS:
        cal.append(day)
        tk = (tickets or {}).get(day) or ticket_day(recipe, day, None)
        rows = synthetic_rows(day, tk["buy_list"])
        panel = fms.one_day_panel(day, rows, cal)
        s = tk.get("s")
        regime = {day: {"predict_score": float("nan") if s is None else float(s)}}
        book = fmb.simulate_book(panel, rec, bars=bars, fees=fees,
                                 regime=regime, resume=prior["state"])
        doc = fms.record_from_book(recipe, day, book)
        doc["mean"] = fms.session_mean(doc.get("equity"), prior)
        days.append({**tk, "buys": doc["buys"], "sells": doc["sells"],
                     "fees": doc["fees"], "cash": doc["cash"],
                     "holdings": doc["holdings"], "equity": doc["equity"],
                     "mean": doc["mean"]})
        prior = doc
    return {"recipe": recipe, "base_day": BASE_DAY, "base_equity": start_eq,
            "base_holdings": sorted(state.get("pos") or {}), "days": days}


def main() -> int:
    doc = {
        "title": TITLE,
        "sealed": False,
        "note": ("Research replay through factor_mine_book.simulate_book. "
                 "Buys only from the first-saved pre-open ticket list; no "
                 "ticket rebuilt. Not a scoreboard row and not in the "
                 "past-day lock."),
        "generated_at": datetime.now(ET).isoformat(timespec="seconds"),
        "recipes": {r: replay(r) for r in RECIPES},
    }
    text = json.dumps(doc, indent=2, sort_keys=True, default=str) + "\n"
    for p in (OUT, DASH):
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text, encoding="utf-8")
    for r, rep in doc["recipes"].items():
        for d in rep["days"]:
            print(r, d["date"], d["label"], d["ticket_committed_at"],
                  "buys", [b["ticker"] for b in d["buys"]],
                  "sells", [b["ticker"] for b in d["sells"]], "eq", d["equity"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
