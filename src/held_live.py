"""Elite marks on OPEN held lots (not the looker list, not closed lots).

Uses existing elite_live_px.quote_book / pull_elite_overview. Soft-fail
if Elite auth is missing. Does not remine, and does not touch
flatten_robust / Webull / hard-red sit.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from . import elite_live_px as elp
from . import factor_mine as fm

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "data" / "factor_mine" / "held_live.json"
DASH_OUT = ROOT / "dashboard" / "factor-mine" / "held_live.json"
PAPER_OUT = ROOT / "data" / "stock_book" / "held_live.json"


def _side_of(row: dict) -> str:
    raw = str(row.get("side") or row.get("lot_side") or "long").lower()
    if raw in ("short", "cover", "sell_short"):
        return "short"
    return "long"


def _shares(row: dict, *keys: str) -> int:
    for k in keys:
        v = row.get(k)
        if v is None or v == "":
            continue
        try:
            n = int(float(v))
        except (TypeError, ValueError):
            continue
        if n:
            return n
    return 0


def _entry(row: dict) -> float | None:
    for k in ("entry", "entry_px", "buy_px", "price"):
        v = fm._finite(row.get(k))
        if v is not None:
            return float(v)
    return None


def replay_open_lots(trades: list[dict] | None) -> list[dict]:
    """FIFO leftover lots from a slim blotter. Ignores looker / closed fills."""
    hold: dict[str, dict] = {}
    for t in trades or []:
        kind = str(t.get("side") or "")
        ticker = str(t.get("ticker") or "").upper()
        if not ticker or kind in ("OPEN", "CLOSE"):
            continue
        shares = _shares(t, "shares")
        if not shares:
            continue
        if kind in ("SELL", "COVER"):
            hold.pop(ticker, None)
            continue
        if kind in ("BUY", "SHORT"):
            hold[ticker] = {
                "ticker": ticker,
                "side": "short" if kind == "SHORT" or _side_of(t) == "short" else "long",
                "shares": shares,
                "entry": _entry(t),
                "entry_date": t.get("date"),
            }
    return [hold[k] for k in sorted(hold)]


def open_held_lots(payload: dict, name: str) -> list[dict]:
    """OPEN lots still on the $10k book. Not the looker list. Not closed lots."""
    if not name or not payload:
        return []
    bk = (payload.get("books") or {}).get(name) or {}
    raw = list(bk.get("open") or [])
    out: list[dict] = []
    if raw:
        for row in raw:
            t = str(row.get("ticker") or "").upper()
            sh = _shares(row, "shares")
            if not t or not sh:
                continue
            out.append({
                "ticker": t,
                "side": _side_of(row),
                "shares": sh,
                "entry": _entry(row),
                "entry_date": row.get("entry_date"),
            })
        return out
    days = (payload.get("daily") or {}).get(name) or []
    if days:
        last = days[-1] or {}
        lots = last.get("lots") or []
        if lots:
            for row in lots:
                t = str((row.get("ticker") if isinstance(row, dict) else "") or "").upper()
                if not t:
                    continue
                sh = _shares(row, "shares", "shares_close") if isinstance(row, dict) else 0
                if not sh:
                    continue
                out.append({
                    "ticker": t,
                    "side": _side_of(row) if isinstance(row, dict) else "long",
                    "shares": sh,
                    "entry": _entry(row) if isinstance(row, dict) else None,
                })
            if out:
                return out
        for m in last.get("marks") or []:
            t = str(m.get("ticker") or "").upper()
            sh = _shares(m, "shares_close", "shares")
            if not t or not sh:
                continue
            out.append({
                "ticker": t,
                "side": _side_of(m),
                "shares": sh,
                "entry": _entry(m),
            })
        if out:
            return out
    return replay_open_lots(bk.get("trades"))


def featured_names(payload: dict) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for n in list(payload.get("featured") or []) + [
        "combo_sh_5050_shared", "combo_sh_macd_5050_shared",
    ]:
        if n and n not in seen:
            seen.add(n)
            out.append(n)
    return out


def stamp_payload_holds(payload: dict, *, pull_live: bool | None = None) -> dict:
    """Attach Elite marks for leftover OPEN lots. Soft-fail. No remine."""
    date = fm.live_session_date(payload) or payload.get("live_session") or ""
    if not date:
        try:
            date = elp.now_et().strftime("%Y-%m-%d")
        except Exception:
            date = str(payload.get("to_date") or "")
    book = elp.quote_book(date, pull_live=pull_live)
    names = featured_names(payload)
    picked = []
    for n in names:
        picked.extend(open_held_lots(payload, n))
    tickers = sorted({r["ticker"] for r in picked if r.get("ticker")})
    opens: dict[str, float] = {}
    if tickers:
        try:
            opens = elp.official_opens(tickers, date)
        except Exception:
            opens = {}
    sleeves: dict[str, list[dict]] = {}
    for n in names:
        lots = open_held_lots(payload, n)
        sleeves[n] = elp.stamp_held_lots(lots, book, opens=opens)
    src = book.get("src") or "missing"
    if elp.is_live_src(src) is False and "elite_live" in str(src):
        # quote_book already refused Theme Radar; keep the stamped src.
        pass
    payload["held_live"] = {
        "date": date,
        "src": src,
        "at": book.get("at"),
        "banner": elp.banner_text(book),
        "error": book.get("error"),
        "n": sum(len(v) for v in sleeves.values()),
        "sleeves": sleeves,
        "prices": book.get("prices") or {},
        "opens": opens,
    }
    payload["quote"] = {
        "src": src,
        "at": book.get("at"),
        "n": book.get("n"),
        "error": book.get("error"),
    }
    return payload


def write_held_live(payload: dict | None = None, *,
                    pull_live: bool | None = None) -> dict:
    """Sidecar JSON for six-metrics / paper-book pollers."""
    if payload is None:
        try:
            payload = fm.load_dash_payload()
        except Exception:
            payload = {}
        if not payload and fm.OUT_JSON.is_file():
            try:
                payload = json.loads(fm.OUT_JSON.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                payload = {}
    payload = fm.attach_live_session(dict(payload or {}))
    stamp_payload_holds(payload, pull_live=pull_live)
    blob = payload.get("held_live") or {}
    text = json.dumps(blob, indent=2)
    for path in (OUT, DASH_OUT, PAPER_OUT):
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text + "\n", encoding="utf-8")
        except OSError:
            pass
    return blob


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--no-live", action="store_true",
                    help="skip Elite pull; use last same-session export")
    args = ap.parse_args(argv)
    pull = False if args.no_live else None
    blob = write_held_live(pull_live=pull)
    src = blob.get("src")
    print(f"[held-live] date={blob.get('date')} src={src} "
          f"n={blob.get('n')} banner={blob.get('banner')} "
          f"err={blob.get('error')}", flush=True)
    if args.write:
        print(f"[held-live] wrote {OUT}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
