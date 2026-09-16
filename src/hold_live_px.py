"""Elite marks for names the $10k butterfly CURRENTLY HOLDS.

Hold list = open lots on featured sleeves (combo_sh_5050 and the other
featured $10k books). Ticker, side, shares, entry. Not the looker
list. Not closed lots.

Price = src.elite_live_px.quote_book / pull_elite_overview. After
09:30 ET + auth → src=elite_live. Before the bell or a failed pull →
last same-session export (preopen/session_export) — never labeled
"live". Theme Radar close dump is refused.

Soft-fail Elite auth — never block deploy.
Does not change flatten_robust / Webull / hard-red sit.
"""
from __future__ import annotations

import argparse
import base64
import gzip
import json
from datetime import datetime
from pathlib import Path

from . import elite_live_px as elp

ROOT = Path(__file__).resolve().parent.parent
DAY_BOARD = ROOT / "data" / "day_board"
DASH_DIR = ROOT / "dashboard" / "factor-mine"
OUT_JSON = ROOT / "03_scoreboard" / "factor_mine.json"
DASH_HTML = DASH_DIR / "index.html"
HOLD_JSON = DAY_BOARD / "hold_live_px.json"
DASH_HOLD = DASH_DIR / "hold_live_px.json"
PIN = ("combo_sh_5050_shared",)
LIVE_SRC = ("elite_live", "elite_live_file")


def _tick(v) -> str:
    return str(v or "").strip().upper()


def _num(v):
    if v is None or v == "":
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    if x != x:
        return None
    return x


def load_pack() -> dict:
    """Baked factor-mine pack (dash HTML, else scoreboard json)."""
    if DASH_HTML.is_file():
        text = DASH_HTML.read_text(encoding="utf-8", errors="replace")
        marker = 'const B64 = "'
        start = text.find(marker)
        if start >= 0:
            start += len(marker)
            end = text.find('"', start)
            if end > start:
                try:
                    return json.loads(
                        gzip.decompress(base64.b64decode(text[start:end]))
                        .decode("utf-8"))
                except Exception:
                    pass
    if OUT_JSON.is_file():
        try:
            return json.loads(OUT_JSON.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
    return {}


def featured_names(pack: dict | None = None) -> list[str]:
    pack = pack if pack is not None else load_pack()
    names: list[str] = []
    for n in list(PIN) + list((pack or {}).get("featured") or []):
        if n and n not in names:
            names.append(n)
    return names


def recipe_side(pack: dict, name: str) -> str:
    for rec in (pack.get("recipes") or []):
        if rec.get("name") == name and rec.get("side"):
            return str(rec["side"])
    for st in (pack.get("stats") or []):
        if st.get("name") == name and st.get("side"):
            return str(st["side"])
    return "long"


def open_lots_for(pack: dict, name: str) -> list[dict]:
    """Currently held lots. Open book first; last daily close lots as fallback.

    Closed lots (sold/covered, not in the open book) are omitted.
    """
    book = ((pack.get("books") or {}).get(name)) or {}
    raw = list(book.get("open") or [])
    if not raw:
        days = ((pack.get("daily") or {}).get(name)) or []
        last = days[-1] if days else {}
        raw = list(last.get("lots") or [])
        if not raw:
            held = last.get("held") or []
            raw = [{"ticker": t} for t in held]
    side0 = recipe_side(pack, name)
    out = []
    seen = set()
    for lot in raw:
        if not isinstance(lot, dict):
            continue
        t = _tick(lot.get("ticker"))
        shares = int(_num(lot.get("shares")) or 0)
        if not t or shares < 1 or t in seen:
            continue
        seen.add(t)
        side = str(lot.get("side") or lot.get("lot_side") or side0 or "long")
        if side not in ("long", "short"):
            side = "long"
        out.append({
            "ticker": t,
            "side": side,
            "shares": shares,
            "entry": _num(lot.get("entry_px") or lot.get("entry") or lot.get("price")),
            "entry_date": lot.get("entry_date"),
            "owner": lot.get("owner"),
        })
    return out


def _signed_pct(px, base, side: str):
    px = _num(px)
    base = _num(base)
    if px is None or base is None or base == 0:
        return None
    raw = (px - base) / base
    return round(100.0 * (raw if side == "long" else -raw), 3)


def _pnl(px, entry, shares, side: str):
    px = _num(px)
    entry = _num(entry)
    if px is None or entry is None or not shares:
        return None
    raw = (px - entry) * int(shares)
    return round(raw if side == "long" else -raw, 2)


def is_live_src(src: str | None) -> bool:
    s = str(src or "")
    if any(m in s.lower() for m in elp.THEME_RADAR_MARKS):
        return False
    head = s.split("+", 1)[0]
    return head in LIVE_SRC


def banner_text(book: dict) -> str:
    src = str(book.get("src") or "")
    at = book.get("at")
    clock = ""
    if at:
        try:
            dt = datetime.fromisoformat(str(at).replace("Z", "+00:00"))
            clock = dt.astimezone(elp.ET).strftime("%H:%M")
        except ValueError:
            clock = str(at)[11:16] if len(str(at)) >= 16 else ""
    clock = clock or elp.now_et().strftime("%H:%M")
    if is_live_src(src):
        return f"Elite Overview as of {clock} ET"
    return f"Elite export as of {clock} ET (not live)"


def stamp_lot(lot: dict, book: dict, opens: dict) -> dict:
    t = lot["ticker"]
    prices = book.get("prices") or {}
    src = book.get("src") or "missing"
    asof = book.get("at")
    row = dict(lot)
    if t in prices:
        row["px"] = prices[t]
        row["px_src"] = src
        row["px_asof"] = asof
    opx = opens.get(t)
    if opx is not None:
        row["open_px"] = opx
        row["open_src"] = "session_open"
    row["vs_open_pct"] = _signed_pct(row.get("px"), row.get("open_px"), row["side"])
    row["vs_entry_pct"] = _signed_pct(row.get("px"), row.get("entry"), row["side"])
    row["pnl"] = _pnl(row.get("px"), row.get("entry"), row.get("shares"), row["side"])
    return row


def build(date: str | None = None, *, pack: dict | None = None,
          pull_live: bool | None = None) -> dict:
    """Quote open lots. Soft-fail Elite — never raises."""
    pack = pack if pack is not None else load_pack()
    date = date or elp.now_et().strftime("%Y-%m-%d")
    names = featured_names(pack)
    lots_by = {n: open_lots_for(pack, n) for n in names}
    tickers = sorted({lot["ticker"] for lots in lots_by.values() for lot in lots})
    try:
        book = elp.quote_book(date, pull_live=pull_live)
    except Exception as e:  # noqa: BLE001
        book = {
            "date": date, "prices": {}, "src": f"missing+{e}",
            "at": elp.now_et().isoformat(), "after_open": elp.after_open(),
            "n": 0, "error": str(e),
        }
    opens: dict[str, float] = {}
    try:
        opens = elp.fallback_opens(date)
    except Exception:
        opens = {}
    try:
        tape = elp.official_opens(tickers, date)
        for t, px in (tape or {}).items():
            if px is not None:
                opens[t] = float(px)
    except Exception:
        pass
    sleeves = {}
    for name in names:
        sleeves[name] = {
            "name": name,
            "lots": [stamp_lot(lot, book, opens) for lot in lots_by[name]],
        }
    src = book.get("src") or "missing"
    return {
        "date": date,
        "generated_at": elp.now_et().isoformat(),
        "featured": names,
        "quote": {
            "src": src,
            "at": book.get("at"),
            "after_open": book.get("after_open"),
            "n": book.get("n"),
            "error": book.get("error"),
            "live": is_live_src(src),
        },
        "banner": banner_text(book),
        "clock_rule": book.get("clock_rule") or (
            "px is Finviz Elite Price from a fresh post-09:30 Overview "
            "when live is allowed. Theme Radar close dump and yesterday "
            "Price are never live. Hold list is open lots only."
        ),
        "sleeves": sleeves,
        "n_lots": sum(len(s["lots"]) for s in sleeves.values()),
        "looker": False,
    }


def write(payload: dict | None = None, date: str | None = None) -> list[Path]:
    payload = payload if payload is not None else build(date)
    text = json.dumps(payload, indent=2)
    wrote = []
    for path in (HOLD_JSON, DASH_HOLD):
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding="utf-8")
            wrote.append(path)
        except OSError as e:
            print(f"[hold-live-px] WARN write {path}: {e}", flush=True)
    print(
        f"[hold-live-px] {payload.get('date')} src={((payload.get('quote') or {}).get('src'))} "
        f"lots={payload.get('n_lots')} banner={payload.get('banner')}",
        flush=True,
    )
    return wrote


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="")
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args(argv)
    date = args.date or None
    try:
        payload = build(date)
    except Exception as e:  # noqa: BLE001
        print(f"[hold-live-px] soft-fail: {e}", flush=True)
        payload = {
            "date": date or elp.now_et().strftime("%Y-%m-%d"),
            "quote": {"src": "missing", "error": str(e), "live": False},
            "banner": "Elite export as of ? ET (not live)",
            "sleeves": {},
            "n_lots": 0,
            "looker": False,
        }
    if args.write:
        write(payload, date)
    q = payload.get("quote") or {}
    print(f"date={payload.get('date')} src={q.get('src')} live={q.get('live')} "
          f"lots={payload.get('n_lots')} {payload.get('banner')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
