"""Fresh Finviz Elite marks for open-pack tickets.

Live px = Elite Overview Price pulled at/after 09:30 ET (export.ashx v=111).
That is not Theme Radar's after-close dump (~16:30) and not yesterday's
Price column. Before the bell, last available same-session or pre-open
export is stamped as such — never as live.

Does not change flatten_robust / Webull / hard-red sit.
"""
from __future__ import annotations

import csv
from datetime import datetime
from io import StringIO
from pathlib import Path
from zoneinfo import ZoneInfo

from . import config

ROOT = Path(__file__).resolve().parent.parent
ET = ZoneInfo(config.TZ)
EXPORTS = ROOT / "data" / "exports"
FINVIZ = ROOT / "data" / "finviz"
LIVE_CSV = EXPORTS / "finviz_live.csv"
THEME_RADAR_MARKS = (
    "theme radar",
    "finviz_market_digest_close",
    "_close.csv",
)


def now_et() -> datetime:
    return datetime.now(ET)


def after_open(when: datetime | None = None) -> bool:
    t = when or now_et()
    return t.hour > 9 or (t.hour == 9 and t.minute >= 30)


def _num(v):
    if v is None or v == "":
        return None
    try:
        x = float(str(v).replace(",", "").replace("%", "").strip())
    except (TypeError, ValueError):
        return None
    if x != x:  # NaN
        return None
    return x


def _is_theme_radar(path: Path) -> bool:
    name = path.name.lower()
    return any(m in name for m in THEME_RADAR_MARKS)


def parse_elite_price_csv(text: str) -> dict[str, float]:
    """Ticker → Price from an Elite Overview (or merged) export."""
    out: dict[str, float] = {}
    if not text or text.lstrip().startswith("<!"):
        return out
    try:
        rows = csv.DictReader(StringIO(text))
    except csv.Error:
        return out
    for row in rows:
        t = str(row.get("Ticker") or row.get("ticker") or "").strip().upper()
        px = _num(row.get("Price") or row.get("price"))
        if t and px is not None:
            out[t] = px
    return out


def load_csv_prices(path: Path) -> dict[str, float]:
    if not path.is_file() or _is_theme_radar(path):
        return {}
    try:
        return parse_elite_price_csv(path.read_text(encoding="utf-8", errors="replace"))
    except OSError:
        return {}


def pull_elite_overview() -> tuple[dict[str, float], str | None]:
    """Authenticated Elite v=111 Overview. Soft-fail; never Theme Radar."""
    try:
        from . import finviz_session as fs
    except Exception as e:  # noqa: BLE001
        return {}, f"finviz_session: {e}"
    if not fs.live_html_allowed():
        return {}, "elite_live_skipped"
    try:
        sess = fs.session()
    except Exception as e:  # noqa: BLE001
        return {}, f"elite_session: {e}"
    if sess is None or sess.headers.get("X-Fullscan-Finviz") == "skipped":
        return {}, "elite_live_skipped"
    try:
        r = fs.get(sess, ["/export.ashx?v=111"], timeout=40)
    except Exception as e:  # noqa: BLE001
        return {}, f"elite_export: {e}"
    if r is None or not getattr(r, "text", None):
        return {}, "elite_export_empty"
    if fs.looks_like_login_html(r.text) or r.text.lstrip().startswith("<!"):
        return {}, "elite_export_login_html"
    prices = parse_elite_price_csv(r.text)
    if not prices:
        return {}, "elite_export_no_price"
    try:
        LIVE_CSV.parent.mkdir(parents=True, exist_ok=True)
        LIVE_CSV.write_text(r.text, encoding="utf-8")
    except OSError:
        pass
    return prices, None


def fallback_export(date: str) -> tuple[Path | None, str]:
    """Same-session / latest Elite CSV. Skip Theme Radar and prior-day Price."""
    candidates = [
        LIVE_CSV,
        EXPORTS / f"finviz_{date}.csv",
        FINVIZ / "latest.csv",
        EXPORTS / "finviz_latest.csv",
    ]
    for path in candidates:
        if path.is_file() and not _is_theme_radar(path) and load_csv_prices(path):
            src = "elite_live_file" if path == LIVE_CSV else "preopen_export"
            if path.name.startswith(f"finviz_{date}"):
                src = "session_export"
            return path, src
    return None, "missing"


def quote_book(date: str, *, pull_live: bool | None = None) -> dict:
    """Price book for ``date``. Live pull only after 09:30 when allowed."""
    at = now_et()
    want_live = after_open(at) if pull_live is None else bool(pull_live)
    prices: dict[str, float] = {}
    src = "missing"
    err = None
    import os
    have_auth = bool(
        (os.environ.get("FINVIZ_EMAIL") or "").strip()
        or (os.environ.get("FINVIZ_AUTH") or "").strip()
        or (os.environ.get("AUTH_TOKEN_FINVIZ") or "").strip()
    )
    if want_live and have_auth:
        prices, err = pull_elite_overview()
        if prices:
            src = "elite_live"
    elif want_live and not have_auth:
        err = "no_elite_auth"
    if not prices:
        path, src = fallback_export(date)
        if path:
            prices = load_csv_prices(path)
        if want_live and src != "elite_live":
            src = src if src != "missing" else "last_available"
            if err and src != "elite_live":
                src = f"{src}+{err}"
    return {
        "date": date,
        "prices": prices,
        "src": src,
        "at": at.isoformat(),
        "after_open": after_open(at),
        "n": len(prices),
        "error": err,
        "clock_rule": (
            "px is Finviz Elite Price from a fresh post-09:30 Overview "
            "export when live is allowed. Theme Radar close dump and "
            "yesterday Price are never live."
        ),
    }


def stamp_rows(rows: list[dict], book: dict, *, opens: dict | None = None) -> list[dict]:
    """Attach live/last Elite px + official 09:30 open onto ticket rows."""
    prices = book.get("prices") or {}
    src = book.get("src") or "missing"
    asof = book.get("at")
    out = []
    for row in rows or []:
        item = dict(row)
        t = str(item.get("ticker") or "").upper()
        if t and t in prices:
            item["px"] = prices[t]
            item["px_src"] = src
            item["px_asof"] = asof
        if t and opens and opens.get(t) is not None:
            item["open_px"] = opens[t]
            item.setdefault("open_src", "session_open")
        out.append(item)
    return out


def official_opens(tickers: list[str], date: str) -> dict[str, float]:
    """09:30 official open when printed. Never Gap / last / Finviz Price."""
    out: dict[str, float] = {}
    try:
        from . import combo_broker as cb
    except Exception:
        return out
    for t in tickers:
        t = str(t or "").upper()
        if not t:
            continue
        try:
            px = cb.quote_px(t, date)
        except Exception:
            px = None
        if px is not None:
            out[t] = float(px)
    return out


def is_live_src(src: str | None) -> bool:
    s = str(src or "")
    if any(m in s.lower() for m in THEME_RADAR_MARKS):
        return False
    return s.startswith("elite_live") and "preopen" not in s and "session_export" not in s


def clock_et(asof: str | None = None) -> str:
    """HH:MM ET from an ISO stamp, else now."""
    if asof:
        try:
            dt = datetime.fromisoformat(str(asof).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=ET)
            return dt.astimezone(ET).strftime("%H:%M")
        except (TypeError, ValueError):
            pass
    return now_et().strftime("%H:%M")


def banner_text(book: dict | None = None) -> str:
    """Never implies a tick-by-tick feed."""
    book = book or {}
    src = str(book.get("src") or "")
    hhmm = clock_et(book.get("at"))
    if is_live_src(src):
        return f"Elite Overview as of {hhmm} ET"
    if "preopen" in src:
        return f"Elite Overview (preopen) as of {hhmm} ET"
    if "session_export" in src or "elite_live_file" in src:
        return f"Elite Overview (session export) as of {hhmm} ET"
    return f"Elite Overview as of {hhmm} ET — not live"


def lot_pnl(shares, last, basis, side: str = "long") -> float | None:
    if last is None or basis is None:
        return None
    try:
        raw = float(shares or 0) * (float(last) - float(basis))
    except (TypeError, ValueError):
        return None
    return round(raw if (side or "long") != "short" else -raw, 2)


def stamp_held_lots(lots: list[dict], book: dict,
                    opens: dict | None = None) -> list[dict]:
    """Elite last + vs official 09:30 open + vs entry + $ P/L on held shares."""
    stamped = stamp_rows(lots, book, opens=opens)
    src = book.get("src") or "missing"
    asof = book.get("at")
    out = []
    for row in stamped:
        item = dict(row)
        t = str(item.get("ticker") or "").upper()
        item["ticker"] = t
        side = str(item.get("side") or "long").lower()
        if side not in ("long", "short"):
            side = "short" if side in ("cover", "sell_short") else "long"
        item["side"] = side
        shares = item.get("shares")
        try:
            shares = int(shares) if shares is not None else 0
        except (TypeError, ValueError):
            shares = 0
        item["shares"] = shares
        entry = item.get("entry")
        if entry is None:
            entry = item.get("entry_px") or item.get("buy_px")
        if entry is not None:
            try:
                entry = float(entry)
            except (TypeError, ValueError):
                entry = None
        item["entry"] = entry
        px = item.get("px")
        open_px = item.get("open_px")
        if px is not None and open_px is not None:
            try:
                item["vs_open"] = round(float(px) - float(open_px), 4)
            except (TypeError, ValueError):
                item["vs_open"] = None
        else:
            item["vs_open"] = None
        if px is not None and entry is not None:
            try:
                item["vs_entry"] = round(float(px) - float(entry), 4)
            except (TypeError, ValueError):
                item["vs_entry"] = None
        else:
            item["vs_entry"] = None
        item["pnl"] = lot_pnl(shares, px, entry, side)
        item["pnl_vs_open"] = lot_pnl(shares, px, open_px, side)
        item.setdefault("px_src", src if px is not None else None)
        item.setdefault("px_asof", asof if px is not None else None)
        out.append(item)
    return out
