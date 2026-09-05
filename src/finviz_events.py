"""Finviz chart E / R / D markers — the same overlay as quote.ashx.

The Elite quote page paints dated chips on the daily chart:

  * **E** green square  — ``chartEvent/earnings`` (report stamp, beat/miss)
  * **R** green/red diamond — ``chartEvent/ratings`` (Upgrade / Downgrade)
  * **D** blue circle — ``chartEvent/dividends`` (ex-dividend date)

Those live in ``var data = {..., "chartEvents":[...]}`` on the quote page,
not in yfinance and not in the Elite export's single next/last fields.

Export still covers **every** ticker for the latest E + D without a
per-name scrape. Quote ``chartEvents`` overlay the full dated series
(and ratings) when the HTML or a cached JSON is present.

Leak-free 09:30 snapshot:
  * E — prior session, or same-day BMO (hour <= 09:30 ET)
  * R — strictly before the session (intraday notes can print after the open)
  * D — on or before the session (ex-div is on the calendar in advance)
"""
from __future__ import annotations

import argparse
import json
import re
from datetime import date, datetime
from html import unescape
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from . import config

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "data" / "events"
EXPORT_DIR = ROOT / "data" / "exports"
ET = ZoneInfo(getattr(config, "TZ", None) or "America/New_York")

KIND_E, KIND_R, KIND_D = "E", "R", "D"
_CHART_KIND = {
    "chartEvent/earnings": KIND_E,
    "chartEvent/ratings": KIND_R,
    "chartEvent/dividends": KIND_D,
    "earnings": KIND_E,
    "ratings": KIND_R,
    "dividends": KIND_D,
    "e": KIND_E,
    "r": KIND_R,
    "d": KIND_D,
}
_FINVIZ_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}
_TOKEN = re.compile(r"(?i)\b([A-Za-z]{3}-\d{1,2}-\d{2,4})\b")
_CACHE: dict[str, list[dict]] = {}
_EXPORT_IDX: dict[str, dict[str, list[dict]]] = {}


def _iso(d: date | datetime | str | None) -> str | None:
    if d is None:
        return None
    if isinstance(d, datetime):
        return d.date().isoformat()
    if isinstance(d, date):
        return d.isoformat()
    s = str(d).strip()
    return s[:10] if len(s) >= 10 and s[4] == "-" else None


def parse_finviz_datetime(raw, *, today: date | None = None) -> tuple[str | None, int | None]:
    """Finviz export / token / ISO → (YYYY-MM-DD, HHMM ET or None)."""
    if raw is None:
        return None, None
    try:
        if raw != raw:  # NaN
            return None, None
    except Exception:
        pass
    if hasattr(raw, "date") and callable(getattr(raw, "date")) and not isinstance(raw, str):
        try:
            dt = raw
            if getattr(dt, "tzinfo", None) is None:
                hm = dt.hour * 100 + dt.minute if hasattr(dt, "hour") else None
                return dt.date().isoformat(), hm
            local = dt.astimezone(ET)
            return local.date().isoformat(), local.hour * 100 + local.minute
        except Exception:
            pass
    s = " ".join(str(raw).split())
    if not s or s.lower() in ("nan", "nat", "none", "-", ""):
        return None, None
    if s.isdigit() and len(s) >= 9:
        return _ts_parts(int(s))
    try:
        ts = float(s)
        if ts > 1_000_000_000:
            return _ts_parts(ts)
    except ValueError:
        pass
    for fmt in (
        "%m/%d/%Y %I:%M:%S %p",
        "%m/%d/%Y %I:%M %p",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%m/%d/%Y",
    ):
        try:
            dt = datetime.strptime(s[:26], fmt)
            hm = None
            if any(tok in fmt for tok in ("%I", "%H")):
                hm = dt.hour * 100 + dt.minute
            return dt.strftime("%Y-%m-%d"), hm
        except ValueError:
            continue
    tok = _TOKEN.search(s)
    if tok:
        parts = tok.group(1).split("-")
        mon = _FINVIZ_MONTHS.get(parts[0][:3].lower())
        if mon:
            try:
                day_n = int(parts[1])
                year = int(parts[2])
                if year < 100:
                    year += 2000
                return date(year, mon, day_n).isoformat(), None
            except ValueError:
                pass
    return None, None


def _ts_parts(ts) -> tuple[str | None, int | None]:
    try:
        dt = datetime.fromtimestamp(float(ts), tz=ET)
    except (OSError, OverflowError, ValueError, TypeError):
        return None, None
    return dt.date().isoformat(), dt.hour * 100 + dt.minute


def extract_chart_events(html: str) -> list[dict]:
    """Pull the ``chartEvents`` array out of a Finviz quote page."""
    if not html:
        return []
    blob = html.strip()
    if blob.startswith("[") or blob.startswith("{"):
        try:
            obj = json.loads(blob)
        except json.JSONDecodeError:
            obj = None
        if isinstance(obj, list):
            return [x for x in obj if isinstance(x, dict)]
        if isinstance(obj, dict):
            raw = obj.get("chartEvents") or obj.get("events") or []
            return [x for x in raw if isinstance(x, dict)]
    idx = html.find('"chartEvents"')
    if idx < 0:
        idx = html.find("'chartEvents'")
    if idx < 0:
        return []
    start = html.find("[", idx)
    if start < 0:
        return []
    depth = 0
    end = None
    for i, ch in enumerate(html[start:], start):
        if ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
            if depth == 0:
                end = i + 1
                break
    if end is None:
        return []
    try:
        raw = json.loads(html[start:end])
    except json.JSONDecodeError:
        return []
    return [x for x in raw if isinstance(x, dict)]


def _num(v) -> float | None:
    try:
        if v is None:
            return None
        f = float(v)
        if f != f:
            return None
        return f
    except (TypeError, ValueError):
        return None


def _clean(text: str) -> str:
    s = unescape(str(text or ""))
    return (
        s.replace("&rarr;", "→")
        .replace("\u2192", "→")
        .replace("&amp;", "&")
        .strip()
    )


def _rating_color(action: str) -> tuple[str, str]:
    al = (action or "").strip().lower()
    if al == "upgrade" or al.startswith("upgrade"):
        return "green", "R_UP"
    if al == "downgrade" or al.startswith("downgrade"):
        return "red", "R_DOWN"
    if "init" in al:
        return "white", "R_INIT"
    return "white", "R"


def _earn_color(actual, estimate) -> tuple[str, str]:
    act, est = _num(actual), _num(estimate)
    if act is None or est is None:
        return "green", "E"
    if act > est:
        return "green", "E_BEAT"
    if act < est:
        return "red", "E_MISS"
    return "white", "E_INLINE"


def _row(ticker: str, kind: str, event_date: str, **extra) -> dict:
    rec = {
        "ticker": ticker.upper(),
        "kind": kind,
        "event_date": event_date,
        "color": extra.pop("color", "white"),
        "label": extra.pop("label", kind),
        "detail": extra.pop("detail", ""),
        "hm": extra.pop("hm", None),
        "source": extra.pop("source", "finviz_chart"),
    }
    rec.update(extra)
    return rec


def events_from_chart(raw_events: list[dict], ticker: str = "") -> list[dict]:
    """Normalize Finviz ``chartEvents`` objects into E / R / D rows."""
    out: list[dict] = []
    for raw in raw_events or []:
        if not isinstance(raw, dict):
            continue
        kind = _CHART_KIND.get(str(raw.get("eventType") or raw.get("type") or "").strip())
        if not kind:
            continue
        ed, hm = parse_finviz_datetime(
            raw.get("dateTimestamp") or raw.get("date") or raw.get("event_date")
        )
        if not ed:
            continue
        t = str(raw.get("ticker") or ticker or "").strip().upper()
        if kind == KIND_E:
            color, label = _earn_color(
                raw.get("epsActual") or raw.get("epsReportedActual"),
                raw.get("epsEstimate") or raw.get("epsReportedEstimate"),
            )
            out.append(_row(
                t, KIND_E, ed, hm=hm, color=color, label=label,
                eps_act=_num(raw.get("epsActual")),
                eps_est=_num(raw.get("epsEstimate")),
                surprise_pct=_surprise(
                    raw.get("epsActual"), raw.get("epsEstimate"),
                ),
                fiscal=raw.get("fiscalPeriod"),
                detail=str(raw.get("fiscalPeriod") or ""),
            ))
            continue
        if kind == KIND_D:
            ordinary = _num(raw.get("ordinary") or raw.get("amount"))
            special = _num(raw.get("special")) or 0.0
            out.append(_row(
                t, KIND_D, ed, hm=hm, color="blue", label="D",
                amount=ordinary, special=special,
                detail=f"${ordinary:g}" if ordinary is not None else "",
            ))
            continue
        ratings = raw.get("ratings")
        if not isinstance(ratings, list) or not ratings:
            action = str(raw.get("action") or raw.get("status") or "")
            color, label = _rating_color(action)
            out.append(_row(
                t, KIND_R, ed, hm=hm, color=color, label=label,
                detail=_clean(action),
            ))
            continue
        for rat in ratings:
            if not isinstance(rat, dict):
                continue
            action = str(rat.get("action") or "")
            color, label = _rating_color(action)
            firm = _clean(rat.get("analyst") or "")
            change = _clean(rat.get("rating") or "")
            target = _clean(rat.get("targetPrice") or "")
            bits = [x for x in (action, firm, change, target) if x]
            out.append(_row(
                t, KIND_R, ed, hm=hm, color=color, label=label,
                detail=" | ".join(bits),
                analyst=firm, action=action, rating=change,
            ))
    out.sort(key=lambda r: (r.get("event_date") or "", r.get("kind") or "", r.get("detail") or ""))
    return out


def _surprise(actual, estimate) -> float | None:
    act, est = _num(actual), _num(estimate)
    if act is None or est is None or est == 0:
        return None
    return (act - est) / abs(est) * 100.0


def parse_ratings_table(html: str, ticker: str = "") -> list[dict]:
    """Fallback: ``table.js-table-ratings`` dated Upgrade / Downgrade rows."""
    if not html or "js-table-ratings" not in html:
        return []
    # Date glued or in its own cell: Aug-17-26 + Upgrade / Downgrade / …
    pat = re.compile(
        r"(?P<date>[A-Z][a-z]{2}-\d{1,2}-\d{2})"
        r"(?P<action>Upgrade|Downgrade|Initiated|Reiterated|Resumed)"
        r"(?P<rest>.{0,160}?)"
        r"(?=(?:[A-Z][a-z]{2}-\d{1,2}-\d{2})(?:Upgrade|Downgrade|Initiated|Reiterated|Resumed)|</table>|$)",
        re.DOTALL,
    )
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"\s+", " ", unescape(text))
    out: list[dict] = []
    seen: set[tuple] = set()
    for m in pat.finditer(text):
        ed, _ = parse_finviz_datetime(m.group("date"))
        if not ed:
            continue
        action = m.group("action")
        color, label = _rating_color(action)
        rest = _clean(m.group("rest"))[:120]
        key = (ed, action, rest[:40])
        if key in seen:
            continue
        seen.add(key)
        out.append(_row(
            ticker, KIND_R, ed, color=color, label=label,
            detail=f"{action} | {rest}".strip(" |"),
            source="finviz_ratings_table",
        ))
    return out


def parse_quote_html(html: str, ticker: str = "") -> list[dict]:
    """Quote page → dated E / R / D. Chart JSON first, ratings table fills R."""
    chart = events_from_chart(extract_chart_events(html), ticker=ticker)
    if chart:
        have_r = any(r.get("kind") == KIND_R for r in chart)
        if have_r:
            return chart
        return _merge(chart, parse_ratings_table(html, ticker=ticker))
    return parse_ratings_table(html, ticker=ticker)


def events_from_export_fields(ticker: str, earn_raw, div_raw) -> list[dict]:
    """One Elite-export row: latest Earnings Date + Dividend Ex Date."""
    out: list[dict] = []
    t = str(ticker or "").strip().upper()
    ed, hm = parse_finviz_datetime(earn_raw)
    if ed:
        # Date-only export is not a beat. Color stays white so 09:30
        # boards do not paint a green circle when polarity is unknown.
        out.append(_row(
            t, KIND_E, ed, hm=hm, color="white", label="E",
            detail=str(earn_raw or ""), source="finviz_export",
        ))
    dd, _ = parse_finviz_datetime(div_raw)
    if dd:
        out.append(_row(
            t, KIND_D, dd, color="blue", label="D",
            detail=str(div_raw or ""), source="finviz_export",
        ))
    return out


def _merge(*groups: list[dict]) -> list[dict]:
    seen: set[tuple] = set()
    out: list[dict] = []
    # Prefer chart rows over export / table when the same kind+date exists.
    rank = {"finviz_chart": 0, "finviz_ratings_table": 1, "finviz_export": 2}
    flat = [r for g in groups for r in (g or [])]
    flat.sort(key=lambda r: (
        rank.get(str(r.get("source") or ""), 9),
        r.get("event_date") or "",
        r.get("kind") or "",
    ))
    for rec in flat:
        key = (rec.get("ticker"), rec.get("kind"), rec.get("event_date"),
               rec.get("label"), rec.get("detail") or "")
        # Collapse export E/D when a chart row already covers that date+kind.
        coarse = (rec.get("ticker"), rec.get("kind"), rec.get("event_date"))
        if rec.get("source") == "finviz_export" and any(
            (x.get("ticker"), x.get("kind"), x.get("event_date")) == coarse
            and x.get("source") == "finviz_chart"
            for x in out
        ):
            continue
        if key in seen:
            continue
        seen.add(key)
        out.append(rec)
    out.sort(key=lambda r: (r.get("event_date") or "", r.get("kind") or ""))
    return out


def cache_path(ticker: str) -> Path:
    return OUT_DIR / f"{str(ticker).strip().upper()}_chart_events.json"


def save_ticker_events(ticker: str, events: list[dict]) -> Path:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    path = cache_path(ticker)
    path.write_text(json.dumps(events, indent=2, default=str), encoding="utf-8")
    _CACHE[ticker.upper()] = events
    return path


def load_cached(ticker: str) -> list[dict]:
    t = ticker.upper()
    if t in _CACHE:
        return _CACHE[t]
    path = cache_path(t)
    if not path.is_file():
        return []
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    rows = [x for x in raw if isinstance(x, dict)]
    _CACHE[t] = rows
    return rows


def load_export_events(asof: str | None = None) -> dict[str, list[dict]]:
    """Latest E + D for every ticker on the as-of Elite export."""
    key = str(asof or "")
    if key in _EXPORT_IDX:
        return _EXPORT_IDX[key]
    try:
        from . import gainer_asof as ga
    except Exception:
        ga = None
    df = None
    if ga is not None and asof:
        try:
            df = ga.load_finviz(asof)
        except Exception:
            df = None
    if df is None or getattr(df, "empty", True):
        paths = sorted(EXPORT_DIR.glob("finviz_????-??-??.csv"))
        if asof:
            paths = [p for p in paths if p.stem.replace("finviz_", "") <= asof]
        if not paths:
            _EXPORT_IDX[key] = {}
            return {}
        try:
            import pandas as pd
            df = pd.read_csv(paths[-1], low_memory=False)
        except Exception:
            _EXPORT_IDX[key] = {}
            return {}
    if df is None or getattr(df, "empty", True) or "Ticker" not in df.columns:
        _EXPORT_IDX[key] = {}
        return {}
    earn_col = "Earnings Date" if "Earnings Date" in df.columns else None
    div_col = "Dividend Ex Date" if "Dividend Ex Date" in df.columns else None
    out: dict[str, list[dict]] = {}
    for rec in df.to_dict("records"):
        t = str(rec.get("Ticker") or "").strip().upper()
        if not t:
            continue
        rows = events_from_export_fields(
            t,
            rec.get(earn_col) if earn_col else None,
            rec.get(div_col) if div_col else None,
        )
        if rows:
            out[t] = rows
    _EXPORT_IDX[key] = out
    return out


def events_for(ticker: str, asof: str | None = None,
               export_index: dict[str, list[dict]] | None = None) -> list[dict]:
    """Best dated series for one name: quote cache, then that day's export."""
    t = str(ticker or "").strip().upper()
    cached = load_cached(t)
    exported = None
    if export_index is not None:
        exported = export_index.get(t) or []
    elif asof:
        exported = (load_export_events(asof) or {}).get(t) or []
    return _merge(cached, exported or [])


def _usable(rec: dict, asof: str) -> bool:
    ed = str(rec.get("event_date") or "")[:10]
    if not ed:
        return False
    kind = rec.get("kind")
    if ed < asof:
        return True
    if ed > asof:
        return False
    if kind == KIND_D:
        return True
    if kind == KIND_E:
        hm = rec.get("hm")
        try:
            return hm is not None and int(hm) <= 930
        except (TypeError, ValueError):
            return False
    return False  # same-day R stays off the 09:30 board


def asof_snapshot(events: list[dict], asof: str) -> dict[str, Any]:
    """Last E / R / D knowable at 09:30 ET on ``asof``."""
    empty = {
        "last_E_date": None, "last_E_color": None, "last_E_label": None,
        "last_E_surprise": None, "days_since_E": None, "flag_E": 0,
        "last_R_date": None, "last_R_color": None, "last_R_label": None,
        "days_since_R": None, "flag_R": 0,
        "last_D_date": None, "last_D_color": None, "last_D_label": None,
        "days_since_D": None,
        "n_E_90d": 0, "n_R_90d": 0, "n_D_90d": 0,
        "earn_react": False, "cell": "—",
    }
    if not events or not asof:
        return empty
    keep = [r for r in events if _usable(r, asof)]
    if not keep:
        return empty
    asof_d = datetime.strptime(asof, "%Y-%m-%d").date()
    out = dict(empty)

    def last(kind: str, colors: tuple[str, ...] | None = None) -> dict | None:
        rows = [r for r in keep if r.get("kind") == kind]
        if colors:
            rows = [r for r in rows if r.get("color") in colors]
        return rows[-1] if rows else None

    e = last(KIND_E)
    if e:
        out["last_E_date"] = e["event_date"]
        out["last_E_color"] = e.get("color")
        out["last_E_label"] = e.get("label")
        out["last_E_surprise"] = e.get("surprise_pct")
        out["days_since_E"] = (asof_d - datetime.strptime(e["event_date"], "%Y-%m-%d").date()).days
        out["flag_E"] = 1 if e.get("color") == "green" else (-1 if e.get("color") == "red" else 0)
        cut = date.fromordinal(asof_d.toordinal() - 90).isoformat()
        out["n_E_90d"] = sum(1 for r in keep if r.get("kind") == KIND_E and r["event_date"] >= cut)
        hm = e.get("hm")
        try:
            hm_i = int(hm) if hm is not None else None
        except (TypeError, ValueError):
            hm_i = None
        out["earn_react"] = (
            (e["event_date"] == asof and hm_i is not None and hm_i <= 930)
            or (out["days_since_E"] == 1 and (hm_i is None or hm_i >= 1600))
        )

    r = last(KIND_R, colors=("green", "red")) or last(KIND_R)
    if r:
        out["last_R_date"] = r["event_date"]
        out["last_R_color"] = r.get("color")
        out["last_R_label"] = r.get("label")
        out["days_since_R"] = (asof_d - datetime.strptime(r["event_date"], "%Y-%m-%d").date()).days
        out["flag_R"] = 1 if r.get("color") == "green" else (-1 if r.get("color") == "red" else 0)
        cut = date.fromordinal(asof_d.toordinal() - 90).isoformat()
        out["n_R_90d"] = sum(
            1 for x in keep
            if x.get("kind") == KIND_R and x.get("color") in ("green", "red")
            and x["event_date"] >= cut
        )

    d = last(KIND_D)
    if d:
        out["last_D_date"] = d["event_date"]
        out["last_D_color"] = d.get("color") or "blue"
        out["last_D_label"] = d.get("label") or "D"
        out["days_since_D"] = (asof_d - datetime.strptime(d["event_date"], "%Y-%m-%d").date()).days
        cut = date.fromordinal(asof_d.toordinal() - 90).isoformat()
        out["n_D_90d"] = sum(1 for x in keep if x.get("kind") == KIND_D and x["event_date"] >= cut)

    out["cell"] = format_cell(out)
    return out


def _chip(kind: str, color: str | None, day: str | None) -> str:
    if not day:
        return ""
    mark = {"green": "🟢", "red": "🔴", "blue": "🔵", "white": "⚪"}.get(str(color or ""), "")
    md = day[5:].replace("-", "-")
    try:
        md = f"{int(day[5:7])}-{int(day[8:10])}"
    except ValueError:
        md = day[5:]
    return f"{kind}{mark} {md}"


def format_cell(snap: dict) -> str:
    bits = [
        _chip("E", snap.get("last_E_color"), snap.get("last_E_date")),
        _chip("R", snap.get("last_R_color"), snap.get("last_R_date")),
        _chip("D", snap.get("last_D_color") or "blue", snap.get("last_D_date")),
    ]
    return " · ".join(b for b in bits if b) or "—"


def attach_row(rec: dict, date: str, ticker: str,
               export_index: dict[str, list[dict]] | None = None) -> dict:
    """Stamp leak-free E/R/D fields onto a flatten-lookback row."""
    snap = asof_snapshot(events_for(ticker, asof=date, export_index=export_index), date)
    rec["erd_cell"] = snap["cell"]
    rec["erd_E_date"] = snap["last_E_date"]
    rec["erd_E_color"] = snap["last_E_color"]
    rec["erd_E_label"] = snap["last_E_label"]
    rec["erd_R_date"] = snap["last_R_date"]
    rec["erd_R_color"] = snap["last_R_color"]
    rec["erd_R_label"] = snap["last_R_label"]
    rec["erd_D_date"] = snap["last_D_date"]
    rec["erd_D_color"] = snap["last_D_color"]
    rec["erd_earn_react"] = bool(snap["earn_react"])
    rec["erd_flag_E"] = snap["flag_E"]
    rec["erd_flag_R"] = snap["flag_R"]
    return rec


def fetch_quote(ticker: str, sess=None) -> list[dict]:
    """Live Elite quote page → chartEvents. Empty when live HTML is blocked."""
    try:
        from . import finviz_session
    except Exception:
        return []
    sess = sess or finviz_session.session()
    r = finviz_session.get(sess, [f"/quote.ashx?t={ticker.upper()}",
                                 f"/quote?t={ticker.upper()}"], timeout=45)
    if r is None or not getattr(r, "text", ""):
        return []
    return parse_quote_html(r.text, ticker=ticker)


def fetch(ticker: str, html: str | None = None, asof: str | None = None,
          live: bool = False) -> list[dict]:
    t = ticker.upper()
    if html:
        rows = parse_quote_html(html, ticker=t)
        if rows:
            return rows
    cached = load_cached(t)
    if cached:
        return cached
    if live:
        live_rows = fetch_quote(t)
        if live_rows:
            save_ticker_events(t, live_rows)
            return live_rows
    exported = (load_export_events(asof) or {}).get(t) or []
    return exported


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("tickers", nargs="*", help="Tickers (default: AAPL)")
    ap.add_argument("--asof", default="", help="PIT snapshot YYYY-MM-DD")
    ap.add_argument("--html", default="", help="Quote-page HTML file")
    ap.add_argument("--live", action="store_true", help="Fetch Elite quote pages")
    ap.add_argument("--export", action="store_true",
                    help="Write latest E/D for every name on the as-of export")
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args()
    html = Path(args.html).read_text(encoding="utf-8") if args.html else None
    asof = args.asof or None
    if args.export:
        idx = load_export_events(asof)
        print(f"[finviz-events] export names={len(idx)}")
        if args.write:
            OUT_DIR.mkdir(parents=True, exist_ok=True)
            path = OUT_DIR / f"{(asof or 'latest')}_export_erd.json"
            slim = {t: ev for t, ev in idx.items()}
            path.write_text(json.dumps(slim, indent=2, default=str), encoding="utf-8")
            print(f"[finviz-events] wrote {path}")
        return
    names = [t.strip().upper() for t in (args.tickers or ["AAPL"]) if t.strip()]
    for t in names:
        ev = fetch(t, html=html, asof=asof, live=args.live)
        if args.write and ev:
            save_ticker_events(t, ev)
        print(f"[finviz-events] {t}: {len(ev)} "
              f"(E={sum(1 for r in ev if r['kind']=='E')} "
              f"R={sum(1 for r in ev if r['kind']=='R')} "
              f"D={sum(1 for r in ev if r['kind']=='D')})")
        if asof:
            print(json.dumps(asof_snapshot(ev, asof), indent=2, default=str))
        else:
            for r in ev[-12:]:
                print(f"  {r.get('event_date')} {r.get('kind')} "
                      f"{r.get('color')} {r.get('label')} {r.get('detail')}")


if __name__ == "__main__":
    main()
