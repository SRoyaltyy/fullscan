"""Finviz homepage market-day prose digest (not the quote-page ticker file).

The live Finviz homepage embeds a `$MARKET` / `market_summary` widget
(`script#why-stock-moving-init-data`) — the narrative like "The S&P 500
rose 0.86%… Brent retreated… hotter CPI… DELL/HPE/HPQ… Monday calendar
light…". That is a different artifact from
`01_daily/news/YYYY-MM-DD_finviz_digest.md` (quote-page headlines + ~400
ticker blurbs).

Outputs:
  01_daily/news/<date>_finviz_market_digest.md
  01_daily/news/<date>_finviz_market_digest.json
  01_daily/news/latest_finviz_market_digest.md

Clock:
  Generated is always America/New_York ISO. Same-morning Pre-Open may
  use the file only when clock_legal is true:
    live    → Generated < that session's 09:30 ET
    wayback / archive.ph → archive snapshot timestamp < 09:30 ET
  After-open captures are still stored (history) with clock_legal=false.
  Theme Radar KEEP/KILL later. Not wired into tape_anchor / predict.

CLI:
  python -m src.finviz_market_digest [--date YYYY-MM-DD] [--html PATH]
  python -m src.finviz_market_digest --backfill --from 2026-08-20 --to 2026-09-12
"""
from __future__ import annotations

import argparse
import gzip
import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests

from . import config, finviz_session

ROOT = Path(__file__).resolve().parent.parent
NEWS_DIR = ROOT / "01_daily" / "news"
ET = ZoneInfo(config.TZ)
UTC = ZoneInfo("UTC")
OPEN_HM = 930
MORNING_REWRITE_HM = 535

HOME_PATHS = ("/", "/index.ashx")
CDX_URL = "https://web.archive.org/cdx/search/cdx"
WAYBACK_ID = "https://web.archive.org/web/{ts}id_/{orig}"
ARCHIVE_PH_HOSTS = (
    "https://archive.ph",
    "https://archive.today",
    "https://archive.md",
)
CDX_TARGETS = (
    "https://finviz.com/",
    "https://finviz.com",
    "https://www.finviz.com/",
)

UA = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}

INDEX_ALIASES = {
    "s&p 500": "SPX",
    "s&p": "SPX",
    "spx": "SPX",
    "spy": "SPY",
    "nasdaq": "COMP",
    "nasdaq-100": "NDX",
    "nasdaq composite": "COMP",
    "comp": "COMP",
    "dow": "DJI",
    "dow jones": "DJI",
    "dow jones industrial average": "DJI",
    "dji": "DJI",
    "russell 2000": "RUT",
    "rut": "RUT",
    "iwm": "IWM",
}

MD_LINK_RE = re.compile(r"\[([^\]]+)\]\((https?://[^)]+)\)")
TICKER_LINK_RE = re.compile(
    r"finviz\.com/(?:quote|quote\.ashx)\?t=([A-Za-z][A-Za-z0-9.-]{0,7})",
    re.I,
)
INDEX_LINK_RE = re.compile(
    r"finviz\.com/index/([A-Za-z0-9]+)",
    re.I,
)
PAREN_TICKER_RE = re.compile(r"\(([A-Z]{1,5})\)")
BOLD_RE = re.compile(r"\*\*([^*]+)\*\*")
INDEX_MOVE_RE = re.compile(
    r"(?i)\b(S&P\s*500|Nasdaq(?:-100| composite)?|Dow(?:\s+Jones(?:\s+Industrial\s+Average)?)?|"
    r"Russell\s*2000)\b"
    r".{0,80}?\b(rose|gained|added|climbed|advanced|jumped|rallied|"
    r"fell|dropped|slid|retreated|lost|slipped|declined)\b"
    r".{0,40}?\b([+-]?\d+(?:\.\d+)?)\s*%"
)
OIL_RE = re.compile(
    r"(?i)\b(Brent(?:\s+crude)?|WTI(?:\s+crude)?|crude(?:\s+oil)?)\b"
    r".{0,100}?\$(\d+(?:\.\d+)?)"
)
FED_ODDS_RE = re.compile(
    r"(?:roughly|about|near|around|implied)?\s*"
    r"(\d+(?:\.\d+)?)\s*%[^\n.]{0,80}?\b(hike|cut|hold|pause)\b"
    r"|"
    r"\b(hike|cut|hold|pause)\b[^\n.]{0,80}?"
    r"(\d+(?:\.\d+)?)\s*%",
    re.I,
)
CAL_HINT_RE = re.compile(
    r"(?i)\b(monday|tuesday|wednesday|thursday|friday|tomorrow|today|"
    r"next week|calendar|earnings slate|auction|retail sales|fomc|"
    r"housing data|ppi|cpi)\b"
)
WHY_SCRIPT_RE = re.compile(
    r'<script[^>]+id=["\']why-stock-moving-init-data["\'][^>]*>\s*(.*?)\s*</script>',
    re.I | re.S,
)
def decode_html_bytes(raw: bytes | str | None) -> str:
    """Decode homepage HTML. Wayback id_ snapshots are often gzip bodies."""
    if raw is None:
        return ""
    if isinstance(raw, str):
        return raw
    if raw[:2] == b"\x1f\x8b":
        try:
            raw = gzip.decompress(raw)
        except OSError:
            pass
    return raw.decode("utf-8", "replace")


def decode_response(resp: requests.Response) -> str:
    raw = resp.content if resp is not None else b""
    text = decode_html_bytes(raw)
    if text:
        return text
    return resp.text or ""


def et_now() -> datetime:
    return datetime.now(ET)


def parse_et_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ET)
    return dt.astimezone(ET)


def wayback_ts_to_dt(ts: str) -> datetime | None:
    ts = re.sub(r"\D", "", ts or "")
    if len(ts) < 14:
        return None
    try:
        return datetime.strptime(ts[:14], "%Y%m%d%H%M%S").replace(tzinfo=UTC)
    except ValueError:
        return None


def session_open(date_str: str) -> datetime:
    return datetime.fromisoformat(f"{date_str}T09:30:00").replace(tzinfo=ET)


def clock_legal_at(dt: datetime | None, session_date: str) -> bool:
    """True when `dt` is strictly before that session's 09:30 ET open."""
    if dt is None:
        return False
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ET)
    return dt.astimezone(ET) < session_open(session_date)


def strip_markup(text: str) -> str:
    if not text:
        return ""
    out = MD_LINK_RE.sub(r"\1", text)
    out = BOLD_RE.sub(r"\1", out)
    out = re.sub(r"<[^>]+>", " ", out)
    out = out.replace("\u00a0", " ")
    out = re.sub(r"[ \t]+", " ", out)
    out = re.sub(r"\n{3,}", "\n\n", out)
    return out.strip()


def _index_code(name: str) -> str:
    return INDEX_ALIASES.get(re.sub(r"\s+", " ", name.lower().strip()), name.upper())


def parse_index_moves(text: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for m in INDEX_MOVE_RE.finditer(text or ""):
        name = re.sub(r"\s+", " ", m.group(1)).strip()
        verb = m.group(2).lower()
        pct = float(m.group(3))
        down = verb in {
            "fell", "dropped", "slid", "retreated", "lost", "slipped", "declined",
        }
        if down and pct > 0:
            pct = -pct
        code = _index_code(name)
        if code in seen:
            continue
        seen.add(code)
        out.append({
            "name": name, "code": code, "change_pct": round(pct, 3), "verb": verb,
        })
    return out


def parse_oil(text: str) -> dict[str, Any] | None:
    m = OIL_RE.search(text or "")
    if not m:
        return None
    name = m.group(1)
    price = float(m.group(2))
    window = (text or "")[max(0, m.start() - 40): m.end() + 40]
    verb = None
    for word in ("retreated", "fell", "dropped", "slid", "rose", "gained", "jumped"):
        if re.search(rf"(?i)\b{word}\b", window):
            verb = word
            break
    return {"name": name, "price": price, "verb": verb}


def parse_fed_odds(text: str) -> dict[str, Any] | None:
    m = FED_ODDS_RE.search(text or "")
    if not m:
        return None
    pct_s = m.group(1) or m.group(4)
    action = (m.group(2) or m.group(3) or "").lower()
    try:
        pct = float(pct_s)
    except (TypeError, ValueError):
        return None
    return {"pct": pct, "action": action, "text": m.group(0).strip()}


def parse_named_tickers(text: str) -> list[str]:
    found: list[str] = []
    skip = {
        "SPX", "SPY", "DJI", "DIA", "COMP", "NDX", "QQQ", "RUT", "IWM",
        "WTI", "CPI", "PPI", "FOMC", "ETF", "USD", "US", "AI", "ET",
        "AM", "PM", "CEO", "EPS", "GDP", "FED",
    }
    for rx in (TICKER_LINK_RE, PAREN_TICKER_RE):
        for m in rx.finditer(text or ""):
            t = m.group(1).upper()
            if t in skip or t in found:
                continue
            if not re.fullmatch(r"[A-Z]{1,5}", t):
                continue
            found.append(t)
    return found


def parse_calendar_bullets(text: str) -> list[str]:
    bullets: list[str] = []
    for raw in re.split(r"\n+|•", text or ""):
        line = strip_markup(raw).lstrip("-").strip()
        if len(line) < 20:
            continue
        if CAL_HINT_RE.search(line):
            bullets.append(line)
    return bullets


def _why_moving_from_obj(data: Any) -> dict | None:
    if not isinstance(data, dict):
        return None
    wm = data.get("whyMoving")
    if not isinstance(wm, dict):
        wm = data if "headline" in data and "summary" in data else None
    if not isinstance(wm, dict):
        return None
    ticker = str(wm.get("ticker") or "")
    source = str(wm.get("source") or "")
    headline = str(wm.get("headline") or "").strip()
    summary = str(wm.get("summary") or "").strip()
    if not (headline or summary):
        return None
    if source == "market_summary" or ticker in ("$MARKET", "MARKET", ""):
        return wm
    # Homepage widget is $MARKET. Ignore single-name "why is this moving".
    if ticker.startswith("$") or "S&P" in headline or "stocks" in headline.lower():
        return wm
    return None


def extract_why_moving(html: str) -> dict | None:
    if not html:
        return None
    m = WHY_SCRIPT_RE.search(html)
    if m:
        try:
            parsed = _why_moving_from_obj(json.loads(m.group(1)))
            if parsed:
                return parsed
        except (json.JSONDecodeError, TypeError, ValueError):
            pass
    # Some archives wrap / escape the script. Search a JSON island.
    m2 = re.search(
        r'\{"whyMoving"\s*:\s*\{.*?"source"\s*:\s*"market_summary".*?\}\s*,\s*"whyMovingRatings"',
        html, re.S,
    )
    blob = m2.group(0) + "}" if m2 else None
    if blob:
        try:
            parsed = _why_moving_from_obj(json.loads(blob))
            if parsed:
                return parsed
        except (json.JSONDecodeError, TypeError, ValueError):
            pass
    return None


def structured_from_text(text: str) -> dict[str, Any]:
    plain = strip_markup(text or "")
    return {
        "index_moves": parse_index_moves(plain),
        "oil": parse_oil(plain),
        "fed_odds": parse_fed_odds(plain),
        "named_tickers": parse_named_tickers(text or ""),
        "calendar": parse_calendar_bullets(plain),
    }


def parse_market_digest_text(text: str, headline: str = "") -> dict[str, Any]:
    """Parse Cyrus-style pasted prose or cleaned summary bullets."""
    raw = (text or "").strip()
    bits = structured_from_text(raw)
    return {
        "headline": (headline or "").strip(),
        "raw_text": strip_markup(raw),
        "raw_summary_md": raw if "**" in raw or "](" in raw else "",
        **bits,
    }


def parse_homepage_html(html: str) -> dict | None:
    """Extract the homepage market-day narrative. None = not present (gap)."""
    html = decode_html_bytes(html)
    if finviz_session.looks_like_login_html(html):
        return None
    wm = extract_why_moving(html)
    if wm:
        headline = str(wm.get("headline") or "").strip()
        summary = str(wm.get("summary") or "").strip()
        parsed = parse_market_digest_text(summary, headline=headline)
        parsed.update({
            "finviz_id": wm.get("id"),
            "finviz_ticker": wm.get("ticker"),
            "finviz_published_at": wm.get("dateTime"),
            "finviz_source": wm.get("source"),
            "finviz_sentiment": wm.get("sentiment"),
            "raw_summary_md": summary,
        })
        if parsed.get("raw_text") or headline:
            return parsed
    return None


def fetch_homepage_html(sess: requests.Session | None = None
                        ) -> tuple[str | None, str, str | None]:
    """Return (html, source_url, error). Elite first, public homepage fallback."""
    sess = sess or finviz_session.session()
    r = finviz_session.get(sess, list(HOME_PATHS), timeout=40)
    if r is not None:
        return decode_response(r), r.url, None
    if not finviz_session.live_html_allowed():
        return None, "", "live_html_skipped"
    try:
        r = sess.get("https://finviz.com/", headers=UA, timeout=40)
    except requests.RequestException as e:
        return None, "", f"public homepage: {e}"
    if r.status_code == 200 and not finviz_session.looks_like_login_html(r.text):
        return decode_response(r), r.url, None
    return None, "", f"homepage empty/{r.status_code}"


def build_report(
    asof: str | None = None,
    html: str | None = None,
    source: str = "live",
    source_url: str = "",
    archive_ts: str | None = None,
    archive_url: str | None = None,
    error: str | None = None,
) -> dict:
    asof = asof or et_now().date().isoformat()
    generated = et_now()
    parsed = parse_homepage_html(html) if html else None
    archive_dt = wayback_ts_to_dt(archive_ts) if archive_ts else None
    if source in ("wayback", "archive.ph"):
        legal_dt = archive_dt.astimezone(ET) if archive_dt else None
    else:
        legal_dt = generated
    clock_legal = clock_legal_at(legal_dt, asof)
    report = {
        "date": asof,
        "generated_at": generated.isoformat(),
        "timezone": "America/New_York",
        "source": source,
        "source_url": source_url,
        "archive_snapshot_ts": archive_ts,
        "archive_snapshot_at": (
            archive_dt.astimezone(ET).isoformat() if archive_dt else None
        ),
        "archive_url": archive_url,
        "clock_legal": clock_legal,
        "clock_rule": (
            "Pre-Open may use this file only if clock_legal is true: "
            "live Generated, or archive snapshot, is before this session's "
            "09:30 ET open. After-open / weekend captures stay on disk "
            "with clock_legal=false. Not wired into tape_anchor / predict."
        ),
        "kind": "finviz_homepage_market_digest",
        "represents": (
            "Live Finviz homepage market-day prose (prior cash close + "
            "overnight / weekend news). Not quote-page ticker blurbs."
        ),
        "error": error,
        "headline": None,
        "raw_text": None,
        "raw_summary_md": None,
        "index_moves": [],
        "oil": None,
        "fed_odds": None,
        "named_tickers": [],
        "calendar": [],
    }
    if parsed:
        for k in (
            "headline", "raw_text", "raw_summary_md", "index_moves", "oil",
            "fed_odds", "named_tickers", "calendar", "finviz_id",
            "finviz_ticker", "finviz_published_at", "finviz_source",
            "finviz_sentiment",
        ):
            if k in parsed:
                report[k] = parsed[k]
    return report


def to_markdown(report: dict) -> str:
    src = report.get("source") or "live"
    gen = report.get("generated_at") or ""
    legal = "true" if report.get("clock_legal") else "false"
    lines = [
        f"# Finviz homepage market digest — {report.get('date')}",
        "",
        f"**Generated:** {gen} (America/New_York)",
        f"**Source:** `{src}`",
    ]
    if report.get("archive_snapshot_ts") or report.get("archive_snapshot_at"):
        lines.append(
            f"**Archive snapshot:** `{report.get('archive_snapshot_ts')}` "
            f"({report.get('archive_snapshot_at')})"
        )
        if report.get("archive_url"):
            lines.append(f"**Archive URL:** {report['archive_url']}")
    lines += [
        f"**clock_legal:** `{legal}`",
        "",
        "## Clock",
        "",
        "This is the live Finviz homepage market-day prose (Weekend Brief / "
        "session recap) — **not** `YYYY-MM-DD_finviz_digest.md` (quote-page "
        "headlines + ~400 ticker blurbs).",
        "",
        "What it represents: often the **prior cash close plus overnight / "
        "weekend news**. A weekday pre-open capture is last session + overnight "
        "wires. A Saturday Weekend Brief is Friday's close + Monday setup.",
        "",
        "Same-morning Pre-Open may use this file **only if `clock_legal` is "
        "true** (Generated, or the archive snapshot timestamp, is before that "
        "session's 09:30 ET open). Theme Radar KEEP/KILL later. Capture + "
        "store only — not wired into tape_anchor / predict.",
        "",
        "## Narrative",
        "",
    ]
    headline = report.get("headline")
    if headline:
        lines.append(f"**{headline}**")
        lines.append("")
    raw = report.get("raw_text") or ""
    if raw:
        lines.append(raw)
        lines.append("")
    else:
        err = report.get("error") or "no homepage market narrative in this snapshot"
        lines.append(f"_unavailable: {err}_")
        lines.append("")
    lines += ["## Structured", ""]
    moves = report.get("index_moves") or []
    if moves:
        bits = [f"{m.get('name')} {m.get('change_pct'):+.2f}%" for m in moves]
        lines.append("- **Index moves:** " + "; ".join(bits))
    oil = report.get("oil")
    if oil:
        verb = f" ({oil['verb']})" if oil.get("verb") else ""
        lines.append(f"- **Oil:** {oil.get('name')} ${oil.get('price')}{verb}")
    fed = report.get("fed_odds")
    if fed:
        lines.append(
            f"- **Fed odds:** {fed.get('pct')}% {fed.get('action')} "
            f"({fed.get('text')})"
        )
    tickers = report.get("named_tickers") or []
    if tickers:
        lines.append("- **Named tickers:** " + ", ".join(tickers))
    cal = report.get("calendar") or []
    if cal:
        lines.append("- **Forward calendar:**")
        for c in cal:
            lines.append(f"  - {c}")
    if report.get("finviz_published_at"):
        lines.append(f"- **Finviz widget stamp:** {report['finviz_published_at']}")
    if report.get("finviz_sentiment"):
        lines.append(f"- **Finviz sentiment:** {report['finviz_sentiment']}")
    lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def save_report(report: dict) -> tuple[Path, Path]:
    NEWS_DIR.mkdir(parents=True, exist_ok=True)
    date_str = report["date"]
    jp = NEWS_DIR / f"{date_str}_finviz_market_digest.json"
    mp = NEWS_DIR / f"{date_str}_finviz_market_digest.md"
    jp.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str),
                  encoding="utf-8")
    mp.write_text(to_markdown(report), encoding="utf-8")
    latest = NEWS_DIR / "latest_finviz_market_digest.md"
    latest.write_text(mp.read_text(encoding="utf-8"), encoding="utf-8")
    return jp, mp


def report_has_narrative(report: dict | None) -> bool:
    if not isinstance(report, dict):
        return False
    raw = strip_markup(str(report.get("raw_text") or ""))
    headline = str(report.get("headline") or "").strip()
    return len(raw) >= 40 or len(headline) >= 20


def existing_morning_ok(date_str: str, force: bool = False) -> bool:
    if force:
        return False
    jp = NEWS_DIR / f"{date_str}_finviz_market_digest.json"
    if not jp.exists():
        return False
    try:
        payload = json.loads(jp.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    if not report_has_narrative(payload):
        return False
    gen = str(payload.get("generated_at") or "")
    if not gen.startswith(date_str) or len(gen) < 16:
        return False
    try:
        hm = int(gen[11:13]) * 100 + int(gen[14:16])
    except ValueError:
        return False
    if hm < MORNING_REWRITE_HM:
        return False
    return True


# ---------------------------------------------------------------------------
# Wayback / archive.ph backfill
# ---------------------------------------------------------------------------

def cdx_search(url: str, start: str, end: str,
               sess: requests.Session | None = None) -> list[dict]:
    """CDX rows for one URL. start/end are YYYY-MM-DD (inclusive)."""
    sess = sess or requests.Session()
    params = {
        "url": url,
        "matchType": "exact",
        "from": start.replace("-", ""),
        "to": end.replace("-", ""),
        "output": "json",
        "filter": "statuscode:200",
        "fl": "timestamp,original,statuscode,mimetype,length",
    }
    try:
        r = sess.get(CDX_URL, params=params, headers=UA, timeout=45)
    except requests.RequestException as e:
        print(f"[market_digest] CDX fail {url}: {e}")
        return []
    if r.status_code != 200:
        print(f"[market_digest] CDX {r.status_code} {url}")
        return []
    try:
        rows = r.json()
    except json.JSONDecodeError:
        return []
    if not isinstance(rows, list) or len(rows) < 2:
        return []
    out = []
    for row in rows[1:]:
        if not row or len(row) < 2:
            continue
        ts, orig = str(row[0]), str(row[1])
        dt = wayback_ts_to_dt(ts)
        if dt is None:
            continue
        et = dt.astimezone(ET)
        out.append({
            "timestamp": ts,
            "original": orig,
            "utc": dt.isoformat(),
            "et": et.isoformat(),
            "et_date": et.date().isoformat(),
            "length": row[4] if len(row) > 4 else None,
        })
    return out


def collect_cdx(start: str, end: str,
                sess: requests.Session | None = None) -> list[dict]:
    sess = sess or requests.Session()
    seen: set[str] = set()
    all_rows: list[dict] = []
    # Pad CDX from/to by one UTC day so evening ET captures are not dropped.
    cdx_from = (
        datetime.fromisoformat(start) - timedelta(days=1)
    ).date().isoformat()
    cdx_to = (
        datetime.fromisoformat(end) + timedelta(days=1)
    ).date().isoformat()
    for url in CDX_TARGETS:
        for row in cdx_search(url, cdx_from, cdx_to, sess=sess):
            key = row["timestamp"] + " " + row["original"]
            if key in seen:
                continue
            seen.add(key)
            all_rows.append(row)
    all_rows.sort(key=lambda r: r["timestamp"])
    return all_rows


def pick_capture_for_date(rows: list[dict], date_str: str) -> dict | None:
    """Best homepage snapshot for ET calendar date `date_str`.

    Prefer the latest capture that day before 09:30 ET (clock_legal).
    Else the latest same-ET-day capture (clock_legal=false).
    """
    day = [r for r in rows if r.get("et_date") == date_str]
    if not day:
        return None
    pre = [r for r in day if clock_legal_at(parse_et_iso(r.get("et")), date_str)]
    pool = pre or day
    return pool[-1]


def fetch_wayback_html(ts: str, original: str,
                       sess: requests.Session | None = None
                       ) -> tuple[str | None, str]:
    sess = sess or requests.Session()
    orig = original if original.startswith("http") else f"https://{original}"
    url = WAYBACK_ID.format(ts=ts, orig=orig)
    try:
        r = sess.get(url, headers=UA, timeout=50, allow_redirects=True)
    except requests.RequestException as e:
        print(f"[market_digest] wayback {ts}: {e}")
        return None, url
    if r.status_code != 200:
        print(f"[market_digest] wayback {ts}: HTTP {r.status_code}")
        return None, url
    return decode_response(r), r.url


def archive_ph_timemap(url: str, sess: requests.Session | None = None
                       ) -> list[dict]:
    """Best-effort archive.ph / archive.today TimeMap. Empty on 429/404."""
    sess = sess or requests.Session()
    out: list[dict] = []
    for host in ARCHIVE_PH_HOSTS:
        for path in (f"/timemap/json/{url}", f"/timemap/link/{url}"):
            try:
                r = sess.get(host + path, headers=UA, timeout=25,
                             allow_redirects=True)
            except requests.RequestException as e:
                print(f"[market_digest] archive.ph {host}: {e}")
                continue
            if r.status_code != 200:
                print(f"[market_digest] archive.ph {r.status_code} {host}{path}")
                continue
            text = r.text or ""
            # JSON TimeMap
            try:
                data = r.json()
            except json.JSONDecodeError:
                data = None
            if isinstance(data, list):
                for row in data:
                    if isinstance(row, dict) and row.get("datetime"):
                        out.append(row)
                    elif isinstance(row, list) and row:
                        out.append({"timestamp": row[0], "original": url})
            # Link-format Memento
            for m in re.finditer(
                r'<([^>]+)>;\s*rel="[^"]*memento[^"]*"[^;]*;\s*datetime="([^"]+)"',
                text, re.I,
            ):
                out.append({"url": m.group(1), "datetime": m.group(2)})
            if out:
                return out
    return out


def fetch_archive_ph_newest(url: str, sess: requests.Session | None = None
                            ) -> tuple[str | None, str | None, str | None]:
    """Return (html, snapshot_url, snapshot_ts_guess)."""
    sess = sess or requests.Session()
    for host in ARCHIVE_PH_HOSTS:
        target = f"{host}/newest/{url}"
        try:
            r = sess.get(target, headers=UA, timeout=40, allow_redirects=True)
        except requests.RequestException as e:
            print(f"[market_digest] archive.ph newest {host}: {e}")
            continue
        if r.status_code != 200:
            print(f"[market_digest] archive.ph newest {r.status_code} {host}")
            continue
        html = decode_response(r)
        ts = None
        m = re.search(r"/(\d{8,14})/", r.url)
        if m:
            ts = m.group(1)
        if "why-stock-moving" in html or "whyMoving" in html:
            return html, r.url, ts
    return None, None, None


def backfill_range(start: str, end: str, force: bool = False,
                   try_archive_ph: bool = True) -> dict:
    """Best-effort historical homepage narratives. Gaps stay gaps."""
    sess = requests.Session()
    sess.headers.update(UA)
    start_d = datetime.fromisoformat(start).date()
    end_d = datetime.fromisoformat(end).date()
    rows = collect_cdx(start, end, sess=sess)
    print(f"[market_digest] CDX captures={len(rows)} "
          f"et_days={len({r['et_date'] for r in rows})}")
    wrote = []
    skipped = []
    gaps = []
    day = start_d
    while day <= end_d:
        date_str = day.isoformat()
        day += timedelta(days=1)
        if existing_morning_ok(date_str, force=force) and not force:
            skipped.append(date_str)
            print(f"[market_digest] {date_str}: keep existing")
            continue
        cap = pick_capture_for_date(rows, date_str)
        html = None
        source = None
        source_url = ""
        archive_ts = None
        archive_url = None
        if cap:
            html, archive_url = fetch_wayback_html(
                cap["timestamp"], cap["original"], sess=sess)
            source = "wayback"
            source_url = archive_url
            archive_ts = cap["timestamp"]
        if (not html or parse_homepage_html(html) is None) and try_archive_ph:
            ph_html, ph_url, ph_ts = fetch_archive_ph_newest(
                "https://finviz.com/", sess=sess)
            parsed_ph = parse_homepage_html(ph_html) if ph_html else None
            # archive.ph /newest is "latest", not dated. Only accept if the
            # widget stamp or snapshot lands on this session date.
            if parsed_ph:
                pub = str(parsed_ph.get("finviz_published_at") or "")
                snap_et = wayback_ts_to_dt(ph_ts).astimezone(ET) if ph_ts and wayback_ts_to_dt(ph_ts) else None
                on_day = pub.startswith(date_str) or (
                    snap_et is not None and snap_et.date().isoformat() == date_str
                )
                if on_day:
                    html, source, source_url = ph_html, "archive.ph", ph_url or ""
                    archive_ts, archive_url = ph_ts, ph_url
                else:
                    print(f"[market_digest] {date_str}: archive.ph newest "
                          f"is other day ({pub[:10]}) — not reused")
        parsed = parse_homepage_html(html) if html else None
        if not parsed:
            gaps.append(date_str)
            print(f"[market_digest] {date_str}: gap (no archive narrative)")
            continue
        report = build_report(
            asof=date_str, html=html, source=source or "wayback",
            source_url=source_url, archive_ts=archive_ts,
            archive_url=archive_url,
        )
        if not report_has_narrative(report):
            gaps.append(date_str)
            print(f"[market_digest] {date_str}: gap (parsed empty)")
            continue
        jp, mp = save_report(report)
        wrote.append(date_str)
        print(
            f"[market_digest] {date_str}: {report['source']} "
            f"clock_legal={report['clock_legal']} "
            f"snap={report.get('archive_snapshot_ts')} → {mp.name}"
        )
    return {
        "from": start, "to": end,
        "wrote": wrote, "skipped": skipped, "gaps": gaps,
        "cdx_n": len(rows),
    }


def _land(date_str: str) -> None:
    try:
        from . import land_file
        land_file.land(
            date_str, "finviz_market_digest",
            title="Finviz homepage market digest",
        )
    except Exception as e:  # noqa: BLE001
        print(f"[market_digest] WARN: land failed: {e}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--html", default=None,
                    help="Parse this HTML file instead of live fetch")
    ap.add_argument("--text", default=None,
                    help="Parse this prose/markdown file (no scrape)")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--backfill", action="store_true")
    ap.add_argument("--from", dest="date_from", default="2026-08-20")
    ap.add_argument("--to", dest="date_to", default=None)
    ap.add_argument("--no-archive-ph", action="store_true")
    args = ap.parse_args()

    if args.backfill:
        end = args.date_to or et_now().date().isoformat()
        summary = backfill_range(
            args.date_from, end, force=args.force,
            try_archive_ph=not args.no_archive_ph,
        )
        print(json.dumps(summary, indent=2))
        for d in summary.get("wrote") or []:
            _land(d)
        return

    date_str = args.date or et_now().date().isoformat()
    if existing_morning_ok(date_str, force=args.force):
        print(f"[market_digest] {date_str}: skip, quality-ok already on disk")
        return
    html = None
    source = "live"
    source_url = "https://finviz.com/"
    err = None
    if args.text:
        raw = Path(args.text).read_text(encoding="utf-8")
        parsed = parse_market_digest_text(raw)
        report = build_report(asof=date_str, html=None, source="text")
        report.update(parsed)
        report["error"] = None
        # text path still needs clock from Generated
        jp, mp = save_report(report)
        print(f"[market_digest] {date_str}: text → {mp}")
        _land(date_str)
        return
    if args.html:
        html = decode_html_bytes(Path(args.html).read_bytes())
        source = "html_file"
        source_url = str(Path(args.html))
    else:
        html, source_url, err = fetch_homepage_html()
        source = "live"
    report = build_report(
        asof=date_str, html=html, source=source,
        source_url=source_url, error=err,
    )
    if not report_has_narrative(report):
        print(f"[market_digest] {date_str}: no narrative ({err or 'empty'})")
        # Do not write a stub that Theme Radar could mistake for a digest.
        raise SystemExit("finviz market digest: no homepage narrative")
    jp, mp = save_report(report)
    print(
        f"[market_digest] {report['date']}: source={report['source']} "
        f"clock_legal={report['clock_legal']} "
        f"tickers={report.get('named_tickers')} "
        f"moves={len(report.get('index_moves') or [])}"
    )
    print(f"[market_digest] {jp}")
    print(f"[market_digest] {mp}")
    _land(date_str)


if __name__ == "__main__":
    main()
