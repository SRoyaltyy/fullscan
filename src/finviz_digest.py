"""Finviz Daily Digest extractor + major-index market digests.

Sources:
  1) Latest data/exports/finviz_YYYY-MM-DD.csv  → per-ticker "Daily Digest" column
  2) Live quote pages for SPY / QQQ / DIA / IWM → top-level market narrative

Outputs (consumed by news_judge + predictors):
  01_daily/news/<date>_finviz_digest.md
  01_daily/news/<date>_finviz_digest.json
  01_daily/news/latest_finviz_digest.md

CLI:
  python -m src.finviz_digest [--date YYYY-MM-DD] [--skip-scrape]
"""
from __future__ import annotations

import argparse
import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from bs4 import BeautifulSoup

from . import config, finviz_session, output_qc, preopen

ROOT = Path(__file__).resolve().parent.parent
EXPORT_DIR = ROOT / "data" / "exports"
NEWS_DIR = ROOT / "01_daily" / "news"
ET = ZoneInfo(config.TZ)

INDEX_TICKERS = {
    "SPY": "S&P 500",
    "QQQ": "Nasdaq-100",
    "DIA": "Dow Jones",
    "IWM": "Russell 2000",
}

UA = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}
QUOTE_URLS = (
    "https://elite.finviz.com/quote.ashx?t={ticker}",
)

# Pure routine noise — keep but rank very low
DIVIDEND_RE = re.compile(
    r"(?i)^(.*(declares|announces).{0,40}(quarterly|regular).{0,20}(cash )?dividend"
    r"|.*dividend of \$?[\d.]+ per share)"
)
# High-signal keywords that should surface
SIGNAL_RE = re.compile(
    r"(?i)\b(earnings|eps|beats?|misses?|guidance|raises?|cuts?|approv|"
    r"fda|ema|certification|acquisition|merger|buyback|repurchase|"
    r"contract|award|tariff|sanction|export control|rate (cut|hike)|"
    r"fomc|cpi|pce|payrolls|recession|bankruptcy|investigation|"
    r"upgrade|downgrade|initiated|surge|plunge|record (high|low)|"
    r"capex|hyperscaler|hbm|shortage|sold.?out|backlog)\b"
)


def _session() -> requests.Session:
    return finviz_session.session()


def _latest_export(asof: str | None = None) -> Path | None:
    files = sorted(EXPORT_DIR.glob("finviz_????-??-??.csv"))
    if not files:
        return None
    if asof:
        exact = EXPORT_DIR / f"finviz_{asof}.csv"
        if exact.exists():
            return exact
        prior = [f for f in files if f.stem.replace("finviz_", "") <= asof]
        return prior[-1] if prior else files[-1]
    return files[-1]


def _load_ticker_digests(path: Path, max_rows: int = 400) -> list[dict]:
    df = pd.read_csv(path, low_memory=False)
    if "Daily Digest" not in df.columns:
        return []
    tcol = "Ticker" if "Ticker" in df.columns else df.columns[0]
    rows = []
    for _, r in df.iterrows():
        dig = r.get("Daily Digest")
        if dig is None or (isinstance(dig, float) and pd.isna(dig)):
            continue
        dig = str(dig).strip()
        if not dig or dig.lower() in ("nan", "none"):
            continue
        ticker = str(r.get(tcol, "")).strip().upper()
        if not ticker:
            continue
        is_div = bool(DIVIDEND_RE.search(dig))
        signal = bool(SIGNAL_RE.search(dig))
        rank = 0.0
        if signal:
            rank += 2.0
        if is_div:
            rank -= 1.5
        mcap = pd.to_numeric(r.get("Market Cap"), errors="coerce")
        if pd.notna(mcap):
            if mcap >= 50_000:
                rank += 0.8
            elif mcap >= 10_000:
                rank += 0.4
            elif mcap >= 2_000:
                rank += 0.15
        news_title = str(r.get("News Title") or "").strip()
        if news_title.lower() in ("nan", "none"):
            news_title = ""
        rows.append({
            "ticker": ticker,
            "digest": dig,
            "news_title": news_title,
            "sector": str(r.get("Sector") or "").strip(),
            "industry": str(r.get("Industry") or "").strip(),
            "is_dividend": is_div,
            "has_signal": signal,
            "rank": round(rank, 3),
            "source": "finviz_export",
        })
    rows.sort(key=lambda x: -x["rank"])
    seen: set[str] = set()
    out = []
    for r in rows:
        key = re.sub(r"\s+", " ", r["digest"].lower())[:100]
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
        if len(out) >= max_rows:
            break
    return out


def _scrape_index_digest(ticker: str, sess: requests.Session) -> dict | None:
    """Pull the top narrative / Daily Digest style summary from a quote page.

    Public finviz.com 403s from cloud IPs. Elite only, then yfinance so
    SPY/QQQ/DIA/IWM are never blank.
    """
    last_err = None
    html = None
    r = finviz_session.get(sess, [f"/quote.ashx?t={ticker}"], timeout=40)
    if r is not None:
        html = r.text
    else:
        last_err = f"elite quote.ashx?t={ticker} empty/403"
        print(f"[finviz_digest] {ticker} quote failed: {last_err}")

    if html:
        soup = BeautifulSoup(html, "html.parser")
        digest = None

        for lab in soup.select("td.snapshot-td2"):
            if "daily digest" in (lab.get_text(strip=True) or "").lower():
                val = lab.find_next_sibling("td")
                if val:
                    digest = val.get_text(" ", strip=True)
                    break

        if not digest:
            news = soup.select_one("table.news-table") or soup.select_one("#news")
            if news:
                first = news.select_one("a") or news.select_one("tr")
                if first:
                    digest = first.get_text(" ", strip=True)[:280]

        if not digest:
            for sel in ("div.quote-links a", "table.fullview-links a", "h2", "h1"):
                el = soup.select_one(sel)
                if el:
                    t = el.get_text(" ", strip=True)
                    if len(t) > 30:
                        digest = t[:280]
                        break

        if digest:
            return {
                "ticker": ticker,
                "index": INDEX_TICKERS.get(ticker, ticker),
                "digest": digest.strip(),
                "source": "finviz_quote",
                "error": None,
            }

    yf_row = _yf_index_digest(ticker)
    if yf_row:
        if last_err:
            yf_row["finviz_error"] = last_err
        return yf_row

    return {
        "ticker": ticker,
        "index": INDEX_TICKERS.get(ticker, ticker),
        "digest": None,
        "error": last_err or "no digest found",
    }


def _yf_index_digest(ticker: str) -> dict | None:
    """Last/prev close from yfinance when Finviz quote pages 403."""
    try:
        import yfinance as yf
        t = yf.Ticker(ticker)
        fi = t.fast_info
        last = fi.get("last_price") if hasattr(fi, "get") else getattr(fi, "last_price", None)
        prev = fi.get("previous_close") if hasattr(fi, "get") else getattr(fi, "previous_close", None)
        last_f = float(last) if last is not None else None
        prev_f = float(prev) if prev is not None else None
    except Exception as e:
        print(f"[finviz_digest] {ticker} yfinance fallback failed: {e}")
        return None
    if last_f is None or last_f <= 0:
        return None
    chg = None
    if prev_f and prev_f > 0:
        chg = (last_f - prev_f) / prev_f * 100.0
    chg_s = f"{chg:+.2f}%" if chg is not None else "n/a"
    prev_s = f"{prev_f:.2f}" if prev_f else "n/a"
    digest = (
        f"{INDEX_TICKERS.get(ticker, ticker)} {last_f:.2f} ({chg_s}) "
        f"prev close {prev_s} — yfinance (Finviz quote page unavailable)"
    )
    return {
        "ticker": ticker,
        "index": INDEX_TICKERS.get(ticker, ticker),
        "digest": digest,
        "source": "yfinance",
        "last": last_f,
        "previous_close": prev_f,
        "change_pct": None if chg is None else round(chg, 3),
        "error": None,
    }


def _scrape_indices(skip: bool = False) -> list[dict]:
    if skip:
        return []
    sess = _session()
    out = []
    for t in INDEX_TICKERS:
        row = _scrape_index_digest(t, sess)
        if row:
            out.append(row)
        time.sleep(0.55)
    return out


def build_report(asof: str | None = None, skip_scrape: bool = False) -> dict:
    asof = asof or datetime.now(ET).date().isoformat()
    export = _latest_export(asof)
    ticker_digests = _load_ticker_digests(export) if export else []
    index_digests = _scrape_indices(skip=skip_scrape)

    signal = [d for d in ticker_digests if d["has_signal"] and not d["is_dividend"]]
    top_signal = signal[:60]
    by_sector: dict[str, list] = {}
    for d in top_signal:
        sec = d["sector"] or "_unknown"
        by_sector.setdefault(sec, []).append(d)

    return {
        "date": asof,
        "generated_at": datetime.now(ET).isoformat(),
        "export_used": str(export.relative_to(ROOT)) if export else None,
        "ticker_digest_count": len(ticker_digests),
        "signal_count": len(signal),
        "index_digests": index_digests,
        "top_signal": top_signal,
        "by_sector": {k: v[:8] for k, v in sorted(
            by_sector.items(), key=lambda kv: -len(kv[1]))},
        "all_ticker_digests": ticker_digests[:200],
    }


def to_markdown(report: dict) -> str:
    lines = [
        f"# Finviz Daily Digest — {report['date']}",
        "",
        f"_Generated {report.get('generated_at')} · export={report.get('export_used')} · "
        f"ticker digests={report.get('ticker_digest_count')} · "
        f"high-signal={report.get('signal_count')}_",
        "",
        "## Major indices (live quote page)",
        "",
    ]
    for ix in report.get("index_digests") or []:
        name = ix.get("index") or ix.get("ticker")
        dig = ix.get("digest")
        if dig:
            lines.append(f"- **{name} ({ix.get('ticker')})**: {dig}")
        else:
            lines.append(f"- **{name} ({ix.get('ticker')})**: _(unavailable: {ix.get('error')})_")
    lines += ["", "## High-signal ticker digests (ranked)", ""]
    for d in (report.get("top_signal") or [])[:40]:
        lines.append(
            f"- **{d['ticker']}** [{d.get('sector','')[:18]}]: {d['digest']}"
        )
    lines += ["", "## By sector (top signal)", ""]
    for sec, items in (report.get("by_sector") or {}).items():
        lines.append(f"### {sec} (n={len(items)})")
        for d in items[:5]:
            lines.append(f"- {d['ticker']}: {d['digest']}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def inject_block(date_str: str | None = None, max_chars: int = 3200) -> str:
    """Helper for news_judge / predict: return compact Finviz digest block."""
    path = NEWS_DIR / "latest_finviz_digest.md"
    if date_str:
        cand = NEWS_DIR / f"{date_str}_finviz_digest.md"
        if cand.exists():
            path = cand
    if not path.exists():
        return ""
    text = path.read_text(encoding="utf-8")
    keep = []
    capture = False
    for line in text.splitlines():
        if line.startswith("## Major indices") or line.startswith("## High-signal"):
            capture = True
        if line.startswith("## By sector"):
            break
        if capture:
            keep.append(line)
    body = "\n".join(keep).strip()
    if not body:
        body = text[:max_chars]
    if len(body) > max_chars:
        body = body[:max_chars] + "\n...(truncated)"
    return (
        "=== FINVIZ DAILY DIGEST (AI catalyst summaries — elevated themes; "
        "prefer these over raw headline noise for B1 / sector S1) ===\n"
        f"{body}\n"
        "=== END FINVIZ DAILY DIGEST ===\n"
    )


def save_report(report: dict) -> tuple[Path, Path]:
    NEWS_DIR.mkdir(parents=True, exist_ok=True)
    date_str = report["date"]
    jp = NEWS_DIR / f"{date_str}_finviz_digest.json"
    mp = NEWS_DIR / f"{date_str}_finviz_digest.md"
    payload = {k: v for k, v in report.items() if k != "all_ticker_digests"}
    payload["all_ticker_digests_sample"] = report.get("all_ticker_digests", [])[:80]
    jp.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str),
                  encoding="utf-8")
    mp.write_text(to_markdown(report), encoding="utf-8")
    latest = NEWS_DIR / "latest_finviz_digest.md"
    latest.write_text(mp.read_text(encoding="utf-8"), encoding="utf-8")
    return jp, mp


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--skip-scrape", action="store_true",
                    help="Skip live index quote scrape (export only)")
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()
    date_str = args.date or datetime.now(ZoneInfo(config.TZ)).date().isoformat()
    jp = NEWS_DIR / f"{date_str}_finviz_digest.json"
    existing = output_qc.qc_finviz_digest(
        jp if jp.exists() else NEWS_DIR / f"{date_str}_finviz_digest.md")
    if existing.ok and not args.force:
        print(f"[finviz_digest] {date_str}: skip, quality-ok already on disk")
        return
    if preopen.past_predict_cutoff() and not args.force:
        if existing.ok:
            print(f"[finviz_digest] {date_str}: past 09:25 ET, keeping quality-ok")
            return
        print(f"[finviz_digest] {date_str}: past 09:25 ET — not writing a late copy")
        return
    report = build_report(asof=args.date, skip_scrape=args.skip_scrape)
    jp, mp = save_report(report)
    qc = output_qc.qc_finviz_digest(jp)
    if not qc.ok:
        print(f"[finviz_digest] QC FAIL ({qc.reason}) — throwing out")
        output_qc.reject(jp, mp)
        raise SystemExit("finviz digest produced no quality-ok file")
    print(
        f"[finviz_digest] {report['date']}: "
        f"tickers={report['ticker_digest_count']} "
        f"signal={report['signal_count']} "
        f"indices={sum(1 for x in report['index_digests'] if x.get('digest'))}"
    )
    print(f"[finviz_digest] {jp}")
    print(f"[finviz_digest] {mp}")
    print("--- inject preview ---")
    print(inject_block(report["date"])[:1200])


if __name__ == "__main__":
    main()
