"""Export per-ticker stock-side profiles for the static backtest page.

Produces dashboard/scanner-backtest/profiles.json:
  {ticker: {name, industry, sector, source, enriched, warnings,
            facts: {kind: [{term, passage, confidence}]},
            filing: {form, filing_date, items: {1: text, "1A": text}}}}

Two profile tiers:
  - description-only: exactly what the engine's board holds for every ticker
    that appears in results.jsonl passes (what the matcher actually used).
  - EDGAR-enriched: for the top-N tickers by pass count, the latest 10-K
    Items 1 & 1A are fetched from SEC EDGAR and run through the same
    build_profile machinery as filing_sections, so the page can show the
    filing-derived facets (inputs, outputs, partners, exposures).

Also writes ticker-index.json: {ticker: [{id, decision, pass_count, reasons}]}
so the page can list every intake article that passed against a ticker.

Usage:
    python -m scripts.export_ticker_profiles --edgar-top 25
"""
from __future__ import annotations

import argparse
import json
import re
import time
from pathlib import Path

from src.link_correlation import ROOT
from src.scanner_backtest import open_program

OUT_DIR = ROOT / "dashboard" / "scanner-backtest"
RESULTS = OUT_DIR / "results.jsonl"

UA = {"User-Agent": "fullscan deterministic-parser research contact research@example.com"}
EDGAR_CACHE = ROOT / "data" / "edgar_cache"
EDGAR_TOP_DEFAULT = 25
MAX_FACTS_PER_KIND = 40
PASSAGE_LIMIT = 200
FILING_TEXT_LIMIT = 24000


def load_pass_tickers() -> dict[str, list[dict]]:
    """ticker -> slim per-article rows, from results.jsonl passes."""
    index: dict[str, list[dict]] = {}
    with RESULTS.open(encoding="utf-8") as handle:
        for line in handle:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            for p in row.get("passes") or []:
                ticker = str(p.get("ticker") or "").upper()
                if not ticker:
                    continue
                index.setdefault(ticker, []).append({
                    "id": row.get("id"),
                    "decision": row.get("decision"),
                    "tier": p.get("tier"),
                    "reasons": p.get("reasons") or [],
                })
    return index


def slim_profile(profile, source: str, enriched: bool) -> dict:
    facts: dict[str, list[dict]] = {}
    for fact in profile.facts:
        entry = {
            "term": fact.text,
            "passage": (fact.passage or "")[:PASSAGE_LIMIT],
            "confidence": getattr(fact, "confidence", ""),
        }
        facts.setdefault(fact.kind, []).append(entry)
    for kind, entries in facts.items():
        # most-confident first, then cap
        entries.sort(key=lambda e: (e["confidence"] != "legal_domain_root"
                                    and e["confidence"] != "exact_legal_tokens", -(len(e["passage"]))))
        facts[kind] = entries[:MAX_FACTS_PER_KIND]
    return {
        "name": profile.name,
        "industry": profile.industry,
        "sector": profile.sector,
        "source": source,
        "enriched": enriched,
        "warnings": list(getattr(profile, "warnings", []) or [])[:6],
        "facts": facts,
    }


# ---------------- SEC EDGAR ----------------

def edgar_get(url: str, cache_name: str, timeout: int = 30):
    import requests
    EDGAR_CACHE.mkdir(parents=True, exist_ok=True)
    cache = EDGAR_CACHE / cache_name
    if cache.is_file():
        return cache.read_text(encoding="utf-8", errors="replace")
    resp = requests.get(url, headers=UA, timeout=timeout)
    if resp.status_code != 200:
        return None
    text = resp.text
    cache.write_text(text, encoding="utf-8")
    time.sleep(0.4)  # SEC fair-access guideline
    return text


def cik_for_ticker(ticker: str) -> str | None:
    payload = edgar_get("https://www.sec.gov/files/company_tickers.json", "company_tickers.json")
    if not payload:
        return None
    for row in json.loads(payload).values():
        if str(row.get("ticker", "")).upper() == ticker:
            return str(row["cik_str"]).zfill(10)
    return None


def latest_10k(cik: str) -> dict | None:
    payload = edgar_get(f"https://data.sec.gov/submissions/CIK{cik}.json", f"sub_{cik}.json")
    if not payload:
        return None
    data = json.loads(payload)
    recent = data.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    for i, form in enumerate(forms):
        if form == "10-K":
            accession = recent["accessionNumber"][i].replace("-", "")
            doc = recent["primaryDocument"][i]
            return {
                "accession": recent["accessionNumber"][i],
                "filing_date": recent["filingDate"][i],
                "url": f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession}/{doc}",
            }
    return None


def extract_items(html: str) -> dict[str, str]:
    """Split a 10-K document into Item -> text.

    Body headings come in three shapes across filers:
    - inline:  "Item 1A. Risk Factors" (one line, NVDA/SPGI)
    - split:   "Item 1A." / "Risk Factors" / paragraph (SPGI alternate)
    - missing: no body heading at all; pages start with standalone page
      numbers and the TOC maps items to pages (BB).
    Titles come from the TOC triplets when parseable, else from a default
    map. Text is sliced between consecutive item headings, or between page
    markers as fallback. Returns {} when no grid is found.
    """
    from bs4 import BeautifulSoup
    text = BeautifulSoup(html, "html.parser").get_text('\n')
    text = re.sub(r"[ \t\xa0]+", " ", text)
    lines = [l.strip() for l in text.split('\n') if l.strip()]
    n = len(lines)

    toc = []
    for i in range(n - 2):
        m = re.match(r"^Item\s+([1-9A-C][A-C]?)[.]?$", lines[i])
        if not m:
            continue
        title = lines[i + 1]
        if (3 <= len(title) <= 80 and re.match(r"^[A-Z0-9\[\(]", title)
                and re.match(r"^\d{1,4}$", lines[i + 2])):
            toc.append((m.group(1).upper(), title, int(lines[i + 2]), i))

    DEFAULT_TITLES = {
        "1": "Business", "1A": "Risk Factors",
        "1B": "Unresolved Staff Comments", "1C": "Cybersecurity",
        "2": "Properties",
        "7": "Management's Discussion and Analysis",
        "7A": "Quantitative and Qualitative Disclosures",
    }
    titles = {l: t for l, t, _, _ in toc} if toc else dict(DEFAULT_TITLES)
    toc_end = max((i for _, _, _, i in toc), default=0) + 3
    pages = {l: pg for l, _, pg, _ in toc}
    wanted = [t for t in ("1", "1A", "1B", "1C", "2", "7", "7A") if t in titles]

    def num_line(i):
        return bool(re.match(r"^\d{1,4}$", lines[i]))

    # strategy 1: body headings (inline or split shape)
    heads = []
    for label in list(titles):
        title = titles[label]
        inline = re.compile(r"^Item\s+" + re.escape(label) + r"[.]?\s+" + re.escape(title) + r"$", re.I)
        for i in range(toc_end, n - 1):
            hit = False
            if inline.match(lines[i]):
                hit = not num_line(i + 1)
            elif (re.match(r"^Item\s+" + re.escape(label) + r"[.]?$", lines[i], re.I)
                    and i + 2 < n and lines[i + 1].lower() == title.lower() and not num_line(i + 2)):
                hit = True
            if hit:
                heads.append((label, i))
                break
    heads.sort(key=lambda h: h[1])

    items = {}
    if len(heads) >= 2:
        for j, (label, start_i) in enumerate(heads):
            if label not in wanted:
                continue
            stop = heads[j + 1][1] if j + 1 < len(heads) else n
            body = '\n'.join(lines[start_i + 1:stop]).strip()
            if len(body) > 400:
                items[label] = body[:FILING_TEXT_LIMIT]
        if items:
            return items

    # strategy 2: page markers (needs a parsed TOC with page numbers)
    if not pages:
        return {}
    markers = []
    for i in range(toc_end, n):
        if num_line(i):
            markers.append((int(lines[i]), i))
    if not markers:
        return {}

    def find_marker(page, after):
        for num, idx in markers:
            if idx > after and num == page:
                return idx
        for num, idx in markers:
            if idx > after and num == page - 1:
                return idx
        return None

    starts = {}
    prev = toc_end
    for label in wanted:
        idx = find_marker(pages[label], prev)
        if idx is None:
            continue
        starts[label] = idx
        prev = idx
    order = sorted(starts, key=lambda l: starts[l])
    for j, label in enumerate(order):
        stop = starts[order[j + 1]] if j + 1 < len(order) else n
        body = '\n'.join(lines[starts[label] + 1:stop]).strip()
        if len(body) > 400:
            items[label] = body[:FILING_TEXT_LIMIT]
    return items

def edgar_filing(ticker: str) -> dict | None:
    try:
        cik = cik_for_ticker(ticker)
        if not cik:
            return None
        filing = latest_10k(cik)
        if not filing:
            return None
        html = edgar_get(filing["url"], f"10k_{cik}_{filing['accession'].replace('-','_')}.html", timeout=60)
        if not html:
            return None
        items = extract_items(html)
        if "1" not in items and "1A" not in items:
            return None
        return {"form": "10-K", "filing_date": filing["filing_date"], "items": items}
    except Exception as exc:  # noqa: BLE001 - enrichment is best-effort
        return {"error": type(exc).__name__}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--edgar-top", type=int, default=EDGAR_TOP_DEFAULT,
                        help="enrich the top-N tickers by pass count from SEC EDGAR 10-Ks")
    parser.add_argument("--also", nargs="*", default=[],
                        help="always-enrich these extra tickers (e.g. proof-of-concept names)")
    args = parser.parse_args(argv)

    started = time.time()
    index = load_pass_tickers()
    for extra in args.also:
        index.setdefault(extra.upper(), [])  # ensure profile exists even with no passes
    counts = {t: len(rows) for t, rows in index.items()}
    top = sorted(counts, key=lambda t: -counts[t])[: args.edgar_top]
    for extra in args.also:
        if extra.upper() not in top:
            top.append(extra.upper())
    print(f"{len(index)} pass tickers; enriching top {len(top)} from EDGAR", flush=True)

    descriptions = ROOT / "excel_bot" / "data" / "finviz_with_descriptions.csv"
    sectors = ROOT / "data" / "finviz" / "latest.csv"
    if not sectors.is_file():
        sectors = None
    board, _names = open_program(descriptions, sectors, None)
    eb = board.evidence_board
    EDGAR_CACHE.mkdir(parents=True, exist_ok=True)

    profiles: dict[str, dict] = {}
    for n, ticker in enumerate(sorted(index), start=1):
        prof = eb.profiles.get(ticker)
        if prof is None:
            profiles[ticker] = {"name": "", "industry": "", "sector": "",
                                "source": "missing", "enriched": False,
                                "warnings": ["ticker not on board"], "facts": {}}
            continue
        profiles[ticker] = slim_profile(prof, "finviz_description", enriched=False)
        if n % 500 == 0:
            print(f"  {n}/{len(index)} ({time.time() - started:.0f}s)", flush=True)

    for ticker in top:
        filing = edgar_filing(ticker)
        if not filing or filing.get("error") or not filing.get("items"):
            print(f"  EDGAR {ticker}: {'error ' + filing['error'] if filing and filing.get('error') else 'no 10-K items'}", flush=True)
            continue
        from src.stock_evidence import build_profile, Catalog
        base = next((r for r in board.source_rows if r.get("ticker") == ticker), None)
        if base is None:
            old = eb.profiles[ticker]
            base = {"ticker": ticker, "name": old.name, "industry": old.industry,
                    "sector": old.sector, "description": ""}
        row = dict(base)
        row["filing_sections"] = [
            {"text": text, "kind": "business" if label == "1" else "risk",
             "source": "sec_edgar", "form": "10-K", "section": f"item_{label}",
             "filing_date": filing["filing_date"], "complete": True}
            for label, text in filing["items"].items()
        ]
        rich = build_profile(row, Catalog(board.source_rows))
        merged = slim_profile(rich, "sec_edgar_10k", enriched=True)
        merged["filing"] = {"form": filing["form"], "filing_date": filing["filing_date"],
                            "items": {k: v[:6000] for k, v in filing["items"].items()}}
        profiles[ticker] = merged
        print(f"  EDGAR {ticker}: {sum(len(v) for v in merged['facts'].values())} facts "
              f"from items {sorted(filing['items'])} ({time.time() - started:.0f}s)", flush=True)

    (OUT_DIR / "profiles.json").write_text(
        json.dumps(profiles, ensure_ascii=False), encoding="utf-8")
    (OUT_DIR / "ticker-index.json").write_text(
        json.dumps(index, ensure_ascii=False), encoding="utf-8")
    size = (OUT_DIR / "profiles.json").stat().st_size
    print(f"profiles.json {size/1e6:.1f}MB · ticker-index.json "
          f"{(OUT_DIR / 'ticker-index.json').stat().st_size/1e6:.1f}MB "
          f"({time.time() - started:.0f}s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
