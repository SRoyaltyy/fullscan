"""Finviz row linker for the Lane one-shot.

Candidates come from the latest ``data/exports/finviz_YYYY-MM-DD.csv``
(Company, Industry, Daily Digest, Description / Profile / Country when
those columns exist) plus on-disk company-profile JSON. An Elite
snapshot ticker is only a hint: it is dropped when the title does not
name that firm.

Never invent a ticker. Callers must keep at most 40 rows.
"""
from __future__ import annotations

import csv
import json
import re
from pathlib import Path

EXPORTS = Path("data/exports")
PROFILE_GLOBS = (
    "data/**/*company*profile*.json",
    "data/**/*profiles*.json",
)
ELITE_DIR = Path("data/theme_radar_snapshots")

_STOP = frozenset(
    "inc corp ltd adr co company group holdings plc sa nv ag class the and "
    "for with from that this into over under stock shares common".split()
)
# Article token -> substring that must already exist in a Finviz Company cell.
# Bound at load time: kept only when that substring hits 1–3 tickers.
_BRAND_IN_COMPANY = {
    "google": "alphabet",
    "gemini": "alphabet",
    "deepmind": "alphabet",
    "youtube": "alphabet",
    "waymo": "alphabet",
    "facebook": "meta platforms",
    "instagram": "meta platforms",
    "whatsapp": "meta platforms",
    "wegovy": "novo nordisk",
    "ozempic": "novo nordisk",
    "saxenda": "novo nordisk",
    "mounjaro": "lilly",
    "zepbound": "lilly",
    "semaglutide": "novo nordisk",
    "tirzepatide": "lilly",
}
_MOLECULE = re.compile(
    r"(?i)\b([a-z]{6,}tide|[a-z]{5,}mab|wegovy|ozempic|mounjaro|zepbound|"
    r"lanreotide|semaglutide|tirzepatide|somatuline)\b"
)
_TICKER_RE = re.compile(r"(?:\$|\()([A-Z]{1,5})\)?")
_WORD = re.compile(r"[a-z0-9][a-z0-9&+\-]{2,}")
# Single tokens that show up inside ordinary headlines. They are not a company.
_GENERIC = frozenset(
    "energy financial technology services capital securities markets health "
    "american united national first general international global power water "
    "medical science digital data group holding resource resources partners "
    "therapeutics pharmaceuticals biotechnology software systems solutions "
    "mission structure permission venues stock trade trades trading".split()
)


def latest_finviz_csv(root: Path | None = None) -> Path | None:
    base = (root or Path(".")) / EXPORTS if root else EXPORTS
    # root may already be the repo; EXPORTS is relative.
    folder = root / "data" / "exports" if root else EXPORTS
    files = []
    if not folder.is_dir():
        return None
    for path in folder.glob("finviz_20*.csv"):
        m = re.search(r"(20\d{2}-\d{2}-\d{2})", path.name)
        if m:
            files.append((m.group(1), path))
    if not files:
        return None
    files.sort()
    return files[-1][1]


def _clean_company(name: str) -> str:
    text = (name or "").lower().replace("(eli)", " eli ")
    text = re.sub(r"\([^)]*\)", " ", text)
    text = re.sub(r"&", " ", text)
    text = re.sub(
        r"\b(inc|corp|ltd|adr|co|company|group|holdings|plc|sa|nv|ag|"
        r"class|the|common|stock)\b",
        " ",
        text,
    )
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def company_aliases(name: str) -> list[str]:
    parts = [p for p in _clean_company(name).split() if p not in _STOP and len(p) > 2]
    if not parts:
        return []
    aliases = [" ".join(parts)]
    if len(parts) >= 2:
        aliases.append(" ".join(parts[:2]))
        aliases.append(f"{parts[1]} {parts[0]}")
    # Single distinctive token (Lilly, Amneal), never a generic English word.
    if len(parts) <= 2:
        for part in parts:
            if len(part) >= 5 and part not in _GENERIC:
                aliases.append(part)
    out, seen = [], set()
    for alias in aliases:
        alias = alias.strip()
        if len(alias) < 4 or alias in seen or alias in _GENERIC:
            continue
        seen.add(alias)
        out.append(alias)
    return out


def _has_phrase(text: str, phrase: str) -> bool:
    if not text or not phrase:
        return False
    return re.search(rf"(?<![a-z0-9]){re.escape(phrase)}(?![a-z0-9])", text) is not None


def _cap(value: str) -> float:
    try:
        return float(str(value or "").replace(",", "").strip() or 0)
    except ValueError:
        return 0.0


def _is_etf(row: dict) -> bool:
    blob = " ".join(
        str(row.get(k) or "")
        for k in ("Industry", "Asset Type", "ETF Type", "Company")
    ).lower()
    if "exchange traded" in blob or "etf" in blob:
        return True
    return False


def _instrument_type(row: dict, parent: bool) -> str:
    if parent:
        return "parent"
    company = str(row.get("Company") or "")
    if re.search(r"\bADR\b", company):
        return "adr"
    if _is_etf(row):
        return "etf"
    return "equity"


def load_profiles(root: Path | None = None) -> dict[str, dict]:
    """ticker -> profile fields, if a JSON dump is on disk. No database."""
    base = root or Path(".")
    found: dict[str, dict] = {}
    paths: list[Path] = []
    for pattern in PROFILE_GLOBS:
        paths.extend(base.glob(pattern))
    for path in paths:
        if "exposure_profiles" in path.name:
            continue
        try:
            blob = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        rows = blob if isinstance(blob, list) else (
            blob.get("profiles") or blob.get("rows") or blob.get("companies") or []
        )
        if isinstance(blob, dict) and not rows:
            rows = list(blob.values()) if all(isinstance(v, dict) for v in blob.values()) else []
        for row in rows:
            if not isinstance(row, dict):
                continue
            tick = str(row.get("ticker") or row.get("symbol") or "").upper().strip()
            if not tick:
                continue
            found[tick] = {
                "description": str(row.get("description") or row.get("profile") or ""),
                "company": str(row.get("company_name") or row.get("company") or ""),
                "source_file": str(path),
            }
    return found


def load_rows(root: Path | None = None) -> tuple[list[dict], str]:
    path = latest_finviz_csv(root)
    if path is None:
        return [], ""
    profiles = load_profiles(root)
    rows = []
    with path.open(newline="", encoding="utf-8", errors="replace") as fh:
        for raw in csv.DictReader(fh):
            tick = str(raw.get("Ticker") or "").upper().strip()
            if not tick:
                continue
            prof = profiles.get(tick) or {}
            desc = " ".join(
                str(raw.get(k) or "")
                for k in ("Description", "Profile", "Country")
                if raw.get(k)
            )
            desc = f"{desc} {prof.get('description') or ''}".strip()
            item = {
                "ticker": tick,
                "company": str(raw.get("Company") or prof.get("company") or "").strip(),
                "sector": str(raw.get("Sector") or "").strip(),
                "industry": str(raw.get("Industry") or "").strip(),
                "country": str(raw.get("Country") or "").strip(),
                "digest": str(raw.get("Daily Digest") or ""),
                "news_title": str(raw.get("News Title") or ""),
                "description": desc,
                "market_cap": _cap(raw.get("Market Cap") or ""),
                "profile_file": prof.get("source_file") or "",
            }
            item["aliases"] = company_aliases(item["company"])
            item["etf"] = _is_etf(raw)
            rows.append(item)
    return rows, str(path)


class FinvizIndex:
    def __init__(self, rows: list[dict], source: str):
        self.rows = rows
        self.source = source
        self.by_ticker = {r["ticker"]: r for r in rows}
        self.brands: dict[str, list[dict]] = {}
        for token, sub in _BRAND_IN_COMPANY.items():
            hits = [r for r in rows if sub in r["company"].lower() and not r["etf"]]
            ticks = {r["ticker"] for r in hits}
            if 1 <= len(ticks) <= 3:
                self.brands[token] = hits

    def title_names(self, title: str, row: dict) -> bool:
        text = (title or "").lower()
        if not text:
            return False
        for alias in row.get("aliases") or []:
            if len(alias) >= 4 and _has_phrase(text, alias):
                return True
        tick = row["ticker"].lower()
        if re.search(rf"\b{re.escape(tick)}\b", text):
            return True
        return False


_INDEX: FinvizIndex | None = None
_INDEX_ROOT: str | None = None


def get_index(root: Path | None = None) -> FinvizIndex:
    global _INDEX, _INDEX_ROOT
    key = str(root or "")
    if _INDEX is None or _INDEX_ROOT != key:
        rows, source = load_rows(root)
        _INDEX = FinvizIndex(rows, source)
        _INDEX_ROOT = key
    return _INDEX


def _add(bag: dict[str, dict], row: dict, score: int, why: str) -> None:
    tick = row["ticker"]
    cur = bag.get(tick)
    if cur is None or score > cur["score"]:
        bag[tick] = {"row": row, "score": score, "why": why}
    elif cur is not None and why not in cur["why"]:
        cur["why"] = f"{cur['why']}; {why}"


def candidate_rows(
    title: str,
    body: str = "",
    *,
    family: str = "",
    event_class: str = "",
    hint_ticker: str = "",
    limit: int = 40,
    root: Path | None = None,
) -> dict:
    """Return ≤limit Finviz instruments and any rejected snapshot hint.

    Scoring uses the article text against Company aliases, bound brand
    names, molecule hits in Daily Digest / description, and a few
    industry labels that are themselves Finviz Industry cells.
    """
    index = get_index(root)
    text = f"{title or ''}\n{body or ''}"
    low = text.lower()
    title_l = (title or "").lower()
    bag: dict[str, dict] = {}

    for match in _TICKER_RE.finditer(text):
        tick = match.group(1).upper()
        row = index.by_ticker.get(tick)
        if row and not row["etf"]:
            _add(bag, row, 12, "ticker_in_text")

    for row in index.rows:
        if row["etf"]:
            continue
        for alias in row["aliases"]:
            # Company names are matched on the title only. A body aside
            # ("not an energy item") must not pull that sector.
            if len(alias) >= 4 and _has_phrase(title_l, alias):
                _add(bag, row, 10, f"company:{alias}")
                break

    for token, hits in index.brands.items():
        if re.search(rf"\b{re.escape(token)}\b", low):
            for row in hits:
                _add(bag, row, 9, f"brand:{token}")

    for match in _MOLECULE.finditer(text):
        token = match.group(1).lower()
        for row in index.rows:
            if row["etf"]:
                continue
            blob = f"{row['digest']} {row['description']}".lower()
            if re.search(rf"\b{re.escape(token)}\b", blob):
                _add(bag, row, 7, f"digest:{token}")

    if re.search(r"\b(tsa|airports?|airlines?|flights?)\b", low):
        for row in index.rows:
            if row["industry"] == "Airlines" and not row["etf"]:
                _add(bag, row, 4, "industry:Airlines")
            elif row["industry"] == "Rental & Leasing Services" and re.search(
                r"avis|budget|hertz", row["company"], re.I
            ):
                _add(bag, row, 6, "industry:rental-car")

    if (
        re.search(r"\b(sec|tokenized|tokenised|exemptive|nms)\b", low)
        and re.search(r"\b(venue|exchange|stock|securit)", low)
    ):
        for row in index.rows:
            if row["sector"] == "Energy":
                continue
            if row["industry"] == "Financial Data & Stock Exchanges" and not row["etf"]:
                _add(bag, row, 5, "industry:exchanges")

    if family == "blast" and event_class == "blast_legal" and bag:
        industries = {bag[t]["row"]["industry"] for t in bag}
        named = {t for t in bag}
        for industry in industries:
            peers = [
                r for r in index.rows
                if r["industry"] == industry and not r["etf"] and r["ticker"] not in named
                and not any(_has_phrase(title_l, alias) for alias in r["aliases"])
            ]
            peers.sort(key=lambda r: r["market_cap"], reverse=True)
            if peers:
                _add(bag, peers[0], 3, "peer:same-industry")

    if family == "structure":
        for tick in list(bag):
            if bag[tick]["row"]["sector"] == "Energy":
                if not index.title_names(title, bag[tick]["row"]):
                    bag.pop(tick, None)

    rejected = []
    hint = str(hint_ticker or "").upper().strip()
    if hint:
        hinted = index.by_ticker.get(hint)
        if hinted is None:
            rejected.append({
                "ticker": hint, "reason": "hint_not_in_finviz",
            })
        elif not index.title_names(title, hinted):
            rejected.append({
                "ticker": hint,
                "company": hinted["company"],
                "reason": "title_does_not_name_company",
            })
            bag.pop(hint, None)
        else:
            _add(bag, hinted, 8, "hint_named_in_title")

    ranked = sorted(bag.values(), key=lambda item: (-item["score"], item["row"]["ticker"]))
    ranked = ranked[: max(1, limit)]
    # Parent share-class: higher market cap of a duplicated company name.
    company_best: dict[str, str] = {}
    for item in ranked:
        key = _clean_company(item["row"]["company"])
        prev = company_best.get(key)
        if prev is None or item["row"]["market_cap"] > index.by_ticker[prev]["market_cap"]:
            company_best[key] = item["row"]["ticker"]

    instruments = []
    for item in ranked:
        row = item["row"]
        key = _clean_company(row["company"])
        parent = company_best.get(key) == row["ticker"] and sum(
            1 for other in ranked if _clean_company(other["row"]["company"]) == key
        ) > 1
        source = "profile" if item["why"].startswith("profile") else "finviz_row"
        if row.get("profile_file") and "digest" in item["why"] and row["description"]:
            source = "profile"
        instruments.append({
            "entity_name": row["company"],
            "ticker": row["ticker"],
            "type": _instrument_type(
                {"Company": row["company"], "Industry": row["industry"]},
                parent,
            ),
            "sector": row["sector"],
            "industry": row["industry"],
            "source": source,
            "score": item["score"],
            "why": item["why"],
            "aliases": list(row.get("aliases") or []),
        })
    return {
        "finviz_file": index.source,
        "instruments": instruments[:limit],
        "rejected_hints": rejected,
        "n_scored": len(bag),
    }
