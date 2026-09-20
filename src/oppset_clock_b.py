"""Theme Radar Clock-B gap+RelVol opportunity-set — optional research feed.

Live on SRoyaltyy/theme-radar ``main`` @ ``a782cc2b``:

    https://github.com/SRoyaltyy/theme-radar/tree/main/research/oppset_clock_b

Clock B: ``join_morning`` is decision T (09:30). Every feature is from
``finviz_asof`` = T−1 (prior session snapshot). Same-day Gap / RelVol /
Change from snapshot T are outcomes and are never joined.

Research-only. Optional panel source + rank/filter. No Webull, no
flatten_robust, no KEEP claim. Pull the CSV over HTTPS — no git clone.
"""
from __future__ import annotations

import csv
import math
import os
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CACHE_DIR = ROOT / "data" / "factor_mine" / "oppset_clock_b"
DEFAULT_REF = "a782cc2b"
DEFAULT_REPO = "SRoyaltyy/theme-radar"
DEFAULT_PATH = "research/oppset_clock_b/oppset_flagged.csv"
RAW_TMPL = "https://raw.githubusercontent.com/{repo}/{ref}/{path}"

# T−1 columns we may stamp. Never aliases of same-day Change/Gap/RelVol.
STAMP_FIELDS = (
    "oppset", "opp_any", "opp_rvol", "opp_gap_pct", "opp_change_pct",
    "opp_finviz_asof",
)

PROOF = {
    "2026-09-16": 242,
    "2026-09-17": 261,
    "2026-09-18": 451,
}

_INDEX: dict[tuple[str, str], dict] | None = None
_INDEX_PATH: str | None = None


def _finite(x):
    if x is None or x == "":
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    if math.isnan(v) or math.isinf(v):
        return None
    return v


def raw_url(*, repo: str = DEFAULT_REPO, ref: str = DEFAULT_REF,
            path: str = DEFAULT_PATH) -> str:
    """HTTPS raw URL — no clone."""
    return RAW_TMPL.format(repo=repo, ref=ref, path=path)


def cache_path() -> Path:
    env = os.environ.get("FULLSCAN_OPPSET_CSV") or ""
    if env:
        return Path(env)
    return CACHE_DIR / "oppset_flagged.csv"


def pull(*, dest: Path | None = None, ref: str = DEFAULT_REF,
         timeout: float = 30.0) -> Path:
    """Download ``oppset_flagged.csv`` from theme-radar raw GitHub.

    Equivalent curl (no clone)::

        curl -fsSL \\
          https://raw.githubusercontent.com/SRoyaltyy/theme-radar/a782cc2b/research/oppset_clock_b/oppset_flagged.csv \\
          -o data/factor_mine/oppset_clock_b/oppset_flagged.csv
    """
    dest = dest or cache_path()
    dest.parent.mkdir(parents=True, exist_ok=True)
    url = raw_url(ref=ref)
    req = urllib.request.Request(url, headers={"User-Agent": "fullscan-oppset"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = resp.read()
    if not data or b"join_morning" not in data.split(b"\n", 1)[0]:
        raise RuntimeError(f"oppset pull did not look like a Clock-B CSV: {url}")
    dest.write_bytes(data)
    reset_index()
    return dest


def discover_csv() -> Path | None:
    """Local cache, env override, or a sibling theme-radar checkout."""
    candidates = [
        cache_path(),
        Path("/tmp/oppset_clock_b/oppset_flagged.csv"),
        ROOT.parent / "theme-radar" / DEFAULT_PATH,
    ]
    for p in candidates:
        if p.is_file() and p.stat().st_size > 100:
            return p
    return None


def parse_rows(text: str) -> list[dict]:
    """Parse Theme Radar flagged/all CSV. Clock-B: asof < join_morning."""
    out: list[dict] = []
    r = csv.DictReader(text.splitlines())
    for rec in r or []:
        morning = str(rec.get("join_morning") or "")[:10]
        asof = str(rec.get("finviz_asof") or "")[:10]
        ticker = str(rec.get("ticker") or "").strip().upper()
        if not morning or not ticker:
            continue
        if asof and asof >= morning:
            continue  # same-day snapshot is a leak
        any_opp = str(rec.get("any_opp") or "").strip() in ("1", "true", "True")
        out.append({
            "join_morning": morning,
            "finviz_asof": asof or None,
            "ticker": ticker,
            "rvol": _finite(rec.get("rvol")),
            "gap_pct": _finite(rec.get("gap_pct")),
            "change_pct": _finite(rec.get("change_pct")),
            "any_opp": any_opp,
        })
    return out


def load_index(path: Path | None = None) -> dict[tuple[str, str], dict]:
    """``(join_morning, ticker) → T−1 features``. Empty if no CSV."""
    global _INDEX, _INDEX_PATH
    p = path or discover_csv()
    key = str(p) if p else ""
    if _INDEX is not None and _INDEX_PATH == key:
        return _INDEX
    _INDEX_PATH = key
    if not p or not p.is_file():
        _INDEX = {}
        return _INDEX
    rows = parse_rows(p.read_text(encoding="utf-8"))
    _INDEX = {(r["join_morning"], r["ticker"]): r for r in rows}
    return _INDEX


def reset_index() -> None:
    global _INDEX, _INDEX_PATH
    _INDEX = None
    _INDEX_PATH = None


def lookup(date: str, ticker: str, index: dict | None = None) -> dict | None:
    idx = index if index is not None else load_index()
    return idx.get((str(date or "")[:10], str(ticker or "").strip().upper()))


def flagged_n(date: str, index: dict | None = None) -> int:
    idx = index if index is not None else load_index()
    d = str(date or "")[:10]
    return sum(1 for (m, _t) in idx if m == d)


def flagged_tickers(date: str, *, top_n: int | None = None,
                    index: dict | None = None) -> list[str]:
    """Flagged names for ``join_morning`` = date, highest T−1 rvol first."""
    idx = index if index is not None else load_index()
    d = str(date or "")[:10]
    rows = [r for (m, _t), r in idx.items() if m == d]
    rows.sort(key=lambda r: (-(r.get("rvol") or 0.0), r["ticker"]))
    names = [r["ticker"] for r in rows]
    if top_n is not None:
        return names[: int(top_n)]
    return names


def union_enabled() -> bool:
    """Opt-in: add flagged names as a panel source on remine (top 30)."""
    v = str(os.environ.get("FULLSCAN_OPPSET_UNION") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def stamp_row(row: dict, index: dict | None = None) -> dict:
    """Stamp T−1 oppset flags. Mutates ``row``. Missing feed → oppset False."""
    if not isinstance(row, dict):
        return row
    hit = lookup(row.get("date") or "", row.get("ticker") or "", index)
    if not hit:
        row.setdefault("oppset", False)
        return row
    row["oppset"] = True
    row["opp_any"] = bool(hit.get("any_opp"))
    row["opp_rvol"] = hit.get("rvol")
    row["opp_gap_pct"] = hit.get("gap_pct")
    row["opp_change_pct"] = hit.get("change_pct")
    row["opp_finviz_asof"] = hit.get("finviz_asof")
    return row


def attach_panel(panel: dict, index: dict | None = None) -> dict:
    if not isinstance(panel, dict):
        return panel
    idx = index if index is not None else load_index()
    for r in panel.get("rows") or []:
        stamp_row(r, idx)
    by_date = panel.get("by_date") or {}
    if isinstance(by_date, dict):
        for rows in by_date.values():
            for r in rows or []:
                stamp_row(r, idx)
    panel["_oppset"] = True
    panel["oppset_ref"] = DEFAULT_REF
    return panel


def apply_to_rec(rec: dict, date: str, ticker: str,
                 index: dict | None = None) -> dict:
    hit = lookup(date, ticker, index)
    if hit:
        rec["oppset"] = True
        rec["opp_any"] = bool(hit.get("any_opp"))
        rec["opp_rvol"] = hit.get("rvol")
        rec["opp_gap_pct"] = hit.get("gap_pct")
        rec["opp_change_pct"] = hit.get("change_pct")
        rec["opp_finviz_asof"] = hit.get("finviz_asof")
    else:
        rec.setdefault("oppset", False)
    return rec
