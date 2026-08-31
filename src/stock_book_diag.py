"""Stock Book readiness — inputs, buy/sell names, and the .io dashboard.

Inspects the workflows that feed the ranker, writes the actual 1d
buy/sell list with lookback red/yellow/green boxes traced to those
inputs, and checks that paper_trade published dashboard/index.html
to GitHub Pages. File existence is not enough: empty, truncated,
carry-forward, timeout stubs, and output_qc failures are FAIL.

A workflow is:

  OK       every required output exists and passes QC
  PARTIAL  at least one required output is OK, but not all
  FAIL     zero required outputs pass QC (missing, empty, or QC fail)

Optional outputs never change the workflow flag. Required *inputs* are
reported separately so a blocked job is obvious.

CLI:
  python -m src.stock_book_diag [--date YYYY-MM-DD] [--write]
                               [--as-of|--strict] [--rebuild-if-missing]
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from . import book_era, config, output_qc
from . import stock_book_diag_signals as signals
from .sector_taxonomy import FINVIZ_SECTORS

ROOT = Path(__file__).resolve().parent.parent
ET = ZoneInfo(config.TZ)
REPO_DEFAULT = "SRoyaltyy/fullscan"

SECTOR_SLUGS = [
    re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")
    for s in FINVIZ_SECTORS
]


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

@dataclass
class FileCheck:
    key: str
    name: str
    path: str
    role: str  # required | optional | input | era
    status: str  # OK | FAIL | MISSING | SKIP
    reason: str = ""
    size: int = 0
    source: str = ""

    def explain(self) -> str:
        extra = f" — {self.reason}" if self.reason else ""
        return f"[{self.status}] {self.name} {self.path}{extra}"


@dataclass
class WorkflowCheck:
    key: str
    name: str
    yaml: str
    status: str
    inputs_ready: bool
    n_req_ok: int = 0
    n_req: int = 0
    n_opt_ok: int = 0
    n_opt: int = 0
    files: list[FileCheck] = field(default_factory=list)
    gh_run: dict | None = None

    @property
    def blocked(self) -> bool:
        return (not self.inputs_ready) and self.status != "OK"


@dataclass
class Report:
    date: str
    generated_at: str
    ranker_ready: bool
    overall: str
    workflows: list[WorkflowCheck] = field(default_factory=list)
    blockers: list[str] = field(default_factory=list)
    decisions: dict = field(default_factory=dict)
    pages: dict = field(default_factory=dict)
    era: dict = field(default_factory=dict)
    rebuilt: bool = False


# ---------------------------------------------------------------------------
# Low-level readers
# ---------------------------------------------------------------------------

def _p(*parts: str) -> Path:
    return ROOT.joinpath(*parts)


def _read(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return ""


def _read_json(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return None


def _csv_header_rows(path: Path) -> tuple[list[str], int]:
    try:
        with path.open(encoding="utf-8", errors="ignore", newline="") as fh:
            reader = csv.reader(fh)
            header = next(reader, [])
            n = sum(1 for _ in reader)
        return header, n
    except OSError:
        return [], 0


def _from_qc(r: output_qc.QCResult) -> tuple[str, str, int]:
    if r.ok:
        return "OK", "", r.size
    if not Path(r.path).exists() or r.reason in (
            "missing", "postclose_baseline_missing"):
        return "MISSING", r.reason or "missing", r.size
    why = r.reason or "qc_fail"
    if r.timeout:
        why = f"timeout_stub ({why})"
    if r.carried:
        why = f"carried ({why})"
    if r.empty:
        why = f"empty/truncated ({why})"
    return "FAIL", why, r.size


def _generic_text(path: Path, min_chars: int = 50) -> tuple[str, str, int]:
    if not path.exists():
        return "MISSING", "missing", 0
    text = _read(path)
    size = len(text)
    if not text.strip() or size < 8:
        return "FAIL", f"empty ({size} chars)", size
    if output_qc.looks_like_timeout(text):
        return "FAIL", "timeout_stub", size
    if size < min_chars:
        return "FAIL", f"too_small({size})", size
    return "OK", "", size


def _stale_date(data: dict, expected: str) -> str:
    for k in ("date", "scan_date", "asof", "asof_date"):
        v = str(data.get(k) or "")[:10]
        if re.match(r"\d{4}-\d{2}-\d{2}$", v) and v != expected:
            return f"pass-over {k}={v} (want {expected})"
    return ""


# ---------------------------------------------------------------------------
# Per-kind QC
# ---------------------------------------------------------------------------

def inspect_kind(kind: str, path: Path, date: str) -> tuple[str, str, int]:
    """Return (status, reason, size) for one artifact."""
    if kind == "map_heat_baseline":
        return _from_qc(output_qc.qc_map_heat_baseline(path))
    if kind == "map_heat":
        return _from_qc(output_qc.qc_map_heat(path))
    if kind == "finviz_digest":
        return _from_qc(output_qc.qc_finviz_digest(path))
    if kind == "news_parse":
        return _from_qc(output_qc.qc_news_parse(path))
    if kind == "events":
        return _from_qc(output_qc.qc_events_date(date))
    if kind == "news_judge":
        return _from_qc(output_qc.qc_news_judge(path))
    if kind == "news_actions":
        return _from_qc(output_qc.qc_news_actions(path))
    if kind == "general_predict":
        return _from_qc(output_qc.qc_general_predict(path))
    if kind == "sector_predict":
        return _from_qc(output_qc.qc_sector_predict(path))
    if kind == "map_heat_research_md":
        return _qc_morning_research_md(path)
    if kind == "map_heat_research_json":
        return _qc_morning_research_json(path)
    if kind == "weather":
        return _qc_weather(path, date)
    if kind == "ab_raw":
        return _qc_ab_csv(path, date, enriched=False)
    if kind == "ab_enriched":
        return _qc_ab_csv(path, date, enriched=True)
    if kind == "catalyst":
        return _qc_catalyst(path, date)
    if kind == "join_ranked":
        return _qc_join(path)
    if kind == "peer_rs":
        return _qc_peer_rs(path)
    if kind == "stock_book_json":
        return _qc_stock_book_json(path, date)
    if kind == "stock_book_md":
        return _generic_text(path, min_chars=200)
    if kind == "preopen_qc":
        return _qc_preopen_qc(path, date)
    if kind == "preopen_status":
        return _qc_preopen_status(path, date)
    if kind == "grok_review":
        return _qc_grok_review(path, date)
    if kind == "sector_board":
        return _qc_sector_board(path, date)
    if kind == "green_json":
        return _qc_json_object(path)
    if kind == "md":
        return _generic_text(path, min_chars=50)
    if kind == "html":
        return _qc_html(path)
    if kind == "dashboard_session":
        return signals.inspect_dashboard_html(path, date)
    if kind == "pages_live":
        return signals.inspect_pages_live(date)
    return _generic_text(path)


def _qc_morning_research_md(path: Path) -> tuple[str, str, int]:
    status, reason, size = _from_qc(output_qc.qc_map_heat_research(path))
    if status != "OK":
        return status, reason, size
    js = Path(str(path).replace("_research.md", "_research.json"))
    data = _read_json(js)
    if isinstance(data, dict) and data.get("phase") == "morning_bootstrap":
        return "FAIL", "morning_bootstrap (not a real refresh)", size
    return status, reason, size


def _qc_morning_research_json(path: Path) -> tuple[str, str, int]:
    if not path.exists():
        return "MISSING", "missing", 0
    data = _read_json(path)
    text = _read(path)
    size = len(text)
    if not isinstance(data, dict):
        return "FAIL", "unparseable_json", size
    if output_qc.looks_like_timeout(text):
        return "FAIL", "timeout_stub", size
    phase = data.get("phase")
    if phase == "morning_bootstrap":
        return "FAIL", "morning_bootstrap (not a real refresh)", size
    if phase != "morning_refresh":
        return "FAIL", f"not_morning_refresh({phase})", size
    n = len(data.get("cards") or [])
    if n < 20:
        return "FAIL", f"too_few_cards({n})", size
    return "OK", f"cards={n}", size


def _qc_weather(path: Path, date: str) -> tuple[str, str, int]:
    if not path.exists():
        return "MISSING", "missing", 0
    data = _read_json(path)
    text = _read(path)
    size = len(text)
    if not isinstance(data, dict):
        return "FAIL", "unparseable_json", size
    stale = _stale_date(data, date)
    if stale:
        return "FAIL", stale, size
    sectors = (data.get("signals") or {}).get("sectors") or {}
    if not isinstance(sectors, dict) or len(sectors) < 5:
        return "FAIL", f"too_few_sectors({len(sectors) if isinstance(sectors, dict) else 0})", size
    return "OK", f"sectors={len(sectors)}", size


def _qc_ab_csv(path: Path, date: str, *, enriched: bool) -> tuple[str, str, int]:
    if not path.exists():
        return "MISSING", "missing", 0
    size = path.stat().st_size
    if size < 32:
        return "FAIL", f"empty ({size} bytes)", size
    header, n = _csv_header_rows(path)
    cols = {c.strip() for c in header}
    if "Ticker" not in cols and not header:
        return "FAIL", "unreadable_csv", size
    if "Ticker" not in cols:
        return "FAIL", "missing Ticker column", size
    if n < 50:
        return "FAIL", f"too_few_rows({n})", size
    if enriched:
        score_col = next(
            (c for c in ("score_enriched", "score", "checklist_score") if c in cols),
            None,
        )
        if score_col is None:
            return "FAIL", "no usable score column", size
        nz = 0
        try:
            with path.open(encoding="utf-8", errors="ignore", newline="") as fh:
                reader = csv.DictReader(fh)
                for i, row in enumerate(reader):
                    if i > 4000:
                        break
                    raw = str(row.get(score_col) or "").strip()
                    try:
                        if float(raw) != 0.0:
                            nz += 1
                    except ValueError:
                        continue
        except OSError:
            return "FAIL", "unreadable_csv", size
        if nz == 0:
            return "FAIL", "all scores zero", size
    return "OK", f"rows={n}", size


def _qc_catalyst(path: Path, date: str) -> tuple[str, str, int]:
    if not path.exists():
        return "MISSING", "missing", 0
    data = _read_json(path)
    text = _read(path)
    size = len(text)
    if not isinstance(data, dict):
        return "FAIL", "unparseable_json", size
    if output_qc.looks_like_timeout(text):
        return "FAIL", "timeout_stub", size
    stale = _stale_date(data, date)
    if stale:
        return "FAIL", stale, size
    rows = data.get("dossiers") or []
    if not isinstance(rows, list):
        return "FAIL", "missing dossiers list", size
    try:
        from .catalyst_daily import usable_dossier
        n_ok = sum(1 for r in rows if isinstance(r, dict) and usable_dossier(r))
    except Exception:  # noqa: BLE001 — keep the diagnostic import-safe
        n_ok = sum(
            1 for r in rows
            if isinstance(r, dict) and r.get("net_signal") and not r.get("error")
        )
    if n_ok < 1:
        return "FAIL", f"zero usable dossiers (n={len(rows)})", size
    return "OK", f"usable={n_ok}/{len(rows)}", size


def _qc_join(path: Path) -> tuple[str, str, int]:
    if not path.exists():
        return "MISSING", "missing", 0
    size = path.stat().st_size
    if size < 8:
        return "FAIL", f"empty ({size} bytes)", size
    header, n = _csv_header_rows(path)
    if "Ticker" not in {c.strip() for c in header}:
        return "FAIL", "missing Ticker column", size
    if n < 1000:
        return "FAIL", f"too_few_rows({n})", size
    return "OK", f"rows={n}", size


def _qc_peer_rs(path: Path) -> tuple[str, str, int]:
    if not path.exists():
        return "MISSING", "missing", 0
    size = path.stat().st_size
    if size < 32:
        return "FAIL", f"empty ({size} bytes)", size
    header, n = _csv_header_rows(path)
    if not header:
        return "FAIL", "unreadable_csv", size
    if n < 100:
        return "FAIL", f"too_few_rows({n})", size
    return "OK", f"rows={n}", size


def _qc_stock_book_json(path: Path, date: str) -> tuple[str, str, int]:
    if not path.exists():
        return "MISSING", "missing", 0
    data = _read_json(path)
    text = _read(path)
    size = len(text)
    if not isinstance(data, dict):
        return "FAIL", "unparseable_json", size
    meta = data.get("meta") if isinstance(data.get("meta"), dict) else {}
    stale = _stale_date(meta or data, date)
    if stale:
        return "FAIL", stale, size
    books = data.get("books")
    if not isinstance(books, dict) or not books:
        return "FAIL", "missing books", size
    n_names = 0
    for entry in books.values():
        if not isinstance(entry, dict):
            continue
        n_names += len(entry.get("buy") or []) + len(entry.get("sell") or [])
    if n_names < 1:
        return "FAIL", "empty books (no buy/sell names)", size
    return "OK", f"horizons={len(books)} names={n_names}", size


def _qc_json_object(path: Path) -> tuple[str, str, int]:
    if not path.exists():
        return "MISSING", "missing", 0
    data = _read_json(path)
    size = path.stat().st_size
    if not isinstance(data, dict):
        return "FAIL", "unparseable_json", size
    if size < 8:
        return "FAIL", f"empty ({size} bytes)", size
    return "OK", "", size


def _qc_preopen_qc(path: Path, date: str) -> tuple[str, str, int]:
    if not path.exists():
        return "MISSING", "missing", 0
    data = _read_json(path)
    size = path.stat().st_size
    if not isinstance(data, dict):
        return "FAIL", "unparseable_json", size
    stale = _stale_date(data, date)
    if stale:
        return "FAIL", stale, size
    if not isinstance(data.get("items"), list):
        return "FAIL", "missing items", size
    flag = data.get("all_ok")
    return "OK", f"all_ok={flag}", size


def _qc_preopen_status(path: Path, date: str) -> tuple[str, str, int]:
    if not path.exists():
        return "MISSING", "missing", 0
    data = _read_json(path)
    size = path.stat().st_size
    if not isinstance(data, dict):
        return "FAIL", "unparseable_json", size
    stale = _stale_date(data, date)
    if stale:
        return "FAIL", stale, size
    if "all_ok" not in data:
        return "FAIL", "missing all_ok", size
    missing = data.get("missing_required") or []
    detail = f"all_ok={data.get('all_ok')}"
    if missing:
        detail += f" missing={missing[:6]}"
    return "OK", detail, size


def _qc_grok_review(path: Path, date: str) -> tuple[str, str, int]:
    if not path.exists():
        return "MISSING", "missing", 0
    data = _read_json(path)
    text = _read(path)
    size = len(text)
    if not isinstance(data, dict):
        return "FAIL", "unparseable_json", size
    if output_qc.looks_like_timeout(text):
        return "FAIL", "timeout_stub", size
    stale = _stale_date(data, date)
    if stale:
        return "FAIL", stale, size
    notes = str(data.get("notes") or data.get("fails") or "")[:120]
    detail = f"ok={data.get('ok')}"
    if notes:
        detail += f" {notes}"
    return "OK", detail, size


def _qc_sector_board(path: Path, date: str) -> tuple[str, str, int]:
    if not path.exists():
        return "MISSING", "missing", 0
    data = _read_json(path)
    size = path.stat().st_size
    if not isinstance(data, dict):
        return "FAIL", "unparseable_json", size
    stale = _stale_date(data, date)
    if stale:
        return "FAIL", stale, size
    n = 0
    for key in ("sectors", "board", "scores"):
        val = data.get(key)
        if isinstance(val, (list, dict)):
            n = max(n, len(val))
    if n < 8 and len(data) < 8:
        return "FAIL", "board too thin", size
    return "OK", "", size


def _qc_html(path: Path) -> tuple[str, str, int]:
    if not path.exists():
        return "MISSING", "missing", 0
    text = _read(path)
    size = len(text)
    if size < 80 or "<html" not in text.lower():
        return "FAIL", f"not html or too_small({size})", size
    return "OK", "", size


# ---------------------------------------------------------------------------
# Workflow contracts (user-specified I/O)
# ---------------------------------------------------------------------------

def _file(key: str, name: str, rel: str, role: str, kind: str,
          source: str = "") -> dict:
    return {
        "key": key, "name": name, "rel": rel, "role": role,
        "kind": kind, "source": source,
    }


def workflow_specs(date: str, as_of: bool = True) -> list[dict]:
    heat = f"01_daily/map_heat/{date}"
    news = f"01_daily/news/{date}"
    ev = f"01_daily/events/{date}"
    gen = f"01_daily/general/{date}"
    sec = f"01_daily/sectors/{date}"
    wx = f"01_daily/weather/{date}"
    cat = f"01_daily/catalyst/{date}"
    ab = f"data/ab_checklist/{date}"
    book = f"data/stock_book/{date}"
    sectors = [
        _file(f"sector_{slug}", f"Sector predict — {slug}",
              f"{sec}/{slug}_predict.md", "required", "sector_predict")
        for slug in SECTOR_SLUGS
    ]
    specs = [
        {
            "key": "postclose",
            "name": "Post-close research",
            "yaml": "map_heat_postclose.yml",
            "files": [
                _file("baseline_json", "Captain baseline JSON",
                      f"{heat}_research_baseline.json", "required",
                      "map_heat_baseline"),
                _file("baseline_md", "Captain baseline MD",
                      f"{heat}_research_baseline.md", "optional", "md"),
                _file("heat_json", "Map heat tables JSON",
                      f"{heat}_map_heat.json", "required", "map_heat"),
                _file("heat_md", "Map heat tables MD",
                      f"{heat}_map_heat.md", "optional", "md"),
            ],
        },
        {
            "key": "finviz",
            "name": "Finviz scrape",
            "yaml": "finviz_preopen_scrape.yml",
            "files": [
                _file("digest_json", "Finviz digest JSON",
                      f"{news}_finviz_digest.json", "required", "finviz_digest"),
                _file("digest_md", "Finviz digest MD",
                      f"{news}_finviz_digest.md", "optional", "md"),
            ],
        },
        {
            "key": "preopen",
            "name": "Pre-Open ALL",
            "yaml": "preopen_all.yml",
            "files": [
                _file("in_digest", "Finviz digest JSON",
                      f"{news}_finviz_digest.json", "input", "finviz_digest",
                      "finviz"),
                _file("in_baseline", "Captain baseline JSON",
                      f"{heat}_research_baseline.json", "input",
                      "map_heat_baseline", "postclose"),
                _file("in_heat", "Map heat tables JSON",
                      f"{heat}_map_heat.json", "input", "map_heat",
                      "postclose"),
                _file("parsed", "News parse JSON",
                      f"{news}_parsed.json", "required", "news_parse"),
                _file("events", "Events JSON",
                      f"{ev}_events.json", "required", "events"),
                _file("judge", "News judge MD",
                      f"{news}_judge.md", "required", "news_judge"),
                _file("research_json", "Morning refresh JSON",
                      f"{heat}_research.json", "required",
                      "map_heat_research_json"),
                _file("research_md", "Morning refresh MD",
                      f"{heat}_research.md", "required",
                      "map_heat_research_md"),
                _file("actions", "News actions JSON",
                      f"{news}_actions.json", "optional", "news_actions"),
                _file("general", "General predict MD",
                      f"{gen}_predict.md", "required", "general_predict"),
                _file("board", "Sector board JSON",
                      f"{sec}/_board.json", "optional", "sector_board"),
                _file("qc", "Pre-open QC JSON",
                      f"01_daily/{date}_preopen_qc.json", "required",
                      "preopen_qc"),
                _file("status", "Pre-open status JSON",
                      f"01_daily/{date}_preopen_status.json", "required",
                      "preopen_status"),
                _file("review", "Grok review JSON",
                      f"01_daily/{date}_grok_review.json", "required",
                      "grok_review"),
                *sectors,
            ],
        },
        {
            "key": "weather",
            "name": "Label + weather",
            "yaml": "label_weather.yml",
            "files": [
                _file("in_digest", "Finviz digest JSON",
                      f"{news}_finviz_digest.json", "input", "finviz_digest",
                      "finviz"),
                _file("weather", "Weather JSON",
                      f"{wx}_weather.json", "required", "weather"),
            ],
        },
        {
            "key": "ab",
            "name": "AB checklist",
            "yaml": "ab_checklist.yml",
            "files": [
                _file("ab_raw", "AB checklist raw",
                      f"{ab}_ab_checklist.csv", "optional", "ab_raw"),
                _file("ab_enriched", "AB checklist enriched",
                      f"{ab}_ab_checklist_enriched.csv", "required",
                      "ab_enriched"),
            ],
        },
        {
            "key": "catalyst",
            "name": "Catalyst dossiers",
            "yaml": "catalyst_daily.yml",
            "files": [
                _file("in_actions", "News actions JSON",
                      f"{news}_actions.json", "input", "news_actions",
                      "preopen"),
                _file("in_digest", "Finviz digest JSON",
                      f"{news}_finviz_digest.json", "input", "finviz_digest",
                      "finviz"),
                _file("dossiers", "Catalyst dossiers JSON",
                      f"{cat}_dossiers.json", "optional", "catalyst"),
            ],
        },
        {
            "key": "stock_book",
            "name": "Stock Book ALL",
            "yaml": "stock_book_all.yml",
            "files": [
                _file("in_general", "General predict MD",
                      f"{gen}_predict.md", "input", "general_predict",
                      "preopen"),
                _file("in_board", "Sector board JSON",
                      f"{sec}/_board.json", "input", "sector_board",
                      "preopen"),
                _file("in_actions", "News actions JSON",
                      f"{news}_actions.json", "input", "news_actions",
                      "preopen"),
                _file("in_digest", "Finviz digest JSON",
                      f"{news}_finviz_digest.json", "input", "finviz_digest",
                      "finviz"),
                _file("in_judge", "News judge MD",
                      f"{news}_judge.md", "input", "news_judge", "preopen"),
                _file("in_weather", "Weather JSON",
                      f"{wx}_weather.json", "input", "weather", "weather"),
                _file("in_ab", "AB checklist enriched",
                      f"{ab}_ab_checklist_enriched.csv", "input",
                      "ab_enriched", "ab"),
                _file("join", "Join ranked CSV",
                      f"data/join/{date}_ranked.csv", "required",
                      "join_ranked"),
                _file("peers", "Peer RS CSV",
                      f"data/peers/{date}_peer_rs.csv", "required", "peer_rs"),
                _file("book_json", "Stock book JSON",
                      f"{book}_stock_book.json", "required", "stock_book_json"),
                _file("book_md", "Stock book MD",
                      f"01_daily/{date}_stock_book.md", "required",
                      "stock_book_md"),
                _file("green", "Green-pile JSON",
                      f"{book}_green.json", "optional", "green_json"),
                _file("paper", "Paper trading MD",
                      "03_scoreboard/PAPER_TRADING.md", "optional", "md"),
                _file("dashboard", "Dashboard HTML",
                      "dashboard/index.html", "optional", "dashboard_session"),
            ],
        },
        {
            "key": "publish",
            "name": "Dashboard / .io",
            "yaml": "deploy-dashboard.yml",
            "files": [
                _file("in_book", "Stock book JSON",
                      f"{book}_stock_book.json", "input", "stock_book_json",
                      "stock_book"),
                _file("dash_html", "Dashboard HTML (repo)",
                      "dashboard/index.html", "required", "dashboard_session"),
                _file("paper", "Paper trading MD",
                      "03_scoreboard/PAPER_TRADING.md", "required", "md"),
                _file("pages", "Live GitHub Pages",
                      signals.PAGES_URL, "required", "pages_live"),
            ],
        },
    ]
    if as_of:
        return book_era.apply_era(specs, date)
    return specs


def aggregate_status(files: list[FileCheck]) -> tuple[str, bool, int, int, int, int]:
    req = [f for f in files if f.role == "required"]
    opt = [f for f in files if f.role == "optional"]
    inp = [f for f in files if f.role == "input"]
    # role=era files are pre-pipeline for this date. They never block.
    n_req_ok = sum(1 for f in req if f.status == "OK")
    n_opt_ok = sum(1 for f in opt if f.status == "OK")
    inputs_ready = all(f.status == "OK" for f in inp) if inp else True
    if not req:
        # Catalyst: only optional output. OK if the optional is OK,
        # PARTIAL if missing (did not run), FAIL if present and QC-fail.
        if opt and all(f.status == "OK" for f in opt):
            status = "OK"
        elif any(f.status == "FAIL" for f in opt):
            status = "FAIL"
        elif any(f.status == "OK" for f in opt):
            status = "PARTIAL"
        else:
            status = "PARTIAL" if inputs_ready else "FAIL"
    elif n_req_ok == len(req):
        status = "OK"
    elif n_req_ok > 0:
        status = "PARTIAL"
    else:
        status = "FAIL"
    return status, inputs_ready, n_req_ok, len(req), n_opt_ok, len(opt)


def _check_file(spec: dict, date: str) -> FileCheck:
    path = _p(spec["rel"])
    role = spec["role"]
    if role == "era":
        if not path.exists() and spec.get("kind") != "pages_live":
            start = spec.get("era_start") or ""
            feature = spec.get("era_feature") or spec["key"]
            return FileCheck(
                key=spec["key"],
                name=spec["name"],
                path=spec["rel"],
                role=role,
                status="SKIP",
                reason=f"not in pipeline as of {date} (first {feature} {start})",
                size=0,
                source=spec.get("source") or "",
            )
    status, reason, size = inspect_kind(spec["kind"], path, date)
    return FileCheck(
        key=spec["key"],
        name=spec["name"],
        path=spec["rel"],
        role=role,
        status=status,
        reason=reason,
        size=size,
        source=spec.get("source") or "",
    )


def audit(date: str, gh_runs: dict[str, dict] | None = None,
          as_of: bool = True) -> Report:
    workflows: list[WorkflowCheck] = []
    for spec in workflow_specs(date, as_of=as_of):
        files = [_check_file(f, date) for f in spec["files"]]
        status, ready, n_ok, n_req, n_opt_ok, n_opt = aggregate_status(files)
        workflows.append(WorkflowCheck(
            key=spec["key"],
            name=spec["name"],
            yaml=spec["yaml"],
            status=status,
            inputs_ready=ready,
            n_req_ok=n_ok,
            n_req=n_req,
            n_opt_ok=n_opt_ok,
            n_opt=n_opt,
            files=files,
            gh_run=(gh_runs or {}).get(spec["yaml"]),
        ))

    book = next(w for w in workflows if w.key == "stock_book")
    book_json = next((f for f in book.files if f.key == "book_json"), None)
    book_written = bool(book_json and book_json.status == "OK")
    era_inputs_ok = all(
        f.status == "OK" for f in book.files if f.role == "input"
    )
    # A book that already exists was produced with that day's packet.
    # Do not treat later-era files (Judge, digest, AB, peers) as blockers.
    historical = date < book_era.today_et()
    ranker_ready = era_inputs_ok or (historical and book_written)
    blockers = []
    for f in book.files:
        if f.role != "input" or f.status == "OK":
            continue
        src = f" (from {f.source})" if f.source else ""
        blockers.append(f"{f.status} {f.name} `{f.path}`{src} — {f.reason or f.status}")
    if historical and book_written:
        blockers = [
            b + " (as-of: ignored — book already written that day)"
            for b in blockers
        ]

    flags = [w.status for w in workflows]
    if all(s == "OK" for s in flags):
        overall = "OK"
    elif any(s == "FAIL" for s in flags) and not any(s == "OK" for s in flags):
        overall = "FAIL"
    elif any(s != "OK" for s in flags):
        overall = "PARTIAL" if any(s in ("OK", "PARTIAL") for s in flags) else "FAIL"
    else:
        overall = "FAIL"

    decisions = signals.extract_decisions(date)
    era = (decisions or {}).get("era") or book_era.describe(
        date, book_era.load_book_meta(date),
    )
    pub = next((w for w in workflows if w.key == "publish"), None)
    pages = {
        "local": next((asdict(f) for f in (pub.files if pub else [])
                       if f.key == "dash_html"), {}),
        "live": next((asdict(f) for f in (pub.files if pub else [])
                      if f.key == "pages"), {}),
        "url": signals.PAGES_URL,
    }

    return Report(
        date=date,
        generated_at=datetime.now(ET).isoformat(),
        ranker_ready=ranker_ready,
        overall=overall,
        workflows=workflows,
        blockers=blockers,
        decisions=decisions,
        pages=pages,
        era=era,
    )


# ---------------------------------------------------------------------------
# GitHub Actions run lookup (observational)
# ---------------------------------------------------------------------------

def fetch_gh_runs(date: str) -> dict[str, dict]:
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN") or ""
    repo = os.environ.get("GITHUB_REPOSITORY") or REPO_DEFAULT
    if not token:
        return {}
    yamls = [s["yaml"] for s in workflow_specs(date)]
    out: dict[str, dict] = {}
    for wf in yamls:
        url = (f"https://api.github.com/repos/{repo}/actions/workflows/{wf}"
               f"/runs?per_page=8")
        req = urllib.request.Request(url, headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "User-Agent": "fullscan-stock-book-diag",
        })
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                payload = json.loads(resp.read().decode())
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError,
                OSError) as e:
            out[wf] = {"error": str(e)[:160]}
            continue
        chosen = None
        for run in payload.get("workflow_runs") or []:
            created = str(run.get("created_at") or "")
            try:
                utc = datetime.fromisoformat(created.replace("Z", "+00:00"))
                et_d = utc.astimezone(ET).date().isoformat()
            except ValueError:
                et_d = ""
            if et_d != date:
                continue
            chosen = {
                "conclusion": run.get("conclusion"),
                "status": run.get("status"),
                "event": run.get("event"),
                "html_url": run.get("html_url"),
                "created_at": created,
            }
            break
        out[wf] = chosen or {"n": 0, "detail": f"no run on {date} ET"}
    return out


# ---------------------------------------------------------------------------
# Render
# ---------------------------------------------------------------------------

_MARK = {"OK": "✅ OK", "PARTIAL": "⚠️ PARTIAL", "FAIL": "❌ FAIL"}
_FILE_MARK = {"OK": "✅", "FAIL": "❌", "MISSING": "⬜", "SKIP": "➖"}


def _gh_cell(run: dict | None) -> str:
    if not run:
        return "—"
    if run.get("error"):
        return f"api error: {run['error']}"
    if run.get("detail") and not run.get("status"):
        return run["detail"]
    status = run.get("conclusion") or run.get("status") or "?"
    url = run.get("html_url") or ""
    when = str(run.get("created_at") or "")[11:16]
    label = f"{status}" + (f" {when}Z" if when else "")
    return f"[{label}]({url})" if url else label


def render_markdown(report: Report) -> str:
    rank = "READY" if report.ranker_ready else "BLOCKED"
    lines = [
        f"# Stock Book readiness — {report.date}",
        "",
        f"**Ranker: {rank}** · overall **{report.overall}** · "
        f"{report.generated_at}",
        "",
    ]
    lines += signals.render_actions_markdown(report.decisions)
    lines += [
        "FAIL = empty, truncated, carry-forward, timeout stub, or QC fail. "
        "PARTIAL = some required outputs are good, others are not. "
        "Optional files do not change the workflow flag.",
        "",
        "| Workflow | Status | Inputs | Required | Optional | Last GH run |",
        "|---|---|---|---|---|---|",
    ]
    for w in report.workflows:
        inputs = "ready" if w.inputs_ready else "blocked"
        req = f"{w.n_req_ok}/{w.n_req}" if w.n_req else "—"
        opt = f"{w.n_opt_ok}/{w.n_opt}" if w.n_opt else "—"
        lines.append(
            f"| {w.name} | {_MARK.get(w.status, w.status)} | {inputs} | "
            f"{req} | {opt} | {_gh_cell(w.gh_run)} |"
        )
    lines += [""] + signals.render_pages_markdown(report.pages)
    lines += signals.render_decisions_markdown(report.decisions)
    if report.blockers:
        lines += ["", "## Why the ranker is blocked", ""]
        for b in report.blockers:
            lines.append(f"- {b}")
    for w in report.workflows:
        lines += ["", f"## {w.name}", ""]
        if w.blocked:
            lines.append("_Required inputs are not all OK — this job cannot finish._")
            lines.append("")
        lines += [
            "| File | Need | Status | Detail |",
            "|---|---|---|---|",
        ]
        for f in w.files:
            if f.role == "era":
                need = "era (not in pipeline yet)"
            elif f.role == "input":
                need = f"input ← {f.source or '?'}"
            else:
                need = f.role
            detail = (f.reason or "").replace("|", "/")
            lines.append(
                f"| `{f.path}` | {need} | "
                f"{_FILE_MARK.get(f.status, f.status)} {f.status} | {detail} |"
            )
    lines.append("")
    return "\n".join(lines)


def render_text(report: Report) -> str:
    rank = "READY" if report.ranker_ready else "BLOCKED"
    lines = [
        signals.render_actions_plain(report.decisions).rstrip(),
        "",
        f"[stock-book-diag] {report.date}  ranker={rank}  overall={report.overall}"
        f"  method={((report.era or {}).get('method') or '?')}",
    ]
    for w in report.workflows:
        inputs = "ready" if w.inputs_ready else "blocked"
        req = f"{w.n_req_ok}/{w.n_req}" if w.n_req else "-"
        lines.append(
            f"  [{w.status:<7}] {w.name:<22} inputs={inputs:<7} "
            f"required={req}"
        )
        for f in w.files:
            if f.status in ("OK", "SKIP"):
                continue
            lines.append(f"           {f.explain()}")
    if report.blockers:
        lines.append("  blockers:")
        for b in report.blockers:
            lines.append(f"    - {b}")
    return "\n".join(lines)


def report_to_json(report: Report) -> dict:
    return {
        "date": report.date,
        "generated_at": report.generated_at,
        "ranker_ready": report.ranker_ready,
        "overall": report.overall,
        "blockers": report.blockers,
        "pages": report.pages,
        "era": report.era,
        "rebuilt": report.rebuilt,
        "decisions": report.decisions,
        "workflows": [
            {
                "key": w.key,
                "name": w.name,
                "yaml": w.yaml,
                "status": w.status,
                "inputs_ready": w.inputs_ready,
                "n_req_ok": w.n_req_ok,
                "n_req": w.n_req,
                "n_opt_ok": w.n_opt_ok,
                "n_opt": w.n_opt,
                "gh_run": w.gh_run,
                "files": [asdict(f) for f in w.files],
            }
            for w in report.workflows
        ],
    }


def write_report(report: Report) -> tuple[Path, Path]:
    out = ROOT / "01_daily"
    out.mkdir(parents=True, exist_ok=True)
    md = out / f"{report.date}_stock_book_diag.md"
    js = out / f"{report.date}_stock_book_diag.json"
    md.write_text(render_markdown(report), encoding="utf-8")
    js.write_text(json.dumps(report_to_json(report), indent=2), encoding="utf-8")
    return md, js


def emit_github_summary(report: Report) -> None:
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(render_markdown(report))
        fh.write("\n")


def today() -> str:
    return datetime.now(ET).date().isoformat()


def _rebuild_as_of(date: str) -> bool:
    """Replay the ranker with the method that was live on `date`."""
    from . import stock_book
    print(f"[stock-book-diag] rebuilding {date} with as-of method "
          f"{book_era.method_for(date)}")
    df, meta = stock_book.build(date, as_of=True)
    stock_book.write_report(df, meta, top_n=int(meta.get("top_n") or 25))
    return True


def action_ok(report: Report) -> bool:
    """Exit status for the Action.

    Today: ranker ready + book OK + names (or HARD_RED stand-down);
    live .io must not be FAIL.

    Historical: the book that was written that day is enough. Missing
    later-era files and a live .io that no longer lists the session
    do not refuse the run.
    """
    book = next(w for w in report.workflows if w.key == "stock_book")
    pub = next((w for w in report.workflows if w.key == "publish"), None)
    names = ((report.decisions or {}).get("horizons") or {}).get("1d") or {}
    has_names = bool(names.get("buy") or names.get("sell"))
    intentional_stand_down = bool(
        (report.decisions or {}).get("intentional_stand_down")
    )
    actionable = has_names or intentional_stand_down
    book_json = next((f for f in book.files if f.key == "book_json"), None)
    book_written = bool(book_json and book_json.status == "OK")
    historical = report.date < today()
    if historical:
        return book_written and actionable
    ok = report.ranker_ready and book.status == "OK" and actionable
    if pub and pub.status == "FAIL":
        ok = False
    return ok


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="", help="session YYYY-MM-DD (default today ET)")
    ap.add_argument("--write", action="store_true",
                    help="Write 01_daily/<date>_stock_book_diag.md|json")
    ap.add_argument("--no-gh", action="store_true",
                    help="Skip GitHub Actions run lookup")
    ap.add_argument("--as-of", dest="as_of", action="store_true",
                    default=True,
                    help="Use the input contract and ranker live on --date "
                         "(default). Historical missing files are era-skip.")
    ap.add_argument("--strict", dest="as_of", action="store_false",
                    help="Score today-era required files even on old dates")
    ap.add_argument("--rebuild-if-missing", action="store_true",
                    help="If this date has join but no book, replay the "
                         "as-of ranker so the Action still prints names")
    args = ap.parse_args()
    date = args.date or today()
    rebuilt = False
    if args.rebuild_if_missing and not book_era.book_exists(date):
        if book_era.join_exists(date):
            try:
                rebuilt = _rebuild_as_of(date)
            except SystemExit as e:
                print(f"[stock-book-diag] as-of rebuild aborted: {e}")
            except Exception as e:  # noqa: BLE001
                print(f"[stock-book-diag] as-of rebuild failed: {e}")
        else:
            print(f"[stock-book-diag] no join for {date} — cannot rebuild")
    gh = {} if args.no_gh else fetch_gh_runs(date)
    report = audit(date, gh_runs=gh, as_of=args.as_of)
    report.rebuilt = rebuilt
    print(render_text(report))
    signals.emit_action_notices(report.decisions)
    if args.write:
        md, js = write_report(report)
        print(f"[stock-book-diag] wrote {md}")
        print(f"[stock-book-diag] wrote {js}")
    emit_github_summary(report)
    raise SystemExit(0 if action_ok(report) else 1)


if __name__ == "__main__":
    main()
