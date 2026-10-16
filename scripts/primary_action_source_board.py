"""Source health board — exhaustive parse status for the primary-action pipeline.

Reads what is already on disk (same contract as src.primary_actions outputs):
  config/primary_action_sources.json   verified channel catalog
  data/primary_actions/latest/report.json      last collect run health
  data/primary_actions/latest/new_items.json   documents newly retained this run

Writes data/source_board/<date>.json + latest.json. The .io page is static
HTML that polls raw.githubusercontent.com, mirroring src/day_board.

CLI: python scripts/primary_action_source_board.py [--write] [--date YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BOARD_DIR = ROOT / "data" / "source_board"
CATALOG = ROOT / "config" / "primary_action_sources.json"
LATEST_DIR = ROOT / "data" / "primary_actions" / "latest"
PAGES_URL = "https://sroyaltyy.github.io/fullscan/dashboard/source-board/"
RAW_BASE = "https://raw.githubusercontent.com/SRoyaltyy/fullscan/main"
MAX_HEADLINES = 300


def _load_json(path: Path, default):
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return default
    return data if isinstance(data, type(default)) else default


def _date_of(iso_stamp: str) -> str:
    try:
        return datetime.fromisoformat(iso_stamp.replace("Z", "+00:00")).date().isoformat()
    except (ValueError, AttributeError):
        return datetime.now(timezone.utc).date().isoformat()


def build(date: str = "") -> dict:
    catalog = _load_json(CATALOG, {})
    report = _load_json(LATEST_DIR / "report.json", {})
    new_items = _load_json(LATEST_DIR / "new_items.json", [])

    channels = []
    for s in catalog.get("sources") or []:
        v = s.get("verification") or {}
        channels.append({
            "id": s.get("id") or "",
            "name": s.get("name") or "",
            "jurisdiction": s.get("jurisdiction") or "",
            "family": s.get("family") or "",
            "adapter": s.get("adapter") or "",
            "enabled": bool(s.get("enabled")),
            "url": s.get("url") or "",
            "status": v.get("status") or "unchecked",
            "http_status": v.get("http_status"),
            "rows": v.get("parsed_rows", v.get("fetched_rows")),
            "sample": (v.get("sample_titles") or [])[:2],
            "warnings": v.get("warnings") or [],
            "exception": v.get("exception_type") or "",
            "checked_at": v.get("checked_at") or "",
        })
    enabled = [c for c in channels if c["enabled"]]
    ok = [c for c in enabled if c["status"] == "ok"]

    failures = []
    for h in report.get("sources") or []:
        if h.get("status") in ("ok", "valid_empty", "unchanged"):
            continue
        failures.append({
            "source_id": h.get("source_id") or "",
            "status": h.get("status") or "",
            "http_status": h.get("http_status"),
            "warnings": h.get("warnings") or [],
            "exception": h.get("exception_type") or "",
        })

    headlines = []
    by_source = Counter()
    for r in new_items or []:
        src = r.get("primary_source_id") or "unknown"
        by_source[src] += 1
        headlines.append({
            "title": r.get("title") or "",
            "url": r.get("url") or "",
            "published_at": r.get("published_at") or "",
            "source": src,
            "issuer": (r.get("source_metadata") or {}).get("issuer") or "",
        })
    headlines.sort(key=lambda r: r["published_at"] or "", reverse=True)

    counts = report.get("counts") or {}
    board_date = date or _date_of(str(report.get("generated_at") or ""))
    return {
        "date": board_date,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "catalog": {
            "generated_at": catalog.get("generated_at") or "",
            "channels": len(channels),
            "enabled": len(enabled),
            "verified_ok": len(ok),
            "by_adapter": dict(Counter(c["adapter"] for c in enabled)),
            "by_jurisdiction": dict(Counter(c["jurisdiction"] for c in enabled).most_common()),
        },
        "collect": {
            "generated_at": report.get("generated_at") or "",
            "enabled_sources": report.get("enabled_sources"),
            "sources_ok": report.get("sources_ok"),
            "sources_failed": report.get("sources_failed"),
            "coverage_status": report.get("coverage_status") or "",
            "counts": counts,
            "current_dated_documents": report.get("current_dated_documents"),
        },
        "failures": failures,
        "channels": channels,
        "new_today": {
            "count": len(headlines),
            "by_source": dict(by_source.most_common()),
            "headlines": headlines[:MAX_HEADLINES],
        },
        "href": {
            "pages": PAGES_URL,
            "catalog_md": f"{RAW_BASE}/docs/primary-actions/source_catalog.md",
            "day_board": "https://sroyaltyy.github.io/fullscan/dashboard/day-board/",
            "workflow": ("https://github.com/SRoyaltyy/fullscan/actions/workflows/"
                         "primary_actions.yml"),
        },
    }


def _merge_prev(board: dict, prev: dict) -> dict:
    """Union newly-parsed headlines across same-day runs (two cron fires/day)."""
    if not isinstance(prev, dict) or not prev.get("new_today"):
        return board
    seen = {(h.get("source"), h.get("url")) for h in board["new_today"]["headlines"]}
    extra = [h for h in (prev["new_today"].get("headlines") or [])
             if (h.get("source"), h.get("url")) not in seen]
    if not extra:
        return board
    merged = dict(board)
    nt = dict(board["new_today"])
    heads = list(nt["headlines"]) + extra
    heads.sort(key=lambda r: r["published_at"] or "", reverse=True)
    nt["headlines"] = heads[:MAX_HEADLINES]
    bs = Counter(nt.get("by_source") or {})
    for h in extra:
        bs[h.get("source") or "unknown"] += 1
    nt["by_source"] = dict(bs.most_common())
    nt["count"] = len(heads)
    merged["new_today"] = nt
    return merged


def _latest_dates(extra: str) -> list[str]:
    dates = set()
    if BOARD_DIR.is_dir():
        for p in BOARD_DIR.glob("20*.json"):
            if len(p.stem) == 10 and p.stem.count("-") == 2:
                dates.add(p.stem)
    dates.add(extra)
    return sorted(dates, reverse=True)[:40]


def write_json(board: dict) -> list[Path]:
    BOARD_DIR.mkdir(parents=True, exist_ok=True)
    date = str(board.get("date") or "")
    prev = _load_json(BOARD_DIR / f"{date}.json", {})
    board = _merge_prev(board, prev)
    day_p = BOARD_DIR / f"{date}.json"
    latest_p = BOARD_DIR / "latest.json"
    day_p.write_text(json.dumps(board, indent=1, ensure_ascii=False), encoding="utf-8")
    latest_p.write_text(json.dumps({
        "date": date,
        "generated_at": board.get("generated_at"),
        "catalog": board.get("catalog") or {},
        "collect": board.get("collect") or {},
        "new_today": {"count": (board.get("new_today") or {}).get("count", 0),
                      "by_source": (board.get("new_today") or {}).get("by_source") or {}},
        "n_failures": len(board.get("failures") or []),
        "dates": _latest_dates(date),
        "href": f"{RAW_BASE}/data/source_board/{date}.json",
        "pages": PAGES_URL,
    }, indent=1, ensure_ascii=False), encoding="utf-8")
    return [day_p, latest_p]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--date", default="")
    args = ap.parse_args()
    board = build(args.date)
    cat = board["catalog"]
    col = board["collect"]
    print(f"[source-board] {board['date']} channels "
          f"{cat['enabled']}/{cat['channels']} enabled, {cat['verified_ok']} verified ok; "
          f"collect ok {col.get('sources_ok')}/{col.get('enabled_sources')}; "
          f"new today {board['new_today']['count']}")
    if args.write:
        for p in write_json(board):
            print(f"[source-board] wrote {p}")


if __name__ == "__main__":
    main()
