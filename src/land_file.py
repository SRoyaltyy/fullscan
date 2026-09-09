"""QC one packet step, then push those files to main immediately.

The 2026-09-08 fire: Pre-Open wrote weather / heat / a book on the
runner, then one late `safe_git_push` of everything hit a sleeve-merge
rebase CONFLICT and the whole packet died. Incremental land means file
A is on GitHub before file B starts, so a later timeout or push race
cannot erase finished QC-ok work.

Push is Actions-only (or FULLSCAN_LAND=1). Local runs write the day
board JSON but do not touch origin/main.

CLI: python -m src.land_file --date YYYY-MM-DD --key news_parse
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from . import config, output_qc

ROOT = Path(__file__).resolve().parent.parent
ET = ZoneInfo(config.TZ)
BOARD_DIR = ROOT / "data" / "day_board"
SCRIPT = ROOT / "scripts" / "safe_git_push.sh"
PREVIEW_CHARS = 900


def _today() -> str:
    return datetime.now(ET).date().isoformat()


def step_paths(date: str, key: str) -> list[Path]:
    """Dated files this packet step owns. Missing paths are omitted later."""
    news = ROOT / "01_daily" / "news"
    heat = ROOT / "01_daily" / "map_heat"
    ev = ROOT / "01_daily" / "events"
    gen = ROOT / "01_daily" / "general"
    sec = ROOT / "01_daily" / "sectors" / date
    wx = ROOT / "01_daily" / "weather"
    cat = ROOT / "01_daily" / "catalyst"
    book = ROOT / "data" / "stock_book"
    mapping: dict[str, list[Path]] = {
        "finviz_digest": [
            news / f"{date}_finviz_digest.json",
            news / f"{date}_finviz_digest.md",
        ],
        "map_heat": [
            heat / f"{date}_map_heat.json",
            heat / f"{date}_map_heat.md",
        ],
        "map_heat_baseline": [
            heat / f"{date}_research_baseline.json",
            heat / f"{date}_research_baseline.md",
        ],
        "weather": [
            wx / f"{date}_weather.json",
            wx / f"{date}_weather.md",
        ],
        "finviz": [
            ROOT / "data" / "exports" / f"finviz_{date}.csv",
            ROOT / "data" / "finviz" / "latest.csv",
        ],
        "universe": [ROOT / "data" / "universe" / f"{date}_membership.csv"],
        "join": [ROOT / "data" / "join" / f"{date}_ranked.csv"],
        "ab": [
            ROOT / "data" / "ab_checklist" / f"{date}_ab_checklist.csv",
            ROOT / "data" / "ab_checklist" / f"{date}_ab_checklist_enriched.csv",
        ],
        "news_parse": [
            news / f"{date}_parsed.json",
            news / f"{date}_parsed.md",
        ],
        "events": [
            ev / f"{date}_events.json",
            ev / f"{date}_events.md",
        ],
        "events_catcher": [
            ev / f"{date}_events.json",
            ev / f"{date}_events.md",
            ev / f"{date}_catcher.md",
        ],
        "news_judge": [
            news / f"{date}_judge.md",
            news / f"{date}_judge.json",
        ],
        "map_heat_research": [
            heat / f"{date}_research.json",
            heat / f"{date}_research.md",
        ],
        "news_actions": [
            news / f"{date}_actions.json",
            news / f"{date}_actions.md",
        ],
        "general_predict": [gen / f"{date}_predict.md"],
        "sector_predict": [sec],
        "sector_board": [sec / "_board.json"],
        "catalyst": [
            cat / f"{date}_dossiers.json",
            cat / f"{date}_dossiers.md",
        ],
        "stock_book": [
            book / f"{date}_stock_book.json",
            book / f"{date}_green.json",
            book / f"{date}_input_health.json",
            ROOT / "01_daily" / f"{date}_stock_book.md",
        ],
        "flatten": [
            ROOT / "01_daily" / f"{date}_flatten_card.md",
            ROOT / "data" / "sleeve_merge" / "today.json",
        ],
        "status": [
            ROOT / "01_daily" / f"{date}_preopen_qc.json",
            ROOT / "01_daily" / f"{date}_preopen_status.json",
            ROOT / "01_daily" / f"{date}_preopen_status.md",
        ],
        "paper": [
            ROOT / "03_scoreboard" / "PAPER_TRADING.md",
            ROOT / "dashboard" / "index.html",
        ],
    }
    return list(mapping.get(key) or [])


def _qc_one(path: Path, date: str) -> output_qc.QCResult:
    """Best-effort QC for a dated artifact. Unknown kinds: exist + size."""
    name = path.name
    try:
        rel = str(path.relative_to(ROOT))
    except ValueError:
        rel = str(path)
    if path.is_dir():
        ok_n = 0
        total = 0
        for p in sorted(path.glob("*_predict.md")):
            total += 1
            if output_qc.qc_sector_predict(p).ok:
                ok_n += 1
        if total == 0:
            return output_qc._fail("sector_predict", rel, "missing", empty=True)
        if ok_n >= 8:
            return output_qc._ok("sector_predict", rel, f"{ok_n}/{total}")
        return output_qc._fail(
            "sector_predict", rel, f"too_few_ok({ok_n}/{total})")
    if not path.is_file():
        return output_qc._fail("file", rel, "missing", empty=True)
    if name.endswith("_parsed.json"):
        return output_qc.qc_news_parse(path)
    if name.endswith("_judge.md"):
        return output_qc.qc_news_judge(path)
    if name.endswith("_actions.json"):
        return output_qc.qc_news_actions(path)
    if name.endswith("_finviz_digest.json") or name.endswith("_finviz_digest.md"):
        return output_qc.qc_finviz_digest(path)
    if name.endswith("_map_heat.json"):
        return output_qc.qc_map_heat(path)
    if name.endswith("_research_baseline.json"):
        return output_qc.qc_map_heat_baseline(path)
    if name.endswith("_research.md") or name.endswith("_research.json"):
        md = path if name.endswith(".md") else path.with_suffix(".md")
        return output_qc.qc_map_heat_research(md)
    if name.endswith("_predict.md") and "sectors" in rel:
        return output_qc.qc_sector_predict(path)
    if name.endswith("_predict.md"):
        return output_qc.qc_general_predict(path)
    if "events" in rel and name.endswith(".json"):
        return output_qc.qc_events_path(path)
    if name.endswith("_weather.json"):
        from . import packet_gates
        ok, reason = packet_gates.weather_ok(
            path, min_bytes=packet_gates.MIN_WEATHER_BYTES)
        if ok:
            return output_qc._ok("weather", rel, reason)
        return output_qc._fail("weather", rel, reason)
    if name.endswith(".json"):
        from . import packet_gates
        tiny = packet_gates.json_too_small(path)
        if tiny:
            return output_qc._fail("file", rel, tiny, empty=True)
    size = path.stat().st_size
    if size < 80:
        return output_qc._fail("file", rel, f"too_small({size})", empty=True)
    return output_qc.QCResult(ok=True, kind="file", path=rel, size=size)


def preview_path(path: Path, limit: int = PREVIEW_CHARS) -> str:
    """Short human brief of what actually landed (file contents, not a path)."""
    if path.is_dir():
        preds = sorted(path.glob("*_predict.md"))
        ok = sum(1 for p in preds if output_qc.qc_sector_predict(p).ok)
        names = [p.stem.replace("_predict", "") for p in preds[:8]]
        return f"{ok}/{len(preds)} sector predicts ok: {', '.join(names)}"
    if not path.is_file():
        return "missing"
    try:
        raw = path.read_text(encoding="utf-8", errors="replace")
    except OSError as e:
        return f"unreadable: {e}"
    if path.suffix.lower() == ".json":
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return raw[:limit]
        return _preview_json(data, path.name)
    if path.suffix.lower() == ".csv":
        lines = [ln for ln in raw.splitlines() if ln.strip()]
        head = lines[0] if lines else ""
        sample = ", ".join((ln.split(",")[0] for ln in lines[1:6]))
        return f"{len(lines) - 1} rows · {head[:120]} · {sample}"
    lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
    keep: list[str] = []
    for ln in lines:
        if ln.startswith("#") and keep:
            break
        keep.append(ln)
        if len("\n".join(keep)) >= limit:
            break
    return "\n".join(keep)[:limit]


def _preview_json(data: object, name: str) -> str:
    if not isinstance(data, dict):
        return json.dumps(data, default=str)[:PREVIEW_CHARS]
    if data.get("error"):
        return f"ERROR {data.get('error')}: {data.get('error_detail') or ''}"[:PREVIEW_CHARS]
    if "usable_top" in data or "raw_count" in data:
        top = data.get("usable_top") or []
        titles = []
        for row in top[:5]:
            if isinstance(row, dict):
                titles.append(str(row.get("title") or row.get("ticker") or "")[:80])
        return (
            f"raw={data.get('raw_count')} usable={data.get('usable_count')} "
            f"single={data.get('single_name_count')} · " + " | ".join(titles)
        )[:PREVIEW_CHARS]
    if "events" in data and isinstance(data.get("events"), list):
        evs = data["events"]
        bits = []
        for e in evs[:6]:
            if isinstance(e, dict):
                bits.append(str(e.get("title") or e.get("event") or e.get("name") or "")[:70])
        return f"{len(evs)} events · " + " | ".join(bits)
    books = data.get("books") if isinstance(data.get("books"), dict) else None
    if books:
        one = books.get("1d") or {}
        buys = [str(r.get("ticker") or "") for r in (one.get("buy") or [])[:8]]
        sells = [str(r.get("ticker") or "") for r in (one.get("sell") or [])[:8]]
        return f"1d BUY {', '.join(x for x in buys if x) or '—'} · SELL {', '.join(x for x in sells if x) or '—'}"
    if "signals" in data:
        secs = ((data.get("signals") or {}).get("sectors") or {})
        bits = [f"{k}={v}" for k, v in list(secs.items())[:8]]
        return f"weather sectors {len(secs)} · " + ", ".join(bits)
    if name.endswith("_green.json") or "green" in name:
        names = []
        rows = data.get("rows") or data.get("tickers") or data.get("names") or []
        if isinstance(rows, list):
            for r in rows[:10]:
                if isinstance(r, dict):
                    names.append(str(r.get("ticker") or r.get("symbol") or ""))
                else:
                    names.append(str(r))
        return f"green {len(rows) if isinstance(rows, list) else '?'} · " + ", ".join(x for x in names if x)
    keys = ", ".join(list(data.keys())[:12])
    return f"keys: {keys}"[:PREVIEW_CHARS]


def _should_push() -> bool:
    if (os.environ.get("FULLSCAN_LAND_NOPUSH") or "").strip() == "1":
        return False
    if (os.environ.get("FULLSCAN_LAND") or "").strip() == "1":
        return True
    return (os.environ.get("GITHUB_ACTIONS") or "").lower() == "true"


def _push(msg: str, paths: list[Path]) -> bool:
    if not _should_push():
        print("[land] skip git push (local / NOPUSH)")
        return True
    if not SCRIPT.is_file():
        print("[land] WARN: safe_git_push.sh missing")
        return False
    rels = [str(p.relative_to(ROOT)) if p.is_absolute() else str(p) for p in paths]
    print(f"[land] push {msg} → {rels}")
    try:
        r = subprocess.run(
            ["bash", str(SCRIPT), msg, *rels],
            cwd=str(ROOT), env=os.environ.copy(), timeout=180,
        )
    except (OSError, subprocess.TimeoutExpired) as e:
        print(f"[land] WARN: push failed: {e}")
        return False
    if r.returncode != 0:
        print(f"[land] WARN: safe-push exited {r.returncode} — files stay on runner")
        return False
    return True


def land(date: str, key: str, title: str = "",
         extra_paths: list[Path] | None = None,
         require_qc: bool = True) -> dict:
    """QC this step's files, refresh the day board, push QC-ok paths.

    Never raises. A failed push does not stop the next step.
    """
    title = title or key
    try:
        return _land_body(date, key, title, extra_paths, require_qc)
    except Exception as e:  # noqa: BLE001 — packet must continue
        print(f"[land] WARN: {key} crashed: {e}")
        return {
            "key": key, "title": title, "date": date,
            "ok": False, "pushed": False,
            "at": datetime.now(ET).isoformat(),
            "files": [],
            "preview": f"land crashed: {e}",
        }


def _land_body(date: str, key: str, title: str,
               extra_paths: list[Path] | None, require_qc: bool) -> dict:
    paths = [p for p in (step_paths(date, key) + list(extra_paths or []))
             if p.exists()]
    checks: list[dict] = []
    ok_paths: list[Path] = []
    for p in paths:
        qc = _qc_one(p, date)
        try:
            rel = str(p.relative_to(ROOT))
        except ValueError:
            rel = str(p)
        brief = preview_path(p) if p.exists() else ""
        row = {
            "path": rel,
            "ok": bool(qc.ok),
            "reason": qc.reason or "",
            "size": int(qc.size or 0),
            "preview": brief,
        }
        checks.append(row)
        if qc.ok or (not require_qc and p.exists()):
            ok_paths.append(p)
        nbytes = p.stat().st_size if p.is_file() else int(qc.size or 0)
        print(f"[land] {key} {rel} ok={qc.ok} {qc.reason or ''} "
              f"({nbytes}B)")

    if require_qc and not ok_paths:
        print(f"[land] skip push {key} — no QC-ok files")
        record = {
            "key": key, "title": title, "date": date,
            "ok": False, "pushed": False,
            "at": datetime.now(ET).isoformat(),
            "files": checks,
            "preview": "; ".join(
                f"{c['path']}: {c['reason'] or 'missing'}" for c in checks
            ) or "no files",
        }
        _record_and_board(date, record)
        return record

    preview = " · ".join(
        (c["preview"] or c["path"]) for c in checks if c.get("preview")
    )[:1200]
    # Write the board as pushed=yes *before* git so the JSON that lands
    # on main matches the files in the same commit (not a later leftover).
    record = {
        "key": key, "title": title, "date": date,
        "ok": bool(ok_paths) and all(c["ok"] for c in checks),
        "pushed": True,
        "at": datetime.now(ET).isoformat(),
        "files": checks,
        "preview": preview,
    }
    board_paths = _write_board_payload(
        date, key, title, checks, pushed=True, preview=preview, land=record)
    pushed = _push(f"auto: land {key} [{date}]", list(ok_paths) + board_paths)
    record["pushed"] = pushed
    if not pushed:
        _write_board_payload(
            date, key, title, checks, pushed=False, preview=preview, land=record)
    return record


def _record_and_board(date: str, record: dict) -> None:
    _write_board_payload(
        date, record["key"], record.get("title") or record["key"],
        record.get("files") or [], pushed=False,
        preview=record.get("preview") or "", land=record,
    )


def _write_board_payload(date: str, key: str, title: str, checks: list[dict],
                         pushed: bool, preview: str = "",
                         land: dict | None = None) -> list[Path]:
    from . import day_board
    return day_board.note_land(
        date, key=key, title=title, files=checks,
        pushed=pushed, preview=preview, land=land,
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="")
    ap.add_argument("--key", required=True)
    ap.add_argument("--title", default="")
    args = ap.parse_args()
    rec = land(args.date or _today(), args.key, title=args.title)
    print(json.dumps({
        "key": rec["key"], "ok": rec["ok"], "pushed": rec["pushed"],
        "preview": rec.get("preview") or "",
    }, indent=2))


if __name__ == "__main__":
    main()
