"""Trading-day process board — Stock Book readiness without running the Action.

Reads files already on disk (same contract as src.stock_book_diag), plus
the incremental land log from src.land_file. The .io page is static HTML
that polls raw.githubusercontent.com so a Pages rebuild is not required
to see file A land while file B is still running.

CLI: python -m src.day_board [--date YYYY-MM-DD] [--write]
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from . import config, stock_book_diag as diag
from . import stock_book_diag_signals as signals
from . import day_board_say as say

ROOT = Path(__file__).resolve().parent.parent
ET = ZoneInfo(config.TZ)
BOARD_DIR = ROOT / "data" / "day_board"
DASH_DIR = ROOT / "dashboard" / "day-board"
RAW_BASE = "https://raw.githubusercontent.com/SRoyaltyy/fullscan/main"
PAGES_URL = "https://sroyaltyy.github.io/fullscan/dashboard/day-board/"
MAX_LANDS = 80


def _today() -> str:
    return datetime.now(ET).date().isoformat()


def _load_json(path: Path) -> dict:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _selections(date: str) -> dict:
    book = signals._load_book(date)  # noqa: SLF001 — same 1d lists as readiness
    buys, sells = signals._horizon_rows(book, "1d")  # noqa: SLF001
    flatten = say.flatten_card(date) or {}
    return {
        "buy_1d": [
            {"ticker": str(r.get("ticker") or ""),
             "score": r.get("score") or r.get("total")}
            for r in buys[:15]
        ],
        "sell_1d": [
            {"ticker": str(r.get("ticker") or ""),
             "score": r.get("score") or r.get("total")}
            for r in sells[:15]
        ],
        "flatten": flatten,
        "general": say.general_predict(date) or {},
        "sectors": say.sector_board(date) or {},
        "news": say.news_parse(date) or {},
        "weather": say.weather(date) or {},
    }


def _disk_workflows(date: str) -> list[diag.WorkflowCheck]:
    """Same file contract as Stock Book readiness, but no GH / Pages fetch."""
    workflows: list[diag.WorkflowCheck] = []
    for spec in diag.workflow_specs(date, as_of=True):
        files = []
        for fspec in spec["files"]:
            if fspec.get("kind") == "pages_live":
                files.append(diag.FileCheck(
                    key=fspec["key"], name=fspec["name"],
                    path=fspec["rel"], role="optional",
                    status="SKIP",
                    reason="day-board reads main disk, not live Pages",
                    size=0, source=fspec.get("source") or "",
                ))
                continue
            files.append(diag._check_file(fspec, date))
        status, ready, n_ok, n_req, n_opt_ok, n_opt = diag.aggregate_status(files)
        workflows.append(diag.WorkflowCheck(
            key=spec["key"], name=spec["name"], yaml=spec["yaml"],
            status=status, inputs_ready=ready,
            n_req_ok=n_ok, n_req=n_req, n_opt_ok=n_opt_ok, n_opt=n_opt,
            files=files, gh_run=None,
        ))
    return workflows


def build(date: str, lands: list[dict] | None = None) -> dict:
    """Audit disk + merge prior land log. No GitHub API (no Action)."""
    workflows = _disk_workflows(date)
    book = next((w for w in workflows if w.key == "stock_book"), None)
    book_files = list(book.files) if book is not None else []
    book_json = next((f for f in book_files if f.key == "book_json"), None)
    book_written = bool(book_json and book_json.status == "OK")
    era_inputs_ok = all(f.status == "OK" for f in book_files if f.role == "input")
    from . import book_era
    historical = date < book_era.today_et()
    ranker_ready = era_inputs_ok or (historical and book_written)
    blockers = []
    for f in book_files:
        if f.role != "input" or f.status == "OK":
            continue
        blockers.append(f"{f.status} {f.name} `{f.path}` — {f.reason or f.status}")
    flags = [w.status for w in workflows]
    if all(s == "OK" for s in flags):
        overall = "OK"
    elif any(s == "FAIL" for s in flags) and not any(s == "OK" for s in flags):
        overall = "FAIL"
    elif any(s != "OK" for s in flags):
        overall = "PARTIAL" if any(s in ("OK", "PARTIAL") for s in flags) else "FAIL"
    else:
        overall = "FAIL"
    prev = _load_json(BOARD_DIR / f"{date}.json")
    if lands is None:
        lands = list(prev.get("lands") or [])
    processes = []
    n_ok = n_fail = n_partial = 0
    for w in workflows:
        files = []
        for f in w.files:
            files.append({
                "key": f.key,
                "name": f.name,
                "path": f.path,
                "role": f.role,
                "status": f.status,
                "reason": f.reason,
                "size": f.size,
            })
        extract = say.summarize_process(w.key, date) or {}
        processes.append({
            "key": w.key,
            "name": w.name,
            "yaml": w.yaml,
            "status": w.status,
            "inputs_ready": w.inputs_ready,
            "n_req_ok": w.n_req_ok,
            "n_req": w.n_req,
            "n_opt_ok": w.n_opt_ok,
            "n_opt": w.n_opt,
            "files": files,
            "said": extract.get("said") or "",
            "bullets": extract.get("bullets") or [],
        })
        if w.status == "OK":
            n_ok += 1
        elif w.status == "FAIL":
            n_fail += 1
        elif w.status == "PARTIAL":
            n_partial += 1
    return {
        "date": date,
        "generated_at": datetime.now(ET).isoformat(),
        "overall": overall,
        "ranker_ready": ranker_ready,
        "blockers": blockers,
        "counts": {
            "ok": n_ok, "partial": n_partial, "fail": n_fail,
            "n": len(processes),
        },
        "processes": processes,
        "selections": _selections(date),
        "lands": lands[-MAX_LANDS:],
        "href": {
            "pages": PAGES_URL,
            "readiness_action": (
                "https://github.com/SRoyaltyy/fullscan/actions/workflows/"
                "stock_book_diag.yml"
            ),
            "factor_mine": (
                "https://sroyaltyy.github.io/fullscan/dashboard/factor-mine/"
            ),
            "paper": "https://sroyaltyy.github.io/fullscan/dashboard/",
        },
    }


def _latest_dates(extra: str) -> list[str]:
    dates = set()
    if BOARD_DIR.is_dir():
        for p in BOARD_DIR.glob("20*.json"):
            if p.stem.count("-") == 2:
                dates.add(p.stem)
    dates.add(extra)
    return sorted(dates, reverse=True)[:40]


def merge_boards(theirs: dict, ours: dict) -> dict:
    """Union incremental lands when two jobs rewrote the same day JSON."""
    if not theirs:
        return dict(ours) if ours else {}
    if not ours:
        return dict(theirs)

    def ts(d: dict) -> str:
        return str(d.get("generated_at") or "")

    newer, older = (ours, theirs) if ts(ours) >= ts(theirs) else (theirs, ours)
    out = dict(newer)
    seen: set[tuple[str, str]] = set()
    lands: list[dict] = []
    for row in list(older.get("lands") or []) + list(newer.get("lands") or []):
        if not isinstance(row, dict):
            continue
        k = (str(row.get("key") or ""), str(row.get("at") or ""))
        if k in seen:
            continue
        seen.add(k)
        lands.append(row)
    lands.sort(key=lambda r: str(r.get("at") or ""))
    out["lands"] = lands[-MAX_LANDS:]
    ns = newer.get("selections") if isinstance(newer.get("selections"), dict) else {}
    osel = older.get("selections") if isinstance(older.get("selections"), dict) else {}
    if not (ns.get("buy_1d") or ns.get("sell_1d")) and (
            osel.get("buy_1d") or osel.get("sell_1d")):
        out["selections"] = osel
    if newer.get("ranker_ready") or older.get("ranker_ready"):
        out["ranker_ready"] = True
    return out


def merge_ours_dir(ours_dir: str) -> None:
    """Union OUR day_board snapshots into the copies currently on disk."""
    src = Path(ours_dir)
    if not src.is_dir():
        print(f"[day-board] merge-ours missing {ours_dir}")
        return
    BOARD_DIR.mkdir(parents=True, exist_ok=True)
    dates: set[str] = set()
    for folder in (BOARD_DIR, src):
        for p in folder.glob("20*.json"):
            if p.stem.count("-") == 2:
                dates.add(p.stem)
    if not dates:
        print("[day-board] merge-ours: no dated boards")
        return
    newest_date = ""
    newest_ts = ""
    for date in sorted(dates):
        theirs = _load_json(BOARD_DIR / f"{date}.json")
        ours = _load_json(src / f"{date}.json")
        merged = merge_boards(theirs, ours)
        if not merged:
            continue
        merged.setdefault("date", date)
        (BOARD_DIR / f"{date}.json").write_text(
            json.dumps(merged, indent=2), encoding="utf-8")
        gat = str(merged.get("generated_at") or "")
        if gat >= newest_ts:
            newest_ts = gat
            newest_date = date
    if not newest_date:
        newest_date = sorted(dates)[-1]
    board = _load_json(BOARD_DIR / f"{newest_date}.json")
    if board:
        write_json(board)
        n = len(board.get("lands") or [])
        print(f"[day-board] merged {ours_dir} -> {newest_date} ({n} lands)")


def write_json(board: dict) -> list[Path]:
    BOARD_DIR.mkdir(parents=True, exist_ok=True)
    date = str(board.get("date") or _today())
    day_p = BOARD_DIR / f"{date}.json"
    latest_p = BOARD_DIR / "latest.json"
    today_p = BOARD_DIR / "today.json"
    day_p.write_text(json.dumps(board, indent=2), encoding="utf-8")
    latest = {
        "date": date,
        "generated_at": board.get("generated_at"),
        "overall": board.get("overall"),
        "ranker_ready": board.get("ranker_ready"),
        "counts": board.get("counts") or {},
        "dates": _latest_dates(date),
        "href": f"{RAW_BASE}/data/day_board/{date}.json",
        "pages": PAGES_URL,
    }
    latest_p.write_text(json.dumps(latest, indent=2), encoding="utf-8")
    sel = board.get("selections") or {}
    today_p.write_text(json.dumps({
        "date": date,
        "generated_at": board.get("generated_at"),
        "overall": board.get("overall"),
        "ranker_ready": board.get("ranker_ready"),
        "counts": board.get("counts") or {},
        "buy_1d": sel.get("buy_1d") or [],
        "sell_1d": sel.get("sell_1d") or [],
        "flatten": sel.get("flatten") or {},
        "general": sel.get("general") or {},
        "sectors": sel.get("sectors") or {},
        "lands": (board.get("lands") or [])[-8:],
        "day_board": PAGES_URL,
    }, indent=2), encoding="utf-8")
    return [day_p, latest_p, today_p]


def note_land(date: str, *, key: str, title: str, files: list[dict],
              pushed: bool, preview: str = "",
              land: dict | None = None) -> list[Path]:
    """Merge one incremental land into the day JSON and rewrite latest."""
    prev = _load_json(BOARD_DIR / f"{date}.json")
    lands = list(prev.get("lands") or [])
    entry = land or {
        "key": key, "title": title, "at": datetime.now(ET).isoformat(),
        "pushed": pushed, "preview": preview, "files": files,
    }
    if "at" not in entry:
        entry["at"] = datetime.now(ET).isoformat()
    if lands and lands[-1].get("key") == key:
        lands[-1] = entry
    else:
        lands.append(entry)
    board = build(date, lands=lands)
    return write_json(board)


def write_html() -> Path:
    """Keep dashboard/day-board/index.html as the source of truth."""
    DASH_DIR.mkdir(parents=True, exist_ok=True)
    return DASH_DIR / "index.html"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="")
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--html", action="store_true",
                    help="Touch dashboard/day-board/index.html path only")
    ap.add_argument("--merge-ours", default="",
                    help="Directory of OUR day_board JSON to union into disk")
    args = ap.parse_args()
    if args.merge_ours:
        merge_ours_dir(args.merge_ours)
        return
    date = args.date or _today()
    board = build(date)
    print(
        f"[day-board] {date} overall={board['overall']} "
        f"ranker={'READY' if board['ranker_ready'] else 'BLOCKED'} "
        f"ok={board['counts']['ok']}/{board['counts']['n']}"
    )
    for p in board["processes"]:
        said = p.get("said") or ""
        if p["status"] == "OK" and not said:
            continue
        extra = f" | {said}" if said else ""
        print(f"  [{p['status']:<7}] {p['name']}{extra}")
    if args.write:
        paths = write_json(board)
        for p in paths:
            print(f"[day-board] wrote {p}")
    if args.html or args.write:
        hp = write_html()
        print(f"[day-board] html {hp}")


if __name__ == "__main__":
    main()
