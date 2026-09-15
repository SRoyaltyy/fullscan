"""Copy the live 09:30 strip onto every Pages surface.

Paper / factor-mine / strategy-board firstOk looks next to the HTML.
On 2026-09-15 the same-origin factor-mine tickets were a 200 without
``quote`` / top-level ``buy_1d``, so loaders never reached raw main
(elite_live MTCH 43.04). A last-write-wins copy then overwrote the
good ``data/day_board`` slim with that stripped file.

Pick the richest Elite file and publish it everywhere, including
factor-mine. If today.json is score-only, restamp 1d rows from tickets.
"""
from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

DEST_REL = (
    ".",
    "dashboard",
    "day-board",
    "dashboard/day-board",
    "strategy-board",
    "dashboard/strategy-board",
    "factor-mine",
    "dashboard/factor-mine",
    "sleeve-merge",
    "dashboard/sleeve-merge",
)


def _load(path: Path) -> dict:
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _quote_live(quote: object) -> bool:
    if not isinstance(quote, dict):
        return False
    src = str(quote.get("src") or "")
    return bool(quote.get("after_open")) and src.startswith("elite_live")


def _row0(rows: object) -> dict:
    if isinstance(rows, list) and rows and isinstance(rows[0], dict):
        return rows[0]
    return {}


def tickets_score(data: dict) -> int:
    if not data:
        return -1
    sb = (data.get("strategies") or {}).get("stock_book_1d") or {}
    buys = data.get("buy_1d") or sb.get("buy") or []
    row = _row0(buys)
    score = 0
    if _quote_live(data.get("quote")):
        score += 100
    if row.get("px") is not None:
        score += 10
    if str(row.get("px_src") or "").startswith("elite_live"):
        score += 10
    if data.get("buy_1d"):
        score += 5
    if data.get("quote"):
        score += 2
    if data.get("date"):
        score += 1
    return score


def strip_score(data: dict) -> int:
    if not data:
        return -1
    row = _row0(data.get("buy_1d"))
    score = 0
    if _quote_live(data.get("quote")):
        score += 100
    if row.get("px") is not None:
        score += 10
    if str(row.get("px_src") or "").startswith("elite_live"):
        score += 10
    if data.get("date"):
        score += 1
    return score


def pick_best(paths: list[Path], score_fn) -> tuple[Path | None, dict]:
    best_path: Path | None = None
    best: dict = {}
    best_score = -1
    for path in paths:
        data = _load(path)
        score = score_fn(data)
        if score > best_score:
            best_score = score
            best_path = path
            best = data
    if best_score < 0:
        return None, {}
    return best_path, best


def ticket_1d(data: dict) -> tuple[list, list, dict]:
    sb = (data.get("strategies") or {}).get("stock_book_1d") or {}
    buys = list(data.get("buy_1d") or sb.get("buy") or [])
    sells = list(data.get("sell_1d") or sb.get("sell") or [])
    return buys, sells, data.get("quote") or {}


def restamp_strip(strip: dict, tickets: dict) -> dict:
    """Keep land metadata; force Elite 1d names + quote from tickets."""
    out = dict(strip) if strip else {}
    buys, sells, quote = ticket_1d(tickets)
    if tickets.get("date"):
        out["date"] = tickets.get("date")
    if buys:
        out["buy_1d"] = buys
    if sells:
        out["sell_1d"] = sells
    if quote:
        out["quote"] = quote
    return out


def candidate_ticket_paths(repo: Path) -> list[Path]:
    return [
        repo / "data" / "day_board" / "today_strategies.json",
        repo / "dashboard" / "today_strategies.json",
        repo / "dashboard" / "factor-mine" / "today_strategies.json",
        repo / "dashboard" / "factor-mine" / "strategy_tickets.json",
        repo / "data" / "day_board" / "strategy_tickets.json",
    ]


def candidate_strip_paths(repo: Path) -> list[Path]:
    return [
        repo / "data" / "day_board" / "today.json",
        repo / "dashboard" / "factor-mine" / "today.json",
        repo / "dashboard" / "today.json",
    ]


def overlay(dest_root: Path, repo: Path | None = None) -> list[str]:
    """Write the winning today.json + today_strategies.json under dest_root."""
    repo = repo or ROOT
    dest_root = Path(dest_root)
    t_path, tickets = pick_best(candidate_ticket_paths(repo), tickets_score)
    s_path, strip = pick_best(candidate_strip_paths(repo), strip_score)
    if tickets and strip_score(strip) < 100 and tickets_score(tickets) >= 100:
        strip = restamp_strip(strip, tickets)
        s_path = None
    wrote: list[str] = []
    for rel in DEST_REL:
        dest = dest_root / rel if rel != "." else dest_root
        dest.mkdir(parents=True, exist_ok=True)
        if tickets:
            out = dest / "today_strategies.json"
            out.write_text(json.dumps(tickets, indent=2), encoding="utf-8")
            wrote.append(str(out))
            print(f"Overlayed live strip {out}", flush=True)
        elif t_path and t_path.is_file():
            out = dest / "today_strategies.json"
            shutil.copy2(t_path, out)
            wrote.append(str(out))
            print(f"Overlayed live strip {out}", flush=True)
        if strip:
            out = dest / "today.json"
            out.write_text(json.dumps(strip, indent=2), encoding="utf-8")
            wrote.append(str(out))
            print(f"Overlayed live strip {out}", flush=True)
        elif s_path and s_path.is_file():
            out = dest / "today.json"
            shutil.copy2(s_path, out)
            wrote.append(str(out))
            print(f"Overlayed live strip {out}", flush=True)
    return wrote


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dest", required=True, help="pages_out directory")
    ap.add_argument("--repo", default="", help="repo root (default: this tree)")
    args = ap.parse_args(argv)
    repo = Path(args.repo) if args.repo else ROOT
    wrote = overlay(Path(args.dest), repo=repo)
    if not wrote:
        print("WARN: no live strip to overlay", flush=True)
        return 0
    print(f"Overlayed live strip n={len(wrote)}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
