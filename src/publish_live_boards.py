"""Refresh live dashboard tickets after the book / packet lands.

Cheap path (this module):
  * rewrite data/day_board/*.json so factor-mine + day-board strips
    poll today's 1d BUY/SELL from main
  * copy the same strip to dashboard/factor-mine/today.json (Pages
    same-origin fallback)
  * rebuild day-board HTML, flatten live card, paper + sleeve-merge
    + strategy-board when those modules are importable

The 90-minute factor-mine recipe grid is NOT run here. Stock Book ALL
and Pre-Open ALL kick `.github/workflows/factor_mine.yml` after the
book exists so every recipe blotter rolls to this date.

CLI: python -m src.publish_live_boards --date YYYY-MM-DD --write
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from . import config

ROOT = Path(__file__).resolve().parent.parent
ET = ZoneInfo(config.TZ)
BOOK = ROOT / "data" / "stock_book"
DAY_BOARD = ROOT / "data" / "day_board"
FM_TODAY = ROOT / "dashboard" / "factor-mine" / "today.json"


def _today() -> str:
    return datetime.now(ET).date().isoformat()


def _book_ok(date: str) -> bool:
    return (BOOK / f"{date}_stock_book.json").is_file() or (
        ROOT / "01_daily" / f"{date}_stock_book.md"
    ).is_file()


def _run(argv: list[str], timeout_s: int = 180) -> int:
    print("[live-boards] $ " + " ".join(argv), flush=True)
    try:
        r = subprocess.run(argv, cwd=str(ROOT), timeout=timeout_s)
    except subprocess.TimeoutExpired:
        print(f"[live-boards] WARN: timeout {timeout_s}s: {' '.join(argv)}",
              flush=True)
        return 124
    except OSError as e:
        print(f"[live-boards] WARN: {e}", flush=True)
        return 1
    if r.returncode:
        print(f"[live-boards] WARN: exit {r.returncode}: {' '.join(argv)}",
              flush=True)
    return r.returncode


def publish(date: str, *, write: bool = True, extras: bool = True) -> dict:
    """Rewrite day-board + optional live HTML. Soft-fail everything."""
    out: dict = {"date": date, "book_ok": _book_ok(date), "wrote": []}
    if not out["book_ok"]:
        print(f"[live-boards] no stock book on disk for {date} — "
              "strip stays empty until the ranker lands", flush=True)
    try:
        from . import day_board
        board = day_board.build(date)
        if write:
            paths = day_board.write_json(board)
            out["wrote"].extend(str(p.relative_to(ROOT)) for p in paths)
            try:
                hp = day_board.write_html()
                out["wrote"].append(str(hp.relative_to(ROOT)))
            except Exception as e:  # noqa: BLE001
                print(f"[live-boards] WARN: day-board html: {e}", flush=True)
        sel = board.get("selections") or {}
        out["buy_1d"] = [r.get("ticker") for r in (sel.get("buy_1d") or [])]
        out["sell_1d"] = [r.get("ticker") for r in (sel.get("sell_1d") or [])]
        out["overall"] = board.get("overall")
        if write:
            FM_TODAY.parent.mkdir(parents=True, exist_ok=True)
            payload = {
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
                "lands": board.get("lands") or [],
                "source": "publish_live_boards",
            }
            FM_TODAY.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            out["wrote"].append("dashboard/factor-mine/today.json")
            dated = DAY_BOARD / f"{date}_tickets.json"
            dated.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            out["wrote"].append(str(dated.relative_to(ROOT)))
    except Exception as e:  # noqa: BLE001
        print(f"[live-boards] WARN: day-board rebuild failed: {e}", flush=True)
        out["error"] = str(e)

    if extras and write:
        py = sys.executable
        _run([py, "-m", "src.sleeve_merge", "--card", "--date", date,
              "--write-card"], timeout_s=120)
        _run([py, "-m", "src.paper_trade", "--date", date, "--top", "10"],
             timeout_s=180)
        _run([py, "-m", "src.strategy_board", "--write"], timeout_s=180)
        _run([py, "-m", "src.sleeve_merge", "--write"], timeout_s=180)

    print(
        f"[live-boards] {date} book_ok={out['book_ok']} "
        f"buy={out.get('buy_1d') or []} sell={out.get('sell_1d') or []} "
        f"wrote={len(out.get('wrote') or [])}",
        flush=True,
    )
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="", help="YYYY-MM-DD (default today ET)")
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--no-extras", action="store_true",
                    help="Only rewrite the 1d strip JSON (no paper/sleeve)")
    args = ap.parse_args(argv)
    date = args.date or _today()
    publish(date, write=args.write, extras=not args.no_extras)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
