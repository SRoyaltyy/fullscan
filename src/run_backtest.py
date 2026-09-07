"""Refresh historical dashboards from books already on disk.

Does not remine factor-mine, does not scrape Finviz, does not rewrite
LIVE_POLICY. Generate already wrote today's tickets; this is the
look-back curve refresh.

CLI: python3 -m src.run_backtest [--date YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from . import config

ROOT = Path(__file__).resolve().parent.parent
ET = ZoneInfo(config.TZ)


def _run(cmd: list[str], timeout_s: int | None = None) -> int:
    print(f"\n>>> {' '.join(cmd)}", flush=True)
    try:
        r = subprocess.run(
            cmd, cwd=str(ROOT), env=os.environ.copy(), timeout=timeout_s)
    except subprocess.TimeoutExpired:
        print(f"[backtest] WARN: timed out after {timeout_s}s: {' '.join(cmd)}",
              flush=True)
        return 124
    return r.returncode


def run(date: str | None = None) -> int:
    date = date or datetime.now(ET).date().isoformat()
    py = sys.executable
    print(f"[backtest] refresh dashboards as of {date} (no remine)")
    _run([py, "-m", "src.paper_trade", "--date", date, "--top", "10"],
         timeout_s=600)
    _run([py, "-m", "src.stock_book_backtest", "--top", "25", "--max-books", "30"],
         timeout_s=600)
    _run([py, "-m", "src.sleeve_combine_bt", "--mode", "io_boost", "--hold", "3d"],
         timeout_s=600)
    # Sweep dashboard only. Does not change LIVE_POLICY.
    _run([py, "-m", "src.sleeve_merge", "--write"], timeout_s=900)
    _run([py, "-m", "src.strategy_board", "--write"], timeout_s=120)
    print("[backtest] dashboards refreshed")
    return 0


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    args = ap.parse_args()
    raise SystemExit(run(args.date))


if __name__ == "__main__":
    main()
