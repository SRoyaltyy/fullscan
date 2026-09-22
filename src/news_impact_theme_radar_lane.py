"""CLI for the Lane-on-Elite theme-radar backtest.

  python -m src.news_impact_theme_radar_lane env
  python -m src.news_impact_theme_radar_lane env --strict
  python -m src.news_impact_theme_radar_lane prepare
  python -m src.news_impact_theme_radar_lane hop --date 2026-09-18
  python -m src.news_impact_theme_radar_lane merge
"""
from __future__ import annotations

import argparse
from pathlib import Path

from src.news_impact.lane_env import require_lane_env
from src.news_impact.theme_radar_lane import (
    BOARD_JSON,
    MANIFEST,
    SCOREBOARD,
    SHARD_DIR,
    hop_shard,
    merge_shards,
    prepare_manifest,
)


def main() -> None:
    ap = argparse.ArgumentParser(description="Lane-on-Elite theme-radar backtest")
    sub = ap.add_subparsers(dest="cmd", required=True)

    env = sub.add_parser("env", help="redacted PRESENT/MISSING check")
    env.add_argument("--strict", action="store_true", help="exit 1 when no current-flash key")

    prep = sub.add_parser("prepare", help="cut Tier A from the Elite ingest (no Lane)")
    prep.add_argument("--date", default="all")
    prep.add_argument("--no-prices", dest="prices", action="store_false", default=True)

    hop = sub.add_parser("hop", help="current-flash hop for one session shard")
    hop.add_argument("--date", required=True)
    hop.add_argument("--manifest", default=str(MANIFEST))
    hop.add_argument("--out", default="")
    hop.add_argument("--no-prices", dest="prices", action="store_false", default=True)

    merge = sub.add_parser("merge", help="merge shards into the Lane scoreboard")
    merge.add_argument("--manifest", default=str(MANIFEST))
    merge.add_argument("--shards", default=str(SHARD_DIR))
    merge.add_argument("--strict", action="store_true", help="exit 1 unless every tier row is Lane-ok")

    args = ap.parse_args()
    if args.cmd == "env":
        require_lane_env(strict=args.strict)
        return
    if args.cmd == "prepare":
        prepare_manifest(args.date, fetch=args.prices)
        return
    if args.cmd == "hop":
        out = Path(args.out) if args.out else SHARD_DIR / f"{args.date}.json"
        hop_shard(args.date, Path(args.manifest), out, fetch=args.prices)
        return
    report = merge_shards(Path(args.manifest), Path(args.shards), SCOREBOARD, BOARD_JSON)
    if args.strict and not report.get("ship_lane"):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
