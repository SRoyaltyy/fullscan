"""Theme-radar Elite news-impact scoreboard.

CLI: python -m src.news_impact_theme_radar --date all
     python -m src.news_impact_theme_radar --date all --lane

Does not edit SRoyaltyy/theme-radar. Does not touch flatten / Webull /
factor-mine. Research-only.
"""
from __future__ import annotations

import argparse

from src.news_impact.theme_radar_board import run


def main() -> None:
    ap = argparse.ArgumentParser(description="Elite theme-radar news-impact scoreboard")
    ap.add_argument("--date", default="all", help="YYYY-MM-DD or all")
    ap.add_argument(
        "--lane",
        action="store_true",
        help="current-flash Lane on the tradable Elite subset; 429 leaves the provider",
    )
    ap.add_argument("--no-prices", dest="prices", action="store_false", default=True)
    ap.add_argument(
        "--search",
        action="store_true",
        help="accepted for the shared workflow; Elite tape grade does not call search",
    )
    ap.add_argument("--limit", type=int, default=0, help="cap unique titles (0 = all)")
    args = ap.parse_args()
    run(args.date, use_lane=args.lane, fetch=args.prices, limit=args.limit)


if __name__ == "__main__":
    main()
