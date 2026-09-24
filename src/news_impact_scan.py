"""CLI: harvest + impact router. Research-only. Does not touch flatten / Webull."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.lane_news_scan import harvest, harvest_all
from src.news_impact.backtest import load_corpus, markdown, run_backtest
from src.news_impact.pipeline import analyze_many, rollup

NEWS_DIR = Path("01_daily/news")
OUTBOX = Path("02_lessons/lane/outbox_impact")


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def run_live(date: str, limit: int, use_lane: bool, use_search: bool,
             use_openclaw: bool = False) -> dict:
    label = "all" if str(date).lower() in {"all", "*", "history"} else date
    if label == "all":
        arts = harvest_all()
    else:
        arts = harvest(date)
    # Fold in parsed-news titles so Grok/pipeline parses are in the universe.
    seen = {(a.get("title") or "").lower()[:160] for a in arts}
    for extra in load_corpus(label):
        k = (extra.get("title") or "").lower()[:160]
        if k and k not in seen:
            arts.append(extra)
            seen.add(k)
    results = analyze_many(
        arts, limit=limit, use_lane=use_lane, use_search=use_search,
        persist=True, ranked=True, use_openclaw=use_openclaw,
    )
    roll = rollup(results)
    report = {
        "date": label,
        "harvested": len(arts if not limit else arts[:limit] if limit else arts),
        "limit": limit,
        "use_lane": use_lane,
        "use_openclaw": use_openclaw,
        "use_search": use_search,
        "rollup": roll,
        "results": results,
    }
    NEWS_DIR.mkdir(parents=True, exist_ok=True)
    out_json = NEWS_DIR / f"{label}_news_impact.json"
    out_md = NEWS_DIR / f"{label}_news_impact.md"
    _write(out_json, json.dumps(report, indent=2, ensure_ascii=False))
    lines = [
        f"# News impact — {label}",
        "",
        f"articles={len(results)} usable={roll.get('usable')} "
        f"discarded={roll.get('discarded')} ratio={roll.get('usable_ratio')}",
        "",
        "## Hopper watermark",
        "",
    ]
    for k, n in (roll.get("hopper_watermark") or {}).items():
        lines.append(f"- {k}: {n}")
    lines += ["", "## Event classes", ""]
    for k, n in (roll.get("event_classes") or {}).items():
        lines.append(f"- {k}: {n}")
    lines += ["", "## Winners", ""]
    for k, n in (roll.get("winners") or {}).items():
        lines.append(f"- {k}: {n}")
    lines += ["", "## Losers", ""]
    for k, n in (roll.get("losers") or {}).items():
        lines.append(f"- {k}: {n}")
    lines += ["", "## Sample", ""]
    for r in results[:20]:
        ev = (r.get("classification") or {}).get("event_class")
        ents = [
            f"{e.get('ticker') or e.get('name')}:{e.get('direction')}"
            for e in (r.get("entities") or [])[:4]
        ]
        lines.append(
            f"- [{r.get('lane')}/{r.get('model')}] [{ev}] "
            f"{(r.get('title') or '')[:100]} {ents}"
        )
    _write(out_md, "\n".join(lines) + "\n")
    OUTBOX.mkdir(parents=True, exist_ok=True)
    (OUTBOX / f"{label}.json").write_text(out_json.read_text(encoding="utf-8"), encoding="utf-8")
    print("wrote", out_json, "usable=", roll.get("usable"), "/", len(results))
    return report


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="all", help="YYYY-MM-DD or all")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument(
        "--mode", choices=("backtest", "live"), default="backtest",
        help="backtest = parsed-news corpus, no hops. live = harvest + optional hops.",
    )
    ap.add_argument("--lane", action="store_true", help="use $0 Lane hops when keys exist")
    ap.add_argument(
        "--openclaw", action="store_true",
        help="hop SuperGrok through OpenClaw (cheapest model >30B)",
    )
    ap.add_argument("--search", action="store_true", help="Google AI Overview + web parse")
    args = ap.parse_args()
    if args.mode == "backtest":
        report = run_backtest(args.date, limit=args.limit, persist=False)
        NEWS_DIR.mkdir(parents=True, exist_ok=True)
        label = "all" if str(args.date).lower() in {"all", "*", "history"} else args.date
        out_json = NEWS_DIR / f"{label}_news_impact_backtest.json"
        # Keep the committed artifact lean: drop per-row bodies if huge.
        slim = dict(report)
        if len(json.dumps(slim.get("results") or [])) > 400_000:
            slim["results"] = []
            slim["results_omitted"] = True
        _write(out_json, json.dumps(slim, indent=2, ensure_ascii=False))
        _write(NEWS_DIR / f"{label}_news_impact_backtest.md", markdown(report))
        print(
            "backtest", label,
            "old", (report.get("old") or {}).get("usable_ratio"),
            "new", (report.get("new") or {}).get("usable_ratio"),
            "rescued", report.get("rescued_n"),
            "killed", report.get("killed_n"),
            "→", out_json,
        )
        return
    run_live(
        args.date, args.limit, use_lane=args.lane, use_search=args.search,
        use_openclaw=args.openclaw,
    )


if __name__ == "__main__":
    main()
