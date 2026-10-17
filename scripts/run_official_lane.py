"""Gate official rows, then link the ones Lane should see to tickers.

Inference uses the existing Lane hop when a model key is loaded.
This environment often has none. The run still classifies with the
deterministic router and links tickers from the Finviz industry book.
A second model is not called.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.news_impact.classify import classify_text
from src.news_impact.material_gate import gate, lane_article
from src.news_impact.ticker_verticals import link_title, load_book


def inference_ready() -> dict:
    from src import lane_route as lane
    keys, ollama_url, gh_direct = lane.load_keys()
    names = sorted(k for k in keys if k != "cloudflare_account")
    return {
        "keys": names,
        "ollama": bool(ollama_url),
        "github_models": bool(gh_direct),
        "first_lane": "openclaw grok-4.6" if "openclaw" in keys else (names[0] if names else ""),
        "ran": False,
    }


def run_rows(rows: list[dict], book: list[dict] | None = None) -> dict:
    book = book if book is not None else load_book()
    plan = inference_ready()
    kept = []
    dropped = {}
    for row in rows:
        decision = gate(row)
        if not decision["keep"]:
            dropped[decision["reason"]] = dropped.get(decision["reason"], 0) + 1
            continue
        article = lane_article(row)
        cls = classify_text(article["title"], article["body"])
        linked = link_title(article["title"], book)
        kept.append({
            "title": article["title"],
            "url": article["url"],
            "published_at": article["published_at"],
            "source": article["source"],
            "gate": decision["reason"],
            "router_class": cls.event_class,
            "router_q5": cls.q5,
            "router_why": cls.why,
            "link_reason": linked["reason"],
            "tickers": linked["tickers"],
        })
    plan["second_model"] = "not called"
    return {
        "rows": len(rows),
        "kept": len(kept),
        "dropped": dropped,
        "inference": plan,
        "items": kept,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--book", type=Path)
    args = parser.parse_args()
    rows = json.loads(args.inputs.read_text(encoding="utf-8"))
    if isinstance(rows, dict):
        rows = rows.get("items") or rows.get("documents") or []
    report = run_rows(rows, load_book(args.book) if args.book else None)
    text = json.dumps(report, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text + "\n", encoding="utf-8")
    print(json.dumps({k: v for k, v in report.items() if k != "items"}, indent=2))
    for item in report["items"]:
        ticks = ", ".join(
            f"{t['ticker']} ({t['role']})" for t in item["tickers"][:8]
        ) or "—"
        print(f"- {item['title'][:110]}")
        print(f"  {item['router_class']} | {item['link_reason']} | {ticks}")


if __name__ == "__main__":
    main()
