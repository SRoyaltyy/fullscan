"""Compare new router vs mechanical news_parse on Grok/pipeline parses."""
from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

from .classify import rank_articles
from .pipeline import analyze_article, rollup

NEWS_DIR = Path("01_daily/news")
GROK_DIR = Path("data/grok_automations")


def load_parsed(path: Path) -> list[dict]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError):
        return []
    if not isinstance(data, dict):
        return []
    out = []
    seen: set[str] = set()
    for it in data.get("all_items") or []:
        if not isinstance(it, dict):
            continue
        title = str(it.get("title") or "").strip()
        key = title.lower()[:160]
        if not title or key in seen:
            continue
        seen.add(key)
        out.append({
            "title": title,
            "body": str(it.get("url") or ""),
            "source_file": str(path),
            "known_at": str(it.get("published_at") or ""),
            "old_usable": bool(it.get("usable")),
            "old_class": str(it.get("class") or ""),
        })
    return out


def load_grok_dumps() -> list[dict]:
    if not GROK_DIR.is_dir():
        return []
    out = []
    for path in sorted(GROK_DIR.glob("*.json")):
        try:
            blob = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        rows = blob if isinstance(blob, list) else (blob or {}).get("results") or [blob]
        for it in rows:
            if not isinstance(it, dict):
                continue
            title = str(it.get("title") or "").strip()
            if not title:
                continue
            out.append({
                "title": title,
                "body": str(it.get("prompt") or it.get("body") or "")[:800],
                "source_file": str(path),
                "known_at": str(it.get("createTime") or ""),
                "old_usable": None,
                "old_class": "grok_automation",
            })
    return out


def load_corpus(date: str | None = None) -> list[dict]:
    arts: list[dict] = []
    if date and date.lower() not in {"all", "*", "history"}:
        p = NEWS_DIR / f"{date}_parsed.json"
        arts.extend(load_parsed(p))
    else:
        for p in sorted(NEWS_DIR.glob("*_parsed.json")):
            arts.extend(load_parsed(p))
    arts.extend(load_grok_dumps())
    # Unique by title
    bag, out = set(), []
    for a in arts:
        k = (a.get("title") or "").lower()[:160]
        if k in bag:
            continue
        bag.add(k)
        out.append(a)
    return out


def run_backtest(
    date: str = "all",
    limit: int = 0,
    persist: bool = False,
) -> dict:
    arts = load_corpus(date)
    arts = rank_articles(arts)
    if limit and limit > 0:
        arts = arts[:limit]
    results = [
        analyze_article(a, use_lane=False, use_search=False, persist=persist)
        for a in arts
    ]
    old_u = sum(1 for a in arts if a.get("old_usable") is True)
    old_n = sum(1 for a in arts if a.get("old_usable") is not None)
    new_u = sum(1 for r in results if r.get("usable"))
    rescued, killed = [], []
    for a, r in zip(arts, results):
        if a.get("old_usable") is False and r.get("usable"):
            rescued.append({
                "title": a["title"][:180],
                "event_class": (r.get("classification") or {}).get("event_class"),
                "entities": [
                    f"{e.get('ticker') or e.get('name')}:{e.get('direction')}"
                    for e in (r.get("entities") or [])[:6]
                ],
            })
        if a.get("old_usable") is True and not r.get("usable"):
            killed.append({
                "title": a["title"][:180],
                "old_class": a.get("old_class"),
                "event_class": (r.get("classification") or {}).get("event_class"),
                "q5": (r.get("classification") or {}).get("q5"),
                "why": (r.get("classification") or {}).get("why"),
            })
    by_old = Counter(a.get("old_class") or "?" for a in arts)
    roll = rollup(results)
    report = {
        "date": date,
        "harvested": len(arts),
        "old": {
            "n_labeled": old_n,
            "usable": old_u,
            "discarded": old_n - old_u,
            "usable_ratio": round(old_u / old_n, 4) if old_n else None,
            "by_class": dict(by_old),
        },
        "new": roll,
        "rescued_n": len(rescued),
        "killed_n": len(killed),
        "rescued_sample": rescued[:25],
        "killed_sample": killed[:25],
        "improvement": {
            "usable_ratio_delta": (
                round(roll["usable_ratio"] - (old_u / old_n), 4)
                if old_n else None
            ),
            "note": (
                "usable = impulse/regime_break with a real mechanism. "
                "rescued = old parse discarded, router kept. "
                "killed = old parse usable (often Hormuz/gold reprints), router weather/discard."
            ),
        },
        "results": results if limit and limit <= 80 else [],
    }
    return report


def markdown(report: dict) -> str:
    old = report.get("old") or {}
    new = report.get("new") or {}
    lines = [
        f"# News-impact backtest — {report.get('date')}",
        "",
        f"articles={report.get('harvested')}  pipeline=news_impact_v1",
        "",
        "## Usable / discarded",
        "",
        f"- Old news_parse: usable={old.get('usable')} discarded={old.get('discarded')} "
        f"ratio={old.get('usable_ratio')}",
        f"- New router: usable={new.get('usable')} discarded={new.get('discarded')} "
        f"ratio={new.get('usable_ratio')}",
        f"- Rescued (old discard → new usable): {report.get('rescued_n')}",
        f"- Killed (old usable → new weather/discard): {report.get('killed_n')}",
        f"- Ratio delta: {(report.get('improvement') or {}).get('usable_ratio_delta')}",
        "",
        "## Hopper watermark",
        "",
    ]
    for k, n in (new.get("hopper_watermark") or {}).items():
        lines.append(f"- {k}: {n}")
    lines += ["", "## Event classes", ""]
    for k, n in (new.get("event_classes") or {}).items():
        lines.append(f"- {k}: {n}")
    lines += ["", "## Rescued sample", ""]
    for r in report.get("rescued_sample") or []:
        lines.append(
            f"- [{r.get('event_class')}] {r.get('title')} → {r.get('entities')}"
        )
    lines += ["", "## Killed sample (old usable, now weather/junk)", ""]
    for r in report.get("killed_sample") or []:
        lines.append(
            f"- [{r.get('q5')}/{r.get('event_class')}] {r.get('title')} "
            f"({r.get('why')})"
        )
    return "\n".join(lines) + "\n"
