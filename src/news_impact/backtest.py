"""Compare new router vs mechanical news_parse on Grok/pipeline parses."""
from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path

from .classify import rank_articles
from .grade import format_performance, performance_rollup
from .pipeline import analyze_article, rollup

NEWS_DIR = Path("01_daily/news")
GROK_DIR = Path("data/grok_automations")
_DATE_IN_NAME = re.compile(r"(\d{4}-\d{2}-\d{2})")

COMPACT_KEYS = (
    "article_id", "title", "source", "url", "source_file",
    "published_at", "retrieved_at", "known_at",
    "sectors", "macro_themes",
    "classification", "q5", "axioms_used", "entities", "conclusion",
    "hop_chain", "models", "reasoning", "performance",
    "usable", "tradable", "macro_factor", "old_usable", "old_class",
    "lane", "model", "inference_source", "pipeline_version",
)


def compact_row(row: dict) -> dict:
    return {k: row[k] for k in COMPACT_KEYS if k in row}


def load_parsed(path: Path) -> list[dict]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError):
        return []
    if not isinstance(data, dict):
        return []
    retrieved = str(data.get("generated_at") or "").strip()
    m = _DATE_IN_NAME.search(path.name)
    if not retrieved and m:
        retrieved = m.group(1)
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
        published = str(it.get("published_at") or "").strip()
        out.append({
            "title": title,
            "body": str(it.get("url") or ""),
            "url": str(it.get("url") or ""),
            "source": str(it.get("source") or ""),
            "source_file": str(path),
            "published_at": published,
            "retrieved_at": retrieved,
            "known_at": published or retrieved,
            "sectors": list(it.get("sectors") or []),
            "macro_themes": list(it.get("macro_themes") or []),
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
            known = str(it.get("createTime") or it.get("published_at") or "")
            out.append({
                "title": title,
                "body": str(it.get("prompt") or it.get("body") or "")[:800],
                "url": str(it.get("url") or ""),
                "source": "grok_automation",
                "source_file": str(path),
                "published_at": known,
                "retrieved_at": known,
                "known_at": known,
                "sectors": [],
                "macro_themes": [],
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
    use_lane: bool = False,
    use_search: bool = False,
    keep_results: bool = True,
) -> dict:
    arts = load_corpus(date)
    arts = rank_articles(arts)
    if limit and limit > 0:
        arts = arts[:limit]
    results = [
        analyze_article(
            a, use_lane=use_lane, use_search=use_search, persist=persist,
        )
        for a in arts
    ]
    old_u = sum(1 for a in arts if a.get("old_usable") is True)
    old_n = sum(1 for a in arts if a.get("old_usable") is not None)
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
        "tape": performance_rollup(results) if any(r.get("performance") for r in results) else {},
        "results": [compact_row(r) for r in results] if keep_results else [],
    }
    return report


def _cell(raw) -> str:
    s = str(raw or "").replace("\r\n", "\n").replace("\r", "\n")
    s = s.replace("|", "/").replace("\n", "<br>")
    return s


def _article_cell(r: dict) -> str:
    title = _cell((r.get("title") or "")[:180])
    src = _cell(r.get("source") or "")
    return f"{title}<br>`{src}`" if src else title


def _llm_cell(r: dict) -> str:
    models = r.get("models") or []
    if models:
        return _cell(" → ".join(str(m) for m in models))
    return _cell(f"{r.get('lane') or '?'}::{r.get('model') or '?'}")


def _reason_cell(r: dict) -> str:
    steps = r.get("reasoning") or []
    if steps:
        return "<br>".join(f"{i}. {_cell(s)}" for i, s in enumerate(steps, 1))
    cls = r.get("classification") or {}
    return _cell(
        f"Q5 {cls.get('q5')} · {cls.get('event_class')} · {cls.get('why')}"
    )


def _conclusion_cell(r: dict) -> str:
    if not r.get("usable"):
        cls = r.get("classification") or {}
        return _cell(
            f"discard / weather — {cls.get('event_class')} ({cls.get('why')})"
        )
    c = r.get("conclusion") or {}
    if not c:
        up = [f"{e.get('ticker') or e.get('name')}" for e in (r.get("entities") or []) if e.get("direction") == "up"]
        down = [f"{e.get('ticker') or e.get('name')}" for e in (r.get("entities") or []) if e.get("direction") == "down"]
        return _cell(f"UP: {', '.join(up) or '—'} · DOWN: {', '.join(down) or '—'}")
    return _cell(
        f"UP: {', '.join(c.get('up') or []) or '—'} · "
        f"DOWN: {', '.join(c.get('down') or []) or '—'} · "
        f"MIXED: {', '.join(c.get('mixed') or []) or '—'} · "
        f"ND: {', '.join(c.get('not_determined') or []) or '—'}"
    )


def _actual_cell(r: dict) -> str:
    if "performance" not in r:
        return "not graded"
    return format_performance(r.get("performance") or [])


def _table(rows: list[dict]) -> list[str]:
    lines = [
        "| # | Article | Published | Retrieved | LLM(s) | Reasoning | Conclusion | Actual |",
        "| ---: | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for i, r in enumerate(rows, 1):
        lines.append(
            "| "
            + " | ".join([
                str(i),
                _article_cell(r),
                _cell(r.get("published_at") or "—"),
                _cell(r.get("retrieved_at") or "—"),
                _llm_cell(r),
                _reason_cell(r),
                _conclusion_cell(r),
                _actual_cell(r),
            ])
            + " |"
        )
    return lines


def markdown(report: dict) -> str:
    old = report.get("old") or {}
    new = report.get("new") or {}
    tape = report.get("tape") or {}
    results = report.get("results") or []
    usable = [r for r in results if r.get("usable")]
    discarded = [r for r in results if not r.get("usable")]
    lines = [
        f"# News-impact backtest — {report.get('date')}",
        "",
        "Human table of every harvested article: what published/when we "
        "retrieved it, which model(s) processed it, the intermediary "
        "reasoning, the up/down conclusion, and the realized tape.",
        "",
        f"articles={report.get('harvested')}  pipeline=news_impact_v2  "
        f"usable_rows={len(usable)}  tradable_rows="
        f"{sum(1 for r in results if r.get('tradable'))}  "
        f"discarded_rows={len(discarded)}",
        "",
        "## Usable / discarded",
        "",
        f"- Old news_parse: usable={old.get('usable')} discarded={old.get('discarded')} "
        f"ratio={old.get('usable_ratio')}",
        f"- New router: usable={new.get('usable')} discarded={new.get('discarded')} "
        f"ratio={new.get('usable_ratio')}  tradable={new.get('tradable')} "
        f"ratio={new.get('tradable_ratio')}",
        f"- Rescued (old discard → new usable): {report.get('rescued_n')}",
        f"- Killed (old usable → new weather/discard): {report.get('killed_n')}",
        f"- Ratio delta: {(report.get('improvement') or {}).get('usable_ratio_delta')}",
        "",
        "## Actual tape (directional calls only)",
        "",
        "Tradable = listed ticker with an up/down call. Ticker-less macro "
        "is tradable only when the factor basket is signed (print or decision), "
        "not on Fed-path color.",
        "",
    ]
    if tape:
        lines += [
            f"- 0-1d (entry-day close vs entry open): "
            f"{tape.get('hit_1d')}/{tape.get('n_1d')} "
            f"hit_rate={tape.get('hit_rate_1d')}",
            f"- 1-4w (~20 sessions): "
            f"{tape.get('hit_20d')}/{tape.get('n_20d')} "
            f"hit_rate={tape.get('hit_rate_20d')}",
            f"- directional calls: {tape.get('directional_calls')}  "
            f"missing tape: {tape.get('missing_tape')}",
            "",
        ]
        if tape.get("avg_ret_1d_by_ticker"):
            lines.append("Average 0-1d return by ticker (directional only):")
            lines.append("")
            for t, v in (tape.get("avg_ret_1d_by_ticker") or {}).items():
                lines.append(f"- {t}: {v:+.2f}%")
            lines.append("")
    else:
        lines += [
            "- not graded in this run (pass `--prices` on the specialised action).",
            "",
        ]
    lines += ["## Hopper watermark", ""]
    for k, n in (new.get("hopper_watermark") or {}).items():
        lines.append(f"- {k}: {n}")
    lines += ["", "## Event classes", ""]
    for k, n in (new.get("event_classes") or {}).items():
        lines.append(f"- {k}: {n}")
    macro = [
        r for r in results
        if (r.get("classification") or {}).get("event_class") == "factor_impulse"
        or r.get("macro_factor")
    ]
    if macro and results:
        lines += [
            "",
            f"## Macro book — ticker-less factor ({len(macro)})",
            "",
            "No single-name in the lede. Listed expressions are duration "
            "(QQQ/TLT), dollar (UUP), credit (HYG), risk (SPY), oil (XLE/USO). "
            "Fed-path color and speeches are discarded as weather.",
            "",
        ]
        lines += _table(macro)
    if not results:
        lines += [
            "",
            "## Articles",
            "",
            "Per-article rows omitted from this artifact. Re-run "
            "`python -m src.news_impact_backtest` (specialised action).",
            "",
        ]
        lines += ["## Rescued sample", ""]
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

    lines += [
        "",
        f"## Usable articles ({len(usable)})",
        "",
        "Sorted the same way the router harvests: gov/macro/structure first.",
        "",
    ]
    lines += _table(usable)
    lines += [
        "",
        f"## Discarded / weather ({len(discarded)})",
        "",
        "Still listed so a human can see why it was thrown out, and when.",
        "",
    ]
    lines += _table(discarded)
    return "\n".join(lines) + "\n"
