"""Compare new router vs mechanical news_parse on Grok/pipeline parses."""
from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path

from .classify import rank_articles
from .grade import format_performance, format_slice_table, performance_rollup, stamp_grade_flags
from .grok_automations import load_grok_dumps, prefer_source
from .hygiene import entry_clock_of
from .pipeline import analyze_article, rollup

NEWS_DIR = Path("01_daily/news")
_DATE_IN_NAME = re.compile(r"(\d{4}-\d{2}-\d{2})")

COMPACT_KEYS = (
    "article_id", "title", "source", "harvest_source", "url", "source_file",
    "published_at", "retrieved_at", "known_at", "entry_clock",
    "sectors", "macro_themes", "ticker_hint",
    "task_id", "automation_slug", "automation_kind", "macro_only",
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


def _skip_quarantine(path: Path) -> bool:
    from ..quarantine_sessions import is_quarantined
    m = _DATE_IN_NAME.search(path.name)
    return bool(m and is_quarantined(m.group(1)))


def _drop_quarantined_articles(arts: list[dict]) -> list[dict]:
    from ..quarantine_sessions import is_quarantined
    out = []
    for art in arts:
        m = _DATE_IN_NAME.search(str(art.get("source_file") or ""))
        if m and is_quarantined(m.group(1)):
            continue
        out.append(art)
    return out


def load_corpus(date: str | None = None) -> list[dict]:
    from ..quarantine_sessions import is_quarantined
    arts: list[dict] = []
    if date and date.lower() not in {"all", "*", "history"}:
        if is_quarantined(date):
            print(f"[news-impact] skip quarantined session {date}")
            return []
        p = NEWS_DIR / f"{date}_parsed.json"
        arts.extend(load_parsed(p))
    else:
        for p in sorted(NEWS_DIR.glob("*_parsed.json")):
            if _skip_quarantine(p):
                continue
            arts.extend(load_parsed(p))
    arts.extend(load_grok_dumps(date=date))
    return prefer_source(_drop_quarantined_articles(arts))


def overlay_existing_tape(results: list[dict], artifact: Path | None = None) -> list[dict]:
    """Re-use already-graded tape from a committed backtest JSON by title."""
    path = artifact or Path("01_daily/news/all_news_impact_backtest.json")
    if not path.is_file():
        return results
    try:
        blob = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return results
    by_title = {}
    for old in blob.get("results") or []:
        if not isinstance(old, dict):
            continue
        t = str(old.get("title") or "")
        if t and old.get("performance"):
            by_title[t] = old["performance"]
    if not by_title:
        return results
    out = []
    for r in results:
        row = dict(r)
        if not row.get("performance"):
            taped = by_title.get(str(row.get("title") or ""))
            if taped:
                row["performance"] = [dict(g) for g in taped if isinstance(g, dict)]
                _align_tape_to_router(row)
        out.append(row)
    return stamp_grade_flags(out)


def _align_tape_to_router(row: dict) -> None:
    """After overlay, pull direction/class from the new router so hygiene sticks."""
    cls = row.get("classification") or {}
    ev = str(cls.get("event_class") or "")
    q5 = str(cls.get("q5") or "")
    by_tick = {
        str(e.get("ticker") or ""): e.get("direction")
        for e in (row.get("entities") or [])
        if isinstance(e, dict) and e.get("ticker")
    }
    default_dir = None
    if ev == "guidance":
        if cls.get("split") and cls.get("sign") is None:
            default_dir = "mixed"
        elif cls.get("sign") == "cut":
            default_dir = "down"
        elif cls.get("sign") == "raise":
            default_dir = "up"
        else:
            default_dir = "not_determined"
    for g in row.get("performance") or []:
        if not isinstance(g, dict):
            continue
        g["event_class"] = ev or g.get("event_class")
        g["q5"] = q5 or g.get("q5")
        tick = str(g.get("ticker") or "")
        if tick in by_tick:
            g["direction"] = by_tick[tick]
        elif default_dir:
            g["direction"] = default_dir


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


def _published_cell(r: dict) -> str:
    pub = str(r.get("published_at") or "").strip()
    clock = r.get("entry_clock") or entry_clock_of(r)
    if pub:
        return _cell(f"{pub} · {clock}")
    return _cell(f"— · {clock}")


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
    return format_performance(r.get("performance") or [], row=r)


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
                _published_cell(r),
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
    results = report.get("results") or []
    if results:
        stamp_grade_flags(results)
    tape = report.get("tape") or {}
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
        "## Actual tape (graded directional calls only)",
        "",
        "0-1d and 1-4w hit rates count a row only when **q5=impulse**, "
        "**direction in {up, down}**, and **tradeable_expression=direct** "
        "(listed ticker or ETF that *is* the expression). "
        "`factor_impulse` / Fed→QQQ/SPY and similar index-factor rows stay "
        "in the article table as **ungraded** context. "
        "`mixed` / `not_determined` stay ungraded — no new scores.",
        "",
        "Hygiene (this book, not a new taxonomy):",
        "- price-reaction titles (plunge / surge N% / rebound / falls N% / "
        "drives N% drop) are discard/regime with empty entities — never graded.",
        "- reaffirm / maintains guidance → sign=None, direction=not_determined; "
        "raise-guidance + miss-EPS → mixed (never a single UP).",
        "- Published timestamp used for the entry clock when the source "
        "provides one; missing Published is still retrieved and marked "
        "`entry_clock=retrieved_only` (never invented).",
        "- macro/factor_impulse reprints collapse to one row per "
        "(factor, session, sign). Headline basket hit (majority of "
        "QQQ/TLT/UUP/HYG/SPY) is a **separate** column from the graded "
        "0-1d / 1-4w rates. Legs are transparency only.",
        "- gate / capacity / blast_cyber / CHIPS-style awards skip the "
        "0-1d hit-rate column (horizon noted); they may still count in 1-4w.",
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
            f"- graded directional calls: {tape.get('directional_calls')}  "
            f"ungraded context rows: {tape.get('ungraded_context')}  "
            f"missing tape: {tape.get('missing_tape')}",
            "",
            "## Slice hit rates",
            "",
            "Graded slices use the same cut as the headline rates. "
            "`factor_impulse` is shown for comparison and is **ungraded**.",
            "",
        ]
        lines += format_slice_table(tape)
        lines.append("")
        mh = tape.get("macro_headline") or {}
        if mh:
            lines += [
                "## Macro headline basket (not in graded 0-1d / 1-4w)",
                "",
                "One story per `(factor, session, sign)`. FOMC-hold reprints "
                "do not add extra legs to any denominator. "
                "**Headline hit** = majority of QQQ/TLT/UUP/HYG/SPY agreeing "
                "with the implied sign. **Legs** are listed for transparency "
                "and are not double-counted into the headline rate or the "
                "graded 0-1d / 1-4w columns.",
                "",
                f"- headline 0-1d: {mh.get('headline_hit_1d')}/{mh.get('headline_n_1d')} "
                f"hit_rate={mh.get('headline_hit_rate_1d')}",
                f"- headline 1-4w: {mh.get('headline_hit_20d')}/{mh.get('headline_n_20d')} "
                f"hit_rate={mh.get('headline_hit_rate_20d')}",
                f"- stories={mh.get('n_stories')}  reprints_collapsed="
                f"{mh.get('reprints_collapsed')}",
                f"- legs (transparency): 0-1d {mh.get('leg_hit_1d')}/{mh.get('leg_n_1d')} "
                f"rate={mh.get('leg_hit_rate_1d')} · "
                f"1-4w {mh.get('leg_hit_20d')}/{mh.get('leg_n_20d')} "
                f"rate={mh.get('leg_hit_rate_20d')}",
                "",
            ]
        if tape.get("avg_ret_1d_by_ticker"):
            lines.append("Average 0-1d return by ticker (graded directional only):")
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
