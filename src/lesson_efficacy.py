"""Did a promoted lesson actually change behavior — or is the bot just
journaling?

For every ACTIVE lesson, attribute it to a graded topic (general or a
sector), then compare that topic's direction hit rate in the graded runs
BEFORE the lesson went active vs AFTER. A lesson whose topic got worse
after promotion is flagged as a retirement candidate; a topic that
improves is (weak, correlational) evidence the loop is working. Small
samples are labeled insufficient rather than spun either way.

This is deliberately blunt arithmetic — the same spirit as report_card:
if the number looks bad here, it is bad.

Outputs
  03_scoreboard/LESSON_EFFICACY.md
  03_scoreboard/lesson_efficacy.json

CLI: python -m src.lesson_efficacy [--window 7]
"""
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from . import config, scoreboard

ROOT = Path(__file__).resolve().parent.parent
ACTIVE_DIR = ROOT / "02_lessons" / "active"
OUT_MD = ROOT / "03_scoreboard" / "LESSON_EFFICACY.md"
OUT_JSON = ROOT / "03_scoreboard" / "lesson_efficacy.json"

MIN_SIDE = 4  # graded runs needed on each side to call a verdict


def _frontmatter(text: str) -> dict[str, str]:
    m = re.match(r"^---\n(.*?)\n---", text, re.S)
    if not m:
        return {}
    out = {}
    for line in m.group(1).splitlines():
        km = re.match(r'^(\w+):\s*"?(.*?)"?\s*$', line)
        if km:
            out[km.group(1)] = km.group(2)
    return out


def _topic_of(fm: dict[str, str]) -> str | None:
    """general | sector:<Name> | None (ops/news/book — not market-graded)."""
    src = fm.get("sources", "")
    m = re.search(r"_sector_([a-z_]+)_lesson", src)
    if m:
        name = " ".join(w.capitalize() for w in m.group(1).split("_"))
        return f"sector:{name}"
    scope = (fm.get("scope") or "").strip().lower()
    if scope.startswith("sector"):
        raw = re.sub(r"^sector[:_]", "", scope)
        name = " ".join(w.capitalize() for w in re.split(r"[_\s]+", raw) if w)
        return f"sector:{name}" if name else None
    if scope in ("general", ""):
        return "general"
    return None  # ops / news / book — graded elsewhere


def _graded_runs_by_topic() -> dict[str, list[tuple[str, bool]]]:
    board = scoreboard.load()
    out: dict[str, list[tuple[str, bool]]] = {}
    for r in board.get("runs", []):
        t, d, h = r.get("topic"), r.get("date"), r.get("direction_hit")
        if not t or not d or h is None:
            continue
        out.setdefault(t, []).append((d, bool(h)))
    for t in out:
        out[t].sort()
    return out


def _rate(runs: list[tuple[str, bool]]) -> float | None:
    return round(sum(h for _, h in runs) / len(runs), 3) if runs else None


def evaluate(window: int = 7) -> dict:
    runs_by_topic = _graded_runs_by_topic()
    rows = []
    for p in sorted(ACTIVE_DIR.glob("*.md")):
        fm = _frontmatter(p.read_text(encoding="utf-8"))
        topic = _topic_of(fm)
        event = fm.get("promoted_on") or fm.get("date") or ""
        row = {
            "lesson": p.name,
            "topic": topic,
            "active_since": event,
            "error_category": fm.get("error_category", ""),
        }
        if topic is None or topic not in runs_by_topic or not event:
            row["verdict"] = "not market-graded" if topic is None else "no graded runs"
            rows.append(row)
            continue
        runs = runs_by_topic[topic]
        before = [x for x in runs if x[0] < event][-window:]
        after = [x for x in runs if x[0] > event][:window]
        row["before"] = {"hit": _rate(before), "n": len(before)}
        row["after"] = {"hit": _rate(after), "n": len(after)}
        if len(before) < MIN_SIDE or len(after) < MIN_SIDE:
            row["verdict"] = "insufficient"
        else:
            delta = row["after"]["hit"] - row["before"]["hit"]
            row["delta"] = round(delta, 3)
            row["verdict"] = ("improved" if delta > 0.05
                              else "WORSE" if delta < -0.05 else "flat")
        rows.append(row)

    judged = [r for r in rows if r.get("delta") is not None]
    summary = {
        "n_active": len(rows),
        "n_judged": len(judged),
        "improved": sum(1 for r in judged if r["verdict"] == "improved"),
        "flat": sum(1 for r in judged if r["verdict"] == "flat"),
        "worse": sum(1 for r in judged if r["verdict"] == "WORSE"),
        "mean_delta": round(sum(r["delta"] for r in judged) / len(judged), 3)
        if judged else None,
    }

    # topic-level trend (independent of lesson attribution)
    topics = []
    for t, runs in sorted(runs_by_topic.items()):
        last = runs[-10:]
        prev = runs[-20:-10]
        topics.append({
            "topic": t,
            "last10": {"hit": _rate(last), "n": len(last)},
            "prev10": {"hit": _rate(prev), "n": len(prev)},
        })

    result = {
        "generated_at": datetime.now(ZoneInfo(config.TZ)).isoformat(),
        "window": window,
        "summary": summary,
        "lessons": rows,
        "topics": topics,
    }
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(result, indent=2), encoding="utf-8")
    _write_md(result)
    return result


def _write_md(res: dict) -> None:
    s = res["summary"]
    L = [
        "# Lesson efficacy — did promoted lessons change outcomes?",
        "",
        f"_Generated {res['generated_at']}_ · window: {res['window']} graded "
        "runs each side of activation · deltas within ±5pp count as flat.",
        "",
        "This is correlation, not proof — but a lesson whose topic got WORSE "
        "after promotion has no evidence of working and is a retirement "
        "candidate for the monthly distill.",
        "",
        f"**Active lessons: {s['n_active']} · judged (enough data both sides): "
        f"{s['n_judged']} · improved: {s['improved']} · flat: {s['flat']} · "
        f"worse: {s['worse']}"
        + (f" · mean delta: {s['mean_delta']:+.3f}**" if s["mean_delta"] is not None
           else "**"),
        "",
        "## Retirement candidates (topic got worse after activation)",
        "",
    ]
    worse = [r for r in res["lessons"] if r.get("verdict") == "WORSE"]
    if worse:
        for r in worse:
            L.append(f"- `{r['lesson']}` ({r['topic']}, since {r['active_since']}): "
                     f"{r['before']['hit']:.0%} → {r['after']['hit']:.0%} "
                     f"({r['delta']:+.0%})")
    else:
        L.append("_(none)_")
    L += [
        "",
        "## Every active lesson",
        "",
        "| Lesson | Topic | Since | Before | After | Δ | Verdict |",
        "|--------|-------|-------|--------|-------|---|---------|",
    ]
    for r in res["lessons"]:
        b = r.get("before") or {}
        a = r.get("after") or {}
        bf = f"{b['hit']:.0%} (n={b['n']})" if b.get("hit") is not None else "—"
        af = f"{a['hit']:.0%} (n={a['n']})" if a.get("hit") is not None else "—"
        d = f"{r['delta']:+.0%}" if r.get("delta") is not None else "—"
        L.append(f"| `{r['lesson'][:48]}` | {r.get('topic') or '—'} | "
                 f"{r.get('active_since') or '—'} | {bf} | {af} | {d} | "
                 f"{r.get('verdict')} |")
    L += [
        "",
        "## Topic trend (last 10 vs previous 10 graded runs)",
        "",
        "| Topic | prev 10 | last 10 |",
        "|-------|---------|---------|",
    ]
    for t in res["topics"]:
        p_, l_ = t["prev10"], t["last10"]
        pf = f"{p_['hit']:.0%} (n={p_['n']})" if p_.get("hit") is not None else "—"
        lf = f"{l_['hit']:.0%} (n={l_['n']})" if l_.get("hit") is not None else "—"
        L.append(f"| {t['topic']} | {pf} | {lf} |")
    OUT_MD.write_text("\n".join(L) + "\n", encoding="utf-8")
    print(f"[lesson-efficacy] wrote {OUT_MD}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--window", type=int, default=7)
    args = ap.parse_args()
    evaluate(window=args.window)


if __name__ == "__main__":
    main()
