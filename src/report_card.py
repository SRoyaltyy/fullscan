"""REPORT CARD — the owner's monthly evaluation, zero LLM, zero domain
expertise required to read. Answers six questions:

Q1. Is rolling accuracy actually going anywhere, or just wobbling?
Q2. Is the engine beating naive baselines ("always up", "same as yesterday")?
Q3. When it says 70% confident, is it right ~70% of the time? (calibration)
Q4. Are the same error categories repeating after being logged as lessons?
Q5. Does the lesson pipeline count look healthy (not stuck, not exploding)?
Q6. What do the latest lessons actually say? (sniff test excerpts)

CLI: python -m src.report_card  → writes 03_scoreboard/report_card.md
"""
from __future__ import annotations

import glob
import os
import re
from datetime import datetime
from zoneinfo import ZoneInfo

from . import config, scoreboard

CATEGORY_PLAIN = {
    "A": "missing evidence", "B": "misweighted evidence",
    "C": "miscalibrated confidence", "D": "upstream data/tool failure",
    "NONE": "no error",
}


def _graded(runs: list[dict]) -> list[dict]:
    return [r for r in runs if r.get("actual_pct_change") is not None
            and r.get("direction_hit") is not None]


def _pct(x: float | None) -> str:
    return f"{x * 100:.0f}%" if x is not None else "n/a"


# ------------------------------------------------------------------ Q1
def q1_trend(graded: list[dict]) -> list[str]:
    lines = ["## Q1. Is accuracy actually improving?\n"]
    for n in (10, 30, 0):
        sub = graded[-n:] if n else graded
        if not sub:
            continue
        d = sum(1 for r in sub if r["direction_hit"]) / len(sub)
        m = sum(1 for r in sub if r.get("magnitude_hit")) / len(sub)
        tag = f"last {n}" if n else "all time"
        lines.append(f"- {tag} (n={len(sub)}): direction **{d * 100:.0f}%**, "
                     f"magnitude {m * 100:.0f}%")
    # first half vs second half — the cheapest possible trend test
    if len(graded) >= 12:
        half = len(graded) // 2
        first = sum(1 for r in graded[:half] if r["direction_hit"]) / half
        second = sum(1 for r in graded[half:] if r["direction_hit"]) / (len(graded) - half)
        verdict = ("📈 improving" if second > first + 0.05 else
                   "📉 degrading" if second < first - 0.05 else "➖ flat / wobbling")
        lines.append(f"- first half {first * 100:.0f}% vs second half "
                     f"{second * 100:.0f}% → **{verdict}**")
    else:
        lines.append("- not enough graded runs yet for a trend split (need 12+)")
    return lines


# ------------------------------------------------------------------ Q2
def q2_baselines(graded: list[dict]) -> list[str]:
    lines = ["\n## Q2. Is it beating 'just guess up' / 'guess yesterday'?\n"]
    if not graded:
        return lines + ["- no graded runs yet"]
    eng = sum(1 for r in graded if r["direction_hit"]) / len(graded)
    up = sum(1 for r in graded if r["actual_direction"] == "up") / len(graded)
    # persistence baseline: predict yesterday's actual direction
    hits = 0
    n = 0
    for prev, cur in zip(graded, graded[1:]):
        n += 1
        if cur["actual_direction"] == prev["actual_direction"]:
            hits += 1
    pers = hits / n if n else None
    lines.append(f"- engine direction accuracy: **{eng * 100:.0f}%** (n={len(graded)})")
    lines.append(f"- 'always guess UP' would score: **{up * 100:.0f}%**")
    if pers is not None:
        lines.append(f"- 'guess same as yesterday' would score: **{pers * 100:.0f}%**")
    beat = [b for b in (up, pers) if b is not None]
    if beat:
        best = max(beat)
        if eng > best + 0.05:
            lines.append(f"- ✅ engine beats the best naive baseline by "
                         f"{(eng - best) * 100:.0f} pts")
        elif eng > best:
            lines.append(f"- ⚠️ engine barely beats the best naive baseline "
                         f"(+{(eng - best) * 100:.0f} pts) — within noise")
        else:
            lines.append(f"- ❌ engine is BEHIND the best naive baseline by "
                         f"{(best - eng) * 100:.0f} pts — the AI is currently "
                         f"adding negative value vs a coin flip")
    return lines


# ------------------------------------------------------------------ Q3
def q3_calibration(graded: list[dict]) -> list[str]:
    lines = ["\n## Q3. Calibration — when it says X% confident, is it right X% of the time?\n"]
    buckets: dict[str, list[bool]] = {}
    for r in graded:
        c = r.get("confidence_score")
        if c is None:
            continue
        lo = int(c * 10) / 10
        key = f"{lo:.1f}–{lo + 0.1:.1f}"
        buckets.setdefault(key, []).append(bool(r["direction_hit"]))
    if not buckets:
        return lines + ["- no confidence scores recorded yet"]
    lines.append("| stated confidence | n | actual hit rate | honest? |")
    lines.append("|---|---|---|---|")
    for key in sorted(buckets):
        hits = buckets[key]
        actual = sum(hits) / len(hits)
        stated = (float(key[:3]) + 0.05)
        honest = "✅" if abs(actual - stated) <= 0.15 else (
            "overconfident ⚠️" if actual < stated else "underconfident")
        lines.append(f"| {key} | {len(hits)} | {actual * 100:.0f}% | {honest} |")
    return lines


# ------------------------------------------------------------------ Q4
def q4_error_categories() -> list[str]:
    lines = ["\n## Q4. Are the same mistakes repeating?\n"]
    tally: dict[str, list[str]] = {}
    for p in sorted(glob.glob(os.path.join(config.LESSONS_CANDIDATE, "*.md"))):
        try:
            head = open(p, encoding="utf-8").read(800)
        except OSError:
            continue
        m = re.search(r'error_category:\s*"(.*?)"', head)
        d = re.search(r'date:\s*"(.*?)"', head)
        cat = m.group(1) if m else "?"
        tally.setdefault(cat, []).append(d.group(1) if d else "?")
    if not tally:
        return lines + ["- no candidate lessons yet"]
    for cat, dates in sorted(tally.items()):
        plain = CATEGORY_PLAIN.get(cat, cat)
        warn = ("  ⚠️ **repeating after being logged — lessons are being "
                "written but not changing behavior**"
                if cat != "NONE" and len(dates) >= 3 else "")
        lines.append(f"- **{cat}** ({plain}): {len(dates)}× "
                     f"({', '.join(dates[-5:])}){warn}")
    return lines


# ------------------------------------------------------------------ Q5
def q5_lesson_pipeline() -> list[str]:
    lines = ["\n## Q5. Lesson pipeline health\n"]
    counts = {}
    for name, path in (("candidate", config.LESSONS_CANDIDATE),
                       ("active", config.LESSONS_ACTIVE),
                       ("archive", config.LESSONS_ARCHIVE)):
        counts[name] = len(glob.glob(os.path.join(path, "*.md")))
    lines.append(f"- candidates: {counts['candidate']} | "
                 f"active: {counts['active']} | archived: {counts['archive']}")
    if counts["candidate"] > 0 and counts["active"] == 0:
        lines.append("- ⚠️ nothing has ever been promoted — either the "
                     "promotion gate is too strict or lessons aren't generalizing")
    elif counts["active"] > 25:
        lines.append("- ⚠️ active-lesson pile is large — risk of narrow, "
                     "contradictory standing rules (overfitting); consider a cull")
    else:
        lines.append("- ✅ pipeline shape looks healthy "
                     "(target: slow growth to a stable ~10–20 active lessons)")
    return lines


# ------------------------------------------------------------------ Q6
def q6_sniff_test() -> list[str]:
    lines = ["\n## Q6. Sniff test — the 3 most recent lessons, verbatim triggers\n"]
    files = sorted(glob.glob(os.path.join(config.LESSONS_CANDIDATE, "*.md")))
    if not files:
        return lines + ["- none yet"]
    lines.append("Read these cold. Specific and falsifiable = good; "
                 "vague and unfalsifiable = warning sign.\n")
    for p in files[-3:]:
        try:
            head = open(p, encoding="utf-8").read(1200)
        except OSError:
            continue
        def field(name):
            m = re.search(rf'{name}:\s*"(.*?)"', head)
            return m.group(1) if m else "(not recorded)"
        lines.append(f"**{os.path.basename(p)}** [{field('error_category')}]")
        lines.append(f"- when: {field('trigger_pattern')}")
        lines.append(f"- do instead: {field('corrected_behavior')[:300]}")
        f = field("falsifier")
        lines.append(f"- wrong if: {f if f != '(not recorded)' else '⚠️ NO FALSIFIER RECORDED'}")
        lines.append("")
    return lines


# ------------------------------------------------------------------ extra
def q7_foreseeability(graded: list[dict]) -> list[str]:
    lines = ["\n## Q7. Hit rate on foreseeable vs shock days\n"]
    know = {"yes": [], "part": [], "no": []}
    for r in graded:
        k = (r.get("knowable_at_9am") or "").lower()
        bucket = ("yes" if k.startswith("yes") else
                  "no" if k.startswith("no") else
                  "part" if k.startswith("part") else None)
        if bucket:
            know[bucket].append(bool(r["direction_hit"]))
    if not any(know.values()):
        return lines + ["- no foreseeability grades yet (starts with the new autopsy format)"]
    for label, key in (("foreseeable at 9am", "yes"),
                       ("partially foreseeable", "part"),
                       ("genuine shock", "no")):
        hits = know[key]
        if hits:
            lines.append(f"- {label}: {sum(hits) / len(hits) * 100:.0f}% "
                         f"direction accuracy (n={len(hits)})")
    if know["no"]:
        lines.append("- misses on genuine-shock days are NOT reasoning failures; "
                     "only the first two rows measure the engine's real skill")
    return lines


def main() -> None:
    board = scoreboard.load()
    graded = _graded(board.get("runs", []))
    now = datetime.now(ZoneInfo(config.TZ)).strftime("%Y-%m-%d %H:%M %Z")

    parts = [f"# 📊 Engine Report Card — {now}\n",
             f"Graded runs on record: **{len(graded)}**. "
             "This report is pure arithmetic over the scoreboard — no LLM, "
             "no spin. If a number looks bad here, it is bad.\n"]
    for section in (q1_trend(graded), q2_baselines(graded),
                    q3_calibration(graded), q4_error_categories(),
                    q5_lesson_pipeline(), q6_sniff_test(),
                    q7_foreseeability(graded)):
        parts.extend(section)
    text = "\n".join(parts) + "\n"

    os.makedirs(os.path.dirname(config.SCOREBOARD_JSON), exist_ok=True)
    out = os.path.join(os.path.dirname(config.SCOREBOARD_JSON),
                       "report_card.md")
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(text)
    print(text)
    print(f"[report_card] written -> {out}")


if __name__ == "__main__":
    main()
