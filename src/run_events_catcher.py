"""Stage EVENTS CATCHER (daily, immediately after run_events): second-pass
gap hunt. Reads the primary scan, then searches specifically for what the
primary MISSED — with deliberate emphasis on government actions (executive
orders, legislation, judicial proceedings, agency regulatory actions),
which market news systematically under-reports.

Missed events are deduplicated against the primary, tagged origin=catcher,
and MERGED into the same outputs so downstream workflows see one list:
  01_daily/events/<date>_events.{md,json}  (updated in place)
  01_daily/events/latest.{md,json}         (updated in place)
  01_daily/events/<date>_catcher_trace.md  readable research trace
  01_daily/_transcripts/<date>_events_catcher.json  full audit

If the primary scan failed (no parseable events), the catcher skips: it
exists to complement the primary, not replace it.

CLI: python -m src.run_events_catcher [--date YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from . import config, deepseek_client
from .event_context import EVENTS_DIR
from .run_events import extract_json, _windows

RUBRIC_PATH = os.path.join(config.GROUNDING, "event_catcher_rubric.md")
SEARCH_ROUNDS = 12


_MONTHS = {"jan", "feb", "mar", "apr", "may", "jun", "jul", "aug",
           "sep", "oct", "nov", "dec"}


def _tokens(s: str) -> set:
    """Comparison tokens: words only, no dates/months (titles describing the
    same event often carry different date phrasing)."""
    raw = re.findall(r"[a-z0-9]+", (s or "").lower())
    return {w for w in raw if w not in _MONTHS and not w.isdigit()}


def _is_dup(title: str, existing: list[str], threshold: float = 0.5) -> bool:
    t = _tokens(title)
    if not t:
        return False
    for e in existing:
        te = _tokens(e)
        if te and len(t & te) / len(t | te) > threshold:
            return True
    return False


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    args = ap.parse_args()
    date_str = args.date or datetime.now(ZoneInfo(config.TZ)).date().isoformat()
    today = datetime.now(ZoneInfo(config.TZ)).date() if not args.date \
        else datetime.strptime(args.date, "%Y-%m-%d").date()
    win = _windows(today)

    json_path = os.path.join(EVENTS_DIR, f"{date_str}_events.json")
    md_path = os.path.join(EVENTS_DIR, f"{date_str}_events.md")
    if not os.path.exists(json_path):
        raise SystemExit(f"[catcher] no primary scan for {date_str}; skipping")
    with open(json_path, encoding="utf-8") as fh:
        primary = json.load(fh)
    primary_events = primary.get("events", [])
    if not primary_events:
        raise SystemExit(f"[catcher] primary scan for {date_str} has no "
                         f"events (parse failed); skipping")
    with open(md_path, encoding="utf-8") as fh:
        primary_md = fh.read()

    with open(RUBRIC_PATH, encoding="utf-8") as fh:
        rubric = fh.read()

    titles = [e.get("title", "") for e in primary_events]
    title_list = "\n".join(f"- {t} ({e.get('date_or_window', '?')})"
                           for t, e in zip(titles, primary_events))

    user_msg = (
        f"TODAY: {date_str} (America/New_York)\n"
        f"WINDOW past:     {win['past']}\n"
        f"WINDOW today:    {win['today']}\n"
        f"WINDOW upcoming: {win['upcoming']}\n\n"
        f"The PRIMARY scan already found these {len(titles)} events "
        f"(do NOT repeat them):\n{title_list}\n\n"
        f"PRIMARY SCAN full text:\n{primary_md[:9000]}\n\n"
        "Now hunt ONLY for what the primary missed. Your primary mission is "
        "government actions: executive orders, legislation, judicial "
        "proceedings, agency regulatory actions — search each explicitly. "
        "Then sweep the secondary categories. End with the fenced ```json "
        "block exactly as specified."
    )

    text = deepseek_client.chat(
        [{"role": "system", "content": rubric},
         {"role": "user", "content": user_msg}],
        model=config.MODEL_PREDICT, tools=True, max_tokens=8000,
        transcript_path=os.path.join("01_daily/_transcripts",
                                     f"{date_str}_events_catcher.json"),
        trace_path=os.path.join(EVENTS_DIR, f"{date_str}_catcher_trace.md"),
        stage_label=f"EVENTS CATCHER {date_str}", max_rounds=SEARCH_ROUNDS)

    data = extract_json(text)
    missed = data.get("missed_events", []) if data else []

    # programmatic dedupe vs primary (belt and suspenders on top of prompt)
    kept, dropped = [], 0
    for e in missed:
        if _is_dup(e.get("title", ""), titles):
            dropped += 1
            continue
        e["origin"] = "catcher"
        e.setdefault("status", "new")
        kept.append(e)

    # merge into the primary outputs
    primary["events"].extend(kept)
    primary["catcher"] = {
        "ran": True,
        "found": len(kept),
        "duplicates_dropped": dropped,
        "coverage_assessment": (data or {}).get("coverage_assessment", ""),
        "biggest_gap": (data or {}).get("biggest_gap", ""),
    }
    with open(json_path, "w", encoding="utf-8") as fh:
        json.dump(primary, fh, indent=2, ensure_ascii=False)
    with open(os.path.join(EVENTS_DIR, "latest.json"), "w",
              encoding="utf-8") as fh:
        json.dump(primary, fh, indent=2, ensure_ascii=False)

    section = ["", "---", "", f"## CATCHER ADDITIONS (second pass) — {len(kept)} missed events", ""]
    if kept:
        gov = [e for e in kept
               if e.get("category") in ("government", "legislative", "judicial")]
        rest = [e for e in kept
                if e.get("category") not in ("government", "legislative", "judicial")]
        if gov:
            section.append("**Government / legislative / judicial:**")
            section.append("")
            for e in gov:
                section.append(
                    f"- **{e.get('title', '?')}** ({e.get('date_or_window', '?')}) — "
                    f"{', '.join(e.get('sectors', []))}. "
                    f"{e.get('why_it_matters', '')} "
                    f"Watch: {e.get('what_to_watch', '')}")
            section.append("")
        if rest:
            section.append("**Other misses:**")
            section.append("")
            for e in rest:
                section.append(
                    f"- **{e.get('title', '?')}** ({e.get('date_or_window', '?')}) — "
                    f"{e.get('category', '?')}; {', '.join(e.get('sectors', []))}. "
                    f"{e.get('why_it_matters', '')}")
            section.append("")
    else:
        section.append("Primary scan was comprehensive — no missed events "
                       "found." + (f" ({dropped} duplicates dropped)"
                                   if dropped else ""))
        section.append("")
    if (data or {}).get("coverage_assessment"):
        section.append(f"**Coverage assessment:** {data['coverage_assessment']}")
        section.append("")
    if (data or {}).get("biggest_gap"):
        section.append(f"**Biggest gap:** {data['biggest_gap']}")
        section.append("")

    with open(md_path, "a", encoding="utf-8") as fh:
        fh.write("\n".join(section))
    with open(md_path, encoding="utf-8") as fh:
        full_md = fh.read()
    with open(os.path.join(EVENTS_DIR, "latest.md"), "w",
              encoding="utf-8") as fh:
        fh.write(full_md)

    print(f"[catcher] {date_str}: +{len(kept)} missed events "
          f"({dropped} dups dropped) merged -> {json_path}")


if __name__ == "__main__":
    main()
