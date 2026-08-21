"""Stage EVENTS CATCHER (daily, immediately after run_events): second-pass
gap hunt. Reads the primary scan, then searches specifically for what the
primary MISSED — with deliberate emphasis on government actions (executive
orders, legislation, judicial proceedings, agency regulatory actions),
which market news systematically under-reports.

If the primary scan failed (0 events / missing file), the catcher does NOT
exit 1. It hunts as a REPLACEMENT scan and writes the event list itself.
That was the 08-16..08-19 weekday killer: parse-failed primary + SystemExit
skipped the commit, so the day had no events file at all.

Missed events are deduplicated against the primary, tagged origin=catcher,
and MERGED into the same outputs so downstream workflows see one list:
  01_daily/events/<date>_events.{md,json}  (updated in place)
  01_daily/events/latest.{md,json}         (updated in place)
  01_daily/events/<date>_catcher_trace.md  readable research trace
  01_daily/_transcripts/<date>_events_catcher.json  full audit

CLI: python -m src.run_events_catcher [--date YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime
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


def _write_md_full(md_path: str, date_str: str, events: list[dict],
                   coverage: str, gap: str) -> None:
    lines = [
        f"# Event Scan — {date_str} (catcher as primary)",
        "",
        f"- events tracked: **{len(events)}** (primary parse failed; catcher replaced it)",
        "",
        f"## CATCHER AS PRIMARY — {len(events)} events",
        "",
    ]
    gov = [e for e in events
           if e.get("category") in ("government", "legislative", "judicial")]
    rest = [e for e in events
            if e.get("category") not in ("government", "legislative", "judicial")]
    if gov:
        lines.append("**Government / legislative / judicial:**")
        lines.append("")
        for e in gov:
            lines.append(
                f"- **{e.get('title', '?')}** ({e.get('date_or_window', '?')}) — "
                f"{', '.join(e.get('sectors', []))}. "
                f"{e.get('why_it_matters', '')} "
                f"Watch: {e.get('what_to_watch', '')}")
        lines.append("")
    if rest:
        lines.append("**Other:**")
        lines.append("")
        for e in rest:
            lines.append(
                f"- **{e.get('title', '?')}** ({e.get('date_or_window', '?')}) — "
                f"{e.get('category', '?')}; {', '.join(e.get('sectors', []))}. "
                f"{e.get('why_it_matters', '')}")
        lines.append("")
    if not events:
        lines.append("Catcher replacement also found nothing parseable.")
        lines.append("")
    if coverage:
        lines.append(f"**Coverage assessment:** {coverage}")
        lines.append("")
    if gap:
        lines.append(f"**Biggest gap:** {gap}")
        lines.append("")
    with open(md_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    args = ap.parse_args()
    date_str = args.date or datetime.now(ZoneInfo(config.TZ)).date().isoformat()
    today = datetime.now(ZoneInfo(config.TZ)).date() if not args.date \
        else datetime.strptime(args.date, "%Y-%m-%d").date()
    win = _windows(today)

    os.makedirs(EVENTS_DIR, exist_ok=True)
    json_path = os.path.join(EVENTS_DIR, f"{date_str}_events.json")
    md_path = os.path.join(EVENTS_DIR, f"{date_str}_events.md")

    primary: dict = {"scan_date": date_str, "events": []}
    primary_md = ""
    if os.path.exists(json_path):
        try:
            with open(json_path, encoding="utf-8") as fh:
                primary = json.load(fh)
        except (OSError, json.JSONDecodeError) as e:
            print(f"[catcher] primary JSON unreadable ({e}); starting empty")
            primary = {"scan_date": date_str, "events": []}
    else:
        print(f"[catcher] no primary scan file for {date_str}; will write one")
    primary_events = primary.get("events") or []
    if os.path.exists(md_path):
        with open(md_path, encoding="utf-8") as fh:
            primary_md = fh.read()

    replacing = not primary_events
    if replacing:
        print("[catcher] primary empty/missing — hunting as REPLACEMENT, not gap hunt")
        titles: list[str] = []
        title_list = "(none — primary parse failed or file missing)"
        mission = (
            "The PRIMARY scan produced ZERO events (JSON parse failed or never "
            "ran). You are now the PRIMARY scan, not a gap hunt. Find ALL "
            "market-moving events in the windows. Your primary mission is "
            "government actions: executive orders, legislation, judicial "
            "proceedings, agency regulatory actions — search each explicitly. "
            "Then sweep the secondary categories. End with the fenced ```json "
            "block. Use key missed_events for the full list (the merger treats "
            "them as the day's events)."
        )
    else:
        titles = [e.get("title", "") for e in primary_events]
        title_list = "\n".join(f"- {t} ({e.get('date_or_window', '?')})"
                               for t, e in zip(titles, primary_events))
        mission = (
            "Now hunt ONLY for what the primary missed. Your primary mission is "
            "government actions: executive orders, legislation, judicial "
            "proceedings, agency regulatory actions — search each explicitly. "
            "Then sweep the secondary categories. End with the fenced ```json "
            "block exactly as specified."
        )

    with open(RUBRIC_PATH, encoding="utf-8") as fh:
        rubric = fh.read()

    user_msg = (
        f"TODAY: {date_str} (America/New_York)\n"
        f"WINDOW past:     {win['past']}\n"
        f"WINDOW today:    {win['today']}\n"
        f"WINDOW upcoming: {win['upcoming']}\n\n"
        f"The PRIMARY scan already found these {len(titles)} events "
        f"(do NOT repeat them):\n{title_list}\n\n"
        f"PRIMARY SCAN full text:\n{primary_md[:9000]}\n\n"
        f"{mission}"
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
    missed = []
    if data:
        missed = data.get("missed_events") or data.get("events") or []
    if not missed:
        print("[catcher] WARN: catcher JSON parse empty — keeping primary as-is")

    # programmatic dedupe vs primary (belt and suspenders on top of prompt)
    kept, dropped = [], 0
    for e in missed:
        if _is_dup(e.get("title", ""), titles):
            dropped += 1
            continue
        e["origin"] = "catcher"
        e.setdefault("status", "new")
        kept.append(e)

    if replacing:
        primary["events"] = kept
        primary.pop("error", None)
        primary.setdefault("scan_date", date_str)
    else:
        primary.setdefault("events", [])
        primary["events"].extend(kept)
    primary["catcher"] = {
        "ran": True,
        "replaced_primary": replacing,
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

    coverage = (data or {}).get("coverage_assessment", "")
    gap = (data or {}).get("biggest_gap", "")
    if replacing:
        _write_md_full(md_path, date_str, kept, coverage, gap)
    else:
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
        if coverage:
            section.append(f"**Coverage assessment:** {coverage}")
            section.append("")
        if gap:
            section.append(f"**Biggest gap:** {gap}")
            section.append("")
        with open(md_path, "a", encoding="utf-8") as fh:
            fh.write("\n".join(section))

    with open(md_path, encoding="utf-8") as fh:
        full_md = fh.read()
    with open(os.path.join(EVENTS_DIR, "latest.md"), "w",
              encoding="utf-8") as fh:
        fh.write(full_md)

    print(f"[catcher] {date_str}: +{len(kept)} events "
          f"(replaced_primary={replacing}, {dropped} dups dropped) -> {json_path}")


if __name__ == "__main__":
    main()
