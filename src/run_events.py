"""Stage EVENTS (daily, runs before PREDICT): scan market-moving events
across all financially significant regions and all 11 sectors, over three
windows — past ~2 weeks (still in play), today, next ~2 weeks.

Uses the standard DeepSeek + web_search tool loop. Writes:
  01_daily/events/<date>_events.md       human-readable scan
  01_daily/events/<date>_events.json     machine-readable event list
  01_daily/events/latest.md / latest.json  stable pointers other workflows read
  01_daily/events/<date>_events_trace.md readable research trace
  01_daily/_transcripts/<date>_events.json full conversation audit

CLI: python -m src.run_events [--date YYYY-MM-DD]
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

RUBRIC_PATH = os.path.join(config.GROUNDING, "event_scanner_rubric.md")


def _windows(today) -> dict:
    return {
        "past": f"{(today - timedelta(days=14)).isoformat()}..{(today - timedelta(days=1)).isoformat()}",
        "today": today.isoformat(),
        "upcoming": f"{(today + timedelta(days=1)).isoformat()}..{(today + timedelta(days=14)).isoformat()}",
    }


def extract_json(text: str) -> dict | None:
    """Pull the fenced ```json block, else the outermost { ... } span."""
    blob = None
    m = re.search(r"```json\s*(.*?)```", text, re.S)
    if m:
        blob = m.group(1)
    else:
        i, j = text.find("{"), text.rfind("}")
        if i != -1 and j > i:
            blob = text[i:j + 1]
    if not blob:
        return None
    try:
        return json.loads(blob)
    except ValueError:
        return None


def strip_json_block(text: str) -> str:
    return re.sub(r"```json\s*.*?```\s*$", "", text, flags=re.S).rstrip()


def _previous_scan() -> str:
    path = os.path.join(EVENTS_DIR, "latest.md")
    if not os.path.exists(path):
        return ""
    try:
        with open(path, encoding="utf-8") as fh:
            prev = fh.read()
    except OSError:
        return ""
    if len(prev) > 6000:
        prev = prev[:6000] + "\n...(truncated)"
    return ("PREVIOUS SCAN (yours, from the last run — update it: mark "
            "resolved events, carry still-live ones forward, drop stale):\n\n"
            + prev)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    args = ap.parse_args()
    date_str = args.date or datetime.now(ZoneInfo(config.TZ)).date().isoformat()
    today = datetime.now(ZoneInfo(config.TZ)).date() if not args.date \
        else datetime.strptime(args.date, "%Y-%m-%d").date()
    win = _windows(today)

    if not config.DEEPSEEK_API_KEY:
        raise SystemExit("DEEPSEEK_API_KEY not set")

    with open(RUBRIC_PATH, encoding="utf-8") as fh:
        rubric = fh.read()

    user_msg = (
        f"TODAY: {date_str} (America/New_York)\n"
        f"WINDOW past:     {win['past']}\n"
        f"WINDOW today:    {win['today']}\n"
        f"WINDOW upcoming: {win['upcoming']}\n\n"
        f"{_previous_scan()}\n\n"
        "Run the full event scan now. Cover every category and every region "
        "with explicit web_search rounds before writing. End your reply with "
        "the fenced ```json block exactly as specified."
    )

    text = deepseek_client.chat(
        [{"role": "system", "content": rubric},
         {"role": "user", "content": user_msg}],
        model=config.MODEL_PREDICT, tools=True, max_tokens=8000,
        transcript_path=os.path.join("01_daily/_transcripts",
                                     f"{date_str}_events.json"),
        trace_path=os.path.join(EVENTS_DIR, f"{date_str}_events_trace.md"),
        stage_label=f"EVENTS {date_str}")

    data = extract_json(text)
    md_body = strip_json_block(text)

    os.makedirs(EVENTS_DIR, exist_ok=True)

    header = [f"# Event Scan — {date_str}", ""]
    if data:
        n = len(data.get("events", []))
        header += [
            f"- events tracked: **{n}**",
            f"- uncertainty: **{data.get('uncertainty', '?')}**",
            f"- summary: {data.get('summary', '')}".rstrip(),
            "",
        ]
    else:
        header += ["⚠️ JSON block failed to parse — raw model output below, "
                   "no machine-readable events today.", ""]

    md_path = os.path.join(EVENTS_DIR, f"{date_str}_events.md")
    with open(md_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(header) + md_body + "\n")

    json_path = os.path.join(EVENTS_DIR, f"{date_str}_events.json")
    payload = data or {"scan_date": date_str, "error": "parse_failed",
                       "events": []}
    payload.setdefault("scan_date", date_str)
    payload["windows"] = win
    with open(json_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, ensure_ascii=False)

    # stable pointers for other workflows
    for src, dst in ((md_path, "latest.md"), (json_path, "latest.json")):
        with open(src, encoding="utf-8") as fh:
            content = fh.read()
        with open(os.path.join(EVENTS_DIR, dst), "w", encoding="utf-8") as fh:
            fh.write(content)

    n = len(payload.get("events", []))
    print(f"[events] {date_str}: {n} events "
          f"({'parse OK' if data else 'PARSE FAILED'}) -> {md_path}")


if __name__ == "__main__":
    main()
