"""Stage EVENTS (daily, runs before PREDICT): scan market-moving events
across all financially significant regions and all 11 sectors, over three
windows — past ~2 weeks (still in play), today, next ~2 weeks.

Uses the standard DeepSeek + web_search tool loop. Writes:
  01_daily/events/<date>_events.md       human-readable scan
  01_daily/events/<date>_events.json     machine-readable event list
  01_daily/events/latest.md / latest.json  stable pointers other workflows read
  01_daily/events/<date>_events_trace.md readable research trace
  01_daily/_transcripts/<date>_events.json full conversation audit

If the model's JSON block fails to parse (or comes back empty), a no-tools
REPAIR pass re-asks for just the JSON from the report it already wrote, so
a formatting slip never costs a whole day's scan.

CLI: python -m src.run_events [--date YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from . import config, deepseek_client, output_qc, preopen
from .event_context import EVENTS_DIR

RUBRIC_PATH = os.path.join(config.GROUNDING, "event_scanner_rubric.md")
SEARCH_ROUNDS = 14  # events stage needs more search budget than predict


def _windows(today) -> dict:
    return {
        "past": f"{(today - timedelta(days=14)).isoformat()}..{(today - timedelta(days=1)).isoformat()}",
        "today": today.isoformat(),
        "upcoming": f"{(today + timedelta(days=1)).isoformat()}..{(today + timedelta(days=14)).isoformat()}",
    }


def extract_json(text: str) -> dict | None:
    """Pull a usable events payload out of messy model output.

    Grok often writes 18 minutes of prose and then a truncated / unfenced
    / trailing-comma JSON block. Treat that as parseable when we can;
    return None when there is no events list so the caller can DeepSeek.
    """
    if not text or not str(text).strip():
        return None
    candidates: list[str] = []
    m = re.search(r"```json\s*(.*?)```", text, re.S)
    if m:
        candidates.append(m.group(1))
    m2 = re.search(r"```json\s*(.*)$", text, re.S)
    if m2:
        candidates.append(m2.group(1))
    i, j = text.find("{"), text.rfind("}")
    if i != -1 and j > i:
        candidates.append(text[i:j + 1])
    seen: set[str] = set()
    parsed_any: dict | None = None
    for blob in candidates:
        key = blob[:200]
        if key in seen:
            continue
        seen.add(key)
        data = _loads_lenient(blob)
        if not isinstance(data, dict):
            continue
        if data.get("events") or data.get("missed_events"):
            return data
        if parsed_any is None:
            parsed_any = data
    return parsed_any


def _loads_lenient(blob: str) -> dict | None:
    raw = (blob or "").strip()
    if not raw:
        return None
    raw = (raw.replace("\u201c", '"').replace("\u201d", '"')
           .replace("\u2018", "'").replace("\u2019", "'"))
    raw = re.sub(r",\s*([}\]])", r"\1", raw)
    for candidate in (raw, raw + "}", raw + "]}", raw + "]}]}"):
        try:
            obj = json.loads(candidate)
            return obj if isinstance(obj, dict) else None
        except ValueError:
            pass
        try:
            obj, _ = json.JSONDecoder().raw_decode(candidate)
            return obj if isinstance(obj, dict) else None
        except ValueError:
            continue
    return None


def strip_json_block(text: str) -> str:
    return re.sub(r"```json\s*.*?```\s*$", "", text, flags=re.S).rstrip()


def strip_model_markup(text: str) -> str:
    """Remove leaked DeepSeek DSML tool-call markup (full-width OR half-width
    vertical bars) that sometimes ends up in the prose body."""
    text = re.sub(r"<[｜|][｜|]DSML[｜|][｜|]tool_calls>.*?</[｜|][｜|]DSML[｜|][｜|]tool_calls>",
                  "", text, flags=re.S)
    text = re.sub(r"</?[｜|][｜|]DSML[｜|][｜|][^>]*>", "", text)
    return re.sub(r"\n{3,}", "\n\n", text).strip()


def _event_key(ev: dict) -> str:
    return re.sub(r"[^a-z0-9]+", " ",
                  str(ev.get("title", "")).lower()).strip()


def merge_with_existing(json_path: str, payload: dict) -> dict:
    """Same-day re-runs must NEVER shrink the event list: union events from
    the existing file with the new run, keyed by normalized title (new run
    wins on duplicates). Rescues the list even if this run's parse failed.

    Carry-forwards are NOT a real prior scan — do not union them back in.
    """
    try:
        with open(json_path, encoding="utf-8") as fh:
            old = json.load(fh)
    except (OSError, ValueError):
        return payload
    if old.get("carried_from"):
        return payload
    old_events = [e for e in (old.get("events") or [])
                  if e.get("status") != "carried"]
    new_events = payload.get("events") or []
    if not old_events:
        return payload
    merged: dict[str, dict] = {}
    for ev in old_events:
        k = _event_key(ev)
        if k:
            merged[k] = ev
    for ev in new_events:
        k = _event_key(ev)
        if k:
            merged[k] = ev  # newer run overwrites same-title duplicates
    if len(merged) <= len(new_events):
        return payload
    out = dict(payload)
    out["events"] = list(merged.values())
    out["merged_with_prior_run"] = True
    print(f"[events] merged with earlier same-day scan: "
          f"{len(new_events)} new + {len(old_events)} prior -> "
          f"{len(merged)} unique")
    return out


def events_section(events: list[dict]) -> str:
    """Human-readable numbered list of every tracked event — always rendered
    from the JSON, so pass-1 events stay visible even if the prose is thin."""
    if not events:
        return ""
    lines = ["", f"## Events tracked ({len(events)})", ""]
    for i, ev in enumerate(events, 1):
        origin = ev.get("origin", "")
        tag = f"  *({origin})*" if origin else ""
        lines.append(f"{i}. **{ev.get('title', '(untitled)')}**{tag}")
        lines.append(
            f"   - {ev.get('category', '?')} | {ev.get('timing', '?')} | "
            f"{ev.get('date_or_window', '?')} | impact "
            f"{ev.get('impact', '?')}/5 | {ev.get('expected_direction', '?')}")
        regions = ", ".join(ev.get("regions") or [])
        sectors = ", ".join(ev.get("sectors") or [])
        if regions or sectors:
            lines.append(f"   - regions: {regions or '—'} | sectors: "
                         f"{sectors or '—'}")
        why = (ev.get("why_it_matters") or "").strip()
        if why:
            lines.append(f"   - {why}")
    return "\n".join(lines)


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


def _repair_json(report: str) -> dict | None:
    """No-tools second chance: reformat the report into the JSON schema."""
    print("[events] JSON parse failed/empty — attempting repair pass")
    prompt = (
        "Below is an event scan report. Re-express it as ONE fenced "
        "```json block and NOTHING else, using exactly this schema:\n"
        '{"scan_date": "YYYY-MM-DD", "events": [{"title": str, '
        '"category": "government|legislative|judicial|macro_data|earnings|'
        'ipo|geopolitical|ongoing", "timing": "past|today|upcoming", '
        '"date_or_window": str, "regions": [str], "sectors": [str], '
        '"expected_direction": "bullish|bearish|mixed|unclear", '
        '"impact": 1-5, "confidence": "low|medium|high", '
        '"why_it_matters": str, "what_to_watch": str, '
        '"status": "new|carried|resolved", "sources": [str]}], '
        '"top_risks": [str], "top_opportunities": [str], '
        '"uncertainty": "low|moderate|elevated|high", "summary": str}\n\n'
        "REPORT:\n\n" + report[:14000]
    )
    out = deepseek_client.chat(
        [{"role": "user", "content": prompt}],
        model=config.MODEL_PREDICT, tools=False, max_tokens=6000,
        force_deepseek=True, stage_label=f"EVENTS REPAIR")
    return extract_json(out)


def _one_scan(date_str: str, win: dict) -> None:
    """One LLM scan + write. Caller owns skip/retry/QC."""
    with open(RUBRIC_PATH, encoding="utf-8") as fh:
        rubric = fh.read()

    user_msg = (
        f"TODAY: {date_str} (America/New_York)\n"
        f"WINDOW past:     {win['past']}\n"
        f"WINDOW today:    {win['today']}\n"
        f"WINDOW upcoming: {win['upcoming']}\n\n"
        f"{_previous_scan()}\n\n"
        "Run the full event scan now. Cover every category and every region "
        "with explicit web_search rounds before writing. Emit the fenced "
        "```json block FIRST (the contract), then the human-readable report. "
        "If you run out of space, truncate the prose, never the JSON."
    )

    text = deepseek_client.chat(
        [{"role": "system", "content": rubric},
         {"role": "user", "content": user_msg}],
        model=config.MODEL_PREDICT, tools=True, max_tokens=8000,
        transcript_path=os.path.join("01_daily/_transcripts",
                                     f"{date_str}_events.json"),
        trace_path=os.path.join(EVENTS_DIR, f"{date_str}_events_trace.md"),
        stage_label=f"EVENTS {date_str}", max_rounds=SEARCH_ROUNDS)

    data = extract_json(text)
    if not data or not data.get("events"):
        # Grok often returns a long essay that is NOT a timeout stub, so
        # chat() will not fall through on its own. DeepSeek this stage only;
        # leave the gateway up for judge / predict / sectors.
        print("[events] OpenClaw JSON unusable — DeepSeek for this stage only "
              "(gateway stays up)")
        ds = deepseek_client.chat(
            [{"role": "system", "content": rubric},
             {"role": "user", "content": user_msg}],
            model=config.MODEL_PREDICT, tools=True, max_tokens=8000,
            transcript_path=os.path.join("01_daily/_transcripts",
                                         f"{date_str}_events.json"),
            trace_path=os.path.join(EVENTS_DIR, f"{date_str}_events_trace.md"),
            stage_label=f"EVENTS {date_str} DEEPSEEK",
            max_rounds=SEARCH_ROUNDS, force_deepseek=True)
        ds_data = extract_json(ds)
        if ds_data and ds_data.get("events"):
            text, data = ds, ds_data
        elif ds:
            text = ds
            data = ds_data

    repaired = False
    if not data or not data.get("events"):
        fixed = _repair_json(text)
        if fixed and fixed.get("events"):
            data, repaired = fixed, True

    os.makedirs(EVENTS_DIR, exist_ok=True)
    json_path = os.path.join(EVENTS_DIR, f"{date_str}_events.json")

    payload = (data if data and data.get("events") else
               {"scan_date": date_str, "error": "parse_failed", "events": []})
    payload.setdefault("scan_date", date_str)
    payload["windows"] = win
    payload["repaired"] = repaired
    # Never let a same-day re-run clobber a richer earlier scan — unless
    # the earlier scan is a carry-forward, which we already threw out.
    payload = merge_with_existing(json_path, payload)
    events = payload.get("events", [])
    if events:
        payload.pop("error", None)
    # Drop carried leftovers if a real scan produced anything.
    if events and any(e.get("status") != "carried" for e in events):
        payload.pop("carried_from", None)

    with open(json_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, ensure_ascii=False)

    md_body = strip_model_markup(strip_json_block(text))

    header = [f"# Event Scan — {date_str}", ""]
    if events:
        header += [
            f"- events tracked: **{len(events)}**"
            + (" (via repair pass)" if repaired else "")
            + (" (merged with earlier same-day scan)"
               if payload.get("merged_with_prior_run") else ""),
            f"- uncertainty: **{payload.get('uncertainty', '?')}**",
            f"- summary: {payload.get('summary', '')}".rstrip(),
            "",
        ]
    else:
        header += ["⚠️ JSON block failed to parse even after the repair "
                   "pass — raw model output below, no machine-readable "
                   "events today.", ""]

    md_path = os.path.join(EVENTS_DIR, f"{date_str}_events.md")
    with open(md_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(header))
        fh.write(events_section(events))
        fh.write("\n\n---\n\n## Model narrative\n\n")
        fh.write(md_body + "\n")

    for src, dst in ((md_path, "latest.md"), (json_path, "latest.json")):
        with open(src, encoding="utf-8") as fh:
            content = fh.read()
        with open(os.path.join(EVENTS_DIR, dst), "w", encoding="utf-8") as fh:
            fh.write(content)

    n = len(payload.get("events", []))
    print(f"[events] {date_str}: {n} events "
          f"({'parse OK' if n else 'PARSE FAILED'}"
          f"{', repaired' if repaired else ''}) -> {md_path}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--retries", type=int, default=1)
    args = ap.parse_args()
    date_str = args.date or datetime.now(ZoneInfo(config.TZ)).date().isoformat()
    today = datetime.now(ZoneInfo(config.TZ)).date() if not args.date \
        else datetime.strptime(args.date, "%Y-%m-%d").date()
    win = _windows(today)

    config.require_llm()

    existing = output_qc.qc_events_date(date_str)
    if existing.ok and not args.force:
        print(f"[events] {date_str}: skip, quality-ok already on disk "
              f"({existing.size} chars)")
        return
    if preopen.past_predict_cutoff() and not args.force:
        print(f"[events] {date_str}: past 09:25 ET with no quality-ok scan "
              f"(existing={existing.reason or 'missing'}) — not rewriting")
        return
    if existing.carried or existing.empty or existing.timeout:
        print(f"[events] {date_str}: existing file rejected "
              f"({existing.reason}) — throwing out and rerunning")
        output_qc.reject_events(date_str)

    preopen.refuse_if_late("events", force=args.force)

    last_qc = existing
    for attempt in range(args.retries + 1):
        if attempt and preopen.past_predict_cutoff() and not args.force:
            print("[events] past 09:25 ET, not retrying")
            break
        _one_scan(date_str, win)
        last_qc = output_qc.qc_events_date(date_str)
        if last_qc.ok:
            print(f"[events] {date_str}: file QC OK after attempt {attempt + 1}")
            return
        print(f"[events] {date_str}: attempt {attempt + 1} QC FAIL "
              f"({last_qc.reason}) — throwing out")
        output_qc.reject_events(date_str)

    print(f"[events] {date_str}: FAIL-CLOSED ({getattr(last_qc, 'reason', '')}) "
          f"— catcher may still replace")
    # Exit 0 so the catcher step still runs as replacement.


if __name__ == "__main__":
    main()
