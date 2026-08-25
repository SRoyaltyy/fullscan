"""Stage EVENTS (daily, runs before PREDICT): scan market-moving events
across all financially significant regions and all 11 sectors, over three
windows — past ~2 weeks (still in play), today, next ~2 weeks.

PRIMARY: OpenClaw / Grok (native web/X search). FALLBACK: DeepSeek +
client-side web_search. Writes:
  01_daily/events/<date>_events.md       human-readable scan
  01_daily/events/<date>_events.json     machine-readable event list
  01_daily/events/latest.md / latest.json  stable pointers other workflows read
  01_daily/events/<date>_events_trace.md readable research trace
  01_daily/_transcripts/<date>_events.json full conversation audit

Contract: the fenced ```json block is FIRST and non-negotiable. Token budget
is intentionally high (SuperGrok Heavy) so a 15–40 event list never truncates.
If Grok's JSON is still unusable, repair from Grok's own prose first (no
tools, high tokens), then DeepSeek this-stage only, then catcher.

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
SEARCH_ROUNDS = 16  # more search budget — events is the research-heavy stage
# SuperGrok Heavy: spare no expense. Full multi-region scan + 15–40 events
# JSON must finish; 8k was the truncation landmine.
EVENTS_MAX_TOKENS = 24000
REPAIR_MAX_TOKENS = 16000


def _windows(today) -> dict:
    return {
        "past": f"{(today - timedelta(days=14)).isoformat()}..{(today - timedelta(days=1)).isoformat()}",
        "today": today.isoformat(),
        "upcoming": f"{(today + timedelta(days=1)).isoformat()}..{(today + timedelta(days=14)).isoformat()}",
    }


def extract_json(text: str) -> dict | None:
    """Pull a usable events payload out of messy model output.

    Prefer the FIRST fenced ```json block (JSON-first contract). Fall back to
    last fence / raw braces. Grok sometimes still writes prose first — treat
    a truncated / unfenced / trailing-comma block as parseable when we can;
    return None when there is no events list so the caller can repair/DeepSeek.
    """
    if not text or not str(text).strip():
        return None
    candidates: list[str] = []
    # Prefer first complete fence (JSON-first contract)
    for m in re.finditer(r"```json\s*(.*?)```", text, re.S):
        candidates.append(m.group(1))
    # Open fence with no closer (truncated at end of response)
    m_open = re.search(r"```json\s*(.*)$", text, re.S)
    if m_open:
        candidates.append(m_open.group(1))
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
    # Truncation recovery: close open strings/arrays/objects aggressively
    suffixes = (
        "",
        "}",
        "]}",
        "]}]}",
        '"]}',
        '"]}]',
        '"}]}',
        '"}]}]}',
        '"]}',
        "\"}]}",
    )
    for candidate in (raw + s for s in suffixes):
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
    """Remove fenced JSON (leading or trailing) so the md narrative is prose."""
    text = re.sub(r"```json\s*.*?```", "", text, flags=re.S)
    # orphan open fence at end
    text = re.sub(r"```json\s*.*$", "", text, flags=re.S)
    return text.strip()


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


def _repair_json(report: str, prefer_openclaw: bool = True) -> dict | None:
    """No-tools second chance: reformat the report into the JSON schema.

    Prefer OpenClaw/Grok first when the gateway is up — it already did the
    research; we only need structure. DeepSeek is the backup reformatter.
    High token budget so a 30-event list never truncates on the repair pass.
    """
    print("[events] JSON parse failed/empty — attempting repair pass "
          f"(prefer_openclaw={prefer_openclaw})")
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
        "Emit ONLY the fenced JSON. No prose before or after.\n\n"
        "REPORT:\n\n" + report[:18000]
    )
    messages = [{"role": "user", "content": prompt}]

    # 1) Try Grok/OpenClaw first (structure-only, no tools) — keeps research
    if prefer_openclaw and deepseek_client.openclaw_available():
        out = deepseek_client.chat(
            messages, model=config.MODEL_PREDICT, tools=False,
            max_tokens=REPAIR_MAX_TOKENS,
            stage_label="EVENTS REPAIR OPENCLAW")
        data = extract_json(out)
        if data and data.get("events"):
            print(f"[events] repair via OpenClaw recovered "
                  f"{len(data['events'])} events")
            return data
        print("[events] OpenClaw repair empty — trying DeepSeek repair")

    # 2) DeepSeek structure-only
    out = deepseek_client.chat(
        messages, model=config.MODEL_PREDICT, tools=False,
        max_tokens=REPAIR_MAX_TOKENS, force_deepseek=True,
        stage_label="EVENTS REPAIR DEEPSEEK")
    data = extract_json(out)
    if data and data.get("events"):
        print(f"[events] repair via DeepSeek recovered "
              f"{len(data['events'])} events")
        return data
    return None


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
        "with explicit web_search rounds before writing.\n\n"
        "OUTPUT ORDER (non-negotiable):\n"
        "1. Emit the fenced ```json block FIRST — this is the contract.\n"
        "2. Then the human-readable report.\n"
        "3. RESEARCH APPENDIX last if you include one.\n"
        "If you run out of space, truncate the prose / appendix, NEVER the JSON. "
        "A long essay with truncated or missing JSON is a failed scan."
    )

    provider_used = "openclaw"
    text = deepseek_client.chat(
        [{"role": "system", "content": rubric},
         {"role": "user", "content": user_msg}],
        model=config.MODEL_PREDICT, tools=True, max_tokens=EVENTS_MAX_TOKENS,
        transcript_path=os.path.join("01_daily/_transcripts",
                                     f"{date_str}_events.json"),
        trace_path=os.path.join(EVENTS_DIR, f"{date_str}_events_trace.md"),
        stage_label=f"EVENTS {date_str}", max_rounds=SEARCH_ROUNDS)

    data = extract_json(text)
    n0 = len((data or {}).get("events") or [])
    print(f"[events] primary OpenClaw parse: "
          f"{'OK n=' + str(n0) if n0 else 'FAIL'} "
          f"(chars={len(text or '')})")

    repaired = False
    source = "openclaw"

    if not data or not data.get("events"):
        # Prefer cheap structure-only repair from Grok's own prose before a
        # full DeepSeek rescan (Grok research is already paid for).
        fixed = _repair_json(text or "", prefer_openclaw=True)
        if fixed and fixed.get("events"):
            data, repaired, source = fixed, True, "repair"
        else:
            # Full DeepSeek this stage only; leave gateway up for later stages
            print("[events] OpenClaw JSON unusable after repair — "
                  "DeepSeek full rescan for this stage only (gateway stays up)")
            provider_used = "deepseek"
            ds = deepseek_client.chat(
                [{"role": "system", "content": rubric},
                 {"role": "user", "content": user_msg}],
                model=config.MODEL_PREDICT, tools=True,
                max_tokens=EVENTS_MAX_TOKENS,
                transcript_path=os.path.join(
                    "01_daily/_transcripts", f"{date_str}_events.json"),
                trace_path=os.path.join(
                    EVENTS_DIR, f"{date_str}_events_trace.md"),
                stage_label=f"EVENTS {date_str} DEEPSEEK",
                max_rounds=SEARCH_ROUNDS, force_deepseek=True)
            ds_data = extract_json(ds)
            n1 = len((ds_data or {}).get("events") or [])
            print(f"[events] DeepSeek rescan parse: "
                  f"{'OK n=' + str(n1) if n1 else 'FAIL'} "
                  f"(chars={len(ds or '')})")
            if ds_data and ds_data.get("events"):
                text, data, source = ds, ds_data, "deepseek"
            elif ds:
                text = ds
                data = ds_data
                # last chance: repair DeepSeek prose
                fixed2 = _repair_json(ds, prefer_openclaw=False)
                if fixed2 and fixed2.get("events"):
                    data, repaired, source = fixed2, True, "repair-deepseek"

    os.makedirs(EVENTS_DIR, exist_ok=True)
    json_path = os.path.join(EVENTS_DIR, f"{date_str}_events.json")

    payload = (data if data and data.get("events") else
               {"scan_date": date_str, "error": "parse_failed", "events": []})
    payload.setdefault("scan_date", date_str)
    payload["windows"] = win
    payload["repaired"] = repaired
    payload["provider"] = source
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

    md_body = strip_model_markup(strip_json_block(text or ""))

    header = [f"# Event Scan — {date_str}", ""]
    if events:
        header += [
            f"- events tracked: **{len(events)}**"
            + (f" (via {source}" + (", repaired" if repaired else "") + ")")
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
          f", source={source}"
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
