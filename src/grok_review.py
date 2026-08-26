"""One-shot Grok auditor of today's pre-open artifacts.

Mechanical `output_qc` is a regex net: timeout phrases, carry markers,
file size, contract tokens. That is how we fail — a stub can dodge the
needles, a carry can look like JSON, two sector essays can be clones.

This module hands Grok 4.6 the actual file text, once, and asks a
hostile question: is this a REAL same-day research packet, or trash?

One LLM call. No web search. Fail-closed: empty reply, timeout stub,
unparseable JSON, or any listed fail → the day is not quality-ok.

CLI:
  python -m src.grok_review --date YYYY-MM-DD
"""
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from . import config, output_qc
from .sector_taxonomy import FINVIZ_SECTORS

ROOT = Path(__file__).resolve().parent.parent
ET = ZoneInfo(config.TZ)

HEAD_CHARS = 9000
TAIL_CHARS = 4000
JSON_CHARS = 7000

SYSTEM = """You are a hostile pre-open artifact auditor for a US-market
prediction desk. You do NOT grade whether the market call is correct.
You do NOT rewrite anything. You READ the files and decide whether each
one is a REAL, SAME-DAY, COMPLETE research artifact versus trash.

Fail a file (and the day) if it is any of:
- timeout / idle-timeout / connection refused / 502 / 503 / shutdown stub
- carry-forward of a previous date (CARRIED FORWARD, carried_from, scan_date
  that is not TODAY)
- wrong date (body is clearly about some other session than TODAY)
- empty, tiny, truncated mid-sentence, or missing the contract markers
  the file type requires (MEMORY_CONFIRM / SCORES_BEGIN / SECTOR_SCORES_BEGIN
  / HIT_GRID_BEGIN / NEWS_PARSE_BEGIN as applicable)
- generic refusal ("I cannot", "as an AI", DeepSeek/OpenClaw error dump)
- copy-paste: two or more sector essays that are substantially the same
- a "predict" that never actually takes a direction

Required core: general predict, events JSON (real scan, not carry), news
judge, news parse, finviz digest, usable map-heat tables, and at least 8
of 11 sector predicts. News actions is optional.

Captain baseline/research is an enhancement. If the post-close baseline is
missing, `phase=morning_bootstrap` is an explicit safe no-signal artifact:
do NOT fail the day for that. The stock book ignores it (`s_heat=0`).
If research claims `phase=morning_refresh`, then fail it when it has fewer
than 20 cards, unsupported sentiment, timeout text, or malformed evidence.
Missing baseline/research may be noted, but must not fail otherwise valid
core artifacts.


Pass ONLY if every required file looks like a human-usable same-day packet.

Return ONLY JSON, no markdown fence, no commentary:
{"ok": true, "fails": [], "notes": "one short paragraph"}
or
{"ok": false, "fails": [{"path": "...", "reason": "..."}], "notes": "..."}
"""


def _slug(sector: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", sector.lower()).strip("_")


def _clip(text: str, limit: int = HEAD_CHARS + TAIL_CHARS) -> str:
    if text is None:
        return ""
    if len(text) <= limit:
        return text
    head, tail = HEAD_CHARS, TAIL_CHARS
    omitted = len(text) - head - tail
    return (
        text[:head]
        + f"\n\n…[{omitted} chars clipped]…\n\n"
        + text[-tail:]
    )


def _read(root: Path, rel: str, json_like: bool = False) -> tuple[str, int, bool]:
    path = root / rel
    if not path.exists():
        return "", 0, False
    try:
        raw = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return "", 0, False
    cap = JSON_CHARS if json_like else (HEAD_CHARS + TAIL_CHARS)
    return _clip(raw, cap), len(raw), True


def bundle_preopen(date: str, root: Path | None = None,
                   mechanical_report: dict | None = None) -> str:
    """Pack today's artifacts into one prompt Grok can actually read."""
    root = root or ROOT
    parts: list[str] = [
        f"TODAY (America/New_York) = {date}",
        "Read every file below. Return the JSON verdict.",
        "",
    ]
    if mechanical_report is not None:
        parts.append("MECHANICAL output_qc (regex) said:")
        parts.append(output_qc.render(mechanical_report))
        parts.append(
            "Regex is necessary but not sufficient for CORE files. "
            "You may FAIL a file the regex passed. "
            "You may NOT pass these CORE files if regex failed them: "
            "general predict, events, news judge, news parse, finviz digest, "
            "map-heat tables (empty futures tape IS a real fail). "
            "EXCEPTIONS that must not fail the day: "
            "(1) map_heat_baseline missing / regex FAIL when research.md is "
            "phase=morning_bootstrap (expected until the first 22:00 post-close "
            "job — note it, keep ok=true); "
            "(2) map_heat_research phase=morning_bootstrap; "
            "(3) a single missing sector if at least 8 of 11 sector essays are "
            "quality-ok (list it in notes; ok stays true if the rest of core "
            "is good)."
        )
        parts.append("")

    catalog = [
        ("general_predict", f"01_daily/general/{date}_predict.md", False),
        ("news_judge", f"01_daily/news/{date}_judge.md", False),
        ("events", f"01_daily/events/{date}_events.json", True),
        ("news_parse", f"01_daily/news/{date}_parsed.json", True),
        ("news_actions", f"01_daily/news/{date}_actions.json", True),
        ("finviz_digest", f"01_daily/news/{date}_finviz_digest.json", True),
        ("map_heat", f"01_daily/map_heat/{date}_map_heat.md", False),
        ("map_heat_baseline", f"01_daily/map_heat/{date}_research_baseline.md", False),
        ("map_heat_research", f"01_daily/map_heat/{date}_research.md", False),
    ]
    digest_json = root / "01_daily" / "news" / f"{date}_finviz_digest.json"
    if not digest_json.exists():
        catalog[5] = (
            "finviz_digest",
            f"01_daily/news/{date}_finviz_digest.md",
            False,
        )
    for sector in FINVIZ_SECTORS:
        catalog.append((
            f"sector:{sector}",
            f"01_daily/sectors/{date}/{_slug(sector)}_predict.md",
            False,
        ))

    for kind, rel, json_like in catalog:
        text, size, exists = _read(root, rel, json_like=json_like)
        parts.append("=" * 72)
        if not exists:
            parts.append(f"FILE {rel}  kind={kind}  MISSING")
            parts.append("")
            continue
        parts.append(f"FILE {rel}  kind={kind}  bytes={size}")
        parts.append(text)
        parts.append("")
    return "\n".join(parts)


def parse_verdict(text: str) -> dict:
    """Extract the auditor JSON. Fail-closed on garbage."""
    if not (text or "").strip():
        return {"ok": False, "fails": [{"path": "(review)",
                                        "reason": "empty_review_reply"}],
                "notes": "Grok returned an empty review"}
    if output_qc.looks_like_timeout(text):
        return {"ok": False, "fails": [{"path": "(review)",
                                        "reason": "timeout_stub"}],
                "notes": "Grok review itself was a timeout/error stub"}
    blob = text.strip()
    fence = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", blob, re.S)
    if fence:
        blob = fence.group(1)
    else:
        start, end = blob.find("{"), blob.rfind("}")
        if start >= 0 and end > start:
            blob = blob[start:end + 1]
    try:
        data = json.loads(blob)
    except (ValueError, json.JSONDecodeError):
        return {"ok": False, "fails": [{"path": "(review)",
                                        "reason": "unparseable_verdict"}],
                "notes": text[:400]}
    fails = data.get("fails") or []
    if not isinstance(fails, list):
        fails = [{"path": "(review)", "reason": "fails_not_a_list"}]
    ok = bool(data.get("ok")) and not fails
    notes = str(data.get("notes") or "")[:800]
    clean = []
    for f in fails:
        if isinstance(f, dict):
            clean.append({"path": str(f.get("path") or "")[:240],
                          "reason": str(f.get("reason") or "")[:400]})
        else:
            clean.append({"path": "(review)", "reason": str(f)[:400]})
    return {"ok": ok, "fails": clean, "notes": notes}


def prior_ok(date: str, root: Path | None = None) -> bool:
    """True when a previous Grok review already passed for this date."""
    path = (root or ROOT) / "01_daily" / f"{date}_grok_review.json"
    if not path.exists():
        return False
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return False
    return bool(data.get("ok")) and not data.get("fails")


def review_preopen(date: str, mechanical_report: dict | None = None,
                   root: Path | None = None, chat_fn=None) -> dict:
    """One Grok call. Writes 01_daily/{date}_grok_review.{json,md}."""
    root = root or ROOT
    prompt = bundle_preopen(date, root=root,
                            mechanical_report=mechanical_report)
    print(f"[grok-review] {date}: sending {len(prompt):,} chars of file "
          f"text to Grok (one call, no search)", flush=True)

    if chat_fn is None:
        from . import deepseek_client
        chat_fn = deepseek_client.chat

    try:
        raw = chat_fn(
            [
                {"role": "system", "content": SYSTEM},
                {"role": "user", "content": prompt},
            ],
            model=config.MODEL_PREDICT,
            tools=False,
            max_tokens=2000,
            temperature=0.0,
            stage_label="grok_review",
        )
    except Exception as e:  # noqa: BLE001 — fail-closed on any client error
        raw = ""
        verdict = {"ok": False,
                   "fails": [{"path": "(review)",
                              "reason": f"review_call_error:{type(e).__name__}"}],
                   "notes": str(e)[:400]}
    else:
        verdict = parse_verdict(raw or "")

    payload = {
        "date": date,
        "generated_at": datetime.now(ET).isoformat(),
        "ok": bool(verdict.get("ok")),
        "fails": verdict.get("fails") or [],
        "notes": verdict.get("notes") or "",
        "prompt_chars": len(prompt),
        "reply_chars": len(raw or ""),
        "reviewer": "grok-4.6-text",
    }
    out_dir = root / "01_daily"
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / f"{date}_grok_review.json"
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    md_lines = [
        f"# Grok text review — {date}",
        "",
        f"ok={payload['ok']}",
        "",
        payload["notes"] or "(no notes)",
        "",
    ]
    if payload["fails"]:
        md_lines.append("## Fails")
        for f in payload["fails"]:
            md_lines.append(f"- `{f.get('path')}`: {f.get('reason')}")
        md_lines.append("")
    md_path = out_dir / f"{date}_grok_review.md"
    md_path.write_text("\n".join(md_lines), encoding="utf-8")
    flag = "PASS" if payload["ok"] else "FAIL"
    print(f"[grok-review] {flag} {date}  fails={len(payload['fails'])}  "
          f"wrote {json_path}", flush=True)
    for f in payload["fails"]:
        print(f"  - {f.get('path')}: {f.get('reason')}", flush=True)
    return payload


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    args = ap.parse_args()
    date = args.date or datetime.now(ET).date().isoformat()
    report = output_qc.preopen_report(date)
    payload = review_preopen(date, mechanical_report=report)
    raise SystemExit(0 if payload.get("ok") else 1)


if __name__ == "__main__":
    main()
