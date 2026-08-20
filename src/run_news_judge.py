"""LLM news priority & framework judge.

Runs AFTER mechanical src.news_parse. Reads the day's *_parsed.json,
asks the model to rank IMPORTANT NEWS first (before analysis), score on
the framework dimensions, rescue false negatives from the noise sample,
and emit a short B1_INJECT block for general/sector predictors.

Outputs:
  01_daily/news/<date>_judge.md   full narrative + machine block
  01_daily/news/<date>_judge.json parsed machine fields + inject text
  01_daily/news/latest_judge.md   copy for easy inject by predict

CLI: python -m src.run_news_judge [--date YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import re
import shutil
from datetime import datetime
from zoneinfo import ZoneInfo

from . import config, deepseek_client, memory

NEWS_DIR = "01_daily/news"
PROMPT_PATH = os.path.join(config.GROUNDING, "news_parse_prompt.md")


def _read(path: str) -> str:
    try:
        with open(path, encoding="utf-8") as fh:
            return fh.read()
    except OSError:
        return ""


def _latest_parsed_date() -> str | None:
    paths = sorted(glob.glob(os.path.join(NEWS_DIR, "*_parsed.json")))
    if not paths:
        return None
    base = os.path.basename(paths[-1])
    m = re.match(r"(\d{4}-\d{2}-\d{2})_parsed\.json$", base)
    return m.group(1) if m else None


def _load_parsed(date_str: str) -> dict:
    path = os.path.join(NEWS_DIR, f"{date_str}_parsed.json")
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def _fmt_items(items: list[dict], limit: int = 40) -> str:
    lines = []
    for it in (items or [])[:limit]:
        pol = it.get("polarity", "?")
        src = it.get("source", "")
        title = it.get("title", "")
        macros = ",".join(it.get("macro_themes") or []) or "-"
        secs = ",".join(it.get("sectors") or []) or "-"
        lines.append(
            f"- [{pol}] {title}  (src={src}; macro={macros}; sector={secs}; "
            f"class={it.get('class', '?')})"
        )
    return "\n".join(lines) or "(empty)"


def _noise_and_single(report: dict) -> tuple[list[dict], list[dict]]:
    """Prefer full all_items buckets when present; fall back to samples."""
    all_items = report.get("all_items") or []
    if all_items:
        noise = [x for x in all_items if x.get("class") == "noise"]
        single = [x for x in all_items if x.get("class") == "single_name"]
        return noise, single
    return list(report.get("noise_sample") or []), list(
        report.get("single_name_top") or []
    )


def _build_user_msg(date_str: str, report: dict) -> str:
    usable = report.get("usable_top") or [
        x for x in (report.get("all_items") or []) if x.get("usable")
    ]
    noise, single = _noise_and_single(report)
    # Give the model a larger noise window so rescue is possible
    noise_show = noise[:60]
    single_show = single[:25]

    grades_path = os.path.join("03_scoreboard", "news_actions_report.md")
    grades_snip = _read(grades_path)[:2500] if os.path.exists(grades_path) else "(none yet)"

    return (
        f"TODAY: {date_str}\n\n"
        f"=== MECHANICAL PARSE SUMMARY ===\n"
        f"raw={report.get('raw_count')} usable={report.get('usable_count')} "
        f"single_name={report.get('single_name_count')} "
        f"noise_dropped={report.get('noise_count')}\n"
        f"usable polarity={report.get('polarity_usable')}\n\n"
        f"=== USABLE SET (mechanical) ===\n{_fmt_items(usable, 40)}\n\n"
        f"=== SINGLE-NAME SIDE BUCKET ===\n{_fmt_items(single_show, 25)}\n\n"
        f"=== NOISE SAMPLE (scan for false negatives / gold) ===\n"
        f"{_fmt_items(noise_show, 60)}\n\n"
        f"=== STANDING ACTIVE LESSONS ===\n{memory.active_lessons()}\n\n"
        f"=== RECENT NEWS_ACTIONS GRADES (optional context) ===\n"
        f"{grades_snip}\n\n"
        "Execute the judge. Rank IMPORTANT NEWS first, then framework-score,\n"
        "then reclassify audit, then B1_INJECT. End with NEWS_PARSE_BEGIN block."
    )


def _parse_machine_block(text: str) -> dict:
    m = re.search(r"NEWS_PARSE_BEGIN(.*?)NEWS_PARSE_END", text, re.S)
    if not m:
        return {"parse_ok": False, "raw_block": ""}
    block = m.group(1)
    out: dict = {"parse_ok": True, "raw_block": block.strip()}

    def _kv(key: str) -> str:
        mm = re.search(rf"^{key}:\s*(.+)$", block, re.M)
        return (mm.group(1).strip() if mm else "")

    out["important_count"] = _kv("IMPORTANT_COUNT")
    out["interactions"] = _kv("INTERACTIONS")
    out["rescued_from_noise"] = _kv("RESCUED_FROM_NOISE")
    out["dropped_from_usable"] = _kv("DROPPED_FROM_USABLE")

    tops = []
    in_tops = False
    inject_lines = []
    in_inject = False
    for line in block.splitlines():
        s = line.strip()
        if s.startswith("TOP_ITEMS:"):
            in_tops = True
            in_inject = False
            continue
        if s.startswith("INTERACTIONS:") or s.startswith("RESCUED_FROM_NOISE:") or \
                s.startswith("DROPPED_FROM_USABLE:"):
            in_tops = False
            in_inject = False
            continue
        if s.startswith("B1_INJECT:"):
            in_tops = False
            in_inject = True
            rest = s[len("B1_INJECT:"):].strip()
            if rest:
                inject_lines.append(rest)
            continue
        if in_tops and s.startswith("-"):
            tops.append(s.lstrip("- ").strip())
        elif in_inject and s:
            inject_lines.append(s)
    out["top_items"] = tops
    out["b1_inject"] = "\n".join(inject_lines).strip()
    return out


def inject_block(date_str: str | None = None, max_chars: int = 3500) -> str:
    """Helper for run_predict / sector predict: return latest judge inject."""
    path = os.path.join(NEWS_DIR, "latest_judge.md")
    if date_str:
        candidate = os.path.join(NEWS_DIR, f"{date_str}_judge.md")
        if os.path.exists(candidate):
            path = candidate
    if not os.path.exists(path):
        return ""
    text = _read(path)
    parsed = _parse_machine_block(text)
    inject = parsed.get("b1_inject") or ""
    if not inject:
        # fall back to a truncated narrative if machine block missing
        inject = text[:max_chars]
    if len(inject) > max_chars:
        inject = inject[:max_chars] + "\n...(truncated)"
    return (
        "=== NEWS JUDGE (ranked market drivers — use for B1 / sector S1; "
        "supersedes raw headline dump when present) ===\n"
        f"{inject}\n"
        "=== END NEWS JUDGE ===\n"
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None,
                    help="YYYY-MM-DD of mechanical parse to judge")
    args = ap.parse_args()

    if not config.DEEPSEEK_API_KEY:
        raise SystemExit("DEEPSEEK_API_KEY not set")

    date_str = args.date or _latest_parsed_date()
    if not date_str:
        raise SystemExit(
            "[news_judge] no *_parsed.json found — run src.news_parse first"
        )

    parsed_path = os.path.join(NEWS_DIR, f"{date_str}_parsed.json")
    if not os.path.exists(parsed_path):
        raise SystemExit(f"[news_judge] missing {parsed_path}")

    report = _load_parsed(date_str)
    prompt = _read(PROMPT_PATH)
    if not prompt.strip():
        raise SystemExit(f"[news_judge] missing prompt at {PROMPT_PATH}")

    user_msg = _build_user_msg(date_str, report)
    text = deepseek_client.chat(
        [{"role": "system", "content": prompt},
         {"role": "user", "content": user_msg}],
        model=config.MODEL_PREDICT,
        tools=False,
        max_tokens=6000,
        transcript_path=os.path.join(
            "01_daily/_transcripts", f"{date_str}_news_judge.json"),
        trace_path=os.path.join(NEWS_DIR, f"{date_str}_judge_trace.md"),
        stage_label=f"NEWS_JUDGE {date_str}",
    )
    if not (text or "").strip():
        raise SystemExit(f"[news_judge] {date_str}: empty model response")

    machine = _parse_machine_block(text)
    os.makedirs(NEWS_DIR, exist_ok=True)
    md_path = os.path.join(NEWS_DIR, f"{date_str}_judge.md")
    with open(md_path, "w", encoding="utf-8") as fh:
        fh.write(f"# News Judge — {date_str}\n\n")
        fh.write(
            f"_Mechanical: usable={report.get('usable_count')} "
            f"single={report.get('single_name_count')} "
            f"noise={report.get('noise_count')}_\n\n"
        )
        if machine.get("b1_inject"):
            fh.write("> ## B1_INJECT (for predictors)\n>\n")
            for line in machine["b1_inject"].splitlines():
                fh.write(f"> {line}\n")
            fh.write("\n---\n\n")
        fh.write(text)
        if not text.endswith("\n"):
            fh.write("\n")

    json_path = os.path.join(NEWS_DIR, f"{date_str}_judge.json")
    payload = {
        "date": date_str,
        "generated_at": datetime.now(ZoneInfo(config.TZ)).isoformat(),
        "mechanical": {
            "raw_count": report.get("raw_count"),
            "usable_count": report.get("usable_count"),
            "single_name_count": report.get("single_name_count"),
            "noise_count": report.get("noise_count"),
        },
        **machine,
    }
    with open(json_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, ensure_ascii=False)

    # Always refresh latest pointer for predict inject
    latest = os.path.join(NEWS_DIR, "latest_judge.md")
    try:
        shutil.copyfile(md_path, latest)
    except OSError as e:
        print(f"[news_judge] latest copy failed: {e}")

    print(
        f"[news_judge] {date_str}: important={machine.get('important_count')} "
        f"parse_ok={machine.get('parse_ok')} "
        f"rescued={machine.get('rescued_from_noise', '')[:80]!r}"
    )
    print(f"[news_judge] {md_path}")
    if machine.get("b1_inject"):
        print("--- B1_INJECT ---")
        print(machine["b1_inject"])


if __name__ == "__main__":
    main()
