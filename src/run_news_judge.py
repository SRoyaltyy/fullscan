"""LLM news priority & framework judge.

Runs AFTER mechanical src.news_parse. Reads the day's *_parsed.json,
asks the model to rank IMPORTANT NEWS first (before analysis), score on
the framework dimensions, rescue false negatives from the noise sample,
and emit a short B1_INJECT block for general/sector predictors.
"""
from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from . import config, deepseek_client, memory
try:
    from .finviz_digest import inject_block as finviz_digest_block
except Exception:  # module may not exist on very old checkouts
    def finviz_digest_block(*_a, **_k):
        return ""

NEWS_DIR = "01_daily/news"
PROMPT_PATH = os.path.join(config.GROUNDING, "news_parse_prompt.md")


def _read(path: str) -> str:
    try:
        return Path(path).read_text(encoding="utf-8")
    except Exception:
        return ""


def _latest_parsed_date() -> str | None:
    files = sorted(Path(NEWS_DIR).glob("*_parsed.json"))
    if not files:
        return None
    return files[-1].name.replace("_parsed.json", "")


def _load_parsed(date_str: str) -> dict:
    p = Path(NEWS_DIR) / f"{date_str}_parsed.json"
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))


def _fmt_items(items: list[dict], limit: int = 40) -> str:
    lines = []
    for it in (items or [])[:limit]:
        t = (it.get("title") or "")[:180]
        pol = it.get("polarity") or ""
        src = it.get("source") or ""
        lines.append(f"- [{pol}] ({src}) {t}")
    return "\n".join(lines) if lines else "(none)"


def _noise_and_single(report: dict) -> tuple[list[dict], list[dict]]:
    noise = report.get("noise_sample") or report.get("noise") or []
    single = report.get("single_name") or report.get("single_name_items") or []
    return noise, single


def _build_user_msg(date_str: str, report: dict) -> str:
    usable = report.get("usable_top") or [
        x for x in (report.get("all_items") or []) if x.get("usable")
    ]
    noise, single = _noise_and_single(report)
    noise_show = noise[:60]
    single_show = single[:25]

    grades_path = os.path.join("03_scoreboard", "news_actions_report.md")
    grades_snip = _read(grades_path)[:2500] if os.path.exists(grades_path) else "(none yet)"

    fv = finviz_digest_block(date_str) or finviz_digest_block()
    fv_section = f"{fv}\n" if fv else ""

    return (
        f"TODAY: {date_str}\n\n"
        f"{fv_section}"
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
        "then reclassify audit, then B1_INJECT. End with NEWS_PARSE_BEGIN block.\n"
        "When FINVIZ DAILY DIGEST is present, treat its index narratives and\n"
        "high-signal ticker digests as elevated, pre-validated themes — prefer\n"
        "them over raw mechanical headlines when they conflict or when the\n"
        "mechanical set is thin."
    )


def _parse_machine_block(text: str) -> dict:
    out = {"b1_inject": "", "important": [], "raw": text}
    for line in text.splitlines():
        s = line.strip()
        if s.startswith("B1_INJECT:"):
            out["b1_inject"] = s[len("B1_INJECT:"):].strip()
    return out


def inject_block(date_str: str | None = None, max_chars: int = 3500) -> str:
    """Return the B1_INJECT / judge summary for predictors."""
    if not date_str:
        date_str = _latest_parsed_date() or datetime.now(ZoneInfo(config.TZ)).date().isoformat()
    path = Path(NEWS_DIR) / f"{date_str}_judge.md"
    if not path.exists():
        path = Path(NEWS_DIR) / "latest_judge.md"
    if not path.exists():
        return ""
    text = path.read_text(encoding="utf-8")
    if len(text) > max_chars:
        text = text[:max_chars] + "\n...(truncated)"
    return (
        "=== NEWS JUDGE (ranked + B1_INJECT) ===\n"
        f"{text}\n"
        "=== END NEWS JUDGE ===\n"
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    args = ap.parse_args()
    date_str = args.date or _latest_parsed_date() or datetime.now(ZoneInfo(config.TZ)).date().isoformat()

    config.require_llm(getattr(config, "MODEL_JUDGE", config.MODEL_PREDICT))

    report = _load_parsed(date_str)
    if not report:
        print(f"[news_judge] no parsed report for {date_str}")
        return

    system = _read(PROMPT_PATH) or "You are the news priority judge. Follow the user instructions exactly."
    user_msg = _build_user_msg(date_str, report)

    text = deepseek_client.chat(
        [{"role": "system", "content": system},
         {"role": "user", "content": user_msg}],
        model=config.MODEL_JUDGE,
        tools=False, max_tokens=4000,
        transcript_path=os.path.join("01_daily/_transcripts", f"{date_str}_judge.json"),
        stage_label=f"NEWS_JUDGE {date_str}",
    )

    os.makedirs(NEWS_DIR, exist_ok=True)
    path = os.path.join(NEWS_DIR, f"{date_str}_judge.md")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(f"# News Judge — {date_str}\n\n")
        fh.write(text)
        fh.write("\n")
    latest = Path(NEWS_DIR) / "latest_judge.md"
    latest.write_text(Path(path).read_text(encoding="utf-8"), encoding="utf-8")
    try:
        from .judge_apply import parse_judge_md
        parsed = parse_judge_md(text)
        parsed["date"] = date_str
        jp = Path(NEWS_DIR) / f"{date_str}_judge.json"
        jp.write_text(json.dumps(parsed, indent=1), encoding="utf-8")
        print(f"[news_judge] structured -> {jp} tickers={list((parsed.get('tickers') or {}).keys())}")
    except Exception as e:
        print(f"[news_judge] structured parse failed: {e}")
    print(f"[news_judge] {date_str} -> {path}")


if __name__ == "__main__":
    main()
