"""Sunday lesson promotion: scan 02_lessons/candidate/*, cluster by
trigger_pattern similarity (token Jaccard >= 0.5), promote clusters with
>= 2 occurrences to 02_lessons/active/ as standing behavioral rules.
An LLM merge produces the final wording; the DECISION to promote is pure code.

CLI: python -m src.promote_lessons
"""
from __future__ import annotations

import glob
import os
import re
from datetime import datetime
from zoneinfo import ZoneInfo

from . import config, deepseek_client

JACCARD_THRESHOLD = 0.5
MIN_OCCURRENCES = 2


def _parse_candidate(path: str) -> dict:
    with open(path, encoding="utf-8") as fh:
        text = fh.read()
    m = re.match(r"---\n(.*?)\n---", text, re.S)
    out = {"path": path, "body": text}
    if m:
        for line in m.group(1).splitlines():
            if ":" in line:
                k, v = line.split(":", 1)
                out[k.strip()] = v.strip().strip('"')
    return out


def _tokens(s: str) -> set:
    return set(re.findall(r"[a-z0-9]+", (s or "").lower()))


def _jaccard(a: set, b: set) -> float:
    return len(a & b) / len(a | b) if a and b else 0.0


def _cluster(cands: list[dict]) -> list[list[dict]]:
    clusters: list[list[dict]] = []
    for c in cands:
        ct = _tokens(c.get("trigger_pattern", ""))
        placed = False
        for cl in clusters:
            if _jaccard(ct, _tokens(cl[0].get("trigger_pattern", ""))) >= JACCARD_THRESHOLD:
                cl.append(c)
                placed = True
                break
        if not placed:
            clusters.append([c])
    return clusters


def _slug(text: str) -> str:
    return re.sub(r"-+", "-", re.sub(r"[^a-z0-9]+", "-",
                                     text.lower()))[:60].strip("-")


def main() -> None:
    paths = sorted(glob.glob(os.path.join(config.LESSONS_CANDIDATE, "*.md")))
    cands = [_parse_candidate(p) for p in paths]
    cands = [c for c in cands if c.get("status", "candidate") == "candidate"]
    print(f"[promote] {len(cands)} open candidates")

    promoted = 0
    for cl in _cluster(cands):
        if len(cl) < MIN_OCCURRENCES:
            continue
        # LLM merges the cluster into one standing rule (wording only)
        digest = "\n\n".join(
            f"- trigger: {c.get('trigger_pattern')}\n"
            f"  current: {c.get('current_behavior')}\n"
            f"  corrected: {c.get('corrected_behavior')}\n"
            f"  evidence: {c.get('evidence_cited')} ({c.get('date')})"
            for c in cl)
        merged = deepseek_client.chat(
            [{"role": "system", "content":
              "You merge repeated trading-prediction lessons into ONE standing "
              "behavioral rule. Be terse and operational. Output Markdown with "
              "fields: RULE (one line), WHEN IT FIRES, WHAT TO DO DIFFERENTLY, "
              "EVIDENCE (dates)."},
             {"role": "user", "content": digest}],
            model=config.MODEL_REFLECT, tools=False, max_tokens=1200)

        today = datetime.now(ZoneInfo(config.TZ)).date().isoformat()
        os.makedirs(config.LESSONS_ACTIVE, exist_ok=True)
        slug = _slug(cl[0].get("trigger_pattern", "rule"))
        apath = os.path.join(config.LESSONS_ACTIVE, f"{slug}.md")
        with open(apath, "w", encoding="utf-8") as fh:
            fh.write("---\n")
            fh.write(f"trigger_pattern: \"{cl[0].get('trigger_pattern', '')}\"\n")
            fh.write(f"occurrences: {len(cl)}\n")
            fh.write(f"promoted_on: \"{today}\"\n")
            fh.write(f"sources: {[os.path.basename(c['path']) for c in cl]}\n")
            fh.write("status: \"active\"\n---\n\n")
            fh.write(merged + "\n")

        # mark candidates promoted (frontmatter status flip)
        for c in cl:
            body = c["body"].replace('status: "candidate"',
                                     'status: "promoted"', 1)
            with open(c["path"], "w", encoding="utf-8") as fh:
                fh.write(body)
        promoted += 1
        print(f"[promote] cluster of {len(cl)} -> {apath}")

    print(f"[promote] done: {promoted} rules promoted")


if __name__ == "__main__":
    main()
