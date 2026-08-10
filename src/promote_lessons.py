"""Lesson promotion: candidate → active standing rules.

Rules:
  - Market lessons (A/B/C): need >=2 similar candidates (Jaccard on `when`)
    AND each must have schema_ok / falsifier.
  - Ops lessons (D): promote on >=1 complete candidate.
  - Incomplete (no when/do_instead/wrong_if): never promote.

CLI: python -m src.promote_lessons [--force-path PATH]  # seed one candidate
"""
from __future__ import annotations

import argparse
import glob
import os
import re
from datetime import datetime
from zoneinfo import ZoneInfo

from . import config, deepseek_client, lesson_schema

JACCARD_THRESHOLD = 0.5
MIN_OCCURRENCES_MARKET = 2
MIN_OCCURRENCES_OPS = 1


def _parse_candidate(path: str) -> dict:
    with open(path, encoding="utf-8") as fh:
        text = fh.read()
    out: dict = {"path": path, "body": text}
    m = re.match(r"---\n(.*?)\n---", text, re.S)
    if m:
        for line in m.group(1).splitlines():
            if ":" in line:
                k, v = line.split(":", 1)
                out[k.strip()] = v.strip().strip('"')
    n = lesson_schema.normalize(out, out.get("date", ""))
    out["_norm"] = n
    out["_complete"] = lesson_schema.is_complete(n) and not lesson_schema.validation_errors(n)
    return out


def _tokens(s: str) -> set:
    return set(re.findall(r"[a-z0-9]+", (s or "").lower()))


def _jaccard(a: set, b: set) -> float:
    return len(a & b) / len(a | b) if a and b else 0.0


def _cluster(cands: list[dict]) -> list[list[dict]]:
    clusters: list[list[dict]] = []
    for c in cands:
        ct = _tokens(c["_norm"].get("when", "") or c.get("trigger_pattern", ""))
        placed = False
        for cl in clusters:
            base = cl[0]["_norm"].get("when", "") or cl[0].get("trigger_pattern", "")
            if _jaccard(ct, _tokens(base)) >= JACCARD_THRESHOLD:
                cl.append(c)
                placed = True
                break
        if not placed:
            clusters.append([c])
    return clusters


def _slug(text: str) -> str:
    return re.sub(r"-+", "-", re.sub(r"[^a-z0-9]+", "-", text.lower()))[:60].strip("-")


def _write_active(cl: list[dict], merged_body: str | None = None) -> str:
    today = datetime.now(ZoneInfo(config.TZ)).date().isoformat()
    os.makedirs(config.LESSONS_ACTIVE, exist_ok=True)
    n0 = cl[0]["_norm"]
    slug = _slug(n0.get("when") or "rule") or "rule"
    apath = os.path.join(config.LESSONS_ACTIVE, f"{slug}.md")
    # prefer structured rule from best candidate
    body = lesson_schema.active_rule_markdown(
        {**n0, "status": "active"},
        extra_body=merged_body or "",
    )
    # prepend promotion meta into frontmatter via rewrite
    sources = [os.path.basename(c["path"]) for c in cl]
    body = body.replace(
        'status: "active"',
        f'status: "active"\noccurrences: "{len(cl)}"\npromoted_on: "{today}"\nsources: "{sources}"',
        1,
    )
    with open(apath, "w", encoding="utf-8") as fh:
        fh.write(body)
    for c in cl:
        new_body = c["body"].replace('status: "candidate"', 'status: "promoted"', 1)
        with open(c["path"], "w", encoding="utf-8") as fh:
            fh.write(new_body)
    return apath


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--force-path", default=None,
                    help="Promote a single complete candidate even if n=1 (seed)")
    args = ap.parse_args()

    if args.force_path:
        c = _parse_candidate(args.force_path)
        if not c["_complete"]:
            raise SystemExit(f"incomplete schema: {lesson_schema.validation_errors(c['_norm'])}")
        path = _write_active([c], merged_body="(seeded via --force-path)")
        print(f"[promote] force-seeded -> {path}")
        return

    paths = sorted(glob.glob(os.path.join(config.LESSONS_CANDIDATE, "*.md")))
    cands = [_parse_candidate(p) for p in paths]
    cands = [c for c in cands if c.get("status", "candidate") == "candidate"]
    print(f"[promote] {len(cands)} open candidates")

    promoted = 0
    skipped_incomplete = 0
    for cl in _cluster(cands):
        complete = [c for c in cl if c["_complete"]]
        if not complete:
            skipped_incomplete += len(cl)
            print(f"[promote] skip cluster size={len(cl)} — incomplete schema")
            continue
        cat = complete[0]["_norm"].get("error_category", "E")
        need = MIN_OCCURRENCES_OPS if cat == "D" else MIN_OCCURRENCES_MARKET
        if len(complete) < need:
            print(f"[promote] cluster cat={cat} complete={len(complete)} "
                  f"need={need} — hold")
            continue

        digest = "\n\n".join(
            f"- when: {c['_norm'].get('when')}\n"
            f"  do_instead: {c['_norm'].get('do_instead')}\n"
            f"  wrong_if: {c['_norm'].get('wrong_if')}\n"
            f"  evidence: {c['_norm'].get('evidence')} ({c['_norm'].get('date')})"
            for c in complete
        )
        merged = ""
        if config.DEEPSEEK_API_KEY and len(complete) >= 2:
            try:
                merged = deepseek_client.chat(
                    [{"role": "system", "content":
                      "Merge repeated lessons into ONE standing rule. "
                      "Keep WHEN / DO / WRONG IF operational and short."},
                     {"role": "user", "content": digest}],
                    model=config.MODEL_REFLECT, tools=False, max_tokens=800,
                )
            except Exception as e:
                print(f"[promote] LLM merge skipped: {e}")
                merged = ""

        apath = _write_active(complete, merged_body=merged)
        promoted += 1
        print(f"[promote] cluster of {len(complete)} cat={cat} -> {apath}")

    print(f"[promote] done: {promoted} rules promoted; "
          f"incomplete_skipped={skipped_incomplete}")


if __name__ == "__main__":
    main()
