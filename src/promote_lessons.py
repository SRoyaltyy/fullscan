"""Lesson promotion: candidate → active standing rules.

Market (A/B/C): need >=2 complete similar candidates.
Ops (D): promote on >=1 complete candidate.
Incomplete schema: never promote.

CLI: python -m src.promote_lessons [--force-path PATH]
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


def _write_active(cl: list[dict], merged_body: str = "") -> str:
    today = datetime.now(ZoneInfo(config.TZ)).date().isoformat()
    os.makedirs(config.LESSONS_ACTIVE, exist_ok=True)
    n0 = cl[0]["_norm"]
    slug = _slug(n0.get("when") or "rule") or "rule"
    apath = os.path.join(config.LESSONS_ACTIVE, f"{slug}.md")
    body = lesson_schema.active_rule_markdown({**n0, "status": "active"}, extra_body=merged_body)
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
        new_body = new_body.replace('status: "candidate_incomplete"', 'status: "promoted"', 1)
        with open(c["path"], "w", encoding="utf-8") as fh:
            fh.write(new_body)
    return apath


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--force-path", default=None)
    args = ap.parse_args()

    if args.force_path:
        c = _parse_candidate(args.force_path)
        if not c["_complete"]:
            raise SystemExit(f"incomplete: {lesson_schema.validation_errors(c['_norm'])}")
        path = _write_active([c], merged_body="(seeded via --force-path)")
        print(f"[promote] force-seeded -> {path}")
        return

    paths = sorted(glob.glob(os.path.join(config.LESSONS_CANDIDATE, "*.md")))
    cands = [_parse_candidate(p) for p in paths]
    cands = [c for c in cands if c.get("status", "candidate") in ("candidate", "candidate_incomplete")]
    # only complete ones promote
    print(f"[promote] {len(cands)} open candidates")

    promoted = 0
    for cl in _cluster(cands):
        complete = [c for c in cl if c["_complete"]]
        if not complete:
            print(f"[promote] skip incomplete cluster size={len(cl)}")
            continue
        cat = complete[0]["_norm"].get("error_category", "E")
        need = MIN_OCCURRENCES_OPS if cat == "D" else MIN_OCCURRENCES_MARKET
        if len(complete) < need:
            print(f"[promote] hold cat={cat} complete={len(complete)} need={need}")
            continue

        digest = "\n\n".join(
            f"- when: {c['_norm'].get('when')}\n"
            f"  do: {c['_norm'].get('do_instead')}\n"
            f"  wrong_if: {c['_norm'].get('wrong_if')}\n"
            f"  evidence: {c['_norm'].get('evidence')}"
            for c in complete
        )
        merged = ""
        if config.DEEPSEEK_API_KEY and len(complete) >= 2:
            try:
                merged = deepseek_client.chat(
                    [{"role": "system", "content":
                      "Merge into ONE standing rule. Keep WHEN/DO/WRONG IF short."},
                     {"role": "user", "content": digest}],
                    model=config.MODEL_REFLECT, tools=False, max_tokens=800,
                )
            except Exception as e:
                print(f"[promote] merge skip: {e}")

        apath = _write_active(complete, merged_body=merged)
        promoted += 1
        print(f"[promote] -> {apath}")

    print(f"[promote] done: {promoted} rules")


if __name__ == "__main__":
    main()
