"""Stratified sampler for the judge step of the loop.

Random articles mostly exercise the trivial "unknown" case. This sampler
draws a fixed mix: adversarial buckets that target the known failure
classes, plus random fill, so every round's sample bites.

    PYTHONPATH=. python3 -m src.eval_sample \
        --news data/news_intake/2026-10-17/documents.json \
        --budget 30 \
        --out eval/pool/2026-10-17.jsonl \
        [--relations relations.json] [--seed 29]

Output rows are unlabeled: send them through judge Prompt B
(eval/judge_prompt.md), human-check, then append to eval/cases.jsonl.
"""
from __future__ import annotations

import argparse
import json
import random
import re
from pathlib import Path

# Tickers that are ordinary English words and therefore prime alias false
# positives. Extend this from the wordlist table once the alias guard lands
# (the guard should export the full list; keep this fallback small).
WORD_TICKERS = frozenset("""
post hood coin fly save jet luv gps cat frog bark wolf race para echo love
wish work ford morn hope opal anet
""".split())

_FALLBACK_PLACES = frozenset("""
new york los angeles chicago houston phoenix philadelphia san antonio san diego
dallas charlotte atlanta miami denver boston seattle washington detroit
north carolina south carolina new jersey new hampshire
""".split())

_TRAILING_SOURCE = re.compile(r"\s+[-|\u2013\u2014]\s+([A-Z][\w&.']*(?: [A-Z][\w&.']*){0,4})$")


def _places() -> set[str]:
    try:
        from src.places import place_names  # repo has the full gazetteer
        return {p.lower() for p in place_names()}
    except Exception:
        return set(_FALLBACK_PLACES)


def _load_rows(path: Path) -> list[dict]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload if isinstance(payload, list) else payload.get("documents") or payload.get("rows") or []
    return [r for r in rows if isinstance(r, dict)]


def _text_of(row: dict) -> str:
    return (str(row.get("title") or "") + "\n" + str(row.get("body") or row.get("text") or "")).strip()


def classify_bucket(row: dict, places: set[str], relation_names: set[str]) -> str:
    text = _text_of(row)
    low = text.lower()
    if len(text) < 200:
        return "thin"
    for token in re.findall(r"[A-Za-z]{2,6}", text):
        if token.lower() in WORD_TICKERS and token.isupper() is False:
            # prose use of a word-ticker (all-caps uses are likely real tickers)
            return "ticker_word"
    match = _TRAILING_SOURCE.search(str(row.get("title") or ""))
    if match and match.group(1).lower() not in places:
        return "publisher"
    for place in places:
        if re.search(rf"\b{re.escape(place)}\b", low):
            return "place"
    for name in relation_names:
        if name and re.search(rf"\b{re.escape(name)}\b", text, re.I):
            return "partner"
    return "random"


CASE_TYPE_HINT = {
    "thin": "thin_unknown",
    "ticker_word": "alias_trap",
    "publisher": "alias_trap",
    "place": "alias_trap",
    "partner": "second_order",
    "random": None,
}


def sample(rows: list[dict], budget: int, relations: set[str], seed: int) -> list[dict]:
    places = _places()
    buckets: dict[str, list[dict]] = {}
    for row in rows:
        buckets.setdefault(classify_bucket(row, places, relations), []).append(row)

    rng = random.Random(seed)
    for pool in buckets.values():
        rng.shuffle(pool)

    # Adversarial buckets get up to a fifth of the budget each, random fills
    # the rest. Small buckets simply contribute what they have.
    picked: list[dict] = []
    seen: set[str] = set()
    per_bucket = max(1, budget // 5)
    for name in ("ticker_word", "publisher", "place", "partner", "thin"):
        for row in buckets.get(name, [])[:per_bucket]:
            key = str(row.get("id") or row.get("url") or _text_of(row)[:60])
            if key not in seen:
                seen.add(key)
                picked.append(row)
    for row in buckets.get("random", []):
        if len(picked) >= budget:
            break
        key = str(row.get("id") or row.get("url") or _text_of(row)[:60])
        if key not in seen:
            seen.add(key)
            picked.append(row)
    return picked


def main() -> None:
    parser = argparse.ArgumentParser(description="Draw a stratified article sample for labeling")
    parser.add_argument("--news", type=Path, required=True, help="documents.json from the scanner")
    parser.add_argument("--budget", type=int, default=30)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--relations", type=Path, default=None,
                        help="Optional relations JSON to bias the partner bucket")
    parser.add_argument("--seed", type=int, default=29)
    args = parser.parse_args()

    rows = _load_rows(args.news)
    relations: set[str] = set()
    if args.relations and args.relations.is_file():
        payload = json.loads(args.relations.read_text(encoding="utf-8"))
        for row in payload if isinstance(payload, list) else payload.get("relations") or []:
            name = str(row.get("partner") or row.get("name") or "")
            if len(name) >= 4:
                relations.add(name)

    places = _places()
    picked = sample(rows, args.budget, relations, args.seed)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", encoding="utf-8") as handle:
        for row in picked:
            text = _text_of(row)
            bucket = classify_bucket(row, places, relations)
            handle.write(json.dumps({
                "id": str(row.get("id") or row.get("url") or text[:60]),
                "kind": "regression",
                "case_type": CASE_TYPE_HINT.get(bucket),
                "title": str(row.get("title") or "")[:240],
                "text": text[:20000],
                "source_url": str(row.get("url") or ""),
                "date": "",
                "text_status": "full",
                "drawn_bucket": bucket,
                "expected": {},
                "labels": {"author": "sampler", "model": "", "human_checked": False,
                            "version": 1, "created": "", "note": "label via eval/judge_prompt.md Prompt B"},
            }, ensure_ascii=False) + "\n")
    counts: dict[str, int] = {}
    for row in picked:
        bucket = classify_bucket(row, places, relations)
        counts[bucket] = counts.get(bucket, 0) + 1
    print(f"sampled {len(picked)} of {len(rows)} rows -> {args.out}")
    print(json.dumps(counts, indent=2))


if __name__ == "__main__":
    main()
