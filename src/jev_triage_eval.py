"""Paired development/validation evaluation; never count review as correct.

Human training grades stay development data. Validation labels are frozen before
this candidate's first live evaluation. No teacher/oracle fills abstentions.
"""
from __future__ import annotations
import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
from .jev_gate import gate, api_key
from .jev_triage import fingerprint, QUESTIONS
from .jev_bits import BIT_QUESTIONS

ROOT = Path(__file__).resolve().parent.parent


def development():
    labels = {}
    for path in sorted((ROOT / "00_grounding/jev_train").glob("20260929_*.json")):
        for row in json.loads(path.read_text()).get("items", []):
            grade = row.get("grade")
            if grade not in {"K", "D"}: continue
            title = row.get("title", "")
            entry = labels.setdefault(title, {"row": row, "grades": set()})
            entry["grades"].add(grade)
    return [{**{k: e["row"].get(k, "") for k in ("title", "source", "published_at", "url")},
             "gold": ("keep" if e["grades"] == {"K"} else "drop" if e["grades"] == {"D"} else "review")}
            for e in labels.values()]


def metrics(items, decisions):
    if len(items) != len(decisions): raise ValueError("incomplete results")
    reviewed = errors = correct = automatic = tp = fp = fn = positives = retained = ambiguous = 0
    for item, out in zip(items, decisions):
        if item["title"] != out["title"]: raise ValueError("misaligned results")
        gold = item["gold"]
        review = bool(out.get("review_required"))
        reviewed += review
        errors += out.get("reason") == "jev_error"
        if gold == "review":
            ambiguous += 1
            continue
        positives += gold == "keep"
        retained += gold == "keep" and out["decision"] == "keep"
        if review: continue
        automatic += 1
        pred = out["decision"]
        correct += pred == gold
        tp += pred == gold == "keep"
        fp += pred == "keep" and gold == "drop"
        fn += pred == "drop" and gold == "keep"
    return {"n":len(items), "gold_ambiguous":ambiguous, "reviews":reviewed, "errors":errors,
            "automatic_labeled": automatic, "automatic_accuracy":correct/automatic if automatic else None,
            "automatic_keep_precision":tp/(tp+fp) if tp+fp else None,
            "automatic_keep_recall":tp/positives if positives else None,
            "retained_keep_recall":retained/positives if positives else None,
            "false_keeps":fp, "false_drops":fn,
            "valid_run": errors == 0}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()
    validation_path = ROOT / "00_grounding/jev_triage_validation.json"
    validation = json.loads(validation_path.read_text())
    datasets = {"development":development(), "validation":validation["items"]}
    key = api_key()
    if not key: raise RuntimeError("JEV_API_KEY is required; no regex substitute")
    report = {"generated_at":dt.datetime.now(dt.timezone.utc).isoformat(),
              "validation_sha256":hashlib.sha256(validation_path.read_bytes()).hexdigest(),
              "validation_provenance":validation["provenance"], "policies":{}}
    for policy, questions in (("sixbit", BIT_QUESTIONS), ("triage", QUESTIONS)):
        results = {"prompt_sha256":fingerprint(questions), "datasets":{}}
        for name, items in datasets.items():
            # Accuracy measures each article, independently of deduplication.
            rows = [{k:v for k,v in item.items() if k in {"title","source","url","published_at","snippet","description","content","summary"}} for item in items]
            for i,row in enumerate(rows): row.update(id=f"{name}-{i}")
            decisions = gate(rows, live=True, key=key, workers=args.workers, policy=policy, deduplicate=False)
            results["datasets"][name] = {"metrics":metrics(items,decisions), "rows":[{"gold":it["gold"], **out} for it,out in zip(items,decisions)],
                                           "models":sorted({r.get("_jev_model", "unknown") for r in rows})}
        report["policies"][policy]=results
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output).write_text(json.dumps(report,indent=2,ensure_ascii=False)+"\n")
    for policy,result in report["policies"].items():
        for dataset, result in result["datasets"].items(): print(policy,dataset,json.dumps(result["metrics"]))
    if any(not result["metrics"]["valid_run"] for p in report["policies"].values() for result in p["datasets"].values()):
        raise SystemExit("API errors: report is incomplete, not an accuracy result")

if __name__ == "__main__": main()
