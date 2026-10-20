"""Diff two evaluation rounds: did the last change help, hurt, or neither?

    PYTHONPATH=. python3 -m src.eval_diff eval/results/round-001.json eval/results/round-002.json

Exit code 1 when any regression case flipped PASS -> FAIL, so this can gate
CI or the next build cycle. TARGET cases are reported separately: a
TARGET_FAIL -> TARGET_ACHIEVED flip is progress, not a regression, and a
target that newly fails after being achieved is called out loudly.
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path


def load_round(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if "cases" not in payload:
        raise SystemExit(f"{path}: not a round result file (missing 'cases')")
    return payload


def diff_rounds(old: dict, new: dict) -> dict:
    old_cases = {c["case_id"]: c for c in old["cases"]}
    new_cases = {c["case_id"]: c for c in new["cases"]}

    class_delta = Counter()
    for row in new["cases"]:
        if row["verdict"] in ("FAIL", "TARGET_FAIL"):
            for cls in row["failure_classes"]:
                class_delta[cls] += 1
    for row in old["cases"]:
        if row["verdict"] in ("FAIL", "TARGET_FAIL"):
            for cls in row["failure_classes"]:
                class_delta[cls] -= 1

    flips = []
    for case_id, new_row in new_cases.items():
        old_row = old_cases.get(case_id)
        if not old_row or old_row["verdict"] == new_row["verdict"]:
            continue
        flips.append({
            "case_id": case_id,
            "kind": new_row["kind"],
            "from": old_row["verdict"],
            "to": new_row["verdict"],
            "now_failing": new_row["failure_classes"],
        })

    regressions = [f for f in flips if f["kind"] == "regression" and f["from"] == "PASS" and f["to"] == "FAIL"]
    improvements = [f for f in flips if f["kind"] == "regression" and f["from"] == "FAIL" and f["to"] == "PASS"]
    targets_achieved = [f for f in flips if f["to"] == "TARGET_ACHIEVED"]
    targets_lost = [f for f in flips if f["from"] == "TARGET_ACHIEVED" and f["to"] == "TARGET_FAIL"]
    new_cases_only = sorted(set(new_cases) - set(old_cases))
    removed_cases = sorted(set(old_cases) - set(new_cases))

    return {
        "old": old.get("generated", "?"),
        "new": new.get("generated", "?"),
        "old_changelog": old.get("changelog", ""),
        "new_changelog": new.get("changelog", ""),
        "failure_class_delta": dict(sorted(class_delta.items(), key=lambda kv: -abs(kv[1]))),
        "regressions": regressions,
        "improvements": improvements,
        "targets_achieved": targets_achieved,
        "targets_lost": targets_lost,
        "new_cases": new_cases_only,
        "removed_cases": removed_cases,
        "old_summary": old.get("summary", {}),
        "new_summary": new.get("summary", {}),
    }


def print_report(report: dict) -> None:
    def section(title: str, rows: list[dict]) -> None:
        print(f"\n== {title} ({len(rows)})")
        for row in rows:
            failing = ",".join(row.get("now_failing") or []) or "-"
            print(f"  {row['case_id']}: {row['from']} -> {row['to']}  [{failing}]")

    print(f"round A: {report['old']}  (changelog: {report['old_changelog'] or '-'})")
    print(f"round B: {report['new']}  (changelog: {report['new_changelog'] or '-'})")
    print(f"\nregression pass rate: {report['old_summary'].get('regression_pass_rate')} -> "
          f"{report['new_summary'].get('regression_pass_rate')}")
    print(f"targets achieved:     {report['old_summary'].get('targets_achieved')} -> "
          f"{report['new_summary'].get('targets_achieved')}")

    print("\n== failure-class delta (positive = more failures in round B)")
    for cls, delta in report["failure_class_delta"].items():
        sign = "+" if delta > 0 else ""
        print(f"  {cls}: {sign}{delta}")

    section("REGRESSIONS (must be zero before merging)", report["regressions"])
    section("improvements", report["improvements"])
    section("targets achieved", report["targets_achieved"])
    section("TARGETS LOST (a fix regressed; investigate)", report["targets_lost"])

    if report["new_cases"]:
        print(f"\n== new cases in round B: {', '.join(report['new_cases'])}")
    if report["removed_cases"]:
        print(f"\n== removed cases (suspicious; labels are never deleted): {', '.join(report['removed_cases'])}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Diff two eval rounds")
    parser.add_argument("old", type=Path)
    parser.add_argument("new", type=Path)
    parser.add_argument("--out", type=Path, default=None, help="Also write the diff as JSON")
    args = parser.parse_args()

    report = diff_rounds(load_round(args.old), load_round(args.new))
    print_report(report)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"\nWROTE={args.out}")
    sys.exit(1 if report["regressions"] or report["targets_lost"] else 0)


if __name__ == "__main__":
    main()
