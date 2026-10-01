"""Development-100 regression test for the six frozen bits.

Teacher labels only: tape / preview / finished_act / official_print /
officer_voice / junk. Keep iff the label is a finished act or a print.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from .jev_bits import decide
from .jev_gate import api_key, gate

STOP_PATH = Path(__file__).with_name("jev_sixbit_stop.json")
KEEP_LABELS = frozenset({"finished_act", "official_print", "officer_voice"})
PREC_MIN = 0.85
RECALL_MIN = 0.80
BANNED_KEEP = (
    "call highlights",
    "should you buy",
    "better buy",
)


def load_stop(path: Path | None = None) -> dict:
    return json.loads((path or STOP_PATH).read_text(encoding="utf-8"))


def teacher_keep(label: str) -> bool:
    return label in KEEP_LABELS


def banned_keep(title: str) -> bool:
    t = (title or "").lower()
    return any(bit in t for bit in BANNED_KEEP)


def score(items: list[dict], decided: list[dict]) -> dict:
    by_title = {(d.get("title") or ""): d for d in decided}
    rows = []
    unresolved = []
    tp = fp = fn = 0
    gold_pos = 0
    banned = []
    for item in items:
        title = item["title"]
        gold = teacher_keep(item["label"])
        got = by_title.get(title) or {}
        valid = bool(got) and not got.get("review_required")
        if not valid:
            unresolved.append(title)
        pred = valid and got.get("decision") == "keep"
        if gold:
            gold_pos += 1
        if pred and gold:
            tp += 1
        elif pred and not gold:
            fp += 1
        elif gold and not pred:
            fn += 1
        if pred and banned_keep(title):
            banned.append(title)
        rows.append({
            "title": title,
            "label": item["label"],
            "gold": "keep" if gold else "drop",
            "pred": got.get("decision") or "",
            "reason": got.get("reason") or "",
            "noul": got.get("noul") or {},
        })
    prec = tp / (tp + fp) if (tp + fp) else 0.0
    rec = tp / gold_pos if gold_pos else 0.0
    return {
        "n": len(items),
        "dataset_role": "development (reused during prompt tuning)",
        "unresolved": unresolved,
        "gold_keep": gold_pos,
        "pred_keep": tp + fp,
        "precision": round(prec, 4),
        "recall_print_done": round(rec, 4),
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "banned_keeps": banned,
        "pass": (
            prec >= PREC_MIN
            and rec >= RECALL_MIN
            and not banned
            and not unresolved
        ),
        "rows": rows,
    }


def run_live(key: str = "", workers: int = 24, poster=None) -> dict:
    blob = load_stop()
    rows = [
        {"title": it["title"], "source": "", "id": f"stop-{i}"}
        for i, it in enumerate(blob["items"], 1)
    ]
    decided = gate(
        rows,
        code_only=False,
        live=True,
        key=key or api_key(),
        workers=workers,
        poster=poster,
    )
    from .jev_triage import fingerprint
    from .jev_bits import BIT_QUESTIONS
    report = score(blob["items"], decided)
    report["prompt_sha256"] = fingerprint(BIT_QUESTIONS)
    report["models"] = sorted({r.get("_jev_model", "unknown") for r in rows})
    return report


def to_markdown(report: dict) -> str:
    lines = [
        "# Jev six-bit stop test",
        "",
        f"n={report['n']} gold_keep={report['gold_keep']} "
        f"pred_keep={report['pred_keep']} "
        f"precision={report['precision']} "
        f"recall_print_done={report['recall_print_done']} "
        f"pass={report['pass']}",
        "",
        f"Need precision ≥ {PREC_MIN}, print/done recall ≥ {RECALL_MIN}, "
        "zero call-highlights / should-you-buy keeps.",
        "",
    ]
    if report["banned_keeps"]:
        lines.append("## Banned keeps")
        for title in report["banned_keeps"]:
            lines.append(f"- {title}")
        lines.append("")
    lines.append("## Misses")
    for row in report["rows"]:
        if row["gold"] == row["pred"]:
            continue
        lines.append(
            f"- gold={row['gold']} pred={row['pred']}/{row['reason']} "
            f"label={row['label']} | {row['title'][:120]}"
        )
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Six-bit development-100 regression test")
    p.add_argument("--live", action="store_true")
    p.add_argument("--workers", type=int, default=24)
    args = p.parse_args(argv)
    if args.live:
        report = run_live(workers=args.workers)
        print(to_markdown(report))
        print(f"JEV_SIXBIT_STOP_PASS={int(report['pass'])}")
        out_dir = Path(__file__).resolve().parent.parent / "00_grounding" / "jev_train"
        if out_dir.is_dir():
            payload = {k: report[k] for k in report}
            (out_dir / "sixbit_stop.json").write_text(
                json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
        return 0 if report["pass"] else 3
    blob = load_stop()
    print(f"stop gold n={len(blob['items'])} "
          f"keep={sum(1 for it in blob['items'] if teacher_keep(it['label']))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
