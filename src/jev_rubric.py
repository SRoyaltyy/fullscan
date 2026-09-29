"""Living hop-0 rubric. Each jev-train grade appends one session.

Jev is questions + decide(). This module does not invent event_class,
polarity, or tickers. It does not edit closed lists. After a grade it
writes few-shot lines and proposes exactly one Jev-analysis change.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from pathlib import Path

RUBRIC_NAME = "RUBRIC.md"
SCHEMA_NOTE = "jev-rubric-1"

# Drop/keep reasons that are Jev questions. Everything else is code.
REASON_TO_QUESTION = {
    "opinion": "is_opinion",
    "reaction": "is_reaction",
    "tabloid": "is_tabloid",
    "low_material": "action_material",
    "core_material": "action_material",
    "geo_other": "geo",
    "other_powerful": "geo",
    "crowd": "actor_power",
    "crowd_fact": "actor_power",
    "state_head_action": "actor_power",
    "reprint_weather": "reprint_weather",
    "chokepoint": "reprint_weather",
    "choke_fact": "reprint_weather",
    "geo_chokepoint_no_hit": "reprint_weather",
}

# Sibling question when the miss is a material floor: Jev also
# under-scored new_instrument on prints / holds / CRs.
MATERIAL_SIBLING = "new_instrument"

CODE_REASONS = frozenset({
    "punct", "source", "junk_shape", "earnings", "tape", "dup",
    "jev_error", "reaction_regex",
})

# Tie-break when two questions have the same miss count.
KNOB_PRIORITY = (
    "action_material",
    "is_opinion",
    "new_instrument",
    "geo",
    "actor_power",
    "reprint_weather",
    "is_reaction",
    "is_tabloid",
)

PENDING_BEGIN = "<!-- PENDING_BEGIN -->"
PENDING_END = "<!-- PENDING_END -->"
SESSIONS_BEGIN = "<!-- SESSIONS_BEGIN -->"

HEADER = """# Jev hop-0 rubric

Living criteria for how Jev analyses a title. Hop-0 filters trash and
holds anything Lane should classify. Jev never picks event_class,
polarity, or a ticker.

Each jev-train submit appends a session below and replaces the pending
one-change block. Closed lists (`jev_closed_lists.json`) are never
edited. At most one `decide()` / threshold change per session. Question
instructions and criteria update from the misses and `human_reason`
lines.

Thresholds stay put unless the pending block names one:
`TRASH_NOUL=0.70`, `MATERIAL_KEEP=0.65`, `INSTRUMENT_KEEP=0.60`,
`CROWD_DROP=0.50`.

"""


def rubric_path(ground: Path) -> Path:
    return ground / "jev_train" / RUBRIC_NAME


def surface_for_reason(reason: str) -> str:
    reason = (reason or "").strip()
    if reason in REASON_TO_QUESTION:
        return REASON_TO_QUESTION[reason]
    if reason in CODE_REASONS or reason.startswith("code_"):
        return f"code:{reason or 'unknown'}"
    if reason:
        return f"code:{reason}"
    return "code:unknown"


def _row_line(row: dict) -> dict:
    return {
        "you": "KEEP" if row.get("human") == "K" else (
            "DROP" if row.get("human") == "D" else (row.get("human") or "")
        ),
        "reason": row.get("reason") or "",
        "question": surface_for_reason(row.get("reason") or ""),
        "human_reason": (row.get("human_reason") or "").replace("\n", " ").strip(),
        "title": (row.get("title") or "").replace("\n", " ").strip(),
        "kind": "false_drop" if row.get("human") == "K" else "false_keep",
    }


def mapped_misses(grade: dict) -> list[dict]:
    out = []
    for row in grade.get("false_drop") or []:
        out.append(_row_line(row))
    for row in grade.get("false_keep") or []:
        out.append(_row_line(row))
    return out


def propose_one_change(grade: dict) -> dict:
    """Exactly one Jev-analysis change. Code misses are notes, not the knob."""
    buckets: Counter[str] = Counter()
    evidence: dict[str, list[dict]] = defaultdict(list)
    code_notes: list[dict] = []
    for row in mapped_misses(grade):
        question = row["question"]
        if question.startswith("code:"):
            code_notes.append(row)
            continue
        buckets[question] += 1
        evidence[question].append(row)
        if question == "action_material":
            # Count the sibling so the proposal names it, but do not
            # let it win the one-change slot over action_material.
            evidence[MATERIAL_SIBLING].append(row)

    if not buckets:
        text = (
            "No Jev-question miss this session. Code drops "
            f"({len(code_notes)}) stay on the code path; do not add a "
            "closed-list line."
        )
        return {
            "question": "",
            "kind": "none",
            "n": 0,
            "text": text,
            "evidence": [],
            "code_notes": code_notes,
        }

    def sort_key(name: str) -> tuple[int, int]:
        try:
            pri = KNOB_PRIORITY.index(name)
        except ValueError:
            pri = len(KNOB_PRIORITY)
        return (-buckets[name], pri)

    question = sorted(buckets, key=sort_key)[0]
    rows = evidence[question]
    keep_lines = [
        row["human_reason"] or row["title"]
        for row in rows if row["kind"] == "false_drop" and (row["human_reason"] or row["title"])
    ]
    drop_lines = [
        row["human_reason"] or row["title"]
        for row in rows if row["kind"] == "false_keep" and (row["human_reason"] or row["title"])
    ]
    seen: set[str] = set()
    keep_uniq = []
    for line in keep_lines:
        if line not in seen:
            seen.add(line)
            keep_uniq.append(line)
    drop_uniq = []
    for line in drop_lines:
        if line not in seen:
            seen.add(line)
            drop_uniq.append(line)

    parts = [f"Rewrite `{question}` criteria from this session's misses."]
    if keep_uniq:
        parts.append("Score toward KEEP / false-opinion / core / powerful when: " + "; ".join(keep_uniq[:8]) + ".")
    if drop_uniq:
        parts.append("Do not loosen so these become keeps: " + "; ".join(drop_uniq[:6]) + ".")
    if question == "action_material":
        parts.append(
            f"Sibling `{MATERIAL_SIBLING}`: dated print / hold / CR / "
            "explores-rules / panel-endorse / named dollar deal is true."
        )
    parts.append("Do not edit closed lists. At most one decide() / threshold change.")
    return {
        "question": question,
        "kind": "criteria",
        "n": buckets[question],
        "text": " ".join(parts),
        "evidence": rows,
        "code_notes": code_notes,
    }


def _cell(text: str) -> str:
    return (text or "").replace("|", "/").replace("\n", " ").strip()


def render_pending_block(proposal: dict, stamp: str = "") -> str:
    question = proposal.get("question") or "(none)"
    kind = proposal.get("kind") or "none"
    n = proposal.get("n") or 0
    text = proposal.get("text") or ""
    label = f"session `{stamp}`" if stamp else "latest session"
    return "\n".join([
        PENDING_BEGIN,
        f"## Pending one change ({label})",
        "",
        f"- question: `{question}`",
        f"- kind: `{kind}`",
        f"- misses on that question: {n}",
        f"- change: {text}",
        "- do not edit `jev_closed_lists.json`",
        PENDING_END,
        "",
    ])


def render_session_section(grade: dict, proposal: dict) -> str:
    stamp = grade.get("stamp") or ""
    counts = grade.get("counts") or {}
    lines = [
        f"## Session `{stamp}`",
        "",
        f"Draw `{grade.get('draw_stamp') or ''}`. "
        f"Human keep {counts.get('human_keep', 0)}, drop {counts.get('human_drop', 0)}. "
        f"Jev keep {counts.get('jev_keep', 0)}, drop {counts.get('jev_drop', 0)}. "
        f"False keep {counts.get('false_keep', 0)}, false drop {counts.get('false_drop', 0)}.",
        "",
        "| You | reason | question | human_reason | title |",
        "|---|---|---|---|---|",
    ]
    mapped = mapped_misses(grade)
    if not mapped:
        lines.append("|  |  |  | none |  |")
    for row in mapped:
        lines.append(
            f"| {_cell(row['you'])} | {_cell(row['reason'])} | "
            f"{_cell(row['question'])} | {_cell(row['human_reason'])} | "
            f"{_cell(row['title'])} |"
        )
    lines += [
        "",
        f"Proposed one change: `{proposal.get('question') or '(none)'}` "
        f"({proposal.get('kind') or 'none'}, n={proposal.get('n') or 0}).",
        "",
        proposal.get("text") or "",
        "",
    ]
    code_notes = proposal.get("code_notes") or []
    if code_notes:
        lines.append("Code-path misses (not the Jev knob this round):")
        for row in code_notes:
            lines.append(
                f"- `{row['reason']}` — {_cell(row['title'])} "
                f"({_cell(row['human_reason'])})"
            )
        lines.append("")
    return "\n".join(lines)


def render_issue_block(proposal: dict, *, rubric_rel: str = "00_grounding/jev_train/RUBRIC.md") -> str:
    lines = [
        "## Rubric (one Jev-analysis change)",
        "",
        f"This grade updates `{rubric_rel}`. Closed lists are not edited. "
        "At most one decide() / threshold change per session.",
        "",
        f"- question: `{proposal.get('question') or '(none)'}`",
        f"- kind: `{proposal.get('kind') or 'none'}`",
        f"- misses on that question: {proposal.get('n') or 0}",
        f"- change: {proposal.get('text') or ''}",
        "",
    ]
    return "\n".join(lines)


def _replace_pending(text: str, block: str) -> str:
    if PENDING_BEGIN in text and PENDING_END in text:
        start = text.index(PENDING_BEGIN)
        end = text.index(PENDING_END) + len(PENDING_END)
        # keep a trailing newline after the block
        after = text[end:]
        if after.startswith("\n"):
            after = after[1:]
        return text[:start] + block + after
    # Insert pending after the header, before sessions.
    if SESSIONS_BEGIN in text:
        idx = text.index(SESSIONS_BEGIN)
        return text[:idx] + block + "\n" + text[idx:]
    return text.rstrip() + "\n\n" + block


def upsert_rubric(
    path: Path,
    grade: dict,
    proposal: dict | None = None,
    *,
    write: bool = True,
) -> str:
    """Append this session and refresh the pending one-change block."""
    proposal = proposal or propose_one_change(grade)
    stamp = str(grade.get("stamp") or "")
    pending = render_pending_block(proposal, stamp)
    session = render_session_section(grade, proposal)
    if path.is_file():
        text = path.read_text(encoding="utf-8")
    else:
        text = HEADER + pending + SESSIONS_BEGIN + "\n\n"
    text = _replace_pending(text, pending)
    if f"## Session `{stamp}`" in text and stamp:
        body = text
    else:
        if SESSIONS_BEGIN in text:
            body = text.rstrip() + "\n\n" + session
        else:
            body = text.rstrip() + "\n\n" + SESSIONS_BEGIN + "\n\n" + session
    if not body.endswith("\n"):
        body += "\n"
    if write:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body, encoding="utf-8")
    return body
