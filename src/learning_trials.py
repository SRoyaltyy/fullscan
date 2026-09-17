"""Freeze learned policy proposals; apply only version-matched forward evidence.

Discovery remains recursive, but repeatedly fitting the same dates never counts
as forward validation. The observed replay contract is documented in the audit.
"""
from datetime import datetime, timezone
import json
from pathlib import Path
from .research_validation import digest, promotion_evidence, write_once, timestamp

ROOT = Path(__file__).resolve().parent.parent
TRIALS = ROOT / "data" / "learning_trials"


def engine_hash():
    names = ("research_validation.py", "learning_replay.py", "portfolio_risk.py",
             "factor_mine_book.py", "stock_book.py", "book_learn.py", "execution_clock.py")
    return digest({**{n: (ROOT / "src" / n).read_text() for n in names},
                   "fees": (ROOT / "00_grounding/futubull_fees.json").read_text()})


def trial_key(incumbent, proposal):
    return digest({"baseline": incumbent, "candidate": proposal, "engine": engine_hash()})


def govern(incumbent, proposal, asof, *, root=None, now=None):
    root = Path(root or TRIALS)
    now = now or datetime.now(timezone.utc).isoformat()
    stamp = timestamp(now)
    base_hash = digest(incumbent)
    result = {"decision": "SHADOW — proposal requires frozen prospective paired evidence",
              "incumbent_hash": base_hash, "trials": []}
    # Freeze distinct candidates without overwriting earlier hypothesis clocks.
    if digest(proposal) != base_hash:
        key = trial_key(incumbent, proposal)
        path = root / "proposals" / (key + ".json")
        if not path.exists():
            write_once(path, {"candidate_hash": key, "baseline_hash": base_hash,
                              "baseline": incumbent, "candidate": proposal, "engine_hash": engine_hash(),
                              "frozen_at": now, "fit_through": asof})
    candidates = []
    paths = sorted((root / "proposals").glob("*.json"),
                   key=lambda p: (json.loads(p.read_text())["frozen_at"], p.name))
    for path in paths:
        trial = json.loads(path.read_text())
        if trial["baseline_hash"] != base_hash or trial.get("engine_hash") != engine_hash():
            continue  # evidence against a different incumbent cannot authorize this change
        rows = []
        for p in sorted((root / "observations" / trial["candidate_hash"]).glob("*.json")):
            rows.append(json.loads(p.read_text()))
        rows = sorted(rows, key=lambda r: r.get("decision_at", ""))[:252]
        evidence = promotion_evidence(rows, frozen_at=trial["frozen_at"],
                                      asof=now, candidate_hash=trial["candidate_hash"],
                                      min_sessions=252, min_blocks=12, block=21)
        result["trials"].append({"candidate_hash": trial["candidate_hash"], **evidence})
        if evidence["status"] == "eligible":
            candidates.append((trial["frozen_at"], trial, evidence))
        # One precommitted candidate per incumbent and one fixed-length test.
        # A failed test cannot be extended until it passes or replaced in hindsight.
        break
    approved = incumbent
    if candidates:
        # Fixed FIFO choice; never pick the best result after looking at holdouts.
        _, trial, evidence = min(candidates, key=lambda x: (x[0], x[1]["candidate_hash"]))
        approved = trial["candidate"]
        result["decision"] = "PROMOTE — prospective paired evidence passed"
        promotion = root / "promotions" / (trial["candidate_hash"] + ".json")
        if not promotion.exists():
            write_once(promotion, {"candidate_hash": trial["candidate_hash"], "promoted_at": now,
                                   "evidence": evidence, "previous_policy": incumbent})
    root.mkdir(parents=True, exist_ok=True)
    (root / "status.json").write_text(json.dumps(result, indent=2) + "\n")
    return approved, result


def capture_frame(df, date, *, root=None, now=None):
    """Record the actually available ranker frame, never backdate a reconstruction."""
    root = Path(root or TRIALS)
    now = now or datetime.now(timezone.utc).isoformat()
    # pandas' JSON conversion handles NaN without nonstandard JSON values.
    records = json.loads(df.to_json(orient="records"))
    from . import lesson_exec
    context = {"earnings": lesson_exec._earnings_map(date), "digest": lesson_exec._digest_map(date)}
    snap = {"session": date, "observed_at": now, "available_at": now,
            "decision_at": date + "T09:30:00" + _offset(date),
            "input_hash": digest({"rows": records, "lesson_context": context}),
            "rows": records, "lesson_context": context}
    path = root / "inputs" / (date + ".json")
    if not path.exists():
        write_once(path, snap)
    # First snapshot is immutable; later heals are separate and never qualify retroactively.
    elif json.loads(path.read_text())["input_hash"] != snap["input_hash"]:
        rebuilt = root / "reconstructions" / date / (snap["input_hash"] + ".json")
        if not rebuilt.exists():
            write_once(rebuilt, snap)
    return snap


def _offset(date):
    from zoneinfo import ZoneInfo
    return datetime.fromisoformat(date + "T09:30:00").replace(
        tzinfo=ZoneInfo("America/New_York")).isoformat()[-6:]
