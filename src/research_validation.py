"""Point-in-time evidence and deterministic promotion gates (stdlib only).

Overlapping cash starts are descriptive, never independent validation samples.
Missing provenance, immature outcomes and unknown execution are not successes.
"""
from __future__ import annotations

import hashlib
import json
import math
import random
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean


def digest(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


def timestamp(value: str) -> datetime:
    dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError("timestamp requires timezone")
    return dt.astimezone(timezone.utc)


def clock_errors(record: dict, *, outcome: bool = False) -> list[str]:
    errors = []
    try:
        decision = timestamp(record["decision_at"])
        available = timestamp(record["available_at"])
        observed = timestamp(record["observed_at"])
        if available > decision or observed > decision:
            errors.append("input arrived after decision")
        if not record.get("input_hash"):
            errors.append("missing input hash")
        if outcome and timestamp(record["outcome_at"]) <= decision:
            errors.append("outcome must follow decision")
    except (KeyError, TypeError, ValueError):
        errors.append("missing or invalid timestamp provenance")
    return errors


def write_once(path: Path, value: dict) -> None:
    """No silent historical rewrites; identical retries are idempotent."""
    path.parent.mkdir(parents=True, exist_ok=True)
    body = json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n"
    try:
        with path.open("x", encoding="utf-8") as out:
            out.write(body)
    except FileExistsError:
        if json.loads(path.read_text()) != value:
            raise ValueError(f"immutable record conflict: {path}")


def block_interval(values: list[float], *, block: int = 5, trials: int = 1000,
                   seed: int = 7) -> tuple[float, float] | None:
    """Circular moving-block bootstrap; input is ONE value per session."""
    if len(values) < 2 or not all(math.isfinite(v) for v in values):
        return None
    rng = random.Random(seed)
    n = len(values)
    estimates = []
    for _ in range(trials):
        sample = []
        while len(sample) < n:
            start = rng.randrange(n)
            sample.extend(values[(start + j) % n] for j in range(min(block, n)))
        estimates.append(mean(sample[:n]))
    estimates.sort()
    return estimates[int(.025 * trials)], estimates[min(trials - 1, int(.975 * trials))]


def promotion_evidence(rows: list[dict], *, frozen_at: str, asof: str,
                       candidate_hash: str, min_sessions: int = 60,
                       min_blocks: int = 12, block: int = 5) -> dict:
    """Paired, prospective, after-cost portfolio increments in return units.

Each row must be one session with a fully observed baseline and challenger.
The caller must freeze the challenger before any evaluation decision, and
persist both policies' execution paths. Trigger returns are not causal uplift.
"""
    frozen, end = timestamp(frozen_at), timestamp(asof)
    accepted, rejected, seen = [], [], set()
    for r in sorted(rows, key=lambda x: x.get("decision_at", "")):
        why = clock_errors(r, outcome=True)
        try:
            decision, outcome = timestamp(r["decision_at"]), timestamp(r["outcome_at"])
            if decision <= frozen or outcome > end:
                why.append("not prospective or outcome not mature")
        except (KeyError, TypeError, ValueError):
            why.append("invalid evaluation clock")
        if r.get("candidate_hash") != candidate_hash:
            why.append("different candidate version")
        if r.get("execution_verified") is not True:
            why.append("unverified execution/costs")
        date = r.get("session")
        if not date or date in seen:
            why.append("missing or duplicate session")
        try:
            baseline, challenger = float(r["baseline_net"]), float(r["challenger_net"])
            if not all(math.isfinite(x) for x in (baseline, challenger)):
                raise ValueError()
        except (KeyError, TypeError, ValueError):
            why.append("invalid paired returns")
        if why:
            rejected.append({"session": date, "reasons": why})
        else:
            seen.add(date)
            accepted.append((challenger - baseline, challenger))
    uplift = [x[0] for x in accepted]
    ci = block_interval(uplift, block=block)
    sufficient = len(accepted) >= min_sessions and len(accepted) // block >= min_blocks
    # Rejected evidence never silently disappears from a promotion decision.
    passed = bool(sufficient and not rejected and ci and ci[0] > 0
                  and mean(x[1] for x in accepted) > 0)
    return {"status": "eligible" if passed else "shadow",
            "n_sessions": len(accepted), "independent_blocks": len(accepted) // block,
            "mean_uplift": mean(uplift) if uplift else None,
            "uplift_interval": ci, "rejected": rejected,
            "reason": "positive prospective paired evidence" if passed else
            "insufficient, invalid, or non-positive prospective evidence"}


def start_diagnostics(starts: list[dict]) -> dict:
    values = [float(x["total_ret_pct"]) for x in starts
              if x.get("total_ret_pct") is not None]
    return {"n_starts": len(values),
            "positive_fraction": sum(v > 0 for v in values) / len(values) if values else None,
            "worst_start_pct": min(values) if values else None,
            "decreasing_fraction": sum(b <= a for a, b in zip(values, values[1:])) /
            (len(values) - 1) if len(values) > 1 else None,
            "independent_evidence": False,
            "note": "Overlapping start-to-end windows; not proof across market regimes."}
