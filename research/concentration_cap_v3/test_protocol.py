"""Lock check. Does not walk a book and does not read a return."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.concentration_cap_v1.protocol import (  # noqa: E402
    PREREG as V1_PREREG,
    TRIES as V1_TRIES,
    fingerprint_sha256 as v1_fingerprint,
)
from research.concentration_cap_v2.protocol import (  # noqa: E402
    PREREG as V2_PREREG,
    TRIES as V2_TRIES,
    candidates as v2_candidates,
    fingerprint_sha256 as v2_fingerprint,
)
from research.concentration_cap_v3.protocol import (  # noqa: E402
    DEPENDENCE_MAX,
    KEEP_FRAC,
    LUCK_N,
    N_CANDIDATES,
    PREREG,
    PRIOR,
    TRIES,
    candidates,
    dependence,
    fingerprint_sha256,
    share_line_passes,
    top3_passes,
)
from research.factor_mine_recipe_search_v4.protocol import FORWARD, STARTS, TUNE  # noqa: E402


def _header(text: str, key: str) -> str:
    for line in text.splitlines():
        if line.startswith(f"- {key}:"):
            return line.split(":", 1)[1].strip().strip("`")
    raise SystemExit(f"missing {key}")


def main() -> None:
    text = PREREG.read_text(encoding="utf-8")
    if fingerprint_sha256(text) != _header(text, "fingerprint_sha256"):
        raise SystemExit("fingerprint mismatch")
    if "PLACEHOLDER" in text:
        raise SystemExit("placeholder")
    if len(candidates()) != 84 or N_CANDIDATES != 84 or TRIES != 84:
        raise SystemExit("try count")
    if V1_TRIES != 45 or V2_TRIES != 84 or PRIOR != 21_796:
        raise SystemExit("prior")
    if LUCK_N != 84 + 84 + 45 + 21_796 or LUCK_N != 22_009:
        raise SystemExit("luck")
    if [row["id"] for row in candidates()] != [row["id"] for row in v2_candidates()]:
        raise SystemExit("grid drift")
    if DEPENDENCE_MAX != 0.20 or KEEP_FRAC != 0.8:
        raise SystemExit("line")
    # Cyrus: 33% to about 13% is about 60% of the gain, and that fails the line.
    cyrus = dependence(0.33, 0.13)
    if cyrus is None or abs(cyrus - (1.0 - 0.13 / 0.33)) > 1e-12:
        raise SystemExit("cyrus dependence")
    if share_line_passes(0.33, 0.13):
        raise SystemExit("cyrus case passed")
    if not share_line_passes(0.25, 0.20):
        raise SystemExit("R_-1 == 0.8 * R failed")
    if share_line_passes(0.25, 0.20 - 1e-9):
        raise SystemExit("R_-1 under 0.8 * R passed")
    if share_line_passes(0.0, 0.0) or share_line_passes(-0.05, -0.01):
        raise SystemExit("non-positive R passed")
    if dependence(0.0, 0.0) is not None:
        raise SystemExit("zero R has a dependence")
    if not top3_passes(0.01) or top3_passes(0.0) or top3_passes(-0.01):
        raise SystemExit("top3")
    if TUNE[-1] != "2026-09-11" or FORWARD[0] != "2026-09-14" or FORWARD[-1] != "2026-09-25":
        raise SystemExit("windows")
    if STARTS != ("2026-08-17", "2026-08-24", "2026-08-31"):
        raise SystemExit("starts")
    v1 = V1_PREREG.read_text(encoding="utf-8")
    if v1_fingerprint(v1) != "c3d52476b6ea9c4ab248a4b8deaa52803a98aa0359397ce19dfa71e4d6e93987":
        raise SystemExit("v1 prereg changed")
    v2 = V2_PREREG.read_text(encoding="utf-8")
    if v2_fingerprint(v2) != "dea5dbf2b7f85660649ad750aa2b22dbb8b237319ab9f59e48892ffa76903b97":
        raise SystemExit("v2 prereg changed")
    for phrase in (
        "R > 0 AND R_-1 >= 0.8 * R",
        "dependence = 1 - R_-1/R < 20%",
        "gross share",
        "R_-3 > 0",
        "22,009",
        "union_ret_5_h3",
        "sits in cash",
        "fwd_ccap3_",
        "60% of gains",
    ):
        if phrase not in text:
            raise SystemExit(f"missing {phrase}")
    print("ok", fingerprint_sha256(text))


if __name__ == "__main__":
    main()
