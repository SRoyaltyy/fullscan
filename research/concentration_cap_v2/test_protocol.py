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
    BASE_IDS,
    CAPS,
    CAP_CASH,
    LUCK_N,
    N_CANDIDATES,
    PREREG,
    PRIOR,
    SHARE_MAX,
    TRIES,
    WIDTHS,
    candidates,
    cap_label,
    fingerprint_sha256,
)
from research.factor_mine_recipe_search_v4.protocol import (  # noqa: E402
    FORWARD,
    STARTS,
    TUNE,
    candidates as v4_candidates,
)


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
    rows = list(candidates())
    if len(rows) != 84 or N_CANDIDATES != 84 or TRIES != 84:
        raise SystemExit("try count")
    if V1_TRIES != 45 or PRIOR != 21_796:
        raise SystemExit("prior")
    if LUCK_N != 84 + 45 + 21_796 or LUCK_N != 21_925:
        raise SystemExit("luck")
    ids = [row["id"] for row in rows]
    if ids != [
        f"{base}__n{width}__c{cap_label(cap)}"
        for base in BASE_IDS
        for width in WIDTHS
        for cap in CAPS
    ]:
        raise SystemExit("id order")
    if WIDTHS != (4, 6, 8, 10) or CAPS != (0.20, 0.25, None) or SHARE_MAX != 0.20:
        raise SystemExit("grid")
    if CAP_CASH != "sit":
        raise SystemExit("cash")
    v4 = {row["id"]: row for row in v4_candidates()}
    for row in rows:
        base = v4[row["base_id"]]
        for key in ("hold", "s_boost", "weather", "rank", "sell", "universe", "side", "forbid"):
            if row[key] != base[key]:
                raise SystemExit(f"gate drift {row['id']} {key}")
    ret = [row for row in rows if row["base_id"] == "union_ret_5_h3__w1"]
    if not ret or ret[0]["rank"] != "ret_5" or ret[0]["hold"] != 3 or ret[0]["weather"] is not True:
        raise SystemExit("ret_5")
    if TUNE[-1] != "2026-09-11" or FORWARD[0] != "2026-09-14" or FORWARD[-1] != "2026-09-25":
        raise SystemExit("windows")
    if STARTS != ("2026-08-17", "2026-08-24", "2026-08-31"):
        raise SystemExit("starts")
    v1 = V1_PREREG.read_text(encoding="utf-8")
    if v1_fingerprint(v1) != "c3d52476b6ea9c4ab248a4b8deaa52803a98aa0359397ce19dfa71e4d6e93987":
        raise SystemExit("v1 prereg changed")
    for phrase in ("under 20%", "20% or more", "21,925", "union_ret_5_h3", "sits in cash", "fwd_ccap2_"):
        if phrase not in text:
            raise SystemExit(f"missing {phrase}")
    print("ok", fingerprint_sha256(text))


if __name__ == "__main__":
    main()
