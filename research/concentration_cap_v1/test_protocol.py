"""Lock check. Does not walk a book and does not read a return."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.concentration_cap_v1.protocol import (  # noqa: E402
    BASE_IDS,
    CAPS,
    CAP_CASH,
    CLEAN_SHA256,
    FEES_SHA256,
    FORWARD,
    LUCK_N,
    N_CANDIDATES,
    PREREG,
    PRIOR_SCREEN,
    PRIOR_V4,
    SPLIT_SHA256,
    STARTS,
    TRIES,
    TUNE,
    WIDTHS,
    candidates,
    cap_label,
    file_sha256,
    fingerprint_sha256,
)
from research.factor_mine_recipe_search_v4.protocol import (  # noqa: E402
    CLEAN_SHA256 as V4_CLEAN,
    FEES_SHA256 as V4_FEES,
    FORWARD as V4_FORWARD,
    INPUTS,
    LUCK_N as V4_LUCK,
    SPLIT_SHA256 as V4_SPLIT,
    STARTS as V4_STARTS,
    TUNE as V4_TUNE,
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
        raise SystemExit("placeholder left in the prereg")
    rows = list(candidates())
    if len(rows) != 45 or N_CANDIDATES != 45 or TRIES != 45:
        raise SystemExit("try count")
    if LUCK_N != 45 + 21_536 + 260 or PRIOR_V4 != 21_536 or PRIOR_SCREEN != 260:
        raise SystemExit("luck")
    if V4_LUCK != 21_536:
        raise SystemExit("v4 luck drifted")
    if LUCK_N != TRIES + PRIOR_V4 + PRIOR_SCREEN:
        raise SystemExit("luck sum")
    ids = [row["id"] for row in rows]
    if len(ids) != len(set(ids)):
        raise SystemExit("duplicate id")
    expected = []
    for base_id in BASE_IDS:
        for width in WIDTHS:
            for cap in CAPS:
                expected.append(f"{base_id}__n{width}__c{cap_label(cap)}")
    if ids != expected:
        raise SystemExit("id order")
    if WIDTHS != (4, 6, 8) or CAPS != (0.20, 0.25, None):
        raise SystemExit("grid")
    if CAP_CASH != "sit":
        raise SystemExit("cash rule")
    v4 = {row["id"]: row for row in v4_candidates()}
    for row in rows:
        base = v4[row["base_id"]]
        for key in ("hold", "s_boost", "weather", "rank", "sell", "universe", "side", "forbid"):
            if row[key] != base[key]:
                raise SystemExit(f"base gate drift {row['id']} {key}")
        if row["top_n"] not in WIDTHS:
            raise SystemExit("width")
        if row["weight_cap"] not in CAPS:
            raise SystemExit("cap")
        if row["cap_cash"] != "sit" or row["name"] != row["id"]:
            raise SystemExit("name")
        if row["source_name"] != base["name"]:
            raise SystemExit("source")
    if TUNE != V4_TUNE or FORWARD != V4_FORWARD or STARTS != V4_STARTS:
        raise SystemExit("windows")
    if TUNE[-1] != "2026-09-11" or FORWARD[0] != "2026-09-14" or FORWARD[-1] != "2026-09-25":
        raise SystemExit("window ends")
    if STARTS != ("2026-08-17", "2026-08-24", "2026-08-31"):
        raise SystemExit("starts")
    if file_sha256(ROOT / "research/factor_mine_recipe_search_v4/INPUTS.json") != _sha_line(text, "INPUTS.json"):
        raise SystemExit("inputs sha")
    if file_sha256(INPUTS) != "0ed63996e5a02fc5190785aa15f001ce6ed79ec1bb7eb5348d04f59d4f659969":
        raise SystemExit("inputs pin")
    if CLEAN_SHA256 != V4_CLEAN or SPLIT_SHA256 != V4_SPLIT or FEES_SHA256 != V4_FEES:
        raise SystemExit("tape pin")
    for phrase in (
        "sits in cash",
        "size_hot4_tickets",
        "keep-held",
        "21,841",
        "concentration_screen_v1",
        "fwd_ccap_",
        "2026-09-28",
    ):
        if phrase not in text:
            raise SystemExit(f"prereg missing {phrase}")
    print("ok", fingerprint_sha256(text))


def _sha_line(text: str, label: str) -> str:
    for line in text.splitlines():
        if label in line and "sha256" in line:
            return line.split("sha256 `", 1)[1].split("`", 1)[0]
    raise SystemExit(f"missing sha line {label}")


if __name__ == "__main__":
    main()
