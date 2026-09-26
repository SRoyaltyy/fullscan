"""Pandas-free lock check. Does not import the scorer."""
from __future__ import annotations

import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.factor_mine_recipe_search_v4.protocol import (
    BASE_ID,
    FORWARD,
    LUCK_N,
    MARKER,
    N_CANDIDATES,
    N_PARTS,
    PICKED_ID,
    PREREG,
    STARTS,
    TRIES_364,
    TRIES_366,
    TRIES_367,
    TRIES_368,
    TRIES_THEME,
    TUNE,
    V4_TRIES,
    candidates,
    file_sha256,
    fingerprint_sha256,
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
    if MARKER not in text:
        raise SystemExit("marker")
    if len(list(candidates())) != N_CANDIDATES or N_CANDIDATES != 50:
        raise SystemExit("candidate count")
    if N_PARTS != 25 or V4_TRIES != 150:
        raise SystemExit("try count")
    if TRIES_364 != 102 or TRIES_366 != 1326 or TRIES_367 != 1326:
        raise SystemExit("prior tries")
    if LUCK_N != V4_TRIES + TRIES_368 + TRIES_THEME + TRIES_364 + TRIES_366 + TRIES_367:
        raise SystemExit("luck")
    if LUCK_N != 21536:
        raise SystemExit(f"luck {LUCK_N}")
    if BASE_ID != "union_hot_score_h3__w1" or PICKED_ID != "union_hot_n4_h1__w1":
        raise SystemExit("ids")
    if any(row["side"] != "long" for row in candidates()):
        raise SystemExit("long only")
    if TUNE[-1] != "2026-09-11" or FORWARD[0] != "2026-09-14":
        raise SystemExit("windows")
    if STARTS != ("2026-08-17", "2026-08-24", "2026-08-31"):
        raise SystemExit("starts")
    names = [row["name"] for row in candidates()]
    if len(names) != len(set(row["id"] for row in candidates())):
        raise SystemExit("duplicate id")
    if "keep-held" not in text:
        raise SystemExit("fill model not in the prereg")
    if not any(phrase in text for phrase in ("sell+rebuy", "sell-then-rebuy", "sell plus a buy")):
        raise SystemExit("renewal figure not in the prereg")
    if "union_hot_n4_h1" not in text or "already picked" not in text.lower():
        raise SystemExit("picked note")
    if "9,132" not in text and "9132" not in text:
        raise SystemExit("theme tries")
    inputs = _header(text, "inputs_sha256")
    got = file_sha256(ROOT / "research/factor_mine_recipe_search_v4/INPUTS.json")
    if got != inputs:
        raise SystemExit("inputs sha")
    src = (ROOT / "research/factor_mine_recipe_search_v4/test_protocol.py").read_text(encoding="utf-8")
    tree = ast.parse(src)
    banned = {"pandas", "research.factor_mine_recipe_search_v4.tune",
              "research.factor_mine_recipe_search_v4.forward",
              "research.factor_mine_recipe_search_v4.engine"}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name in banned or alias.name.startswith("pandas"):
                    raise SystemExit(f"import {alias.name}")
        if isinstance(node, ast.ImportFrom) and node.module in banned:
            raise SystemExit(f"from {node.module}")
    print("factor_mine_recipe_search_v4 protocol ok")


if __name__ == "__main__":
    main()
