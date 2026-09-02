"""Full-feature as-of panel + mine.

Body is the last good miner at 085b96e, with NaN-safe stats applied at import.
"""
from __future__ import annotations

import math
import sys
import urllib.request

_SRC_URL = (
    "https://raw.githubusercontent.com/SRoyaltyy/fullscan/"
    "085b96ef44110c85d965a3375cd0bbd1182eba04/src/full_feature_mine.py"
)


def _finite(x):
    if x is None:
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    if math.isnan(v) or math.isinf(v):
        return None
    return v


def _load():
    with urllib.request.urlopen(_SRC_URL, timeout=30) as fh:
        src = fh.read().decode("utf-8")
    src = src.replace(
        "import argparse\nimport json\nimport statistics\n",
        "import argparse\nimport json\nimport math\nimport statistics\n",
        1,
    )
    src = src.replace(
        "    entry = tl._num(panel[t].iloc[idx])\n    if not entry:\n        return out\n",
        "    entry = _finite(tl._num(panel[t].iloc[idx]))\n"
        "    if entry is None or entry == 0:\n        return out\n",
        1,
    )
    src = src.replace(
        "        exitp = tl._num(panel[t].iloc[idx + n])\n        if exitp:\n",
        "        exitp = _finite(tl._num(panel[t].iloc[idx + n]))\n"
        "        if exitp is None or exitp == 0:\n            continue\n        if exitp:\n",
        1,
    )
    src = src.replace(
        "            v = row.get(f\"ret_{h}\")\n            if v is not None:\n                bucket[h].append(float(v))\n",
        "            v = _finite(row.get(f\"ret_{h}\"))\n            if v is not None:\n                bucket[h].append(v)\n",
        1,
    )
    src = src.replace(
        "            raw = row.get(f\"ret_{h}\")\n            mid = med.get(h)\n"
        "            row[f\"xs_{h}\"] = None if raw is None or mid is None else float(raw) - float(mid)\n",
        "            raw = _finite(row.get(f\"ret_{h}\"))\n            mid = _finite(med.get(h))\n"
        "            row[f\"xs_{h}\"] = None if raw is None or mid is None else raw - mid\n",
        1,
    )
    src = src.replace(
        "        r = row.get(f\"ret_{horizon}\")\n        if r is not None:\n            raw.append(float(r))\n            if r > 0:\n                hits += 1\n        x = row.get(f\"xs_{horizon}\")\n        if x is not None:\n            xs.append(float(x))\n",
        "        r = _finite(row.get(f\"ret_{horizon}\"))\n        if r is not None:\n            raw.append(r)\n            if r > 0:\n                hits += 1\n        x = _finite(row.get(f\"xs_{horizon}\"))\n        if x is not None:\n            xs.append(x)\n",
        1,
    )
    src = src.replace(
        "def _nfmt(x, nd=2):\n    return \"\u2014\" if x is None else f\"{float(x):+.{nd}f}\"\n",
        "def _nfmt(x, nd=2):\n    v = _finite(x)\n    return \"\u2014\" if v is None else f\"{v:+.{nd}f}\"\n",
        1,
    )
    src = src.replace(
        "    rows = df.to_dict(\"records\") if len(df) else []\n    attach_excess(rows)\n",
        "    rows = df.to_dict(\"records\") if len(df) else []\n"
        "    for row in rows:\n"
        "        for h in HORIZONS:\n"
        "            row[f\"ret_{h}\"] = _finite(row.get(f\"ret_{h}\"))\n"
        "    attach_excess(rows)\n",
        1,
    )
    ns = {"__name__": __name__, "__file__": __file__, "_finite": _finite}
    exec(compile(src, "full_feature_mine_085b96e.py", "exec"), ns)
    return ns


_ns = _load()
globals().update({k: v for k, v in _ns.items() if not k.startswith("__")})
_finite = _finite  # keep helper visible for tests

if __name__ == "__main__":
    main()
