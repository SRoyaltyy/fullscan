#!/usr/bin/env python3
"""One-shot: decode staged .b64 patches and write the real source files."""
from pathlib import Path
import base64
import subprocess
import sys

ROOT = Path(__file__).resolve().parent.parent
PATCHES = {
    "src/industry_predict.py": ROOT / "scripts" / "_patch_industry_predict.b64",
    "src/run_news_judge.py": ROOT / "scripts" / "_patch_news_judge.b64",
}

def main() -> int:
    changed = []
    for target, b64path in PATCHES.items():
        if not b64path.exists():
            print(f"missing {b64path}")
            continue
        data = base64.b64decode(b64path.read_text().strip())
        out = ROOT / target
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_bytes(data)
        print(f"wrote {target} ({len(data)} bytes)")
        changed.append(target)
    if not changed:
        print("nothing to apply")
        return 1
    # self-clean the staging files so they don't linger
    for p in PATCHES.values():
        if p.exists():
            p.unlink()
            print(f"removed {p.name}")
    apply_self = Path(__file__)
    print("done — commit the written sources")
    return 0

if __name__ == "__main__":
    sys.exit(main())
