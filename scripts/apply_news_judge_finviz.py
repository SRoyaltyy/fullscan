#!/usr/bin/env python3
from pathlib import Path
import base64
ROOT = Path(__file__).resolve().parent.parent
CHUNKS = []
# placeholder - will be filled
data = base64.b64decode("".join(CHUNKS))
out = ROOT / "src/run_news_judge.py"
out.write_bytes(data)
print(f"wrote {out} ({len(data)} bytes)")
