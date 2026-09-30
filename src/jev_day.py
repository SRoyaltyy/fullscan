"""Calendar-day draw for the Jev trainer. Mixed draw stays in jev_train."""
from __future__ import annotations

import datetime as dt
import json
import re
from pathlib import Path

from .jev_eval import _digest_items, _parsed_items, _row as _eval_row, title_id
from .jev_gate import NEWS_DIR, ROOT, _load_json, _write_json
from .jev_bits import collapse_dupes

DAY_CAP = 500
DAY_RE = re.compile(r"^20\d{2}-\d{2}-\d{2}$")
_SECRET_RE = re.compile(
    r"ghp_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,}|