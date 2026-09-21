"""Working memory for flash-model chains. Not a prediction tree."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

SCRATCH_DIR = Path("02_lessons/lane/scratch_impact")


def article_id(title: str, known_at: str = "") -> str:
    raw = f"{(title or '').strip().lower()}|{known_at}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def path_for(aid: str) -> Path:
    SCRATCH_DIR.mkdir(parents=True, exist_ok=True)
    return SCRATCH_DIR / f"{aid}.json"


def load(aid: str) -> dict | None:
    p = path_for(aid)
    if not p.is_file():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def save(row: dict) -> Path:
    aid = row.get("article_id") or article_id(str(row.get("title") or ""))
    row["article_id"] = aid
    row["updated_at"] = datetime.now(timezone.utc).isoformat()
    p = path_for(aid)
    p.write_text(json.dumps(row, indent=2, ensure_ascii=False), encoding="utf-8")
    return p
