"""Keep usable pre-open artifacts. Resolve off-name files. Salvage JSON.

Rules the one-stop runner must obey:
  1. Read the file as text. Garbled / empty / timeout = trash.
  2. Retry the *failed step*, never abort the whole day because a sibling
     is thin.
  3. Never overwrite a usable file with a stub (bootstrap, timeout, empty).
  4. If the canonical filename is missing, look for files that match the
     job (same date + research/captain/map_heat/events/…).
"""
from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

KIND_PATTERNS: dict[str, list[str]] = {
    "general_predict": [
        "01_daily/general/{date}_predict.md",
        "01_daily/general/{date}*predict*.md",
    ],
    "events": [
        "01_daily/events/{date}_events.json",
        "01_daily/events/{date}*events*.json",
    ],
    "news_judge": [
        "01_daily/news/{date}_judge.md",
        "01_daily/news/{date}*judge*.md",
    ],
    "news_parse": [
        "01_daily/news/{date}_parsed.json",
        "01_daily/news/{date}*parsed*.json",
        "01_daily/news/{date}*parse*.json",
    ],
    "news_actions": [
        "01_daily/news/{date}_actions.json",
        "01_daily/news/{date}*actions*.json",
    ],
    "finviz_digest": [
        "01_daily/news/{date}_finviz_digest.json",
        "01_daily/news/{date}_finviz_digest.md",
        "01_daily/news/{date}*finviz*.json",
        "01_daily/news/{date}*finviz*.md",
    ],
    "map_heat": [
        "01_daily/map_heat/{date}_map_heat.json",
        "01_daily/map_heat/{date}*map_heat*.json",
    ],
    "map_heat_baseline": [
        "01_daily/map_heat/{date}_research_baseline.json",
        "01_daily/map_heat/{date}*baseline*.json",
        "01_daily/map_heat/{date}*postclose*.json",
    ],
    "map_heat_research": [
        "01_daily/map_heat/{date}_research.md",
        "01_daily/map_heat/{date}_research.json",
        "01_daily/map_heat/{date}*research*.md",
        "01_daily/map_heat/{date}*research*.json",
        "01_daily/map_heat/{date}*captain*.md",
        "01_daily/map_heat/{date}*captain*.json",
    ],
}

PATH_KIND_HINTS: list[tuple[str, str]] = [
    ("research_baseline", "map_heat_baseline"),
    ("_research", "map_heat_research"),
    ("captain", "map_heat_research"),
    ("map_heat", "map_heat"),
    ("finviz_digest", "finviz_digest"),
    ("_parsed", "news_parse"),
    ("_judge", "news_judge"),
    ("_actions", "news_actions"),
    ("/events/", "events"),
    ("general/", "general_predict"),
    ("_predict.md", "general_predict"),
    ("/sectors/", "sector_predict"),
]


def salvage_json(text: str):
    """Best-effort parse of slightly garbled LLM JSON."""
    if not (text or "").strip():
        return None
    blob = text.strip()
    fence = re.search(r"```(?:json)?\s*(.*?)```", blob, re.S)
    if fence:
        blob = fence.group(1).strip()
    start, end = blob.find("{"), blob.rfind("}")
    if start >= 0 and end > start:
        blob = blob[start:end + 1]
    blob = re.sub(r",\s*([}\]])", r"\1", blob)
    blob = blob.replace("\u201c", '"').replace("\u201d", '"').replace("\u2018", "'").replace("\u2019", "'")
    try:
        return json.loads(blob)
    except (ValueError, json.JSONDecodeError):
        pass
    try:
        obj, _ = json.JSONDecoder().raw_decode(blob)
        return obj
    except (ValueError, json.JSONDecodeError):
        pass
    try:
        return json.loads(re.sub(r"'([^']*)'", r'"\1"', blob))
    except (ValueError, json.JSONDecodeError):
        return None


def looks_garbled_json(text: str) -> bool:
    if not (text or "").strip():
        return True
    s = text.strip()
    if s[0] not in "{[":
        if "{" not in s:
            return False
    return salvage_json(s) is None and s.lstrip().startswith(("{", "["))


def _read(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""


def _load_json(path: Path):
    raw = _read(path)
    if not raw.strip():
        return None
    try:
        return json.loads(raw)
    except (ValueError, json.JSONDecodeError):
        return salvage_json(raw)


def resolve_artifact(kind: str, date: str, preferred: str | Path | None = None,
                     root: Path | None = None) -> Path | None:
    """Return the best existing file for this job+date, even if renamed."""
    root = root or ROOT
    candidates: list[Path] = []
    if preferred:
        candidates.append(root / preferred if not Path(preferred).is_absolute()
                          else Path(preferred))
    for pat in KIND_PATTERNS.get(kind, []):
        glob = pat.format(date=date)
        candidates.extend(sorted(root.glob(glob)))
    seen = set()
    hits: list[Path] = []
    for p in candidates:
        try:
            rp = p.resolve()
        except OSError:
            rp = p
        if rp in seen or not p.exists() or not p.is_file():
            continue
        if p.stat().st_size < 40:
            continue
        seen.add(rp)
        hits.append(p)
    if not hits:
        return None
    def score(p: Path) -> tuple:
        name = p.name
        canonical = 1 if preferred and p.name == Path(preferred).name else 0
        return (canonical, p.stat().st_mtime, p.stat().st_size, name)
    hits.sort(key=score, reverse=True)
    return hits[0]


def research_usable(path: str | Path | None = None, data: dict | None = None,
                    text: str = "") -> bool:
    """True when a first-print or refresh packet is worth keeping."""
    p = Path(path) if path else None
    if data is None and p is not None:
        js = p if p.suffix == ".json" else Path(str(p).replace("_research.md", "_research.json"))
        if not js.exists() and p is not None and p.parent.exists():
            date_m = re.match(r"(\d{4}-\d{2}-\d{2})", p.name)
            if date_m:
                for cand in p.parent.glob(f"{date_m.group(1)}*research*.json"):
                    js = cand
                    break
        data = _load_json(js) if js.exists() else None
        if not text and p.exists() and p.suffix != ".json":
            text = _read(p)
    if data is None or not isinstance(data, dict):
        return False
    phase = str(data.get("phase") or "")
    cards = data.get("cards") or []
    if phase == "morning_bootstrap" and len(cards) < 3:
        return False
    if len(cards) < 3:
        return False
    n_caps = 0
    for c in cards:
        if not isinstance(c, dict):
            continue
        caps = c.get("captains") or []
        n_caps += sum(1 for x in caps if isinstance(x, dict) and x.get("ticker"))
        if c.get("industry") and not caps:
            n_caps += 1
    if n_caps < 3:
        return False
    if text and ("LLM request timed out" in text or "idle timeout" in text.lower()):
        if len(text) < 2500:
            return False
    return True


def research_quality(path: str | Path | None = None, data: dict | None = None) -> str:
    """strict | usable | stub | missing"""
    if data is None and path:
        p = Path(path)
        js = p if p.suffix == ".json" else Path(str(p).replace("_research.md", "_research.json"))
        data = _load_json(js) if js.exists() else None
    if not isinstance(data, dict):
        return "missing"
    cards = data.get("cards") or []
    phase = str(data.get("phase") or "")
    if phase == "morning_bootstrap" and len(cards) < 3:
        return "stub"
    if phase == "morning_refresh" and len(cards) >= 20:
        return "strict"
    if research_usable(data=data):
        return "usable"
    if len(cards) == 0:
        return "stub"
    return "stub"


def should_overwrite(dest: Path, src: Path | None = None) -> bool:
    """False when dest is usable and src is stub/smaller/bootstrap."""
    if not dest.exists():
        return True
    dest_kind = _kind_from_path(str(dest))
    if dest_kind == "map_heat_research":
        dest_q = research_quality(dest)
        src_q = research_quality(src) if src and src.exists() else "missing"
        rank = {"strict": 3, "usable": 2, "stub": 1, "missing": 0}
        if rank[dest_q] > rank[src_q]:
            return False
        if dest_q in ("strict", "usable") and src_q == dest_q:
            return False
        return rank[src_q] > rank[dest_q]
    try:
        dest_sz = dest.stat().st_size
    except OSError:
        return True
    if src is None or not src.exists():
        return False
    try:
        src_sz = src.stat().st_size
    except OSError:
        return False
    src_txt = _read(src)
    if dest_sz > 400 and src_sz < 200:
        return False
    if "BOOTSTRAP" in src_txt and "BOOTSTRAP" not in _read(dest):
        return False
    return True


def _kind_from_path(path: str) -> str:
    low = path.replace("\\", "/").lower()
    for needle, kind in PATH_KIND_HINTS:
        if needle in low:
            return kind
    return ""


def step_for_path(path: str) -> str | None:
    kind = _kind_from_path(path)
    if kind:
        return kind
    if path in ("(review)", ""):
        return "grok_review"
    return None


def promote_to_canonical(kind: str, date: str, found: Path, root: Path | None = None) -> Path:
    """Copy an off-name file to the canonical name without clobbering better files."""
    root = root or ROOT
    pats = KIND_PATTERNS.get(kind) or []
    canonical_rel = next(
        (pat for pat in pats if Path(pat).suffix == found.suffix),
        pats[0] if pats else None,
    )
    if not canonical_rel:
        return found
    dest = root / canonical_rel.format(date=date)
    if dest.resolve() == found.resolve():
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and not should_overwrite(dest, found):
        return dest
    dest.write_bytes(found.read_bytes())
    if found.suffix == ".json":
        md_src = found.with_suffix(".md")
        md_dest = dest.with_suffix(".md")
        if md_src.exists() and (not md_dest.exists() or should_overwrite(md_dest, md_src)):
            md_dest.write_bytes(md_src.read_bytes())
    return dest
