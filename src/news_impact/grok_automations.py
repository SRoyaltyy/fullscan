"""Grok Automations harvest source. Research-only. Not a new taxonomy.

Reads `data/grok_automations/{date}_{slug}.json` when present.
Empty / missing directory is a valid zero — never crash.

Daily refresh (GH Actions tokens cannot call the Automations API):
  1. Bot / Cursor ingests Gmail from noreply@x.ai Automation mails, OR
     `automation_get_results` when an X/Grok connector exists.
  2. Run `python3 -m scripts.ingest_grok_automations` (or this helper).
  3. Commit the dated dump. The harvest loader picks it up next restamp.

13-questions (7f250154-…) is macro-only: factor_impulse / regime, no tickers.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Iterable

GROK_DIR = Path("data/grok_automations")
SOURCE = "grok_automations"
_DATE_IN_NAME = re.compile(r"(\d{4}-\d{2}-\d{2})")

KNOWN_TASKS: dict[str, dict[str, Any]] = {
    "23a24524-4eaa-4b9e-bd39-5b30e2ec74ae": {
        "slug": "hype-factor",
        "kind": "hype-factor",
        "macro_only": False,
    },
    "5b4f01c3-fe5b-463a-a270-8bc9def8e26f": {
        "slug": "google-news-prompt",
        "kind": "google-news-prompt",
        "macro_only": False,
    },
    "7f250154-6401-4836-b5ff-5862211c5468": {
        "slug": "13-questions",
        "kind": "13-questions",
        "macro_only": True,
    },
}

_MACRO_SLUGS = frozenset({
    "13-questions", "13_questions", "thirteen-questions", "13questions",
})
_FINVIZ = re.compile(r"(?i)(finviz|elite_news|digest)")


def task_meta(task_id: str | None = None, slug: str | None = None) -> dict[str, Any]:
    tid = str(task_id or "").strip()
    if tid in KNOWN_TASKS:
        return dict(KNOWN_TASKS[tid], task_id=tid)
    sl = str(slug or "").strip().lower()
    for kid, meta in KNOWN_TASKS.items():
        if meta["slug"] == sl or meta["kind"] == sl:
            return dict(meta, task_id=kid)
    return {
        "task_id": tid,
        "slug": sl or "unknown",
        "kind": sl or "unknown",
        "macro_only": sl in _MACRO_SLUGS,
    }


def is_macro_only(
    task_id: str | None = None,
    slug: str | None = None,
    kind: str | None = None,
    art: dict | None = None,
) -> bool:
    if art:
        task_id = task_id or art.get("task_id") or art.get("automation_task_id")
        slug = slug or art.get("automation_slug") or art.get("slug")
        kind = kind or art.get("automation_kind") or art.get("kind")
    meta = task_meta(task_id, slug or kind)
    if meta.get("macro_only"):
        return True
    token = str(slug or kind or "").strip().lower()
    return token in _MACRO_SLUGS


def is_grok_automations(art: dict | None) -> bool:
    if not art:
        return False
    src = str(art.get("harvest_source") or art.get("source") or "").strip().lower()
    return src in {SOURCE, "grok_automation"}


def harvest_source_rank(art: dict | None) -> int:
    """Higher = preferred when the same fact appears.

    Automations headline ranks above a Finviz wrap of the same title.
    """
    if not art:
        return 0
    hs = str(art.get("harvest_source") or art.get("source") or "").strip().lower()
    if hs in {SOURCE, "grok_automation"}:
        return 90
    if hs in {"parsed", "events", "actions_keep"}:
        return 40
    if _FINVIZ.search(hs):
        return 5
    return 20


def _date_of(path: Path) -> str:
    m = _DATE_IN_NAME.search(path.name)
    return m.group(1) if m else ""


def _as_str(raw: Any) -> str:
    if raw is None:
        return ""
    return str(raw).strip()


def _items_from_blob(blob: Any) -> list[dict]:
    """Flexible: {items|results|output}, a list, or a single row."""
    if blob is None:
        return []
    if isinstance(blob, list):
        return [it for it in blob if isinstance(it, dict)]
    if not isinstance(blob, dict):
        return []
    for key in ("items", "results", "headlines", "output", "data"):
        rows = blob.get(key)
        if isinstance(rows, list):
            return [it for it in rows if isinstance(it, dict)]
        if isinstance(rows, dict):
            inner = rows.get("items") or rows.get("results") or []
            if isinstance(inner, list):
                return [it for it in inner if isinstance(it, dict)]
    if any(_as_str(blob.get(k)) for k in ("headline", "title", "text")):
        return [blob]
    return []


def _article_from_item(
    it: dict,
    *,
    path: Path,
    blob: dict | None,
    file_date: str,
) -> dict | None:
    title = _as_str(
        it.get("headline") or it.get("title") or it.get("text") or it.get("question")
    )
    if not title:
        return None
    task_id = _as_str(
        it.get("task_id") or (blob or {}).get("task_id") or ""
    )
    slug = _as_str(
        it.get("slug") or (blob or {}).get("slug") or ""
    )
    kind = _as_str(
        it.get("kind") or (blob or {}).get("kind") or slug
    )
    if not slug:
        # filename {date}_{slug}.json
        stem = path.stem
        if file_date and stem.startswith(file_date + "_"):
            slug = stem[len(file_date) + 1:]
        else:
            slug = stem
    meta = task_meta(task_id, slug or kind)
    if not task_id:
        task_id = str(meta.get("task_id") or "")
    if not kind:
        kind = str(meta.get("kind") or slug)
    published = _as_str(
        it.get("published") if it.get("published") is not None
        else it.get("published_at")
    )
    retrieved = _as_str(
        it.get("retrieved")
        or it.get("retrieved_at")
        or it.get("createTime")
        or (blob or {}).get("retrieved")
        or (blob or {}).get("retrieved_at")
        or file_date
    )
    body = _as_str(
        it.get("raw_excerpt") or it.get("excerpt") or it.get("body")
        or it.get("prompt") or it.get("answer")
    )[:800]
    return {
        "title": title,
        "body": body,
        "url": _as_str(it.get("url") or it.get("link")),
        "source": SOURCE,
        "harvest_source": SOURCE,
        "source_file": str(path),
        "published_at": published,
        "retrieved_at": retrieved,
        "known_at": published or retrieved,
        "ticker_hint": "",
        "company": "",
        "sectors": [],
        "macro_themes": [],
        "old_usable": None,
        "old_class": SOURCE,
        "task_id": task_id,
        "automation_slug": str(meta.get("slug") or slug),
        "automation_kind": str(meta.get("kind") or kind),
        "macro_only": bool(meta.get("macro_only") or is_macro_only(
            task_id, slug, kind,
        )),
    }


def load_grok_dumps(
    root: Path | None = None,
    date: str | None = None,
) -> list[dict]:
    """Read every dump. Missing / empty dir → []. Never raises on empty."""
    folder = Path(root) if root is not None else GROK_DIR
    if not folder.is_dir():
        return []
    try:
        paths = sorted(folder.glob("*.json"))
    except OSError:
        return []
    if date and str(date).lower() not in {"all", "*", "history", ""}:
        paths = [p for p in paths if _date_of(p) == date]
    out: list[dict] = []
    for path in paths:
        try:
            raw = path.read_text(encoding="utf-8")
        except OSError:
            continue
        if not raw.strip():
            continue
        try:
            blob = json.loads(raw)
        except json.JSONDecodeError:
            continue
        file_date = _date_of(path)
        header = blob if isinstance(blob, dict) else {}
        for it in _items_from_blob(blob):
            art = _article_from_item(
                it, path=path, blob=header, file_date=file_date,
            )
            if art:
                out.append(art)
    return out


def list_dump_paths(root: Path | None = None) -> list[Path]:
    folder = Path(root) if root is not None else GROK_DIR
    if not folder.is_dir():
        return []
    try:
        return sorted(folder.glob("*.json"))
    except OSError:
        return []


def dump_span(root: Path | None = None) -> tuple[str, str]:
    dates = [_date_of(p) for p in list_dump_paths(root) if _date_of(p)]
    if not dates:
        return "", ""
    return min(dates), max(dates)


def counts(root: Path | None = None) -> dict[str, Any]:
    paths = list_dump_paths(root)
    arts = load_grok_dumps(root)
    by_slug: dict[str, int] = {}
    for a in arts:
        sl = str(a.get("automation_slug") or "unknown")
        by_slug[sl] = by_slug.get(sl, 0) + 1
    earliest, latest = dump_span(root)
    return {
        "n_files": len(paths),
        "n_items": len(arts),
        "by_slug": by_slug,
        "earliest": earliest,
        "latest": latest,
        "status": "used" if paths else "empty",
    }


# --- ingest stub (Gmail plaintext / connector JSON) --------------------------

_SUBJECT_SLUG = re.compile(
    r"(?i)(?:automation[:\s]+|\[automation\]\s*)([a-z0-9][\w-]{1,40})"
)
_HEADLINE_LINE = re.compile(
    r"(?i)^\s*(?:[-*•]\s+|headline[:\s]+|title[:\s]+)?(.+\S)\s*$"
)
_KV = re.compile(
    r"(?i)^(headline|title|source|published|retrieved|task_id|slug|kind|excerpt"
    r"|raw_excerpt|url)\s*[:\-]\s*(.+)$"
)


def parse_connector_results(blob: Any) -> dict[str, Any]:
    """Normalize automation_get_results / connector payload → dump schema."""
    if isinstance(blob, str):
        try:
            blob = json.loads(blob)
        except json.JSONDecodeError:
            return parse_gmail_plaintext(blob)
    header = blob if isinstance(blob, dict) else {}
    items = []
    for it in _items_from_blob(blob):
        headline = _as_str(
            it.get("headline") or it.get("title") or it.get("text")
        )
        if not headline:
            continue
        items.append({
            "headline": headline,
            "source": _as_str(it.get("source") or SOURCE),
            "published": it.get("published") if it.get("published") is not None
            else it.get("published_at"),
            "retrieved": _as_str(
                it.get("retrieved") or it.get("retrieved_at")
                or header.get("retrieved")
            ),
            "task_id": _as_str(it.get("task_id") or header.get("task_id")),
            "raw_excerpt": _as_str(
                it.get("raw_excerpt") or it.get("excerpt") or it.get("body")
            )[:800],
        })
    tid = _as_str(header.get("task_id") or (items[0].get("task_id") if items else ""))
    meta = task_meta(tid, _as_str(header.get("slug") or header.get("kind")))
    return {
        "task_id": tid or meta.get("task_id") or "",
        "slug": _as_str(header.get("slug") or meta.get("slug")),
        "kind": _as_str(header.get("kind") or meta.get("kind")),
        "retrieved": _as_str(header.get("retrieved") or header.get("retrieved_at")),
        "items": items,
    }


def parse_gmail_plaintext(
    text: str,
    *,
    subject: str = "",
    retrieved: str = "",
    task_id: str = "",
    slug: str = "",
) -> dict[str, Any]:
    """Best-effort parse of a noreply@x.ai Automation mail body.

    Looks for `Headline:` / `Source:` blocks, then bullet/plain lines.
    Unknown shape still produces a dump the loader can read (0 items OK).
    """
    subj = subject or ""
    m = _SUBJECT_SLUG.search(subj)
    if m and not slug:
        slug = m.group(1).strip().lower()
    meta = task_meta(task_id, slug)
    slug = slug or str(meta.get("slug") or "")
    tid = task_id or str(meta.get("task_id") or "")

    items: list[dict] = []
    cur: dict[str, Any] = {}

    def _flush() -> None:
        nonlocal cur
        headline = _as_str(cur.get("headline") or cur.get("title"))
        if headline:
            items.append({
                "headline": headline,
                "source": _as_str(cur.get("source") or SOURCE),
                "published": cur.get("published") or None,
                "retrieved": _as_str(cur.get("retrieved") or retrieved),
                "task_id": _as_str(cur.get("task_id") or tid),
                "raw_excerpt": _as_str(cur.get("raw_excerpt") or cur.get("excerpt")),
            })
        cur = {}

    for line in (text or "").splitlines():
        kv = _KV.match(line.strip())
        if kv:
            key = kv.group(1).lower()
            val = kv.group(2).strip()
            if key in {"headline", "title"} and cur:
                _flush()
            if key == "title":
                key = "headline"
            if key == "excerpt":
                key = "raw_excerpt"
            cur[key] = val
            continue
        # blank line ends a block
        if not line.strip():
            if cur:
                _flush()
            continue
        # skip mail chrome
        if re.search(r"(?i)^(from:|to:|date:|subject:|--|sent from)", line):
            continue
        if "headline" not in cur and not _KV.match(line.strip()):
            guess = line.strip().lstrip("-*• ").strip()
            if len(guess) >= 12 and not guess.lower().startswith("hi "):
                if cur:
                    _flush()
                cur["headline"] = guess
    if cur:
        _flush()
    return {
        "task_id": tid,
        "slug": slug or "unknown",
        "kind": str(meta.get("kind") or slug or "unknown"),
        "retrieved": retrieved,
        "items": items,
    }


def write_dump(
    dump: dict[str, Any],
    *,
    date: str,
    root: Path | None = None,
) -> Path:
    folder = Path(root) if root is not None else GROK_DIR
    folder.mkdir(parents=True, exist_ok=True)
    slug = _as_str(dump.get("slug") or "unknown") or "unknown"
    slug = re.sub(r"[^a-z0-9_-]+", "-", slug.lower()).strip("-") or "unknown"
    path = folder / f"{date}_{slug}.json"
    payload = {
        "task_id": _as_str(dump.get("task_id")),
        "slug": slug,
        "kind": _as_str(dump.get("kind") or slug),
        "retrieved": _as_str(dump.get("retrieved") or date),
        "items": [
            {
                "headline": _as_str(it.get("headline")),
                "source": _as_str(it.get("source") or SOURCE),
                "published": it.get("published") if it.get("published") not in {"", None} else None,
                "retrieved": _as_str(it.get("retrieved") or dump.get("retrieved") or date),
                "task_id": _as_str(it.get("task_id") or dump.get("task_id")),
                "raw_excerpt": _as_str(it.get("raw_excerpt")),
            }
            for it in (dump.get("items") or [])
            if isinstance(it, dict) and _as_str(it.get("headline"))
        ],
    }
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return path


def prefer_source(arts: Iterable[dict]) -> list[dict]:
    """Title-dedupe keeping the higher-ranked harvest source (automations > Finviz)."""
    best: dict[str, dict] = {}
    order: list[str] = []
    for a in arts:
        k = (a.get("title") or "").lower()[:160]
        if not k:
            continue
        if k not in best:
            best[k] = a
            order.append(k)
            continue
        if harvest_source_rank(a) > harvest_source_rank(best[k]):
            best[k] = a
    return [best[k] for k in order]
