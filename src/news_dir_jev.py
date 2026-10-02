"""Score news-dir results with the frozen evidence-context-v2 rubric.

Writes dashboard/news-dir/scores/<stamp>.json so the page can poll.
"""
from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import os
import re
from pathlib import Path

from .jev_eval import title_id
from .jev_gate import api_key, gate
from .news_dir import ROOT

DIR = ROOT / "dashboard" / "news-dir"
SCORES = DIR / "scores"
WALK_KEYS = (
    "items", "rows", "keeps", "dropped", "drops", "decisions", "all_items",
)
STAMP_RE = re.compile(r"^20\d{2}-\d{2}-\d{2}[a-z0-9_-]*$")


def verdict(node: dict) -> str:
    raw = str(node.get("decision") or node.get("jev") or node.get("verdict") or "").upper()
    if raw in {"KEEP", "DROP"}:
        return raw
    if node.get("keep") is True:
        return "KEEP"
    if node.get("drop") is True:
        return "DROP"
    return ""


def walk_verdicts(node, into: list[dict] | None = None) -> list[dict]:
    into = into if into is not None else []
    if isinstance(node, list):
        for item in node:
            walk_verdicts(item, into)
        return into
    if not isinstance(node, dict):
        return into
    title = node.get("title") or node.get("headline") or node.get("name")
    decision = verdict(node)
    if title and decision:
        into.append({
            "title": str(title),
            "decision": decision,
            "reason": str(node.get("reason") or node.get("why") or ""),
        })
    for key in WALK_KEYS:
        child = node.get(key)
        if isinstance(child, list):
            walk_verdicts(child, into)
    return into


def publish_keep_file(src: Path, dest: Path, date: str) -> dict:
    doc = json.loads(src.read_text(encoding="utf-8"))
    items = walk_verdicts(doc)
    if not items:
        raise RuntimeError(f"no verdicts in {src}")
    out = {"date": date, "stamp": date, "n": len(items), "items": items}
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    write_manifest(dest.parent, date)
    return out


def load_titles(blob: dict) -> list[dict]:
    rows = blob.get("rows") or blob.get("items") or blob.get("titles") or []
    out = []
    seen: set[str] = set()
    for raw in rows:
        if isinstance(raw, str):
            title = raw.strip()
            row = {"title": title}
        elif isinstance(raw, dict):
            title = str(raw.get("title") or raw.get("headline") or "").strip()
            row = {
                "title": title,
                "source": str(raw.get("source") or ""),
                "url": str(raw.get("url") or ""),
                "date": str(raw.get("parse") or raw.get("date") or ""),
            }
        else:
            continue
        if not title:
            continue
        tid = title_id(title)
        if tid in seen:
            continue
        seen.add(tid)
        row["id"] = tid
        out.append(row)
        if len(out) >= 200:
            break
    return out


def decode_payload(raw: str) -> dict:
    text = (raw or "").strip()
    if not text:
        return {}
    if text.startswith("{"):
        return json.loads(text)
    pad = "=" * (-len(text) % 4)
    return json.loads(base64.b64decode(text + pad).decode("utf-8"))


def score_rows(rows: list[dict], *, stamp: str, live: bool = True) -> dict:
    key = api_key()
    if live and not key:
        raise RuntimeError("JEV_API_KEY / TYPESAFE_API_KEY is empty")
    decided = gate(
        rows,
        live=live,
        key=key,
        workers=16 if key else 1,
        policy=os.environ.get("JEV_GATE_POLICY", "candidate-v2"),
    )
    items = []
    for row in decided:
        decision = verdict(row)
        if not decision:
            continue
        items.append({
            "title": row.get("title") or "",
            "decision": decision,
            "reason": str(row.get("reason") or ""),
        })
    return {
        "stamp": stamp,
        "n": len(items),
        "keep": sum(1 for it in items if it["decision"] == "KEEP"),
        "drop": sum(1 for it in items if it["decision"] == "DROP"),
        "policy": os.environ.get("JEV_GATE_POLICY", "candidate-v2"),
        "model": os.environ.get("JEV_MODEL", ""),
        "items": items,
    }


def write_manifest(directory: Path, stamp: str) -> None:
    if stamp in {"latest", ""} or stamp.endswith("-req"):
        return
    path = directory / "index.json"
    stamps: list[str] = []
    if path.is_file():
        try:
            stamps = list(json.loads(path.read_text(encoding="utf-8")).get("stamps") or [])
        except (OSError, json.JSONDecodeError):
            stamps = []
    if stamp not in stamps:
        stamps.append(stamp)
    path.write_text(json.dumps({"stamps": stamps}, ensure_ascii=False, indent=2), encoding="utf-8")


def write_scores(doc: dict, stamp: str, dest_dir: Path | None = None) -> Path:
    if not STAMP_RE.match(stamp):
        raise RuntimeError(f"bad score stamp {stamp!r}")
    directory = dest_dir or SCORES
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{stamp}.json"
    text = json.dumps(doc, ensure_ascii=False, indent=2)
    path.write_text(text, encoding="utf-8")
    (directory / "latest.json").write_text(text, encoding="utf-8")
    write_manifest(directory, stamp)
    return path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Score news-dir titles")
    parser.add_argument("--stamp", default="")
    parser.add_argument("--payload", default="", help="JSON or base64 JSON of titles")
    parser.add_argument("--from-req", default="", help="Request JSON written by the page")
    parser.add_argument("--from-keep", default="", help="Existing *_jev_keep.json")
    parser.add_argument("--date", default="")
    args = parser.parse_args(argv)
    stamp = args.stamp or args.date or dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d-%H%M")
    if args.from_keep:
        src = Path(args.from_keep)
        dest = SCORES / f"{stamp}.json"
        doc = publish_keep_file(src, dest, args.date or stamp)
        (SCORES / "latest.json").write_text(
            dest.read_text(encoding="utf-8"), encoding="utf-8"
        )
        print(f"NEWS_DIR_JEV_N={doc['n']}")
        return 0
    if args.from_req:
        blob = json.loads(Path(args.from_req).read_text(encoding="utf-8"))
    else:
        blob = decode_payload(args.payload or os.environ.get("NEWS_DIR_TITLES", ""))
    rows = load_titles(blob)
    if not rows:
        raise SystemExit("no titles to score")
    if blob.get("stamp"):
        stamp = str(blob["stamp"])
    doc = score_rows(rows, stamp=stamp, live=True)
    path = write_scores(doc, stamp)
    print(f"NEWS_DIR_JEV_N={doc['n']} KEEP={doc['keep']} DROP={doc['drop']} PATH={path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
