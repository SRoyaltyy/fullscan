"""Jev hop-0 trainer. Cyrus grades a draw; the action records it.

Draw, per run, after gold titles and any title hash already stored under
00_grounding/jev_train/ are removed:

  40   01_daily/news/*_parsed.json, stratified by date
       (digest only when that day has no parse)
  40   today's Google News RSS watchlist, after the hop-0 Jaccard dedupe
  20   frozen holdout (00_grounding/jev_holdout.json) when 20 unseen
       titles remain; otherwise the next slice of the rotating
       hard-misses file (00_grounding/jev_hard_misses.json)

The current hop-0 gate (code bits + Jev) scores every title. This module
does not write keep.json, does not call Lane, and does not edit
jev_closed_lists.json or an existing holdout file.
"""
from __future__ import annotations

import base64
import datetime as dt
import json
import os
import random
import re
from collections import Counter
from pathlib import Path

from .jev_eval import (
    HASH_ALGO,
    fetch_rss,
    gold_norms as _gold_norms,
    load_archive,
    pool_draw,
    stratified_draw,
    title_id,
)
from .jev_eval import score_sample as _score_sample
from .jev_gate import (
    GROUND,
    INSTRUMENT_KEEP,
    NEWS_DIR,
    POWERFUL,
    ROOT,
    _load_json,
    _write_json,
    api_key,
    normalize_title,
    tokens,
)
try:
    from .jev_gate import GateKnobs
except ImportError:
    GateKnobs = None

PARSED_N = 40
RSS_N = 40
EXAM_N = 20
MIN_MARKS = 30
BIT_ORDER = (
    "opinion", "tape", "earnings", "print", "lever",
    "signed", "blast", "chokepoint", "actor", "geo",
)
TRAIN_DIRNAME = "jev_train"
DRAW_NAME = "draw.json"
SCHEMA_DRAW = "jev-train-draw-1"
SCHEMA_SESSION = "jev-train-session-1"
SCHEMA_GRADE = "jev-train-grade-1"
SCHEMA_GRADES = "jev-train-grades-1"
SCHEMA_HARD = "jev-hard-misses-1"
ISSUE_LABEL = "jev-train"
DEPLOY_BRANCH = "main"
REPO_SLUG = "SRoyaltyy/fullscan"
STAMP_RE = re.compile(r"^\d{8}_\d{4}$")
ID_RE = re.compile(r"^[0-9a-f]{16}$")
SECRET_RE = re.compile(
    r"sk-[A-Za-z0-9]{8,}|ghp_[A-Za-z0-9]{8,}|github_pat_[A-Za-z0-9_]{8,}|"
    r"JEV_API_KEY\s*=|Bearer [A-Za-z0-9._\-]{12,}"
)
COMMIT_ALLOW = (
    "00_grounding/jev_train/",
    "00_grounding/jev_hard_misses.json",
    "dashboard/jev-train/draw.json",
)
HARD_MISS_NOTE = (
    "Rotating exam bank for the Jev trainer. The frozen holdout in "
    "jev_holdout.json supplies the 20-title exam while 20 of its titles "
    "are still absent from 00_grounding/jev_train/. After that, each draw "
    "takes the next unseen slice, starting at cursor, and skips any title "
    "hash already stored under jev_train/ and any title in jev_gold.json. "
    "Graded false keeps and false drops are appended here as a record; a "
    "hash already in jev_train/ is not drawn again. This file is not a "
    "closed list. The trainer never edits jev_closed_lists.json, never "
    "rewrites an existing jev_holdout.json, and never writes keep.json. "
    "Initial items were stratified from the parsed archive with seed "
    "20260929, excluding gold and the frozen holdout."
)
HOLDOUT_STUB_NOTE = (
    "Frozen holdout for hop-0. The trainer does not invent titles and "
    "does not rewrite this file once it has items. The eval workflow "
    "freezes the real bank. While items is empty, the 20-title exam "
    "comes from jev_hard_misses.json."
)

_CHOKE_REASONS = frozenset({
    "code_choke", "chokepoint", "reprint_weather", "geo_chokepoint_no_hit",
})
_ACTOR_REASONS = frozenset({"state_head_action", "other_powerful"})

# Bit detectors used when the live gate has not imported hop-0 code rules.
# Same shapes as the eval gate so the homework columns stay stable.
_POLICY_CTX = re.compile(
    r"(?i)\b(?:fda|fomc|fed|cpi|pce|ppi|nfp|nbs|pboc|ecb|boj|bis|nhtsa|"
    r"epa|carb|sec|ftc|doj|cafe|wasde|eia|ism|gdp|ustr|treasury|cms|opec)\b"
)
_EARNINGS = re.compile(
    r"(?i)(?:"
    r"\bearnings\b"
    r"|\bprice[- ]targets?\b"
    r"|\beps\b"
    r"|\bprofit warning\b"
    r"|\brevenue\b.{0,24}\b(?:beat|miss)"
    r"|\b(?:beats?|miss(?:es|ed)?)\b.{0,30}\b(?:earnings|estimates|expectations)\b"
    r"|\bguidance\b"
    r"|\b(?:raise[sd]?|cuts?|hikes?|lowers?|lowered|boosts?|boosted|slash(?:ed|es)?)\s+pt\b"
    r"|\bpt\s+(?:raise[sd]?|cuts?|hikes?|lowers?|lowered|boosts?|boosted|slash(?:ed|es)?)\b"
    r")"
)
_SHARE_PCT = re.compile(
    r"(?i)(?:"
    r"\b(?:shares|stock)\b.{0,24}(?:\+\d+(?:\.\d+)?%|up \d+(?:\.\d+)?%)"
    r"|(?:\+\d+(?:\.\d+)?%|up \d+(?:\.\d+)?%).{0,24}\b(?:shares|stock)\b"
    r")"
)
_TAPE = re.compile(
    r"(?i)(?:"
    r"\bgold\b.{0,24}\b(?:falls?|drops?|plunges?|declines?|rises?|jumps?|fell|rose|slides?|slid)\b"
    r"|\b(?:falls?|drops?|plunges?|rises?|jumps?|fell|rose)\b.{0,16}\bgold\b"
    r"|\boil price today\b"
    r"|\bbrent\b.{0,20}\b(?:rises?|falls?|jumps?|rose|fell)\b"
    r"|\boil prices?\b.{0,24}\b(?:jump|jumps|jumped|rise|rises|rose|fall|falls|fell|climb|climbs|climbed|slide|slides|slid)\b"
    r"|\boil\b.{0,12}\b(?:rises?|jumps?|climbs?|falls?|rose|fell)\b"
    r"|\bstocks?\b.{0,40}\bhalt(?:s|ed)?\b"
    r"|\bhalt (?:their|the) slide\b"
    r"|\bstocks?\s+(?:jump|jumps|jumped|rally|rallies|rallied|fall|falls|fell)\s+as\b"
    r")"
)
_INSTRUMENT = re.compile(
    r"(?i)(?:"
    r"\bexecutive orders?\b"
    r"|\bfederal register\b"
    r"|\bfinal rules?\b"
    r"|\bcafe\b"
    r"|\baccelerated approval\b"
    r"|\bcomplete response letter\b"
    r"|\bcrl\b"
    r"|\badcomm\b"
    r"|\bfda\b.{0,40}\b(?:approval|approves|approved|rejects|rejection)\b"
    r"|\b(?:approval|approves|approved)\b.{0,40}\bfda\b"
    r"|\b(?:sec|ftc|doj)\b.{0,50}\b(?:order|consent order|charges)\b"
    r"|\b(?:consent order|charges)\b.{0,40}\b(?:sec|ftc|doj)\b"
    r"|\bbis\b.{0,40}\b(?:export|entity)\b"
    r"|\bcourt orders?\b"
    r"|\bcourt rulings?\b"
    r"|\binjunction\b"
    r"|\btro\b"
    r")"
)
_PRINT = re.compile(
    r"(?i)(?:"
    r"\b(?:cpi|pce|ppi|nfp)\b"
    r"|\bnonfarm payrolls\b"
    r"|\b(?:jobless|initial) claims\b"
    r"|\bretail sales\b"
    r"|\bgdp\b"
    r"|\bwasde\b"
    r"|\bindustrial profits\b"
    r"|\bism\b"
    r"|\beia\b"
    r"|\bapi\b.{0,30}\b(?:crude|inventor(?:y|ies))\b"
    r")"
)
_RATE = re.compile(
    r"(?i)(?:"
    r"\b(?:fomc|boj|ecb|pboc|federal reserve|fed)\b.{0,80}"
    r"\b(?:holds?|hikes?|cuts?|pauses?|raises?|lowers?)\b.{0,40}"
    r"\b(?:rates?|basis points?|bps|percent|%)\b"
    r"|\b(?:fomc|boj|ecb|pboc|federal reserve|fed)\b.{0,60}\b(?:rate )?decision\b"
    r"|\b(?:fomc|boj|ecb|pboc|federal reserve|fed)\b.{0,40}"
    r"\d+(?:\.\d+)?\s*(?:%|percent|bps|basis points)\b"
    r")"
)
_LEVER_ACTOR = re.compile(
    r"(?i)\b(?:"
    r"trump|biden|harris|powell|yellen|xi|lagarde|starmer|ishiba|modi|"
    r"potus|president|white house|cabinet|fed official|federal reserve|"
    r"fomc|warsh|pboc|ecb|boj|treasury|ustr|"
    r"commerce secretary|energy secretary|defense secretary|"
    r"secretary of commerce|secretary of energy|secretary of defense|"
    r"secretary of the treasury|secretary of state"
    r")\b"
)
_LEVER_VERB = re.compile(
    r"(?i)\b(?:"
    r"bans?|banned|tariffs?|sanctions?|quota|exports?|dut(?:y|ies)|"
    r"ceasefire|hikes?|cuts?|pauses?|emergency|executive orders?|eo|rules?"
    r")\b"
)
_OPS = re.compile(
    r"(?i)(?:"
    r"\b(?:plant|factory|refinery|pipeline|rig|mine)\b.{0,40}"
    r"\b(?:explosion|fire|blast|explodes|exploded|outage)\b"
    r"|\b(?:explosion|blast|explodes|exploded)\b.{0,40}"
    r"\b(?:plant|factory|refinery|pipeline|rig|mine|terminal|port)\b"
    r"|\bfaa\b.{0,40}\b(?:ground|grounds|grounding|grounded)\b"
    r"|\b(?:port|rail) strike\b"
    r"|\b(?:trading|exchange) halt\b"
    r"|\bransomware\b"
    r"|\bcyber ?attacks?\b"
    r"|\bcyber\b.{0,20}\b(?:breach|hack)\b"
    r"|\bzero[- ]day\b"
    r"|\bmine outage\b"
    r"|\bopec\b"
    r")"
)


def is_earnings(title: str) -> bool:
    title = title or ""
    if _SHARE_PCT.search(title) and not _POLICY_CTX.search(title):
        return True
    match = _EARNINGS.search(title)
    if not match:
        return False
    hit = match.group(0).lower()
    if "guidance" in hit and "earnings" not in title.lower() and _POLICY_CTX.search(title):
        return False
    return True


def tape_hit(title: str) -> bool:
    return bool(_TAPE.search(title or ""))


def _instrument_hit(title: str) -> bool:
    return bool(_INSTRUMENT.search(title or ""))


def _print_hit(title: str) -> bool:
    return bool(_PRINT.search(title or "") or _RATE.search(title or ""))


def _lever_hit(title: str) -> bool:
    return bool(_LEVER_ACTOR.search(title or "") and _LEVER_VERB.search(title or ""))


def train_dir(ground: Path | None = None) -> Path:
    return (ground or GROUND) / TRAIN_DIRNAME


def hard_miss_path(ground: Path | None = None) -> Path:
    return (ground or GROUND) / "jev_hard_misses.json"


def holdout_path(ground: Path | None = None) -> Path:
    return (ground or GROUND) / "jev_holdout.json"


def dashboard_draw_path(root: Path | None = None) -> Path:
    return (root or ROOT) / "dashboard" / "jev-train" / DRAW_NAME


def commit_allowed(path: str) -> bool:
    rel = path.replace("\\", "/").lstrip("./")
    return any(rel == prefix or rel.startswith(prefix) for prefix in COMMIT_ALLOW)


def ensure_pool_files(ground: Path) -> None:
    """Create documented pool files only when they are absent."""
    hold = holdout_path(ground)
    if not hold.exists():
        _write_json(hold, {
            "created_at": "",
            "algo": HASH_ALGO,
            "note": HOLDOUT_STUB_NOTE,
            "ids": [],
            "items": [],
        })
    hard = hard_miss_path(ground)
    if not hard.exists():
        _write_json(hard, {
            "schema": SCHEMA_HARD,
            "note": HARD_MISS_NOTE,
            "cursor": 0,
            "seed": None,
            "items": [],
        })


def read_holdout_items(path: Path) -> list[dict]:
    blob = _load_json(path)
    if not isinstance(blob, dict):
        return []
    out = []
    for it in blob.get("items") or []:
        if isinstance(it, dict) and (it.get("title") or "").strip():
            out.append(dict(it))
    return out


def read_hard_misses(path: Path) -> dict:
    blob = _load_json(path)
    if not isinstance(blob, dict):
        return {"schema": SCHEMA_HARD, "note": HARD_MISS_NOTE, "cursor": 0, "items": []}
    items = [dict(it) for it in (blob.get("items") or []) if isinstance(it, dict)]
    try:
        cursor = int(blob.get("cursor") or 0)
    except (TypeError, ValueError):
        cursor = 0
    blob = dict(blob)
    blob["items"] = items
    blob["cursor"] = max(0, cursor)
    blob.setdefault("schema", SCHEMA_HARD)
    blob.setdefault("note", HARD_MISS_NOTE)
    return blob


def _row_id(row: dict) -> str:
    existing = str(row.get("id") or "")
    if ID_RE.match(existing):
        return existing
    return title_id(row.get("title") or "")


def trained_ids(directory: Path) -> set[str]:
    """Title hashes already recorded under jev_train/.

    JSON title fields are hashed with the hop-0 title id. Bare 16-hex
    ids in any file in the directory count too, which covers the markdown
    sheet and a grades file that stored the hash without the title.
    """
    found: set[str] = set()
    if not directory.is_dir():
        return found
    for path in sorted(directory.iterdir()):
        if not path.is_file() or path.name.startswith("."):
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        for match in re.findall(r"\b[0-9a-f]{16}\b", text):
            found.add(match)
        if path.suffix.lower() != ".json":
            continue
        try:
            blob = json.loads(text)
        except json.JSONDecodeError:
            continue
        _walk_titles(blob, found)
    return found


def _walk_titles(node, found: set[str]) -> None:
    if isinstance(node, dict):
        title = node.get("title")
        if isinstance(title, str) and title.strip():
            found.add(title_id(title))
        ident = node.get("id")
        if isinstance(ident, str) and ID_RE.match(ident):
            found.add(ident)
        for value in node.values():
            _walk_titles(value, found)
    elif isinstance(node, list):
        for value in node:
            _walk_titles(value, found)


def _banned(row: dict, gold: set[str], banned: set[str]) -> str:
    title = row.get("title") or ""
    norm = normalize_title(title)
    if not norm:
        return "empty"
    tid = _row_id(row)
    if norm in gold:
        return "gold"
    if tid in banned or title_id(title) in banned:
        return "trained"
    return ""


def _split(rows: list[dict], gold: set[str], banned: set[str]) -> tuple[list[dict], set[str], set[str]]:
    kept: list[dict] = []
    gold_hit: set[str] = set()
    trained_hit: set[str] = set()
    for row in rows:
        why = _banned(row, gold, banned)
        if why == "gold":
            gold_hit.add(normalize_title(row.get("title") or ""))
            continue
        if why == "trained":
            trained_hit.add(_row_id(row))
            continue
        if why == "empty":
            continue
        kept.append(row)
    return kept, gold_hit, trained_hit


def take_rotating(items: list[dict], n: int, cursor: int,
                  gold: set[str], banned: set[str]) -> tuple[list[dict], int]:
    """Next unseen slice. cursor is the index to try first; it wraps once."""
    if n <= 0 or not items:
        return [], int(cursor or 0)
    start = int(cursor or 0) % len(items)
    picked: list[dict] = []
    idx = start
    scanned = 0
    while scanned < len(items) and len(picked) < n:
        row = items[idx]
        if not _banned(row, gold, banned):
            picked.append(row)
        idx = (idx + 1) % len(items)
        scanned += 1
    return picked, idx


def why_bits(title: str, decision: dict | None) -> list[str]:
    """Bits that fired on the current hop-0 gate. Order is fixed."""
    decision = decision or {}
    title = title or ""
    reason = str(decision.get("reason") or "")
    geo = str(decision.get("geo") or "")
    actor = str(decision.get("actor_power") or "")
    try:
        instrument = float(decision.get("new_instrument") or 0.0)
    except (TypeError, ValueError):
        instrument = 0.0
    fired: list[str] = []

    def add(name: str, cond: bool) -> None:
        if cond and name not in fired:
            fired.append(name)

    add("opinion", reason == "opinion")
    add("tape", reason == "tape" or tape_hit(title))
    add("earnings", reason == "earnings" or is_earnings(title))
    add("print", reason == "code_print" or _print_hit(title))
    add("lever", reason == "code_lever" or _lever_hit(title))
    add(
        "signed",
        reason == "code_instrument"
        or _instrument_hit(title)
        or instrument >= INSTRUMENT_KEEP,
    )
    add("blast", reason == "code_ops" or bool(_OPS.search(title)))
    add("chokepoint", geo == "chokepoint" or reason in _CHOKE_REASONS)
    add(
        "actor",
        actor in POWERFUL or reason in _ACTOR_REASONS or bool(_LEVER_ACTOR.search(title)),
    )
    add("geo", geo in {"core", "chokepoint"})
    return [name for name in BIT_ORDER if name in fired]


def _tag(row: dict, pool: str) -> dict:
    out = dict(row)
    out["id"] = _row_id(row)
    out["pool"] = pool
    return out


def select_draw(
    *,
    archive: list[dict],
    rss_rows: list[dict],
    holdout_items: list[dict],
    hard_items: list[dict],
    hard_cursor: int,
    gold: set[str],
    banned_ids: set[str],
    rng: random.Random,
    parsed_n: int = PARSED_N,
    rss_n: int = RSS_N,
    exam_n: int = EXAM_N,
) -> dict:
    """Pick the three pools. Raises if a pool cannot be filled."""
    gold_hit: set[str] = set()
    trained_hit: set[str] = set()

    def absorb(pair):
        kept, g, t = pair
        gold_hit.update(g)
        trained_hit.update(t)
        return kept

    hold_kept = absorb(_split(holdout_items, gold, banned_ids))
    if len(hold_kept) >= exam_n:
        exam = [_tag(row, "holdout") for row in hold_kept[:exam_n]]
        exam_source = "holdout"
        new_cursor = int(hard_cursor or 0)
    else:
        picked, new_cursor = take_rotating(
            hard_items, exam_n, hard_cursor, gold, banned_ids,
        )
        # Count skips inside the rotation for the report.
        absorb(_split(hard_items, gold, banned_ids))
        if len(picked) < exam_n:
            raise RuntimeError(
                f"exam short {len(picked)}/{exam_n}: "
                f"holdout unseen {len(hold_kept)}, "
                f"hard-misses {len(hard_items)} (cursor {hard_cursor}). "
                "Add unseen titles to 00_grounding/jev_hard_misses.json. "
                "Graded hashes in 00_grounding/jev_train/ stay excluded."
            )
        exam = [_tag(row, "hard_miss") for row in picked]
        exam_source = "hard_miss"

    exam_ids = {row["id"] for row in exam}
    reserved = set(banned_ids) | exam_ids
    reserved |= {_row_id(row) for row in holdout_items}
    reserved |= {_row_id(row) for row in hard_items}
    tuning = absorb(_split(archive, gold, reserved))
    blocked = [tokens(row.get("title") or "") for row in exam]
    parsed, _gold_n = stratified_draw(
        tuning, parsed_n, rng, gold=set(gold), blocked=list(blocked),
    )
    if len(parsed) < parsed_n:
        raise RuntimeError(
            f"parsed sample short {len(parsed)}/{parsed_n} from {len(tuning)} titles"
        )
    parsed = [_tag(row, "parsed") for row in parsed]

    blocked = list(blocked) + [tokens(row.get("title") or "") for row in parsed]
    parsed_ids = {row["id"] for row in parsed}
    rss_fetched = len(rss_rows)
    rss_unique: list[dict] = []
    seen: set[str] = set()
    for row in rss_rows:
        tid = _row_id(row)
        if tid in seen or tid in exam_ids or tid in parsed_ids or tid in reserved:
            continue
        if _banned(row, gold, banned_ids):
            if _banned(row, gold, banned_ids) == "gold":
                gold_hit.add(normalize_title(row.get("title") or ""))
            else:
                trained_hit.add(tid)
            continue
        seen.add(tid)
        rss_unique.append(row)
    rss, _g = pool_draw(
        rss_unique, rss_n, rng, gold=set(gold), blocked=list(blocked),
    )
    if len(rss) < rss_n:
        raise RuntimeError(
            f"rss sample short {len(rss)}/{rss_n} "
            f"from {len(rss_unique)} unique wires ({rss_fetched} fetched)"
        )
    rss = [_tag(row, "rss") for row in rss]

    picked_ids = [row["id"] for row in parsed + rss + exam]
    if len(picked_ids) != len(set(picked_ids)):
        raise RuntimeError("draw repeated a title hash")
    overlap = sorted(set(picked_ids) & set(banned_ids))
    if overlap:
        raise RuntimeError(f"draw contains trained hashes: {overlap[:5]}")
    for row in parsed + rss + exam:
        if normalize_title(row.get("title") or "") in gold:
            raise RuntimeError("gold title survived the draw")
    return {
        "parsed": parsed,
        "rss": rss,
        "exam": exam,
        "exam_source": exam_source,
        "hard_cursor": new_cursor,
        "gold_excluded": len(gold_hit),
        "trained_excluded": len(trained_hit),
        "rss_fetched": rss_fetched,
        "rss_unique": len(rss_unique),
        "archive_n": len(archive),
    }


def _decision_view(row: dict) -> dict:
    try:
        instrument = float(row.get("new_instrument") or 0.0)
    except (TypeError, ValueError):
        instrument = 0.0
    return {
        "reason": row.get("reason") or "",
        "geo": row.get("geo") or "",
        "actor_power": row.get("actor_power") or "",
        "new_instrument": instrument,
        "decision": "keep" if row.get("jev") == "KEEP" else "drop",
    }


def annotate_gate(rows: list[dict], *, live: bool, key: str, workers: int,
                  poster, asof: dt.date) -> tuple[list[dict], str]:
    score_kwargs = {
        "live": live, "key": key, "workers": workers, "poster": poster, "asof": asof,
    }
    if GateKnobs is not None:
        score_kwargs["knobs"] = GateKnobs()
    decided, model = _score_sample(rows, **score_kwargs)
    items = []
    for index, row in enumerate(rows, start=1):
        dec = decided.get(normalize_title(row.get("title") or ""))
        if not dec:
            raise RuntimeError(f"no gate decision for {(row.get('title') or '')[:80]}")
        jev = "KEEP" if dec.get("decision") == "keep" else "DROP"
        item = {
            "n": index,
            "id": row.get("id") or title_id(row.get("title") or ""),
            "pool": row.get("pool") or "",
            "title": row.get("title") or "",
            "source": row.get("source") or "",
            "published_at": row.get("published_at") or "",
            "url": row.get("url") or "",
            "date": row.get("date") or "",
            "query": row.get("query") or "",
            "jev": jev,
            "reason": dec.get("reason") or "",
            "geo": dec.get("geo") or "",
            "actor_power": dec.get("actor_power") or "",
            "new_instrument": dec.get("new_instrument") or 0,
            "bits": why_bits(row.get("title") or "", dec),
        }
        items.append(item)
    return items, model


def _stamp_now(now: dt.datetime | None = None) -> str:
    now = now or dt.datetime.now(dt.timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=dt.timezone.utc)
    return now.strftime("%Y%m%d_%H%M")


def allocate_stamp(directory: Path, requested: str, now: dt.datetime, *,
                   kind: str) -> str:
    """kind is 'draw' or 'session'. Seconds only when the minute is taken."""
    def taken(stamp: str) -> bool:
        if kind == "draw":
            return (directory / f"{stamp}_draw.json").exists()
        return (directory / f"{stamp}.json").exists() or (directory / f"{stamp}.md").exists()

    base = requested if STAMP_RE.match(requested or "") else _stamp_now(now)
    if not taken(base):
        return base
    sec = now.strftime("%Y%m%d_%H%M%S")
    if STAMP_RE.match(sec) or not taken(sec):
        if not taken(sec):
            return sec
    raise FileExistsError(f"refusing to overwrite stamp {base}")


def propose_hard_miss_bank(archive: list[dict], holdout_items: list[dict],
                           gold: set[str], n: int, rng: random.Random) -> list[dict]:
    """Unseen archive titles for the rotating bank. Does not write."""
    reserved = {_row_id(row) for row in holdout_items}
    eligible, _, _ = _split(archive, gold, reserved)
    blocked = [tokens(row.get("title") or "") for row in holdout_items]
    picked, _gold_n = stratified_draw(
        eligible, n, rng, gold=set(gold), blocked=list(blocked),
    )
    if len(picked) < n:
        raise RuntimeError(f"hard-miss bank short {len(picked)}/{n}")
    out = []
    for row in picked:
        out.append({
            "id": _row_id(row),
            "title": row.get("title") or "",
            "source": row.get("source") or "",
            "published_at": row.get("published_at") or "",
            "url": row.get("url") or "",
            "date": row.get("date") or "",
            "archive": row.get("archive") or "parsed",
        })
    return out


def run_draw(
    *,
    live: bool,
    key: str = "",
    workers: int = 16,
    seed: int | None = None,
    stamp: str = "",
    rss_rows: list[dict] | None = None,
    poster=None,
    gold: set[str] | None = None,
    now: dt.datetime | None = None,
    rng: random.Random | None = None,
    root: Path | None = None,
    ground: Path | None = None,
    news_dir: Path | None = None,
    write: bool = False,
    parsed_n: int = PARSED_N,
    rss_n: int = RSS_N,
    exam_n: int = EXAM_N,
    opener=None,
) -> dict:
    now = now or dt.datetime.now(dt.timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=dt.timezone.utc)
    root = root or ROOT
    ground = ground or (root / "00_grounding")
    news_dir = news_dir or (root / "01_daily" / "news")
    if rng is None:
        if seed is None:
            seed = random.SystemRandom().randrange(1, 2**31 - 1)
        rng = random.Random(seed)
    elif seed is None:
        seed = 0
    ensure_pool_files(ground)
    hold_file = holdout_path(ground)
    hold_bytes = hold_file.read_bytes() if hold_file.exists() else b""
    archive = load_archive(news_dir)
    gold = _gold_norms() if gold is None else set(gold)
    banned = trained_ids(train_dir(ground))
    hold_items = read_holdout_items(hold_file)
    hard_blob = read_hard_misses(hard_miss_path(ground))
    if rss_rows is None:
        rss_rows = fetch_rss(opener=opener)
    sample = select_draw(
        archive=archive,
        rss_rows=rss_rows,
        holdout_items=hold_items,
        hard_items=list(hard_blob.get("items") or []),
        hard_cursor=int(hard_blob.get("cursor") or 0),
        gold=gold,
        banned_ids=banned,
        rng=rng,
        parsed_n=parsed_n,
        rss_n=rss_n,
        exam_n=exam_n,
    )
    ordered = sample["parsed"] + sample["rss"] + sample["exam"]
    items, model = annotate_gate(
        ordered, live=live, key=key, workers=workers, poster=poster, asof=now.date(),
    )
    directory = train_dir(ground)
    if write:
        directory.mkdir(parents=True, exist_ok=True)
        used = allocate_stamp(directory, stamp, now, kind="draw")
    else:
        used = stamp if STAMP_RE.match(stamp or "") else _stamp_now(now)
    report = {
        "schema": SCHEMA_DRAW,
        "stamp": used,
        "generated_at": now.isoformat(),
        "seed": seed,
        "model": model,
        "exam_source": sample["exam_source"],
        "gate": "hop0-code-bits+jev" if live else "hop0-code-bits",
        "sample": {
            "parsed": len(sample["parsed"]),
            "rss": len(sample["rss"]),
            "exam": len(sample["exam"]),
            "parsed_target": parsed_n,
            "rss_target": rss_n,
            "exam_target": exam_n,
            "archive_n": sample["archive_n"],
            "gold_excluded": sample["gold_excluded"],
            "trained_excluded": sample["trained_excluded"],
            "rss_fetched": sample["rss_fetched"],
            "rss_unique": sample["rss_unique"],
            "exam_source": sample["exam_source"],
            "hard_cursor": sample["hard_cursor"],
        },
        "items": items,
    }
    _reject_secrets(json.dumps(report))
    if write:
        if hold_file.exists() and hold_file.read_bytes() != hold_bytes:
            raise RuntimeError("refusing to rewrite jev_holdout.json")
        draw_path = directory / f"{used}_draw.json"
        if draw_path.exists():
            raise FileExistsError(f"refusing to overwrite {draw_path.name}")
        _write_json(draw_path, report)
        dash = dashboard_draw_path(root)
        _write_json(dash, report)
        if sample["exam_source"] == "hard_miss":
            hard_blob["cursor"] = sample["hard_cursor"]
            hard_blob["schema"] = SCHEMA_HARD
            _write_json(hard_miss_path(ground), hard_blob)
        # Holdout bytes must still match.
        if hold_file.exists() and hold_file.read_bytes() != hold_bytes:
            raise RuntimeError("holdout changed during the draw")
        report["paths"] = {
            "draw": str(draw_path.relative_to(root)) if _under(draw_path, root) else str(draw_path),
            "page": str(dash.relative_to(root)) if _under(dash, root) else str(dash),
        }
    _print_draw(report)
    return report


def _under(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def _print_draw(report: dict) -> None:
    sample = report.get("sample") or {}
    print(
        f"[jev_train] draw stamp={report.get('stamp')} "
        f"exam={sample.get('exam_source')} "
        f"parsed={sample.get('parsed')} rss={sample.get('rss')} exam_n={sample.get('exam')}"
    )
    print(f"JEV_TRAIN_STAMP={report.get('stamp')}")


def _reject_secrets(text: str) -> None:
    if SECRET_RE.search(text or ""):
        raise RuntimeError("refusing to write secret-like material")


def _b64_json(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        raise ValueError("empty grades")
    if raw[0] not in "{[":
        try:
            raw = base64.b64decode(raw, validate=True).decode("utf-8")
        except (ValueError, UnicodeDecodeError) as exc:
            raise ValueError("grades are not JSON or base64 JSON") from exc
    return raw


def parse_grades_payload(raw: str) -> dict:
    raw = _b64_json(raw)
    _reject_secrets(raw)
    blob = json.loads(raw)
    if not isinstance(blob, dict):
        raise ValueError("grades JSON must be an object")
    rows_in = blob.get("rows")
    if not isinstance(rows_in, list) or not rows_in:
        raise ValueError("grades JSON needs rows")
    if len(rows_in) > 120:
        raise ValueError("grades JSON has more than 120 rows")
    rows = []
    seen: set[str] = set()
    for raw_row in rows_in:
        if not isinstance(raw_row, dict):
            raise ValueError("grade row is not an object")
        title = str(raw_row.get("title") or "").strip()
        given_id = str(raw_row.get("id") or "").strip().lower()
        if title:
            tid = title_id(title)
        elif ID_RE.match(given_id):
            tid = given_id
        else:
            raise ValueError("grade row missing title")
        if tid in seen:
            raise ValueError(f"duplicate title hash {tid}")
        seen.add(tid)
        grade = str(raw_row.get("grade") or raw_row.get("human") or "").strip().upper()
        if grade not in {"K", "D", "?"}:
            raise ValueError(f"grade must be K, D, or ?, got {grade!r}")
        jev = str(raw_row.get("jev") or "").strip().upper()
        if jev and jev not in {"KEEP", "DROP"}:
            raise ValueError(f"jev must be KEEP or DROP, got {jev!r}")
        if raw_row.get("human_reason") is not None:
            human_reason = str(raw_row.get("human_reason") or "")
        else:
            human_reason = str(raw_row.get("note") or "")
        human_reason = human_reason.replace("\n", " ").strip()[:500]
        try:
            instrument = float(raw_row.get("new_instrument") or 0.0)
        except (TypeError, ValueError):
            instrument = 0.0
        rows.append({
            "id": tid,
            "title": title[:400],
            "source": str(raw_row.get("source") or "")[:160],
            "pool": str(raw_row.get("pool") or "")[:32],
            "published_at": str(raw_row.get("published_at") or "")[:80],
            "url": str(raw_row.get("url") or "")[:400],
            "date": str(raw_row.get("date") or "")[:32],
            "jev": jev,
            "reason": str(raw_row.get("reason") or "")[:80],
            "geo": str(raw_row.get("geo") or "")[:32],
            "actor_power": str(raw_row.get("actor_power") or "")[:32],
            "new_instrument": instrument,
            "grade": grade,
            "human_reason": human_reason,
        })
    return {
        "schema": SCHEMA_GRADES,
        "draw_stamp": str(blob.get("draw_stamp") or "")[:32],
        "nonce": str(blob.get("nonce") or "")[:80],
        "rows": rows,
    }


def read_draw(*, stamp: str = "", root: Path | None = None,
              ground: Path | None = None) -> dict | None:
    """The page only sends id / grade / reason. The draw on main has the rest."""
    root = root or ROOT
    ground = ground or (root / "00_grounding")
    paths: list[Path] = []
    if stamp:
        paths.append(train_dir(ground) / f"{stamp}_draw.json")
    paths.append(root / "dashboard" / "jev-train" / DRAW_NAME)
    for path in paths:
        if not path.is_file():
            continue
        try:
            blob = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(blob, dict) or not isinstance(blob.get("items"), list):
            continue
        if stamp and blob.get("stamp") and blob.get("stamp") != stamp:
            continue
        return blob
    return None


def hydrate_grades(payload: dict, draw: dict | None) -> dict:
    """Fill slim rows from the matching draw. Full rows stay as sent."""
    rows = list(payload.get("rows") or [])
    needs = any(not row.get("title") or row.get("jev") not in {"KEEP", "DROP"} for row in rows)
    if not needs:
        return payload
    if not draw or not isinstance(draw.get("items"), list):
        raise RuntimeError("slim grades need the matching draw.json on main")
    by_id = {}
    for item in draw.get("items") or []:
        if not isinstance(item, dict):
            continue
        tid = str(item.get("id") or "").strip().lower()
        if not tid and item.get("title"):
            tid = title_id(str(item.get("title")))
        if tid:
            by_id[tid] = item
    filled = []
    for row in rows:
        src = by_id.get(row.get("id") or "")
        if src is None:
            raise RuntimeError(f"grade id {row.get('id')} is not in draw {draw.get('stamp') or ''}")
        jev = str(src.get("jev") or row.get("jev") or "").strip().upper()
        if jev not in {"KEEP", "DROP"}:
            raise RuntimeError(f"draw row {row.get('id')} has no Jev KEEP/DROP")
        title = str(src.get("title") or row.get("title") or "").strip()
        if not title:
            raise RuntimeError(f"draw row {row.get('id')} has no title")
        try:
            instrument = float(src.get("new_instrument") or row.get("new_instrument") or 0.0)
        except (TypeError, ValueError):
            instrument = 0.0
        filled.append({
            **row,
            "id": title_id(title),
            "title": title[:400],
            "source": str(src.get("source") or row.get("source") or "")[:160],
            "pool": str(src.get("pool") or row.get("pool") or "")[:32],
            "published_at": str(src.get("published_at") or row.get("published_at") or "")[:80],
            "url": str(src.get("url") or row.get("url") or "")[:400],
            "date": str(src.get("date") or row.get("date") or "")[:32],
            "jev": jev,
            "reason": str(src.get("reason") or row.get("reason") or "")[:80],
            "geo": str(src.get("geo") or row.get("geo") or "")[:32],
            "actor_power": str(src.get("actor_power") or row.get("actor_power") or "")[:32],
            "new_instrument": instrument,
        })
    out = dict(payload)
    out["rows"] = filled
    if not out.get("draw_stamp"):
        out["draw_stamp"] = str(draw.get("stamp") or "")[:32]
    return out


def count_marks(rows: list[dict]) -> int:
    return sum(1 for row in rows if row.get("grade") in {"K", "D"})


def score_grades(rows: list[dict]) -> dict:
    """False keep = Jev KEEP and human D. False drop = Jev DROP and human K.

    ? is not a miss. Bits are recomputed from the title and the gate fields.
    """
    false_keep = []
    false_drop = []
    bit_keep: Counter = Counter()
    bit_drop: Counter = Counter()
    graded_rows = []
    for row in rows:
        bits = why_bits(row.get("title") or "", _decision_view(row))
        item = dict(row)
        item["bits"] = bits
        graded_rows.append(item)
        grade = row.get("grade")
        jev = row.get("jev")
        if grade not in {"K", "D"}:
            continue
        entry = {
            "id": item["id"],
            "title": item["title"],
            "source": item.get("source") or "",
            "pool": item.get("pool") or "",
            "jev": jev,
            "human": grade,
            "reason": item.get("reason") or "",
            "bits": bits,
            "human_reason": item.get("human_reason") or "",
        }
        if jev == "KEEP" and grade == "D":
            false_keep.append(entry)
            bit_keep.update(bits)
        elif jev == "DROP" and grade == "K":
            false_drop.append(entry)
            bit_drop.update(bits)
    human_keep = sum(1 for row in rows if row.get("grade") == "K")
    human_drop = sum(1 for row in rows if row.get("grade") == "D")
    unsure = sum(1 for row in rows if row.get("grade") == "?")
    jev_keep = sum(1 for row in rows if row.get("jev") == "KEEP")
    jev_drop = sum(1 for row in rows if row.get("jev") == "DROP")
    return {
        "rows": graded_rows,
        "false_keep": false_keep,
        "false_drop": false_drop,
        "bits_on_misses": {
            "false_keep": {name: int(bit_keep.get(name, 0)) for name in BIT_ORDER},
            "false_drop": {name: int(bit_drop.get(name, 0)) for name in BIT_ORDER},
        },
        "counts": {
            "rows": len(rows),
            "human_keep": human_keep,
            "human_drop": human_drop,
            "unsure": unsure,
            "marked": human_keep + human_drop,
            "jev_keep": jev_keep,
            "jev_drop": jev_drop,
            "false_keep": len(false_keep),
            "false_drop": len(false_drop),
        },
    }


def you_as_jev(grade: str) -> str:
    """K lines up with KEEP and D with DROP. ? stays ?."""
    return {"K": "KEEP", "D": "DROP"}.get(grade or "", grade or "")


def marks_disagree(grade: str, jev: str) -> bool:
    return you_as_jev(grade) != (jev or "")


def blank_disagreements(rows: list[dict]) -> list[dict]:
    """You != Jev and human_reason is blank. Empty reasons are still allowed."""
    flagged = []
    for row in rows:
        if str(row.get("human_reason") or "").strip():
            continue
        if marks_disagree(str(row.get("grade") or "?"), str(row.get("jev") or "")):
            flagged.append(row)
    return flagged


def _cell(text: str) -> str:
    return (text or "").replace("|", "/").replace("\n", " ").strip()


def render_session_md(session: dict) -> str:
    counts = session.get("counts") or {}
    lines = [
        f"# Jev train {session.get('stamp')}",
        "",
        f"Draw `{session.get('draw_stamp') or ''}`.",
        f"Human keep {counts.get('human_keep', 0)}, "
        f"drop {counts.get('human_drop', 0)}, "
        f"unsure {counts.get('unsure', 0)}.",
        f"Jev keep {counts.get('jev_keep', 0)}, drop {counts.get('jev_drop', 0)}.",
        f"False keep {counts.get('false_keep', 0)}. "
        f"False drop {counts.get('false_drop', 0)}.",
        "",
        "| # | jev | you | bits | source | title | note |",
        "|---:|---|---|---|---|---|---|",
    ]
    for index, row in enumerate(session.get("items") or [], start=1):
        bits = " ".join(row.get("bits") or [])
        lines.append(
            f"| {index} | {_cell(row.get('jev') or '')} | {_cell(row.get('grade') or '')} "
            f"| {_cell(bits)} | {_cell(row.get('source') or '')} | {_cell(row.get('title') or '')} "
            f"| {_cell(row.get('human_reason') or '')} |"
        )
    lines.append("")
    return "\n".join(lines)


def issue_body(session: dict, grade: dict, *, json_url: str, grade_url: str) -> str:
    counts = grade.get("counts") or session.get("counts") or {}
    lines = [
        "Paste this to Grok to discuss.",
        "",
        f"Session `{session.get('stamp')}` · draw `{session.get('draw_stamp') or ''}`.",
        f"JSON: {json_url}",
        f"Grade: {grade_url}",
        "",
        "| | keep | drop |",
        "|---|---:|---:|",
        f"| human | {counts.get('human_keep', 0)} | {counts.get('human_drop', 0)} |",
        f"| Jev | {counts.get('jev_keep', 0)} | {counts.get('jev_drop', 0)} |",
        "",
        f"False keep: {counts.get('false_keep', 0)}",
        f"False drop: {counts.get('false_drop', 0)}",
        f"Unsure: {counts.get('unsure', 0)}",
        "",
        "## Rows",
        "",
        "| title | Jev | You | human_reason |",
        "|---|---|---|---|",
    ]
    items = list(session.get("items") or [])
    if not items:
        lines.append("|  |  |  | none |")
    for row in items:
        lines.append(
            f"| {_cell(row.get('title') or '')} | {_cell(row.get('jev') or '')} "
            f"| {_cell(row.get('grade') or '')} | {_cell(row.get('human_reason') or '')} |"
        )
    flagged = blank_disagreements(items)
    lines += [
        "",
        "## FLAG blank reason",
        "",
        "Rows where You != Jev and human_reason is blank: "
        f"{len(flagged)}.",
        "",
        "| title | Jev | You | human_reason |",
        "|---|---|---|---|",
    ]
    if not flagged:
        lines.append("|  |  |  | none |")
    for row in flagged:
        lines.append(
            f"| {_cell(row.get('title') or '')} | {_cell(row.get('jev') or '')} "
            f"| {_cell(row.get('grade') or '')} | |"
        )
    lines += [
        "",
        "## False keeps",
        "",
        "| bits | reason | title |",
        "|---|---|---|",
    ]
    for row in grade.get("false_keep") or []:
        lines.append(
            f"| {_cell(' '.join(row.get('bits') or []))} | {_cell(row.get('reason') or '')} "
            f"| {_cell(row.get('title') or '')} |"
        )
    if not grade.get("false_keep"):
        lines.append("|  |  | none |")
    lines += [
        "",
        "## False drops",
        "",
        "| bits | reason | title |",
        "|---|---|---|",
    ]
    for row in grade.get("false_drop") or []:
        lines.append(
            f"| {_cell(' '.join(row.get('bits') or []))} | {_cell(row.get('reason') or '')} "
            f"| {_cell(row.get('title') or '')} |"
        )
    if not grade.get("false_drop"):
        lines.append("|  |  | none |")
    bits = grade.get("bits_on_misses") or {}
    lines += ["", "## Bits on misses", ""]
    for kind in ("false_keep", "false_drop"):
        fired = [
            f"{name} {n}"
            for name, n in (bits.get(kind) or {}).items()
            if n
        ]
        lines.append(f"- {kind}: {', '.join(fired) if fired else 'none'}")
    nonce = session.get("nonce") or ""
    if nonce:
        lines += ["", f"nonce: {nonce}"]
    lines.append("")
    return "\n".join(lines)


def append_hard_misses(blob: dict, misses: list[dict]) -> dict:
    """Record misses. Does not delete items and does not touch closed lists."""
    items = [dict(it) for it in (blob.get("items") or []) if isinstance(it, dict)]
    have = {_row_id(it) for it in items}
    for row in misses:
        tid = _row_id(row)
        if tid in have:
            continue
        items.append({
            "id": tid,
            "title": row.get("title") or "",
            "source": row.get("source") or "",
            "published_at": row.get("published_at") or "",
            "url": row.get("url") or "",
            "date": row.get("date") or "",
            "archive": "hard_miss",
            "from_grade": True,
        })
        have.add(tid)
    out = dict(blob)
    out["items"] = items
    out["schema"] = SCHEMA_HARD
    out.setdefault("note", HARD_MISS_NOTE)
    out.setdefault("cursor", 0)
    return out


def blob_urls(stamp: str, repo: str | None = None, branch: str = DEPLOY_BRANCH) -> tuple[str, str]:
    repo = repo or os.environ.get("GITHUB_REPOSITORY") or REPO_SLUG
    server = os.environ.get("GITHUB_SERVER_URL") or "https://github.com"
    base = f"{server}/{repo}/blob/{branch}/00_grounding/jev_train/{stamp}"
    return base + ".json", base + "_grade.json"


def write_grade(
    payload: dict,
    *,
    stamp: str = "",
    now: dt.datetime | None = None,
    root: Path | None = None,
    ground: Path | None = None,
    write: bool = False,
    minimum: int = MIN_MARKS,
) -> dict:
    now = now or dt.datetime.now(dt.timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=dt.timezone.utc)
    root = root or ROOT
    ground = ground or (root / "00_grounding")
    payload = hydrate_grades(
        payload,
        read_draw(stamp=str(payload.get("draw_stamp") or ""), root=root, ground=ground),
    )
    rows = payload.get("rows") or []
    marked = count_marks(rows)
    if marked < minimum:
        raise RuntimeError(f"need at least {minimum} rows marked K or D, got {marked}")
    scored = score_grades(rows)
    directory = train_dir(ground)
    if write:
        directory.mkdir(parents=True, exist_ok=True)
        used = allocate_stamp(directory, stamp, now, kind="session")
    else:
        used = stamp if STAMP_RE.match(stamp or "") else _stamp_now(now)
    session = {
        "schema": SCHEMA_SESSION,
        "stamp": used,
        "draw_stamp": payload.get("draw_stamp") or "",
        "nonce": payload.get("nonce") or "",
        "generated_at": now.isoformat(),
        "counts": scored["counts"],
        "items": [
            {
                "id": row["id"],
                "title": row["title"],
                "source": row.get("source") or "",
                "pool": row.get("pool") or "",
                "published_at": row.get("published_at") or "",
                "url": row.get("url") or "",
                "date": row.get("date") or "",
                "jev": row["jev"],
                "reason": row.get("reason") or "",
                "geo": row.get("geo") or "",
                "actor_power": row.get("actor_power") or "",
                "new_instrument": row.get("new_instrument") or 0,
                "bits": row.get("bits") or [],
                "grade": row.get("grade"),
                "human_reason": row.get("human_reason") or "",
            }
            for row in scored["rows"]
        ],
    }
    grade = {
        "schema": SCHEMA_GRADE,
        "stamp": used,
        "draw_stamp": payload.get("draw_stamp") or "",
        "nonce": payload.get("nonce") or "",
        "generated_at": now.isoformat(),
        "counts": scored["counts"],
        "false_keep": scored["false_keep"],
        "false_drop": scored["false_drop"],
        "bits_on_misses": scored["bits_on_misses"],
    }
    json_url, grade_url = blob_urls(used)
    session["json_url"] = json_url
    grade["json_url"] = json_url
    grade["grade_url"] = grade_url
    body = issue_body(session, grade, json_url=json_url, grade_url=grade_url)
    _reject_secrets(json.dumps(session) + json.dumps(grade) + body)
    result = {"session": session, "grade": grade, "issue_body": body, "stamp": used}
    if write:
        session_path = directory / f"{used}.json"
        md_path = directory / f"{used}.md"
        grade_path = directory / f"{used}_grade.json"
        for path in (session_path, md_path, grade_path):
            if path.exists():
                raise FileExistsError(f"refusing to overwrite {path.name}")
        _write_json(session_path, session)
        md_path.write_text(render_session_md(session), encoding="utf-8")
        _write_json(grade_path, grade)
        hard_path = hard_miss_path(ground)
        ensure_pool_files(ground)
        hard_blob = read_hard_misses(hard_path)
        before = json.dumps(hard_blob.get("items"), sort_keys=True)
        hard_blob = append_hard_misses(
            hard_blob, scored["false_keep"] + scored["false_drop"],
        )
        after = json.dumps(hard_blob.get("items"), sort_keys=True)
        if after != before:
            _write_json(hard_path, hard_blob)
        closed = ground / "jev_closed_lists.json"
        result["paths"] = {
            "session": str(session_path),
            "markdown": str(md_path),
            "grade": str(grade_path),
        }
        result["closed_lists_untouched"] = closed
    print(
        f"[jev_train] grade stamp={used} "
        f"false_keep={scored['counts']['false_keep']} "
        f"false_drop={scored['counts']['false_drop']}"
    )
    print(f"JEV_TRAIN_STAMP={used}")
    return result


def _github_json(method: str, url: str, token: str, body: dict | None = None):
    import urllib.error
    import urllib.request
    data = None if body is None else json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "User-Agent": "fullscan-jev-train/1",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8")
            return resp.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        if exc.code == 422 and "already_exists" in detail and method == "POST" and url.endswith("/labels"):
            return exc.code, {}
        raise RuntimeError(f"GitHub {exc.code} {method} {url}: {detail[:300]}") from None


def publish_issue(session: dict, grade: dict, *, token: str, repo: str,
                  body: str | None = None) -> str:
    """Open or update the issue titled `jev-train STAMP`. Token stays in Actions."""
    if not token:
        raise RuntimeError("GITHUB_TOKEN is empty")
    title = f"jev-train {session.get('stamp')}"
    json_url = session.get("json_url") or blob_urls(session.get("stamp") or "", repo)[0]
    grade_url = (grade.get("grade_url") or blob_urls(session.get("stamp") or "", repo)[1])
    text = body or issue_body(session, grade, json_url=json_url, grade_url=grade_url)
    _reject_secrets(text)
    api = f"https://api.github.com/repos/{repo}"
    _github_json("POST", f"{api}/labels", token, {
        "name": ISSUE_LABEL,
        "color": "1d4ed8",
        "description": "Jev hop-0 training sheet",
    })
    _status, listing = _github_json(
        "GET",
        f"{api}/issues?state=all&labels={ISSUE_LABEL}&per_page=50",
        token,
    )
    found = None
    if isinstance(listing, list):
        for issue in listing:
            if isinstance(issue, dict) and issue.get("title") == title:
                found = issue
                break
    if found and found.get("number"):
        _status, updated = _github_json(
            "PATCH", f"{api}/issues/{found['number']}", token,
            {"title": title, "body": text, "state": "open", "labels": [ISSUE_LABEL]},
        )
        url = updated.get("html_url") or found.get("html_url") or ""
    else:
        _status, created = _github_json(
            "POST", f"{api}/issues", token,
            {"title": title, "body": text, "labels": [ISSUE_LABEL]},
        )
        url = created.get("html_url") or ""
    if not url:
        raise RuntimeError("GitHub did not return an issue URL")
    print(f"JEV_TRAIN_ISSUE={url}")
    return url


def _load_pair(ground: Path, stamp: str) -> tuple[dict, dict]:
    directory = train_dir(ground)
    session = _load_json(directory / f"{stamp}.json")
    grade = _load_json(directory / f"{stamp}_grade.json")
    if not isinstance(session, dict) or not isinstance(grade, dict):
        raise RuntimeError(f"missing session or grade for {stamp}")
    return session, grade


def cmd_draw(args) -> int:
    key = api_key()
    if not key:
        print("[jev_train] JEV_API_KEY / TYPESAFE_API_KEY is empty")
        return 1
    seed = int(args.seed) if str(args.seed or "").strip() else None
    try:
        run_draw(
            live=True, key=key, workers=max(1, int(args.workers or 16)),
            seed=seed, stamp=args.stamp or "", write=True,
        )
    except Exception as exc:  # noqa: BLE001 — CLI boundary
        print(f"[jev_train] {type(exc).__name__}: {exc}")
        return 1
    return 0


def cmd_grade(args) -> int:
    raw = os.environ.get("GRADES_JSON") or ""
    if args.grades:
        raw = Path(args.grades).read_text(encoding="utf-8")
    if not raw.strip():
        print("[jev_train] GRADES_JSON is empty")
        return 1
    try:
        payload = parse_grades_payload(raw)
        write_grade(payload, stamp=args.stamp or "", write=True)
    except Exception as exc:  # noqa: BLE001 — CLI boundary
        print(f"[jev_train] {type(exc).__name__}: {exc}")
        return 1
    return 0


def cmd_issue(args) -> int:
    token = (os.environ.get("GITHUB_TOKEN") or "").strip()
    repo = (os.environ.get("GITHUB_REPOSITORY") or REPO_SLUG).strip()
    stamp = (args.stamp or os.environ.get("STAMP") or "").strip()
    if not stamp:
        print("[jev_train] stamp is empty")
        return 1
    try:
        session, grade = _load_pair(GROUND, stamp)
        publish_issue(session, grade, token=token, repo=repo)
    except Exception as exc:  # noqa: BLE001 — CLI boundary
        print(f"[jev_train] {type(exc).__name__}: {exc}")
        return 1
    return 0


def build_parser():
    import argparse
    parser = argparse.ArgumentParser(description="Jev hop-0 trainer")
    sub = parser.add_subparsers(dest="cmd", required=True)
    draw = sub.add_parser("draw", help="Draw 100 titles and run the current hop-0 gate")
    draw.add_argument("--stamp", default="")
    draw.add_argument("--seed", default="")
    draw.add_argument("--workers", type=int, default=16)
    draw.set_defaults(func=cmd_draw)
    grade = sub.add_parser("grade", help="Write the session, markdown, and grade JSON")
    grade.add_argument("--stamp", default="")
    grade.add_argument("--grades", default="", help="Path to grades JSON. Default: GRADES_JSON env.")
    grade.set_defaults(func=cmd_grade)
    issue = sub.add_parser("issue", help="Open or update the jev-train issue")
    issue.add_argument("--stamp", default="")
    issue.set_defaults(func=cmd_issue)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
