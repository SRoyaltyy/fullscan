"""Hop-0 eval draw. jev_gold.json is not the exam.

Each run draws a fresh unlabeled sheet and scores it with the current
gate. Labels stay empty until a person fills them. One knob may change
per round; a missed headline is never added as a closed-list line.

  50 titles   01_daily/news/*_parsed.json (digest if that day has no parse),
              stratified by date
  50 titles   today's Google News RSS watchlist, after Jaccard so the
              same wire is not graded twice
  20 titles   frozen holdout. Hashes live in 00_grounding/jev_holdout.json
              and are excluded from every tuning sample.

Does not write keep.json and does not call Lane.
"""
from __future__ import annotations

import hashlib
import inspect
import json
import os
import random
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from dataclasses import replace
from pathlib import Path
from xml.etree import ElementTree as ET

from .jev_gate import (
    GROUND,
    INSTRUMENT_KEEP,
    JACCARD_DROP,
    MATERIAL_KEEP,
    NEWS_DIR,
    ROOT,
    TRASH_NOUL,
    _load_json,
    _write_json,
    calendar_day,
    gate,
    jaccard,
    load_chokepoint_state,
    load_gold,
    normalize_title,
    tokens,
)
try:
    from .jev_gate import ALLOWED_KNOBS, GateKnobs, state_for_knobs
except ImportError:
    # Live main gate has no eval knobs. Draw still uses gate() as it is.
    from dataclasses import dataclass as _dataclass

    ALLOWED_KNOBS = ()

    @_dataclass
    class GateKnobs:
        action_material: float = MATERIAL_KEEP
        new_instrument: float = INSTRUMENT_KEEP
        reaction_regex: str | None = None
        listed_token_in_title: bool = False
        listed_tokens: frozenset = frozenset()
        reprint_weather_days: int | None = None
        new_verbs: list | None = None

    def state_for_knobs(state: dict, knobs=None) -> dict:
        return state
import datetime as dt

PARSED_N = 50
RSS_N = 50
HOLDOUT_N = 20
DROP_RATE_MIN = 0.70
SHOULD_KEEP_FALSE_DROP_MAX = 2
SHOULD_DROP_FALSE_KEEP_MAX = 3
ROUNDS_DIR = GROUND / "jev_rounds"
HOLDOUT_PATH = GROUND / "jev_holdout.json"
HASH_ALGO = "sha256-norm-16"

# Same search queries as collectors/rss_news.py. when:1d keeps the draw on today.
RSS_WATCHLIST = (
    ("rss_google_markets", "stock market OR earnings OR IPO"),
    ("rss_google_macro", "federal reserve OR interest rate OR inflation OR GDP"),
    ("rss_google_commodities", "oil price OR gold price OR commodities market"),
    ("rss_google_geopolitics", "trade war OR tariffs OR sanctions OR geopolitical risk"),
)
_UA = "Mozilla/5.0 (compatible; fullscan-jev-eval/1)"

RUBRIC = (
    ("must_keep", "dated instrument / listed name / regulator print (CAFE, Boeing glitch, Apple verdict, PDUFA)"),
    ("should_keep", "borderline but Lane should see it (SpaceX earnings, named Chapter 11, Citrix zero-day)"),
    ("should_drop", "opinion, tape recap, gold-falls-on-Fed, protester arrest, stale Hormuz"),
    ("must_drop", 'tabloid, ? column, "what it means"'),
)


def title_id(title: str) -> str:
    norm = normalize_title(title)
    return hashlib.sha256(norm.encode("utf-8")).hexdigest()[:16]


def gold_norms(gold: dict | None = None) -> set[str]:
    blob = gold if gold is not None else load_gold()
    out = set()
    for it in (blob or {}).get("items") or []:
        if isinstance(it, dict):
            norm = normalize_title(str(it.get("title") or ""))
            if norm:
                out.add(norm)
    return out


def _row(title: str, source: str, published_at: str, url: str,
         day: str, archive: str, query: str = "") -> dict | None:
    title = (title or "").strip()
    norm = normalize_title(title)
    if not title or not norm:
        return None
    return {
        "id": title_id(title),
        "title": title[:400],
        "source": (source or "")[:160],
        "published_at": published_at or day,
        "url": (url or "")[:400],
        "date": day,
        "archive": archive,
        "query": query,
    }


def _parsed_items(blob: dict, day: str) -> list[dict]:
    out = []
    for it in blob.get("all_items") or []:
        if not isinstance(it, dict):
            continue
        row = _row(
            str(it.get("title") or ""),
            str(it.get("source") or ""),
            str(it.get("published_at") or day),
            str(it.get("url") or ""),
            day,
            "parsed",
        )
        if row:
            out.append(row)
    return out


def _digest_items(blob: dict, day: str) -> list[dict]:
    out = []
    items = list(blob.get("top_signal") or []) + list(blob.get("index_digests") or [])
    for it in items:
        if not isinstance(it, dict):
            continue
        row = _row(
            str(it.get("news_title") or it.get("digest") or it.get("title") or ""),
            str(it.get("source") or "finviz_digest"),
            day,
            "",
            day,
            "digest",
        )
        if row:
            out.append(row)
    return out


def load_archive(news_dir: Path | None = None) -> list[dict]:
    """One row per normalized title. Parsed wins; digest fills an empty day."""
    news_dir = news_dir or NEWS_DIR
    by_date: dict[str, list[dict]] = {}
    if news_dir.is_dir():
        for path in sorted(news_dir.glob("*_parsed.json")):
            m = re.search(r"(20\d{2}-\d{2}-\d{2})", path.name)
            if not m:
                continue
            blob = _load_json(path)
            if isinstance(blob, dict):
                rows = _parsed_items(blob, m.group(1))
                if rows:
                    by_date[m.group(1)] = rows
        for path in sorted(news_dir.glob("*_finviz_digest.json")):
            m = re.search(r"(20\d{2}-\d{2}-\d{2})", path.name)
            if not m or m.group(1) in by_date:
                continue
            blob = _load_json(path)
            if isinstance(blob, dict):
                rows = _digest_items(blob, m.group(1))
                if rows:
                    by_date[m.group(1)] = rows
    seen: set[str] = set()
    out: list[dict] = []
    for day in sorted(by_date):
        for row in by_date[day]:
            if row["id"] in seen:
                continue
            seen.add(row["id"])
            out.append(row)
    return out


def parse_rss_xml(data: bytes, query_name: str) -> list[dict]:
    try:
        root = ET.fromstring(data)
    except ET.ParseError:
        return []
    rows = []
    for it in root.findall(".//item"):
        title = (it.findtext("title") or "").strip()
        link = (it.findtext("link") or "").strip()
        published = (it.findtext("pubDate") or it.findtext("published") or "").strip()
        src_el = it.find("source")
        source = ""
        if src_el is not None and src_el.text:
            source = src_el.text.strip()
        row = _row(
            title,
            source or query_name,
            published,
            link,
            calendar_day(published),
            "rss",
            query_name,
        )
        if row:
            rows.append(row)
    return rows


def fetch_rss(queries=RSS_WATCHLIST, timeout: float = 25.0,
              opener=None) -> list[dict]:
    """Today's Google News RSS watchlist. Stdlib only."""
    opener = opener or urllib.request.urlopen
    rows: list[dict] = []
    for name, query in queries:
        url = (
            "https://news.google.com/rss/search?q="
            + urllib.parse.quote_plus(query + " when:1d")
            + "&hl=en-US&gl=US&ceid=US:en"
        )
        req = urllib.request.Request(url, headers={"User-Agent": _UA, "Accept": "application/rss+xml"})
        try:
            with opener(req, timeout=timeout) as resp:
                data = resp.read()
        except (OSError, urllib.error.URLError, TimeoutError) as exc:
            print(f"[jev_eval] rss {name} failed: {exc}")
            continue
        got = parse_rss_xml(data, name)
        print(f"[jev_eval] rss {name}: {len(got)}")
        rows.extend(got)
    return rows


def _allocate(counts: dict[str, int], n: int) -> dict[str, int]:
    total = sum(counts.values())
    if total <= 0 or n <= 0:
        return {k: 0 for k in counts}
    raw = {k: n * v / total for k, v in counts.items()}
    alloc = {k: min(counts[k], int(raw[k])) for k in counts}
    left = n - sum(alloc.values())
    order = sorted(counts, key=lambda k: (raw[k] - int(raw[k]), counts[k]), reverse=True)
    while left > 0:
        moved = False
        for key in order:
            if left <= 0:
                break
            if alloc[key] < counts[key]:
                alloc[key] += 1
                left -= 1
                moved = True
        if not moved:
            break
    return alloc


def _near(row: dict, blocked: list[frozenset[str]]) -> bool:
    toks = tokens(row.get("title") or "")
    return any(jaccard(toks, other) >= JACCARD_DROP for other in blocked)


def stratified_draw(rows: list[dict], n: int, rng: random.Random, *,
                    gold: set[str],
                    blocked: list[frozenset[str]] | None = None
                    ) -> tuple[list[dict], int]:
    """Stratify by date. A gold title that comes up is discarded and redrawn."""
    blocked = list(blocked or [])
    by_date: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        by_date[row.get("date") or ""].append(row)
    counts = {day: len(bucket) for day, bucket in by_date.items()}
    alloc = _allocate(counts, n)
    gold_n = 0
    picked: list[dict] = []
    picked_ids: set[str] = set()
    leftovers: list[dict] = []

    def consider(row: dict, into: list[dict], quota: int) -> str:
        nonlocal gold_n
        if len(into) >= quota and quota >= 0:
            return "full"
        norm = normalize_title(row.get("title") or "")
        if norm in gold:
            gold_n += 1
            return "gold"
        if row.get("id") in picked_ids:
            return "skip"
        if _near(row, blocked):
            return "skip"
        into.append(row)
        picked_ids.add(row.get("id") or "")
        blocked.append(tokens(row.get("title") or ""))
        return "take"

    for day, bucket in by_date.items():
        order = list(bucket)
        rng.shuffle(order)
        got: list[dict] = []
        want = alloc.get(day, 0)
        for row in order:
            if len(got) >= want:
                leftovers.append(row)
                continue
            mark = consider(row, got, want)
            if mark == "skip" or mark == "gold":
                if mark != "gold":
                    leftovers.append(row)
        picked.extend(got)

    if len(picked) < n:
        rng.shuffle(leftovers)
        for row in leftovers:
            if len(picked) >= n:
                break
            consider(row, picked, n)
    return picked, gold_n


def pool_draw(rows: list[dict], n: int, rng: random.Random, *,
              gold: set[str],
              blocked: list[frozenset[str]] | None = None
              ) -> tuple[list[dict], int]:
    """Random draw. Gold hits are discarded and redrawn. Near-dupes are skipped."""
    blocked = list(blocked or [])
    order = list(rows)
    rng.shuffle(order)
    picked: list[dict] = []
    picked_ids: set[str] = set()
    gold_n = 0
    for row in order:
        if len(picked) >= n:
            break
        norm = normalize_title(row.get("title") or "")
        if norm in gold:
            gold_n += 1
            continue
        if row.get("id") in picked_ids:
            continue
        if _near(row, blocked):
            continue
        picked.append(row)
        picked_ids.add(row.get("id") or "")
        blocked.append(tokens(row.get("title") or ""))
    return picked, gold_n


def load_holdout(path: Path) -> dict | None:
    blob = _load_json(path)
    if not isinstance(blob, dict) or not blob.get("ids") or not blob.get("items"):
        return None
    return blob


def freeze_holdout(items: list[dict], now: dt.datetime) -> dict:
    return {
        "created_at": now.isoformat(),
        "algo": HASH_ALGO,
        "note": (
            "Frozen before tuning. Eval tuning samples must not contain "
            "these ids. Do not rewrite this file."
        ),
        "ids": [it["id"] for it in items],
        "items": items,
    }


def draw_sample(*, news_dir: Path, holdout_path: Path, rng: random.Random,
                now: dt.datetime, rss_rows: list[dict] | None,
                parsed_n: int, rss_n: int, holdout_n: int,
                opener=None) -> dict:
    archive = load_archive(news_dir)
    gold = gold_norms()
    gold_n = 0
    if holdout_path.exists():
        existing = load_holdout(holdout_path)
        if existing is None:
            raise RuntimeError(f"holdout file exists but is unreadable: {holdout_path}")
    else:
        existing = None
    created = False
    if existing is None:
        holdout, g = stratified_draw(archive, holdout_n, rng, gold=gold)
        gold_n += g
        if len(holdout) < holdout_n:
            raise RuntimeError(
                f"holdout short {len(holdout)}/{holdout_n} from {len(archive)} archive titles"
            )
        holdout_blob = freeze_holdout(holdout, now)
        created = True
    else:
        holdout_blob = existing
        holdout = list(existing.get("items") or [])

    holdout_ids = {str(i) for i in holdout_blob.get("ids") or []}
    holdout_ids |= {it.get("id") for it in holdout}
    blocked = [tokens(it.get("title") or "") for it in holdout]
    tuning = [r for r in archive if r.get("id") not in holdout_ids]
    parsed, g = stratified_draw(tuning, parsed_n, rng, gold=gold, blocked=blocked)
    gold_n += g
    if len(parsed) < parsed_n:
        raise RuntimeError(
            f"parsed sample short {len(parsed)}/{parsed_n} from {len(tuning)} tuning titles"
        )

    blocked = blocked + [tokens(r.get("title") or "") for r in parsed]
    if rss_rows is None:
        rss_raw = fetch_rss(opener=opener)
    else:
        rss_raw = list(rss_rows)
    rss_fetched = len(rss_raw)
    rss_unique = []
    seen_rss: set[str] = set()
    for row in rss_raw:
        if row.get("id") in seen_rss or row.get("id") in holdout_ids:
            continue
        seen_rss.add(row.get("id") or "")
        rss_unique.append(row)
    rss, g = pool_draw(rss_unique, rss_n, rng, gold=gold, blocked=blocked)
    gold_n += g
    if len(rss) < rss_n:
        raise RuntimeError(
            f"rss sample short {len(rss)}/{rss_n} "
            f"from {len(rss_unique)} unique wires ({rss_fetched} fetched)"
        )

    tuning_ids = {r["id"] for r in parsed} | {r["id"] for r in rss}
    overlap = sorted(tuning_ids & holdout_ids)
    if overlap:
        raise RuntimeError(f"tuning sample contains holdout ids: {overlap[:5]}")
    for row in parsed + rss + holdout:
        if normalize_title(row.get("title") or "") in gold:
            raise RuntimeError("gold title survived the draw")
    return {
        "archive_n": len(archive),
        "parsed": parsed,
        "rss": rss,
        "holdout": holdout,
        "holdout_blob": holdout_blob,
        "holdout_created": created,
        "gold_discarded_redrawn": gold_n,
        "rss_fetched": rss_fetched,
        "rss_unique": len(rss_unique),
        "tuning_overlap_holdout": overlap,
    }


def _unit_float(raw: str) -> float:
    try:
        val = float(raw)
    except ValueError as exc:
        raise ValueError(f"threshold must be a float, got {raw!r}") from exc
    if not 0.0 <= val <= 1.0:
        raise ValueError(f"threshold {val} is outside 0..1")
    return val


def _as_bool(raw: str) -> bool:
    val = raw.strip().lower()
    if val in {"1", "true", "yes", "on"}:
        return True
    if val in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"boolean knob got {raw!r}")


def resolve_knobs(specs: list[str] | None) -> tuple[GateKnobs, str | None, str | None]:
    """Return knobs, changed-name, raw-value. Exactly one spec, or none."""
    specs = [s.strip() for s in (specs or []) if s and str(s).strip()]
    if len(specs) > 1:
        raise ValueError("tweak ONE parameter per round")
    if not specs:
        return GateKnobs(), None, None
    spec = specs[0]
    if "=" not in spec:
        raise ValueError(f"knob must be name=value, got {spec!r}")
    name, raw = spec.split("=", 1)
    name = name.strip()
    raw = raw.strip()
    if name not in ALLOWED_KNOBS:
        raise ValueError(
            f"knob {name!r} is not allowed ({', '.join(ALLOWED_KNOBS)}). "
            "Add a shape to the closed list only after the same miss recurs, "
            "never the full headline, and never from this flag."
        )
    base = GateKnobs()
    if name == "action_material":
        knobs = replace(base, action_material=_unit_float(raw))
    elif name == "new_instrument":
        knobs = replace(base, new_instrument=_unit_float(raw))
    elif name == "reaction_regex":
        if len(raw) > 240:
            raise ValueError("reaction_regex is a shape, not a headline")
        re.compile(raw, re.IGNORECASE)
        knobs = replace(base, reaction_regex=raw)
    elif name == "listed_token_in_title":
        knobs = replace(base, listed_token_in_title=_as_bool(raw))
    elif name == "reprint_weather_days":
        try:
            days = int(raw)
        except ValueError as exc:
            raise ValueError(f"reprint_weather_days must be an int, got {raw!r}") from exc
        if days < 0 or days > 3650:
            raise ValueError("reprint_weather_days out of range")
        knobs = replace(base, reprint_weather_days=days)
    elif name == "new_verbs":
        verbs = tuple(v.strip().lower() for v in raw.split(",") if v.strip())
        if not verbs:
            raise ValueError("new_verbs is empty")
        if any(len(v) > 48 for v in verbs):
            raise ValueError("new_verbs entries are shapes, not headlines")
        knobs = replace(base, new_verbs=verbs)
    else:
        raise ValueError(name)
    return knobs, name, raw


def load_finviz_tickers(root: Path | None = None) -> frozenset[str]:
    root = root or ROOT
    folder = root / "data" / "finviz"
    path = folder / "latest.csv"
    if not path.is_file():
        found = sorted(folder.glob("*.csv")) if folder.is_dir() else []
        path = found[-1] if found else None
    if path is None or not Path(path).is_file():
        return frozenset()
    import csv
    out: set[str] = set()
    with Path(path).open(newline="", encoding="utf-8", errors="replace") as fh:
        reader = csv.DictReader(fh)
        field = ""
        for col in reader.fieldnames or []:
            if col.strip().lower() == "ticker":
                field = col
                break
        if not field:
            return frozenset()
        for rec in reader:
            tok = str(rec.get(field) or "").strip().lower()
            if tok.isalnum() and len(tok) >= 2:
                out.add(tok)
    return frozenset(out)


def prepare_knobs(knobs: GateKnobs) -> GateKnobs:
    if knobs.listed_token_in_title and not knobs.listed_tokens:
        toks = load_finviz_tickers()
        if not toks:
            raise RuntimeError(
                "listed_token_in_title is on but data/finviz/*.csv has no tickers"
            )
        knobs = replace(knobs, listed_tokens=toks)
    return knobs


def effective_params(knobs: GateKnobs, state: dict) -> dict:
    verbs = list(knobs.new_verbs) if knobs.new_verbs else list(state.get("new_verbs_default") or [])
    days = knobs.reprint_weather_days
    if days is None:
        days = int(state.get("days_threshold") or 14)
    return {
        "action_material": knobs.action_material,
        "new_instrument": knobs.new_instrument,
        "reaction_regex": knobs.reaction_regex,
        "listed_token_in_title": bool(knobs.listed_token_in_title),
        "reprint_weather_days": int(days),
        "new_verbs": verbs,
        "hop0_code_rules": "on",
        "jaccard_drop": JACCARD_DROP,
        "trash_noul": TRASH_NOUL,
        "material_keep_default": MATERIAL_KEEP,
        "instrument_keep_default": INSTRUMENT_KEEP,
    }


def knob_changed_record(name: str | None, knobs: GateKnobs, state_before: dict) -> dict | None:
    if not name:
        return None
    before = effective_params(GateKnobs(), state_before)
    after = effective_params(knobs, state_for_knobs(state_before, knobs))
    return {"name": name, "from": before.get(name), "to": after.get(name)}


def diff_knobs(params: dict, previous: dict | None) -> dict | None:
    """One changed param against the previous round. None when there is no previous."""
    if previous is None:
        return None
    changed = [
        key for key in sorted(set(params) | set(previous))
        if params.get(key) != previous.get(key)
    ]
    if len(changed) > 1:
        raise RuntimeError("one knob per round; changed " + ", ".join(changed))
    if not changed:
        return None
    key = changed[0]
    return {"name": key, "from": previous.get(key), "to": params.get(key)}


def _latest_round_params(rounds_dir: Path) -> dict | None:
    if not rounds_dir.is_dir():
        return None
    files = sorted(
        p for p in rounds_dir.glob("*.json")
        if p.is_file() and not p.name.endswith("_sheet.json")
    )
    if not files:
        return None
    try:
        blob = json.loads(files[-1].read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    params = blob.get("params") if isinstance(blob, dict) else None
    return params if isinstance(params, dict) else None


def keep_via(decision: str, reason: str) -> str:
    if decision != "keep":
        return ""
    if reason == "code_shape":
        return "keep_shaped"
    if reason.startswith("code_") and reason != "code_leftover":
        return "code_override"
    return "jev"


def score_rules(items: list[dict], *, drop_rate: float | None = None) -> dict:
    """Four hop-0 rules. pass is null while any label is empty."""
    unlabeled = [it for it in items if not str(it.get("label") or "").strip()]
    labeled = [it for it in items if str(it.get("label") or "").strip()]

    def n_where(pred: str, lab: str) -> int:
        return sum(
            1 for it in labeled
            if it.get("predicted") == pred and it.get("label") == lab
        )

    if drop_rate is None:
        n_all = len(items)
        n_drop = sum(1 for it in items if it.get("predicted") != "keep")
        drop_rate = (n_drop / n_all) if n_all else 0.0
    fd_must = n_where("drop", "must_keep")
    fd_should = n_where("drop", "should_keep")
    fk_must = n_where("keep", "must_drop")
    fk_should = n_where("keep", "should_drop")
    passed = (
        fd_must == 0
        and fd_should <= SHOULD_KEEP_FALSE_DROP_MAX
        and fk_must == 0
        and fk_should <= SHOULD_DROP_FALSE_KEEP_MAX
        and drop_rate >= DROP_RATE_MIN
    )
    return {
        "false_drop_must_keep": fd_must,
        "false_drop_must_keep_max": 0,
        "false_drop_should_keep": fd_should,
        "false_drop_should_keep_max": SHOULD_KEEP_FALSE_DROP_MAX,
        "false_keep_must_drop": fk_must,
        "false_keep_must_drop_max": 0,
        "false_keep_should_drop": fk_should,
        "false_keep_should_drop_max": SHOULD_DROP_FALSE_KEEP_MAX,
        "drop_rate": round(float(drop_rate), 4),
        "drop_rate_min": DROP_RATE_MIN,
        "unlabeled": len(unlabeled),
        "pass": None if unlabeled else passed,
    }


def _copy_row(row: dict) -> dict:
    return {
        "id": row.get("id") or title_id(row.get("title") or ""),
        "title": row.get("title") or "",
        "source": row.get("source") or "",
        "published_at": row.get("published_at") or "",
        "url": row.get("url") or "",
        "date": row.get("date") or "",
    }


def score_sample(rows: list[dict], *, live: bool, key: str,
                 workers: int, poster, asof: dt.date,
                 knobs: GateKnobs | None = None) -> tuple[dict[str, dict], str]:
    """Gate the sample. Retry leftovers that came back jev_error. Do not label."""
    by_norm: dict[str, dict] = {}
    model = ""
    pending = list(rows)
    gate_kwargs = {
        "code_only": not live,
        "live": live,
        "key": key,
        "workers": workers,
        "poster": poster,
        "asof": asof,
    }
    if knobs is not None and "knobs" in inspect.signature(gate).parameters:
        gate_kwargs["knobs"] = knobs
    for attempt in range(3):
        if not pending:
            break
        copies = [_copy_row(r) for r in pending]
        decided = gate(copies, **gate_kwargs)
        for row in copies:
            got = str(row.get("_jev_model") or "")
            if got and not got.startswith("error:"):
                model = got
        mapped = {normalize_title(d.get("title") or ""): d for d in decided}
        nxt = []
        for row in pending:
            dec = mapped.get(normalize_title(row.get("title") or ""))
            if dec is None or (dec.get("reason") == "jev_error" and attempt < 2):
                nxt.append(row)
                continue
            by_norm[normalize_title(row.get("title") or "")] = dec
        pending = nxt
        if pending and attempt < 2:
            time.sleep(1.0 * (attempt + 1))
    if pending:
        titles = [r.get("title", "")[:80] for r in pending[:5]]
        raise RuntimeError(f"Jev errors remain on {len(pending)} titles: {titles}")
    return by_norm, model


def _pool_counts(items: list[dict]) -> dict:
    out = {}
    for pool in ("parsed", "rss", "holdout"):
        chunk = [it for it in items if it.get("pool") == pool]
        keep = sum(1 for it in chunk if it.get("predicted") == "keep")
        drop = len(chunk) - keep
        out[pool] = {"n": len(chunk), "keep": keep, "drop": drop}
    keep = sum(1 for it in items if it.get("predicted") == "keep")
    drop = len(items) - keep
    out["overall"] = {
        "n": len(items),
        "keep": keep,
        "drop": drop,
        "drop_rate": round(drop / len(items), 4) if items else 0.0,
    }
    return out


def _join_items(sample: dict, decided: dict[str, dict]) -> list[dict]:
    items = []
    for pool in ("parsed", "rss", "holdout"):
        for row in sample[pool]:
            dec = decided.get(normalize_title(row.get("title") or ""))
            if not dec:
                raise RuntimeError(f"no gate decision for {row.get('title', '')[:80]}")
            items.append({
                "id": row.get("id"),
                "pool": pool,
                "archive": row.get("archive") or "",
                "date": row.get("date") or "",
                "query": row.get("query") or "",
                "title": row.get("title") or "",
                "source": row.get("source") or "",
                "published_at": row.get("published_at") or "",
                "url": row.get("url") or "",
                "predicted": dec.get("decision") or "",
                "reason": dec.get("reason") or "",
                "keep_via": keep_via(dec.get("decision") or "", dec.get("reason") or ""),
                "label": "",
                "geo": dec.get("geo") or "",
                "actor_power": dec.get("actor_power") or "",
                "action_material": dec.get("action_material") or 0,
                "new_instrument": dec.get("new_instrument") or 0,
                "reprint_weather": dec.get("reprint_weather") or 0,
            })
    return items


def _cell(text: str) -> str:
    return (text or "").replace("|", "/").replace("\n", " ").strip()


def render_sheet(report: dict) -> str:
    lines = [
        f"# Jev hop-0 eval sheet — {report.get('stamp')} — UNLABELED",
        "",
        "Do not auto-label. Leave `label` blank until a person fills it with one of:",
        "",
    ]
    for name, text in RUBRIC:
        lines.append(f"- `{name}`: {text}")
    lines += [
        "",
        f"Round {report.get('round')} · run `{report.get('run_id')}` · "
        f"knob_changed={report.get('knob_changed')}",
        "",
        "Predicted keep/drop is the current gate. It is not a label.",
        "",
        f"Earnings stripped before Jev: {(report.get('sample') or {}).get('earnings_stripped', 0)}",
        "",
    ]
    sections = (
        ("parsed", "Archive sample (parsed, stratified by date)"),
        ("rss", "Google News RSS after Jaccard"),
        ("holdout", "Holdout (frozen; never used while tuning)"),
    )
    n = 0
    for pool, heading in sections:
        lines.append(f"## {heading}")
        lines.append("")
        lines.append("| n | pool | predicted | reason | kept_by | label | title |")
        lines.append("|---|------|-----------|--------|---------|-------|-------|")
        for it in report.get("items") or []:
            if it.get("pool") != pool:
                continue
            n += 1
            lines.append(
                f"| {n} | {pool} | {it.get('predicted')} | {_cell(it.get('reason') or '')} "
                f"| {_cell(it.get('keep_via') or '')} |  | {_cell(it.get('title') or '')} |"
            )
        lines.append("")
    keeps = [it for it in (report.get("items") or []) if it.get("predicted") == "keep"]
    lines.append("## Predicted keeps")
    lines.append("")
    lines.append("| pool | kept_by | reason | title |")
    lines.append("|------|---------|--------|-------|")
    for it in keeps:
        lines.append(
            f"| {it.get('pool')} | {_cell(it.get('keep_via') or '')} "
            f"| {_cell(it.get('reason') or '')} | {_cell(it.get('title') or '')} |"
        )
    lines.append("")
    return "\n".join(lines) + "\n"


def _stamp(now: dt.datetime, rounds_dir: Path) -> str:
    base = now.strftime("%Y%m%d_%H%M")
    if (rounds_dir / f"{base}.json").exists():
        base = now.strftime("%Y%m%d_%H%M%S")
    return base


def _round_index(rounds_dir: Path) -> int:
    n = 0
    if rounds_dir.is_dir():
        for path in rounds_dir.glob("*.json"):
            if path.name.endswith("_sheet.json"):
                continue
            n += 1
    return n + 1


def _run_meta() -> dict:
    run_id = os.environ.get("GITHUB_RUN_ID") or "local"
    repo = os.environ.get("GITHUB_REPOSITORY") or ""
    server = os.environ.get("GITHUB_SERVER_URL") or "https://github.com"
    url = f"{server}/{repo}/actions/runs/{run_id}" if repo and run_id != "local" else ""
    return {
        "run_id": run_id,
        "run_url": url,
        "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT") or "",
        "sha": os.environ.get("GITHUB_SHA") or "",
        "ref": os.environ.get("GITHUB_REF_NAME") or os.environ.get("GITHUB_REF") or "",
    }


def run_eval(*, live: bool = True, workers: int = 16, seed: int | None = None,
             knob_specs: list[str] | None = None, news_dir: Path | None = None,
             ground_dir: Path | None = None, now: dt.datetime | None = None,
             rng: random.Random | None = None, rss_rows: list[dict] | None = None,
             poster=None, key: str = "", write: bool = True,
             parsed_n: int = PARSED_N, rss_n: int = RSS_N, holdout_n: int = HOLDOUT_N,
             opener=None) -> dict:
    knobs, knob_name, _raw = resolve_knobs(knob_specs)
    knobs = prepare_knobs(knobs)
    now = now or dt.datetime.now(dt.timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=dt.timezone.utc)
    if rng is None:
        if seed is None:
            seed = random.SystemRandom().randrange(1, 2**31 - 1)
        rng = random.Random(seed)
    news_dir = news_dir or NEWS_DIR
    ground = ground_dir or GROUND
    holdout_path = ground / "jev_holdout.json"
    rounds_dir = ground / "jev_rounds"
    state_before = load_chokepoint_state()
    sample = draw_sample(
        news_dir=news_dir, holdout_path=holdout_path, rng=rng, now=now,
        rss_rows=rss_rows, parsed_n=parsed_n, rss_n=rss_n, holdout_n=holdout_n,
        opener=opener,
    )
    ordered = []
    for pool in ("parsed", "rss", "holdout"):
        for row in sample[pool]:
            ordered.append(row)
    asof = now.date()
    if live and not (key or os.environ.get("JEV_API_KEY") or os.environ.get("TYPESAFE_API_KEY") or poster):
        raise RuntimeError("JEV_API_KEY / TYPESAFE_API_KEY is empty")
    decided, model = score_sample(
        ordered, knobs=knobs, live=live, key=key, workers=workers,
        poster=poster, asof=asof,
    )
    items = _join_items(sample, decided)
    counts = _pool_counts(items)
    meta = _run_meta()
    stamp = _stamp(now, rounds_dir if write else Path("/tmp"))
    # Stamp against the real dir when writing; tests pass write=True with a temp ground.
    if write:
        rounds_dir.mkdir(parents=True, exist_ok=True)
        stamp = _stamp(now, rounds_dir)
    report = {
        "schema": "hop0-eval-1",
        "round": _round_index(rounds_dir) if write else 1,
        "stamp": stamp,
        "timestamp": now.isoformat(),
        "labeled": False,
        "run_id": meta["run_id"],
        "run_url": meta["run_url"],
        "run_attempt": meta["run_attempt"],
        "sha": meta["sha"],
        "ref": meta["ref"],
        "model": model,
        "seed": seed,
        "params": effective_params(knobs, state_for_knobs(state_before, knobs)),
        "knob_changed": None,
        "sample": {
            "parsed_target": parsed_n,
            "rss_target": rss_n,
            "holdout_target": holdout_n,
            "archive_n": sample["archive_n"],
            "parsed": len(sample["parsed"]),
            "rss": len(sample["rss"]),
            "holdout": len(sample["holdout"]),
            "gold_discarded_redrawn": sample["gold_discarded_redrawn"],
            "rss_fetched": sample["rss_fetched"],
            "rss_unique": sample["rss_unique"],
            "holdout_created": sample["holdout_created"],
            "holdout_algo": HASH_ALGO,
            "holdout_ids": list(sample["holdout_blob"].get("ids") or []),
            "tuning_overlap_holdout": sample["tuning_overlap_holdout"],
            "earnings_stripped": sum(1 for it in items if it.get("reason") == "earnings"),
            "parsed_dates": dict(Counter(r.get("date") or "" for r in sample["parsed"])),
            "rss_queries": dict(Counter(r.get("query") or "" for r in sample["rss"])),
            "sources": {
                "parsed": "01_daily/news/*_parsed.json (digest fallback)",
                "rss": "Google News RSS watchlist when:1d, after Jaccard",
                "holdout": "00_grounding/jev_holdout.json",
            },
        },
        "predicted": counts,
        "rules": None,
        "items": items,
    }
    explicit = knob_changed_record(knob_name, knobs, state_before)
    previous = _latest_round_params(rounds_dir) if rounds_dir.is_dir() else None
    inferred = diff_knobs(report["params"], previous)
    if inferred and explicit and inferred.get("name") != explicit.get("name"):
        raise RuntimeError(
            f"knob flag {explicit.get('name')} disagrees with param diff {inferred.get('name')}"
        )
    report["knob_changed"] = inferred if previous is not None else explicit
    # Recompute round index after stamp dir exists. _round_index counts json
    # files; this report is not written yet, so index is correct.
    if write:
        if holdout_path.exists():
            # Append-only: an existing holdout file is never rewritten.
            pass
        elif sample["holdout_created"]:
            _write_json(holdout_path, sample["holdout_blob"])
        round_path = rounds_dir / f"{stamp}.json"
        sheet_path = rounds_dir / f"{stamp}_sheet.md"
        if round_path.exists() or sheet_path.exists():
            raise FileExistsError(f"refusing to overwrite {round_path.name}")
        report["paths"] = {
            "round": str(round_path.relative_to(ROOT)) if _under(round_path, ROOT) else str(round_path),
            "sheet": str(sheet_path.relative_to(ROOT)) if _under(sheet_path, ROOT) else str(sheet_path),
            "holdout": str(holdout_path.relative_to(ROOT)) if _under(holdout_path, ROOT) else str(holdout_path),
        }
        _write_json(round_path, report)
        sheet_path.write_text(render_sheet(report), encoding="utf-8")
        report["_round_path"] = str(round_path)
        report["_sheet_path"] = str(sheet_path)
    _print_summary(report)
    return report


def _under(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def _print_summary(report: dict) -> None:
    pred = report.get("predicted") or {}
    overall = pred.get("overall") or {}
    sample = report.get("sample") or {}
    print(
        f"[jev_eval] round={report.get('round')} stamp={report.get('stamp')} "
        f"run_id={report.get('run_id')}"
    )
    print(
        f"[jev_eval] parsed={sample.get('parsed')} rss={sample.get('rss')} "
        f"holdout={sample.get('holdout')} "
        f"gold_discarded_redrawn={sample.get('gold_discarded_redrawn')}"
    )
    print(
        f"[jev_eval] keep={overall.get('keep')} drop={overall.get('drop')} "
        f"drop_rate={overall.get('drop_rate')} "
        f"earnings_stripped={sample.get('earnings_stripped')}"
    )
    for pool in ("parsed", "rss", "holdout"):
        chunk = pred.get(pool) or {}
        print(f"[jev_eval] {pool} keep={chunk.get('keep')} drop={chunk.get('drop')} n={chunk.get('n')}")
    for it in report.get("items") or []:
        if it.get("predicted") != "keep":
            continue
        print(
            f"[jev_eval] KEEP pool={it.get('pool')} via={it.get('keep_via')} "
            f"reason={it.get('reason')} title={it.get('title')}"
        )
    if report.get("_sheet_path"):
        print(f"[jev_eval] sheet={report.get('_sheet_path')}")
        print(f"[jev_eval] round_json={report.get('_round_path')}")
    summary = {
        "round": report.get("round"),
        "stamp": report.get("stamp"),
        "run_id": report.get("run_id"),
        "run_url": report.get("run_url"),
        "sheet": report.get("paths", {}).get("sheet"),
        "round_json": report.get("paths", {}).get("round"),
        "sample": {
            "parsed": sample.get("parsed"),
            "rss": sample.get("rss"),
            "holdout": sample.get("holdout"),
            "gold_discarded_redrawn": sample.get("gold_discarded_redrawn"),
            "earnings_stripped": sample.get("earnings_stripped"),
        },
        "predicted": pred,
    }
    print("JEV_EVAL_SUMMARY " + json.dumps(summary, ensure_ascii=False))


def run_eval_cli(args) -> int:
    try:
        resolve_knobs(args.knob)
    except ValueError as exc:
        print(f"[jev_eval] {exc}")
        return 2
    if getattr(args, "code_only", False):
        print("[jev_eval] --code-only is not the current gate. Refusing.")
        return 2
    key = (
        os.environ.get("JEV_API_KEY")
        or os.environ.get("TYPESAFE_API_KEY")
        or ""
    ).strip()
    if not key:
        print("[jev_eval] JEV_API_KEY / TYPESAFE_API_KEY is empty")
        return 1
    try:
        run_eval(
            live=True,
            workers=max(1, int(args.workers or 16)),
            seed=args.seed,
            knob_specs=args.knob,
            key=key,
        )
    except Exception as exc:  # noqa: BLE001 — CLI boundary
        print(f"[jev_eval] {type(exc).__name__}: {exc}")
        return 1
    return 0
