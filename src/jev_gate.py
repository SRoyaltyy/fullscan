"""Hop-0 news net: code first, one Jev pack on leftovers.

Sorting, not classifying. Jev never picks a 52-class, polarity, or
ticker expansion. Spend tokens only after dups / junk-shapes / source
deny / chokepoint reprint-clock have already thrown most titles away.

  harvest → normalize → Jaccard dedup → regex trash → reprint clock
        → one Jev pack (trash / geo / actor / material / instrument)
        → keep.json  (only keeps go to hop-1 / hop-2)

Key lives in env JEV_API_KEY (or TYPESAFE_API_KEY). Never in git.

CLI:
  python -m src.jev_gate --gold
  python -m src.jev_gate --date latest --limit 200
  python -m src.jev_gate --code-only --date 2026-09-28
  python -m src.jev_gate --mine
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import re
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from email.utils import parsedate_to_datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
NEWS_DIR = ROOT / "01_daily" / "news"
EVENTS_DIR = ROOT / "01_daily" / "events"
EXPORTS_DIR = ROOT / "data" / "exports"
GROK_DIR = ROOT / "data" / "grok_automations"
GROUND = ROOT / "00_grounding"
SCOREBOARD = ROOT / "03_scoreboard" / "JEV_GATE.md"

JEV_HOSTS = (
    "https://api.typesafe.ai/v1/systemone",
    "https://thejevai.com/v1/systemone",
)
JEV_MODEL = "jev-latest"

STOP = frozenset(
    {
        "the", "and", "for", "with", "from", "after", "over", "this", "that",
        "into", "onto", "than", "then", "have", "has", "had", "was", "were",
        "are", "not", "its", "his", "her", "their", "you", "your", "how",
        "why", "what", "when", "who", "but", "via",
    }
)
SOURCE_SUFFIX = re.compile(r"\s+[-–—|:]\s+[A-Za-z0-9 .,&+/]{2,48}$")
DATE_RE = re.compile(r"(20\d{2}-\d{2}-\d{2})")
SOURCE_DENY = re.compile(
    r"(?i)(seeking.?alpha|benzinga|motley|fool\.com|the.?fool|"
    r"tipranks|zacks|thestreet|simplywall|investorplace|"
    r"marketbeat|insidermonkey)"
)
PUNCT_TRASH = re.compile(r"[?!]")

JACCARD_DROP = 0.72
TRASH_NOUL = 0.70
MATERIAL_KEEP = 0.65
INSTRUMENT_KEEP = 0.60
CROWD_DROP = 0.50
POWERFUL = frozenset(
    {"state_head", "regulator", "listed_firm", "infrastructure"}
)
CHOKE_HIT = re.compile(
    r"(?i)\b(oil|tanker|strait|canal|pipeline|port|shipping|lng)\b"
)

# Jev question ids. Do not add event_class / polarity / ticker.
QUESTIONS: dict = {
    "is_opinion": {
        "type": "noul",
        "instructions": (
            "Is this commentary, a column, or a market-reaction recap "
            "rather than a first report of a fact?"
        ),
        "criteria": {
            "true": "Column, recap, or 'what it means' commentary",
            "false": "First report of a fact, print, filing, or decision",
        },
    },
    "is_tabloid": {
        "type": "noul",
        "instructions": (
            "Is the source or framing sensational / celebrity / "
            "crime-blotter with no policy or company action?"
        ),
    },
    "is_reaction": {
        "type": "noul",
        "instructions": (
            "Does the title only describe how stocks or traders already "
            "reacted, with no new underlying event?"
        ),
    },
    "geo": {
        "type": "choice",
        "instructions": "Where is the event, for US-listed market relevance?",
        "criteria": {
            "core": (
                "US, China, EU/EZ, Japan, Korea, India, or a G10 central "
                "bank / regulator"
            ),
            "chokepoint": (
                "Hormuz, Red Sea / Bab el-Mandeb, Suez, Panama, Taiwan "
                "Strait, Malacca, or a named tanker/port there"
            ),
            "other": (
                "Anywhere else, including Yemen/Palestine/UK-local "
                "politics with no US/G10 hook"
            ),
        },
    },
    "actor_power": {
        "type": "choice",
        "instructions": "Who is the main actor in the title?",
        "criteria": {
            "state_head": "President, PM, monarch, cabinet minister, central banker",
            "regulator": (
                "SEC, FDA, Fed, NHTSA, NBS, ECB, PBOC, court with binding order"
            ),
            "listed_firm": "Named company that has or plausibly has a US ticker",
            "infrastructure": "Port, strait, exchange, grid, pipeline operator",
            "crowd": "Protesters, activists, tourists, unnamed residents",
            "other_person": "Private individual with no state or corporate seat",
        },
    },
    "action_material": {
        "type": "noul",
        "instructions": (
            "If this headline is true, could it change cash flows or "
            "rules for a US-listed name this week? Arrest of protesters = no. "
            "Arrest of a sitting US president = yes. A final CAFE rule = yes. "
            "A local rally = no."
        ),
    },
    "new_instrument": {
        "type": "noul",
        "instructions": (
            "Is there a signed rule, print, halt, filing, seizure, or "
            "dated official decision in the title — not a speech, protest, "
            "or rumor?"
        ),
    },
    "reprint_weather": {
        "type": "noul",
        "instructions": (
            "Is this a rerun of a months-old situation with no new closure, "
            "ceasefire, or first strike?"
        ),
    },
}

FORBIDDEN_QUESTION_BITS = (
    "event_class", "bullish", "bearish", "polarity", "q5",
    "52 class", "ticker expansion",
)


def _load_json(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError):
        return None


def _write_json(path: Path, blob) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(blob, indent=2, ensure_ascii=False) + "\n",
                    encoding="utf-8")


def load_junk_shapes(path: Path | None = None) -> list[str]:
    blob = _load_json(path or GROUND / "jev_junk_shapes.json") or {}
    shapes = [str(s).lower() for s in (blob.get("shapes") or []) if s]
    return shapes


def junk_shape_re(shapes: list[str] | None = None) -> re.Pattern:
    shapes = shapes if shapes is not None else load_junk_shapes()
    if not shapes:
        shapes = ["what it means", "stock of the day", "why this matters"]
    parts = [re.escape(s) for s in shapes]
    return re.compile(r"(?i)(?:" + "|".join(parts) + r")")


JUNK_RE = junk_shape_re()


def load_chokepoint_state(path: Path | None = None) -> dict:
    blob = _load_json(path or GROUND / "jev_chokepoint_state.json")
    if not isinstance(blob, dict):
        return {"days_threshold": 14, "places": [], "new_verbs_default": []}
    return blob


def load_gold(path: Path | None = None) -> dict:
    blob = _load_json(path or GROUND / "jev_gold.json")
    if not isinstance(blob, dict):
        return {"items": [], "must_keep": [], "must_drop": []}
    return blob


def load_closed_lists(path: Path | None = None) -> dict:
    blob = _load_json(path or GROUND / "jev_closed_lists.json")
    if not isinstance(blob, dict):
        return {
            "venues": [], "agencies": [], "state_heads": [],
            "newness_verbs": [], "head_actions": [],
        }
    return blob


def _has_phrase(norm: str, phrase: str) -> bool:
    p = str(phrase or "").lower().strip()
    if not p:
        return False
    if " " in p:
        return p in norm
    return f" {p} " in f" {norm} "


def code_hints(title: str, lists: dict | None = None) -> dict:
    """Closed lists only. Jev is not allowed to invent English."""
    lists = lists or load_closed_lists()
    norm = normalize_title(title)

    def hits(key: str) -> list[str]:
        return [str(p) for p in (lists.get(key) or []) if _has_phrase(norm, str(p))]

    venue = hits("venues")
    agency = hits("agencies")
    head = hits("state_heads")
    newness = hits("newness_verbs")
    head_act = hits("head_actions")
    return {
        "venue": bool(venue),
        "agency": bool(agency),
        "state_head": bool(head),
        "newness": bool(newness),
        "head_action": bool(head_act),
        "hits": {
            "venue": venue,
            "agency": agency,
            "state_head": head,
            "newness": newness,
            "head_action": head_act,
        },
    }


def api_key() -> str:
    return (
        os.environ.get("JEV_API_KEY")
        or os.environ.get("TYPESAFE_API_KEY")
        or ""
    ).strip()


def normalize_title(title: str) -> str:
    t = (title or "").strip()
    t = SOURCE_SUFFIX.sub("", t)
    t = t.lower().replace("'", "").replace("'", "").replace("'", "")
    t = re.sub(r"[^a-z0-9\s]", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def tokens(title: str) -> frozenset[str]:
    return frozenset(
        w for w in normalize_title(title).split()
        if len(w) >= 3 and w not in STOP
    )


def jaccard(a: frozenset[str], b: frozenset[str]) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def published_sort_key(raw: str, index: int) -> tuple:
    raw = (raw or "").strip()
    try:
        return (parsedate_to_datetime(raw).timestamp(), index)
    except (TypeError, ValueError, IndexError):
        pass
    try:
        return (dt.datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp(), index)
    except ValueError:
        pass
    day = calendar_day(raw)
    if day:
        try:
            return (dt.datetime.fromisoformat(day).timestamp(), index)
        except ValueError:
            pass
    return (float("inf"), index)


def calendar_day(raw: str, fallback: str = "") -> str:
    raw = (raw or "").strip()
    if not raw:
        return fallback
    m = DATE_RE.search(raw)
    if m:
        return m.group(1)
    try:
        return parsedate_to_datetime(raw).date().isoformat()
    except (TypeError, ValueError, IndexError):
        return fallback


def parse_iso_date(raw: str) -> dt.date | None:
    day = calendar_day(raw)
    if not day:
        return None
    try:
        return dt.date.fromisoformat(day)
    except ValueError:
        return None


def source_denied(source: str, url: str = "") -> bool:
    return bool(SOURCE_DENY.search(source or "") or SOURCE_DENY.search(url or ""))


def punct_trash(title: str) -> bool:
    return bool(PUNCT_TRASH.search(title or ""))


def junk_shape_hit(title: str, rx: re.Pattern | None = None) -> str:
    m = (rx or JUNK_RE).search(title or "")
    return (m.group(0) or "").lower() if m else ""


def place_verbs(place: dict, state: dict) -> list[str]:
    verbs = list(place.get("new_verbs") or [])
    verbs.extend(state.get("new_verbs_default") or [])
    # de-dupe, keep order
    seen: set[str] = set()
    out: list[str] = []
    for v in verbs:
        v = str(v).lower().strip()
        if v and v not in seen:
            seen.add(v)
            out.append(v)
    return out


def match_place(title: str, state: dict) -> dict | None:
    t = (title or "").lower()
    best = None
    best_len = -1
    for place in state.get("places") or []:
        for key in place.get("keys") or []:
            k = str(key).lower()
            if k and k in t and len(k) > best_len:
                best = place
                best_len = len(k)
    return best


def has_new_verb(title: str, place: dict, state: dict) -> bool:
    t = (title or "").lower()
    return any(v in t for v in place_verbs(place, state))


def days_since_new_verb(place: dict, asof: dt.date) -> int | None:
    last = parse_iso_date(str(place.get("last_new_verb_date") or ""))
    if last is None:
        return None
    return (asof - last).days


def reprint_weather_code(title: str, asof: dt.date, state: dict) -> dict:
    """Tiny dated state: place is stale iff no new verb past the clock."""
    place = match_place(title, state)
    if not place:
        return {"hit": False, "stale": False, "place": "", "has_new_verb": False}
    new = has_new_verb(title, place, state)
    days = days_since_new_verb(place, asof)
    thresh = int(state.get("days_threshold") or 14)
    stale = (days is not None and days > thresh and not new)
    return {
        "hit": True,
        "stale": stale,
        "place": place.get("id") or "",
        "has_new_verb": new,
        "days_since_new_verb": days,
        "choke_keyword": bool(CHOKE_HIT.search(title or "")),
    }


def code_drop_reason(row: dict, *, asof: dt.date, state: dict,
                     junk_rx: re.Pattern | None = None) -> str:
    if source_denied(row.get("source") or "", row.get("url") or ""):
        return "source"
    if punct_trash(row.get("title") or ""):
        return "punct"
    if junk_shape_hit(row.get("title") or "", junk_rx):
        return "junk_shape"
    clock = reprint_weather_code(row.get("title") or "", asof, state)
    row["_clock"] = clock
    if clock["hit"] and clock["stale"]:
        return "reprint_weather"
    return ""


def make_state(row: dict) -> str:
    return (
        f"TITLE: {row.get('title') or ''}\n"
        f"SOURCE: {row.get('source') or ''}\n"
        f"DATE: {row.get('published_at') or row.get('date') or ''}"
    )


def parse_answers(payload: dict) -> dict:
    out: dict = {}
    for key, ans in (payload.get("answers") or {}).items():
        if not isinstance(ans, dict):
            continue
        kind = ans.get("type")
        if kind == "noul":
            try:
                out[key] = float(ans.get("noul") or 0.0)
            except (TypeError, ValueError):
                out[key] = 0.0
        elif kind == "choice":
            out[key] = str(ans.get("choice") or "")
    return out


def answers_from_gold(item: dict) -> dict:
    raw = item.get("answers") or {}
    return {k: raw[k] for k in QUESTIONS if k in raw}


def decide(row: dict, answers: dict | None) -> dict:
    """Pure keep/drop given a code-flagged row + optional Jev answers.

    Jev is not allowed to invent an event class or polarity here.
    """
    title = row.get("title") or ""
    code = row.get("code_reason") or ""
    clock = row.get("_clock") or {}
    geo = ""
    actor = ""
    material = 0.0
    instrument = 0.0
    reprint = 0.0

    def pack(decision: str, reason: str, geo_out: str = "") -> dict:
        return {
            "title": title,
            "source": row.get("source") or "",
            "published_at": row.get("published_at") or "",
            "url": row.get("url") or "",
            "id": row.get("id") or "",
            "decision": decision,
            "reason": reason,
            "geo": geo_out or geo or "",
            "actor_power": actor,
            "action_material": material,
            "new_instrument": instrument,
            "reprint_weather": reprint,
            "place": clock.get("place") or "",
            "has_new_verb": bool(clock.get("has_new_verb")),
        }

    if code:
        geo_out = "chokepoint" if code == "reprint_weather" else ""
        return pack("drop", code, geo_out)

    if not answers:
        return pack("keep", "code_leftover")

    opinion = float(answers.get("is_opinion") or 0.0)
    tabloid = float(answers.get("is_tabloid") or 0.0)
    reaction = float(answers.get("is_reaction") or 0.0)
    reprint = float(answers.get("reprint_weather") or 0.0)
    material = float(answers.get("action_material") or 0.0)
    instrument = float(answers.get("new_instrument") or 0.0)
    geo = str(answers.get("geo") or "other")
    actor = str(answers.get("actor_power") or "other_person")
    hints = code_hints(title)

    if hints["state_head"]:
        actor = "state_head"
    elif hints["agency"]:
        actor = "regulator"
    elif hints["venue"] and actor not in POWERFUL:
        actor = "listed_firm"

    if hints["agency"] or hints["venue"]:
        if geo == "other":
            geo = "core"
        if hints["newness"]:
            instrument = max(instrument, INSTRUMENT_KEEP)

    # Jev over-fires Yemen/Palestine as Hormuz cousins. Place file or
    # tanker/strait keyword required to keep a chokepoint label.
    choke_kw = bool(CHOKE_HIT.search(title) or clock.get("hit"))
    if geo == "chokepoint" and not choke_kw:
        geo = "other"

    if opinion >= TRASH_NOUL:
        return pack("drop", "opinion")
    if reaction >= TRASH_NOUL:
        return pack("drop", "reaction")
    if tabloid >= TRASH_NOUL and actor not in POWERFUL:
        return pack("drop", "tabloid")
    if actor == "crowd" and material < CROWD_DROP:
        return pack("drop", "crowd")
    if actor == "state_head" and hints["head_action"]:
        return pack("keep", "state_head_action")

    if geo == "other":
        if actor in POWERFUL and (
            material >= MATERIAL_KEEP or instrument >= INSTRUMENT_KEEP
        ):
            return pack("keep", "other_powerful")
        return pack("drop", "geo_other")

    if geo == "chokepoint":
        if reprint >= TRASH_NOUL and not clock.get("has_new_verb"):
            return pack("drop", "reprint_weather")
        if clock.get("hit") and not clock.get("choke_keyword") and not clock.get("has_new_verb"):
            return pack("drop", "geo_chokepoint_no_hit")
        return pack("keep", "chokepoint")

    # core — high recall, but still require material or a real instrument
    if material >= MATERIAL_KEEP or (
        actor in POWERFUL and instrument >= INSTRUMENT_KEEP
    ):
        return pack("keep", "core_material")
    return pack("drop", "low_material")


def dedup_rows(rows: list[dict], session_day: str = "") -> list[dict]:
    """Keep the earliest dated wire; drop later titles with Jaccard ≥ 0.72."""
    decorated: list[tuple[tuple, int, dict, frozenset[str], str]] = []
    for i, row in enumerate(rows):
        day = calendar_day(row.get("published_at") or "", session_day)
        toks = tokens(row.get("title") or "")
        decorated.append((published_sort_key(row.get("published_at") or "", i), i, row, toks, day))
    decorated.sort(key=lambda x: x[0])
    kept: list[dict] = []
    seen: list[tuple[str, frozenset[str], dict]] = []
    for _key, _i, row, toks, day in decorated:
        dropped = False
        for sday, stoks, srow in seen:
            if sday != day:
                continue
            if jaccard(toks, stoks) >= JACCARD_DROP:
                row["code_reason"] = "dup"
                row["dup_of"] = srow.get("title") or ""
                dropped = True
                break
        kept.append(row)
        if not dropped:
            seen.append((day, toks, row))
    return kept


def apply_code(rows: list[dict], *, asof: dt.date | None = None,
               state: dict | None = None) -> list[dict]:
    asof = asof or dt.date.today()
    state = state or load_chokepoint_state()
    junk_rx = junk_shape_re()
    out = []
    for row in rows:
        if row.get("code_reason"):
            out.append(row)
            continue
        reason = code_drop_reason(row, asof=asof, state=state, junk_rx=junk_rx)
        if reason:
            row["code_reason"] = reason
        elif "_clock" not in row:
            row["_clock"] = reprint_weather_code(
                row.get("title") or "", asof, state
            )
        out.append(row)
    return out


def jev_post(state: str, questions: dict, key: str,
             timeout: float = 30.0) -> dict:
    body = json.dumps(
        {"model": JEV_MODEL, "state": state, "questions": questions},
        ensure_ascii=False,
    ).encode("utf-8")
    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "fullscan-jev-gate/1",
    }
    last: Exception | None = None
    for host in JEV_HOSTS:
        for attempt in range(5):
            req = urllib.request.Request(
                host, data=body, method="POST", headers=headers,
            )
            try:
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    return json.loads(resp.read().decode("utf-8"))
            except urllib.error.HTTPError as exc:
                last = exc
                if exc.code in (401, 403):
                    raise RuntimeError("Jev auth failed (check JEV_API_KEY)") from None
                if exc.code == 429:
                    wait = exc.headers.get("Retry-After") or exc.headers.get(
                        "retry-after-ms"
                    )
                    try:
                        sleep_s = float(wait)
                        if sleep_s > 100:
                            sleep_s = sleep_s / 1000.0
                    except (TypeError, ValueError):
                        sleep_s = min(2 ** attempt, 20)
                    time.sleep(sleep_s)
                    continue
                break
            except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
                last = exc
                break
    raise RuntimeError(f"Jev HTTP failed: {last!r}") from last


def jev_many(rows: list[dict], key: str, workers: int = 24,
             poster=None) -> list[tuple[dict, dict | None, str]]:
    poster = poster or jev_post
    out: list[tuple[dict, dict | None, str]] = []
    if not rows:
        return out

    def one(row: dict):
        payload = poster(make_state(row), QUESTIONS, key)
        return row, parse_answers(payload), payload.get("model") or JEV_MODEL

    workers = max(1, min(int(workers), 50))
    if workers == 1 or len(rows) == 1:
        for row in rows:
            try:
                out.append(one(row))
            except Exception as exc:
                out.append((row, None, f"error:{exc}"))
        return out

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(one, row): row for row in rows}
        for fut in as_completed(futs):
            row = futs[fut]
            try:
                out.append(fut.result())
            except Exception as exc:
                out.append((row, None, f"error:{exc}"))
    return out


def gate(rows: list[dict], *, code_only: bool = False, live: bool = False,
         key: str = "", workers: int = 24, asof: dt.date | None = None,
         state: dict | None = None, poster=None,
         gold_answers: dict | None = None) -> list[dict]:
    """Run hop-0. gold_answers maps row id → answer dict (tests / dry gold)."""
    asof = asof or dt.date.today()
    state = state or load_chokepoint_state()
    rows = dedup_rows(list(rows), session_day=asof.isoformat())
    rows = apply_code(rows, asof=asof, state=state)

    leftovers = [r for r in rows if not r.get("code_reason")]
    answers_by_id: dict[str, dict] = gold_answers or {}

    if live and not code_only and leftovers:
        key = key or api_key()
        if not key:
            raise RuntimeError("JEV_API_KEY / TYPESAFE_API_KEY is empty")
        for row, answers, model in jev_many(leftovers, key, workers, poster):
            row["_jev_model"] = model
            if answers is None:
                row["code_reason"] = "jev_error"
                row["_answers"] = {}
            else:
                row["_answers"] = answers
    else:
        for row in leftovers:
            rid = str(row.get("id") or "")
            if code_only:
                row["_answers"] = None
            elif rid and rid in answers_by_id:
                row["_answers"] = answers_by_id[rid]
            else:
                # dry gold path uses per-row answers already attached
                row["_answers"] = row.get("answers") or answers_by_id.get(rid)

    decided = []
    for row in rows:
        decided.append(decide(row, row.get("_answers")))
    return decided


def list_session_dates() -> list[str]:
    dates: set[str] = set()
    for folder, glob in (
        (NEWS_DIR, "*_parsed.json"),
        (NEWS_DIR, "*_finviz_digest.json"),
        (EVENTS_DIR, "*_events.json"),
        (EXPORTS_DIR, "finviz_*.csv"),
    ):
        if not folder.is_dir():
            continue
        for path in folder.glob(glob):
            m = DATE_RE.search(path.name)
            if m:
                dates.add(m.group(1))
    return sorted(dates)


def latest_session_date() -> str:
    dates = list_session_dates()
    return dates[-1] if dates else dt.date.today().isoformat()


def _add_title(bag: dict[str, dict], title: str, source: str = "",
               published_at: str = "", url: str = "", extra: dict | None = None) -> None:
    title = (title or "").strip()
    if not title:
        return
    key = normalize_title(title)[:180]
    if not key or key in bag:
        return
    row = {
        "title": title[:400],
        "source": (source or "")[:160],
        "published_at": published_at or "",
        "url": (url or "")[:400],
    }
    if extra:
        row.update(extra)
    bag[key] = row


def load_titles(date: str) -> list[dict]:
    """Read on-disk harvest. all_items includes noise — hop-0 wants the firehose."""
    bag: dict[str, dict] = {}
    parsed = _load_json(NEWS_DIR / f"{date}_parsed.json")
    if isinstance(parsed, dict):
        for it in parsed.get("all_items") or []:
            if isinstance(it, dict):
                _add_title(
                    bag,
                    str(it.get("title") or ""),
                    str(it.get("source") or ""),
                    str(it.get("published_at") or ""),
                    str(it.get("url") or ""),
                    {"known_class": it.get("class") or ""},
                )
    digest = _load_json(NEWS_DIR / f"{date}_finviz_digest.json")
    if isinstance(digest, dict):
        for it in (digest.get("top_signal") or []) + (digest.get("index_digests") or []):
            if isinstance(it, dict):
                _add_title(
                    bag,
                    str(it.get("news_title") or it.get("digest") or it.get("title") or ""),
                    str(it.get("source") or "finviz_digest"),
                    date,
                )
    actions = _load_json(NEWS_DIR / f"{date}_actions.json")
    if isinstance(actions, dict):
        for it in actions.get("events") or actions.get("items") or []:
            if isinstance(it, dict):
                _add_title(
                    bag,
                    str(it.get("title") or it.get("headline") or ""),
                    "actions",
                    date,
                )
    ev = _load_json(EVENTS_DIR / f"{date}_events.json")
    if isinstance(ev, dict):
        for it in ev.get("events") or []:
            if isinstance(it, dict):
                _add_title(
                    bag,
                    str(it.get("title") or it.get("event") or it.get("name") or ""),
                    "events",
                    str(it.get("when") or it.get("timing") or date),
                )
    export = EXPORTS_DIR / f"finviz_{date}.csv"
    if export.is_file():
        with export.open(newline="", encoding="utf-8", errors="replace") as fh:
            for row in csv.DictReader(fh):
                _add_title(
                    bag,
                    str(row.get("News Title") or ""),
                    "finviz_export",
                    str(row.get("News Time") or date),
                )
    if GROK_DIR.is_dir():
        for path in sorted(GROK_DIR.glob(f"{date}_*.json")):
            blob = _load_json(path)
            rows = blob if isinstance(blob, list) else (blob or {}).get("results") or [blob]
            for it in rows:
                if isinstance(it, dict):
                    _add_title(
                        bag,
                        str(it.get("title") or ""),
                        "grok_automations",
                        str(it.get("createTime") or date),
                    )
    return list(bag.values())


def gold_rows() -> list[dict]:
    blob = load_gold()
    rows = []
    for it in blob.get("items") or []:
        row = {
            "id": it.get("id") or "",
            "title": it.get("title") or "",
            "source": it.get("source") or "",
            "published_at": it.get("published_at") or "",
            "url": "",
            "answers": answers_from_gold(it),
            "expect": it.get("expect") or "",
            "code_must": it.get("code_must") or "",
        }
        rows.append(row)
    return rows


def summarize(decided: list[dict], *, date: str, mode: str,
              code_only: bool, live: bool, n_in: int) -> dict:
    keeps = [r for r in decided if r.get("decision") == "keep"]
    drops = [r for r in decided if r.get("decision") != "keep"]
    reasons: Counter[str] = Counter(r.get("reason") or "unknown" for r in decided)
    return {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "date": date,
        "mode": mode,
        "code_only": code_only,
        "live": live,
        "n_in": n_in,
        "n_keep": len(keeps),
        "n_drop": len(drops),
        "drop_rate": round(len(drops) / n_in, 4) if n_in else 0.0,
        "reasons": dict(reasons),
        "keeps": keeps,
        "drops": drops,
    }


def gold_check(decided: list[dict], rows: list[dict], *,
               live: bool, code_only: bool) -> dict:
    gold = load_gold()
    by_id = {r.get("id"): r for r in decided}
    src = {r.get("id"): r for r in rows}
    results = []
    fails = []
    for item in gold.get("items") or []:
        rid = item.get("id")
        got = by_id.get(rid) or {}
        decision = got.get("decision") or ""
        reason = got.get("reason") or ""
        code_must = item.get("code_must") or ""
        expect = item.get("expect") or ""
        ok = True
        note = ""
        code_kills = {
            "source", "punct", "junk_shape", "dup", "reprint_weather",
        }
        if code_must == "drop" and reason not in code_kills:
            ok = False
            note = f"code_must drop, got {decision}/{reason}"
        elif code_must == "leftover" and reason in code_kills:
            ok = False
            note = f"code killed leftover: {reason}"
        if code_only:
            if code_must == "leftover" and decision != "keep":
                ok = False
                note = note or f"code_only leftover should survive, got {decision}/{reason}"
        elif expect and decision != expect:
            ok = False
            note = note or f"expect {expect}, got {decision}/{reason}"
        rec = {
            "id": rid,
            "ok": ok,
            "expect": expect,
            "code_must": code_must,
            "decision": decision,
            "reason": reason,
            "geo": got.get("geo") or "",
            "note": note,
            "title": (src.get(rid) or {}).get("title") or got.get("title") or "",
        }
        results.append(rec)
        if not ok:
            fails.append(rec)
    must_keep_fail = [
        r for r in results
        if r["id"] in set(gold.get("must_keep") or []) and r["decision"] != "keep"
        and not code_only
    ]
    must_drop_fail = [
        r for r in results
        if r["id"] in set(gold.get("must_drop") or []) and r["decision"] != "drop"
        and not code_only
    ]
    return {
        "n": len(results),
        "n_fail": len(fails),
        "must_keep_fail": must_keep_fail,
        "must_drop_fail": must_drop_fail,
        "rows": results,
        "ok": not fails and not must_keep_fail and not must_drop_fail,
    }


def to_markdown(report: dict) -> str:
    lines = [
        f"# Jev hop-0 gate — {report.get('date')}",
        "",
        f"mode={report.get('mode')} code_only={report.get('code_only')} "
        f"live={report.get('live')} in={report.get('n_in')} "
        f"keep={report.get('n_keep')} drop={report.get('n_drop')} "
        f"drop_rate={report.get('drop_rate')}",
        "",
        "Jev does not classify event types or polarity. Only keeps leave this hop.",
        "",
        "## Reasons",
    ]
    for reason, n in sorted(
        (report.get("reasons") or {}).items(), key=lambda kv: -kv[1]
    ):
        lines.append(f"- {reason}: {n}")
    gold = report.get("gold")
    if gold:
        lines += [
            "",
            f"## Gold  ok={gold.get('ok')} fail={gold.get('n_fail')}/{gold.get('n')}",
        ]
        for row in gold.get("rows") or []:
            mark = "ok" if row.get("ok") else "FAIL"
            lines.append(
                f"- [{mark}] {row.get('id')} expect={row.get('expect')} "
                f"got={row.get('decision')}/{row.get('reason')} "
                f"{(row.get('title') or '')[:80]}"
            )
    lines += ["", "## Keeps"]
    for row in (report.get("keeps") or [])[:80]:
        lines.append(
            f"- [{row.get('reason')}|{row.get('geo')}] "
            f"{(row.get('title') or '')[:140]}"
        )
    if not report.get("keeps"):
        lines.append("- (none)")
    lines += ["", "## Drop sample"]
    by_reason = defaultdict(list)
    for row in report.get("drops") or []:
        by_reason[row.get("reason") or "?"].append(row)
    for reason, rows in sorted(by_reason.items()):
        lines.append(f"### {reason} ({len(rows)})")
        for row in rows[:8]:
            lines.append(f"- {(row.get('title') or '')[:140]}")
    return "\n".join(lines) + "\n"


def write_report(report: dict, date: str) -> tuple[Path, Path]:
    json_path = NEWS_DIR / f"{date}_jev_keep.json"
    _write_json(json_path, report)
    SCOREBOARD.parent.mkdir(parents=True, exist_ok=True)
    SCOREBOARD.write_text(to_markdown(report), encoding="utf-8")
    return json_path, SCOREBOARD


def mine_junk_shapes(top: int = 50, min_junk: int = 8) -> dict:
    junk_c: Counter[str] = Counter()
    keep_c: Counter[str] = Counter()
    n_j = n_k = 0
    for path in sorted(NEWS_DIR.glob("*_parsed.json")):
        blob = _load_json(path)
        if not isinstance(blob, dict):
            continue
        for it in blob.get("all_items") or []:
            if not isinstance(it, dict):
                continue
            words = list(tokens(str(it.get("title") or "")))
            # preserve order for bigrams via normalize split
            ordered = [
                w for w in normalize_title(str(it.get("title") or "")).split()
                if len(w) >= 3 and w not in STOP
            ]
            grams = ordered + [" ".join(p) for p in zip(ordered, ordered[1:])]
            is_junk = (not it.get("usable")) or it.get("class") == "noise"
            if is_junk:
                junk_c.update(grams)
                n_j += 1
            else:
                keep_c.update(grams)
                n_k += 1
    scored = []
    for gram, n in junk_c.items():
        if n < min_junk:
            continue
        k = keep_c.get(gram, 0)
        lift = n / max(k, 1)
        if lift < 3:
            continue
        scored.append({"shape": gram, "junk": n, "keep": k, "lift": round(lift, 2)})
    scored.sort(key=lambda r: (-r["lift"], -r["junk"]))
    report = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "junk_titles": n_j,
        "keep_titles": n_k,
        "top": scored[:top],
        "note": (
            "Soft deny-hints only. Do not promote beats/ipo/guidance — "
            "those leak CAFE / listings. Closed shapes stay in "
            "00_grounding/jev_junk_shapes.json."
        ),
    }
    out = NEWS_DIR / "jev_junk_shapes_mined.json"
    _write_json(out, report)
    md = ["# Mined junk-title shapes", "", report["note"], ""]
    for row in report["top"][:top]:
        md.append(
            f"- lift={row['lift']} junk={row['junk']} keep={row['keep']}  "
            f"`{row['shape']}`"
        )
    (NEWS_DIR / "jev_junk_shapes_mined.md").write_text(
        "\n".join(md) + "\n", encoding="utf-8"
    )
    return report


def questions_are_hop0(questions: dict = QUESTIONS) -> None:
    blob = json.dumps(questions).lower()
    for bit in FORBIDDEN_QUESTION_BITS:
        if bit in blob:
            raise AssertionError(f"Jev pack must not ask {bit}")
    if set(questions) & {"event_class", "polarity", "q5", "direction"}:
        raise AssertionError("Jev pack grew a classifier question")


def run_gold(*, live: bool, code_only: bool, workers: int,
             poster=None) -> dict:
    rows = gold_rows()
    gold_answers = {r["id"]: r.get("answers") or {} for r in rows}
    decided = gate(
        rows,
        code_only=code_only,
        live=live,
        workers=workers,
        poster=poster,
        gold_answers=None if live else gold_answers,
        asof=dt.date.fromisoformat("2026-09-27"),
    )
    report = summarize(
        decided, date="gold", mode="gold",
        code_only=code_only, live=live, n_in=len(rows),
    )
    report["gold"] = gold_check(decided, rows, live=live, code_only=code_only)
    return report


def run_harvest(date: str, limit: int, *, code_only: bool, live: bool,
                workers: int, poster=None) -> dict:
    if date == "latest":
        date = latest_session_date()
    if date == "all":
        rows: list[dict] = []
        for day in list_session_dates():
            rows.extend(load_titles(day))
        session = "all"
    else:
        rows = load_titles(date)
        session = date
    if limit and limit > 0:
        rows = rows[:limit]
    asof = parse_iso_date(session if session != "all" else "") or dt.date.today()
    decided = gate(
        rows, code_only=code_only, live=live, workers=workers,
        poster=poster, asof=asof,
    )
    return summarize(
        decided, date=session, mode="harvest",
        code_only=code_only, live=live, n_in=len(rows),
    )


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Jev hop-0 news gate")
    p.add_argument("--date", default="", help="YYYY-MM-DD, latest, or all")
    p.add_argument("--limit", type=int, default=200)
    p.add_argument("--gold", action="store_true")
    p.add_argument("--code-only", action="store_true")
    p.add_argument("--live", action="store_true",
                   help="Force Jev HTTP (default when a key is set and not --code-only)")
    p.add_argument("--mine", action="store_true")
    p.add_argument("--workers", type=int, default=24)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    questions_are_hop0()
    if args.mine:
        mined = mine_junk_shapes()
        print(f"[jev_gate] mined {len(mined.get('top') or [])} shapes "
              f"from {mined.get('junk_titles')} junk titles")
        if not args.gold and not args.date:
            return 0

    key = api_key()
    live = bool(args.live or (key and not args.code_only))
    if args.gold:
        if args.live and not key and not args.code_only:
            print("[jev_gate] --live requested but JEV_API_KEY is empty")
            return 1
        report = run_gold(
            live=live and not args.code_only,
            code_only=args.code_only,
            workers=args.workers,
        )
        path, md = write_report(report, "gold")
        print(f"[jev_gate] gold keep={report['n_keep']} drop={report['n_drop']} "
              f"ok={report['gold']['ok']} → {path}")
        print(md.read_text(encoding="utf-8")[:2000])
        if not report["gold"]["ok"]:
            print("[jev_gate] gold mismatches:")
            for row in report["gold"]["rows"]:
                if not row.get("ok"):
                    print(" ", row)
            return 2
        if not args.date:
            return 0

    date = args.date or "latest"
    if live and not key and not args.code_only:
        print("[jev_gate] JEV_API_KEY / TYPESAFE_API_KEY is empty")
        return 1
    report = run_harvest(
        date, args.limit,
        code_only=args.code_only,
        live=live and not args.code_only,
        workers=args.workers,
    )
    path, md = write_report(report, report["date"])
    print(
        f"[jev_gate] {report['date']} in={report['n_in']} "
        f"keep={report['n_keep']} drop={report['n_drop']} "
        f"drop_rate={report['drop_rate']} → {path}"
    )
    print(md.read_text(encoding="utf-8")[:1800])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
