"""M1–M5 meta-question checks.

The floor model writes the questions. This module only keeps, drops, and
checks them. It does not invent a question the model did not ask.
"""
from __future__ import annotations

import re

PACK_FAMILIES = frozenset({"blast", "structure", "quantity", "permission"})
SLOTS = frozenset({
    "C", "T", "H", "E", "S", "R", "A", "D", "I", "P", "Y-S", "Y-T",
})
CHANGES = frozenset({
    "direction", "class", "clock", "unit_vs_parent", "tradeable_expression",
})
_STOP = frozenset(
    "the a an of to and or for on in with as at by from that this after "
    "before over into its their was were are been have has had not but who "
    "what when where which will just per than then also".split()
)
_AI = re.compile(r"(?i)what'?s the ai angle|what is the ai angle|\bai angle\b")
_BUY = re.compile(r"(?i)who should i buy")
_WEATHER = re.compile(r"(?i)\bweather\b")
_DATED = re.compile(r"\b(?:19|20)\d{2}\b|\d{4}-\d{2}-\d{2}")


def article_nouns(title: str, body: str = "") -> set[str]:
    words = re.findall(r"[A-Za-z][A-Za-z0-9'’-]{2,}", f"{title or ''} {body or ''}")
    return {w.lower() for w in words if w.lower() not in _STOP}


def _bullshit(question: str, article: str) -> str:
    if _AI.search(question or ""):
        return "ai_angle"
    if _BUY.search(question or ""):
        return "who_should_i_buy"
    if _WEATHER.search(question or "") and not _DATED.search(question or ""):
        return "undated_weather"
    folded = re.sub(r"\s+", " ", (question or "").lower()).strip(" ?.")
    art = re.sub(r"\s+", " ", (article or "").lower())
    if len(folded) > 24 and folded in art:
        return "already_in_article"
    return ""


def _noun_in(noun: str, question: str, nouns: set[str], article: str) -> bool:
    token = (noun or "").strip().lower()
    if len(token) < 3 or token not in nouns:
        return False
    if token not in (article or "").lower():
        return False
    return bool(re.search(rf"(?i)\b{re.escape(token)}\b", question or ""))


def normalize_meta(
    parsed: dict | None,
    *,
    title: str,
    body: str,
    family: str,
) -> dict | None:
    """Validate a floor-model M1–M3 object. Empty M2 is a miss."""
    if not isinstance(parsed, dict):
        return None
    m1 = parsed.get("m1") if isinstance(parsed.get("m1"), dict) else {}
    need = str(m1.get("need_context") or "").strip().lower()
    if need not in {"yes", "no"}:
        return None
    article = f"{title or ''}\n{body or ''}"
    nouns = article_nouns(title, body)
    kept: list[dict] = []
    dropped: list[dict] = []
    for row in parsed.get("m3_dropped") or []:
        if not isinstance(row, dict):
            continue
        q = str(row.get("question") or "").strip()
        why = str(row.get("why") or "").strip()[:80]
        if q:
            dropped.append({"question": q[:300], "why": why or "dropped"})
    for row in parsed.get("m2") or []:
        if not isinstance(row, dict):
            continue
        question = str(row.get("question") or "").strip()
        slot = str(row.get("slot") or "").strip()
        noun = str(row.get("noun") or "").strip()
        changes = str(row.get("changes") or "").strip()
        if not question:
            continue
        why = _bullshit(question, article)
        if not why and slot not in SLOTS:
            why = "bad_slot"
        if not why and changes not in CHANGES:
            why = "no_direction_change"
        if not why and not _noun_in(noun, question, nouns, article):
            why = "theme_fishing"
        if why:
            dropped.append({"question": question[:300], "why": why})
            continue
        blocks = [str(b) for b in (row.get("blocks") or []) if str(b) in CHANGES]
        if changes == "direction" and "direction" not in blocks:
            blocks.append("direction")
        if not blocks:
            blocks = [changes]
        kept.append({
            "slot": slot,
            "noun": noun,
            "question": question[:400],
            "changes": changes,
            "blocks": blocks,
            "status": "pending",
        })
    if not kept:
        return None
    pack_required = need == "no" or family in PACK_FAMILIES
    return {
        "m1": {"need_context": need, "pack_required": pack_required},
        "m2": kept,
        "m3_dropped": dropped,
        "m4": {"invert": "", "pack_complete": False},
        "m5": {"instruments_cited": [], "tickers_emitted": []},
    }


def meta_acceptable(parsed: dict | None, title: str, body: str, family: str) -> bool:
    return normalize_meta(parsed, title=title, body=body, family=family) is not None


def apply_pack_complete(meta: dict, parsed: dict | None) -> bool:
    """Fold M4 onto meta. True when every direction block is answered or blocked."""
    slots: dict[str, str] = {}
    invert = ""
    if isinstance(parsed, dict):
        invert = str(parsed.get("invert") or "").strip()[:300]
        for row in parsed.get("slots") or []:
            if not isinstance(row, dict):
                continue
            slots[str(row.get("question") or "").strip()] = str(row.get("status") or "").lower()
    open_block = False
    for q in meta.get("m2") or []:
        status = slots.get(q.get("question") or "")
        if "direction" not in (q.get("blocks") or []):
            if status in {"answered", "blocked"}:
                q["status"] = status
            continue
        if status in {"answered", "blocked"}:
            q["status"] = status
        else:
            q["status"] = "unanswered"
            open_block = True
    meta["m4"] = {"invert": invert, "pack_complete": not open_block}
    return not open_block


def pack_complete_acceptable(parsed: dict | None, questions: list[dict]) -> bool:
    if not isinstance(parsed, dict) or not str(parsed.get("invert") or "").strip():
        return False
    got: dict[str, str] = {}
    for row in parsed.get("slots") or []:
        if isinstance(row, dict):
            got[str(row.get("question") or "").strip()] = str(row.get("status") or "").lower()
    for q in questions:
        if "direction" not in (q.get("blocks") or []):
            continue
        if got.get(q.get("question") or "") not in {"answered", "blocked"}:
            return False
    return True


def direction_blocked(meta: dict) -> bool:
    """A blocked direction question is not_determined, not a guess."""
    for q in meta.get("m2") or []:
        if "direction" in (q.get("blocks") or []) and q.get("status") == "blocked":
            return True
    return not bool((meta.get("m4") or {}).get("pack_complete"))


def pack_nonempty(facts: list[dict]) -> bool:
    for fact in facts or []:
        if str(fact.get("status") or "") == "unknown":
            continue
        if not str(fact.get("text") or "").strip():
            continue
        if fact.get("source") in {"google_ai_overview", "finviz_row", "websearch"}:
            return True
    return False


def finviz_facts(instruments: list[dict]) -> list[dict]:
    facts = []
    for inst in instruments or []:
        tick = str(inst.get("ticker") or "").upper()
        if not tick:
            continue
        inst["source"] = inst.get("source") or "finviz_row"
        text = " | ".join(
            bit for bit in (
                tick,
                str(inst.get("entity_name") or ""),
                str(inst.get("industry") or ""),
            ) if bit
        )
        facts.append({
            "text": text[:400],
            "url": "",
            "source": "finviz_row",
            "status": "quote",
            "ticker": tick,
        })
    return facts


def apply_m5(
    entities: list[dict],
    instruments: list[dict],
    facts: list[dict],
) -> tuple[list[dict], list[str]]:
    """Drop any ticker that is not on a cited Finviz or pack row."""
    cited: set[str] = set()
    for inst in instruments or []:
        tick = str(inst.get("ticker") or "").upper()
        if tick and (inst.get("source") in {None, "", "finviz_row"} or inst.get("entity_name")):
            cited.add(tick)
    for fact in facts or []:
        if fact.get("source") not in {"google_ai_overview", "finviz_row", "websearch"}:
            continue
        tick = str(fact.get("ticker") or "").upper()
        if tick:
            cited.add(tick)
        text = str(fact.get("text") or "").upper()
        for inst in instruments or []:
            inst_tick = str(inst.get("ticker") or "").upper()
            if inst_tick and inst_tick in text.split():
                cited.add(inst_tick)
    kept = []
    for ent in entities or []:
        tick = str(ent.get("ticker") or "").upper()
        if tick and tick not in cited:
            continue
        kept.append(ent)
    return kept, sorted(cited)
