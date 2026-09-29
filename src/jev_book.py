"""Hop-2 book: Finviz word/industry lookup, then Jev sides the names.

Hop-1 already named event_class|q5. This hop only attaches listed
expressions from the on-disk Finviz export. It does not invent a
ticker. It does not call Lane. keep.json stays unwired.

  hop-1 class → Finviz title-word + same-industry lookup
             → one Jev pack (named_side / peer_side / attach_peers)
             → book[{ticker, company, industry, side, role}]

Code prior is news_impact.finviz_linker.candidate_rows. Live Jev
overrides sides only among those candidates. Weather / discard → [].
"""
from __future__ import annotations

import argparse
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from .jev_gate import api_key, jev_post, parse_answers
from .news_impact.finviz_linker import candidate_rows
from .news_impact.schema import DISCARD_OR_WEATHER, EVENT_CLASSES, Q5_STATUS

BOOK_LIMIT = 6
SIDES = ("up", "down", "mixed", "out")
NAMED_WHY = ("ticker_in_text", "company:", "brand:", "hint_named")
PEER_WHY = ("industry:", "peer:", "digest:")

QUESTIONS: dict = {
    "named_side": {
        "type": "choice",
        "instructions": (
            "For names the title actually says (or their listed parent), "
            "which way does the hop-1 class push them? out = drop them. "
            "Pick only from the CANDIDATES list in the state. "
            "Do not add a listed name that is not on that list."
        ),
        "criteria": {
            "up": "The named name is helped by this fact today",
            "down": "The named name is hurt by this fact today",
            "mixed": "Helped on one path, hurt on another, or unsigned",
            "out": "The named name is not a channel for this fact",
        },
    },
    "peer_side": {
        "type": "choice",
        "instructions": (
            "For same-industry cousins that are NOT in the title, "
            "which way does the class push them? out = do not attach them."
        ),
        "criteria": {
            "up": "Industry substitute / unscathed cousin is helped",
            "down": "Industry peer is hurt with the named name",
            "mixed": "Unsigned or path-dependent",
            "out": "Do not attach industry cousins",
        },
    },
    "attach_peers": {
        "type": "noul",
        "instructions": (
            "Should same-industry cousins that are not in the title "
            "be attached at all? High only when the family test needs "
            "a substitute or peer (blast harm, gate open, input cost)."
        ),
        "criteria": {
            "true": (
                "Blast harm set, gate that opens a scarce permit, "
                "input-cost payers vs sellers, or a venue/rail swap"
            ),
            "false": (
                "Single-name print, own-firm cash/paper, weather, "
                "or the title already named everyone who matters"
            ),
        },
    },
}

FORBIDDEN_HOP2_BITS = (
    "bullish",
    "bearish",
    "polarity",
    "ticker expansion",
)

ATTACH_PEERS = 0.55


def questions_are_hop2(questions: dict = QUESTIONS) -> None:
    blob = json.dumps(questions).lower()
    for bit in FORBIDDEN_HOP2_BITS:
        if bit in blob:
            raise AssertionError(f"hop-2 pack must not ask {bit}")
    if set(questions) != {"named_side", "peer_side", "attach_peers"}:
        raise AssertionError(f"hop-2 pack drifted: {sorted(questions)}")
    if set(questions["named_side"]["criteria"]) != set(SIDES):
        raise AssertionError("hop-2 named_side drifted")
    if set(questions["peer_side"]["criteria"]) != set(SIDES):
        raise AssertionError("hop-2 peer_side drifted")


def default_sides(family: str, event_class: str, sign: str | None,
                  q5: str) -> dict[str, str] | None:
    """Code prior. Empty book when there is no tradeable mechanism."""
    if q5 == "regime" or event_class in DISCARD_OR_WEATHER:
        return None
    if family == "blast":
        return {"named": "down", "peer": "up"}
    if family == "permission":
        if sign == "shut":
            return {"named": "down", "peer": "up"}
        return {"named": "up", "peer": "down"}
    if family == "quantity":
        if event_class == "input_cost" or sign == "destroy":
            return {"named": "down", "peer": "up"}
        if sign == "add":
            return {"named": "up", "peer": "down"}
        return {"named": "mixed", "peer": "mixed"}
    if family == "print":
        if sign in {"raise", "up"}:
            return {"named": "up", "peer": "out"}
        if sign in {"cut", "down"}:
            return {"named": "down", "peer": "out"}
        return {"named": "mixed", "peer": "out"}
    if family == "structure":
        return {"named": "mixed", "peer": "mixed"}
    if family == "firm":
        return {"named": "mixed", "peer": "out"}
    if family == "flow":
        return {"named": "mixed", "peer": "out"}
    return {"named": "mixed", "peer": "out"}


def _is_named(why: str) -> bool:
    why = why or ""
    return any(why.startswith(bit) or bit in why for bit in NAMED_WHY)


def _is_peer(why: str) -> bool:
    why = why or ""
    return any(why.startswith(bit) or bit in why for bit in PEER_WHY)


def _named_or_peer(title: str, family: str, event_class: str,
                   why: str) -> tuple[bool, bool]:
    """Industry hits are peers unless the family test treats them as the set."""
    if _is_named(why):
        return True, False
    if (
        family == "blast"
        and event_class == "blast_ops"
        and "industry:Airlines" in (why or "")
    ):
        return True, False
    if _is_peer(why):
        return False, True
    return False, False


def lookup_candidates(title: str, *, family: str = "", event_class: str = "",
                      root: Path | None = None,
                      limit: int = BOOK_LIMIT) -> list[dict]:
    """Finviz word + industry hit list. Never invents a ticker."""
    hit = candidate_rows(
        title or "",
        "",
        family=family,
        event_class=event_class,
        limit=40,
        root=root,
    )
    out = []
    for row in hit.get("instruments") or []:
        tick = str(row.get("ticker") or "").upper().strip()
        if not tick:
            continue
        why = str(row.get("why") or "")
        named, peer = _named_or_peer(title, family, event_class, why)
        out.append({
            "ticker": tick,
            "company": str(row.get("entity_name") or ""),
            "industry": str(row.get("industry") or ""),
            "sector": str(row.get("sector") or ""),
            "why": why,
            "score": int(row.get("score") or 0),
            "named": named,
            "peer": peer,
        })
        if len(out) >= limit:
            break
    return out


def _clean_side(raw) -> str:
    side = str(raw or "").strip().lower()
    return side if side in SIDES else "out"


def decide_book(title: str, *, family: str = "", event_class: str = "",
                sign: str | None = None, q5: str = "",
                answers: dict | None = None,
                root: Path | None = None) -> list[dict]:
    """Pure hop-2 decide. Jev may only side names already in the lookup."""
    defaults = default_sides(family, event_class, sign, q5)
    if defaults is None:
        return []
    named_side = defaults["named"]
    peer_side = defaults["peer"]
    attach = peer_side != "out"
    if answers:
        named_side = _clean_side(answers.get("named_side") or named_side)
        peer_side = _clean_side(answers.get("peer_side") or peer_side)
        try:
            attach = float(answers.get("attach_peers") or 0.0) >= ATTACH_PEERS
        except (TypeError, ValueError):
            attach = peer_side != "out"
        if peer_side == "out":
            attach = False
    book = []
    for row in lookup_candidates(
        title, family=family, event_class=event_class, root=root,
    ):
        if row["named"]:
            side = named_side
            role = "named"
        elif row["peer"] and attach:
            side = peer_side
            role = "substitute" if family == "blast" else "peer"
        else:
            continue
        if side == "out":
            continue
        book.append({
            "ticker": row["ticker"],
            "company": row["company"],
            "industry": row["industry"],
            "sector": row["sector"],
            "side": side,
            "role": role,
            "why": row["why"],
        })
        if len(book) >= BOOK_LIMIT:
            break
    return book


def book_reason(book: list[dict]) -> str:
    bits = []
    for row in book:
        tick = row.get("ticker") or ""
        side = row.get("side") or ""
        if not tick:
            continue
        mark = {"up": "↑", "down": "↓", "mixed": "~"}.get(side, "")
        bits.append(f"{tick}{mark}")
    return " ".join(bits)


def empty_book_fields() -> dict:
    return {"book": [], "book_reason": "", "book_source": ""}


def fields_from_book(book: list[dict], *, source: str) -> dict:
    return {
        "book": book,
        "book_reason": book_reason(book),
        "book_source": source if book else "",
    }


def classify_book(row: dict, answers: dict | None = None,
                  root: Path | None = None) -> dict:
    ev = str(row.get("event_class") or "")
    q5 = str(row.get("q5") or "")
    if ev not in EVENT_CLASSES or q5 not in Q5_STATUS:
        return empty_book_fields()
    book = decide_book(
        row.get("title") or "",
        family=str(row.get("family") or ""),
        event_class=ev,
        sign=row.get("sign"),
        q5=q5,
        answers=answers,
        root=root,
    )
    source = "jev" if answers else "code"
    return fields_from_book(book, source=source)


def apply_code_book(decided: list[dict], *, root: Path | None = None) -> list[dict]:
    """Stamp a Finviz book on hop-1 keeps. Drops stay empty."""
    for row in decided:
        if row.get("decision") != "keep":
            row.update(empty_book_fields())
            continue
        if row.get("book"):
            continue
        row.update(classify_book(row, row.get("_book_answers"), root=root))
    return decided


def make_book_state(row: dict, candidates: list[dict]) -> str:
    lines = [
        f"TITLE: {row.get('title') or ''}",
        f"CLASS: {row.get('event_class') or ''} q5={row.get('q5') or ''} "
        f"sign={row.get('sign') or 'none'} family={row.get('family') or ''}",
        "CANDIDATES (pick sides only among these; do not add a name):",
    ]
    if not candidates:
        lines.append("- (none)")
    for item in candidates:
        kind = "named" if item.get("named") else "peer"
        lines.append(
            f"- {item['ticker']} {item.get('company') or ''} "
            f"[{item.get('industry') or ''}] {kind}"
        )
    return "\n".join(lines)


def jev_book_many(rows: list[dict], key: str, workers: int = 24,
                  poster=None, root: Path | None = None
                  ) -> list[tuple[dict, dict | None, str]]:
    poster = poster or jev_post
    out: list[tuple[dict, dict | None, str]] = []
    if not rows:
        return out

    def one(row: dict):
        cands = lookup_candidates(
            row.get("title") or "",
            family=str(row.get("family") or ""),
            event_class=str(row.get("event_class") or ""),
            root=root,
        )
        payload = poster(make_book_state(row, cands), QUESTIONS, key)
        return row, parse_answers(payload), payload.get("model") or ""

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


def apply_jev_book(decided: list[dict], *, key: str, workers: int = 24,
                   poster=None, root: Path | None = None) -> list[dict]:
    """Second Jev pack on hop-1 keeps. Invalid answers keep the code book."""
    keeps = [
        row for row in decided
        if row.get("decision") == "keep"
        and row.get("event_class") in EVENT_CLASSES
    ]
    if not keeps:
        return decided
    key = key or api_key()
    if not key:
        raise RuntimeError("JEV_API_KEY / TYPESAFE_API_KEY is empty")
    for row, answers, model in jev_book_many(
        keeps, key, workers, poster, root=root,
    ):
        row["_book_answers"] = answers or {}
        if model:
            row["_book_model"] = model
        row.update(classify_book(row, answers, root=root))
    return decided


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Jev hop-2 book")
    p.add_argument("--title", default="", help="Classify one title against Finviz")
    p.add_argument("--family", default="")
    p.add_argument("--event-class", default="")
    p.add_argument("--sign", default="")
    p.add_argument("--q5", default="impulse")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    questions_are_hop2()
    if not args.title:
        print("[jev_book] hop-2 pack ready. Pass --title to look up a book.")
        return 0
    book = decide_book(
        args.title,
        family=args.family,
        event_class=args.event_class,
        sign=args.sign or None,
        q5=args.q5,
    )
    print(json.dumps({"title": args.title, "book": book,
                      "book_reason": book_reason(book)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
