"""Jev unique-title book backtest: hop-0/1/2 sides vs publication tape.

Same corpus as the old unique-title book. Assignment is Jev's Finviz
book (never an invented ticker), not Lane families.analyze. Clock is
published_at → next RTH, never parse time. keep.json stays unwired.

  unique titles → hop-0 code decide → hop-1 classify_text → hop-2 book
               → entities[ticker, up|down] → grade_results(parquet)
               → ledger n_bull / n_bear (X / Y) vs Z-session returns

Code path only. Live Jev HTTP is not spent on the full harvest.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
from collections import Counter
from pathlib import Path

from .jev_book import apply_code_book
from .jev_classify import apply_code_classify
from .jev_gate import apply_code, decide, load_chokepoint_state
from .news_impact.corpus import dedupe_titles, inventory, load_all_sources
from .news_impact.grade import grade_results, performance_rollup, signal_dt
from .news_impact.ledger import build_ledger

SCOREBOARD = Path("03_scoreboard/JEV_BOOK_BACKTEST.md")
RATES = Path("01_daily/news/all_jev_book_backtest.json")

Z_HORIZONS = (
    ("0-1d", "ret_1d"),
    ("2d", "ret_2d"),
    ("3d", "ret_3d"),
    ("4d", "ret_4d"),
    ("5d", "ret_5d"),
    ("1-4w", "ret_20d"),
)


def article_to_row(art: dict) -> dict:
    return {
        "title": art.get("title") or "",
        "source": art.get("harvest_source") or art.get("source") or "",
        "published_at": art.get("published_at") or art.get("known_at") or "",
        "retrieved_at": art.get("retrieved_at") or "",
        "url": art.get("url") or "",
        "id": art.get("id") or "",
    }


def book_to_entities(book: list[dict]) -> list[dict]:
    """Jev sides → ledger votes. mixed does not vote. Never add a ticker."""
    out = []
    seen: set[str] = set()
    for row in book or []:
        tick = str(row.get("ticker") or "").upper().strip()
        side = str(row.get("side") or "")
        if not tick or tick in seen or side not in {"up", "down"}:
            continue
        seen.add(tick)
        role = str(row.get("role") or "named")
        out.append({
            "name": row.get("company") or tick,
            "ticker": tick,
            "role": role,
            "direction": side,
            "tradeable_expression": "direct" if role == "named" else "proxy",
        })
    return out


def evaluate_articles(arts: list[dict], *, asof: dt.date | None = None,
                      root: Path | None = None) -> list[dict]:
    """Hop-0/1/2 code path on every title. Leftovers stay for hop-1."""
    asof = asof or dt.date.today()
    rows = [article_to_row(art) for art in arts]
    rows = apply_code(rows, asof=asof, state=load_chokepoint_state())
    decided = [decide(row, None) for row in rows]
    apply_code_classify(decided)
    print(
        f"[jev_backtest] classified keeps="
        f"{sum(1 for row in decided if row.get('decision') == 'keep')}",
        flush=True,
    )
    step = 2000
    for i in range(0, len(decided), step):
        apply_code_book(decided[i:i + step], root=root)
        print(
            f"[jev_backtest] book {min(i + step, len(decided))}/{len(decided)}",
            flush=True,
        )
    out = []
    for art, dec in zip(arts, decided):
        entities = book_to_entities(dec.get("book") or [])
        q5 = str(dec.get("q5") or "")
        ev = str(dec.get("event_class") or "")
        keep = dec.get("decision") == "keep"
        impulse = keep and q5 == "impulse" and bool(entities)
        published = (
            art.get("published_at") or dec.get("published_at")
            or art.get("known_at") or ""
        )
        out.append({
            "title": art.get("title") or dec.get("title") or "",
            "source": art.get("harvest_source") or art.get("source") or "",
            "published_at": published,
            "retrieved_at": art.get("retrieved_at") or "",
            "known_at": art.get("known_at") or published,
            "url": art.get("url") or "",
            "decision": dec.get("decision") or "",
            "reason": dec.get("reason") or "",
            "event_class": ev,
            "q5": q5,
            "sign": dec.get("sign"),
            "family": dec.get("family") or "",
            "class_reason": dec.get("class_reason") or "",
            "book_reason": dec.get("book_reason") or "",
            "book_source": dec.get("book_source") or "",
            "classification": {
                "event_class": ev,
                "sign": dec.get("sign"),
                "q5": q5 or "regime",
                "constraint": dec.get("constraint") or "",
            },
            "entities": entities,
            "usable": impulse,
            "clock": "published_at",
        })
    return out


def vote_hit_rates(results: list[dict]) -> dict:
    """Per article-ticker side vs Z-session returns. Publication clock."""
    bags = {
        "named": {hz: {"hits": 0, "n": 0} for hz, _ in Z_HORIZONS},
        "substitute": {hz: {"hits": 0, "n": 0} for hz, _ in Z_HORIZONS},
        "all": {hz: {"hits": 0, "n": 0} for hz, _ in Z_HORIZONS},
    }
    for row in results:
        if not signal_dt(row):
            continue
        by_tick = {}
        for g in row.get("performance") or []:
            tick = str(g.get("ticker") or "").upper()
            if tick:
                by_tick[tick] = g
        for ent in row.get("entities") or []:
            tick = str(ent.get("ticker") or "").upper()
            side = str(ent.get("direction") or "")
            if side not in {"up", "down"} or tick not in by_tick:
                continue
            g = by_tick[tick]
            role = "named" if ent.get("role") == "named" else "substitute"
            for hz, key in Z_HORIZONS:
                ret = g.get(key)
                if ret is None:
                    continue
                hit = (side == "up" and float(ret) > 0) or (
                    side == "down" and float(ret) < 0
                )
                for bag_name in (role, "all"):
                    bags[bag_name][hz]["n"] += 1
                    bags[bag_name][hz]["hits"] += int(hit)
    for group in bags.values():
        for slot in group.values():
            n = slot["n"]
            slot["hit_rate"] = round(slot["hits"] / n, 4) if n else None
    return bags


def _pct(h, n) -> str:
    if not n:
        return "n/a"
    return f"{h}/{n} = {h / n:.1%}"


def _rate_line(rates: dict, bucket: str) -> str:
    bits = []
    for hz, _ in Z_HORIZONS:
        slot = ((rates.get(bucket) or {}).get(hz) or {})
        bits.append(f"{hz} {_pct(slot.get('hits') or 0, slot.get('n') or 0)}")
    return "; ".join(bits)


def run(*, date: str = "all", limit: int = 0, fetch: bool = False,
        asof: dt.date | None = None, write: bool = True) -> dict:
    asof = asof or dt.date.today()
    inv = inventory()
    raw, raw_meta = load_all_sources(date)
    unique = dedupe_titles(raw)
    if limit and limit > 0:
        unique = unique[: int(limit)]
    print(f"[jev_backtest] unique={len(unique)} raw={raw_meta.get('n_raw')}",
          flush=True)
    results = evaluate_articles(unique, asof=asof)
    booked = [
        r for r in results
        if r.get("usable") and r.get("entities")
    ]
    print(f"[jev_backtest] kept={sum(1 for r in results if r['decision']=='keep')} "
          f"booked={len(booked)}", flush=True)
    if booked:
        graded = grade_results(booked, fetch=fetch)
        by_title = {str(r.get("title") or ""): r.get("performance") for r in graded}
        for r in results:
            taped = by_title.get(str(r.get("title") or ""))
            if taped:
                r["performance"] = taped
    tape = performance_rollup(results)
    ledger = build_ledger(results)
    votes = vote_hit_rates(results)
    reasons = Counter(r.get("reason") or "" for r in results)
    classes = Counter(
        r.get("event_class") or ""
        for r in results if r.get("decision") == "keep" and r.get("event_class")
    )
    payload = {
        "book": "jev_unique_title",
        "date": date,
        "limit": limit,
        "lane": "none",
        "clock": "published_at → next RTH (not parse time)",
        "assignment": "jev hop-0/1/2 code book; Finviz listed names only",
        "inventory": inv,
        "raw_meta": raw_meta,
        "n_unique": len(unique),
        "n_keep": sum(1 for r in results if r.get("decision") == "keep"),
        "n_drop": sum(1 for r in results if r.get("decision") == "drop"),
        "n_booked": len(booked),
        "n_signed_votes": sum(len(r.get("entities") or []) for r in booked),
        "reasons": dict(reasons.most_common(24)),
        "classes": dict(classes.most_common(20)),
        "tape": {
            "n_1d": tape.get("n_1d"),
            "hit_1d": tape.get("hit_1d"),
            "hit_rate_1d": tape.get("hit_rate_1d"),
            "n_20d": tape.get("n_20d"),
            "hit_20d": tape.get("hit_20d"),
            "hit_rate_20d": tape.get("hit_rate_20d"),
            "missing_tape": tape.get("missing_tape"),
        },
        "votes": votes,
        "ledger": {
            "counts": ledger.get("counts"),
            "hit_rates": ledger.get("hit_rates"),
            "n_name_groups": ledger.get("n_name_groups"),
            "top_converge": ledger.get("top_converge"),
            "rule": ledger.get("rule"),
        },
        "note": (
            "X = n_bull, Y = n_bear on one ticker in one entry session. "
            "Z is trading sessions after the next RTH open after published_at. "
            "No invented ticker. No Lane. keep.json unwired. Parquet tape only."
        ),
    }
    if write:
        RATES.parent.mkdir(parents=True, exist_ok=True)
        RATES.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        SCOREBOARD.parent.mkdir(parents=True, exist_ok=True)
        SCOREBOARD.write_text("\n".join(_markdown(payload)) + "\n", encoding="utf-8")
        print("→", SCOREBOARD, RATES, flush=True)
    print(json.dumps({
        "unique": payload["n_unique"],
        "keep": payload["n_keep"],
        "booked": payload["n_booked"],
        "named_1d": votes["named"]["0-1d"],
        "ledger_groups": ledger.get("n_name_groups"),
        "converge": (ledger.get("counts") or {}).get("converge"),
        "singleton": (ledger.get("counts") or {}).get("singleton"),
    }, indent=2), flush=True)
    return payload


def _markdown(payload: dict) -> list[str]:
    tape = payload.get("tape") or {}
    votes = payload.get("votes") or {}
    ledger = payload.get("ledger") or {}
    counts = ledger.get("counts") or {}
    rates = ledger.get("hit_rates") or {}
    window = ((payload.get("inventory") or {}).get("window") or {})
    lines = [
        "# Jev book backtest",
        "",
        "Every unique title, Jev hop-0/1/2 code path, Finviz-listed sides only. "
        "Clock is **published_at** (next RTH), not parse time. "
        "X / Y = n_bull / n_bear on one ticker in one entry session. "
        "Z = trading sessions after that entry open.",
        "",
        "## Scoreboard",
        "",
        f"- unique titles: **{payload.get('n_unique')}** "
        f"(raw={((payload.get('raw_meta') or {}).get('n_raw'))})",
        f"- hop-0 keep / drop: **{payload.get('n_keep')}** / "
        f"**{payload.get('n_drop')}**",
        f"- booked (impulse + listed up/down): **{payload.get('n_booked')}** "
        f"votes={payload.get('n_signed_votes')}",
        f"- named tape 0-1d: {_pct(tape.get('hit_1d') or 0, tape.get('n_1d') or 0)}",
        f"- named tape 1-4w: {_pct(tape.get('hit_20d') or 0, tape.get('n_20d') or 0)}",
        "",
        "## Per-vote hit (article × ticker, publication clock)",
        "",
        f"- named: {_rate_line(votes, 'named')}",
        f"- substitute / peer: {_rate_line(votes, 'substitute')}",
        f"- all booked: {_rate_line(votes, 'all')}",
        "",
        "## Ledger X / Y (same ticker, same entry session)",
        "",
        f"- name groups: {ledger.get('n_name_groups')}  "
        f"singleton={counts.get('singleton')}  "
        f"converge={counts.get('converge')}  "
        f"clash={counts.get('clash')}",
        f"- singleton: {_rate_line(rates, 'singleton')}",
        f"- converge (X≥2, Y=0 or Y≥2, X=0): {_rate_line(rates, 'converge')}",
        f"- clash (X≥1 and Y≥1, net side): {_rate_line(rates, 'clash')}",
        "",
        ledger.get("rule") or "",
        "",
        "## Window",
        "",
        f"- earliest on disk: {window.get('earliest_on_disk')}. "
        f"earliest parse: {window.get('earliest_parse')}. "
        f"latest: {window.get('latest')}.",
        "",
        payload.get("note") or "",
        "",
        "## Top converge (X vs Y)",
        "",
        "| date | ticker | X up | Y down | net | 0-1d | 5d | titles |",
        "|---|---|---:|---:|---:|---:|---:|---|",
    ]
    for g in (ledger.get("top_converge") or [])[:20]:
        titles = "; ".join((g.get("titles") or [])[:2]).replace("|", "/")
        lines.append(
            f"| {g.get('date')} | {g.get('ticker')} | {g.get('n_bull')} | "
            f"{g.get('n_bear')} | {g.get('net')} | {g.get('ret_1d')} | "
            f"{g.get('ret_5d')} | {titles[:120]} |"
        )
    lines += [
        "",
        "## Hop-0 reasons (top)",
        "",
    ]
    for reason, n in (payload.get("reasons") or {}).items():
        lines.append(f"- {reason}: {n}")
    lines += [
        "",
        "## Hop-1 classes on keeps (top)",
        "",
    ]
    for ev, n in (payload.get("classes") or {}).items():
        lines.append(f"- {ev}: {n}")
    lines += [
        "",
        "No Lane. No invented ticker. `keep.json` unwired.",
        "",
    ]
    return lines


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Jev hop-2 unique-title backtest")
    p.add_argument("--date", default="all")
    p.add_argument("--limit", type=int, default=0,
                   help="Cap unique titles (0 = all)")
    p.add_argument("--fetch", action="store_true",
                   help="yfinance fill for missing names (off by default)")
    p.add_argument("--no-write", action="store_true")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    run(
        date=args.date,
        limit=args.limit,
        fetch=args.fetch,
        write=not args.no_write,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
