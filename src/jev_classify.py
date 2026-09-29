"""Hop-1 classify: Jev names q5 + family + event_class + sign.

Hop-0 already kept the title. This hop only emits the locked PR #301
JSON (src/news_impact/schema.py). It does not pick tickers, polarity,
or winners. It does not call Lane. keep.json stays unwired.

  hop-0 keep → one Jev pack (q5 / family / class_* / sign)
            → {event_class, sign, q5, constraint, split, split_facts, why}

Code prior is news_impact.classify.classify_text. Live Jev overrides
when the answers land on the closed enum. discard / regime here means
"no tradeable mechanism," not "trash title."
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from .jev_gate import (
    GROUND,
    _load_json,
    api_key,
    jev_post,
    make_state,
    parse_answers,
)
from .news_impact.classify import classify_text
from .news_impact.schema import (
    EVENT_CLASSES,
    Q5_STATUS,
    SIGNS,
    Classification,
    family_of,
)

FAMILIES = (
    "quantity",
    "permission",
    "structure",
    "print",
    "firm",
    "blast",
    "flow",
    "time",
)

# One fallback per family when Jev names the family but misses the class.
FAMILY_DEFAULT = {
    "quantity": "demand",
    "permission": "gate",
    "structure": "market_structure",
    "print": "print_vs_priced",
    "firm": "corporate_action_mna",
    "blast": "blast_ops",
    "flow": "flow_index",
    "time": "statement_public",
}

# Short locked meanings. A new class needs a new loser AND winner set —
# do not invent a 53rd line here.
CLASS_CRITERIA = {
    "capacity": "Supply node add|destroy (fab, mine, plant, scarce tool)",
    "demand": "New orders / bookings / preorders for a thing that still exists",
    "input_cost": "Who pays the input vs who sells it (fuel, freight, copper)",
    "inventory_print": "EIA / crude / inventory build or draw vs who was positioned",
    "channel_stock": "Distributor / channel fill or destock, not an EIA print",
    "reserve_revision": "Proven / booked reserve number revised",
    "gate": "Became legal to sell or build (FDA approval, permit, award)",
    "trial_readout": "Phase data; probability of a future gate, not approval",
    "ip_ruling": "Court or PTAB ruling on a patent / IP right",
    "market_structure": "Who collects old venue / broker / listing rent vs new rail",
    "tax_fiscal": "Tax, tariff schedule, or fiscal rule that rewrites a cash-flow",
    "price_cap": "Regulated price / rate case / cap",
    "subsidy": "Named subsidy, credit, or grant that changes a cash-flow",
    "breakup_remedy": "Forced split / structural antitrust remedy",
    "standard_mandate": "CAFE / DMA / standard that every name in the set must meet",
    "access_control": "Who may sell or stand in the room (export curb, entity list)",
    "sanction_lift": "A prior access curb is lifted",
    "fx_translation": "FX translation of booked earnings, not a new demand shock",
    "policy_personnel": "Named chair / secretary / governor seat changes the rulebook",
    "print_vs_priced": "One name vs its number (beat / miss EPS or revenue)",
    "guidance": "Outlook / guidance cut, raise, or reaffirm vs the prior range",
    "preannounce": "Warning or preannounce before the scheduled print",
    "factor_impulse": "Macro number or FOMC / CPI / NFP / tariff that reprices a factor",
    "peer_spill": "Named print; cousin moves via a shared factor",
    "corporate_action_mna": "Cash / perimeter of the firm (acquire, merger, sale)",
    "corporate_action_spinoff": "Spin, split-off, or tracking stock",
    "dilution": "New paper (offering, ATM, convert)",
    "capital_return": "Buyback or cash dividend declared",
    "credit_funding": "New credit line, bond, or funding package",
    "distress_restruct": "Default, Chapter 11, or distressed recap",
    "integrity": "Restatement, auditor resign, delayed 10-K/10-Q, short report",
    "key_person": "CEO / founder death, ouster, or sudden seat change",
    "insider_flow": "Form 4 / CEO sale — weak signal",
    "activist_campaign": "13D, proxy fight, nominated directors — no cash moved",
    "strategic_review": "Strategic review / exploring alternatives",
    "regulatory_probe": "SEC / DOJ opens a probe; nothing filed as a finding",
    "deal_review": "Already-announced deal under review, not a new M&A",
    "sovereign_credit": "Sovereign / SIFI rating or default print",
    "listing_flow": "IPO, index add, or forced listing paper",
    "lockup_expiry": "Named lockup expiry",
    "flow_index": "Index rebalance / inclusion flow",
    "flow_mechanical": "Mechanical buy/sell (option pin, expiry, rebal)",
    "flow_forced_liq": "Forced liquidation / margin / CTA de-risk",
    "blast_legal": "Filed complaint / named defendants (not a speech)",
    "blast_ops": "Ops disruption — plant, airport, fleet, unpaid TSA",
    "blast_cyber": "Cyberattack, ransomware, or material breach",
    "product_harm": "Recall, contamination, grounding of a product",
    "labor_stop": "Strike, walkout, dock / port stoppage",
    "labor_organize": "Organize / unionize drive, not a stoppage",
    "cat_weather": "Named storm / fire / quake with a listed pipe",
    "regime_state": "Weather reprint — constraint already in the tape",
    "regime_break": "Verified change in the physical or legal constraint",
    "statement_public": "Words, not a binding constraint (speech, essay, 'mulls')",
    "rumor": "Unconfirmed report with no instrument",
    "discard": "No constraint — newsletter, stock-of-the-day, analyst PT",
}

QUESTIONS: dict = {
    "q5": {
        "type": "choice",
        "instructions": (
            "Q5 first. What kind of fact is this title? "
            "Do not pick a listed name or a book."
        ),
        "criteria": {
            "impulse": (
                "New dated fact that changes a constraint today: a print, "
                "signed rule, filed complaint, FDA approval, named deal, "
                "or a first strike / accept / reject / seize verb."
            ),
            "regime": (
                "Weather reprint. The constraint is already in the tape. "
                "Hormuz-today, gold-on-Fed, class-action wrap of an old MDL, "
                "stock-market-today recap of a known move."
            ),
            "regime_break": (
                "Verified change in the physical or legal constraint: "
                "ceasefire + hulls move, EO actually signed, FDA approval "
                "that opens a gate, a closed strait reopens."
            ),
        },
    },
    "family": {
        "type": "choice",
        "instructions": (
            "Which family owns the mechanism? Themes (AI, geopolitics, "
            "China, ESG) are not families. Second order is a role, not a family."
        ),
        "criteria": {
            "quantity": (
                "Capacity, demand, input cost, inventory, channel stock, "
                "or reserve revision"
            ),
            "permission": "Gate open/shut, trial readout, or IP ruling",
            "structure": (
                "Venue / tax / cap / subsidy / mandate / access / sanction "
                "/ breakup / FX translation / policy personnel"
            ),
            "print": (
                "Beat/miss, guidance, preannounce, peer spill, or a macro "
                "factor impulse (FOMC, CPI, NFP, tariff)"
            ),
            "firm": (
                "The issuer's own cash, paper, or control: M&A, dilution, "
                "buyback, credit, distress, integrity, key person, activist"
            ),
            "blast": (
                "Harm set: legal filing, ops hit, cyber, product harm, "
                "labor stop, organize, catastrophe"
            ),
            "flow": "Forced paper: IPO, lockup, index, mechanical, forced liq",
            "time": (
                "Regime weather, regime break, public statement, rumor, "
                "or discard (no constraint)"
            ),
        },
    },
    "class_quantity": {
        "type": "choice",
        "instructions": (
            "If family is quantity, pick the event_class. "
            "If family is not quantity, pick demand."
        ),
        "criteria": {
            ev: CLASS_CRITERIA[ev]
            for ev in (
                "capacity", "demand", "input_cost", "inventory_print",
                "channel_stock", "reserve_revision",
            )
        },
    },
    "class_permission": {
        "type": "choice",
        "instructions": (
            "If family is permission, pick the event_class. "
            "If family is not permission, pick gate."
        ),
        "criteria": {
            ev: CLASS_CRITERIA[ev]
            for ev in ("gate", "trial_readout", "ip_ruling")
        },
    },
    "class_structure": {
        "type": "choice",
        "instructions": (
            "If family is structure, pick the event_class. "
            "If family is not structure, pick market_structure."
        ),
        "criteria": {
            ev: CLASS_CRITERIA[ev]
            for ev in (
                "market_structure", "tax_fiscal", "price_cap", "subsidy",
                "breakup_remedy", "standard_mandate", "access_control",
                "sanction_lift", "fx_translation", "policy_personnel",
            )
        },
    },
    "class_print": {
        "type": "choice",
        "instructions": (
            "If family is print, pick the event_class. "
            "If family is not print, pick print_vs_priced."
        ),
        "criteria": {
            ev: CLASS_CRITERIA[ev]
            for ev in (
                "print_vs_priced", "guidance", "preannounce",
                "factor_impulse", "peer_spill",
            )
        },
    },
    "class_firm": {
        "type": "choice",
        "instructions": (
            "If family is firm, pick the event_class. "
            "If family is not firm, pick corporate_action_mna."
        ),
        "criteria": {
            ev: CLASS_CRITERIA[ev]
            for ev in (
                "corporate_action_mna", "corporate_action_spinoff", "dilution",
                "capital_return", "credit_funding", "distress_restruct",
                "integrity", "key_person", "insider_flow", "activist_campaign",
                "strategic_review", "regulatory_probe", "deal_review",
                "sovereign_credit",
            )
        },
    },
    "class_blast": {
        "type": "choice",
        "instructions": (
            "If family is blast, pick the event_class. "
            "If family is not blast, pick blast_ops."
        ),
        "criteria": {
            ev: CLASS_CRITERIA[ev]
            for ev in (
                "blast_legal", "blast_ops", "blast_cyber", "product_harm",
                "labor_stop", "labor_organize", "cat_weather",
            )
        },
    },
    "class_flow": {
        "type": "choice",
        "instructions": (
            "If family is flow, pick the event_class. "
            "If family is not flow, pick flow_index."
        ),
        "criteria": {
            ev: CLASS_CRITERIA[ev]
            for ev in (
                "listing_flow", "lockup_expiry", "flow_index",
                "flow_mechanical", "flow_forced_liq",
            )
        },
    },
    "class_time": {
        "type": "choice",
        "instructions": (
            "If family is time, pick the event_class. "
            "If family is not time, pick statement_public. "
            "No constraint → discard. Weather reprint → regime_state. "
            "Verified constraint change → regime_break."
        ),
        "criteria": {
            ev: CLASS_CRITERIA[ev]
            for ev in (
                "regime_state", "regime_break", "statement_public",
                "rumor", "discard",
            )
        },
    },
    "sign": {
        "type": "choice",
        "instructions": (
            "Closed sign only. none if the title does not name a direction. "
            "Do not invent a long/short call."
        ),
        "criteria": {
            "add": "Capacity / supply added",
            "destroy": "Capacity / supply destroyed",
            "up": "Demand, cost, or print up",
            "down": "Demand, cost, or print down",
            "open": "Gate / access opened",
            "shut": "Gate / access shut",
            "tighten": "Access or rule tightened",
            "lift": "Access curb or constraint lifted",
            "cut": "Guidance / rate / subsidy cut",
            "raise": "Guidance / rate / subsidy raised",
            "none": "No signed direction in the title",
        },
    },
}

FORBIDDEN_HOP1_BITS = (
    "ticker expansion",
    "bullish",
    "bearish",
    "polarity",
    "winner",
    "winners",
)

CLASS_QUESTION = {f"class_{fam}": fam for fam in FAMILIES}


def classes_by_family() -> dict[str, tuple[str, ...]]:
    bags: dict[str, list[str]] = defaultdict(list)
    for ev in EVENT_CLASSES:
        bags[family_of(ev)].append(ev)
    return {fam: tuple(bags.get(fam) or ()) for fam in FAMILIES}


def questions_are_hop1(questions: dict = QUESTIONS) -> None:
    """Hop-1 must ask the locked taxonomy and must not pick a ticker."""
    blob = json.dumps(questions).lower()
    for bit in FORBIDDEN_HOP1_BITS:
        if bit in blob:
            raise AssertionError(f"hop-1 pack must not ask {bit}")
    required = {"q5", "family", "sign"} | set(CLASS_QUESTION)
    missing = required - set(questions)
    if missing:
        raise AssertionError(f"hop-1 pack missing {sorted(missing)}")
    q5 = set((questions["q5"].get("criteria") or {}))
    if q5 != set(Q5_STATUS):
        raise AssertionError(f"hop-1 q5 drifted: {sorted(q5)}")
    fam = set((questions["family"].get("criteria") or {}))
    if fam != set(FAMILIES):
        raise AssertionError(f"hop-1 family drifted: {sorted(fam)}")
    by_fam = classes_by_family()
    for key, fam_name in CLASS_QUESTION.items():
        got = set((questions[key].get("criteria") or {}))
        expect = set(by_fam[fam_name])
        if got != expect:
            raise AssertionError(
                f"hop-1 {key} drifted extra={sorted(got - expect)} "
                f"missing={sorted(expect - got)}"
            )
    named = set()
    for key in CLASS_QUESTION:
        named.update((questions[key].get("criteria") or {}))
    if named != set(EVENT_CLASSES):
        raise AssertionError(
            f"hop-1 classes drifted extra={sorted(named - set(EVENT_CLASSES))} "
            f"missing={sorted(set(EVENT_CLASSES) - named)}"
        )


def _clean_sign(raw) -> str | None:
    sign = str(raw or "").strip().lower()
    if sign in {"", "none", "null", "nil"}:
        return None
    return sign if sign in SIGNS else None


def class_reason(event_class: str, q5: str, sign: str | None = None) -> str:
    ev = str(event_class or "").strip()
    logic = str(q5 or "").strip()
    if not ev:
        return ""
    bits = [ev, logic] if logic else [ev]
    if sign:
        bits.append(str(sign))
    return "|".join(bits)


def fields_from_class(cls: Classification, *, source: str) -> dict:
    ev = cls.event_class if cls.event_class in EVENT_CLASSES else "discard"
    q5 = cls.q5 if cls.q5 in Q5_STATUS else "regime"
    sign = cls.sign if cls.sign in SIGNS else None
    why = str(cls.why or "").strip() or f"hop-1 {source} {ev}/{q5}"
    constraint = str(cls.constraint or "").strip()
    return {
        "event_class": ev,
        "sign": sign,
        "q5": q5,
        "constraint": constraint,
        "split": bool(cls.split),
        "split_facts": list(cls.split_facts or []),
        "why": why,
        "family": family_of(ev),
        "class_reason": class_reason(ev, q5, sign),
        "class_source": source,
    }


def empty_class_fields() -> dict:
    return {
        "event_class": "",
        "sign": None,
        "q5": "",
        "constraint": "",
        "split": False,
        "split_facts": [],
        "why": "",
        "family": "",
        "class_reason": "",
        "class_source": "",
    }


def classification_json(row: dict) -> dict:
    """Lane classify hop shape. No tickers. No winners."""
    sign = row.get("sign")
    if sign not in SIGNS:
        sign = None
    ev = str(row.get("event_class") or "")
    return {
        "event_class": ev if ev in EVENT_CLASSES else "",
        "sign": sign,
        "q5": str(row.get("q5") or "") if row.get("q5") in Q5_STATUS else "",
        "constraint": str(row.get("constraint") or ""),
        "split": bool(row.get("split") or False),
        "split_facts": list(row.get("split_facts") or []),
        "why": str(row.get("why") or ""),
    }


def _constraint_line(event_class: str, title: str) -> str:
    one = " ".join((title or "").split())[:160]
    return f"{event_class}: {one}" if one else event_class


def decide_classify(title: str, answers: dict | None) -> Classification:
    """Pure hop-1 decide. Invalid answers fall through to family defaults."""
    answers = answers or {}
    q5 = str(answers.get("q5") or "").strip()
    if q5 not in Q5_STATUS:
        q5 = "impulse"
    family = str(answers.get("family") or "").strip()
    if family not in FAMILIES:
        family = "time"
    raw_class = str(answers.get(f"class_{family}") or "").strip()
    if raw_class in EVENT_CLASSES and family_of(raw_class) == family:
        event_class = raw_class
    elif q5 == "regime_break" and family == "time":
        event_class = "regime_break"
    elif q5 == "regime" and family == "time":
        event_class = "regime_state"
    else:
        event_class = FAMILY_DEFAULT[family]
    sign = _clean_sign(answers.get("sign"))
    return Classification(
        event_class=event_class,
        sign=sign,
        q5=q5,
        constraint=_constraint_line(event_class, title),
        why=f"jev hop-1 {family}/{event_class}/{q5}",
        family=family_of(event_class),
    )


def code_classify(title: str) -> Classification:
    return classify_text(title or "")


def classify_keep(row: dict, answers: dict | None = None) -> dict:
    """Jev answers win when they land on the enum; else the code prior."""
    title = row.get("title") or ""
    prior = code_classify(title)
    if not answers:
        return fields_from_class(prior, source="code")
    got = decide_classify(title, answers)
    if got.event_class in EVENT_CLASSES and got.q5 in Q5_STATUS:
        return fields_from_class(got, source="jev")
    return fields_from_class(prior, source="code")


def apply_code_classify(decided: list[dict]) -> list[dict]:
    """Stamp hop-1 fields on keeps. Drops stay filter-reasons only."""
    for row in decided:
        if row.get("decision") != "keep":
            row.update(empty_class_fields())
            continue
        if row.get("event_class") in EVENT_CLASSES:
            continue
        row.update(classify_keep(row, row.get("_classify_answers")))
    return decided


def jev_classify_many(rows: list[dict], key: str, workers: int = 24,
                      poster=None) -> list[tuple[dict, dict | None, str]]:
    poster = poster or jev_post
    out: list[tuple[dict, dict | None, str]] = []
    if not rows:
        return out

    def one(row: dict):
        payload = poster(make_state(row), QUESTIONS, key)
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


def apply_jev_classify(decided: list[dict], *, key: str, workers: int = 24,
                       poster=None) -> list[dict]:
    """Second Jev pack on hop-0 keeps only. Invalid answers keep the code prior."""
    keeps = [row for row in decided if row.get("decision") == "keep"]
    if not keeps:
        return decided
    key = key or api_key()
    if not key:
        raise RuntimeError("JEV_API_KEY / TYPESAFE_API_KEY is empty")
    for row, answers, model in jev_classify_many(keeps, key, workers, poster):
        row["_classify_answers"] = answers or {}
        if model:
            row["_classify_model"] = model
        row.update(classify_keep(row, answers))
    return decided


def load_classify_gold(path: Path | None = None) -> dict:
    blob = _load_json(path or GROUND / "jev_classify_gold.json")
    if not isinstance(blob, dict):
        return {"items": []}
    return blob


def gold_rows() -> list[dict]:
    rows = []
    for it in (load_classify_gold().get("items") or []):
        rows.append({
            "id": it.get("id") or "",
            "title": it.get("title") or "",
            "source": it.get("source") or "",
            "answers": {
                k: (it.get("answers") or {}).get(k)
                for k in QUESTIONS
                if k in (it.get("answers") or {})
            },
            "expect": it.get("expect") or {},
        })
    return rows


def gold_check(rows: list[dict] | None = None) -> dict:
    rows = rows if rows is not None else gold_rows()
    results = []
    fails = []
    for item in rows:
        got = classify_keep(item, item.get("answers"))
        expect = item.get("expect") or {}
        ok = True
        note = ""
        for key in ("event_class", "q5", "family"):
            want = expect.get(key)
            if want and got.get(key) != want:
                ok = False
                note = f"expect {key}={want}, got {got.get(key)}"
                break
        want_sign = expect.get("sign", "__skip__")
        if want_sign != "__skip__" and got.get("sign") != (
            None if want_sign in {None, "", "none"} else want_sign
        ):
            ok = False
            note = note or f"expect sign={want_sign}, got {got.get('sign')}"
        rec = {
            "id": item.get("id"),
            "ok": ok,
            "note": note,
            "title": item.get("title") or "",
            "got": classification_json(got),
            "expect": expect,
        }
        results.append(rec)
        if not ok:
            fails.append(rec)
    return {
        "n": len(results),
        "n_fail": len(fails),
        "ok": not fails,
        "rows": results,
    }


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Jev hop-1 classify")
    p.add_argument("--gold", action="store_true")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    questions_are_hop1()
    if args.gold:
        report = gold_check()
        print(
            f"[jev_classify] gold ok={report['ok']} "
            f"fail={report['n_fail']}/{report['n']}"
        )
        for row in report["rows"]:
            mark = "ok" if row["ok"] else "FAIL"
            print(
                f"  [{mark}] {row['id']} "
                f"got={row['got'].get('event_class')}/{row['got'].get('q5')} "
                f"{(row.get('title') or '')[:72]}"
            )
        return 0 if report["ok"] else 2
    print("[jev_classify] hop-1 pack ready. --gold checks the fixture.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
