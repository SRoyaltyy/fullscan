"""Family analyzers: one winner/loser test per family. Signs flip arrows."""
from __future__ import annotations

import re

from .axioms import retrieve
from .classify import extract_named
from .grok_automations import is_macro_only
from .horizons import apply_class_horizons
from .macro import macro_entities
from .schema import Classification, Entity, empty_entity_ok

AIRLINES = [
    ("AAL", "American Airlines"),
    ("DAL", "Delta"),
    ("UAL", "United"),
    ("LUV", "Southwest"),
    ("ALK", "Alaska Air"),
]
RENTAL = [("CAR", "Avis Budget"), ("HTZ", "Hertz")]
AI_LABS_PRIVATE = [
    ("OpenAI", None),
    ("Anthropic", None),
    ("xAI", None),
]
DRAM_INCUMBENTS = [("MU", "Micron"), ("005930.KS", "Samsung"), ("000660.KS", "SK Hynix")]


def _ent(**kw) -> Entity:
    return Entity(**kw)


def analyze(art: dict, cls: Classification, pack: dict | None = None) -> list[Entity]:
    """Dispatch to the family for this class. Never run another family's test."""
    pack = pack or {}
    if cls.q5 == "regime" or cls.event_class in {"discard", "regime_state", "rumor"}:
        return []
    # 13-questions: factor basket only. Never fish a ticker from the Q.
    if is_macro_only(art=art):
        rows = macro_entities(_title(art), cls)
        apply_class_horizons(
            rows, cls.event_class, str(art.get("title") or ""), cls.sign,
        )
        return rows
    fn = {
        "blast": _blast,
        "structure": _structure,
        "quantity": _quantity,
        "permission": _permission,
        "print": _print,
        "firm": _firm,
        "flow": _flow,
        "time": _time,
    }.get(cls.family or "", _time)
    rows = fn(art, cls, pack)
    # Honesty: if the class is not weather/discard, name someone.
    if not rows and not empty_entity_ok(cls):
        named = extract_named(str(art.get("title") or ""), str(art.get("body") or ""))
        hint = str(art.get("ticker_hint") or "").strip().upper()
        if named:
            tick, name = named[0]
            rows = [_ent(
                name=name, ticker=tick, role="named",
                direction="not_determined", horizon="0-1d",
                if_unknown="pack incomplete", inferred=False,
            )]
        elif hint:
            rows = [_ent(
                name=str(art.get("company") or hint), ticker=hint, role="named",
                direction="not_determined", horizon="0-1d",
                if_unknown="finviz ticker map; pack incomplete", inferred=True,
            )]
        else:
            rows = [_ent(
                name="listed expression unknown", ticker=None, role="named",
                direction="not_determined", horizon="0-1d",
                tradeable_expression="none",
                if_unknown="no listed whole-company channel",
            )]
    apply_class_horizons(
        rows, cls.event_class, str(art.get("title") or ""), cls.sign,
    )
    return rows


def _title(art: dict) -> str:
    return f"{art.get('title') or ''}\n{art.get('body') or ''}"


def _blast(art: dict, cls: Classification, pack: dict) -> list[Entity]:
    text = _title(art)
    named = extract_named(art.get("title") or "", art.get("body") or "")
    out: list[Entity] = []
    if cls.event_class == "blast_ops" and re.search(
        r"(?i)(tsa|airport|travel weekend|unpaid)", text
    ):
        for tick, name in AIRLINES:
            out.append(_ent(
                name=name, ticker=tick, role="named", direction="down",
                horizon="0-1d", relation="primary", axiom_id="A_AIR_01",
                inferred=True,
            ))
        for tick, name in RENTAL:
            out.append(_ent(
                name=name, ticker=tick, role="substitute", direction="up",
                horizon="0-1d", relation="substitute", axiom_id="A_AIR_02",
                inferred=True,
            ))
        out.append(_ent(
            name="Energy / jet fuel", ticker="XLE", role="complement",
            direction="not_determined", horizon="0-1d",
            if_unknown="cancellations vs late departures not in title",
            axiom_id="A_AIR_03", inferred=True, unit_vs_parent="slice",
        ))
        return out
    if cls.event_class == "blast_legal":
        harm = [t for t, _ in named if t in {"GOOGL", "MSFT", "AMZN", "TSLA"}]
        # AI labs complaint: Google is the only clean listed defendant.
        if re.search(r"(?i)(anthropic|openai|xai|spacexai|gemini|claude|grok)", text):
            out.append(_ent(
                name="Alphabet", ticker="GOOGL", role="parent_of_named",
                named_unit="Google / Gemini / DeepMind",
                unit_vs_parent="slice", direction="down", horizon="0-1d",
                axiom_id="A_AT_02",
            ))
            out.append(_ent(
                name="Meta", ticker="META", role="unscathed_rival",
                direction="up", horizon="1-4w", basis="relative",
                stays_out=True, wins_either_outcome=True,
                relation="relative_winner", axiom_id="A_AT_03",
                inferred=True, unit_vs_parent="slice",
            ))
            out.append(_ent(
                name="NVIDIA", ticker="NVDA", role="arms_dealer",
                direction="not_determined", horizon="1-4w",
                if_unknown="IF compute capped THEN down; IF pact dies THEN up",
                stays_out=True, axiom_id="A_AT_04", inferred=True,
            ))
            for lab, tick in AI_LABS_PRIVATE:
                if re.search(lab, text, re.I):
                    out.append(_ent(
                        name=lab, ticker=tick, role="named",
                        direction="down", horizon="0-1d",
                        tradeable_expression="none", unit_vs_parent="whole",
                        axiom_id="A_AT_01",
                    ))
            return out
        for tick, name in named:
            out.append(_ent(
                name=name, ticker=tick, role="named",
                direction="down", horizon="0-1d",
            ))
        return out
    if cls.event_class == "blast_cyber":
        for tick, name in named:
            out.append(_ent(
                name=name, ticker=tick, role="named",
                direction="down", horizon="1-4w",
            ))
        # Do not auto-add PANW — spend must rise is unanswered.
        if named:
            out.append(_ent(
                name="security vendors", ticker=None, role="arms_dealer",
                direction="not_determined", horizon="1-4w",
                if_unknown="IF spend must rise THEN up",
                tradeable_expression="none", inferred=True,
            ))
        return out
    if cls.event_class in {"labor_stop", "product_harm", "cat_weather"}:
        for tick, name in named:
            out.append(_ent(
                name=name, ticker=tick, role="named",
                direction="down", horizon="0-1d",
            ))
        return out
    for tick, name in named:
        out.append(_ent(name=name, ticker=tick, role="named",
                        direction="down", horizon="0-1d"))
    return out


def _structure(art: dict, cls: Classification, pack: dict) -> list[Entity]:
    text = _title(art)
    out: list[Entity] = []
    if cls.event_class == "market_structure" and re.search(
        r"(?i)(tsv\b|exemptive|tokenized (securit|stock|nms|equit)|"
        r"innovation exemption)",
        text,
    ):
        out.extend([
            _ent(name="Coinbase", ticker="COIN", role="new_venue",
                 direction="up", horizon="1-6m", unit_vs_parent="slice",
                 axiom_id="A_TSV_01"),
            _ent(name="Robinhood", ticker="HOOD", role="new_venue",
                 direction="mixed", horizon="1-6m", unit_vs_parent="slice",
                 if_unknown="offshore product must be rebuilt to TSV rules",
                 axiom_id="A_TSV_05"),
            _ent(name="Circle", ticker="CRCL", role="arms_dealer",
                 direction="up", horizon="1-6m", axiom_id="A_TSV_01"),
            _ent(name="Nasdaq", ticker="NDAQ", role="incumbent_intermediary",
                 direction="mixed", horizon="6m+",
                 if_unknown="incumbent_response already_in_motion",
                 axiom_id="A_TSV_06"),
            _ent(name="ICE / NYSE", ticker="ICE", role="incumbent_intermediary",
                 direction="mixed", horizon="6m+", axiom_id="A_TSV_06"),
            _ent(name="Charles Schwab", ticker="SCHW", role="incumbent_intermediary",
                 direction="down", horizon="6m+"),
            _ent(name="Securitize", ticker="SECZ", role="new_venue",
                 direction="up", horizon="1-6m"),
        ])
        return out
    if cls.event_class == "access_control" and cls.q5 != "regime":
        named = extract_named(art.get("title") or "", art.get("body") or "")
        sign_blocked = "down" if cls.sign != "lift" else "up"
        sign_sub = "up" if cls.sign != "lift" else "down"
        for tick, name in named:
            out.append(_ent(name=name, ticker=tick, role="named",
                            direction=sign_blocked, horizon="1-4w"))
        if re.search(r"(?i)(china|export|gpu|accelerator)", text):
            out.append(_ent(
                name="NVIDIA", ticker="NVDA", role="named",
                direction="mixed", horizon="1-4w",
                if_unknown="blocked SKU vs allowed substitute SKU",
            ))
        if not out:
            out.append(_ent(
                name="blocked exporter", ticker=None, role="named",
                direction=sign_blocked, horizon="1-4w",
                tradeable_expression="none",
            ))
        _ = sign_sub
        return out
    named = extract_named(art.get("title") or "", art.get("body") or "")
    for tick, name in named:
        out.append(_ent(name=name, ticker=tick, role="named",
                        direction="mixed", horizon="1-6m"))
    return out


def _quantity(art: dict, cls: Classification, pack: dict) -> list[Entity]:
    text = _title(art)
    named = extract_named(art.get("title") or "", art.get("body") or "")
    out: list[Entity] = []
    if cls.event_class == "input_cost":
        payers_down = cls.sign != "down"
        if re.search(r"(?i)(jet fuel|airline|fuel costs)", text):
            for tick, name in AIRLINES:
                out.append(_ent(
                    name=name, ticker=tick, role="named",
                    direction="down" if payers_down else "up",
                    horizon="1-4w", axiom_id="A_AIR_03", inferred=True,
                ))
            out.append(_ent(
                name="Energy / refiners", ticker="XLE", role="supplier",
                direction="up" if payers_down else "down",
                horizon="1-4w", axiom_id="A_AIR_03", inferred=True,
            ))
            return out
        for tick, name in named:
            out.append(_ent(
                name=name, ticker=tick, role="named",
                direction="down" if payers_down else "up",
                horizon="1-4w",
            ))
        return out
    if cls.event_class == "capacity":
        if re.search(r"(?i)(cxmt|changxin|mass production)", text):
            out.append(_ent(
                name="CXMT", ticker=None, role="named",
                direction="up", horizon="1-6m",
                tradeable_expression="none", axiom_id="A_CAP_01",
            ))
            for tick, name in DRAM_INCUMBENTS[:1]:
                out.append(_ent(
                    name=name, ticker=tick, role="competitor",
                    direction="down", horizon="1-6m",
                    if_unknown="IF product competes and controls do not block scale",
                    axiom_id="A_CAP_01", inferred=True,
                ))
            return out
        if re.search(r"(?i)(asml|euv|sold out)", text):
            out.append(_ent(
                name="ASML", ticker="ASML", role="named",
                direction="up", horizon="1-6m", axiom_id="A_CAP_02",
            ))
            out.append(_ent(
                name="TSMC / foundry buyers", ticker="TSM", role="customer",
                direction="mixed", horizon="1-6m",
                if_unknown="allocation already priced vs capex cut",
                inferred=True,
            ))
            return out
        for tick, name in named:
            out.append(_ent(
                name=name, ticker=tick, role="named",
                direction="up" if cls.sign != "destroy" else "down",
                horizon="1-6m",
            ))
        return out
    if cls.event_class == "inventory_print":
        # Build → producers down; draw → producers up
        build = bool(re.search(r"(?i)build", text))
        for tick, name in named:
            out.append(_ent(
                name=name, ticker=tick, role="named",
                direction="down" if build else "up",
                horizon="0-1d",
            ))
        if not any(t == "DVN" for t, _ in named) and re.search(r"(?i)(crude|oil|dvn)", text):
            out.append(_ent(
                name="Devon", ticker="DVN", role="named",
                direction="down" if build else "up",
                horizon="0-1d", inferred=True,
            ))
        return out
    if cls.event_class == "demand":
        for tick, name in named:
            out.append(_ent(
                name=name, ticker=tick, role="named",
                direction="up" if cls.sign != "down" else "down",
                horizon="1-4w",
            ))
        return out
    for tick, name in named:
        out.append(_ent(name=name, ticker=tick, role="named",
                        direction="mixed", horizon="1-4w"))
    return out


def _permission(art: dict, cls: Classification, pack: dict) -> list[Entity]:
    named = extract_named(art.get("title") or "", art.get("body") or "")
    direction = "up" if cls.sign != "shut" else "down"
    if cls.event_class == "trial_readout":
        direction = "mixed"
    out = []
    for tick, name in named:
        out.append(_ent(
            name=name, ticker=tick, role="named",
            direction=direction, horizon="1-6m" if cls.event_class == "gate" else "1-4w",
            unit_vs_parent="slice" if tick in {"IBM", "GD"} else "whole",
        ))
    return out


def _print(art: dict, cls: Classification, pack: dict) -> list[Entity]:
    text = _title(art)
    named = extract_named(art.get("title") or "", art.get("body") or "")
    out: list[Entity] = []
    if cls.event_class == "peer_spill":
        for tick, name in named:
            out.append(_ent(
                name=name, ticker=tick, role="named" if tick != "DDOG" else "competitor",
                direction="up", horizon="0-1d",
                axiom_id="peer_shared_factor" if tick == "DDOG" else None,
                inferred=tick == "DDOG",
            ))
        return out
    if cls.event_class == "factor_impulse":
        # Ticker-less macro trades the factor basket, never a name in the lede.
        return macro_entities(text, cls)
    direction = "not_determined"
    if cls.event_class == "guidance":
        # Hygiene: reaffirm → not_determined; raise+miss EPS → mixed.
        if cls.split and cls.sign is None:
            direction = "mixed"
        elif cls.sign == "cut":
            direction = "down"
        elif cls.sign == "raise":
            direction = "up"
        else:
            direction = "not_determined"
    elif re.search(r"(?i)(miss|plunge|drop|soft)", text):
        direction = "down"
    elif re.search(r"(?i)(beat|raise|record)", text):
        direction = "up"
    for tick, name in named:
        hz = "1-4w" if cls.event_class in {"guidance", "print_vs_priced"} else "0-1d"
        out.append(_ent(
            name=name, ticker=tick, role="named",
            direction=direction, horizon=hz,
        ))
    return out


def _firm(art: dict, cls: Classification, pack: dict) -> list[Entity]:
    text = _title(art)
    named = extract_named(art.get("title") or "", art.get("body") or "")
    out: list[Entity] = []
    if cls.event_class == "capital_return":
        # Routine dividend: name the issuer, do not invent a sector trade.
        direction = "not_determined"
        if re.search(r"(?i)(cut|suspend|slash).{0,20}dividend", text):
            direction = "down"
        elif re.search(r"(?i)(buyback|repurchase|raises? .{0,20}dividend)", text):
            direction = "up"
        for tick, name in named:
            out.append(_ent(
                name=name, ticker=tick, role="named",
                direction=direction, horizon="0-1d",
                if_unknown="routine vs policy change unanswered" if direction == "not_determined" else None,
            ))
        if not out:
            out.append(_ent(
                name="issuer / Financials payout", ticker=None, role="named",
                direction="not_determined", horizon="0-1d",
                tradeable_expression="none",
                if_unknown="routine dividend — no flip vs last print",
            ))
        return out
    if cls.event_class == "integrity":
        for tick, name in named:
            # Never map Nasdaq-the-exchange onto a filing notice.
            if tick == "NDAQ" and re.search(r"(?i)(notice|10-q|10-k|filing delay)", text):
                continue
            out.append(_ent(
                name=name, ticker=tick, role="named",
                direction="not_determined", horizon="0-1d",
                axiom_id="A_INS_01",
                if_unknown="governance print; squeeze ≠ Technology",
            ))
        if not out:
            out.append(_ent(
                name="named issuer", ticker=None, role="named",
                direction="not_determined", horizon="0-1d",
                axiom_id="A_INS_01", tradeable_expression="none",
            ))
        return out
    if cls.event_class == "insider_flow":
        for tick, name in named:
            out.append(_ent(
                name=name, ticker=tick, role="named",
                direction="not_determined", horizon="0-1d",
                if_unknown="weak signal — not guidance",
            ))
        return out
    direction = "mixed"
    if cls.event_class == "dilution":
        direction = "down"
    elif cls.event_class == "corporate_action_mna":
        direction = "mixed"
    for tick, name in named:
        out.append(_ent(
            name=name, ticker=tick, role="named",
            direction=direction, horizon="1-4w",
        ))
    return out


def _flow(art: dict, cls: Classification, pack: dict) -> list[Entity]:
    named = extract_named(art.get("title") or "", art.get("body") or "")
    direction = "up" if cls.event_class == "listing_flow" else "down"
    return [
        _ent(name=name, ticker=tick, role="named",
             direction=direction, horizon="0-1d")
        for tick, name in named
    ]


def _time(art: dict, cls: Classification, pack: dict) -> list[Entity]:
    if cls.event_class == "regime_break":
        text = _title(art)
        if re.search(r"(?i)hormuz|strait", text):
            return [
                _ent(name="crude / Energy", ticker="XLE", role="named",
                     direction="down", horizon="0-1d", inferred=True,
                     axiom_id="A_Q5_01"),
                _ent(name="US passenger airlines", ticker="AAL", role="named",
                     direction="up", horizon="1-4w", inferred=True),
            ]
    if cls.event_class == "statement_public":
        return []
    return []


def axioms_for(entities: list[Entity]) -> list[dict]:
    ids = [e.axiom_id for e in entities if e.axiom_id]
    return retrieve(ids)
