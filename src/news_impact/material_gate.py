"""Decide which official rows are worth a Lane call.

Useful means a reported public decision whose title states an economic
mechanism: a sanction, a duty, a law or presentment, an order, a named
penalty, an approval, or a macro print. A referral, a meeting, a speech,
a case caption, and a local airspace or safety-zone rule are not.
"""
from __future__ import annotations

import re

DROP = re.compile(
    r"(?ix)"
    r"(information\ collection|comment\ request|sunshine\ act|"
    r"notice\ of\ (?:closed\ |open\ |public\ )?meeting|"
    r"advisory\ committee|request\ for\ nomination|"
    r"privacy\ act|senior\ executive\ service|"
    r"referred\ to\ the\ committee|read\ twice\ and\ referred|"
    r"remarks\b|exchange\ with\ reporters|"
    r"digest\ of\ other\ white\ house|checklist\ of\ white\ house|"
    r"\(IH\)|\(IS\)|\(RH\)|"
    r"safety\ zone|special\ local\ regulation|class\ e\ airspace|"
    r"standard\ instrument\ approach)"
)

KEEP = re.compile(
    r"(?ix)"
    r"(became\ public\ law|presented\ to\ president|"
    r"executive\ order|memorandum\ on|"
    r"sanction|general\ licen[cs]e|"
    r"antidumping|countervailing|less\ than\ fair\ value|"
    r"weekly\ claims|civil\ money\ penalty|"
    r"early\ termination|"
    r"\bfda\ approves|expanded\ indication|"
    r"significant\ new\ use|"
    r"foreign-trade\ zone.{0,80}authorization|"
    r"license\ to\ drill|"
    r"imposes?\s+\$\s?\d)"
)


def gate(row: dict) -> dict:
    """Return keep, reason. Title and stage only. No body, no ticker."""
    title = str(row.get("title") or "")
    stage = str(row.get("document_stage") or "")
    if not title.strip():
        return {"keep": False, "reason": "empty"}
    if DROP.search(title):
        return {"keep": False, "reason": "procedure_speech_or_local_rule"}
    if stage == "proposal_published":
        return {"keep": False, "reason": "proposal"}
    if KEEP.search(title) or stage in {"law_published"}:
        return {"keep": True, "reason": "operative_mechanism"}
    if stage == "final_rule_published" and re.search(
        r"(?i)(tax|tariff|duty|subsid|penalty|bank|energy|oil|drug)", title
    ):
        return {"keep": True, "reason": "final_rule_with_mechanism"}
    return {"keep": False, "reason": "no_stated_mechanism"}


def lane_article(row: dict) -> dict:
    """Official metadata in the shape Lane already reads."""
    summary = str(row.get("summary") or row.get("body") or "")
    return {
        "title": str(row.get("title") or ""),
        "body": summary[:1200],
        "url": str(row.get("url") or ""),
        "published_at": str(row.get("published_at") or ""),
        "known_at": str(row.get("published_at") or ""),
        "source": str(row.get("issuer") or row.get("source") or ""),
        "document_stage": str(row.get("document_stage") or ""),
        "harvest_source": "official_primary",
    }
