"""Large-batch first screens. No family test. No tickers. No ACTION.

Pass 1 — usability: kill tabloid / opinion / tape / bad source / no market hook.
Pass 2 — event_class + Q5 on KEEP rows only.
"""
from __future__ import annotations

from .prompts import ENUM, PROMPT_VERSION, fingerprint

USABILITY_SYSTEM = (
    "You are a trading-desk intake clerk. "
    "Score a BATCH of headlines for whether a market desk should read them. "
    "Return ONE JSON object. No markdown. No tickers. No event_class."
)

CLASSIFY_BATCH_SYSTEM = (
    "You classify a BATCH of headlines that already passed usability. "
    "Return ONE JSON object. No markdown. No tickers. No winners."
)

USABILITY_REASONS = (
    "tape | tabloid | opinion | rumor | bad_source | ir_empty | dividend_only | "
    "form4 | listicle | pt_only | weather_reprint | no_market_hook | keep"
)


def _lines(arts: list[dict]) -> str:
    rows = []
    for i, art in enumerate(arts):
        title = str(art.get("title") or "").replace("\n", " ")[:240]
        src = str(art.get("source") or art.get("harvest_source") or "")[:40]
        aid = str(art.get("article_id") or art.get("id") or i)
        rows.append(f"{i}\t{aid}\t{src}\t{title}")
    return "\n".join(rows)


def usability_batch_prompt(arts: list[dict]) -> str:
    text = (
        f"PROMPT_VERSION={PROMPT_VERSION}\n"
        "Pass 1 of 2. USABILITY only. Do not classify the event.\n\n"
        "KEEP if a listed market can plausibly move because of a dated fact "
        "in this headline (company print, regulator/government action, "
        "capacity, lawsuit with named defendants, gate/approval, "
        "signed order, named guidance change).\n\n"
        "KILL with exactly one reason:\n"
        "- tape: price reaction already in the tape "
        "(stock surges / plunges / outperforms / falls N%).\n"
        "- tabloid: gossip, celebrity, clickbait, no issuer or rule.\n"
        "- opinion: column, op-ed, 'should you buy', Cramer, listicle.\n"
        "- rumor: unconfirmed, 'sources say' with no document.\n"
        "- bad_source: known junk / social screenshot as the only source.\n"
        "- ir_empty: schedules a call, 'to announce earnings', no number.\n"
        "- dividend_only: routine dividend, no other fact.\n"
        "- form4: Form 4 / 144 / insider sale reprint.\n"
        "- listicle: N stocks to watch / PT hike with no print.\n"
        "- pt_only: analyst target, no smuggled print or guidance.\n"
        "- weather_reprint: Hormuz-still-closed, gold-on-Fed, old MDL wrap.\n"
        "- no_market_hook: real news that does not touch a listed book.\n"
        "- keep: usable.\n\n"
        "UNSURE only when the title is short and could hide a print. "
        "UNSURE goes to classify. KILL does not.\n\n"
        "Market relevance: a government action, a named issuer, a named "
        "product class with listed sponsors, a signed rule, or a locked "
        "macro print in the title. 'AI' or 'geopolitics' alone is KILL "
        "no_market_hook.\n\n"
        "One row per input index. Do not drop rows. Do not invent rows.\n\n"
        f"ROWS (index, id, source, title):\n{_lines(arts)}\n\n"
        "STRICT JSON:\n"
        '{"rows":[{"i":0,"id":"","verdict":"keep|kill|unsure",'
        f'"reason":"{USABILITY_REASONS}"]}}'
    )
    fingerprint(text, "usability_batch")
    return text


def classify_batch_prompt(arts: list[dict]) -> str:
    text = (
        f"PROMPT_VERSION={PROMPT_VERSION}\n"
        "Pass 2 of 2. These rows already passed usability. "
        "Pick Q5 then exactly one event_class.\n\n"
        "Q5: impulse | regime | regime_break.\n"
        "impulse = new dated fact that changes a constraint today.\n"
        "regime = weather reprint. Hormuz-today, gold-on-Fed, old MDL wrap.\n"
        "regime_break = verified change in the physical or legal constraint.\n\n"
        f"event_class enum:\n{ENUM}\n\n"
        "Rules:\n"
        "- No tickers. No winners. No essays.\n"
        "- Themes are not classes.\n"
        "- Second order is a role, not a class.\n"
        "- Analyst PT is discard unless it smuggles a print.\n"
        "- If no constraint, event_class=discard.\n"
        "- Do not dump a lawsuit or an FDA approval into regime_break.\n"
        "- Speech / considering / expected-to-hold is statement_public, "
        "not access_control and not regime_break.\n\n"
        "One row per input index. Do not drop rows.\n\n"
        f"ROWS (index, id, source, title):\n{_lines(arts)}\n\n"
        "STRICT JSON:\n"
        '{"rows":[{"i":0,"id":"","q5":"impulse|regime|regime_break",'
        '"event_class":"","sign":null,"constraint":"","split":false}]}'
    )
    fingerprint(text, "classify_batch")
    return text
