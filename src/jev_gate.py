"""Hop-0 news net: code first, one Jev pack on leftovers.

Sorting, not classifying. Jev never picks a 52-class, polarity, or
ticker expansion. Spend tokens only after dups / junk-shapes / source
deny / chokepoint reprint-clock have thrown trash away. Dated prints,
deals, and policy titles are kept in code so Jev cannot hide them as
low_material. Hop-0 filters trash and holds what is worth classifying.

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
from .jev_bits import BIT_QUESTIONS, decide as bits_decide

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

# Hop-0 keeps anything Lane should classify. Trash still drops.
# These fire before Jev so a low material score cannot hide a print,
# a named deal, or a dated policy move.
_CLASS_PRINT = re.compile(
    r"(?i)(?:"
    r"\beia\b"
    r"|\bwasde\b"
    r"|\bnonfarm payrolls\b"
    r"|\b(?:jobless|initial) claims\b"
    r"|\bholds? rates?\b"
    r"|\bfederal reserve holds\b"
    r"|\bfed holds rates\b"
    r"|\bpetroleum and natural gas storage\b"
    r"|\brig count\b"
    r")"
)
_CLASS_DEAL = re.compile(
    r"(?i)(?:"
    r"\b(?:merger|acquisition|acquires?)\b"
    r"|\bpays \$?\d"
    r"|\bsold to\b"
    r"|\bbuyout\b"
    r"|\bproduct launch\b"
    r"|\bintroduces\b"
    r"|\boutlook raise\b"
    r"|\bstrong results\b.{0,32}\boutlook\b"
    r"|\b(?:targets?|record|files?|launches?|prices its|priced)\b.{0,40}\bipo\b"
    r"|\bipo\b.{0,40}(?:targets?|files?|priced|prices its)"
    r")"
)
_CLASS_POLICY = re.compile(
    r"(?i)(?:"
    r"\b(?:cftc|fda|ustr)\b"
    r"|\btariffs?\b"
    r"|\bsecondary sanctions?\b"
    r"|\bcontinuing resolution\b"
    r"|\bhouse clears\b"
    r"|\bshutdown risk\b"
    r"|\bsocial security\b.{0,48}\b(?:tax|cut|change|plan|reform|benefits?|mulls)\b"
    r"|\b(?:tax|cut|change|plan|reform|mulls)\b.{0,48}\bsocial security\b"
    r"|\bcredit default swaps?\b"
    r"|\bchinese evs\b"
    r"|\bcyclospora\b"
    r"|\b(?:401k|401\(k\)|ira)\b.{0,56}\b(?:funds?|invest|rule|proposal|allow)\b"
    r"|\b(?:saudi|pakistan|turkey|turkiye).{0,48}\bpact\b"
    r"|\bafter fid\b"
    r"|\bfinal investment decision\b"
    r")"
)
_CLASS_OIL = re.compile(
    r"(?i)(?:azeri light|oil price|brent|wti|crude).{0,48}(?:\d|%|bcf|mb\b)"
)
_CLASS_FX = re.compile(
    r"(?i)\byen\b.{0,48}\b(?:dollar|rate hike|fed|u\.s\.)\b"
)
_CLASS_AI = re.compile(
    r"(?i)(?:"
    r"\b(?:openai|anthropic)\b.{0,56}"
    r"\b(?:introduces?|launches?|files?|targets?|warns?|product)\b"
    r"|"
    r"\b(?:introduces?|launches?|files?|targets?|prices?)\b.{0,56}"
    r"\b(?:openai|anthropic)\b"
    r")"
)
_CLASS_FED_VOICE = re.compile(
    r"(?i)\b(?:fed|federal reserve|cook|powell|warsh)\b.{0,56}"
    r"\b(?:warns?|holds rates)\b"
)
_CLASS_CHOKE_NEW = re.compile(
    r"(?i)\b(?:rejects?|accepts?|seizes?|strikes?|hits?|escalat\w*)\b.{0,48}"
    r"\b(?:peace|iran|hormuz|iraq|jordan)\b"
    r"|\b(?:peace|iran|hormuz|iraq|jordan)\b.{0,48}"
    r"\b(?:rejects?|accepts?|seizes?|strikes?|hits?|escalat\w*)\b"
    r"|\b(?:war spreading|is war spreading)\b"
)
_CLASS_HOME = re.compile(
    r"(?i)\bbond market\b.{0,40}\bhomebuild"
    r"|\bcharges against\b"
    r"|\bdismisses some charges\b"
)
CODE_KEEP = frozenset({
    "code_print", "code_deal", "code_policy", "code_oil",
    "code_fx", "code_ai", "code_fed", "code_choke", "code_home",
    "classifiable",
})

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

# Legacy questions kept below as _LEGACY_QUESTIONS. Live pack is BIT_QUESTIONS.
QUESTIONS = BIT_QUESTIONS
_LEGACY_QUESTIONS: dict = {
    "is_opinion": {
        "type": "noul",
        "instructions": (
            "Is this ONLY commentary, a column, or a recap, with no "
            "first-class fact, print, deal, or policy in the title?"
        ),
        "criteria": {
            "true": (
                "Column, recap, or 'what it means' AND no dated print, "
                "named IPO or M&A, regulator / Fed / House / CFTC / FDA "
                "action, pathogen-recall scare, or peace accept/reject. "
                "'Best move if the market crashes', celebrity, theme park. "
                "Forecast / odds / live tape that only name-drops PCE, IPO, "
                "or a rate-hike. Investor-rebuke or 'could still gain' "
                "columns with no warn / hold / print. Leaders 'expected' "
                "an outcome with no accept/reject/deal/escalation. Social "
                "Security explainer with no tax/plan/reform. Reclaim / IPO "
                "price of an already-public name. Open-bell '5 things' "
                "listicle with no named print/deal/policy. 'Tech stocks "
                "today' OpenAI / AI recap or 'earnings provide next test' "
                "with no dated print. 'Should you' whale / billionaire "
                "column with no named 13F / insider lot. 'Reportedly wants' "
                "a buyer with no signed deal. Law-firm tombstone guiding a "
                "foreign IPO. 'How rate hikes impact' explainer with no "
                "new hike. 'Stock market today' recap of already-printed "
                "earnings. Gold-fell-amid a rate-hike threat (not a "
                "yield-driven crash). Fed-hike odds / 'sees rising odds' "
                "tape that only name-drops PCE."
            ),
            "false": (
                "Title contains a first-class fact even inside a listicle "
                "or column frame: Fed / EIA / House / CFTC / FDA action, "
                "named IPO or M&A or IPO delay, pathogen + consumers/recalls, "
                "Fed officer sees/says/warns inflation or oil, Fed chair / "
                "Jackson Hole remarks, jobs / rents as Fed-path "
                "context, peace accept/reject, dated official print or hold, "
                "Conference Board / consumer-confidence print, court dismiss "
                "or listed-name litigation, housing or credit freeze "
                "that if true changes a listed sector prior, EV / tariff / "
                "Social Security policy, sitting-president foreign-policy "
                "action (gambit, backing, corridor) or scheduled summit/"
                "dinner with a counterpart, US/China national "
                "industrial policy (AI / chips / capital markets), named-firm "
                "mass layoff, cost-cut / deleveraging, or Chapter 11, "
                "G7/UK national fiscal budget or tax rise, "
                "EU/G7 trade or Buy-European procurement rules, G10 CB "
                "hawkish/dovish stance, national exchange trading debut, "
                "outbreak + vaccination drive, food-import supply-shock, "
                "tanker/newbuild cost inversion, yield-driven gold crash, "
                "or CDS affecting a listed sector, sitting-president "
                "secondary sanctions, Hormuz / Iran escalation or other "
                "named-strait status-change, named-fund / 13F / billionaire "
                "flow into a listed ticker (not a 'should you' column), "
                "named-officer insider sale, named-broker downgrade plus "
                "price-target cut, a national pump-price cut, sitting-"
                "president 401k / IRA investment-rule proposal, named-"
                "ticker results plus outlook raise, named product launch, "
                "named LNG / project FID, named-state pact / alliance, or "
                "Iran hits / war-spreading strikes."
            ),
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
            "reacted, with no new underlying event? True: 'stock market "
            "today' recap of already-printed earnings, 'tech stocks gain "
            "on' IPO optimism / aftereffects, gold-fell-amid a rate-hike "
            "threat, or 'how rate hikes impact' with no new hike. False: "
            "'things to know before the open', a dated Fed hold / EIA / "
            "CR, or a first-class print that has not happened yet."
        ),
    },
    "geo": {
        "type": "choice",
        "instructions": "Where is the event, for US-listed market relevance?",
        "criteria": {
            "core": (
                "US, China, EU/EZ, Japan, Korea, India, a G10 central "
                "bank / regulator, G10 FX (yen + dollar + Fed), a numbered "
                "oil-price print (Brent, WTI, Azeri Light, EIA storage, "
                "basin rigs), G7/UK national fiscal budget or tax rise "
                "(not a local coin / council story), sitting-US-president "
                "action in Korea or on an India-Europe corridor, a "
                "named steel / freight trade-flow disruption, Conference "
                "Board / consumer-confidence print, national exchange "
                "trading debut, EU/G7 public-procurement or Buy-European "
                "trade rules, yield-driven gold / Treasury move, tanker / "
                "newbuild vessel-cost inversion, or a food-import "
                "supply-shock, a named-strait escalation / status-change, "
                "or a national fuel pump-price cut, a sitting-president "
                "401k / IRA investment-rule proposal, a US named product "
                "launch, or a named-state pact involving a US ally"
            ),
            "chokepoint": (
                "Hormuz, Red Sea / Bab el-Mandeb, Suez, Panama, Taiwan "
                "Strait, Malacca, or a named tanker/port there, including "
                "an escalation or other status-change at that strait"
            ),
            "other": (
                "Anywhere else with no US / G10 / oil-print / G7-fiscal "
                "hook and no named M&A or named-firm layoff. Yemen / "
                "Palestine / UK-local politics stay other. Leaders "
                "'expected' an Iran/geo outcome with no accept/reject/"
                "deal/escalation stays other and is not a fact. A named "
                "strait escalation is chokepoint, not other. Africa outbreak "
                "with no vaccination / pharma-contract hook stays other. "
                "A law-firm tombstone guiding a foreign IPO is not the "
                "debut. Gold-fell-amid a rate-hike threat is tape, not "
                "a yield-driven crash."
            ),
        },
    },
    "actor_power": {
        "type": "choice",
        "instructions": "Who is the main actor in the title?",
        "criteria": {
            "state_head": (
                "President, PM, monarch, cabinet minister, central banker, "
                "US House or Senate acting as a body"
            ),
            "regulator": (
                "SEC, FDA, Fed, FOMC, CFTC, NHTSA, NBS, ECB, PBOC, EIA, "
                "a named Fed governor, Conference Board official print, "
                "Fed chair / named governor sees/says, or a court with "
                "a binding order"
            ),
            "listed_firm": (
                "Named company that has or plausibly has a US ticker, "
                "including a named acquirer, a major AI lab with a "
                "dated product or IPO, or a named firm with an IPO "
                "delay, mass layoff, cost-cut / deleveraging, Chapter 11 "
                "/ bankruptcy, authorized buyback, listed-name going "
                "to court, a named-fund / 13F / billionaire flow into "
                "that ticker, a named-officer insider sale, a "
                "named-broker downgrade plus price-target cut, a named "
                "product launch, or named-ticker results plus outlook raise"
            ),
            "infrastructure": (
                "Port, strait, exchange, grid, pipeline operator, a "
                "G10 FX pair / oil benchmark, a named steel / "
                "freight trade-flow channel, a national exchange "
                "trading debut, an industry-association network launch, "
                "or a tanker / newbuild channel, national oil "
                "marketers executing a posted pump-price cut, or a "
                "named LNG / project FID"
            ),
            "crowd": (
                "Protesters, activists, tourists, unnamed residents. "
                "Not FX tape + Fed hike bets, not a named firm or agency. "
                "Not a Conference Board / consumer-confidence print. "
                "Not an industry-wide association or state banking "
                "association launching a network. Not national fuel "
                "marketers posting a pump-price cut."
            ),
            "other_person": "Private individual with no state or corporate seat",
        },
    },
    "action_material": {
        "type": "noul",
        "instructions": (
            "If this headline is true, could it change prices, policy, "
            "cash flows, or the prior for a US-listed name or a core "
            "macro / oil / FX factor this week? Arrest of protesters = no. "
            "Arrest of a sitting US president = yes. A final CAFE rule = yes. "
            "A local rally = no. A weekly EIA print, Fed hold, House CR, "
            "named M&A, regulator exploring rules, major-AI product or IPO, "
            "court dismiss of a listed name, pathogen scare that can trigger "
            "FDA/recalls, EV / tariff / Social Security policy, or 'mulls' "
            "a named EV / tariff / Social Security instrument = yes. "
            "A housing or credit freeze that if true changes a listed "
            "sector prior = yes. US/China national industrial policy "
            "(AI / chips / capital markets) = yes. Named steel / freight "
            "trade-flow disruption = yes. IPO delay or listing limbo = yes. "
            "Sitting-president foreign-policy action (gambit, backing a "
            "corridor) or a scheduled summit/dinner with a counterpart = yes. "
            "Named-firm mass layoff, cost-cut / deleveraging, Chapter 11, "
            "or authorized buyback = yes. G7/UK national "
            "fiscal budget or tax rise = yes. Jobs / rents as Fed-path "
            "context = yes. Conference Board / consumer-confidence print = yes. "
            "Industry-wide association launching a network = yes. "
            "EU/G7 trade or Buy-European procurement rules = yes. "
            "National exchange trading debut = yes. Fed officer "
            "sees/says/warns inflation or oil, or Fed chair / Jackson Hole "
            "remarks = yes. G10 CB hawkish/dovish stance = yes. "
            "Outbreak + vaccination drive = yes. Food-import supply-shock = yes. "
            "Tanker/newbuild cost inversion = yes. Yield-driven gold crash "
            "(yields + gold plunge) = yes. Sitting-president secondary "
            "sanctions = yes. Hormuz / Iran escalation or named-strait "
            "status-change = yes. Named-fund / 13F / billionaire flow "
            "into a listed ticker = yes. Named-officer insider sale = yes. "
            "Named-broker downgrade plus price-target cut = yes. National "
            "pump-price cut = yes. Sitting-president 401k / IRA "
            "investment-rule proposal = yes. Named-ticker results plus "
            "outlook raise = yes. Named product launch = yes. Named LNG / "
            "project FID = yes. Named-state pact / alliance = yes. Iran "
            "hits / war-spreading strikes = yes. Leaders 'expected' an outcome "
            "with no accept/reject/deal/escalation = no. Social Security explainer "
            "with no tax/plan/reform = no. Reclaim / IPO price of an "
            "already-public name = no. Celebrity, theme park, 'best move if crash', "
            "gold-tumbles forecast tape, earnings-look-right, or forecast / odds / "
            "live tape that only name-drops a print = no. 'Tech stocks today' "
            "OpenAI recap or 'earnings provide next test' = no. 'Should you' "
            "whale column with no named 13F / insider lot = no. 'Reportedly "
            "wants' a buyer with no signed deal = no. Law-firm tombstone "
            "guiding a foreign IPO = no. 'How rate hikes impact' explainer "
            "with no new hike = no. 'Stock market today' recap of already-"
            "printed earnings = no. Gold-fell-amid a rate-hike threat "
            "(not a yield-driven crash) = no."
        ),
        "criteria": {
            "true": (
                "If true, could change prices, policy, cash flows, or the "
                "prior for a US-listed name or a core macro / oil / FX / "
                "steel-freight factor this week: dated print / hold / CR, "
                "Conference Board / consumer-confidence print, named M&A or "
                "IPO delay, regulator exploring rules, major-AI product, "
                "court dismiss or listed-name litigation, pathogen-recall "
                "scare or outbreak + vaccination drive, EV / tariff / "
                "Social Security / national industrial policy, "
                "sitting-president foreign-policy action or scheduled "
                "summit/dinner, named-firm mass layoff / cost-cut / "
                "Chapter 11 / authorized buyback, G7/UK fiscal budget or "
                "tax rise, EU/G7 trade or Buy-European procurement rules, "
                "G10 CB hawkish/dovish, national exchange trading debut, "
                "food-import supply-shock, tanker/newbuild cost inversion, "
                "yield-driven gold crash, jobs / rents as Fed-path context, "
                "Fed officer sees/says/warns or Fed chair / Jackson Hole, "
                "sitting-president secondary sanctions, Hormuz / Iran "
                "escalation, named-fund / 13F / billionaire flow, "
                "named-officer insider sale, named-broker downgrade plus "
                "price-target cut, a national pump-price cut, sitting-"
                "president 401k / IRA investment-rule proposal, named-"
                "ticker results plus outlook raise, named product launch, "
                "named LNG / project FID, named-state pact / alliance, or "
                "Iran hits / war-spreading strikes."
            ),
            "false": (
                "Protester arrest, local rally, celebrity, theme park, "
                "'best move if crash', gold-tumbles forecast tape, "
                "earnings-look-right, forecast / odds / live tape that "
                "only name-drops a print, leaders 'expected' an outcome "
                "with no accept/reject/deal, Social Security explainer "
                "with no tax/plan/reform, reclaim / IPO price of an "
                "already-public name, open-bell '5 things' listicle "
                "with no named print/deal/policy, 'tech stocks today' "
                "OpenAI recap or 'earnings provide next test', 'should you' "
                "whale column with no named 13F / insider lot, 'reportedly "
                "wants' a buyer with no signed deal, law-firm tombstone "
                "guiding a foreign IPO, 'how rate hikes impact' explainer "
                "with no new hike, 'stock market today' recap of already-"
                "printed earnings, or gold-fell-amid a rate-hike threat."
            ),
        },
    },
    "new_instrument": {
        "type": "noul",
        "instructions": (
            "Is there a signed rule, print, halt, filing, seizure, dated "
            "official decision, weekly official figure, rate hold/hike/cut, "
            "continuing resolution through a date, regulator exploring or "
            "endorsing a rule, advisory-panel endorse, named dollar deal, "
            "court dismiss, IPO delay, or a dated G7/UK budget / tax rise "
            "— not a speech, protest, or rumor? "
            "EIA storage, Fed holds rates, House CR, CFTC explores rules, "
            "FDA panel endorse = yes. 'Mulls' a named policy instrument "
            "(EV, tariff, Social Security, CAFE) = yes. Conference Board / "
            "consumer-confidence print = yes. Chapter 11 / bankruptcy "
            "filing = yes. Authorized dollar buyback = yes. National "
            "exchange trading debut = yes. G10 CB hawkish/dovish = yes. "
            "Published EU/G7 trade / procurement rule = yes. Official "
            "vaccination drive = yes. Secondary sanctions = yes. Named "
            "13F / fund stake or Form-4 insider lot = yes. Named-broker "
            "rating plus price-target change = yes. Sitting-president "
            "401k / IRA investment-rule proposal = yes. Named-ticker "
            "results plus outlook raise = yes. Named product launch = yes. "
            "Named LNG / project FID = yes. Named-state pact / alliance "
            "= yes. 'Mulls' with no named instrument, a speech, a protest, "
            "a rumor, 'reportedly wants' a buyer, a law-firm tombstone, "
            "forecast / odds / live tape, 'sees rising odds' of a Fed "
            "hike, 'how rate hikes impact', 'stock market today' recap, "
            "gold-fell-amid a threat, leaders 'expected' an outcome, "
            "IPO-price reclaim, or a Social Security explainer = no."
        ),
    },
    "reprint_weather": {
        "type": "noul",
        "instructions": (
            "Is this a rerun of a months-old situation with no new closure, "
            "ceasefire, first strike, escalation / de-escalation / "
            "status-change, or new accept/reject/seize verb?"
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


def classifiable_reason(title: str) -> str:
    """Hop-0 keep: filter trash, hold anything worth a hop-1 look.

    Empty string means Jev still has to score it. A hit means the title
    is not trash and must not die as low_material, opinion, punct, or source.
    """
    title = title or ""
    if _CLASS_PRINT.search(title):
        return "code_print"
    if _CLASS_OIL.search(title):
        return "code_oil"
    if _CLASS_POLICY.search(title):
        return "code_policy"
    if _CLASS_DEAL.search(title):
        return "code_deal"
    if _CLASS_AI.search(title):
        return "code_ai"
    if _CLASS_FED_VOICE.search(title):
        return "code_fed"
    if _CLASS_CHOKE_NEW.search(title):
        return "code_choke"
    if _CLASS_FX.search(title):
        return "code_fx"
    if _CLASS_HOME.search(title):
        return "code_home"
    return ""


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
    title = row.get("title") or ""
    keep = classifiable_reason(title)
    if keep:
        row["_code_keep"] = keep
        clock = reprint_weather_code(title, asof, state)
        row["_clock"] = clock
        return ""
    if source_denied(row.get("source") or "", row.get("url") or ""):
        return "source"
    if punct_trash(title):
        return "punct"
    if junk_shape_hit(title, junk_rx):
        return "junk_shape"
    clock = reprint_weather_code(title, asof, state)
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
    return bits_decide(row, answers)
    # legacy body unused
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

    keep = str(row.get("_code_keep") or "") or classifiable_reason(title)
    # Tip-sheet / junk-shape is hop-0 trash. A Fool title that name-drops
    # earnings must not ride classifiable_reason back to KEEP.
    if code in {"source", "junk_shape"}:
        return pack("drop", code)
    if code in CODE_KEEP or (keep and code in {"", "punct"}):
        return pack("keep", code if code in CODE_KEEP else keep)
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
    # Closed-list agency/newness may lift instrument later. Opinion
    # veto uses Jev's own scores so a Fed-hinted column cannot sneak
    # past (session 409 Treasury/Bitcoin leftover). Session 1803
    # reaction uses the same raw instrument, not the boost.
    jev_instrument = instrument
    # Concrete action only. Material ("could this matter this week")
    # is what let week-ahead / wallet / odds columns through.
    jev_fact = instrument >= INSTRUMENT_KEEP
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

    # A first-class fact in the title is not trash just because the
    # frame is a column or listicle. Session 409: opinion dropped
    # Cook / Anthropic IPO / Cyclospora / housing before material ran.
    fact_keep = jev_fact
    if opinion >= TRASH_NOUL and not fact_keep:
        return pack("drop", "opinion")
    # Session 1803: recaps ("stock market today", gold-fell-amid,
    # how-rate-hikes-impact) scored material and skipped reaction.
    # A dated instrument still keeps. Opinion skip stays (409).
    if reaction >= TRASH_NOUL and jev_instrument < INSTRUMENT_KEEP:
        return pack("drop", "reaction")
    if tabloid >= TRASH_NOUL and actor not in POWERFUL:
        return pack("drop", "tabloid")
    # Session 1703: Conference Board prints and industry-body launches
    # sat in actor=crowd. Skipping the crowd drop was not enough —
    # core still requires a powerful actor for instrument-only keeps.
    # A first-class fact is not crowd-trash.
    if actor == "crowd":
        if fact_keep:
            return pack("keep", "crowd_fact")
        if material < CROWD_DROP:
            return pack("drop", "crowd")
    if actor == "state_head" and hints["head_action"]:
        return pack("keep", "state_head_action")

    # Session 1640: steel trade-flow / G7 fiscal / named-firm layoff
    # scored material but sat in geo=other with no powerful actor, so
    # other_powerful never fired. A first-class fact is not other-trash.
    if geo == "other":
        if fact_keep:
            return pack(
                "keep",
                "other_powerful" if actor in POWERFUL else "other_fact",
            )
        return pack("drop", "geo_other")

    # Session 1732: Hormuz "escalation" is a status-change, not weather.
    # Skipping the reprint drop only helps if Jev already scored a fact.
    # Months-old "tensions persist" with low material still drops.
    if geo == "chokepoint":
        if reprint >= TRASH_NOUL and not clock.get("has_new_verb"):
            if fact_keep:
                return pack("keep", "choke_fact")
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
        elif row.get("_code_keep"):
            row["code_reason"] = row["_code_keep"]
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
