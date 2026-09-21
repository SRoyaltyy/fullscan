"""Deterministic event-class + Q5 router.

The classifier is the one-size piece: pick exactly one class, then the
family analyzer runs. Lane may confirm; it must not see 52 tests at once.
"""
from __future__ import annotations

import re
from typing import Iterable

from .schema import Classification, EVENT_CLASSES, family_of

_STOP_TICK = frozenset(
    "THE AND FOR ARE WAS NOT BUT YOU ALL CAN OUR CEO FDA SEC ETF IPO "
    "USD NYSE Nasdaq NASDAQ AI US UK EU Q1 Q2 Q3 Q4 EPS GAAP".split()
)

# $AAPL, (AAPL), "AAPL drop/plunge/surge"
_TICK_RE = re.compile(
    r"(?:\$([A-Z]{1,5})\b|\(([A-Z]{1,5})\)|"
    r"\b([A-Z]{1,5})\b(?=\s+(?:stock|shares?|plunge|drop|surge|jump|gain|fall|miss|beat)))"
)

NAME_TO_TICKER: list[tuple[re.Pattern, str, str]] = [
    (re.compile(r"(?i)\bavis|budget group|\bCAR\b"), "CAR", "Avis Budget"),
    (re.compile(r"(?i)\bhertz|\bHTZ\b"), "HTZ", "Hertz"),
    (re.compile(r"(?i)american airlines|\bAAL\b"), "AAL", "American Airlines"),
    (re.compile(r"(?i)\bdelta air|\bDAL\b"), "DAL", "Delta"),
    (re.compile(r"(?i)united airlines|\bUAL\b"), "UAL", "United"),
    (re.compile(r"(?i)southwest airlines|\bLUV\b"), "LUV", "Southwest"),
    (re.compile(r"(?i)\balaska air|\bALK\b"), "ALK", "Alaska Air"),
    (re.compile(r"(?i)\bwarner bros|discovery|\bWBD\b|\bCNN\b"), "WBD", "Warner Bros. Discovery"),
    (re.compile(r"(?i)\bcomcast|\bCMCSA\b|ms now|msnbc"), "CMCSA", "Comcast"),
    (re.compile(r"(?i)\bfox corp|\bFOXA\b|\bfox news\b"), "FOXA", "Fox"),
    (re.compile(r"(?i)\bdisney|\bDIS\b|\babc news\b"), "DIS", "Disney"),
    (re.compile(r"(?i)(\balphabet\b|\bGOOGL\b|google (deepmind|gemini)|"
                 r"\bdeepmind\b)"), "GOOGL", "Alphabet"),
    (re.compile(r"(?i)\bmeta platforms|\bMETA\b|\bllama\b"), "META", "Meta"),
    (re.compile(r"(?i)\bnvidia|\bNVDA\b"), "NVDA", "NVIDIA"),
    (re.compile(r"(?i)\bmicrosoft|\bMSFT\b"), "MSFT", "Microsoft"),
    (re.compile(r"(?i)\bamazon|\bAMZN\b"), "AMZN", "Amazon"),
    (re.compile(r"(?i)\bcoinbase|\bCOIN\b"), "COIN", "Coinbase"),
    (re.compile(r"(?i)\brobinhood|\bHOOD\b"), "HOOD", "Robinhood"),
    (re.compile(r"(?i)\bcircle|\bCRCL\b|\busdc\b"), "CRCL", "Circle"),
    (re.compile(r"(?i)(\bnasdaq,?\s+inc\b|\bNDAQ\b)"), "NDAQ", "Nasdaq"),
    (re.compile(r"(?i)(\bintercontinental exchange\b|\bNYSE\b)"), "ICE", "ICE"),
    (re.compile(r"(?i)charles schwab|\bSCHW\b"), "SCHW", "Schwab"),
    (re.compile(r"(?i)\basml\b"), "ASML", "ASML"),
    (re.compile(r"(?i)\bmicron\b"), "MU", "Micron"),
    (re.compile(r"(?i)boston scientific|\bBSX\b"), "BSX", "Boston Scientific"),
    (re.compile(r"(?i)\bamgen|\bAMGN\b"), "AMGN", "Amgen"),
    (re.compile(r"(?i)\bibm\b"), "IBM", "IBM"),
    (re.compile(r"(?i)general dynamics|\bGD\b"), "GD", "General Dynamics"),
    (re.compile(r"(?i)\bcummins|\bCMI\b"), "CMI", "Cummins"),
    (re.compile(r"(?i)\balcoa\b"), "AA", "Alcoa"),
    (re.compile(r"(?i)\bagilent\b"), "A", "Agilent"),
    (re.compile(r"(?i)atlantic american|\bAAME\b"), "AAME", "Atlantic American"),
    (re.compile(r"(?i)\bbank of america|\bBAC\b"), "BAC", "Bank of America"),
    (re.compile(r"(?i)\bdevon\b|\bDVN\b"), "DVN", "Devon"),
    (re.compile(r"(?i)\badobe\b|\bADBE\b"), "ADBE", "Adobe"),
    (re.compile(r"(?i)\bsnowflake\b|\bSNOW\b"), "SNOW", "Snowflake"),
    (re.compile(r"(?i)\bdatadog\b|\bDDOG\b"), "DDOG", "Datadog"),
    (re.compile(r"(?i)\bsecuritize\b|\bSECZ\b"), "SECZ", "Securitize"),
    (re.compile(r"(?i)\bbullish\b|\bBLSH\b"), "BLSH", "Bullish"),
    (re.compile(r"(?i)\bcxmt\b|changxin"), "CXMT", "CXMT"),
    (re.compile(r"(?i)elevance|\bELV\b"), "ELV", "Elevance"),
    (re.compile(r"(?i)\bbbva\b"), "BBVA", "BBVA"),
    (re.compile(r"(?i)(barclays plc|\bBCS\b|drive BCS\b)"), "BCS", "Barclays"),
    (re.compile(r"(?i)canadian natural|\bCNQ\b"), "CNQ", "Canadian Natural"),
    (re.compile(r"(?i)kinder morgan|\bKMI\b"), "KMI", "Kinder Morgan"),
    (re.compile(r"(?i)\blinde\b|\bLIN\b"), "LIN", "Linde"),
    (re.compile(r"(?i)arthur j\.? gallagher|\bAJG\b"), "AJG", "Arthur J. Gallagher"),
    (re.compile(r"(?i)cardinal health|\bCAH\b"), "CAH", "Cardinal Health"),
    (re.compile(r"(?i)\bfabrinet\b|\bamphenol\b|\bAPH\b"), "APH", "Amphenol"),
    (re.compile(r"(?i)\bfastenal\b"), "FAST", "Fastenal"),
    (re.compile(r"(?i)\bcenovus\b"), "CVE", "Cenovus"),
    (re.compile(r"(?i)\bcencora\b"), "COR", "Cencora"),
    (re.compile(r"(?i)\bdominion energy\b|\bdominion,?\b"), "D", "Dominion"),
    (re.compile(r"(?i)\bnext ?era\b|\bNEE\b"), "NEE", "NextEra"),
    (re.compile(r"(?i)\beni lifts|\beni \b"), "E", "Eni"),
    (re.compile(r"(?i)ing groep"), "ING", "ING"),
    (re.compile(r"(?i)bank of nova scotia|\bBNS\b"), "BNS", "Scotiabank"),
    (re.compile(r"(?i)canadian imperial|cibc"), "CM", "CIBC"),
    (re.compile(r"(?i)\bembecta\b|\bEMBC\b"), "EMBC", "Embecta"),
    (re.compile(r"(?i)\bsalesforce\b|\bCRM\b"), "CRM", "Salesforce"),
    (re.compile(r"(?i)abercrombie|\bANF\b"), "ANF", "Abercrombie"),
    (re.compile(r"(?i)\bduke energy\b|\bDUK\b"), "DUK", "Duke Energy"),
]


def _blob(title: str, body: str = "") -> str:
    return f"{title or ''}\n{body or ''}"


def extract_named(title: str, body: str = "") -> list[tuple[str, str]]:
    """Return [(ticker, name)] mentioned or mapped. No fishing."""
    text = _blob(title, body)
    found: dict[str, str] = {}
    for pat, tick, name in NAME_TO_TICKER:
        if pat.search(text):
            found[tick] = name
    for m in _TICK_RE.finditer(text):
        tick = next(g for g in m.groups() if g)
        if tick in _STOP_TICK or len(tick) < 2:
            continue
        found.setdefault(tick, tick)
    return [(t, n) for t, n in found.items()]


def _rule(event_class: str, pattern: str, sign: str | None, q5: str,
          why: str) -> tuple:
    return event_class, re.compile(pattern), sign, q5, why


# First match wins. More specific before generic.
# (event_class, compiled, sign, q5, why)
_RULES: list[tuple[str, re.Pattern, str | None, str, str]] = [
    # --- discard (tape / newsletter / routine IR) ---
    _rule("discard",
          r"(?i)(jim cramer|stock of the day|should you buy|is it too late to buy|"
          r"top stocks? to|top stock reports|zacks (upgrade|downgrade)|"
          r"price target (raised|cut|to \$)|why is .{0,50}(falling|rising|down \d))",
          None, "regime", "reaction/newsletter — no new constraint"),
    _rule("discard",
          r"(?i)(ticker symbol to change|will change its (nasdaq |nyse )?ticker|"
          r"schedules? .{0,40}(earnings release|conference call)|"
          r"earnings call (highlights|summary)$)",
          None, "regime", "routine IR / ticker change"),
    # --- Q5 weather reprints ---
    _rule("regime_state",
          r"(?i)((hormuz|strait of hormuz).{0,80}(still|remains|closed|high risk|"
          r"despite|capped|talks|hopes|optimism|elusive)|"
          r"(oil (prices? )?(tumble|slip|slide|fall).{0,40}hormuz)|"
          r"(hormuz.{0,40}(crisis|blockade|shipping)))",
          None, "regime", "Hormuz already the weather"),
    _rule("regime_state",
          r"(?i)(gold (price|prices)?.{0,60}(fed|rate cut|rate hike|warsh|cut bets)|"
          r"gold (surge|slide|slides|jumps|tumbles).{0,40}(fed|warsh|hike|cut))",
          None, "regime", "gold-on-Fed reaction reprint"),
    # --- regime break ---
    _rule("regime_break",
          r"(?i)((hormuz|strait).{0,80}(ceasefire|withdraw|reopen|traffic resumes|"
          r"both (sides|navies) leave|hulls)|"
          r"(ceasefire).{0,40}(hormuz|strait))",
          "lift", "regime_break", "verified change in the strait constraint"),
    # --- blast / harm ---
    _rule("blast_legal",
          r"(?i)(class[- ]action|antitrust lawsuit|sued |lawsuit.{0,40}(accus|"
          r"against|filed)|v\.\s+\w+.{0,20}(antitrust|conspira)|illegal agreement)",
          None, "impulse", "filed complaint / named defendants"),
    _rule("blast_cyber",
          r"(?i)(cyberattack|ransomware|data breach|hack(ed|ers)? (hit|will|"
          r"material))",
          None, "impulse", "ops/cyber hit on a named firm"),
    _rule("blast_ops",
          r"(?i)((tsa|airport).{0,40}(unpaid|chaos|shutdown|short-staff)|"
          r"government shutdown.{0,40}(airport|tsa|travel)|"
          r"(grounding|fleet grounded|irregular ops))",
          None, "impulse", "ops disruption — substitutes required"),
    _rule("labor_stop",
          r"(?i)(\bstrike\b|walkout|uaw |dock (strike|workers)|port (strike|shutdown))",
          None, "impulse", "labor stoppage"),
    _rule("product_harm",
          r"(?i)(recall|contamination|grounded 737|max grounding)",
          None, "impulse", "product harm / recall"),
    # --- structure / permission ---
    _rule("market_structure",
          r"(?i)(tokeniz|tokenised|tsv\b|exemptive relief|innovation exemption|"
          r"tokenized (securit|stock|nms|equit)|permissioned amm|"
          r"sec .{0,40}(exemption|order granting))",
          None, "impulse", "who collects old rent vs new rails"),
    _rule("access_control",
          r"(?i)(export (curb|control|ban|licen)|entity list|sanction(s|ed)? |"
          r"no-export|bans? .{0,30}(from the white house|press pool|cnn))",
          "tighten", "impulse", "who may sell / who may stand in the room"),
    _rule("gate",
          r"(?i)(fda (approval|approves|label)|chips act award|wins? \$\d|"
          r"contract (award|win)|spectrum (auction|award)|pipeline permit)",
          "open", "impulse", "binary permission / award"),
    _rule("trial_readout",
          r"(?i)(phase[- ]?[123]|trial (data|readout|results)|arros-1|pivotal study)",
          None, "impulse", "probability of a future gate, not approval"),
    # --- quantity ---
    _rule("capacity",
          r"(?i)(sold out .{0,20}(euv|capacity|2027)|mass production|"
          r"cxmt|changxin|new (fab|capacity) |nearly sold out)",
          "add", "impulse", "incremental supply / scarce tools"),
    _rule("inventory_print",
          r"(?i)((crude|oil|eia) inventory (build|draw)|surprise .{0,20}"
          r"(inventory|build|draw))",
          None, "impulse", "print vs who was positioned"),
    _rule("input_cost",
          r"(?i)(jet fuel|oil shock|fuel costs? .{0,20}(\$|lift|cut)|"
          r"copper tariff|input cost|freight rates? (soar|spike))",
          "up", "impulse", "who pays the input vs who sells it"),
    _rule("demand",
          r"(?i)(preorder|bookings? (surge|jump)|bess (deal|contract)|"
          r"iphone (preorder|demand)|sold-out concert)",
          "up", "impulse", "new orders for a thing that still exists"),
    # --- information vs strip ---
    _rule("guidance",
          r"(?i)((soft|cuts?|raises?|reaffirm).{0,30}(outlook|guidance)|"
          r"outlook .{0,20}(plunge|cut|raise)|no longer expects to meet)",
          "cut", "impulse", "guidance vs prior range"),
    _rule("print_vs_priced",
          r"(?i)(beats? estimates|misses? estimates|eps \$\d|revenue \$\d|"
          r"q[1-4] .{0,20}(beat|miss))",
          None, "impulse", "one name vs its number"),
    _rule("peer_spill",
          r"(?i)(supports \b[A-Z]{2,5}\b|peers? (rally|sell)|sympathy)",
          None, "impulse", "named print, cousin via shared factor"),
    _rule("price_cap",
          r"(?i)(files to lower customer rates|base-rate hike|rate case|"
          r"utility .{0,20}rate (cut|increase))",
          None, "impulse", "regulated price/rate filing"),
    _rule("factor_impulse",
          r"(?i)(fomc|federal reserve|\bfed\b|payrolls|non-?farm|cpi |pce |"
          r"rate hike odds|retail sales.{0,20}(warm|hot|miss|beat)|jackson hole)",
          None, "impulse", "macro number reprices a factor"),
    # --- claims on the firm ---
    _rule("corporate_action_mna",
          r"(?i)(to (buy|acquire)|acquires? |merger |cash deal|\$\d+(\.\d+)?\s*b(illion)? "
          r"(deal|acquisition|cash))",
          None, "impulse", "cash / perimeter of the firm"),
    _rule("dilution",
          r"(?i)(public offering|secondary offering|atm offering|prices \$\d.{0,20}notes)",
          None, "impulse", "new paper"),
    _rule("capital_return",
          r"(?i)(buyback|share repurchase|cash dividend|declares? a .{0,20}dividend|"
          r"quarterly cash dividend)",
          "up", "impulse", "cash out to holders"),
    _rule("integrity",
          r"(?i)(nasdaq notice|delayed .{0,20}(10-q|10-k|filing)|restatement|"
          r"short report|auditor resign)",
          None, "impulse", "governance / integrity print"),
    _rule("insider_flow",
          r"(?i)(ceo sells? \$\d|insider (sale|sold)|form 4)",
          None, "impulse", "weak signal"),
    _rule("listing_flow",
          r"(?i)(ipo |index (add|inclusion)|joins? the .{0,20}(s&p|russell))",
          None, "impulse", "forced paper"),
    _rule("regulatory_probe",
          r"(?i)(opens? (an )?investigation|sec probe|doj (opens|probe))",
          None, "impulse", "overhang, nothing filed as a finding"),
    _rule("activist_campaign",
          r"(?i)(activist|13d |proxy fight|nominates? .{0,20}directors)",
          None, "impulse", "claim on control, no cash moved"),
    # --- leftover talk ---
    _rule("statement_public",
          r"(?i)(essay calling|publicly agreed|five questions for the (ecb|fed)|"
          r"navigating the new)",
          None, "impulse", "words, not a binding constraint"),
]


def _match_rules(text: str) -> Classification | None:
    for event_class, pat, sign, q5, why in _RULES:
        if pat.search(text):
            # guidance raise vs cut
            if event_class == "guidance":
                if re.search(r"(?i)(raises?|reaffirm|beats?.{0,20}outlook)", text):
                    sign = "raise"
                elif re.search(r"(?i)(cut|soft|no longer expects|plunge)", text):
                    sign = "cut"
            if event_class == "input_cost":
                if re.search(r"(?i)(dump|collapse|plunge|fall|cut).{0,20}(oil|fuel|freight)", text):
                    sign = "down"
            if event_class == "capacity":
                if re.search(r"(?i)(fire|outage|destroyed|offline|strike.{0,20}fab)", text):
                    sign = "destroy"
            if event_class == "access_control":
                if re.search(r"(?i)(lift|ease|remove).{0,20}(sanction|ban|curb)", text):
                    sign = "lift"
                    event_class = "sanction_lift"
            constraint = _constraint_line(event_class, text)
            # WH camera ban is access, but it is the regime under this president
            # and has no whole-company cash-flow channel — treat as weather.
            if event_class == "access_control" and re.search(
                r"(?i)(white house).{0,40}(ban|banned|press)|bans? .{0,20}"
                r"(cnn|politico|msnbc|ms now)",
                text,
            ):
                return Classification(
                    event_class="access_control",
                    sign="tighten",
                    q5="regime",
                    constraint="WH press-pool access fight (regime-normal)",
                    why="access fight is the state; no listed whole cash-flow channel",
                    family="structure",
                )
            return Classification(
                event_class=event_class,
                sign=sign,
                q5=q5,
                constraint=constraint,
                why=why,
                family=family_of(event_class),
            )
    return None


def _constraint_line(event_class: str, text: str) -> str:
    one = re.sub(r"\s+", " ", (text or "").split("\n")[0]).strip()[:160]
    return f"{event_class}: {one}"


def classify_text(title: str, body: str = "") -> Classification:
    text = _blob(title, body)
    if not (title or "").strip() and not (body or "").strip():
        return Classification(
            event_class="discard", sign=None, q5="regime",
            constraint="", why="empty", family="time",
        )
    # Two-fact split: inventory print glued to a Fed hike
    split_facts: list[str] = []
    if re.search(r"(?i)inventory", text) and re.search(r"(?i)(fed|rate hike|fomc)", text):
        split_facts = ["inventory_print", "factor_impulse"]
    hit = _match_rules(text)
    if hit:
        if split_facts and hit.event_class in split_facts:
            hit.split = True
            hit.split_facts = split_facts
        return hit
    # Named single-name IR with no other class → still a firm print, not noise.
    if extract_named(title, body) and re.search(
        r"(?i)(earnings|outlook|guidance|dividend|acquire|contract|fda)",
        text,
    ):
        return Classification(
            event_class="print_vs_priced",
            sign=None,
            q5="impulse",
            constraint=_constraint_line("print_vs_priced", text),
            why="named print, no other class matched",
            family="print",
        )
    return Classification(
        event_class="discard",
        sign=None,
        q5="regime",
        constraint=_constraint_line("discard", text),
        why="no constraint identified",
        family="time",
    )


def classify_article(art: dict) -> Classification:
    return classify_text(str(art.get("title") or ""), str(art.get("body") or ""))


def validate_class(event_class: str) -> bool:
    return event_class in EVENT_CLASSES


def merge_lane_class(base: Classification, lane: dict | None) -> Classification:
    """Lane may confirm a valid enum. Invalid / missing → keep deterministic."""
    if not isinstance(lane, dict):
        return base
    ev = str(lane.get("event_class") or "").strip()
    if ev not in EVENT_CLASSES:
        return base
    q5 = str(lane.get("q5") or base.q5).strip()
    if q5 not in ("impulse", "regime", "regime_break"):
        q5 = base.q5
    sign = lane.get("sign")
    if sign in ("", "null", "none"):
        sign = None
    return Classification(
        event_class=ev,
        sign=sign if sign in (None, *{
            "add", "destroy", "up", "down", "open", "shut",
            "tighten", "lift", "cut", "raise",
        }) else base.sign,
        q5=q5,
        constraint=str(lane.get("constraint") or base.constraint),
        split=bool(lane.get("split") or base.split),
        split_facts=list(lane.get("split_facts") or base.split_facts),
        why=str(lane.get("why") or base.why),
        family=family_of(ev),
    )


def harvest_rank_score(title: str, body: str = "") -> int:
    """Gov/macro/structure before A–Z. Higher = analyze first."""
    text = _blob(title, body)
    score = 0
    for pat, pts in (
        (r"(?i)(sec |tokeniz|exemptive|tsv\b|antitrust|class-action|class action)", 50),
        (r"(?i)(fed |fomc|white house|fda |chips act|export curb)", 40),
        (r"(?i)(hormuz|ceasefire|war |tariff|lawsuit|cyberattack)", 35),
        (r"(?i)(euv|sold out|mass production|inventory build|jet fuel)", 30),
        (r"(?i)(guidance|outlook|beats? estimates|misses? estimates)", 15),
        (r"(?i)(dividend|conference call|ticker symbol|price target)", -20),
    ):
        if re.search(pat, text):
            score += pts
    return score


def rank_articles(arts: Iterable[dict]) -> list[dict]:
    rows = list(arts)
    return sorted(
        rows,
        key=lambda a: (-harvest_rank_score(str(a.get("title") or ""),
                                           str(a.get("body") or "")),
                       str(a.get("title") or "")),
    )
