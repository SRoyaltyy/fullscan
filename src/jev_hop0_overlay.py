"""Live hop-0 formula overlay.

Keep iff (done OR print OR spoke) AND NOT tip.
tape/soft cannot veto a title that already has a keep bit.
listed is retired. Criteria describe jobs, not homework titles.

Imported for side effect: patches src.jev_bits in place so jev_gate
and the trainer pick this up without rewriting the regex pack.
"""
from __future__ import annotations

from . import jev_bits as bits

SIX_BITS = ("tape", "soft", "tip", "done", "print", "spoke")
BIT_NOUL = bits.BIT_NOUL

BIT_QUESTIONS = {
    "tape": {
        "type": "noul",
        "instructions": (
            "TRUE only if the title IS the already-moved price: a percent, "
            "'stock today', 'outperforms', or a close recap with no act. "
            "FALSE if an act, award, official print, or named official is "
            "also in the sentence — the move is then just a wrapper."
        ),
        "criteria": {
            "true": (
                "A named stock outperforms competitors on a strong trading day. "
                "A stock-market-today wrap with tickers in focus."
            ),
            "false": (
                "A company already sold a unit or won an award, even if the "
                "headline also says the stock jumped. "
                "A sitting official already denied a policy in this sentence, "
                "even if oil or gold reacts in the same line."
            ),
        },
    },
    "soft": {
        "type": "noul",
        "instructions": (
            "TRUE only if the whole title is a preview: ahead of, expected, "
            "what to watch, call highlights, transcript. "
            "FALSE if a named official is speaking now, even if the word "
            "could or may appears in what they said."
        ),
        "criteria": {
            "true": (
                "Asia stocks gain ahead of a US inflation print. "
                "An earnings call highlights or transcript page."
            ),
            "false": (
                "A numbered official print already landed. "
                "A sitting Fed president or governor is speaking in this sentence."
            ),
        },
    },
    "tip": {
        "type": "noul",
        "instructions": (
            "TRUE if this is a buy/sell list, which-is-better matchup, "
            "or a column telling the reader what to do. "
            "FALSE if a named firm finished an act or an official print landed."
        ),
        "criteria": {
            "true": (
                "N funds to consider as the Fed signals hikes. "
                "Which of two stocks is a better buy this year?"
            ),
            "false": (
                "A named venue already agreed to buy another venue. "
                "A named firm already completed an acquisition."
            ),
        },
    },
    "done": {
        "type": "noul",
        "instructions": (
            "TRUE if a named firm or state already finished an act in this "
            "sentence: sold a unit, won a government award, printed results "
            "or guidance that already beat or missed, changed market hours, "
            "closed a deal, pulled a listing, or a sitting head of state "
            "already accepted or denied a live policy. "
            "FALSE if it is only talks, a rumor, an analyst price target, "
            "a personnel hire to advise an agency, or a recap of an old IPO price."
        ),
        "criteria": {
            "true": (
                "A named firm already sold a division or won a government "
                "services award. "
                "A company already printed results or guidance that beat or missed."
            ),
            "false": (
                "Talks to buy a target, or takeover talks with no close. "
                "A broker revamps a price target before earnings, or an "
                "executive is hired to advise an agency."
            ),
        },
    },
    "print": {
        "type": "noul",
        "instructions": (
            "TRUE if this sentence IS a numbered official release that already "
            "printed (CPI, PPI, PCE, NFP, confidence, a nationwide agency recall) "
            "or a central bank that already hiked, cut, or held. "
            "FALSE if the print is only upcoming, expected, or a tip sheet "
            "that mentions the Fed."
        ),
        "criteria": {
            "true": (
                "A stats agency already printed the number below or above "
                "expectations. "
                "A central bank already held or hiked in this sentence."
            ),
            "false": (
                "Markets await the print, or the print is expected later. "
                "A tip sheet or mutual-fund list that only mentions the Fed."
            ),
        },
    },
    "spoke": {
        "type": "noul",
        "instructions": (
            "TRUE if a sitting official is speaking in this sentence: a Fed "
            "governor or regional president, a central-bank head, or a head "
            "of state announcing or denying a live policy. "
            "FALSE if a columnist, TV host, or expert reacts is talking, "
            "or if someone was only hired to advise an agency."
        ),
        "criteria": {
            "true": (
                "A named Fed president hints, sees, or warns in this sentence. "
                "A sitting president already denies a live sanctions report."
            ),
            "false": (
                "A letters-to-the-editor or opinion column about the Fed. "
                "A reporter previews Jackson Hole or a bond-market test ahead "
                "of a debut."
            ),
        },
    },
}


def _noul(answers, key):
    return bits._noul(answers, key)


def bit_on(answers, name):
    return _noul(answers, name) >= BIT_NOUL


def keep_bits_on(answers) -> bool:
    return any(bit_on(answers, name) for name in ("done", "print", "spoke"))


def formula_reason(answers) -> tuple[str, str]:
    """Keep iff (done OR print OR spoke) AND NOT tip."""
    if bit_on(answers, "tip"):
        return "drop", "tip"
    if bit_on(answers, "print"):
        return "keep", "print"
    if bit_on(answers, "spoke"):
        return "keep", "spoke"
    if bit_on(answers, "done"):
        return "keep", "done"
    if bit_on(answers, "tape"):
        return "drop", "tape"
    if bit_on(answers, "soft"):
        return "drop", "soft"
    return "drop", "no_keep_bit"


def decide(row: dict, answers=None) -> dict:
    title = row.get("title") or ""
    source = row.get("source") or ""
    blob = f"{title} {source}"

    def pack(decision: str, reason: str) -> dict:
        out = {
            "title": title,
            "source": source,
            "published_at": row.get("published_at") or "",
            "url": row.get("url") or "",
            "id": row.get("id") or "",
            "decision": decision,
            "reason": reason,
            "geo": "",
            "actor_power": "",
            "action_material": 0.0,
            "new_instrument": 1.0 if decision == "keep" else 0.0,
            "reprint_weather": 0.0,
            "place": "",
            "has_new_verb": False,
        }
        if bits.has_bit_answers(answers):
            out["noul"] = {key: round(_noul(answers, key), 3) for key in BIT_QUESTIONS}
        return out

    cheap = bits.cheap_veto(blob)
    if cheap:
        return pack("drop", cheap)

    if bits.has_bit_answers(answers):
        hard = keep_bits_on(answers) and not bit_on(answers, "tip")
        if bits.SOURCE_DENY.search(blob) and not hard:
            return pack("drop", "source")
        decision, reason = formula_reason(answers)
        return pack(decision, reason)

    veto = bits.code_veto(blob)
    if veto:
        return pack("drop", veto)
    keep = bits.code_keep(title)
    if keep:
        return pack("keep", keep)
    return pack("drop", "no_keep_bit")


def install() -> None:
    bits.SIX_BITS = SIX_BITS
    bits.BIT_QUESTIONS = BIT_QUESTIONS
    bits.formula_reason = formula_reason
    bits.decide = decide
    bits.keep_bits_on = keep_bits_on


install()
