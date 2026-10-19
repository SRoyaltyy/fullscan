"""Fold UK spellings onto the US form before any other comparison.

The fold is one-way and whole-word. ``hour`` does not become ``hor``,
``promise`` does not become ``promize``, and ``check`` is left alone
(only ``cheque`` moves).
"""
from __future__ import annotations

# Whole words whose British form is not a suffix of a general rule.
_EXACT = {
    "defence": "defense",
    "offence": "offense",
    "licence": "license",
    "pretence": "pretense",
    "practise": "practice",
    "analogue": "analog",
    "catalogue": "catalog",
    "dialogue": "dialog",
    "aluminium": "aluminum",
    "grey": "gray",
    "tyre": "tire",
    "cheque": "check",
    "kerb": "curb",
    "draught": "draft",
    "plough": "plow",
    "sceptic": "skeptic",
    "sceptical": "skeptical",
    "manoeuvre": "maneuver",
    "oestrogen": "estrogen",
    "paediatric": "pediatric",
    "foetus": "fetus",
    "haemoglobin": "hemoglobin",
    "leukaemia": "leukemia",
    "jewellery": "jewelry",
    "aeroplane": "airplane",
    "cosy": "cozy",
    "yoghurt": "yogurt",
    "mould": "mold",
    "moult": "molt",
    "smoulder": "smolder",
    "towards": "toward",
    "whilst": "while",
    "amongst": "among",
}

# British -our words. The denylist is every -our word that is not that suffix.
_OUR_DENY = frozenset("""
hour four your our tour pour contour detour amour troubadour velour
""".split())

# centre, fibre, metre. ``ogre`` and ``acre`` are not this pattern.
_RE = frozenset("""
centre fibre litre metre theatre calibre spectre sabre sombre lustre meagre
mitre nitre reconnoitre
""".split())

# British -ise verbs. ``promise``, ``enterprise``, ``exercise`` are not in here.
_ISE = frozenset("""
organise recognise realise analyse paralyse emphasise summarise minimise
maximise standardise specialise criticise authorise characterise industrialise
nationalise privatise globalise legalise penalise finalise utilise mobilise
stabilise visualise categorise prioritise customise optimise harmonise
subsidise revitalise normalise familiarise memorise apologise agonise
capitalise itemise modernise popularise digitise computerise commercialise
centralise decentralise liberalise generalise serialise vocalise localise
""".split())

_ISE_DENY = frozenset("""
promise precise premise franchise surprise comprise advise rise wise
paradise treatise exercise enterprise advertise compromise expertise
supervise merchandise paradise mortise valise cerise concise
""".split())


def spelling(word: str) -> str:
    """Return the US spelling of one lowercase word, or the word itself."""
    mapped = _one(word)
    if mapped != word:
        return mapped
    # defences -> defenses, colours -> colors, organised already handled.
    if len(word) > 4 and word.endswith("s") and not word.endswith("ss"):
        stem = _one(word[:-1])
        if stem != word[:-1]:
            return stem + "s"
    return word


def _one(word: str) -> str:
    if word in _EXACT:
        return _EXACT[word]
    if word in _RE:
        return word[:-2] + "er"
    if word.endswith("our") and len(word) > 4 and word not in _OUR_DENY:
        return word[:-3] + "or"
    if word in _ISE:
        return word[:-3] + "ize"
    # organised -> organized, organising -> organizing, organises -> organizes.
    if word.endswith("ised") and word[:-1] in _ISE:
        return word[:-4] + "ized"
    if word.endswith("ising") and (word[:-3] + "e") in _ISE:
        return word[:-5] + "izing"
    if word.endswith("ises") and word[:-1] in _ISE:
        return word[:-4] + "izes"
    if word.endswith("isation") and len(word) > 9:
        return word[:-7] + "ization"
    if word.endswith("isations") and len(word) > 10:
        return word[:-8] + "izations"
    return word
