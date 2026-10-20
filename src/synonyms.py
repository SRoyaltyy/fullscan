"""Group words that are the same meaning, and discount words that are only close.

True synonyms fold to one token in ``prepare``, the same way a spelling
variant does. ``quick`` and ``fast`` become one term. Near pairs
(``reentry`` / ``splashdown``, ``administer`` / ``administration``) do not
fold: they contribute at ``NEAR_CREDIT`` and only when that does not pull
the article into a different industry.
"""
from __future__ import annotations

# Each tuple folds onto its first item. Homonyms stay out: bank/lender,
# cell/battery, space/office, charge/accuse, rocket/rise.
_GROUPS = (
    ("fast", "quick", "rapid", "swift"),
    ("attorney", "lawyer"),
    ("gasoline", "petrol"),
    ("truck", "lorry"),
    ("movie", "film"),
    ("prison", "jail"),
    ("physician", "doctor"),
    ("disease", "illness"),
    ("purchase", "buy"),
    ("prohibit", "ban"),
    ("airplane", "aeroplane"),
)

# Close, not identical. Deliberately small: a wide derivational net
# (operate/operation, invest/investor) would credit almost every filing.
_NEAR = (
    ("reentry", "splashdown"),
    ("administer", "administration", "administrator", "administrative"),
    ("acquire", "acquisition"),
)

NEAR_CREDIT = 0.45

_CANONICAL: dict[str, str] = {}
for group in _GROUPS:
    head = group[0]
    for word in group:
        _CANONICAL[word] = head

_NEAR_OF: dict[str, tuple[str, ...]] = {}
for group in _NEAR:
    for word in group:
        _NEAR_OF[word] = tuple(other for other in group if other != word)


def canonical(word: str) -> str:
    """Fold a true synonym onto the group's head word."""
    return _CANONICAL.get(word, word)


def near_of(word: str) -> tuple[str, ...]:
    return _NEAR_OF.get(word, ())


def with_near(query: dict[str, float], industry_of: dict[str, str]) -> dict[str, float]:
    """Copy ``query`` and add near terms at a discount.

    A near term is added only when it does not point at a different industry
    from the word that summoned it. If the article word has no industry and
    the near word does, the near word can still help a description match, but
    it is not given enough weight to open that industry by itself: the caller
    scores industries from the original query.
    """
    expanded = dict(query)
    for term, importance in query.items():
        home = industry_of.get(term, "")
        for other in near_of(term):
            if other in query:
                continue
            other_home = industry_of.get(other, "")
            if home and other_home and home != other_home:
                continue
            expanded[other] = expanded.get(other, 0.0) + importance * NEAR_CREDIT
    return expanded
