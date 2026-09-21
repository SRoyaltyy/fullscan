# News-impact harvest + label hygiene

Research-only restamp after the five hygiene filters. Not a new taxonomy. Deterministic router preserved. 0-1d / 1-4w still use the #305 grade cut (`q5=impulse`, direction `up`/`down`, `tradeable_expression=direct`).

articles=5657  usable=724  tradable=115

## Graded rates after hygiene

- **0-1d** hit rate: 60/105 = 57.1%
- **1-4w** hit rate: 26/51 = 51.0%
- **guidance slice** 0-1d (after sign hygiene): 3/5 = 60.0%
- **guidance slice** 1-4w: n/a
- **macro headline-level basket** 0-1d (not legs): 6/8 = 75.0%
- **macro headline-level basket** 1-4w: 2/5 = 40.0%
- macro stories=13  reprints_collapsed=9  legs (transparency only) 0-1d 22/38 = 57.9%

## What the columns mean

- **0-1d / 1-4w** = graded directional calls only. `factor_impulse` legs, `mixed` / `not_determined`, reaction titles, and long-horizon 0-1d skips are out of these denominators.
- **guidance slice** = same grade cut, after reaffirm→None and raise+miss→mixed. Reaffirm rows are ungraded context.
- **macro headline basket** = one row per `(factor, session, sign)`. Hit = majority of QQQ/TLT/UUP/HYG/SPY agreeing with the implied sign. This is **not** added into the graded 0-1d / 1-4w columns. Leg counts are transparency only.

## Filters

1. Reaction titles (plunge / surge N% / rebound / falls N% / drives N% drop) → `event_class=discard`, `q5=regime`, `entities=[]`.
2. Guidance: reaffirm / maintains guidance → `sign=None`, `direction=not_determined`. Raise + miss EPS → mixed / split, never a single UP.
3. `entry_clock=published` when the source Published timestamp parses; else `retrieved_only`. Published is never invented.
4. Macro reprints collapse to `(factor, session, sign)`.
5. gate / capacity / blast_cyber / CHIPS-style awards skip 0-1d; they may still count in 1-4w.

