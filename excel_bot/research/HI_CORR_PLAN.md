# H/I correlation mine — plan first, then run

Research only. Live `flatten_robust` frozen. A–F is seeded from Yahoo /
`excel-state` rows, never Excel’s frozen STOCKHISTORY cache.

## Why #144 looked like “crickets”

The workbook is huge (A1:JL, ~35k formulas, 173 conditional-format rules)
but it is **not 275 independent signals**. Almost every cell is a transform
of the STOCKHISTORY spill:

| Col | Meaning (exact Excel) | Clock |
|---|---|---|
| A | date (IR) | open |
| B | close (IS) | close |
| C | open (IT) | open |
| D | high | close |
| E | low | close |
| F | volume | close |
| G | F[t]/F[t−1] relative volume | value close; fill tested open |
| **H** | `(B−C)/C` = (close−open)/open | **label** (close) |
| **I** | `(B−B[t−1])/B[t−1]` = close vs yesterday | **label** (close) |
| J | (C−C[t−1])/C[t−1] open-to-open | open |

Identity (same row): `I = gap + H×(1+gap)` where
`gap = (C−B[t−1])/B[t−1]`. At the open, gap is already known, so
predicting today’s I is the same problem as predicting today’s H.
Same-row “column X correlates with I” is often a tautology (X is built
from H, I, gap, or volume that prints with them).

#144’s discrete threshold sweep (eq1 / ge1 / green-fill) therefore
mostly rediscovered:

1. **One real open encoding** — the morning five-cell green streak + O
   green (± AH / FR). That is a *regime* over several morning-knowable
   fills, not a new data source.
2. **Same-close twins** — e.g. F green with today’s I (volume paint
   and the daily % print together).
3. **Ghosts** — leftover letters that looked hot on a May cut and died
   in Q1 / on a handful of names.

That is an expected ceiling for binary mining of a DAG on six OHLCV
fields over one 2026 tape. It is **not** proof that H and I are
unpredictable. It is proof that *another eq1 sweep of the same letters*
will reprint twins and ghosts.

## What this pass does instead

1. **Feed A–F ourselves** — `fetch_af.py` refreshes
   `excel_bot/data/rows/*.json` from Yahoo v8 (same path as
   `fastfetch.py` / daily bot). No `--from-cache` Excel snapshot.
2. **Labels = Excel H and I** (fractions, not “%” display) on
   1d / 2d / 3d / 1w / 2w. Same-row H/I are never features for that
   row. Prior-row H/I are fair.
3. **Features = numbers + reconstructed Excel formulas + lags**, not
   highlight-only. Open-entry features are morning-knowable (gap, J,
   lagged H/I, AH, prior volume). Close-entry may use today’s H/I/G
   to forecast *later* sessions.
4. **Continuous first** — Spearman of each feature vs each label, then
   quintile means, then a short list of *motivated* gates and pairs
   (gap fade, washout, heat, AH). Not 4,000 random AND-clauses.
5. **Soft regimes** for the green/red regions you can see but not
   hard-code: prior 5-day mean I (heat), prior SPY tape, 20-day vol.
   A global KEEP needs both SPY tapes; regime-conditional keeps are
   reported as such.
6. **Two tapes** — official 3,603-name excel-state window, then a
   longer Yahoo extend so we are not stuck on 2026-only.
7. **Ship bar** — discovery ≠ holdout tickers (`holdout_split.json`),
   both 2026 halves, both SPY tapes, n + ticker floor, lottery / top-5
   name share, edge vs buy-everyone after Futubull costs. Standing
   #144 light+O is a *check*, not re-litigated as a fill mine.

## What would count as a win

Plain English, same shape as the war-room rule:

> When **X** is knowable at the open (or close) of day T, those names’
> **H or I over the next N days** averaged ___ vs everyone, n=___,
> both up/down markets, after fees.

Anything leak-free that clears the bar is a keep — including a combo
with other in-repo PIT fields. A clean null on this method is also a
result.
