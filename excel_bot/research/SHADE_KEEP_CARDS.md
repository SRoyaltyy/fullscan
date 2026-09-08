# Research cards — shade hex open entry (same-day H)

_Generated 2026-09-08 · tip `5be0f044` / `SHADE_OPEN.md` · **research cards only** · live `flatten_robust` frozen · **not** under `strategies/` · ghost/name deepen on #150 **landed**: family **GHOST CONDITIONAL**._

## Plain English

Shade ≠ any-green. From the open-knowable fill gate (`A B C G J K L M O IR IS IT`), two recipes stand out on same-day **H** (intraday %) at **open entry**:

1. Morning cell **M** mid-green hex `#95CA82` (score ≥ 1.5) — ~**+10.6%** holdout after fees vs book (~+7.9 pp vs any-green M).
2. Morning cell **O** red→green onset — ~**+7.2%** holdout after fees.

Soft-regime majority held on the shade board (M mid 8/9). Ghost/name deepen **cleared standing M mid** (GHOST PASS — drop-5 leftover still +9.51%). **M hex-onset is GHOST CONDITIONAL** (holdout top-5 15.1%). **O red→green onset is GHOST FAIL** (INHD 19.7% of holdout P&L; leftover no longer beats any-green O). **Not a live wire.** Clock: M fill is open-knowable at 9:30; M **value** is still close-only — this card is **fill shade**, not the number.

## Card 1 — M mid-green `#95CA82`

| field | value |
|---|---|
| name | `research_M_hex_95CA82_1d_H` |
| recipe | morning M fill hex `#95CA82` (mid green / score ≥ 1.5) |
| entry | open |
| label | H (intraday %, same day) |
| horizon | 1d |
| holdout | **+10.57%** (n=4423) |
| vs book | +10.66 pp |
| vs any-green M | +7.86 pp |
| SPY↑ / ↓ | +10.30% / +10.22% |
| top-5 / July | 7% / 10% |
| holdout top-5 (ghost) | 13.9% (INHD 8.0%) · drop-5 leftover +9.51% |
| codes | `M_hex_95CA82`, `M_ge15` (same print on this dump) |
| verdict | **KEEP** · **GHOST PASS** |
| status | research card · not live |

## Card 2 — M mid-green onset `#95CA82`

| field | value |
|---|---|
| name | `research_M_onset_hex_95CA82_1d_H` |
| recipe | M flips onto `#95CA82` today |
| entry | open |
| label | H |
| horizon | 1d |
| holdout | **+10.34%** (n=3731) |
| vs book | +10.43 pp |
| holdout top-5 (ghost) | 15.1% (INHD 9.5%) · drop-5 leftover +9.10% |
| code | `M_onset_hex_95CA82` |
| verdict | **GHOST CONDITIONAL** (holdout top-5 just over 15%) |
| status | research card · not live · not demoted |

## Card 3 — O red→green onset

| field | value |
|---|---|
| name | `research_O_onset_red2green_1d_H` |
| recipe | O red yesterday → green today |
| entry | open |
| label | H |
| horizon | 1d |
| holdout | **+7.21%** (n=2502) |
| vs book | +7.30 pp |
| vs parent any-green O | +1.52 pp |
| SPY↑ / ↓ | +6.52% / +6.39% |
| top-5 / July | 12% / 11% |
| holdout top-5 (ghost) | **25.6%** (INHD 19.7%) · drop-5 leftover +5.56% (loses vs parent) |
| code | `O_onset_red2green` |
| verdict | **GHOST FAIL** (demoted by name check) |
| status | research card · **demoted** · not live |

## Explicitly not carded (for now)

| candidate | why |
|---|---|
| M any-green / M pale `#DCEDD5` | weaker / loses to mid |
| A / B / L standing shade | KILL on shade board |
| K deep `#3B7D23` | weaker; New Bot called regime-conditional |
| heat-green (prior I) | out of scope |
| light+O family (pre-shade) | soft-regime demoted |

## Source

`excel_bot/research/SHADE_OPEN.md` tip `5be0f044` · PR #150. Fill gate: Excel open list. Research only. Live frozen.
