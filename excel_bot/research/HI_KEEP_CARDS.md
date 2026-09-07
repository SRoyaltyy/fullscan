# Research cards — H/I KEEP horizons only

_Generated 2026-09-07 · tip `7b6ede3` / `HI_HORIZON.md` · **research cards only** · live `flatten_robust` frozen · **not** under `strategies/` · **not** a forecast wire._

## Plain English

These are the **only** recipes to card from the H/I board:

- Morning **five-cell light + green O**, optionally **+AH** or **+FR** (not both stacked — AH∧FR failed the bar).
- Labels are Excel **H** (intraday % close vs open) and **I** (daily % close vs yesterday).
- Horizons that KEEP: **same-day H**, **same-day I**, and **1–2 day stacked I**. Longer (3d / 1w / 2w) stay **KILL**.
- **Do not card** today’s **F-green** as a forecast — it is a same-close volume fill tied to the I print (research caveat only).
- H/I are **labels only** — never same-row features. Open entry uses the locked open fills + `OPEN_SAME_ROW_LABELS.md` value set.

Ship bar still open. Push gated on Cyrus.

## Card 1 — light + green O → same-day H

| field | value |
|---|---|
| name | `research_light_O_1d_H` |
| recipe | five-cell morning light + green O |
| entry | open (open-knowable fills) |
| label | H (intraday %, same day) |
| horizon | 1d |
| holdout | **+2.11%** (n=4012) |
| SPY↑ / ↓ / flat | +2.43% / +1.98% / +1.96% |
| top-5 | 6% |
| verdict | **KEEP** |
| status | research card · not live |

## Card 2 — light + green O → same-day I / 1d stacked I

| field | value |
|---|---|
| name | `research_light_O_1d_I` |
| recipe | five-cell morning light + green O |
| entry | open |
| label | I daily print = I stacked 1d |
| horizon | 1d |
| holdout | **+2.04%** (n=4012) |
| SPY↑ / ↓ / flat | +2.73% / +1.63% / +1.67% |
| top-5 | 10% |
| verdict | **KEEP** |
| status | research card · not live |

## Card 3 — light + green O → 2d stacked I

| field | value |
|---|---|
| name | `research_light_O_2d_I_stack` |
| recipe | five-cell morning light + green O |
| entry | open |
| label | I stacked |
| horizon | 2d |
| holdout | **+2.08%** (n=4012) |
| SPY↑ / ↓ / flat | +2.54% / +1.70% / +2.23% |
| top-5 | 13% |
| verdict | **KEEP** |
| status | research card · not live |

## Card 4 — light+O ∧ AH≥1 → same-day H

| field | value |
|---|---|
| name | `research_light_O_AH_1d_H` |
| recipe | light+O and AH≥1 (recent big same-day drops) |
| entry | open |
| label | H |
| horizon | 1d |
| holdout | **+3.51%** (n=1277) |
| SPY↑ / ↓ / flat | +3.84% / +3.27% / +3.83% |
| top-5 | 10% |
| verdict | **KEEP** |
| status | research card · not live |

## Card 5 — light+O ∧ AH≥1 → 1d I / 1d stacked I

| field | value |
|---|---|
| name | `research_light_O_AH_1d_I` |
| recipe | light+O and AH≥1 |
| entry | open |
| label | I / I stacked 1d |
| horizon | 1d |
| holdout | **+3.39%** (n=1277) |
| SPY↑ / ↓ / flat | +4.22% / +2.60% / +3.69% |
| top-5 | 16% |
| verdict | **KEEP** |
| status | research card · not live |

## Card 6 — light+O ∧ AH≥1 → 2d stacked I

| field | value |
|---|---|
| name | `research_light_O_AH_2d_I_stack` |
| recipe | light+O and AH≥1 |
| entry | open |
| label | I stacked |
| horizon | 2d |
| holdout | **+3.42%** (n=1277) |
| SPY↑ / ↓ / flat | +3.82% / +2.80% / +4.64% |
| top-5 | 24% |
| verdict | **KEEP** |
| status | research card · not live |

## Card 7 — light+O ∧ FR≥1 → same-day H

| field | value |
|---|---|
| name | `research_light_O_FR_1d_H` |
| recipe | light+O and FR≥1 (recent volume/G) |
| entry | open |
| label | H |
| horizon | 1d |
| holdout | **+2.48%** (n=2222) |
| SPY↑ / ↓ / flat | +2.77% / +2.40% / +2.54% |
| top-5 | 9% |
| verdict | **KEEP** |
| status | research card · not live |

## Card 8 — light+O ∧ FR≥1 → 1d I / 1d stacked I

| field | value |
|---|---|
| name | `research_light_O_FR_1d_I` |
| recipe | light+O and FR≥1 |
| entry | open |
| label | I / I stacked 1d |
| horizon | 1d |
| holdout | **+2.34%** (n=2222) |
| SPY↑ / ↓ / flat | +3.04% / +2.04% / +2.10% |
| top-5 | 15% |
| verdict | **KEEP** |
| status | research card · not live |

## Card 9 — light+O ∧ FR≥1 → 2d stacked I

| field | value |
|---|---|
| name | `research_light_O_FR_2d_I_stack` |
| recipe | light+O and FR≥1 |
| entry | open |
| label | I stacked |
| horizon | 2d |
| holdout | **+2.44%** (n=2222) |
| SPY↑ / ↓ / flat | +2.89% / +2.15% / +2.54% |
| top-5 | 20% |
| verdict | **KEEP** |
| status | research card · not live |

## Explicitly not carded

| candidate | why |
|---|---|
| light+O ∧ AH≥1 ∧ FR≥1 (any horizon) | **KILL** (hold1_without_hold2 / name ghost) |
| 3d / 1w / 2w on H, I print, or stacked I | **KILL** (ticker ghosts / regime splits) |
| Cyrus O[t−2]∧AA[t] | **KILL** |
| today’s F-green (± twins) on 1d I | same-close volume fill tied to I — **not a lagged forecast**; research caveat only |
| open locked-44 value/text pairs | accepted null (KEEP 0 / KILL 4266) |

## Source

Numbers from `excel_bot/research/HI_HORIZON.md` at tip `7b6ede3`. Clock gate: `OPEN_SAME_ROW_LABELS.md`. Research only. Live frozen.
