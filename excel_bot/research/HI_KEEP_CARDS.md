# Research cards — H/I KEEP horizons only

_Generated 2026-09-07 · family **DEMOTE** after soft-regime tip `3ac6d7b` / `HI_SOFT_REGIME.md` (PR #148) · numbers still from `HI_HORIZON.md` at `7b6ede3` · **research cards only** · live `flatten_robust` frozen · **not** under `strategies/` · **not** a forecast wire._

## Plain English

Soft-regime **demoted the standing family**. KEEP 2 · DEMOTE 6 · REGIME-CONDITIONAL 1 of the nine HI slots. The hand-gate Excel A–JL / H/I mine is **exhausted on this tape**. Remaining KEEPs are research-only: **same-day H** on five-cell light+green O, and **same-day H** on light+O ∧ AH. FR on H is **regime-conditional only** (4–4 split). Same-day I and 1–2d stacked I do not survive the heat×tape cells (usually a five-name ghost). Not a live wire. Next cut is multi-year full-sheet ML.

H/I stay **labels only** — never same-row features. Open entry uses the locked open fills + `OPEN_SAME_ROW_LABELS.md` value set. **Do not card** today’s F-green as a forecast. Ship bar still open. Push gated on Cyrus.

**Family verdict: DEMOTE**

## Soft-regime (tip `3ac6d7b` / PR #148)

Source: `excel_bot/research/HI_SOFT_REGIME.md` on `cursor/hi-soft-regime-dc24` at `3ac6d7b`. Heat is the open-knowable prior-5-session mean of I (discovery terciles: cold ≤ −0.45%, hot ≥ 0.48%). Tape is SPY up / down / flat. Each slot needs a strict majority of the 9 heat×tape cells vs same-cell buy-everyone after Futubull 0.15%.

| recipe | same-day H | same-day I | 2d stacked I |
|---|---|---|---|
| light+O | **KEEP** 7/1/1 | **DEMOTE** 3/5/1 | **DEMOTE** 2/6/1 |
| light+O ∧ AH | **KEEP** 5/2/2 | **DEMOTE** 2/5/2 | **DEMOTE** 2/5/2 |
| light+O ∧ FR | **REGIME-CONDITIONAL** 4/4/1 | **DEMOTE** 2/6/1 | **DEMOTE** 2/6/1 |

Cells: KEEP 29 · KILL 40 · THIN 12. Cold×flat is THIN (n=17–28) on every recipe. Same-day H still beats the book in most buckets (e.g. light+O, cold × SPY-up: +1.14 pp, holdout n=178). I and 2d stacks usually still print a mean and then fail the five-name ghost bar.

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
| status | KEEP (soft-regime majority 7/9 cells) · research-only · not live |

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
| verdict | **DEMOTE** |
| status | DEMOTE (soft-regime 3/5/1) · research-only · not live |

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
| verdict | **DEMOTE** |
| status | DEMOTE (soft-regime 2/6/1) · research-only · not live |

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
| status | KEEP (soft-regime majority 5 cells) · research-only · not live |

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
| verdict | **DEMOTE** |
| status | DEMOTE (soft-regime 2/5/2) · research-only · not live |

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
| verdict | **DEMOTE** |
| status | DEMOTE (soft-regime 2/5/2) · research-only · not live |

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
| verdict | **REGIME-CONDITIONAL** |
| status | REGIME-CONDITIONAL (4–4 split) · research-only · not live |

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
| verdict | **DEMOTE** |
| status | DEMOTE (soft-regime 2/6/1) · research-only · not live |

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
| verdict | **DEMOTE** |
| status | DEMOTE (soft-regime 2/6/1) · research-only · not live |

## Explicitly not carded

| candidate | why |
|---|---|
| light+O ∧ AH≥1 ∧ FR≥1 (any horizon) | **KILL** (hold1_without_hold2 / name ghost) |
| 3d / 1w / 2w on H, I print, or stacked I | **KILL** (ticker ghosts / regime splits) |
| Cyrus O[t−2]∧AA[t] | **KILL** |
| today’s F-green (± twins) on 1d I | same-close volume fill tied to I — **not a lagged forecast**; research caveat only |
| open locked-44 value/text pairs | accepted null (KEEP 0 / KILL 4266) |
| same-day I and 1–2d stacked I on light+O ± AH/FR | **DEMOTE** after soft-regime (five-name ghost in most heat×tape cells) |
| light+O ∧ FR on same-day H | **REGIME-CONDITIONAL** only — not a family keep |

## Source

Holdout headline numbers from `excel_bot/research/HI_HORIZON.md` at tip `7b6ede3`. Soft-regime family verdict from `excel_bot/research/HI_SOFT_REGIME.md` at tip `3ac6d7b` (PR #148). Clock gate: `OPEN_SAME_ROW_LABELS.md`. Research only. Live frozen. Next cut: multi-year full-sheet ML.
