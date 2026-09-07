## Open-stack verdict (light+O vs AH / FR)

_Generated 2026-09-07 · live `flatten_robust` frozen. Yahoo/rows A–F seed only. No merge. Close-letter shortboard (T, BA, …) is out of scope._

## Plain English

The leftover harden left two open-knowable unique KEEPs: **AH** (recent 5% down-day count) and **FR** (recent volume over 1M and/or G ≥ 3). The standing open keep is still **light + green O** (three recipes after Q3). This beat asks: if you already wait for that morning light and a green O, does also requiring AH≥1 or FR≥1 add money after Futubull fees?

**On the five-cell light + green O, AH is stronger** (about +116 to +133 bp after fees) and **FR is stronger** (about +35 bp). Both KEEP hold1 and hold2. **On the nine-cell light + green O both stacks print stronger but KILL** — AH is a one-day lottery (fattest day over 25%) with a weak holdout t, and FR misses the holdout t-bar. AH alone already KEEP as a leftover letter and is a bit stronger than light+O on a much wider book. FR alone already KEEP versus buy-everyone, but **weaker** than light+O — it is not a substitute for the light. Neither letter is a new standing open keep unless the stack itself clears.

Ship bar is the same as light+O: both ticker halves, both calendar halves (cut 2026-05-01), **Q3 (2026-07-01)**, both SPY tapes, fattest day under 25% of winning-day P&L, and at least 20 bp better than the parent (light+O on the same dump-covered dates). Grids **3603**. Names with both a grid and an AH/FR dump: **3603**.

Stacks: **KEEP 4** · **KILL 2** · **THIN 0**. Standing light+O recipes stay KEEP. No card.

### Standing light+O (baseline KEEP)

Published Q3 numbers, then the same recipes re-scored only on days that also have an AH/FR dump (the parent used for stacks).

| recipe | hold | published holdout | overlap holdout | Q3 (overlap) | verdict |
|---|---|---|---|---|---|
| the five morning cells that are already known at 9:30 (A, B, C, G, J) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. plus green O. | next 1 | +1.98% (n=4812) | +1.86% (n=4345) | +1.78% (n=3048) | **KEEP** |
| the five morning cells that are already known at 9:30 (A, B, C, G, J) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. plus green O. | next 2 | +1.88% (n=4812) | +1.81% (n=4345) | +1.72% (n=3048) | **KEEP** |
| all nine morning cells known at 9:30 (A, B, C, G, J, K, L, M, O) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. plus green O. | next 2 | +1.99% (n=5264) | +2.09% (n=4500) | +2.37% (n=3307) | **KEEP** |

### Stacks vs light+O

| recipe | hold | layer | holdout | vs light+O | Q3 | day-lottery | verdict | why | code |
|---|---|---|---|---|---|---|---|---|---|
| the five morning cells that are already known at 9:30 (A, B, C, G, J) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. plus green O. | next 1 | light+O ∧ AH≥1 | +3.19% (n=1317) | +1.33 pp (stronger) | +3.02% (n=927) | 10.5% | **KEEP** | — | `hyst_open_core_e5_x2__O_green__AH_ge1` |
| the five morning cells that are already known at 9:30 (A, B, C, G, J) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. plus green O. | next 1 | light+O ∧ FR≥1 | +2.21% (n=2282) | +0.35 pp (stronger) | +2.08% (n=1676) | 8.6% | **KEEP** | — | `hyst_open_core_e5_x2__O_green__FR_ge1` |
| the five morning cells that are already known at 9:30 (A, B, C, G, J) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. plus green O. | next 2 | light+O ∧ AH≥1 | +2.98% (n=1317) | +1.16 pp (stronger) | +3.50% (n=927) | 14.2% | **KEEP** | — | `hyst_open_core_e5_x2__O_green__AH_ge1` |
| the five morning cells that are already known at 9:30 (A, B, C, G, J) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. plus green O. | next 2 | light+O ∧ FR≥1 | +2.16% (n=2282) | +0.35 pp (stronger) | +2.21% (n=1676) | 11.7% | **KEEP** | — | `hyst_open_core_e5_x2__O_green__FR_ge1` |
| all nine morning cells known at 9:30 (A, B, C, G, J, K, L, M, O) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. plus green O. | next 2 | light+O ∧ AH≥1 | +4.61% (n=1373) | +2.52 pp (stronger) | +6.59% (n=987) | 25.2% | **KILL** | hold_t,lottery_day | `hyst_open_score_e5_x2__O_green__AH_ge1` |
| all nine morning cells known at 9:30 (A, B, C, G, J, K, L, M, O) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. plus green O. | next 2 | light+O ∧ FR≥1 | +2.93% (n=2356) | +0.84 pp (stronger) | +3.76% (n=1814) | 22.2% | **KILL** | hold_t | `hyst_open_score_e5_x2__O_green__FR_ge1` |

### AH / FR alone vs light+O (already KEEP, not re-mined)

Leftover hold2 survivors. Incremental is versus the published light+O hold2 on the matching recipe, not a new mine.

| letter | what it is | leftover hold2 | vs five-cell+O hold2 | vs nine-cell+O hold2 | note |
|---|---|---|---|---|---|
| **AH** | count of recent same-day drops of 5% or more (open-knowable walk) | +2.22% (n=51855) | +0.33 pp | +0.22 pp | already KEEP as leftover; stack must still beat light+O |
| **FR** | recent volume over 1M and/or G ≥ 3 (open-knowable walk) | +0.94% (n=85081) | -0.95 pp | -1.05 pp | already KEEP as leftover; stack must still beat light+O |

### What this does not change

- Standing A–O research keeps stay the **three light + green O** recipes.
- Close-letter leftover KEEPs (T, BA, CZ, EH, IB and peers) are out of scope this beat.
- Finviz volume stays **BLOCKED**. AB / weather / book stay dead.
- Live `flatten_robust` is not imported or changed. No cards.
- Calendar half cut **2026-05-01**. Q3 cut **2026-07-01**. Futubull 0.15% long / 0.20% short.

Research only. One 2026 regime.

