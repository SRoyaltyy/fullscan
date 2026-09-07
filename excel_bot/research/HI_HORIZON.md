## H/I multi-horizon (standing keep + close pair)

_Generated 2026-09-07 · live `flatten_robust` frozen. Yahoo/rows A–F seed only. No merge. Label is H/I, not Futubull-only. T/BA highlight ghosts not the search._

## Plain English

The mine now predicts **H** (intraday %, close vs open) and **I** (daily %, close vs yesterday) on the ticker train/test split. Horizons: 1d, 2d, 3d, 1 week, 2 weeks. Same-row H and I are the labels — they are never inputs. Yesterday’s H/I may be a feature. Standing five-cell light + green O ± AH/FR is re-scored here. Close-entry pairs may use AA today. Futubull open→close is not the sole label.

**Standing KEEP 6** on stacked daily I (of 20 standing I-horizon rows). Research only — not a card.
**Close-entry KEEP 1 singles / 33 pairs** on 1d stacked I. 30 of those are today's F-green (volume fill) or a twin of it — same-close association with the I print, not a lagged forecast. AA-today pairs do not KEEP (SPY-down red / no edge). Research only — not a card. Trees only if a non-twin pair KEEPs; this F-green cluster is one print, not a new family.

Dumps **3603**. Close atoms **228**. Alive **37**. Pairs **430**. Everyone-else 1d stacked I +0.63% (n=498610). Everyone-else 1d H +0.11% (n=498610). Both SPY tapes required for a global KEEP. Light+O is the check, not a new search.

### Standing light+O ± AH/FR on stacked daily I (holdout moves)

| meaning | horizon | holdout I-stack | vs everyone | Q1 | spy↑ | spy↓ | heat hot | July | top-5 | verdict | why |
|---|---|---|---|---|---|---|---|---|---|---|---|
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light stays on until that sum falls to +2, and O is green | 1d | +2.04% (n=4012) | +1.41 pp | +3.00% (n=977) | +2.73% (n=5049) | +1.63% (n=3428) | +2.20% (n=9935) | 0% | 10% | **KEEP** | — |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light stays on until that sum falls to +2, and O is green | 2d | +2.08% (n=4012) | +1.10 pp | +3.03% (n=977) | +2.54% (n=5049) | +1.70% (n=3428) | +2.20% (n=9935) | 0% | 13% | **KEEP** | — |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light stays on until that sum falls to +2, and O is green | 3d | +3.48% (n=3992) | +2.21 pp | +2.66% (n=977) | +3.68% (n=4991) | +1.67% (n=3428) | +2.84% (n=9877) | 0% | 27% | **KILL** | ticker_ghost |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light stays on until that sum falls to +2, and O is green | 1w | +8.22% (n=3957) | +6.36 pp | +2.07% (n=977) | +3.97% (n=4947) | +1.52% (n=3391) | +4.76% (n=9796) | 0% | 53% | **KILL** | hold_t,ticker_ghost |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light stays on until that sum falls to +2, and O is green | 2w | +4.55% (n=3852) | +0.74 pp | +1.16% (n=977) | +5.19% (n=4860) | +3.16% (n=3306) | +4.83% (n=9548) | 0% | 45% | **KILL** | ticker_ghost |
| same five-cell light + green O, and the recent 5% down-day count (AH) is at least 1 | 1d | +3.39% (n=1277) | +2.76 pp | +4.78% (n=341) | +4.22% (n=1700) | +2.60% (n=1080) | +3.60% (n=3177) | 0% | 16% | **KEEP** | — |
| same five-cell light + green O, and the recent 5% down-day count (AH) is at least 1 | 2d | +3.42% (n=1277) | +2.44 pp | +4.29% (n=341) | +3.82% (n=1700) | +2.80% (n=1080) | +3.57% (n=3177) | 0% | 24% | **KEEP** | — |
| same five-cell light + green O, and the recent 5% down-day count (AH) is at least 1 | 3d | +7.20% (n=1277) | +5.93 pp | +3.58% (n=341) | +6.78% (n=1700) | +2.41% (n=1080) | +5.17% (n=3177) | 0% | 45% | **KILL** | hold_t,ticker_ghost |
| same five-cell light + green O, and the recent 5% down-day count (AH) is at least 1 | 1w | +6.82% (n=1266) | +4.96 pp | +2.04% (n=341) | +7.15% (n=1686) | +1.55% (n=1066) | +4.92% (n=3149) | 0% | 41% | **KILL** | ticker_ghost |
| same five-cell light + green O, and the recent 5% down-day count (AH) is at least 1 | 2w | +10.00% (n=1226) | +6.19 pp | -0.17% (n=341) | +9.50% (n=1658) | +1.61% (n=1037) | +6.78% (n=3062) | 0% | 58% | **KILL** | hold_t,q1_sign,ticker_ghost |
| same five-cell light + green O, and recent volume over 1M and/or G ≥ 3 (FR) is at least 1 | 1d | +2.34% (n=2222) | +1.72 pp | +3.31% (n=556) | +3.04% (n=2753) | +2.04% (n=1857) | +2.56% (n=5400) | 0% | 15% | **KEEP** | — |
| same five-cell light + green O, and recent volume over 1M and/or G ≥ 3 (FR) is at least 1 | 2d | +2.44% (n=2222) | +1.46 pp | +3.57% (n=556) | +2.89% (n=2753) | +2.15% (n=1857) | +2.58% (n=5400) | 0% | 20% | **KEEP** | — |
| same five-cell light + green O, and recent volume over 1M and/or G ≥ 3 (FR) is at least 1 | 3d | +4.70% (n=2210) | +3.43 pp | +3.03% (n=556) | +4.80% (n=2719) | +1.97% (n=1857) | +3.59% (n=5366) | 0% | 38% | **KILL** | ticker_ghost |
| same five-cell light + green O, and recent volume over 1M and/or G ≥ 3 (FR) is at least 1 | 1w | +12.77% (n=2186) | +10.90 pp | +2.29% (n=556) | +4.90% (n=2694) | +1.62% (n=1832) | +6.88% (n=5316) | 0% | 67% | **KILL** | hold_t,ticker_ghost |
| same five-cell light + green O, and recent volume over 1M and/or G ≥ 3 (FR) is at least 1 | 2w | +6.29% (n=2117) | +2.47 pp | +0.97% (n=556) | +7.20% (n=2638) | +4.29% (n=1778) | +6.81% (n=5158) | 0% | 59% | **KILL** | ticker_ghost |
| same five-cell light + green O, and both AH and FR are at least 1 | 1d | +3.81% (n=796) | +3.19 pp | +5.56% (n=203) | +4.70% (n=1011) | +3.26% (n=680) | +4.14% (n=1947) | 0% | 22% | **KILL** | hold1_without_hold2 |
| same five-cell light + green O, and both AH and FR are at least 1 | 2d | +3.93% (n=796) | +2.95 pp | +5.55% (n=203) | +4.39% (n=1011) | +3.65% (n=680) | +4.25% (n=1947) | 0% | 34% | **KILL** | ticker_ghost |
| same five-cell light + green O, and both AH and FR are at least 1 | 3d | +10.03% (n=796) | +8.76 pp | +4.52% (n=203) | +9.28% (n=1011) | +2.80% (n=680) | +6.76% (n=1947) | 0% | 56% | **KILL** | hold_t,ticker_ghost |
| same five-cell light + green O, and both AH and FR are at least 1 | 1w | +8.22% (n=787) | +6.36 pp | +2.47% (n=203) | +8.83% (n=1001) | +1.48% (n=670) | +5.84% (n=1927) | 0% | 54% | **KILL** | hold_t,ticker_ghost |
| same five-cell light + green O, and both AH and FR are at least 1 | 2w | +13.61% (n=758) | +9.79 pp | -1.40% (n=203) | +13.18% (n=988) | -0.13% (n=650) | +8.40% (n=1876) | 0% | 73% | **KILL** | disc_t,hold_t,spy_regime,q1_sign,ticker_ghost,regime_split |

Code names (after the English): `light_O` 1d, `light_O` 2d, `light_O` 3d, `light_O` 1w, `light_O` 2w, `light_O_AH` 1d, `light_O_AH` 2d, `light_O_AH` 3d.

### Same standing recipes on 1d H (intraday print)

| meaning | holdout H | spy↑ | spy↓ | verdict | why |
|---|---|---|---|---|---|
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light stays on until that sum falls to +2, and O is green | +2.11% (n=4012) | +2.43% (n=5049) | +1.98% (n=3428) | **KEEP** | — |
| same five-cell light + green O, and the recent 5% down-day count (AH) is at least 1 | +3.51% (n=1277) | +3.84% (n=1700) | +3.27% (n=1080) | **KEEP** | — |
| same five-cell light + green O, and recent volume over 1M and/or G ≥ 3 (FR) is at least 1 | +2.48% (n=2222) | +2.77% (n=2753) | +2.40% (n=1857) | **KEEP** | — |
| same five-cell light + green O, and both AH and FR are at least 1 | +3.93% (n=796) | +4.27% (n=1011) | +3.85% (n=680) | **KILL** | hold1_without_hold2 |

### Close-entry pair+lag on 1d stacked I (AA today legal)

| meaning | clock | holdout I-stack | vs everyone | Q1 | spy↑ | spy↓ | July | top-5 | verdict | why |
|---|---|---|---|---|---|---|---|---|---|---|
| F today is green AND G today is at least 1 | close | +2.38% (n=32383) | +1.75 pp | +2.89% (n=3713) | +2.94% (n=42410) | +1.64% (n=27050) | 0% | 4% | **KEEP** | — |
| F today is green | close | +2.91% (n=70223) | +2.28 pp | +1.36% (n=8056) | +2.87% (n=89698) | +2.63% (n=59668) | 0% | 23% | **KEEP** | — |
| AA today equals 1 | close | +0.86% (n=37862) | +0.24 pp | -0.41% (n=13315) | +1.28% (n=44204) | +0.06% (n=36380) | 0% | 79% | **KILL** | disc_t,hold_t,no_edge_vs_uncond,q1_sign,ticker_ghost |
| O 2 days ago is under 1 AND AA today equals 1 | close | +1.02% (n=27401) | +0.39 pp | -0.45% (n=8101) | +1.33% (n=32302) | +0.25% (n=25923) | 0% | 90% | **KILL** | disc_t,hold_t,lottery_day,no_edge_vs_uncond,q1_sign,ticker_ghost |
| F today is green AND K today is above 0 | close | +2.78% (n=64057) | +2.16 pp | +1.67% (n=7262) | +3.04% (n=82890) | +2.10% (n=54311) | 0% | 17% | **KEEP** | — |
| J yesterday is under 1 AND F today is green | close | +2.91% (n=70157) | +2.29 pp | +1.31% (n=8049) | +2.87% (n=89601) | +2.64% (n=59597) | 0% | 23% | **KEEP** | — |
| H 4 days ago is under 1 AND F today is green | close | +2.91% (n=70214) | +2.28 pp | +1.36% (n=8055) | +2.87% (n=89681) | +2.63% (n=59655) | 0% | 23% | **KEEP** | — |
| H yesterday is under 1 AND F today is green | close | +2.91% (n=70213) | +2.28 pp | +1.37% (n=8054) | +2.87% (n=89673) | +2.63% (n=59657) | 0% | 23% | **KEEP** | — |
| H 2 days ago is under 1 AND F today is green | close | +2.91% (n=70214) | +2.28 pp | +1.37% (n=8054) | +2.87% (n=89675) | +2.63% (n=59654) | 0% | 23% | **KEEP** | — |
| J 5 days ago is under 1 AND F today is green | close | +2.91% (n=70160) | +2.29 pp | +1.36% (n=8054) | +2.87% (n=89621) | +2.63% (n=59584) | 0% | 23% | **KEEP** | — |
| H 5 days ago is under 1 AND F today is green | close | +2.91% (n=70215) | +2.28 pp | +1.36% (n=8056) | +2.87% (n=89681) | +2.63% (n=59655) | 0% | 23% | **KEEP** | — |
| I 5 days ago is under 1 AND F today is green | close | +2.91% (n=70167) | +2.29 pp | +1.36% (n=8054) | +2.86% (n=89620) | +2.63% (n=59590) | 0% | 23% | **KEEP** | — |
| Q 5 days ago is under 1 AND F today is green | close | +2.91% (n=70223) | +2.28 pp | +1.36% (n=8056) | +2.87% (n=89698) | +2.63% (n=59668) | 0% | 23% | **KEEP** | — |
| Q yesterday is under 1 AND F today is green | close | +2.91% (n=70223) | +2.28 pp | +1.36% (n=8056) | +2.87% (n=89698) | +2.63% (n=59668) | 0% | 23% | **KEEP** | — |
| Q 3 days ago is under 1 AND F today is green | close | +2.91% (n=70223) | +2.28 pp | +1.36% (n=8056) | +2.87% (n=89698) | +2.63% (n=59668) | 0% | 23% | **KEEP** | — |
| Q 2 days ago is under 1 AND F today is green | close | +2.91% (n=70223) | +2.28 pp | +1.36% (n=8056) | +2.87% (n=89698) | +2.63% (n=59668) | 0% | 23% | **KEEP** | — |
| F today is green AND M today is under 1 | close | +2.91% (n=70223) | +2.28 pp | +1.36% (n=8056) | +2.87% (n=89698) | +2.63% (n=59668) | 0% | 23% | **KEEP** | — |
| I yesterday is under 1 AND F today is green | close | +2.91% (n=70163) | +2.28 pp | +1.35% (n=8049) | +2.86% (n=89596) | +2.64% (n=59603) | 0% | 23% | **KEEP** | — |
| H 3 days ago is under 1 AND F today is green | close | +2.91% (n=70213) | +2.28 pp | +1.36% (n=8055) | +2.87% (n=89674) | +2.63% (n=59655) | 0% | 23% | **KEEP** | — |
| DE yesterday is at least 1 AND F today is green | close | +2.91% (n=69921) | +2.29 pp | +1.33% (n=7326) | +2.88% (n=88961) | +2.63% (n=59663) | 0% | 23% | **KEEP** | — |

Code names (after the English): `F_l0_green__and__G_l0_ge1`, `F_l0_green`, `AA_l0_eq1`, `O_l2_lt1__and__AA_l0_eq1`, `F_l0_green__and__K_l0_gt0`, `J_l1_lt1__and__F_l0_green`, `H_l4_lt1__and__F_l0_green`, `H_l1_lt1__and__F_l0_green` ….

### Soft regimes

Sheet heat is the morning five-cell sum (hot ≥5 / mixed / cold ≤0). Tape is SPY up / down / flat. A global KEEP needs SPY-up and SPY-down to agree. Conditional notes stay in the why column (`regime_split`).

### What this does not change

- Standing open Futubull keep is still the research baseline, not a card. This file only re-scores it on H/I.
- Open-entry locked-44 pair+lag stays the accepted null (KEEP 0 / KILL 4266).
- T / BA / CZ / EH / IB / HO / IL / GV stay KILL as highlight ghosts — not this search.
- Same-row H and I are never features.
- Finviz BLOCKED. Live `flatten_robust` frozen. No cards.
- Q1 cut **2026-04-01**. Q3 cut **2026-07-01**.

Board totals (all labels × horizons): KEEP 85 · KILL 1227 · THIN 0.

Research only. One 2026 regime.

