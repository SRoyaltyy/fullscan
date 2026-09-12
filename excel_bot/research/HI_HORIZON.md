## H/I multi-horizon (standing keep + close pair)

_Generated 2026-09-07 · live `flatten_robust` frozen. Yahoo/rows A–F seed only. No merge. Label is H/I, not Futubull-only. T/BA highlight ghosts not the search._

## Plain English

The mine now predicts **H** (intraday %, close vs open) and **I** (daily %, close vs yesterday) on the ticker train/test split. Horizons: 1d, 2d, 3d, 1 week, 2 weeks. Same-row H and I are the labels — they are never inputs. Yesterday’s H/I may be a feature. Standing five-cell light + green O ± AH/FR is re-scored here. Close-entry pairs may use AA today. Futubull open→close is not the sole label.

**Standing KEEP 6** on stacked daily I (of 20 standing I-horizon rows). Research only — not a card.
**Close-entry KEEP 1 singles / 33 pairs** on 1d stacked I. 30 of those are today's F-green (volume fill) or a twin of it — same-close association with the I print, not a lagged forecast. AA-today pairs do not KEEP (SPY-down red / no edge). Research only — not a card. Trees only if a non-twin pair KEEPs; this F-green cluster is one print, not a new family.

Excel gate: H and I are **labels only** (same-row close). Open features are the locked 44 + open fills. Close-entry may use other close cols. Same-row H/I never enter as day-X features.

Dumps **3603**. Close atoms **228**. Alive **37**. Pairs **430**. Everyone-else 1d stacked I +0.63% (n=498610). Everyone-else 1d H +0.11% (n=498610). Everyone-else 1d I +0.63% (n=498610). Both SPY tapes required for a global KEEP. Light+O is the check, not a new search.

### Standing KEEP/KILL grid (H, I print, stacked I × horizon)

Holdout mean. **KEEP** / **KILL** after the ship + ghost + both-tape bar. 1d H is the same-day intraday print; 1d I is the same-day daily print; stacked I is the k-day compound.

| recipe | label | 1d | 2d | 3d | 1w | 2w |
|---|---|---|---|---|---|---|
| five-cell light + green O | H (intraday) | **KEEP** +2.11% (n=4012) | **KILL** +0.04% (n=4012) | **KILL** +0.06% (n=3992) | **KILL** -0.06% (n=3957) | **KILL** -0.07% (n=3852) |
| five-cell light + green O | I (daily print) | **KEEP** +2.04% (n=4012) | **KILL** -0.03% (n=4012) | **KILL** +1.04% (n=3992) | **KILL** +7.32% (n=3957) | **KILL** -0.08% (n=3852) |
| five-cell light + green O | I stacked | **KEEP** +2.04% (n=4012) | **KEEP** +2.08% (n=4012) | **KILL** +3.48% (n=3992) | **KILL** +8.22% (n=3957) | **KILL** +4.55% (n=3852) |
| light+O ∧ AH≥1 | H (intraday) | **KEEP** +3.51% (n=1277) | **KILL** -0.16% (n=1277) | **KILL** -0.17% (n=1277) | **KILL** -0.25% (n=1266) | **KILL** -0.40% (n=1226) |
| light+O ∧ AH≥1 | I (daily print) | **KEEP** +3.39% (n=1277) | **KILL** -0.18% (n=1277) | **KILL** +2.69% (n=1277) | **KILL** +3.36% (n=1266) | **KILL** -0.47% (n=1226) |
| light+O ∧ AH≥1 | I stacked | **KEEP** +3.39% (n=1277) | **KEEP** +3.42% (n=1277) | **KILL** +7.20% (n=1277) | **KILL** +6.82% (n=1266) | **KILL** +10.00% (n=1226) |
| light+O ∧ FR≥1 | H (intraday) | **KEEP** +2.48% (n=2222) | **KILL** +0.07% (n=2222) | **KILL** -0.02% (n=2210) | **KILL** -0.22% (n=2186) | **KILL** -0.02% (n=2117) |
| light+O ∧ FR≥1 | I (daily print) | **KEEP** +2.34% (n=2222) | **KILL** -0.03% (n=2222) | **KILL** +1.60% (n=2210) | **KILL** +12.81% (n=2186) | **KILL** -0.12% (n=2117) |
| light+O ∧ FR≥1 | I stacked | **KEEP** +2.34% (n=2222) | **KEEP** +2.44% (n=2222) | **KILL** +4.70% (n=2210) | **KILL** +12.77% (n=2186) | **KILL** +6.29% (n=2117) |
| light+O ∧ AH≥1 ∧ FR≥1 | H (intraday) | **KILL** +3.93% (n=796) | **KILL** -0.16% (n=796) | **KILL** -0.03% (n=796) | **KILL** -0.68% (n=787) | **KILL** -0.34% (n=758) |
| light+O ∧ AH≥1 ∧ FR≥1 | I (daily print) | **KILL** +3.81% (n=796) | **KILL** -0.22% (n=796) | **KILL** +4.34% (n=796) | **KILL** +4.32% (n=787) | **KILL** -0.51% (n=758) |
| light+O ∧ AH≥1 ∧ FR≥1 | I stacked | **KILL** +3.81% (n=796) | **KILL** +3.93% (n=796) | **KILL** +10.03% (n=796) | **KILL** +8.22% (n=787) | **KILL** +13.61% (n=758) |

Code names (after the English): `light_O`, `light_O_AH`, `light_O_FR`, `light_O_AH_FR`.

### Standing detail (holdout, both tapes, why)

| recipe | label | horizon | holdout | spy↑ | spy↓ | spy flat | July | top-5 | verdict | why |
|---|---|---|---|---|---|---|---|---|---|---|
| five-cell light + green O | H (intraday) | 1d | +2.11% (n=4012) | +2.43% (n=5049) | +1.98% (n=3428) | +1.96% (n=1458) | 0% | 6% | **KEEP** | — |
| five-cell light + green O | H (intraday) | 2d | +0.04% (n=4012) | -0.06% (n=5049) | +0.15% (n=3428) | +0.42% (n=1458) | 0% | 46% | **KILL** | disc_t,hold_t,tape_split,spy_regime,q3_sign,q3_split,no_edge_vs_uncond,ticker_ghost,regime_split |
| five-cell light + green O | H (intraday) | 3d | +0.06% (n=3992) | +0.20% (n=4991) | -0.12% (n=3428) | +0.15% (n=1458) | 0% | 61% | **KILL** | disc_t,hold_t,tape_split,spy_regime,q3_sign,q3_split,no_edge_vs_uncond,ticker_ghost,regime_split |
| five-cell light + green O | H (intraday) | 1w | -0.06% (n=3957) | -0.13% (n=4947) | -0.09% (n=3391) | +0.27% (n=1458) | 0% | -75% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond |
| five-cell light + green O | H (intraday) | 2w | -0.07% (n=3852) | -0.06% (n=4860) | -0.03% (n=3306) | -0.00% (n=1382) | 0% | -97% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign |
| five-cell light + green O | I (daily print) | 1d | +2.04% (n=4012) | +2.73% (n=5049) | +1.63% (n=3428) | +1.67% (n=1458) | 0% | 10% | **KEEP** | — |
| five-cell light + green O | I (daily print) | 2d | -0.03% (n=4012) | -0.05% (n=5049) | +0.05% (n=3428) | +0.42% (n=1458) | 0% | 266% | **KILL** | disc_t,hold_t,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,q3_split,no_edge_vs_uncond,q1_sign,ticker_ghost,regime_split |
| five-cell light + green O | I (daily print) | 3d | +1.04% (n=3992) | +1.04% (n=4991) | -0.07% (n=3428) | +0.11% (n=1458) | 0% | 77% | **KILL** | disc_t,hold_t,spy_regime,no_edge_vs_uncond,q1_sign,ticker_ghost,regime_split |
| five-cell light + green O | I (daily print) | 1w | +7.32% (n=3957) | +1.00% (n=4947) | -0.18% (n=3391) | +16.87% (n=1458) | 0% | 101% | **KILL** | disc_t,hold_t,disc_sign,lottery_day,spy_regime,no_edge_vs_uncond,q1_sign,ticker_ghost,regime_split |
| five-cell light + green O | I (daily print) | 2w | -0.08% (n=3852) | -0.12% (n=4860) | +1.50% (n=3306) | +0.20% (n=1382) | 0% | 117% | **KILL** | disc_t,hold_t,hold_sign,lottery_day,spy_regime,no_edge_vs_uncond,ticker_ghost,regime_split |
| five-cell light + green O | I stacked | 1d | +2.04% (n=4012) | +2.73% (n=5049) | +1.63% (n=3428) | +1.67% (n=1458) | 0% | 10% | **KEEP** | — |
| five-cell light + green O | I stacked | 2d | +2.08% (n=4012) | +2.54% (n=5049) | +1.70% (n=3428) | +2.23% (n=1458) | 0% | 13% | **KEEP** | — |
| five-cell light + green O | I stacked | 3d | +3.48% (n=3992) | +3.68% (n=4991) | +1.67% (n=3428) | +2.68% (n=1458) | 0% | 27% | **KILL** | ticker_ghost |
| five-cell light + green O | I stacked | 1w | +8.22% (n=3957) | +3.97% (n=4947) | +1.52% (n=3391) | +14.97% (n=1458) | 0% | 53% | **KILL** | hold_t,ticker_ghost |
| five-cell light + green O | I stacked | 2w | +4.55% (n=3852) | +5.19% (n=4860) | +3.16% (n=3306) | +7.53% (n=1382) | 0% | 45% | **KILL** | ticker_ghost |
| light+O ∧ AH≥1 | H (intraday) | 1d | +3.51% (n=1277) | +3.84% (n=1700) | +3.27% (n=1080) | +3.83% (n=397) | 0% | 10% | **KEEP** | — |
| light+O ∧ AH≥1 | H (intraday) | 2d | -0.16% (n=1277) | -0.33% (n=1700) | -0.03% (n=1080) | +0.56% (n=397) | 0% | -93% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,q3_split,no_edge_vs_uncond,q1_sign |
| light+O ∧ AH≥1 | H (intraday) | 3d | -0.17% (n=1277) | +0.43% (n=1700) | -0.51% (n=1080) | -0.14% (n=397) | 0% | 308% | **KILL** | disc_t,hold_t,hold_sign,tape_split,spy_regime,q3_sign,q3_split,no_edge_vs_uncond,ticker_ghost,regime_split |
| light+O ∧ AH≥1 | H (intraday) | 1w | -0.25% (n=1266) | -0.25% (n=1686) | -0.24% (n=1066) | -0.09% (n=397) | 0% | -49% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign |
| light+O ∧ AH≥1 | H (intraday) | 2w | -0.40% (n=1226) | -0.39% (n=1658) | -0.24% (n=1037) | -0.13% (n=367) | 0% | -28% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign |
| light+O ∧ AH≥1 | I (daily print) | 1d | +3.39% (n=1277) | +4.22% (n=1700) | +2.60% (n=1080) | +3.69% (n=397) | 0% | 16% | **KEEP** | — |
| light+O ∧ AH≥1 | I (daily print) | 2d | -0.18% (n=1277) | +0.02% (n=1700) | -0.08% (n=1080) | +0.47% (n=397) | 0% | 1065% | **KILL** | disc_t,hold_t,hold_sign,lottery_day,spy_regime,q3_split,no_edge_vs_uncond,q1_sign,ticker_ghost,regime_split |
| light+O ∧ AH≥1 | I (daily print) | 3d | +2.69% (n=1277) | +2.73% (n=1700) | -0.54% (n=1080) | -0.13% (n=397) | 0% | 96% | **KILL** | disc_t,hold_t,spy_regime,no_edge_vs_uncond,q1_sign,ticker_ghost,regime_split |
| light+O ∧ AH≥1 | I (daily print) | 1w | +3.36% (n=1266) | +2.60% (n=1686) | -0.40% (n=1066) | -0.05% (n=397) | 0% | 124% | **KILL** | disc_t,hold_t,disc_sign,lottery_day,spy_regime,q3_split,no_edge_vs_uncond,q1_sign,ticker_ghost,regime_split |
| light+O ∧ AH≥1 | I (daily print) | 2w | -0.47% (n=1226) | -0.58% (n=1658) | -0.38% (n=1037) | +0.02% (n=367) | 0% | -31% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond |
| light+O ∧ AH≥1 | I stacked | 1d | +3.39% (n=1277) | +4.22% (n=1700) | +2.60% (n=1080) | +3.69% (n=397) | 0% | 16% | **KEEP** | — |
| light+O ∧ AH≥1 | I stacked | 2d | +3.42% (n=1277) | +3.82% (n=1700) | +2.80% (n=1080) | +4.64% (n=397) | 0% | 24% | **KEEP** | — |
| light+O ∧ AH≥1 | I stacked | 3d | +7.20% (n=1277) | +6.78% (n=1700) | +2.41% (n=1080) | +5.77% (n=397) | 0% | 45% | **KILL** | hold_t,ticker_ghost |
| light+O ∧ AH≥1 | I stacked | 1w | +6.82% (n=1266) | +7.15% (n=1686) | +1.55% (n=1066) | +4.53% (n=397) | 0% | 41% | **KILL** | ticker_ghost |
| light+O ∧ AH≥1 | I stacked | 2w | +10.00% (n=1226) | +9.50% (n=1658) | +1.61% (n=1037) | +9.10% (n=367) | 0% | 58% | **KILL** | hold_t,q1_sign,ticker_ghost |
| light+O ∧ FR≥1 | H (intraday) | 1d | +2.48% (n=2222) | +2.77% (n=2753) | +2.40% (n=1857) | +2.54% (n=790) | 0% | 9% | **KEEP** | — |
| light+O ∧ FR≥1 | H (intraday) | 2d | +0.07% (n=2222) | +0.00% (n=2753) | +0.24% (n=1857) | +0.19% (n=790) | 0% | 58% | **KILL** | disc_t,hold_t,tape_split,q3_sign,q3_split,no_edge_vs_uncond,ticker_ghost |
| light+O ∧ FR≥1 | H (intraday) | 3d | -0.02% (n=2210) | +0.24% (n=2719) | -0.15% (n=1857) | +0.04% (n=790) | 0% | 128% | **KILL** | disc_t,hold_t,hold_sign,tape_split,spy_regime,q3_sign,q3_split,no_edge_vs_uncond,q1_sign,ticker_ghost,regime_split |
| light+O ∧ FR≥1 | H (intraday) | 1w | -0.22% (n=2186) | -0.18% (n=2694) | -0.15% (n=1832) | +0.19% (n=790) | 0% | -50% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign |
| light+O ∧ FR≥1 | H (intraday) | 2w | -0.02% (n=2117) | -0.02% (n=2638) | -0.12% (n=1778) | +0.06% (n=742) | 0% | -154% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign |
| light+O ∧ FR≥1 | I (daily print) | 1d | +2.34% (n=2222) | +3.04% (n=2753) | +2.04% (n=1857) | +2.10% (n=790) | 0% | 15% | **KEEP** | — |
| light+O ∧ FR≥1 | I (daily print) | 2d | -0.03% (n=2222) | +0.12% (n=2753) | +0.09% (n=1857) | +0.18% (n=790) | 0% | 225% | **KILL** | disc_t,hold_t,hold_sign,lottery_day,no_edge_vs_uncond,ticker_ghost |
| light+O ∧ FR≥1 | I (daily print) | 3d | +1.60% (n=2210) | +1.72% (n=2719) | -0.27% (n=1857) | +0.04% (n=790) | 0% | 95% | **KILL** | disc_t,hold_t,spy_regime,no_edge_vs_uncond,q1_sign,ticker_ghost,regime_split |
| light+O ∧ FR≥1 | I (daily print) | 1w | +12.81% (n=2186) | +1.45% (n=2694) | -0.23% (n=1832) | +30.96% (n=790) | 0% | 102% | **KILL** | disc_t,hold_t,disc_sign,lottery_day,spy_regime,no_edge_vs_uncond,q1_sign,ticker_ghost,regime_split |
| light+O ∧ FR≥1 | I (daily print) | 2w | -0.12% (n=2117) | -0.25% (n=2638) | +2.71% (n=1778) | +0.23% (n=742) | 0% | 123% | **KILL** | disc_t,hold_t,hold_sign,lottery_day,tape_split,spy_regime,q3_split,no_edge_vs_uncond,q1_sign,ticker_ghost,regime_split |
| light+O ∧ FR≥1 | I stacked | 1d | +2.34% (n=2222) | +3.04% (n=2753) | +2.04% (n=1857) | +2.10% (n=790) | 0% | 15% | **KEEP** | — |
| light+O ∧ FR≥1 | I stacked | 2d | +2.44% (n=2222) | +2.89% (n=2753) | +2.15% (n=1857) | +2.54% (n=790) | 0% | 20% | **KEEP** | — |
| light+O ∧ FR≥1 | I stacked | 3d | +4.70% (n=2210) | +4.80% (n=2719) | +1.97% (n=1857) | +3.22% (n=790) | 0% | 38% | **KILL** | ticker_ghost |
| light+O ∧ FR≥1 | I stacked | 1w | +12.77% (n=2186) | +4.90% (n=2694) | +1.62% (n=1832) | +25.83% (n=790) | 0% | 67% | **KILL** | hold_t,ticker_ghost |
| light+O ∧ FR≥1 | I stacked | 2w | +6.29% (n=2117) | +7.20% (n=2638) | +4.29% (n=1778) | +11.45% (n=742) | 0% | 59% | **KILL** | ticker_ghost |
| light+O ∧ AH≥1 ∧ FR≥1 | H (intraday) | 1d | +3.93% (n=796) | +4.27% (n=1011) | +3.85% (n=680) | +4.48% (n=256) | 0% | 14% | **KILL** | hold1_without_hold2 |
| light+O ∧ AH≥1 ∧ FR≥1 | H (intraday) | 2d | -0.16% (n=796) | -0.20% (n=1011) | -0.05% (n=680) | +0.47% (n=256) | 0% | -295% | **KILL** | disc_t,hold_t,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,q3_split,no_edge_vs_uncond,q1_sign |
| light+O ∧ AH≥1 ∧ FR≥1 | H (intraday) | 3d | -0.03% (n=796) | +0.71% (n=1011) | -0.70% (n=680) | -0.06% (n=256) | 0% | 155% | **KILL** | disc_t,hold_t,hold_sign,tape_split,spy_regime,q3_sign,q3_split,no_edge_vs_uncond,ticker_ghost,regime_split |
| light+O ∧ AH≥1 ∧ FR≥1 | H (intraday) | 1w | -0.68% (n=787) | -0.48% (n=1001) | -0.40% (n=670) | -0.05% (n=256) | 0% | -35% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign |
| light+O ∧ AH≥1 ∧ FR≥1 | H (intraday) | 2w | -0.34% (n=758) | -0.23% (n=988) | -0.34% (n=650) | +0.11% (n=238) | 0% | -60% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign |
| light+O ∧ AH≥1 ∧ FR≥1 | I (daily print) | 1d | +3.81% (n=796) | +4.70% (n=1011) | +3.26% (n=680) | +4.29% (n=256) | 0% | 22% | **KILL** | hold1_without_hold2 |
| light+O ∧ AH≥1 ∧ FR≥1 | I (daily print) | 2d | -0.22% (n=796) | +0.39% (n=1011) | -0.01% (n=680) | +0.29% (n=256) | 0% | 307% | **KILL** | disc_t,hold_t,hold_sign,lottery_day,spy_regime,q3_split,no_edge_vs_uncond,q1_sign,ticker_ghost,regime_split |
| light+O ∧ AH≥1 ∧ FR≥1 | I (daily print) | 3d | +4.34% (n=796) | +4.48% (n=1011) | -1.04% (n=680) | +0.06% (n=256) | 0% | 101% | **KILL** | disc_t,hold_t,spy_regime,no_edge_vs_uncond,q1_sign,ticker_ghost,regime_split |
| light+O ∧ AH≥1 ∧ FR≥1 | I (daily print) | 1w | +4.32% (n=787) | +3.59% (n=1001) | -0.66% (n=670) | +0.09% (n=256) | 0% | 133% | **KILL** | disc_t,hold_t,disc_sign,lottery_day,spy_regime,q3_split,no_edge_vs_uncond,q1_sign,ticker_ghost,regime_split |
| light+O ∧ AH≥1 ∧ FR≥1 | I (daily print) | 2w | -0.51% (n=758) | -0.67% (n=988) | -0.54% (n=650) | +0.49% (n=238) | 0% | -31% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign |
| light+O ∧ AH≥1 ∧ FR≥1 | I stacked | 1d | +3.81% (n=796) | +4.70% (n=1011) | +3.26% (n=680) | +4.29% (n=256) | 0% | 22% | **KILL** | hold1_without_hold2 |
| light+O ∧ AH≥1 ∧ FR≥1 | I stacked | 2d | +3.93% (n=796) | +4.39% (n=1011) | +3.65% (n=680) | +5.30% (n=256) | 0% | 34% | **KILL** | ticker_ghost |
| light+O ∧ AH≥1 ∧ FR≥1 | I stacked | 3d | +10.03% (n=796) | +9.28% (n=1011) | +2.80% (n=680) | +7.34% (n=256) | 0% | 56% | **KILL** | hold_t,ticker_ghost |
| light+O ∧ AH≥1 ∧ FR≥1 | I stacked | 1w | +8.22% (n=787) | +8.83% (n=1001) | +1.48% (n=670) | +5.55% (n=256) | 0% | 54% | **KILL** | hold_t,ticker_ghost |
| light+O ∧ AH≥1 ∧ FR≥1 | I stacked | 2w | +13.61% (n=758) | +13.18% (n=988) | -0.13% (n=650) | +11.85% (n=238) | 0% | 73% | **KILL** | disc_t,hold_t,spy_regime,q1_sign,ticker_ghost,regime_split |

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

### Soft regimes (standing 1d KEEPs + F-green)

Sheet heat is the morning five-cell sum (hot ≥5 / mixed / cold ≤0). The standing light **is** the hot bucket — mixed/cold are empty on those rows. Tape is SPY up / down / flat (|SPY| < 15 bp). A global KEEP needs SPY-up and SPY-down to agree.

| recipe | label | spy↑ | spy↓ | spy flat | heat hot | agree? |
|---|---|---|---|---|---|---|
| five-cell light + green O | H (intraday) | +2.43% (n=5049) | +1.98% (n=3428) | +1.96% (n=1458) | +2.21% (n=9935) | yes |
| five-cell light + green O | I (daily print) | +2.73% (n=5049) | +1.63% (n=3428) | +1.67% (n=1458) | +2.20% (n=9935) | yes |
| five-cell light + green O | I stacked | +2.73% (n=5049) | +1.63% (n=3428) | +1.67% (n=1458) | +2.20% (n=9935) | yes |
| light+O ∧ AH≥1 | H (intraday) | +3.84% (n=1700) | +3.27% (n=1080) | +3.83% (n=397) | +3.64% (n=3177) | yes |
| light+O ∧ AH≥1 | I (daily print) | +4.22% (n=1700) | +2.60% (n=1080) | +3.69% (n=397) | +3.60% (n=3177) | yes |
| light+O ∧ AH≥1 | I stacked | +4.22% (n=1700) | +2.60% (n=1080) | +3.69% (n=397) | +3.60% (n=3177) | yes |
| light+O ∧ FR≥1 | H (intraday) | +2.77% (n=2753) | +2.40% (n=1857) | +2.54% (n=790) | +2.61% (n=5400) | yes |
| light+O ∧ FR≥1 | I (daily print) | +3.04% (n=2753) | +2.04% (n=1857) | +2.10% (n=790) | +2.56% (n=5400) | yes |
| light+O ∧ FR≥1 | I stacked | +3.04% (n=2753) | +2.04% (n=1857) | +2.10% (n=790) | +2.56% (n=5400) | yes |
| light+O ∧ AH≥1 ∧ FR≥1 | H (intraday) | +4.27% (n=1011) | +3.85% (n=680) | +4.48% (n=256) | +4.15% (n=1947) | yes |
| light+O ∧ AH≥1 ∧ FR≥1 | I stacked | +4.70% (n=1011) | +3.26% (n=680) | +4.29% (n=256) | +4.14% (n=1947) | yes |
| F today is green (close, same-print) | I stacked 1d | +2.87% (n=89698) | +2.63% (n=59668) | +1.65% (n=22757) | +0.75% (n=80034) | yes |

### Soft regimes (notes)

Standing light+O ± AH or FR: both SPY tapes green on 1d H, 1d I, and 1d stacked I. Flat tape is also green. Heat is hot by construction. 3d+ stacked I stays KILL (five-name ghost) even when both tapes are green. Light+O ∧ both AH and FR fails the 2d name-ghost bar, so 1d is not a keep. F-green agrees on both tapes but is a same-close volume print, not a forecast.

### What this does not change

- Standing open Futubull keep is still the research baseline, not a card. This file only re-scores it on H/I.
- Open-entry locked-44 pair+lag stays the accepted null (KEEP 0 / KILL 4266).
- T / BA / CZ / EH / IB / HO / IL / GV stay KILL as highlight ghosts — not this search.
- Same-row H and I are never features.
- Finviz BLOCKED. Live `flatten_robust` frozen. No cards.
- Q1 cut **2026-04-01**. Q3 cut **2026-07-01**.

Board totals (all labels × horizons): KEEP 85 · KILL 1227 · THIN 0.

Research only. One 2026 regime.

