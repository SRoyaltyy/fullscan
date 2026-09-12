## Join verdict (light vs color vs leak-free joins)

_Generated 2026-09-07. Joins on top of the six KEEP lights. Futubull fees. Live `flatten_robust` frozen. No cards._

### Incremental layers

Same ship bar as harden: both 2026 halves (cut 2026-05-01), SPY up and down, top-day lottery, beat the previous layer by ≥20 bp. AB / weather / overnight book / Finviz **as-of** use the last file **dated before** the entry. Same-day files are not knowable at 9:30.

Finviz Elite dated exports: **21** days (2026-04-26–2026-09-06). One spring file (2026-04-26), then a gap until mid-August. That is not a 2026 history. The old snapshot `fz_volM` KEEP is **not shippable**.

Second-regime cut for surviving color folds: **2026-07-01** (Q1–Q2 vs Q3), past the May 1 half already used to KEEP the lights.

Grids **3603**, Yahoo/rows only.

### Light alone vs light+color vs joins

| layer | meaning | hold | holdout after fees | vs light | verdict | why |
|---|---|---|---|---|---|---|
| light alone | the five morning cells that are already known at 9:30 (A, B, C, G, J) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. | next 1 | +1.74% (n=10931) | — | **KEEP** | already hardened |
| light + color | best open color on that light (O green) | next 1 | +1.98% (n=4812) | +0.24 pp | **KEEP** | green O ≥20 bp |
| light alone | all nine morning cells known at 9:30 (A, B, C, G, J, K, L, M, O) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. | next 1 | +1.33% (n=10862) | — | **KEEP** | already hardened |
| light + color | best open color on that light (M green) | next 1 | +1.49% (n=3267) | +0.16 pp | **KILL** | no_edge_vs_parent |
| light alone | the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more). The light stays on until that sum falls to 0. | next 1 | +1.28% (n=11120) | — | **KEEP** | already hardened |
| light + color | best open color on that light (L green) | next 1 | +1.46% (n=6403) | +0.18 pp | **KILL** | no_edge_vs_parent |
| light alone | the five morning cells that are already known at 9:30 (A, B, C, G, J) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. | next 2 | +1.62% (n=10931) | — | **KEEP** | already hardened |
| light + color | best open color on that light (O green) | next 2 | +1.88% (n=4812) | +0.27 pp | **KEEP** | green O ≥20 bp |
| light alone | the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more). The light stays on until that sum falls to 0. | next 2 | +1.21% (n=11120) | — | **KEEP** | already hardened |
| light + color | best open color on that light (O green) | next 2 | +1.38% (n=4903) | +0.17 pp | **KILL** | no_edge_vs_parent |
| light alone | all nine morning cells known at 9:30 (A, B, C, G, J, K, L, M, O) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. | next 2 | +1.54% (n=10862) | — | **KEEP** | already hardened |
| light + color | best open color on that light (O green) | next 2 | +1.99% (n=5264) | +0.45 pp | **KEEP** | green O ≥20 bp |
| Finviz snapshot (not as-of) | today's high-vol tag on the light — current CSV, not 2026 history | next 1 | +2.22% (n=5287) | +0.48 pp | **BLOCKED** | no historical as-of; cannot ship |
| Finviz snapshot (not as-of) | today's high-vol tag on the light — current CSV, not 2026 history | next 2 | +2.19% (n=5238) | +0.64 pp | **BLOCKED** | no historical as-of; cannot ship |
| Finviz snapshot (not as-of) | today's high-vol tag on the light — current CSV, not 2026 history | next 2 | +2.02% (n=5287) | +0.40 pp | **BLOCKED** | no historical as-of; cannot ship |
| Finviz snapshot (not as-of) | today's high-vol tag on the light — current CSV, not 2026 history | next 1 | +1.65% (n=5238) | +0.32 pp | **BLOCKED** | no historical as-of; cannot ship |
| Finviz snapshot (not as-of) | today's high-vol tag on the light — current CSV, not 2026 history | next 1 | +1.61% (n=5553) | +0.33 pp | **BLOCKED** | no historical as-of; cannot ship |
| Finviz snapshot (not as-of) | today's high-vol tag on the light — current CSV, not 2026 history | next 2 | +1.45% (n=5553) | +0.24 pp | **BLOCKED** | no historical as-of; cannot ship |
| Finviz as-of high-vol | prior Elite export had Volatility (Month) > 8% | next 1 | +2.59% (n=806) | +1.30 pp | **KILL** | finviz_asof_gappy |
| Finviz as-of high-vol | prior Elite export had Volatility (Month) > 8% | next 2 | +2.39% (n=806) | +1.18 pp | **KILL** | finviz_asof_gappy,disc_t |
| Finviz as-of high-vol | prior Elite export had Volatility (Month) > 8% | next 1 | +2.82% (n=1425) | +1.53 pp | **KILL** | finviz_asof_gappy |
| Finviz as-of high-vol | prior Elite export had Volatility (Month) > 8% | next 2 | +2.34% (n=1425) | +1.13 pp | **KILL** | finviz_asof_gappy |
| Finviz as-of high-vol | prior Elite export had Volatility (Month) > 8% | next 1 | +4.02% (n=738) | +2.28 pp | **KILL** | finviz_asof_gappy |
| Finviz as-of high-vol | prior Elite export had Volatility (Month) > 8% | next 2 | +3.77% (n=738) | +2.15 pp | **KILL** | finviz_asof_gappy |
| Finviz as-of high-vol | prior Elite export had Volatility (Month) > 8% | next 1 | +4.08% (n=1311) | +2.34 pp | **KILL** | finviz_asof_gappy |
| Finviz as-of high-vol | prior Elite export had Volatility (Month) > 8% | next 2 | +3.61% (n=1311) | +1.99 pp | **KILL** | finviz_asof_gappy |
| Finviz as-of high-vol | prior Elite export had Volatility (Month) > 8% | next 1 | +2.18% (n=778) | +0.86 pp | **KILL** | finviz_asof_gappy |
| Finviz as-of high-vol | prior Elite export had Volatility (Month) > 8% | next 2 | +7.22% (n=778) | +5.68 pp | **KILL** | finviz_asof_gappy,disc_t,hold_t,lottery_day |
| Finviz as-of high-vol | prior Elite export had Volatility (Month) > 8% | next 1 | +2.49% (n=1244) | +1.17 pp | **KILL** | finviz_asof_gappy |
| Finviz as-of high-vol | prior Elite export had Volatility (Month) > 8% | next 2 | +5.18% (n=1244) | +3.63 pp | **KILL** | finviz_asof_gappy,hold_t,lottery_day |
| light + O + as-of high-vol | green O and prior Elite high-vol together | next 1 | +2.59% (n=806) | +1.30 pp | **KILL** | finviz_asof_gappy |
| light + O + as-of high-vol | green O and prior Elite high-vol together | next 2 | +2.39% (n=806) | +1.18 pp | **KILL** | finviz_asof_gappy,disc_t |
| light + O + as-of high-vol | green O and prior Elite high-vol together | next 1 | +4.02% (n=738) | +2.28 pp | **KILL** | finviz_asof_gappy |
| light + O + as-of high-vol | green O and prior Elite high-vol together | next 2 | +3.77% (n=738) | +2.15 pp | **KILL** | finviz_asof_gappy |
| light + O + as-of high-vol | green O and prior Elite high-vol together | next 1 | +2.18% (n=778) | +0.86 pp | **KILL** | finviz_asof_gappy |
| light + O + as-of high-vol | green O and prior Elite high-vol together | next 2 | +7.22% (n=778) | +5.68 pp | **KILL** | finviz_asof_gappy,disc_t,hold_t,lottery_day |
| AB (yesterday) | Same morning light as `hyst_open_core_e5_x2`, and yesterday's AB tape already liked the name. | next 2 | +1.27% (n=251) | -0.35 pp | **KILL** | date_bar,lottery_day,tape_split,no_edge_vs_parent |
| weather (yesterday) | Same morning light as `hyst_open_core_e5_x2`, and prior weather was risk-off. | next 2 | +2.21% (n=496) | +0.59 pp | **KILL** | date_bar,lottery_day,tape_split |
| overnight book (yesterday) | Same morning light as `hyst_open_core_e5_x2`, and the overnight 1-day book had it as a buy. | next 2 | +1.69% (n=7) | +0.07 pp | **KILL** | thin_disc,thin_hold,disc_t,hold_t,ticker_bar,date_bar,lottery_trade,lottery_day,tape_split,no_edge_vs_parent |

### Second regime (Q1–Q2 vs Q3) on light + green O

Held-out regime starts **2026-07-01**. KEEP only if Q3 stays green, still beats the parent light by 20 bp on the ticker-holdout, and clears the usual tape / lottery bar.

| meaning | hold | Q1–Q2 | Q3 (held out) | ticker-holdout | vs light | fattest day | verdict | why |
|---|---|---|---|---|---|---|---|---|
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more). The light stays on until that sum falls to 0. plus green O. | next 1 | +1.62% (n=8929) | +1.21% (n=3064) | +1.45% (n=4903) | +0.17 pp | 2.8% | **KILL** | no_edge_vs_parent |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more). The light stays on until that sum falls to 0. plus green O. | next 2 | +1.61% (n=8929) | +1.00% (n=3064) | +1.38% (n=4903) | +0.17 pp | 7.9% | **KILL** | no_edge_vs_parent |
| the five morning cells that are already known at 9:30 (A, B, C, G, J) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. plus green O. | next 1 | +2.15% (n=8836) | +1.78% (n=3048) | +1.98% (n=4812) | +0.24 pp | 5.0% | **KEEP** | — |
| the five morning cells that are already known at 9:30 (A, B, C, G, J) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. plus green O. | next 2 | +2.04% (n=8836) | +1.72% (n=3048) | +1.88% (n=4812) | +0.27 pp | 6.9% | **KEEP** | — |
| all nine morning cells known at 9:30 (A, B, C, G, J, K, L, M, O) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. plus green O. | next 1 | +1.57% (n=9636) | +0.98% (n=3307) | +1.37% (n=5264) | +0.04 pp | 2.9% | **KILL** | no_edge_vs_parent |
| all nine morning cells known at 9:30 (A, B, C, G, J, K, L, M, O) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. plus green O. | next 2 | +1.50% (n=9636) | +2.37% (n=3307) | +1.99% (n=5264) | +0.45 pp | 13.0% | **KEEP** | — |

**Join finding.** The only incremental KEEP versus the light is a green **O**. AB / weather / book are yesterday-knowable but too short (late Aug–Sep) — **KILL**. Finviz high-vol as a **snapshot** is **BLOCKED** (not 2026 history). Dated Elite exports are too gappy to ship. Light+O still has to clear Q3; see the table. Research only. No live wire.

