# Sector Prediction — Basic Materials — 2026-09-11

- ETF: **XLB**
- rubric: `00_grounding/sectors/basic_materials.md`
- predicted_direction: **flat**
- predicted_magnitude_band: **flat**
- total_score: **0.0** (mult 0.9)
- regime: mixed
- divergence_flagged: **False**

## Channel 1 sector ETF tape

```
ETF XLB vs SPY (yfinance, through 2026-09-10):
  1d: XLB -1.23% | SPY -0.60% | rel -0.63%
  3d: XLB -3.20% | SPY -1.60% | rel -1.60%
  1w: XLB -4.14% | SPY -0.96% | rel -3.18%
  1m: XLB -4.66% | SPY -1.65% | rel -3.01%
```

MEMORY_CONFIRM: Sector Basic Materials / XLB only. Memory index unavailable this run (embedding metadata missing) — used injected scoreboard/lessons only. Rolling last-10 dir=0.6 mag=0.7 (n=10); last graded 2026-09-10 down/notable vs XLB −1.23% / SPY −0.60% / rel −0.63% (dir HIT, mag HIT). Active XLB rules checked: **09-10 gap-at-open rule is the BINDING magnitude rule** — if |open vs prior close| ≥ 1.0%, the mild band is falsified at the bell and must be raised to at least notable; **09-09 composition/magnitude rule** — when the 8/18 metals-co-move floor ban fires with all four S1 sub-channels negative and zero offset, score S1=−2, S2=−1; **8/18 metals-as-floor ban** — do NOT use copper/gold as a floor on oil-shock risk-off days; **8/14 gold-offset** — score the monetary bid only if gold/silver are green; **8/25 composition/transmission** — chemicals ~40–50% of XLB vs copper miners ~10–15%; **8/27 S4-cap** — 1d rel <0.5% cannot be a ± confirmation; **8/28 leftover-S2/S4 down-mandate** — S4 confirms only the session being predicted; **09-03 exhaustion-bounce** — sleeve-driven >+1% reversal within a negative 1w is a bounce, not an inflection; **09-04 T-1-lag** — do not copy a prior-day lag into S4 as fresh. No open experiment for `sector_basic_materials`. DO-INSTEAD: keep direction; shrink confidence on modest |score| when magnitude historically misses.

## Analysis — XLB, session of 2026-09-11

This is a **CPI-day risk-on bounce attempt after a four-session slide**, with the oil/Hormuz shock **de-escalating at the margin** (WTI −2.54% to $99.91, Brent −2.86% to $104.62) but still above $100 Brent. It is **not** a copper-squeeze day and **not** a fresh kinetic escalation. Channel 1 tape through 09-10 is decisively negative across every horizon: 1d rel **−0.63%**, 3d **−1.60%**, 1w **−3.18%**, 1m **−3.01%**.

### 1. Shared macro as it hits materials (S0)

The dominant live driver is the **scheduled CPI print** (News Judge #1: futures rise after a 4-day slide, CPI looms large). This is a **two-sided binary** — do not pre-score the print. What *is* live and knowable at the open:

- **Futures are green and confirming**: ES **+0.63%**, NQ **+0.65%**, Russell **+0.63%**, DJIA **+0.53%** — all ≥ +0.5%, so the 08-21 reversal checklist is **ON** (this is a ban on a stale down call, not a license for up).
- **Oil is offered**: WTI **−2.54%**, Brent **−2.86%** — the stagflation spine is **easing**, not escalating. This is the inverse of the 09-09/09-10 fresh-kinetic setup. The 8/18 metals-co-move floor ban is **not cleanly firing** (oil is down, not spiking into equity risk-off).
- **Rates**: DGS10 **4.83 (+0.03 1d, +0.11 1m)**, DGS30 **5.28**, DFII10 **2.46 (+0.03 1d)** — real yields still elevated and grinding higher. 5-day 10Y–SPX corr **−0.745** (negative but less extreme than 09-10's −0.969). Live notes are **flat-to-marginally-bid** (10Y note +0.03%, 30Y +0.03%) — no fresh long-end smash.
- **USD**: DXY **+0.01% 1d / −0.91% 1m** — not a spike vs the complex.
- **VIX 17.24** with VIX/VIX3M **1.111 backwardation** — caution, not panic. EPU **275.99 (+26.8 1d, +78.88 1w)** — policy uncertainty elevated.
- **Asia red** (composite **−1.27%**: Nikkei −1.93%, Kospi −1.76%, Shanghai −1.18%) vs **Europe green** (composite **+0.53%**). Mixed, not a clean risk-off confirmation.

**S0 = 0.** Not +1 (CPI pending, real yields still rising, Asia red, backwardation). Not −1 (futures confirming green ≥ +0.5%, oil offered, USD flat, no fresh kinetic increment — the 09-09/09-10 risk-off spine has eased).

### 2. Spine + secondary (S1)

**Industrial metals — flat-to-marginally-firm, off the collapse.** Copper **$6.553 (+0.05%)** — stabilizing after the 09-10 −2.89% collapse. Aluminum **−1.84%**, iron ore **−0.69%**, steel HRC **−0.08%**. Spine "surge" **off**; spine "collapse" **off** (copper is flat, not falling). The 09-10 collapse has stopped.

**Monetary metals — flat-to-soft.** Gold **$4,393.1 (−0.33%)**, silver **$64.665 (−0.40%)**. Not green, so 8/14 does **not** pay. Platinum **+0.27%**, palladium **+1.57%** — a mild PGM bid, not a monetary-metals surge.

**China demand — still the industrial offset.** August NBS mfg **49.8** (still <50), construction **46.9**. T-1, not a same-morning miss, and **not** a rebound.

**Oil offered = cost relief for the chemicals majority sleeve.** The chemicals-heavy book (LIN ~13%, SHW, ECL — ~40–50% combined) gets **feedstock/energy cost relief** from WTI −2.54% / Brent −2.86%. This is the **inverse** of the 09-08/09-09/09-10 oil-cost squeeze. Per 8/25 composition-weighting, this is a genuine majority-sleeve positive.

**Supply disruption / tariffs — stale.** DRC concentrate ban and Section 232 copper remain on the books; not a same-open catalyst. APD's Q3 beat/raise (Finviz digest) is **already traded** and carries a $2.9B clean-energy exit charge — a single-name positive, not an XLB-wide thrust. BHP ADRs fell on copper retreating from records amid US tariff uncertainty — a miner-sleeve drag.

**S1 = 0.** Oil-offered cost relief for the chemicals majority sleeve offsets the flat copper / soft gold / China contraction. Not +1: no metal surge, gold not green, China still <50, BHP/tariff overhang. Not −1: the 09-10 metals collapse has stopped, oil is offering cost relief, copper is flat not falling.

### 3. Breadth (S2)

The 09-09/09-10 uniform-negative breadth (chemicals + metals + gold all down together) has **broken**: oil is offering chemicals cost relief while copper stabilizes. This is a **compositional split**, not a clean breadth expansion or failure. Per 8/28, do not copy the prior-session lag into S2; require same-morning confirmation. **S2 = 0.**

### 4. Flows / positioning (S3)

XLB ~1m net outflows from prior logs (~−$180M range). Not a washout, not a volume spike. **S3 = 0.**

### 5. Tape (S4, confirmation only)

1d rel **−0.63%** — negative but sub-0.5% threshold is breached on the negative side; per 8/27 the S4-cap governs *positive* confirmation from a sub-0.5% tape. The prior-day lag is **T-1** (09-10 close) and per 09-04 must not be copied as fresh. The live premarket tape (futures green, oil offered) is **not** confirming continued downside. **S4 = 0.**

### Reconciliation

Total = (0 + 0 + 0 + 0 + 0) × 0.9 = **0.0 → flat/flat**.

**Gap check (09-10 binding rule):** XLB prior close ~50.76; premarket futures are green and the ETF is not gapping ≥1% in either direction. The 09-10 gap rule does **not** fire — no same-morning magnitude print forces a band upgrade.

**Divergence:** leading factors (S0–S3 net 0) and tape (S4 0) agree on flat. No divergence.

**CPI caveat:** a high-impact print is pending. Per the 09-03 lesson, when a scheduled binary is the load-bearing catalyst and S0/S2/S4 are neutral, do not let S1 alone create a signed direction — emit flat/no-sign. The band is held at flat with reduced confidence; the CPI resolution is the dominant variance source and is not knowable at the snapshot.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: 0
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.9
CONFIDENCE: 0.5
REGIME: mixed
SECTOR_SCORES_END

HORIZON_3D: flat/flat
HORIZON_1W: down/mild
HORIZON_2W: down/mild
HORIZON_1M: down/mild

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion | HIT | 0.70 | 2026-09-11 | https://www.finviz.com/
Risk-off tape / flight to safety | MISS | 0.60 | 2026-09-11 | https://www.finviz.com/
Real yields rising | HIT | 0.65 | 2026-09-11 | https://fred.stlouisfed.org/series/DFII10
USD weakening | MISS | 0.55 | 2026-09-11 | https://www.finviz.com/
Industrial metal price collapse | MISS | 0.60 | 2026-09-11 | https://www.finviz.com/
Gold/silver price surge (monetary metals) | MISS | 0.65 | 2026-09-11 | https://www.finviz.com/
China demand shock / property stress | HIT | 0.60 | 2026-09-11 | https://www.reuters.com/
Margin compression / cost inflation without pricing power | MISS | 0.55 | 2026-09-11 | https://www.finviz.com/
Sector rotation out of materials | HIT | 0.60 | 2026-09-11 | https://www.finviz.com/
Sector breadth failure (ETF up, names flat) | MISS | 0.50 | 2026-09-11 | https://www.finviz.com/
Sector ETF outflow / volume dry-up | HIT | 0.50 | 2026-09-11 | https://www.finviz.com/
Crowded long (extreme relative performance + valuation) | MISS | 0.55 | 2026-09-11 | https://www.finviz.com/
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': 0.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.9, 'leading_sum': 0.0, 'divergence_flagged': False, 'total_score': 0.0, 'predicted_direction': 'flat', 'predicted_magnitude_band': 'flat', 'confidence_score': 0.5, 'regime': 'mixed'}
```
