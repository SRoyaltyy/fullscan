# Sector Prediction — Basic Materials — 2026-09-10

- ETF: **XLB**
- rubric: `00_grounding/sectors/basic_materials.md`
- predicted_direction: **down**
- predicted_magnitude_band: **notable**
- total_score: **-9.0** (mult 0.9)
- regime: risk_off
- divergence_flagged: **True**

## Channel 1 sector ETF tape

```
ETF XLB vs SPY (yfinance, through 2026-09-09):
  1d: XLB -1.06% | SPY -0.46% | rel -0.59%
  3d: XLB -2.34% | SPY -1.39% | rel -0.94%
  1w: XLB -1.31% | SPY +0.08% | rel -1.39%
  1m: XLB -3.37% | SPY -1.38% | rel -1.99%
```

MEMORY_CONFIRM: Sector Basic Materials / XLB only. Memory index unavailable this run (embedding metadata missing) — used injected scoreboard/lessons only. Rolling last-10 dir=0.6 mag=0.7 (n=10); last graded 2026-09-09 down/mild vs XLB −1.06% / SPY −0.46% / rel −0.59% (dir HIT, mag MISS — actual notable). Active XLB rules checked: **09-09 composition/magnitude lesson is the BINDING rule** — when the 8/18 metals-co-move floor ban fires (oil spiking into equity risk-off, copper/gold co-moving DOWN with equities), there is NO defensive pocket inside XLB; if all four S1 sub-channels align negative with zero offset, score S1=−2 and S2=−1, not −1/0. **8/18 metals-as-floor ban ON** (Brent >$102, WTI +1.44%, copper −2.89%, silver −2.43%, platinum −2.32% — metals co-moving down with equities, do NOT use metals as a floor). **8/14 gold-offset OFF** (GC −0.56%, SI −2.43%, not green). **8/25 composition/transmission** (chemicals ~40-50% of XLB vs copper miners ~10-15%). **8/27 S4-cap** (1d rel −0.59% <0.5% → S4 cannot be a ± confirmation). **8/28 leftover-S2/S4 down-mandate OFF** (S0/S1 live-negative, not net-zero). **09-03 exhaustion-bounce** (1w rel −1.39% still negative, sleeve-driven bounce already faded). **09-04 T-1-lag** (do not copy prior-day lag into S4 as fresh). No open experiment for `sector_basic_materials`. DO-INSTEAD: keep direction; shrink confidence on modest |score| when magnitude historically misses.

## Analysis — XLB, session of 2026-09-10

This is a **second consecutive fresh-kinetic Hormuz/oil escalation day** (News Judge #1: oil crosses **$101**, Iran war escalation, SPX/Dow/Nasdaq closed lower, yields popped). It is **not** a copper-squeeze day and **not** a leftover-chemicals fade. Channel 1 tape through 09-09 is decisively negative across every horizon: 1d rel **−0.59%**, 3d **−0.94%**, 1w **−1.39%**, 1m **−1.99%**.

### 1. Shared macro as it hits materials (S0)

The dominant live driver is the **oil/Iran escalation**: WTI **$97.44 (+1.44%)**, Brent **$102.08 (+0.85%)** — Brent above $100 for a second session. News Judge #1 is the single dominant cross-asset driver: it sets the risk-off tape, the inflation/Fed-path repricing, and the sector winners/losers (Energy up, cyclicals/duration down). News Judge #2 (Bessent expanded Treasury buyback → **yields popped**) is the rates channel: the buyback did **not** cap yields. News Judge #3 (Warsh hawkish JH → Sep hike odds up, gold slides >3%) is the regime-level hawkish repricing.

Live tape: ES **+0.11%**, NQ **−0.17%** (NQ weaker than ES — duration/growth selling, **not** the 8/25 tech-led green light). Asia composite **−0.56%** (Hang Seng −1.27%, ASX −1.03%). Europe **−0.06%**. VIX **16.51** with VIX/VIX3M **1.079 backwardation**. 5-day 10Y-SPX corr **−0.969** (strongly negative — rising yields hit equities broadly). DGS10 **4.80 (+0.02 1d, +0.15 1m)**, DGS30 **5.25**, DFII10 **2.43**. HY OAS **2.67** still tight. DXY **−0.02%** (not a USD spike).

The materials-specific overlay is the **8/18 metals-co-move floor ban firing cleanly**: oil spiking >$100 into equity risk-off, and the entire metals complex is **co-moving DOWN with equities** — copper **−2.89%**, silver **−2.43%**, platinum **−2.32%**, palladium **−1.48%**, gold **−0.56%**. This is risk-asset liquidation, not a hedge. Do **not** score S0 as risk_on because oil is up.

**S0 = −1.** Fresh Hormuz/oil escalation >$100 + hawkish Fed + equity risk-off + backwardation map negative to this cyclical. Not −2: ES is only +0.11%, DXY is not a spike, no same-morning China print, CPI is tomorrow (09-11).

### 2. Spine + secondary (S1)

**Industrial metals — COLLAPSE, not surge.** Copper **−2.89%** to $6.681, silver **−2.43%**, platinum **−2.32%**. The record copper bid that supported XLB on 09-08 has fully reversed. Spine "surge" **off**; spine "collapse" is a **clean HIT** (copper down ~3% in a single session, off record highs).

**Inventory draw — off.** LME stocks rebuilt through mid-August; no fresh draw.

**China demand — still the industrial offset.** August NBS mfg **49.8** (still <50), construction **46.9**, property FAI **−19.2% YoY**. T-1, not a same-morning miss, and **not** a rebound. **Do not let gold cancel this — and today gold is not even green.**

**Monetary metals — FADE.** GC **−0.56%**, SI **−2.43%**. 8/14 does not pay. News Judge #5 confirms the metals complex is splitting: monetary metals down on real yields, industrial metals up on supply — but today **both** are down, so the split has collapsed into a uniform metals selloff.

**Oil-up is a cost headwind for processors, not an XLB squeeze.** Count Hormuz/oil once in S0 as the risk-off overlay; do not also credit copper as a positive floor (8/18). The chemicals-heavy book (LIN ~13%, SHW, ECL — ~40-50% combined) faces a direct oil feedstock/energy cost squeeze.

**Supply disruption / tariffs — stale.** DRC concentrate ban and Section 232 copper remain on the books; not a same-open catalyst. APD's Q3 beat/raise (News Judge #5 / Finviz digest) is **already traded** and carries a $2.9B clean-energy exit charge — a single-name positive, not an XLB-wide thrust.

**S1 = −2.** Per the 09-09 lesson: all four sub-channels align negative (chemicals oil-cost drag + copper collapse + gold/silver fade + China contraction) with **zero offsetting positive** anywhere in the book. The "not a collapse" cap does **not** apply — copper is down ~3%, a genuine collapse, and the minority sleeve that could have provided offset is also negative. Score S1 = −2.

### 3. Breadth (S2)

Per the 09-09 lesson: when the 8/18 metals-co-move pattern fires, there is **no defensive pocket** within XLB — chemicals, metals, and gold miners all decline together. Breadth is uniformly negative. Do **not** copy yesterday's lag into S2 as a fresh confirmation (8/28), but the live same-morning metals collapse (copper −2.89%, silver −2.43%) **is** same-morning confirmation of broad-based weakness across the ETF's sleeves.

**S2 = −1.**

### 4. Flows / positioning (S3)

XLB ~1m net outflows (~−$180M range from prior logs). Not a washout, not a volume spike. **S3 = 0.**

### 5. Tape (S4, confirmation only)

1d rel **−0.59%** is below the 0.5% confirmation threshold (8/27 S4-cap), so S4 cannot be a ± confirmation. The 3d/1w/1m are decisively negative but per 09-04 the prior-day lag is not automatically live confirmation. **S4 = 0.**

### Reconciliation

Total = (−1 + −2 + −1 + 0 + 0) × 0.9 = **−3.6** → **down/mild**.

Direction: **down**. Magnitude: **mild** (not notable — ES is only +0.11%, no same-morning China print, CPI is tomorrow, and rolling mag discipline favors mild on modest |score|). Confidence: **0.58** (direction well-supported by the live metals collapse + oil escalation; magnitude capped by the modest futures tape and the two-sided CPI-eve setup).

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: -2
S2_BREADTH: -1
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.9
CONFIDENCE: 0.58
REGIME: risk_off
SECTOR: Basic Materials
ETF: XLB
PREDICTED_DIRECTION: down
PREDICTED_MAGNITUDE_BAND: mild
TOTAL_SCORE: -3.6
DIVERGENCE_FLAGGED: False
HORIZON_3D: down/mild
HORIZON_1W: down/mild
HORIZON_2W: flat/mild
HORIZON_1M: flat/mild
SECTOR_SCORES_END

HIT_GRID_BEGIN
Industrial metal price collapse|HIT|0.85|2026-09-10|https://www.bloomberg.com/markets/commodities
Gold/silver price surge (monetary metals)|MISS|0.80|2026-09-10|https://www.kitco.com/
China demand shock / property stress|HIT|0.75|2026-09-10|https://www.reuters.com/markets/asia/
Margin compression / cost inflation without pricing power|HIT|0.70|2026-09-10|https://www.reuters.com/business/energy/
Risk-off tape / flight to safety|HIT|0.80|2026-09-10|https://www.cnbc.com/markets/
Real yields rising|HIT|0.65|2026-09-10|https://www.cnbc.com/bonds/
USD spike vs commodity complex|MISS|0.60|2026-09-10|https://www.reuters.com/markets/currencies/
Sector rotation out of materials|HIT|0.70|2026-09-10|https://www.reuters.com/markets/
Supply disruption (mine/export ban)|NEUTRAL|0.50|2026-09-10|https://www.reuters.com/markets/commodities/
Inventory draw (LME/exchange stocks down)|MISS|0.55|2026-09-10|https://www.lme.com/
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -2.0, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.9, 'leading_sum': -10.0, 'divergence_flagged': True, 'total_score': -9.0, 'predicted_direction': 'down', 'predicted_magnitude_band': 'notable', 'confidence_score': 0.58, 'regime': 'risk_off'}
```
