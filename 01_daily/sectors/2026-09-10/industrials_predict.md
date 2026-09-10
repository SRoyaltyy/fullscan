# Sector Prediction — Industrials — 2026-09-10

- ETF: **XLI**
- rubric: `00_grounding/sectors/industrials.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-4.05** (mult 0.9)
- regime: risk_off
- divergence_flagged: **False**

## Channel 1 sector ETF tape

```
ETF XLI vs SPY (yfinance, through 2026-09-09):
  1d: XLI -1.51% | SPY -0.46% | rel -1.04%
  3d: XLI -1.59% | SPY -1.39% | rel -0.19%
  1w: XLI -0.54% | SPY +0.08% | rel -0.63%
  1m: XLI -6.94% | SPY -1.38% | rel -5.56%
```

MEMORY_CONFIRM: Memory index unavailable this run (embedding metadata mismatch); used injected Industrials scoreboard + 08-11..09-09 sector logs. Rolling dir=0.2 / mag=0.1 (n=10); last 30 dir=0.25 / mag=0.083 (n=12). Last graded 08-28: narrative down/mild vs pipeline down/flat, actual XLI −0.93% (dir HIT, mag MISS on pipeline flat). 09-01 down/mild, 09-02 flat/flat, 09-03 flat/flat (missed +1.03% on ISM Services beat), 09-04 down/flat (missed +0.41% up on laggard-shield), 09-08 flat/flat (pipeline −3.6 vs narrative down/mild, actual XLI −0.485% — dir MISS), 09-09 flat/flat (pipeline −4.05 vs narrative down/mild, actual XLI −1.51% — dir MISS). **Governing today: 09-09 lesson (A-category) — when the tape CONFIRMS the negative score (1d rel negative) on a live oil-shock day, do NOT let sector_rs_veto/calendar_size_gate override the narrative's down call to flat; emit down/mild.** Also 09-08 (reconcile narrative vs pipeline explicitly), 09-04 laggard-shield (score the laggard ONCE, not in both S2 and S4), 08-27 (1w/1m laggard → forbid up), 08-18 (cap S1 at 0/+1, don't use GEV/ETN as cushion), 08-11/08-12 supply-shock cap (verify live oil). DO-INSTEAD: when score fights tape, cut conviction / prefer flat/mild — **NOT binding today**: the tape (1d rel −1.04%, 1m rel −5.56%) CONFIRMS the negative score, so the 09-09 correction applies (emit the directional call, don't flatten).

## XLI near-session environment (not an SPX call)

### 1. Shared macro as it hits Industrials — S0 = −1
This is a **live, escalating geopolitical/oil supply-shock day** for a cyclical sector — the 09-08 shock has escalated, not faded.

- **Oil has breached $101 Brent.** Channel 1: WTI **$97.44 (+1.44%)**, Brent **$102.08 (+0.85%)**; `CL=F +1.38% / BZ=F +0.76%` 1d. News Judge #1: "Oil crosses $101 / Iran war escalation; SPX, Dow, Nasdaq close lower; yields pop" — the single dominant cross-asset driver. This is the **08-11/08-12 trigger** (live Hormuz/oil supply shock), now escalated. Do **not** call oil flat. For XLI, the oil spike is a **cost/stagflation headwind** for transports/manufacturers.
- **Futures do not confirm a bounce.** Channel 1: ES **+0.11%**, NQ **−0.17%**, RTY **+0.08%**, DJIA **+0.19%**. 08-21's ES/NQ ≥ +0.3% reversal gate is **off**. Asia composite **−0.56%** (Hang Seng −1.27%, ASX −1.03%), Europe **−0.06%**. Mixed-to-soft globals, not a risk-on impulse.
- **Rates: long end is the live pressure.** Channel 1: 30Y Bond **−0.32%** price (yield up), Ultra Bond **−0.37%**, 10Y Note **−0.12%**. DGS30 **5.25** / DGS10 **4.80** / DFII10 **2.43** (prior-close). 5-day 10Y–SPX corr **−0.969** (strongly negative — rising yields crush equities). News Judge #2: Bessent's expanded Treasury buyback plan → **yields popped** (buybacks read as insufficient vs supply/inflation). This is a genuine duration/cyclical drag.
- **Fed path hawkish.** News Judge #3: Warsh's Jackson Hole hawkish comments → September hike odds up; gold slid >3%. This is **already paid** (08-28 → 09-09) — do not double-count, but the oil spike adds an inflation impulse that supports the hawkish side.
- **VIX 16.51 (+0.05 1d, +2.19 1w) with VIX/VIX3M 1.079 — BACKWARDATION.** Not a panic print, but the term structure is inverted (near-term stress > 3-month). HY OAS **2.67** (tight, +0.02 1w). Not a credit-stress crash, but a genuine risk-off overlay on a cyclical.

**S0 = −1, regime risk_off.** Not −2: VIX is not a panic print, credit is tight, no hard-data miss. Not 0: oil is confirmed up >$101 Brent on a live, escalating supply shock, the long end is selling off, and the 10Y–SPX corr is −0.969. Oil counted **once here**, not again in S1.

### 2. Spine + secondary — S1 = 0 (capped)
**No fresh same-morning industrials print in hand.** August ISM manufacturing already printed **09-01**: PMI **54.6** (expansion, 8th month) vs July 55.6; new orders **53.7** (−3.0 pts). Still **expansion** — **not** an ISM-contraction HIT, but **slowing**. ISM Services printed **09-03** (55.4). Both in the tape. 08-18/08-27: **cap S1 at 0/+1**; +2 is forbidden without same-morning confirmation.

- **Grid / AI power — HIT, carried.** GEV ~$176B RPO / 116 GW gas book remains structural. Finviz digest: **BE (Bloom Energy)** target raised to $330 on expected S&P inclusion; **AME** completed $5.0B Indicor acquisition (stale M&A, 08-26). But 08-18: **not** a downside cushion and **not** a same-session raise on an oil-shock day — GEV/ETN can still roll.
- **Aerospace & defense — MIXED.** The Iran/Houthi escalation is a defense-order narrative, but defense names have been volatile on this conflict (Fortune: defense-tech investors burned in the Iran war). SPEEA talks resumed (constructive, not a strike). Do **not** cancel ISM (expansion) with one award, and do **not** treat geo as a fresh defense-order HIT.
- **Freight — MIXED / negative-leaning.** Oil >$101 is a direct **fuel-cost headwind** for trucking/air freight. Cass trucking still soft; rail carloads modestly positive. Not a same-morning recovery HIT.
- **Construction slowdown — HIT, carried.** Manufacturing construction off the 2025 peak; AI/nonres is the offset, not a broad build boom.
- **Copper −2.89%** (Channel 1) is a **global-growth/industrial-demand negative** — a direct read-through to electrical equipment/machinery demand expectations.

Net: carried ISM expansion (slowing) + structural grid vs oil-cost headwind + copper fade + mixed freight. **S1 = 0** (capped; no fresh same-morning confirmation, and the oil/copper complex is a live negative for the cyclical spine).

### 3. Breadth — S2 = −1
XLI is a **deep laggard**. Channel 1 through 09-09: 1d rel **−1.04%**, 3d **−0.19%**, 1w **−0.63%**, 1m **−5.56%**. Leadership is large-cap AI-power carry (GEV/BE), not % of names expanding. Score the lag **once** here (09-04: do not double-count in S4).

### 4. Flows — S3 = 0
Checked, nothing material returned on XLI flows this morning. Rotation has been out of industrials into tech/AI-infra. Not a crowded long (1m rel −5.56%). **S3 = 0.**

### 5. ETF tape (confirmation only) — S4 = −1
Channel 1 through 09-09: 1d rel **−1.04%** (decisively negative), 3d **−0.19%**, 1w **−0.63%**, 1m **−5.56%**. The 1d tape is **negative and confirming** — this is the 09-09 trigger condition (tape confirms the negative score → do NOT flatten). Confirmation of underperformance, not an independent second thesis.

### 6. Catalysts / calendar
- **Oil >$101 / Iran escalation** — dominant macro driver (risk-off, stagflation impulse).
- **Bessent expanded Treasury buyback → yields pop** — rates channel, long-end pressure.
- **Warsh hawkish / September hike odds up** — already paid, but oil adds inflation impulse.
- **Nvidia AI deal + Dell server backlog → semis rally** (AMAT +5%, LITE +10%, FIX +11%, ALAB +12%) — the one live bullish sector force, but it is **XLK/semis beta, not XLI** (08-27: do not map non-holdings mega-cap AHR into XLI S0 = +1).
- **Copper −2.89%, silver −2.43%** — industrial-demand negative read-through.

### Self-audit
- Lens: cyclical; rates only in S0, not re-counted in S1.
- Band: **mild**, not notable (VIX not a panic print, credit tight, no hard-data miss; but the tape confirms down).
- Skew: GEV/BE do not drive the ETF call.
- Same-shock: oil counted once in S0; laggard counted once in S2 (not S4).
- 09-09 correction: tape (1d rel −1.04%) CONFIRMS the negative score → emit **down/mild**, do NOT let sector_rs_veto/calendar_size_gate flatten to flat.
- 08-27: 1w/1m laggard + non-holdings AHR → forbid up. Applied.

**Divergence:** Leading factors (S0 −1 oil/rates, S2 −1 laggard, S4 −1 confirming tape) point down, and the tape **confirms** (1d rel −1.04%). No divergence — factors and tape agree. Per 09-09, this is the configuration where the directional call must be emitted, not flattened.

**Final: down/mild.** Σ(S0..S4) = −1 + 0 − 1 + 0 − 1 = −3; × mult 0.9 = −2.7 → down/mild. Confidence 0.55 (modest |score|, rolling mag discipline).

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: 0
S2_BREADTH: -1
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: -1
MULTIPLIER: 0.9
CONFIDENCE: 0.55
REGIME: risk_off
PREDICTED_DIRECTION: down
PREDICTED_MAGNITUDE_BAND: mild
TOTAL_SCORE: -2.7
DIVERGENCE_FLAGGED: False
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-off tape / flight to safety|HIT|0.85|2026-09-10|https://www.reuters.com/markets/
Real yields rising|HIT|0.75|2026-09-10|https://www.cnbc.com/bonds/
Sector breadth failure (ETF up, names flat)|HIT|0.7|2026-09-10|https://finviz.com/quote.ashx?t=XLI
Grid / electrical equipment backlog (AI power)|HIT|0.6|2026-09-10|https://www.reuters.com/business/energy/
Freight recession|HIT|0.55|2026-09-10|https://www.ttnews.com/
Construction slowdown|HIT|0.5|2026-09-10|https://www.census.gov/construction/
Sector rotation out of industrials|HIT|0.6|2026-09-10|https://www.morningstar.com/
Aerospace & defense order / budget upside|MIXED|0.4|2026-09-10|https://www.defensenews.com/
ISM manufacturing / new orders expansion|MIXED|0.5|2026-09-10|https://www.ismworld.org/
Sector ETF inflow / relative volume spike|CHECKED_NOTHING_MATERIAL|0.3|2026-09-10|https://etfdb.com/etf/XLI/
HIT_GRID_END

HORIZON_3D: down/mild
HORIZON_1W: down/mild
HORIZON_2W: flat/mild
HORIZON_1M: flat/mild

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': 0.0, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': -1.0}, 'multiplier': 0.9, 'leading_sum': -4.0, 'divergence_flagged': False, 'total_score': -4.05, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.55, 'regime': 'risk_off'}
```
