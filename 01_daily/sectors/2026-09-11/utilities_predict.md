# Sector Prediction — Utilities — 2026-09-11

- ETF: **XLU**
- rubric: `00_grounding/sectors/utilities.md`
- predicted_direction: **down**
- predicted_magnitude_band: **flat**
- total_score: **-2.925** (mult 0.9)
- regime: mixed
- divergence_flagged: **False**

## Channel 1 sector ETF tape

```
ETF XLU vs SPY (yfinance, through 2026-09-10):
  1d: XLU -0.98% | SPY -0.60% | rel -0.38%
  3d: XLU -1.30% | SPY -1.60% | rel +0.30%
  1w: XLU -0.35% | SPY -0.96% | rel +0.61%
  1m: XLU -2.54% | SPY -1.65% | rel -0.89%
```

MEMORY_CONFIRM: Utilities/XLU only — rolling dir=0.4 / mag=0.4 (n=10); last graded 09-10 down/mild vs XLU −0.978% / SPY −0.599% / rel −0.379% (dir HIT, mag HIT). Applied: 09-10 (gate the 08-18 relative-beat frame on a DEEP risk-off bid — VIX ≥ ~20 or explicit flight-to-quality; with VIX <20 backwardated, a rising long end is a relative-LAG signal for XLU, not a cushion; score a live long-end auction in S1, not as an unscored caveat); 09-09 (a 1d cushion built on a STATIC shock is mean-reversion fuel, not a flat override); 09-08 (oil at elevated $90+ = inflation/duration unwind, not an FTS bid); 08-18 (risk-off + rising long end → relative beat / flat-to-negative absolute; do not mint absolute up); 08-21 (live curve, not stale FRED 1d); 08-27 (do not pay carried easing in S0 and S1; no fresh XLU catalyst → relative lag); 08-25 (S0=S1=0 → do not manufacture down from carried lag); 08-12 (AI-power is a 1d dampener, not a band engine); 08-13 (S2/S4 confirmation only); 08-28 (do not promote a single-name regulatory item into S1); 09-03/09-04 (widen band when a high-impact catalyst is live; calendar scan must include ALL employment/rates releases). Open experiment: require one extra confirming source in the dominant bucket before full weight. Memory index unavailable this run; used injected sector logs + Channel 1 + web search.

## Utilities (XLU) — 2026-09-11

Object is the **near-session XLU environment**, not SPX and not a stock pick.

### Channel 1 (trusted, not re-derived)

XLU vs SPY through 2026-09-10: **1d −0.98% / −0.60% (rel −0.38%)**; **3d −1.30% / −1.60% (rel +0.30%)**; **1w −0.35% / −0.96% (rel +0.61%)**; **1m −2.54% / −1.65% (rel −0.89%)**.

Macro panel: VIX **17.24** (−0.6 1d, **+2.71 1w**) with **VIX/VIX3M 1.111 — backwardation**; DGS10 **4.83** (as of 09-09; +3 bp 1d, +4 bp 1w, **+11 bp 1m**); DGS30 **5.28** (+3 bp 1d, +1 bp 1w, +3 bp 1m); DFII10 **2.46** (+3 bp 1d, +2 bp 1w, +3 bp 1m); HY OAS 2.71 (+4 bp 1d, +5 bp 1w — creeping, not blowing out); EPU 275.99 (+26.8 1d, +78.88 1w); **CL=F −2.78% / BZ=F −3.37%** (WTI $99.91, Brent $104.62 — oil **offering hard**); GC=F +0.69%; DXY 98.85 (+0.01% 1d, −0.91% 1m); **ES=F +0.63% / NQ=F +0.65%** premarket (green, NQ marginally leading); Asia composite **−1.27%** (Nikkei −1.93%, Kospi −1.76%, Shanghai −1.18%); Europe **+0.53%**; 5-day 10Y–SPX corr **−0.745**; F&G unavailable.

**Live curve:** 10Y ~**4.83%** (20-month high zone), 30Y ~**5.28%** (19-year high zone). **Sticky-high, not easing.** Bond futures are **flat-to-marginally-green** (10Y note +0.03%, 30Y bond +0.03%) — a stabilization, not a duration-relief impulse.

**Calendar (08-14 / 08-27 / 09-04):** **CPI is the dominant scheduled binary today** (News Judge #1; futures rise after a 4-day slide, CPI looms large). No other 8:30 print of note. This is a **two-sided high-impact release** — per 09-03/09-04, do not pre-score either branch, but do **not** call flat into it.

### Channel 2

**1. Shared macro → this sector.** The dominant object is the **CPI binary** layered on a **live stagflation spine**: oil has been >$100 (Brent $104.62) driving September hike odds up (News Judge #2), and Warsh's Jackson Hole hawkishness repriced the front end and real yields (News Judge #3, gold −3%). Today oil is **offering hard** (WTI −2.78%, Brent −3.37%) — that is the 08-25 inflation→yield channel easing at the margin, which **forbids forcing down** but does **not** authorize up. Futures are green (ES +0.63%, NQ +0.65%) — a risk-on tilt into the print. VIX 17.24 is **backwardated** (ratio 1.111) — shallow, not a >20 flight-to-quality bid. Per the **09-10 gate**, with VIX <20 and backwardated, a rising long end is a **relative-lag** signal for XLU, not a relative cushion. Net: the macro is **mixed-to-mildly-negative** for a bond proxy — green futures + oil-offering are offsets, but sticky-high long end + CPI binary + backwardated VIX cap the upside. **S0 = 0** (do not score +1 from green futures/oil-slide; do not score −2 from rates — the CPI binary is unresolved and the oil slide is a genuine offset).

**2. Spine / secondary.**
- **Data-center load growth / power demand upside:** structural HIT, **stale** for a 1d call. No fresh same-day XLU-wide catalyst. Rubric: do **not** let the multi-year AI-power story override a 1d rate tape.
- **Rates falling (bond-proxy bid):** **MISS**. Live 10Y 4.83% / 30Y 5.28%, bond futures flat. Not falling.
- **Rates rising (bond-proxy selloff):** **PARTIAL / carried**. 1m DGS10 +11 bp, 30Y +3 bp, hike odds elevated — already in the price. This morning the curve is **not** independently rising (bond futures green). Do **not** HIT and do **not** double-count with S0.
- **Risk-on rotation away from utilities:** **PARTIAL**. Green ES/NQ + a CPI-day risk-on setup is a mild rotation-away pressure, but NQ is only marginally leading ES (+0.65 vs +0.63) — not an 08-27-style NQ≥+0.5% anti-FTS rip. Mild.
- **Risk-off tape / flight to safety:** **MISS**. VIX 17.24 backwardated, futures green, Europe green — no flight-to-quality impulse. Asia red is not enough.
- **Nuclear / gas generation policy support:** structural HIT, stale.
- **Grid CapEx approval / recovery:** structural HIT, stale.
- **Adverse rate case / load-growth disappointment / regulatory smash:** carried (WoodMac/Texas/Ohio). Not fresh. Single-name must not drive the ETF (08-28 rule).
Net: **Rates rising (carried) + mild risk-on rotation away** are the dominant fresh factors; structural positives stale; no FTS bid. **S1 = −1**.

**3. Breadth.** 1d rel −0.38% (yesterday's fade), 3d rel +0.30%, 1w rel +0.61%, 1m rel −0.89%. Mixed — medium-term relative is mildly positive but 1m is negative and 1d is red. No durable breadth expansion today; no live premarket breakdown. **S2 = 0**.

**4. Flows.** Prior logs noted modest flows (5d ~+$131M, 1m ~+$22M through early Sept). No confirmed same-day inflow spike or outflow lid. **S3 = 0**.

**5. Catalysts.** CPI is the live two-sided binary (News Judge #1). Oil offering hard (News Judge #2 context). No fresh XLU-wide rate-order/load print. No single-name item promoted.

### Lessons → scores

**09-10 gate is the operative veto on up:** VIX 17.24 <20 and backwardated → a rising long end is a **relative-lag** signal, not a relative cushion. Do **not** write a "relative beat" clause. Reinforces 08-13 (S2/S4 confirmation only).

**08-18:** risk-off + rising long end → relative beat / flat-to-negative absolute. Today is **not** risk-off (futures green, VIX shallow), so the frame does not fire as a relative-beat license.

**08-21:** live 10Y 4.83% is **sticky-high**, not falling. No easing impulse.

**08-25:** does **not** force flat — S1 is not 0 (rates-rising carried + mild rotation-away) and S4 is negative on the 1d tape.

**08-27:** no fresh XLU catalyst → relative lag / flat-to-down absolute. Supports down.

**09-03/09-04:** CPI is a live high-impact binary → do **not** call flat; widen the band to at least mild. This is the key magnitude input.

**09-09:** the 09-08 cushion (+1.41% rel) was built on a **static** shock and reverted — do not treat any prior-session defensive artifact as a forward cushion.

**08-12:** AI-power is a 1d dampener, not a band engine.

**08-28:** do not promote a single-name regulatory item into S1.

### Divergence check

Leading factors (S0 0, S1 −1, S2 0, S3 0) sum to **−1**; S4 is **−0.5** (1d rel −0.38%, 1m rel −0.89%). Factors and tape **agree in sign** (mild negative) — no divergence. But the CPI binary is unresolved and futures are green, so conviction is capped: **multiplier 0.9, confidence 0.55**. Band = **down/mild** (the CPI binary forbids flat per 09-03/09-04; the green futures + oil-slide forbid notable).

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: -1
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: -0.5
MULTIPLIER: 0.9
CONFIDENCE: 0.55
REGIME: mixed
PREDICTED_DIRECTION: down
PREDICTED_MAGNITUDE_BAND: mild
TOTAL_SCORE: -1.35
DIVERGENCE_FLAGGED: False
SECTOR_SCORES_END

HORIZON_3D: down/mild
HORIZON_1W: flat/mild
HORIZON_2W: flat/mild
HORIZON_1M: down/mild

HIT_GRID_BEGIN
Rates rising (bond-proxy selloff)|HIT|0.60|2026-09-11|https://www.federalreserve.gov/releases/h15/
Rates falling (bond-proxy bid)|MISS|0.70|2026-09-11|https://www.federalreserve.gov/releases/h15/
Risk-on rotation away from utilities|PARTIAL|0.50|2026-09-11|https://www.cnbc.com/pre-markets/
Risk-off tape / flight to safety|MISS|0.65|2026-09-11|https://www.cboe.com/tradable_products/vix/
Data-center load growth / power demand upside|STALE|0.55|2026-09-11|https://www.utilitydive.com/
Sector breadth failure (ETF up, names flat)|MISS|0.50|2026-09-11|https://finance.yahoo.com/quote/XLU/
Sector ETF inflow / relative volume spike|NEUTRAL|0.45|2026-09-11|https://www.etfdb.com/etf/XLU/
Crowded long (extreme relative performance + valuation)|MISS|0.60|2026-09-11|https://finance.yahoo.com/quote/XLU/
Real yields rising|HIT|0.55|2026-09-11|https://fred.stlouisfed.org/series/DFII10
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': -1.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': -0.5}, 'multiplier': 0.9, 'leading_sum': -3.0, 'divergence_flagged': False, 'total_score': -2.925, 'predicted_direction': 'down', 'predicted_magnitude_band': 'flat', 'confidence_score': 0.55, 'regime': 'mixed'}
```
