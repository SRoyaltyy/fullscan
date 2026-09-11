# Sector Prediction — Real Estate — 2026-09-11

- ETF: **XLRE**
- rubric: `00_grounding/sectors/real_estate.md`
- predicted_direction: **down**
- predicted_magnitude_band: **flat**
- total_score: **-1.125** (mult 0.9)
- regime: mixed
- divergence_flagged: **False**

## Channel 1 sector ETF tape

```
ETF XLRE vs SPY (yfinance, through 2026-09-10):
  1d: XLRE -0.83% | SPY -0.60% | rel -0.23%
  3d: XLRE -2.00% | SPY -1.60% | rel -0.40%
  1w: XLRE -1.55% | SPY -0.96% | rel -0.60%
  1m: XLRE -2.34% | SPY -1.65% | rel -0.68%
```

MEMORY_CONFIRM: Reviewed prior runs for sector Real Estate. Last graded: 2026-09-10 down/mild vs XLRE −0.829% / SPY −0.599% / rel −0.230% (dir HIT, mag HIT). 08-31/09-01/09-02 down/mild, 09-03 flat/flat, 09-04 flat/flat, 09-08 down/mild, 09-09 down/mild still ungraded. Rolling dir=0.4 mag=0.4 (n=10). Applied active REIT lessons: **(1) 08-27** — 08-25 is a ban on forcing down, not an up license; default flat unless live 10Y/30Y/TIPS independently verified still falling; 30Y in stress zone ⇒ cap S0/S1 at 0; do not double-count one rate shock; do not pad S1 with always-on DC/industrial; green/NQ beta ≠ REIT duration relief. **(2) 08-25** — verify live curve, do not force down off a stale prior-close rising table if the open curve is falling. **(3) 08-21 level-vs-change** — 30Y ~5.28 still a multi-decade stress zone; a small dip is not relief. **(4) 08-17/08-18** — live long-end + hawkish path → absolute down; 1d relative bid is a mag cap only. **(5) 08-11 geo/oil** — oil is DOWN today (CL −2.78%, BZ −3.37%, WTI $99.91, Brent $104.62) — an **oil slide**, not a spike; the 08-11 spike branch does NOT fire, and 08-25 restricts the oil-slide positive to a verified falling curve. **(6) 08-12** — two-sided policy events stay two-sided; do not one-way score S0. **(7) 09-04 asymmetric-downside** — when the structural backdrop is hawkish/unresolved (30Y stress zone, real yields up 1w/1m) and the sector is a rate-sensitive bond-proxy with 1w/1m lags, pre-score asymmetric downside rather than treating a flat-to-easing open as a symmetric offset. **(8) 09-08 cushion lesson** — when the 1d relative cushion is ≥ +0.4% AND prior session confirmed XLRE outperforming SPY on a risk-off day, the cushion is a positive S4 input / direction override toward flat. **Today the 1d rel is −0.23% (negative, NOT a +0.4% cushion), so the 09-08 cushion override does NOT fire.** **(9) 09-03 macro-surprise** — when a high-impact two-sided macro catalyst is identified, widen the band to at least mild (not flat). **(10) 08-14 reconcile Σ×mult.** Open `sector_real_estate` experiment: keep direction, shrink confidence on modest |score| when magnitude historically misses.

## Real Estate (XLRE) — 2026-09-11

### Channel 1 (used as given, not re-derived)
Rates through **2026-09-09**: DGS10 **4.83** (1d **+0.03** / 1w **+0.04** / 1m **+0.11**), DGS30 **5.28** (1d **+0.03** / 1w **+0.01** / 1m **+0.03**), DFII10 **2.46** (1d **+0.03** / 1w **+0.02** / 1m **+0.03**) — **real yields rising on every listed horizon**. That 1d column is **Wednesday's close**, not this open. VIX **17.24** (1d −0.6, **1w +2.71**), VIX/VIX3M **1.111 — BACKWARDATION** (stress). **ES=F +0.63%**, **NQ=F +0.65%**, Russell +0.63%, DJIA +0.53% — **futures clearly green/risk-on**. Asia composite **−1.27%** (Nikkei −1.93%, Kospi −1.76%, Shanghai −1.18%), Europe **+0.53%** (in progress). **Oil DOWN**: CL=F **−2.78%**, BZ=F **−3.37%**; Finviz WTI **$99.91 (−2.54%)**, Brent **$104.62 (−2.86%)**. Gold **+0.69%** (GC=F), Finviz Gold −0.33%. DXY **+0.01%** (flat). **10Y note +0.03%**, **5Y note 0.00%**, **2Y note −0.01%**, **30Y bond +0.03%**, Ultra Bond **+0.09%** (prices up = **yields flat-to-slightly-down this morning**). 5-day 10Y-SPX corr **−0.745**. HY OAS **2.71** (+0.04 1d). EPU **275.99** (1d +26.8, 1w +78.88, 1m +142.83 — elevated). RRP **4.736** (1d +4.304 — big drain reversal).

XLRE vs SPY through **2026-09-10**: 1d **−0.83 / −0.60 / rel −0.23**; 3d rel **−0.40**; 1w rel **−0.60**; 1m rel **−0.68**. **Every horizon is a relative laggard**, but the lags are modest and **no defensive cushion exists** (09-08 override does NOT fire).

### Channel 2

**1. Shared macro as it hits REITs.** This is a **CPI-day risk-on setup**, not a risk-off overlay. Futures are **clearly green** (ES +0.63%, NQ +0.65%) after a 4-day slide, with **CPI looming large** as the dominant scheduled binary (News Judge #1). **Oil is DOWN** (WTI −2.54%, Brent −2.86%) — an oil slide that eases the inflation channel, the opposite of the 08-11 spike. **Gold is mixed** (GC=F +0.69%, Finviz −0.33%) after Warsh's hawkish Jackson Hole comments boosted September hike expectations (News Judge #3) — a regime-level hawkish repricing that is **already printed** (T+2), not a fresh same-morning shock. The live curve is **flat-to-slightly-down** (10Y note +0.03%, 30Y bond +0.03%, Ultra Bond +0.09% — prices up = yields down). The 30Y sits at **5.28%**, still inside the multi-decade stress zone (08-21). For REITs: the persistent hawkish backdrop (30Y stress zone, real yields up 1w/1m) is the structural headwind, but the **live curve is not rising** this morning, oil is **falling**, and futures are **green**. This is the 08-25 / 09-03 setup: a modest easing open with a two-sided CPI binary pending. **S0 = 0** (mixed — hawkish backdrop offset by flat-to-easing live curve, falling oil, and green futures; CPI is two-sided and unprinted).

**2. Spine (count the rate object once in S1; S0 is the regime map, not a second copy of the same backup).**
- Rates falling / REIT duration relief: **MISS.** Live curve is flat-to-slightly-down, not a verified second-day falling curve. 30Y still ~5.28% in the stress zone (08-21).
- Rates rising / REIT selloff: **not a clean HIT at the open.** Prior-close FRED was up; live curve is flat-to-slightly-down. 08-25 forbids treating the 9/9 1d column as today's tape. The hawkish Fed backdrop is real but already in the term premium (T+2).
- Real yields rising: Channel 1 1d **+3 bp**, 1w **+2 bp**, 1m **+3 bp** — backdrop, same duration channel, **not** a second independent shock.

**3. Secondary.**
- Data-center REIT demand / rent upside: **HIT, stale.** EQIX/DLR Q2 guides already raised in July. **08-27: not a same-day up vote.** EQIX/DLR must not define XLRE.
- Industrial REIT occupancy / rent growth: **HIT, stale** (PLD quality sleeve). Same rule.
- Refinancing window / cap-rate compression: **MISS.** 30Y ~5.28%; no compression.
- Office vacancy / mark-to-market: **HIT, small sleeve.** CBRE Q2 vacancy ~18%. Office ~1% of XLRE (BXP). Do not let office set the ETF.
- Refinancing wall: **HIT, structural.** 2026 CRE maturity wall intact; not a same-morning print.
- Sector rotation out of real estate: **HIT on 1w/1m price** (see S2/S4). Do not also dump it into S1.

**4. Breadth / leadership.** XLRE 1d rel **−0.23%** — a mild lag, not a cushion and not a smash. No WELL-only carry and no breadth expansion. Multi-horizon lag is **sector-wide duration**, not ETF-only. WELL cannot set the call.

**5. Flows / positioning.** XLRE has been in near-term outflow into a −0.68% 1m relative laggard. No same-day volume spike. Not a crowded long.

**6. Earnings / policy.** No fresh REIT print this morning. Dominant objects are **already-printed Warsh hawkish path** + **CPI pending today** (two-sided, unprinted). The 09-03 lesson says widen the band to at least mild when a high-impact two-sided catalyst is identified — so **flat is not available**; the band is at least mild.

### Divergence check
Leading factors: S0 0, S1 0, S2 −0.5, S3 0, S4 −0.5 → net **−1.0**. Tape: 1d rel −0.23% (mild lag), futures green, oil falling. **Factors and tape agree on a mild-negative relative lean but the absolute direction is genuinely two-sided** (green futures + falling oil + easing curve vs hawkish backdrop + CPI binary + multi-horizon lag). Per the 09-03 macro-surprise lesson, the flat band is unavailable; per the 08-27 no-force-down rule, the easing open forbids a forced down call. The honest call is **flat/mild with a slight down lean**, confidence reduced.

### Reconciliation
Σ(S0..S4) = 0 + 0 − 0.5 + 0 − 0.5 = **−1.0**; × mult 0.9 = **−0.9** → **flat/mild**. Pipeline and narrative agree.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: 0
S2_BREADTH: -0.5
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: -0.5
MULTIPLIER: 0.9
CONFIDENCE: 0.5
REGIME: mixed
SECTOR_SCORES_END

HIT_GRID_BEGIN
Rates falling / REIT duration relief | MISS | 0.70 | 2026-09-11 | https://www.cnbc.com/quotes/US10Y
Rates rising / REIT selloff | MISS | 0.60 | 2026-09-11 | https://fred.stlouisfed.org/series/DGS30
Real yields rising | HIT | 0.65 | 2026-09-11 | https://fred.stlouisfed.org/series/DFII10
Real yields falling | MISS | 0.65 | 2026-09-11 | https://fred.stlouisfed.org/series/DFII10
Risk-on tape / equity beta expansion | HIT | 0.60 | 2026-09-11 | https://www.finviz.com/futures.ashx
Risk-off tape / flight to safety | MISS | 0.55 | 2026-09-11 | https://www.finviz.com/futures.ashx
Sector breadth failure (ETF up, names flat) | HIT | 0.50 | 2026-09-11 | https://finance.yahoo.com/quote/XLRE
Sector rotation out of real estate | HIT | 0.55 | 2026-09-11 | https://finance.yahoo.com/quote/XLRE
Data-center REIT demand / rent upside | MISS | 0.60 | 2026-09-11 | https://www.equinix.com/newsroom
Industrial REIT occupancy / rent growth | MISS | 0.55 | 2026-09-11 | https://www.prologis.com/news
Office vacancy / mark-to-market stress | HIT | 0.55 | 2026-09-11 | https://www.cbre.com/insights
Refinancing wall stress | HIT | 0.50 | 2026-09-11 | https://www.trepp.com
Cap-rate expansion | HIT | 0.50 | 2026-09-11 | https://www.cbre.com/insights
Refinancing window opening | MISS | 0.55 | 2026-09-11 | https://www.cbre.com/insights
Cap-rate compression | MISS | 0.55 | 2026-09-11 | https://www.cbre.com/insights
Sector ETF outflow / volume dry-up | HIT | 0.50 | 2026-09-11 | https://www.etfdb.com/etf/XLRE/
Crowded long (extreme relative performance + valuation) | MISS | 0.60 | 2026-09-11 | https://finance.yahoo.com/quote/XLRE
Large-cap leadership inside sector | MISS | 0.50 | 2026-09-11 | https://finance.yahoo.com/quote/WELL
Low-beta leadership inside sector | HIT | 0.45 | 2026-09-11 | https://finance.yahoo.com/quote/XLRE
HIT_GRID_END

HORIZON_3D: flat/mild
HORIZON_1W: down/mild
HORIZON_2W: down/mild
HORIZON_1M: down/mild

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': 0.0, 'S2_BREADTH': -0.5, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': -0.5}, 'multiplier': 0.9, 'leading_sum': -1.0, 'divergence_flagged': False, 'total_score': -1.125, 'predicted_direction': 'down', 'predicted_magnitude_band': 'flat', 'confidence_score': 0.5, 'regime': 'mixed'}
```
