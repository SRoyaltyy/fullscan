# Sector Prediction — Real Estate — 2026-09-10

- ETF: **XLRE**
- rubric: `00_grounding/sectors/real_estate.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-6.75** (mult 0.9)
- regime: risk_off
- divergence_flagged: **False**

## Channel 1 sector ETF tape

```
ETF XLRE vs SPY (yfinance, through 2026-09-09):
  1d: XLRE -1.12% | SPY -0.46% | rel -0.65%
  3d: XLRE -1.90% | SPY -1.39% | rel -0.51%
  1w: XLRE -1.43% | SPY +0.08% | rel -1.51%
  1m: XLRE -2.23% | SPY -1.38% | rel -0.85%
```

MEMORY_CONFIRM: Reviewed prior runs for sector Real Estate. Last graded: 2026-09-09 down/mild vs XLRE −1.12% / SPY −0.46% / rel −0.65% (dir HIT, mag MISS — actual at upper edge of mild). 08-31/09-01/09-02 down/mild, 09-03 flat/flat, 09-04 flat/flat, 09-08 down/mild still ungraded. Rolling dir=0.4 mag=0.4 (n=10). Applied active REIT lessons: **(1) 08-27** — 08-25 is a ban on forcing down, not an up license; default flat unless live 10Y/30Y/TIPS independently verified still falling; 30Y in stress zone ⇒ cap S0/S1 at 0; do not double-count one rate shock; do not pad S1 with always-on DC/industrial; green/NQ beta ≠ REIT duration relief. **(2) 08-25** — verify live curve, do not force down off a stale prior-close rising table if the open curve is falling. **(3) 08-21 level-vs-change** — 30Y ~5.25 still a multi-decade stress zone; a small dip is not relief. **(4) 08-17/08-18** — live long-end + hawkish path → absolute down; 1d relative bid is a mag cap only. **(5) 08-11 geo/oil** — oil is UP again (WTI $97.44 +1.44%, Brent $102.08 +0.85%, >$101 headline) on Iran escalation — a **live, escalating geopolitical/oil supply-shock overlay** that adds inflation/stagflation risk to long-duration assets. **(6) 08-12** — two-sided policy events stay two-sided; do not one-way score S0. **(7) 09-04 asymmetric-downside** — when the structural backdrop is hawkish/unresolved (30Y stress zone, real yields up 1w/1m, Warsh Sep hike risk) and the sector is a rate-sensitive bond-proxy with 1w/1m lags, pre-score asymmetric downside rather than treating a flat-to-easing open as a symmetric offset. **(8) 09-08 cushion lesson** — when the 1d relative cushion is ≥ +0.4% AND prior session confirmed XLRE outperforming SPY on a risk-off day, the cushion is a positive S4 input / direction override toward flat. **Today the 1d rel is −0.65% (negative, NOT a +0.4% cushion), so the 09-08 cushion override does NOT fire** — the 09-04 asymmetric-downside lesson applies instead. **(9) 09-03 macro-surprise** — when a high-impact two-sided macro catalyst is identified, widen the band to at least mild (not flat). **(10) 08-14 reconcile Σ×mult.** Open `sector_real_estate` experiment: keep direction, shrink confidence on modest |score| when magnitude historically misses.

## Real Estate (XLRE) — 2026-09-10

### Channel 1 (used as given, not re-derived)
Rates through **2026-09-08**: DGS10 **4.80** (1d **+0.02** / 1w **+0.05** / 1m **+0.15**), DGS30 **5.25** (1d **+0.01** / 1w **0.0** / 1m **+0.06**), DFII10 **2.43** (1d **0.0** / 1w **−0.01** / 1m **+0.03**). That 1d column is **last Monday's close**, not this open. VIX **16.51** (+0.05 1d, **+2.19 1w**), VIX/VIX3M **1.079 — BACKWARDATION** (stress signal). **ES=F +0.11%**, **NQ=F −0.17%**, Russell +0.08%, DJIA +0.19% — **mixed/flat futures**. Asia composite **−0.56%** (Hang Seng −1.27%, ASX −1.03%), Europe **−0.06%**. **Oil UP**: WTI **$97.44 (+1.44%)**, Brent **$102.08 (+0.85%)** — **>$101 headline, live Iran escalation**. Gold **−0.56%** (GC=F +0.58% 1d), Silver **−2.43%**, Copper **−2.89%**. DXY **−0.02%** (flat). **10Y note −0.12%**, **30Y bond −0.32%**, Ultra Bond −0.37% (price down = **yields up this morning**). 5-day 10Y-SPX corr **−0.969** (strongly negative). EPU **263.53** (1d −202.84, 1w +13.84, 1m +56.58). HY OAS **2.67**. RRP **0.432** (draining).

XLRE vs SPY through **2026-09-09**: 1d **−1.12 / −0.46 / rel −0.65**; 3d rel **−0.51**; 1w rel **−1.51**; 1m rel **−0.85**. **Every horizon is a relative laggard.** No defensive cushion (09-08 override does NOT fire).

### Channel 2

**1. Shared macro as it hits REITs.** This is a **risk-off / oil-shock / hawkish-Fed overlay**, not a flight-to-safety bid into REITs. **Oil has crossed $101** (WTI $97.44, Brent $102.08) on **Iran war escalation** — the single dominant cross-asset driver (News Judge #1). **Bessent's expanded Treasury buyback plan → yields popped** (News Judge #2) — the buyback did NOT cap yields, which is the rates channel that matters for REITs. **Warsh's Jackson Hole hawkish comments → September hike odds up; gold slid >3%** (News Judge #3) — regime-level hawkish repricing. **CPI is due this week** ("This Week's Inflation Data Will Decide If the Fed Hikes Rates"). Futures are **mixed/flat** (ES +0.11%, NQ −0.17%) — no risk-on confirmation. VIX/VIX3M **backwardation at 1.079** is a stress tell. For REITs this is a **negative rate spine**: rising long-end yields (30Y bond −0.32%, Ultra Bond −0.37%) + oil-driven inflation/stagflation + hawkish Fed repricing + risk-off equity tape. The 09-04 lesson says when the structural backdrop is hawkish/unresolved (30Y stress zone, real yields up 1w/1m, Warsh Sep hike risk) and the sector is a rate-sensitive bond-proxy with 1w/1m lags, pre-score asymmetric downside. Today the open is **NOT flat-to-easing** — it is **oil >$101 + yields popping + hawkish Fed + risk-off tape**. S0 is **negative**.

**2. Spine (count the rate shock once in S1; S0 is the regime map, not a second copy of the same backup).**
- Rates falling / REIT duration relief: **MISS.** Live curve is rising (30Y bond −0.32%, Ultra Bond −0.37%), not falling.
- Rates rising / REIT selloff: **HIT.** 30Y ~5.25% stress zone, 30Y bond futures −0.32%, oil-driven inflation risk, hawkish Fed repricing, risk-off tape. This is the spine negative.
- Real yields rising: **HIT** (DFII10 +3 bp 1m; 1w flat). Same duration channel as the nominal backup — **not** a second independent shock.

**3. Secondary.**
- Data-center REIT demand / rent upside: **HIT, stale.** EQIX/DLR Q2 guides already raised in July. **08-27: not a same-day up vote.** EQIX/DLR must not define XLRE.
- Industrial REIT occupancy / rent growth: **HIT, stale** (PLD quality sleeve). Same rule.
- Refinancing window / cap-rate compression: **MISS.** Long end ~5.25%; no compression.
- Office vacancy / mark-to-market: **HIT, small sleeve.** CBRE Q2 vacancy ~18%. Office ~1% of XLRE (BXP). Do not let office set the ETF.
- Refinancing wall: **HIT, structural.** 2026 CRE maturity wall intact; not a same-morning print.
- Sector rotation out of real estate: **HIT on 1d/3d/1w/1m price** (see S2/S4). Do not also dump it into S1.

**4. Breadth / leadership.** XLRE is a **relative laggard on every horizon** (1d rel −0.65%, 3d −0.51%, 1w −1.51%, 1m −0.85%). The sector is being sold on the hawkish Fed repricing + oil shock. Data-center/senior-housing large-caps (EQIX/DLR/WELL) are not carrying the ETF today — the basket is lagging broadly. That is **sector-wide lag / large-cap inability to offset duration**, not breadth expansion. WELL cannot set the call.

**5. Flows / positioning.** XLRE has been in near-term outflow into a −1.5% 1w / −0.85% 1m relative laggard. No same-day volume spike. Not a crowded long; not a washout setup yet. Trailing 5d/1m demand-soft.

**6. Earnings / policy.** No fresh REIT print this morning. Dominant objects are **live Iran/oil escalation (>$101)** + **Bessent buyback/yields pop** + **already-printed Warsh hawkish path**. CPI this week is the scheduled binary — two-sided, not pre-scored.

### Divergence check
Leading factors (S0 −1, S1 −1, S2 −1, S4 −1) sum negative; tape (1d rel −0.65%) **confirms** the negative lean. No divergence — factors and tape agree. Per the open `sector_real_estate` experiment, keep direction and shrink confidence on the modest |score|.

### Self-audit
- **Lens:** REIT duration/rates, not SPX beta. ✓
- **Band:** mild (not notable) — no 08-18-style long-end smash at the open (30Y bond −0.32% is a backup, not a rip); rolling mag discipline. ✓
- **Skew:** asymmetric downside per 09-04 (hawkish/unresolved backdrop, no cushion). ✓
- **Same-shock double-count:** oil shock counted once in S0 (regime map); rate backup counted once in S1 (spine). Not stacked. ✓
- **Single-ticker:** EQIX/DLR/WELL explicitly barred from defining the ETF call. ✓
- **Reconcile:** Σ(S0..S4) × mult = (−1 −1 −1 0 −1) × 0.9 = **−3.6** → down/mild. ✓

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: -1
S2_BREADTH: -1
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: -1
MULTIPLIER: 0.9
CONFIDENCE: 0.55
REGIME: risk_off
SECTOR: Real Estate
ETF: XLRE
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
Rates rising / REIT selloff|HIT|0.80|2026-09-10|https://www.cnbc.com/bonds/
Real yields rising|HIT|0.60|2026-09-10|https://fred.stlouisfed.org/series/DFII10
Risk-off tape / flight to safety|HIT|0.65|2026-09-10|https://www.finviz.com/futures.ashx
Sector rotation out of real estate|HIT|0.70|2026-09-10|https://finance.yahoo.com/quote/XLRE
Sector breadth failure (ETF up, names flat)|HIT|0.55|2026-09-10|https://finance.yahoo.com/quote/XLRE
Sector ETF outflow / volume dry-up|HIT|0.50|2026-09-10|https://www.etfdb.com/etf/XLRE/
Rates falling / REIT duration relief|MISS|0.75|2026-09-10|https://www.cnbc.com/bonds/
Refinancing window opening|MISS|0.70|2026-09-10|https://www.cbre.com/insights
Cap-rate compression|MISS|0.70|2026-09-10|https://www.cbre.com/insights
Data-center REIT demand / rent upside|HIT|0.40|2026-09-10|https://www.equinix.com/newsroom
Industrial REIT occupancy / rent growth|HIT|0.40|2026-09-10|https://www.prologis.com/news
Office vacancy / mark-to-market stress|HIT|0.55|2026-09-10|https://www.cbre.com/insights
Refinancing wall stress|HIT|0.50|2026-09-10|https://www.trepp.com/
Crowded long (extreme relative performance + valuation)|MISS|0.60|2026-09-10|https://www.etfdb.com/etf/XLRE/
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -1.0, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': -1.0}, 'multiplier': 0.9, 'leading_sum': -7.0, 'divergence_flagged': False, 'total_score': -6.75, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.55, 'regime': 'risk_off'}
```
