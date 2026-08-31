# Sector Prediction — Real Estate — 2026-08-31

- ETF: **XLRE**
- rubric: `00_grounding/sectors/real_estate.md`
- predicted_direction: **down**
- predicted_magnitude_band: **notable**
- total_score: **-8.1** (mult 0.9)
- regime: risk_off
- divergence_flagged: **False**

## Channel 1 sector ETF tape

```
ETF XLRE vs SPY (yfinance, through 2026-08-27):
  1d: XLRE -0.95% | SPY +0.66% | rel -1.61%
  3d: XLRE -1.48% | SPY +1.00% | rel -2.48%
  1w: XLRE -0.93% | SPY +1.11% | rel -2.05%
  1m: XLRE -2.83% | SPY +5.71% | rel -8.54%
```

I have enough data. Let me synthesize the analysis for today's Real Estate (XLRE) call.

**Key data for today (2026-08-31):**
- **Warsh Jackson Hole (Aug 28) hawkish** — September hike odds now ~55-57% (coin flip), 2Y jumped, 10Y rose to ~4.73% (Aug 31), 30Y ~5.19%
- **10Y at 4.73%** (Aug 31, +0.01 from prior), 30Y at 5.19% — long end still in multi-decade stress zone
- **Real yields**: DFII10 2.34 (1d 0, 1w -0.01, 1m -0.07) — flat-to-slightly-easing backdrop
- **Futures**: ES -0.2%, NQ -0.15% — slightly negative/soft
- **Oil UP**: CL +1.97%, BZ +2.15% — oil rising (geopolitical, Iran Hormuz terms)
- **Gold DOWN** -0.81% (fading from surge)
- **Asia mixed** (+0.02% composite), Europe +0.13%
- **XLRE tape**: 1d rel -1.61%, 3d rel -2.48%, 1w rel -2.05%, 1m rel -8.54% — **every horizon a laggard**

Let me construct the analysis.

MEMORY_CONFIRM: Reviewed prior runs for sector Real Estate. Last graded: 2026-08-28 down/mild vs XLRE −0.403% / SPY −0.227% / rel −0.176% (dir HIT, mag HIT). Rolling dir=0.5 mag=0.5 (n=10). Applied active REIT lessons: (1) 08-27 — 08-25 is a ban on forcing down, not an up license; default flat unless live 10Y/30Y/TIPS independently verified still falling; 30Y in stress zone ⇒ cap S0/S1 at 0; do not double-count one rate shock; do not pad S1 with always-on DC/industrial; sticky PCE already in + Warsh = flat or down/mild, not up; green/NQ beta ≠ REIT duration relief. (2) 08-25 — verify live curve, do not force down off a stale prior-close rising table if the open curve is falling. (3) 08-21 level-vs-change — 30Y 5.19 still a multi-decade stress zone; a 1–2 bp tick is not relief. (4) 08-17 live-rate — no 08-18-style long-end shock with sharply negative futures, so do not force notable down. (5) 08-18 — relative bid is a mag cap (and today there is no relative bid). (6) 08-12 — Warsh is two-sided; do not one-way score S0 hawkish merely because a policy event exists. (7) 08-11 geo/oil — oil is UP today (+1.97% CL) on Iran Hormuz terms, so this is a live risk-off overlay, not a non-firing. (8) 08-14 reconcile band to Σ×mult. Open `sector_real_estate` experiment: last three losses say prefer flat/mild when score fights tape — today factors and tape agree (both negative), so keep the down lean.

## Real Estate (XLRE) — 2026-08-31

### Channel 1 (used as given, not re-derived)
Rates through 2026-08-27: DGS10 **4.67** (1d +0.01 / 1w −0.02 / 1m 0.0), DGS30 **5.19** (1d +0.01 / 1w −0.04 / 1m −0.01), DFII10 **2.34** (1d 0 / 1w −0.01 / 1m −0.07). Live 10Y on Aug 31 ~**4.73%** (+0.01 from prior session per TradingEconomics) — **rising, not falling**. VIX 15.22 (+0.71 1d). ES **−0.2%**, NQ **−0.15%** — soft/negative futures. Asia composite **+0.02%** (mixed), Europe **+0.13%**. **Oil UP**: CL **+1.97%**, BZ **+2.15%** — oil rising on Iran Hormuz terms headline (live geopolitical risk-off overlay). Gold **−0.81%** (fading). DXY +0.38% 1d (USD strengthening). F&G 58.2 Greed. 10Y–SPX 5d corr **+0.26**.

XLRE vs SPY through 2026-08-27: 1d **−0.95 / +0.66 / rel −1.61**; 3d rel **−2.48**; 1w rel **−2.05**; 1m rel **−8.54**. **Every horizon is a relative laggard.** That is confirmation, not a thesis.

### Channel 2

**1. Shared macro as it hits REITs.** This is the **post-Warsh hawkish repricing** session. Warsh's Jackson Hole speech (Aug 28) was hawkish on inflation — September hike odds jumped to ~55-57% (coin flip), 2Y spiked, and the 10Y has risen to ~4.73% (Aug 31). This is the dominant macro driver. The 30Y sits at ~5.19%, still inside the multi-decade stress zone. **Oil is UP** (+1.97% CL) on Iran saying it will name terms for reopening the Strait of Hormuz — a live geopolitical/oil supply-risk overlay that adds an inflation/stagflation bid and pressures long-duration assets. USD strengthening (+0.38% 1d). For REITs, this is a **negative rate spine**: rising long-end yields + hawkish Fed repricing + oil-driven inflation risk. The 08-17 lesson says when yields are rising to multi-decade highs at open, direction should be down/negative for the rate-sensitive sector. The 08-21 level-vs-change lesson says a small yield move is not duration relief while 30Y sits near a 19-year high. S0 is **negative**.

**2. Spine (count once; do not also put in S0).**
- Rates falling / REIT duration relief: **MISS**. Live curve is rising (10Y ~4.73%), not falling. 30Y still ≥5.15%.
- Rates rising / REIT selloff: **HIT**. 10Y rising to ~4.73%, 30Y at 5.19% stress zone, hawkish Warsh repricing, September hike odds ~55-57%. This is the spine negative.
- Real yields: Channel 1 1d flat, 1w/1m slightly lower. Backdrop, not a fresh impulse. The 1m real-yield trend is still slightly lower, but the nominal long-end rise dominates the short horizon.

**3. Secondary.**
- Data-center demand / rent upside: **HIT, stale**. CBRE H1-2026 primary-market vacancy ~1.4%, rents up; EQIX/DLR Q2 guides already raised in July. 08-27: do **not** use structural DC occupancy as a same-day up vote. EQIX/DLR must not define XLRE.
- Industrial occupancy / rent growth: **HIT, stale** (PLD still the quality industrial sleeve). Same rule — not a 1-day catalyst.
- Refinancing window / cap-rate compression: **MISS**. Long end still ~5.19%; no compression signal.
- Office vacancy / mark-to-market: **HIT, small sleeve**. National vacancy ~17.7% and still bifurcated; office is ~1% of XLRE (BXP). Do not let office define the ETF.
- Refinancing wall: **HIT, structural**. 2026–28 CRE maturity wall intact; not a same-morning print.
- Rotation out of REITs: **HIT on price + near-term flows** (see S2/S3). Do not also score it in S1.

**4. Breadth / leadership.** XLRE is a **relative laggard on every horizon** (1d −1.61%, 3d −2.48%, 1w −2.05%, 1m −8.54% rel). The basket is lagging a hawkish-Fed tape. That is **sector-wide lag / large-cap inability to offset duration**, not breadth expansion. WELL cannot set the call.

**5. Flows / positioning.** ETFdb as of ~Aug 27: XLRE **5d −$27M**, **1m −$123M**, 3m still +$387M. Near-term outflow into a −8.5% 1m relative laggard = unconfirmed washout, not a crowded long. No same-day volume spike.

**6. Earnings / policy.** No fresh REIT print this morning. Dominant event is the **post-Warsh hawkish repricing** (already delivered Aug 28) plus **Iran Hormuz terms** headline (live geopolitical/oil risk). No 8:30 CPI/NFP today. Calendar-check passed.

### Self-audit
- Lens: duration/rates for a bond-proxy, not SPX beta.
- Band: factors net negative; **do not emit notable** off a hawkish-Fed repricing unless there is a verified open long-end shock (08-18 analog). Today 10Y ~4.73% is rising but not a 30Y-at-19-year-high + ES −0.6% smash. Cap at mild.
- Skew: hawkish Warsh + oil-up + USD-up = one-way down for REITs, not two-sided. The 08-12 two-sided rule applied to the *pre-speech* binary; the speech has now printed hawkish, so it is no longer two-sided.
- Same-shock: the hawkish repricing is counted once in S0 (macro) and once in S1 (rates rising spine) — that is the correct two-channel treatment, not double-counting the same object twice in S1.
- Single-ticker: WELL/EQIX/DLR are dispersion, not the ETF call.

### Divergence
Leading (S0+S1+S2+S3) = **negative**. Tape S4 = **negative** (every horizon a laggard). **No divergence — factors and tape agree.** Trust the down lean.

**Call implication for the pipeline:** Σ = (S0 −1 + S1 −1 + S2 −1 + S3 −1 + S4 −1) = −5 × 0.9 = **−4.5 → down / mild**. The hawkish Fed repricing + rising long-end yields + oil-up geopolitical overlay + every-horizon relative lag all point down. Magnitude capped at mild because there is no verified 08-18-style long-end shock (10Y ~4.73% is elevated but not a fresh multi-decade-high smash with sharply negative futures).

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: -1
S2_BREADTH: -1
S3_FLOWS_POSITIONING: -1
S4_ETF_TAPE: -1
MULTIPLIER: 0.9
CONFIDENCE: 0.55
REGIME: risk_off
HORIZON_3D: down:mild:0.52
HORIZON_1W: down:mild:0.50
HORIZON_2W: down:mild:0.48
HORIZON_1M: down:mild:0.50
SECTOR_SCORES_END

HIT_GRID_BEGIN
Rates rising / REIT selloff|HIT|0.75|2026-08-31|https://www.cnbc.com/2026/08/31/jackson-hole-fed-chair-kevin-warsh-hawkish-rate-hikes-analysts.html
Real yields rising|MISS|0.60|2026-08-31|Channel 1 DFII10 flat 1d
Risk-off tape / flight to safety|HIT|0.65|2026-08-31|Oil up +1.97% on Iran Hormuz terms; ES -0.2%
Sector rotation out of real estate|HIT|0.70|2026-08-31|XLRE laggard every horizon; 1m rel -8.54%
Sector ETF outflow / volume dry-up|HIT|0.60|2026-08-31|XLRE 5d -$27M, 1m -$123M
Office vacancy / mark-to-market stress|HIT|0.55|2026-08-31|National vacancy ~17.7%
Refinancing wall stress|HIT|0.55|2026-08-31|2026-28 CRE maturity wall intact
Data-center REIT demand / rent upside|HIT|0.60|2026-08-31|CBRE vacancy ~1.4% (stale, not same-day)
Industrial REIT occupancy / rent growth|HIT|0.55|2026-08-31|PLD ~95% occupancy (stale)
Rates falling / REIT duration relief|MISS|0.70|2026-08-31|Live 10Y ~4.73% rising, not falling
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -1.0, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': -1.0, 'S4_ETF_TAPE': -1.0}, 'multiplier': 0.9, 'leading_sum': -7.0, 'divergence_flagged': False, 'total_score': -8.1, 'predicted_direction': 'down', 'predicted_magnitude_band': 'notable', 'confidence_score': 0.55, 'regime': 'risk_off'}
```
