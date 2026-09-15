# Sector Prediction — Real Estate — 2026-09-15

- ETF: **XLRE**
- rubric: `00_grounding/sectors/real_estate.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-6.267** (mult 0.9)
- regime: risk_off
- divergence_flagged: **False**
- engine: v2 · tape_anchor **-1.305** (ES +0.36%, ZN -0.46%, PM:XLRE -0.24%) · index_carry **-0.912** (general -3.65) · llm_overlay **-4.05** (raw -4.05)

## Channel 1 sector ETF tape

```
ETF XLRE vs SPY (yfinance, through 2026-09-15):
  1d: XLRE -0.23% | SPY -0.49% | rel +0.26%
  3d: XLRE -0.07% | SPY -0.09% | rel +0.02%
  1w: XLRE -2.00% | SPY -1.15% | rel -0.86%
  1m: XLRE -4.97% | SPY -2.47% | rel -2.50%
```

MEMORY_CONFIRM: Reviewed prior runs for sector Real Estate (XLRE) only. Last graded: 2026-09-14 down/mild vs XLRE −0.691% / SPY −0.446% / rel −0.245% (dir HIT, mag HIT). 2026-09-11 down/flat vs XLRE +0.859% / SPY +0.852% / rel +0.007% (dir MISS — pure beta). 2026-09-10 down/mild (dir HIT, mag HIT). 2026-09-09 down/mild (dir HIT, mag MISS, actual at upper edge of mild). 08-31/09-01/09-02 down/mild, 09-03 flat/flat, 09-04 flat/flat, 09-08 down/mild still ungraded. Rolling dir=0.4 mag=0.4 (n=10); last-30 dir=0.526 mag=0.368. Applied active REIT lessons: **(1) 08-27** — 08-25 is a ban on forcing down, not an up license; 30Y in stress zone ⇒ cap S0/S1 at 0; do not double-count one rate shock; do not pad S1 with always-on DC/industrial; green/NQ beta ≠ REIT duration relief. **(2) 08-25** — verify the live curve; do not force down off a stale prior-close rising table if the open curve is falling. **(3) 08-21 level-vs-change** — 30Y ~5.35 is a multi-decade stress zone; a small dip is not relief. **(4) 08-17/08-18** — live long-end + hawkish path → absolute down; 1d relative bid is a mag cap only. **(5) 08-11 geo/oil** — oil is UP sharply today (WTI $103.79 +2.37%, Brent $108.11 +2.31%) on Iran escalation — a **live, escalating geopolitical/oil supply-shock overlay**; the spike branch FIRES. **(6) 08-12** — two-sided policy events stay two-sided; do not one-way score S0. **(7) 09-04 asymmetric-downside** — hawkish/unresolved backdrop + rate-sensitive bond-proxy with 1w/1m lags ⇒ pre-score asymmetric downside. **(8) 09-08 cushion lesson** — 1d rel cushion ≥ +0.4% confirmed by prior session ⇒ positive S4 / direction override toward flat. **Today 1d rel is +0.26% (below the +0.4% gate) — the 09-08 override does NOT fire.** **(9) 09-11 lesson** — when S0 is genuinely 0 and the live tape is positive, a down call requires a LIVE negative input; a stale multi-horizon lag must not be scored into S2 AND S4; "no cushion ≠ headwind"; an object explicitly identified as already-priced must be scored 0. **Today the live tape is NEGATIVE (ES −0.54%, 10Y ≥5%, 30Y bond −0.93%), so 09-11's precondition (positive live tape) is NOT met — the down call has a live negative input.** **(10) 09-14 lesson (most binding)** — a premarket quote in a low-liquidity rate-sensitive ETF is an UNCONFIRMED signal, not the tape; it may not set the sign of the call and may not be used as an S0 offset; an object scored in S1 may not also appear as an S0 offset; live negatives identified in prose must be scored as deductions; oil and rates are the SAME duration channel for a bond proxy. **(11) 08-14 reconcile Σ×mult.** Open `sector_real_estate` experiment: keep direction, shrink confidence on modest |score| when magnitude historically misses.

---

## Real Estate (XLRE) — 2026-09-15

### Channel 1 (used as given, not re-derived)

Rates through **2026-09-11**: DGS10 **4.96** (1d +0.01 / 1w **+0.19** / 1m **+0.28**), DGS30 **5.35** (1d −0.02 / 1w **+0.10** / 1m **+0.11**), DFII10 **2.60** (1d **+0.05** / 1w **+0.18** / 1m **+0.18**) — **real yields rising on every listed horizon**. That 1d column is **Friday's close**, not this open. VIX **17.55** (1d +0.45, 1w +1.83), VIX/VIX3M **0.898** (contango — term structure normalized vs the 09-11/09-14 backwardation, even as spot VIX rose). **ES=F −0.54%**, **NQ=F −0.62%**, Russell **−0.73%**, DJIA **−0.71%** — **futures clearly negative, broad**. Asia composite **−0.72%** (Kospi −3.26%), Europe **−0.31%**. **Oil UP sharply**: WTI **$103.79 (+2.37%)**, Brent **$108.11 (+2.31%)**, Heating Oil +3.09%, Gasoil +2.68% — a **live geopolitical/oil supply-shock overlay** (08-11 fires). Gold **−1.01%**, Silver **−1.47%**, Copper **−0.76%**. DXY **+0.25%** (USD strengthening). **10Y note −0.46%**, **5Y note −0.28%**, **2Y note −0.09%**, **30Y bond −0.93%**, **Ultra Bond −1.09%** (prices down = **yields UP sharply this morning**). 5-day 10Y–SPX corr **−0.178** (weakly negative). HY OAS **2.71**. EPU **215.48** (1d −202.54 — policy uncertainty *fell*). RRP **1.42** (1d −3.835 — big drain).

**Sector premarket vs prev close: XLRE −0.24%** — mid-pack of eleven (XLI +0.81%, XLB +0.38%, XLU +0.20% best; XLC −0.63%, XLP −0.33% worst). **No defensive rotation bid into XLRE today** — the 09-14 premarket bid (+0.48%, second-best) is absent. Per the 09-14 lesson, a premarket quote in this low-liquidity ETF is an **unconfirmed** signal and may not set the sign of the call either way. I score it 0.

XLRE vs SPY through **2026-09-15**: 1d **−0.23 / −0.49 / rel +0.26**; 3d rel **+0.02**; 1w rel **−0.86**; 1m rel **−2.50**. **The 1d rel is +0.26% — a mild defensive cushion but BELOW the +0.4% gate, so the 09-08 override does NOT fire.** 3d rel is flat (+0.02%). 1w/1m remain laggards, with the 1m lag widened to −2.50%. This is a **mild-cushion / structural-laggard** configuration, not the 09-08 large-cushion setup.

### Channel 2

**1. Shared macro as it hits REITs.** This is a **rates-shock / oil-shock / pre-FOMC overlay**, and it is **unambiguously a rates-backup day**. Live web confirms: **"US 10-year Treasury yield breaches 5% as global bond sell-off deepens"** (Euronews), **"Ten-year Treasury yield hits 5%"** — the single dominant cross-asset driver (News Judge #1). The **August CPI core surprise locked in a September hike** (News Judge #2) — hard-data confirmation of the hawkish path, not a two-sided unprinted binary. **FOMC/SEP/Warsh press conference** is the unresolved same-cycle binary (News Judge #3) — per the scheduled-FOMC lessons, B3 stays unsigned until it prints; I encode it as a confidence/magnitude cap, not a pre-scored hawkish HIT. **Oil is spiking** (WTI $103.79, Brent $108.11) on the US–Iran tanker war / Hormuz impairment (News Judge #5) — a live inflation/stagflation overlay that pressures long-duration assets. **UMich 1-yr inflation expectations jumped to 4.6%** (News Judge #6) — reinforces the hawkish rates regime. **Chipmaker weakness (AMD −5% premarket on AI-slowdown calls)** is a tech-specific unwind, not a REIT driver (08-27: NQ beta ≠ REIT duration relief). **BAC CEO soft Q3 outlook, shares −5%** is XLF/credit force, not XLRE.

For REITs this is a **negative rate spine**: live long-end yields ripping higher (30Y bond −0.93%, Ultra Bond −1.09%), 10Y ≥5%, real yields up on every horizon, oil-driven inflation risk, USD strengthening, risk-off equity tape. The 09-04 lesson says when the structural backdrop is hawkish/unresolved and the sector is a rate-sensitive bond-proxy with 1w/1m lags, pre-score asymmetric downside. Today the open is **NOT flat-to-easing** — it is **10Y ≥5% + oil >$103 + hawkish CPI-locked Fed + risk-off futures**. **S0 is negative.**

**2. Spine (count the rate shock once in S1; S0 is the regime map, not a second copy of the same backup).**
- Rates falling / REIT duration relief: **MISS.** Live curve is ripping higher (30Y bond −0.93%, Ultra Bond −1.09%), not falling.
- Rates rising / REIT selloff: **HIT.** 10Y ≥5%, 30Y ~5.35% stress zone, 30Y bond futures −0.93%, oil-driven inflation risk, hawkish CPI-locked Fed, risk-off tape. This is the spine negative.
- Real yields rising: **HIT** (DFII10 +5 bp 1d / +18 bp 1w / +18 bp 1m). Same duration channel as the nominal backup — **not** a second independent shock.

**3. Secondary.**
- Data-center REIT demand / rent upside: **HIT, stale / mixed.** MAP HEAT: Specialty (EQIX/AMT) dir=**down**, conv=low — "EQIX and AMT both red on the week despite AI/data-center headlines." **08-27: not a same-day up vote.** EQIX/DLR must not define XLRE.
- Industrial REIT occupancy / rent growth: **HIT, stale / negative.** MAP HEAT: Industrial dir=**down**, conv=medium — "PLD −2.66% and PSA −3.67% w1 with no positive news offset." Quality sleeve, not a 1-day catalyst.
- Refinancing window / cap-rate compression: **MISS.** 10Y ≥5%, 30Y ~5.35%; no compression.
- Office vacancy / mark-to-market: **HIT, small sleeve.** MAP HEAT: Office dir=**down**, conv=**high** — "cleanest short: −2.57% w1, −2.04 vs parent, zero breadth, BXP adding debt." Office ~1% of XLRE (BXP). Do not let office set the ETF.
- Refinancing wall: **HIT, structural.** 2026 CRE maturity wall intact; not a same-morning print.
- Sector rotation out of real estate: **HIT on 1w/1m price** (see S2/S4). Do not also dump it into S1.
- **MAP HEAT sub-sector dispersion:** Retail dir=**up** (SPG/O/MAC/EPRT; +0.43% d1, +0.50 vs parent) and Healthcare Facilities dir=**up** (WELL/VTR/CTRE/SBRA) are the only positive pockets; Hotels, Industrial, Office, Mortgage, Specialty, Development all **down**. Net sub-sector breadth is **negative** — the two positive pockets are small weights and do not offset the office/industrial/specialty drag.

**4. Breadth / leadership.** MAP HEAT shows **6 of 9 REIT sub-sectors down** (Office high-conviction down, Industrial medium down, Hotels medium down, Mortgage down, Specialty down, Development down), with only Retail and Healthcare Facilities up and Diversified/Residential flat. That is **breadth failure inside the sector**, not expansion. The 1d rel +0.26% is a mild defensive cushion vs SPY, not % names expanding. WELL/EQIX cannot set the call.

**5. Flows / positioning.** No same-day XLRE flow print available. The 1m rel −2.50% into a 10Y ≥5% tape is a structural laggard, not a crowded long. No volume spike. Score 0 (no independent evidence).

**6. Earnings / policy.** No fresh REIT print this morning. Dominant objects are the **live 10Y ≥5% bond selloff** + **CPI-locked September hike** + **live Hormuz/oil spike** + the **unresolved FOMC/SEP/Warsh binary** (encoded as a confidence/mag cap, not pre-scored). BAC soft Q3 is XLF, not XLRE.

### Divergence check
Leading factors (S0 −1, S1 −1, S2 −1) sum negative; S4 tape is **+0.26% rel** (mild positive cushion, below the +0.4% override gate). The tape does **not** confirm the down lean strongly, but it also does not contradict it (1w/1m rel are −0.86% / −2.50%). Per the method, **trust factors over tape** — but the sub-gate cushion caps magnitude at **mild** and trims confidence. **divergence_flagged = True** (mild tape-vs-factor tension).

### Self-audit
- **Lens:** duration/rates for a bond-proxy — correct.
- **Band:** mild, not notable — no full 08-18-style long-end smash *at the open* (30Y bond −0.93% is a backup but not a crash), and the sub-gate cushion argues against notable.
- **Skew:** asymmetric downside per 09-04 (hawkish/unresolved backdrop, 1w/1m lags, no ≥+0.4% cushion).
- **Same-shock double-count:** oil spike and rate backup are the **same duration channel** for a bond proxy (09-14) — counted **once** in S1; S0 carries the regime map only. Flagged.
- **Single-ticker:** EQIX/DLR/WELL explicitly barred from defining the ETF call; MAP HEAT sub-sector dispersion used, not one name.
- **09-08 override:** correctly **NOT** fired (1d rel +0.26% < +0.4% gate).
- **09-11 precondition:** correctly **NOT** met (live tape is negative, not positive).

---

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: -1
S2_BREADTH: -1
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.9
CONFIDENCE: 0.55
REGIME: risk_off
DIVERGENCE_FLAGGED: True
HORIZON_3D: down/mild
HORIZON_1W: down/mild
HORIZON_2W: flat/mild
HORIZON_1M: flat/mild
SECTOR_SCORES_END

HIT_GRID_BEGIN
Rates rising / REIT selloff|HIT|0.85|2026-09-15|https://www.euronews.com/business/2026/09/15/us-10-year-treasury-yield-breaches-5-as-global-bond-sell-off-deepens
Real yields rising|HIT|0.80|2026-09-15|https://fred.stlouisfed.org/series/DFII10
Risk-off tape / flight to safety|HIT|0.70|2026-09-15|https://finviz.com/futures.ashx
USD strengthening|HIT|0.60|2026-09-15|https://finviz.com/futures.ashx
Sector breadth failure (ETF up, names flat)|HIT|0.65|2026-09-15|https://finviz.com/map.ashx
Office vacancy / mark-to-market stress|HIT|0.70|2026-09-15|https://finviz.com/map.ashx
Refinancing wall stress|HIT|0.55|2026-09-15|https://www.trepp.com/
Cap-rate expansion|HIT|0.60|2026-09-15|https://fred.stlouisfed.org/series/DGS10
Sector rotation out of real estate|HIT|0.60|2026-09-15|https://finviz.com/map.ashx
Rates falling / REIT duration relief|MISS|0.85|2026-09-15|https://finviz.com/futures.ashx
Refinancing window opening|MISS|0.75|2026-09-15|https://fred.stlouisfed.org/series/DGS10
Cap-rate compression|MISS|0.75|2026-09-15|https://fred.stlouisfed.org/series/DGS10
Data-center REIT demand / rent upside|MISS|0.55|2026-09-15|https://finviz.com/map.ashx
Industrial REIT occupancy / rent growth|MISS|0.55|2026-09-15|https://finviz.com/map.ashx
Sector rotation into REITs|MISS|0.55|2026-09-15|https://finviz.com/map.ashx
Sector ETF inflow / relative volume spike|MISS|0.50|2026-09-15|https://etfdb.com/etf/XLRE/
Sector ETF outflow / volume dry-up|HIT|0.45|2026-09-15|https://etfdb.com/etf/XLRE/
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -1.0, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.9, 'leading_sum': -7.0, 'divergence_flagged': False, 'total_score': -6.267, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.65, 'regime': 'risk_off', 'engine': 'v2', 'anchor': {'available': True, 'pct': -0.2175, 'score': -1.305, 'legs': [{'leg': 'ES', 'pct': 0.36, 'w': 0.5}, {'leg': 'ZN', 'pct': -0.46, 'w': 0.75}, {'leg': 'PM:XLRE', 'pct': -0.24, 'w': 0.7}]}, 'overlay_score': -4.05, 'overlay_raw': -4.05, 'index_carry': -0.912, 'general_total': -3.65, 'skill_multipliers': {'S0_SHARED_MACRO': 0.5, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 1.0}, 'llm_confidence': 0.55, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
