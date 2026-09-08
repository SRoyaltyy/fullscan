# Sector Prediction — Real Estate — 2026-09-08

- ETF: **XLRE**
- rubric: `00_grounding/sectors/real_estate.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-4.5** (mult 0.9)
- regime: risk_off
- divergence_flagged: **False**

## Channel 1 sector ETF tape

```
ETF XLRE vs SPY (yfinance, through 2026-09-08):
  1d: XLRE +0.24% | SPY -0.28% | rel +0.52%
  3d: XLRE +0.70% | SPY +0.38% | rel +0.32%
  1w: XLRE -0.17% | SPY +0.13% | rel -0.30%
  1m: XLRE -2.10% | SPY -0.68% | rel -1.43%
```

MEMORY_CONFIRM: Reviewed prior runs for sector Real Estate. Last graded: 2026-08-28 down/mild vs XLRE −0.403% / SPY −0.227% / rel −0.176% (dir HIT, mag HIT). 08-31/09-01/09-02 down/mild and 09-03 flat/flat and 09-04 flat/flat still ungraded. Rolling dir=0.5 mag=0.5 (n=10). Applied active REIT lessons: **(1) 08-27** — 08-25 is a ban on forcing down, not an up license; default flat unless live 10Y/30Y/TIPS independently verified still falling; 30Y in stress zone ⇒ cap S0/S1 at 0; do not double-count one rate shock; do not pad S1 with always-on DC/industrial; green/NQ beta ≠ REIT duration relief. **(2) 08-25** — verify live curve, do not force down off a stale prior-close rising table if the open curve is falling. **(3) 08-21 level-vs-change** — 30Y ~5.25 still a multi-decade stress zone; a small dip is not relief. **(4) 08-17/08-18** — live long-end + hawkish path → absolute down; 1d relative bid is a mag cap only. **(5) 08-11 geo/oil** — oil is UP sharply today (CL +3.18%, Brent +2.16%, WTI $94.4), a live geopolitical/oil supply-shock overlay that adds inflation/stagflation risk to long-duration assets. **(6) 08-12** — two-sided policy events stay two-sided; do not one-way score S0. **(7) 09-04 asymmetric-downside** — when the structural backdrop is hawkish/unresolved (30Y stress zone, real yields up 1w/1m) and the sector is a rate-sensitive bond-proxy with 1w/1m lags, pre-score asymmetric downside rather than treating a flat-to-easing open as a symmetric offset. **(8) 09-03 macro-surprise** — when a high-impact two-sided macro catalyst is identified, widen the band to at least mild (not flat). **(9) 08-14 reconcile Σ×mult.** Open `sector_real_estate` experiment: keep direction, shrink confidence on modest |score| when magnitude historically misses.

## Real Estate (XLRE) — 2026-09-08

### Channel 1 (used as given, not re-derived)
Rates through **2026-09-03**: DGS10 **4.77** (1d **−0.02** / 1w **+0.10** / 1m **+0.14**), DGS30 **5.25** (1d **−0.02** / 1w **+0.06** / 1m **+0.08**), DFII10 **2.42** (1d **−0.03** / 1w **+0.08** / 1m **+0.01**). That 1d column is **last Thursday's close**, not this open. VIX **15.49** (+0.19 1d), VIX/VIX3M **0.851** (contango). **ES=F −0.44%**, **NQ=F −0.16%**, Russell −0.63%, DJIA −0.91% — **futures clearly negative/risk-off**. Asia composite **−0.48%** (Nikkei −1.7%), Europe **−0.14%**. **Oil UP sharply**: CL **+3.18%** (WTI $94.4), Brent **+2.16%** ($99.1), Heating Oil +3.22% — a **live geopolitical/oil supply-shock overlay** (08-11 fires). Gold **−0.84%**, Silver −0.39%. Copper **+1.31%**. DXY **−0.24%** (USD weakening). **10Y note −0.15%**, **30Y bond −0.46%** (price down = yields up this morning). 5-day 10Y-SPX corr **−0.948** (strongly negative). EPU **546.32** (1d −217, 1w +310, 1m +392 — elevated).

XLRE vs SPY through **2026-09-08**: 1d **+0.24 / −0.28 / rel +0.52**; 3d rel **+0.32**; 1w rel **−0.30**; 1m rel **−1.43**. **1d/3d are defensive relative cushions; 1w/1m remain laggards.** Confirmation mix, not a duration-relief thesis.

### Channel 2

**1. Shared macro as it hits REITs.** This is a **risk-off / oil-shock overlay**, not a flight-to-safety bid into REITs. Live futures are **clearly negative** (ES −0.44%, DJIA −0.91%, Russell −0.63%). **Oil is spiking** (WTI +3.18% to $94.4, Brent +2.16% to $99.1) — a live geopolitical/oil supply-shock that adds an inflation/stagflation bid and pressures long-duration assets (08-11 fires). The 30Y sits at **5.25%**, still inside the multi-decade stress zone (08-21). Real yields up on 1w/1m (DFII10 +8 bp 1w). The 5-day 10Y-SPX correlation is **−0.948** — strongly negative, meaning rising yields crush equities broadly. **30Y bond futures −0.46%** (yields rising this morning). For REITs, this is a **negative rate spine**: rising long-end yields + oil-driven inflation risk + risk-off equity tape. The 09-04 lesson says when the structural backdrop is hawkish/unresolved and the sector is a rate-sensitive bond-proxy with 1w/1m lags, pre-score asymmetric downside rather than treating a flat-to-easing open as a symmetric offset. Today the open is NOT flat-to-easing — it is **rising yields + risk-off + oil spike**. S0 is **negative**.

**2. Spine (count the rate shock once in S1; S0 is the regime map, not a second copy of the same backup).**
- Rates falling / REIT duration relief: **MISS.** Live curve is rising (30Y bond −0.46%), not falling.
- Rates rising / REIT selloff: **HIT.** 30Y ~5.25% stress zone, 30Y bond futures −0.46%, oil-driven inflation risk, risk-off tape. This is the spine negative.
- Real yields rising: **HIT** (DFII10 +8 bp 1w). Same duration channel as the nominal backup — **not** a second independent shock.

**3. Secondary.**
- Data-center REIT demand / rent upside: **HIT, stale.** EQIX/DLR Q2 guides already raised in July. **08-27: not a same-day up vote.** EQIX/DLR must not define XLRE.
- Industrial REIT occupancy / rent growth: **HIT, stale** (PLD quality sleeve). Same rule.
- Refinancing window / cap-rate compression: **MISS.** Long end ~5.25%; no compression.
- Office vacancy / mark-to-market: **HIT, small sleeve.** CBRE Q2 vacancy ~18%. Office ~1% of XLRE (BXP). Do not let office set the ETF.
- Refinancing wall: **HIT, structural.** 2026 CRE maturity wall intact; not a same-morning print.
- Sector rotation out of real estate: **HIT on 1w/1m price** (see S2/S4). Do not also dump it into S1.

**4. Breadth / leadership.** XLRE 1d rel **+0.52%** (defensive cushion vs SPY on the risk-off day), 3d rel +0.32%, but 1w/1m still laggards (−0.30% / −1.43%). The 1d relative cushion is a **defensive bid** (REITs falling less than SPY on risk-off), not breadth expansion. MAP HEAT shows **Mortgage REITs strongest** (+2.04 w1 spread, breadth 0.718) while **Industrial weakest** (breadth 0.067, PLD −4.21% w1), **Specialty weak** (EQIX −5.59% w1), **Office weak** (SLG −5.98% w1). This is **sub-sector dispersion**, not broad XLRE leadership. WELL cannot set the call.

**5. Flows / positioning.** XLRE has been in near-term outflow (5d/1m negative in prior runs) into a −1.4% 1m relative laggard. No same-day volume spike. Not a crowded long.

**6. Earnings / policy.** No fresh REIT print this morning. Dominant objects are the **oil spike** (live geopolitical supply shock) and the **hawkish Fed repricing** backdrop (30Y stress zone). No scheduled 8:30 high-impact print today (calendar-check passed).

### Self-audit
- Lens: duration/rates for a bond-proxy, not SPX beta.
- Band: factors net negative; **do not emit notable** off the 1d defensive cushion — the oil spike + rising yields + risk-off tape is the dominant driver, and the defensive relative bid caps magnitude at mild (08-18).
- Skew: oil spike is a fresh geopolitical supply shock (08-11 fires) — this is not a stale leftover.
- Same-shock double-count: rate backup counted once in S1; S0 is the macro map.
- Single-ticker: EQIX/DLR/WELL must not define XLRE.

**Direction:** down. **Magnitude:** mild (the 1d defensive cushion caps it; no 08-18-style 30Y smash with ES −0.6%+... actually ES is −0.44% and oil is spiking, but the 1d rel +0.52% cushion argues for mild, not notable).

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: -1
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.9
CONFIDENCE: 0.55
REGIME: risk_off
HORIZON_3D: down:mild:0.5
HORIZON_1W: down:mild:0.45
HORIZON_2W: flat:mild:0.4
HORIZON_1M: flat:mild:0.35
SECTOR_SCORES_END

HIT_GRID_BEGIN
Rates rising / REIT selloff|HIT|0.8|2026-09-08|30Y ~5.25% stress zone, 30Y bond futures −0.46%, oil-driven inflation risk
Real yields rising|HIT|0.7|2026-09-08|DFII10 +8 bp 1w, same duration channel as nominal backup
Risk-off tape / flight to safety|HIT|0.6|2026-09-08|ES −0.44%, DJIA −0.91%, oil +3.18% — REITs as relative defensive bid only
Data-center REIT demand / rent upside|HIT|0.5|2026-09-08|Stale structural, not a same-day up vote (08-27)
Industrial REIT occupancy / rent growth|HIT|0.5|2026-09-08|Stale structural, not a same-day up vote (08-27)
Office vacancy / mark-to-market stress|HIT|0.4|2026-09-08|Small sleeve ~1% of XLRE (BXP)
Refinancing wall stress|HIT|0.4|2026-09-08|Structural, 2026 CRE maturity wall
Sector rotation out of real estate|HIT|0.4|2026-09-08|1w/1m relative laggard (−0.30% / −1.43%)
Rates falling / REIT duration relief|MISS|0.0|2026-09-08|Live curve rising, not falling
Cap-rate compression|MISS|0.0|2026-09-08|Long end ~5.25%, no compression
HIT_GRID_END

**Total:** (S0 −1 + S1 −1 + S2 0 + S3 0 + S4 0) × 0.9 = **−1.8 → down/mild**

**Rationale:** The dominant driver today is a **live oil spike** (WTI +3.18% to $94.4, Brent +2.16% to $99.1) on geopolitical supply risk, layered on a **persistent hawkish Fed backdrop** (30Y ~5.25% stress zone, real yields up 1w/1m). Futures are clearly risk-off (ES −0.44%, DJIA −0.91%). For a rate-sensitive bond-proxy like XLRE, this is a **negative rate spine** — rising long-end yields + oil-driven inflation risk + risk-off equity tape. The 09-04 lesson says pre-score asymmetric downside when the structural backdrop is hawkish/unresolved and the sector is a 1w/1m laggard. The 1d relative cushion (+0.52% vs SPY) is a **defensive bid** (REITs falling less than SPY on risk-off), which caps magnitude at **mild** (08-18) rather than allowing notable. Direction is **down**.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -1.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.9, 'leading_sum': -5.0, 'divergence_flagged': False, 'total_score': -4.5, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.55, 'regime': 'risk_off', 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
