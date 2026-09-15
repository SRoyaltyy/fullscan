# Sector Prediction — Real Estate — 2026-09-15

- ETF: **XLRE**
- rubric: `00_grounding/sectors/real_estate.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-8.5** (mult 0.9)
- regime: risk_off
- divergence_flagged: **False**
- engine: v2 · tape_anchor **-1.884** (ES +0.51%, ZN -0.46%, PM:XLRE -0.41%) · index_carry **-1.554** (general -6.215) · llm_overlay **-5.062** (raw -5.062)

## Channel 1 sector ETF tape

```
ETF XLRE vs SPY (yfinance, through 2026-09-14):
  1d: XLRE -0.69% | SPY -0.45% | rel -0.24%
  3d: XLRE -0.67% | SPY -0.20% | rel -0.47%
  1w: XLRE -1.84% | SPY -1.21% | rel -0.64%
  1m: XLRE -4.43% | SPY -2.19% | rel -2.25%
```

I now have full Channel 2 coverage. Writing the analysis.

MEMORY_CONFIRM: Reviewed prior runs for sector Real Estate. Last graded: 2026-09-14 down/mild vs XLRE −0.691% / SPY −0.446% / rel −0.245% (dir HIT, mag HIT). 2026-09-11 down/flat vs XLRE +0.859% / SPY +0.852% / rel +0.007% (dir MISS — pure beta). 2026-09-10 down/mild (dir HIT, mag HIT). 2026-09-09 down/mild (dir HIT, mag MISS). 08-31/09-01/09-02 down/mild, 09-03 flat/flat, 09-04 flat/flat, 09-08 down/mild still ungraded. Rolling dir=0.4 mag=0.4 (n=10); last-30 dir=0.526 mag=0.368. Applied active REIT lessons: **(1) 08-27** — 08-25 is a ban on forcing down, not an up license; 30Y in stress zone ⇒ cap S0/S1 at 0; do not double-count one rate shock; do not pad S1 with always-on DC/industrial; green/NQ beta ≠ REIT duration relief. **(2) 08-25** — verify the live curve; do not force down off a stale prior-close rising table if the open curve is falling. **(3) 08-21 level-vs-change** — 30Y ~5.38 is a multi-decade stress zone; a small dip is not relief. **(4) 08-17/08-18** — live long-end + hawkish path → absolute down; 1d relative bid is a mag cap only. **(5) 08-11 geo/oil** — oil is UP sharply today (WTI $103.79 +2.37%, Brent $108.11 +2.31%) on Iran escalation — a **live, escalating geopolitical/oil supply-shock overlay**; the spike branch FIRES. **(6) 08-12** — two-sided policy events stay two-sided; do not one-way score S0. **(7) 09-04 asymmetric-downside** — hawkish/unresolved backdrop + rate-sensitive bond-proxy with 1w/1m lags ⇒ pre-score asymmetric downside. **(8) 09-08 cushion lesson** — 1d rel cushion ≥ +0.4% confirmed by prior session ⇒ positive S4 / direction override toward flat. **Today 1d rel is −0.24% — NOT a +0.4% cushion, so the 09-08 override does NOT fire.** **(9) 09-11 lesson** — when S0 is genuinely 0 and the live tape is positive, a down call requires a LIVE negative input; a stale multi-horizon lag must not be scored into S2 AND S4; "no cushion ≠ headwind"; an object explicitly identified as already-priced must be scored 0. **Today the live tape is NEGATIVE (ES −0.54%, XLRE premarket −0.41%, 10Y +6bp to 5.025%), so 09-11's precondition (positive live tape) is NOT met — the down call has a live negative input.** **(10) 09-14 lesson (NEW, most binding)** — a premarket quote in a low-liquidity rate-sensitive ETF is an UNCONFIRMED signal, not the tape; it may not set the sign of the call and may not be used as an S0 offset; an object scored in S1 may not also appear as an S0 offset; live negatives identified in prose must be scored as deductions; oil and rates are the SAME duration channel for a bond proxy. **(11) 08-14 reconcile Σ×mult.** Open `sector_real_estate` experiment: keep direction, shrink confidence on modest |score| when magnitude historically misses.

---

## Real Estate (XLRE) — 2026-09-15

### Channel 1 (used as given, not re-derived)

Rates through **2026-09-11**: DGS10 **4.96** (1d **+0.01** / 1w **+0.19** / 1m **+0.28**), DGS30 **5.35** (1d **−0.02** / 1w **+0.10** / 1m **+0.11**), DFII10 **2.60** (1d **+0.05** / 1w **+0.18** / 1m **+0.18**) — **real yields rising on every listed horizon**. That 1d column is **Friday's close**, not this open. VIX **17.56** (1d **+0.46**, 1w **+1.84**), VIX/VIX3M **1.123 — BACKWARDATION** (stress). **ES=F −0.54%**, **NQ=F −0.62%**, Russell **−0.73%**, DJIA **−0.71%** — **futures clearly negative, broad**. Asia composite **−0.66%** (Hang Seng −1.0%, Kospi −0.85%), Europe **−0.44%** (DAX −0.42%, EuroStoxx50 −0.41%). **Oil UP sharply**: WTI **$103.79 (+2.37%)**, Brent **$108.11 (+2.31%)**, Heating Oil +3.09%, Gasoil +2.68% — a **live geopolitical/oil supply-shock overlay** (08-11 fires). Gold **−1.01%**, Silver **−1.47%**, Copper **−0.76%**. DXY **+0.25%** (USD strengthening). **10Y note −0.46%**, **5Y note −0.28%**, **2Y note −0.09%**, **30Y bond −0.93%**, **Ultra Bond −1.09%** (prices down = **yields UP sharply this morning**). 5-day 10Y–SPX corr **−0.107** (weakly negative). HY OAS **2.65**. EPU **395.54** (1d **+190**, 1w **+148** — elevated policy uncertainty). RRP **1.42** (1d −3.835 — big drain).

**Sector premarket vs prev close: XLRE −0.41%** — mid-pack of eleven (XLE +0.54%, XLB +0.03% best; XLF −0.60%, XLI −0.36% worst). **No defensive rotation bid into XLRE today** — the 09-14 premarket bid is absent.

XLRE vs SPY through **2026-09-14**: 1d **−0.69 / −0.45 / rel −0.24**; 3d rel **−0.47**; 1w rel **−0.64**; 1m rel **−2.25**. **Every horizon is a relative laggard**, and the 1m lag has widened to −2.25%. No defensive cushion (09-08 override does NOT fire).

### Channel 2

**1. Shared macro as it hits REITs.** This is a **rates-shock / oil-shock / pre-FOMC overlay**, and unlike 09-14 it is **unambiguously a rates-backup day**. Live web confirms: **"10-year Treasury yield rises to highest since 2007 as Fed rate-hike expectations rise"** (CNBC, 05:09 GMT today) — **10Y jumped >6 bp to 5.025%**, **30Y up >5 bp to 5.384%**. That is a **verified live long-end smash**, not a stale prior-close table (08-25 branch is OFF). The **FOMC meets today and tomorrow (Sep 15–16)**, decision **Sep 16**; markets are **bracing for a hike** — "Markets Are Bracing for a Rate Hike at This Week's Fed Meeting" (Business Insider, ~1h ago), J.P. Morgan Wealth expects a **0.25% hike**, FedWatch ~56–66% for a hike. **Oil is spiking** (WTI $103.79, Brent $108.11) on Iran escalation — a live inflation/stagflation impulse that lifts term premium. **Gold −1.01% / Silver −1.47%** — no bond-proxy FTS bid in metals. **USD +0.25%** — a mild headwind for a domestic sector. **VIX 17.56 with VIX/VIX3M 1.123 backwardation** — a genuine stress tell. For REITs this is a **negative rate spine**: rising long-end yields + oil-driven inflation + hawkish Fed repricing into a live FOMC + risk-off equity tape. **S0 is negative.**

**2. Spine (count the rate shock once in S1; S0 is the regime map, not a second copy of the same backup).**
- Rates falling / REIT duration relief: **MISS.** Live curve is rising hard (30Y bond −0.93%, Ultra Bond −1.09%), 10Y at a 2007 high.
- Rates rising / REIT selloff: **HIT.** 10Y 5.025% (highest since 2007), 30Y 5.384% stress zone, 30Y bond futures −0.93%, hike odds ~56–66% into a live FOMC. This is the spine negative.
- Real yields rising: **HIT** (DFII10 +5 bp 1d / +18 bp 1w / +18 bp 1m). Same duration channel as the nominal backup — **not** a second independent shock.

**3. Secondary.**
- Data-center REIT demand / rent upside: **HIT, stale / mixed.** MAP HEAT: Specialty (EQIX/AMT) **dir=down, conv=low** — "EQIX and AMT both red on the week despite AI/data-center headlines." ASML sold-out EUV is a semis signal, not a REIT duration vote. **08-27: not a same-day up vote.** EQIX/DLR must not define XLRE.
- Industrial REIT occupancy / rent growth: **HIT, stale and now negative.** MAP HEAT: Industrial **dir=down, conv=medium** — "PLD −2.66% and PSA −3.67% w1 with no positive news offset." Quality sleeve is leaking.
- Refinancing window / cap-rate compression: **MISS.** 10Y 5.025% / 30Y 5.384%; no compression — cap-rate **expansion** risk.
- Office vacancy / mark-to-market: **HIT, small sleeve, and the cleanest short.** MAP HEAT: Office **dir=down, conv=high** — "−2.57% w1, −2.04 vs parent, zero breadth, BXP adding debt." Office ~1% of XLRE (BXP). Do not let office set the ETF.
- Refinancing wall: **HIT, structural.** 2026 CRE maturity wall intact; a 5% 10Y tightens it further. Not a same-morning print.
- Sector rotation out of real estate: **HIT on 1d/3d/1w/1m price** (see S2/S4). Do not also dump it into S1.

**4. Breadth / leadership.** MAP HEAT is decisively negative on breadth: **8 of 9 REIT sub-sectors flat-to-down** (Office down/high, Hotel down/medium, Industrial down/medium, Mortgage down, Specialty down, Development down/medium, Diversified flat, Residential flat); only **Retail up/medium** (SPG/O/MAC/EPRT, d1 +0.43%, +0.50 vs parent) and **Healthcare Facilities up/medium** (WELL/VTR/CTRE/SBRA). That is **breadth failure**, not expansion — the two positive pockets are narrow and cannot carry the ETF. **WELL/EQIX must not set the call** (08-27, 09-14).

**5. Flows / positioning.** No same-day flow print available; the 1m relative lag has **widened to −2.25%** and the sector is a persistent multi-horizon laggard into a rate shock. No evidence of a washout/crowded-long unwind that would justify a reflex bounce (contrast the 09-14 premarket bid, which is absent today). Treated as **neutral-to-slightly-negative**, not a positive.

**6. Earnings / policy.** No fresh REIT print this morning. The dominant objects are the **live 10Y/30Y smash to 2007 highs**, the **live FOMC (Sep 15–16, decision Sep 16)** with hike odds ~56–66%, and the **live oil spike**. All three are same-day, knowable-at-open negatives for a long-duration bond proxy. No scheduled REIT-specific catalyst.

### Divergence check
Leading factors (S0 −1, S1 −1.5, S2 −1, S3 −0.5) sum to a clear negative; the tape confirmation (S4 −1) **agrees**. **No divergence.** The 09-11 "stale lag vs positive live tape" trap does **not** apply — the live tape is negative (ES −0.54%, XLRE premarket −0.41%, 10Y +6 bp). The 09-14 "premarket bid double-count" trap does **not** apply — there is no premarket bid today.

### Self-audit
- **Lens:** duration/rates for a bond proxy — correct.
- **Band:** magnitude capped at **mild** per rolling mag discipline (mag=0.368) and the 08-27/09-04 caps; a **notable** band would require a full intraday long-end smash beyond the open, which is possible but not yet confirmed.
- **Skew:** asymmetric downside per 09-04 (hawkish/unresolved backdrop + 1w/1m lags + no cushion).
- **Same-shock double-count:** the rate backup is counted **once** in S1; S0 is the regime map (risk-off + oil + FOMC event risk), not a second copy of the same 6 bp. Oil is treated as the **same duration channel** (09-14) — not stacked as an independent S0 hit.
- **Single-ticker:** WELL/EQIX/PLD/BXP explicitly barred from defining the ETF call; MAP HEAT sub-sector dispersion used as breadth evidence, not as a parent-ETF thesis.

---

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1.0
S1_SECTOR_FACTORS: -1.5
S2_BREADTH: -1.0
S3_FLOWS_POSITIONING: -0.5
S4_ETF_TAPE: -1.0
MULTIPLIER: 0.9
CONFIDENCE: 0.6
REGIME: risk_off
SECTOR_SCORES_END

HORIZON_3D: down/mild — the FOMC decision (Sep 16) is the dominant binary; a hike confirms the rate spine, a hold could produce a relief bounce, so the 3d path is down-leaning but two-sided.
HORIZON_1W: down/mild — 10Y at a 2007 high with real yields rising on every horizon is a structural duration headwind; XLRE is a 1m relative laggard (−2.25%) with no cushion.
HORIZON_2W: down/mild — unless the long end breaks lower post-FOMC, cap-rate expansion and the CRE refinancing wall keep XLRE a relative laggard.
HORIZON_1M: down/mild — persistent multi-horizon relative laggard; needs a verified falling long end to turn.

HIT_GRID_BEGIN
Rates rising / REIT selloff|HIT|0.85|2026-09-15|https://www.cnbc.com/2026/09/15/10-year-treasury-yield-rises-to-highest-since-2007.html
Real yields rising|HIT|0.8|2026-09-15|https://fred.stlouisfed.org/series/DFII10
Risk-off tape / flight to safety|HIT|0.7|2026-09-15|https://www.businessinsider.com/interest-rate-hike-fed-meeting-kevin-warsh-economy-stock-market-2026-9
Sector breadth failure (ETF up, names flat)|HIT|0.7|2026-09-15|MAP HEAT REIT sub-sector grid
Sector rotation out of real estate|HIT|0.65|2026-09-15|Channel 1 XLRE vs SPY tape
Office vacancy / mark-to-market stress|HIT|0.6|2026-09-15|MAP HEAT REIT - Office dir=down conv=high
Refinancing wall stress|HIT|0.55|2026-09-15|MAP HEAT REIT - Mortgage dir=down; BXMT 52wk low
Cap-rate expansion|HIT|0.6|2026-09-15|https://www.cnbc.com/2026/09/15/10-year-treasury-yield-rises-to-highest-since-2007.html
USD strengthening|HIT|0.5|2026-09-15|Channel 1 DXY +0.25%
Rates falling / REIT duration relief|MISS|0.85|2026-09-15|https://www.cnbc.com/2026/09/15/10-year-treasury-yield-rises-to-highest-since-2007.html
Refinancing window opening|MISS|0.7|2026-09-15|Channel 1 DGS10 4.96 / DGS30 5.35
Cap-rate compression|MISS|0.7|2026-09-15|Channel 1 DGS10 4.96 / DGS30 5.35
Data-center REIT demand / rent upside|PARTIAL|0.4|2026-09-15|MAP HEAT REIT - Specialty dir=down conv=low
Industrial REIT occupancy / rent growth|PARTIAL|0.4|2026-09-15|MAP HEAT REIT - Industrial dir=down conv=medium
Sector rotation into REITs|PARTIAL|0.35|2026-09-15|MAP HEAT REIT - Retail up/medium, Healthcare Facilities up/medium
Sector ETF inflow / relative volume spike|MISS|0.5|2026-09-15|no same-day flow print; 1m rel lag widened to -2.25%
Crowded long (extreme relative performance + valuation)|MISS|0.5|2026-09-15|XLRE 1m rel -2.25% (laggard, not crowded long)
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -1.5, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': -0.5, 'S4_ETF_TAPE': -1.0}, 'multiplier': 0.9, 'leading_sum': -8.5, 'divergence_flagged': False, 'total_score': -8.5, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.65, 'regime': 'risk_off', 'engine': 'v2', 'anchor': {'available': True, 'pct': -0.314, 'score': -1.884, 'legs': [{'leg': 'ES', 'pct': 0.51, 'w': 0.5}, {'leg': 'ZN', 'pct': -0.46, 'w': 0.75}, {'leg': 'PM:XLRE', 'pct': -0.41, 'w': 0.7}]}, 'overlay_score': -5.062, 'overlay_raw': -5.062, 'index_carry': -1.554, 'general_total': -6.215, 'skill_multipliers': {'S0_SHARED_MACRO': 0.5, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 1.0}, 'llm_confidence': 0.6, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
