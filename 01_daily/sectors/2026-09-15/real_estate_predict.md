# Sector Prediction — Real Estate — 2026-09-15

- ETF: **XLRE**
- rubric: `00_grounding/sectors/real_estate.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-4.541** (mult 0.9)
- regime: risk_off
- divergence_flagged: **True**
- engine: v2 · tape_anchor **0.225** (ES +0.80%, ZN -0.46%, PM:XLRE +0.03%) · index_carry **-0.716** (general -2.865) · llm_overlay **-4.05** (raw -4.05)

## Channel 1 sector ETF tape

```
ETF XLRE vs SPY (yfinance, through 2026-09-14):
  1d: XLRE -0.69% | SPY -0.45% | rel -0.24%
  3d: XLRE -0.67% | SPY -0.20% | rel -0.47%
  1w: XLRE -1.84% | SPY -1.21% | rel -0.64%
  1m: XLRE -4.43% | SPY -2.19% | rel -2.25%
```

MEMORY_CONFIRM: Reviewed prior runs for sector Real Estate (XLRE) only. Last graded: 2026-09-14 down/mild vs XLRE −0.691% / SPY −0.446% / rel −0.245% (dir HIT, mag HIT). 2026-09-11 down/flat vs XLRE +0.859% / SPY +0.852% / rel +0.007% (dir MISS — pure beta). 2026-09-10 down/mild (dir HIT, mag HIT). 2026-09-09 down/mild (dir HIT, mag MISS, actual at upper edge of mild). 08-31/09-01/09-02 down/mild, 09-03 flat/flat, 09-04 flat/flat, 09-08 down/mild still ungraded. Rolling dir=0.4 mag=0.4 (n=10); last-30 dir=0.526 mag=0.368. Applied active REIT lessons: **(1) 08-27** — 08-25 is a ban on forcing down, not an up license; 30Y in stress zone ⇒ cap S0/S1 at 0; do not double-count one rate shock; do not pad S1 with always-on DC/industrial; green/NQ beta ≠ REIT duration relief. **(2) 08-25** — verify the live curve; do not force down off a stale prior-close rising table if the open curve is falling. **(3) 08-21 level-vs-change** — 30Y ~5.35 is a multi-decade stress zone; a small dip is not relief. **(4) 08-17/08-18** — live long-end + hawkish path → absolute down; 1d relative bid is a mag cap only. **(5) 08-11 geo/oil** — oil is UP sharply today (WTI $103.79 +2.37%, Brent $108.11 +2.31%) on Iran escalation — a **live, escalating geopolitical/oil supply-shock overlay**; the spike branch FIRES. **(6) 08-12** — two-sided policy events stay two-sided; do not one-way score S0. **(7) 09-04 asymmetric-downside** — hawkish/unresolved backdrop + rate-sensitive bond-proxy with 1w/1m lags ⇒ pre-score asymmetric downside. **(8) 09-08 cushion lesson** — 1d rel cushion ≥ +0.4% confirmed by prior session ⇒ positive S4 / direction override toward flat. **Today 1d rel is −0.24% — NOT a +0.4% cushion, so the 09-08 override does NOT fire.** **(9) 09-11 lesson** — when S0 is genuinely 0 and the live tape is positive, a down call requires a LIVE negative input; a stale multi-horizon lag must not be scored into S2 AND S4; "no cushion ≠ headwind"; an object explicitly identified as already-priced must be scored 0. **Today the live tape is NEGATIVE (ES −0.54%, XLRE premarket −0.41%, 10Y +6bp to 5.025%), so 09-11's precondition (positive live tape) is NOT met — the down call has a live negative input.** **(10) 09-14 lesson (NEW, most binding)** — a premarket quote in a low-liquidity rate-sensitive ETF is an UNCONFIRMED signal, not the tape; it may not set the sign of the call and may not be used as an S0 offset; an object scored in S1 may not also appear as an S0 offset; live negatives identified in prose must be scored as deductions; oil and rates are the SAME duration channel for a bond proxy. **(11) 08-14 reconcile Σ×mult.** Open `sector_real_estate` experiment: keep direction, shrink confidence on modest |score| when magnitude historically misses.

---

## Real Estate (XLRE) — 2026-09-15

### Channel 1 (used as given, not re-derived)

Rates through **2026-09-11**: DGS10 **4.96** (1d **+0.01** / 1w **+0.19** / 1m **+0.28**), DGS30 **5.35** (1d **−0.02** / 1w **+0.10** / 1m **+0.11**), DFII10 **2.60** (1d **+0.05** / 1w **+0.18** / 1m **+0.18**) — **real yields rising on every listed horizon**. That 1d column is **Friday's close**, not this open. VIX **17.05** (1d −0.05, 1w **+1.33**), VIX/VIX3M **1.093 — BACKWARDATION** (stress). **ES=F −0.54%**, **NQ=F −0.62%**, Russell **−0.73%**, DJIA **−0.71%** — **futures clearly negative, broad**. Asia composite **−0.66%** (Hang Seng −1.0%, Kospi −0.85%), Europe **−0.18%** (partial). **Oil UP sharply**: WTI **$103.79 (+2.37%)**, Brent **$108.11 (+2.31%)**, Heating Oil +3.09%, Gasoil +2.68% — a **live geopolitical/oil supply-shock overlay** (08-11 fires). Gold **−1.01%**, Silver **−1.47%**, Copper **−0.76%**. DXY **+0.25%** (USD strengthening). **10Y note −0.46%**, **5Y note −0.28%**, **2Y note −0.09%**, **30Y bond −0.93%**, **Ultra Bond −1.09%** (prices down = **yields UP sharply this morning**). 5-day 10Y–SPX corr **−0.107** (weakly negative). HY OAS **2.65**. EPU **395.54** (1d **+190**, 1w **+148** — elevated policy uncertainty). RRP **1.42** (1d −3.835 — big drain).

**Sector premarket vs prev close: XLRE +0.03%** — mid-pack of eleven (XLB +0.68%, XLE +0.24%, XLU +0.19% best; XLC −0.63%, XLP −0.43% worst). **No defensive rotation bid into XLRE today** — the 09-14 premarket bid (+0.48%, second-best) is absent. Note the Channel 1 premarket line (XLRE +0.03%) and the engine's tape_anchor PM:XLRE (−0.41%) disagree; per the 09-14 lesson a premarket quote in this low-liquidity ETF is an **unconfirmed** signal and may not set the sign of the call either way. I score it 0.

XLRE vs SPY through **2026-09-14**: 1d **−0.69 / −0.45 / rel −0.24**; 3d rel **−0.47**; 1w rel **−0.64**; 1m rel **−2.25**. **Every horizon is a relative laggard**, and the 1m lag has widened to −2.25%. No defensive cushion (09-08 override does NOT fire).

### Channel 2

**1. Shared macro as it hits REITs.** This is a **rates-shock / oil-shock / pre-FOMC overlay**, and unlike 09-14 it is **unambiguously a rates-backup day**. Live web confirms: **"US 10-year Treasury yield breaches 5% as global bond sell-off deepens"** (Yahoo Finance, 05:51 GMT today), **"Bond selloff drives US benchmark beyond 5%; stocks rattled"** (WSAU, 08:06 GMT), **"Stock futures slide as 10-year Treasury yield tops 5% ahead of Fed decision"** (Seeking Alpha, 08:17 GMT), **"Global bond yields hit 2008 highs, raising stakes for big borrowers"** (Reuters, 11:47 GMT). The 10Y is at **~5.025%**, the highest since 2007; the 30Y is **~5.384%**. That is a **verified live long-end smash**, not a stale prior-close table — the 08-25 "don't force down off a stale table" branch is **OFF**.

The **FOMC meets today and tomorrow (Sep 15–16)**, decision **Sep 16**. Hike odds have surged: **"With Just 5 Days to Next FOMC Meeting, Odds of Fed Rate Hike Surge to Over 85%"** (24/7 Wall St., 09-11), **"Fed Rate Hike Odds Near 90%"** (BeInCrypto, 09-11), **"White House ups pressure on Kevin Warsh's Fed as Wall Street expects hike"** (Fortune, 09-07). This is a **hawkish, near-resolved** path, not a two-sided binary to park — but the decision itself is still unprinted, so it is a **magnitude/confidence cap**, not a pre-scored HIT.

**Oil is spiking** (WTI $103.79, Brent $108.11) on the US–Iran tanker war with Hormuz traffic impaired — a live stagflation tax on multiples. **Gold −1.01% / Silver −1.47%** — no bond-proxy flight-to-safety bid in metals. **USD +0.25%** — a mild headwind for a domestic sector. **VIX 17.05 with VIX/VIX3M 1.093 backwardation** — a genuine stress tell. **EPU +190 1d** — a policy-uncertainty spike.

For REITs this is a **negative rate spine**: a verified 5%+ 10Y, a 5.38% 30Y, real yields up on every horizon, oil-driven inflation, hawkish Fed repricing, and a risk-off equity tape. **S0 = −1.**

**2. Spine (count the rate shock once in S1; S0 is the regime map, not a second copy of the same backup).**
- Rates falling / REIT duration relief: **MISS.** Live curve is ripping higher (30Y bond −0.93%, Ultra Bond −1.09%).
- Rates rising / REIT selloff: **HIT.** 10Y through 5%, 30Y ~5.38%, hike odds ~85–90%, oil-driven inflation. This is the spine negative.
- Real yields rising: **HIT** (DFII10 +5 bp 1d / +18 bp 1w / +18 bp 1m). Same duration channel as the nominal backup — **not** a second independent shock.

**3. Secondary.**
- Data-center REIT demand / rent upside: **HIT, stale / mixed.** MAP HEAT: Specialty (EQIX/AMT) **dir=down, conv=low** — "EQIX and AMT both red on the week despite AI/data-center headlines." ASML sold-out-2027-EUV is a semis story, not a REIT one. **08-27: not a same-day up vote.** EQIX/DLR must not define XLRE.
- Industrial REIT occupancy / rent growth: **HIT, stale and now negative on tape.** MAP HEAT: Industrial **dir=down, conv=medium** — "PLD −2.66% and PSA −3.67% w1 with no positive news offset."
- Refinancing window / cap-rate compression: **MISS.** 10Y through 5%; no compression.
- Office vacancy / mark-to-market: **HIT, small sleeve, and the cleanest short.** MAP HEAT: Office **dir=down, conv=high** — "−2.57% w1, −2.04 vs parent, zero breadth, BXP adding debt." Office ~1% of XLRE. Do not let office set the ETF.
- Refinancing wall: **HIT, structural.** 2026 CRE maturity wall intact; not a same-morning print.
- Sector rotation out of real estate: **HIT on 1w/1m price** (see S2/S4). Do not also dump it into S1.

**4. Breadth / leadership.** MAP HEAT is the live breadth read and it is **overwhelmingly negative**: Office down/high-conviction, Industrial down/medium, Hotel down/medium, Mortgage down/low, Specialty down/low, Development down/medium. Only two pockets are positive — **Retail (dir=up, conv=medium; SPG/O/MAC/EPRT, d1 +0.43%, +0.50 vs parent)** and **Healthcare Facilities (dir=up, conv=medium; WELL/VTR/CTRE/SBRA)**. That is **two sub-sectors up, six down** — a breadth failure, not expansion. Critically, the two positive pockets are **small weights**; the ETF's largest weights (PLD, AMT, EQIX, WELL, SPG) are split, with PLD/AMT/EQIX all red on the week. **WELL cannot set the call** (08-27 / DO-NOT). **S2 = −1.**

**5. Flows / positioning.** No same-day XLRE flow print available this run; the 1m relative lag has widened to **−2.25%** with the 1w at −0.64%, which is a persistent distribution pattern rather than a washout. Per the 09-11 lesson I will **not** score the stale multi-horizon lag into both S2 and S4 — it is scored once, in S2 (breadth/leadership), and S4 is scored on the **live** tape only. **S3 = 0** (no fresh flow evidence either way; do not manufacture a positioning score).

**6. Earnings / policy.** No fresh REIT print this morning. The dominant objects are the **live 5% 10Y / global bond selloff**, the **FOMC decision tomorrow (Sep 16)** with hike odds ~85–90%, the **already-printed August CPI core surprise** that locked the hike in, and the **live oil/Hormuz shock**. The FOMC is the only scheduled binary and it is **unprinted** — encoded as a confidence/mag cap, not a pre-scored HIT.

**7. S4 — ETF tape (confirmation only).** Live: ES −0.54%, NQ −0.62%, Russell −0.73%, DJIA −0.71% — a broad risk-off tape with **no** defensive rotation into XLRE (premarket +0.03%, mid-pack; engine PM:XLRE −0.41%). The 1d rel is **−0.24%** — negative, so the 09-08 cushion override does not fire, and the 09-11 "positive live tape" precondition is not met. **S4 = −1.**

### Divergence / self-audit

- **Lens:** duration/rates, not SPX beta. The 5% 10Y is the operative object.
- **Band:** |Σ| is large but the FOMC decision is tomorrow and unprinted, and the 09-14 lesson warns that a premarket quote in this ETF is unconfirmed. Cap at **mild**.
- **Skew:** asymmetric downside (09-04) — a hawkish, unresolved backdrop with a rate-sensitive bond-proxy carrying 1w/1m lags.
- **Same-shock double-count:** the oil shock and the rate backup are the **same duration channel** for a bond proxy (09-14). Counted **once** in S1; S0 carries the regime map, not a second copy.
- **Single-ticker:** WELL (Healthcare, up) and EQIX/AMT (Specialty, down) are nested overrides and must not define the ETF. MAP HEAT's six-down/two-up breadth is the ETF-level read.
- **09-11 check:** the precondition (positive live tape) is **not met** — the live tape is negative, so a down call has a live negative input.
- **09-08 check:** 1d rel −0.24%, no +0.4% cushion — override does **not** fire.
- **Divergence flag:** factors and tape **agree** (both negative). No divergence.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: -1
S2_BREADTH: -1
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: -1
MULTIPLIER: 0.9
CONFIDENCE: 0.55
REGIME: risk_off
SECTOR_DIRECTION: down
SECTOR_MAGNITUDE_BAND: mild
DIVERGENCE_FLAGGED: false
HORIZON_3D: down/mild
HORIZON_1W: down/mild
HORIZON_2W: flat/mild
HORIZON_1M: flat/mild
SECTOR_SCORES_END

HIT_GRID_BEGIN
Rates rising / REIT selloff|HIT|0.90|2026-09-15|https://news.google.com/rss/articles/CBMihAFBVV95cUxQbmpYd0ktWGt5bnp2RlRSRURMNHZFSmNTLXl1MXZBOEN5eWNSd0pkVUxTaUdPdHphS3pHeElZUkdnQnZQY1dRSEZzNW5VU0sxVlpHR2ZDUWMta0ZHSWRpdmh6R3k2ckViR0hkT2xvT2JHdXd4SW1mS3RzdG1vaHB4MGhRMUc
Real yields rising|HIT|0.85|2026-09-15|https://fred.stlouisfed.org/series/DFII10
Risk-off tape / flight to safety|HIT|0.80|2026-09-15|https://news.google.com/rss/articles/CBMiiAFBVV95cUxPN1pPUzNsMm4yMVREVjN2WE5jUkN0cTdqbkRJeUtLWUdNVGczY1dxQkhTeTBUSHUtMF95UG5kZkFBR3Brcno0aFF1VmI0SGVzVkltQ1RhMWh2VHZNRUZZYlBxd1F0YUdJNkE4TkJUeXo5NnpYRnE3cWg4VWQ1WWw2UU5JMl9hbEdv
Sector breadth failure (ETF up, names flat)|HIT|0.75|2026-09-15|https://news.google.com/rss/articles/CBMiqgFBVV95cUxQeVVUU3YxN0hod3p0YXhiSFVMV3NaZktzZUNDRjlXNVdpR29vZW00RlYxYUt5Ykl5QXBQcFdUcm9IV1FuMGxqcjVoLVVyQVl6enJLYzBneW52VkF0RURPUWxjRk9WeG1jeWN1M0hDRkRJeGwwY21QZnY4bmVPLS05OWRXbXVYQThxbFN1V3lIZUJtYWJGcWxjNDVMMHh1TzhVUEhKekhFZ0R6Zw
Office vacancy / mark-to-market stress|HIT|0.80|2026-09-15|
Industrial REIT occupancy / rent growth|HIT|0.60|2026-09-15|
Refinancing wall stress|HIT|0.60|2026-09-15|
Cap-rate expansion|HIT|0.65|2026-09-15|
Sector rotation out of real estate|HIT|0.70|2026-09-15|
USD strengthening|HIT|0.55|2026-09-15|
Data-center REIT demand / rent upside|MISS|0.55|2026-09-15|
Rates falling / REIT duration relief|MISS|0.90|2026-09-15|
Refinancing window opening|MISS|0.80|2026-09-15|
Cap-rate compression|MISS|0.80|2026-09-15|
Sector rotation into REITs|MISS|0.70|2026-09-15|
Sector breadth expansion (% names up)|MISS|0.75|2026-09-15|
Sector ETF inflow / relative volume spike|MISS|0.50|2026-09-15|
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -1.0, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': -1.0}, 'multiplier': 0.9, 'leading_sum': -7.0, 'divergence_flagged': True, 'total_score': -4.541, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.582, 'regime': 'risk_off', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.0375, 'score': 0.225, 'legs': [{'leg': 'ES', 'pct': 0.8, 'w': 0.5}, {'leg': 'ZN', 'pct': -0.46, 'w': 0.75}, {'leg': 'PM:XLRE', 'pct': 0.03, 'w': 0.7}]}, 'overlay_score': -4.05, 'overlay_raw': -4.05, 'index_carry': -0.716, 'general_total': -2.865, 'skill_multipliers': {'S0_SHARED_MACRO': 0.5, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 1.0}, 'llm_confidence': 0.55, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
