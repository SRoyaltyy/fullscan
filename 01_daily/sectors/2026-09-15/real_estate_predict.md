# Sector Prediction — Real Estate — 2026-09-15

- ETF: **XLRE**
- rubric: `00_grounding/sectors/real_estate.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-7.345** (mult 0.9)
- regime: risk_off
- divergence_flagged: **False**
- engine: v2 · tape_anchor **-1.323** (ES +0.34%, ZN -0.46%, PM:XLRE -0.24%) · index_carry **-0.959** (general -3.836) · llm_overlay **-5.062** (raw -5.062)

## Channel 1 sector ETF tape

```
ETF XLRE vs SPY (yfinance, through 2026-09-15):
  1d: XLRE -0.15% | SPY -0.46% | rel +0.31%
  3d: XLRE +0.01% | SPY -0.06% | rel +0.07%
  1w: XLRE -1.92% | SPY -1.12% | rel -0.80%
  1m: XLRE -4.89% | SPY -2.44% | rel -2.45%
```

I now have full Channel 2 coverage. Writing the analysis.

---

MEMORY_CONFIRM: Reviewed prior runs for sector Real Estate (XLRE) only. Last graded: 2026-09-14 down/mild vs XLRE −0.691% / SPY −0.446% / rel −0.245% (dir HIT, mag HIT). 2026-09-11 down/flat vs XLRE +0.859% / SPY +0.852% / rel +0.007% (dir MISS — pure beta). 2026-09-10 down/mild (dir HIT, mag HIT). 2026-09-09 down/mild (dir HIT, mag MISS, actual at upper edge of mild). 08-31/09-01/09-02 down/mild, 09-03 flat/flat, 09-04 flat/flat, 09-08 down/mild still ungraded. Rolling dir=0.4 mag=0.4 (n=10); last-30 dir=0.526 mag=0.368. Applied active REIT lessons: **(1) 08-27** — 08-25 is a ban on forcing down, not an up license; 30Y in stress zone ⇒ cap S0/S1 at 0; do not double-count one rate shock; do not pad S1 with always-on DC/industrial; green/NQ beta ≠ REIT duration relief. **(2) 08-25** — verify the live curve; do not force down off a stale prior-close rising table if the open curve is falling. **(3) 08-21 level-vs-change** — 30Y ~5.35 is a multi-decade stress zone; a small dip is not relief. **(4) 08-17/08-18** — live long-end + hawkish path → absolute down; 1d relative bid is a mag cap only. **(5) 08-11 geo/oil** — oil is UP sharply today (WTI $103.79 +2.37%, Brent $108.11 +2.31%) on Iran/Hormuz escalation — a **live, escalating geopolitical/oil supply-shock overlay**; the spike branch FIRES. **(6) 08-12** — two-sided policy events stay two-sided; do not one-way score S0. **(7) 09-04 asymmetric-downside** — hawkish/unresolved backdrop + rate-sensitive bond-proxy with 1w/1m lags ⇒ pre-score asymmetric downside. **(8) 09-08 cushion lesson** — 1d rel cushion ≥ +0.4% confirmed by prior session ⇒ positive S4 / direction override toward flat. **Today 1d rel is +0.31% (below the +0.4% gate) — the 09-08 override does NOT fire.** **(9) 09-11 lesson** — when S0 is genuinely 0 and the live tape is positive, a down call requires a LIVE negative input; a stale multi-horizon lag must not be scored into S2 AND S4; "no cushion ≠ headwind"; an object explicitly identified as already-priced must be scored 0. **Today the live tape is NEGATIVE (ES −0.54%, 10Y ≥5.0%, 30Y bond −0.93%, Ultra Bond −1.09%), so 09-11's precondition (positive live tape) is NOT met — the down call has a live negative input.** **(10) 09-14 lesson (most binding)** — a premarket quote in a low-liquidity rate-sensitive ETF is an UNCONFIRMED signal, not the tape; it may not set the sign of the call and may not be used as an S0 offset; an object scored in S1 may not also appear as an S0 offset; live negatives identified in prose must be scored as deductions; oil and rates are the SAME duration channel for a bond proxy. **(11) 08-14 reconcile Σ×mult.** Open `sector_real_estate` experiment: keep direction, shrink confidence on modest |score| when magnitude historically misses.

---

## Real Estate (XLRE) — 2026-09-15

### Channel 1 (used as given, not re-derived)

Rates through **2026-09-11**: DGS10 **4.96** (1d +0.01 / 1w **+0.19** / 1m **+0.28**), DGS30 **5.35** (1d −0.02 / 1w **+0.10** / 1m **+0.11**), DFII10 **2.60** (1d **+0.05** / 1w **+0.18** / 1m **+0.18**) — **real yields rising on every listed horizon**. That 1d column is **Friday's close**, not this open. VIX **17.49** (1d +0.39, 1w +1.77), VIX/VIX3M **0.897** (contango — term structure normalized vs the 09-11/09-14 backwardation, even as spot VIX rose). **ES=F −0.54%**, **NQ=F −0.62%**, Russell **−0.73%**, DJIA **−0.71%** — **futures clearly negative, broad**. Asia composite **−0.72%** (Kospi −3.26%), Europe **−0.31%**. **Oil UP sharply**: WTI **$103.79 (+2.37%)**, Brent **$108.11 (+2.31%)**, Heating Oil +3.09%, Gasoil +2.68% — a **live geopolitical/oil supply-shock overlay** (08-11 fires). Gold **−1.01%**, Silver **−1.47%**, Copper **−0.76%**. DXY **+0.25%** (USD strengthening). **10Y note −0.46%**, **5Y note −0.28%**, **2Y note −0.09%**, **30Y bond −0.93%**, **Ultra Bond −1.09%** (prices down = **yields UP sharply this morning**). 5-day 10Y–SPX corr **−0.151** (weakly negative). HY OAS **2.71**. EPU **215.48** (1d −202.54 — policy uncertainty *fell*). RRP **1.42** (1d −3.835 — big drain).

**Sector premarket vs prev close: XLRE −0.24%** — mid-pack of eleven (XLI +0.81%, XLB +0.38%, XLU +0.20% best; XLC −0.63%, XLP −0.33% worst). **No defensive rotation bid into XLRE today** — the 09-14 premarket bid (+0.48%, second-best) is absent. Per the 09-14 lesson, a premarket quote in this low-liquidity ETF is an **unconfirmed** signal and may not set the sign of the call either way. I score it 0.

XLRE vs SPY through **2026-09-15**: 1d **−0.15 / −0.46 / rel +0.31**; 3d rel **+0.07**; 1w rel **−0.80**; 1m rel **−2.45**. **The 1d rel is +0.31% — a mild defensive cushion but BELOW the +0.4% gate, so the 09-08 override does NOT fire.** 3d rel is flat (+0.07%). 1w/1m remain laggards, with the 1m lag widened to −2.45%. This is a **mild-cushion / structural-laggard** configuration, not the 09-08 large-cushion setup.

### Channel 2

**1. Shared macro as it hits REITs.** This is a **rates-shock / oil-shock / pre-FOMC overlay**, and it is **unambiguously a rates-backup day**. Live web confirms: **"US 10-year Treasury yield breaches 5% as global bond sell-off deepens"** (Euronews), **"Ten-year Treasury yield hits highest level since 2007"** (FT), **"Global bond yields hit fresh highs"** (Reuters), **"Stock market slips as 10-year Treasury yield tops 5% ahead of Fed decision"** (Seeking Alpha/TradingView). The single dominant cross-asset driver is the **global duration shock** — and the live futures tape confirms it: 30Y bond **−0.93%**, Ultra Bond **−1.09%**, 10Y note **−0.46%**. This is the **08-17/08-18 REIT worst case**: rising long-end yields + risk-off equity tape. **Oil is spiking** (WTI $103.79, Brent $108.11) on the US–Iran tanker war / impaired Hormuz traffic — a live inflation/stagflation overlay that pressures long-duration assets (08-11 fires). **USD +0.25%** — a mild headwind for a domestic sector. **FOMC is tomorrow (Sep 16)** with **hike odds ~94%** per prediction markets (Manifold) and "Gaming the Fed Odds for a Rate Hike Wednesday" (TradingView) — the hawkish path is **already printed and priced** (Warsh "work to do," Aug core CPI surprise "locks in" September hike). Per 09-11/09-14, an already-priced object is scored **0**, not as a fresh headwind. **UMich Sept prelim 47.8 with 1-yr inflation expectations at 4.6%** confirms the hike path and squeezes risk appetite. **VIX/VIX3M 0.897 contango** — no term-structure stress signal, so I do not add a VIX-backwardation deduction (unlike 09-14).

**S0 = −1.** The live, knowable-at-open rate shock (10Y ≥5.0%, 30Y bond −0.93%, Ultra Bond −1.09%) is a genuine, live negative for a long-duration bond proxy — this is the 08-17/08-18 branch, not the 09-11 "no live negative" branch. I do **not** add a second S0 hit for oil (09-14: oil and rates are the SAME duration channel for a bond proxy — count once). I do **not** score the already-priced FOMC/hike path as a fresh headwind.

**2. Spine (count the rate shock once in S1; S0 is the regime map, not a second copy of the same backup).**
- Rates falling / REIT duration relief: **MISS.** Live curve is rising sharply (30Y bond −0.93%, Ultra Bond −1.09%), not falling.
- Rates rising / REIT selloff: **HIT.** 10Y breaches 5% (highest since 2007), 30Y ~5.35% stress zone, 30Y bond futures −0.93%. This is the spine negative.
- Real yields rising: **HIT** (DFII10 +5 bp 1d / +18 bp 1w / +18 bp 1m). Same duration channel as the nominal backup — **not** a second independent shock.

**S1 = −1.5.** The rate spine is a clean HIT and the sector's own 1w/1m relative tape (−0.80% / −2.45%) independently confirms the transmission channel is firing — this satisfies the "sector-specific transmission must be confirmed by the sector's own relative tape" gate. I do **not** stack the stale DC/industrial occupancy items as up votes (08-27), and I do **not** add a separate S1 hit for the oil shock (same duration channel).

**3. Secondary.**
- Data-center REIT demand / rent upside: **HIT, but MIXED-to-NEGATIVE today.** EQIX and DLR **slumped** on AI-slowdown warnings (CNBC 09-15: "Digital Realty and Equinix... saw their stocks slump after warnings over AI advancements"). DLR's CEO pushed back ("not end of the world"), but the tape is red. This is a **live negative for the DC sleeve**, not a stale positive. **08-27: EQIX/DLR must not define XLRE** — I score this as a small negative inside S1, not the ETF driver.
- Industrial REIT occupancy / rent growth: **HIT, stale/negative.** MAP HEAT: PLD −2.66% w1, PSA −3.67% w1, no positive offset. Quality sleeve, not a 1-day catalyst.
- Refinancing window / cap-rate compression: **MISS.** 10Y ≥5.0%, 30Y ~5.35%; no compression — cap-rate expansion pressure instead.
- Office vacancy / mark-to-market: **HIT, small sleeve.** MAP HEAT: office is "the cleanest short," −2.57% w1, −2.04 vs parent, BXP adding debt. Office ~1% of XLRE. Do not let office set the ETF.
- Refinancing wall: **HIT, structural.** 2026 CRE maturity wall intact ($100B+ CMBS maturing, office delinquency >11–12%). Not a same-morning print.
- Sector rotation out of real estate: **HIT on 1w/1m price** (see S2/S4). Do not also dump it into S1.

**4. Breadth / leadership.** MAP HEAT is decisively negative on breadth: **Office down (high conv), Hotel down, Industrial down, Mortgage down, Specialty down, Development down**; only **Retail up (medium)** and **Healthcare Facilities up (medium)**. That is **2 up / 7 down** sub-sectors — genuine breadth failure, not ETF-only weakness. Premarket XLRE −0.24% mid-pack with no rotation bid. The 1d rel +0.31% is a mild defensive cushion vs SPY, not % names expanding. **WELL/CTRE (healthcare) and SPG/O/MAC (retail) must not define the ETF call** — they are the two positive pockets inside a broadly leaking sector.

**S2 = −1.** Breadth failure is confirmed by independent sub-sector evidence (MAP HEAT), not by a single stale rel print — this is the legitimate use of S2, distinct from the 09-11 error (where a single stale 1d rel print was scored into both S2 and S4). Here S2 rests on the sub-sector dispersion table.

**5. Flows / positioning.** TradingView (09-15): **"Eight of 11 sectors record outflows; the financial sector leads inflows"** — real estate is among the outflow sectors. Seeking Alpha (09-01): short sellers targeted five REIT stocks up to $2B market cap in August. No same-day volume spike; no crowded-long unwind signal. Trailing outflow into a −2.45% 1m relative laggard is **demand-soft, not a washout setup**. **S3 = −0.5.**

**6. Earnings / policy.** No fresh REIT earnings this morning. Live sector catalysts are **mixed-to-positive but small**: Realty Income (O) + KKR form a **€528M European net-lease JV** (private capital validating net-lease; O is a top-10 XLRE weight); VICI dividend raise; CTRE deal flow; National Health Investors $272M signed opportunities. These are **single-name / sub-sector positives** that do not repair the duration-driven ETF tape. Dominant objects remain the **live 10Y ≥5% shock** + **live Hormuz oil spike** + **FOMC tomorrow (already priced)**.

### Divergence check
Leading factors (S0 −1, S1 −1.5, S2 −1, S3 −0.5) sum to **−4.0**; tape confirmation S4 = **+0.5** (mild positive 1d rel cushion, below the override gate). **Factors and tape disagree in sign** — the tape's mild cushion fights the factor stack. Per the divergence rule, I **trust factors over tape** (the cushion is +0.31%, below the +0.4% gate, and the live rate shock is unambiguous), but I **flag the divergence** and cut confidence. This is the 09-08 lesson's boundary condition: the cushion is real but sub-gate, so it caps magnitude rather than flipping direction.

### Self-audit
- **Lens:** duration/rates for a bond proxy — correct lens, not SPX beta.
- **Band:** mild — no full 08-18 long-end smash (30Y bond −0.93% is a backup, not a capitulation), and the +0.31% cushion caps downside extent.
- **Skew:** asymmetric downside per 09-04 (hawkish/unresolved backdrop + 1w/1m lags + no ≥+0.4% cushion).
- **Same-shock double-count:** oil and rates counted ONCE (S0 regime map); the rate backup counted ONCE (S1 spine); the DC-sleeve AI-slowdown weakness is a distinct sector-specific object, not a re-score of the macro shock.
- **Single-ticker:** EQIX/DLR/WELL/O/SPG explicitly barred from defining the ETF call; the two positive sub-sectors (retail, healthcare) are noted but not allowed to carry the call.
- **09-11 gate:** precondition (positive live tape) NOT met — live tape is negative, so the down call has a live negative input. ✓
- **09-14 gate:** premarket quote scored 0, not used as an S0 offset; no object scored in both S1 and S0. ✓

**Direction: down. Magnitude: mild. Regime: risk_off. Confidence: 0.55 (cut for the sub-gate cushion divergence and the pending FOMC binary).**

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1.0
S1_SECTOR_FACTORS: -1.5
S2_BREADTH: -1.0
S3_FLOWS_POSITIONING: -0.5
S4_ETF_TAPE: 0.5
MULTIPLIER: 0.9
CONFIDENCE: 0.55
REGIME: risk_off
DIVERGENCE_FLAGGED: True
SECTOR_DIRECTION: down
SECTOR_MAGNITUDE_BAND: mild
HORIZON_3D: down/mild
HORIZON_1W: down/mild
HORIZON_2W: flat/mild
HORIZON_1M: flat/mild
SECTOR_SCORES_END

HIT_GRID_BEGIN
Rates rising / REIT selloff|HIT|0.85|2026-09-15|https://www.ft.com/content/ten-year-treasury-yield-highest-since-2007
Real yields rising|HIT|0.80|2026-09-15|https://fred.stlouisfed.org/series/DFII10
Risk-off tape / flight to safety|HIT|0.70|2026-09-15|https://qz.com/stock-futures-fall-10-year-treasury-yield-tops-5
Sector breadth failure (ETF up, names flat)|HIT|0.75|2026-09-15|https://www.money365.market/news/sector/realestate/brief/2026-09-15
Sector rotation out of real estate|HIT|0.70|2026-09-15|https://www.tradingview.com/news/weekly-etfs-eight-of-11-sectors-outflows
Sector ETF outflow / volume dry-up|HIT|0.65|2026-09-15|https://www.tradingview.com/news/weekly-etfs-eight-of-11-sectors-outflows
Cap-rate expansion|HIT|0.60|2026-09-15|https://www.credaily.com/briefs/cmbs-office-distress-hits-2026-maturity-wall/
Office vacancy / mark-to-market stress|HIT|0.70|2026-09-15|https://trybiut.com/blog/commercial-real-estate-office-vacancy-crisis-2026
Refinancing wall stress|HIT|0.60|2026-09-15|https://deluair.com/consultancy/insights/us-cre-office-distress-2026
USD strengthening|HIT|0.55|2026-09-15|https://www.reuters.com/markets/global-bond-yields-hit-fresh-highs
Data-center REIT demand / rent upside|PARTIAL|0.50|2026-09-15|https://www.cnbc.com/2026/09/15/data-center-reit-ceo-ai-slowdown.html
Industrial REIT occupancy / rent growth|PARTIAL|0.45|2026-09-15|https://www.money365.market/news/sector/realestate/brief/2026-09-15
Refinancing window opening|MISS|0.70|2026-09-15|https://fred.stlouisfed.org/series/DGS10
Cap-rate compression|MISS|0.70|2026-09-15|https://fred.stlouisfed.org/series/DGS10
Rates falling / REIT duration relief|MISS|0.85|2026-09-15|https://www.bloomberg.com/news/global-bond-selloff-10-year-treasury-cusp-of-5
Real yields falling|MISS|0.80|2026-09-15|https://fred.stlouisfed.org/series/DFII10
Sector rotation into REITs|MISS|0.60|2026-09-15|https://www.benzinga.com/etfs/sector-etfs/26/09/61789034/leading-and-lagging-sectors-september-15-2026
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -1.5, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': -0.5, 'S4_ETF_TAPE': 0.5}, 'multiplier': 0.9, 'leading_sum': -8.5, 'divergence_flagged': False, 'total_score': -7.345, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.65, 'regime': 'risk_off', 'engine': 'v2', 'anchor': {'available': True, 'pct': -0.2205, 'score': -1.323, 'legs': [{'leg': 'ES', 'pct': 0.34, 'w': 0.5}, {'leg': 'ZN', 'pct': -0.46, 'w': 0.75}, {'leg': 'PM:XLRE', 'pct': -0.24, 'w': 0.7}]}, 'overlay_score': -5.062, 'overlay_raw': -5.062, 'index_carry': -0.959, 'general_total': -3.836, 'skill_multipliers': {'S0_SHARED_MACRO': 0.5, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 1.0}, 'llm_confidence': 0.55, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
