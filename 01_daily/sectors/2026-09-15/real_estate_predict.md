# Sector Prediction — Real Estate — 2026-09-15

- ETF: **XLRE**
- rubric: `00_grounding/sectors/real_estate.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-4.931** (mult 0.9)
- regime: risk_off
- divergence_flagged: **False**
- engine: v2 · tape_anchor **-1.359** (ES +0.30%, ZN -0.46%, PM:XLRE -0.24%) · index_carry **-1.097** (general -4.389) · llm_overlay **-2.475** (raw -2.475)

## Channel 1 sector ETF tape

```
ETF XLRE vs SPY (yfinance, through 2026-09-15):
  1d: XLRE -0.05% | SPY -0.46% | rel +0.42%
  3d: XLRE +0.12% | SPY -0.06% | rel +0.18%
  1w: XLRE -1.82% | SPY -1.12% | rel -0.70%
  1m: XLRE -4.79% | SPY -2.44% | rel -2.35%
```

# Sector Prediction — Real Estate — 2026-09-15

MEMORY_CONFIRM: Reviewed prior runs for sector Real Estate (XLRE) only. Last graded: 2026-09-14 down/mild vs XLRE −0.691% / SPY −0.446% / rel −0.245% (dir HIT, mag HIT). 2026-09-11 down/flat vs XLRE +0.859% / SPY +0.852% / rel +0.007% (dir MISS — pure beta). 2026-09-10 down/mild (dir HIT, mag HIT). 2026-09-09 down/mild (dir HIT, mag MISS, actual at upper edge of mild). 08-31/09-01/09-02 down/mild, 09-03 flat/flat, 09-04 flat/flat, 09-08 down/mild still ungraded. Rolling dir=0.4 mag=0.4 (n=10); last-30 dir=0.526 mag=0.368. Applied active REIT lessons: **(1) 08-27** — 08-25 is a ban on forcing down, not an up license; 30Y in stress zone ⇒ cap S0/S1 at 0; do not double-count one rate shock; do not pad S1 with always-on DC/industrial; green/NQ beta ≠ REIT duration relief. **(2) 08-25** — verify the live curve; do not force down off a stale prior-close rising table if the open curve is falling. **(3) 08-21 level-vs-change** — 30Y ~5.35 is a multi-decade stress zone; a small dip is not relief. **(4) 08-17/08-18** — live long-end + hawkish path → absolute down; 1d relative bid is a mag cap only. **(5) 08-11 geo/oil** — oil is UP sharply today (WTI $103.79 +2.37%, Brent $108.11 +2.31%) on Iran escalation — a **live, escalating geopolitical/oil supply-shock overlay**; the spike branch FIRES. **(6) 08-12** — two-sided policy events stay two-sided; do not one-way score S0. **(7) 09-04 asymmetric-downside** — hawkish/unresolved backdrop + rate-sensitive bond-proxy with 1w/1m lags ⇒ pre-score asymmetric downside. **(8) 09-08 cushion lesson** — 1d rel cushion ≥ +0.4% confirmed by prior session ⇒ positive S4 / direction override toward flat. **Today 1d rel is +0.42% — this is a genuine ≥ +0.4% cushion, so the 09-08 override IS in play and must be weighed.** **(9) 09-11 lesson** — when S0 is genuinely 0 and the live tape is positive, a down call requires a LIVE negative input; a stale multi-horizon lag must not be scored into S2 AND S4; "no cushion ≠ headwind"; an object explicitly identified as already-priced must be scored 0. **Today the live tape is NEGATIVE (ES −0.54%, 10Y >5%, 30Y bond −0.93%), so 09-11's precondition (positive live tape) is NOT met — the down call has a live negative input.** **(10) 09-14 lesson (NEW, most binding)** — a premarket quote in a low-liquidity rate-sensitive ETF is an UNCONFIRMED signal, not the tape; it may not set the sign of the call and may not be used as an S0 offset; an object scored in S1 may not also appear as an S0 offset; live negatives identified in prose must be scored as deductions; oil and rates are the SAME duration channel for a bond proxy. **(11) 08-14 reconcile Σ×mult.** Open `sector_real_estate` experiment: keep direction, shrink confidence on modest |score| when magnitude historically misses.

---

## Real Estate (XLRE) — 2026-09-15

### Channel 1 (used as given, not re-derived)

Rates through **2026-09-11**: DGS10 **4.96** (1d +0.01 / 1w **+0.19** / 1m **+0.28**), DGS30 **5.35** (1d −0.02 / 1w **+0.10** / 1m **+0.11**), DFII10 **2.60** (1d **+0.05** / 1w **+0.18** / 1m **+0.18**) — **real yields rising on every listed horizon**. That 1d column is **Friday's close**, not this open. VIX **17.75** (1d +0.65, 1w **+2.03**), VIX/VIX3M **0.901** (contango — note this differs from the backwardation of 09-11/09-14; the term structure has normalized even as spot VIX rose). **ES=F −0.54%**, **NQ=F −0.62%**, Russell **−0.73%**, DJIA **−0.71%** — **futures clearly negative, broad**. Asia composite **−1.1%** (Kospi −3.26%, Hang Seng −1.0%), Europe **−0.39%**. **Oil UP sharply**: WTI **$103.79 (+2.37%)**, Brent **$108.11 (+2.31%)**, Heating Oil +3.09%, Gasoil +2.68% — a **live geopolitical/oil supply-shock overlay** (08-11 fires). Gold **−1.01%**, Silver **−1.47%**, Copper **−0.76%**. DXY **+0.25%** (USD strengthening). **10Y note −0.46%**, **5Y note −0.28%**, **2Y note −0.09%**, **30Y bond −0.93%**, **Ultra Bond −1.09%** (prices down = **yields UP sharply this morning**). 5-day 10Y–SPX corr **−0.172** (weakly negative). HY OAS **2.71**. EPU **215.48** (1d −202.54 — a big drop, i.e. policy uncertainty *fell*). RRP **1.42** (1d −3.835 — big drain).

**Sector premarket vs prev close: XLRE −0.24%** — mid-pack of eleven (XLI +0.81%, XLB +0.38%, XLU +0.20% best; XLC −0.63%, XLP −0.33% worst). **No defensive rotation bid into XLRE today** — the 09-14 premarket bid (+0.48%, second-best) is absent. Per the 09-14 lesson, a premarket quote in this low-liquidity ETF is an **unconfirmed** signal and may not set the sign of the call either way. I score it 0.

XLRE vs SPY through **2026-09-15**: 1d **−0.05 / −0.46 / rel +0.42**; 3d rel **+0.18**; 1w rel **−0.70**; 1m rel **−2.35**. **The 1d rel is a genuine +0.42% defensive cushion (≥ +0.4% gate), and 3d rel is also positive (+0.18%).** But 1w/1m remain laggards, with the 1m lag widened to −2.35%. This is the 09-08 configuration — a large, pre-established defensive cushion into a risk-off session.

### Channel 2

**1. Shared macro as it hits REITs.** This is a **rates-shock / oil-shock / pre-FOMC overlay**, and it is **unambiguously a rates-backup day**. Live web confirms: **"US 10-year Treasury yield breaches 5% as global bond sell-off deepens"** (Euronews, 05:51 GMT), **"Ten-year Treasury yield hits highest level since 2007"** (FT, 04:37 GMT), **"Global bond yields hit 2008 highs, raising stakes for big borrowers"** (Reuters, 11:47 GMT), **"Global Bond Selloff Deepens as U.S. 10-Year Yield Tops 5%, 30-Year Breaks 5.4%"** (bloomingbit, 08:01 GMT), **"Stock market slips as 10-year Treasury yield tops 5% ahead of Fed decision"** (tradingview, 13:33 GMT). The 10Y note future is **−0.46%** and the Ultra Bond **−1.09%** — the long end is being sold hard, not drifting. **WRE News**: "10-Year Treasury Hits 5% as Mortgage Rates Face Fresh Pressure" — the direct REIT/cap-rate transmission channel is live and named.

The **FOMC decision lands tomorrow (Sep 16)** with the SEP/dot plot and Warsh press conference. Per the News Judge's RULES_APPLIED, this is an **unresolved binary — do not pre-score hawkish B3; keep it at 0 until it prints.** But the *pre-binary tape* is knowable and must be scored: the 10Y breaking 5% is the market pricing a hawkish outcome, and **August CPI core 0.3% MoM has already locked in a September hike** (News Judge #3, severity: session, confidence 0.80). FedWatch-derived odds are **60–65% for a 25bp hike** (Wall Street Times, 4 days ago), with one source at 91% — the range is wide but the direction is unambiguous: hike risk is live and rising.

**Oil is spiking** (WTI $103.79, Brent $108.11) on the US–Iran tanker war / impaired Hormuz — a live inflation/stagflation overlay that pressures long-duration assets (08-11 fires). **Gold −1.01% / Silver −1.47%** — no bond-proxy flight-to-safety bid in metals. **USD +0.25%** — a mild headwind for a domestic sector. **VIX 17.75 with VIX/VIX3M 0.901** — spot VIX rose but the term structure is in contango, so this is *not* the backwardated stress tell of 09-11/09-14; it is elevated-but-orderly.

**Critical: oil and rates are the SAME duration channel for a bond proxy** (09-14 lesson). An oil-driven inflation impulse that lifts term premium and real yields is a rate impulse. I count the duration shock **once** in S1 (the rate backup), and treat the oil spike as the *inflation mechanism behind* that backup — not as a second independent S1 hit. S0 carries the regime map (risk-off + rates-shock + pre-FOMC), which is a distinct object from the sector's own rate sensitivity.

**S0 = −1.** This is not a mixed day. The live curve is verified rising (10Y >5%, 30Y >5.4%, Ultra Bond −1.09%), futures are broadly red, oil is spiking, and a hawkish CPI print is already in the market ahead of tomorrow's FOMC. The 08-25 "don't force down off a stale table" branch does **not** apply — the live curve confirms the direction of the stale table rather than contradicting it.

**2. Spine (count the rate shock once in S1; S0 is the regime map, not a second copy of the same backup).**
- Rates falling / REIT duration relief: **MISS.** Live curve is rising hard (10Y >5%, 30Y >5.4%).
- Rates rising / REIT selloff: **HIT.** 10Y breaches 5% (highest since 2007), 30Y breaks 5.4%, 30Y bond futures −0.93%, Ultra Bond −1.09%, CPI core 0.3% locks a hike, FOMC tomorrow. This is the spine negative.
- Real yields rising: **HIT** (DFII10 +5 bp 1d / +18 bp 1w / +18 bp 1m). Same duration channel as the nominal backup — **not** a second independent shock.

**3. Secondary.**
- Data-center REIT demand / rent upside: **HIT, stale / mixed.** MAP HEAT: Specialty (EQIX/AMT) **dir=down, conv=low** — "EQIX and AMT both red on the week despite AI/data-center headlines." The AI-complex unwind (AMD −5% premarket, chip weakness) is a **headwind** to the DC sleeve today, not a support. **08-27: not a same-day up vote.** EQIX/DLR must not define XLRE.
- Industrial REIT occupancy / rent growth: **HIT, stale and negative on tape.** MAP HEAT: Industrial **dir=down, conv=medium** — "PLD −2.66% and PSA −3.67% w1 with no positive news offset." Quality sleeve, not a 1-day catalyst.
- Refinancing window / cap-rate compression: **MISS.** 10Y >5%, 30Y >5.4%; cap rates expanding, not compressing.
- Office vacancy / mark-to-market: **HIT, small sleeve, and the cleanest negative.** MAP HEAT: Office **dir=down, conv=high** — "−2.57% w1, −2.04 vs parent, zero breadth, BXP adding debt." Office ~1% of XLRE (BXP). Do not let office set the ETF.
- Refinancing wall: **HIT, structural.** 2026 CRE maturity wall intact; a 5%+ 10Y tightens the refinancing window further. Not a same-morning print.
- Sector rotation out of real estate: **HIT on 1w/1m price** (see S2/S4). Do not also dump it into S1.

**4. Breadth / leadership.** MAP HEAT is unusually informative today and is **net negative on breadth**: of nine REIT sub-sectors, **five are dir=down** (Office high-conviction, Hotel, Industrial, Mortgage, Specialty, Development), **two flat** (Diversified, Residential), and only **two up** (Healthcare Facilities medium, Retail medium). The two positive pockets are narrow: Healthcare Facilities (WELL/VTR/CTRE) and Retail (SPG/O/MAC/EPRT). That is **not** sector breadth expansion — it is a two-pocket dispersion story inside a broadly leaking sector. The 1d rel cushion (+0.42%) is a *relative* artifact of SPY falling harder (−0.46%) than XLRE (−0.05%), not evidence of % names expanding. **S2 = −0.5** (mild breadth failure — the ETF's relative resilience is not corroborated by sub-sector breadth).

**5. Flows / positioning.** No fresh same-day XLRE flow print available (ICI weekly data is lagged to the week ended Sep 2; ETF.com/Trefis pages did not return a live number). The 1m relative lag of −2.35% into a 5%+ 10Y is consistent with persistent demand-soft positioning, but I have **no live flow evidence** to score. Per the 09-11 lesson, I will not manufacture a negative from a stale object. **S3 = 0** (no live flow signal; not a crowded long — a −2.35% 1m laggard is not crowded).

**6. Earnings / policy.** No fresh REIT print this morning. The dominant objects are the **live 10Y >5% break** (already scored in S1), the **already-printed CPI core 0.3%** (the mechanism behind the rate backup — not a second hit), and **tomorrow's FOMC/SEP/Warsh** (unresolved binary — scored 0, encoded as a confidence cap). MAP HEAT notes VICI's dividend raise as the only live single-name catalyst in Diversified — a single-name item that must not define XLRE.

### Divergence check

The engine's `tape_anchor` is **+0.225** (ES +0.80%, ZN −0.46%, PM:XLRE +0.03%) while `llm_overlay` is **−4.05**, and `divergence_flagged: True`. The tape_anchor's ES +0.80% input conflicts with Channel 1's ES **−0.54%** — the anchor appears to be built on a stale/incorrect futures read. Channel 1 is authoritative and says futures are **red**. The ZN −0.46% input agrees with Channel 1's rates-backup read. So the divergence is largely an artifact of a bad ES input in the anchor, not a genuine signal conflict. **I trust the factors (S0/S1) over the tape anchor**, per the shared method's divergence rule, but I flag it and cut confidence.

### Self-audit

- **Lens:** duration/rates for a bond-proxy REIT ETF. Correct.
- **Band:** |Σ| is modest; rolling mag discipline (0.368 last-30) plus a pending FOMC binary ⇒ cap at **mild**.
- **Skew:** the 09-04 asymmetric-downside lesson applies (hawkish/unresolved backdrop, 1w/1m lags) — but it is **partially offset** by the 09-08 cushion override, which genuinely fires today (+0.42% ≥ +0.4% gate, confirmed by the prior session's actuals: XLRE −0.05% vs SPY −0.46%). Per 09-08, when the cushion is large and the negative spine is structural rather than a fresh same-day shock to REITs specifically, I cap the combined S0+S1 at **−1.5** rather than −2. That is what I have done (S0 −1, S1 −0.5).
- **Same-shock double-count:** oil and rates are the same duration channel — counted once (S1). The CPI print is the mechanism behind the rate backup — not scored separately. The premarket quote is scored 0 and is **not** used as an S0 offset (09-14 lesson).
- **Single-ticker:** WELL, EQIX, DLR, PLD, BXP are all explicitly barred from defining the ETF call. MAP HEAT sub-sector reads are used as breadth evidence, not as the thesis.

**Net:** Σ = −1 (S0) + −0.5 (S1) + −0.5 (S2) + 0 (S3) + −0.5 (S4) = **−2.5**; × mult 0.9 = **−2.25**. Direction **down**, magnitude **mild**, confidence reduced for the pending FOMC binary and the genuine +0.42% cushion.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: -0.5
S2_BREADTH: -0.5
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: -0.5
MULTIPLIER: 0.9
CONFIDENCE: 0.5
REGIME: risk_off
DIVERGENCE_FLAGGED: True
HORIZON_3D: down/mild
HORIZON_1W: down/mild
HORIZON_2W: flat/mild
HORIZON_1M: flat/mild
SECTOR_SCORES_END

HIT_GRID_BEGIN
Rates rising / REIT selloff|HIT|0.85|2026-09-15|https://www.ft.com/content/ten-year-treasury-yield-highest-since-2007
Real yields rising|HIT|0.75|2026-09-15|https://fred.stlouisfed.org/series/DFII10
Risk-off tape / flight to safety|HIT|0.7|2026-09-15|https://www.tradingview.com/news/stock-market-slips-10-year-treasury-yield-tops-5
Cap-rate expansion|HIT|0.6|2026-09-15|https://wrenews.com/10-year-treasury-5-percent-mortgage-rates-fed-september-2026/
Office vacancy / mark-to-market stress|HIT|0.7|2026-09-15|https://www.marketbeat.com/instant-alerts/list-promising-real-estate-stocks-to-watch-now-september-12th-2026-09-12/
Refinancing wall stress|HIT|0.5|2026-09-15|https://investmentgrade.com/investment-grade-reit-bonds/
Sector breadth failure (ETF up, names flat)|HIT|0.55|2026-09-15|https://www.thedesperatetrader.com/sector-performance
Sector rotation out of real estate|HIT|0.5|2026-09-15|https://marketindicators.net/sectors
USD strengthening|HIT|0.4|2026-09-15|https://www.reuters.com/markets/currencies/
Rates falling / REIT duration relief|MISS|0.85|2026-09-15|https://www.euronews.com/business/us-10-year-treasury-yield-breaches-5
Real yields falling|MISS|0.75|2026-09-15|https://fred.stlouisfed.org/series/DFII10
Refinancing window opening|MISS|0.6|2026-09-15|https://wrenews.com/10-year-treasury-5-percent-mortgage-rates-fed-september-2026/
Cap-rate compression|MISS|0.6|2026-09-15|https://wrenews.com/10-year-treasury-5-percent-mortgage-rates-fed-september-2026/
Data-center REIT demand / rent upside|MISS|0.5|2026-09-15|https://www.marketbeat.com/instant-alerts/list-promising-real-estate-stocks-to-watch-now-september-12th-2026-09-12/
Industrial REIT occupancy / rent growth|MISS|0.5|2026-09-15|https://www.marketbeat.com/instant-alerts/list-promising-real-estate-stocks-to-watch-now-september-12th-2026-09-12/
Sector rotation into REITs|MISS|0.5|2026-09-15|https://www.thedesperatetrader.com/sector-performance
Sector ETF inflow / relative volume spike|UNCONFIRMED|0.3|2026-09-15|https://www.ici.org/research/stats/combined_flows
Crowded long (extreme relative performance + valuation)|UNCONFIRMED|0.3|2026-09-15|https://www.trefis.com/data/etfs/XLRE
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -0.5, 'S2_BREADTH': -0.5, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': -0.5}, 'multiplier': 0.9, 'leading_sum': -4.5, 'divergence_flagged': False, 'total_score': -4.931, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.65, 'regime': 'risk_off', 'engine': 'v2', 'anchor': {'available': True, 'pct': -0.2265, 'score': -1.359, 'legs': [{'leg': 'ES', 'pct': 0.3, 'w': 0.5}, {'leg': 'ZN', 'pct': -0.46, 'w': 0.75}, {'leg': 'PM:XLRE', 'pct': -0.24, 'w': 0.7}]}, 'overlay_score': -2.475, 'overlay_raw': -2.475, 'index_carry': -1.097, 'general_total': -4.389, 'skill_multipliers': {'S0_SHARED_MACRO': 0.5, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 1.0}, 'llm_confidence': 0.5, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
