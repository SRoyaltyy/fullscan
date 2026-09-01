# Sector Prediction — Real Estate — 2026-09-01

- ETF: **XLRE**
- rubric: `00_grounding/sectors/real_estate.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-6.75** (mult 0.9)
- regime: risk_off
- divergence_flagged: **False**

## Channel 1 sector ETF tape

```
ETF XLRE vs SPY (yfinance, through 2026-08-31):
  1d: XLRE -0.83% | SPY -0.30% | rel -0.53%
  3d: XLRE -2.17% | SPY +0.13% | rel -2.30%
  1w: XLRE -2.69% | SPY +0.47% | rel -3.16%
  1m: XLRE -2.13% | SPY +2.68% | rel -4.81%
```

MEMORY_CONFIRM: Memory index unavailable this run (embedding metadata mismatch) — used injected Real Estate scoreboard, standing REIT lessons, and last-10 XLRE logs only. Last graded: 2026-08-28 down/mild vs XLRE −0.403% / SPY −0.227% / rel −0.176% (dir HIT, mag HIT). 2026-08-31 down/mild still ungraded. Rolling dir=0.5 mag=0.5 (n=10). Applied: **08-25** live-curve check (CNBC US10Y ~4.79 vs ~4.76 prior close — **rising**, not a second-day decline, so the oil-slide “don’t force down” branch is **off**); **08-27** (08-25 is not an up license; 30Y still in the multi-decade stress zone; do not pad S1 with always-on DC/industrial; leftover NQ/AI beta ≠ duration relief); **08-21** level-vs-change (30Y ~5.27% is still the 19-year-high zone; a 3–6 bp backup is a live shock, not noise); **08-17/08-18** live long-end + oil/geo inflation → absolute **down**, relative bid only a mag cap — and today there is **no** relative bid; **08-11** geo/oil **does fire** (Hormuz tanker strike, Brent >$90, CL +2%); **08-12** two-sided rule does **not** park Warsh (speech already printed hawkish 8/28; News Judge: post-Warsh path is live); **08-28** (do not import XLF/XLY leftover-S2/S4 down-bans onto XLRE while the duration overlay is live; keep mag **mild** unless a full 08-18 smash); **08-14** reconcile Σ×mult. Open `sector_real_estate` experiment: last losses said prefer flat/mild when score fights tape — **today factors and tape agree**, so keep the down lean and shrink confidence.

## Real Estate (XLRE) — 2026-09-01

### Channel 1 (used as given)
Rates through **2026-08-28**: DGS10 **4.73** (1d **+0.06** / 1m **+0.05**), DGS30 **5.22** (1d **+0.03**), DFII10 **2.42** (1d **+0.08** / 1w **+0.02** / 1m **+0.01**) — **real yields rising on every listed horizon**. VIX **15.81** (+0.89), VIX/VIX3M **1.036 backwardation**. ES **−0.53%**, NQ **−1.01%**. Asia **−0.22%**, Europe **−0.69%**. CL **+2.06%** / BZ **+1.49%**; Finviz WTI **$88.01 (+2.59%)**, Brent **$92.25 (+1.89%)**. Gold **−1.31%**. DXY **+0.16%**. 30Y bond futures **−0.43%**, 10Y note **−0.15%**. HY OAS **2.6** (still tight). 10Y–SPX 5d corr **−0.481**.

XLRE vs SPY through **2026-08-31**: 1d **−0.83 / −0.30 / rel −0.53**; 3d rel **−2.30**; 1w rel **−3.16**; 1m rel **−4.81**. **Every horizon is a relative laggard.** Confirmation, not a thesis.

### Channel 2

**1. Shared macro as it hits REITs.** This is a duration/risk-off overlay, not a flight-to-safety bid into REITs. Live 10Y (CNBC US10Y) **~4.79%** (open ~4.76, prior close ~4.76) — **verified rising**, not the 08-25 falling-curve setup. 30Y **~5.27%**, still inside the 2007 / 19-year stress zone (08-21). Real yields already **+8 bp 1d** in Channel 1. Warsh (8/28) is **printed hawkish**: Sep hike odds **~60–67%** (Fool ~60%; MacroMicro/CME ~66–67%), not a two-sided speech still in the future (08-12/08-28 parking rule is off). Hormuz is **live**: UKMTO tanker struck by three projectiles; US–Iran strikes resumed; CNBC 8:19 ET Brent **$91.90 (+1.6%)**, WTI **$87.48 (+2%)**. Gold is **down**, not a bond-proxy bid. Equity risk-off with **rising** long-end yields is the 08-18 REIT worst case: absolute down, and unlike 08-18 there is **no** defensive relative cushion. ISM Manufacturing / JOLTS **10:00 ET** are two-sided and **unprinted** — encoded as a mag/confidence cap, not a pre-scored hawkish HIT. Green-futures reversal checklist **does not fire**.

**2. Spine (count the rate shock once as the sector factor; S0 is the macro map, not a second copy of the same 6 bp).**
- Rates falling / REIT duration relief: **MISS**. Live curve is up; 30Y ≥5.15%.
- Rates rising / REIT selloff: **HIT**. Live 10Y ~4.79, 30Y ~5.27 stress zone, 30Y futures −0.43%, hike odds doubled post-Warsh.
- Real yields rising: **HIT** (DFII10 +0.08 1d / +0.02 1w). Same duration channel as the nominal backup — not a second independent shock.

**3. Secondary.**
- Data-center REIT demand / rent upside: **HIT, stale**. EQIX/DLR Q2 guides already raised in July; Aug Singapore/Zurich expansions and Mizuho Outperform are 1w–1m dispersion. **08-27: not a same-day up vote.** EQIX/DLR must not define XLRE.
- Industrial occupancy / rent growth: **HIT, stale** (PLD quality sleeve). Same rule.
- Refinancing window / cap-rate compression: **MISS**. Long end ~5.27%; no compression.
- Office vacancy / mark-to-market: **HIT, small sleeve**. CBRE Q2 vacancy **18.3%** (down 30 bp — modest heal, still stressed). Office ~1% of XLRE (BXP). Do not let office set the ETF.
- Refinancing wall: **HIT, structural**. 2026 CRE maturity wall intact; Trepp **$12.1B** performing office loans with DSCR <1. Not a same-morning print.
- Sector rotation out of real estate: **HIT on price** (see S2/S4). Do not also dump it into S1.

**4. Breadth / leadership.** Premarket: WELL ~flat, EQIX ~flat, AMT ~−0.3%, **PLD ~−0.8%**. Large-caps are **not** carrying; no WELL-only smash and no breadth expansion. Multi-horizon lag is **sector-wide duration**, not ETF-only. WELL cannot set the call.

**5. Flows / positioning.** ETFdb ~Sep 1: XLRE **5d +$20.5M**, **1m −$122.5M**, 3m still **+$340M**. Near-term mixed; 1m outflow into a −4.8% 1m relative laggard is unconfirmed washout, **not** a crowded long and **not** a 1-day lid. No same-day volume spike.

**6. Earnings / policy.** No fresh REIT print this morning. Dominant objects are **already-printed Warsh hawkish path** + **live Hormuz/oil**. ISM/JOLTS 10:00 are the only scheduled binaries — two-sided, not scored as REIT-down before they print. Calendar-check passed (no fake 8:30 CPI).

### Self-audit
- **Lens:** duration/rates for a bond-proxy, not an SPX beta call. NQ −1.01% is XLK, not REIT relief (08-27).
- **Band:** live long-end + oil is 08-18-like for **direction**, not a 30Y smash + ES −1.5% for **notable**. Rolling mag 0.5; ISM pending; VIX 15.8 not panic. **Mild.**
- **Skew:** hawkish path is printed; ISM is the only remaining two-sided print — do not pre-score it.
- **Same-shock:** rates/real-yields/hike-odds/oil-inflation = **one duration cluster**. Paid in S0 (macro map) and S1 (spine HIT), not again in S4 as a thesis.
- **Single-ticker:** WELL/EQIX/DLR/PLD are dispersion. Premarket is mixed-to-soft, not one name.

### Divergence
Leading (S0+S1+S2+S3) = **−3**. Tape S4 = **−1**. **Not flagged.** Factors and tape agree. Trust the rate spine; do not fade a live 10Y/30Y backup just because 8/31 already printed a down day — the curve and Hormuz premium are **still live** at this open (continuation, not leftover).

**Component arithmetic (pipeline owns totals/direction):** Σ = −4 × 0.9 = **−3.6** → environment **down / mild**. Absolute XLRE can still print a few tenths if ISM misses and a brief FTS bid appears; 08-28 utilities showed a growth miss does **not** FTS-bid duration when the long end is sticky. Do not promote to notable off yesterday’s rel −0.53% or off unprinted ISM.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: -1
S2_BREADTH: -1
S3_FLOWS_POSITIONING: 0
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
Risk-on tape / equity beta expansion|MISS|0.80|2026-09-01|https://www.cnbc.com/quotes/US10Y
Risk-off tape / flight to safety|HIT|0.75|2026-09-01|https://www.cnbc.com/2026/09/01/us-iran-war-trump-hormuz-tanker-attack-shipping-sanctions-.html
Real yields rising|HIT|0.85|2026-09-01|Channel 1 DFII10 2.42 +0.08 1d
Real yields falling|MISS|0.85|2026-09-01|Channel 1 DFII10
USD strengthening|PARTIAL|0.55|2026-09-01|Channel 1 DXY +0.16% 1d
USD weakening|MISS|0.55|2026-09-01|Channel 1 DXY
Sector breadth expansion (% names up)|MISS|0.70|2026-09-01|premarket WELL/EQIX flat, PLD ~-0.8%
Sector breadth failure (ETF up, names flat)|MISS|0.65|2026-09-01|XLRE 1d already red vs SPY
Large-cap leadership inside sector|MISS|0.60|2026-09-01|WELL/EQIX not carrying
Small/mid leadership inside sector|MISS|0.50|2026-09-01|checked, nothing material
High-beta leadership inside sector|MISS|0.50|2026-09-01|checked, nothing material
Low-beta leadership inside sector|PARTIAL|0.45|2026-09-01|duration lag is basket-wide, not a defensive win
Sector ETF inflow / relative volume spike|MISS|0.60|2026-09-01|https://etfdb.com/etf/XLRE/
Sector ETF outflow / volume dry-up|PARTIAL|0.55|2026-09-01|https://etfdb.com/etf/XLRE/
Crowded long (extreme relative performance + valuation)|MISS|0.70|2026-09-01|1m rel -4.81%
Index rebalance / inclusion tailwind|MISS|0.40|2026-09-01|checked, nothing material
Index exclusion / forced selling|MISS|0.40|2026-09-01|checked, nothing material
Rates falling / REIT duration relief|MISS|0.85|2026-09-01|https://www.cnbc.com/quotes/US10Y
Data-center REIT demand / rent upside|HIT|0.70|2026-08-25|https://www.globenewswire.com/news-release/2026/08/25/3350172/0/en/digital-realty-selected-to-develop-50-megawatts-of-new-data-center-capacity-in-singapore.html
Industrial REIT occupancy / rent growth|HIT|0.55|2026-09-01|stale PLD sleeve; not same-day
Refinancing window opening|MISS|0.75|2026-09-01|30Y ~5.27% stress zone
Cap-rate compression|MISS|0.70|2026-09-01|checked, nothing material
Rates rising / REIT selloff|HIT|0.85|2026-09-01|https://www.cnbc.com/quotes/US10Y
Office vacancy / mark-to-market stress|HIT|0.60|2026-09-01|https://www.cbre.com/insights/figures/q2-2026-us-office-market-report
Refinancing wall stress|HIT|0.65|2026-08-21|https://cred-iq.com/blog/2026/08/21/the-maturity-wall-is-getting-taller/
Cap-rate expansion|PARTIAL|0.45|2026-09-01|implied by 30Y stress; no fresh print
Sector rotation into REITs|MISS|0.75|2026-09-01|Channel 1 XLRE rel 1d/3d/1w/1m all negative
Sector rotation out of real estate|HIT|0.75|2026-09-01|https://seekingalpha.com/news/4637971-real-estate-stocks-lose-out-as-investors-raise-rate-hike-bets
HIT_GRID_END

## RESEARCH APPENDIX

**Queries run**
- memory_search: Real Estate XLRE REIT duration yields prediction lessons (index unavailable)
- web_search: US 10 year Treasury yield 30 year TIPS real yield today September 1 2026
- web_search: XLRE REIT stocks premarket September 1 2026 rates real estate sector
- web_search: CME FedWatch September 2026 rate hike odds Warsh
- web_search: Hormuz oil Iran strait Brent WTI September 2026
- web_search: 10 year treasury yield live September 1 2026 CNBC Bloomberg
- web_search: economic calendar September 1 2026 ISM manufacturing PMI
- web_search: XLRE holdings WELL PLD EQIX AMT premarket September 1 2026
- web_search: US 30 year Treasury yield September 1 2026
- web_search: XLRE ETF flows outflows September 2026
- web_search: data center REIT Equinix Digital Realty news September 2026
- web_search: office vacancy REIT refinancing wall 2026 September
- x_search: 10 year yield 30 year REIT XLRE rates September 1 (2026-08-31 to 2026-09-01)
- web_fetch: CNBC Hormuz/oil; Fool Warsh hike odds; Seeking Alpha REIT lag; CNBC US10Y (quote page body empty)

**Key sources and facts used**
- CNBC (2026-09-01, ~8:19 ET): tanker struck in Hormuz; Brent **$91.90 (+1.6%)**, WTI **$87.48 (+2%)**; US–Iran strikes resumed. https://www.cnbc.com/2026/09/01/us-iran-war-trump-hormuz-tanker-attack-shipping-sanctions-.html
- CNBC US10Y quote (search snapshot 2026-09-01): **4.788%**, open 4.764%, prior close 4.758%. https://www.cnbc.com/quotes/US10Y
- GuruFocus / MacroMicro: 30Y **~5.27%** on 2026-09-01 (vs Channel 1 5.22 on 8/28). https://www.gurufocus.com/economic_indicators/111/30-year-yield
- Motley Fool (2026-09-01): Sep hike odds **~35% → ~60%** after Warsh JH. https://www.fool.com/investing/2026/09/01/odds-sept-rate-hike-doubled-fed-chair-kevin-warsh/
- CryptoBriefing / MacroMicro: CME FedWatch **~66–67%** Sep 25 bp hike. https://cryptobriefing.com/fed-rate-hike-probability-september-2026/
- Seeking Alpha: real estate stocks lag as hike bets rise post-Warsh. https://seekingalpha.com/news/4637971-real-estate-stocks-lose-out-as-investors-raise-rate-hike-bets
- ETFdb (~Sep 1): XLRE 5d **+$20.5M**, 1m **−$122.5M**, 3m **+$340M**. https://etfdb.com/etf/XLRE/
- Premarket holdings snapshots: WELL ~flat, PLD ~−0.8%, EQIX flat, AMT ~−0.3%; XLRE 8/31 close **$44.11 (−0.83%)**.
- ISM Manufacturing PMI due **2026-09-01 10:00 ET** (consensus ~55.2 vs 55.6). https://www.financecalendar.com/us-ism-manufacturing-pmi/
- CBRE Q2 2026 office vacancy **18.3%** (−30 bp). https://www.cbre.com/insights/figures/q2-2026-us-office-market-report
- Digital Realty Singapore 50 MW (2026-08-25) / Equinix Q2 guide raise: stale DC HIT, not same-day. https://www.globenewswire.com/news-release/2026/08/25/3350172/0/en/digital-realty-selected-to-develop-50-megawatts-of-new-data-center-capacity-in-singapore.html
- Channel 1 panel (unaltered): DFII10 **+0.08 1d**, ES **−0.53%**, NQ **−1.01%**, CL **+2.06%**, XLRE 1d/3d/1w/1m relative all negative.

**Not used as same-day S1 positives:** EQIX quantum/Sydney PR, DLR Switzerland groundbreaking, Mizuho DC price targets, WELL as ETF driver.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -1.0, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': -1.0}, 'multiplier': 0.9, 'leading_sum': -7.0, 'divergence_flagged': False, 'total_score': -6.75, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.55, 'regime': 'risk_off'}
```
