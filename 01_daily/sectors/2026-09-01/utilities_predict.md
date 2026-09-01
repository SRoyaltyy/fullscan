# Sector Prediction — Utilities — 2026-09-01

- ETF: **XLU**
- rubric: `00_grounding/sectors/utilities.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-3.15** (mult 0.9)
- regime: mixed
- divergence_flagged: **False**

## Channel 1 sector ETF tape

```
ETF XLU vs SPY (yfinance, through 2026-08-31):
  1d: XLU -2.20% | SPY -0.53% | rel -1.67%
  3d: XLU -2.49% | SPY +0.15% | rel -2.64%
  1w: XLU -1.26% | SPY +0.17% | rel -1.44%
  1m: XLU -5.44% | SPY +3.42% | rel -8.86%
```

MEMORY_CONFIRM: Utilities/XLU only — memory index unavailable this run (used injected sector logs). Rolling dir=0.4 / mag=0.4 (n=10). Last graded: 08-28 down/mild vs XLU −1.04% / SPY −0.23% / rel −0.82% (dir HIT, mag MISS — hawkish Warsh resolution not knowable at open). 08-27 up/notable vs −0.76% (dir MISS). 08-31 down/mild still ungraded. Applied: 08-18 (risk-off bid + rising 10Y/long-end → relative beat / flat-to-negative absolute; do not mint absolute up); 08-21 (live curve, not stale FRED 1d — 10Y ~4.78% and DFII10 +8 bp, not easing); 08-27 (do not pay carried easing in S0 and S1; no fresh XLU catalyst); 08-17 (no same-day yield rally / no 8:30 miss → relative, not absolute up); 08-13 (S2/S4 confirmation only); 08-12 (AI-power is a 1d dampener, not a band engine); 08-14 (calendar: no 8:30 CPI/PCE/NFP; ISM/JOLTS/construction 10:00 ET two-sided, not a scored FTS HIT); 08-11 (does not flip up: yields not easing, multi-horizon rel still red); 08-25 (does not force flat: S1 is not 0); 08-28 (do not pre-score a 10:00 print into notable-down; do not promote a single-name smash into S1). Open experiment: extra confirming source before full-weight lean — used live 10Y + CME hike odds + Channel 1 DFII10. |score| will be small → keep mild, not notable.

## Utilities (XLU) — 2026-09-01

Object is the **near-session XLU environment**, not SPX and not a stock pick.

### Channel 1 (trusted, not re-derived)

XLU vs SPY through 2026-08-31: **1d −2.20% / −0.53% (rel −1.67%)**; **3d −2.49% / +0.15% (rel −2.64%)**; **1w −1.26% / +0.17% (rel −1.44%)**; **1m −5.44% / +3.42% (rel −8.86%)**.

Macro panel: VIX **15.81** (+0.89) with VIX/VIX3M **1.036 backwardation**; DGS10 **4.73** as of **2026-08-28** (+6 bp 1d / −1 bp 1w / +5 bp 1m); DGS30 **5.22** (+3 bp 1d); DFII10 **2.42** (**+8 bp 1d**, +2 bp 1w, +1 bp 1m); HY 2.6 tight; EPU 311.48 (+194.51 as of 8/30); CL=F **+2.06%** / BZ=F **+1.49%**; DXY +0.16% 1d; **ES=F −0.53% / NQ=F −1.01%**; Asia **−0.22%**; Europe **−0.69%**; F&G 58.2 Greed (stale 8/27); 5-day 10Y–SPX corr **−0.481**.

**Live curve (08-21 / 08-25 duration check — not the 8/28 1d column as “today’s easing”):** 10Y ~**4.78%** (GuruFocus/MarketWatch vs FRED 4.73); 30Y ~**5.27%**. Sticky-to-higher, still in the long-end stress zone. **Not** an easing impulse.

**Calendar (08-14 / 08-27):** No 8:30 ET CPI/PCE/NFP/retail-sales regime-flip. Today 10:00 ET: ISM Manufacturing PMI (cons. ~55.2–55.3 vs July 55.6), JOLTS, construction spending — **two-sided event risk**, not a scored HIT at 09:30. Warsh is **already printed (8/28)**; News Judge: post-Warsh path is live hawkish (Sep hike ~**66–67%**, up from ~35% pre-speech). The 08-28 “unresolved Chair binary → S0=0” rule **does not fire as a speech-wait**. The rate object is the **live curve + hike odds**, counted once.

### Channel 2

**1. Shared macro → this sector.** Classical map is **real/nominal yields**. Live 10Y/30Y/TIPS are **up**, not down. That is a bond-proxy absolute headwind. Overlay: ES/NQ red, Europe red, VIX backwardation, Hormuz/oil **up** (WTI ~$88 / Brent ~$92, CL +2.1%). That is a **relative** defensive bid, not an absolute FTS bid — VIX is still 15.8, not a >20 flight-to-quality spike, and long-end is **rising** (08-18: when risk-off and 10Y rise together, default **relative outperformance / flat-to-negative absolute**). Oil-up is two-sided (inflation/rate headwind vs geo risk-off). Do **not** score S0=+1 from Hormuz. Do **not** score S0=−2 from mild-red futures. Do **not** pre-score ISM. Net shared map: **mixed / S0=0**. The duration shock is paid in S1, not again here.

**2. Spine / secondary.**
- **Data-center load growth / power demand upside:** structural HIT, **stale** for a 1d call (TVA large-load rate Oct 1, NES class, nuclear PPAs). Rubric: do **not** let the multi-year AI-power story override a 1d rate tape without a fresh XLU catalyst.
- **Rates falling (bond-proxy bid):** **MISS**. Live 10Y ~4.78% / 30Y ~5.27% / DFII10 +8 bp.
- **Rates rising (bond-proxy selloff):** **HIT**. One shock: FRED + live quotes + hike odds ~66%. Dampen with AI-power, do **not** escalate to −2/−3.
- **Risk-on rotation away from utilities:** **MISS**. NQ **weaker** than ES (−1.01% vs −0.53%) is risk-off, not an 08-27 NVDA/XLK anti-FTS rip.
- **Nuclear / grid CapEx / favorable ROE:** structural, no same-session order.
- **Adverse rate case / load-growth disappointment / regulatory smash:** carried (WoodMac/Texas/Ohio, AEP/Oklahoma). Not fresh. Single-name must not drive the ETF call (08-28 PCG rule).
Net **S1 = −1**.

**3. Breadth.** Monday’s −2.2% / −1.67% rel was **broad** (paid). Premarket is **not** a live breakdown: XLU ~**+$0.06 / +0.14%**, NEE ~+0.3%, DUK ~+0.3–0.4%, CEG ~−0.3%. Do **not** copy yesterday’s internals into S2 (leftover-tape rule). **S2 = 0**.

**4. Flows.** ETFdb: 5d **+$45.4M**, 1m **−$7.8M**. Modest, not a relative-volume spike, not a 1-day outflow lid. 1m rel −8.86% is de-risked, not a crowded-long unwind that forces a bounce. **S3 = 0**.

**5. Catalysts.** No fresh XLU-wide rate-order/load print. ISM/JOLTS at 10:00 remain two-sided. ADSK/ADI are **not** XLU. Raymond James defensive-relative note is 08-18 **relative** support, not an absolute-up license.

### Lessons → scores

**08-18 is the veto on up:** risk-off + rising 10Y → relative bid, **flat-to-negative absolute**. Premarket XLU green vs red ES is that relative bid starting — it does not flip S0/S1 positive.

**08-21:** live 10Y ~4.78% is **rising**. No easing credit.

**08-27:** no fresh utility catalyst; do not mint S0/S1 = +1. NQ is not leading ES, so the anti-FTS rotation-away HIT is **off**.

**08-25 does not force flat:** S1 is −1 (live rates), not a carried-S2/S3-only book.

**08-11 does not flip up:** yields are not easing; 1d/3d/1w/1m rel are all red.

**08-12 / 08-17:** AI-power and any carried defensive read stay dampeners. Band **mild**.

**08-14 / 08-28:** ISM is knowable event risk, not an open-book FTS HIT. Do **not** invent notable-down before 10:00.

**08-13:** S4 is confirmation only. Channel 1 1d rel **−1.67%** (and 3d/1w/1m all red) confirms the rate spine. Tape is counted **once** here — S2 stays 0.

**Self-audit:** rate lens over AI narrative on a 1d horizon; one yield shock (S1, not S0+S1); no single-ticker (CEG) driving the ETF; |Σ| small and mag accuracy 0.4 → **mild**, not notable. Premarket +0.14% is 08-18 relative support, not a factor-tape fight that zeros S1.

**Divergence:** leading (S0 0 + S1 −1 + S2 0 + S3 0 = −1) and S4 (−1) **agree**. No flag. Trust factors; S4 only confirms.

Component arithmetic: (−1) × 0.9 = **−1.8** → down, mild cap. Narrative and scores are the same band.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: -1
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: -1
MULTIPLIER: 0.9
CONFIDENCE: 0.52
REGIME: mixed
HORIZON_3D: down:mild:0.50
HORIZON_1W: down:mild:0.48
HORIZON_2W: down:mild:0.45
HORIZON_1M: down:mild:0.42
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|MISS|0.70|2026-09-01|https://fedratecalc.com/us-economic-calendar/september-2026/
Risk-off tape / flight to safety|PARTIAL|0.62|2026-09-01|https://www.morningstar.com/news/dow-jones/20260901635/oil-rises-triggering-broad-selloff-across-equity-bond-markets-in-asia-update
Real yields rising|HIT|0.78|2026-09-01|https://ycharts.com/indicators/10_year_treasury_rate
Real yields falling|MISS|0.78|2026-09-01|https://ycharts.com/indicators/10_year_treasury_rate
USD strengthening|PARTIAL|0.55|2026-09-01|
USD weakening|MISS|0.55|2026-09-01|
Sector breadth expansion (% names up)|MISS|0.60|2026-09-01|
Sector breadth failure (ETF up, names flat)|MISS|0.55|2026-09-01|
Large-cap leadership inside sector|PARTIAL|0.50|2026-09-01|https://stockanalysis.com/stocks/nee/
Small/mid leadership inside sector|MISS|0.45|2026-09-01|
High-beta leadership inside sector|MISS|0.50|2026-09-01|
Low-beta leadership inside sector|PARTIAL|0.50|2026-09-01|
Sector ETF inflow / relative volume spike|MISS|0.58|2026-09-01|https://etfdb.com/etf/XLU
Sector ETF outflow / volume dry-up|MISS|0.58|2026-09-01|https://etfdb.com/etf/XLU
Crowded long (extreme relative performance + valuation)|MISS|0.55|2026-09-01|
Index rebalance / inclusion tailwind|MISS|0.40|2026-09-01|
Index exclusion / forced selling|MISS|0.40|2026-09-01|
Data-center load growth / power demand upside|HIT|0.70|2026-09-01|https://newschannel9.com/news/local/tva-proposes-new-data-center-power-rate-starting-in-october-with-10-average-increase-tennessee-valley-authority
Rates falling (bond-proxy bid)|MISS|0.80|2026-09-01|https://www.gurufocus.com/economic_indicators/37/10-year-treasury-yield
Favorable rate case / allowed ROE|MISS|0.45|2026-09-01|
Nuclear / gas generation policy support|HIT|0.55|2026-09-01|https://www.datacenterfrontier.com/energy/article/55394098/nuclear-momentum-meets-the-megawatt-test
Grid CapEx approval / recovery|HIT|0.50|2026-09-01|https://www.utilitydive.com/news/2026-q2-roundup-utilities-emphasize-project-execution-ratepayer-protec/827997/
Rates rising (bond-proxy selloff)|HIT|0.80|2026-09-01|https://www.cnbc.com/2026/08/28/-september-fed-decision-now-a-coin-flip-as-rate-hike-odds-increase.html
Adverse rate case|MISS|0.45|2026-09-01|
Load growth disappointment|MISS|0.50|2026-09-01|
Regulatory disallowance / project cancel|MISS|0.45|2026-09-01|
Risk-on rotation away from utilities|MISS|0.65|2026-09-01|
Sector rotation into utilities|MISS|0.60|2026-09-01|
Sector rotation out of utilities|HIT|0.70|2026-09-01|
HIT_GRID_END

Same-shock note: **Real yields rising** and **Rates rising (bond-proxy selloff)** are one duration object — paid in **S1 only**. Data-center / nuclear / grid CapEx are structural HITs **damped** (not S1 +). **Sector rotation out of utilities** is the Channel 1 tape, paid in **S4 only** (not S2).

## RESEARCH APPENDIX

**Queries run**
- US economic calendar September 1 2026 8:30 releases ISM PMI
- US 10 year treasury yield today September 1 2026
- 30 year treasury yield September 1 2026
- XLU utilities ETF premarket September 1 2026 rotation yields
- NEE DUK SO CEG XLU premarket September 1 2026
- utility rate case data center power demand nuclear grid 2026 September
- CME FedWatch September 2026 hike odds Warsh
- XLU ETF flows September 2026 inflows outflows
- site:etfdb.com XLU fund flows
- Hormuz oil utilities defensive rotation September 1 2026
- ISM Manufacturing PMI August 2026 consensus forecast September 1
- X search: XLU utilities 10-year yield premarket September 1 2026 bond proxy (2026-08-31 to 2026-09-01)
- Fetch: https://fedratecalc.com/us-economic-calendar/september-2026/
- Fetch: https://www.gurufocus.com/economic_indicators/37/10-year-treasury-yield (403 / ASN banned)

**Key sources (title + URL + timestamp where available)**
- FedRateCalc September 2026 calendar — https://fedratecalc.com/us-economic-calendar/september-2026/ — fetched 2026-09-01T13:04:16Z. First listed print: JOLTS Tue Sep 1 10:00 ET; no 8:30 on this page.
- ISM/BLS calendars (via search, 2026-09-01) — https://www.ismworld.org/supply-management-news-and-reports/reports/rob-report-calendar/ ; https://www.bls.gov/schedule/2026/09_sched_list.htm — ISM Manufacturing PMI 10:00 ET Sep 1; no 8:30 high-impact listed.
- Morningstar/DJ data-week preview — https://www.morningstar.com/news/dow-jones/202608316043/nonfarm-payrolls-expected-to-rebound-data-week-ahead-update — ISM Aug consensus **55.3** (July 55.6).
- FXStreet ISM preview — https://www.fxstreet.com/news/ism-manufacturing-pmi-set-to-signal-steady-expansion-in-us-factory-activity-202609010900 — consensus **55.2**.
- GuruFocus / YCharts / MarketWatch 10Y — https://www.gurufocus.com/economic_indicators/37/10-year-treasury-yield ; https://ycharts.com/indicators/10_year_treasury_rate — ~**4.78%** Sep 1 vs ~4.75% Aug 31 / FRED 4.73 Aug 28.
- GuruFocus 30Y — https://www.gurufocus.com/economic_indicators/111/30-year-yield — ~**5.27%** Sep 1.
- CNBC / CryptoBriefing FedWatch — https://www.cnbc.com/2026/08/28/-september-fed-decision-now-a-coin-flip-as-rate-hike-odds-increase.html ; https://cryptobriefing.com/fed-rate-hike-probability-september-2026/ — Sep hike ~**66–67%** as of Sep 1 after Warsh.
- Webull XLU — https://www.webull.ca/ticker/nysearca-xlu — premarket ~**$42.29 (+0.14%)** vs ~$42.23 prior close.
- StockAnalysis/MarketWatch/Public.com — NEE ~+0.3%, DUK ~+0.3–0.4%, CEG ~−0.25% premarket Sep 1.
- ETFdb XLU flows — https://etfdb.com/etf/XLU — 5d **+$45.43M**, 1m **−$7.84M** (page update ~Sep 1 / Aug 29).
- Morningstar/DJ Asia wrap — https://www.morningstar.com/news/dow-jones/20260901635/oil-rises-triggering-broad-selloff-across-equity-bond-markets-in-asia-update — oil up, broad Asia/bond selloff Sep 1.
- Raymond James (via Investing.com) — https://uk.investing.com/news/analyst-ratings/raymond-james-says-large-caps-to-outperform-amid-hormuz-closure-93CH-4548573 — utilities/staples/healthcare as relative defensives on Hormuz; reverse if oil falls.
- NewsChannel9 TVA — https://newschannel9.com/news/local/tva-proposes-new-data-center-power-rate-starting-in-october-with-10-average-increase-tennessee-valley-authority — data-center rate Oct 1 2026 (~10% all-in), not a same-session XLU print.
- Data Center Frontier nuclear — https://www.datacenterfrontier.com/energy/article/55394098/nuclear-momentum-meets-the-megawatt-test — structural nuclear/PPA tape, stale for 1d.
- X posts 2026-08-31 — https://x.com/somoscdi/status/2094551226515849417 ; https://x.com/MicahMcDonald8/status/2094565489104371719 — XLU lag / “precarious with rates moving higher”; no Sep 1 premarket XLU thread.

**Facts taken**
- No 8:30 ET high-impact print today; ISM/JOLTS/construction at 10:00 ET; ISM cons. ~55.2–55.3 vs 55.6.
- Live 10Y ~4.78%, 30Y ~5.27%; Channel 1 DFII10 +8 bp 1d — rates **rising**.
- Sep FOMC hike odds ~66–67% (hawkish path printed, not a wait-for-Warsh binary).
- ES −0.53% / NQ −1.01%; Europe −0.69%; WTI/Brent green on Hormuz — risk-off + oil-up, NQ not leading.
- XLU Channel 1 1d/3d/1w/1m all relative red; premarket only +0.14% with NEE/DUK slightly green, CEG slightly red.
- XLU 5d flows +$45M / 1m −$8M — not a spike.
- AI-power/nuclear/grid items are structural or October-dated (TVA), not a fresh 1d XLU catalyst.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': -1.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': -1.0}, 'multiplier': 0.9, 'leading_sum': -3.0, 'divergence_flagged': False, 'total_score': -3.15, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.52, 'regime': 'mixed'}
```
