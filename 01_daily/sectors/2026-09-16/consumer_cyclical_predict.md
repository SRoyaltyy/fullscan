# Sector Prediction — Consumer Cyclical — 2026-09-16

- ETF: **XLY**
- rubric: `00_grounding/sectors/consumer_cyclical.md`
- predicted_direction: **flat**
- predicted_magnitude_band: **flat**
- total_score: **2.692** (mult 0.9)
- regime: mixed
- divergence_flagged: **True**
- engine: v2 · tape_anchor **7.368** (ES +1.14%, ER2 +0.08%, NQ +1.50%) · index_carry **1.324** (general 5.297) · llm_overlay **-6.0** (raw -6.75)

## Channel 1 sector ETF tape

```
ETF XLY vs SPY (yfinance, through 2026-09-15):
  1d: XLY -1.75% | SPY -0.46% | rel -1.29%
  3d: XLY -0.96% | SPY -0.06% | rel -0.91%
  1w: XLY -2.73% | SPY -1.12% | rel -1.61%
  1m: XLY -6.19% | SPY -2.44% | rel -3.75%
```

MEMORY_CONFIRM: Memory index is paused (embedding metadata missing); used injected Consumer Cyclical scoreboard + standing THIS-scope lessons only. Rolling dir=0.5 / mag=0.1 (n=10); last graded 2026-09-15 down/mild vs XLY −1.75% / SPY −0.46% / rel −1.29% (dir HIT, mag MISS). No open experiment for `sector_consumer_cyclical`. Scope DO-INSTEAD: 09-15 win = keep direction, shrink confidence on modest |score| (mag historically misses); 09-11/09-14 “cut conviction / prefer flat/mild when score fights tape” does **not** fire — Channel 1 1d rel is already **−1.29%** (confirming, not sub-gate). **08-11 oil-shock does NOT fire** (Finviz WTI $104.16 **−1.59%**, Brent $107.67 **−1.02%**, RBOB **−0.54%** — live *sign* is relief; News Judge: no kinetic/oil increment). **08-21 reversal does NOT cleanly fire** (Finviz ES **+0.20%** < +0.3%, NQ **+0.41%**; oil offered). **08-27 NVDA/XLK-map FIRES as a ban on S0=+1** (XLK PM **+0.65%**, ASML/Adobe are non-holdings; do not map NQ/XLK into XLY beta). **08-28 inherited-lag does NOT fire** (S0 not 0; 1d rel is a fresh confirming print, not only 1w/1m). **08-18 severe-cap** is a ceiling (AMZN/TSLA/HD not breaking premarket). **08-25 sector-owned print FIRES**: Census August retail sales **8:30 ET today** (unprinted as of 07:28 ET). **09-03 stale-S1-vs-pending-print** PARTIALLY fires (do not let July retail dominate) but UMich 47.8 is **5d**, not 2–4 weeks stale. **09-04 hawkish-asymmetry** PARTIALLY fires (Warsh presser same-day) but hike is **~92.5% priced** and oil is offered → S0 = −1 not −2. **09-10 triple-count / mild-cap** does NOT fully fire (futures not a continuation oil-spike session; 1d rel already ≤ −1%). **09-11 futures≥+0.5% mean-shift ban** does NOT fire (Finviz ES +0.20% / NQ +0.41%, both < +0.5%). **09-14 exogenous-AI** does NOT fire (1d rel not sub-gate; AMZN/TSLA *are* the book; XLK is the PM leader, not the victim). **09-15 “don’t cap S0 on middle-of-pack PM when oil is spiking”** does NOT fire — oil is **offered**. Trust Finviz live futures over Channel 1 `ES=F +1.14% / NQ=F +1.50%` vs prev close (same protocol as prior crude prior-close vs live-tape splits; Reuters/TipRanks agree with Finviz modest green).

# Consumer Cyclical (XLY) — 2026-09-16

Object is the **near-session XLY environment**, not SPX and not a stock pick. XLY remains AMZN ~24% + TSLA ~17–20% + HD ~5.4% (~46–49% combined). Score **broad consumer health**, not Adobe/ASML and not a single name.

## Channel 1 (used as given)

- Tape: XLY vs SPY **1d rel −1.29%** (XLY −1.75% / SPY −0.46%), **3d −0.91%**, **1w −1.61%**, **1m −3.75%**. Confirming lag. Yesterday was a notable absolute down day; 1d is **not** sub-gate.
- Macro: VIX **16.98** (−0.22d / +0.52w); **VIX/VIX3M 0.877 — contango** (stress backwardation of 09-10/11/14 is off). DGS10 **4.97** (+0.01d / +0.19w / +0.34 1m); DGS30 **5.34**; **DFII10 2.60** (1d 0 / +0.17w / +0.21 1m — real yields *elevated*, not rising today). HY 2.71 (+0.06d). **5-day 10Y–SPX corr −0.155** (yields are not the week’s equity driver). Finviz **WTI $104.16 (−1.59%) / Brent $107.67 (−1.02%) / RBOB −0.54%**; CL=F −2.36% / BZ=F −1.50%. DXY ~flat. **Finviz ES +0.20% / NQ +0.41% / RTY +0.08% / DJIA +0.11%** (modest green, not ≥ +0.5%). Asia **+0.65%**, Europe **+0.45%**. Gold +0.90%. Sector PM (injected): XLK **+0.65%** leads; XLE **−0.56%**; XLY **absent** from the injected PM list.
- Calendar: **August retail sales 8:30 ET** (sector spend spine, unprinted) + **FOMC/SEP/Warsh 14:00/14:30 ET**.

## Channel 2

**1. Shared macro as it hits THIS sector (S0)**

Two live objects, and they do **not** both point down for XLY.

**(a) FOMC / Warsh — the rates object, endogenous to AMZN/TSLA.** Decision + SEP + presser today. CME FedWatch ~**92.5%** of a 25 bp hike (3.50–3.75% → 3.75–4.00%). News Judge #1–#2: yesterday’s cash tape already sold Mag7/AI **including AMZN and TSLA** into a priced hike. That is XLY’s book, not an XLK-only story. The two-sided-Fed lesson applies: Chair **speaks today**, so a “carried S0=0” is invalid. Warsh’s known lean is hawkish → binary risk with a **hawkish skew**, not a coin-flip 0. Real yields are still high (DFII10 2.60, +17 bp 1w) — macro map: real yields up **−** for this growth-heavy basket.

**(b) Oil — 08-11 does **not** re-fire.** Live sign is **down** ~1–2% with RBOB offered. Level remains a tax (~$104 / ~$108; AAA regular **$4.367**), but the *increment* is relief. News Judge: no kinetic/oil increment. Do **not** put oil in S0 today.

**(c) Pre-binary tape.** Finviz futures are modestly green, not a 09-11 ≥+0.5% mean-shift. Overnight Asia/Europe green. Gold **up** (not the Warsh gold-slide). VIX contango. 10Y–SPX corr only −0.155. XLK is the PM leader — **08-27 forbids mapping that into S0=+1**, and XLY’s own 1d rel (−1.29%) does not confirm participation.

**S0 = −1.** Hawkish-leaning FOMC binary (Warsh same-day) on a duration-heavy book. Not −2: hike is ~92% priced, oil is offered, futures are not red, no fresh kinetic squeeze. Not 0: Chair appearance makes “carried” invalid. Not +1: 08-27 + confirming XLY lag.

Regime **mixed**.

**2. Spine + secondary (S1)**

- **Retail sales / card spend upside — UNPRINTED.** Census 8:30 ET; WSJ consensus **+0.8% m/m** headline / **+0.5%** ex-autos after July **−0.6%**. Do not pre-score the print. BofA card ~+4% YoY (resilient, slowing) and NRF core +0.1% m/m are secondary, not the 8:30 object.
- **Retail miss / traffic down — HIT, stale + partial fresh.** July Census −0.6% remains the last official print (~33d). NRSInsights August SSS **−1.1% YoY** (units −1.6%) is a same-month traffic miss, not a Census miss.
- **Consumer confidence jump — miss.**
- **Consumer confidence collapse — HIT, live-enough:** UMich prelim Sep **47.8** (09-11) vs 51.7 Aug / ~51 consensus; expectations **45.8**; 1y inflation **4.6%**. Second-lowest on record. Fuel + Iran/trade cited. Not 2–4 weeks stale — 09-11’s “stale cluster cannot net a live positive to negative” does **not** zero this.
- **Employment / wage support for discretionary — HIT, cooling not collapsing:** claims **206k** (week of 09-05); U3 **4.1%**. NFP was hot, not a wage-support gift into a hike.
- **Jobless claims / unemployment spike — checked, nothing material.**
- **Credit conditions easing — checked, nothing material.**
- **Credit tightening / delinquency rise — HIT, carried:** NY Fed/Equifax 90+ still elevated; SLOOS net tightening on cards. K-shaped. No new August TransUnion print.
- **Gasoline spike crushing discretionary — LEVEL HIT, live SIGN relief.** AAA regular **$4.367** (up vs ~$4.14 on 09-03). Crude/RBOB **down today**. Count the pump *level* as a tax; do **not** also score a spike (that would double-count a non-event). 08-11 live-sign rule: this is not a fresh S1 = −2 gasoline shock.
- **Auto SAAR / dealer inventory healthy — HIT:** Cox Aug SAAR **~16.8M** (vs 16.3M forecast; sixth month >16M). MAP HEAT: Auto Manufacturers residual **up** but breadth **0.26** — GM/TSLA, not the sleeve.
- **Travel / hotel RevPAR beat — HIT, cooling:** STR streak intact; Labor Day week calendar-boosted; FY26 RevPAR outlook still **+4.4%**. Not XLY’s load-bearing weight.
- **Sector rotation out of discretionary — HIT:** 1w/1m rel −1.61%/−3.75%; MAP HEAT nested shorts in **Footwear (NKE/DECK, medium)** and **Home Improvement (HD/LOW, medium)** — HD is a top-3 XLY weight, not a nested toy.
- Same-morning color: **no AMZN/TSLA/HD earnings.** Adobe/ASML/CRWD are **not** XLY. AMZN PM ~+0.2%, TSLA ~+0.1%, XLY PM ~+0.4% — **no mega-cap breakdown** (08-12 / 08-18).

**Net S1 = −1.** Fresh UMich collapse + rotation-out (including HD sleeve) outweigh auto/RevPAR and oil *relief*. Not −2/−3: gasoline is not spiking, claims are not spiking, Census is unprinted, 08-17 does not force notable from July retail. S0 is FOMC/rates; S1 is consumer spine — **not the same object** (09-11 two-component zero does not apply).

**3. Breadth / leadership (S2)**

Independent of the 1d rel (reserved for S4): MAP HEAT is **down** in apparel mfg/retail, dealers, parts, dept stores, footwear (medium), furnishings, home improvement (medium). Auto Manufacturers **up** on GM/TSLA residual with breadth **0.26** — do not buy the auto sleeve. Gambling up is not XLY. `size_gate=True`. This is **not** % names expansion and **not** small/mid leadership. Large-cap is mixed (AMZN/TSLA modest green vs HD HEAT down). **S2 = −1.** Not −2: conv mostly low; do not let two names drive the ETF call in either direction.

**4. Flows / positioning (S3)**

XLY ~**−$528M** week of 09-11 (largest sector-ETF outflow sleeve in that print); ~**−$1.1B** 1m (ETFdb). Sep 8 ~$184M day. Not a crowded long — a 1m laggard. 08-28: do **not** treat trailing 5d/1w outflows as a 1-day lid. No fresh Sep 15 daily flow in the tape. **S3 = 0.**

**5. ETF tape (S4) — confirmation only**

Channel 1: 1d **−1.29%**, 3d **−0.91%**, 1w **−1.61%**, 1m **−3.75%**. Confirming lag after a −1.75% cash day. Not leftover-only 1w/1m. Not a 09-14 sub-gate. **S4 = −1.** Do not also restack the same 1d into S2.

**6. Earnings / policy catalysts**

FOMC/SEP/Warsh is the policy catalyst (in S0). Retail sales is the sector-owned 8:30 binary (unscored until printed). No XLY-weight earnings.

## Self-audit

- **Lens:** XLY session environment, not SPX, not AMZN/TSLA stock-pick.
- **Same-shock double-count:** oil is **not** in S0 and not a live S1 spike. FOMC in S0 only. 1d rel in S4 only. HEAT in S2 only.
- **Single-ticker:** AMZN/TSLA modest PM green does not flip the ETF; HD HEAT down is a top-weight *group* (HD+LOW), not one headline.
- **Divergence:** leading (S0+S1+S2+S3) = **−3**; tape S4 = **−1**. Both down — **no divergence**. Factors and tape agree; do not invent a fight with breadth in order to flatten.
- **Band/skew:** two unprinted binaries (8:30 retail, 14:00 FOMC) + Finviz futures inside ±0.5% + no mega-cap PM breakdown → **do not manufacture notable**. Mag hit-rate 0.1 → shrink confidence, keep direction. 08-18 severe-cap binds. `size_gate=True`.
- **09-15 trap:** do not re-apply “oil at run highs → S0=−2 / notable” on a **relief** crude morning.
- **yfinance ES/NQ +1.14%/+1.50%:** not used as ≥+0.5% confirmation; live Finviz/Reuters modest green is the futures object.

**HORIZON:** 3d/1w remain a relative lag unless the 8:30 beat *and* Warsh is non-hawkish on dots — that joint outcome is not the base case. 2w/1m stay structurally weak (real yields up, pump level, UMich, HD/NKE nested) until oil *and* the hike path both ease.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: -1
S2_BREADTH: -1
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: -1
MULTIPLIER: 0.9
CONFIDENCE: 0.52
REGIME: mixed
DIVERGENCE: 0
HORIZON_3D: down
HORIZON_1W: down
HORIZON_2W: down
HORIZON_1M: down
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|MISS|0.55|2026-09-16|https://www.reuters.com/business/wall-st-futures-edge-higher-countdown-fed-decision-2026-09-16/
Risk-off tape / flight to safety|PARTIAL|0.50|2026-09-16|https://www.businessinsider.com/fed-meeting-fomc-interest-rate-hike-live-updates-2026-9
Real yields rising|HIT_CARRIED|0.70|2026-09-14|Channel1:DFII10=2.60,1w=+0.17
Real yields falling|MISS|0.70|2026-09-16|Channel1:DFII10 1d=0
USD strengthening|MISS|0.60|2026-09-16|Channel1:DXY 1d=+0.02%
USD weakening|MISS|0.60|2026-09-16|Channel1:USD -0.02% Finviz
Sector breadth expansion (% names up)|MISS|0.65|2026-09-16|MAP HEAT size_gate=True
Sector breadth failure (ETF up, names flat)|MISS|0.55|2026-09-15|Channel1:XLY 1d -1.75% (ETF not up)
Large-cap leadership inside sector|PARTIAL|0.45|2026-09-16|AMZN/TSLA PM modest green; HD HEAT down
Small/mid leadership inside sector|MISS|0.60|2026-09-16|MAP HEAT nested mostly down
High-beta leadership inside sector|MISS|0.50|2026-09-16|MAP HEAT Auto Mfrs breadth 0.26
Low-beta leadership inside sector|MISS|0.45|2026-09-16|off-price nested, not group bid
Sector ETF inflow / relative volume spike|MISS|0.60|2026-09-16|https://etfdb.com/etf/XLY/
Sector ETF outflow / volume dry-up|HIT|0.62|2026-09-11|https://seekingalpha.com/news/4643112-weekly-etfs-eight-of-11-sectors-record-outflows-financial-sector-leads-inflows
Crowded long (extreme relative performance + valuation)|MISS|0.70|2026-09-15|Channel1:1m rel -3.75%
Index rebalance / inclusion tailwind|checked, nothing material|0.40|2026-09-16|
Index exclusion / forced selling|checked, nothing material|0.40|2026-09-16|
Retail sales / card spend upside|PENDING|0.55|2026-09-16|https://www.census.gov/retail/release_schedule.html
Consumer confidence jump|MISS|0.80|2026-09-11|https://www.sca.isr.umich.edu/
Employment / wage support for discretionary|PARTIAL|0.65|2026-09-10|https://www.dol.gov/newsroom/releases/ETA
Credit conditions easing for consumers|MISS|0.55|2026-09-16|SLOOS net tightening
Auto SAAR / dealer inventory healthy|HIT|0.70|2026-09-08|https://www.coxautoinc.com/insights/auto-market-weekly-summary-09-08-26/
Travel / hotel RevPAR beat|HIT|0.60|2026-09-16|https://www.hoteldive.com/news/costar-tourism-economics-raise-us-hotel-performance-outlook-2026/827318/
Retail miss / traffic down|HIT_STALE|0.60|2026-08-14|July Census -0.6%; NRSInsights Aug SSS -1.1% YoY
Consumer confidence collapse|HIT|0.78|2026-09-11|https://www.cnbc.com/2026/09/11/consumer-outlook-plunges-in-september-as-inflation-outlook-worsens.html
Jobless claims / unemployment spike|MISS|0.75|2026-09-10|claims 206k
Credit tightening / delinquency rise|HIT_CARRIED|0.60|2026-08-01|https://libertystreeteconomics.newyorkfed.org/2026/08/how-distressed-are-consumers-reconciling-diverging-credit-card-delinquency-measures/
Gasoline spike crushing discretionary|HIT_LEVEL_NOT_SIGN|0.72|2026-09-16|https://gasprices.aaa.com/?ftag=YHF4eb9d17
Sector rotation into discretionary|MISS|0.70|2026-09-16|Channel1 1d/1w/1m rel negative
Sector rotation out of discretionary|HIT|0.72|2026-09-16|MAP HEAT Footwear+Home Improvement medium down
HIT_GRID_END

## RESEARCH APPENDIX

**Queries run**
- FOMC meeting September 16 2026 Fed decision Warsh hike odds
- WTI Brent crude oil price gasoline AAA September 16 2026
- XLY premarket Amazon Tesla Home Depot stock September 16 2026
- consumer confidence University of Michigan September 2026
- XLY ETF flows outflows September 2026 consumer discretionary
- retail sales August 2026 US card spend consumer spending
- jobless claims unemployment September 2026 latest
- hotel RevPAR STR September 2026 auto SAAR August 2026
- credit card delinquencies consumer credit tightening 2026
- US August 2026 retail sales release date Census 8:30
- Cox Automotive August 2026 SAAR US auto sales
- Iran Hormuz oil supply shock September 16 2026
- XLY vs SPY relative performance breadth AMZN TSLA HD September 16 2026
- CME FedWatch September 2026 25bp hike probability
- August 2026 retail sales consensus forecast economists 8:30 September 16
- XLY ETF flow September 15 2026 outflows
- stock futures September 16 2026 FOMC premarket S&P Nasdaq
- X search: XLY AMZN TSLA consumer discretionary premarket FOMC oil gasoline September 16 2026 (from 2026-09-15 to 2026-09-16)
- Fetches: Business Insider FOMC live; AAA gasprices (403)

**Key sources (title + URL + timestamp where available)**
- Business Insider — Fed meeting updates / 92.5% hike odds — https://www.businessinsider.com/fed-meeting-fomc-interest-rate-hike-live-updates-2026-9 — updated 2026-09-16T10:59:14Z (fetched 2026-09-16T11:29:39Z)
- Reuters — Wall St futures edge higher, countdown to Fed — https://www.reuters.com/business/wall-st-futures-edge-higher-countdown-fed-decision-2026-09-16/
- TipRanks — futures rise ahead of Fed — https://www.tipranks.com/news/stock-futures-rise-ahead-of-feds-interest-rate-decision
- Bloomberg — history shows Fed will deliver hike markets locked in — https://www.bloomberg.com/news/articles/2026-09-15/history-shows-fed-will-deliver-rate-hike-markets-have-locked-in
- Census retail release schedule — August advance **Sep 16, 2026 8:30 a.m. EDT** — https://www.census.gov/retail/release_schedule.html / https://www.census.gov/marts/www/marts_current.pdf
- Morningstar/DJ — retail sales expected rebound +0.8% / +0.5% ex-auto — https://www.morningstar.com/news/dow-jones/202609154267/retail-sales-expected-to-rebound-data-week-ahead
- UMich SCA — Sep prelim 47.8 — https://www.sca.isr.umich.edu/
- CNBC — consumer outlook plunges — https://www.cnbc.com/2026/09/11/consumer-outlook-plunges-in-september-as-inflation-outlook-worsens.html
- Detroit News — UMich drop — https://www.detroitnews.com/story/business/2026/09/11/university-of-michigan-survey-shows-drop-in-consumer-confidence/91709687007/
- AAA — national regular **$4.3672** as of 9/16/26 — https://gasprices.aaa.com/?ftag=YHF4eb9d17
- Cox Automotive — Aug SAAR ~16.8M — https://www.coxautoinc.com/insights/auto-market-weekly-summary-09-08-26/
- Hotel Dive / CoStar — 2026 RevPAR outlook +4.4% — https://www.hoteldive.com/news/costar-tourism-economics-raise-us-hotel-performance-outlook-2026/827318/
- DOL / YCharts — claims 206k week ending Sep 5 — https://www.dol.gov/newsroom/releases/ETA
- ETFdb / Seeking Alpha weekly flows — XLY ~−$1.1B 1m; ~$528M week cited in 09-15 note / sector outflows — https://etfdb.com/etf/XLY/ ; https://seekingalpha.com/news/4643112-weekly-etfs-eight-of-11-sectors-record-outflows-financial-sector-leads-inflows
- NY Fed Liberty Street — card delinquency measures 2026 — https://libertystreeteconomics.newyorkfed.org/2026/08/how-distressed-are-consumers-reconciling-diverging-credit-card-delinquency-measures/
- Al Jazeera / Menadue — Hormuz still impaired, oil >$100, no fresh 09-16 kinetic increment in News Judge — https://www.aljazeera.com/news/2026/9/14/us-says-its-clearing-hormuz-traffic-why-are-oil-futures-beyond-100
- Public.com / MarketWatch — AMZN/TSLA/XLY PM modest green — https://public.com/stocks/amzn/pre-market
- Channel 1 injected panel (VIX, Finviz futures, DGS/DFII, XLY vs SPY tape) — 2026-09-16 pre-open snapshot
- MAP HEAT research block (injected) — nested Apparel/Footwear/HD-LOW downs; Auto Mfrs residual up, breadth 0.26

**Facts taken**
- FOMC today; Warsh presser; ~92.5% 25 bp hike; SEP/dots are the unpriced tail.
- Live crude **down** (WTI −1.59% / Brent −1.02% / RBOB −0.54%) at still-high **$104 / $108**; AAA **$4.367**.
- Finviz ES/NQ **+0.20% / +0.41%** (modest); Reuters agrees “edge higher”; not a ≥+0.5% four-future impulse.
- XLY 1d/3d/1w/1m rel **−1.29 / −0.91 / −1.61 / −3.75%**; cash 09-15 XLY **−1.75%**.
- UMich Sep prelim **47.8**; claims **206k**; U3 **4.1%**; Cox SAAR **16.8M**; RevPAR outlook still positive.
- Census retail **unprinted** at 07:28 ET; consensus **+0.8%** rebound — two-sided, not pre-scored.
- XLY outflows trailing (weekly/1m), not a 1-day lid.
- No fresh AMZN/TSLA/HD earnings; no mega-cap PM breakdown; XLK PM **+0.65%** is the exogenous bid.
- News Judge: no kinetic/oil increment; hike-priced Mag7 tape (AMZN/TSLA) is endogenous to XLY.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -1.0, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': -1.0}, 'multiplier': 0.9, 'leading_sum': -7.0, 'divergence_flagged': True, 'total_score': 2.692, 'predicted_direction': 'flat', 'predicted_magnitude_band': 'flat', 'confidence_score': 0.508, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': 1.228, 'score': 7.368, 'legs': [{'leg': 'ES', 'pct': 1.14, 'w': 0.8}, {'leg': 'ER2', 'pct': 0.08, 'w': 0.2}, {'leg': 'NQ', 'pct': 1.5, 'w': 0.2}]}, 'overlay_score': -6.0, 'overlay_raw': -6.75, 'index_carry': 1.324, 'general_total': 5.297, 'skill_multipliers': {'S0_SHARED_MACRO': 1.25, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 1.25}, 'llm_confidence': 0.52, 'sector_rs_veto_applied': True, 'sector_rs_tape': {'d1': -0.39, 'w1': -1.96}, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
