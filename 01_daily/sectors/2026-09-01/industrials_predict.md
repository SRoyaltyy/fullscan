# Sector Prediction — Industrials — 2026-09-01

- ETF: **XLI**
- rubric: `00_grounding/sectors/industrials.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-4.05** (mult 0.9)
- regime: risk_off
- divergence_flagged: **False**

## Channel 1 sector ETF tape

```
ETF XLI vs SPY (yfinance, through 2026-08-31):
  1d: XLI -2.05% | SPY -0.53% | rel -1.53%
  3d: XLI -1.83% | SPY +0.15% | rel -1.98%
  1w: XLI -2.84% | SPY +0.17% | rel -3.01%
  1m: XLI -1.83% | SPY +3.42% | rel -5.25%
```

MEMORY_CONFIRM: Memory index unavailable this run (embedding metadata mismatch); used injected Industrials scoreboard + 08-11..08-28 sector logs. Rolling dir=0.2 mag=0.1 (n=10); last 30 dir=0.25 mag=0.083 (n=12). Last graded 08-28: narrative down/mild vs pipeline down/flat, actual XLI −0.93% (dir HIT, mag MISS on pipeline flat). Last ungraded 08-31: down/mild pending. Governing today: 08-11/08-12 (live Hormuz/oil-up → verify oil sign, S0 ≤ 0/negative, multiplier ≤1.0, no severe-up); 08-13 does **not** fire (oil is **up** on supply, not down on demand); 08-18 (do not hold S1 at +2 on stale ISM; do not use GEV/ETN as a downside cushion; reconcile narrative vs components); 08-21 reversal checklist **off** (ES −0.53%, NQ −1.01%); 08-25 (all-timeframe lag is not rescued by a SPY bounce — today futures are already red); 08-27 (1w/1m laggard after non-holdings mega-cap AHR → prefer flat or down:mild; cap S1 at 0/+1); 08-28 (emit narrative down/mild, do **not** import XLF leftover-S2/S4 down-bans; keep S1 at 0 until the sector’s own print). DO-INSTEAD: when score fights tape, cut conviction / prefer flat/mild — **not binding**: leading factors and tape agree. Open experiment: keep direction, shrink confidence on modest |score| given mag=0.1.

## XLI near-session environment (not an SPX call)

### 1. Shared macro as it hits Industrials — S0 = −1
Channel 1 is a **live risk-off overlay for a cyclical**, not a bounce and not a two-sided Warsh wait.

- **Futures independently confirm weakness:** ES **−0.53%**, NQ **−1.01%**. Asia composite **−0.22%**, Europe **−0.69%** (DAX −1.13%). 08-21’s ES/NQ ≥ +0.3% reversal gate is **off**.
- **Oil is live-up on supply, not a stale headline.** Channel 1: CL=F **+2.06%**, BZ=F **+1.49%**; Finviz WTI **$88.01 (+2.59%)**, Brent **$92.25 (+1.89%)**. Al Jazeera (2026-09-01): Brent topped **$92** after the first US–Iran fire exchange in over a month (US strikes on Larak Island; Iranian attacks on Jordan bases; UKMTO tanker hit in Hormuz; Trump “hit them hard”). This is the 08-11/08-12 trigger: **do not call oil flat**. For XLI, the oil spike is a **cost/stagflation headwind** for transports/manufacturers. Defense/aerospace can theoretically bid on geo, but that is composition, not a license for S0 = 0.
- **Real yields rising (Channel 1, do not re-derive):** DFII10 **2.42 (+0.08 1d)**; DGS10 **4.73 (+0.06)**; DGS30 **5.22 (+0.03)**. 5-day 10Y–SPX corr **−0.481**. Duration is secondary to ISM/CapEx for this book, but the rate impulse is a same-session equity-beta drag.
- **Warsh is printed hawkish, not pending.** News Judge #1: September hike a coin flip, odds up from the mid-30s into ~58–67%. 08-28’s two-sided-speech rule **does not fire**.
- VIX **15.81 (+0.89)** with VIX/VIX3M **1.036 backwardation**. Not a crash (HY OAS still **2.6**, tight). USD **+0.16%** 1d. Gold/copper risk-off (GC −0.03% / copper −1.32% on the Finviz tape).

**S0 = −1, regime risk_off.** Not −2: credit is not blowing out, VIX is not a panic print, and August ISM is still unprinted. Not 0: oil is confirmed up, futures are ≤ −0.5%, and the hawkish path is already in the tape. Oil is counted **once here**, not again in S1.

### 2. Spine + secondary — S1 = 0 (capped)
**No fresh same-morning industrials print in hand.** August ISM manufacturing is **today 10:00 ET** (consensus ~**55.2** vs July **55.6**). That is the sector’s own spine print and it is **two-sided until it hits**. Do not pre-score expansion or contraction. July 55.6 / new orders 56.7 (released 8/3) is still expansion but **stale**. 08-18/08-27: **cap S1 at 0/+1**; +2 is forbidden without a same-morning confirmation.

July durables already printed **08-26** (headline +1.1%; core cap-goods +0.2%) — traded, not a same-morning HIT.

**Secondary is carried / split, not a same-session raise:**
- **Grid / AI power — HIT, already in the tape.** GEV ~$176B RPO / 116 GW gas book remains structural. Premarket GEV **~−1.3%**, CAT **~−1.1%**. 08-18: do **not** use GEV/ETN as a downside cushion after those names roll.
- **Aerospace & defense — MIXED / stale.** F-15 **ceiling** (08-24) already faded BA. SPEEA authorization is ≥1 session old. Premarket BA **~−1.7%**. Do **not** cancel ISM (unprinted) with one award, and do not treat geo as a fresh defense-order HIT.
- **Freight — MIXED.** Rail/intermodal still expanding on lagged AAR/IANA; Cass trucking still the soft leg. Not a same-morning recovery HIT.
- **Construction slowdown — HIT, carried.** June total construction **−3.2% y/y**; manufacturing construction off the 2025 peak. July construction spending is **also 10:00 ET** (consensus ~0% MoM) — two-sided, not scored until print. AI/nonres is the offset, not a broad build boom.
- AME Indicor close (08-26) is **stale M&A**. Finviz ADSK/ADI/AMGN are **not** XLI spines.

Net: stale ISM expansion vs construction drag vs oil-cost overlay vs **no fresh same-morning confirmation**. **S1 = 0.**

### 3. Breadth — S2 = −1
XLI is a **laggard, not a leader**. Channel 1 through 08-31: 1d rel **−1.53%**, 3d **−1.98%**, 1w **−3.01%**, 1m **−5.25%**. 08-26’s +1.07% bounce is long dead. Premarket CAT/GEV/BA all red with ES already ≤ −0.5% is **live** large-cap confirmation, not leftover tape. Mega-name AI-power/defense carry is **not** saving the ETF this morning. Score the lag **once** here.

### 4. Flows — S3 = 0
ETFdb ~**+$151M** over 1m vs a large **08-27 ~−$370M** day and mixed August prints. Not a crowded long (1m rel **−5.25%**). Rotation **out of industrials** is the multi-week tape, not a same-day inflow spike. Do not triple-count the same fade in S2/S3/S4 — trailing units stay **S3 = 0**.

### 5. ETF tape (confirmation only) — S4 = −1
Channel 1: 1d XLI **−2.05%** vs SPY **−0.53%** (rel **−1.53%**); 3d/1w/1m all negative relative. Decisive lag. Confirmation of underperformance, **not** an independent second thesis. 08-28 leftover-S4 bans from XLF/XLY are **not imported** (08-28 Industrials). The bounce gate is off and S0 is live-negative, so S4 is allowed to confirm.

### 6. Catalysts / calendar
- **Today 10:00 ET:** ISM Manufacturing (consensus 55.2 vs 55.6), Construction Spending (consensus ~0%), JOLTS. **Do not claim “no macro print.”** ISM is the sector-owned binary; leave S1 at 0 until it prints. A hot ISM would be the bounce path; a miss would validate down:mild without needing a notable upgrade pre-release.
- **Not today:** durables, G.17, PCE, Warsh. Warsh is **already hawkish**.
- NVDA/XLK leftover is **not** an XLI spine (08-27). NQ −1.01% is shared risk-off, counted in S0, not as tech-idiosyncratic up-license.

### Self-audit
- **Lens:** cyclical; rates/oil only in S0, not re-counted in S1.
- **Band:** **mild**, not notable (pending ISM, mag accuracy 0.1, HY still tight, defense composition can buffer absolute). 08-11/08-12 forbid severe-up; they do not force notable-down.
- **Skew:** GEV/BA/CAT do not drive the ETF call; all three are red anyway.
- **Same-shock:** Hormuz/oil counted in S0 only. 08-31 lag counted in S2 (live premarket) and confirmed in S4, **not** in S3.
- **Divergence:** leading S0+S1+S2+S3 = **−2** vs S4 **−1** — **same sign**. No divergence. Trust factors over tape; tape agrees.
- **Reconcile:** Σ(S0..S4) = **−3** × 0.9 = **−2.7** → **down/mild**. Narrative and components match. Do not let a pipeline rewrite to flat (08-28).

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: 0
S2_BREADTH: -1
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: -1
MULTIPLIER: 0.9
CONFIDENCE: 0.56
REGIME: risk_off
HORIZON_3D: down:mild:0.55
HORIZON_1W: down:mild:0.52
HORIZON_2W: flat:mild:0.45
HORIZON_1M: flat:mild:0.42
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-off tape / flight to safety|HIT|0.80|2026-09-01|https://www.reuters.com/business/wall-st-futures-kick-off-september-under-pressure-yields-oil-prices-rise-2026-09-01/
Risk-on tape / equity beta expansion|MISS|0.75|2026-09-01|Channel 1 ES -0.53% / NQ -1.01%
Real yields rising|HIT|0.85|2026-08-28|Channel 1 DFII10 2.42 (+0.08 1d)
Real yields falling|MISS|0.85|2026-08-28|Channel 1 DFII10 +0.08 1d
USD strengthening|HIT|0.55|2026-09-01|Channel 1 DXY +0.16% 1d
USD weakening|MISS|0.55|2026-09-01|Channel 1 DXY +0.16%
Sector breadth expansion (% names up)|MISS|0.70|2026-09-01|Channel 1 XLI 1d/1w/1m rel all negative
Sector breadth failure (ETF up, names flat)|MISS|0.60|2026-09-01|ETF itself is down, not a mega-carry up day
Large-cap leadership inside sector|MISS|0.70|2026-09-01|https://public.com/stocks/gev/pre-market
Small/mid leadership inside sector|MISS|0.50|2026-09-01|checked, nothing material
High-beta leadership inside sector|MISS|0.55|2026-09-01|NQ -1.01%; industrials not high-beta lead
Low-beta leadership inside sector|MISS|0.50|2026-09-01|checked, nothing material
Sector ETF inflow / relative volume spike|MISS|0.60|2026-08-29|https://etfdb.com/etf/XLI/
Sector ETF outflow / volume dry-up|HIT|0.55|2026-08-28|https://www.etf.com/sections/daily-etf-flows/daily-etf-flows-xle-sees-inflows
Crowded long (extreme relative performance + valuation)|MISS|0.70|2026-08-31|Channel 1 1m rel -5.25%
Index rebalance / inclusion tailwind|MISS|0.40|2026-09-01|checked, nothing material
Index exclusion / forced selling|MISS|0.40|2026-09-01|checked, nothing material
ISM manufacturing / new orders expansion|HIT|0.45|2026-08-03|https://www.prnewswire.com/news-releases/manufacturing-pmi-at-55-6-july-2026-ism-manufacturing-pmi-report-302840669.html
Durable goods / CapEx upside|MISS|0.55|2026-08-26|July durables already traded 08-26; not same-morning
Grid / electrical equipment backlog (AI power)|HIT|0.70|2026-08-31|https://www.trefis.com/stock/gev/articles/613701/the-engine-behind-gev-stock-has-real-parts/2026-08-31
Aerospace & defense order / budget upside|MISS|0.55|2026-09-01|F-15 ceiling stale; BA red premarket; do not cancel ISM with one award
Freight / trucking / rail volume recovery|HIT|0.45|2026-08-15|https://www.supplychain247.com/article/intermodal_volumes_remain_on_a_growth_track_in_july_reports_iana
Reshoring / industrial policy funding|MISS|0.40|2026-09-01|checked, nothing material
ISM contraction|MISS|0.70|2026-09-01|https://www.financecalendar.com/us-ism-manufacturing-pmi/
CapEx cuts / order cancellation|MISS|0.50|2026-09-01|checked, nothing material
Freight recession|MISS|0.50|2026-09-01|rail still expanding; trucking soft is not a recession HIT
Construction slowdown|HIT|0.65|2026-08-03|https://www.census.gov/construction/c30/current/
Sector rotation into industrials|MISS|0.75|2026-08-31|Channel 1 1w rel -3.01% / 1m rel -5.25%
Sector rotation out of industrials|HIT|0.75|2026-08-31|Channel 1 1d rel -1.53%; 1m rel -5.25%
HIT_GRID_END

## RESEARCH APPENDIX

**Queries run**
- ISM manufacturing PMI August 2026 release September 1
- XLI industrials ETF news GE Vernova Eaton Boeing freight ISM September 1 2026
- Strait of Hormuz IRGC blockade oil price September 2026
- US economic calendar September 1 2026 ISM manufacturing durable goods
- XLI ETF flows inflows outflows August 2026
- September 2026 Fed hike odds Warsh Jackson Hole CME FedWatch
- US construction spending July 2026 forecast September 1
- Caterpillar GE Vernova Eaton Boeing RTX premarket September 1 2026
- oil prices climb US Iranian attacks September 1 2026 Brent
- ISM manufacturing PMI August 2026 actual result 10:00
- X search: XLI industrials oil Hormuz ISM manufacturing premarket September 1 2026 (2026-08-31 to 2026-09-01)
- web_fetch: Al Jazeera 2026-09-01 oil/Hormuz
- web_fetch: Reuters 2026-09-01 futures (JS wall / 401)

**Key sources (title + URL + timestamp where available)**
- Al Jazeera — “Oil prices jump as US, Iranian attacks stoke fears of escalation” — https://www.aljazeera.com/economy/2026/9/1/oil-prices-climb-as-us-iranian-attacks-stoke-fears-of-escalation — fetched 2026-09-01T12:41:14Z
- Reuters (search citation) — Wall St futures kick off September under pressure as yields, oil rise — https://www.reuters.com/business/wall-st-futures-kick-off-september-under-pressure-yields-oil-prices-rise-2026-09-01/
- Reuters (search citation) — Oil prices rise as latest fighting resurrects Middle East supply-disruption risks — https://www.reuters.com/business/energy/oil-prices-rise-latest-fighting-resurrects-middle-east-supply-disruption-risks-2026-09-01/
- ISM / PR Newswire — Manufacturing PMI at 55.6, July 2026 — https://www.prnewswire.com/news-releases/manufacturing-pmi-at-55-6-july-2026-ism-manufacturing-pmi-report-302840669.html
- Finance Calendar — US ISM Manufacturing PMI (Sep 1, 10:00 ET) — https://www.financecalendar.com/us-ism-manufacturing-pmi/
- ISM report calendar — https://www.ismworld.org/supply-management-news-and-reports/reports/rob-report-calendar/
- Trading Economics — US business confidence / ISM consensus ~55.2 — https://tradingeconomics.com/united-states/business-confidence
- FedRateCalc — September 2026 US economic calendar (ISM, JOLTS, construction spending Sep 1) — https://fedratecalc.com/us-economic-calendar/september-2026/
- Census C30 construction spending (June: $2,166.5B SAAR, −0.1% MoM, −3.2% y/y) — https://www.census.gov/construction/c30/current/
- Trefis — GEV backlog / engine (2026-08-31) — https://www.trefis.com/stock/gev/articles/613701/the-engine-behind-gev-stock-has-real-parts/2026-08-31
- Turbomachinery International — GEV gas-turbine backlog 116 GW — https://www.turbomachinerymag.com/view/ge-vernova-gas-turbine-backlog-hits-116-gw-as-power-orders-more-than-double
- ETFDB — XLI flows ~+$151M 1m — https://etfdb.com/etf/XLI/
- ETF.com — daily flows (XLI ~−$370M on 08-27 report) — https://www.etf.com/sections/daily-etf-flows/daily-etf-flows-xle-sees-inflows
- CNBC — September Fed decision a coin flip as hike odds increase (post-Warsh) — https://www.cnbc.com/2026/08/28/-september-fed-decision-now-a-coin-flip-as-rate-hike-odds-increase.html
- Federal Reserve — Warsh Jackson Hole speech 2026-08-28 — https://www.federalreserve.gov/newsevents/speech/warsh20260828a.htm
- Public.com premarket — CAT / GEV snapshots Sep 1 — https://public.com/stocks/cat/pre-market ; https://public.com/stocks/gev/pre-market
- IANA / Supply Chain 247 — July intermodal volumes still growing — https://www.supplychain247.com/article/intermodal_volumes_remain_on_a_growth_track_in_july_reports_iana
- Channel 1 pre-fetched panel (VIX, ES/NQ, CL/BZ, DFII10/DGS10/DGS30, DXY, XLI vs SPY tape through 2026-08-31) — injected, not altered

**Facts taken**
- ISM August **not printed** at compile time; 10:00 ET; consensus ~55.2 vs July 55.6 expansion.
- Construction spending July also 10:00 ET; June −3.2% y/y; July consensus ~0% MoM.
- Live oil-up: Brent ~$92 / WTI ~$88; Channel 1 CL +2.06% / BZ +1.49%; weekend US–Iran strikes + Hormuz tanker hit.
- ES −0.53% / NQ −1.01%; Europe −0.69%; VIX 15.81 backwardation.
- DFII10 +0.08 1d to 2.42; 10Y 4.73; 30Y 5.22.
- XLI vs SPY: 1d rel −1.53%, 1w −3.01%, 1m −5.25%.
- Premarket CAT ~−1.1%, GEV ~−1.3%, BA ~−1.7%.
- XLI flows mixed: 1m modest inflow, 08-27 large outflow.
- Sep FOMC hike odds ~58–67% after hawkish Warsh 08-28 (no longer two-sided).
- GEV structural backlog intact but already in price and red premarket.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': 0.0, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': -1.0}, 'multiplier': 0.9, 'leading_sum': -4.0, 'divergence_flagged': False, 'total_score': -4.05, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.56, 'regime': 'risk_off'}
```
