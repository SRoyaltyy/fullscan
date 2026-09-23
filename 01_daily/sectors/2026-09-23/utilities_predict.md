# Sector Prediction — Utilities — 2026-09-23

- ETF: **XLU**
- rubric: `00_grounding/sectors/utilities.md`
- predicted_direction: **flat**
- predicted_magnitude_band: **flat**
- total_score: **0.717** (mult 0.9)
- regime: mixed
- divergence_flagged: **False**
- engine: v2 · tape_anchor **0.144** (ES +0.03%, ZN -0.03%, PM:XLU +0.04%) · index_carry **0.573** (general 2.293) · llm_overlay **0.0** (raw 0.0)

## Channel 1 sector ETF tape

```
ETF XLU vs SPY (yfinance, through 2026-09-21):
  1d: XLU -0.34% | SPY +1.55% | rel -1.89%
  3d: XLU -0.87% | SPY +2.83% | rel -3.71%
  1w: XLU -2.06% | SPY +1.91% | rel -3.97%
  1m: XLU -6.42% | SPY +1.68% | rel -8.10%
```

MEMORY_CONFIRM: Utilities/XLU only — memory index paused (`openclaw memory status --index` / `openclaw memory index --force`); used injected sector logs + Channel 1 + Channel 2. Rolling dir=0.5 / mag=0.3 (n=10); last 30 dir=0.435 / mag=0.348 (n=23). Last graded: 09-22 down/mild vs XLU −0.32% / SPY −0.02% / rel −0.30% (dir HIT, mag HIT — S0–S3 honestly 0, bound 09-14 S4 signed mild leftover vs a flat parent). 09-21 down/mild vs −1.09% / rel −2.65% (dir HIT, mag MISS). Applied: 09-22 (keep S0–S3 at 0 on a no-catalyst T+1; do **not** overlay-veto a *bound* 09-14 S4; do **not** restack 1w/1m into notable; do **not** re-arm S0/S1 on sticky ~5% or leftover Nasdaq — **09-14 does not bind today**: freshest 1d is 09-22 rel −0.30%, sub-gate); 09-21 (S0 −0.5/−1 only if ≥0.5% rip + PM red vs a leading peer + 4-horizon lag — **does not bind**: ES vs-close +0.03% / NQ −0.10%, Finviz ES +0.20% / NQ +0.41% under the rip gate, PM:XLU +0.04% vs XLK −0.05% not a lag vs a leader; extra-confirm is a *ceiling*, not a floor that zeros a live smash that is not present); 09-18 (don’t treat missing AM smash-confirm as a license to re-HIT Friday’s paid backup; leftover slot is now **several sessions old**); 09-16 (do not restack the paid 09-16 hike; AM leftover risk-on alone does not sign the close; do **not** let trailing 1d/1w/1m lag pay another down close as the thesis); 09-17 (rotation-away is relative, not an absolute ceiling); 09-14 (**does not bind**: Channel 1 09-21 |1d rel| 1.89% was already paid 09-22; freshest 1d fails |rel|≥~1%); 09-11 (no CPI/NFP/FOMC — do **not** apply both-branches S0=−1 to flash PMI / Barr / 5Y auction); 09-10 (VIX 14.21 / VIX3M 0.786 contango fails VIX≥20 FTS gate → no 08-18 relative-beat; 5Y note is **not** a 10Y/30Y S1 auction HIT); 09-09 (PM +0.04% is not a ≥0.4% cushion); 09-08 (oil **offering** from war-premium = inflation channel fading, not FTS); 08-28 (do **not** promote IPP CEG/VST SPLIT, SO/Google nuclear, NEE/Dominion, or Duke $10B equity into S1); 08-27 (Nasdaq-record chips tape is T-1; live NQ vs-close is red/modest — relative lag is a *descriptor*, not a fresh smash); 08-25 (**binds**: S0=S1=0 → do not manufacture down from carried S2/S3/stale lag); 08-21 (live 10Y ~4.97–4.98%, not FRED 09-21 4.96 as “today’s move”); 08-13 (one trailing rel print does not pay S2 **and** S4); 08-12 (AI-power / data-center CapEx is a 1d dampener, not a band engine). Open experiment: extra confirm before full weight — **no extra confirm for a fresh rates smash** (ZN −0.03%, 10Y sticky) **and none for a second easing leg**. Scope do-instead (09-21/09-22 wins): shrink confidence on modest |score|. Same-shock: sticky ~5% long end counted in S0 as **carried/not HIT**; not re-HIT in S1. Ex-div **already printed 09-21**; today is pay date — do not restack.

# Utilities (XLU) — 2026-09-23

Object is the **near-session XLU environment**, not SPX and not a stock pick.

## Channel 1 (trusted, not re-derived)

XLU vs SPY through 2026-09-21: **1d −0.34% / +1.55% (rel −1.89%)**; **3d −0.87% / +2.83% (rel −3.71%)**; **1w −2.06% / +1.91% (rel −3.97%)**; **1m −6.42% / +1.68% (rel −8.10%)**. That 1d is **Monday’s paid rotation**, already expressed. Do **not** smuggle a relative-beat clause. Do **not** let 1w/1m pay a second (third) down close (09-16 / 09-22).

Live Channel 2 overlay on the missing session: **09-22 actual XLU −0.32% / SPY −0.02% / rel −0.30%** — mild leftover vs a flat parent, which is exactly what bound 09-14 S4 was allowed to sign yesterday. Freshest 1d rel is **sub-gate** for 09-14.

**HORIZON_3D:** lag (−3.71% rel). **HORIZON_1W:** lag (−3.97% rel). **HORIZON_2W:** lag (no independent 2w print; 1w and 1m both deep red). **HORIZON_1M:** deep lag (−8.10% rel). Structural descriptor, not a same-session catalyst.

Macro: VIX **14.21** (−0.66 1d, −2.99 1w), **VIX/VIX3M 0.786 — CONTANGO** (no stress); DGS10 **4.96** as of 09-21 (−5 bp 1d, −1 bp 1w, **+27 bp 1m**); DGS30 **5.29** (−5 bp 1d, −5 bp 1w, +6 bp 1m); DFII10 **2.62** (−6 bp 1d, +2 bp 1w, **+27 bp 1m**); HY OAS 2.66 (tight); EPU 220.02; **CL=F −4.97% / BZ=F −3.68%** vs Finviz WTI **$104.16 −1.59%** / Brent **$107.67 −1.02%** (both tapes **offering**); DXY **+0.38%** 1d; **ES=F +0.03% / NQ=F −0.10%** vs prior close vs Finviz live **ES +0.20% / NQ +0.41% / RTY +0.08% / DJIA +0.11%** (NQ leads the *live* tape modestly and **fails** the ≥0.5% rip gate; vs-close is **flat-to-red**, not a rip); **XLU PM +0.04%** vs XLK **−0.05%**, XLP **−0.07%**, XLE **+0.21%**, XLY **+0.10%**, XLF **+0.09%** — XLU is **mid-pack / non-haven**, slightly green, **not** the funding source of a live rip; Asia **+0.33%**; Europe **−0.25%**; 5-day 10Y–SPX corr **−0.79**. Bond futures: 10Y note **−0.03%**, 30Y **−0.06%**, Ultra Bond **−0.06%** — a **tiny backup**, not a smash and not relief.

**Live curve (08-21):** CountryEconomy **10Y 4.97% on 09/22** (no 09-23 print yet); cluster ~**4.97–4.98%** (GuruFocus/MarketWatch). 30Y live **~5.30–5.32%** vs FRED 09-21 5.29%. This is **stabilization inside the ~5% stress zone**, not a scored easing impulse and not a fresh backup. Do **not** pay FRED 09-21 4.96%, Wednesday’s FOMC, Friday’s 5.00%, Monday’s rotation, or Tuesday’s leftover twice.

**Calendar (08-14 / 09-04 / 09-11):** **No 8:30 CPI/PCE/NFP. No FOMC** (printed 09-16). **No long-end 10Y/30Y auction.** **$70B 5-Year note** + FRN reopening are **front/intermediate supply** — 09-10’s S1 auction rule is 10Y/30Y, not 5Y. **S&P Global flash PMIs ~9:45 ET** and **Barr speech** are **not** CPI-class; branch test is two-sided (strong PMI → relative lag; weak PMI → possible duration bid). Do **not** apply the 09-11 S0=−1 template. Fed-speaker leftover is event risk → lower confidence, **not** a signed call. XLU **pay date** today; ex-div already printed 09-21.

## Channel 2

**1. Shared macro → this sector.** Classical map is **real/nominal yields**; AI load is structural offset only.

- **No live rip, no live FTS.** ES +0.03% / NQ −0.10% vs prior close; Finviz NQ +0.41% under the 09-21 ≥0.5% gate; VIX 14.21 deep contango. 09-21’s S0-negative template **fails**. Nasdaq-record/chips (News Judge #1) is **T-1 path**, not this morning’s tape.
- **Sticky ~5% long end, not independently repricing.** Live 10Y ~4.97–4.98%, ZN −0.03%, ZB −0.06%. Extra-confirm **fails** for a smash. 09-22: do **not** re-arm S0 on sticky yields. 09-08: oil offering from war-premium fades the inflation channel; it does **not** mint FTS, and it does **not** mint a duration bid by itself.
- **09-10 gate:** VIX 14.21 < 20 and contango — rising/sticky long end is a **relative-lag descriptor**, not an 08-18 relative beat. No FTS impulse.
- **Unprinted flash PMI / Barr / 5Y auction:** two-sided event risk. Strong PMI is the 09-11 *in-line/growth* branch (rotation-away, negative-to-neutral for a defensive); weak PMI is the one duration-positive leg. **Not fully asymmetric-negative.** Do not pre-score either branch (09-11 scoped to CPI/NFP/FOMC).

**S0 = 0.** Mixed/carried. Extra-confirm ceiling holds; rip gate fails; do not manufacture S0− from leftover Nasdaq or sticky 5%.

**2. Spine / secondary.**
- **Data-center load growth / power demand upside:** structural HIT, **stale** for a 1d call. CapEx / large-load tariffs / nuclear PPAs are multi-month. Rubric: do **not** let the multi-year AI-power story override a 1d rate tape without a fresh catalyst. 08-12 = dampener only. MAP HEAT: regulated-electric breadth **0.098** (two names carrying); IPP **SPLIT down** (CEG/VST/HNRG) — nested override, **not** parent XLU.
- **Rates falling (bond-proxy bid):** **MISS**. Live 10Y ~4.97–4.98%, bond futures tiny red.
- **Rates rising (bond-proxy selloff):** **PARTIAL / carried**. Tiny ZN/ZB backup inside the stress zone. Extra-confirm fail. Do **not** HIT.
- **Risk-on rotation away from utilities:** **MISS as a live 1d HIT**. T-1 Nasdaq record is paid; this morning XLK PM −0.05% vs XLU +0.04%. 09-17: rotation is relative, not an absolute ceiling. 09-16: leftover risk-on alone does not sign the close.
- **Risk-off tape / flight to safety:** **MISS**. VIX 14.21 contango, no FTS.
- **Nuclear / gas generation policy support:** SO/Google Vogtle/Hatch (09-21) already public — 08-28, not an ETF driver.
- **Grid CapEx / favorable ROE / Duke $10B equity / Florida large-load hearing:** single-name or carried. 08-28.
- **Adverse rate case / load-growth disappointment / regulatory smash:** carried (Texas/WoodMac). Not fresh.
- **Sector rotation into utilities:** **MISS**.
- **Sector rotation out of utilities:** **carried** from Monday; not live in the PM board.

Net: no fresh spine HIT. **S1 = 0.**

**3. Breadth.** MAP HEAT parent sleeves **flat** (diversified / regulated electric / gas). Water **up** is nested, not parent. IPP and renewables are **SPLIT down** — do not average into XLU. Channel 1 4-horizon lag is a **structural descriptor** (09-16) and does not pay S2 once S4 is not being restacked (08-13). No live % names-up expansion. **S2 = 0.**

**4. Flows / positioning.** ETFdb through 09-22: **5d −$184M, 1m −$65M, 3m +$287M** — modest short-term outflows, not a same-session volume spike. X chatter flags outflows with XLI/XLV, not a utilities-specific forced flow. Crowded-long is **inverted** (1m rel −8.1%, near 52-week lows) — not a squeeze setup for a 1d call. 08-25: carried S3 must not manufacture down when S0=S1=0. **S3 = 0.**

**5. Earnings / policy catalysts.** No XLU-wide print. Flash PMI / Barr / 5Y auction = event risk, not a signed HIT. Pay date ≠ ex-div. **checked, nothing material** as a same-session ETF catalyst.

**S4_ETF_TAPE.** Channel 1 through 09-21 is still 4-horizon red with |1d rel| 1.89%, but that 1d **already paid** 09-21 and was confirmed as mild leftover on 09-22. Freshest 1d rel **−0.30%** fails the 09-14 |1d|≥~1% floor. 09-16 forbids paying trailing lag as another down close. 08-13: S4 is confirmation only, never the thesis. Unbound S4 does **not** get to sign the card. **S4 = 0.**

**Self-audit.** Lens = XLU 1d, not SPX, not CEG/VST/NEE/SO. Band: leading sum 0, size_gate on, |score| modest → shrink confidence (open experiment / 09-21–09-22 wins). Skew: not manufacturing down from carried lag (08-25). Same-shock: sticky 5% counted once in S0 as carried/not HIT. Single-ticker must not drive the ETF: IPP SPLIT and SO/Google/Duke equity stay nested. Divergence: leading factors **do not fight** an unbound S4; trust factors over leftover 1w/1m tape. Residual is **flat**.

**S0 = 0, S1 = 0, S2 = 0, S3 = 0, S4 = 0.** Mixed regime. Multiplier 0.9 (event-risk PMI/Barr, unsigned card). Confidence 0.52.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: 0
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.9
CONFIDENCE: 0.52
REGIME: mixed
DIVERGENCE_FLAGGED: false
HORIZON_3D: lag
HORIZON_1W: lag
HORIZON_2W: lag
HORIZON_1M: lag
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|PARTIAL|0.45|2026-09-23|https://www.businesstimes.com.sg/companies-markets/capital-markets-currencies/us-stocks-nasdaq-reaches-record-high-close-ai-stocks-rally
Risk-off tape / flight to safety|MISS|0.80|2026-09-23|https://www.morningstar.com/news/dow-jones/202609227774/nasdaq-composite-rises-045-to-2724428-record-high-close-data-talk
Real yields rising|PARTIAL|0.40|2026-09-21|https://countryeconomy.com/bonds/usa
Real yields falling|MISS|0.70|2026-09-23|https://countryeconomy.com/bonds/usa
USD strengthening|PARTIAL|0.40|2026-09-23|
USD weakening|MISS|0.60|2026-09-23|
Sector breadth expansion (% names up)|MISS|0.70|2026-09-23|https://breadthmarket.com/
Sector breadth failure (ETF up, names flat)|MISS|0.60|2026-09-23|
Large-cap leadership inside sector|PARTIAL|0.45|2026-09-23|
Small/mid leadership inside sector|MISS|0.55|2026-09-23|
High-beta leadership inside sector|MISS|0.65|2026-09-23|
Low-beta leadership inside sector|PARTIAL|0.40|2026-09-23|
Sector ETF inflow / relative volume spike|MISS|0.70|2026-09-22|https://etfdb.com/etf/XLU/
Sector ETF outflow / volume dry-up|PARTIAL|0.55|2026-09-22|https://etfdb.com/etf/XLU/
Crowded long (extreme relative performance + valuation)|MISS|0.75|2026-09-23|
Index rebalance / inclusion tailwind|MISS|0.80|2026-09-23|
Index exclusion / forced selling|MISS|0.80|2026-09-23|
Data-center load growth / power demand upside|HIT|0.70|2026-09-23|https://www.spglobal.com/market-intelligence/en/news-insights/research/2026/04/surging-energy-demand-puts-us-utility-capex-forecast-near-1-3t-in-2026-30
Rates falling (bond-proxy bid)|MISS|0.75|2026-09-23|https://countryeconomy.com/bonds/usa
Favorable rate case / allowed ROE|PARTIAL|0.40|2026-09-22|https://www.utilitydive.com/news/duke-energy-florida-psc-large-load-rate/828951/
Nuclear / gas generation policy support|PARTIAL|0.50|2026-09-21|https://www.reuters.com/legal/litigation/southern-co-unit-signs-deal-with-google-add-nuclear-capacity-2026-09-21/
Grid CapEx approval / recovery|PARTIAL|0.45|2026-09-23|https://www.spglobal.com/market-intelligence/en/news-insights/research/2026/04/surging-energy-demand-puts-us-utility-capex-forecast-near-1-3t-in-2026-30
Rates rising (bond-proxy selloff)|PARTIAL|0.40|2026-09-23|https://www.gurufocus.com/economic_indicators/37/10-year-treasury-yield
Adverse rate case|MISS|0.60|2026-09-23|
Load growth disappointment|MISS|0.55|2026-09-23|
Regulatory disallowance / project cancel|MISS|0.60|2026-09-23|
Risk-on rotation away from utilities|PARTIAL|0.40|2026-09-22|https://www.businesstimes.com.sg/companies-markets/capital-markets-currencies/us-stocks-nasdaq-reaches-record-high-close-ai-stocks-rally
Sector rotation into utilities|MISS|0.75|2026-09-23|
Sector rotation out of utilities|PARTIAL|0.50|2026-09-22|https://etfdb.com/etf/XLU/
HIT_GRID_END

## RESEARCH APPENDIX

**Queries run**
- US 10 year treasury yield today September 23 2026
- XLU utilities ETF news flows premarket September 23 2026
- US economic calendar September 23 2026 Treasury auction FOMC
- data center power demand utilities rate case nuclear grid CapEx September 2026
- 30 year treasury yield September 23 2026
- CME FedWatch September 2026 October hike odds
- XLU vs SPY relative performance utilities rotation September 22 2026
- S&P Global flash PMI September 23 2026 United States
- XLU ETF fund flows September 2026
- utilities sector breadth NEE SO DUK CEG September 23 2026
- US 10 year yield live CountryEconomy September 23 2026
- utilities rate case news Duke NextEra Southern September 22 23 2026
- risk on equity market breadth September 23 2026 Nasdaq record utilities lag
- X search: XLU vs yields/SPY premarket Sep 23 2026
- web_fetch: https://www.gurufocus.com/economic_indicators/37/10-year-treasury-yield (403 ASN banned)

**Key sources and facts taken**
- GuruFocus / MarketWatch (search summary, 2026-09-23): 10Y ~4.98% / ~4.975%, slight up from 4.96%. https://www.gurufocus.com/economic_indicators/37/10-year-treasury-yield
- CountryEconomy (as of 2026-09-22): US 10Y **4.97%**; no 09-23 update yet. https://countryeconomy.com/bonds/usa
- Trading Economics / WSJ cluster (2026-09-23): 30Y ~**5.30–5.32%**. https://tradingeconomics.com/united-states/30-year-bond-yield
- MarketWatch / Yahoo (2026-09-23 premarket): XLU prior close **$40.53** (−0.32% on 09-22); PM ~$40.54–$40.57, thin volume; pay date 09-23, ex-div 09-21. https://www.marketwatch.com/investing/fund/xlu/download-data
- Scotiabank / CME Econoday / Fed calendar (2026-09-23): no FOMC; flash PMIs; MBA; EIA; Barr speech; **$70B 5Y** + FRN — not 10Y/30Y. https://www.scotiabank.com/ca/en/about/economics/economics-publications/post.other-publications.calendar-of-economic-release-dates.calendar-of-economic-release-dates--september-2026-.html
- Phemex/CME FedWatch summary (2026-09-23): Oct **~54% hike / ~46% hold**. https://phemex.com/news/article/cme-fedwatch-542-probability-of-rate-hike-at-october-meeting-97512
- ETFdb (as of 2026-09-22): XLU 5d **−$184M**, 1m **−$65M**, 3m **+$287M**. https://etfdb.com/etf/XLU/
- Business Times / Morningstar (09-22 close): Nasdaq record, SPX flat, utilities lag, narrow breadth. https://www.businesstimes.com.sg/companies-markets/capital-markets-currencies/us-stocks-nasdaq-reaches-record-high-close-ai-stocks-rally
- Breadthmarket: ~1/31 utilities above 20-day SMA. https://breadthmarket.com/
- Reuters (2026-09-21): Southern / Google nuclear uprate at Georgia Power. https://www.reuters.com/legal/litigation/southern-co-unit-signs-deal-with-google-add-nuclear-capacity-2026-09-21/
- Utility Dive (2026-09-22): Duke $10B equity; Duke Florida large-load hearing. https://www.utilitydive.com/news/duke-energy-to-issue-10b-in-equity-to-capture-once-in-a-generation-gro/827039/
- S&P Global MI: structural ~$1.3T 2026–30 utility CapEx / data-center load — stale for 1d. https://www.spglobal.com/market-intelligence/en/news-insights/research/2026/04/surging-energy-demand-puts-us-utility-capex-forecast-near-1-3t-in-2026-30
- X (2026-09-22/23): outflows in XLU with other defensives; RS laggard; AI-power thematic not driving 1d inflows. https://x.com/ETFSignalHQ/status/2102427811071897819

**Not used as 1d HITs:** ASML EUV sold-out, FIX/Lumentum AI backlog, Warsh JH gold dump (paid), crude build/XLE, APH, copper tariffs, apartment debt wall.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': 0.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.9, 'leading_sum': 0.0, 'divergence_flagged': False, 'total_score': 0.717, 'predicted_direction': 'flat', 'predicted_magnitude_band': 'flat', 'confidence_score': 0.529, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.0239, 'score': 0.144, 'legs': [{'leg': 'ES', 'pct': 0.03, 'w': 0.3}, {'leg': 'ZN', 'pct': -0.03, 'w': 0.75}, {'leg': 'PM:XLU', 'pct': 0.04, 'w': 0.7}]}, 'overlay_score': 0.0, 'overlay_raw': 0.0, 'index_carry': 0.573, 'general_total': 2.293, 'skill_multipliers': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 1.25}, 'llm_confidence': 0.52, 'sector_rs_veto_applied': True, 'sector_rs_tape': {'d1': -1.5, 'w1': -3.18}, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
