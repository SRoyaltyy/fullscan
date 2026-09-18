# Sector Prediction — Technology — 2026-09-18

- ETF: **XLK**
- rubric: `00_grounding/sectors/technology.md`
- predicted_direction: **flat**
- predicted_magnitude_band: **flat**
- total_score: **11.236** (mult 0.9)
- regime: risk_on
- divergence_flagged: **False**
- engine: v2 · tape_anchor **5.296** (NQ +1.50%, ES +1.14%, PM:XLK +0.60%) · index_carry **1.215** (general 4.861) · llm_overlay **4.725** (raw 4.725)

## Channel 1 sector ETF tape

```
ETF XLK vs SPY (yfinance, through 2026-09-17):
  1d: XLK +2.25% | SPY +1.13% | rel +1.11%
  3d: XLK +2.05% | SPY +0.23% | rel +1.83%
  1w: XLK +1.53% | SPY +0.63% | rel +0.90%
  1m: XLK +1.31% | SPY -0.63% | rel +1.95%
```

MEMORY_CONFIRM: Technology/XLK only. Memory index unavailable this run (embedding metadata missing); used injected Technology/XLK logs, scoreboard, standing lessons, and mutable policy only. Last graded 2026-09-17 predicted flat/flat vs XLK +2.245% (dir MISS, mag MISS — actual notable). 2026-09-16 predicted flat/flat vs +0.103% (dir MISS, mag HIT). 2026-09-15 predicted down/mild (ungraded). Rolling dir=0.4 mag=0.2 (n=10); 30-run dir=0.4 mag=0.4 (n=20). Open experiment for scope `sector_technology`: **none** (listed opens are utilities/news). Applied: **09-17 stale-RS-veto** — leftover 1w/1m RS must not flip direction when live tape confirms; today’s PM:XLK is +0.60% (below the +1% suppression threshold) but **NQ=F +1.50% vs prior close independently ≥ +0.5%**, so 09-16 still binds DIRECTION = up. **09-16 split** — confirming NQ binds direction; calendar-size/FOMC gate is **idle** (FOMC+SEP+Warsh printed 09-16; this is day-3). **09-11 crowding-zero** — 09-10 crowded-long-fuel ZEROED (oil offered, corr −0.437 not ≤ −0.9, VIX/VIX3M 0.82 contango). **09-10 crowded-long-fuel** — does not fire; trailing 4-horizon rel leadership is not unwind fuel without a live escalating overlay. **09-14 band** — PM gap is direction, not a magnitude extrapolant (XLK PM only +0.60%). **09-09 naming** — Apple iPhone 18 Pro / Pro Max retail availability is **TODAY, 09-18**. **08-12 notable-up FAIL** (no fresh index-relevant mega-cap earnings beat; Apple availability ≠ beat; hawkish path is residual, not benign-macro license for notable). **08-14 stale-positive** — TSMC/HBM/hyperscaler/ASML EUV/Intel memory comments are carried, one AI-infra cluster, not a same-session raise. **08-28 day-2** — mega-cap-earnings-over-macro-drag is open-session only; no fresh beat. **08-21** — do not emit flat/down against confirming NQ. **08-10 Hormuz idle** (CL=F −6.31%, BZ=F −6.17%; Reuters oil −1% on limited-disruption hopes). **08-18 severe-down OFF**. **09-04 hawkish-binary overlay ZEROED** (binary printed; corr/oil/backwardation legs absent). DO-INSTEAD: score sign and live tape **agree up** → keep direction; shrink confidence (mag hit 0.2).

# Technology (XLK) — Sector Environment Analysis — 2026-09-18

Object is the **near-session XLK environment**, not SPX and not a stock picker. US cash session (Friday). **FOMC+SEP+Warsh printed 09-16** (unanimous 25 bp hike to 3.75–4.00%, dots showing one more 2026 hike) — **day-3**, not an unprinted policy binary. **BOJ hiked 09-18 as expected** (7–2 vote to 1.25%; yen weakened). Secondary 09:15 ET industrial production / capacity utilization and 10:00 ET LEI are two-sided and **not** FOMC-class — do not pre-score them, do not flatten direction against confirming NQ.

## Channel 1 (trusted, unaltered)

**Futures are mixed-to-soft on the Finviz board but the yfinance vs-prior-close series is independently green**: Finviz SPX +0.20%, Nasdaq 100 +0.41%, RTY +0.08%, DJIA +0.11%; **ES=F +1.14% vs prev close; NQ=F +1.50% vs prev close**. **XLK premarket +0.60%** — among the greenest of the injected sector PM set (XLB +0.51%, XLI +0.27%, XLY +0.14%, XLF +0.15%; XLE −0.56%). VIX **15.22 (1d −0.22, 1w −0.62)**; VIX3M 18.55; **VIX/VIX3M 0.82 — contango, not backwardation**. **Oil is offered**: Finviz WTI −1.59% / Brent −1.02%; **CL=F −6.31% 1d, BZ=F −6.17% 1d**; live wire: Reuters 09-18 *“Oil prices fall 1% on hopes of limited supply disruptions”* (Brent ~$103.8 / WTI ~$100.9, still >$100 **level**, not a live spike). **Real yields**: DFII10 **2.68 (1d +0.06, 1w +0.22, 1m +0.24)** — duration tax is the *level and 1w trend*, not an 08-10 spike; DGS10 5.01 (1d +0.01, 1w +0.18); DGS30 5.35 (1d −0.01). **5-day 10Y–SPX corr −0.437** (negative, nowhere near ≤ −0.9). USD mixed (Finviz USD −0.02%; DXY 1d +0.12%). HY OAS 2.70 (1d −0.06, still tight). **Asia green** (Nikkei +1.38%, Hang Seng +0.60%, Shanghai +0.94%, **Kospi +2.66%** — the memory/semi tell is *up*; composite +1.11%). **Europe red** (FTSE −0.53%, DAX −0.41%, CAC −0.56%, EuroStoxx50 −0.35%; composite −0.46%). XLK vs SPY through 09-17: **1d rel +1.11%, 3d +1.83%, 1w +0.90%, 1m +1.95%** — XLK is a **multi-timeframe relative leader across all four horizons**.

## Channel 2

**1. Shared macro → this sector.** One regime object, counted once. The *knowable* tape is risk-on for long-duration tech: oil falling on limited-disruption hopes (Reuters), VIX down and in contango, Asia broadly green with **Kospi +2.66%**, and **NQ independently +1.50% vs prior close**. Europe’s red composite (−0.46%) is a partial offset, not a US-tech kill. The *already-printed* overlay is the hawkish Fed (hike + dots) — **paid into 09-16/09-17**; 09-17 was the shake-off session (XLK +2.25% / SPY +1.13%). **BOJ hiked as expected** and the yen weakened — carry-trade-friendly, not a global-liquidity shock. 09-04’s full hawkish overlay does **not** fire: corr −0.437 not −0.943, live DFII10 impulse +6 bp (not a spike), oil offered, backwardation absent. Per 09-11, **zero that lesson’s S0 penalty rather than damp it**. 08-10 Hormuz is idle. Do **not** pre-score industrial production / LEI. Do **not** ignore confirming NQ. Real-yield *level* (DFII10 2.68, 1w +22 bp) is a duration tax that **caps S0 below +2**, not a sign-flip. **S0 = +1.0**. Regime: **risk_on** (session); hawkish path is residual level, not today’s impulse.

**2. Spine — one AI-infra cluster, not three hits.** Hyperscaler 2026 capex still huge (forecasts still $600B+ for the big five), TSMC leading-edge/CoWoS booked (2026 capex $60–64B, leading-edge near full util), HBM 2026 output sold out / prices 5–7× per Intel CEO Lip-Bu Tan (AI Infra Summit ~09-15/16), cloud last-prints still accelerating (AWS +37%, Azure +43%, GCP +82% in Q2) — **structurally intact, already in the tape, not a same-session raise**. Do **not** count capex + foundry + HBM as three spines. Live same-morning:
- **Apple iPhone 18 Pro / Pro Max retail availability TODAY (09-18)** in 65+ markets after the 09-09 event and 09-12 pre-orders. Per **09-09**, this **must be named**. Apple is XLK’s largest holding. MAP HEAT **OVERRIDE Consumer Electronics dir=up** (AAPL). This is a **scheduled mega-cap product-availability catalyst**, not an earnings beat, and **must not by itself set notable**. Modest same-session support for the top weight.
- **Intel CEO: memory prices 5–7×, CPUs meeting ~50% of demand, no relief until ~2028** (TrendForce / Seoul Economic Daily, 09-16). Same AI-infra tightness cluster, **T+2 / carried** (08-14). Not a new HIT.
- **ASML 2027 low-NA EUV nearly sold out on AI demand (JPM)** — News Judge #4, **carried**. Same cluster.
- **Export controls** — checked, nothing material this morning. H200 case-by-case is old; BIS China-fab authorization noise is not a same-session tightening print. **No HIT.**
- **AI-spend peak / Amodei pacing** — T+n and already faded 09-15/17; not a fresh kill.
- Do **not** let NVDA alone define XLK.

Net: spine **intact with one named scheduled mega-cap support, not a raise, not a kill**. **S1 = +1**.

**3. Secondary / taxonomy checklist.** Software net-retention / large-deal upside is **partial** (MAP HEAT Software-Application HEAT up on Dreamforce/CRM, but CRM closed −3.07% on 09-17; NOW/INTU also red — sleeve is not a clean XLK driver). Software multiple-compression is **carried**, not a fresh scare. **Sector rotation into technology** is the live 4-horizon rel tape (1d/3d/1w/1m all positive) plus XLK PM +0.60%. Real yields rising is scored in S0, not restacked here. Crowding is a positioning fact (see S3), not a second spine.

**4. Breadth / leadership inside the sector.** Yesterday was **not** ETF-only: SOX +3.14%, SMH +2.76% with NVDA/MU/INTC/AMD/AVGO participating (09-17 close). This morning **Kospi +2.66%** confirms the memory/semi complex is bid in Asia, not washing out. MAP HEAT nested (stale vs that rebound) still shows a **split book**: OVERRIDE Consumer Electronics **up**, IT Services **up**, Software-Application **up**, vs HEAT Semiconductors / Computer Hardware / Electronic Components **down** and SPLIT Semi-Equipment **down** (LRCX/AMAT w1 sold). Nested OVERRIDE/SPLIT is noted, but **live Channel 1 + Kospi outrank leftover HEAT** (same spirit as 09-17 live-tape-over-leftover-RS). Leadership is **large-cap / AI-hardware**, which **is** the XLK thesis — not a breadth-failure fade. Not a small/mid expansion. **S2 = +0.5** (constructive participation, not a broad % names-up melt-up).

**5. Flows / positioning / crowding.** XLK is again a **4-horizon relative leader** after 09-17’s +2.25%. That *looks* like crowded-long fuel — and 09-10 would treat it as unwind fuel **if** oil were spiking, corr ≤ −0.9, and VIX backwardated. Those legs are **absent/inverted** (oil offered, corr −0.437, contango). Per 09-11, **ZERO the S3 penalty**. Flow commentary (ETF Action / rotation notes) shows mixed XLK creations/redemptions and some SOXX/SMH de-crowding earlier in September — **not** a same-morning forced-flow impulse. Do not over-weight S3 as a magnitude cap (08-12 family) when the overlay is easing. **S3 = 0**.

**6. Earnings / guidance / policy catalysts.** No mega-cap earnings this session. **Apple availability is the named scheduled catalyst.** FOMC is paid. No fresh BIS tightening. Industrial production 09:15 / LEI 10:00 are not XLK spines. News Judge #1 (Warsh JH hike-odds) is **stale** relative to the 09-16 print — do not re-score it as a live Chair shock.

## S4 tape (confirmation only)

Channel 1 relative returns are **unambiguously positive** on 1d/3d/1w/1m. That **confirms** the factor lean; it is **not** the thesis and is **not** an absolute-up certificate by itself (XLK can still fall while beating SPY). **S4 = +1**.

## Divergence / self-audit

Leading sum S0+S1+S2+S3 = **+1.0 + 1.0 + 0.5 + 0 = +2.5** (positive). S4 = +1 (positive). **No leading-vs-tape fight.** DO-INSTEAD 09-16/09-17 (“prefer flat/mild when score fights tape”) does **not** bind; keep direction, shrink confidence because mag hit = 0.2.

- **Lens:** XLK sector environment, not SPX, not NVDA-only.
- **Band:** 08-12 notable-up **fails** (no fresh mega-cap beat). 09-14: PM +0.60% is **not** a notable extrapolant. MAP HEAT `size_gate=True`. Mild, not notable/severe.
- **Skew:** residual hawkish *level* and Europe red cap S0 at +1, not +2. Oil relief counted **once** in S0.
- **Same-shock double-count:** AI-infra (capex/foundry/HBM/Intel memory/ASML) = **one cluster**. Oil = S0 only. Real yields = S0 only.
- **Single-ticker:** Apple is named and supports S1 modestly; **does not solely drive** the XLK call. NVDA does not define the sector.

**Multiplier 0.9** (mag accuracy poor; no extreme overlay). **Confidence 0.55**.

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|HIT|0.72|2026-09-18|https://www.reuters.com/business/nasdaq-futures-lead-wall-st-gains-oil-retreat-eases-inflation-worries-2026-09-18/
Risk-off tape / flight to safety|ABSENT|0.70|2026-09-18|https://www.reuters.com/business/energy/oil-prices-fall-1-hopes-limited-supply-disruptions-2026-09-18/
Real yields rising|PARTIAL|0.65|2026-09-16|channel1:DFII10=2.68,1d=+0.06,1w=+0.22
Real yields falling|ABSENT|0.65|2026-09-16|channel1:DFII10_1d=+0.06
USD strengthening|PARTIAL|0.50|2026-09-18|channel1:DXY_1d=+0.12
USD weakening|ABSENT|0.50|2026-09-18|channel1:Finviz_USD=-0.02
Sector breadth expansion (% names up)|PARTIAL|0.58|2026-09-18|https://historyofmarket.com/semi/semi-price/
Sector breadth failure (ETF up, names flat)|ABSENT|0.55|2026-09-17|channel1:XLK_1d=+2.25%;SOX_+3.14%
Large-cap leadership inside sector|HIT|0.70|2026-09-18|MAP_HEAT:OVERRIDE_Consumer_Electronics_AAPL
Small/mid leadership inside sector|ABSENT|0.55|2026-09-18|MAP_HEAT:semi_equip_SPLIT_down
High-beta leadership inside sector|PARTIAL|0.60|2026-09-18|channel1:NQ=F=+1.50%;Kospi=+2.66%
Low-beta leadership inside sector|ABSENT|0.55|2026-09-18|checked, nothing material
Sector ETF inflow / relative volume spike|ABSENT|0.45|2026-09-18|https://www.etfaction.com/large-cap-blend-surges-as-tech-sees-broad-outflows/
Sector ETF outflow / volume dry-up|PARTIAL|0.40|2026-09-18|https://www.etfaction.com/large-cap-blend-surges-as-tech-sees-broad-outflows/
Crowded long (extreme relative performance + valuation)|PARTIAL|0.62|2026-09-18|channel1:XLK_rel_1d/3d/1w/1m_all_positive;precondition_inverted
Index rebalance / inclusion tailwind|ABSENT|0.50|2026-09-18|checked, nothing material
Index exclusion / forced selling|ABSENT|0.50|2026-09-18|checked, nothing material
Hyperscaler CapEx raise / AI infra spend upside|PARTIAL|0.68|2026-09-16|https://techblog.comsoc.org/2026/09/16/delloro-data-center-capex-grew-92-in-2q-2026-caveats-galore/
Semiconductor demand / foundry utilization up|PARTIAL|0.70|2026-09-14|https://www.trendforce.com/news/2026/09/14/news-tsmc-reportedly-targets-22-2nm-16-3nm-capacity-boost-by-mid-2027-cowos-to-double-by-2028/
HBM / advanced packaging shortage pricing power|PARTIAL|0.72|2026-09-16|https://www.trendforce.com/news/2026/09/16/news-intel-ceo-flags-ai-supply-squeeze-memory-prices-up-5-7x-its-cpus-meet-just-50-of-demand/
Cloud consumption growth acceleration|PARTIAL|0.60|2026-08-18|https://www.fool.com/investing/2026/08/18/google-cloud-grew-82-last-quarter-azure-grew-43-and-aws-grew-37/
Software net retention / large deal upside|PARTIAL|0.45|2026-09-18|MAP_HEAT:Software-Application_up;CRM_09-17_-3.07%
Hyperscaler CapEx cut / AI spend peak narrative|ABSENT|0.65|2026-09-18|checked, nothing material this morning
Semi downturn / inventory correction|ABSENT|0.60|2026-09-18|https://www.trendforce.com/news/2026/09/16/news-intel-ceo-flags-ai-supply-squeeze-memory-prices-up-5-7x-its-cpus-meet-just-50-of-demand/
Cloud growth deceleration|ABSENT|0.60|2026-09-18|checked, last prints still accelerating
Export controls tightening|ABSENT|0.55|2026-09-18|checked, nothing material this morning
Software multiple compression / growth scare|PARTIAL|0.45|2026-09-17|CRM/NOW/INTU_09-17_red;carried_not_fresh
Sector rotation into technology|HIT|0.70|2026-09-17|channel1:XLK_rel_1d=+1.11%,3d=+1.83%,1w=+0.90%,1m=+1.95%
Sector rotation out of technology|ABSENT|0.65|2026-09-18|channel1:PM_XLK=+0.60%
HIT_GRID_END

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 1.0
S1_SECTOR_FACTORS: 1.0
S2_BREADTH: 0.5
S3_FLOWS_POSITIONING: 0.0
S4_ETF_TAPE: 1.0
MULTIPLIER: 0.9
CONFIDENCE: 0.55
REGIME: risk_on
DIVERGENCE_FLAGGED: false
HORIZON_3D: up
HORIZON_1W: up
HORIZON_2W: mixed
HORIZON_1M: up
SECTOR_SCORES_END

## RESEARCH APPENDIX

**Queries run**
- web_search: `stock market futures Nasdaq S&P premarket September 18 2026` (freshness=day)
- web_search: `Apple iPhone 18 Pro availability September 18 2026` (freshness=week)
- web_search: `TSMC foundry utilization HBM shortage hyperscaler capex 2026 September` (freshness=week)
- web_search: `BOJ rate hike September 18 2026 yen stocks` (freshness=day)
- web_search: `Intel CEO memory prices 5-7x CPU supply constraints September 2026` (freshness=week)
- web_search: `XLK ETF flows crowding semiconductor positioning September 2026` (freshness=week)
- web_search: `export controls semiconductors China BIS September 2026` (freshness=week)
- web_search: `US economic calendar September 18 2026 retail sales housing CPI FOMC` (freshness=day)
- web_search: `Nasdaq semiconductor SOX SMH breadth leadership September 18 2026` (freshness=day)
- web_search: `oil prices fall limited supply disruptions Reuters September 18 2026` (freshness=day)
- web_search: `cloud spending growth AWS Azure Google Q2 2026 deceleration acceleration` (freshness=week)
- web_search: `FedWatch September 2026 rate odds after FOMC hike` (freshness=week)
- web_search: `software stocks CRM NOW INTU ADBE September 18 2026` (freshness=day)
- web_fetch: `https://www.reuters.com/business/nasdaq-futures-lead-wall-st-gains-oil-retreat-eases-inflation-worries-2026-09-18/` (401/JS wall)
- x_search: `XLK Nasdaq semiconductors premarket Apple iPhone 18 September 18 2026` (from 2026-09-17 to 2026-09-18)
- memory_search: `Technology XLK sector prediction lessons catalysts crowding` (index unavailable)

**Key sources and facts taken**
- Reuters — Nasdaq futures lead Wall St gains as oil retreat eases inflation worries (2026-09-18): NQ leading premarket; oil retreat as inflation-optics support. https://www.reuters.com/business/nasdaq-futures-lead-wall-st-gains-oil-retreat-eases-inflation-worries-2026-09-18/
- Reuters — Oil prices fall 1% on hopes of limited supply disruptions (2026-09-18): Brent −$1.01 to ~$103.77, WTI −$1.03 to ~$100.88; Saudi repair/alternative-route hopes. https://www.reuters.com/business/energy/oil-prices-fall-1-hopes-limited-supply-disruptions-2026-09-18/
- Reuters — BOJ raises rates to 31-year high, widely expected (2026-09-18): +25 bp to 1.25%, 7–2 vote; yen weaker, Nikkei higher. https://www.reuters.com/world/asia-pacific/boj-raises-interest-rates-31-year-high-widely-expected-move-2026-09-18/
- MacObserver / 9to5Mac / MacRumors — iPhone 18 Pro availability 2026-09-18 in 65+ markets after 09-09 event / 09-12 pre-orders. https://www.macobserver.com/tips/round-ups/iphone-18-pro-september-18-what-to-do-before-the-box-arrives/
- TrendForce (2026-09-16) — Intel CEO: memory prices up 5–7×, CPUs meet ~50% of demand. https://www.trendforce.com/news/2026/09/16/news-intel-ceo-flags-ai-supply-squeeze-memory-prices-up-5-7x-its-cpus-meet-just-50-of-demand/
- TrendForce (2026-09-14) — TSMC 2nm/3nm capacity boost / CoWoS tightness. https://www.trendforce.com/news/2026/09/14/news-tsmc-reportedly-targets-22-2nm-16-3nm-capacity-boost-by-mid-2027-cowos-to-double-by-2028/
- Fool (2026-08-18) — GCP +82%, Azure +43%, AWS +37% Q2 2026 (carried cloud-acceleration print). https://www.fool.com/investing/2026/08/18/google-cloud-grew-82-last-quarter-azure-grew-43-and-aws-grew-37/
- ETF Action — tech/semi ETF mixed outflows / rotation, not liquidation. https://www.etfaction.com/large-cap-blend-surges-as-tech-sees-broad-outflows/
- SOX/SMH 09-17 close: SOX +3.14% to 11,599.49; SMH +2.76% to $560.61. https://historyofmarket.com/semi/semi-price/
- Forex TradingCharts / Value Line calendar — 2026-09-18: industrial production 09:15 ET, LEI 10:00 ET; no CPI/NFP/FOMC today (FOMC 09-16, CPI 09-11). https://forex.tradingcharts.com/economic_calendar/2026-09-18.html?code=USD
- FinanceFeeds / Reuters GS — post-09-16 FedWatch: ~40–50% October hike odds, higher December cumulative; residual hawkish path, not a live binary. https://financefeeds.com/will-the-fed-raise-interest-rates-again-october-odds-45/
- Channel 1 panel (injected, unaltered): VIX 15.22, VIX/VIX3M 0.82, NQ=F +1.50%, ES=F +1.14%, XLK PM +0.60%, DFII10 2.68, corr −0.437, Kospi +2.66%, XLK–SPY rel +1.11%/+1.83%/+0.90%/+1.95%.
- MAP HEAT (injected): Consumer Electronics OVERRIDE up (AAPL); Semiconductors/Hardware/Components HEAT down; Semi-Equipment SPLIT down; Software-Application and IT Services HEAT up; size_gate=True.

**Not used as live same-morning hits:** News Judge Warsh JH item (stale vs 09-16 FOMC print); Finviz APH −6.5% (prior-week); ASML EUV as a *new* spine (carried, same AI-infra cluster).

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 0.5, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 1.0}, 'multiplier': 0.9, 'leading_sum': 6.0, 'divergence_flagged': False, 'total_score': 11.236, 'predicted_direction': 'flat', 'predicted_magnitude_band': 'flat', 'confidence_score': 0.55, 'regime': 'risk_on', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.8826, 'score': 5.296, 'legs': [{'leg': 'NQ', 'pct': 1.5, 'w': 0.8}, {'leg': 'ES', 'pct': 1.14, 'w': 0.3}, {'leg': 'PM:XLK', 'pct': 0.6, 'w': 0.7}]}, 'overlay_score': 4.725, 'overlay_raw': 4.725, 'index_carry': 1.215, 'general_total': 4.861, 'skill_multipliers': {'S0_SHARED_MACRO': 1.25, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 1.25, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 1.25}, 'llm_confidence': 0.55, 'sector_rs_veto_applied': True, 'sector_rs_tape': {'d1': -2.05, 'w1': -2.07}, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
