# Sector Prediction — Industrials — 2026-10-02

- news_mode: **on**
- ETF: **XLI**
- rubric: `00_grounding/sectors/industrials.md`
- predicted_direction: **flat**
- predicted_magnitude_band: **flat**
- total_score: **6.163** (mult 0.8)
- regime: mixed
- divergence_flagged: **False**
- engine: v2 · tape_anchor **3.472** (ES +0.50%, ER2 +0.08%, HG +0.66%, PM:XLI +0.62%) · index_carry **1.091** (general 4.365) · llm_overlay **1.6** (raw 1.6)

## Channel 1 sector ETF tape

```
ETF XLI vs SPY (yfinance, through 2026-10-01):
  1d: XLI +0.99% | SPY +0.18% | rel +0.82%
  3d: XLI -0.08% | SPY -0.21% | rel +0.13%
  1w: XLI -0.11% | SPY -0.42% | rel +0.30%
  1m: XLI -2.11% | SPY +0.54% | rel -2.64%
```

MEMORY_CONFIRM: Memory index unavailable this run (embedding metadata mismatch; `openclaw memory status --index` or `openclaw memory index --force` would rebuild). Used injected Industrials scoreboard + 08-11..10-01 sector logs + in-prompt lesson pack only. Rolling dir=0.4 / mag=0.8 (n=10); last 30 dir=0.345 / mag=0.379 (n=29). Last graded 10-01: predicted down/mild vs XLI **+0.994%** / SPY +0.18% / rel +0.82% — **dir MISS** (ISM 54.5 expansion-with-better-internals; overlay flat/mild was the better process call; engine down/mild graded). 09-25 down/mild MISS (durables/core capex beat after pre-scoring HEAT). 09-24 down/mild HIT. 09-23 flat/flat HIT (relative lean unexpressed). 09-22 down/mild MISS (PM gap died at the open). **Governing today: 10-01 (A)** — pending own-spine + mixed inside-band ES + leftover lag that is **not** ≤−5% must not sign S1/S2 −1; 08-27 is not a direction lock once the spine can print expansion; do not attach a relative-underperform lean unless 09-23’s full conjunction is met. **ISM is paid (10-01), not pending.** **09-25 (A)** — do not pre-score a pending own-spine as a miss; NFP is **shared macro**, and it has **already printed** (8:30 ET). **09-22 (A)** — do not mint down/mild from MAP HEAT + lag + a PM quote while ES is mixed; today PM:XLI is **green**, not a smash gap. **09-24 (A)** — |NQ=F| +0.68% is outside the ±0.5% mixed band; derive direction from ES/NQ **sign** (green); a flat/green PM does not veto a signed S0. **09-21 (A)** — “index rallies, my sector doesn’t” is **OFF** (XLI is **on** the PM board at +0.62%, not absent/red). **09-23 (C)** — **OFF** (1m rel **−2.64%**, not ≤−5%; PM is green, not flat-to-slightly-negative). **09-04/09-10** — 1m lag is a **condition**, scored once, not a down print. **08-27** forbid-up **OFF/weak** (1w rel **+0.30%**, not a 1w laggard). **08-18** — cap S1 at 0/+1; GEV/VRT/FIX/RTX **must not** raise or sink the ETF. **08-11/08-12** **OFF** (Channel 1 oil **down**). **09-16** — oil-down ≠ S1 trucking relief. **09-14 S4=−1** **OFF** (no live oil/duration smash; PM is **better**, not worse). **09-11** four-index ≥+0.5% **OFF**. **09-09** emit-down **OFF**. Fed-speaker lesson — Kashkari is **printed 10-01**, not pending; NFP is the live binary and is **paid**. DO-INSTEAD (sector_industrials flatten when score fights tape): **NOT binding** — leading S0 and the sector tape **agree** modestly positive. Open experiment: keep direction, shrink confidence on modest |score| — **applied**. Checklist: (1) shrink-confidence experiment applied; flatten-experiment does not fire; (2) 10-01 miss factor (signing leftover S1/S2 into a pending/just-printed spine) is not repeated; (3) NFP / oil / paid ISM / 1m lag / RTX award each counted once; (4) S0 carries the printed NFP + rates-relief mapping; S1 is unsigned (no fresh ETF-level spine).

## XLI near-session environment (not an SPX call)

Object is the **Oct 2 cash session for XLI**, not SPX and not a stock pick. Channel 1 numbers used as given (tape through **2026-10-01**; do not re-derive). **NFP printed 8:30 ET** (this snapshot is ~8:42 ET Asia/Shanghai 20:42). ISM manufacturing **paid 10-01**. Durable goods **paid 09-25**. Census C30 August **paid 10-01**; September C30 is November.

### 1. Shared macro as it hits Industrials — S0 = +1

This is a **printed-NFP, yields-offered, oil-offered, partial risk-on tape** — not 09-24/09-25’s one-way hawkish smash, not 10-01’s *pending* ISM binary, and not 09-21’s XLK-only melt-up with XLI absent.

- **NFP is paid, and the miss is the session driver.** BLS September employment: **+29,000** vs ~84–90k, unemployment **4.2%** vs 4.1%, AHE **+0.1%** vs ~+0.3%, July–August revisions **−60k**. CNBC: futures **jumped**, yields **slumped**, October hold further cemented. Manufacturing payrolls **+9k** (little changed); construction **+11k** (little changed) — a **labor miss**, not an ISM-contraction print. Count NFP **once, here**. Do **not** pre-score it as a recession smash for XLI while yesterday’s ISM new orders **55.3** / backlog **56.4** still govern the spine.
- **Operator futures vs overnight sleeve — do not re-derive.** Channel 1 `ES=F +0.50% / NQ=F +0.68%` vs prior close. Finviz: S&P **+0.20%**, Nasdaq **+0.41%**, RTY **+0.08%**, DJIA **+0.11%**. Same conflict-handling as 09-15..10-01. Per 09-21/09-24: **direction** from ES/NQ **sign** = **green**; NQ=F is **outside** the 09-22 ±0.5% mixed band; ES=F is **at the edge**. **Breadth** from all four = **narrow** (RTY/DJIA tiny). 09-11 unanimous ≥+0.5% **OFF**. An index rebound is not automatically an XLI certificate — but **PM:XLI +0.62%** is participation, not 09-21 absence.
- **XLI is green on the injected PM board, not the worst cyclical.** Channel 1: **PM:XLI +0.62%** vs XLK +0.78% / XLP +0.60% / XLY +0.39% / XLF +0.25% / XLE −0.99%. That is **~+12 bp vs ES=F**, not a 09-14 smash gap. 09-14 convert-the-gap **does not fire**. 09-15 better-than-index fade **OFF** (no live oil/duration shock; oil is **down**).
- **Oil is DOWN, not a squeeze.** Finviz WTI **$104.16 (−1.59%)**, Brent **$107.67 (−1.02%)**; `CL=F −3.97% / BZ=F −2.48%`. Hormuz remains a **level** (restricted transits / tanker incidents) — 08-11/08-12 **does not fire** on a down tape. Count oil **once, here**. Do **not** treat oil-down as S1 freight recovery (09-16).
- **Rates: level still high, *session change* is relief.** DGS10 **5.29** / DFII10 **2.93** through 09-30 (elevated **level**). Finviz 10Y note **−0.03%**, 30Y **−0.06%**; post-NFP coverage has the 10Y easing toward ~5.23–5.25% after a ~5.34% spike. Real-yield *level* is a condition; the *print-day change* is **PARTIAL** “real yields falling,” not 09-15’s long-end washout and not a recession-scare un-easing. 5-day 10Y–SPX corr **−0.576** (softer than the −0.96 extreme). DXY **−0.09% 1d** — tiny, not an exporter shock.
- **Fed path: October hike fading, not a live speaker binary.** Channel 2: Oct hike odds pulled toward ~25–38% / hold preferred after cooler PCE; Kashkari (10-01) still wants **one more hike this year** but was open-minded on October vs later. **Paid.** Do not restack Warsh/PCE. NFP is the same-morning resolver.
- **Asia/Europe:** Asia composite **−0.4%** (Hang Seng **−2.6%**); Europe in-progress **+0.79%**. VIX **15.95 (−0.44)**, VIX/VIX3M **0.858** contango — not a vol shock.

**S0 mapping for this cyclical:** risk-on tape is **PARTIAL-to-confirmed** on ES/NQ; rates relief is **PARTIAL**; growth-scare from 29k is **real but not spine-confirming** given paid ISM expansion and little-changed mfg/construction jobs. Net **+1**, not +2 (narrow internals, Asia red, 10Y still >5.2% **level**, oil still a high **level**). Not 0: two same-side PARTIALs plus a printed dovish labor miss that the tape is **already treating as risk-on**, with XLI **participating**.

### 2. Sector factors — S1 = 0

**Spine (mandatory):** ISM manufacturing **54.5** / new orders **55.3** / backlog **56.4** is **yesterday’s print** — expansion, not contraction. Do **not** restack as a same-morning +HIT (10-01 hygiene in reverse). No fresh durables/CapEx print today. No ISM contraction. No CapEx-cancellation headline.

**Secondary:**
- **Grid / electrical (AI power):** GEV backlog/structural AI-power demand is **CARRIED**. MAP HEAT Electrical is a **SPLIT down** (VRT unwind vs pending Atkore). **08-18 cuts both ways** — GEV/VRT/FIX **must not** raise the ETF.
- **Aero/defense:** RTX SM-6 multiyear **up to $24.4B** is a real award; BA SPEEA ratification is **paid 10-01**. Rubric: **do not cancel ISM (or drive XLI) with one award.** Single-name, even top-weight, does not sign S1 (10-01 / 08-18).
- **Freight:** Cass August ended the 42-month YoY shipments downturn — **stale** (September Cass ~Oct 13). AAR rail late-Sep still positive YoY. Not a same-morning recovery HIT; 09-16 still blocks oil-down → trucking.
- **Reshoring / industrial policy:** checked, nothing material same-session.
- **Construction:** August C30 **+0.9%** paid 10-01; NFP construction little changed. **Not** a construction-slowdown HIT today.
- **Rotation:** 1d rel **+0.82%** yesterday; PM XLI **+0.62%** vs XLK **+0.78%** — participation, not rotation-out.

Net of spine + secondary **HITs** today: **0**. Cap respected.

### 3. Breadth — S2 = 0

MAP HEAT: Aerospace, Airlines, Building Products, Conglomerates, E&C, Machinery all **dir=down** (low/medium conv); Consulting **up** (best miss vs parent, news does **not** confirm); Electrical **SPLIT**. That nested book is **leftover HEAT**, not a live up-index participation failure. ~38% of S&P industrials above 200-dma is a **condition**.

Live PM: XLI **+0.62%** with XLK/XLP also green — **not** 09-21 non-participation. 10-01: leftover RS is scored **once at 0**, not a same-session −1. Do not mint S2 = −1 from HEAT. Do not mint +1 from one-day ISM captains. **0**.

### 4. Flows / positioning — S3 = 0

Channel 2: XLI ~$30B AUM; ~**−$773M** 1m net outflow vs ~**+$4B** 1y inflow; Sep 30 industrials among least-bought. No same-session inflow/relative-volume spike. Not a crowded long (1m rel still **−2.64%**). **Checked, nothing material** for a same-session S3 sign. Residual 1m outflow is a condition.

### 5. ETF tape (confirmation only) — S4 = 0

Channel 1 (through 10-01, not re-derived):

```
1d: XLI +0.99% | SPY +0.18% | rel +0.82%
3d: XLI -0.08% | SPY -0.21% | rel +0.13%
1w: XLI -0.11% | SPY -0.42% | rel +0.30%
1m: XLI -2.11% | SPY +0.54% | rel -2.64%
```

Short-horizon rel is **non-negative**; 1m still lags but **not** the ≤−5% deep-laggard clause. Not a 4-horizon smash (S4 ≠ −1). Not a confirmation +1 (1m still red; yesterday’s +0.82% is **paid ISM path**, not today’s thesis). Independent same-session tape fact is PM:XLI **+0.62%** — a quote, not the cash open (09-22). **S4 = 0**. Lag scored **once** as a condition, not again here.

### Divergence / self-audit

- **Leading sum (S0..S3) = +1.** S4 = 0. **No fight.** Tape (PM +0.62%, 1d/3d/1w rel ≥ 0) **agrees** with a modest positive S0. Trust factors. DO-INSTEAD flatten **does not fire**.
- **Lens:** XLI session environment, not SPX, not CAT/GEV/RTX/BA.
- **Band:** pending-NFP variance is **resolved at 8:30**; remaining variance is path (gap-and-hold vs trend) around a **mild** sleeve. Four-index not ≥+0.5%; nested HEAT still red → **do not** imply notable. Open experiment: **shrink confidence** on modest |score|.
- **Skew / double-count:** NFP + yields + oil live in **S0 only**. Paid ISM not restacked into S1. RTX/BA/GEV not driving S1. 1m lag not copied into S2 and S4.
- **08-27** does not lock against a non-negative lean: 1w rel is **+0.30%**.
- **Path qualifier:** a large fraction of any up print may already be in the **PM gap** (XLI +0.62% / ES +0.50%). Mild = gap-capture band, not a guaranteed cash-session grind.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 1
S1_SECTOR_FACTORS: 0
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.8
CONFIDENCE: 0.48
REGIME: mixed
DIVERGENCE_FLAGGED: false
HORIZON_3D: +1
HORIZON_1W: 0
HORIZON_2W: 0
HORIZON_1M: 0
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|PARTIAL|0.62|2026-10-02|https://www.cnbc.com/2026/10/02/jobs-report-september-2026.html
Risk-off tape / flight to safety|MISS|0.70|2026-10-02|https://www.cnbc.com/2026/10/02/jobs-report-september-2026.html
Real yields rising|MISS|0.58|2026-10-02|https://www.cnbc.com/2026/10/02/treasury-yields-bonds-nonfarm-payrolls.html
Real yields falling|PARTIAL|0.55|2026-10-02|https://www.cnbc.com/2026/10/02/treasury-yields-bonds-nonfarm-payrolls.html
USD strengthening|MISS|0.50|2026-10-02|channel1
USD weakening|PARTIAL|0.40|2026-10-02|channel1
Sector breadth expansion (% names up)|MISS|0.65|2026-10-02|map_heat
Sector breadth failure (ETF up, names flat)|PARTIAL|0.50|2026-10-02|map_heat
Large-cap leadership inside sector|PARTIAL|0.45|2026-10-02|map_heat
Small/mid leadership inside sector|MISS|0.55|2026-10-02|map_heat
High-beta leadership inside sector|PARTIAL|0.40|2026-10-02|channel1
Low-beta leadership inside sector|MISS|0.45|2026-10-02|channel1
Sector ETF inflow / relative volume spike|MISS|0.50|2026-10-02|https://etfdb.com/etf/XLI/
Sector ETF outflow / volume dry-up|CARRIED|0.45|2026-10-02|https://etfdb.com/etf/XLI/
Crowded long (extreme relative performance + valuation)|MISS|0.70|2026-10-02|channel1
Index rebalance / inclusion tailwind|MISS|0.80|2026-10-02|checked_nothing_material
Index exclusion / forced selling|MISS|0.80|2026-10-02|checked_nothing_material
ISM manufacturing / new orders expansion|CARRIED|0.75|2026-10-01|https://www.prnewswire.com/news-releases/manufacturing-pmi-at-54-5-september-2026-ism-manufacturing-pmi-report-302894520.html
Durable goods / CapEx upside|CARRIED|0.50|2026-09-25|prior_session_paid
Grid / electrical equipment backlog (AI power)|CARRIED|0.55|2026-10-02|https://www.turbomachinerymag.com/view/ge-vernova-gas-turbine-backlog-hits-116-gw-as-power-orders-more-than-double
Aerospace & defense order / budget upside|PARTIAL|0.50|2026-10-02|https://www.business-standard.com/world-news/rtx-recieves-24-4-billion-us-navy-missile-deal-to-boost-sm-6-output-126100200190_1.html
Freight / trucking / rail volume recovery|CARRIED|0.40|2026-09-13|https://www.cassinfo.com/freight-audit-payment/cass-transportation-indexes/august-2026
Reshoring / industrial policy funding|MISS|0.60|2026-10-02|checked_nothing_material
ISM contraction|MISS|0.80|2026-10-01|https://www.prnewswire.com/news-releases/manufacturing-pmi-at-54-5-september-2026-ism-manufacturing-pmi-report-302894520.html
CapEx cuts / order cancellation|MISS|0.70|2026-10-02|checked_nothing_material
Freight recession|MISS|0.55|2026-10-02|https://www.thetrucker.com/trucking-news/business/cass-freight-index-longest-freight-downturn-on-record-as-measured-y-y-ended-in-august
Construction slowdown|MISS|0.55|2026-10-01|https://www.census.gov/construction/c30/current/
Sector rotation into industrials|PARTIAL|0.45|2026-10-02|channel1
Sector rotation out of industrials|MISS|0.55|2026-10-02|channel1
HIT_GRID_END

## RESEARCH APPENDIX

**Queries run**
- US nonfarm payrolls October 2 2026 NFP release date
- ISM manufacturing October 2026 new orders PMI industrials
- XLI ETF flows industrials sector performance October 2026
- GE Vernova grid backlog AI power electrical equipment October 2026
- September 2026 nonfarm payrolls NFP jobs report October 2 2026 actual
- US jobs report September 2026 unemployment rate wages NFP beat miss
- freight trucking rail volume Cass Freight Index September October 2026
- aerospace defense orders RTX Boeing SPEEA October 2026
- XLI premarket October 2 2026 industrials vs SPY breadth
- FedWatch October 2026 hike odds October 2 PCE Kashkari
- stock futures after September 2026 jobs report October 2 2026 yields 10 year
- XLI CAT GE RTX Boeing stock reaction jobs report October 2 2026 *(treated as untrusted for cash-close prints; session is pre-open)*
- construction spending September 2026 Census C30
- oil WTI Brent October 2 2026 Hormuz shipping
- September 2026 jobs report manufacturing employment construction jobs BLS
- US 10 year yield after jobs report October 2 2026
- industrials sector breadth CAT DE UNP ETN GE October 2 2026 premarket
- site:etf.com XLI flows October 2026
- X search: September jobs report NFP 29,000 industrials XLI futures reaction October 2 2026 (from 2026-10-01 to 2026-10-02)
- Fetches: CNBC jobs report; BLS empsit (403); Reuters futures (401)

**Key sources (title + URL + timestamp/facts taken)**

1. **BLS October 2026 release calendar** — https://www.bls.gov/schedule/2026/10_sched_list.htm — Employment Situation **Fri Oct 2, 2026 8:30 a.m. ET**.
2. **CNBC, “Labor market faltered in September…”** — https://www.cnbc.com/2026/10/02/jobs-report-september-2026.html — fetched **2026-10-02T12:43Z**. NFP **+29k** vs DJ **84k**; U-rate **4.2%** vs **4.1%**; Aug **+133k**, July **−10k**, revisions **−60k**; **futures jumped, yields slumped**; October hold cemented.
3. **CoinDesk jobs wrap** — https://www.coindesk.com/markets/2026/10/02/u-s-added-just-29-000-jobs-in-september-with-unemployment-rate-rising-to-4-2 — +29k, U-rate 4.2%.
4. **BLS Employment Situation (via search; direct fetch 403)** — https://www.bls.gov/news.release/empsit.nr0.htm — AHE **+$0.05 / +0.1%** to $37.81, **+3.0% y/y**; manufacturing **+9k**; construction **+11k**; plastics/machinery +5k each.
5. **ThinkMarkets NFP preview** — https://www.thinkmarkets.com/au/market-news/september-nfp-preview-market-impact/ — consensus ~84–90k, U-rate 4.1%, AHE +0.3% m/m.
6. **ISM Manufacturing PMI September 2026 (PR Newswire)** — https://www.prnewswire.com/news-releases/manufacturing-pmi-at-54-5-september-2026-ism-manufacturing-pmi-report-302894520.html — PMI **54.5**, new orders **55.3**, production 56.7, employment 52.7, backlog **56.4**, prices 77.9; released **Oct 1**; October ISM due **Nov 2**.
7. **Census C30** — https://www.census.gov/construction/c30/current/ — August total **$2,203.1B, +0.9%**; private **+1.1%**; September C30 **Nov 2**.
8. **Cass Freight Index August 2026** — https://www.cassinfo.com/freight-audit-payment/cass-transportation-indexes/august-2026 — shipments **1.038, +2.1% y/y** (first y/y gain since Jan 2023); September Cass not out.
9. **The Trucker / Cass** — https://www.thetrucker.com/trucking-news/business/cass-freight-index-longest-freight-downturn-on-record-as-measured-y-y-ended-in-august — 42-month y/y downturn ended.
10. **AAR weekly rail** — https://www.railwayage.com/freight/class-i/aar-u-s-rail-traffic-uptick-continues-in-week-38/ — week ending Sep 26 total traffic **+4.8% y/y**.
11. **RTX SM-6** — https://www.business-standard.com/world-news/rtx-recieves-24-4-billion-us-navy-missile-deal-to-boost-sm-6-output-126100200190_1.html — up to **$24.4B** Navy multiyear.
12. **Boeing SPEEA** — https://www.seattletimes.com/business/boeing-aerospace/boeings-white-collar-union-approves-contract-offer-avoiding-strike/ — ratified **Oct 1**; 10% wage increase effective Oct 2; strike averted.
13. **GE Vernova backlog** — https://www.turbomachinerymag.com/view/ge-vernova-gas-turbine-backlog-hits-116-gw-as-power-orders-more-than-double — ~**$176B** total backlog; gas slots **116 GW**; Electrification/data-center orders elevated. **Structural / not same-morning XLI driver (08-18).**
14. **ETFDB / flow context** — https://etfdb.com/etf/XLI/ — ~**−$773M** 1m outflow, ~**+$3.9B** 1y inflow.
15. **GuruFocus Sep 30 sector flows** — https://www.gurufocus.com/news/9107297/sector-trends-technology-most-bought-industrials-least-bought-on-sept-30 — industrials least-bought Sep 30.
16. **FedWatch / hike odds** — https://www.worldports.org/bets-on-october-fed-rate-hike-wane-after-soft-inflation-data-dovish-comments/ — October hike odds faded post-PCE; hold preferred.
17. **Reuters Kashkari** — https://www.reuters.com/business/feds-kashkari-expects-more-rate-hikes-unsure-need-act-this-month-2026-10-01/ — more hikes expected; **no strong view** on October vs later.
18. **CNBC yields** — https://www.cnbc.com/2026/10/02/treasury-yields-bonds-nonfarm-payrolls.html — 10Y had spiked toward multi-year highs; soft NFP supportive for lower yields.
19. **Safety4Sea / Hormuz** — https://safety4sea.com/fresh-strike-on-tanker-adds-to-hormuz-shipping-turmoil/ — tanker strike ~Oct 1; **level** risk. Channel 1 oil **down** on the day.
20. **Channel 1 (injected, not altered)** — VIX 15.95; ES=F **+0.50%**, NQ=F **+0.68%**; PM:XLI **+0.62%**; CL=F **−3.97%**; DGS10 **5.29**; DFII10 **2.93**; XLI vs SPY 1d/3d/1w/1m rel **+0.82 / +0.13 / +0.30 / −2.64**.

**X search:** no usable contemporaneous posts confirming the print (agent returned pre-release chatter). Relied on CNBC/BLS-cited web results instead.

**Not used:** hdfcsky/beststocks “Oct 2 close” prints (BA +3.35%, cash session not closed at this snapshot); live WTI ~$89 vs Channel 1 **$104.16** (same oil-source conflict as 09-15..10-01 — **Channel 1 numbers trusted**, live **sign** is down).

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 0.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.8, 'leading_sum': 2.0, 'divergence_flagged': False, 'total_score': 6.163, 'predicted_direction': 'flat', 'predicted_magnitude_band': 'flat', 'confidence_score': 0.55, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.5786, 'score': 3.472, 'legs': [{'leg': 'ES', 'pct': 0.5, 'w': 0.8}, {'leg': 'ER2', 'pct': 0.08, 'w': 0.2}, {'leg': 'HG', 'pct': 0.66, 'w': 0.1}, {'leg': 'PM:XLI', 'pct': 0.62, 'w': 0.7}]}, 'overlay_score': 1.6, 'overlay_raw': 1.6, 'index_carry': 1.091, 'general_total': 4.365, 'skill_multipliers': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.5}, 'llm_confidence': 0.48, 'sector_rs_veto_applied': True, 'sector_rs_tape': {'d1': -1.65, 'w1': -2.74}, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
