# Sector Prediction — Industrials — 2026-09-03

- ETF: **XLI**
- rubric: `00_grounding/sectors/industrials.md`
- predicted_direction: **flat**
- predicted_magnitude_band: **flat**
- total_score: **0.0** (mult 0.9)
- regime: mixed
- divergence_flagged: **False**

## Channel 1 sector ETF tape

```
ETF XLI vs SPY (yfinance, through 2026-09-02):
  1d: XLI +0.03% | SPY +0.44% | rel -0.41%
  3d: XLI -3.37% | SPY -0.77% | rel -2.60%
  1w: XLI -3.15% | SPY -0.10% | rel -3.05%
  1m: XLI -5.67% | SPY +0.99% | rel -6.66%
```

MEMORY_CONFIRM: Memory index unavailable this run (embedding metadata mismatch; `openclaw memory status --index` or `openclaw memory index --force` would rebuild). Used injected Industrials scoreboard + 08-11..09-02 sector logs only. Rolling dir=0.2 / mag=0.1 (n=10); last 30 dir=0.25 / mag=0.083 (n=12). Last graded 08-28: narrative down/mild vs pipeline down/flat, actual XLI −0.93% (dir HIT, mag MISS on pipeline flat). 08-31 / 09-01 / 09-02 still ungraded. Governing today: 08-11/08-12 supply-shock cap **does not fire** (live WTI/Brent **down**, not a fresh Hormuz squeeze); 08-13 **does** fire as a cap (oil down on demand/risk while the old supply headline remains → S0 ≤ 0, do not treat Hormuz as live-up); 08-18 (do not hold S1 at +2 on stale ISM; do not use GEV/ETN as a cushion; reconcile narrative vs components); 08-21 reversal **off** (ES +0.13%, NQ +0.16% — not ≥ +0.3%); 08-25 needs a *fresh* single-name smash plus all-timeframe lag to *force* down:mild — HON/HONA $2M FCA is not that, SPEEA is stale, CAT/GEV/BA are not breaking premarket; 08-27 (1w/1m laggard + already-public **non-holdings** mega-cap AHR → **forbid up**, cap S1 at 0/+1, prefer **flat or down:mild**; AVGO printed 09-02 AHR, NQ is **not** independently leading ES); 08-28 Industrials (emit narrative band; do **not** import XLF leftover-S2/S4 down-bans as a *new* factor lesson) **and** 09-02’s own residual rule (do **not** restack a completed 09-01 smash into S2+S3+S4 when S0=S1=0 and ES is flat). DO-INSTEAD: when score fights tape, cut conviction / prefer flat/mild. Open experiment: keep direction, shrink confidence on modest |score| — **applied** (Σ ≈ 0). Calendar: **8:30 ET claims / trade / productivity** and **10:00 ET ISM Services** are live two-sided — do **not** write “no macro print.”

## XLI near-session environment (not an SPX call)

Object is the **Sep 3 cash session for XLI**, not SPX and not a stock pick. Channel 1 numbers are used as given.

### 1. Shared macro as it hits Industrials — S0 = 0
This is a **mixed/pause tape**, not 09-01’s confirmed cyclical risk-off and not an 08-21 bounce.

- **Futures do not independently confirm.** Channel 1: ES **+0.13%**, NQ **+0.16%**. Finviz cash futures SPX **+0.16%** / NQ **+0.24%** / DJIA **+0.26%** / RTY **−0.00%**. News Judge’s “pre-market futures lower” is **overridden by Channel 1** (do not re-derive). 08-21’s ES/NQ ≥ +0.3% bounce gate is **off**. 08-10-style flat-futures rule: do not manufacture mild-down (or up) off a moderate headline stack without futures confirmation.
- **Oil: live-verify, session change is down.** Channel 1 `CL=F −0.26% / BZ=F −0.28%`; Finviz WTI **−1.19%**, Brent **−1.07%**. Absolute level is still elevated (~$90 / ~$95) — a **cost headwind for transports/manufacturers**, not a same-session squeeze. 08-11/08-12 does **not** fire. 08-13: old Hormuz headline is the stale leg; oil-down is demand/risk, **not** a cyclical tailwind. Count oil **once**, here.
- **AVGO is XLK, not XLI beta.** Broadcom printed **09-02 AHR** (EPS $3.32 / rev $29.6B, raise, doubled 2027–28 AI guide). 08-27 family: do **not** map a non-holdings mega-cap AHR into XLI S0 = +1. mega-cap-earnings-over-macro-drag is an **index** down-forbid, not an XLI up-license. NQ is not ≥ +0.5% vs ES.
- **Rates: do not score the 09-01 FRED 1d column as live.** DGS10 4.79 / DGS30 5.27 / DFII10 2.44 are **prior-close levels** (30Y still in the stress zone; 1w real yield **+12 bp**). Live Finviz: 10Y note **+0.17%**, 30Y **+0.38%** → **yields easing at the open**, not a second-day backup. Warsh/hike-odds (~60–70% September hike in secondary tape) are **already paid** (08-28 → 09-01). Do not double-count. 5-day 10Y–SPX corr **−0.795** is a duration overlay, secondary to ISM/CapEx for this book.
- **Globals are flat, not a crash.** Asia composite **+0.04%**, Europe **+0.19%**. VIX **15.18**, VIX/VIX3M **0.999**, HY OAS **2.65** (tight). USD **−0.31%/−0.35%** — mild, not an exporter regime shift.
- **Calendar is two-sided, not scored until print.** 8:30 ET: initial claims (consensus **~205k** vs prior **203k**), goods/services trade, Q2 productivity revision. 10:00 ET: **ISM Services** (consensus **~54.2–54.3** vs July **54.1**). Encode as event risk in S0. Do **not** pre-score a miss or a beat. ISM Services is **not** the manufacturing spine.

**S0 = 0, regime mixed.** Not −1: futures are not ≤ −0.5%, oil is not independently spiking, 09-01 kinetic increment is not live. Not +1: NQ is not leading, 1w/1m XLI is a laggard, AVGO is foreign beta. 08-13 keeps this ≤ 0.

### 2. Spine + secondary — S1 = 0 (capped)
**No fresh same-morning industrials print in hand.** August **ISM manufacturing already printed 09-01**: PMI **54.6** vs ~55.2 / July 55.6; new orders **53.7** (−3.0 pts from 56.7). Still **expansion** (8th month) — **not** an ISM-contraction HIT. New orders cooled; prices **71.1** still hot. XLI **already paid** this (09-01 −1.37% / rel −0.68%). 08-18/08-27: **cap S1 at 0/+1**; +2 is forbidden without same-morning confirmation.

- **Durable goods / CapEx:** July durables **+1.1%** / core **+0.2%** (08-26). Factory orders July **+0.9%** already out. Not a same-morning HIT. August durables are mid-month.
- **Grid / electrical equipment backlog (AI power) — HIT, carried.** GEV ~**$176B** RPO / **116 GW** gas book / DC equipment orders **>$5B YTD**; ETN electrical/DC backlog still structural. Premarket GEV **~+0.32%**. 08-18: **not** a downside cushion and **not** a same-session raise. Goolsbee “dangerous AI spending” is an XLK-multiple dampener, not a GEV order cancellation.
- **Aerospace & defense — MIXED / stale.** MAP HEAT residual is **up vs parent** (GE/RTX/MOG-A/VSEC Navy/MRO color, low conviction) — honor as a **sleeve**, do **not** let it drive the ETF call. F-15 **ceiling** already faded. SPEEA talks **Sep 8** / earliest strike **Oct 6**. HONA **$2.04M** cyber-FCA settlement (09-01) is **not** an XLI spine (spun-off name, tiny dollars). Do **not** cancel ISM with one award.
- **Freight — MIXED, lean negative on rails.** AAR rail/intermodal still expanding on lagged prints; Cass July shipments **−4.8% y/y** (trucking volume recession). MAP HEAT **Railroads dir=down** (UP–NS STB procedural calendar, notices ~Sep 4). Not a same-morning recovery HIT.
- **Construction slowdown — HIT, already printed 09-01.** July SAAR **−0.5% m/m**, **−3.8% y/y**; residential weaker, private nonres the offset. AI/nonres is **not** a broad build boom.
- **CAT FieldAI (09-02)** is **already traded** (CAT **+1.68%** to $792.28). Classify as **stale-positive / in the tape**, not a fresh S1 driver (catalyst-freshness rule).
- **AME Indicor $5.0B close** is **stale M&A** (08-26).

Net: slowing-but-still-expansion ISM vs construction drag vs mixed freight vs carried grid, **no fresh same-morning confirmation**. **S1 = 0.**

### 3. Breadth — S2 = 0
XLI remains a **1w/1m laggard** (1w rel **−3.05%**, 1m rel **−6.66%**), but that lag **already printed**. Premarket XLI **~$173.00 (+0.13%)** vs prior **$172.78**; GEV modestly green; CAT already paid FieldAI. **No live CAT/GEV/BA breakdown.** MAP HEAT: most industrial sleeves quiet-down vs parent; A&D residual up; rails down — **do not average nested OVERRIDE/SPLIT into XLI**, and **size_gate** kills MATX/XRX as ETF drivers. 09-02 / leftover rule: with S0=S1=0, do **not** copy the completed 09-01 smash into S2. **S2 = 0** unless a live premarket mega-name break — it is not.

### 4. Flows — S3 = 0
ETFdb-style tape: **5-day ~−$521M** vs **1m still ~+$111M**. Not a crowded long (1m rel **−6.66%**). Trailing unit outflows are **not** a 1-day lid. Rotation **out of industrials** is the multi-week tape, not a same-morning inflow spike. Do not triple-count the fade. **S3 = 0.**

### 5. ETF tape (confirmation only) — S4 = 0
Channel 1 through **09-02**: 1d XLI **+0.03%** vs SPY **+0.44%** (rel **−0.41%**); 3d rel **−2.60%**; 1w **−3.05%**; 1m **−6.66%**. S4 confirms **this** session, not the prior close (08-28 XLB / 09-02 Industrials). Yesterday’s modest lag is **history**. Premarket XLI is **not** confirming a fresh smash or a fresh leadership day. **S4 = 0.** Relative lag **caps any up impulse**; it does not, by itself, sign today down.

### 6. Earnings / policy
No index-wide industrials earnings cluster at the open. AVGO is **XLK**. CAT FieldAI is **T+1**. Claims + ISM Services are **two-sided until they print**. Do not pre-score hawkish Warsh again. Do not cancel ISM expansion with Golden Dome / one defense award.

### Self-audit
- **Lens:** cyclical; rates/oil only in S0, not re-counted in S1.
- **Band:** **flat**, not notable (ES/NQ inside ±0.5%, no fresh hard-data miss, mag accuracy 0.1). 08-27 **forbids up**. 08-25 does **not** force down:mild (no fresh single-name smash). Residual after 09-01 smash + 09-02 pause = **flat**.
- **Skew:** CAT/GEV/BA/HON/UNP do **not** drive the ETF call. Nested A&D heat is a sleeve, not the book.
- **Same-shock:** AVGO/tech counted as **non-transmission**, not as S0 = +1. Oil counted **once** in S0. ISM Aug counted **once** (already paid), not again as today’s binary.
- **Single-ticker:** FieldAI, HONA FCA, UP–NS STB, AME close — none dominate XLI.
- **Divergence:** leading S0–S3 = **0** vs S4 = **0**. **No divergence.** Factors and tape agree on a pause. Do not invent down from leftover S2/S4.
- **Reconcile:** narrative **flat/flat**; Σ(S0..S4)×mult = **0 × 0.9 = 0**. Do **not** let the pipeline rewrite this to up (08-27) or to down/flat from leftover tape (09-02). Pending 8:30/10:00 keeps **confidence shrunk**, not a signed tilt.

**Near-session call (components only; pipeline owns the official band):** absolute **flat**, relative **lag vs SPY still the base case**. 3d/1w stay **down:mild** on the unpaid laggard + hawkish path + elevated oil **level**, not on today’s open tape.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: 0
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.9
CONFIDENCE: 0.52
REGIME: mixed
HORIZON_3D: down:mild:0.55
HORIZON_1W: down:mild:0.52
HORIZON_2W: flat:mild:0.45
HORIZON_1M: down:mild:0.48
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|MISS|0.62|2026-09-03|channel1 ES +0.13% / NQ +0.16% inside ±0.5%
Risk-off tape / flight to safety|MISS|0.70|2026-09-03|VIX 15.18, HY OAS 2.65, oil down not spiking
Real yields rising|MISS|0.58|2026-09-03|https://fred.stlouisfed.org — DFII10 2.44 1d +0.0; live 10Y/30Y notes up (yields easing)
Real yields falling|MIXED|0.55|2026-09-03|open notes bid vs 1w DFII10 +0.12 and 30Y 5.27 stress zone
USD strengthening|MISS|0.72|2026-09-03|DXY 1d -0.35%; Finviz USD -0.31%
USD weakening|HIT|0.60|2026-09-03|mild DXY dip; not an exporter regime for XLI
Sector breadth expansion (% names up)|MISS|0.68|2026-09-03|1w/1m XLI rel -3.05%/-6.66%; MAP HEAT sleeves mostly quiet-down
Sector breadth failure (ETF up, names flat)|MISS|0.55|2026-09-03|XLI 1d +0.03% not an ETF-up/names-flat day
Large-cap leadership inside sector|HIT|0.58|2026-09-02|https://www.caterpillar.com/en/news/corporate-press-releases/h/caterpillar-and-fieldai-advance-ai-powered-industrial-innovation.html — CAT +1.68% on FieldAI; mega-name carry not % expansion
Small/mid leadership inside sector|MISS|0.60|2026-09-03|no evidence
High-beta leadership inside sector|MISS|0.62|2026-09-03|RTY -0.00%; not a high-beta industrial bid
Low-beta leadership inside sector|MISS|0.50|2026-09-03|checked, nothing material
Sector ETF inflow / relative volume spike|MISS|0.64|2026-09-03|https://etfdb.com/etf/XLI/ — 5d ~-$521M
Sector ETF outflow / volume dry-up|MIXED|0.58|2026-09-03|https://etfdb.com/etf/XLI/ — 5d outflow vs 1m still ~+$111M; not a 1-day lid
Crowded long (extreme relative performance + valuation)|MISS|0.75|2026-09-03|1m rel -6.66% is the opposite of crowded
Index rebalance / inclusion tailwind|MISS|0.80|2026-09-03|checked, nothing material
Index exclusion / forced selling|MISS|0.80|2026-09-03|checked, nothing material
ISM manufacturing / new orders expansion|HIT|0.80|2026-09-01|https://www.ismworld.org/supply-management-news-and-reports/reports/ism-pmi-reports/pmi/august/ — 54.6 / NO 53.7 still >50, slowing, already in 09-01 tape
Durable goods / CapEx upside|MIXED|0.70|2026-08-26|https://www.census.gov/manufacturing/m3/adv/pdf/durgd.pdf — July +1.1% / core +0.2%; not same-morning
Grid / electrical equipment backlog (AI power)|HIT|0.78|2026-09-03|https://www.turbomachinerymag.com/view/ge-vernova-gas-turbine-backlog-hits-116-gw-as-power-orders-more-than-double — GEV 116 GW / ~$176B RPO carried
Aerospace & defense order / budget upside|MIXED|0.55|2026-09-03|MAP HEAT A&D residual up vs parent, low conv; F-15 ceiling stale; do not cancel ISM
Freight / trucking / rail volume recovery|MISS|0.66|2026-09-03|Cass July shipments -4.8% y/y; rail HEAT down on UP-NS STB
Reshoring / industrial policy funding|MIXED|0.50|2026-09-03|checked, nothing material same-morning
ISM contraction|MISS|0.82|2026-09-01|https://www.ismworld.org/supply-management-news-and-reports/reports/ism-pmi-reports/pmi/august/ — 54.6 still expansion
CapEx cuts / order cancellation|MISS|0.60|2026-09-03|checked, nothing material
Freight recession|HIT|0.62|2026-09-03|trucking volume still the soft leg (Cass); rail not a same-morning recovery
Construction slowdown|HIT|0.74|2026-09-01|Census July -0.5% m/m / -3.8% y/y already paid
Sector rotation into industrials|MISS|0.70|2026-09-03|1w/1m rel deeply negative
Sector rotation out of industrials|HIT|0.68|2026-09-03|Channel 1 1w rel -3.05% / 1m -6.66%; 5d XLI outflow — scored as context, not a second S2/S4 vote
HIT_GRID_END

## RESEARCH APPENDIX

**Queries run**
- US economic calendar September 3 2026 ISM jobless claims
- XLI industrials ETF premarket CAT GE Vernova Boeing Honeywell September 3 2026
- ISM manufacturing August 2026 new orders durable goods factory orders
- WTI Brent oil price Hormuz Iran September 3 2026
- XLI ETF flows Caterpillar GE Vernova Boeing Honeywell news September 2026
- ISM Services PMI August 2026 consensus forecast September 3
- Union Pacific Norfolk Southern STB antitrust railroad September 2026
- FedWatch September 2026 hike odds Warsh Goolsbee
- GE Vernova Eaton grid backlog AI power September 2026
- Honeywell HON cyber FCA September 2026
- initial jobless claims week ending August 29 2026 consensus
- Caterpillar FieldAI collaboration September 2 2026 stock reaction
- Broadcom earnings date August September 2026 AVGO report
- X search: XLI industrials CAT GEV Boeing premarket September 3 2026 (2026-09-02 to 2026-09-03)
- web_fetch https://etfdb.com/etf/XLI/ (403 / blocked)

**Key sources (title + URL + facts taken)**
- ISM August Manufacturing PMI — https://www.ismworld.org/supply-management-news-and-reports/reports/ism-pmi-reports/pmi/august/ — PMI 54.6, new orders 53.7, prices 71.1, 8th expansion month; released ~Sep 1.
- Census durable goods (July) — https://www.census.gov/manufacturing/m3/adv/pdf/durgd.pdf — new orders +1.1% to $339.3B.
- Trading Economics factory orders — https://tradingeconomics.com/united-states/factory-orders — July +0.9%.
- Value Line / Myfxbook / Scotiabank calendars — 8:30 claims (~205k) + trade + productivity; 10:00 ISM Services Aug 2026. ISM manufacturing already out Sep 1.
- MarketWatch XLI — https://www.marketwatch.com/investing/fund/xli — premarket ~$173.00 +0.13% vs prior close $172.78.
- Public.com GEV premarket — ~$924.90 +0.32%.
- Caterpillar FieldAI PR — https://www.caterpillar.com/en/news/corporate-press-releases/h/caterpillar-and-fieldai-advance-ai-powered-industrial-innovation.html — Sep 2 collab; CAT +1.68% to $792.28 that session.
- Turbomachinery International GEV — https://www.turbomachinerymag.com/view/ge-vernova-gas-turbine-backlog-hits-116-gw-as-power-orders-more-than-double — 116 GW gas book, power orders more than doubled, DC equipment >$5B YTD.
- ETFDB XLI — https://etfdb.com/etf/XLI/ — 5d ~−$521M, 1m ~+$111M (page fetch 403; figures from search extract).
- DOJ HONA FCA — https://www.justice.gov/opa/pr/honeywell-aerospace-inc-agrees-pay-over-2m-settle-false-claims-act-allegations-failing — $2.04M cyber-FCA, not an XLI driver.
- Broadcom IR — https://investors.broadcom.com/news-releases/news-release-details/broadcom-inc-announce-third-quarter-fiscal-year-2026-financial — Q3 FY26 reported **Sep 2 AHR**; EPS $3.32, rev ~$29.6B.
- WSJ FedWatch card — https://www.wsj.com/livecoverage/stock-market-today-dow-sp-500-nasdaq-09-02-2026/card/odds-of-fed-rate-hike-in-september-hit-70--PQ04igZVrpQ5Hy4h87rp — September hike odds cited ~60–70% post-Warsh (secondary; Channel 1 FedWatch unscrapeable).
- FreightWaves / STB UP–NS — merger review advancing; notices ~early Sep; not a same-morning volume recovery.
- Channel 1 (injected, not altered): ES +0.13%, NQ +0.16%, CL/BZ down, DXY −0.35%, XLI vs SPY tape through 09-02 as printed.

**Not used as open-known:** one ISM Services search snippet claimed an already-released **51.5** print. At ~05:45 ET that is **not knowable**; 10:00 ET ISM Services stays **two-sided / unscored**.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': 0.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.9, 'leading_sum': 0.0, 'divergence_flagged': False, 'total_score': 0.0, 'predicted_direction': 'flat', 'predicted_magnitude_band': 'flat', 'confidence_score': 0.52, 'regime': 'mixed', 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'high-impact Finviz economic calendar'}
```
