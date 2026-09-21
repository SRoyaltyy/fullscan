# Sector Prediction — Real Estate — 2026-09-21

- ETF: **XLRE**
- rubric: `00_grounding/sectors/real_estate.md`
- predicted_direction: **flat**
- predicted_magnitude_band: **flat**
- total_score: **3.17** (mult 0.9)
- regime: mixed
- divergence_flagged: **True**
- engine: v2 · tape_anchor **0.965** (ES +1.35%, ZN -0.03%, PM:XLRE -0.05%) · index_carry **3.218** (general 12.871) · llm_overlay **-1.012** (raw -1.012)

## Channel 1 sector ETF tape

```
ETF XLRE vs SPY (yfinance, through 2026-09-18):
  1d: XLRE -0.95% | SPY +0.13% | rel -1.08%
  3d: XLRE -1.25% | SPY +0.82% | rel -2.07%
  1w: XLRE -2.05% | SPY -0.09% | rel -1.96%
  1m: XLRE -5.47% | SPY -0.71% | rel -4.76%
```

MEMORY_CONFIRM: Memory index unavailable this run (embedding metadata mismatch) — used injected Real Estate scoreboard, standing REIT lessons, last-10 XLRE logs, and mutable policy only. Last graded: 2026-09-18 flat/flat vs XLRE −0.955% / SPY −0.119% / rel −0.835% (dir MISS, mag MISS — T+2 unsigned card vs same-session 10Y +5–7 bp through 5%). 2026-09-17 flat/flat vs +0.304% / rel −0.830% (dir MISS, mag MISS — faded gap). 2026-09-16 flat/flat vs −0.604% (dir MISS, mag MISS — unprinted FOMC+SEP/presser). Rolling dir=0.4 mag=0.2 (n=10); last-30 dir=0.478 mag=0.304 (n=23). Open `sector_real_estate` experiment **applies**: 09-16/09-17/09-18 losses → when score sign conflicts with tape/breadth, cut conviction, prefer flat/mild; shrink confidence on modest |score|. Applied: **(1) 08-27** — 08-25 is a ban on forcing down, not an up license; 30Y in stress zone ⇒ cap S0/S1 at 0; do not double-count one rate/oil object; do not pad S1 with always-on DC/industrial; green NQ ≠ REIT duration relief. **(2) 08-25** — live curve independently verified (CNBC 10Y **4.967%, −3 bp**; 30Y **5.306%, −3 bp**); do not force down off the 09-17 FRED 4.94/5.29 table. **(3) 08-21** — live 30Y **~5.31** still the multi-decade stress zone; a 3 bp tick is not relief. **(4) 08-17/08-18** smash **OFF** (no live long-end rip). **(5) 08-11** spike **OFF** (Channel 1 WTI **−1.59%** / Brent **−1.02%**, CL=F **−5.94%** — oil slide; News Judge leftover Warsh/gold/Dow-week is T+printed, not a fresh Hormuz spike). **(6) 08-12** — FOMC+SEP+presser is **printed (T+3)**; do not one-way re-score. October-hike ~45–57% is the same already-printed path, not a second shock. **(7) 09-04** asymmetric-downside **does not fully fire** — needs an unresolved pre-binary backup or a live *rising* curve; today’s open is −3 bp after the path already printed into Wed/Fri tape. **(8) 09-08** cushion **does NOT fire** (1d rel **−1.08%**, not ≥ +0.4%). **(9) 09-11** **does fire on the no-force-down branch** — S0 genuinely mixed, oil offered, live curve not rising; Finviz ES **+0.20%** is **not** the ≥ +0.5% green-futures branch; leftover ES=F **+1.35%** vs Friday close is not a second tape; stale 1w/1m lag must not be scored into S2 **and** S4; already-priced hike/SEP = 0. **(10) 09-14** — XLRE premarket **−0.05%** is unconfirmed in this ETF; may not set sign or offset S0. **(11) 09-15** flatten-mag / de-correlate is for a *telegraphed live level smash* with sub-gate **green** rel — not this open (1d rel already red; no live 5% smash). **(12) 09-16** mag-expansion **does not fire** — that trigger is an *unprinted same-session* FOMC+SEP+presser; today’s binary already printed. **(13) 09-17** keep-flatten — do not promote leftover ES=F **+1.35%** into up. **(14) 09-18** joint down-gate **does NOT fire** — that needs 1d rel ≲ −0.5% **and** a live 10Y *failed round-number hold* (open only flat/1 bp). Hard lag is present (**−1.08%**); the open curve is **easing −3 bp**, so this is the 09-17 easing branch, not Friday’s failed-hold. **(15) 08-14** reconcile Σ×mult.

## Real Estate (XLRE) — 2026-09-21

### Channel 1 (used as given, not re-derived)

Rates through **2026-09-17**: DGS10 **4.94** (1d **−0.07** / 1w **−0.01** / 1m **+0.23**), DGS30 **5.29** (1d **−0.06** / 1w **−0.08** / 1m **+0.01**), DFII10 **2.61** (1d **−0.07** / 1w **+0.06** / 1m **+0.20**) — **real yields still up on 1w/1m**. That 1d column is **Thursday’s close**, not this open. VIX **14.98** (1d **+0.17**, 1w **−2.12**), VIX/VIX3M **0.821 — contango**. Finviz: ES **+0.20%**, NQ **+0.41%**, Russell **+0.08%**, DJIA **+0.11%**. Channel 1 also prints **ES=F +1.35% / NQ=F +2.12% vs prev close** — weekend gap from Friday’s cash close, **not** a second independent tape (same treatment as 09-16/09-17/09-18). Asia **+1.04%**, Europe **+0.95%**. **Oil DOWN**: Finviz WTI **$104.16 (−1.59%)**, Brent **$107.67 (−1.02%)**; CL=F **−5.94%** / BZ=F **−5.76%** — slide, still >$100 as a *level*. Gold mixed (Finviz **+0.90%**, GC=F **−0.77%**). DXY Finviz **−0.02%** / 1d **+0.08%**. **10Y note −0.03%**, **30Y bond −0.06%**, Ultra Bond **−0.06%** (Finviz prices slightly down = **yields ~flat-to-+1 bp on that board**). HY OAS **2.70**. 5-day 10Y–SPX corr **−0.592**. EPU **342.15** (1d **+163**).

**Sector premarket vs prev close: XLRE −0.05%** — flat and non-participating (XLK **+0.98%**, XLC **+0.59%**; XLE **−1.29%**, XLP **−0.65%**, XLU **−0.63%**). **Not** a 09-14-style second-best defensive rotation bid. Per 09-14 this print is **unconfirmed** and is scored **0** — it does not set sign and is not an S0 offset. Index-sleeve green with |PM:XLRE| ≤ 0.1% is **not** a participation certificate.

XLRE vs SPY through **2026-09-18**: 1d **−0.95 / +0.13 / rel −1.08**; 3d rel **−2.07**; 1w rel **−1.96**; 1m rel **−4.76**. **Every horizon is a relative laggard.** No defensive cushion (09-08 override does NOT fire). Confirmation mix, not duration relief.

MAP HEAT: **split, not a parent vote** — Hotel / Residential nested **up**; Office / Mortgage / Specialty (EQIX) nested **down**; Industrial / Diversified / Healthcare / Retail **flat**. `size_gate=True`. Do not let WELL, EQIX, PLD, or BXP define XLRE.

### Channel 2

**1. Shared macro as it hits REITs.** This is a **Monday risk-on / oil-slide / post-FOMC digestion tape**, not a rates-backup session and not a flight-to-safety bid into REITs. FOMC **already printed** 2026-09-16: unanimous **+25 bp to 3.75–4.00%**, hawkish Warsh presser, extra-2026 dots. That object is **in Wednesday’s and Friday’s XLRE prints** (T+3). 09-11: already-priced = **0**; do not restack. Live CNBC (fetched 2026-09-21): 10Y **4.967% (−3 bp)** after last week’s **19-year high 5.041%**; 2Y **4.729% (−1 bp)**; 30Y **5.306% (−3 bp)**. That is **stabilization off a round-number high, not relief** (08-21) and **not a smash** (08-18 OFF). 30Y still **≥5.15% stress**. 08-25 verifies the dip; it does **not** authorize up. Oil is **offered** (WTI −1.59%, CL −5.94%) — 08-11 spike OFF; oil and the modest yield dip are the **same duration channel** — count once, do not book the slide as S1 duration relief. NQ/XLK leading is **tech beta**, not REIT duration relief (08-27). MACRO MAP: equity risk-on often **leaves REITs lagging** (funding source). Finviz ES **+0.20%** is not the 09-11 ≥ +0.5% branch; leftover ES=F **+1.35%** is capped for this sector because PM:XLRE is flat and non-haven. No pending CPI/NFP/FOMC binary today (PMI Wed / claims Thu). **S0 = 0.**

**2. Spine (count the rate object once; S0 is the map, not a second copy).**
- Rates falling / REIT duration relief: **MISS.** Live −3 bp is not relief while 30Y ~5.31% sits in the stress zone (08-21).
- Rates rising / REIT selloff: **MISS at the open.** Curve is easing, not ripping. Friday’s failed 5% hold is **already in Friday’s −0.95% print** — do not restack (09-17). 09-18’s joint gate needs a *live* failed hold; that is absent.
- Real yields rising: **structural, not a same-morning HIT.** DFII10 +6 bp 1w / +20 bp 1m; live 10Y −3 bp. Same duration channel as the nominal dip — **not** a second independent shock.

**3. Secondary.**
- Data-center REIT demand / rent upside: **HIT, stale / nested down.** DLR Q2 backlog / EQIX $5–7B AI build are already in. MAP HEAT Specialty **down** (EQIX neg). **08-27: not a same-day up vote.** EQIX must not define XLRE.
- Industrial REIT occupancy / rent growth: **HIT, stale.** PLD Q2 occupancy 95.5%, cash rent +22.3% — MAP HEAT Industrial **flat**. Not a same-day up vote.
- Refinancing window / cap-rate compression: **MISS.** 30Y ~5.31%; CRE maturity wall ~$875B still a 2026 reset, not a window.
- Office vacancy / mark-to-market: **HIT, small sleeve / nested down.** National office vacancy ~17.8%; MAP HEAT Office **down**. Office is not the XLRE parent.
- Refinancing wall stress: **HIT, structural.** Not a same-morning print.
- Cap-rate expansion: **PARTIAL/structural** (no compression at 5.3% 30Y). Not a same-day shock.
- Sector rotation into REITs: **MISS.**
- Sector rotation out of real estate: **HIT, live as a cross-sector board, not as the XLRE tick.** Defensives offered (XLP/XLU/XLRE) vs XLK **+0.98%**. Score **once** in S1. Do not also use it as an S0 offset. Do not let the XLRE **−0.05%** PM tick set sign (09-14).

**4. Breadth / leadership.** MAP HEAT is **split** (Hotel/Residential up vs Office/Mortgage/Specialty down). Nested OVERRIDE/SPLIT beats the parent — do not average into XLRE. ~3% of REIT names above 20d SMA is a **structural** weakness print through Friday, not a same-morning breadth smash. 09-11: do **not** dump 1w/1m lag into S2 **and** S4. **S2 = 0.**

**5. Flows / positioning.** XLRE 5d/1m net outflows (order **−$136M / −$328M** in recent snapshots); 1y flows negative. Not a crowded long (1m rel **−4.76%** is the opposite). Outflows are near-term demand, not a washout buy. Modest **S3 = −0.5**. Engine already half-weights S3.

**6. Earnings / policy catalysts.** No REIT earnings print this morning. FOMC paid. October hike ~50/50 is path, not a 1d object. Williams/Barkin speeches this week are two-sided and unprinted — do not one-way S0.

### Self-audit
- **Lens:** XLRE duration + funding-source rotation, not SPX.
- **Band:** modest |leading|; rolling mag **0.2** → shrink confidence; size_gate on.
- **Skew:** risk-on leaves REITs lagging on a *relative* basis; absolute can still be flat if the curve eases 3 bp.
- **Same-shock double-count:** oil slide + 10Y −3 bp counted **once** as “not relief” (S0=0), not as positive S1.
- **Single-ticker:** WELL / EQIX / PLD / BXP barred from the parent call. MAP HEAT split respected.
- **09-18 vs 09-17:** hard 1d lag is present; live failed-hold is **not**. Do not force down off Friday’s print. Do not force up off leftover ES.
- **Divergence:** leading S0–S3 = **−1.0**; S4 = **−1**. They **agree**. No leading-vs-tape fight. Trust factors; S4 is confirmation only.

### Horizons
- **HORIZON_3D:** Mixed absolute, residual **relative lag** vs SPY if NQ/XLK leadership holds. 30Y still ≥5.15% caps any duration bounce. Mild band.
- **HORIZON_1W:** Duration still the boss. PMI/claims + Fed speakers; October hike odds ~50/50. Structural 1m rel **−4.76%** decays slowly. Mild, two-sided.
- **HORIZON_2W:** Into late-September / early-October path. No refinancing-window open while 30Y ~5.3%.
- **HORIZON_1M:** Real yields **+20 bp 1m**, CRE maturity wall, office/mortgage nested down vs residential/hotel nested up — parent XLRE stays a **rate-constrained laggard**, not a catch-up compounder.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: -0.5
S2_BREADTH: 0
S3_FLOWS_POSITIONING: -0.5
S4_ETF_TAPE: -1
MULTIPLIER: 0.9
CONFIDENCE: 0.48
REGIME: mixed
DIVERGENCE: 0
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|HIT|0.72|2026-09-21|https://finance.yahoo.com/markets/live/stock-market-today-monday-september-21-dow-sp-500-nasdaq-080214605.html
Risk-off tape / flight to safety|MISS|0.70|2026-09-21|https://www.cnbc.com/2026/09/21/treasury-yields-government-bonds.html
Real yields rising|PARTIAL|0.55|2026-09-17|https://fred.stlouisfed.org/series/DFII10
Real yields falling|MISS|0.60|2026-09-21|https://www.cnbc.com/2026/09/21/treasury-yields-government-bonds.html
USD strengthening|MISS|0.55|2026-09-21|channel1
USD weakening|MISS|0.55|2026-09-21|channel1
Sector breadth expansion (% names up)|MISS|0.65|2026-09-18|https://breadthmarket.com/
Sector breadth failure (ETF up, names flat)|MISS|0.60|2026-09-18|channel1
Large-cap leadership inside sector|PARTIAL|0.50|2026-09-21|map_heat
Small/mid leadership inside sector|MISS|0.50|2026-09-21|map_heat
High-beta leadership inside sector|MISS|0.55|2026-09-21|map_heat
Low-beta leadership inside sector|PARTIAL|0.50|2026-09-21|map_heat
Sector ETF inflow / relative volume spike|MISS|0.62|2026-09-21|https://etfdb.com/etf/XLRE/
Sector ETF outflow / volume dry-up|HIT|0.58|2026-09-21|https://www.trefis.com/data/etfs/XLRE
Crowded long (extreme relative performance + valuation)|MISS|0.70|2026-09-18|channel1
Index rebalance / inclusion tailwind|MISS|0.40|2026-09-21|checked, nothing material
Index exclusion / forced selling|MISS|0.40|2026-09-21|checked, nothing material
Rates falling / REIT duration relief|MISS|0.75|2026-09-21|https://www.cnbc.com/2026/09/21/treasury-yields-government-bonds.html
Data-center REIT demand / rent upside|STALE|0.60|2026-09-19|https://www.marketbeat.com/instant-alerts/event-equinix-plans-5b-7b-annual-data-center-buildout-as-ai-demand-accelerates-2026-09-19/
Industrial REIT occupancy / rent growth|STALE|0.62|2026-09-20|https://www.marketbeat.com/instant-alerts/event-prologis-sees-leasing-surge-data-centers-fuel-growth-outlook-2026-09-20/
Refinancing window opening|MISS|0.70|2026-09-21|https://www.pcbb.com/bid/2026-07-27-the-2026-cre-refinancing-test-what-cfis-should-know
Cap-rate compression|MISS|0.65|2026-09-21|https://www.cbre.com/insights/reports/us-cap-rate-survey-h1-2026
Rates rising / REIT selloff|MISS|0.72|2026-09-21|https://www.cnbc.com/2026/09/21/treasury-yields-government-bonds.html
Office vacancy / mark-to-market stress|HIT|0.58|2026-09-21|https://www.commercialcafe.com/blog/national-office-report/
Refinancing wall stress|HIT|0.60|2026-09-21|https://brookmontcapital.net/insights/cre-refinancing-wall-2026
Cap-rate expansion|PARTIAL|0.50|2026-09-21|https://www.cbre.com/insights/reports/us-cap-rate-survey-h1-2026
Sector rotation into REITs|MISS|0.70|2026-09-21|channel1
Sector rotation out of real estate|HIT|0.68|2026-09-21|channel1
HIT_GRID_END

## RESEARCH APPENDIX

**Queries run**
- US 10 year Treasury yield 30 year TIPS real yield today September 21 2026
- XLRE REIT real estate ETF premarket September 21 2026
- Fed path Warsh rate hike odds September 2026 Treasury yields
- data center REIT Equinix Digital Realty demand September 2026
- CNBC US 10 year Treasury yield live September 21 2026
- office vacancy REIT BXP SLG September 2026
- XLRE ETF flows positioning REIT rotation September 2026
- industrial REIT occupancy Prologis rent growth September 2026
- CRE refinancing wall commercial real estate 2026 cap rates
- stock market futures Monday September 21 2026 risk on yields oil
- CME FedWatch October 2026 rate hike probability September 21
- XLRE vs SPY real estate sector breadth leadership Monday September 21 2026
- X search: XLRE REIT yields 10-year real estate sector today September 21 2026 (2026-09-18..2026-09-21)
- Fetches: https://www.cnbc.com/2026/09/21/treasury-yields-government-bonds.html ; https://www.cnbc.com/quotes/US10Y ; https://www.cnbc.com/quotes/US30Y ; Yahoo live markets page (fetch failed)

**Key sources (title + URL + timestamp / as-of)**
- CNBC — “Treasury yields ease as global borrowing costs tumble” — https://www.cnbc.com/2026/09/21/treasury-yields-government-bonds.html — fetched 2026-09-21T10:28Z. **Facts used:** 10Y **4.967% (−3 bp)** after last week’s **5.041%** 19-year high; 2Y **4.729% (−1 bp)**; 30Y **5.306% (−3 bp)**; oil down; FOMC hike still being digested; PMI Wed / claims Thu / Williams & Barkin speeches.
- CNBC US10Y quote search snapshot — https://www.cnbc.com/quotes/US10Y — 2026-09-21. **Facts used:** ~**4.961%** early ET vs prev close **4.996%** (confirms modest easing, not a rip).
- QZ / CNBC Warsh FOMC coverage — https://qz.com/federal-reserve-interest-rate-hike-fomc-meeting-091626 ; https://www.cnbc.com/2026/09/18/three-words-from-kevin-warsh-have-wall-street-wondering-how-far-the-fed-will-go-with-rate-hikes.html. **Facts used:** Sep 16 hike **+25 bp to 3.75–4.00%**, unanimous; path still hawkish; **already printed**.
- FedWatch / FinanceFeeds — https://growbeansprout.com/tools/fedwatch ; https://financefeeds.com/will-the-fed-raise-interest-rates-again-october-odds-45/. **Facts used:** October 27–28 hike odds ~**45–57%** — two-sided, not a same-open binary.
- Channel 1 (pipeline, not re-derived) — 2026-09-21 envelope. **Facts used:** VIX 14.98 / ratio 0.821; Finviz ES +0.20% / NQ +0.41%; ES=F +1.35% / NQ=F +2.12%; WTI −1.59% / CL −5.94%; XLRE PM **−0.05%**; XLRE vs SPY 1d/3d/1w/1m rel **−1.08 / −2.07 / −1.96 / −4.76**; DGS10 4.94 / DGS30 5.29 / DFII10 2.61 as of 2026-09-17.
- MAP HEAT research (injected) — 2026-09-21. **Facts used:** split nested (Hotel/Residential up; Office/Mortgage/Specialty down); size_gate=True.
- ETFDB / Trefis XLRE flows — https://etfdb.com/etf/XLRE/ ; https://www.trefis.com/data/etfs/XLRE. **Facts used:** recent 5d/1m net **outflows**; not a crowded-long tape.
- DLR / EQIX — CNBC 2026-09-15 AI-slowdown interview; MarketBeat 2026-09-19 EQIX $5–7B plan; GlobeNewswire 2026-09-17 DLR survey. **Facts used:** DC demand intact but **stale / nested Specialty down** — not a parent up vote.
- Prologis Q2 supplemental / MarketBeat 2026-09-20 — occupancy **95.5%**, cash rent **+22.3%**. **Facts used:** industrial quality sleeve **stale**; MAP HEAT Industrial **flat**.
- CommercialCafe national office ~**17.8%** vacancy (Aug 2026); BXP/SLG Q2 occupancy better in premier stock. **Facts used:** office stress is **sub-type**, not XLRE.
- PCBB / Brookmont CRE wall 2026 — ~**$875B** maturities; CMBS coupon-to-refi gap ~**114 bp**. **Facts used:** refinancing window **closed**; structural, not same-morning.
- BreadthMarket / sector-health dashboards through ~Sep 18. **Facts used:** REIT participation extremely weak (~3% above 20d SMA) — **structural**, not scored into both S2 and S4.
- X search (2026-09-18..21). **Facts used:** XLRE/XLU discussed as rate-pressured; **no live same-morning REIT catalyst**. Checked, nothing material beyond the yield/oil tape.

**Not used as live S0/S1 (leftover / T+printed):** News Judge Warsh JH / gold −3% / “Dow worst week in six months” — already in last week’s cash path. Fear & Greed 58.2 stamped **2026-08-27**. CME FedWatch “not scrapable” in Channel 1; used Channel 2 ~50/50 October odds only as path context, scored **0**.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': -0.5, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': -0.5, 'S4_ETF_TAPE': -1.0}, 'multiplier': 0.9, 'leading_sum': -1.5, 'divergence_flagged': True, 'total_score': 3.17, 'predicted_direction': 'flat', 'predicted_magnitude_band': 'flat', 'confidence_score': 0.527, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.1608, 'score': 0.965, 'legs': [{'leg': 'ES', 'pct': 1.35, 'w': 0.5}, {'leg': 'ZN', 'pct': -0.03, 'w': 0.75}, {'leg': 'PM:XLRE', 'pct': -0.05, 'w': 0.7}]}, 'overlay_score': -1.012, 'overlay_raw': -1.012, 'index_carry': 3.218, 'general_total': 12.871, 'skill_multipliers': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 1.0}, 'llm_confidence': 0.48, 'sector_rs_veto_applied': True, 'sector_rs_tape': {'d1': -0.56, 'w1': -2.06}, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
