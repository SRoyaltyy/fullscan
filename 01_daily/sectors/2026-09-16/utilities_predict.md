# Sector Prediction — Utilities — 2026-09-16

- ETF: **XLU**
- rubric: `00_grounding/sectors/utilities.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-0.468** (mult 0.9)
- regime: risk_on
- divergence_flagged: **True**
- engine: v2 · tape_anchor **1.583** (ES +1.14%, ZN -0.03%, PM:XLU +0.24%) · index_carry **1.324** (general 5.297) · llm_overlay **-3.375** (raw -3.375)

## Channel 1 sector ETF tape

```
ETF XLU vs SPY (yfinance, through 2026-09-15):
  1d: XLU -1.20% | SPY -0.46% | rel -0.74%
  3d: XLU -2.82% | SPY -0.06% | rel -2.76%
  1w: XLU -4.90% | SPY -1.12% | rel -3.78%
  1m: XLU -6.75% | SPY -2.44% | rel -4.31%
```

MEMORY_CONFIRM: Utilities/XLU only — memory index unavailable this run (used injected sector logs + Channel 1). Rolling dir=0.4 / mag=0.4 (n=10); last 30 dir=0.444 / mag=0.389 (n=18). Last graded: 09-14 down/mild vs XLU −1.34% / SPY −0.45% / rel −0.90% (dir HIT, mag MISS — S4 under-scored). 09-15 down/mild still ungraded. Applied: 09-11 (FOMC/CPI-class binary for a defensive: test each branch; risk-on inputs are headwinds, not cushions); 09-10 (VIX 16.98 contango 0.877 fails VIX≥20 FTS gate → sticky/rising long end = relative-LAG, not 08-18 beat); 09-09 (no ≥0.4% cushion; PM +0.24% is not a defensive spike); 09-08 (oil still elevated ~$104, offering today = inflation channel fading, not a new FTS bid); 08-28 (do not pre-score the 2pm Warsh decision/presser as a hawkish HIT; no notable-down from the hawkish branch before the statement); 08-27 (8:30 already printed; NQ leads ES → relative lag / flat-to-down absolute unless a fresh same-session yield impulse — there is none); 08-25 (S0/S1 are not both 0, so the “don’t manufacture down from carried lag” gate does not bind); 08-21 (live curve ~4.96–4.97, not FRED 09-14 1d as “today’s move”); 08-12 (AI-power is a 1d dampener); 08-13 (S2/S4 confirmation only; one trailing rel print does not pay S2 and S4); 08-14/09-04 (calendar: 8:30 retail sales + 2:00 FOMC, not a light day). Open experiment (extra confirm before full weight): used ES/NQ + Asia/Europe + XLK PM + hot retail for rotation; live 10Y/30Y/real yield + bond futures for duration. |score| modest → shrink confidence (scope do-instead). Same-shock: rates/Fed counted in S0 only; rotation-away in S1 only.

# Utilities (XLU) — 2026-09-16

Object is the **near-session XLU environment**, not SPX and not a stock pick.

### Channel 1 (trusted, not re-derived)

XLU vs SPY through 2026-09-15: **1d −1.20% / −0.46% (rel −0.74%)**; **3d −2.82% / −0.06% (rel −2.76%)**; **1w −4.90% / −1.12% (rel −3.78%)**; **1m −6.75% / −2.44% (rel −4.31%)**. Every horizon is red; the lag is **widening**.

Macro: VIX **16.98** (−0.22 1d, +0.52 1w), **VIX/VIX3M 0.877 — CONTANGO**; DGS10 **4.97** as of 09-14 (+1 bp 1d, **+19 bp 1w, +34 bp 1m**); DGS30 **5.34** (−1 bp 1d, +10 bp 1w, +13 bp 1m); DFII10 **2.60** (0 bp 1d, **+17 bp 1w, +21 bp 1m**); HY 2.71 (+6 bp 1d, still tight); EPU 215.48; **CL=F −2.36% / BZ=F −1.50%** (Finviz WTI **−1.59%** / Brent **−1.02%**); DXY ~flat; **ES=F +1.14% / NQ=F +1.50%** vs prior close (Finviz live futures milder but still green: ES +0.20%, NQ +0.41%, RTY +0.08%, DJIA +0.11% — NQ leads either tape); **XLU PM +0.24%** vs XLK **+0.65%**, XLP −0.04%, XLRE +0.05%; Asia **+0.65%**; Europe **+0.45%**; 5-day 10Y–SPX corr **−0.155**. Bond futures: 10Y note **−0.03%**, 30Y **−0.06%** — a tiny backup, not a relief bid.

**Live curve (08-21):** 10Y hit **5.041%** Tuesday (highest since 2007) and has pulled back to **~4.96–4.97%** (~7:38 AM ET, CNBC). That is a **stabilization inside the stress zone**, not a scored easing impulse. Do not pay yesterday’s 5% breach twice.

**Calendar (08-14 / 09-04 / 09-11):** **8:30 ET Advance Retail Sales already out: +1.2% MoM / +6.0% YoY** (Census) — a hot growth print. **FOMC 2:00 ET + Warsh presser 2:30 ET** still unresolved. No CPI/NFP today.

### Channel 2

**1. Shared macro → this sector.** Classical map is **real/nominal yields**; AI load is structural offset only.

- **Pre-binary tape is risk-on, NQ-led:** ES +1.14% / NQ +1.50% vs prior close; Asia and Europe green; VIX 16.98 in contango; XLK PM +0.65%. Per 09-11, for a defensive those are **headwinds, not cushions**. Per 08-27, NQ leading ES with mega-cap/AI news already public (Adobe beat/raise; ASML 2027 EUV sold out) defaults XLU to **relative lag / flat-to-down absolute** unless a fresh same-session yield impulse appears. It has not: bond futures are flat-red, 10Y ~4.96–4.97%.
- **Hot retail sales (knowable):** +1.2% MoM confirms growth into a 90%+ hike FOMC. That is the 09-11 in-line/growth branch: **rotation away from defensives**, not a duration bid.
- **FOMC 2pm (unknowable — do not pre-score the statement):** CME FedWatch **>90% / ~92.5%** of a 25 bp hike to 3.75–4.00% (CNBC/Quartz). Branch test (09-11 / 08-28): **in-line hike** → risk-on continuation / rotation-away (negative-to-neutral for XLU); **hawkish dots/presser** → long-end up, bond-proxy down (negative); **dovish hold** (~7–10%) is the one duration-positive branch. Unlike 09-11 CPI, the binary is **not fully one-sided**, so the pending decision stays **event risk**, not a hawkish HIT. Modal pre-tape is still the risk-on/sticky-yield mix.
- **Oil offering from war-premium levels** (WTI ~$104, −1.6/−2.4%): 09-08 says elevated oil is an inflation/duration negative when it is **rising**; today it is **fading**, which forbids forcing a fresh rates smash from oil and does **not** mint FTS. For a defensive, oil-offering is another risk-on input.
- **09-10 gate:** VIX 16.98 < 20 and **contango** — no FTS. Sticky ~5% long end is a **relative-lag** signal, not an 08-18 relative beat. When the long end was the cause of this week’s risk-off (08-18 qualifier), XLU was the transmission channel; that damage is already in the 1w/1m tape.

**S0 = −1.** Risk-on mapping + sticky real yields + hot retail. Extra confirms in the dominant bucket (open experiment). Do not also HIT the 2pm decision. Do not score + from green futures.

**2. Spine / secondary.**
- **Data-center load growth / power demand upside:** structural HIT, **stale** for a 1d call. Texas interconnection freeze (247 Wall St 09-07) and Duke Florida large-load cost allocation (Orlando Sentinel 09-16) are **not** a fresh XLU-wide catalyst; 08-28 bans promoting a single-name rate-case item. Rubric: do **not** let the multi-year AI-power story override today’s rate/rotation tape. 08-12 dampener only.
- **Rates falling (bond-proxy bid):** **MISS.** Live 10Y ~4.96–4.97 / 30Y 5.34 / real 2.60. Bond futures red ticks.
- **Rates rising (bond-proxy selloff):** **PARTIAL / carried.** 1w DGS10 +19 bp and yesterday’s 5.04% print are already in the price; this morning the curve is **not** independently rising. Counted in S0, not again here.
- **Risk-on rotation away from utilities:** **HIT.** NQ-led green tape, XLK PM +0.65% vs XLU +0.24%, hot retail, Adobe/ASML AI confirmation. Extra confirms: ES, NQ, Asia, Europe, XLK PM.
- **Risk-off / FTS:** **MISS.** VIX contango, futures green.
- **Nuclear / gas / grid CapEx / favorable ROE:** structural, no same-session order. MAP HEAT captains all **none**, nested heat/split **flat**.
- **Adverse rate case / load-growth disappointment / regulatory smash:** carried (Texas/Ohio/WoodMac). Duke Florida is single-name.
- **Sector rotation into utilities:** **MISS.** Out: **HIT** — same object as rotation-away; not double-counted.

**S1 = −1.** Fresh factor is rotation-away. Rates-rising not independently HIT (same-shock audit vs S0). AI-power does not override.

**3. Breadth.** MAP HEAT: diversified / regulated electric / gas / water / IPP / renewables all **flat / none**. No constituent expansion. Trailing 1d rel −0.74% is lag, not smash. Live PM: XLU +0.24% vs ES +1.14% is **lag**, not leadership. Independent breadth evidence is absent, so the trailing 1d print is reserved for S4 (one-print rule). Do **not** copy 1w/1m lag into S2. **S2 = 0.**

**4. Flows / positioning.** ETFDB: 5d **−$4.05M**, 1m **−$62.5M**, 3m **+$552M**. Modest 1m outflow, not a relative-volume spike. 1m rel −4.31% = **de-risked**, not a crowded long. Yesterday’s volume ~24.2M vs ~20M avg is only a mild down-day bump. De-risked complex does not get an extra S3 negative. **S3 = 0.**

**5. ETF tape (confirmation only).** Unidirectional multi-horizon lag (1d/3d/1w/1m all < 0). Freshest 1d rel **−0.74%** is sub-gate vs the 09-14 ≥1% floor, but live PM relative vs ES is already ~−90 bp and 1w/1m lags are severe. 09-10: no relative-beat clause. 08-13: confirmation, not the thesis. **S4 = −1.**

**Self-audit.** Lens = bond-proxy + defensive-on-risk-on, not an SPX call. Same-shock: Warsh/hike-odds/sticky 10Y counted once in S0; rotation-away once in S1; S4 confirms the lag, does not invent a second rates smash. Single-name (Duke FL, CEG/VST IPP) does not drive the ETF; MAP HEAT none. Band: FOMC can gap the afternoon (09-03: do not use a flat band into a high-impact print) but 08-28 forbids minting **notable-down** from the hawkish branch before 2pm. Open experiment: extra confirms present; |leading| modest → keep conviction in check. Divergence: leading S0+S1 = −2 and S4 = −1 **agree** (no leading-vs-tape fight). 09-11 analog (green futures, oil offered, sticky long end, pending macro binary) realized XLU **absolute down / relative lag**; that is the modal mapping, with FOMC leftover variance after 14:00 ET.

**Relative clause (required):** On this risk-on, VIX<20, contango tape, XLU should **lag SPY**. Absolute path is the factor score; do not smuggle an 08-18 relative-beat.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: -1
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: -1
MULTIPLIER: 0.9
CONFIDENCE: 0.60
REGIME: risk_on
HORIZON_3D: down
HORIZON_1W: down
HORIZON_2W: mixed
HORIZON_1M: mixed
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|HIT|0.80|2026-09-16|https://www.cnbc.com/2026/09/16/fed-meeting-today-live-updates.html
Risk-off tape / flight to safety|MISS|0.75|2026-09-16|https://www.cnbc.com/2026/09/16/fed-meeting-today-live-updates.html
Real yields rising|PARTIAL|0.70|2026-09-16|https://www.gurufocus.com/economic_indicators/37/10-year-treasury-yield
Real yields falling|MISS|0.70|2026-09-16|https://www.gurufocus.com/economic_indicators/37/10-year-treasury-yield
USD strengthening|MISS|0.55|2026-09-16|
USD weakening|MISS|0.55|2026-09-16|
Sector breadth expansion (% names up)|MISS|0.65|2026-09-16|
Sector breadth failure (ETF up, names flat)|MISS|0.60|2026-09-16|
Large-cap leadership inside sector|MISS|0.50|2026-09-16|
Small/mid leadership inside sector|MISS|0.50|2026-09-16|
High-beta leadership inside sector|MISS|0.55|2026-09-16|
Low-beta leadership inside sector|PARTIAL|0.55|2026-09-16|
Sector ETF inflow / relative volume spike|MISS|0.60|2026-09-16|https://etfdb.com/etf/XLU/
Sector ETF outflow / volume dry-up|PARTIAL|0.55|2026-09-16|https://etfdb.com/etf/XLU/
Crowded long (extreme relative performance + valuation)|MISS|0.70|2026-09-16|
Index rebalance / inclusion tailwind|MISS|0.40|2026-09-16|
Index exclusion / forced selling|MISS|0.40|2026-09-16|
Data-center load growth / power demand upside|PARTIAL|0.60|2026-09-07|https://247wallst.com/investing/etf/2026/09/07/xlus-ai-power-story-crumbles-as-texas-freezes-data-center-demand/
Rates falling (bond-proxy bid)|MISS|0.80|2026-09-16|https://www.cnbc.com/2026/09/16/fed-meeting-today-live-updates.html
Favorable rate case / allowed ROE|MISS|0.45|2026-09-16|
Nuclear / gas generation policy support|MISS|0.40|2026-09-16|
Grid CapEx approval / recovery|MISS|0.40|2026-09-16|
Rates rising (bond-proxy selloff)|PARTIAL|0.70|2026-09-15|https://www.morningstar.com/news/dow-jones/202609157714/utilities-down-as-treasury-yields-hit-multiyear-highs-utilities-roundup
Adverse rate case|PARTIAL|0.45|2026-09-16|https://www.orlandosentinel.com/2026/09/16/how-much-could-data-centers-cost-duke-energy-florida-customers-its-redacted/
Load growth disappointment|PARTIAL|0.50|2026-09-07|https://247wallst.com/investing/etf/2026/09/07/xlus-ai-power-story-crumbles-as-texas-freezes-data-center-demand/
Regulatory disallowance / project cancel|MISS|0.40|2026-09-16|
Risk-on rotation away from utilities|HIT|0.80|2026-09-16|https://www.cnbc.com/2026/09/16/fed-meeting-today-live-updates.html
Sector rotation into utilities|MISS|0.75|2026-09-16|
Sector rotation out of utilities|HIT|0.75|2026-09-16|https://www.morningstar.com/news/dow-jones/202609157714/utilities-down-as-treasury-yields-hit-multiyear-highs-utilities-roundup
HIT_GRID_END

## RESEARCH APPENDIX

**Queries run**
- web_search: `FOMC September 16 2026 Warsh rate decision hike odds FedWatch` (freshness=day)
- web_search: `10 year Treasury yield today September 16 2026` (freshness=day)
- web_search: `XLU utilities ETF rotation data center power demand rate case September 2026` (freshness=week)
- web_search: `CME FedWatch September 2026 FOMC hike probability today` (freshness=day)
- web_search: `XLU ETF flows volume utilities stocks premarket September 16 2026` (freshness=day)
- web_search: `NextEra Southern Duke Constellation utilities stocks today FOMC` (freshness=day)
- web_search: `US 10 year yield 30 year real yield TIPS September 16 2026` (freshness=day)
- web_search: `utilities sector rotation out FOMC risk-on September 2026` (freshness=day)
- web_search: `economic calendar September 16 2026 FOMC 2pm CPI NFP` (freshness=day)
- web_search: `US retail sales August 2026 released September 16` (freshness=day)
- web_search: `utilities down treasury yields multiyear highs September 15 2026` (freshness=week)
- web_search: `XLU ETFDB fund flows September 2026` (freshness=week)
- web_fetch: `https://www.cnbc.com/2026/09/16/fed-meeting-today-live-updates.html`
- x_search: `XLU utilities ETF FOMC yields today September 16 2026` (2026-09-15 to 2026-09-16)
- memory_search: Utilities/XLU lessons (index unavailable)

**Key sources (title + URL + timestamp/facts taken)**

1. **CNBC — Fed meeting live updates** — https://www.cnbc.com/2026/09/16/fed-meeting-today-live-updates.html — fetched 2026-09-16T12:38:57Z. Facts: FedWatch **>90%** of a 25 bp hike to 3.75–4.00%; first hike since July 2023 after 175 bp of cuts; Warsh Jackson Hole + August CPI + oil >$100 drove the repricing; 10Y **high 5.041% Tuesday**, **~4.96% at 7:38 AM ET**; decision 2:00 ET, SEP/dot plot including 2029; Morgan Stanley now 2 hikes (this week + December).
2. **Quartz / Fox Business (via search)** — https://qz.com/stock-futures-fed-rate-decision-september-091626 — 2026-09-16. Fact: FedWatch cited at **92.5%**.
3. **GuruFocus 10-year yield** — https://www.gurufocus.com/economic_indicators/37/10-year-treasury-yield — ~**4.97%** on 2026-09-16.
4. **US Census Advance Monthly Retail Sales** — https://www.census.gov/retail/sales.html — 2026-09-16 release. Facts: August sales **+1.2% MoM** to $773.9B, **+6.0% YoY**; July revised to −0.5%.
5. **Morningstar/Dow Jones utilities roundup** — https://www.morningstar.com/news/dow-jones/202609157714/utilities-down-as-treasury-yields-hit-multiyear-highs-utilities-roundup — 2026-09-15. Facts: utilities sold as 10Y breached ~5% (2007 highs); XLU **>4% YTD** down; power producers hit.
6. **ETFDB XLU** — https://etfdb.com/etf/XLU/ — mid-Sep 2026. Facts: 5d **−$4.05M**, 1m **−$62.52M**, 3m **+$552.33M**; Sep 15 close **$41.32 (−1.20%)**, volume ~24.2M.
7. **247 Wall St — XLU AI-power / Texas freeze** — https://247wallst.com/investing/etf/2026/09/07/xlus-ai-power-story-crumbles-as-texas-freezes-data-center-demand/ — 2026-09-07. Fact: ERCOT pause on new data-center interconnections; carried load-growth friction, not a same-session XLU catalyst.
8. **Orlando Sentinel — Duke Energy Florida data-center costs** — https://www.orlandosentinel.com/2026/09/16/how-much-could-data-centers-cost-duke-energy-florida-customers-its-redacted/ — 2026-09-16. Fact: same-day single-name rate-case/cost-allocation item; not promoted into S1 (08-28).
9. **fedratecalc US economic calendar** — https://fedratecalc.com/us-economic-calendar/ — 2026-09-16. Facts: 8:30 Advance Retail Sales; FOMC 2:00 ET / presser 2:30 ET; CPI was 09-11, NFP was 09-04.
10. **X search (xAI)** — 2026-09-15/16. Fact: **checked, nothing material** on XLU-specific FOMC tape; one 09-15 post had XLU YTD **−2.80%** as a weak sector. Not used as a score input.

Channel 1 numbers were not altered. MAP HEAT nested captains returned **none/flat** — no IPP/regulated override.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -1.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': -1.0}, 'multiplier': 0.9, 'leading_sum': -5.0, 'divergence_flagged': True, 'total_score': -0.468, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.419, 'regime': 'risk_on', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.2638, 'score': 1.583, 'legs': [{'leg': 'ES', 'pct': 1.14, 'w': 0.3}, {'leg': 'ZN', 'pct': -0.03, 'w': 0.75}, {'leg': 'PM:XLU', 'pct': 0.24, 'w': 0.7}]}, 'overlay_score': -3.375, 'overlay_raw': -3.375, 'index_carry': 1.324, 'general_total': 5.297, 'skill_multipliers': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': 1.25, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 1.25}, 'llm_confidence': 0.6, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
