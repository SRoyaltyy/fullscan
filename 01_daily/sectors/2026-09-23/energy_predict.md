# Sector Prediction — Energy — 2026-09-23

- ETF: **XLE**
- rubric: `00_grounding/sectors/energy.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-2.34** (mult 0.85)
- regime: mixed
- divergence_flagged: **False**
- engine: v2 · tape_anchor **-0.363** (CL -1.59%, QA -1.02%, ES +0.03%, PM:XLE +0.21%) · index_carry **0.573** (general 2.293) · llm_overlay **-2.55** (raw -2.55)

## Channel 1 sector ETF tape

```
ETF XLE vs SPY (yfinance, through 2026-09-21):
  1d: XLE -2.30% | SPY +1.55% | rel -3.85%
  3d: XLE -1.87% | SPY +2.83% | rel -4.71%
  1w: XLE -2.63% | SPY +1.91% | rel -4.54%
  1m: XLE -1.44% | SPY +1.68% | rel -3.12%
```

MEMORY_CONFIRM: Energy/XLE — memory index unavailable this run (used injected scoreboard/lessons only). Last-10 dir=0.7 mag=0.4 (n=10); last-30 dir=0.56 mag=0.32 (n=25); last graded 09-22 down/mild vs XLE −1.089% (dir HIT, mag MISS — notable at the 1.0% cut). Open sector_energy experiment **does** apply: keep direction, shrink confidence after mag misses. Applied: **08-11 live-oil verify FIRES** — Channel 1 Finviz WTI $104.16 (−1.59%) / Brent $107.67 (−1.02%) is the leftover 09-16 column; CL=F −4.97% / BZ=F −3.68% is rejected as the live increment (same expiry/roll distortion as 09-22). Independent live: WTI **~$89.5–90.0** (~−0.6% vs ~$90.52), Brent **~$99.0–99.8**; Oilprice 22 Sep 15:55 ET WTI **$89.14 / Brent $97.85**. Sign **DOWN**, increment **sub-1.5%**, not a collapse. **08-14 green-oil does NOT fire.** **09-15 physical-increment notable license does NOT fire** (oil offered; diplomacy/Gulf-flow hopes are the fade, not a new outage). **09-14 backwardation debit does NOT fire** (VIX/VIX3M **0.786 contango**). **09-11 pending-binary flatten does NOT fire** (no CPI/NFP/FOMC; ES=F **+0.03%** / NQ=F **−0.10%**, not ≥ +0.5%). **09-10 crowded-long does NOT fire** (1m rel **−3.12%** ≪ +8%; already de-risked). **09-21 S0-laggard notable lift does NOT fire** (PM:XLE **+0.21%**, not ≤ −1%; live index not green ≥ +1%; energy is **not** the board laggard). **09-17 leftover-S4 gate FIRES**: do **not** reuse Channel 1 1d rel **−3.85%** (through 09-21) or 09-22’s **−1.07%** as live S4; PM **+0.21%** is **not** smash extension. **09-03 emit-signed-down is weak, not off**: barrel still offered, but PM is mildly green so the “S4=0 because prior tape leftover, not because PM is bid” clause is only half-met. **09-18 band refinement FIRES toward mild/flat** (oil ~sub-1%, |PM|<1%, 1m not crowded, nested refiners a sleeve). **09-08/09-09 mag-discipline + size_gate=True FIRES**. EIA WPSR **today 10:30 ET unprinted, two-sided**. News Judge #4 / DVN −5.6% is **09-16 leftover** — do not restack.

# Energy / XLE — 2026-09-23

This is a **sixth-session oil-premium fade**, not a fresh sector_shock and not a collapse sequel to Monday/Tuesday. Yesterday’s object (XLE **−1.09% / rel −1.07%**, oil extended on post-open Hormuz-offer/East-West color) **already printed**. This morning the **active** barrel is still **offered**, but the **incremental** print is only ~**0.6%**, XLE is **mildly green in premarket (+0.21%)**, and live ES/NQ are **flat**. Count the oil-down + geo-fade + API-build lean **once**. Do **not** treat Channel 1 CL=F −4.97% or Finviz $104 as today’s smash. Do **not** rerun 09-21/09-22 notable-down hindsight. Do **not** flip to up on leftover Nasdaq/AI beta. Do **not** pre-score the 10:30 EIA.

## Channel 2

**1. Shared macro as it hits energy.** Live tape is **flat, not a commodity bid and not a 09-21 risk-on funding squeeze**: ES=F **+0.03%**, NQ=F **−0.10%**. Finviz ES/NQ leftover greens (+0.20% / +0.41%) are **not** the live impulse. Asia composite **+0.33%**, Europe **−0.25%**. **VIX 14.21 (−0.66) with VIX/VIX3M 0.786 — deep contango**, so the 09-14 full-weight de-risking debit is **off**. **USD strengthening** (DXY **+0.38%** 1d) is a mild producer headwind, secondary vs oil. DFII10 **2.62, −0.06 1d** — real yields easing overnight, also secondary. News Judge #1 (Nasdaq record / chips / oil cools) is **already in 09-22’s price**. Cheaper oil is SPX-positive inflation optics; that can leak beta **only if PM is participating**. It is, weakly: **XLE +0.21%** vs XLK **−0.05%** / XLI **−0.02%** — energy is **a mild leader on a flat board**, not the clear laggard. 09-21’s “score S0 negative when the sector is the clear laggard on a green tape” **does not meet its precondition**. 09-11’s “don’t score S0 negative on green futures” is **moot** (futures are flat). **S0 = 0.**

**2. Spine (S1).** One cluster: live offered barrel **plus** geo-premium fade **plus** API build as a lean. Do not triple-count.
- **Crude: offered ~0.6% today, not a surge, not a collapse.** Live-verified (08-11): WTI **~$89.5–90.0** (~−0.6%), Brent **~$99.0–99.8**; Reuters/IOL 23 Sep: oil slides on Gulf-supply hopes and Trump “very good” US–Iran talks at UNGA. Channel 1 Finviz $104.16 / CL=F −4.97% is **rejected**. Still ~$90 / ~$99 — a dip, not 08-25’s smash and not 09-15’s +2.3% surge. Incremental move is **sub-1.5%**.
- **Geo premium still physically tight, not transmitting — fading.** Hormuz visible traffic remains a handful of ships (Kpler/Reuters: **2** commodity crossings 22 Sep, **3** on 23 Sep vs ~10-day avg ~15 and pre-war 85–125). That is **not** a fresh kinetic increment. The price is falling on **diplomacy + East-West/Gulf-flow hopes** already in Monday–Tuesday’s tape. **Do not** score Crude oil price surge. **Do not** score a fresh Geopolitical supply risk premium HIT. 09-15’s physical-increment license is **off**. Residual chokepoint is why S1 is not −2, not a bid.
- **Inventory: API is a lean; EIA is unprinted.** API week ending 9/18: crude **+1.786 Mb** (Oilprice, 22 Sep 16:08 CDT); Cushing **+2.082 Mb**; gasoline **−2.16 Mb**, distillates **−2.16 Mb**. WSJ survey (22 Sep 13:29 ET): EIA crude **−0.5 Mb** expected (range −2.6 to +1.6). Last official EIA (week ended 9/11, released 9/16): crude **−0.64 Mb** to **423.4 Mb**. **Today’s WPSR 10:30 ET is two-sided until it prints.** Do not date API as a full Inventory-build HIT; do not ignore it — it is inside the oil-down cluster.
- **OPEC+ (carried offset):** Sep **+188 kb/d** completed the 2023 voluntary-cut rollback; **6 Sep** meeting **held October unchanged**. Next core-group meeting **Oct 4**. Not a cut, not a quota break.
- **Demand destruction (carried official):** IEA/China 2026 lid is leftover. Channel 2 overnight: **checked, nothing material** as a same-session HIT.
- **Cracks:** 3-2-1 still extreme (~$68–77; diesel cracks historically wide). **Refiner sleeve only.** MAP HEAT Refining dir=up (MPC/VLO) is **nested** — dampen; do not let VLO/MPC set the ETF. Channel 1 HO **+0.18%** / RBOB **−0.54%** is the stale Finviz column.
- **Nat gas ~$3.00–3.17** — no surge; N/A for oil-weighted XLE.
- **MAP HEAT nested:** OFS/Drilling/Coal/Uranium **OVERRIDE down**; E&P/Integrated residual **up** (XOM/CVX PM green; COP red). Nested overrides **do not average into XLE**.

Net **S1 = −1**. Live oil sign is down once. Not −2: increment is sub-1.5%, EIA is two-sided (could print a draw vs API), Hormuz is still physically tight. Not 0: the barrel is still offered on a multi-session fade with an API crude build and live diplomacy-fade headlines.

**3. Breadth.** Channel 1 1d rel **−3.85%** is **09-21 leftover** — do not copy 3d/1w/1m into S2. Live PM: XLE **+0.21%**, XOM ~**+0.3%**, CVX ~**+0.1–0.4%**, COP ~**−0.5%** — mixed majors, not expansion, not ETF-only carry, not a smash. MAP HEAT split (integrated/E&P residual up, OFS/drilling override down) is **not** sector-wide breadth. **S2 = 0.**

**4. Flows / positioning.** XLE **5-day ~−$874M**, **1m ~−$908M** (ETFdb/ETF Action through ~22 Sep) — outflows, not an inflow spike. 1m rel **−3.12%** is **already de-risked**, not crowded-long. Outflows are the same fade already in S1, not a second unwind. Washout optionality is 2W/1M, not today’s HIT. **S3 = 0.**

**5. Catalysts.** **EIA crude WPSR 10:30 ET** is the only same-session hard energy print — **unprinted, two-sided**. No fresh XLE-wide earnings. UNGA/Iran talks are a **fade binary**, not a new outage. OPEC+ next **Oct 4**.

### Scoring logic

S0 muted: flat futures + contango + mild USD headwind are not a veto and not a bid. Energy is not the 09-21 laggard.

S1 is the live offered barrel counted once. EIA is not dated.

S2/S4 stay 0: leftover Channel 1 rel is prior-session fact (09-17). Live PM is slightly green, so 09-04 aligned-negative is **off**.

S3 stays 0: outflows exist but are not a separate crowded-long debit.

**Divergence:** leading factor sum (**S1 = −1**) fights tape confirmation (**S4 = 0**, PM +0.21%). Flag it; **trust factors over tape**. Do **not** import 09-11’s flatten (that was a CPI-day, futures ≥ +0.5%, oil dip that did not extend). Do **not** import 09-17’s mixed/up license as a direction flip — that day had live index repair (ES/NQ green); today ES/NQ are **flat**. 09-03 still supports a **signed down** from a live offered barrel when S4 is 0 because leftover tape is not a counter-signal. PM +0.21% **caps magnitude**, it does not flip sign.

**Magnitude:** size_gate=True; mag hit-rate **0.4**; oil not >5%; XLE PM not >2%. 09-18: oil sub-1% and |PM|<1% → **mild, not notable**. Open experiment: keep direction, **shrink confidence**. Nested refiners are a sleeve, not an XLE cushion that zeros S1.

**Self-audit:** lens = XLE environment, not SPX, not XOM. Band = mild (size_gate + 09-18). Skew = none. Same-shock (oil + diplomacy fade + API) counted **once**. Single-ticker (COP red / XOM green / VLO nested) does not set the ETF.

### Horizons
- **3D:** Still a fade tape unless EIA prints a decisive draw *and* oil reverses >1.5%. Base case: residual down/flat digestion.
- **1W:** Geo premium fading faster than physical Hormuz tightness; Oct 4 OPEC+ is the next supply meeting. Mild negative bias while WTI holds sub-$92.
- **2W:** Washout optionality if outflows persist and 1m rel stays negative; not today’s object.
- **1M:** De-risked vs the early-Sep crowded run; structural geo residual remains a floor, not a 1d bid.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: -1
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.85
CONFIDENCE: 0.42
REGIME: mixed
DIVERGENCE_FLAGGED: true
HORIZON_3D: down
HORIZON_1W: down
HORIZON_2W: flat
HORIZON_1M: flat
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|MISS|0.55|2026-09-23|https://www.investopedia.com/stock-market-today-futures-little-changed-after-nasdaq-closes-at-fresh-record-12136110
Risk-off tape / flight to safety|MISS|0.70|2026-09-23|channel1
Real yields rising|MISS|0.60|2026-09-21|channel1
Real yields falling|HIT|0.45|2026-09-21|channel1
USD strengthening|HIT|0.55|2026-09-23|channel1
USD weakening|MISS|0.70|2026-09-23|channel1
Sector breadth expansion (% names up)|MISS|0.60|2026-09-23|channel1
Sector breadth failure (ETF up, names flat)|MISS|0.55|2026-09-23|https://stockanalysis.com/stocks/xom/
Large-cap leadership inside sector|HIT|0.45|2026-09-23|https://stockanalysis.com/stocks/xom/
Small/mid leadership inside sector|MISS|0.50|2026-09-23|map_heat
High-beta leadership inside sector|MISS|0.50|2026-09-23|map_heat
Low-beta leadership inside sector|MISS|0.40|2026-09-23|map_heat
Sector ETF inflow / relative volume spike|MISS|0.70|2026-09-22|https://etfdb.com/etf/XLE/
Sector ETF outflow / volume dry-up|HIT|0.60|2026-09-22|https://etfdb.com/etf/XLE/
Crowded long (extreme relative performance + valuation)|MISS|0.75|2026-09-21|channel1
Index rebalance / inclusion tailwind|MISS|0.30|2026-09-23|
Index exclusion / forced selling|MISS|0.30|2026-09-23|
Crude oil price surge (WTI/Brent)|MISS|0.80|2026-09-23|https://www.investing.com/commodities/crude-oil
Natural gas price surge|MISS|0.70|2026-09-23|https://tradingeconomics.com/commodity/natural-gas
Inventory draw (EIA crude/products)|MISS|0.70|2026-09-23|https://www.eia.gov/petroleum/supply/weekly/
OPEC+ cut / supply discipline|MISS|0.65|2026-09-06|https://www.opec.org/pr-detail/1835613-6-september-2026.html
Crack spread / refining margin expansion|HIT|0.50|2026-09-23|https://tradingeconomics.com/commodity/te-crack-spread-index
Geopolitical supply risk premium|MISS|0.65|2026-09-23|https://www.reuters.com/business/energy/oil-falls-increased-gulf-supply-hopes-us-iran-talks-2026-09-23/
Crude price collapse|MISS|0.70|2026-09-23|https://www.investing.com/commodities/crude-oil
OPEC+ production increase / quota break|MISS|0.60|2026-09-06|https://www.energyconnects.com/news/oil/2026/september/opecplus-keeps-output-policy-unchanged-for-october/
Demand destruction (recession/China weak)|MISS|0.40|2026-09-23|
Inventory build|HIT|0.55|2026-09-22|https://oilprice.com/Latest-Energy-News/World-News/US-Gasoline-Distillate-Inventories-Continue-to-Fall-as-Crude-Stocks-Hold.html
Crack spread collapse|MISS|0.65|2026-09-23|https://tradingeconomics.com/commodity/te-crack-spread-index
Sector rotation into energy|MISS|0.55|2026-09-23|channel1
Sector rotation out of energy|MISS|0.50|2026-09-23|channel1
HIT_GRID_END

## RESEARCH APPENDIX

**Queries run**
- web_search: `WTI Brent crude oil price today September 23 2026`
- web_search: `EIA weekly petroleum status report September 23 2026 crude inventory forecast API`
- web_search: `XLE energy ETF premarket oil stocks Exxon Chevron September 23 2026`
- web_search: `OPEC+ meeting oil production Hormuz Strait tanker September 2026`
- web_search: `crack spread diesel gasoline refining margin September 23 2026`
- web_search: `WTI crude oil price September 23 2026 oilprice investing.com`
- web_search: `XLE ETF fund flows September 2026 energy sector inflows outflows`
- web_search: `Hormuz oil shipments Strait tanker traffic September 22 23 2026`
- web_search: `natural gas price Henry Hub September 23 2026`
- web_search: `Exxon Chevron ConocoPhillips premarket September 23 2026`
- web_search: `risk on Nasdaq record oil cools energy rotation September 23 2026`
- web_search: `oil prices reverse course US-Iran diplomacy September 23 2026 WTI Brent`
- web_search: `XLE 5 day ETF flows ETF Action September 2026`
- x_search: WTI/Brent/XLE/EIA/Hormuz/OPEC, 2026-09-21 to 2026-09-23
- web_fetch: oilprice.com API inventories article; eia.gov WPSR page; Morningstar/DJ EIA survey
- memory_search: paused (index metadata missing)

**Key sources (title + URL + timestamp where available) and facts taken**

1. Oilprice — “US Gasoline, Distillate Inventories Continue to Fall as Crude Stocks Hold” — https://oilprice.com/Latest-Energy-News/World-News/US-Gasoline-Distillate-Inventories-Continue-to-Fall-as-Crude-Stocks-Hold.html — **Sep 22, 2026, 4:08 PM CDT**. API week ending Sep 18: crude **+1.786 Mb** (prior week +7.14 Mb); Cushing **+2.082 Mb**; gasoline **−2.16 Mb**; distillates **−2.164 Mb**; SPR **−0.4 Mb** to 284.6 Mb. At 3:55 pm ET Wednesday: Brent **$97.85 (−2.48%)**, WTI **$89.14 (−3.50%)**.
2. Morningstar / Dow Jones — “Analysts See Weekly Decline in U.S. Crude Oil Stockpiles” — https://www.morningstar.com/news/dow-jones/202609226646/analysts-see-weekly-decline-in-us-crude-oil-stockpiles — **Sep 22, 2026 13:29 ET**. WSJ survey avg crude **−0.5 Mb** to 422.9 Mb (range −2.6 to +1.6); gasoline unch; distillates **−0.5 Mb**; EIA **10:30 a.m. ET Wednesday**.
3. EIA WPSR page — https://www.eia.gov/petroleum/supply/weekly/ — fetched **2026-09-23T11:48Z**. Release after 10:30 a.m.; Sep 23 format changes; **no printed week-ended-9/18 table yet** at fetch time.
4. Investing.com / web_search snapshot — https://www.investing.com/commodities/crude-oil — **Sep 23, 2026**. WTI ~**$89.98 (−0.60%)**, previous close **$90.52**, range 88.72–90.50.
5. Trading Economics / GuruFocus search snapshot — Brent ~**$99.12–99.37**; WTI ~**$89.4–90.1** on Sep 23.
6. Reuters — “Oil falls on increased Gulf supply, hopes for US-Iran talks” — https://www.reuters.com/business/energy/oil-falls-increased-gulf-supply-hopes-us-iran-talks-2026-09-23/ — **Sep 23, 2026**. Diplomacy + Gulf-flow hopes driving the fade.
7. IOL — “Oil prices slide as Trump signals very good US-Iran talks at UN” — https://iol.co.za/business/economy/2026-09-23-oil-prices-slide-as-trump-signals-very-good-us-iran-talks-at-un/ — **Sep 23, 2026**. Brent sub-$100; WTI ~$89–90.
8. Reuters Hormuz traffic — https://www.reuters.com/world/middle-east/hormuz-vessel-traffic-falls-two-data-shows-2026-09-22/ — **Sep 22, 2026**. Kpler: **2** commodity crossings Sep 22; Economic Times/NST: **3** on Sep 23, below ~15 10-day average.
9. OPEC — 6 Sep 2026 hold-October statement — https://www.opec.org/pr-detail/1835613-6-september-2026.html ; next meeting **Oct 4**.
10. ETFdb / ETF Action — https://etfdb.com/etf/XLE/ — ~Sep 22, 2026. XLE 5-day flows **~−$874M**, 1-month **~−$908M**.
11. MarketWatch / StockAnalysis premarket — XLE prior close **$61.78 (−1.09% on Sep 22)**; PM ~**$61.84–61.94 (+0.10% to +0.25%)**. XOM PM ~**+$0.3%**; CVX PM **+0.09–0.39%**; COP PM **−0.02% to −0.51%**.
12. Investopedia — futures little changed after Nasdaq record — https://www.investopedia.com/stock-market-today-futures-little-changed-after-nasdaq-closes-at-fresh-record-12136110 — **Sep 23**. Confirms leftover Nasdaq record vs flat open.
13. TE crack spread index — https://tradingeconomics.com/commodity/te-crack-spread-index — ~**77.70** on Sep 23 (elevated; refiner sleeve only).
14. Channel 1 (injected, unaltered): VIX 14.21, VIX/VIX3M 0.786, ES=F +0.03%, NQ=F −0.10%, PM:XLE +0.21%, DXY +0.38%, XLE vs SPY through **2026-09-21** (1d rel −3.85% leftover).

**Not used as live oil:** Channel 1 Finviz WTI $104.16 / Brent $107.67 and CL=F −4.97% (stale column + contract roll), per 08-11.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': -1.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.85, 'leading_sum': -3.0, 'divergence_flagged': False, 'total_score': -2.34, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.594, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': -0.0605, 'score': -0.363, 'legs': [{'leg': 'CL', 'pct': -1.59, 'w': 0.35}, {'leg': 'QA', 'pct': -1.02, 'w': 0.15}, {'leg': 'ES', 'pct': 0.03, 'w': 0.6}, {'leg': 'PM:XLE', 'pct': 0.21, 'w': 0.7}]}, 'overlay_score': -2.55, 'overlay_raw': -2.55, 'index_carry': 0.573, 'general_total': 2.293, 'skill_multipliers': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 1.0}, 'llm_confidence': 0.42, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
