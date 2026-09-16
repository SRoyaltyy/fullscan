# Sector Prediction — Energy — 2026-09-16

- ETF: **XLE**
- rubric: `00_grounding/sectors/energy.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-2.88** (mult 0.85)
- regime: mixed
- divergence_flagged: **False**
- engine: v2 · tape_anchor **-2.398** (CL -1.59%, QA -1.02%, ES +1.14%, PM:XLE -0.56%) · index_carry **1.324** (general 5.297) · llm_overlay **-1.806** (raw -1.806)

## Channel 1 sector ETF tape

```
ETF XLE vs SPY (yfinance, through 2026-09-15):
  1d: XLE +2.17% | SPY -0.46% | rel +2.63%
  3d: XLE +1.54% | SPY -0.06% | rel +1.60%
  1w: XLE +1.79% | SPY -1.12% | rel +2.91%
  1m: XLE +6.49% | SPY -2.44% | rel +8.93%
```

MEMORY_CONFIRM: Energy/XLE — memory index unavailable this run (used injected scoreboard/lessons only). Last-10 dir=0.4 mag=0.4 (n=10); last-30 dir=0.45 mag=0.35 (n=20); last graded 09-15 up/mild vs XLE +2.17% (dir HIT, mag MISS). Open sector_energy experiment **does** apply: keep direction, shrink confidence after mag misses; 09-14 DO-INSTEAD (score vs tape conflict → prefer flat/mild) also in force. Applied: **08-11 live-oil verify FIRES** — Channel 1 CL=F −2.36% / BZ=F −1.5% agrees with Finviz WTI $104.16 (−1.59%) / Brent $107.67 (−1.02%) and CNBC WTI −1.29% to $104.46 / Brent −1.02% to $107.64; sign **DOWN**. **08-14 green-oil does NOT fire.** **09-15 physical-increment notable license does NOT fire** (oil is offered; News Judge: no kinetic/oil increment; Wright “days” is a premium-fade, not a new outage). **09-14 backwardation debit does NOT fire** (VIX/VIX3M 0.877 contango). **09-11 Energy FIRES** (FOMC binary pending; ES +1.14% / NQ +1.50% both ≥ +0.5%; barrel is a ~1.3–2.4% dip, not a >5% break — S0 must not go negative; dip sets relative sign, not absolute down). **09-10 crowded-long FIRES as fragility** (1m rel +8.93% ≥ +8%) but 09-15 exemption (no same-session S3 debit while crude ≥ +2%) is **off**. **09-03/09-04 exhaustion does NOT fully fire** (1w rel +2.91%, not >+4%; 1d rel +2.63% is leftover leadership, not a stalled/≤ −1.5% tape). **08-12 stale-surge cap does NOT fire** (1w rel not >+4%). API +7.1 Mb is a lean, not today’s EIA HIT (WPSR 10:30 ET unprinted). No double-count of oil-offered + API + Wright fade.

# Energy / XLE — 2026-09-16

This is **not** a 09-15 sector_shock sequel. Yesterday’s object (East-West still shut, oil >+2%, XLE +2.17% / rel +2.63%) is **already in the price**. This morning the barrel is **offered**, XLE is **red in premarket**, API printed a **surprise crude build**, and the only same-session hard energy print (EIA 10:30) is unprinted. The broad tape is **mildly risk-on into FOMC 14:00 ET**, VIX term structure is **contango**, and News Judge explicitly has **no kinetic/oil increment**. Count the oil-down cluster **once**. Do not rerun 09-15’s up/notable book. Do not manufacture a collapse.

## Channel 2

**1. Shared macro as it hits energy.** Equity tape is **risk-on beta, not a commodity bid**: Channel 1 ES +1.14%, NQ +1.50%; Finviz ES/NQ/RTY/DJIA all green but smaller (+0.20% / +0.41% / +0.08% / +0.11%). Asia composite **+0.65%**, Europe **+0.45%**. **VIX 16.98 (−0.22) with VIX/VIX3M 0.877 — contango**, so the 09-14 full-weight de-risking debit is **off**. USD is a non-event (DXY +0.02% / Finviz USD −0.02%). DFII10 **2.60, 1d 0.0** — real yields are a 1w leftover (+0.17), not a live impulse; secondary vs oil. News Judge #1–#3 are the **hawkish Fed / Warsh / 10Y** cluster already in SPX duration — FOMC/SEP is a **two-sided 14:00 binary**, not an XLE spine and **not pre-scored**. 09-11: green futures ≥ +0.5% across the board is a **tailwind for the absolute print of a beta/commodity sector**; do **not** assign S0 a negative sign. Oil offered **caps participation**, so this is not a full cyclical bid. **S0 = +0.5.**

**2. Spine (S1).** One cluster: live offered barrel **plus** inventory-build lean **plus** geo-premium fade. Do not triple-count.
- **Crude: offered, not a surge, not a collapse.** Live-verified: Finviz WTI **$104.16 (−1.59%)**, Brent **$107.67 (−1.02%)**; CL=F **−2.36%**, BZ=F **−1.5%**; CNBC WTI **−1.29% to $104.46**, Brent **−1.02% to $107.64**. Still $104 / $107 — a dip, not 08-25’s smash and not 09-15’s +2.3% surge. 08-11 passes; Channel 1 and live sign **agree DOWN**.
- **Geo premium still present, not transmitting.** East-West remains the multi-day constraint, but Wright (15 Sep, CNBC) called it a **“brief interruption… measured in days”** vs Lipow “months.” Oil is **falling**, so this is fade/non-confirmation — **do not** score Crude oil price surge or a fresh Geopolitical supply risk premium HIT. 09-15’s physical-increment license is **off**.
- **Inventory: API build is a lean; EIA is unprinted.** API week ending 9/11: crude **+7.1 Mb vs ~−1.6 Mb expected**; gasoline and distillates also built (Reuters/CNBC). Last official EIA (week ending 9/4, released 9/10) was a **tiny −0.4 Mb** to ~424.1 Mb. **Today’s WPSR 10:30 ET is two-sided until it prints.** Do not date API as a full Inventory-build HIT; do not ignore it either — it is the knowable-at-open reason the barrel is offered. Counted **inside** the oil-down cluster, not as a second spine.
- **OPEC+ (carried offset):** Sep **+188 kb/d** completed the 2023 voluntary-cut rollback; 6 Sep meeting **held October unchanged**. Not a cut, not a quota break.
- **Demand destruction (carried official):** IEA 2026 bearish vs OPEC still constructive. Offset only; not today’s HIT.
- **Cracks:** Asia diesel margins at records (~$87/bbl, Reuters 16 Sep); US 3-2-1 still ~$61–65. **Refiner sleeve only.** Today HO **+0.18%**, RBOB **−0.54%**, gasoil **−0.26%** — products are **not** a clean squeeze that can drive whole XLE. MAP HEAT Refining dir=up is nested (MPC/VLO) — dampen; do not let VLO/MPC set the ETF.
- **Nat gas $2.902 (−0.51%)** — no surge; N/A for oil-weighted XLE.
- **MAP HEAT nested:** E&P/Integrated residual **up** (COP silent; XOM LNG color); OFS/Drilling/Coal/Uranium **OVERRIDE down**. Nested overrides **do not** average into XLE and **do not** set S1.

Net **S1 = −1**. Live oil sign is down once. Not −2: not a collapse, Hormuz/pipeline still constrained, EIA unprinted, 09-11 forbids letting a ~2% dip set absolute direction, 08-27 forbids restoring −2 off leftover geo. Not 0: WTI −1.6% / CL −2.4% plus API +7.1 Mb is a real offered barrel and 08-14’s green-oil license is off. Not +1: 09-15’s physical shock is yesterday’s object.

**3. Breadth.** Channel 1 1d rel **+2.63%** is **yesterday’s trend day** — already printed; do **not** copy it into S2 (09-10/09-14/09-15: one prior-close series may not buy two positive scores). **Live** tape: Channel 1 PM:XLE **−0.56%**; XOM ~−0.5% to −0.8%, CVX ~−0.3% to −0.5%, COP softer. Large-caps are **fading with oil**, not carrying a new up day and not a confirmed smash. MAP HEAT mixed (E&P/integrated residual bid vs OFS/drilling override) is nested color. **S2 = 0.**

**4. Flows / positioning.** ETFDB through ~15 Sep: XLE **5d −$112M / 1m −$36M / 3m −$1.14B** (1y still +$2.6B). 1m rel **+8.93%** after yesterday’s +2.17% **is the crowded-long condition** — do not gate it on 1w rel >+5% (09-10). 09-15’s “no same-session S3 debit while crude ≥ +2%” is **off** because crude is falling. This is unwind **risk**, not a 1-day lid. Trailing unit outflows are not independently scored on top. **S3 = −0.5.**

**5. Catalysts.** **FOMC/SEP 14:00 ET** is the dominant binary — two-sided, not the energy spine. **EIA WPSR 10:30 ET** is the only same-session energy print (API already leaned build). No fresh XLE-wide earnings. Wright restart vs “months” remains a **fade binary**, not an oil-up license at 9:30.

### Scoring logic

S0 modestly **positive**: 09-11 forbids a negative S0 on a green-futures FOMC morning for a beta sector; USD/real yields are not a live impulse; contango kills the 09-14 de-risking debit. Oil offered keeps it at +0.5, not +1.

S1 is the live oil-offered + API-build cluster, netted **once**, with carried OPEC+/IEA and still-extreme cracks as dampeners only.

S2/S4 do **not** re-vote 09-15. S4 is confirmation of **this** session’s Channel 1 series only as a leftover: 1d rel +2.63% would be +1 if treated as live, but it is yesterday’s transmission. Live PM:XLE −0.56% is the open tape. **S4 = 0** (not +1 leftover, not −1 — Channel 1 1d is not ≤ −1.5%).

**Leading-vs-tape divergence: YES.** Factors net modestly down (oil offered, crowded, PM red). Channel 1 1d/3d/1w rel is still green leftover. Trust **factors over leftover tape**. Live PM agrees with factors. 09-11: when factors lean down against a risk-on FOMC tape and the barrel is a dip not a break, **do not let S1 force an absolute down close** — mixed signs belong in the flat band. 09-04’s must-be-down rule does **not** fire (S4 is not −1; 1d rel is not ≤ −1.5%). 09-03’s exhaustion rule does **not** fully fire (1w rel +2.91% not >+4%).

Magnitude: Energy mag hit-rate 0.4 / 0.35; FOMC binary pending; oil dip <5%; XLE PM only −0.56% (not >2% either way). Cap conviction. Multiplier **0.85**. Do not emit notable/severe.

Self-audit: energy lens only; mild band; refiners damped; oil+API+Wright counted once; no single-ticker (XOM/VLO/BKR) driving XLE.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0.5
S1_SECTOR_FACTORS: -1
S2_BREADTH: 0
S3_FLOWS_POSITIONING: -0.5
S4_ETF_TAPE: 0
MULTIPLIER: 0.85
CONFIDENCE: 0.48
REGIME: mixed
DIVERGENCE_FLAGGED: true
HORIZON_3D: fade/two-sided — offered barrel + API build vs still-elevated $104/$107 and unprinted EIA; FOMC 14:00 caps extension either way
HORIZON_1W: mild mean-reversion risk after 09-15 +2.17% and 1m rel +8.93% unless a fresh kinetic increment re-bids oil
HORIZON_2W: geo floor intact if East-West stays offline / Hormuz impaired; Wright “days” is the fade path
HORIZON_1M: crowded 1m rel +8.93% is fragility, not a same-week long; needs a new supply step-change to extend
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|HIT|0.72|2026-09-16|channel1 ES+1.14% NQ+1.50%
Risk-off tape / flight to safety|MISS|0.70|2026-09-16|VIX/VIX3M 0.877 contango; VIX 16.98 −0.22
Real yields rising|MISS|0.65|2026-09-16|DFII10 2.60 1d 0.0 (1w +0.17 leftover)
Real yields falling|MISS|0.65|2026-09-16|DFII10 1d unchanged
USD strengthening|MISS|0.70|2026-09-16|DXY +0.02% / Finviz USD −0.02%
USD weakening|MISS|0.70|2026-09-16|USD flat
Sector breadth expansion (% names up)|MISS|0.68|2026-09-16|PM XLE −0.56%; XOM/CVX/COP red with oil
Sector breadth failure (ETF up, names flat)|MISS|0.60|2026-09-16|ETF itself red premarket; not an ETF-up/names-flat day
Large-cap leadership inside sector|PARTIAL|0.55|2026-09-16|MAP HEAT integrated/E&P residual up; live XOM/CVX/COP fading
Small/mid leadership inside sector|MISS|0.50|2026-09-16|checked, nothing material
High-beta leadership inside sector|MISS|0.50|2026-09-16|checked, nothing material
Low-beta leadership inside sector|MISS|0.50|2026-09-16|checked, nothing material
Sector ETF inflow / relative volume spike|MISS|0.62|2026-09-16|ETFDB 5d −$112M
Sector ETF outflow / volume dry-up|PARTIAL|0.58|2026-09-16|5d −$112M / 3m −$1.14B; 1m only −$36M
Crowded long (extreme relative performance + valuation)|HIT|0.74|2026-09-16|1m rel +8.93% after 09-15 +2.17%
Index rebalance / inclusion tailwind|MISS|0.40|2026-09-16|checked, nothing material
Index exclusion / forced selling|MISS|0.40|2026-09-16|checked, nothing material
Crude oil price surge (WTI/Brent)|MISS|0.85|2026-09-16|WTI −1.59% / Brent −1.02% / CL −2.36%
Natural gas price surge|MISS|0.80|2026-09-16|NG −0.51%
Inventory draw (EIA crude/products)|MISS|0.70|2026-09-16|last EIA −0.4 Mb (we 9/4); today unprinted
OPEC+ cut / supply discipline|MISS|0.75|2026-09-16|Oct quotas unchanged after Sep +188 kb/d
Crack spread / refining margin expansion|PARTIAL|0.66|2026-09-16|Asia diesel record ~$87; US 3-2-1 ~$61–65; HO +0.18% RBOB −0.54% — refiner sleeve only
Geopolitical supply risk premium|PARTIAL|0.60|2026-09-16|East-West still offline but Wright “days”; oil falling = non-confirmation
Crude price collapse|MISS|0.80|2026-09-16|WTI still ~$104; dip not collapse
OPEC+ production increase / quota break|PARTIAL|0.55|2026-09-16|Sep +188 kb/d already done; Oct held
Demand destruction (recession/China weak)|MISS|0.50|2026-09-16|carried IEA only; not a live HIT
Inventory build|PARTIAL|0.72|2026-09-16|API +7.1 Mb vs −1.6e; EIA 10:30 unprinted
Crack spread collapse|MISS|0.70|2026-09-16|cracks still extreme
Sector rotation into energy|MISS|0.70|2026-09-16|PM XLE −0.56% vs XLK +0.65%; leftover 1d rel is yesterday
Sector rotation out of energy|PARTIAL|0.62|2026-09-16|live PM fade vs green ES/NQ; not a confirmed session rotation until cash
HIT_GRID_END

## RESEARCH APPENDIX

**Queries run**
- WTI Brent crude oil price today September 16 2026
- EIA weekly petroleum status report crude inventories September 2026
- OPEC+ production meeting oil supply September 2026
- XLE energy ETF premarket XOM CVX COP September 16 2026
- Saudi East-West pipeline outage Hormuz oil supply September 2026
- XLE ETF fund flows September 2026
- crack spread diesel gasoline refining margins September 16 2026
- oil prices fall September 16 2026 WTI Brent pullback
- EIA weekly petroleum status report September 16 2026 release
- Finviz WTI crude oil Brent quote September 16 2026
- XOM CVX COP stock premarket September 16 2026
- API crude oil inventory build 7.1 million barrels September 15 2026
- US Energy Secretary Wright Saudi pipeline back online days September 2026
- risk on equity futures September 16 2026 Fed meeting
- X-search: WTI Brent crude oil price pullback inventory API EIA September 16 2026

**Key sources (title + URL + timestamp where available)**
- CNBC, “Oil falls as U.S. crude inventories reportedly rise, traders weigh Saudi pipeline closure” — https://www.cnbc.com/2026/09/16/oil-prices-today-brent-wti-hormuz-iran-war.html — fetched 2026-09-16T11:49Z. Facts: Brent Nov −1.02% to $107.64; WTI Oct −1.29% to $104.46; API crude +7.1 Mb (we 9/11) vs ~−1.6 Mb expected; gasoline and distillates also built; Wright “brief interruption… days” vs Lipow “months.”
- Reuters search cluster / Seeking Alpha — https://seekingalpha.com/news/4643199-u-s-crude-stockpiles-rose-7_1m-barrels-last-week-api-says — API +7.14 Mb, vs prior week −0.3 Mb.
- EIA WPSR page — https://www.eia.gov/petroleum/supply/weekly/ — fetched 2026-09-16T11:47Z. Facts: next/this release 10:30 ET; prior we 9/4 commercial crude ~424.1 Mb, −0.4 Mb.
- OPEC 6 Sep 2026 — https://www.opec.org/pr-detail/1835613-6-september-2026.html and Energy Connects — Oct quotas unchanged after Sep +188 kb/d.
- Reuters (13 Sep) Saudi pipeline ~4% global supply — https://www.reuters.com/business/energy/saudi-pipeline-outage-threatens-loss-4-global-oil-supply-2026-09-13/
- Reuters (15 Sep) Wright “within days” — https://www.reuters.com/business/energy/us-energy-chief-says-saudi-arabia-oil-pipeline-should-be-back-within-days-2026-09-15/
- Reuters (16 Sep) Asia diesel refining margins record >$87/bbl — https://www.reuters.com/business/energy/asia-diesel-refining-margins-record-high-more-than-87-barrel-data-shows-2026-09-16/
- ETFDB XLE flows — https://etfdb.com/etf/XLE/ — 5d −$111.81M, 1m −$35.84M, 3m −$1.14B, 1y +$2.63B (as of ~15 Sep).
- MarketWatch/premarket summaries — XLE prior close $65.93 (+2.17% on 15 Sep); PM ~$65.52–65.68 (−0.4% to −0.6%); XOM prior $169.32 PM ~−0.5% to −0.8%; CVX prior $217.77 PM ~−0.3% to −0.5%.
- Channel 1 (injected, unaltered) — Finviz WTI $104.16 (−1.59%), Brent $107.67 (−1.02%); CL=F −2.36%; BZ=F −1.5%; ES=F +1.14%; NQ=F +1.50%; PM:XLE −0.56%; VIX 16.98, VIX/VIX3M 0.877; XLE vs SPY through 15 Sep: 1d rel +2.63%, 1w rel +2.91%, 1m rel +8.93%.
- News Judge 2026-09-16 — ranked set is Fed/hike/Warsh/IWM/ASML/Adobe; **“no kinetic/oil increment.”**

**Facts taken**
- Live oil sign DOWN (~−1.0% to −2.4%), still ~$104 WTI / ~$107 Brent — dip not collapse.
- API +7.1 Mb crude build is the knowable inventory lean; EIA not printed at 07:44 ET snapshot.
- Geo premium fading at the margin (Wright days) with no fresh kinetic HIT today.
- XLE PM red with majors; leftover 1d rel +2.63% is yesterday.
- Flows still leaking on 5d/3m; 1m rel crowded.
- Macro: green futures into FOMC, VIX contango, USD/real yields flat on the day.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.5, 'S1_SECTOR_FACTORS': -1.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': -0.5, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.85, 'leading_sum': -2.0, 'divergence_flagged': False, 'total_score': -2.88, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.615, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': -0.3997, 'score': -2.398, 'legs': [{'leg': 'CL', 'pct': -1.59, 'w': 0.35}, {'leg': 'QA', 'pct': -1.02, 'w': 0.15}, {'leg': 'ES', 'pct': 1.14, 'w': 0.6}, {'leg': 'PM:XLE', 'pct': -0.56, 'w': 0.7}]}, 'overlay_score': -1.806, 'overlay_raw': -1.806, 'index_carry': 1.324, 'general_total': 5.297, 'skill_multipliers': {'S0_SHARED_MACRO': 1.25, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 1.0}, 'llm_confidence': 0.48, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
