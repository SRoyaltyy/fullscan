# Sector Prediction — Energy — 2026-09-18

- ETF: **XLE**
- rubric: `00_grounding/sectors/energy.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-3.733** (mult 0.85)
- regime: mixed
- divergence_flagged: **False**
- engine: v2 · tape_anchor **-2.398** (CL -1.59%, QA -1.02%, ES +1.14%, PM:XLE -0.56%) · index_carry **1.215** (general 4.861) · llm_overlay **-2.55** (raw -2.55)

## Channel 1 sector ETF tape

```
ETF XLE vs SPY (yfinance, through 2026-09-17):
  1d: XLE +0.70% | SPY +1.13% | rel -0.43%
  3d: XLE -0.08% | SPY +0.23% | rel -0.30%
  1w: XLE -0.69% | SPY +0.63% | rel -1.32%
  1m: XLE +1.26% | SPY -0.63% | rel +1.89%
```

MEMORY_CONFIRM: Energy/XLE — memory index unavailable this run (used injected scoreboard/lessons only). Last-10 dir=0.5 mag=0.4 (n=10); last-30 dir=0.5 mag=0.364 (n=22); last graded 09-17 up/mild vs XLE +0.70% (dir HIT, mag HIT). Open sector_energy experiment **does** apply: keep direction, shrink confidence after mag misses. Applied: **08-11 live-oil verify FIRES** — Channel 1 Finviz WTI $104.16 (−1.59%) / Brent $107.67 (−1.02%) is a leftover 09-16 column; CL=F −6.31% / BZ=F −6.17% **conflicts** with independent live quotes (WTI ~$100.0–$100.2 vs 09-17 settle ~$101.91, ~−1 to −2%; Brent ~$102–103 vs ~$104.82). Live sign **DOWN**, increment **sub-2%**, not a collapse. **08-14 green-oil does NOT fire.** **09-15 physical-increment notable license does NOT fire** (oil offered; Reuters: hopes of **limited** Saudi East-West disruption / workarounds). **09-14 backwardation debit does NOT fire** (VIX/VIX3M **0.82 contango**). **09-11 pending-binary flatten does NOT fire** (FOMC/SEP **already printed 09-16**; Finviz ES/NQ/RTY/DJIA are **not** all ≥ +0.5%). **09-10 crowded-long does NOT fire** (1m rel **+1.89%** ≪ +8%; 09-16 already de-risked). **09-04 S1+S4 aligned-negative does NOT fire** (1d rel **−0.43%**, not ≤ −1.5%). **09-17 leftover-S4 gate FIRES as a non-force**: do **not** reuse 09-16’s −2.44% rel; **but** today’s PM:XLE **−0.56%** with XOM/CVX/COP red **is** live extension, so 09-17’s “allow mixed/up when PM is flat” is **OFF**. **09-03 emit-signed-down FIRES**: live offered barrel + S4=0 because prior 1d is near-flat, not because PM is bid. **09-08/09-09 mag-discipline FIRES** (mag 0.36–0.40 < 0.4; oil not >5%; XLE PM not >2%). size_gate=True. EIA is **already in** 09-16’s tape (next WPSR **Sep 23**).

# Energy / XLE — 2026-09-18

This is a **third-session oil-fade**, not a fresh sector_shock and not a collapse. Yesterday’s object (post-FOMC repair, XLE +0.70% / rel −0.43% while oil stayed offered) **already printed**. This morning the barrel is **still offered ~1–2%**, XLE is **red in premarket (−0.56%)**, and the majors are **with the ETF**. News Judge #3 (oil drop / API-EIA build / DVN −5.6%) is the **09-16** hit — do not restack. Count the oil-down + geo-premium-fade cluster **once**. Do not rerun 09-16’s notable-down hindsight off CL=F −6.31%. Do not flip to up on a tech-led green tape that energy is not in.

## Channel 2

**1. Shared macro as it hits energy.** Tape is **mild risk-on, tech-led, not a commodity bid**: Finviz ES **+0.20%**, NQ **+0.41%**, RTY **+0.08%**, DJIA **+0.11%**. ES=F **+1.14%** / NQ=F **+1.50%** vs prior close is overnight index repair, not an energy impulse (same structure as 09-17). Premarket board: **XLK +0.60% / XLB +0.51% vs XLE −0.56%** — energy is the **only red sector**. Asia composite **+1.11%**, Europe **−0.46%**. **VIX 15.22 (−0.22) with VIX/VIX3M 0.82 — contango**, so the 09-14 full-weight de-risking debit is **off**. **USD strengthening** (DXY **+0.12%** 1d) is a mild producer headwind. **Real yields rising** (DFII10 **2.68, +0.06 1d / +0.22 1w**) — secondary vs oil. News Judge #1–#2 (Warsh JH / hike odds / BNY prime) are **already in 09-16’s price** — SPX duration, not a fresh XLE spine. Cheaper oil is SPX-positive inflation optics; that can leak beta **only if PM is participating**. It is not. **S0 = 0.**

**2. Spine (S1).** One cluster: live offered barrel **plus** geo-premium fade. Do not triple-count oil + pipeline-recovery + leftover inventory.
- **Crude: offered, not a surge, not a collapse.** Live-verified: WTI **~$100.0–$100.2** (~−1 to −2% vs 09-17 settle ~$101.91); Brent **~$102–103** (~−1 to −1.5% vs ~$104.82). Reuters (18 Sep): oil **falls ~1%** on hopes of limited supply disruptions. 08-11 passes on **sign** (DOWN) and **rejects** Channel 1 CL=F −6.31% / Finviz $104.16 as the live increment. Still ~$100 / ~$102 — a dip, not 08-25’s smash and not 09-15’s +2.3% surge.
- **Geo premium still present, not transmitting — fading.** East-West remains damaged (three pumping stations), but this morning’s object is **recovery/workaround**: half-capacity in days, full in ~six weeks, STS loadings off Oman, Wright-style “days not months.” Oil is **falling**, so this is fade/non-confirmation. **Do not** score Crude oil price surge. **Do not** score a fresh Geopolitical supply risk premium HIT. 09-15’s physical-increment license is **off**.
- **Inventory: already printed.** EIA week ended 9/11 (released **9/16**): crude **−0.64 Mb** vs ~−1.4 to −1.6 Mb expected (relative miss) to **423.4 Mb**; gasoline/distillates built. API **+7.1 Mb** was the 09-16 lean. Next WPSR **Sep 23**. Do **not** date it as today’s HIT. News Judge #3 / DVN −5.6% is leftover.
- **OPEC+ (carried offset):** Sep **+188 kb/d** completed the 2023 voluntary-cut rollback; **6 Sep** meeting **held October unchanged**. Next core-group meeting **Oct 4**. Not a cut, not a quota break.
- **Demand destruction (carried):** hawkish Fed/SEP is a medium-horizon demand lid, not a 1d HIT. IEA vs OPEC disagreement is leftover. Channel 2: **checked, nothing material** overnight.
- **Cracks:** diesel still extreme (Asia record >$87/bbl; US 3-2-1 ~$65–67). **Refiner sleeve only.** Today HO **+0.18%**, RBOB **−0.54%**, gasoil **−0.26%** — products are **not** a clean squeeze that can drive whole XLE. MAP HEAT Refining dir=up (MPC/VLO) is **nested** — dampen; **do not let VLO/MPC set the ETF while oil is offered**.
- **Nat gas ~$2.85–2.90 (−0.5 to −1.7%)** — no surge; N/A for oil-weighted XLE. **Checked, nothing material.**
- **MAP HEAT nested:** OFS/Drilling/Coal/Uranium **OVERRIDE down**; E&P/Integrated residual **up** on quiet/stale captains. Nested overrides **do not average into XLE**.

Net **S1 = −1**. Live oil sign is down once. Not −2: not a collapse, increment is ~1–2%, Hormuz still restricted, pipeline not fully restored, 08-27 forbids restoring −2 off a leftover print. Not 0: live offered barrel and PM confirmation. Not +1: geo is fading, not expanding.

**3. Breadth.** Channel 1 tape: XLE 1d rel **−0.43%** (XLE +0.70% vs SPY +1.13% on 09-17) — stalled/lag, not a smash. 3d rel −0.30%, 1w rel −1.32%, 1m rel **+1.89%**. **Live PM is the same-morning signal**: XLE **−0.56%**; **XOM ~−0.91%, CVX ~−0.88%, COP ~−1.25%** — large-caps are **fading with oil**, not carrying a bounce and not an ETF-only print. That is constituent confirmation of the oil-down, **not** independent breadth expansion. Do **not** copy 1w/1m into S2. Do **not** score the ETF’s own PM gap as S2 (09-14). **S2 = 0.**

**4. Flows / positioning.** XLE 5d outflow ~**−$296M**, week ~**−$157M**, 1m modest ~**−$90M**. Trailing unit outflows are **not** a 1-day lid (08-28). 1m rel **+1.89%** is **not** the 09-10 crowded-long trigger (≥ +8% / record-close sequence). 09-16 already washed the Hormuz run. **S3 = 0.**

**5. Catalysts.** No crude EIA today (next **Sep 23**). No fresh XLE-wide earnings (**checked, nothing material**). FOMC/SEP **already printed 09-16**. OPEC+ next **Oct 4**. Pipeline-repair headlines are the fade binary, already in the offered barrel.

### Scoring logic

S0 muted: mild green ES/NQ is tech, not energy; USD-up / real-yield backup are secondary vs oil; 09-11’s green-futures tailwind does **not** fire (no pending binary; Finviz futures not all ≥ +0.5%; PM is red).

S1 is the live oil-offered + geo-fade cluster, netted once.

S2 is not a second copy of the ETF print. S4 is Channel 1 confirmation only: 1d rel **−0.43%** is **sub-threshold** (not ≤ −1.5%). 09-17 forbids scoring leftover 09-16 rel −2.44% as live S4. **S4 = 0.**

**No leading-vs-tape divergence** — factors lean down; tape is near-flat leftover, not a fight. Trust the live barrel over the 09-17 repair close. 09-03: a flat prior-day tape is a **neutral starting point**, not a veto of a live offered barrel. PM **−0.56%** with majors red is the knowable-at-open extension.

Magnitude discipline: mag hit-rate **0.36–0.40**, size_gate=True, oil not >5%, XLE PM not >2%. Cap **mild**. Open experiment: **keep direction, shrink confidence**.

Knowable-at-open: every bullish item (cracks, nested refiners, mild ES) is already-printed and **not transmitting** to XLE. The only forward energy print is **EIA Sep 23** (not today). Base case is oil-fade continuation, not a risk-on catch-up.

Self-audit: lens = XLE not SPX; band = mild; skew = refiners damped; same-shock counted once; no single-ticker (DVN leftover, MAP HEAT nested) driving the ETF.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: -1
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.85
CONFIDENCE: 0.45
REGIME: mixed
DIVERGENCE_FLAGGED: false
HORIZON_3D: down
HORIZON_1W: mixed
HORIZON_2W: mixed
HORIZON_1M: mixed
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|PARTIAL|0.55|2026-09-18|https://www.marketwatch.com/investing/fund/xle
Risk-off tape / flight to safety|MISS|0.70|2026-09-18|
Real yields rising|HIT|0.65|2026-09-18|
Real yields falling|MISS|0.70|2026-09-18|
USD strengthening|HIT|0.55|2026-09-18|
USD weakening|MISS|0.70|2026-09-18|
Sector breadth expansion (% names up)|MISS|0.70|2026-09-18|
Sector breadth failure (ETF up, names flat)|MISS|0.60|2026-09-18|
Large-cap leadership inside sector|PARTIAL|0.60|2026-09-18|
Small/mid leadership inside sector|MISS|0.50|2026-09-18|
High-beta leadership inside sector|MISS|0.55|2026-09-18|
Low-beta leadership inside sector|MISS|0.50|2026-09-18|
Sector ETF inflow / relative volume spike|MISS|0.60|2026-09-18|https://etfdb.com/etf/XLE/
Sector ETF outflow / volume dry-up|PARTIAL|0.55|2026-09-18|https://etfdb.com/etf/XLE/
Crowded long (extreme relative performance + valuation)|MISS|0.75|2026-09-18|
Index rebalance / inclusion tailwind|MISS|0.80|2026-09-18|
Index exclusion / forced selling|MISS|0.80|2026-09-18|
Crude oil price surge (WTI/Brent)|MISS|0.85|2026-09-18|https://www.reuters.com/business/energy/oil-prices-fall-1-hopes-limited-supply-disruptions-2026-09-18/
Natural gas price surge|MISS|0.80|2026-09-18|https://tradingeconomics.com/commodity/natural-gas
Inventory draw (EIA crude/products)|MISS|0.80|2026-09-18|https://www.eia.gov/petroleum/supply/weekly/
OPEC+ cut / supply discipline|MISS|0.75|2026-09-18|https://asia.nikkei.com/business/energy/opec-keeps-oil-output-policy-unchanged-for-october
Crack spread / refining margin expansion|PARTIAL|0.60|2026-09-18|https://www.oilpriceapi.com/crack-spread
Geopolitical supply risk premium|MISS|0.70|2026-09-18|https://www.reuters.com/business/energy/oil-prices-fall-1-hopes-limited-supply-disruptions-2026-09-18/
Crude price collapse|MISS|0.75|2026-09-18|
OPEC+ production increase / quota break|MISS|0.70|2026-09-18|
Demand destruction (recession/China weak)|MISS|0.55|2026-09-18|
Inventory build|MISS|0.70|2026-09-18|https://www.eia.gov/petroleum/supply/weekly/
Crack spread collapse|MISS|0.70|2026-09-18|
Sector rotation into energy|MISS|0.75|2026-09-18|
Sector rotation out of energy|HIT|0.65|2026-09-18|
HIT_GRID_END

## RESEARCH APPENDIX

**Queries run**
- WTI crude oil price today September 18 2026
- Brent crude oil price today September 18 2026
- EIA weekly petroleum status report crude inventories September 2026
- XLE energy ETF premarket oil drop September 18 2026
- oil prices fall hopes limited supply disruptions Saudi pipeline September 18 2026
- XLE ETF flows holdings XOM CVX COP premarket September 18 2026
- OPEC+ production October 2026 meeting oil supply
- crack spread diesel gasoline refining margins September 2026
- CME FedWatch September 2026 rate hike odds Warsh
- Exxon Chevron ConocoPhillips stock premarket September 18 2026
- natural gas price Henry Hub September 18 2026
- XLE ETF fund flows September 2026 outflow
- X search: WTI Brent crude oil price today XLE energy stocks September 18 2026
- Fetches (blocked/JS): reuters.com oil-prices-fall-1-… ; marketwatch.com CL.1 ; thevibes.com oil-prices-fall-for-third-session…

**Key sources (title + URL + timestamp/facts taken)**
- Reuters — “Oil prices fall 1% on hopes of limited supply disruptions” — https://www.reuters.com/business/energy/oil-prices-fall-1-hopes-limited-supply-disruptions-2026-09-18/ — 2026-09-18: Brent/WTI ~−1%; Saudi East-West recovery/workarounds (STS off Oman) outweigh fresh Houthi copy; third down session.
- MarketWatch / WSJ futures quotes (via search) — https://www.marketwatch.com/investing/future/cl.1/download-data — WTI ~$100.13–$100.16 vs 09-17 settle ~$101.91.
- Investing.com / Tindex Brent — https://hk.investing.com/commodities/brent-oil-historical-data — Brent ~$102–103 vs ~$104.82 prior settle.
- EIA WPSR — https://www.eia.gov/petroleum/supply/weekly/ — week ended 2026-09-11, released 2026-09-16: commercial crude 423.4 Mb, −0.6 Mb; next release 2026-09-23.
- Rigzone EIA recap — https://www.rigzone.com/news/usa_crude_oil_stocks_drop_week_on_week-17-sep-2026-184643-article/ — 2026-09-17: −0.6 Mb draw, ~1% above 5-year avg.
- Nation Thailand / The Vibes recaps of Reuters — pipeline half-capacity in days, full ~six weeks; third-session oil decline.
- ETFdb / Nasdaq outflow alert — https://etfdb.com/etf/XLE/ — ~−$296M 5d, ~−$90M 1m, ~−$157M week to 09-17.
- MarketWatch/StockAnalysis premarket — XOM ~$161.79 (−0.91%), CVX ~$209.70 (−0.88%), COP ~$131.52 (−1.25%); XLE ~$64.00 (−0.73% vs $64.48).
- Nikkei/Rigzone OPEC+ — https://asia.nikkei.com/business/energy/opec-keeps-oil-output-policy-unchanged-for-october — 6 Sep: October unchanged; next meeting 4 Oct 2026.
- OilPriceAPI crack spread — https://www.oilpriceapi.com/crack-spread — US 3-2-1 ~$65–67/bbl as of ~18 Sep; diesel cracks still extreme.
- TradingEconomics / World Oil Monitor — Henry Hub NG ~$2.85–2.86, down ~1.4–1.7%.
- NYT/Axios Fed — 16 Sep 2026: 25 bp hike to 3.75–4.00%; Warsh hawkish path already printed.

**Facts used vs discarded**
- Used: live WTI/Brent sign DOWN ~1–2%; XLE PM −0.56% with XOM/CVX/COP red; VIX/VIX3M 0.82 contango; 1m rel +1.89%; EIA stale until 9/23; OPEC+ Oct unchanged; geo premium fading on repair headlines; refiners nested only.
- Discarded as stale/conflicting: Channel 1 Finviz WTI $104.16 and CL=F −6.31% as the live increment (08-11); News Judge oil/DVN line as a same-morning HIT; MAP HEAT refining/integrated residual as XLE spine.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': -1.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.85, 'leading_sum': -3.0, 'divergence_flagged': False, 'total_score': -3.733, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.649, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': -0.3997, 'score': -2.398, 'legs': [{'leg': 'CL', 'pct': -1.59, 'w': 0.35}, {'leg': 'QA', 'pct': -1.02, 'w': 0.15}, {'leg': 'ES', 'pct': 1.14, 'w': 0.6}, {'leg': 'PM:XLE', 'pct': -0.56, 'w': 0.7}]}, 'overlay_score': -2.55, 'overlay_raw': -2.55, 'index_carry': 1.215, 'general_total': 4.861, 'skill_multipliers': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 1.0}, 'llm_confidence': 0.45, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
