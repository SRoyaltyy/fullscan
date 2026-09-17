# Sector Prediction — Energy — 2026-09-17

- ETF: **XLE**
- rubric: `00_grounding/sectors/energy.md`
- predicted_direction: **up**
- predicted_magnitude_band: **mild**
- total_score: **0.202** (mult 0.85)
- regime: mixed
- divergence_flagged: **True**
- engine: v2 · tape_anchor **0.906** (CL -1.59%, QA -1.02%, ES +1.71%, PM:XLE +0.08%) · index_carry **1.846** (general 7.383) · llm_overlay **-2.55** (raw -2.55)

## Channel 1 sector ETF tape

```
ETF XLE vs SPY (yfinance, through 2026-09-16):
  1d: XLE -2.88% | SPY -0.44% | rel -2.44%
  3d: XLE -1.70% | SPY -1.34% | rel -0.36%
  1w: XLE -1.96% | SPY -1.10% | rel -0.86%
  1m: XLE +2.32% | SPY -2.41% | rel +4.73%
```

MEMORY_CONFIRM: Energy/XLE — memory index unavailable this run (used injected scoreboard/lessons only). Last-10 dir=0.4 mag=0.4 (n=10); last-30 dir=0.476 mag=0.333 (n=21); last graded 09-16 down/mild vs XLE −2.88% (dir HIT, mag MISS). Open sector_energy experiment **does** apply: keep direction, shrink confidence after mag misses. Applied: **08-11 live-oil verify FIRES** — Finviz WTI $104.16 (−1.59%) / Brent $107.67 (−1.02%) is a leftover 09-16 snapshot; live-verified sign is still **DOWN** (CL=F −0.98% / BZ=F −1.30%; WTI ~$101.8 vs 09-16 settle ~$102.43; Brent ~$104–105 vs ~$105.83). **08-14 green-oil does NOT fire.** **09-15 physical-increment notable license does NOT fire** (oil offered; News Judge: no fresh kinetic). **09-14 backwardation debit does NOT fire** (VIX/VIX3M **0.813 contango**). **09-11 pending-binary flatten does NOT fire** (FOMC/SEP **already printed 09-16**; Finviz ES/NQ/RTY/DJIA are **not** all ≥ +0.5%). **09-10 crowded-long does NOT fire** (1m rel **+4.73%** < +8%; 09-16 −2.44% rel already de-risked). **09-03/09-04 exhaustion 1w>+4%/1m>+10% does NOT fire** (1w rel **−0.86%**). **09-04 S1+S4 aligned-negative rule DOES fire** (1d rel **−2.44%** ≤ −1.5%) → direction **down**, mag **mild**. **09-08/09-09 mag-discipline FIRES** (mag ~0.33–0.40 < 0.4; oil not >5%; XLE PM not >2%). size_gate=True. EIA/API inventory is **already in** 09-16’s tape, not today’s print.

# Energy / XLE — 2026-09-17

This is **not** a fresh sector_shock and **not** a collapse. Yesterday’s object (offered barrel + API/EIA miss + hawkish FOMC) **already printed** in XLE **−2.88% / rel −2.44%**. This morning the barrel is **still offered**, but the **incremental** oil print is sub-1.5%, XLE premarket is **flat (+0.08%)**, and the majors are not extending the smash. Count the oil-down cluster **once**. Do not rerun 09-16’s notable-down hindsight. Do not flip to up on a tech-led bounce that energy is not in.

## Channel 2

**1. Shared macro as it hits energy.** Tape is a **post-FOMC relief bounce, not a commodity bid**: Finviz ES **+0.20%**, NQ **+0.41%**, RTY **+0.08%**, DJIA **+0.11%**; ES=F **+1.71%** / NQ=F **+2.10%** vs prior close is the overnight repair of 09-16’s dump, not a new energy impulse. Asia composite **−0.03%**, Europe **+0.45%**. **VIX 16.04 (−1.67) with VIX/VIX3M 0.813 — deep contango**, so the 09-14 full-weight de-risking debit is **off**. USD is a non-event (DXY **−0.13%** / Finviz USD **−0.02%**) — mild producer tailwind at most. DFII10 **2.62, +0.02 1d / +0.19 1w** — real yields are a leftover 1w backup, not a live impulse; **secondary vs oil**. News Judge #1–#2 (warm retail, Warsh/hike odds) and the **25 bp hike to 3.75–4.00%** are **already in yesterday’s price** — do not pre-score an unprinted FOMC and do not restack them as a fresh energy spine. Premarket sector board: **XLK +1.28% / XLY +0.61% vs XLE +0.08%** — risk-on is **tech**, not energy. Risk-on is a mild cyclical plus in the taxonomy, but it is **not transmitting** to this sector. **S0 = 0.**

**2. Spine (S1).** One cluster: live offered barrel **plus** leftover inventory/Fed demand-moderation **plus** geo-premium non-transmission. Do not triple-count.
- **Crude: offered, not a surge, not a collapse.** Live-verified: CL=F **−0.98%**, BZ=F **−1.30%**; independent quotes WTI **~$101.8** / Brent **~$104–105** vs 09-16 settles **~$102.43 / ~$105.83**. Finviz **$104.16 / $107.67** is the stale 09-16 column — **do not use it as the live level**. Sign is **DOWN**. Not 09-15’s +2.3% surge. Not 08-25’s smash. Incremental move is **sub-1.5%**.
- **Geo premium still present, not transmitting.** Hormuz traffic remains crushed; tanker rates/war-risk premia are still extreme. Oil is **falling**, so this is fade/non-confirmation. News Judge: kinetic Iran/Hormuz overnight rules **do not fire**; tanker-rate spike is not a confirmed fresh increment. **Do not** score Crude oil price surge. **Do not** score a fresh Geopolitical supply risk premium HIT.
- **Inventory: already printed.** EIA week ended 9/11 (released **9/16**): crude **−0.64 Mb vs ~−1.4 to −1.6 Mb expected** (relative miss); gasoline **+0.79 Mb**; distillates **+1.6 Mb**. API **+7.1 Mb** was the 09-16 lean. That cluster is **in** yesterday’s −2.88%. Next WPSR **Sep 23**. Do **not** date it as today’s HIT.
- **OPEC+ (carried offset):** Sep **+188 kb/d** completed the 2023 voluntary-cut rollback; **6 Sep** meeting **held October unchanged**. Next core-group meeting **Oct 4**. Not a cut, not a quota break.
- **Demand destruction (carried):** hawkish Fed/SEP is a medium-horizon demand lid, not a 1d EIA-style HIT. IEA vs OPEC disagreement is leftover.
- **Cracks:** diesel still extreme (Asia ~$87/bbl 9/16; USGC still elevated). **Refiner sleeve only.** Today HO **+0.18%**, RBOB **−0.54%**, gasoil **−0.26%** — products are **not** a clean squeeze that can drive whole XLE. MAP HEAT Refining dir=up (MPC/VLO) is **nested** — dampen; do not let VLO/MPC set the ETF.
- **Nat gas $2.902 (−0.51%)** — no surge; N/A for oil-weighted XLE.
- **MAP HEAT nested:** OFS/Drilling/Coal/Uranium **OVERRIDE down**; E&P/Integrated residual **up** on thin captains (XOM Venezuela MOU; only MGY has news). Nested overrides **do not average into XLE**.

Net **S1 = −1**. Live oil sign is down once. Not −2: not a collapse, increment is sub-1.5% after a large down day, Hormuz still a residual floor, 08-27 forbids restoring −2 off leftover inventory/Fed. Not 0: the barrel is still offered and 08-14’s green-oil license is off. Not +1: Finviz “Oil Surges, Tanker Rates Soar” is **stale index copy** against a live down print.

**3. Breadth.** Channel 1 1d rel **−2.44%** is reserved for S4 — do **not** copy it into S2. Live premarket: XLE **+0.08%**; XOM ~flat, CVX ~flat, COP ~flat after 09-16’s dump (COP was **−6.15%**). That is **stabilization, not expansion and not a confirmed smash**. MAP HEAT is split (integrated/E&P residual up vs OFS/drilling override down) at low-to-medium conviction. **S2 = 0.**

**4. Flows / positioning.** ETFDB-style prints: ~**−$157M 5d / −$62M 1m**, 3m still leaking; 09-16 volume was heavy (~48M). **1m rel is now +4.73%** — the +8–13% crowded-long of 09-08→09-15 **unwound yesterday**. Binding crowded-long precondition is **absent** → S3 contribution **zero**, not a leftover debit (and not a reflex-bounce plus: oil is still offered). No same-morning inflow/relative-volume spike. **S3 = 0.**

**5. Catalysts.** FOMC/SEP/presser **printed 09-16**. No crude EIA today. No XLE-wide earnings. Hormuz remains a **fade binary**, not a fresh increment. Checked: nothing material that is both fresh and sector-wide.

### Scoring logic

S0 muted: post-hike risk-on is a tech bounce (XLK +1.28%), not an energy bid; USD/real yields are not a live oil impulse.

S1 is the live oil-offered print, netted once, with **stale** EIA/API and non-transmitting Hormuz as dampeners only.

S2 is independent of the prior-close rel series. S4 is Channel 1 confirmation only: 1d rel **−2.44%** is decisive (≤ −1.5%); 3d **−0.36%**, 1w **−0.86%**, 1m **+4.73%**.

**No leading-vs-tape divergence** — S1 and S4 agree down. 09-04: when S1 and S4 are both negative and 1d rel ≤ −1.5%, direction is **down**; DO-INSTEAD / mag-discipline **cap magnitude at mild**, they do **not** flatten direction. Confidence is **shrunk** because PM:XLE is flat (not extending) and the incremental oil move is sub-1.5% after a already-large down session.

Self-audit: energy lens (not SPX, not DVN/VLO stock-pick); mild band (mag hit-rate + size_gate + sub-1.5% increment); no refiner-skew; oil+inventory+geo counted **once**; single-ticker (DVN −5.6%, XOM Venezuela, SLB/BKR) does not drive the ETF.

HORIZON_3D: offered barrel + leftover inventory/Fed still the near tape; bounce risk after −2.88% if oil stabilizes, but 3d rel already only −0.36% so further absolute downside is the mild continuation, not a new smash.
HORIZON_1W: 1w rel −0.86%; Hormuz floor vs hawkish-Fed demand lid — two-sided, no fresh kinetic.
HORIZON_2W: next EIA **Sep 23**; OPEC+ core group **Oct 4** — both unprinted, do not pre-score.
HORIZON_1M: 1m rel +4.73% still a residual relative cushion vs SPY, no longer the +8% crowded-long of mid-month.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: -1
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: -1
MULTIPLIER: 0.85
CONFIDENCE: 0.44
REGIME: mixed
HORIZON_3D: down
HORIZON_1W: mixed
HORIZON_2W: mixed
HORIZON_1M: mixed
DIVERGENCE_FLAGGED: false
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|PARTIAL|0.55|2026-09-17|https://www.dimsumdaily.hk/stock-futures-edge-higher-after-fed-rate-hike-sparks-sell%E2%80%91off/
Risk-off tape / flight to safety|MISS|0.70|2026-09-17|Channel 1 VIX 16.04 / VIX/VIX3M 0.813
Real yields rising|PARTIAL|0.40|2026-09-17|Channel 1 DFII10 2.62 +0.02 1d / +0.19 1w (leftover, not live impulse)
Real yields falling|MISS|0.70|2026-09-17|Channel 1 DFII10
USD strengthening|MISS|0.65|2026-09-17|Channel 1 DXY 1d -0.13%
USD weakening|PARTIAL|0.35|2026-09-17|Channel 1 DXY 1d -0.13% (sub-threshold)
Sector breadth expansion (% names up)|MISS|0.60|2026-09-17|PM XLE +0.08%; XOM/CVX/COP flat
Sector breadth failure (ETF up, names flat)|MISS|0.55|2026-09-17|ETF not up; PM flat
Large-cap leadership inside sector|PARTIAL|0.40|2026-09-17|MAP HEAT Integrated residual up (XOM), low-to-medium conv
Small/mid leadership inside sector|MISS|0.50|2026-09-17|MAP HEAT E&P only MGY news; DVN leftover
High-beta leadership inside sector|MISS|0.55|2026-09-17|PM XLK +1.28% vs XLE +0.08%
Low-beta leadership inside sector|MISS|0.50|2026-09-17|checked, nothing material
Sector ETF inflow / relative volume spike|MISS|0.55|2026-09-17|https://etfdb.com/etf/XLE/
Sector ETF outflow / volume dry-up|PARTIAL|0.45|2026-09-17|https://etfdb.com/etf/XLE/ (~-$157M 5d; 09-16 volume already printed)
Crowded long (extreme relative performance + valuation)|MISS|0.70|2026-09-17|Channel 1 1m rel +4.73% after 09-16 unwind
Index rebalance / inclusion tailwind|MISS|0.80|2026-09-17|checked, nothing material
Index exclusion / forced selling|MISS|0.80|2026-09-17|checked, nothing material
Crude oil price surge (WTI/Brent)|MISS|0.80|2026-09-17|https://www.oilpriceapi.com/live/wti-crude-oil-price
Natural gas price surge|MISS|0.75|2026-09-17|Channel 1 NG -0.51%
Inventory draw (EIA crude/products)|MISS|0.70|2026-09-17|https://www.morningstar.com/news/dow-jones/202609164509/us-crude-oil-stockpiles-post-moderate-draw
OPEC+ cut / supply discipline|MISS|0.75|2026-09-17|https://www.energyconnects.com/news/oil/2026/september/opecplus-keeps-output-policy-unchanged-for-october/
Crack spread / refining margin expansion|PARTIAL|0.50|2026-09-17|https://hydrocarbonprocessing.com/news/2026/09/asia-diesel-refining-margins-at-record-high-of-more-than-87-a-barrel/ (nested refiners only)
Geopolitical supply risk premium|PARTIAL|0.45|2026-09-17|https://straits.live/report (present, not transmitting; oil offered)
Crude price collapse|MISS|0.70|2026-09-17|live WTI ~$101.8 / Brent ~$104-105; sub-1.5% increment
OPEC+ production increase / quota break|PARTIAL|0.35|2026-09-17|https://www.energyconnects.com/news/oil/2026/september/opecplus-keeps-output-policy-unchanged-for-october/ (Sep +188 kb/d already done; Oct unchanged)
Demand destruction (recession/China weak)|PARTIAL|0.35|2026-09-17|https://www.federalreserve.gov/newsevents/pressreleases/monetary20260916a.htm (hawkish Fed leftover, not a 1d HIT)
Inventory build|PARTIAL|0.55|2026-09-17|https://cryptobriefing.com/us-crude-settles-lower-inventory-build/ (API +7.1 Mb / EIA relative miss — already in 09-16)
Crack spread collapse|MISS|0.65|2026-09-17|HO +0.18% / RBOB -0.54%; diesel cracks still extreme
Sector rotation into energy|MISS|0.70|2026-09-17|PM XLE +0.08% vs XLK +1.28%
Sector rotation out of energy|PARTIAL|0.50|2026-09-17|PM board: energy lagging the bounce; 1d rel -2.44% leftover
HIT_GRID_END

## RESEARCH APPENDIX

**Queries run**
- WTI Brent crude oil price today September 17 2026
- EIA crude oil inventory build September 2026 XLE energy
- OPEC+ oil production meeting September 2026
- XLE ETF premarket flows energy stocks XOM CVX September 17 2026
- WTI crude oil live price September 17 2026
- FOMC decision September 17 2026 Fed meeting energy oil
- Hormuz Iran tanker oil supply premium September 17 2026
- XOM CVX COP premarket September 17 2026
- XLE ETF flows September 2026 energy sector rotation
- crack spread diesel refining margins September 2026
- US retail sales September 2026 release FOMC aftermath stock futures
- energy stocks rebound after selloff September 17 2026 XLE
- DVN Devon energy -5.6% crude inventory build September 2026
- X search: WTI Brent crude oil price XLE energy stocks September 17 2026 (2026-09-16 to 2026-09-17)
- web_fetch: oilpriceapi live oil-market-status (403)
- web_fetch: boereport EIA 2026-09-16 (403)

**Key sources (title + URL + timestamp/facts taken)**
- OilPriceAPI WTI live — https://www.oilpriceapi.com/live/wti-crude-oil-price — ~2026-09-17: WTI ~$101.78
- Investing.com WTI/Brent historical — https://hk.investing.com/commodities/crude-oil-historical-data / brent-oil-historical-data — WTI ~$101.77–101.80, range ~$100.40–$102.45; Brent ~$103.99–$104.80
- MarketWatch CL/BRN download — Sep 16 settlement WTI ~$102.43, Brent ~$105.83
- Morningstar/Dow Jones EIA — https://www.morningstar.com/news/dow-jones/202609164509/us-crude-oil-stockpiles-post-moderate-draw — week ended 9/11, released 9/16: crude −0.64 Mb to 423.4 Mb (vs ~−1.4–1.6 expected); Cushing −342 kb; gasoline +0.794 Mb; distillates +1.6 Mb
- Boer Report EIA recap — https://boereport.com/2026/09/16/us-crude-stocks-fall-on-strong-exports-fuel-inventories-rise-eia-says/ — exports +1.41 mbpd to 4.83 mbpd; utilization 96.8%; next EIA Sep 23
- CryptoBriefing API — https://cryptobriefing.com/us-crude-settles-lower-inventory-build/ — API +7.1 Mb; WTI settle ~−3.2% at $102.43 on 9/16
- Energy Connects OPEC+ — https://www.energyconnects.com/news/oil/2026/september/opecplus-keeps-output-policy-unchanged-for-october/ — Sep 6: October unchanged; Sep +188 kb/d already done; next meeting Oct 4
- Federal Reserve FOMC — https://www.federalreserve.gov/newsevents/pressreleases/monetary20260916a.htm — 9/16 2pm ET: +25 bp to 3.75–4.00%, unanimous; effective 9/17
- CNBC Fed — https://www.cnbc.com/2026/09/16/fed-rate-decision-september-2026.html — first hike since 2023; hawkish SEP/dots; energy-inflation context
- Reuters Fed aftermath — https://www.reuters.com/markets/wealth/fed-builds-credibility-hawkish-turn-leaves-investors-edgy-2026-09-17/ — 9/17 futures rebound, investors edgy
- Dimsum Daily futures — https://www.dimsumdaily.hk/stock-futures-edge-higher-after-fed-rate-hike-sparks-sell%E2%80%91off/ — 9/16 selloff then 9/17 futures bounce
- Benzinga 9/16 tape — https://www.benzinga.com/markets/market-summary/26/09/61822672/oil-sinks-below-103-optics-stocks-rip-fed-first-hike-since-2023-markets-wednesday — XLE worst sector; WTI ~−3.6% toward $102
- Tradesmith XLE — https://tradesmith.com/stockdata/XLE:NYSE — 9/16 close $64.03 (−2.88%); 9/17 PM ~$64.09–$64.13
- ETFDB XLE flows — https://etfdb.com/etf/XLE/ — ~−$157M 5d / ~−$62M 1m
- Hydrocarbon Processing Asia diesel — https://hydrocarbonprocessing.com/news/2026/09/asia-diesel-refining-margins-at-record-high-of-more-than-87-a-barrel/ — Asia diesel cracks >$87/bbl as of 9/16
- Straits.live Hormuz — https://straits.live/report — transits still ~9% of normal; war-risk 15–40× peacetime
- Yahoo DVN — https://finance.yahoo.com/markets/stocks/articles/devon-energy-dvn-declines-more-215003296.html — DVN ~−5.63% on 9/16 (leftover E&P hit)
- MarketWatch/WSJ XOM CVX COP PM — XOM ~$163.41–163.47 (~flat), CVX ~$211.64 (~flat), COP ~$132.50–133.10 (~flat) vs 9/16 closes
- X search 9/16–9/17 — returned internally inconsistent $62 WTI / $92 XLE; **discarded** vs Channel 1 + Investing/OilPriceAPI

**Facts used vs discarded**
- Used: live oil still down modestly; FOMC already printed; EIA/API already in 09-16; XLE PM ~flat; 1d rel −2.44%; VIX contango; OPEC+ Oct unchanged; nested refiners/OFS; no fresh kinetic.
- Discarded: Finviz “Oil Surges, Tanker Rates Soar” as today’s oil sign; Finviz $104.16/−1.59% as the live level; X-search $62 WTI; averaging nested MAP HEAT overrides into XLE; restacking printed FOMC/retail as a new S1 HIT.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': -1.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': -1.0}, 'multiplier': 0.85, 'leading_sum': -3.0, 'divergence_flagged': True, 'total_score': 0.202, 'predicted_direction': 'up', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.408, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.151, 'score': 0.906, 'legs': [{'leg': 'CL', 'pct': -1.59, 'w': 0.35}, {'leg': 'QA', 'pct': -1.02, 'w': 0.15}, {'leg': 'ES', 'pct': 1.71, 'w': 0.6}, {'leg': 'PM:XLE', 'pct': 0.08, 'w': 0.7}]}, 'overlay_score': -2.55, 'overlay_raw': -2.55, 'index_carry': 1.846, 'general_total': 7.383, 'skill_multipliers': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 1.0}, 'llm_confidence': 0.44, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
