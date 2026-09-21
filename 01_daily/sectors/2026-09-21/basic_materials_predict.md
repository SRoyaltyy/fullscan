# Sector Prediction — Basic Materials — 2026-09-21

- ETF: **XLB**
- rubric: `00_grounding/sectors/basic_materials.md`
- predicted_direction: **flat**
- predicted_magnitude_band: **flat**
- total_score: **1.904** (mult 0.85)
- regime: mixed
- divergence_flagged: **True**
- engine: v2 · tape_anchor **0.811** (ES +1.35%, HG +0.66%, GC +0.90%, DX -0.02%, PM:XLB -0.28%) · index_carry **3.218** (general 12.871) · llm_overlay **-2.125** (raw -2.125)

## Channel 1 sector ETF tape

```
ETF XLB vs SPY (yfinance, through 2026-09-18):
  1d: XLB -1.42% | SPY +0.13% | rel -1.55%
  3d: XLB -1.46% | SPY +0.82% | rel -2.28%
  1w: XLB -1.88% | SPY -0.09% | rel -1.79%
  1m: XLB -4.82% | SPY -0.71% | rel -4.10%
```

MEMORY_CONFIRM: Sector Basic Materials / XLB only. Memory index unavailable this run (embedding metadata missing) — used injected scoreboard/lessons only. Rolling last-10 dir=0.4 mag=0.3 (n=10); last-30 dir=0.478 mag=0.435 (n=23); last graded 2026-09-18 flat/flat vs XLB −1.42% / SPY +0.13% / rel −1.55% (dir MISS, mag MISS). No open experiment for `sector_basic_materials`. Active XLB rules checked: **09-18 HEAT-down + non-held PM is BINDING** — nested MAP HEAT is majority-down (Cu/Al/Au/ag-inputs/met-coal/other metals), parent PM is **red (XLB −0.28%)**, 1d/1w/1m rel all negative; do **not** book green ES/NQ into S0>0; prefer **down/mild** over another flat. **09-17 residual-mild-up is OFF** — condition (c) requires a green parent PM; XLB is red, so the all-zero→mild-up residual does **not** fire. **09-16 transmission haircut ON as process, OFF as a FOMC-down trigger** — hike/SEP/Warsh is T+5 and already in Wednesday’s close; do not re-score the binary; do **not** pay oil-offered + gold + green Cu as cash-XLB support. **09-15 nested-bid ON as process, OFF as copper-HEAT trigger** — MAP HEAT Copper is **down** (FCX tariff-premium unwind); do not invent a nested bid; PM is not green so the don’t-flip-down triad is off. **09-11 four-index ≥+0.5% up-gate OFF** (Finviz SPX +0.20 / NQ +0.41 / RTY +0.08 / DJIA +0.11). **09-10 gap-at-open OFF** (|PM| 0.28% << 1%). **09-09 S1=−2 / S2=−1 OFF** (8/18 co-move not firing; offset not zero). **8/18 metals-as-floor OFF** (oil offered, not a Hormuz squeeze into equity risk-off). **8/14 gold-offset ON as a sleeve only** (Finviz GC +0.90%, SI +1.96%) — HEAT Gold is **down**, so not a book bid. **8/25 up-ban ON** (NQ=F +2.12% >> ES=F +1.35%; XLK PM +0.98% vs XLB −0.28%; 1d rel −1.55% < 0.5% — cannot emit confirmed-up). **8/27 S4-cap ON as conviction cap**; **09-04 T-1-lag ON** — do not copy Friday’s −1.55% rel into S4. **8/28 leftover-down OFF** (S0/S1 are live, not a stale-chemicals net-zero). **China/gold split ON** — do not let gold cancel China/industrial metals. DO-INSTEAD (last three BM losses): when score sign fights tape/breadth, cut conviction; prefer flat/mild. **Today sign does not fight tape** (factors down, PM red, HEAT-down) — keep direction, band mild. size_gate=True.

## Analysis — XLB, session of 2026-09-21 (Monday cash open)

This is a **Monday lagging-cyclical open after Friday’s worst-sector dump**, not a copper-squeeze day and not a fresh kinetic oil shock. Channel 1 tape through 09-18 is decisively negative on every horizon: 1d rel **−1.55%**, 3d **−2.28%**, 1w **−1.79%**, 1m **−4.10%**. Live **PM:XLB −0.28%** while ES=F **+1.35%** / NQ=F **+2.12%** and XLK PM **+0.98%**. Repeating last week’s flat/flat cards is the named miss cluster (09-08 / 09-16 / 09-17 / 09-18). XLB is also **ex-dividend today** (~$0.21–$0.23); that can mechanically print a slightly lower open, but it does **not** convert a red parent + majority-down nested book into a 09-17 participation certificate.

### 1. Shared macro as it hits materials (S0)

Knowable this morning (Channel 1, do not re-derive):

- **No pending CPI/NFP/FOMC binary.** News Judge applied none of those gates. Warsh JH / gold −3% and “Barrick surge on cut bets” are **stale vs live Finviz GC +0.90%** and vs the already-printed 09-16 hike. Do not re-open the binary (09-15 / 09-16).
- **Index tape is tech-led green vs prior close, not a materials thrust.** ES=F **+1.35%**, NQ=F **+2.12%**. Finviz four-index **fails** ≥ +0.5%. **8/25 transmission:** NQ >> ES and XLK PM +0.98% vs XLB **−0.28%** is not an XLB green light. The 09-16/09-18 overlay is explicit: when the sector’s own print is the lagging/red name on a green index sleeve, **zero the index legs for this sector**.
- **Oil offered, 8/18 OFF.** WTI **−1.59%** to $104.16, Brent **−1.02%**, CL=F **−5.94%** 1d. News Judge #4 (crude inventory build + hike → E&P lower) is an energy-factor hit, not a metals squeeze. Hormuz remains a *level* (Brent still >$100); the live increment is down. Count feedstock relief in S1 with the 09-16 haircut, not a second S0 plus.
- **USD / real yields.** Finviz USD **−0.02%**; DXY 1d **+0.08%** / 1m **+1.42%** — firm, **not** a spike vs the complex. DFII10 **2.61 (−0.07 1d, +0.20 1m)** is still an elevated *level* with a modest same-print dip. Live notes slightly bid (10Y **−0.03%**, 30Y **−0.06%**). 5-day 10Y–SPX corr **−0.592** — yields still matter, not the −0.97 stress tape.
- **VIX 14.98** with VIX/VIX3M **0.821 contango**. HY OAS **2.70** contained. EPU **342.15** still elevated vs mid-month. **Asia +1.04%** and **Europe +0.95%** are both green — Friday’s Europe-red overlay is **off**.
- **09-18 vs 09-17.** 09-17 would want mild-up-with-lag if S0–S3 netted to zero **and** PM were green. PM is **red** and nested HEAT is **majority-down**, so 09-17 is off and 09-18 binds: **S0 ≤ 0**.

**S0 = 0.** Not +0.5/+1: four-index gate off, NQ>>ES is not XLB participation, parent PM red, nested book already contradicting the index bounce (09-18). Not −1: oil offered, USD not spiking, Asia/Europe green, no kinetic increment, no same-morning China miss, futures not red. Mixed regime for *this* cyclical: index risk-on is a funding rotation **away** from lagging materials, not a materials bid.

### 2. Spine + secondary (S1)

**Industrial metals — bounce, not surge, not collapse.** COMEX copper **$6.489 (+0.66%)**; aluminum **+1.10%**; iron ore **−0.14%**; steel HRC **−0.16%**. LME cash last print **~$14,529/t** (09-18 official) after the mid-month dip toward ~$13,900 and well off the early-September record (~$14,858). Spine “surge” **OFF**. Spine “collapse” **OFF**. MAP HEAT Copper **down** (FCX −4.66% w1, IE neg) — do **not** average a nested long into the parent (09-15 process).

**Inventory draw — inverted (glut HIT).** LME copper stocks **255.1 kt** (09-18), **~+20% since mid-August** (rebuild from ~233.5 kt toward 255.9 kt). Cash-3M squeeze has unwound toward flat. Inverse of the spine’s inventory-draw HIT. Not a full historical glut (mid-percentile), but the **direction** is rebuild, not draw.

**China demand — still contraction, not a rebound; do not let gold cancel it.** August NBS mfg **49.8** (<50), construction **46.9**, property FAI still ~**−19–20% YoY**. Next NBS PMI is ~09-29/30 — **not today**. Yangshan import premium **~$118–124/t** (multi-year high) and SHFE stocks near multi-month lows are a **physical restocking** signal after the dip — partial industrial offset, **not** a PMI/property rebound HIT. Hang Seng/Shanghai are **green**, so it is not a US-open China shock (09-15 level-vs-shock). Carried industrial drag remains.

**Monetary metals — 8/14 sleeve ON, book bid OFF.** Finviz gold **+0.90%**, silver **+1.96%**, platinum **+0.61%**, palladium **+1.64%**. GC=F **−0.77% 1d** is Friday’s print, not the live board. MAP HEAT Gold **down** (NEM/SSRM). 09-16: gold futures are not a cash-XLB bid once discount rates/equity beta are the live overlay. NEM ~8% is a **sleeve**.

**Chemicals majority sleeve — oil-offered cost relief, unconfirmed in cash.** LIN ~13%, SHW, ECL ~40–50% combined get feedstock relief from WTI/Brent down. MAP HEAT Chemicals **flat/quiet** (DOW/HUN none). 09-16 haircut: do **not** pay oil-down + gold + green Cu as cash-XLB support when PM is red and nested metals are down.

**Steel guidance — S1 once (09-18).** Nucor (09-17 after close): Q3 EPS **$5.55–$5.65**, below consensus (~$5.99–$6.17); mills/products up sequentially, raw materials down; stock sold off ~5% with STLD. First cash session that still carries that print. Single-name, not the whole book — count **once**, do not let NUE drive the ETF call.

**Tariffs — uncertainty, not a same-open support.** Section 232 on **refined** copper still unresolved (market watching ~09-28). FCX faded as the tariff premium unwound. Critical-minerals policy is **not** a HIT this morning.

**S1 = −1.** NUE/steel guide + LME rebuild + carried China/property + nested Cu/Au HEAT-down outweigh a non-surge Cu/Al bounce, an 8/14 gold *sleeve*, and unconfirmed oil-relief. Not −2: 8/18 is off, copper is not collapsing, offset is not zero. Not 0: that was Friday’s miss — netting gold+oil against China/glut while HEAT was already majority-down.

### 3. Breadth (S2)

Nested HEAT is **majority-down vs parent**: Copper down, Gold down, Aluminum down (SPLIT, breadth 0.0), Agricultural Inputs down, Coking Coal down SPLIT, Other Industrial Metals down, Other Precious down. Chemicals **flat**, Building Materials **flat**, Lumber **flat**. No defensive pocket inside the book. Friday was the **worst sector** (rel −1.55%); Monday PM is still red while XLK leads. This is **rotation out of materials**, not ETF-only carry.

**S2 = −1.** Not −2: chemicals/building/lumber are quiet-flat, not a uniform wipeout. Not 0: 09-18 forbids treating HEAT-down as “mixed/no edge.”

### 4. Flows / positioning (S3)

ETFdb through ~09-18: XLB **5-day −$55M**, **1-month −$208M**, 3-month still **+$368M**. Near-term **outflow**, not a volume spike and not a washout. 1m rel **−4.10%** is the opposite of a crowded long. No rebalance/inclusion catalyst.

**S3 = 0.** Outflows are real but not extreme enough for −1 after already scoring rotation in S1/S2; engine also haircuts S3 ×0.5. Do not double-count Friday’s dump as a flow shock.

### 5. Tape (S4, confirmation only)

Channel 1 1d rel **−1.55%** is **Friday’s already-printed session**. 09-04 / 8/28: S4 confirms **this** session, not T−1. Live PM **−0.28%** is modestly red, not a ≥1% gap (09-10 off) and not confirmation-eligible as a ±1. 8/27 remains a **conviction cap** (no confirmed-up), not a license to copy Friday’s −1.55% into −1.

**S4 = 0.**

### Self-audit

- **Lens:** XLB environment, not SPX, not FCX/NUE stock-picking.
- **Band:** mild. No ≥1% open gap; S1 is −1 not −2; size_gate on; rolling mag 0.3.
- **Skew:** chemicals-heavy book; nested miners/steel are the drag; do not let Cu +0.66% or gold +0.90% cancel China/LME rebuild.
- **Same-shock double-count:** oil-offered counted as S1 haircut, not S0 plus; Friday rel not in S4; NUE once in S1.
- **Single-ticker:** NUE/FCX/NEM do not drive the ETF call; nested HEAT is the breadth object.
- **Divergence:** leading S0–S3 = **−2**, S4 = 0. Factors and live PM **agree** down/soft — **no flag**. Trust factors. Last-three DO-INSTEAD (cut to flat when sign fights tape) does **not** apply.

**Call expression (pipeline owns totals):** absolute **down / mild**, relative **lag vs SPY**. Tech-led ES/NQ bounce is not XLB participation. Ex-div can add a few tenths of mechanical downside; it is not the thesis.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: -1
S2_BREADTH: -1
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.85
CONFIDENCE: 0.48
REGIME: mixed
HORIZON_3D: down
HORIZON_1W: down
HORIZON_2W: mixed
HORIZON_1M: down
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|PARTIAL|0.62|2026-09-21|https://finance.yahoo.com/markets/live/stock-market-today-monday-september-21-dow-sp-500-nasdaq-080214605.html
Risk-off tape / flight to safety|OFF|0.55|2026-09-21|https://finance.yahoo.com/markets/live/stock-market-today-monday-september-21-dow-sp-500-nasdaq-080214605.html
Real yields rising|PARTIAL|0.58|2026-09-21|
Real yields falling|PARTIAL|0.50|2026-09-21|
USD strengthening|OFF|0.60|2026-09-21|
USD weakening|OFF|0.55|2026-09-21|
Sector breadth expansion (% names up)|OFF|0.70|2026-09-21|
Sector breadth failure (ETF up, names flat)|OFF|0.65|2026-09-21|
Large-cap leadership inside sector|OFF|0.50|2026-09-21|
Small/mid leadership inside sector|OFF|0.50|2026-09-21|
High-beta leadership inside sector|OFF|0.55|2026-09-21|
Low-beta leadership inside sector|PARTIAL|0.45|2026-09-21|
Sector ETF inflow / relative volume spike|OFF|0.60|2026-09-21|https://etfdb.com/etf/XLB/
Sector ETF outflow / volume dry-up|HIT|0.62|2026-09-21|https://etfdb.com/etf/XLB/
Crowded long (extreme relative performance + valuation)|OFF|0.70|2026-09-21|
Index rebalance / inclusion tailwind|OFF|0.80|2026-09-21|
Index exclusion / forced selling|OFF|0.80|2026-09-21|
Industrial metal price surge (copper/aluminum/iron ore)|OFF|0.72|2026-09-21|
Gold/silver price surge (monetary metals)|PARTIAL|0.64|2026-09-21|
China PMI / property demand rebound|OFF|0.75|2026-09-21|https://www.stats.gov.cn/english/PressRelease/202607/t20260701_1964047.html
Inventory draw (LME/exchange stocks down)|OFF|0.78|2026-09-21|https://thevaultreport.com/lme/copper
Supply disruption (mine/export ban)|OFF|0.70|2026-09-21|
Critical-minerals policy / domestic tariff support|PARTIAL|0.55|2026-09-21|https://finimize.com/content/copper-slips-as-us-tariff-uncertainty-clouds-inventory-flows
Industrial metal price collapse|OFF|0.70|2026-09-21|
China demand shock / property stress|HIT|0.68|2026-09-21|
USD spike vs commodity complex|OFF|0.72|2026-09-21|
Supply glut / new capacity online|HIT|0.70|2026-09-21|https://mining.com.au/copper-price-hits-lowest-in-nearly-a-month-as-lme-stocks-rise/
Margin compression / cost inflation without pricing power|OFF|0.58|2026-09-21|
Sector rotation into materials|OFF|0.74|2026-09-21|
Sector rotation out of materials|HIT|0.76|2026-09-21|
HIT_GRID_END

## RESEARCH APPENDIX

**Queries run**
- web_search: `copper LME price inventory stocks September 21 2026`
- web_search: `China PMI property demand copper imports September 2026`
- web_search: `gold silver price Fed hike Warsh September 21 2026`
- web_search: `XLB materials ETF premarket breadth LIN SHW FCX NEM September 21 2026`
- web_search: `XLB ETF flows positioning outflows September 2026`
- web_search: `Nucor NUE guidance steel chemicals XLB September 2026`
- web_search: `Section 232 copper tariff refined copper FCX September 2026`
- web_search: `US stock futures Monday September 21 2026 yields oil materials`
- web_search: `LME copper stocks rebuild glut September 2026`
- web_search: `XLB ex-dividend date September 21 2026`
- web_search: `risk on equity market breadth Monday September 21 2026`
- x_search: `XLB materials copper gold LME China PMI premarket Monday September 21 2026` (2026-09-18 to 2026-09-21)
- web_fetch: `https://etfdb.com/etf/XLB/` (403 / blocked)
- web_fetch: `https://www.prnewswire.com/news-releases/nucor-announces-guidance-for-the-third-quarter-of-2026-earnings-302882443.html`
- memory_search: Basic Materials XLB lessons (index unavailable)

**Key sources and facts taken**

- Westmetall / Vault Report / MacroMicro (via search, ~2026-09-18 LME close): LME Cu cash **$14,529/t**, 3M **$14,515/t**, warehouse stocks **255,100 t** (−800 t d/d from 255,900); rebuild vs mid-August lows. https://www.westmetall.com/de/markdaten.php/home.html?action=table&field=LME_Cu_cash ; https://thevaultreport.com/lme/copper
- mining.com.au / Finimize: LME stocks rise contributed to pullback from ~$14,858 record toward $13,900–$14,500; metal repositioning after COMEX/US tariff stockpiling, not a full historical glut. https://mining.com.au/copper-price-hits-lowest-in-nearly-a-month-as-lme-stocks-rise/
- NBS / Forex Factory / Trading Economics: official mfg PMI **49.8 in August** (July 49.2); September print due ~09-29/30. Property FAI / home prices still contractionary. https://www.stats.gov.cn/english/PressRelease/202607/t20260701_1964047.html
- ADMIS / mining.com.au: Yangshan Cu import premium **~$118–124/t**; SHFE inventories near multi-month lows — physical restock, not a PMI rebound. https://www.admis.com/chinese-demand-is-back-for-copper/
- Axios / Bloomberg / Kitco: FOMC 09-16 **+25 bp to 3.75–4.00%**, SEP majority another hike; Warsh hawkish; gold had buckled then partially recovered. Live Channel 1 Finviz gold **+0.90%** used over stale “gold −3%” News Judge line.
- Bloomberg / Yahoo / Morningstar (09-21): US futures up on cheaper oil and US-China talk hopes; 10Y ~**4.96%** easing; WTI/Brent extending declines. Used as Channel 2 color; **did not override** Channel 1 ES +1.35% / NQ +2.12% / WTI −1.59%.
- ETFdb (via search, ~09-18): XLB 5d **−$55.13M**, 1m **−$208.44M**, 3m **+$368.48M**. https://etfdb.com/etf/XLB/
- Nucor PR (fetched 2026-09-21T09:25Z): Q3 EPS guide **$5.55–$5.65**; mills/products up, raw materials down; Q2 had $130M non-repeat procurement refunds. https://www.prnewswire.com/news-releases/nucor-announces-guidance-for-the-third-quarter-of-2026-earnings-302882443.html
- CoinCentral / TipRanks: NUE ~**−5%** on below-consensus guide. https://coincentral.com/nucor-nue-stock-drops-5-as-earnings-guidance-disappoints-wall-street/
- TerraVista / DiscoveryAlert / Finimize / Trefis: refined-copper Section 232 still **pending**; FCX faded on tariff-premium doubts. https://finimize.com/content/copper-slips-as-us-tariff-uncertainty-clouds-inventory-flows
- stockevents.app: XLB **ex-div 2026-09-21**, pay 09-23, ~**$0.21/sh**. https://stockevents.app/en/stock/XLB/dividends
- X search: no Monday-specific XLB/PMI break; Friday MetalBenderHQ note on Yangshan **$124/t** and LME curve re-tightening — treated as T−1 physical color, not a US-open surge.

**Not used as Channel 1 substitutes:** Fear & Greed 58.2 (stale 2026-08-27); Finviz “gold surge on cut bets” / AU gold −3% (contradicted by live Finviz GC +0.90% and by the paid 09-16 hike); weekend LME (no 09-21 official).

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': -1.0, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.85, 'leading_sum': -5.0, 'divergence_flagged': True, 'total_score': 1.904, 'predicted_direction': 'flat', 'predicted_magnitude_band': 'flat', 'confidence_score': 0.476, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.1352, 'score': 0.811, 'legs': [{'leg': 'ES', 'pct': 1.35, 'w': 0.6}, {'leg': 'HG', 'pct': 0.66, 'w': 0.3}, {'leg': 'GC', 'pct': 0.9, 'w': 0.1}, {'leg': 'DX', 'pct': -0.02, 'w': -0.3}, {'leg': 'PM:XLB', 'pct': -0.28, 'w': 0.7}]}, 'overlay_score': -2.125, 'overlay_raw': -2.125, 'index_carry': 3.218, 'general_total': 12.871, 'skill_multipliers': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 0.5, 'S3_FLOWS_POSITIONING': 1.25, 'S4_ETF_TAPE': 0.0}, 'llm_confidence': 0.48, 'sector_rs_veto_applied': True, 'sector_rs_tape': {'d1': -2.08, 'w1': -4.78}, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
