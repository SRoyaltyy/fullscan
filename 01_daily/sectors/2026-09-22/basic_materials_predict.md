# Sector Prediction — Basic Materials — 2026-09-22

- ETF: **XLB**
- rubric: `00_grounding/sectors/basic_materials.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-1.587** (mult 0.85)
- regime: mixed
- divergence_flagged: **True**
- engine: v2 · tape_anchor **1.512** (ES -0.07%, HG +0.66%, GC +0.90%, DX -0.02%) · index_carry **-0.124** (general -0.497) · llm_overlay **-2.975** (raw -2.975)

## Channel 1 sector ETF tape

```
ETF XLB vs SPY (yfinance, through 2026-09-21):
  1d: XLB -0.10% | SPY +1.55% | rel -1.65%
  3d: XLB -0.84% | SPY +2.83% | rel -3.67%
  1w: XLB -1.09% | SPY +1.91% | rel -3.00%
  1m: XLB -4.73% | SPY +1.68% | rel -6.41%
```

MEMORY_CONFIRM: Sector Basic Materials / XLB only. Memory index unavailable this run (embedding metadata missing) — used injected scoreboard/lessons only. Rolling last-10 dir=0.3 mag=0.2 (n=10); last-30 dir=0.458 mag=0.417 (n=24); last graded 2026-09-21 flat/flat vs XLB −0.50% / SPY +1.55% / rel −2.05% (dir MISS, mag MISS). No open experiment for `sector_basic_materials`. Active XLB rules checked: **09-21 RS-veto-flatten is ON as process** — 1d rel **−1.65% ≤ −1.5%** and nested HEAT is majority-down; do **not** let leftover RS / size_gate compress a signed down overlay to another flat/flat; exact 09-21 triad is only partial (ES/NQ are **flat −0.07%**, not green ≥+1%; PM:XLB ~**+0.07%**, not red). **09-18 HEAT-down + non-held PM is BINDING** — nested MAP HEAT majority-down (Cu/Al/Au/ag-inputs/met-coal/other metals/other precious); PM∈(0,1%) is **non-information**, do not park it in S0; prefer **down/mild** over flat. **09-17 residual-mild-up is OFF** — needs ES/NQ ≥ +0.5% (ideally ≥ +1%) and a participating green PM; ES/NQ **−0.07%**. **09-16 transmission haircut ON as process, OFF as FOMC-down trigger** — hike/SEP/Warsh is T+6; do **not** pay oil-offered + gold + green Cu futures as cash-XLB. **09-15 nested-bid ON as process, OFF as copper-HEAT trigger** — MAP HEAT Copper is **down**; do not invent a nested bid; don’t-flip-down triad off. **09-11 four-index ≥ +0.5% OFF** (SPX +0.20 / NQ +0.41 / RTY +0.08 / DJIA +0.11). **09-10 gap-at-open OFF** (|PM| ~0.07% << 1%). **09-09 S1=−2 / S2=−1 OFF** (8/18 co-move not firing; offset not zero). **8/18 metals-as-floor OFF** (WTI **−1.59%**, not a Hormuz squeeze into equity risk-off). **8/14 gold-offset ON as a sleeve only** (Finviz GC **+0.90%**, SI **+1.96%**) — HEAT Gold is **down**, so not a book bid. **8/25 up-ban ON** (1d rel **−1.65% < 0.5%**; cannot emit confirmed-up). **8/27 S4-cap ON as conviction cap**; **09-04 / 8/28 T-1** — do **not** copy Monday’s −1.65% rel into S4. **China/gold split ON** — do not let gold cancel China/industrial metals. **Commodity-bullish-vs-flat-futures cap ON** — Cu/Al green on a flat ES/NQ tape cannot build an up call. DO-INSTEAD (last three BM losses): when score sign fights tape/breadth, cut conviction; **today sign agrees with tape** (factors down, HEAT-down, multi-horizon lag) — keep direction, band **mild**. size_gate=True.

## Analysis — XLB, session of 2026-09-22 (Tuesday cash)

This is a **lagging-cyclical Tuesday after Monday’s AI/Nasdaq leftover**, not a copper-squeeze day and not a fresh kinetic oil shock. Channel 1 tape through 09-21 is a deep relative hole on every horizon: 1d rel **−1.65%**, 3d **−3.67%**, 1w **−3.00%**, 1m **−6.41%**. Live index futures are **dead flat** (ES=F **−0.07%**, NQ=F **−0.07%**). XLB is **absent from the Channel 1 sector PM board**; a separate premarket print is ~**$49.75 / +0.07%** vs 09-21 close $49.71 — a non-held tick, not participation. Repeating last week’s flat/flat cluster (09-08 / 09-16 / 09-17 / 09-18 / 09-21) is the named miss. News Judge’s materials line is **#7 BHP / Section 232 refined-copper tariff uncertainty**, not the AI tape.

### 1. Shared macro as it hits materials (S0)

Knowable this morning (Channel 1, do not re-derive):

- **No pending CPI/NFP/FOMC binary.** News Judge applied none of those gates. The Nasdaq/AMD $1T pop is **prior-close leftover growth beta**, not an XLB impulse. 8/25 still binds: NQ leftover ≠ materials green light.
- **Index tape is flat, not a thrust.** Finviz four-index **fails** ≥ +0.5%. ES/NQ vs prior close **−0.07%**. 09-17 condition (a) is **off**. 09-11 up-gate is **off**. Do **not** import Monday’s SPY **+1.55%** as today’s carry — XLB already printed **−0.10% / rel −1.65%** against that tape.
- **Oil offered, 8/18 OFF.** WTI **−1.59%** to $104.16, Brent **−1.02%**, CL=F **−4.78%** 1d. News Judge #4 (surprise crude build / DVN) is an energy-factor hit, not a metals squeeze. Hormuz remains a *level* (Brent still >$100); the live increment is down. Count feedstock relief in S1 with the **09-16 haircut**, not a second S0 plus.
- **USD / real yields.** Finviz USD **−0.02%**; DXY 1d **+0.06%** / 1m **+1.71%** — firm, **not** a spike vs the complex. DFII10 **2.68** and DGS10 **5.01** (as of 09-18) are elevated *levels*; live notes are slightly offered (10Y **−0.03%**, 30Y **−0.06%**). 5-day 10Y–SPX corr **−0.79** — yields still matter, not the −0.97 stress tape.
- **VIX 14.88** with VIX/VIX3M **0.823 contango**. HY OAS **2.68** contained. Asia **+0.41%** / Europe **+0.07%** — neither a China impulse nor a risk-off confirmation.
- **09-18 vs 09-17 vs 09-21.** 09-17 would want mild-up-with-lag only if S0–S3 netted to zero **and** ES/NQ were independently ≥ +0.5% **and** PM were a held green. Futures are flat and nested HEAT is majority-down, so 09-17 is off. 09-18: **S0 ≤ 0** from a modest PM that nested names already contradict. 09-21: do not let RS-veto/size_gate flatten a down factor card when 1d rel ≤ −1.5% and HEAT is majority-down.

**S0 = 0.** Not +0.5/+1: four-index off, ES/NQ flat, leftover AI tape is a **funding rotation away** from lagging materials, parent PM is a non-print, nested book already red. Not −1: oil offered, USD not spiking, Asia/Europe not red, no kinetic increment, no same-morning China miss, futures not red. Mixed regime for *this* cyclical.

### 2. Spine + secondary (S1)

**Industrial metals — bounce, not surge, not collapse.** Channel 1: copper **$6.489 (+0.66%)**, aluminum **+1.10%**, iron ore **−0.14%**, steel HRC **−0.16%**. Spine “surge” **OFF**. Spine “collapse” **OFF**. News Judge #7 / Finviz BHP is the **policy overlay**: refined-copper tariff stall, not a same-open squeeze. MAP HEAT Copper is **down** (FCX tariff-premium unwind, IE −7.1%) — **do not average a nested long into the parent** (09-15). Green Cu futures without nested equity confirmation is the 09-16 transmission miss, not an S1 plus.

**Inventory draw — inverted (glut HIT).** LME copper warehouse stocks ~**255.9 kt** as of 09-21, **+~7–8% over 30 days** (255.1 kt on 09-18). Cancelled-warrant tightness is a curve story; total stocks have **rebuilt**. Inverse of the spine’s inventory-draw HIT. COMEX stocks remain elevated on tariff-front-running — that is positioning, not a China draw.

**China demand — still contraction, not a rebound; do not let gold cancel it.** August NBS mfg PMI **49.8** (<50); property FAI **−19.9% YoY** (Jan–Aug); new-home prices **−0.1% m/m / −3.0% y/y**. T-1/T-7, Asia green, so it is a **carried industrial offset**, not a US-open miss (09-15). Spine rebound **OFF**; property-stress HIT remains the industrial sleeve.

**Monetary metals — 8/14 sleeve ON, book bid OFF.** Finviz gold **+0.90%**, silver **+1.96%**, platinum **+0.61%**, palladium **+1.64%**. GC=F **−0.66%** 1d is **T-1**, not this morning’s quote. NEM ~8% of XLB; HEAT Gold is **down** (NEM −2.8% d1, no company news). 09-16: gold futures are not cash-XLB once equity beta/discount rates are the live macro. Score the sleeve; do **not** let it wash China/glut.

**Chemicals majority sleeve — oil-offered cost relief, unconfirmed at the cash layer.** LIN ~12.8%, SHW ~4.75%, ECL — combined chemicals still ~40–50% of XLB. HEAT Chemicals is **flat / news-dead** (relative winner vs parent on 1w, captains silent). 09-16 haircut: oil-down + gold + green Cu **does not transmit** when PM is flat and LIN is unconfirmed. NUE Q3 $5.55–$5.65 guide is **already in** Friday/Monday steel tape — do not re-score as a fresh open smash.

**Tariffs / critical minerals — uncertainty HIT, not domestic-support HIT.** Section 232 refined-copper decision still stalled (Reuters 09-10; News Judge #7). That is a **miner-sleeve negative** (BHP, FCX HEAT-down), not a same-open producer tailwind.

**S1 = −1.** China contraction + LME rebuild/glut + tariff-uncertainty / nested-copper-down outweigh a haircut gold sleeve and a non-surge Cu/Al bounce. Not −2: 09-09 needs all four sub-channels negative with **zero** offset — gold is green, oil is offered, Cu futures are green, so the cap does not apply. Not 0: that would recreate the all-zero → flat card that 09-18/09-21 forbade while HEAT is majority-down. Not +1: no metal surge, gold is not a book bid, China still <50.

### 3. Breadth (S2)

Same-morning MAP HEAT is **majority-down vs parent**: Copper down, Aluminum down (CENX −5.7%, CSTM −3.4%), Gold down, Ag inputs down, Coking coal SPLIT down, Other industrial metals down, Other precious down. Flat pockets: Chemicals, Building materials, Lumber — **not** an expansion. This is **rotation out of materials** with nested leadership already contradicting any parent tick. Large-cap quality (LIN) is inert, not carrying the ETF. Do **not** copy Monday’s chemicals lag a second time (8/28); this is live nested HEAT.

**S2 = −1.** Not −2: chemicals sleeve is flat, not a uniform 8/18 wipeout. Not 0: 09-18 — when breadth fights a non-held PM bid, do not net to zero.

### 4. Flows / positioning (S3)

ETFDB (as of ~09-19): XLB **5d −$53M**, **1m −$238M** net outflow; 3m still positive. Volume 09-21 ~10.6M is **average**, not a dry-up or spike. 1m rel **−6.41%** is the opposite of a crowded long — washed laggard, not forced liquidation today. No index rebalance catalyst.

**S3 = 0.** Modest residual outflow is not a same-session flow shock; do not stack the lag into positioning.

### 5. ETF tape confirmation (S4)

Channel 1 1d rel **−1.65%** is Monday’s print. 8/28 / 09-04: S4 confirms **this** session, not the prior close. 8/27: sub-0.5% 1d cannot be ± confirmation anyway; a large **prior** negative is leftover. Live PM ~**+0.07%** is not confirmation-eligible either. XLB missing from the PM board → follow the factor card, not index_carry (09-21 / XLC-style non-participation).

**S4 = 0.**

### Self-audit

- **Lens:** XLB environment, not SPX, not FCX/BHP/NUE stock-picking.
- **Band:** mild, not notable — no ≥1% gap, gold/silver sleeve still green, USD not a spike, |leading sum| modest; 09-15 gold-green rule caps a down call at **low-conviction down/mild**.
- **Skew:** chemicals-heavy book; minority Cu/Au nested books are the ones already down.
- **Same-shock double-count:** oil once (S1 haircut, not S0); China carried once in S1; Monday rel not in S4; NUE already traded.
- **Single-ticker:** BHP/FCX/NUE/NEM are nested evidence, not the ETF call.
- **Divergence:** leading S0–S3 = **−2** vs S4 = **0**. Factors vs confirmation: **trust factors**. Do not let tape_anchor/index_carry or RS-veto flatten this to flat (09-21). Sign **agrees** with live HEAT/relative tape, so DO-INSTEAD “cut to flat” does **not** apply.

**Call expression (for the pipeline, not a substitute for scores):** absolute **down / mild**, relative lag vs SPY still the primary object. HORIZON: 3d rel **−3.67%**, 1w **−3.00%**, 1m **−6.41%** are structural lag, not a same-open smash — they justify the relative lean, not a notable band.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: -1
S2_BREADTH: -1
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.85
CONFIDENCE: 0.40
REGIME: mixed
HORIZON_3D: lag
HORIZON_1W: lag
HORIZON_2W: lag
HORIZON_1M: lag
DIVERGENCE: factors_down_vs_S4_zero
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|MISS|0.55|2026-09-22|Channel 1 ES/NQ -0.07%; Finviz four-index fails ≥+0.5%; leftover Nasdaq AI is T-1 growth beta not XLB
Risk-off tape / flight to safety|MISS|0.60|2026-09-22|VIX 14.88 contango 0.823; HY OAS 2.68; oil offered; no kinetic increment
Real yields rising|HIT|0.50|2026-09-18|https://fred.stlouisfed.org/graph/?g=yh5W
Real yields falling|MISS|0.55|2026-09-22|DFII10 2.68 elevated level; live 10Y note -0.03% is not a smash
USD strengthening|MISS|0.55|2026-09-22|Finviz USD -0.02%; DXY 1d +0.06% not a spike
USD weakening|MISS|0.55|2026-09-22|DXY 1m +1.71%; not a commodity-dollar unwind
Sector breadth expansion (% names up)|MISS|0.70|2026-09-22|MAP HEAT majority-down vs parent
Sector breadth failure (ETF up, names flat)|HIT|0.65|2026-09-22|PM:XLB ~+0.07% vs nested Cu/Al/Au/met-coal down
Large-cap leadership inside sector|MISS|0.50|2026-09-22|LIN/chemicals news-dead; not carrying
Small/mid leadership inside sector|MISS|0.55|2026-09-22|CENX/CSTM/IE nested red
High-beta leadership inside sector|MISS|0.55|2026-09-22|copper/gold nested HEAT down
Low-beta leadership inside sector|HIT|0.45|2026-09-22|HEAT Chemicals flat relative winner vs parent; defensive inside a cyclical
Sector ETF inflow / relative volume spike|MISS|0.60|2026-09-22|https://etfdb.com/etf/XLB/
Sector ETF outflow / volume dry-up|HIT|0.50|2026-09-19|https://etfdb.com/etf/XLB/
Crowded long (extreme relative performance + valuation)|MISS|0.70|2026-09-21|1m rel -6.41% is washed laggard not crowded
Index rebalance / inclusion tailwind|MISS|0.40|2026-09-22|checked, nothing material
Index exclusion / forced selling|MISS|0.40|2026-09-22|checked, nothing material
Industrial metal price surge (copper/aluminum/iron ore)|MISS|0.65|2026-09-22|Channel 1 Cu +0.66% / Al +1.10% / Fe -0.14% — bounce not surge
Gold/silver price surge (monetary metals)|HIT|0.55|2026-09-22|Finviz GC +0.90% SI +1.96%; sleeve only (HEAT Gold down)
China PMI / property demand rebound|MISS|0.75|2026-09-16|https://www.stats.gov.cn/english/PressRelease/202609/t20260916_1965342.html
Inventory draw (LME/exchange stocks down)|MISS|0.70|2026-09-21|https://thevaultreport.com/lme/copper
Supply disruption (mine/export ban)|MISS|0.45|2026-09-22|Chinese refinery maintenance is Oct–Nov, not same-open
Critical-minerals policy / domestic tariff support|MISS|0.60|2026-09-10|https://www.reuters.com/world/us/white-house-copper-tariff-plan-stalls-amid-affordability-concerns-sources-say-2026-09-10/
Industrial metal price collapse|MISS|0.65|2026-09-22|Channel 1 copper +0.66%, not a collapse
China demand shock / property stress|HIT|0.70|2026-09-16|https://www.stats.gov.cn/english/PressRelease/202609/t20260916_1965342.html
USD spike vs commodity complex|MISS|0.60|2026-09-22|DXY +0.06% 1d, not a spike
Supply glut / new capacity online|HIT|0.60|2026-09-21|https://thevaultreport.com/lme/copper
Margin compression / cost inflation without pricing power|HIT|0.50|2026-09-21|NUE Q3 guide already in Friday/Monday steel tape; oil-offered is the offset
Sector rotation into materials|MISS|0.75|2026-09-21|Channel 1 1d/3d/1w/1m rel all deeply negative
Sector rotation out of materials|HIT|0.75|2026-09-21|XLB vs SPY 1d rel -1.65%; 1m rel -6.41%; nested HEAT majority-down
HIT_GRID_END

## RESEARCH APPENDIX

**Queries run**
- memory_search: Basic Materials XLB sector prediction lessons copper China PMI (index unavailable)
- web_search: copper price LME inventory China PMI property September 2026
- web_search: XLB premarket September 22 2026 materials sector ETF
- web_search: gold silver copper prices today tariff refined copper BHP
- web_search: US 10 year yield real yields DXY risk on off September 22 2026
- web_search: XLB ETF holdings LIN SHW FCX NEM flows volume September 2026
- web_search: LME copper stocks warehouse inventory September 2026
- web_search: China property investment home prices August 2026 copper demand
- web_search: materials sector rotation XLB underperform SPY September 2026
- web_search: Nucor Sherwin Williams Linde premarket September 22 2026
- web_search: XLB ETF 5 day fund flows ETFDB September 2026
- web_search: Section 232 refined copper tariff uncertainty BHP September 2026
- x_search: XLB materials copper gold LME China PMI premarket September 22 2026 (2026-09-20..2026-09-22)
- web_fetch: Reuters China home prices (JS wall); ETFDB XLB (403)

**Key sources and facts used**
- Channel 1 injected panel (2026-09-22 lock): VIX 14.88 / VIX3M 18.08 ratio 0.823; Finviz Cu $6.489 +0.66%, Au $4370.8 +0.90%, Ag $65.1 +1.96%, Al +1.10%, Fe −0.14%, WTI $104.16 −1.59%, Brent $107.67 −1.02%, USD −0.02%; ES=F −0.07%, NQ=F −0.07%; DGS10 5.01 / DFII10 2.68 as of 09-18; Asia composite +0.41%, Europe +0.07%; XLB vs SPY through 09-21: 1d rel −1.65%, 3d −3.67%, 1w −3.00%, 1m −6.41%.
- News Judge 2026-09-22: #7 BHP / refined-copper tariff uncertainty as the materials policy line; #1 Nasdaq AI leftover; #4 crude inventory build (oil offered). RULES_APPLIED: none.
- MAP HEAT research (injected): Copper/Aluminum/Gold/Ag-inputs/Coking coal/Other industrial/Other precious **down**; Chemicals/Building/Lumber **flat**. Nested override vs parent.
- TradeSmith XLB historical: prior close 09-21 $49.71; premarket ~$49.75 (+0.07%). https://tradesmith.com/stockdata/XLB:NYSE/historical-data
- LME copper stocks ~255.9 kt 09-21, ~255.1 kt 09-18, +~7–8% / 30d. https://thevaultreport.com/lme/copper
- NBS China: Jan–Aug property investment −19.9% YoY; August new-home prices −0.1% m/m, −3.0% y/y; mfg PMI 49.8. https://www.stats.gov.cn/english/PressRelease/202609/t20260916_1965342.html
- Reuters 2026-09-10: White House copper tariff plan stalled on affordability. https://www.reuters.com/world/us/white-house-copper-tariff-plan-stalls-amid-affordability-concerns-sources-say-2026-09-10/
- ETFDB XLB flows: 5d −$53.25M, 1m −$237.5M (as of ~09-19). https://etfdb.com/etf/XLB/
- TradingView/TradeSmith holdings: LIN ~12.8%, NEM ~8.0%, FCX ~6.2%, SHW ~4.75%.
- X/premarket color (untrusted, used only as corroboration): LME copper bid on China physical/refinery maintenance; COPX indication green — **not** used to override nested HEAT-down or Channel 1 relative tape.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': -1.0, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.85, 'leading_sum': -5.0, 'divergence_flagged': True, 'total_score': -1.587, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.463, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.252, 'score': 1.512, 'legs': [{'leg': 'ES', 'pct': -0.07, 'w': 0.6}, {'leg': 'HG', 'pct': 0.66, 'w': 0.3}, {'leg': 'GC', 'pct': 0.9, 'w': 0.1}, {'leg': 'DX', 'pct': -0.02, 'w': -0.3}]}, 'overlay_score': -2.975, 'overlay_raw': -2.975, 'index_carry': -0.124, 'general_total': -0.497, 'skill_multipliers': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 1.25, 'S4_ETF_TAPE': 0.0}, 'llm_confidence': 0.4, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
