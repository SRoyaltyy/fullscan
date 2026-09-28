# Sector Prediction — Basic Materials — 2026-09-28

- news_mode: **on**
- ETF: **XLB**
- rubric: `00_grounding/sectors/basic_materials.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-6.779** (mult 0.85)
- regime: risk_off
- divergence_flagged: **False**
- engine: v2 · tape_anchor **-0.144** (ES -0.53%, HG +0.66%, GC +0.90%, DX -0.02%) · index_carry **-1.535** (general -6.14) · llm_overlay **-5.1** (raw -5.1)

## Channel 1 sector ETF tape

```
ETF XLB vs SPY (yfinance, through 2026-09-25):
  1d: XLB +0.24% | SPY +0.54% | rel -0.30%
  3d: XLB -1.44% | SPY -0.26% | rel -1.18%
  1w: XLB +0.08% | SPY +1.27% | rel -1.19%
  1m: XLB -6.78% | SPY +0.94% | rel -7.72%
```

MEMORY_CONFIRM: Sector **Basic Materials / XLB only**. Memory index unavailable this run (embedding metadata missing) — used injected scoreboard/lessons only. Rolling last-10 dir=0.2 mag=0.2 (n=10); last-30 dir=0.429 mag=0.393 (n=28). Last graded **2026-09-25 flat/flat vs XLB +0.242% / SPY +0.544% / rel −0.302% (dir MISS, mag HIT)**. No open experiment for `sector_basic_materials` (open experiments are Utilities/news only). Active XLB rules checked: **09-24 rate-shock-vs-minority-sleeve is BINDING** — ES=F **−0.53%** / NQ=F **−0.97%** are both beyond ±0.5% and red; chemicals-majority book; do **not** pay a minority Cu/Au sleeve as S1 +1; keep the S0 down sign. **09-25 green-tape/tight-spine resolve-up is OFF** — that lesson needs a *green* live tape; this morning’s tape is red. **09-23 divergence-to-spine** — engine-style flag does **not** fire (leading factors and S4 do not fight); leftover tightness is a *level*, not a same-morning book bid. **09-22 Cu-tightness vs flat-index is OFF** (|NQ| 0.97% > 0.5%, index not dead-flat). **09-18 HEAT-down** — nested MAP HEAT is majority-down (Cu/Al/Au/ag-inputs/other metals/precious/coking coal); XLB **absent** from the Channel 1 PM board (non-print, not a 09-17 participation certificate). **09-21 RS-veto triad OFF** (needs PM red *and* ES/NQ ≥ +1%; futures are red). **09-17 residual-mild-up OFF**. **09-16 oil+gold cash-transmission haircut ON as process** — and those sleeves are **not** even green this morning (GC=F **−3.25%**, oil **up**). **09-15 nested-bid ON as process, OFF as copper-HEAT long** (HEAT Copper **down**). **09-11 four-index ≥ +0.5% OFF** (SPX +0.20 / NQ +0.41 / RTY +0.08 / DJIA +0.11). **09-10 gap-at-open OFF** (no XLB PM). **09-09 / 8/18 full oil-shock co-move OFF as a −2 mandate** — News Judge: Hormuz is standoff/impasse vs Friday easing, **not** a documented fresh kinetic wave, so B1=−3 oil-shock lessons do **not** fire; still do **not** use metals as a floor. **8/14 gold sleeve OFF** (live dump, not a bid). **8/25 / 8/27** — confirmed-up ban / S4 conviction cap (1d rel **−0.30% < 0.5%**). **8/17** — cap severe, not a license for flat. **09-04 / 8/28** — do **not** copy Friday’s −0.30% rel into S4. **China/gold split ON** — gold does **not** cancel China/industrial demand. DO-INSTEAD: last three BM = cut conviction when sign *fights* tape (09-23, 09-25 losses) / keep direction and shrink confidence on modest |score| (09-24 win). **Today sign agrees with tape** (red ES/NQ, HEAT-down, gold dump, XLI −0.77%) — keep **down**, band **mild**. size_gate=True.

## Analysis — XLB, session of 2026-09-28 (Monday cash)

This is a **Monday duration/oil risk-off open**, not a copper-squeeze day and not a same-morning China PMI print (NBS September PMI is **~Sep 30**, still unprinted). Channel 1 tape through **2026-09-25**: 1d XLB **+0.24%** / SPY **+0.54%** / rel **−0.30%**, 3d rel **−1.18%**, 1w rel **−1.19%**, 1m rel **−7.72%**. XLB is **not on the Channel 1 sector PM board**. Analog cyclicals are already red: **XLI −0.77%**, **XLK −0.89%**, **XLF −0.27%**; the only green commodity sleeve on that board is **XLE +1.52%** (oil), which is **not** XLB.

Repeating Friday’s 09-25 error (keep a printed hawkish *level* while the live tape was green and copper was record-tight) would be the wrong sibling: **this morning the live tape is red**. Repeating 09-22 (sign down against green Cu on a *flat* index) is also the wrong sibling: **NQ is −0.97%** and Channel 2 copper is **off**, not squeezing.

### 1. Shared macro as it hits materials (S0)

Knowable this morning (Channel 1, do not re-derive):

- **Futures are red, NQ worse than ES.** ES=F **−0.53%**, NQ=F **−0.97%**. Finviz four-index **fails** ≥ +0.5%. **09-24 condition (red ES/NQ beyond the flat band) is ON.** **09-17 / 09-25 green-tape residuals are OFF.** 8/25: NQ < ES is a duration/tech hit, **not** an XLB green light.
- **Rates are the spine, and they are live.** Channel 1: DGS10 **5.18** (+0.07 1d / +0.54 1m), DFII10 **2.85** (+0.09 1d / +0.53 1m), DGS30 **5.47**. Channel 2: 10Y **~5.22%**, 30Y **~5.53%** (CNBC 5:xx ET). News Judge #1/#2: 10Y at multi-decade highs; October hike odds **~64–69%**. Real yields **rising** maps negative to a chemicals-heavy cyclical. Do **not** re-score the already-printed 09-16 hike as a same-open smash (09-15/09-16); **do** score the *live increment* (yields bid again, hike odds re-accelerating, ES/NQ red).
- **USD firm, not a spike.** DXY **+0.17% 1d / +2.0% 1m**; Finviz USD **−0.02%**. Headwind for the complex, **not** a USD-spike HIT.
- **VIX 16.34 (+1.47)** with VIX/VIX3M **0.911** — stress building, still contango, not panic. HY OAS **2.80** contained. 5-day 10Y–SPX corr **−0.877**. EPU **306** is a *level* spike, not a materials-specific print.
- **Asia composite −0.88%** (Shanghai **−1.67%**, Kospi **−2.7%**, Hang Seng **+0.54%**) — China equity tape is soft; **not** a PMI miss (that print is still ahead). Europe **+0.14%** — not a confirmation either way.
- **Oil up, 8/18 full co-move mandate OFF.** Channel 1 CL=F **+3.79% 1d** vs stale Finviz WTI **−1.59%** — trust the live crude print + News Judge #4 (Hormuz standoff vs Friday’s easing recap). News Judge: **not** a documented fresh kinetic wave, so oil-shock **B1=−3 lessons do not fire**. For *this* book, live oil-up is **feedstock cost** for the chemicals majority and a **risk-off overlay**, not a metals floor.
- **Pending NFP / core PCE** are later-week binaries. Do **not** pre-score them. Today’s signed lean is the **knowable** red futures + rising real yields tape.

**S0 = −1.** Rate/duration increment + red ES/NQ + firm USD + rising real yields + oil-up risk-off map negative to this cyclical. Not −2: DXY is not a spike, HY is contained, VIX is not backwardated-panic, Hormuz is standoff not kinetic, Europe is not red. Not 0: that was Friday’s miss on a *green* tape; 09-24 forbids zeroing S0 when ES/NQ are red beyond 0.5%.

### 2. Spine + secondary (S1)

**Industrial metals — tightness is a LEVEL, live increment is not a surge.** Channel 1 (do not alter): copper **$6.489 (+0.66%)**, aluminum **+1.10%**, iron ore **−0.14%**, steel HRC **−0.16%**. Channel 2 (this morning): COMEX copper **~−2%** toward **$6.56–6.61/lb**; LME still physically tight (cash-3M backwardation, cancelled warrants ~45% of ~251.5 kt, SHFE stocks still low) but **price is not surging into the US open**. Spine **surge OFF**. Spine **collapse OFF** (a ~2% pullback from records is not a collapse HIT). **09-24 / 8/17:** do **not** pay leftover tightness as S1 +1 into a red ES/NQ chemicals book. **09-22** does not protect a down sign today.

**Inventory draw — mixed, not a clean HIT.** Cancelled-warrant tightness remains; total LME tonnes rebuilt vs early-September lows. Do **not** double-count tightness as both surge and draw.

**China demand — carried contraction, not a rebound; do not let gold cancel it (gold is dumping anyway).** August NBS mfg PMI **49.8**, construction ~**46.9**, property still the drag. September official PMI **unprinted**. Shanghai **−1.67%**. Yangshan/SHFE tightness is a *physical* offset already in the copper *level* — **not** a PMI/property rebound HIT.

**Monetary metals — 8/14 INVERTED.** Channel 1 GC=F **−3.25% 1d**. Channel 2: gold futures **−3.34% to $4,176.80**, spot **−3.27%**; silver futures **−5.1%** (CNBC ~5:40 a.m. ET). NEM is ~**7%** of XLB — a **sleeve hit**, not a book bid. Finviz gold **+0.90% / silver +1.96%** is **stale vs live**. MAP HEAT Gold **down**.

**Chemicals majority sleeve — oil-up is cost, not relief.** LIN ~12%, SHW/ECL/APD/DOW are the book. Live WTI/CL bid is the **inverse** of last week’s oil-offered haircut. Premarket color: LIN only **~+0.23%**, SHW **flat**, DOW mixed — **not** a chemicals bid. MAP HEAT Chemicals **flat / low conviction**.

**Tariffs / critical minerals — being repriced, not a tailwind.** MAP HEAT Copper **down** (FCX/IE tariff-doubt). Section 232 refined-copper duties still **unsettled**. Do **not** score a policy plus.

**S1 = −1.** Net of live gold/silver dump + oil-up chemicals cost + carried China/property + nested Cu/Al HEAT-down, versus leftover physical tightness that **must not be paid as +1** (09-24 / 8/17). Not −2/−3: 8/18 full co-move is OFF, copper did not collapse, China PMI is not a same-morning miss. Not 0: 09-24’s “cap at 0” was for a still-green minority sleeve; this morning that sleeve is **red**.

### 3. Breadth / leadership (S2)

Nested MAP HEAT (do **not** average into the parent): Copper **down**, Aluminum **down**, Gold **down**, Ag inputs **down**, Other industrial metals **down**, Other precious **down**, Coking coal **down**. Only Chemicals / Building Materials / Lumber are **flat**. That is **majority-down nested book**, not expansion.

XLB absent from PM board. XLI **−0.77%** is the closest cyclical analog. Do **not** copy Friday’s 1d/3d/1w rel hole into S2 (09-04). Do **not** invent a nested copper long (09-15). 09-25’s “don’t sign breadth negative when XLB is up while cyclicals fall” is **Friday’s autopsy**, not this tape.

**S2 = −1.** Nested majority-down + no parent PM bid. Not −2: chemicals/building materials are flat, not a washout print.

### 4. Flows / positioning (S3)

ETFdb (through ~Sep 25): **5-day −$82.3M**, **1-month −$171.7M**, 3-month still slightly positive. Mid-September saw a ~**$155M** weekly redemption. Volume ~10–13M is **average**, not a dry-up spike. **Crowded long is OFF** (1m rel **−7.72%** is a hole, not an extension). Rotation **out** of materials is the live descriptor; it is **not** a forced index-exclusion event.

**S3 = −1.** Persistent outflows + rotation-out. Not −2: no mechanical forced selling, AUM hit is modest vs ~$8B.

### 5. ETF tape confirmation only (S4)

Channel 1 1d rel **−0.30% < 0.5%** → **8/27 S4-cap ON** (cannot confirm up). 3d/1w/1m rel are **T-1 leftover** — **09-04 / 8/28**: do **not** copy them into S4 as today’s signal.

**S4 = 0.**

### Self-audit

- **Lens:** XLB environment, not SPX, not FCX/NEM/SHW stock-picking. SHW Investor Day is **T-1**.
- **Band:** size_gate + last-10 mag **0.2** + 8/17 severe-cap + unprinted NFP/PCE later this week → **mild**, not notable.
- **Skew:** chemicals/gases are the weight-majority; leftover Cu tightness and Friday’s gold sleeve **cannot** drive the ETF call.
- **Same-shock double-count:** Hormuz/oil scored as **S0 risk-off overlay** and **once** as S1 chemicals cost — not again as an 8/18 metals-collapse or as a magnitude floor.
- **Divergence:** leading S0–S3 = **−4** vs S4 **0**. That is **unconfirmed down**, not a fight. **No flag.** Trust factors over leftover 1m hole *and* over leftover Cu tightness.
- **09-25 vs 09-24:** 09-25 would want S0 zeroed only on a **green** tape with a **live record spine**. Neither condition holds. 09-24 holds.

**Call expression (for the pipeline, not a substitute for it):** absolute **down / mild**, relative lag vs SPY still the structural backdrop. Do **not** flatten this into another 09-08/09-16/09-18/09-21 flat/flat.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: -1
S2_BREADTH: -1
S3_FLOWS_POSITIONING: -1
S4_ETF_TAPE: 0
MULTIPLIER: 0.85
CONFIDENCE: 0.40
REGIME: risk_off
DIVERGENCE: 0
HORIZON_3D: down
HORIZON_1W: down
HORIZON_2W: mixed
HORIZON_1M: down
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|OFF|0.80|2026-09-28|https://investrade.com/morning-preview-september-28-2026/
Risk-off tape / flight to safety|HIT|0.78|2026-09-28|https://www.cnbc.com/2026/09/28/treasury-yields-bonds-selloff.html
Real yields rising|HIT|0.82|2026-09-28|https://www.cnbc.com/2026/09/28/treasury-yields-bonds-selloff.html
Real yields falling|OFF|0.82|2026-09-28|https://www.cnbc.com/2026/09/28/treasury-yields-bonds-selloff.html
USD strengthening|HIT|0.60|2026-09-28|channel1:DXY
USD weakening|OFF|0.60|2026-09-28|channel1:DXY
Sector breadth expansion (% names up)|OFF|0.70|2026-09-28|MAP_HEAT
Sector breadth failure (ETF up, names flat)|OFF|0.55|2026-09-28|MAP_HEAT
Large-cap leadership inside sector|OFF|0.45|2026-09-28|MAP_HEAT
Small/mid leadership inside sector|OFF|0.45|2026-09-28|MAP_HEAT
High-beta leadership inside sector|OFF|0.50|2026-09-28|MAP_HEAT
Low-beta leadership inside sector|WATCH|0.40|2026-09-28|MAP_HEAT
Sector ETF inflow / relative volume spike|OFF|0.70|2026-09-28|https://etfdb.com/etf/XLB/
Sector ETF outflow / volume dry-up|HIT|0.68|2026-09-28|https://etfdb.com/etf/XLB/
Crowded long (extreme relative performance + valuation)|OFF|0.75|2026-09-28|channel1:XLB_1m_rel
Index rebalance / inclusion tailwind|OFF|0.30|2026-09-28|
Index exclusion / forced selling|OFF|0.30|2026-09-28|
Industrial metal price surge (copper/aluminum/iron ore)|OFF|0.72|2026-09-28|https://tradingeconomics.com/commodity/copper
Gold/silver price surge (monetary metals)|OFF|0.88|2026-09-28|https://www.cnbc.com/2026/09/28/gold-silver-prices-bond-yields.html
China PMI / property demand rebound|OFF|0.70|2026-09-28|https://www.lundgreensinvestorinsights.com/next-week-in-china-28-september-2-october-2026/
Inventory draw (LME/exchange stocks down)|WATCH|0.55|2026-09-28|https://thevaultreport.com/lme/copper
Supply disruption (mine/export ban)|OFF|0.62|2026-09-28|https://economictimes.indiatimes.com/markets/commodities/news/oil-price-today-september-28-crude-oil-gains-to-106-as-trump-rejects-iran-deal-where-are-prices-headed/articleshow/134529904.cms
Critical-minerals policy / domestic tariff support|OFF|0.60|2026-09-28|MAP_HEAT_Copper
Industrial metal price collapse|OFF|0.65|2026-09-28|https://tradingeconomics.com/commodity/copper
China demand shock / property stress|HIT|0.66|2026-09-28|https://www.focus-economics.com/countries/china/news/pmi/china-pmi-02-09-2026-manufacturing-and-non-manufacturing-pmis-stay-soft-in-august/
USD spike vs commodity complex|OFF|0.70|2026-09-28|channel1:DXY
Supply glut / new capacity online|WATCH|0.45|2026-09-28|https://thevaultreport.com/lme/copper
Margin compression / cost inflation without pricing power|HIT|0.64|2026-09-28|channel1:CL=F
Sector rotation into materials|OFF|0.70|2026-09-28|channel1:XLB_rel
Sector rotation out of materials|HIT|0.72|2026-09-28|https://etfdb.com/etf/XLB/
HIT_GRID_END

## RESEARCH APPENDIX

**Queries run**
- web_search: `copper LME price inventory cancelled warrants September 28 2026`
- web_search: `gold silver price dump Treasury yields 5% September 28 2026`
- web_search: `China PMI property copper demand September 2026`
- web_search: `XLB premarket LIN SHW FCX NEM DOW September 28 2026`
- web_search: `FedWatch October 2026 hike odds 10 year yield 5.2`
- web_search: `Hormuz oil copper industrial metals risk off September 28 2026`
- web_search: `Section 232 copper tariff FCX XLB September 2026`
- web_search: `XLB ETF flows volume positioning September 2026`
- web_search: `copper price today COMEX LME September 28 2026`
- web_search: `Linde Sherwin Williams Dow Chemical stock premarket September 28 2026`
- web_search: `China official PMI September 30 2026 manufacturing property`
- web_search: `XLB holdings breadth LIN NEM FCX SHW APD ECL September 28`
- web_search: `XLB ETF 5 day fund flows outflows September 2026 ETFdb`
- web_search: `risk on equity market breadth September 28 2026 futures down yields`
- web_fetch: `https://www.cnbc.com/2026/09/28/gold-silver-prices-bond-yields.html`
- web_fetch: `https://www.cnbc.com/2026/09/28/treasury-yields-bonds-selloff.html`
- web_fetch: `https://www.home.saxo/en-gb/content/articles/macro/market-quick-take---fresh-surge-in-crude-oil-on-hormuz-woes-spikes-risk-sentiment---28-september-2026-28092026` (title only; body did not extract)
- web_fetch: `https://tradingeconomics.com/commodity/copper` (403)
- web_fetch: `https://etfdb.com/etf/XLB/` (403)
- x_search: copper/LME/COMEX tightness, gold dump, XLB (2026-09-26..28)
- memory_search: Basic Materials XLB copper China PMI lessons — **disabled** (index metadata missing)

**Key sources and facts taken**

1. **CNBC — Gold and silver prices fall sharply as higher bond yields weigh on metals** — https://www.cnbc.com/2026/09/28/gold-silver-prices-bond-yields.html — fetched 2026-09-28T11:31:20Z — Gold futures **−3.34% to $4,176.80**, spot **−3.27% to $4,145.88** ~5:40 a.m. ET; silver futures **−5.1% to $61.52**; miners lower in premarket. Used for 8/14 inversion / S1 gold sleeve hit.
2. **CNBC — Treasury yields edge higher** — https://www.cnbc.com/2026/09/28/treasury-yields-bonds-selloff.html — fetched 2026-09-28T11:33:13Z — 10Y **5.219% (+3 bp)**, 30Y **5.529% (+2 bp)**, 2Y **4.916% (+5 bp)**; WTI cited **+4% to $96.13**; NFP/PCE/JOLTS this week. Used for S0 real-yield/duration HIT; binaries left unscored.
3. **Economic Times — Oil gains as Trump rejects Iran deal** — https://economictimes.indiatimes.com/markets/commodities/news/oil-price-today-september-28-crude-oil-gains-to-106-as-trump-rejects-iran-deal-where-are-prices-headed/articleshow/134529904.cms — 2026-09-28 — Hormuz **standoff/impasse**, not a confirmed new attack wave; oil bid. Aligns with News Judge: 8/18 kinetic **does not fire**.
4. **The Vault Report — LME copper** — https://thevaultreport.com/lme/copper — as of ~Sep 25 warehouse data — LME cash **~$14,740/t**, 3M **~$14,647/t** (backwardation ~$93/t), stocks **~251.5 kt**, cancelled warrants **~45%**. Used as tightness *level*, not a same-morning surge.
5. **Trading Economics / Metalcharts (search)** — https://tradingeconomics.com/commodity/copper — 2026-09-28 — copper **~$6.57/lb, ~−2%** on the day. Used as Channel 2 live increment vs Channel 1 leftover **+0.66%**.
6. **FocusEconomics / Xinhua (August PMI)** — https://www.focus-economics.com/countries/china/news/pmi/china-pmi-02-09-2026-manufacturing-and-non-manufacturing-pmis-stay-soft-in-august/ — NBS mfg PMI **49.8**, non-mfg **49.0**, construction soft. Used as **carried** China HIT.
7. **Lundgreens — Next week in China 28 Sep–2 Oct 2026** — https://www.lundgreensinvestorinsights.com/next-week-in-china-28-september-2-october-2026/ — official September PMI **not yet released**, due **~Sep 30**. Used to block a same-morning China miss and a rebound HIT.
8. **ETFdb XLB (search; page 403 on fetch)** — https://etfdb.com/etf/XLB/ — 5d **−$82.3M**, 1m **−$171.7M**, 3m **+$57.1M**. Used for S3 outflow HIT.
9. **Nasdaq — XLB big outflow** — https://www.nasdaq.com/articles/state-street-materials-select-sector-spdr-etf-experiences-big-outflow — mid-Sep weekly **~$155M** redemption. Supporting S3.
10. **Yahoo/search premarket LIN/SHW/DOW** — LIN ~**+$0.23%**, SHW **flat**, DOW mixed overnight. Used to refuse a chemicals-led bid.
11. **XLB holdings** — https://stockmarketwatch.com/etf/xlb/holdings — LIN **~11.85%**, NEM **~7.0%**, FCX **~5.7%**, ECL/SHW/APD ~4.5–4.7%. Used for composition skew / single-ticker ban.
12. **Polymarket / FedWatch search** — October 25 bp hike **~55–65%** (Polymarket snapshot **~64.5%**). Aligns with News Judge #2; S0 hawkish overlay.
13. **Investrade morning preview 2026-09-28** — https://investrade.com/morning-preview-september-28-2026/ — ES/NQ/Dow futures lower; oil/yields the drivers. Confirms Channel 1 red futures.
14. **X/Grok posts (Sep 26–28)** — tightness/backwardation still cited into the weekend; **not** used to override Monday’s live Cu pullback + gold dump.

**Checked, nothing material (beyond “no print”):** same-morning China official PMI; XLB on the Channel 1 PM board; index rebalance/exclusion; fresh mine outage distinct from Hormuz standoff; crowded-long valuation extreme.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -1.0, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': -1.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.85, 'leading_sum': -7.0, 'divergence_flagged': False, 'total_score': -6.779, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.65, 'regime': 'risk_off', 'engine': 'v2', 'anchor': {'available': True, 'pct': -0.024, 'score': -0.144, 'legs': [{'leg': 'ES', 'pct': -0.53, 'w': 0.6}, {'leg': 'HG', 'pct': 0.66, 'w': 0.3}, {'leg': 'GC', 'pct': 0.9, 'w': 0.1}, {'leg': 'DX', 'pct': -0.02, 'w': -0.3}]}, 'overlay_score': -5.1, 'overlay_raw': -5.1, 'index_carry': -1.535, 'general_total': -6.14, 'skill_multipliers': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 0.5, 'S3_FLOWS_POSITIONING': 1.0, 'S4_ETF_TAPE': 0.0}, 'llm_confidence': 0.4, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
